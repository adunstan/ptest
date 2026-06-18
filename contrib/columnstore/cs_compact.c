/*-------------------------------------------------------------------------
 *
 * cs_compact.c
 *	  Delta-to-columnar compaction for columnstore tables.
 *
 * When VACUUM is called, we scan the delta store for committed-visible
 * rows, encode them into columnar row groups (one chunk per column),
 * write the column data pages, create a row group catalog entry, and
 * update the metapage.  After compaction, the delta store is truncated.
 *
 * Delta rows are processed in batches of CS_ROWS_PER_ROWGROUP to bound
 * memory usage.  Each batch produces one row group; the per-batch
 * allocations are freed before starting the next batch.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * contrib/columnstore/cs_compact.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cs_internal.h"
#include "access/detoast.h"
#include "access/reloptions.h"
#include "access/generic_xlog.h"
#include "access/heapam.h"
#include "catalog/pg_type.h"
#include "storage/procarray.h"
#include "access/heaptoast.h"
#include "access/htup_details.h"
#include "access/nbtree.h"
#include "access/xact.h"
#include "catalog/index.h"
#include "catalog/pg_collation.h"
#include "commands/defrem.h"
#include "common/pg_lzcompress.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "storage/smgr.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/pg_locale.h"
#include "utils/rel.h"
#include "utils/relcache.h"

#include "utils/datum.h"
#include "utils/numeric.h"

#ifdef USE_LZ4
#include <lz4.h>
#endif

/* Forward declarations for static functions */
static char *cs_compress_column(const char *data, uint32 data_len,
								uint32 *compressed_len, uint8 *method);
static void cs_consider_candidate(char **best_data, uint32 *best_len,
								  uint8 *best_preenc, char *col_data,
								  char *cand_data, uint32 cand_len,
								  uint8 cand_flags);
static char *cs_try_numeric_int64_encode(Datum *values, bool *nulls,
										 uint32 nrows,
										 Form_pg_attribute attr,
										 bool has_nulls,
										 uint32 *encoded_len,
										 Datum **int64_values_out);
static char *cs_try_for_encode(Datum *values, bool *nulls, uint32 nrows,
							   Form_pg_attribute attr, bool has_nulls,
							   uint32 *encoded_len);
static char *cs_try_delta_encode(Datum *values, bool *nulls, uint32 nrows,
								 Form_pg_attribute attr, bool has_nulls,
								 uint32 *encoded_len,
								 Datum **delta_values_out);
static char *cs_try_rle_encode(Datum *values, bool *nulls, uint32 nrows,
							   Form_pg_attribute attr, bool has_nulls,
							   uint32 *encoded_len);
static void cs_sort_batch(Relation rel, TupleDesc tupdesc, int natts,
						  Datum **col_values, bool **col_nulls, uint32 nrows,
						  const char *sort_key);
static char *cs_try_gorilla_encode(Datum *values, bool *nulls, uint32 nrows,
								   Form_pg_attribute attr, bool has_nulls,
								   uint32 *encoded_len);
static int	cs_sort_compare(const void *a, const void *b, void *arg);
static int	cs_datum_cmp(const void *a, const void *b);
static void cs_delta_page_set_fence(Relation rel, Buffer buf, bool fenced);
static char *cs_try_dict_encode(Datum *values, bool *nulls, uint32 nrows,
								Form_pg_attribute attr,
								uint32 *encoded_len);
static BlockNumber cs_write_rowgroup_catalog(Relation rel,
											 CSRowGroupDesc *rg_desc,
											 int16 natts,
											 CSFreeRange *freelist,
											 uint32 *fl_nranges);
static void cs_rebuild_indexes(Relation rel);
static void cs_cache_get_value(CSColumnCache *cache, TupleDesc tupdesc,
							   int col, uint32 row,
							   Datum *val, bool *isnull);
static void cs_free_rowgroup_pages(CSRowGroupDesc *rg_desc, int natts,
								   BlockNumber catalog_block,
								   CSFreeRange **freelist,
								   uint32 *fl_nranges, uint32 *fl_max);
static void cs_update_rowgroup_catalog(Relation rel, BlockNumber catalog_block,
									   CSRowGroupDesc *rg_desc, int16 natts);

/*
 * Numeric internal format definitions, copied from numeric.c.
 *
 * The on-disk numeric format is stable — these definitions are needed
 * to read digit arrays directly for the NUMERIC_INT64 encoding, which
 * converts numeric values to scaled int64 during compaction.
 */
#define CS_NBASE			10000
#define CS_DEC_DIGITS		4

/* Local copies of the numeric on-disk struct layout */
struct cs_NumericShort
{
	uint16		n_header;
	int16		n_data[FLEXIBLE_ARRAY_MEMBER];
};

struct cs_NumericLong
{
	uint16		n_sign_dscale;
	int16		n_weight;
	int16		n_data[FLEXIBLE_ARRAY_MEMBER];
};

union cs_NumericChoice
{
	uint16		n_header;
	struct cs_NumericLong n_long;
	struct cs_NumericShort n_short;
};

struct cs_NumericData
{
	int32		vl_len_;
	union cs_NumericChoice choice;
};

#define CS_NUMERIC_SIGN_MASK	0xC000
#define CS_NUMERIC_POS			0x0000
#define CS_NUMERIC_NEG			0x4000
#define CS_NUMERIC_SHORT		0x8000
#define CS_NUMERIC_SPECIAL		0xC000

#define CS_NUMERIC(n)		((struct cs_NumericData *)(n))
#define CS_NUMERIC_FLAGBITS(n)		(CS_NUMERIC(n)->choice.n_header & CS_NUMERIC_SIGN_MASK)
#define CS_NUMERIC_IS_SHORT(n)		(CS_NUMERIC_FLAGBITS(n) == CS_NUMERIC_SHORT)
#define CS_NUMERIC_IS_SPECIAL(n)	(CS_NUMERIC_FLAGBITS(n) == CS_NUMERIC_SPECIAL)

#define CS_NUMERIC_HEADER_IS_SHORT(n)	((CS_NUMERIC(n)->choice.n_header & 0x8000) != 0)
#define CS_NUMERIC_HEADER_SIZE(n) \
	(VARHDRSZ + sizeof(uint16) + \
	 (CS_NUMERIC_HEADER_IS_SHORT(n) ? 0 : sizeof(int16)))

#define CS_NUMERIC_SHORT_SIGN_MASK		0x2000
#define CS_NUMERIC_SHORT_DSCALE_MASK	0x1F80
#define CS_NUMERIC_SHORT_DSCALE_SHIFT	7
#define CS_NUMERIC_SHORT_WEIGHT_SIGN_MASK	0x0040
#define CS_NUMERIC_SHORT_WEIGHT_MASK	0x003F

#define CS_NUMERIC_DSCALE_MASK		0x3FFF

#define CS_NUMERIC_SIGN(n) \
	(CS_NUMERIC_IS_SHORT(n) ? \
		((CS_NUMERIC(n)->choice.n_short.n_header & CS_NUMERIC_SHORT_SIGN_MASK) ? \
		 CS_NUMERIC_NEG : CS_NUMERIC_POS) : \
		CS_NUMERIC_FLAGBITS(n))
#define CS_NUMERIC_DSCALE(n)	(CS_NUMERIC_HEADER_IS_SHORT(n) ? \
	(CS_NUMERIC(n)->choice.n_short.n_header & CS_NUMERIC_SHORT_DSCALE_MASK) \
		>> CS_NUMERIC_SHORT_DSCALE_SHIFT \
	: (CS_NUMERIC(n)->choice.n_long.n_sign_dscale & CS_NUMERIC_DSCALE_MASK))
#define CS_NUMERIC_WEIGHT(n)	(CS_NUMERIC_HEADER_IS_SHORT(n) ? \
	((CS_NUMERIC(n)->choice.n_short.n_header & CS_NUMERIC_SHORT_WEIGHT_SIGN_MASK ? \
		~CS_NUMERIC_SHORT_WEIGHT_MASK : 0) \
	 | (CS_NUMERIC(n)->choice.n_short.n_header & CS_NUMERIC_SHORT_WEIGHT_MASK)) \
	: (CS_NUMERIC(n)->choice.n_long.n_weight))

#define CS_NUMERIC_DIGITS(n) (CS_NUMERIC_HEADER_IS_SHORT(n) ? \
	CS_NUMERIC(n)->choice.n_short.n_data : CS_NUMERIC(n)->choice.n_long.n_data)
#define CS_NUMERIC_NDIGITS(n) \
	((VARSIZE(n) - CS_NUMERIC_HEADER_SIZE(n)) / sizeof(int16))

/*
 * Minimum number of visible delta rows required before compaction will
 * create a row group.  Prevents autovacuum (with default thresholds) from
 * creating many tiny, poorly-compressed row groups on insert-heavy tables.
 * A manual or autovacuum-triggered VACUUM that finds fewer rows than this
 * simply returns, leaving the delta intact for the next cycle.
 */
#define CS_COMPACT_MIN_DELTA_ROWS	(CS_ROWS_PER_ROWGROUP / 10)

/*
 * Maximum number of distinct values that dictionary encoding supports.
 * Limited by the uint16 dict_count stored in the on-disk header and
 * the 2-byte index width.
 */
#define CS_DICT_MAX_ENTRIES		UINT16_MAX

/*
 * Compare an encoding candidate against the running best for one column
 * during compaction.  If cand_data is non-NULL and shorter than the
 * current best, install it; otherwise free cand_data.  *best_data is
 * never freed when it equals col_data, since col_data is the caller's
 * baseline buffer that it owns separately.
 */
static void
cs_consider_candidate(char **best_data, uint32 *best_len, uint8 *best_preenc,
					  char *col_data, char *cand_data, uint32 cand_len,
					  uint8 cand_flags)
{
	if (cand_data == NULL)
		return;
	if (cand_len < *best_len)
	{
		if (*best_data != col_data)
			pfree(*best_data);
		*best_data = cand_data;
		*best_len = cand_len;
		*best_preenc = cand_flags;
	}
	else
		pfree(cand_data);
}

/*
 * Try to compress column data.  Returns a palloc'd buffer with the
 * compressed data and sets *compressed_len.  If compression doesn't
 * help (result >= original), returns NULL and the caller should store
 * uncompressed data.
 *
 * Sets *method to the compression method used.
 */
static char *
cs_compress_column(const char *data, uint32 data_len,
				   uint32 *compressed_len, uint8 *method)
{
	char	   *compressed;

#ifdef USE_LZ4
	{
		int			max_size = LZ4_compressBound(data_len);
		int			result;

		compressed = palloc(max_size);
		result = LZ4_compress_default(data, compressed, data_len, max_size);
		if (result > 0 && (uint32) result < data_len)
		{
			*compressed_len = result;
			*method = CS_COMPRESS_LZ4;
			return compressed;
		}
		pfree(compressed);
	}
#endif

	/* Try PGLZ as fallback */
	{
		int32		result;

		compressed = palloc(PGLZ_MAX_OUTPUT(data_len));
		result = pglz_compress(data, data_len, compressed, NULL);
		if (result >= 0 && (uint32) result < data_len)
		{
			*compressed_len = result;
			*method = CS_COMPRESS_PGLZ;
			return compressed;
		}
		pfree(compressed);
	}

	*method = CS_COMPRESS_NONE;
	return NULL;
}

/*
 * Convert a numeric column to scaled int64 fixed-point representation.
 *
 * Each numeric value is multiplied by 10^dscale to produce an exact integer,
 * then stored as int64.  All values must have the same dscale, must not be
 * special (NaN/Inf), and must fit in int64 after scaling.
 *
 * On-disk format:
 *   [dscale: uint16] [null_bitmap if has_nulls: (nrows+7)/8 bytes]
 *   [int64 values: 8 bytes × nrows]
 *
 * After this encoding, the result looks like a fixed-width int64 column,
 * so FOR encoding can be applied on top.
 *
 * Returns a palloc'd buffer with the encoded data, or NULL if the column
 * doesn't qualify.  Also returns the int64 Datum arrays (values/nulls) for
 * the FOR encoder to use directly, avoiding a re-parse.
 */
static char *
cs_try_numeric_int64_encode(Datum *values, bool *nulls, uint32 nrows,
							Form_pg_attribute attr, bool has_nulls,
							uint32 *encoded_len,
							Datum **int64_values_out)
{
	static const int64 pow10[] = {
		1, 10, 100, 1000,
		10000, 100000, 1000000, 10000000,
		100000000, 1000000000, 10000000000LL,
		100000000000LL, 1000000000000LL, 10000000000000LL,
		100000000000000LL, 1000000000000000LL, 10000000000000000LL,
		100000000000000000LL, 1000000000000000000LL
	};
	int			dscale = -1;
	int64	   *scaled;
	Datum	   *int64_values;
	uint32		bitmap_size;
	uint32		total_size;
	char	   *result;
	char	   *ptr;

	/* Only applies to numeric (varlena) columns */
	if (attr->attlen != -1 || attr->atttypid != NUMERICOID)
		return NULL;

	scaled = palloc(sizeof(int64) * nrows);

	for (uint32 row = 0; row < nrows; row++)
	{
		Numeric		num;
		int			num_dscale;
		int			num_weight;
		int			num_sign;
		int			ndigits;
		int16	   *digits;
		int64		val;
		int			i;
		int			scale_digits;

		if (nulls[row])
		{
			scaled[row] = 0;
			continue;
		}

		num = DatumGetNumeric(values[row]);

		/* Reject NaN/Inf */
		if (CS_NUMERIC_IS_SPECIAL(num))
		{
			pfree(scaled);
			return NULL;
		}

		num_dscale = CS_NUMERIC_DSCALE(num);
		num_weight = CS_NUMERIC_WEIGHT(num);
		num_sign = CS_NUMERIC_SIGN(num);
		ndigits = CS_NUMERIC_NDIGITS(num);
		digits = CS_NUMERIC_DIGITS(num);

		/* All values must have the same dscale */
		if (dscale == -1)
			dscale = num_dscale;
		else if (num_dscale != dscale)
		{
			pfree(scaled);
			return NULL;
		}

		/*
		 * Accumulate the integer value from base-10000 digits.
		 *
		 * The numeric value is: sign * sum(digits[i] * 10000^(weight - i)) We
		 * need: sign * value * 10^dscale
		 *
		 * The integer part uses digits 0..weight, and fractional digits come
		 * after that.  We need (weight + 1 + ceil(dscale/4)) digits worth of
		 * precision, all accumulated into int64.
		 */
		val = 0;
		scale_digits = (dscale + CS_DEC_DIGITS - 1) / CS_DEC_DIGITS;

		for (i = 0; i < ndigits && i <= num_weight + scale_digits; i++)
		{
			/*
			 * >= rather than >: at exactly PG_INT64_MAX / CS_NBASE the
			 * multiply still fits but adding the next base-10000 digit can
			 * overflow.
			 */
			if (unlikely(val >= PG_INT64_MAX / CS_NBASE))
			{
				pfree(scaled);
				return NULL;
			}
			val = val * CS_NBASE + digits[i];
		}

		/*
		 * Pad with trailing zeros if we ran out of digits before reaching the
		 * required scale position.
		 */
		for (; i <= num_weight + scale_digits; i++)
		{
			if (unlikely(val >= PG_INT64_MAX / CS_NBASE))
			{
				pfree(scaled);
				return NULL;
			}
			val = val * CS_NBASE;
		}

		/*
		 * Now val represents the numeric with DEC_DIGITS*scale_digits decimal
		 * places.  If dscale isn't a multiple of DEC_DIGITS, we may have
		 * extra precision — divide off the excess.
		 */
		{
			int			extra = scale_digits * CS_DEC_DIGITS - dscale;

			if (extra > 0)
				val /= pow10[extra];
		}

		if (num_sign == CS_NUMERIC_NEG)
			val = -val;

		scaled[row] = val;
	}

	if (dscale < 0)
		dscale = 0;				/* all NULLs — use dscale 0 */

	/* Build the on-disk format: [dscale:2] [null_bitmap] [int64 array] */
	bitmap_size = has_nulls ? (nrows + 7) / 8 : 0;
	total_size = sizeof(uint16) + bitmap_size + sizeof(int64) * nrows;
	result = palloc(total_size);
	ptr = result;

	/* dscale header */
	{
		uint16		ds = (uint16) dscale;

		memcpy(ptr, &ds, sizeof(uint16));
		ptr += sizeof(uint16);
	}

	/* null bitmap */
	if (has_nulls)
	{
		memset(ptr, 0, bitmap_size);
		for (uint32 row = 0; row < nrows; row++)
		{
			if (!nulls[row])
				CS_SET_NOTNULL(ptr, row);
		}
		ptr += bitmap_size;
	}

	/* int64 values */
	memcpy(ptr, scaled, sizeof(int64) * nrows);

	*encoded_len = total_size;

	/* Return the int64 Datum array for FOR to use */
	int64_values = palloc(sizeof(Datum) * nrows);
	for (uint32 row = 0; row < nrows; row++)
		int64_values[row] = Int64GetDatum(scaled[row]);
	*int64_values_out = int64_values;

	pfree(scaled);
	return result;
}

/*
 * Frame-of-Reference (FOR) + bit-packing encoder for fixed-width integer
 * columns (int16, int32, int64, date, timestamp).
 *
 * Encodes the value array as: min_value + bit-packed deltas.  Each delta
 * uses the minimum number of bits to represent (max_value - min_value).
 *
 * On-disk format (after null bitmap, which is handled separately):
 *   [min_value: attlen bytes] [bits_per_value: 1 byte]
 *   [bit-packed deltas: ceil(nrows * bits_per_value / 8) bytes]
 *
 * NULL rows are stored as delta=0; the null bitmap disambiguates.
 *
 * Returns a palloc'd buffer with the FOR-encoded data, or NULL if the
 * encoding doesn't help (e.g., the range is too wide to save space).
 * The null bitmap prefix (if has_nulls) is included in the output.
 */
static char *
cs_try_for_encode(Datum *values, bool *nulls, uint32 nrows,
				  Form_pg_attribute attr, bool has_nulls, uint32 *encoded_len)
{
	int			attlen = attr->attlen;
	uint64		min_val = PG_UINT64_MAX;
	uint64		max_val = 0;
	uint64		range;
	int			bits_per_value;
	uint32		bitmap_size;
	uint32		for_header_size;
	uint32		for_body_size;
	uint32		total_size;
	uint32		orig_size;
	char	   *result;
	char	   *ptr;
	uint64		bit_buffer = 0;
	int			bits_in_buffer = 0;

	/* Only support 2, 4, 8 byte by-value integer types */
	if (!attr->attbyval || (attlen != 2 && attlen != 4 && attlen != 8))
		return NULL;

	/* Find min/max over non-null values */
	{
		bool		found = false;

		for (uint32 row = 0; row < nrows; row++)
		{
			uint64		v;

			if (nulls[row])
				continue;

			switch (attlen)
			{
				case 2:
					v = (uint64) DatumGetUInt16(values[row]);
					break;
				case 4:
					v = (uint64) DatumGetUInt32(values[row]);
					break;
				case 8:
					v = DatumGetUInt64(values[row]);
					break;
				default:
					pg_unreachable();
			}

			if (!found)
			{
				min_val = max_val = v;
				found = true;
			}
			else
			{
				if (v < min_val)
					min_val = v;
				if (v > max_val)
					max_val = v;
			}
		}

		if (!found)
			return NULL;		/* all NULLs -- nothing to encode */
	}

	range = max_val - min_val;

	/* Compute bits needed for the range */
	if (range == 0)
		bits_per_value = 0;
	else
	{
		bits_per_value = pg_leftmost_one_pos64(range) + 1;
		Assert(bits_per_value > 0 && bits_per_value <= attlen * 8);
	}

	/* Check if FOR encoding actually saves space */
	bitmap_size = has_nulls ? (nrows + 7) / 8 : 0;
	for_header_size = attlen + 1;	/* min_value + bits_per_value byte */
	for_body_size = ((uint64) nrows * bits_per_value + 7) / 8;
	total_size = bitmap_size + for_header_size + for_body_size;
	orig_size = bitmap_size + (uint32) attlen * nrows;

	/* Require at least 20% savings to justify the decode overhead */
	if (total_size >= orig_size * 4 / 5)
		return NULL;

	/* Encode */
	result = palloc(total_size);
	ptr = result;

	/* Copy null bitmap if present */
	if (has_nulls)
	{
		memset(ptr, 0, bitmap_size);
		for (uint32 row = 0; row < nrows; row++)
		{
			if (!nulls[row])
				CS_SET_NOTNULL(ptr, row);
		}
		ptr += bitmap_size;
	}

	/* Write header: min_value + bits_per_value */
	cs_store_att_byval(ptr, UInt64GetDatum(min_val), attlen);
	ptr += attlen;
	*ptr++ = (uint8) bits_per_value;

	/* Bit-pack deltas */
	if (bits_per_value > 0)
	{
		char	   *out = ptr;

		memset(out, 0, for_body_size);

		for (uint32 row = 0; row < nrows; row++)
		{
			uint64		delta;

			if (nulls[row])
				delta = 0;
			else
			{
				uint64		v;

				switch (attlen)
				{
					case 2:
						v = (uint64) DatumGetUInt16(values[row]);
						break;
					case 4:
						v = (uint64) DatumGetUInt32(values[row]);
						break;
					case 8:
						v = DatumGetUInt64(values[row]);
						break;
					default:
						pg_unreachable();
				}
				delta = v - min_val;
			}

			bit_buffer |= (delta << bits_in_buffer);
			bits_in_buffer += bits_per_value;

			while (bits_in_buffer >= 8)
			{
				*out++ = (uint8) (bit_buffer & 0xFF);
				bit_buffer >>= 8;
				bits_in_buffer -= 8;
			}
		}

		/* Flush remaining bits */
		if (bits_in_buffer > 0)
			*out++ = (uint8) (bit_buffer & 0xFF);
	}

	*encoded_len = total_size;
	return result;
}

/*
 * Try delta encoding on an integer column.
 *
 * Stores value[i] - value[i-1] (with the first non-null value stored as the
 * base).  The encoded buffer has the same size as the raw serialization, so
 * delta encoding on its own does not save space.  The benefit comes from
 * reducing the value range so that a subsequent FOR bit-packing or LZ4
 * compression is much more effective.
 *
 * Returns a palloc'd buffer and sets *encoded_len, plus a Datum array
 * of the computed deltas in *delta_values_out (caller can pass these to
 * cs_try_for_encode for the DELTA+FOR combination).  Returns NULL if
 * delta encoding is not applicable or not beneficial.
 *
 * On-disk format:
 *   [null_bitmap: (nrows+7)/8 bytes, if has_nulls]
 *   [base_value: attlen bytes]
 *   [delta array: attlen * nrows bytes, signed per-row differences]
 *
 * Heuristic: the range of deltas must be smaller than the range of absolute
 * values, otherwise delta won't help subsequent FOR/LZ4.
 */
static char *
cs_try_delta_encode(Datum *values, bool *nulls, uint32 nrows,
					Form_pg_attribute attr, bool has_nulls,
					uint32 *encoded_len, Datum **delta_values_out)
{
	int			attlen = attr->attlen;
	uint64		min_val = PG_UINT64_MAX;
	uint64		max_val = 0;
	uint64		abs_range;
	uint64		delta_min;
	uint64		delta_max;
	uint64		delta_range;
	uint64		prev;
	bool		found;
	uint32		bitmap_size;
	uint32		total_size;
	char	   *result;
	char	   *ptr;
	Datum	   *delta_values;

	*delta_values_out = NULL;

	/* Only support 2, 4, 8 byte by-value integer types */
	if (!attr->attbyval || (attlen != 2 && attlen != 4 && attlen != 8))
		return NULL;

	/*
	 * First pass: find min/max of absolute values and compute deltas to check
	 * whether the delta range is smaller than the absolute range.
	 */
	delta_values = palloc(sizeof(Datum) * nrows);
	found = false;
	prev = 0;
	delta_min = PG_UINT64_MAX;
	delta_max = 0;

	for (uint32 row = 0; row < nrows; row++)
	{
		uint64		v;
		uint64		d;

		if (nulls[row])
		{
			delta_values[row] = (Datum) 0;
			continue;
		}

		switch (attlen)
		{
			case 2:
				v = (uint64) DatumGetUInt16(values[row]);
				break;
			case 4:
				v = (uint64) DatumGetUInt32(values[row]);
				break;
			case 8:
				v = DatumGetUInt64(values[row]);
				break;
			default:
				pg_unreachable();
		}

		if (!found)
		{
			min_val = max_val = v;
			found = true;
			/* First non-null: store absolute value as the initial delta */
			d = v;
		}
		else
		{
			if (v < min_val)
				min_val = v;
			if (v > max_val)
				max_val = v;
			d = v - prev;

			/*
			 * Track delta range only for inter-row deltas (skip the first
			 * non-null which stores the absolute base value).
			 */
			if (d < delta_min)
				delta_min = d;
			if (d > delta_max)
				delta_max = d;
		}

		delta_values[row] = (Datum) d;
		prev = v;
	}

	if (!found)
	{
		pfree(delta_values);
		return NULL;			/* all NULLs */
	}

	/*
	 * If we never computed any inter-row deltas (only one non-null value),
	 * delta encoding is pointless.
	 */
	if (delta_min == PG_UINT64_MAX)
	{
		pfree(delta_values);
		return NULL;
	}

	abs_range = max_val - min_val;
	delta_range = delta_max - delta_min;

	/*
	 * If the delta range is not strictly smaller than the absolute range,
	 * delta encoding won't help FOR or LZ4.  Bail out.
	 */
	if (delta_range >= abs_range)
	{
		pfree(delta_values);
		return NULL;
	}

	/*
	 * Serialize: [null_bitmap] [base_value] [delta array]
	 */
	bitmap_size = has_nulls ? (nrows + 7) / 8 : 0;
	total_size = bitmap_size + attlen + (uint32) attlen * nrows;

	result = palloc(total_size);
	ptr = result;

	/* Write null bitmap */
	if (has_nulls)
	{
		memset(ptr, 0, bitmap_size);
		for (uint32 row = 0; row < nrows; row++)
		{
			if (!nulls[row])
				CS_SET_NOTNULL(ptr, row);
		}
		ptr += bitmap_size;
	}

	/* Write base value (first non-null absolute value) */
	cs_store_att_byval(ptr, UInt64GetDatum(min_val == max_val ? min_val : 0), attlen);

	/*
	 * We store base as the first non-null value, not min.  Recompute deltas
	 * relative to the predecessor (this was already done above -- the delta
	 * array is correct).  But we need to store the actual base value.
	 */
	{
		/* Find the first non-null value to use as base */
		for (uint32 row = 0; row < nrows; row++)
		{
			if (!nulls[row])
			{
				uint64		base;

				switch (attlen)
				{
					case 2:
						base = (uint64) DatumGetUInt16(values[row]);
						break;
					case 4:
						base = (uint64) DatumGetUInt32(values[row]);
						break;
					case 8:
						base = DatumGetUInt64(values[row]);
						break;
					default:
						pg_unreachable();
				}
				cs_store_att_byval(ptr, UInt64GetDatum(base), attlen);
				break;
			}
		}
	}
	ptr += attlen;

	/* Write delta array */
	for (uint32 row = 0; row < nrows; row++)
	{
		cs_store_att_byval(ptr, delta_values[row], attlen);
		ptr += attlen;
	}

	Assert(ptr == result + total_size);

	*encoded_len = total_size;
	*delta_values_out = delta_values;
	return result;
}

/*
 * Try run-length encoding on a column.
 *
 * Replaces consecutive identical values with (value, run_length) pairs.
 * Works for both by-value and variable-length types.  NULL rows use a
 * sentinel value (the null bitmap disambiguates).
 *
 * Returns a palloc'd buffer and sets *encoded_len, or NULL if RLE is not
 * worthwhile (too many runs relative to row count).
 *
 * On-disk format:
 *   [null_bitmap: (nrows+7)/8 bytes, if has_nulls]
 *   [4 bytes: num_runs (uint32)]
 *   For by-value: [per run: {value: attlen bytes} {count: uint32}] * num_runs
 *   For varlen:   [per run: {len: int32} {data: len bytes} {count: uint32}]
 */
static char *
cs_try_rle_encode(Datum *values, bool *nulls, uint32 nrows,
				  Form_pg_attribute attr, bool has_nulls,
				  uint32 *encoded_len)
{
	uint32		num_runs;
	uint32		bitmap_size;
	uint32		header_size;
	uint32		runs_size;
	uint32		total_size;
	uint32		orig_size;
	char	   *result;
	char	   *ptr;

	if (nrows == 0)
		return NULL;

	/* RLE does not apply to fixed-length by-reference types */
	if (!attr->attbyval && attr->attlen > 0)
		return NULL;

	/*
	 * First pass: count runs.  Two consecutive rows are in the same run if
	 * both are null, or both are non-null with the same value.
	 */
	num_runs = 1;
	for (uint32 row = 1; row < nrows; row++)
	{
		if (nulls[row] != nulls[row - 1])
		{
			num_runs++;
			continue;
		}

		if (nulls[row])
			continue;			/* both null -- same run */

		if (attr->attbyval && attr->attlen > 0)
		{
			if (values[row] != values[row - 1])
				num_runs++;
		}
		else if (attr->attlen == -1)
		{
			/* Variable-length: compare detoasted bytes */
			struct varlena *va = (struct varlena *) DatumGetPointer(values[row]);
			struct varlena *vb = (struct varlena *) DatumGetPointer(values[row - 1]);
			Size		la = VARSIZE_ANY_EXHDR(va);
			Size		lb = VARSIZE_ANY_EXHDR(vb);

			if (la != lb || memcmp(VARDATA_ANY(va), VARDATA_ANY(vb), la) != 0)
				num_runs++;
		}
		else if (attr->attlen == -2)
		{
			/* cstring */
			if (strcmp(DatumGetCString(values[row]),
					   DatumGetCString(values[row - 1])) != 0)
				num_runs++;
		}
		else
		{
			/* Fixed-length by-reference */
			if (memcmp(DatumGetPointer(values[row]),
					   DatumGetPointer(values[row - 1]),
					   attr->attlen) != 0)
				num_runs++;
		}
	}

	/* Require at least 75% reduction in entries (num_runs <= nrows/4) */
	if (num_runs > nrows / 4)
		return NULL;

	/*
	 * Estimate encoded size.  For by-value types the size is exact.  For
	 * varlen types we compute the exact size with a second pass below.
	 */
	bitmap_size = has_nulls ? (nrows + 7) / 8 : 0;
	header_size = sizeof(uint32);	/* num_runs */

	if (attr->attbyval && attr->attlen > 0)
	{
		runs_size = num_runs * ((uint32) attr->attlen + sizeof(uint32));
		total_size = bitmap_size + header_size + runs_size;
		orig_size = bitmap_size + (uint32) attr->attlen * nrows;

		/* Require 20% savings */
		if (total_size >= orig_size * 4 / 5)
			return NULL;
	}
	else
	{
		/*
		 * For varlen types, we need to compute the exact serialized size by
		 * scanning the run values.
		 */
		runs_size = 0;
		{
			uint32		run_idx PG_USED_FOR_ASSERTS_ONLY = 0;

			for (uint32 row = 0; row < nrows;)
			{
				uint32		run_end = row + 1;

				while (run_end < nrows)
				{
					if (nulls[run_end] != nulls[row])
						break;
					if (nulls[run_end])
					{
						run_end++;
						continue;
					}

					if (attr->attlen == -1)
					{
						struct varlena *va = (struct varlena *) DatumGetPointer(values[run_end]);
						struct varlena *vb = (struct varlena *) DatumGetPointer(values[row]);
						Size		la = VARSIZE_ANY_EXHDR(va);
						Size		lb = VARSIZE_ANY_EXHDR(vb);

						if (la != lb || memcmp(VARDATA_ANY(va), VARDATA_ANY(vb), la) != 0)
							break;
					}
					else if (attr->attlen == -2)
					{
						if (strcmp(DatumGetCString(values[run_end]),
								   DatumGetCString(values[row])) != 0)
							break;
					}
					else
					{
						if (memcmp(DatumGetPointer(values[run_end]),
								   DatumGetPointer(values[row]),
								   attr->attlen) != 0)
							break;
					}
					run_end++;
				}

				/* Value size for this run */
				if (nulls[row])
				{
					/* NULL run: no value stored, just the count */
					runs_size += sizeof(uint32);
				}
				else if (attr->attlen == -1)
				{
					struct varlena *v = (struct varlena *) DatumGetPointer(values[row]);

					runs_size += sizeof(int32) + VARSIZE_ANY(v) + sizeof(uint32);
				}
				else if (attr->attlen == -2)
				{
					runs_size += sizeof(int32) + strlen(DatumGetCString(values[row])) + 1 + sizeof(uint32);
				}
				else
				{
					runs_size += sizeof(int32) + attr->attlen + sizeof(uint32);
				}

				row = run_end;
				run_idx++;
			}

			Assert(run_idx == num_runs);
		}

		total_size = bitmap_size + header_size + sizeof(uint8) + runs_size;

		/*
		 * Compare against a rough estimate of original varlen size.  We can't
		 * easily compute the exact original size without scanning, so use the
		 * runs_size which already accounts for deduplication.  Just check
		 * that num_runs is small enough (the 75% threshold above already
		 * ensures significant reduction).
		 */
	}

	/*
	 * Second pass: serialize the RLE data.
	 */
	result = palloc(total_size);
	ptr = result;

	/* Write null bitmap */
	if (has_nulls)
	{
		memset(ptr, 0, bitmap_size);
		for (uint32 row = 0; row < nrows; row++)
		{
			if (!nulls[row])
				CS_SET_NOTNULL(ptr, row);
		}
		ptr += bitmap_size;
	}

	/* Write num_runs header */
	memcpy(ptr, &num_runs, sizeof(uint32));
	ptr += sizeof(uint32);

	/* For varlen types, write a has_null flag byte */
	if (!(attr->attbyval && attr->attlen > 0))
	{
		uint8		hn = has_nulls ? 1 : 0;

		memcpy(ptr, &hn, sizeof(uint8));
		ptr += sizeof(uint8);
	}

	/* Write runs */
	for (uint32 row = 0; row < nrows;)
	{
		uint32		run_end = row + 1;
		uint32		count;

		/* Find end of this run */
		while (run_end < nrows)
		{
			if (nulls[run_end] != nulls[row])
				break;
			if (nulls[run_end])
			{
				run_end++;
				continue;
			}

			if (attr->attbyval && attr->attlen > 0)
			{
				if (values[run_end] != values[row])
					break;
			}
			else if (attr->attlen == -1)
			{
				struct varlena *va = (struct varlena *) DatumGetPointer(values[run_end]);
				struct varlena *vb = (struct varlena *) DatumGetPointer(values[row]);
				Size		la = VARSIZE_ANY_EXHDR(va);
				Size		lb = VARSIZE_ANY_EXHDR(vb);

				if (la != lb || memcmp(VARDATA_ANY(va), VARDATA_ANY(vb), la) != 0)
					break;
			}
			else if (attr->attlen == -2)
			{
				if (strcmp(DatumGetCString(values[run_end]),
						   DatumGetCString(values[row])) != 0)
					break;
			}
			else
			{
				if (memcmp(DatumGetPointer(values[run_end]),
						   DatumGetPointer(values[row]),
						   attr->attlen) != 0)
					break;
			}
			run_end++;
		}

		count = run_end - row;

		/* Write value + count */
		if (attr->attbyval && attr->attlen > 0)
		{
			Datum		val = nulls[row] ? (Datum) 0 : values[row];

			cs_store_att_byval(ptr, val, attr->attlen);
			ptr += attr->attlen;
			memcpy(ptr, &count, sizeof(uint32));
			ptr += sizeof(uint32);
		}
		else if (nulls[row])
		{
			/* NULL run: just the count */
			memcpy(ptr, &count, sizeof(uint32));
			ptr += sizeof(uint32);
		}
		else if (attr->attlen == -1)
		{
			struct varlena *v = (struct varlena *) DatumGetPointer(values[row]);
			int32		len = VARSIZE_ANY(v);

			memcpy(ptr, &len, sizeof(int32));
			ptr += sizeof(int32);
			memcpy(ptr, DatumGetPointer(values[row]), len);
			ptr += len;
			memcpy(ptr, &count, sizeof(uint32));
			ptr += sizeof(uint32);
		}
		else if (attr->attlen == -2)
		{
			int32		len = strlen(DatumGetCString(values[row])) + 1;

			memcpy(ptr, &len, sizeof(int32));
			ptr += sizeof(int32);
			memcpy(ptr, DatumGetCString(values[row]), len);
			ptr += len;
			memcpy(ptr, &count, sizeof(uint32));
			ptr += sizeof(uint32);
		}
		else
		{
			/* Fixed-length by-reference */
			int32		len = attr->attlen;

			memcpy(ptr, &len, sizeof(int32));
			ptr += sizeof(int32);
			memcpy(ptr, DatumGetPointer(values[row]), len);
			ptr += len;
			memcpy(ptr, &count, sizeof(uint32));
			ptr += sizeof(uint32);
		}

		row = run_end;
	}

	Assert(ptr == result + total_size);

	*encoded_len = total_size;
	return result;
}

/*
 * Context for cs_sort_compare: passed through qsort_arg.
 */
typedef struct CSSortContext
{
	Datum	  **col_values;
	bool	  **col_nulls;
	int		   *sort_col_indices;	/* 0-based column indices */
	int			nsort_cols;
	FmgrInfo   *cmp_finfos;
	Oid		   *collations;
} CSSortContext;

/*
 * qsort_arg comparator for sorting row indices by sort key columns.
 */
static int
cs_sort_compare(const void *a, const void *b, void *arg)
{
	uint32		row_a = *(const uint32 *) a;
	uint32		row_b = *(const uint32 *) b;
	CSSortContext *ctx = (CSSortContext *) arg;
	int			i;

	for (i = 0; i < ctx->nsort_cols; i++)
	{
		int			col = ctx->sort_col_indices[i];
		bool		null_a = ctx->col_nulls[col][row_a];
		bool		null_b = ctx->col_nulls[col][row_b];
		int32		cmp;

		/* NULLs sort last */
		if (null_a && null_b)
			continue;
		if (null_a)
			return 1;
		if (null_b)
			return -1;

		cmp = DatumGetInt32(FunctionCall2Coll(&ctx->cmp_finfos[i],
											  ctx->collations[i],
											  ctx->col_values[col][row_a],
											  ctx->col_values[col][row_b]));
		if (cmp != 0)
			return cmp;
	}
	return 0;
}

/*
 * Gorilla encoding: delta-of-delta for integers, XOR for floats.
 *
 * For integer types (int2/int4/int8): stores delta-of-delta values using
 * variable-length prefix codes.  Effective for timestamps and counters
 * with near-constant intervals -- a single outlier costs only that one
 * value, while fixed-width delta+FOR sizes all values to the worst case.
 *
 * For float types (float4/float8): XOR each value with its predecessor,
 * then variable-length encode the XOR using leading/trailing zero tracking.
 * Effective for slowly-changing sensor readings where successive values
 * share most of their bits.
 *
 * On-disk format:
 *   [null_bitmap: (nrows+7)/8 bytes, if has_nulls]
 *   [1 byte: type_tag (0 = DoD-integer, 1 = XOR-float)]
 *   [attlen bytes: first non-null value]
 *   [variable: bit-packed stream]
 *
 * DoD prefix codes (signed delta-of-delta):
 *   0             -> DoD = 0                        (1 bit)
 *   10 + 7 bits   -> DoD in [-63, 64]              (9 bits)
 *   110 + 10 bits -> DoD in [-511, 512]             (13 bits)
 *   1110 + 13 bits -> DoD in [-4095, 4096]          (16 bits)
 *   1111 + attlen*8 bits -> full value              (4 + attlen*8 bits)
 *
 * XOR prefix codes:
 *   0             -> XOR = 0 (same as previous)     (1 bit)
 *   10 + bits     -> meaningful bits within prior window (2 + N bits)
 *   11 + 6 + 6 + bits -> new window                (14 + N bits)
 */
static char *
cs_try_gorilla_encode(Datum *values, bool *nulls, uint32 nrows,
					  Form_pg_attribute attr, bool has_nulls,
					  uint32 *encoded_len)
{
	int			attlen = attr->attlen;
	bool		is_float;
	uint32		bitmap_size;
	uint32		raw_size;
	char	   *result;
	uint8	   *out;
	uint32		out_pos;
	uint64		bit_buffer = 0;
	int			bits_in_buffer = 0;
	uint32		max_out_size;

	/* Support only fixed-width by-value types */
	if (!attr->attbyval)
		return NULL;

	is_float = (attr->atttypid == FLOAT4OID || attr->atttypid == FLOAT8OID);

	/* DoD: int2/int4/int8 only */
	if (!is_float && attlen != 2 && attlen != 4 && attlen != 8)
		return NULL;

	/* XOR: float4/float8 only */
	if (is_float && attlen != 4 && attlen != 8)
		return NULL;

	bitmap_size = has_nulls ? (nrows + 7) / 8 : 0;
	raw_size = bitmap_size + (uint32) attlen * nrows;

	/*
	 * Allocate worst-case output buffer.  Worst case is every value needing
	 * the full-width encoding (4 prefix bits + attlen*8 data bits per value),
	 * which is slightly larger than the raw size.  We'll check the actual
	 * size at the end.
	 */

	/*
	 * Worst case per value: 2 control bits plus the widest window header and
	 * a full-width payload.  For the float XOR "new window" case that is 2 +
	 * 6 (leading) + 6 (length) + attlen*8 meaningful bits; for the integer
	 * DoD '1111' fallback it is 4 + attlen*8.  Both are below (2 + attlen)
	 * bytes per row (16 + attlen*8 bits), so budget that, plus the raw first
	 * value, the null bitmap, the type tag, and a byte of flush slack.  A
	 * per-row guard in the loops below is a hard backstop.
	 */
	max_out_size = bitmap_size + 1 + attlen +
		(uint32) nrows * (2 + attlen) + 8;
	result = palloc(max_out_size);
	out = (uint8 *) result;
	out_pos = 0;

	/* Write null bitmap */
	if (has_nulls)
	{
		memset(result, 0, bitmap_size);
		for (uint32 row = 0; row < nrows; row++)
		{
			if (!nulls[row])
				CS_SET_NOTNULL(result, row);
		}
		out_pos = bitmap_size;
	}

	/* Write type tag */
	out[out_pos++] = is_float ? 1 : 0;

/* Helper: append N bits to the output buffer */
#define GORILLA_PUT_BITS(val, nbits) \
	do { \
		bit_buffer |= ((uint64)(val)) << bits_in_buffer; \
		bits_in_buffer += (nbits); \
		while (bits_in_buffer >= 8) { \
			out[out_pos++] = (uint8)(bit_buffer & 0xFF); \
			bit_buffer >>= 8; \
			bits_in_buffer -= 8; \
		} \
	} while (0)

/*
 * Append N (1..64) bits.  GORILLA_PUT_BITS shifts the value into a 64-bit
 * staging buffer that can already hold up to 7 bits, so a single call is
 * only safe below 58 bits; split wider payloads (XOR windows reach the
 * full 64 bits of a float8) into two halves.
 */
#define GORILLA_PUT_BITS_WIDE(val, nbits) \
	do { \
		if ((nbits) > 32) \
		{ \
			GORILLA_PUT_BITS((val) & 0xFFFFFFFF, 32); \
			GORILLA_PUT_BITS((val) >> 32, (nbits) - 32); \
		} \
		else \
			GORILLA_PUT_BITS((val), (nbits)); \
	} while (0)

	if (!is_float)
	{
		/*
		 * Delta-of-delta encoding for integers.
		 */
		int64		prev_val = 0;
		int64		prev_delta = 0;
		bool		found_first = false;

		for (uint32 row = 0; row < nrows; row++)
		{
			int64		val;
			int64		delta;
			int64		dod;

			if (nulls[row])
				continue;

			/* Hard backstop against ever writing past the buffer. */
			if (out_pos + attlen + 4 > max_out_size)
			{
				pfree(result);
				return NULL;
			}

			switch (attlen)
			{
				case 2:
					val = (int64) DatumGetInt16(values[row]);
					break;
				case 4:
					val = (int64) DatumGetInt32(values[row]);
					break;
				case 8:
					val = DatumGetInt64(values[row]);
					break;
				default:
					pg_unreachable();
			}

			if (!found_first)
			{
				/* Write first value raw */
				cs_store_att_byval((char *) (out + out_pos),
								   Int64GetDatum(val), attlen);
				out_pos += attlen;
				prev_val = val;
				prev_delta = 0;
				found_first = true;
				continue;
			}

			delta = val - prev_val;
			dod = delta - prev_delta;

			if (dod == 0)
			{
				GORILLA_PUT_BITS(0, 1); /* '0' */
			}
			else if (dod >= -63 && dod <= 64)
			{
				GORILLA_PUT_BITS(0x01 | ((uint64) (dod + 63) << 2), 9);
				/* '10' prefix (LSB-first: 1,0) + 7 bits value */
			}
			else if (dod >= -511 && dod <= 512)
			{
				GORILLA_PUT_BITS(0x03 | ((uint64) (dod + 511) << 3), 13);
				/* '110' prefix (LSB-first: 1,1,0) + 10 bits value */
			}
			else if (dod >= -4095 && dod <= 4096)
			{
				GORILLA_PUT_BITS(0x07 | ((uint64) (dod + 4095) << 4), 17);
				/* '1110' prefix (LSB-first: 1,1,1,0) + 13 bits value */
			}
			else
			{
				/*
				 * '1111' prefix + full-width absolute value.  Write the
				 * prefix, flush the bit buffer, then write the value as raw
				 * bytes to avoid bit_buffer overflow for 64-bit types.
				 */
				GORILLA_PUT_BITS(0x0F, 4);
				if (bits_in_buffer > 0)
				{
					out[out_pos++] = (uint8) (bit_buffer & 0xFF);
					bit_buffer = 0;
					bits_in_buffer = 0;
				}
				cs_store_att_byval((char *) (out + out_pos),
								   Int64GetDatum(val), attlen);
				out_pos += attlen;

				/*
				 * Store absolute value for large DoD to avoid error
				 * accumulation
				 */
				prev_val = val;
				prev_delta = delta;
				continue;
			}

			prev_val = val;
			prev_delta = delta;
		}
	}
	else
	{
		/*
		 * XOR encoding for floats.
		 */
		uint64		prev_val = 0;
		uint8		prev_leading = 0;
		uint8		prev_trailing = 0;
		bool		found_first = false;
		int			val_bits = attlen * 8;

		for (uint32 row = 0; row < nrows; row++)
		{
			uint64		val;
			uint64		xor_val;

			if (nulls[row])
				continue;

			/* Hard backstop against ever writing past the buffer. */
			if (out_pos + attlen + 4 > max_out_size)
			{
				pfree(result);
				return NULL;
			}

			if (attlen == 4)
			{
				union
				{
					float		f;
					uint32		u;
				}			conv;

				conv.f = DatumGetFloat4(values[row]);
				val = (uint64) conv.u;
			}
			else
			{
				union
				{
					double		d;
					uint64		u;
				}			conv;

				conv.d = DatumGetFloat8(values[row]);
				val = conv.u;
			}

			if (!found_first)
			{
				cs_store_att_byval((char *) (out + out_pos),
								   values[row], attlen);
				out_pos += attlen;
				prev_val = val;
				found_first = true;
				continue;
			}

			xor_val = val ^ prev_val;

			if (xor_val == 0)
			{
				GORILLA_PUT_BITS(0, 1); /* '0' = same as previous */
			}
			else
			{
				uint8		leading;
				uint8		trailing;
				uint8		meaningful;

				leading = (xor_val == 0) ? val_bits :
					(uint8) (val_bits - 1 - pg_leftmost_one_pos64(xor_val));
				trailing = (uint8) pg_rightmost_one_pos64(xor_val);
				meaningful = val_bits - leading - trailing;

				if (leading >= prev_leading &&
					trailing >= prev_trailing &&
					found_first && prev_leading + prev_trailing < (uint8) val_bits)
				{
					/*
					 * Meaningful bits fit within the previous window.  Encode
					 * with '10' prefix + just the meaningful bits within the
					 * old window.
					 */
					uint8		prev_meaningful = val_bits - prev_leading - prev_trailing;

					/*
					 * leading >= prev_leading and trailing >= prev_trailing,
					 * so the shift alone confines the value to the previous
					 * window; masking with (1 << prev_meaningful) - 1 would
					 * be undefined for a full-width 64-bit window.
					 */
					uint64		bits = xor_val >> prev_trailing;

					GORILLA_PUT_BITS(0x01, 2);	/* '10' (LSB-first: 1,0) */
					GORILLA_PUT_BITS_WIDE(bits, prev_meaningful);
				}
				else
				{
					/*
					 * New window.  Encode with '11' prefix + 6 bits leading
					 * zeros + 6 bits meaningful length + the meaningful bits.
					 * The shift alone confines the value to the window (the
					 * top "leading" bits of xor_val are zero).  A full-width
					 * float8 window has meaningful = 64, which does not fit
					 * in the 6-bit length field: it is stored as 0, and the
					 * decoder maps 0 back to the full width (a zero-length
					 * window cannot occur, since xor_val != 0 here).
					 */
					uint64		bits = xor_val >> trailing;

					GORILLA_PUT_BITS(0x03, 2);	/* '11' */
					GORILLA_PUT_BITS(leading, 6);
					GORILLA_PUT_BITS(meaningful & 0x3F, 6);
					GORILLA_PUT_BITS_WIDE(bits, meaningful);

					prev_leading = leading;
					prev_trailing = trailing;
				}
			}

			prev_val = val;
		}
	}

#undef GORILLA_PUT_BITS_WIDE
#undef GORILLA_PUT_BITS

	/* Flush remaining bits */
	if (bits_in_buffer > 0)
		out[out_pos++] = (uint8) (bit_buffer & 0xFF);

	/* Check if Gorilla encoding actually saves space (require 20% savings) */
	if (out_pos >= raw_size * 4 / 5)
	{
		pfree(result);
		return NULL;
	}

	*encoded_len = out_pos;
	return result;
}

/*
 * Sort the col_values/col_nulls parallel arrays by the specified sort key.
 *
 * The sort_key is a comma-separated list of column names.  Each column must
 * have a default btree operator class.  The sort is stable (NULLs last).
 *
 * This modifies col_values and col_nulls in place.
 */
static void
cs_sort_batch(Relation rel, TupleDesc tupdesc, int natts,
			  Datum **col_values, bool **col_nulls, uint32 nrows,
			  const char *sort_key)
{
	CSSortContext ctx;
	char	   *key_copy;
	char	   *token;
	char	   *saveptr;
	int			max_cols;
	uint32	   *perm;
	Datum	   *tmp_vals;
	bool	   *tmp_nulls;
	int			col;

	if (sort_key == NULL || sort_key[0] == '\0' || nrows <= 1)
		return;

	/* Parse sort key column names */
	max_cols = natts;
	ctx.sort_col_indices = palloc(sizeof(int) * max_cols);
	ctx.cmp_finfos = palloc(sizeof(FmgrInfo) * max_cols);
	ctx.collations = palloc(sizeof(Oid) * max_cols);
	ctx.nsort_cols = 0;
	ctx.col_values = col_values;
	ctx.col_nulls = col_nulls;

	key_copy = pstrdup(sort_key);

	for (token = strtok_r(key_copy, ",", &saveptr);
		 token != NULL;
		 token = strtok_r(NULL, ",", &saveptr))
	{
		Form_pg_attribute attr;
		Oid			opclass;
		Oid			opfamily;
		Oid			opcintype;
		Oid			cmp_proc;
		int			attnum = -1;

		/* Strip leading/trailing whitespace */
		while (*token == ' ')
			token++;
		{
			char	   *end = token + strlen(token) - 1;

			while (end > token && *end == ' ')
				*end-- = '\0';
		}

		/* Find column by name */
		for (col = 0; col < natts; col++)
		{
			attr = TupleDescAttr(tupdesc, col);
			if (pg_strcasecmp(token, NameStr(attr->attname)) == 0)
			{
				attnum = col;
				break;
			}
		}

		if (attnum < 0)
		{
			ereport(WARNING,
					(errmsg("columnstore sort_key column \"%s\" not found, skipping sort",
							token)));
			pfree(key_copy);
			pfree(ctx.sort_col_indices);
			pfree(ctx.cmp_finfos);
			pfree(ctx.collations);
			return;
		}

		attr = TupleDescAttr(tupdesc, attnum);
		opclass = GetDefaultOpClass(attr->atttypid, BTREE_AM_OID);
		if (!OidIsValid(opclass))
		{
			ereport(WARNING,
					(errmsg("columnstore sort_key column \"%s\" has no btree opclass, skipping sort",
							token)));
			pfree(key_copy);
			pfree(ctx.sort_col_indices);
			pfree(ctx.cmp_finfos);
			pfree(ctx.collations);
			return;
		}

		opfamily = get_opclass_family(opclass);
		opcintype = get_opclass_input_type(opclass);
		cmp_proc = get_opfamily_proc(opfamily, opcintype, opcintype,
									 BTORDER_PROC);
		if (!OidIsValid(cmp_proc))
		{
			ereport(WARNING,
					(errmsg("columnstore sort_key column \"%s\" has no btree comparison function, skipping sort",
							token)));
			pfree(key_copy);
			pfree(ctx.sort_col_indices);
			pfree(ctx.cmp_finfos);
			pfree(ctx.collations);
			return;
		}

		ctx.sort_col_indices[ctx.nsort_cols] = attnum;
		fmgr_info(cmp_proc, &ctx.cmp_finfos[ctx.nsort_cols]);
		ctx.collations[ctx.nsort_cols] = attr->attcollation;
		ctx.nsort_cols++;
	}

	pfree(key_copy);

	if (ctx.nsort_cols == 0)
	{
		pfree(ctx.sort_col_indices);
		pfree(ctx.cmp_finfos);
		pfree(ctx.collations);
		return;
	}

	/* Build permutation index array */
	perm = palloc(sizeof(uint32) * nrows);
	for (uint32 row = 0; row < nrows; row++)
		perm[row] = row;

	/* Sort the permutation */
	qsort_arg(perm, nrows, sizeof(uint32), cs_sort_compare, &ctx);

	/* Apply permutation to all column arrays */
	tmp_vals = palloc(sizeof(Datum) * nrows);
	tmp_nulls = palloc(sizeof(bool) * nrows);

	for (col = 0; col < natts; col++)
	{
		for (uint32 row = 0; row < nrows; row++)
		{
			tmp_vals[row] = col_values[col][perm[row]];
			tmp_nulls[row] = col_nulls[col][perm[row]];
		}
		memcpy(col_values[col], tmp_vals, sizeof(Datum) * nrows);
		memcpy(col_nulls[col], tmp_nulls, sizeof(bool) * nrows);
	}

	pfree(tmp_vals);
	pfree(tmp_nulls);
	pfree(perm);
	pfree(ctx.sort_col_indices);
	pfree(ctx.cmp_finfos);
	pfree(ctx.collations);
}

/*
 * Datum comparator for qsort (by-value fixed-length types).
 * Compares raw datum bits as unsigned integers.
 */
static int
cs_datum_cmp(const void *a, const void *b)
{
	Datum		da = *(const Datum *) a;
	Datum		db = *(const Datum *) b;

	if (da < db)
		return -1;
	if (da > db)
		return 1;
	return 0;
}

/*
 * Try to dictionary-encode a column's data.
 *
 * Returns a palloc'd buffer containing the dict-encoded data and sets
 * *encoded_len, or returns NULL if dictionary encoding is not worthwhile
 * (too many distinct values or the encoded size is not smaller).
 *
 * The dict-encoded format is:
 *   [2 bytes: dict_count (uint16)]
 *   [1 byte:  index_width (1/2/4)]
 *   [1 byte:  has_null (0/1)]
 *   [dictionary values]  -- format depends on attbyval/attlen
 *   [nrows * index_width bytes: index array]
 */
static char *
cs_try_dict_encode(Datum *values, bool *nulls, uint32 nrows,
				   Form_pg_attribute attr, uint32 *encoded_len)
{
	bool		has_null = false;
	uint32		non_null_count = 0;

	/* Check for NULLs and count non-null rows */
	for (uint32 i = 0; i < nrows; i++)
	{
		if (nulls[i])
			has_null = true;
		else
			non_null_count++;
	}

	/* No non-null values -- don't bother with dict encoding */
	if (non_null_count == 0)
		return NULL;

	if (attr->attbyval && attr->attlen > 0)
	{
		/*
		 * By-value fixed-length: sort a copy of non-null values to find
		 * distinct count, then build dictionary via binary search.
		 */
		Datum	   *sorted;
		Datum	   *dict;
		uint32		dict_count;
		uint8		index_width;
		uint16		null_idx;
		uint16		dict_count_u16;
		Size		dict_data_size;
		Size		index_size;
		Size		total_size;
		char	   *result;
		char	   *ptr;

		/* Collect non-null values into a sortable array */
		sorted = palloc(sizeof(Datum) * non_null_count);
		{
			uint32		idx = 0;

			for (uint32 i = 0; i < nrows; i++)
			{
				if (!nulls[i])
					sorted[idx++] = values[i];
			}
		}

		qsort(sorted, non_null_count, sizeof(Datum), cs_datum_cmp);

		/* Count distinct values */
		dict_count = 1;
		for (uint32 i = 1; i < non_null_count; i++)
		{
			if (sorted[i] != sorted[i - 1])
			{
				dict_count++;
				if (dict_count > CS_DICT_MAX_ENTRIES)
					break;
			}
		}

		if (dict_count > CS_DICT_MAX_ENTRIES || (uint64) dict_count * 4 > nrows)
		{
			pfree(sorted);
			return NULL;
		}

		/* Build compact dictionary array (unique sorted values) */
		dict = palloc(sizeof(Datum) * dict_count);
		dict[0] = sorted[0];
		{
			uint32		didx = 1;

			for (uint32 i = 1; i < non_null_count; i++)
			{
				if (sorted[i] != sorted[i - 1])
					dict[didx++] = sorted[i];
			}
			Assert(didx == dict_count);
		}
		pfree(sorted);

		/* Determine index width */
		null_idx = has_null ? 1 : 0;
		if (dict_count + null_idx <= 255)
			index_width = 1;
		else
			index_width = 2;

		/* Compute sizes */
		dict_data_size = (Size) dict_count * attr->attlen;
		index_size = (Size) nrows * index_width;
		total_size = 4 + dict_data_size + index_size;	/* 4-byte header */

		result = palloc(total_size);
		ptr = result;

		/* Header */
		dict_count_u16 = (uint16) dict_count;
		memcpy(ptr, &dict_count_u16, sizeof(uint16));
		ptr += sizeof(uint16);
		*ptr++ = index_width;
		*ptr++ = has_null ? 1 : 0;

		/* Dictionary values */
		for (uint32 i = 0; i < dict_count; i++)
		{
			cs_store_att_byval(ptr, dict[i], attr->attlen);
			ptr += attr->attlen;
		}

		/* Index array: look up each row's value via binary search */
		for (uint32 i = 0; i < nrows; i++)
		{
			uint32		idx;

			if (nulls[i])
			{
				idx = dict_count;	/* NULL sentinel */
			}
			else
			{
				/* Binary search in dict */
				int			lo = 0,
							hi = dict_count - 1;

				while (lo <= hi)
				{
					int			mid = lo + (hi - lo) / 2;

					if (dict[mid] == values[i])
					{
						lo = mid;
						break;
					}
					else if (dict[mid] < values[i])
						lo = mid + 1;
					else
						hi = mid - 1;
				}
				idx = lo;
				Assert(idx < dict_count && dict[idx] == values[i]);
			}

			if (index_width == 1)
				*((uint8 *) ptr) = (uint8) idx;
			else
				cs_write_u16(ptr, (uint16) idx);
			ptr += index_width;
		}

		Assert(ptr == result + total_size);
		pfree(dict);

		*encoded_len = total_size;
		return result;
	}
	else
	{
		/*
		 * Variable-length / by-reference: use a simple open-addressing hash
		 * table to find distinct values, then build index array.
		 */
		typedef struct DictEntry
		{
			uint32		hash;
			uint32		value_idx;	/* index into values[] of first occurrence */
			uint32		dict_idx;	/* assigned dictionary index */
			bool		used;
		}			DictEntry;

		uint32		htab_size;
		DictEntry  *htab;
		uint32	   *first_occurrence;	/* row indices of first occurrence */
		uint32		dict_count = 0;
		uint32		nonnull_seen = 0;
		uint8		index_width;
		uint16		null_idx;
		Size		dict_data_size;
		Size		index_size;
		Size		total_size;
		char	   *result;
		char	   *ptr;

		/* Size hash table to ~2x expected entries, power-of-2 */
		htab_size = non_null_count * 2;
		if (htab_size < 128)
			htab_size = 128;
		{
			uint32		v = htab_size - 1;

			v |= v >> 1;
			v |= v >> 2;
			v |= v >> 4;
			v |= v >> 8;
			v |= v >> 16;
			htab_size = v + 1;
		}

		htab = palloc0(sizeof(DictEntry) * htab_size);
		first_occurrence = palloc(sizeof(uint32) * (CS_DICT_MAX_ENTRIES + 2));

		/*
		 * Insert non-null values into hash table.  We track the number of
		 * non-null rows processed so we can bail early if the column looks
		 * unlikely to be a good dictionary candidate.
		 */
#define CS_DICT_SAMPLE_ROWS 10000
		for (uint32 i = 0; i < nrows; i++)
		{
			uint32		h;
			Size		val_len;
			char	   *val_ptr;
			uint32		slot;

			if (nulls[i])
				continue;

			nonnull_seen++;

			/*
			 * Early-exit heuristic: if, after a sample of non-null rows, at
			 * least 95% introduced a new distinct value (i.e. the duplicate
			 * rate is below 5%), the column is very likely high-cardinality
			 * and not worth dictionary encoding.
			 *
			 * The rate of discovering new distinct values naturally decreases
			 * as more rows are processed (coupon-collector effect), so a
			 * naive linear extrapolation would over-estimate cardinality.
			 * Instead we use the raw duplicate rate, which is
			 * distribution-agnostic.
			 *
			 * The 95% threshold gives a safety margin above the boundary
			 * case: a column with exactly CS_DICT_MAX_ENTRIES (65,535)
			 * distinct values uniformly distributed over 100k rows shows
			 * ~7.3% duplicates after 10k rows, comfortably above the 5%
			 * cutoff and thus correctly retained.  Columns with 100k+
			 * distinct values show <5% duplicates and are abandoned early.
			 */
			if (nonnull_seen == CS_DICT_SAMPLE_ROWS &&
				dict_count * 100 >= nonnull_seen * 95)
			{
				pfree(htab);
				pfree(first_occurrence);
				return NULL;
			}

			/* Get pointer and length for this value */
			if (attr->attlen == -1)
			{
				val_ptr = DatumGetPointer(values[i]);
				val_len = VARSIZE_ANY(val_ptr);
			}
			else if (attr->attlen == -2)
			{
				val_ptr = DatumGetCString(values[i]);
				val_len = strlen(val_ptr) + 1;
			}
			else
			{
				val_ptr = DatumGetPointer(values[i]);
				val_len = attr->attlen;
			}

			/* FNV-1a hash */
			h = 2166136261U;
			for (Size b = 0; b < val_len; b++)
				h = (h ^ (unsigned char) val_ptr[b]) * 16777619U;

			slot = h & (htab_size - 1);
			for (;;)
			{
				if (!htab[slot].used)
				{
					/* New distinct value */
					if (dict_count > CS_DICT_MAX_ENTRIES)
					{
						pfree(htab);
						pfree(first_occurrence);
						return NULL;
					}
					htab[slot].used = true;
					htab[slot].hash = h;
					htab[slot].value_idx = i;
					htab[slot].dict_idx = dict_count;
					first_occurrence[dict_count] = i;
					dict_count++;
					break;
				}

				/* Check if same value (hash + full comparison) */
				if (htab[slot].hash == h)
				{
					uint32		existing = htab[slot].value_idx;
					char	   *existing_ptr;
					Size		existing_len;

					if (attr->attlen == -1)
					{
						existing_ptr = DatumGetPointer(values[existing]);
						existing_len = VARSIZE_ANY(existing_ptr);
					}
					else if (attr->attlen == -2)
					{
						existing_ptr = DatumGetCString(values[existing]);
						existing_len = strlen(existing_ptr) + 1;
					}
					else
					{
						existing_ptr = DatumGetPointer(values[existing]);
						existing_len = attr->attlen;
					}

					if (val_len == existing_len &&
						memcmp(val_ptr, existing_ptr, val_len) == 0)
						break;	/* duplicate */
				}

				slot = (slot + 1) & (htab_size - 1);
			}
		}

		/* Check heuristic */
		if ((uint64) dict_count * 4 > nrows)
		{
			pfree(htab);
			pfree(first_occurrence);
			return NULL;
		}

		/* Determine index width */
		null_idx = has_null ? 1 : 0;
		if (dict_count + null_idx <= 255)
			index_width = 1;
		else
			index_width = 2;

		/* Compute dictionary data size */
		dict_data_size = 0;
		for (uint32 d = 0; d < dict_count; d++)
		{
			uint32		vi = first_occurrence[d];

			dict_data_size += sizeof(int32);
			if (attr->attlen == -1)
				dict_data_size += VARSIZE_ANY(DatumGetPointer(values[vi]));
			else if (attr->attlen == -2)
				dict_data_size += strlen(DatumGetCString(values[vi])) + 1;
			else
				dict_data_size += attr->attlen;
		}

		index_size = (Size) nrows * index_width;
		total_size = 4 + dict_data_size + index_size;

		result = palloc(total_size);
		ptr = result;

		/* Header */
		{
			uint16		dict_count_u16 = (uint16) dict_count;

			memcpy(ptr, &dict_count_u16, sizeof(uint16));
		}
		ptr += sizeof(uint16);
		*ptr++ = index_width;
		*ptr++ = has_null ? 1 : 0;

		/* Write dictionary values */
		for (uint32 d = 0; d < dict_count; d++)
		{
			uint32		vi = first_occurrence[d];
			char	   *val_ptr;
			int32		len;

			if (attr->attlen == -1)
			{
				val_ptr = DatumGetPointer(values[vi]);
				len = VARSIZE_ANY(val_ptr);
			}
			else if (attr->attlen == -2)
			{
				val_ptr = DatumGetCString(values[vi]);
				len = strlen(val_ptr) + 1;
			}
			else
			{
				val_ptr = DatumGetPointer(values[vi]);
				len = attr->attlen;
			}

			memcpy(ptr, &len, sizeof(int32));
			ptr += sizeof(int32);
			memcpy(ptr, val_ptr, len);
			ptr += len;
		}

		/* Build index array by looking up each row in the hash table */
		for (uint32 i = 0; i < nrows; i++)
		{
			uint32		idx;

			if (nulls[i])
			{
				idx = dict_count;
			}
			else
			{
				char	   *val_ptr;
				Size		val_len;
				uint32		h;
				uint32		s;

				if (attr->attlen == -1)
				{
					val_ptr = DatumGetPointer(values[i]);
					val_len = VARSIZE_ANY(val_ptr);
				}
				else if (attr->attlen == -2)
				{
					val_ptr = DatumGetCString(values[i]);
					val_len = strlen(val_ptr) + 1;
				}
				else
				{
					val_ptr = DatumGetPointer(values[i]);
					val_len = attr->attlen;
				}

				h = 2166136261U;
				for (Size b = 0; b < val_len; b++)
					h = (h ^ (unsigned char) val_ptr[b]) * 16777619U;

				s = h & (htab_size - 1);
				for (;;)
				{
					if (htab[s].used && htab[s].hash == h)
					{
						/* Verify value matches (handles hash collisions) */
						uint32		ei = htab[s].value_idx;
						char	   *ep;
						Size		el;

						if (attr->attlen == -1)
						{
							ep = DatumGetPointer(values[ei]);
							el = VARSIZE_ANY(ep);
						}
						else if (attr->attlen == -2)
						{
							ep = DatumGetCString(values[ei]);
							el = strlen(ep) + 1;
						}
						else
						{
							ep = DatumGetPointer(values[ei]);
							el = attr->attlen;
						}

						if (val_len == el && memcmp(val_ptr, ep, val_len) == 0)
						{
							idx = htab[s].dict_idx;
							break;
						}
					}
					s = (s + 1) & (htab_size - 1);
				}
			}

			if (index_width == 1)
				*((uint8 *) ptr) = (uint8) idx;
			else
				cs_write_u16(ptr, (uint16) idx);
			ptr += index_width;
		}

		Assert(ptr == result + total_size);
		pfree(htab);
		pfree(first_occurrence);

		*encoded_len = total_size;
		return result;
	}
}

/*
 * Write a row group catalog entry, spanning multiple pages if needed.
 *
 * If freelist/fl_nranges are non-NULL, try to allocate pages from the
 * free list before extending.  Returns the block number of the first
 * catalog page.
 */
static BlockNumber
cs_write_rowgroup_catalog(Relation rel, CSRowGroupDesc *rg_desc, int16 natts,
						  CSFreeRange *freelist, uint32 *fl_nranges)
{
	Size		entry_size = CSRowGroupDescSize(natts);
	BlockNumber start_block = InvalidBlockNumber;

	if (freelist != NULL && fl_nranges != NULL)
		start_block = cs_freelist_alloc(freelist, fl_nranges,
										CS_PAGES_FOR_DATA(entry_size));

	cs_write_column_data(rel, &start_block,
						 (const char *) rg_desc, entry_size);

	return start_block;
}

/*
 * Write one row group from in-memory column arrays.
 *
 * col_values[col][row] and col_nulls[col][row] are indexed from 0..nrows-1.
 * rg_id is the global row group identifier to assign.
 *
 * If freelist/fl_nranges are non-NULL, pages are allocated from the free
 * list before extending the relation.
 *
 * Returns the block number of the row group catalog entry.
 */
BlockNumber
cs_write_one_rowgroup(Relation rel, TupleDesc tupdesc, int natts,
					  Datum **col_values, bool **col_nulls,
					  uint32 nrows, uint32 rg_id,
					  CSFreeRange *freelist, uint32 *fl_nranges)
{
	Size		rg_size = CSRowGroupDescSize(natts);
	CSRowGroupDesc *rg_desc;
	CSZoneMap  *zonemaps;
	BlockNumber catalog_block;

	/*
	 * Each row group's rows are addressed by virtual TIDs that encode rg_id
	 * in the block number (cs_encode_virtual_tid: base + rg_id *
	 * CS_VIRTUAL_BLOCKS_PER_RG).  Refuse a row group whose rows could not be
	 * addressed without the BlockNumber wrapping into another group's TID
	 * space.  This bound is astronomically high (~9.3M row groups), so it is
	 * a safety stop, not an expected limit.
	 */
	if (rg_id > (PG_UINT32_MAX - CS_COLUMNAR_BLKNO_BASE) / CS_VIRTUAL_BLOCKS_PER_RG)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("columnstore table has too many row groups")));

	rg_desc = palloc0(rg_size);
	rg_desc->rg_id = rg_id;
	rg_desc->rg_num_rows = nrows;
	rg_desc->rg_num_deleted = 0;
	rg_desc->rg_delbitmap_block = InvalidBlockNumber;
	rg_desc->rg_natts = natts;
	zonemaps = CSRowGroupGetZoneMaps(rg_desc);

	for (int col = 0; col < natts; col++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, col);
		char	   *col_data;
		uint32		data_len;
		BlockNumber start_block;
		uint32		npages;
		bool		has_nulls = false;
		bool		all_nulls = true;

		/* Check for nulls (and whether any non-null value exists at all) */
		for (uint32 row = 0; row < nrows; row++)
		{
			if (col_nulls[col][row])
				has_nulls = true;
			else
				all_nulls = false;
			if (has_nulls && !all_nulls)
				break;
		}

		/*
		 * Serialize column data.  For fixed-length by-value types, pack
		 * datums directly.  For variable-length, store the detoasted data
		 * with a length prefix per value.
		 */
		if (attr->attbyval && attr->attlen > 0)
		{
			uint32		bitmap_size = 0;
			char	   *values_start;

			if (has_nulls)
			{
				bitmap_size = (nrows + 7) / 8;
				data_len = bitmap_size + (uint32) attr->attlen * nrows;
				col_data = palloc0(data_len);

				for (uint32 row = 0; row < nrows; row++)
				{
					if (!col_nulls[col][row])
						CS_SET_NOTNULL(col_data, row);
				}
			}
			else
			{
				data_len = (uint32) attr->attlen * nrows;
				col_data = palloc(data_len);
			}

			values_start = col_data + bitmap_size;
			for (uint32 row = 0; row < nrows; row++)
			{
				if (col_nulls[col][row])
				{
					memset(values_start + row * attr->attlen, 0,
						   attr->attlen);
				}
				else
				{
					cs_store_att_byval(values_start + row * attr->attlen,
									   col_values[col][row],
									   attr->attlen);
				}
			}
		}
		else
		{
			Size		total = 0;
			char	   *ptr;

			for (uint32 row = 0; row < nrows; row++)
			{
				total += sizeof(int32);
				if (!col_nulls[col][row])
				{
					if (attr->attlen == -1)
						total += VARSIZE_ANY(DatumGetPointer(col_values[col][row]));
					else if (attr->attlen == -2)
						total += strlen(DatumGetCString(col_values[col][row])) + 1;
					else
						total += attr->attlen;
				}
			}

			/*
			 * data_len is uint32 and the buffer is palloc'd, so a column
			 * chunk cannot exceed MaxAllocSize.  100k rows of large varlena
			 * values (each up to 1GB) could in principle sum past that;
			 * refuse rather than truncate the 64-bit total into data_len and
			 * overrun the under-sized buffer.
			 */
			if (total > MaxAllocSize)
				ereport(ERROR,
						(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
						 errmsg("columnstore row group column is too large to compact"),
						 errdetail("Column %d of a %u-row row group needs %zu bytes, exceeding the %zu-byte maximum.",
								   col + 1, nrows, total, (Size) MaxAllocSize)));

			data_len = total;
			col_data = palloc(data_len);
			ptr = col_data;

			for (uint32 row = 0; row < nrows; row++)
			{
				if (col_nulls[col][row])
				{
					int32		null_marker = -1;

					memcpy(ptr, &null_marker, sizeof(int32));
					ptr += sizeof(int32);
				}
				else
				{
					int32		len;

					if (attr->attlen == -1)
					{
						len = VARSIZE_ANY(DatumGetPointer(col_values[col][row]));
						memcpy(ptr, &len, sizeof(int32));
						ptr += sizeof(int32);
						memcpy(ptr,
							   DatumGetPointer(col_values[col][row]),
							   len);
						ptr += len;
					}
					else if (attr->attlen == -2)
					{
						len = strlen(DatumGetCString(col_values[col][row])) + 1;
						memcpy(ptr, &len, sizeof(int32));
						ptr += sizeof(int32);
						memcpy(ptr,
							   DatumGetCString(col_values[col][row]),
							   len);
						ptr += len;
					}
					else
					{
						len = attr->attlen;
						memcpy(ptr, &len, sizeof(int32));
						ptr += sizeof(int32);
						memcpy(ptr,
							   DatumGetPointer(col_values[col][row]),
							   len);
						ptr += len;
					}
				}
			}

			Assert(ptr == col_data + data_len);
		}

		/*
		 * Try all applicable pre-encoding layers and pick the one that
		 * produces the smallest output after base compression.
		 *
		 * Pipeline: 1. NUMERIC_INT64 (optional, for numeric columns) 2. Try
		 * each of: FOR, DELTA, DELTA+FOR, DICT, RLE 3. Pick the smallest
		 * candidate 4. Apply LZ4/PGLZ base compression
		 *
		 * NUMERIC_INT64 + FOR/DELTA can stack; DICT and RLE are mutually
		 * exclusive with everything else.
		 */
		{
			char	   *best_data = col_data;
			uint32		best_len = data_len;
			uint8		best_preenc = 0;
			Datum	   *enc_values = col_values[col];
			bool	   *enc_nulls = col_nulls[col];
			Datum	   *ni64_values = NULL;
			Datum	   *delta_values = NULL;
			int			dscale = 0;
			bool		have_ni64 = false;
			FormData_pg_attribute fake_int8_attr;
			Form_pg_attribute enc_attr = attr;

			/* Try NUMERIC_INT64 encoding first (always applied if it helps) */
			{
				uint32		ni64_len;
				char	   *ni64_data;

				ni64_data = cs_try_numeric_int64_encode(col_values[col],
														col_nulls[col],
														nrows, attr, has_nulls,
														&ni64_len,
														&ni64_values);
				if (ni64_data != NULL && ni64_len < data_len)
				{
					pfree(col_data);
					col_data = ni64_data;
					data_len = ni64_len;
					best_data = col_data;
					best_len = data_len;
					best_preenc = CS_COMPRESS_NUMERIC_INT64;
					have_ni64 = true;
					{
						uint16		ds;

						memcpy(&ds, ni64_data, sizeof(uint16));
						dscale = ds;
					}

					enc_values = ni64_values;
					enc_nulls = col_nulls[col];
					memset(&fake_int8_attr, 0, sizeof(fake_int8_attr));
					fake_int8_attr.attlen = sizeof(int64);
					fake_int8_attr.attbyval = true;
					enc_attr = &fake_int8_attr;
				}
				else
				{
					if (ni64_data != NULL)
						pfree(ni64_data);
					if (ni64_values != NULL)
					{
						pfree(ni64_values);
						ni64_values = NULL;
					}
				}
			}

			/* Candidate: FOR alone */
			{
				uint32		for_len;
				char	   *for_data;

				for_data = cs_try_for_encode(enc_values, enc_nulls,
											 nrows, enc_attr,
											 has_nulls, &for_len);
				if (for_data != NULL)
				{
					uint32		cand_len = for_len;
					char	   *cand_data = for_data;
					uint8		cand_flags = CS_COMPRESS_FOR;

					/* Prepend NI64 dscale header if stacking */
					if (have_ni64)
					{
						cand_data = palloc(sizeof(uint16) + for_len);
						{
							uint16		ds = (uint16) dscale;

							memcpy(cand_data, &ds, sizeof(uint16));
						}
						memcpy(cand_data + sizeof(uint16), for_data, for_len);
						cand_len = sizeof(uint16) + for_len;
						cand_flags |= CS_COMPRESS_NUMERIC_INT64;
						pfree(for_data);
					}

					cs_consider_candidate(&best_data, &best_len, &best_preenc,
										  col_data, cand_data, cand_len,
										  cand_flags);
				}
			}

			/* Candidate: DELTA, and DELTA+FOR */
			{
				uint32		delta_len;
				char	   *delta_data;

				delta_data = cs_try_delta_encode(enc_values, enc_nulls,
												 nrows, enc_attr,
												 has_nulls, &delta_len,
												 &delta_values);
				if (delta_data != NULL)
				{
					uint32		cand_len = delta_len;
					char	   *cand_data = delta_data;
					uint8		cand_flags = CS_COMPRESS_DELTA;

					/* Prepend NI64 dscale header if stacking */
					if (have_ni64)
					{
						cand_data = palloc(sizeof(uint16) + delta_len);
						{
							uint16		ds = (uint16) dscale;

							memcpy(cand_data, &ds, sizeof(uint16));
						}
						memcpy(cand_data + sizeof(uint16), delta_data,
							   delta_len);
						cand_len = sizeof(uint16) + delta_len;
						cand_flags |= CS_COMPRESS_NUMERIC_INT64;
						pfree(delta_data);
						delta_data = NULL;
					}

					cs_consider_candidate(&best_data, &best_len, &best_preenc,
										  col_data, cand_data, cand_len,
										  cand_flags);

					/*
					 * Try DELTA+FOR: apply FOR to the delta values.  This is
					 * the big win for monotonic columns -- deltas have a tiny
					 * range so FOR bit-packs them in very few bits.
					 */
					if (delta_values != NULL)
					{
						uint32		dfor_len;
						char	   *dfor_data;

						dfor_data = cs_try_for_encode(delta_values, enc_nulls,
													  nrows, enc_attr,
													  has_nulls, &dfor_len);
						if (dfor_data != NULL)
						{
							uint8		df_flags = CS_COMPRESS_DELTA |
								CS_COMPRESS_FOR;

							cand_data = dfor_data;
							cand_len = dfor_len;

							if (have_ni64)
							{
								cand_data = palloc(sizeof(uint16) + dfor_len);
								{
									uint16		ds = (uint16) dscale;

									memcpy(cand_data, &ds, sizeof(uint16));
								}
								memcpy(cand_data + sizeof(uint16), dfor_data,
									   dfor_len);
								cand_len = sizeof(uint16) + dfor_len;
								df_flags |= CS_COMPRESS_NUMERIC_INT64;
								pfree(dfor_data);
							}

							cs_consider_candidate(&best_data, &best_len,
												  &best_preenc, col_data,
												  cand_data, cand_len,
												  df_flags);
						}
					}
				}
			}

			/* Candidate: DICT (only if no NUMERIC_INT64) */
			if (!have_ni64)
			{
				uint32		dict_len;
				char	   *dict_data;

				dict_data = cs_try_dict_encode(col_values[col],
											   col_nulls[col],
											   nrows, attr, &dict_len);
				cs_consider_candidate(&best_data, &best_len, &best_preenc,
									  col_data, dict_data, dict_len,
									  CS_COMPRESS_DICT);
			}

			/* Candidate: RLE (only if no NUMERIC_INT64) */
			if (!have_ni64)
			{
				uint32		rle_len;
				char	   *rle_data;

				rle_data = cs_try_rle_encode(col_values[col],
											 col_nulls[col],
											 nrows, attr, has_nulls,
											 &rle_len);
				cs_consider_candidate(&best_data, &best_len, &best_preenc,
									  col_data, rle_data, rle_len,
									  CS_COMPRESS_RLE);
			}

			/*
			 * Candidate: Gorilla (DoD for integers, XOR for floats). Applied
			 * to the original column values only -- NI64 columns use
			 * delta+FOR instead, which handles int64 values more robustly.
			 */
			if (!have_ni64)
			{
				uint32		gor_len;
				char	   *gor_data;

				gor_data = cs_try_gorilla_encode(col_values[col],
												 col_nulls[col],
												 nrows, attr,
												 has_nulls, &gor_len);
				cs_consider_candidate(&best_data, &best_len, &best_preenc,
									  col_data, gor_data, gor_len,
									  CS_COMPRESS_GORILLA);
			}

			if (delta_values != NULL)
				pfree(delta_values);
			if (ni64_values != NULL)
				pfree(ni64_values);

			/* Apply base compression (LZ4/PGLZ) to the best candidate */
			{
				uint32		compressed_len;
				uint8		comp_method;
				char	   *compressed;
				char	   *write_data;
				uint32		write_len;

				compressed = cs_compress_column(best_data, best_len,
												&compressed_len, &comp_method);
				if (compressed != NULL)
				{
					write_data = compressed;
					write_len = compressed_len;
				}
				else
				{
					write_data = best_data;
					write_len = best_len;
					comp_method = CS_COMPRESS_NONE;
				}

				comp_method |= best_preenc;

				start_block = InvalidBlockNumber;
				if (freelist != NULL && fl_nranges != NULL)
					start_block = cs_freelist_alloc(freelist, fl_nranges,
													CS_PAGES_FOR_DATA(write_len));

				npages = cs_write_column_data(rel, &start_block,
											  write_data, write_len);

				rg_desc->rg_columns[col].cc_start_block = start_block;
				rg_desc->rg_columns[col].cc_npages = npages;
				rg_desc->rg_columns[col].cc_compressed_size = write_len;
				rg_desc->rg_columns[col].cc_uncompressed_size = best_len;
				rg_desc->rg_columns[col].cc_compression = comp_method;
				rg_desc->rg_columns[col].cc_has_nulls = has_nulls;
				rg_desc->rg_columns[col].cc_nullbitmap_block = InvalidBlockNumber;

				if (compressed)
					pfree(compressed);
			}

			/* Clean up: free best_data if it's not col_data */
			if (best_data != col_data)
				pfree(col_data);
			col_data = best_data;
		}

		/*
		 * Compute zone map using the type's btree comparison function.
		 *
		 * Supports all types that have a default btree operator class.
		 * By-value types store the Datum directly (CS_ZM_BYVAL). By-reference
		 * types store the varlena inline if it fits within
		 * CS_ZONEMAP_INLINE_SIZE bytes (CS_ZM_EXACT), as truncated prefix
		 * bounds for byte-wise-ordered collations such as C, POSIX, and
		 * C.UTF-8 (CS_ZM_PREFIX), or as collation sort key prefixes for ICU
		 * locales (CS_ZM_SORTKEY).  Non-C libc collations that exceed the
		 * inline size get no zone map.
		 *
		 * NULLs are skipped — a column with some NULLs still gets a valid
		 * min/max over its non-NULL values.
		 */
		zonemaps[col].zm_has_minmax = false;

		/*
		 * Record "every row is NULL" separately from "no min/max": the latter
		 * is also true for types without a btree opclass and for by-reference
		 * bounds that cannot be stored (e.g. long values in a
		 * non-deterministic libc collation), where non-null rows certainly
		 * exist.  Only zm_all_null may be used to skip a row group for an IS
		 * NOT NULL qual.
		 */
		zonemaps[col].zm_all_null = all_nulls;
		zonemaps[col].zm_mode = CS_ZM_BYVAL;
		zonemaps[col].zm_min_len = 0;
		zonemaps[col].zm_max_len = 0;

		/*
		 * Dropped attributes have atttypid 0 (the opclass lookup below would
		 * fail) and can never appear in a qual; store no zone map.
		 */
		if (!attr->attisdropped)
		{
			Oid			opclass;
			Oid			opfamily;
			Oid			opcintype;
			Oid			cmp_func_oid;
			FmgrInfo	cmp_finfo;
			Datum		minval = 0;
			Datum		maxval = 0;
			bool		found_nonnull = false;

			opclass = GetDefaultOpClass(attr->atttypid, BTREE_AM_OID);
			if (OidIsValid(opclass))
			{
				opfamily = get_opclass_family(opclass);
				opcintype = get_opclass_input_type(opclass);
				cmp_func_oid = get_opfamily_proc(opfamily, opcintype,
												 opcintype, BTORDER_PROC);

				if (OidIsValid(cmp_func_oid))
				{
					Oid			collation = attr->attcollation;
					bool		byref = !attr->attbyval;

					fmgr_info(cmp_func_oid, &cmp_finfo);

					for (uint32 row = 0; row < nrows; row++)
					{
						Datum		val = col_values[col][row];

						if (col_nulls[col][row])
							continue;

						if (!found_nonnull)
						{
							minval = val;
							maxval = val;
							found_nonnull = true;
						}
						else
						{
							if (DatumGetInt32(FunctionCall2Coll(&cmp_finfo,
																collation,
																val,
																minval)) < 0)
								minval = val;
							if (DatumGetInt32(FunctionCall2Coll(&cmp_finfo,
																collation,
																val,
																maxval)) > 0)
								maxval = val;
						}
					}

					if (found_nonnull)
					{
						if (byref && attr->attlen > 0)
						{
							/*
							 * Fixed-length by-reference type (uuid, interval,
							 * ...): the value is attlen raw bytes, not a
							 * varlena, so VARSIZE_ANY would misread its first
							 * bytes as a length header.  Store the raw bytes
							 * inline; the comparison side hands the type's
							 * comparator a plain pointer, as for any by-ref
							 * bound.
							 */
							if (attr->attlen <= CS_ZONEMAP_INLINE_SIZE)
							{
								memcpy(zonemaps[col].zm_min_data,
									   DatumGetPointer(minval), attr->attlen);
								memcpy(zonemaps[col].zm_max_data,
									   DatumGetPointer(maxval), attr->attlen);
								zonemaps[col].zm_min_len = attr->attlen;
								zonemaps[col].zm_max_len = attr->attlen;
								zonemaps[col].zm_mode = CS_ZM_EXACT;
								zonemaps[col].zm_has_minmax = true;
							}
							/* else: oversized fixed type, no zone map */
						}
						else if (byref && attr->attlen == -1)
						{
							/*
							 * Varlena type: store inline if the detoasted
							 * values fit within the inline storage.  The
							 * Datum pointers reference col_values which will
							 * be freed, so we must copy the data into the
							 * zone map.
							 *
							 * For values exceeding the inline size, build
							 * truncated prefix bounds when the type's btree
							 * order is the byte order of its representation
							 * (text-family types under a byte-ordered
							 * collation, and bytea).  Other varlenas --
							 * numeric's digit array, arrays, ranges -- have
							 * no such property, and a truncated value is not
							 * even structurally valid, so they get no zone
							 * map beyond the exact-fit case.  The min prefix
							 * is a simple truncation (always <= original);
							 * the max prefix is truncated and then
							 * incremented to produce an upper bound (always
							 * >= original).
							 */
							Size		min_size = VARSIZE_ANY(DatumGetPointer(minval));
							Size		max_size = VARSIZE_ANY(DatumGetPointer(maxval));
							bool		byte_ordered_type =
								(attr->atttypid == TEXTOID ||
								 attr->atttypid == VARCHAROID ||
								 attr->atttypid == BPCHAROID ||
								 attr->atttypid == BYTEAOID);

							if (min_size <= CS_ZONEMAP_INLINE_SIZE &&
								max_size <= CS_ZONEMAP_INLINE_SIZE)
							{
								memcpy(zonemaps[col].zm_min_data,
									   DatumGetPointer(minval), min_size);
								memcpy(zonemaps[col].zm_max_data,
									   DatumGetPointer(maxval), max_size);
								zonemaps[col].zm_min_len = min_size;
								zonemaps[col].zm_max_len = max_size;
								zonemaps[col].zm_mode = CS_ZM_EXACT;
								zonemaps[col].zm_has_minmax = true;
							}
							else if (byte_ordered_type &&
									 (collation == InvalidOid ||
									  collation == C_COLLATION_OID ||
									  pg_newlocale_from_collation(collation)->collate_is_c))
							{
								/*
								 * Build truncated prefix bounds. The min
								 * prefix is the first CS_ZONEMAP_INLINE_SIZE
								 * bytes — always <= the full value in
								 * byte-wise order.
								 */
								char	   *minptr = DatumGetPointer(minval);
								char	   *maxptr = DatumGetPointer(maxval);
								Size		prefix_len = CS_ZONEMAP_INLINE_SIZE;
								bool		can_increment;

								memcpy(zonemaps[col].zm_min_data,
									   minptr,
									   Min(min_size, prefix_len));
								zonemaps[col].zm_min_len =
									Min(min_size, prefix_len);

								/*
								 * Max prefix: truncate then increment the
								 * last byte to form an upper bound. Propagate
								 * carry if 0xFF.
								 */
								memcpy(zonemaps[col].zm_max_data,
									   maxptr,
									   Min(max_size, prefix_len));
								zonemaps[col].zm_max_len =
									Min(max_size, prefix_len);

								can_increment = false;
								if (max_size > prefix_len)
								{
									uint8	   *bytes = (uint8 *) zonemaps[col].zm_max_data;
									int			len = zonemaps[col].zm_max_len;

									/*
									 * Skip the varlena header (first 4 bytes
									 * for 4-byte header, or 1 byte for short
									 * header) and increment payload bytes
									 * from the end.
									 */
									int			hdr_len = VARHDRSZ;

									if (VARATT_IS_SHORT(maxptr))
										hdr_len = VARHDRSZ_SHORT;

									for (int k = len - 1; k >= hdr_len; k--)
									{
										if (bytes[k] < 0xFF)
										{
											bytes[k]++;
											can_increment = true;
											break;
										}
										bytes[k] = 0;
									}
								}

								if (max_size <= prefix_len || can_increment)
								{
									/*
									 * Set the varlena length header to match
									 * the truncated size.
									 */
									if (VARATT_IS_SHORT(minptr))
										SET_VARSIZE_SHORT(zonemaps[col].zm_min_data,
														  zonemaps[col].zm_min_len);
									else
										SET_VARSIZE(zonemaps[col].zm_min_data,
													zonemaps[col].zm_min_len);

									if (VARATT_IS_SHORT(maxptr))
										SET_VARSIZE_SHORT(zonemaps[col].zm_max_data,
														  zonemaps[col].zm_max_len);
									else
										SET_VARSIZE(zonemaps[col].zm_max_data,
													zonemaps[col].zm_max_len);

									zonemaps[col].zm_mode = CS_ZM_PREFIX;
									zonemaps[col].zm_has_minmax = true;
								}
								/* else: all payload bytes 0xFF, skip */
							}
							else if (byte_ordered_type)
							{
								/*
								 * Text-family value under a non-C collation
								 * exceeding the inline size.  If the
								 * collation supports sort key prefix
								 * generation (ICU), store truncated sort key
								 * prefixes.  Sort keys have the byte-order =
								 * sort-order property, so memcmp comparison
								 * is valid.
								 */
								pg_locale_t locale;

								locale = pg_newlocale_from_collation(collation);
								if (pg_strxfrm_prefix_enabled(locale))
								{
									char	   *minstr;
									char	   *maxstr;
									ssize_t		minstrlen;
									ssize_t		maxstrlen;
									size_t		min_sk_len;
									size_t		max_sk_len;
									bool		can_increment;

									minstr = VARDATA_ANY(DatumGetPointer(minval));
									minstrlen = VARSIZE_ANY_EXHDR(DatumGetPointer(minval));
									maxstr = VARDATA_ANY(DatumGetPointer(maxval));
									maxstrlen = VARSIZE_ANY_EXHDR(DatumGetPointer(maxval));

									min_sk_len = pg_strnxfrm_prefix(
																	zonemaps[col].zm_min_data,
																	CS_ZONEMAP_INLINE_SIZE,
																	minstr, minstrlen, locale);
									max_sk_len = pg_strnxfrm_prefix(
																	zonemaps[col].zm_max_data,
																	CS_ZONEMAP_INLINE_SIZE,
																	maxstr, maxstrlen, locale);

									if (min_sk_len > 0 && max_sk_len > 0)
									{
										zonemaps[col].zm_min_len = Min(min_sk_len,
																	   CS_ZONEMAP_INLINE_SIZE);
										zonemaps[col].zm_max_len = Min(max_sk_len,
																	   CS_ZONEMAP_INLINE_SIZE);

										/*
										 * Increment the max sort key prefix
										 * to form an upper bound, same
										 * carry-propagation logic as the
										 * C-locale prefix path.
										 */
										can_increment = false;
										if (max_sk_len >= CS_ZONEMAP_INLINE_SIZE)
										{
											uint8	   *bytes = (uint8 *) zonemaps[col].zm_max_data;
											int			sklen = zonemaps[col].zm_max_len;

											for (int k = sklen - 1; k >= 0; k--)
											{
												if (bytes[k] < 0xFF)
												{
													bytes[k]++;
													can_increment = true;
													break;
												}
												bytes[k] = 0;
											}
										}

										if (max_sk_len < CS_ZONEMAP_INLINE_SIZE ||
											can_increment)
										{
											zonemaps[col].zm_mode = CS_ZM_SORTKEY;
											zonemaps[col].zm_has_minmax = true;
										}
									}
								}

								/*
								 * Non-C libc collations without sort key
								 * support: no zone map for oversized values.
								 * zm_has_minmax remains false.
								 */
							}
						}
						else
						{
							zonemaps[col].zm_min = minval;
							zonemaps[col].zm_max = maxval;
							zonemaps[col].zm_mode = CS_ZM_BYVAL;
							zonemaps[col].zm_has_minmax = true;
						}
					}
				}
			}
		}

		pfree(col_data);
	}

	catalog_block = cs_write_rowgroup_catalog(rel, rg_desc, natts,
											  freelist, fl_nranges);
	pfree(rg_desc);

	return catalog_block;
}

/*
 * Rebuild all indexes on the relation.
 *
 * Used after delta→columnar freeze and row group compaction to replace
 * stale TIDs (old delta or old row group) with current virtual TIDs.
 *
 * We do not pre-clear indisvalid because reindex_index() sets it true
 * unconditionally after rebuild.  Pre-clearing would leave the index
 * marked invalid within the current transaction, which breaks ON CONFLICT
 * and other planner features that require a valid unique index.
 */
static void
cs_rebuild_indexes(Relation rel)
{
	List	   *indexes = RelationGetIndexList(rel);

	if (indexes != NIL)
	{
		ReindexParams reindex_params = {0};

		reindex_relation(NULL, RelationGetRelid(rel), 0,
						 &reindex_params);
	}

	list_free(indexes);
}

/*
 * Update an existing row group catalog entry in-place.
 *
 * Overwrites the entry with the new rg_desc and writes it back via
 * GenericXLog.  A descriptor can exceed one page (roughly natts >= 68), in
 * which case it was created spanning CS_PAGES_FOR_DATA(entry_size) consecutive
 * blocks (see cs_write_rowgroup_catalog); mirror cs_read_rowgroup_catalog and
 * rewrite every page rather than memcpy'ing the whole entry into the first
 * page, which would overrun the buffer.  Each page is its own GenericXLog
 * record (the entry may span more than the generic-xlog per-record page
 * limit); for the common single-page case this is exactly the prior behavior.
 */
static void
cs_update_rowgroup_catalog(Relation rel, BlockNumber catalog_block,
						   CSRowGroupDesc *rg_desc, int16 natts)
{
	Size		entry_size = CSRowGroupDescSize(natts);
	Size		offset = 0;
	BlockNumber blk = catalog_block;

	while (offset < entry_size)
	{
		Buffer		buf;
		Page		page;
		GenericXLogState *state;
		Size		chunk;

		buf = ReadBuffer(rel, blk++);
		LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

		state = GenericXLogStart(rel);
		page = GenericXLogRegisterBuffer(state, buf, 0);

		chunk = Min(entry_size - offset, (Size) CS_COLDATA_PER_PAGE);
		memcpy(PageGetContents(page), (const char *) rg_desc + offset, chunk);

		GenericXLogFinish(state);
		UnlockReleaseBuffer(buf);
		offset += chunk;
	}
}


/*
 * Classification of a delta tuple against the removal horizon.
 *
 * MOVABLE rows may be promoted into (xid-less, always-visible) columnar
 * row groups: their insert is visible to every possible snapshot and no
 * delete can resurrect.  DEAD rows are invisible to everyone and can be
 * dropped.  Everything else is RETAINED: it stays in the delta with its
 * MVCC information (and its TID) intact.
 */
typedef enum CSDeltaTupleClass
{
	CS_DELTA_TUPLE_MOVABLE,
	CS_DELTA_TUPLE_DEAD,
	CS_DELTA_TUPLE_RETAINED,
} CSDeltaTupleClass;

static CSDeltaTupleClass
cs_classify_delta_tuple(HeapTupleHeader htup, TransactionId horizon)
{
	TransactionId xmin = HeapTupleHeaderGetRawXmin(htup);
	TransactionId xmax = HeapTupleHeaderGetRawXmax(htup);

	/* a tombstone still present could not be materialized yet: retain */
	if (CS_IS_TOMBSTONE(htup))
		return CS_DELTA_TUPLE_RETAINED;

	/* insert side */
	if (HeapTupleHeaderXminFrozen(htup))
	{
		/* frozen: visible to all; fall through to the delete side */
	}
	else if (htup->t_infomask & HEAP_XMIN_INVALID)
		return CS_DELTA_TUPLE_DEAD;
	else if (!(htup->t_infomask & HEAP_XMIN_COMMITTED))
	{
		if (!TransactionIdIsValid(xmin))
			return CS_DELTA_TUPLE_DEAD;
		/* in-progress before committed; see xact.c on the ordering */
		if (TransactionIdIsCurrentTransactionId(xmin) ||
			TransactionIdIsInProgress(xmin))
			return CS_DELTA_TUPLE_RETAINED;
		if (!TransactionIdDidCommit(xmin))
			return CS_DELTA_TUPLE_DEAD; /* aborted insert */
	}
	if (!HeapTupleHeaderXminFrozen(htup) &&
		!TransactionIdPrecedes(xmin, horizon))
		return CS_DELTA_TUPLE_RETAINED; /* not yet visible to all */

	/* delete side */
	if (TransactionIdIsValid(xmax) &&
		!(htup->t_infomask & HEAP_XMAX_INVALID))
	{
		/*
		 * A lock-only xmax (cs_tuple_lock) is not a delete.  While the locker
		 * is live the row must stay at its TID -- moving it to columnar would
		 * detach it from the lock -- so retain it; once the locker has ended
		 * (committed or aborted), the lock is irrelevant and the row is
		 * movable.
		 */
		if (htup->t_infomask & HEAP_XMAX_LOCK_ONLY)
		{
			if (TransactionIdIsCurrentTransactionId(xmax) ||
				TransactionIdIsInProgress(xmax))
				return CS_DELTA_TUPLE_RETAINED;
			return CS_DELTA_TUPLE_MOVABLE;
		}

		if (TransactionIdIsCurrentTransactionId(xmax) ||
			TransactionIdIsInProgress(xmax))
			return CS_DELTA_TUPLE_RETAINED;
		if (TransactionIdDidCommit(xmax))
		{
			if (TransactionIdPrecedes(xmax, horizon))
				return CS_DELTA_TUPLE_DEAD; /* delete visible to all */
			return CS_DELTA_TUPLE_RETAINED;
		}
		/* aborted delete: the row is live, fall through */
	}

	return CS_DELTA_TUPLE_MOVABLE;
}

/*
 * Set or clear the fence flag on an exclusively locked delta page.  The
 * flag is changed through generic WAL like every other delta-page
 * modification: an unlogged write would break WAL consistency on replay
 * (the generic rmgr cannot mask it).
 */
static void
cs_delta_page_set_fence(Relation rel, Buffer buf, bool fenced)
{
	GenericXLogState *fstate = GenericXLogStart(rel);
	Page		fpage = GenericXLogRegisterBuffer(fstate, buf, 0);

	if (fenced)
		CSDeltaPageGetOpaque(fpage)->cs_flags |= CS_DELTA_FENCED;
	else
		CSDeltaPageGetOpaque(fpage)->cs_flags &= ~CS_DELTA_FENCED;
	GenericXLogFinish(fstate);
}

/*
 * Freeze the delta store: replace transaction information older than
 * 'horizon' with permanent state, so pg_class.relfrozenxid can advance
 * and the table never holds back datfrozenxid (columnar rows carry no
 * xids at all).
 *
 * For every tuple still in the delta after compaction:
 *   - dead to everyone (aborted insert, or delete visible to all):
 *     mark the line pointer LP_DEAD, removing its xids from play;
 *   - visible to everyone but not movable (e.g. parked behind a
 *     blocked prefix page): set HEAP_XMIN_FROZEN, and clear any xmax
 *     belonging to a transaction that has ended (an aborted delete or
 *     a released row lock);
 *   - everything else holds only xids >= horizon by definition of the
 *     horizon, and needs nothing.
 *
 * After this pass no xid older than 'horizon' remains in the relation,
 * so the caller can report 'horizon' as the new relfrozenxid.
 */
void
cs_freeze_delta(Relation rel, TransactionId horizon)
{
	CSMetaPageData meta;
	BlockNumber end_blk;

	cs_read_metapage(rel, &meta);

	if (meta.cs_delta_start == InvalidBlockNumber ||
		meta.cs_delta_nblocks == 0)
		return;

	end_blk = meta.cs_delta_start + meta.cs_delta_nblocks;

	for (BlockNumber blkno = meta.cs_delta_start; blkno < end_blk; blkno++)
	{
		Buffer		buf;
		Page		page;
		OffsetNumber maxoff;
		GenericXLogState *state = NULL;

		CHECK_FOR_INTERRUPTS();

		buf = ReadBuffer(rel, blkno);
		LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
		page = BufferGetPage(buf);

		if (!CSPageIsDelta(page))
		{
			/* foreign page interleaved into the delta range; skip */
			UnlockReleaseBuffer(buf);
			continue;
		}

		maxoff = PageGetMaxOffsetNumber(page);

		for (OffsetNumber off = FirstOffsetNumber; off <= maxoff; off++)
		{
			ItemId		itemid = PageGetItemId(page, off);
			HeapTupleHeader htup;
			TransactionId xmax;
			bool		freeze_xmin = false;
			bool		clear_xmax = false;
			bool		mark_dead = false;

			if (!ItemIdIsNormal(itemid))
				continue;

			htup = (HeapTupleHeader) PageGetItem(page, itemid);

			if (CS_IS_TOMBSTONE(htup))
			{
				/*
				 * Tombstones older than the horizon were materialized (or,
				 * for spent row locks, reaped) before compaction; whatever
				 * remains carries xids >= horizon.
				 */
				continue;
			}

			switch (cs_classify_delta_tuple(htup, horizon))
			{
				case CS_DELTA_TUPLE_DEAD:
					mark_dead = true;
					break;

				case CS_DELTA_TUPLE_MOVABLE:
					if (!HeapTupleHeaderXminFrozen(htup))
						freeze_xmin = true;
					xmax = HeapTupleHeaderGetRawXmax(htup);
					if (TransactionIdIsValid(xmax) &&
						!(htup->t_infomask & HEAP_XMAX_INVALID))
						clear_xmax = true;
					break;

				case CS_DELTA_TUPLE_RETAINED:
					break;
			}

			if (!(freeze_xmin || clear_xmax || mark_dead))
				continue;

			if (state == NULL)
			{
				state = GenericXLogStart(rel);
				page = GenericXLogRegisterBuffer(state, buf, 0);
			}

			/* redo the lookup against the registered page image */
			itemid = PageGetItemId(page, off);
			htup = (HeapTupleHeader) PageGetItem(page, itemid);

			if (mark_dead)
			{
				ItemIdMarkDead(itemid);
				continue;
			}

			if (freeze_xmin)
				htup->t_infomask |= HEAP_XMIN_FROZEN;

			if (clear_xmax)
			{
				htup->t_infomask &= ~(HEAP_XMAX_COMMITTED |
									  HEAP_XMAX_IS_MULTI |
									  HEAP_XMAX_LOCK_ONLY |
									  HEAP_XMAX_KEYSHR_LOCK |
									  HEAP_XMAX_EXCL_LOCK);
				htup->t_infomask |= HEAP_XMAX_INVALID;
				HeapTupleHeaderSetXmax(htup, InvalidTransactionId);
			}
		}

		if (state != NULL)
			GenericXLogFinish(state);
		UnlockReleaseBuffer(buf);
	}
}

/*
 * Repair indexes left stale by a compaction that was cancelled (or
 * crashed) between its metapage flip and the completion of its index
 * rebuild.  Called at the start of VACUUM, before anything can reclaim
 * the consumed pages the stale index entries still point at.
 */
void
cs_repair_pending_reindex(Relation rel)
{
	CSMetaPageData meta;

	if (RelationGetNumberOfBlocks(rel) == 0)
		return;

	cs_read_metapage(rel, &meta);

	if ((meta.cs_flags & CS_META_PENDING_REINDEX) == 0)
		return;

	ereport(LOG,
			(errmsg("columnstore relation \"%s\" has indexes pending rebuild from an interrupted compaction; rebuilding",
					RelationGetRelationName(rel))));

	cs_rebuild_indexes(rel);
	cs_metapage_clear_flags(rel, CS_META_PENDING_REINDEX);
}

/*
 * Materialize committed tombstones into deletion bitmaps.
 *
 * Scans all delta pages for tombstone tuples with committed xmin.
 * For each committed tombstone, sets the corresponding bit in the
 * row group's deletion bitmap (allocating one if needed) and increments
 * rg_num_deleted.  This transfers the delete from the MVCC tombstone
 * into the permanent deletion bitmap, after which the tombstone can
 * be discarded during compaction.
 */
void
cs_materialize_tombstones(Relation rel, TransactionId horizon)
{
	CSMetaPageData meta;
	BlockNumber blkno;
	BlockNumber end_blk;
	int			natts;
	BlockNumber *rgdir;

	/*
	 * Collect locations of dead tombstones (aborted or already-materialized
	 * duplicates) so we can mark them LP_DEAD in a second pass.
	 */
	ItemPointerData *dead_locs = NULL;
	int			ndead = 0;
	int			dead_capacity = 0;

	cs_read_metapage(rel, &meta);

	if (meta.cs_delta_nblocks == 0 || meta.cs_nrowgroups == 0)
		return;

	natts = RelationGetDescr(rel)->natts;
	rgdir = cs_read_rgdir(rel, &meta);
	if (rgdir == NULL)
		return;

	end_blk = meta.cs_delta_start + meta.cs_delta_nblocks;

	for (blkno = meta.cs_delta_start; blkno < end_blk; blkno++)
	{
		Buffer		buf;
		Page		page;
		OffsetNumber maxoff;
		OffsetNumber off;

		buf = ReadBuffer(rel, blkno);
		LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
		page = BufferGetPage(buf);

		if (!CSPageIsDelta(page))
		{
			/* foreign page interleaved into the delta range; skip */
			UnlockReleaseBuffer(buf);
			continue;
		}
		maxoff = PageGetMaxOffsetNumber(page);

		for (off = FirstOffsetNumber; off <= maxoff; off++)
		{
			ItemId		itemid;
			HeapTupleHeader htup;
			TransactionId xmin;
			BlockNumber target_blkno;
			OffsetNumber target_offnum;
			uint32		rg_id;
			uint32		row_offset;
			CSRowGroupDesc *rg_desc;
			Size		rg_size;
			BlockNumber rg_catalog_block;
			bool		is_dead = false;

			itemid = PageGetItemId(page, off);
			if (!ItemIdIsNormal(itemid))
				continue;

			htup = (HeapTupleHeader) PageGetItem(page, itemid);
			if (!CS_IS_TOMBSTONE(htup))
				continue;

			xmin = HeapTupleHeaderGetRawXmin(htup);

			/*
			 * A lock-only tombstone records a row lock, never a delete: it
			 * must not reach the deletion bitmap.  Once its locking
			 * transaction has ended (committed or aborted) it is spent; reap
			 * it so it stops blocking page consumption.
			 */
			if (CS_TOMBSTONE_IS_LOCK_ONLY(htup))
			{
				if (TransactionIdIsValid(xmin) &&
					!TransactionIdIsCurrentTransactionId(xmin) &&
					!TransactionIdIsInProgress(xmin))
				{
					is_dead = true;
					goto record_dead;
				}
				continue;
			}

			/* Skip in-progress tombstones from concurrent transactions */
			if (TransactionIdIsValid(xmin) &&
				!TransactionIdDidCommit(xmin) &&
				!TransactionIdDidAbort(xmin))
				continue;

			/*
			 * Mark aborted tombstones for cleanup.  No hint bit is set: delta
			 * pages may only be modified through generic WAL (an unlogged
			 * write would break WAL consistency on replay, since the generic
			 * rmgr cannot mask hint bits the way heap does), and the LP_DEAD
			 * marking in the second pass makes the hint redundant anyway.
			 */
			if (!TransactionIdDidCommit(xmin))
			{
				is_dead = true;
				goto record_dead;
			}

			/*
			 * No committed hint bit is stamped here either (see the aborted
			 * case above): tombstones that stay behind simply re-check
			 * pg_xact on later passes, which is cheap relative to breaking
			 * generic-WAL consistency.
			 */

			/*
			 * A committed delete that some snapshot may still not see must
			 * NOT be folded into the (snapshot-blind) deletion bitmap yet;
			 * the tombstone stays behind and keeps answering visibility per
			 * snapshot.  It also keeps its delta page from being consumed,
			 * which is what preserves it.
			 */
			if (TransactionIdIsValid(xmin) &&
				!TransactionIdPrecedes(xmin, horizon))
				continue;

			/* Decode target virtual TID */
			target_blkno = ItemPointerGetBlockNumber(&htup->t_ctid);
			target_offnum = ItemPointerGetOffsetNumber(&htup->t_ctid);

			rg_id = (target_blkno - CS_COLUMNAR_BLKNO_BASE) /
				CS_VIRTUAL_BLOCKS_PER_RG;
			row_offset = ((target_blkno - CS_COLUMNAR_BLKNO_BASE) %
						  CS_VIRTUAL_BLOCKS_PER_RG) *
				CS_ROWS_PER_VIRTUAL_BLOCK + (target_offnum - 1);

			if (rg_id >= meta.cs_nrowgroups)
				continue;

			rg_catalog_block = rgdir[rg_id];
			if (rg_catalog_block == InvalidBlockNumber)
				continue;

			/* Read the row group catalog entry */
			rg_size = CSRowGroupDescSize(natts);
			rg_desc = palloc(rg_size);
			cs_read_rowgroup_catalog(rel, rg_catalog_block, rg_desc, natts);

			if (row_offset >= rg_desc->rg_num_rows)
			{
				pfree(rg_desc);
				continue;
			}

			/* Check if already marked in bitmap (duplicate tombstone) */
			if (rg_desc->rg_delbitmap_block != InvalidBlockNumber &&
				cs_delbitmap_test_bit(rel, rg_desc->rg_delbitmap_block,
									  row_offset))
			{
				pfree(rg_desc);
				is_dead = true;
				goto record_dead;
			}

			/* Allocate deletion bitmap if needed */
			if (rg_desc->rg_delbitmap_block == InvalidBlockNumber)
				rg_desc->rg_delbitmap_block =
					cs_alloc_delbitmap(rel, rg_desc->rg_num_rows);

			/* Set the deletion bit and update the catalog */
			cs_delbitmap_set_bit(rel, rg_desc->rg_delbitmap_block,
								 row_offset);
			rg_desc->rg_num_deleted++;
			cs_update_rowgroup_catalog(rel, rg_catalog_block, rg_desc,
									   natts);

			pfree(rg_desc);

			/* This tombstone was just materialized; mark it for cleanup */
			is_dead = true;

	record_dead:
			if (is_dead)
			{
				if (ndead >= dead_capacity)
				{
					if (dead_capacity == 0)
					{
						dead_capacity = 64;
						dead_locs = palloc_array(ItemPointerData,
												 dead_capacity);
					}
					else
					{
						dead_capacity *= 2;
						dead_locs = repalloc_array(dead_locs,
												   ItemPointerData,
												   dead_capacity);
					}
				}
				ItemPointerSet(&dead_locs[ndead++], blkno, off);
			}
		}

		UnlockReleaseBuffer(buf);
	}

	pfree(rgdir);

	/*
	 * Second pass: mark dead tombstones as LP_DEAD so they stop consuming
	 * scan time.  We batch by page to minimize buffer lock acquisitions.
	 */
	if (ndead > 0)
	{
		int			i = 0;

		while (i < ndead)
		{
			Buffer		buf;
			Page		page;
			GenericXLogState *state;
			BlockNumber cur_blkno = ItemPointerGetBlockNumber(&dead_locs[i]);

			buf = ReadBuffer(rel, cur_blkno);
			LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
			state = GenericXLogStart(rel);
			page = GenericXLogRegisterBuffer(state, buf, 0);

			while (i < ndead &&
				   ItemPointerGetBlockNumber(&dead_locs[i]) == cur_blkno)
			{
				OffsetNumber offnum;
				ItemId		itemid;

				offnum = ItemPointerGetOffsetNumber(&dead_locs[i]);
				itemid = PageGetItemId(page, offnum);
				ItemIdSetDead(itemid);
				i++;
			}

			GenericXLogFinish(state);
			UnlockReleaseBuffer(buf);
		}

		pfree(dead_locs);
	}
}

/*
 * Compact the delta store into columnar row groups.
 *
 * This scans delta pages for committed-visible tuples in batches of
 * CS_ROWS_PER_ROWGROUP rows.  Each batch is serialized, compressed,
 * and written as a single row group before moving to the next batch.
 * This bounds peak memory to one batch worth of column data.
 *
 * After all batches, we build the full row group directory (old entries
 * plus new), write it to directory pages, and do a single atomic
 * metapage update.
 */
void
cs_compact_delta(Relation rel)
{
	CSMetaPageData meta;
	TupleDesc	tupdesc = RelationGetDescr(rel);
	int			natts = tupdesc->natts;
	MemoryContext compact_ctx;
	MemoryContext batch_ctx;
	MemoryContext old_ctx;

	/* Accumulated catalog block numbers across all batches */
	BlockNumber *new_catalog_blocks;
	uint32		n_new_rgs = 0;
	uint32		max_new_rgs = 16;

	/* Delta scan resume position */
	BlockNumber cur_blkno;
	OffsetNumber cur_off;
	bool		scan_done = false;
	TransactionId horizon;
	uint32		consumed_pages = 0;
	bool		prefix_blocked = false;

	/* Original delta start, saved for TOAST cleanup after metapage update */
	BlockNumber orig_delta_start;

	/*
	 * First, materialize any committed tombstones into deletion bitmaps. This
	 * must happen before compaction so the tombstone tuples can be skipped in
	 * the main scan loop (they have natts=0 and are not data).
	 */

	/*
	 * The removal horizon: only rows whose insert is visible to every
	 * possible snapshot may move into (xid-less, always-visible) row groups,
	 * and only deletes older than this may be folded into the deletion
	 * bitmaps.  Anything younger stays in the delta with its MVCC information
	 * intact.
	 */
	horizon = GetOldestNonRemovableTransactionId(rel);

	cs_materialize_tombstones(rel, horizon);

	cs_read_metapage(rel, &meta);

	if (meta.cs_delta_start == InvalidBlockNumber || meta.cs_delta_nblocks == 0)
		return;

	orig_delta_start = meta.cs_delta_start;

	compact_ctx = AllocSetContextCreate(CurrentMemoryContext,
										"columnstore compact",
										ALLOCSET_DEFAULT_SIZES);
	old_ctx = MemoryContextSwitchTo(compact_ctx);

	new_catalog_blocks = palloc(sizeof(BlockNumber) * max_new_rgs);

	batch_ctx = AllocSetContextCreate(compact_ctx,
									  "columnstore compact batch",
									  ALLOCSET_DEFAULT_SIZES);

	cur_blkno = meta.cs_delta_start;
	cur_off = FirstOffsetNumber;

	/*
	 * Writers that find a page fenced wait on this page-0 heavyweight lock
	 * (see cs_delta_delete): hold it from before the first fence is planted
	 * until the metapage flip has either consumed the collected pages or the
	 * batch is discarded and the fences cleared.  It must be released before
	 * index rebuilding: reindex waits for concurrent DML, and a writer
	 * waiting here while holding its table lock would deadlock.
	 */
	LockPage(rel, CS_METAPAGE_BLKNO, ExclusiveLock);

	while (!scan_done)
	{
		Datum	  **col_values;
		bool	  **col_nulls;
		uint32		nrows = 0;
		BlockNumber catalog_block;

		CHECK_FOR_INTERRUPTS();

		MemoryContextReset(batch_ctx);
		MemoryContextSwitchTo(batch_ctx);

		/* Allocate column arrays for up to one row group */
		col_values = palloc(sizeof(Datum *) * natts);
		col_nulls = palloc(sizeof(bool *) * natts);
		for (int i = 0; i < natts; i++)
		{
			col_values[i] = palloc(sizeof(Datum) * CS_ROWS_PER_ROWGROUP);
			col_nulls[i] = palloc(sizeof(bool) * CS_ROWS_PER_ROWGROUP);
		}

		/*
		 * Scan delta pages, collecting up to CS_ROWS_PER_ROWGROUP visible
		 * rows.  Resume from (cur_blkno, cur_off) left by the prior batch.
		 */
		while (cur_blkno < meta.cs_delta_start + meta.cs_delta_nblocks &&
			   nrows + MaxHeapTuplesPerPage <= CS_ROWS_PER_ROWGROUP &&
			   !prefix_blocked)
		{
			Buffer		buf;
			Page		page;
			OffsetNumber maxoff;
			bool		page_clean = true;

			buf = ReadBuffer(rel, cur_blkno);
			LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
			page = BufferGetPage(buf);

			if (!CSPageIsDelta(page))
			{
				/*
				 * A foreign page (column data a previous VACUUM wrote while
				 * an insert was extending the delta) interleaved into the
				 * nominal range.  It is not delta content: advance past it,
				 * but it belongs to live data and must not be reclaimed
				 * (phase 2 re-checks page identity before freeing).
				 */
				UnlockReleaseBuffer(buf);
				cur_blkno++;
				cur_off = FirstOffsetNumber;
				consumed_pages++;
				continue;
			}
			maxoff = PageGetMaxOffsetNumber(page);

			/*
			 * The delta is consumed strictly from the front, in whole pages,
			 * so retained tuples never change TIDs: a page may only be
			 * consumed when every tuple on it is movable or dead.  The first
			 * page holding an in-flight or not-yet-all-visible tuple (or an
			 * unmaterialized tombstone) ends the consumable prefix; it and
			 * everything after it stay in the delta untouched.
			 */
			for (OffsetNumber off = FirstOffsetNumber; off <= maxoff; off++)
			{
				ItemId		itemid = PageGetItemId(page, off);

				if (!ItemIdIsNormal(itemid))
					continue;
				if (cs_classify_delta_tuple((HeapTupleHeader) PageGetItem(page, itemid),
											horizon) == CS_DELTA_TUPLE_RETAINED)
				{
					page_clean = false;
					break;
				}
			}
			if (!page_clean)
			{
				UnlockReleaseBuffer(buf);
				prefix_blocked = true;
				break;
			}

			/*
			 * Fence the page before collecting its rows, under the same
			 * exclusive lock (which is why this loop takes it rather than a
			 * share lock).  An insert that targets this page from a stale
			 * metapage view serializes on the page lock and sees the fence,
			 * extending instead; otherwise its tuple, added after we
			 * collected and classified, would be silently dropped when the
			 * page is consumed.  If we error out before consuming, the fence
			 * merely wastes the page's remaining free space until a later
			 * VACUUM consumes the page (or clears the fence on discard).
			 *
			 * A page without room for even a minimal tuple cannot receive an
			 * insert, and delta pages never regain free space, so the fence
			 * (a full-page generic WAL record) is skipped for it.
			 */
			if (!CSDeltaPageIsFenced(page) &&
				PageGetExactFreeSpace(page) >= MAXALIGN(SizeofHeapTupleHeader))
				cs_delta_page_set_fence(rel, buf, true);

			for (OffsetNumber off = cur_off; off <= maxoff; off++)
			{
				ItemId		itemid = PageGetItemId(page, off);
				HeapTupleData tuple;

				if (!ItemIdIsNormal(itemid))
					continue;

				tuple.t_data = (HeapTupleHeader) PageGetItem(page, itemid);
				tuple.t_len = ItemIdGetLength(itemid);
				tuple.t_tableOid = RelationGetRelid(rel);
				ItemPointerSet(&tuple.t_self, cur_blkno, off);

				/* page is clean: every tuple is movable or dead */
				if (cs_classify_delta_tuple(tuple.t_data, horizon) !=
					CS_DELTA_TUPLE_MOVABLE)
					continue;

				/* Deform tuple into per-column arrays */
				{
					Datum		values[MaxTupleAttributeNumber];
					bool		isnull[MaxTupleAttributeNumber];

					heap_deform_tuple(&tuple, tupdesc, values, isnull);
					for (int i = 0; i < natts; i++)
					{
						col_nulls[i][nrows] = isnull[i];

						if (isnull[i])
							col_values[i][nrows] = (Datum) 0;
						else if (!TupleDescAttr(tupdesc, i)->attbyval)
						{
							/*
							 * By-reference datum: deep-copy into batch_ctx so
							 * the value survives buffer release.
							 *
							 * For varlena types, detoast the value first.
							 * Delta tuples may contain TOAST pointers to
							 * out-of-line data; we must fetch the actual data
							 * before serializing into columnar format.
							 */
							Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
							Size		len;

							if (attr->attlen == -1)
							{
								varlena    *val;

								val = detoast_attr((varlena *)
												   DatumGetPointer(values[i]));
								col_values[i][nrows] = PointerGetDatum(val);
							}
							else
							{
								char	   *copy;

								if (attr->attlen == -2)
									len = strlen(DatumGetCString(values[i])) + 1;
								else
									len = attr->attlen;

								copy = palloc(len);
								memcpy(copy, DatumGetPointer(values[i]), len);
								col_values[i][nrows] = PointerGetDatum(copy);
							}
						}
						else
							col_values[i][nrows] = values[i];
					}
				}
				nrows++;
			}

			UnlockReleaseBuffer(buf);
			cur_blkno++;
			cur_off = FirstOffsetNumber;
			consumed_pages++;
		}

		/*
		 * The scan is over when the prefix ended or the last delta page was
		 * consumed; a full batch just pauses it.
		 */
		if (prefix_blocked ||
			cur_blkno >= meta.cs_delta_start + meta.cs_delta_nblocks)
			scan_done = true;

		if (nrows == 0)
			break;

		/*
		 * If autovacuum triggered this compaction and the entire delta has
		 * fewer than CS_COMPACT_MIN_DELTA_ROWS visible rows (with no full
		 * batches written yet), skip compaction.  The rows stay in the delta
		 * for the next cycle, avoiding tiny, poorly-compressed row groups.
		 * Manual VACUUM always compacts regardless of delta size.
		 */
		if (scan_done && n_new_rgs == 0 &&
			nrows < CS_COMPACT_MIN_DELTA_ROWS &&
			AmAutoVacuumWorkerProcess())
		{
			/*
			 * The collected rows are being discarded, so the pages they came
			 * from must NOT be consumed -- advancing past them would silently
			 * drop committed rows.  (This was once exactly such a bug.)
			 * Dead-page reclamation waits for a manual VACUUM. Clear the
			 * fences so the pages accept inserts again.
			 */
			for (BlockNumber blk = orig_delta_start;
				 blk < orig_delta_start + consumed_pages; blk++)
			{
				Buffer		fbuf = ReadBuffer(rel, blk);
				Page		fpage;

				LockBuffer(fbuf, BUFFER_LOCK_EXCLUSIVE);
				fpage = BufferGetPage(fbuf);
				if (CSPageIsDelta(fpage) && CSDeltaPageIsFenced(fpage))
					cs_delta_page_set_fence(rel, fbuf, false);
				UnlockReleaseBuffer(fbuf);
			}
			consumed_pages = 0;
			break;
		}

		/* Sort this batch by sort key if configured */
		{
			const char *sort_key = cs_get_sort_key(rel);

			if (sort_key != NULL)
				cs_sort_batch(rel, tupdesc, natts,
							  col_values, col_nulls, nrows, sort_key);
		}

		/* Write this batch as one row group */
		catalog_block = cs_write_one_rowgroup(rel, tupdesc, natts,
											  col_values, col_nulls,
											  nrows,
											  meta.cs_nrowgroups + n_new_rgs,
											  NULL, NULL);

		/* Accumulate catalog block in parent context */
		MemoryContextSwitchTo(compact_ctx);

		if (n_new_rgs >= max_new_rgs)
		{
			max_new_rgs *= 2;
			new_catalog_blocks = repalloc(new_catalog_blocks,
										  sizeof(BlockNumber) * max_new_rgs);
		}
		new_catalog_blocks[n_new_rgs++] = catalog_block;
	}

	MemoryContextSwitchTo(compact_ctx);
	MemoryContextDelete(batch_ctx);

	if (n_new_rgs == 0)
	{
		/*
		 * No data rows to compact, but a prefix of the delta may consist
		 * purely of dead tuples and materialized tombstones; advance past it
		 * so future scans don't keep re-reading them.  Pages holding anything
		 * retained stay put, TIDs intact.
		 */
		if (consumed_pages > 0)
			cs_write_metapage_advance_delta(rel, &meta, consumed_pages);

		UnlockPage(rel, CS_METAPAGE_BLKNO, ExclusiveLock);

		MemoryContextSwitchTo(old_ctx);
		MemoryContextDelete(compact_ctx);
		return;
	}

	/*
	 * Phase 2: Build full directory, reclaim freed pages, and write.
	 *
	 * We read the existing directory, append new entries, write the combined
	 * directory to new pages, and do a single atomic metapage update.
	 *
	 * Reclaimed pages (old delta store, old directory pages, old freelist
	 * pages) are added to the free list so that future column data writes can
	 * reuse them instead of extending the relation indefinitely.
	 */
	{
		uint32		new_total = meta.cs_nrowgroups + n_new_rgs;
		BlockNumber *full_dir;
		BlockNumber rgdir_start = InvalidBlockNumber;
		uint32		rgdir_npages;
		CSFreeRange *freelist;
		uint32		fl_nranges;
		uint32		fl_max;

		/* Load existing free list, or start with an empty one */
		freelist = cs_read_freelist(rel, &meta);
		fl_nranges = meta.cs_freelist_nranges;
		if (freelist != NULL)
		{
			fl_max = fl_nranges;
		}
		else
		{
			fl_max = 16;
			freelist = palloc(sizeof(CSFreeRange) * fl_max);
			fl_nranges = 0;
		}

		/*
		 * Reclaim the consumed prefix of the old delta.  Foreign pages
		 * interleaved into the range (column data of live row groups) are
		 * skipped by re-checking each page's identity; freeing them would
		 * hand live data pages to the allocator.
		 */
		for (BlockNumber blk = orig_delta_start;
			 blk < orig_delta_start + consumed_pages; blk++)
		{
			Buffer		fbuf = ReadBuffer(rel, blk);
			bool		is_delta;

			LockBuffer(fbuf, BUFFER_LOCK_SHARE);
			is_delta = CSPageIsDelta(BufferGetPage(fbuf));
			UnlockReleaseBuffer(fbuf);

			if (is_delta)
				cs_freelist_add(&freelist, &fl_nranges, &fl_max, blk, 1);
		}

		/* Reclaim old directory pages (they'll be rewritten) */
		if (meta.cs_rgdir_start != InvalidBlockNumber &&
			meta.cs_rgdir_npages > 0)
			cs_freelist_add(&freelist, &fl_nranges, &fl_max,
							meta.cs_rgdir_start, meta.cs_rgdir_npages);

		/* Reclaim old freelist pages (they'll be rewritten too) */
		if (meta.cs_freelist_start != InvalidBlockNumber &&
			meta.cs_freelist_npages > 0)
			cs_freelist_add(&freelist, &fl_nranges, &fl_max,
							meta.cs_freelist_start, meta.cs_freelist_npages);

		full_dir = palloc(sizeof(BlockNumber) * new_total);

		/* Copy existing directory entries */
		if (meta.cs_nrowgroups > 0)
		{
			BlockNumber *old_dir = cs_read_rgdir(rel, &meta);

			memcpy(full_dir, old_dir, sizeof(BlockNumber) * meta.cs_nrowgroups);
			pfree(old_dir);
		}

		/* Append new entries */
		memcpy(full_dir + meta.cs_nrowgroups, new_catalog_blocks,
			   sizeof(BlockNumber) * n_new_rgs);

		/* Write combined directory */
		rgdir_npages = cs_write_rgdir(rel, full_dir, new_total, &rgdir_start);

		/* Write updated free list */
		cs_write_freelist(rel, &meta, freelist, fl_nranges);

		/*
		 * Atomic metapage update: publish the new row groups and advance the
		 * delta start past the consumed prefix in one write, so a crash can
		 * never leave a state where the moved rows are visible both ways (or
		 * neither way).  PENDING_REINDEX rides along in the same write: from
		 * this instant until reindexing completes, the indexes reference
		 * pre-move TIDs.
		 */
		meta.cs_nrowgroups = new_total;
		meta.cs_rgdir_start = rgdir_start;
		meta.cs_rgdir_npages = rgdir_npages;
		meta.cs_flags |= CS_META_PENDING_REINDEX;

		cs_write_metapage_advance_delta(rel, &meta, consumed_pages);

		UnlockPage(rel, CS_METAPAGE_BLKNO, ExclusiveLock);

		pfree(full_dir);
		pfree(freelist);
	}

	/*
	 * Phase 3: Clean up TOAST data from the now-consumed delta rows.
	 *
	 * The metapage no longer covers the consumed prefix, so the columnar data
	 * is committed.  If we crash before finishing TOAST cleanup, the orphaned
	 * toast chunks are harmless and will be reclaimed by a future VACUUM on
	 * the TOAST table.
	 *
	 * Concurrent readers are protected by ordinary MVCC on the TOAST
	 * relation: heap_toast_delete uses simple_heap_delete, which merely
	 * stamps our xid as xmax.  SnapshotToast ignores xmax, so a scan that
	 * captured the old metapage and is still reading the consumed pages can
	 * keep detoasting these values; the chunks are physically removed only by
	 * a later VACUUM of the TOAST table, whose horizon cannot advance past
	 * that scan's snapshot.  Consumed pages contain only tuples that were
	 * moved to columnar (their values re-stored there) or are dead to every
	 * snapshot, so no future fetch needs this TOAST data.
	 */
	if (OidIsValid(rel->rd_rel->reltoastrelid))
	{
		bool		has_toastable = false;

		for (int i = 0; i < natts; i++)
		{
			Form_pg_attribute attr = TupleDescAttr(tupdesc, i);

			if (attr->attlen == -1 && attr->attstorage != TYPSTORAGE_PLAIN)
			{
				has_toastable = true;
				break;
			}
		}

		if (has_toastable)
		{
			BlockNumber delta_end = orig_delta_start + consumed_pages;

			for (BlockNumber blk = orig_delta_start;
				 blk < delta_end; blk++)
			{
				Buffer		buf;
				Page		page;
				OffsetNumber maxoff;

				buf = ReadBuffer(rel, blk);
				LockBuffer(buf, BUFFER_LOCK_SHARE);
				page = BufferGetPage(buf);

				if (!CSPageIsDelta(page))
				{
					/* foreign page interleaved into the delta range */
					UnlockReleaseBuffer(buf);
					continue;
				}
				maxoff = PageGetMaxOffsetNumber(page);

				for (OffsetNumber off = FirstOffsetNumber;
					 off <= maxoff; off++)
				{
					ItemId		itemid = PageGetItemId(page, off);
					HeapTupleData htup;

					if (!ItemIdIsNormal(itemid))
						continue;

					htup.t_data = (HeapTupleHeader)
						PageGetItem(page, itemid);
					htup.t_len = ItemIdGetLength(itemid);
					htup.t_tableOid = RelationGetRelid(rel);
					ItemPointerSet(&htup.t_self, blk, off);

					if (HeapTupleHasExternal(&htup))
					{
						HeapTuple	copy;

						/*
						 * Copy the tuple before unpinning: htup.t_data points
						 * into the page, which may be evicted and replaced
						 * the moment the pin is dropped, and
						 * heap_toast_delete dereferences the tuple's
						 * attributes.
						 */
						copy = heap_copytuple(&htup);
						UnlockReleaseBuffer(buf);
						heap_toast_delete(rel, copy, false);
						heap_freetuple(copy);
						buf = ReadBuffer(rel, blk);
						LockBuffer(buf, BUFFER_LOCK_SHARE);
						page = BufferGetPage(buf);
						maxoff = PageGetMaxOffsetNumber(page);
					}
				}

				UnlockReleaseBuffer(buf);
			}
		}
	}

	pfree(new_catalog_blocks);

	/*
	 * Phase 4: Rebuild indexes.
	 *
	 * Old index entries point to delta-store TIDs that are no longer valid.
	 * Rebuild all indexes so they contain the correct columnar virtual TIDs,
	 * then retire the pending-reindex flag the flip set.  If we are cancelled
	 * before finishing, the flag stays and the next VACUUM repairs the
	 * indexes (cs_repair_pending_reindex) before any page can be reused.
	 */
	cs_rebuild_indexes(rel);
	cs_metapage_clear_flags(rel, CS_META_PENDING_REINDEX);

	MemoryContextSwitchTo(old_ctx);
	MemoryContextDelete(compact_ctx);
}

/*
 * Extract a single (Datum, isnull) from a loaded CSColumnCache.
 *
 * Handles all storage formats: direct-access fixed-len, varlen with
 * col_values arrays, and dictionary-encoded columns.  The column must
 * already be loaded via cs_ensure_column_loaded().
 */
static void
cs_cache_get_value(CSColumnCache *cache, TupleDesc tupdesc,
				   int col, uint32 row, Datum *val, bool *isnull)
{
	if (cache->col_dict && cache->col_dict[col] != NULL)
	{
		CSDictColumn *dict = cache->col_dict[col];
		uint32		idx;

		switch (dict->index_width)
		{
			case 1:
				idx = ((uint8 *) dict->index_data)[row];
				break;
			case 2:
				idx = cs_read_u16((const char *) dict->index_data + (Size) (row) * sizeof(uint16));
				break;
			default:
				idx = cs_read_u32((const char *) dict->index_data + (Size) (row) * sizeof(uint32));
				break;
		}

		if (dict->has_null && idx == dict->dict_count)
		{
			*isnull = true;
			*val = (Datum) 0;
		}
		else
		{
			*isnull = false;
			*val = dict->dict_values[idx];
		}
	}
	else if (cache->col_values[col] != NULL)
	{
		*val = cache->col_values[col][row];
		*isnull = cache->col_nulls[col][row];
	}
	else
	{
		/* Fixed-length by-value, direct access from raw_data */
		Form_pg_attribute attr = TupleDescAttr(tupdesc, col);

		if (cache->col_null_bitmap[col] != NULL)
		{
			if (CS_ISNULL(cache->col_null_bitmap[col], row))
			{
				*isnull = true;
				*val = (Datum) 0;
				return;
			}
		}

		*isnull = false;
		if (cache->col_ni64_buf[col] != NULL)
		{
			int64		ival = ((int64 *) cache->col_raw_data[col])[row];

			*val = cs_int64_to_numeric_buf(ival,
										   (int) cache->col_ni64_dscale[col],
										   cache->col_ni64_buf[col]);
		}
		else
		{
			*val = cs_fetch_att(cache->col_raw_data[col] +
								(Size) row * attr->attlen,
								true, attr->attlen);
		}
	}
}

/*
 * Compact row groups with too many deleted rows.
 *
 * Scans all row groups and rewrites those whose dead-row fraction exceeds
 * columnstore_rowgroup_compaction_threshold.  Surviving rows from each
 * compacted row group are written as a new row group with a fresh catalog
 * entry.  The old row group's directory slot is replaced atomically.
 *
 * After any compaction, all indexes on the relation are rebuilt
 * automatically via reindex_relation() so their entries match the
 * post-compaction virtual TIDs.
 */
/*
 * Helper: add all pages belonging to a row group to the free list.
 */
static void
cs_free_rowgroup_pages(CSRowGroupDesc *rg_desc, int natts,
					   BlockNumber catalog_block,
					   CSFreeRange **freelist, uint32 *fl_nranges,
					   uint32 *fl_max)
{
	/* Free catalog pages */
	cs_freelist_add(freelist, fl_nranges, fl_max, catalog_block,
					CS_PAGES_FOR_DATA(CSRowGroupDescSize(natts)));

	/* Free column data pages */
	for (int col = 0; col < natts; col++)
	{
		CSColumnChunkDesc *cc = &rg_desc->rg_columns[col];

		if (cc->cc_npages > 0)
			cs_freelist_add(freelist, fl_nranges, fl_max,
							cc->cc_start_block, cc->cc_npages);

		if (cc->cc_nullbitmap_block != InvalidBlockNumber)
			cs_freelist_add(freelist, fl_nranges, fl_max,
							cc->cc_nullbitmap_block, 1);
	}

	/* Free deletion bitmap pages */
	if (rg_desc->rg_delbitmap_block != InvalidBlockNumber)
		cs_freelist_add(freelist, fl_nranges, fl_max,
						rg_desc->rg_delbitmap_block,
						CS_DELBITMAP_NPAGES(rg_desc->rg_num_rows));
}

void
cs_compact_rowgroups(Relation rel)
{
	CSMetaPageData meta;
	TupleDesc	tupdesc = RelationGetDescr(rel);
	int			natts = tupdesc->natts;
	Size		rg_size = CSRowGroupDescSize(natts);
	BlockNumber *dir;
	bool		any_compacted = false;
	MemoryContext compact_ctx;
	MemoryContext rg_ctx;
	MemoryContext old_ctx;
	int			n_above_threshold;

	/* Free list state (lives in compact_ctx) */
	CSFreeRange *freelist;
	uint32		fl_nranges;
	uint32		fl_max;

	/*
	 * Merge accumulator: surviving rows from multiple compacted row groups
	 * are collected here and flushed as full-sized merged row groups.
	 * By-reference datum copies are allocated in compact_ctx and freed when
	 * the context is deleted at the end.
	 */
	Datum	  **accum_values;
	bool	  **accum_nulls;
	uint32		accum_nrows;

	/* Catalog blocks of merged output row groups */
	BlockNumber *merged_blocks;
	uint32		n_merged;
	uint32		max_merged;

	cs_read_metapage(rel, &meta);

	if (meta.cs_nrowgroups == 0)
		return;

	dir = cs_read_rgdir(rel, &meta);
	if (dir == NULL)
		return;

	compact_ctx = AllocSetContextCreate(CurrentMemoryContext,
										"cs_compact_rowgroups",
										ALLOCSET_DEFAULT_SIZES);
	old_ctx = MemoryContextSwitchTo(compact_ctx);

	/* Load existing free list, or start with an empty one */
	freelist = cs_read_freelist(rel, &meta);
	fl_nranges = meta.cs_freelist_nranges;
	if (freelist != NULL)
	{
		fl_max = fl_nranges;
	}
	else
	{
		fl_max = 64;
		freelist = palloc(sizeof(CSFreeRange) * fl_max);
		fl_nranges = 0;
	}

	/* Add old free list pages to the free list (they'll be rewritten) */
	if (meta.cs_freelist_start != InvalidBlockNumber && meta.cs_freelist_npages > 0)
		cs_freelist_add(&freelist, &fl_nranges, &fl_max,
						meta.cs_freelist_start, meta.cs_freelist_npages);

	/*
	 * Pre-scan: count row groups that exceed the compaction dead-ratio
	 * threshold.  If any exist, we also pull in undersized row groups (left
	 * behind by previous compaction cycles) so they can be merged into
	 * full-sized groups.
	 */
	n_above_threshold = 0;
	for (uint32 rg_idx = 0; rg_idx < meta.cs_nrowgroups; rg_idx++)
	{
		CSRowGroupDesc *rg_desc = palloc(rg_size);

		cs_read_rowgroup_catalog(rel, dir[rg_idx], rg_desc, natts);

		if (rg_desc->rg_num_deleted > 0 && rg_desc->rg_num_rows > 0)
		{
			double		dead_ratio;
			uint32		surviving;

			dead_ratio = (double) rg_desc->rg_num_deleted / rg_desc->rg_num_rows;
			surviving = rg_desc->rg_num_rows - rg_desc->rg_num_deleted;

			if (dead_ratio >= columnstore_rowgroup_compaction_threshold &&
				surviving > 0)
				n_above_threshold++;
		}

		pfree(rg_desc);
	}

	/* Initialize merge accumulator (reused across flushes) */
	accum_values = palloc(sizeof(Datum *) * natts);
	accum_nulls = palloc(sizeof(bool *) * natts);
	for (int col = 0; col < natts; col++)
	{
		accum_values[col] = palloc(sizeof(Datum) * CS_ROWS_PER_ROWGROUP);
		accum_nulls[col] = palloc(sizeof(bool) * CS_ROWS_PER_ROWGROUP);
	}
	accum_nrows = 0;

	max_merged = 16;
	merged_blocks = palloc(sizeof(BlockNumber) * max_merged);
	n_merged = 0;

	rg_ctx = AllocSetContextCreate(compact_ctx,
								   "cs_compact_rowgroups rg",
								   ALLOCSET_DEFAULT_SIZES);

	for (uint32 rg_idx = 0; rg_idx < meta.cs_nrowgroups; rg_idx++)
	{
		CSRowGroupDesc *rg_desc;
		CSColumnCache cache;
		double		dead_ratio;
		uint32		surviving;
		bool		is_merge_candidate;
		BlockNumber old_catalog_block = dir[rg_idx];

		CHECK_FOR_INTERRUPTS();

		MemoryContextReset(rg_ctx);
		MemoryContextSwitchTo(rg_ctx);

		/* Read catalog entry for this row group */
		rg_desc = palloc(rg_size);
		cs_read_rowgroup_catalog(rel, dir[rg_idx], rg_desc, natts);

		if (rg_desc->rg_num_rows == 0)
		{
			pfree(rg_desc);
			continue;
		}

		surviving = rg_desc->rg_num_rows - rg_desc->rg_num_deleted;

		/* Fully-deleted row group: free its pages */
		if (surviving == 0 && rg_desc->rg_num_deleted > 0)
		{
			dead_ratio = (double) rg_desc->rg_num_deleted / rg_desc->rg_num_rows;
			if (dead_ratio >= columnstore_rowgroup_compaction_threshold)
			{
				MemoryContextSwitchTo(compact_ctx);
				cs_free_rowgroup_pages(rg_desc, natts, old_catalog_block,
									   &freelist, &fl_nranges, &fl_max);
				dir[rg_idx] = InvalidBlockNumber;
				any_compacted = true;
			}
			pfree(rg_desc);
			continue;
		}

		/*
		 * Determine if this row group is a merge candidate:
		 *
		 * (a) dead ratio exceeds the compaction threshold, OR
		 *
		 * (b) it is undersized (fewer than CS_ROWS_PER_ROWGROUP rows) and we
		 * already have above-threshold candidates to consolidate with.  This
		 * catches small row groups left behind by previous compaction cycles.
		 */
		is_merge_candidate = false;
		if (rg_desc->rg_num_deleted > 0)
		{
			dead_ratio = (double) rg_desc->rg_num_deleted / rg_desc->rg_num_rows;
			if (dead_ratio >= columnstore_rowgroup_compaction_threshold)
				is_merge_candidate = true;
		}

		if (!is_merge_candidate &&
			n_above_threshold > 0 &&
			rg_desc->rg_num_rows < CS_ROWS_PER_ROWGROUP)
			is_merge_candidate = true;

		if (!is_merge_candidate)
		{
			pfree(rg_desc);
			continue;
		}

		elog(DEBUG1, "columnstore: compacting row group %u (%u/%u rows deleted, %u surviving)",
			 rg_desc->rg_id, rg_desc->rg_num_deleted, rg_desc->rg_num_rows,
			 surviving);

		/*
		 * Load the row group into a column cache, then extract surviving rows
		 * into the merge accumulator.
		 */
		memset(&cache, 0, sizeof(cache));
		cache.cur_rg_desc = rg_desc;

		if (!cs_load_rowgroup_into(&cache, rel, dir[rg_idx], natts))
		{
			pfree(rg_desc);
			continue;
		}

		/* Load all columns */
		for (int col = 0; col < natts; col++)
			cs_ensure_column_loaded(&cache, rel, col);

		/*
		 * Copy surviving rows into the merge accumulator.  By-reference
		 * values must be deep-copied because the column cache buffers live in
		 * rg_ctx which is reset for each source row group.  When the
		 * accumulator reaches CS_ROWS_PER_ROWGROUP, flush it as a merged row
		 * group.
		 */
		for (uint32 row = 0; row < rg_desc->rg_num_rows; row++)
		{
			if (cache.cur_delbitmap != NULL &&
				CS_ISDELETED(cache.cur_delbitmap, row))
				continue;

			MemoryContextSwitchTo(compact_ctx);

			for (int col = 0; col < natts; col++)
			{
				Datum		val;
				bool		isnull;

				cs_cache_get_value(&cache, tupdesc, col, row, &val, &isnull);

				accum_nulls[col][accum_nrows] = isnull;
				if (isnull)
					accum_values[col][accum_nrows] = (Datum) 0;
				else if (!TupleDescAttr(tupdesc, col)->attbyval)
					accum_values[col][accum_nrows] = datumCopy(val, false,
															   TupleDescAttr(tupdesc, col)->attlen);
				else
					accum_values[col][accum_nrows] = val;
			}
			accum_nrows++;

			MemoryContextSwitchTo(rg_ctx);

			/* Flush full accumulator as a merged row group */
			if (accum_nrows >= CS_ROWS_PER_ROWGROUP)
			{
				MemoryContext write_ctx;
				BlockNumber catalog_block;

				/* Sort before writing if configured */
				{
					const char *sk = cs_get_sort_key(rel);

					if (sk != NULL)
						cs_sort_batch(rel, tupdesc, natts,
									  accum_values, accum_nulls,
									  CS_ROWS_PER_ROWGROUP, sk);
				}

				write_ctx = AllocSetContextCreate(compact_ctx,
												  "cs_compact merge flush",
												  ALLOCSET_DEFAULT_SIZES);
				MemoryContextSwitchTo(write_ctx);

				catalog_block = cs_write_one_rowgroup(rel, tupdesc, natts,
													  accum_values,
													  accum_nulls,
													  CS_ROWS_PER_ROWGROUP,
													  0, NULL, NULL);

				MemoryContextSwitchTo(rg_ctx);
				MemoryContextDelete(write_ctx);

				if (n_merged >= max_merged)
				{
					MemoryContextSwitchTo(compact_ctx);
					max_merged *= 2;
					merged_blocks = repalloc(merged_blocks,
											 sizeof(BlockNumber) * max_merged);
					MemoryContextSwitchTo(rg_ctx);
				}
				merged_blocks[n_merged++] = catalog_block;
				accum_nrows = 0;
			}
		}

		/* Free old row group's pages */
		MemoryContextSwitchTo(compact_ctx);
		cs_free_rowgroup_pages(rg_desc, natts, old_catalog_block,
							   &freelist, &fl_nranges, &fl_max);
		dir[rg_idx] = InvalidBlockNumber;
		any_compacted = true;

		MemoryContextSwitchTo(rg_ctx);
		cs_column_cache_free(&cache, natts);
	}

	MemoryContextSwitchTo(compact_ctx);
	MemoryContextDelete(rg_ctx);

	/* Flush any remaining rows in the accumulator */
	if (accum_nrows > 0)
	{
		MemoryContext write_ctx;
		BlockNumber catalog_block;

		/* Sort before writing if configured */
		{
			const char *sk = cs_get_sort_key(rel);

			if (sk != NULL)
				cs_sort_batch(rel, tupdesc, natts,
							  accum_values, accum_nulls,
							  accum_nrows, sk);
		}

		write_ctx = AllocSetContextCreate(compact_ctx,
										  "cs_compact merge flush",
										  ALLOCSET_DEFAULT_SIZES);
		MemoryContextSwitchTo(write_ctx);

		catalog_block = cs_write_one_rowgroup(rel, tupdesc, natts,
											  accum_values, accum_nulls,
											  accum_nrows, 0, NULL, NULL);

		MemoryContextSwitchTo(compact_ctx);
		MemoryContextDelete(write_ctx);

		if (n_merged >= max_merged)
		{
			max_merged *= 2;
			merged_blocks = repalloc(merged_blocks,
									 sizeof(BlockNumber) * max_merged);
		}
		merged_blocks[n_merged++] = catalog_block;
	}

	if (any_compacted)
	{
		BlockNumber rgdir_start = InvalidBlockNumber;
		uint32		rgdir_npages;
		uint32		new_count;
		BlockNumber *new_dir;
		uint32		j;

		/*
		 * Rebuild directory: kept entries (in original order) followed by
		 * merged output entries.
		 */
		new_count = 0;
		for (uint32 i = 0; i < meta.cs_nrowgroups; i++)
		{
			if (dir[i] != InvalidBlockNumber)
				new_count++;
		}
		new_count += n_merged;

		new_dir = palloc(sizeof(BlockNumber) * new_count);
		j = 0;
		for (uint32 i = 0; i < meta.cs_nrowgroups; i++)
		{
			if (dir[i] != InvalidBlockNumber)
				new_dir[j++] = dir[i];
		}
		for (uint32 i = 0; i < n_merged; i++)
			new_dir[j++] = merged_blocks[i];
		Assert(j == new_count);

		/*
		 * Fix up rg_id in catalog entries so they match their new directory
		 * position.  Merging changes the directory layout: kept entries may
		 * have shifted and merged entries were written with placeholder IDs.
		 */
		for (j = 0; j < new_count; j++)
		{
			CSRowGroupDesc *rg_desc = palloc(rg_size);

			cs_read_rowgroup_catalog(rel, new_dir[j], rg_desc, natts);
			if (rg_desc->rg_id != j)
			{
				rg_desc->rg_id = j;
				cs_update_rowgroup_catalog(rel, new_dir[j], rg_desc, natts);
			}
			pfree(rg_desc);
		}

		/*
		 * Write new directory by extending, then free old directory pages.
		 * Same principle as for row group data: old directory pages are still
		 * the recovery target until the metapage commits, so we must not
		 * overwrite them.  The freed pages enter the free list for reuse in
		 * future operations.
		 */
		rgdir_npages = cs_write_rgdir(rel, new_dir, new_count,
									  &rgdir_start);

		if (meta.cs_rgdir_start != InvalidBlockNumber &&
			meta.cs_rgdir_npages > 0)
			cs_freelist_add(&freelist, &fl_nranges, &fl_max,
							meta.cs_rgdir_start, meta.cs_rgdir_npages);

		/* Write updated free list (always extends, never from free list) */
		cs_write_freelist(rel, &meta, freelist, fl_nranges);

		/* Atomic metapage update */
		meta.cs_nrowgroups = new_count;
		meta.cs_rgdir_start = rgdir_start;
		meta.cs_rgdir_npages = rgdir_npages;
		meta.cs_flags |= CS_META_PENDING_REINDEX;
		cs_write_metapage(rel, &meta);

		pfree(new_dir);

		cs_rebuild_indexes(rel);
		cs_metapage_clear_flags(rel, CS_META_PENDING_REINDEX);
	}

	pfree(merged_blocks);
	pfree(freelist);
	pfree(dir);
	MemoryContextSwitchTo(old_ctx);
	MemoryContextDelete(compact_ctx);
}
