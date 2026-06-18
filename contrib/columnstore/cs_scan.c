/*-------------------------------------------------------------------------
 *
 * cs_scan.c
 *	  Sequential scan implementation for columnstore tables.
 *
 * Scans the delta store first (heap-format pages with MVCC visibility
 * checking), then scans columnar row groups.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * contrib/columnstore/cs_scan.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cs_internal.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/nbtree.h"
#include "catalog/pg_am_d.h"
#include "commands/defrem.h"
#include "common/pg_lzcompress.h"
#include "lib/bloomfilter.h"
#include "nodes/bitmapset.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/procarray.h"
#include "utils/array.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/numeric.h"
#include "utils/pg_locale.h"
#include "utils/snapmgr.h"

#ifdef USE_LZ4
#include <lz4.h>
#endif

/*
 * Numeric internal format constants (replicated from numeric.c).
 */
#define CS_NBASE			10000
#define CS_DEC_DIGITS		4
#define CS_NUMERIC_SHORT	0x8000
#define CS_NUMERIC_SHORT_SIGN_MASK			0x2000
#define CS_NUMERIC_SHORT_DSCALE_SHIFT		7
#define CS_NUMERIC_SHORT_WEIGHT_SIGN_MASK	0x0040
#define CS_NUMERIC_SHORT_WEIGHT_MASK		0x003F
#define CS_NUMERIC_HDRSZ_SHORT	(VARHDRSZ + sizeof(uint16))

/*
 * FOR decode helpers: vectorizable loops for aligned bits_per_value.
 *
 * These replace the generic bit-extraction state machine when the packed
 * width is byte-aligned (8, 16, or 32 bits).  The simple loop bodies
 * are auto-vectorized by gcc/clang on all major architectures (NEON,
 * SSE2, AVX2).
 */

/*
 * Fill a subrange of a decoded array with a constant value.
 * Used by both FOR decode (bits_per_value == 0) and RLE expansion.
 * pg_noinline ensures the compiler auto-vectorizes the inner loop
 * independently of the caller's complexity.
 */
static pg_noinline void
cs_fill_values(char *decoded, uint32 start, uint32 count, int attlen,
			   uint64 val)
{
	switch (attlen)
	{
		case 1:
			memset(decoded + start, (int) (val & 0xFF), count);
			break;
		case 2:
			{
				int16	   *d = (int16 *) decoded + start;
				int16		v = (int16) val;

				for (uint32 i = 0; i < count; i++)
					d[i] = v;
			}
			break;
		case 4:
			{
				int32	   *d = (int32 *) decoded + start;
				int32		v = (int32) val;

				for (uint32 i = 0; i < count; i++)
					d[i] = v;
			}
			break;
		case 8:
			{
				int64	   *d = (int64 *) decoded + start;
				int64		v = (int64) val;

				for (uint32 i = 0; i < count; i++)
					d[i] = v;
			}
			break;
	}
}

/* Fill a full decoded array with a constant value (bits_per_value == 0). */
static inline void
cs_for_fill(char *decoded, uint32 nrows, int attlen, uint64 val)
{
	cs_fill_values(decoded, 0, nrows, attlen, val);
}

/* Add min_val to every element of a full-width decoded array. */
static pg_noinline void
cs_for_add_min(char *decoded, uint32 nrows, int attlen, uint64 min_val)
{
	switch (attlen)
	{
		case 2:
			{
				int16	   *d = (int16 *) decoded;
				int16		mv = (int16) min_val;

				for (uint32 i = 0; i < nrows; i++)
					d[i] += mv;
			}
			break;
		case 4:
			{
				int32	   *d = (int32 *) decoded;
				int32		mv = (int32) min_val;

				for (uint32 i = 0; i < nrows; i++)
					d[i] += mv;
			}
			break;
		case 8:
			{
				int64	   *d = (int64 *) decoded;
				int64		mv = (int64) min_val;

				for (uint32 i = 0; i < nrows; i++)
					d[i] += mv;
			}
			break;
	}
}

/*
 * Decode byte-aligned FOR data with widening.
 *
 * The packed data has bits_per_value bits per element (8, 16, or 32),
 * which may be narrower than the output attlen.  Widen each element
 * and add min_val.
 */
static pg_noinline void
cs_for_decode_aligned(char *decoded, char *packed, uint32 nrows,
					  int attlen, int bits_per_value, uint64 min_val)
{
	if (bits_per_value == 8)
	{
		uint8	   *src = (uint8 *) packed;

		switch (attlen)
		{
			case 2:
				{
					int16	   *dst = (int16 *) decoded;
					int16		mv = (int16) min_val;

					for (uint32 i = 0; i < nrows; i++)
						dst[i] = (int16) src[i] + mv;
				}
				break;
			case 4:
				{
					int32	   *dst = (int32 *) decoded;
					int32		mv = (int32) min_val;

					for (uint32 i = 0; i < nrows; i++)
						dst[i] = (int32) src[i] + mv;
				}
				break;
			case 8:
				{
					int64	   *dst = (int64 *) decoded;
					int64		mv = (int64) min_val;

					for (uint32 i = 0; i < nrows; i++)
						dst[i] = (int64) src[i] + mv;
				}
				break;
		}
	}
	else if (bits_per_value == 16)
	{
		char	   *src = (char *) packed;

		switch (attlen)
		{
			case 4:
				{
					int32	   *dst = (int32 *) decoded;
					int32		mv = (int32) min_val;

					for (uint32 i = 0; i < nrows; i++)
						dst[i] = (int32) cs_read_u16(src + (Size) i * sizeof(uint16)) + mv;
				}
				break;
			case 8:
				{
					int64	   *dst = (int64 *) decoded;
					int64		mv = (int64) min_val;

					for (uint32 i = 0; i < nrows; i++)
						dst[i] = (int64) cs_read_u16(src + (Size) i * sizeof(uint16)) + mv;
				}
				break;
		}
	}
	else if (bits_per_value == 32)
	{
		char	   *src = (char *) packed;

		/* Only attlen=8 is possible (attlen=4 would be full-width) */
		{
			int64	   *dst = (int64 *) decoded;
			int64		mv = (int64) min_val;

			for (uint32 i = 0; i < nrows; i++)
				dst[i] = (int64) cs_read_u32(src + (Size) i * sizeof(uint32)) + mv;
		}
	}
}

/* Forward declarations */
static bool cs_delta_getnext(CScanDesc scan, ScanDirection direction,
							 TupleTableSlot *slot);
static char *cs_decompress_base(uint8 base_compression, const char *src,
								uint32 src_len, uint32 dst_len);
static bool cs_load_rowgroup_countonly(CScanDesc scan, Relation rel,
									   uint32 rg_id);
static bool cs_columnar_getnext_countonly(CScanDesc scan,
										  TupleTableSlot *slot);
static bool cs_columnar_getnext(CScanDesc scan, TupleTableSlot *slot);

static void *cs_cache_alloc(CSColumnCache *cache, Size size);
static Datum cs_byref_value(CSColumnCache *cache, int attlen, char *p, int32 len);
static void cs_column_cache_reset_columns(CSColumnCache *cache, int natts);
static void cs_cache_load_delbitmap(CSColumnCache *cache, Relation rel,
									CSRowGroupDesc *rg_desc);
static bool cs_load_rowgroup_into_internal(CSColumnCache *cache, Relation rel,
										   BlockNumber catalog_block,
										   int natts);
static void cs_ensure_column_loaded_internal(CSColumnCache *cache,
											 Relation rel, int col);
static void cs_base_decompress_column_internal(CSColumnCache *cache,
											   Relation rel, int col);
bool		cs_load_rowgroup(CScanDesc scan, Relation rel, uint32 rg_id);
bool		cs_load_rowgroup_into(CSColumnCache *cache, Relation rel,
								  BlockNumber catalog_block, int natts);
void		cs_ensure_column_loaded(CSColumnCache *cache, Relation rel,
									int col);
static void cs_zonemap_init_cmp_cache(CScanDesc scan);
static bool cs_zonemap_skip_rowgroup(CScanDesc scan, CSRowGroupDesc *rg_desc);
static void cs_init_array_scan_keys(CScanDesc scan);
static Datum cs_cache_get_value(CSColumnCache *cache, TupleDesc tupdesc,
								int col, uint32 row, bool *isnull);
static void cs_build_dict_match_arrays(CScanDesc scan, CSColumnCache *cache,
									   TupleDesc tupdesc);
static bool cs_row_passes_scan_keys(CScanDesc scan, CSColumnCache *cache,
									uint32 row);
static void cs_preload_qual_columns(CScanDesc scan, CSColumnCache *cache,
									Relation rel);
static void cs_build_selection_bitmap(CScanDesc scan, CSColumnCache *cache,
									  Relation rel);
static uint32 cs_sel_count(const char *sel_bitmap, uint32 nrows);
static bool cs_load_rowgroup_countonly(CScanDesc scan, Relation rel,
									   uint32 rg_id);
static bool cs_columnar_getnext_countonly(CScanDesc scan,
										  TupleTableSlot *slot);
static bool cs_parallel_getnextslot(TableScanDesc sscan,
									ScanDirection direction,
									TupleTableSlot *slot);


/*
 * Convert a scaled int64 to Numeric in a caller-provided buffer.
 *
 * Zero-allocation equivalent of int64_div_fast_to_numeric().  Writes the
 * Numeric directly into 'buf' (which must be >= CS_NI64_BUF_SIZE bytes).
 *
 * Uses the Numeric short format (dscale <= 63, weight in [-64,63]).
 * Falls back to palloc-based int64_div_fast_to_numeric() for edge cases.
 */
Datum
cs_int64_to_numeric_buf(int64 val, int dscale, char *buf)
{
	static const uint64 pow10[] = {1, 10, 100, 1000};
	int16		digit_buf[6];
	int			w = dscale / CS_DEC_DIGITS;
	int			m = dscale % CS_DEC_DIGITS;
	bool		neg = (val < 0);
	uint64		uval = neg ? (uint64) (-(val + 1)) + 1 : (uint64) val;
	int			first;
	int			last;
	int			ndigits;
	int			weight;
	int			pos;
	Size		len;
	uint16		header;
	char	   *ptr;

	/* Scale to align with NBASE boundary */
	if (m > 0)
	{
		uint64		factor = pow10[CS_DEC_DIGITS - m];
		uint64		new_uval = uval * factor;

		if (unlikely(new_uval / factor != uval))
			return NumericGetDatum(
								   int64_div_fast_to_numeric(val, dscale));
		uval = new_uval;
		w++;
	}

	/* Zero */
	if (uval == 0)
	{
		header = CS_NUMERIC_SHORT |
			((uint16) dscale << CS_NUMERIC_SHORT_DSCALE_SHIFT);
		SET_VARSIZE(buf, CS_NUMERIC_HDRSZ_SHORT);
		memcpy(buf + VARHDRSZ, &header, sizeof(uint16));
		return PointerGetDatum(buf);
	}

	/* Convert to NBASE digits, filling digit_buf from the end */
	pos = 6;
	ndigits = 0;
	do
	{
		uint64		newuval = uval / CS_NBASE;

		digit_buf[--pos] = (int16) (uval - newuval * CS_NBASE);
		ndigits++;
		uval = newuval;
	} while (uval);

	first = pos;
	last = 5;
	weight = ndigits - 1 - w;

	/* Strip leading zeros */
	while (first <= last && digit_buf[first] == 0)
	{
		first++;
		weight--;
	}

	/* Strip trailing zeros */
	while (last >= first && digit_buf[last] == 0)
		last--;

	ndigits = last - first + 1;
	if (ndigits <= 0)
	{
		ndigits = 0;
		weight = 0;
		neg = false;
	}

	/* Fall back for values that don't fit short format */
	if (unlikely(weight > 63 || weight < -64 || dscale > 63))
		return NumericGetDatum(
							   int64_div_fast_to_numeric(val, dscale));

	/*
	 * Build the short-format Numeric directly in buf.
	 *
	 * Layout: [varlena header (4 bytes)] [n_header (uint16)] [digits...]
	 */
	len = CS_NUMERIC_HDRSZ_SHORT + ndigits * sizeof(int16);
	SET_VARSIZE(buf, len);

	header = CS_NUMERIC_SHORT;
	if (neg)
		header |= CS_NUMERIC_SHORT_SIGN_MASK;
	header |= (uint16) dscale << CS_NUMERIC_SHORT_DSCALE_SHIFT;
	if (weight < 0)
		header |= CS_NUMERIC_SHORT_WEIGHT_SIGN_MASK;
	header |= (uint16) weight & CS_NUMERIC_SHORT_WEIGHT_MASK;

	ptr = buf + VARHDRSZ;
	memcpy(ptr, &header, sizeof(uint16));
	ptr += sizeof(uint16);

	if (ndigits > 0)
		memcpy(ptr, &digit_buf[first], ndigits * sizeof(int16));

	return PointerGetDatum(buf);
}

/*
 * Begin a sequential scan on a columnstore table.
 */
TableScanDesc
cs_scan_begin(Relation rel, Snapshot snapshot, int nkeys, ScanKeyData *key,
			  ParallelTableScanDesc pscan, uint32 flags)
{
	CScanDesc	scan;

	scan = palloc0(sizeof(CScanDescData));

	scan->rs_base.rs_rd = rel;
	scan->rs_base.rs_snapshot = snapshot;
	scan->rs_base.rs_nkeys = nkeys;
	scan->rs_base.rs_key = key;
	scan->rs_base.rs_flags = flags;
	scan->rs_base.rs_parallel = pscan;

	/* Initialize delta scan state */
	scan->delta_buf = InvalidBuffer;
	scan->delta_done = false;
	scan->delta_cur_block = InvalidBlockNumber;
	scan->delta_cur_offset = InvalidOffsetNumber;

	/*
	 * Allocated here rather than lazily in the scan loop: getnext can run
	 * inside a caller's per-row memory context (e.g. ATRewriteTable), which
	 * must not own scan-lifetime state.
	 */
	scan->delta_visible_tuples =
		palloc(sizeof(HeapTuple) * MaxHeapTuplesPerPage);
	scan->delta_tuple_cxt =
		AllocSetContextCreate(CurrentMemoryContext,
							  "columnstore delta tuples",
							  ALLOCSET_DEFAULT_SIZES);
	scan->delta_nvisible = 0;
	scan->delta_visible_idx = 0;

	/* Initialize columnar scan state */
	scan->columnar_done = false;
	scan->cur_rowgroup = 0;
	scan->cur_row_in_group = 0;

	/* Read metapage to get layout info */
	if (pscan != NULL)
	{
		/*
		 * Parallel scan: copy metadata from the shared descriptor that was
		 * initialized by cs_parallelscan_initialize.  Start with both phases
		 * "done" -- work units drive the scan via cs_parallel_getnextslot.
		 */
		CSParallelScanDesc cpscan = (CSParallelScanDesc) pscan;

		scan->delta_start = cpscan->pcs_delta_start;
		scan->delta_nblocks = cpscan->pcs_delta_nblocks;
		scan->nrowgroups = cpscan->pcs_nrowgroups;
		scan->natts = cpscan->pcs_natts;

		/*
		 * Read the row group directory from the coordinates captured in the
		 * shared descriptor (not the live metapage, which a concurrent VACUUM
		 * may have advanced past what the leader saw).
		 */
		scan->rg_catalog_blocks = cs_read_rgdir_at(rel,
												   cpscan->pcs_rgdir_start,
												   cpscan->pcs_rgdir_npages,
												   cpscan->pcs_nrowgroups);

		scan->delta_done = true;
		scan->columnar_done = true;
	}
	else if (RelationGetNumberOfBlocks(rel) > 0)
	{
		/* Non-parallel scan: read metapage directly */
		CSMetaPageData meta;

		cs_read_metapage(rel, &meta);
		scan->delta_start = meta.cs_delta_start;
		scan->delta_nblocks = meta.cs_delta_nblocks;
		scan->nrowgroups = meta.cs_nrowgroups;
		scan->natts = meta.cs_natts;
		scan->delta_cur_block = meta.cs_delta_start;
		scan->rg_catalog_blocks = cs_read_rgdir(rel, &meta);
	}
	else
	{
		scan->delta_done = true;
		scan->columnar_done = true;
		scan->rg_catalog_blocks = NULL;
	}

	/* Projection list */
	scan->needed_col_list = NULL;
	scan->needed_col_count = 0;
	scan->rg_desc_preloaded = false;

	/* Scan key and zone map state */
	scan->cs_nkeys = 0;
	scan->cs_keys = NULL;
	scan->zm_cmp_finfo = NULL;
	scan->zm_cmp_collation = NULL;
	scan->zm_cmp_initialized = false;

	/* Column buffers allocated on demand during columnar scan */
	memset(&scan->colcache, 0, sizeof(CSColumnCache));
	scan->colcache.cc_cxt =
		AllocSetContextCreate(CurrentMemoryContext,
							  "columnstore column cache",
							  ALLOCSET_DEFAULT_SIZES);

	return (TableScanDesc) scan;
}

/*
 * Free all resources in a CSColumnCache.
 */
void
cs_column_cache_free(CSColumnCache *cache, int natts)
{
	/*
	 * With a dedicated context, one reset releases everything the loaders
	 * allocated; only the struct's pointers need clearing.
	 */
	if (cache->cc_cxt)
	{
		MemoryContext cxt = cache->cc_cxt;

		MemoryContextReset(cxt);
		memset(cache, 0, sizeof(CSColumnCache));
		cache->cc_cxt = cxt;
		return;
	}

	if (cache->col_values)
	{
		for (int i = 0; i < natts; i++)
		{
			if (cache->col_dict && cache->col_dict[i])
			{
				pfree(cache->col_dict[i]->dict_values);
				pfree(cache->col_dict[i]);
				cache->col_dict[i] = NULL;
			}
			if (cache->col_null_bitmap && cache->col_null_bitmap[i])
			{
				pfree(cache->col_null_bitmap[i]);
				cache->col_null_bitmap[i] = NULL;
			}
			if (cache->col_raw_data && cache->col_raw_data[i])
			{
				pfree(cache->col_raw_data[i]);
				cache->col_raw_data[i] = NULL;
			}
			if (cache->col_values[i])
				pfree(cache->col_values[i]);
			if (cache->col_nulls[i])
				pfree(cache->col_nulls[i]);
			if (cache->col_ni64_buf && cache->col_ni64_buf[i])
				pfree(cache->col_ni64_buf[i]);
			if (cache->col_base_data && cache->col_base_data[i])
				pfree(cache->col_base_data[i]);
		}
		pfree(cache->col_values);
		pfree(cache->col_nulls);
		pfree(cache->col_loaded);
		cache->col_values = NULL;
		cache->col_nulls = NULL;
		cache->col_loaded = NULL;

		if (cache->col_raw_data)
		{
			pfree(cache->col_raw_data);
			cache->col_raw_data = NULL;
		}
		if (cache->col_null_bitmap)
		{
			pfree(cache->col_null_bitmap);
			cache->col_null_bitmap = NULL;
		}
		if (cache->col_dict)
		{
			pfree(cache->col_dict);
			cache->col_dict = NULL;
		}
		if (cache->col_ni64_dscale)
		{
			pfree(cache->col_ni64_dscale);
			cache->col_ni64_dscale = NULL;
		}
		if (cache->col_ni64_buf)
		{
			pfree(cache->col_ni64_buf);
			cache->col_ni64_buf = NULL;
		}
		if (cache->col_base_data)
		{
			pfree(cache->col_base_data);
			cache->col_base_data = NULL;
		}
		if (cache->col_point_reads)
		{
			pfree(cache->col_point_reads);
			cache->col_point_reads = NULL;
		}
	}

	if (cache->cur_rg_desc)
	{
		pfree(cache->cur_rg_desc);
		cache->cur_rg_desc = NULL;
	}
	if (cache->cur_delbitmap)
	{
		pfree(cache->cur_delbitmap);
		cache->cur_delbitmap = NULL;
	}
	cache->col_nrows = 0;
}

/*
 * End a scan, release all resources.
 */
void
cs_scan_end(TableScanDesc sscan)
{
	CScanDesc	scan = (CScanDesc) sscan;

	if (sscan->rs_flags & SO_TEMP_SNAPSHOT)
		UnregisterSnapshot(sscan->rs_snapshot);

	if (BufferIsValid(scan->delta_buf))
	{
		ReleaseBuffer(scan->delta_buf);
		scan->delta_buf = InvalidBuffer;
	}

	if (scan->delta_visible_tuples)
		pfree(scan->delta_visible_tuples);
	if (scan->delta_tuple_cxt)
		MemoryContextDelete(scan->delta_tuple_cxt);

	if (scan->rg_catalog_blocks)
		pfree(scan->rg_catalog_blocks);
	if (scan->needed_cols)
		bms_free(scan->needed_cols);
	if (scan->needed_col_list)
		pfree(scan->needed_col_list);
	if (scan->cs_keys)
		pfree(scan->cs_keys);
	if (scan->zm_cmp_finfo)
		pfree(scan->zm_cmp_finfo);
	if (scan->zm_cmp_collation)
		pfree(scan->zm_cmp_collation);
	if (scan->sa_nelem)
		pfree(scan->sa_nelem);
	if (scan->sa_elem_values)
		pfree(scan->sa_elem_values);
	if (scan->sa_elem_nulls)
		pfree(scan->sa_elem_nulls);
	if (scan->dk_match)
	{
		for (int i = 0; i < scan->cs_nkeys; i++)
		{
			if (scan->dk_match[i])
				pfree(scan->dk_match[i]);
		}
		pfree(scan->dk_match);
	}

	/* Clean up bloom filter state */
	cs_scan_set_bloom_filter((TableScanDesc) scan, 0, NULL,
							 InvalidOid, InvalidOid);

	/*
	 * All cache-lifetime scan state (column data, bitmaps, tombstones) lives
	 * in the cache context, which scan_begin always creates; a single delete
	 * frees it all.
	 */
	MemoryContextDelete(scan->colcache.cc_cxt);

	pfree(scan);
}

/*
 * Rescan: reset to the beginning.
 */
void
cs_scan_rescan(TableScanDesc sscan, ScanKeyData *key,
			   bool set_params, bool allow_strat,
			   bool allow_sync, bool allow_pagemode)
{
	CScanDesc	scan = (CScanDesc) sscan;

	if (BufferIsValid(scan->delta_buf))
	{
		ReleaseBuffer(scan->delta_buf);
		scan->delta_buf = InvalidBuffer;
	}

	scan->delta_done = false;
	scan->delta_cur_block = scan->delta_start;
	scan->delta_cur_offset = InvalidOffsetNumber;
	scan->delta_nvisible = 0;
	scan->delta_visible_idx = 0;
	MemoryContextReset(scan->delta_tuple_cxt);

	scan->columnar_done = false;
	scan->cur_rowgroup = 0;
	scan->cur_row_in_group = 0;
	scan->colcache.col_nrows = 0;

	if (scan->colcache.cur_delbitmap)
	{
		pfree(scan->colcache.cur_delbitmap);
		scan->colcache.cur_delbitmap = NULL;
	}

	scan->sel_bitmap_valid = false;

	/*
	 * The visible-tombstone set is a pure function of the snapshot, so keep
	 * it across rescans (nestloop inner scans rescan once per outer row) and
	 * re-collect only if the scan was given a new snapshot.
	 */
	if (scan->tombstones_collected &&
		scan->tombstone_snapshot != sscan->rs_snapshot)
	{
		if (scan->tombstone_tids)
		{
			pfree(scan->tombstone_tids);
			scan->tombstone_tids = NULL;
		}
		scan->tombstone_count = 0;
		scan->tombstones_collected = false;
	}

	/* Restart TABLESAMPLE and bitmap-scan state */
	scan->sample_inited = false;
	scan->bm_started = false;
	scan->bm_noffsets = 0;
	scan->bm_curoff = 0;

	/*
	 * Under a parallel scan, work units are handed out through the shared
	 * claim counter (reset by ReInitializeDSM); marking the local state
	 * "done" here makes this participant claim its first unit instead of
	 * re-emitting the whole table alongside the workers.
	 */
	if (sscan->rs_parallel != NULL)
	{
		scan->delta_done = true;
		scan->columnar_done = true;
	}

	/*
	 * Restart instrumentation: EXPLAIN ANALYZE totals would otherwise
	 * accumulate across rescans while the per-loop row counts do not.
	 */
	scan->instr_rg_examined = 0;
	scan->instr_rg_zonemap_skipped = 0;
	scan->instr_bloom_rg_probed = 0;
	scan->instr_bloom_rg_skipped = 0;
	scan->instr_bloom_rows_removed = 0;
	scan->bf_rows_examined = 0;

	if (key)
		sscan->rs_key = key;
}

/*
 * Collect tombstones visible to the current snapshot.
 *
 * Scans all delta pages for tombstone tuples (natts = 0, t_ctid in columnar
 * space) and records the target virtual TIDs of those visible under the
 * scan's snapshot.  The array is sorted for binary search during row-level
 * visibility checks.
 */
void
cs_collect_tombstones(CScanDesc scan)
{
	/*
	 * Index builds must index rows with pending or committed deletes too:
	 * older snapshots may still see them, and the fetch path hides what a
	 * given snapshot must not.  Applying tombstones here -- worse, under
	 * SnapshotAny, where even an aborted delete reads as one -- would leave
	 * those rows out of the index for good.
	 */
	Relation	rel = scan->rs_base.rs_rd;
	Snapshot	snapshot = scan->rs_base.rs_snapshot;
	BlockNumber blkno;
	BlockNumber end_blk;
	ItemPointerData *tids = NULL;
	int			count = 0;
	int			capacity = 0;

	if (scan->rs_index_build)
	{
		scan->tombstone_count = 0;
		scan->tombstones_collected = true;
		return;
	}

	scan->tombstones_collected = true;
	scan->tombstone_snapshot = snapshot;
	scan->tombstone_count = 0;
	scan->tombstone_tids = NULL;

	if (scan->delta_nblocks == 0)
		return;

	end_blk = scan->delta_start + scan->delta_nblocks;

	for (blkno = scan->delta_start; blkno < end_blk; blkno++)
	{
		Buffer		buf;
		Page		page;
		OffsetNumber maxoff;
		OffsetNumber off;

		buf = ReadBuffer(rel, blkno);
		LockBuffer(buf, BUFFER_LOCK_SHARE);
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
			HeapTupleData tuple;

			itemid = PageGetItemId(page, off);
			if (!ItemIdIsNormal(itemid))
				continue;

			htup = (HeapTupleHeader) PageGetItem(page, itemid);
			if (!CS_IS_TOMBSTONE(htup))
				continue;

			/* row locks are not deletes */
			if (CS_TOMBSTONE_IS_LOCK_ONLY(htup))
				continue;

			/*
			 * Check visibility: is this delete committed from our
			 * perspective?
			 */
			tuple.t_data = htup;
			tuple.t_len = ItemIdGetLength(itemid);
			tuple.t_tableOid = RelationGetRelid(rel);
			ItemPointerSet(&tuple.t_self, blkno, off);

			if (!cs_delta_satisfies_visibility(&tuple, snapshot))
				continue;

			/* Add target TID to array */
			if (count >= capacity)
			{
				if (capacity == 0)
				{
					/*
					 * The array must outlive any per-row context we may be
					 * called from; repalloc keeps the original context.
					 */
					capacity = 32;
					tids = MemoryContextAlloc(scan->colcache.cc_cxt,
											  sizeof(ItemPointerData) * capacity);
				}
				else
				{
					capacity *= 2;
					tids = repalloc_array(tids, ItemPointerData, capacity);
				}
			}
			tids[count++] = htup->t_ctid;
		}

		UnlockReleaseBuffer(buf);
	}

	if (count > 1)
	{
		/* Sort by TID for binary search */
		qsort(tids, count, sizeof(ItemPointerData),
			  (int (*) (const void *, const void *)) ItemPointerCompare);
	}

	scan->tombstone_tids = tids;
	scan->tombstone_count = count;
}

/*
 * Binary search for a virtual TID in a sorted tombstone array.
 */
bool
cs_tombstone_lookup(ItemPointerData *tids, int count, ItemPointer target)
{
	int			lo = 0;
	int			hi = count - 1;

	while (lo <= hi)
	{
		int			mid = lo + (hi - lo) / 2;
		int			cmp = ItemPointerCompare(&tids[mid], target);

		if (cmp == 0)
			return true;
		else if (cmp < 0)
			lo = mid + 1;
		else
			hi = mid - 1;
	}
	return false;
}

/*
 * Store the set of columns needed by the query.
 *
 * Only these columns will be decompressed during columnar scan,
 * avoiding I/O and CPU for unused columns.
 */
void
cs_scan_set_projection(TableScanDesc sscan, Bitmapset *needed_cols)
{
	CScanDesc	scan = (CScanDesc) sscan;
	int			col;
	int			count;

	if (scan->needed_cols)
		bms_free(scan->needed_cols);
	if (scan->needed_col_list)
	{
		pfree(scan->needed_col_list);
		scan->needed_col_list = NULL;
	}
	scan->needed_col_count = 0;

	if (needed_cols == NULL || bms_is_empty(needed_cols))
	{
		scan->needed_cols = NULL;
		scan->count_optimized = true;
		return;
	}

	scan->needed_cols = bms_copy(needed_cols);

	/* Build a flat sorted array for fast iteration in getsomeattrs */
	count = bms_num_members(needed_cols);
	scan->needed_col_list = palloc(sizeof(int) * count);
	scan->needed_col_count = 0;

	col = -1;
	while ((col = bms_next_member(needed_cols, col)) >= 0)
		scan->needed_col_list[scan->needed_col_count++] = col;
}

/*
 * Store scan keys pushed down by the CustomScan layer for zone map
 * filtering.  We copy the keys because the caller frees its
 * ScanKeyData array immediately after this returns.
 */
void
cs_scan_set_qual_keys(TableScanDesc sscan, int nkeys, ScanKeyData *keys)
{
	CScanDesc	scan = (CScanDesc) sscan;

	if (scan->cs_keys)
		pfree(scan->cs_keys);

	if (nkeys > 0)
	{
		scan->cs_keys = palloc(sizeof(ScanKeyData) * nkeys);
		memcpy(scan->cs_keys, keys, sizeof(ScanKeyData) * nkeys);
	}
	else
		scan->cs_keys = NULL;

	scan->cs_nkeys = nkeys;

	/*
	 * Build the comparison cache now, in the caller's (executor-lifetime)
	 * memory context: the scan loops that consult it can run inside a
	 * short-lived per-row context that must not own scan state.
	 */
	scan->zm_cmp_initialized = false;
	if (scan->zm_cmp_finfo)
	{
		pfree(scan->zm_cmp_finfo);
		scan->zm_cmp_finfo = NULL;
	}
	if (scan->zm_cmp_collation)
	{
		pfree(scan->zm_cmp_collation);
		scan->zm_cmp_collation = NULL;
	}
	/* No row groups means the cache could never be consulted */
	if (nkeys > 0 && scan->nrowgroups > 0)
		cs_zonemap_init_cmp_cache(scan);
}

/*
 * Initialize the comparison function cache for zone map evaluation.
 *
 * For each scan key, look up the btree comparison function for the
 * column's type.  This is done lazily on first use.
 */
static void
cs_zonemap_init_cmp_cache(CScanDesc scan)
{
	Relation	rel = scan->rs_base.rs_rd;
	TupleDesc	tupdesc = RelationGetDescr(rel);
	int			nkeys = scan->cs_nkeys;

	scan->zm_cmp_finfo = palloc0(sizeof(FmgrInfo) * nkeys);
	scan->zm_cmp_collation = palloc0(sizeof(Oid) * nkeys);

	for (int i = 0; i < nkeys; i++)
	{
		ScanKey		key = &scan->cs_keys[i];
		AttrNumber	attno = key->sk_attno;
		Form_pg_attribute attr;
		Oid			opclass;
		Oid			opfamily;
		Oid			opcintype;
		Oid			right_type;
		Oid			cmp_proc;

		if (attno < 1 || attno > tupdesc->natts)
			continue;

		attr = TupleDescAttr(tupdesc, attno - 1);
		opclass = GetDefaultOpClass(attr->atttypid, BTREE_AM_OID);
		if (!OidIsValid(opclass))
			continue;

		opfamily = get_opclass_family(opclass);
		opcintype = get_opclass_input_type(opclass);

		/* The scan key's right-hand side type */
		right_type = key->sk_subtype;
		if (!OidIsValid(right_type))
			right_type = opcintype;

		cmp_proc = get_opfamily_proc(opfamily, opcintype,
									 right_type, BTORDER_PROC);
		if (OidIsValid(cmp_proc))
		{
			fmgr_info(cmp_proc, &scan->zm_cmp_finfo[i]);
			scan->zm_cmp_collation[i] = attr->attcollation;
		}
	}

	scan->zm_cmp_initialized = true;
}

/*
 * Lazily initialize deconstructed array elements for SK_SEARCHARRAY scan keys.
 * Called once per scan, not per row group.
 */
static void
cs_init_array_scan_keys(CScanDesc scan)
{
	int			nkeys = scan->cs_nkeys;

	scan->dk_match = palloc0(sizeof(bool *) * nkeys);
	scan->sa_nelem = palloc0(sizeof(int) * nkeys);
	scan->sa_elem_values = palloc0(sizeof(Datum *) * nkeys);
	scan->sa_elem_nulls = palloc0(sizeof(bool *) * nkeys);
	scan->dk_match_rg = UINT32_MAX;

	for (int i = 0; i < nkeys; i++)
	{
		ScanKey		key = &scan->cs_keys[i];
		ArrayType  *arrayval;
		Oid			elemtype;
		int16		typlen;
		bool		typbyval;
		char		typalign;
		int			nelems;
		Datum	   *elem_values;
		bool	   *elem_nulls;

		if (!(key->sk_flags & SK_SEARCHARRAY))
			continue;

		arrayval = DatumGetArrayTypeP(key->sk_argument);
		elemtype = ARR_ELEMTYPE(arrayval);
		get_typlenbyvalalign(elemtype, &typlen, &typbyval, &typalign);
		deconstruct_array(arrayval, elemtype, typlen, typbyval, typalign,
						  &elem_values, &elem_nulls, &nelems);

		scan->sa_nelem[i] = nelems;
		scan->sa_elem_values[i] = elem_values;
		scan->sa_elem_nulls[i] = elem_nulls;
	}

	scan->sa_initialized = true;
}

/*
 * Retrieve a single value from the column cache for row-level filtering.
 *
 * Handles dictionary-encoded, varlen (col_values), and fixed-width by-value
 * (raw_data / NI64) columns.  The column must already be loaded via
 * cs_ensure_column_loaded().
 */
static Datum
cs_cache_get_value(CSColumnCache *cache, TupleDesc tupdesc,
				   int col, uint32 row, bool *isnull)
{
	/* Dictionary-encoded column */
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
			return (Datum) 0;
		}
		*isnull = false;
		return dict->dict_values[idx];
	}

	/* Variable-length column (Datum/bool arrays) */
	if (cache->col_values[col] != NULL)
	{
		*isnull = cache->col_nulls[col][row];
		return cache->col_values[col][row];
	}

	/* Fixed-width by-value column or NUMERIC_INT64 (raw data) */
	{
		CompactAttribute *att = TupleDescCompactAttr(tupdesc, col);

		if (cache->col_null_bitmap[col] != NULL)
		{
			if (CS_ISNULL(cache->col_null_bitmap[col], row))
			{
				*isnull = true;
				return (Datum) 0;
			}
		}

		*isnull = false;

		/*
		 * NI64 is identified by the conversion buffer, not dscale > 0 --
		 * whole-number columns are encoded with dscale 0
		 */
		if (cache->col_ni64_buf[col] != NULL)
		{
			int64		ival = ((int64 *) cache->col_raw_data[col])[row];

			return cs_int64_to_numeric_buf(ival,
										   (int) cache->col_ni64_dscale[col],
										   cache->col_ni64_buf[col]);
		}
		return cs_fetch_att(cache->col_raw_data[col] +
							(Size) row * att->attlen,
							true, att->attlen);
	}
}

/*
 * Build dictionary code match arrays for scan keys on dict-encoded columns
 * in the current row group.  Handles both SK_SEARCHARRAY (= ANY) and simple
 * BTEqual keys.
 */
static void
cs_build_dict_match_arrays(CScanDesc scan, CSColumnCache *cache,
						   TupleDesc tupdesc)
{
	int			nkeys = scan->cs_nkeys;
	ScanKeyData *keys = scan->cs_keys;

	for (int i = 0; i < nkeys; i++)
	{
		ScanKey		key = &keys[i];
		int			col = key->sk_attno - 1;
		CSDictColumn *dict;
		FmgrInfo   *cmp;
		Oid			collation;
		bool	   *match;
		bool		is_array = (key->sk_flags & SK_SEARCHARRAY) != 0;
		bool		is_equal = (key->sk_strategy == BTEqualStrategyNumber);

		/* Free previous match array if any */
		if (scan->dk_match[i])
		{
			pfree(scan->dk_match[i]);
			scan->dk_match[i] = NULL;
		}

		/* Only handle array keys and simple equality keys */
		if (!is_array && !is_equal)
			continue;
		if (col < 0 || col >= tupdesc->natts)
			continue;
		if (!cache->col_dict || cache->col_dict[col] == NULL)
			continue;

		dict = cache->col_dict[col];
		cmp = &scan->zm_cmp_finfo[i];
		if (!OidIsValid(cmp->fn_oid))
			continue;

		collation = scan->zm_cmp_collation[i];

		/*
		 * This array is rebuilt once per row group and reused across the
		 * group's rows, so it must outlive whatever (possibly per-row,
		 * reset-between-rows) context the scan callback was entered with.
		 * Allocate it in the cache context, like the selection bitmap; see
		 * cs_cache_alloc.
		 */
		match = MemoryContextAllocZero(cache->cc_cxt ? cache->cc_cxt
									   : CurrentMemoryContext,
									   sizeof(bool) * (dict->dict_count + 1));

		if (is_array)
		{
			int			nelems = scan->sa_nelem[i];
			Datum	   *elem_values = scan->sa_elem_values[i];
			bool	   *elem_nulls = scan->sa_elem_nulls[i];

			for (int d = 0; d < dict->dict_count; d++)
			{
				for (int e = 0; e < nelems; e++)
				{
					int			cmp_result;

					if (elem_nulls[e])
						continue;

					cmp_result = DatumGetInt32(
											   FunctionCall2Coll(cmp, collation,
																 dict->dict_values[d],
																 elem_values[e]));
					if (cmp_result == 0)
					{
						match[d] = true;
						break;
					}
				}
			}
		}
		else
		{
			/* Simple equality: col = const */
			if (key->sk_flags & SK_ISNULL)
				goto set_match;

			for (int d = 0; d < dict->dict_count; d++)
			{
				int			cmp_result;

				cmp_result = DatumGetInt32(
										   FunctionCall2Coll(cmp, collation,
															 dict->dict_values[d],
															 key->sk_argument));

				/*
				 * No early break: the dictionary dedups by byte image, so
				 * several entries can be btree-equal to the argument (float
				 * +0.0/-0.0, numeric 1.0/1.00); all of them match.
				 */
				if (cmp_result == 0)
					match[d] = true;
			}
		}

set_match:
		/* NULL dict index (dict_count) never matches */
		scan->dk_match[i] = match;
	}

	scan->dk_match_rg = scan->cur_rowgroup;
}

/*
 * Test whether a datum value is absent from a bloom filter.
 *
 * The filter is keyed on the 32-bit join hash value (the hashjoin builds it
 * that way via ExecBuildHash32Expr), so we hash the value with the join key's
 * hash function rather than the raw bytes.  For a single-key join this matches
 * the combined filter exactly; for a multi-key join it matches the
 * corresponding per-key filter.  'hashfn'/'coll' come from the join operator's
 * outer hash function (see cs_apply_pushed_bloom_filters); the Hash node
 * built the filter with the operator's *inner* hash function, which is safe
 * because the hash-join opfamily contract requires both sides to hash equal
 * values identically.
 */
static inline bool
bloom_lacks_value(bloom_filter *filter, FmgrInfo *hashfn, Oid coll, Datum val)
{
	uint32		hash = DatumGetUInt32(FunctionCall1Coll(hashfn, coll, val));

	return bloom_lacks_element(filter,
							   (unsigned char *) &hash, sizeof(hash));
}

/*
 * Evaluate pushed-down scan keys against a single row in the column cache.
 *
 * Returns true if the row passes all scan keys (or there are no keys).
 * Uses the same btree comparison functions cached for zone map evaluation.
 * For dict-encoded columns, uses pre-computed match arrays for fast lookup.
 */
static bool
cs_row_passes_scan_keys(CScanDesc scan, CSColumnCache *cache, uint32 row)
{
	Relation	rel = scan->rs_base.rs_rd;
	TupleDesc	tupdesc = RelationGetDescr(rel);
	int			nkeys = scan->cs_nkeys;
	ScanKeyData *keys = scan->cs_keys;
	bool		have_scan_keys = (nkeys > 0 && keys != NULL);

	if (!have_scan_keys && scan->bf_nfilters == 0)
		return true;

	if (have_scan_keys)
	{
		/* Lazily initialize comparison function cache (shared with zone maps) */
		if (!scan->zm_cmp_initialized)
			cs_zonemap_init_cmp_cache(scan);

		/* Lazily deconstruct array arguments for SK_SEARCHARRAY keys */
		if (!scan->sa_initialized)
			cs_init_array_scan_keys(scan);

		/* Rebuild dict match arrays when the row group changes */
		if (scan->dk_match_rg != scan->cur_rowgroup)
			cs_build_dict_match_arrays(scan, cache, tupdesc);
	}

	if (have_scan_keys)
	{
		for (int i = 0; i < nkeys; i++)
		{
			ScanKey		key = &keys[i];
			int			col = key->sk_attno - 1;
			FmgrInfo   *cmp;
			Oid			collation;
			Datum		val;
			bool		isnull;
			int			cmp_result;

			if (col < 0 || col >= tupdesc->natts)
				continue;

			/* IS NULL / IS NOT NULL: test the row's null status */
			if (key->sk_flags & (SK_SEARCHNULL | SK_SEARCHNOTNULL))
			{
				bool		row_isnull;

				cs_ensure_column_loaded(cache, rel, col);
				cs_cache_get_value(cache, tupdesc, col, row, &row_isnull);

				if (key->sk_flags & SK_SEARCHNULL)
				{
					if (!row_isnull)
						return false;
				}
				else
				{
					if (row_isnull)
						return false;
				}
				continue;
			}

			/* Skip keys with NULL argument (but not SK_SEARCHARRAY) */
			if ((key->sk_flags & SK_ISNULL) && !(key->sk_flags & SK_SEARCHARRAY))
				return false;

			/* Ensure the qual column is decompressed */
			cs_ensure_column_loaded(cache, rel, col);

			/*
			 * Fast path for dict-encoded columns with a pre-computed match
			 * array (both SK_SEARCHARRAY and simple BTEqual keys).
			 */
			if (scan->dk_match[i])
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

				/* NULL dict entry (idx == dict_count) never matches */
				if (dict->has_null && idx == dict->dict_count)
					return false;

				if (!scan->dk_match[i][idx])
					return false;

				continue;
			}

			/*
			 * Slow path for SK_SEARCHARRAY on non-dict columns: get the row
			 * value and compare against each array element.
			 */
			if (key->sk_flags & SK_SEARCHARRAY)
			{
				int			nelems = scan->sa_nelem[i];
				Datum	   *elem_values = scan->sa_elem_values[i];
				bool	   *elem_nulls = scan->sa_elem_nulls[i];
				bool		found = false;

				cmp = &scan->zm_cmp_finfo[i];
				if (!OidIsValid(cmp->fn_oid))
					elog(ERROR, "columnstore: no comparison function for pushed scan key %d", i);

				val = cs_cache_get_value(cache, tupdesc, col, row, &isnull);
				if (isnull)
					return false;

				collation = scan->zm_cmp_collation[i];
				for (int e = 0; e < nelems; e++)
				{
					if (elem_nulls[e])
						continue;
					cmp_result = DatumGetInt32(
											   FunctionCall2Coll(cmp, collation,
																 val, elem_values[e]));
					if (cmp_result == 0)
					{
						found = true;
						break;
					}
				}

				if (!found)
					return false;

				continue;
			}

			/* Skip columns without a cached comparison function */
			cmp = &scan->zm_cmp_finfo[i];
			if (!OidIsValid(cmp->fn_oid))
				continue;

			/* Get the row's value */
			val = cs_cache_get_value(cache, tupdesc, col, row, &isnull);

			/* NULL fails all comparisons */
			if (isnull)
				return false;

			collation = scan->zm_cmp_collation[i];
			cmp_result = DatumGetInt32(FunctionCall2Coll(cmp, collation,
														 val,
														 key->sk_argument));

			switch (key->sk_strategy)
			{
				case BTLessStrategyNumber:
					if (cmp_result >= 0)
						return false;
					break;
				case BTLessEqualStrategyNumber:
					if (cmp_result > 0)
						return false;
					break;
				case BTEqualStrategyNumber:
					if (cmp_result != 0)
						return false;
					break;
				case BTGreaterEqualStrategyNumber:
					if (cmp_result < 0)
						return false;
					break;
				case BTGreaterStrategyNumber:
					if (cmp_result <= 0)
						return false;
					break;
				default:
					break;
			}
		}
	}

	/* Bloom filter check (skipped once adaptively disabled) */
	for (int f = 0; !scan->bf_disabled && f < scan->bf_nfilters; f++)
	{
		int			bf_col = scan->bf_attnos[f] - 1;
		Datum		val;
		bool		isnull;

		cs_ensure_column_loaded(cache, rel, bf_col);

		/* Dict-encoded: use precomputed match array */
		if (cache->col_dict && cache->col_dict[bf_col] != NULL)
		{
			CSDictColumn *dict = cache->col_dict[bf_col];
			uint32		idx;

			/* Build bloom dict match array once per row group */
			if (scan->bf_dk_match_rg[f] != scan->cur_rowgroup)
			{
				if (scan->bf_dk_match[f])
					pfree(scan->bf_dk_match[f]);

				scan->bf_dk_match[f] = palloc0(sizeof(bool) * (dict->dict_count + 1));

				for (int d = 0; d < dict->dict_count; d++)
				{
					if (!bloom_lacks_value(scan->bf_filters[f],
										   &scan->bf_hashfn[f],
										   scan->bf_hashcoll[f],
										   dict->dict_values[d]))
						scan->bf_dk_match[f][d] = true;
				}

				scan->bf_dk_match_rg[f] = scan->cur_rowgroup;
			}

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

			if (!scan->bf_dk_match[f][idx])
				return false;
		}
		else
		{
			/* Raw column: test each row individually */
			val = cs_cache_get_value(cache, tupdesc, bf_col, row, &isnull);

			if (isnull)
				return false;

			if (bloom_lacks_value(scan->bf_filters[f], &scan->bf_hashfn[f],
								  scan->bf_hashcoll[f], val))
				return false;
		}
	}

	return true;
}

/*
 * Pre-load (decompress) all columns referenced by scan keys.
 *
 * Called once per row group when scan keys are present, so that
 * cs_row_passes_scan_keys() can evaluate filter predicates without
 * triggering decompression on every row.
 */
static void
cs_preload_qual_columns(CScanDesc scan, CSColumnCache *cache, Relation rel)
{
	TupleDesc	tupdesc = RelationGetDescr(rel);

	for (int i = 0; i < scan->cs_nkeys; i++)
	{
		int			col = scan->cs_keys[i].sk_attno - 1;

		if (col >= 0 && col < tupdesc->natts)
			cs_ensure_column_loaded(cache, rel, col);
	}

	/* Also preload bloom filter columns */
	for (int f = 0; f < scan->bf_nfilters; f++)
	{
		int			bf_col = scan->bf_attnos[f] - 1;

		if (bf_col >= 0 && bf_col < tupdesc->natts)
			cs_ensure_column_loaded(cache, rel, bf_col);
	}
}

/*
 * Build a selection bitmap for the current row group.
 *
 * The bitmap has one bit per row: bit set = passes all quals, bit clear =
 * filtered out.  It incorporates the deletion bitmap and all pushed-down
 * predicates (scan keys and bloom filters), so the inner scan loop only
 * needs to test bits and check tombstones.
 *
 * For fixed-width integer types the comparison is done in tight loops
 * over contiguous column arrays, enabling compiler auto-vectorization.
 * Dictionary-encoded columns use pre-computed match arrays.  Other types
 * fall back to per-row FunctionCall2Coll evaluation.
 *
 * Must be called after cs_preload_qual_columns() has decompressed all
 * qual columns for the current row group.
 */
/*
 * Count the rows still selected in a selection bitmap.  Used to measure how
 * many rows a pushed-down bloom filter removed, both for EXPLAIN ANALYZE
 * instrumentation and for the adaptive disable decision.
 */
static uint32
cs_sel_count(const char *sel_bitmap, uint32 nrows)
{
	uint32		count = 0;

	for (uint32 i = 0; i < nrows; i++)
	{
		if (CS_ISSELECTED(sel_bitmap, i))
			count++;
	}
	return count;
}

/*
 * Apply a dictionary match array to the selection bitmap.
 * Deselects rows whose dictionary code does not appear in the match table.
 */
static void
cs_sel_bitmap_apply_dict(char *sel_bitmap, CSDictColumn *dict,
						 bool *match, uint32 nrows)
{
	switch (dict->index_width)
	{
		case 1:
			{
				uint8	   *idx = (uint8 *) dict->index_data;

				for (uint32 i = 0; i < nrows; i++)
				{
					if (CS_ISSELECTED(sel_bitmap, i) && !match[idx[i]])
						CS_DESELECT_ROW(sel_bitmap, i);
				}
			}
			break;
		case 2:
			{
				char	   *idx = (char *) dict->index_data;

				for (uint32 i = 0; i < nrows; i++)
				{
					if (CS_ISSELECTED(sel_bitmap, i) &&
						!match[cs_read_u16(idx + (Size) i * sizeof(uint16))])
						CS_DESELECT_ROW(sel_bitmap, i);
				}
			}
			break;
		default:
			{
				char	   *idx = (char *) dict->index_data;

				for (uint32 i = 0; i < nrows; i++)
				{
					if (CS_ISSELECTED(sel_bitmap, i) &&
						!match[cs_read_u32(idx + (Size) i * sizeof(uint32))])
						CS_DESELECT_ROW(sel_bitmap, i);
				}
			}
			break;
	}
}

/*
 * Check whether a type OID is eligible for the fast integer comparison path.
 * Returns the signed comparison width (2, 4, or 8), or 0 if ineligible.
 */
static int
cs_fast_int_width(Oid typid)
{
	switch (typid)
	{
		case INT2OID:
			return 2;
		case INT4OID:
		case DATEOID:
			return 4;
		case INT8OID:
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
			return 8;
		default:
			return 0;
	}
}

/*
 * Apply a fast integer comparison to the selection bitmap.
 * Deselects rows that fail the btree-strategy comparison against sk_argument.
 * The null bitmap must already be AND'd into sel_bitmap before calling.
 */
static void
cs_sel_bitmap_apply_int(char *sel_bitmap, char *raw_data, uint32 nrows,
						int width, int strategy, Datum sk_argument)
{
	uint32		i;

	switch (width)
	{
		case 2:
			{
				int16	   *data = (int16 *) raw_data;
				int16		val = DatumGetInt16(sk_argument);

				switch (strategy)
				{
					case BTLessStrategyNumber:
						for (i = 0; i < nrows; i++)
							if (CS_ISSELECTED(sel_bitmap, i) && data[i] >= val)
								CS_DESELECT_ROW(sel_bitmap, i);
						break;
					case BTLessEqualStrategyNumber:
						for (i = 0; i < nrows; i++)
							if (CS_ISSELECTED(sel_bitmap, i) && data[i] > val)
								CS_DESELECT_ROW(sel_bitmap, i);
						break;
					case BTEqualStrategyNumber:
						for (i = 0; i < nrows; i++)
							if (CS_ISSELECTED(sel_bitmap, i) && data[i] != val)
								CS_DESELECT_ROW(sel_bitmap, i);
						break;
					case BTGreaterEqualStrategyNumber:
						for (i = 0; i < nrows; i++)
							if (CS_ISSELECTED(sel_bitmap, i) && data[i] < val)
								CS_DESELECT_ROW(sel_bitmap, i);
						break;
					case BTGreaterStrategyNumber:
						for (i = 0; i < nrows; i++)
							if (CS_ISSELECTED(sel_bitmap, i) && data[i] <= val)
								CS_DESELECT_ROW(sel_bitmap, i);
						break;
				}
			}
			break;

		case 4:
			{
				int32	   *data = (int32 *) raw_data;
				int32		val = DatumGetInt32(sk_argument);

				switch (strategy)
				{
					case BTLessStrategyNumber:
						for (i = 0; i < nrows; i++)
							if (CS_ISSELECTED(sel_bitmap, i) && data[i] >= val)
								CS_DESELECT_ROW(sel_bitmap, i);
						break;
					case BTLessEqualStrategyNumber:
						for (i = 0; i < nrows; i++)
							if (CS_ISSELECTED(sel_bitmap, i) && data[i] > val)
								CS_DESELECT_ROW(sel_bitmap, i);
						break;
					case BTEqualStrategyNumber:
						for (i = 0; i < nrows; i++)
							if (CS_ISSELECTED(sel_bitmap, i) && data[i] != val)
								CS_DESELECT_ROW(sel_bitmap, i);
						break;
					case BTGreaterEqualStrategyNumber:
						for (i = 0; i < nrows; i++)
							if (CS_ISSELECTED(sel_bitmap, i) && data[i] < val)
								CS_DESELECT_ROW(sel_bitmap, i);
						break;
					case BTGreaterStrategyNumber:
						for (i = 0; i < nrows; i++)
							if (CS_ISSELECTED(sel_bitmap, i) && data[i] <= val)
								CS_DESELECT_ROW(sel_bitmap, i);
						break;
				}
			}
			break;

		case 8:
			{
				int64	   *data = (int64 *) raw_data;
				int64		val = DatumGetInt64(sk_argument);

				switch (strategy)
				{
					case BTLessStrategyNumber:
						for (i = 0; i < nrows; i++)
							if (CS_ISSELECTED(sel_bitmap, i) && data[i] >= val)
								CS_DESELECT_ROW(sel_bitmap, i);
						break;
					case BTLessEqualStrategyNumber:
						for (i = 0; i < nrows; i++)
							if (CS_ISSELECTED(sel_bitmap, i) && data[i] > val)
								CS_DESELECT_ROW(sel_bitmap, i);
						break;
					case BTEqualStrategyNumber:
						for (i = 0; i < nrows; i++)
							if (CS_ISSELECTED(sel_bitmap, i) && data[i] != val)
								CS_DESELECT_ROW(sel_bitmap, i);
						break;
					case BTGreaterEqualStrategyNumber:
						for (i = 0; i < nrows; i++)
							if (CS_ISSELECTED(sel_bitmap, i) && data[i] < val)
								CS_DESELECT_ROW(sel_bitmap, i);
						break;
					case BTGreaterStrategyNumber:
						for (i = 0; i < nrows; i++)
							if (CS_ISSELECTED(sel_bitmap, i) && data[i] <= val)
								CS_DESELECT_ROW(sel_bitmap, i);
						break;
				}
			}
			break;
	}
}

/*
 * Apply null bitmap to selection bitmap.
 * For IS NULL: keep only rows where the column IS NULL.
 * For IS NOT NULL: keep only rows where the column IS NOT NULL.
 * For normal predicates: deselect null rows (nulls fail all comparisons).
 *
 * The null bitmap convention: bit=1 means NOT NULL (present).
 */
static void
cs_sel_bitmap_apply_nulls(char *sel_bitmap, char *null_bitmap,
						  uint32 nrows, bool want_null)
{
	uint32		nbytes = CS_DELBITMAP_BYTES(nrows);

	if (null_bitmap == NULL)
	{
		/* No nulls in this column */
		if (want_null)
			memset(sel_bitmap, 0, nbytes);	/* IS NULL: nothing matches */
		/* IS NOT NULL or normal: nothing to do, all rows are present */
	}
	else if (want_null)
	{
		/* IS NULL: keep only null rows (invert null bitmap) */
		for (uint32 b = 0; b < nbytes; b++)
			sel_bitmap[b] &= ~null_bitmap[b];
	}
	else
	{
		/* IS NOT NULL or normal predicate: deselect null rows */
		for (uint32 b = 0; b < nbytes; b++)
			sel_bitmap[b] &= null_bitmap[b];
	}
}

static void
cs_build_selection_bitmap(CScanDesc scan, CSColumnCache *cache, Relation rel)
{
	TupleDesc	tupdesc = RelationGetDescr(rel);
	uint32		nrows = cache->col_nrows;
	uint32		nbytes = CS_DELBITMAP_BYTES(nrows);
	int			nkeys = scan->cs_nkeys;
	ScanKeyData *keys = scan->cs_keys;

	/*
	 * Allocate or reuse the bitmap; it is reused across row groups, so it
	 * must not live in whatever per-row context the caller has current
	 */
	if (scan->sel_bitmap == NULL || scan->sel_bitmap_nrows < nrows)
	{
		if (scan->sel_bitmap)
			pfree(scan->sel_bitmap);
		scan->sel_bitmap = cs_cache_alloc(&scan->colcache, nbytes);
		scan->sel_bitmap_nrows = nrows;
	}

	/* Start with all rows selected */
	memset(scan->sel_bitmap, 0xFF, nbytes);

	/* AND in deletion bitmap (convention: bit=1 means deleted, so invert) */
	if (cache->cur_delbitmap != NULL)
	{
		for (uint32 b = 0; b < nbytes; b++)
			scan->sel_bitmap[b] &= ~cache->cur_delbitmap[b];
	}

	/* Ensure lazy-init state is ready */
	if (nkeys > 0 && keys != NULL)
	{
		if (!scan->zm_cmp_initialized)
			cs_zonemap_init_cmp_cache(scan);
		if (!scan->sa_initialized)
			cs_init_array_scan_keys(scan);
		if (scan->dk_match_rg != scan->cur_rowgroup)
			cs_build_dict_match_arrays(scan, cache, tupdesc);
	}

	/* Apply each scan key to the bitmap */
	for (int i = 0; i < nkeys; i++)
	{
		ScanKey		key = &keys[i];
		int			col = key->sk_attno - 1;
		Form_pg_attribute attr;
		char	   *null_bitmap;

		if (col < 0 || col >= tupdesc->natts)
			continue;

		attr = TupleDescAttr(tupdesc, col);

		/* IS NULL / IS NOT NULL */
		if (key->sk_flags & (SK_SEARCHNULL | SK_SEARCHNOTNULL))
		{
			bool		want_null = (key->sk_flags & SK_SEARCHNULL) != 0;

			if (cache->col_dict && cache->col_dict[col] != NULL)
			{
				/*
				 * Dict-encoded: build a match array where only the null
				 * sentinel (or non-null entries) match.
				 */
				CSDictColumn *dict = cache->col_dict[col];
				bool	   *match = palloc0(sizeof(bool) * (dict->dict_count + 1));

				if (want_null)
					match[dict->dict_count] = true; /* only null matches */
				else
				{
					for (int d = 0; d < dict->dict_count; d++)
						match[d] = true;	/* all non-null match */
				}
				cs_sel_bitmap_apply_dict(scan->sel_bitmap, dict, match, nrows);
				pfree(match);
			}
			else if (cache->col_values[col] != NULL)
			{
				/*
				 * Variable-length column: nulls are in a bool array
				 * (col_nulls[col][row]), not a bit-packed bitmap.
				 */
				bool	   *nulls = cache->col_nulls[col];

				for (uint32 row = 0; row < nrows; row++)
				{
					if (!CS_ISSELECTED(scan->sel_bitmap, row))
						continue;
					if (want_null && !nulls[row])
						CS_DESELECT_ROW(scan->sel_bitmap, row);
					else if (!want_null && nulls[row])
						CS_DESELECT_ROW(scan->sel_bitmap, row);
				}
			}
			else
			{
				null_bitmap = cache->col_null_bitmap[col];
				cs_sel_bitmap_apply_nulls(scan->sel_bitmap, null_bitmap,
										  nrows, want_null);
			}
			continue;
		}

		/* NULL scan argument (not SK_SEARCHARRAY) fails all rows */
		if ((key->sk_flags & SK_ISNULL) && !(key->sk_flags & SK_SEARCHARRAY))
		{
			memset(scan->sel_bitmap, 0, nbytes);
			break;
		}

		/* Dictionary fast path */
		if (scan->dk_match[i])
		{
			cs_sel_bitmap_apply_dict(scan->sel_bitmap,
									 cache->col_dict[col],
									 scan->dk_match[i], nrows);
			continue;
		}

		/*
		 * Fast integer comparison path: eligible for by-value fixed-width
		 * integer types that are not dict-encoded, not NI64, and not
		 * SK_SEARCHARRAY, with no cross-type comparison.
		 */
		if (!(key->sk_flags & SK_SEARCHARRAY) &&
			attr->attbyval && attr->attlen > 0 &&
			cache->col_dict[col] == NULL &&
			cache->col_ni64_dscale[col] == 0 &&
			(!OidIsValid(key->sk_subtype) ||
			 key->sk_subtype == attr->atttypid))
		{
			int			width = cs_fast_int_width(attr->atttypid);

			if (width > 0 && cache->col_raw_data[col] != NULL)
			{
				/* Deselect null rows first */
				null_bitmap = cache->col_null_bitmap[col];
				if (null_bitmap != NULL)
					cs_sel_bitmap_apply_nulls(scan->sel_bitmap, null_bitmap,
											  nrows, false);

				cs_sel_bitmap_apply_int(scan->sel_bitmap,
										cache->col_raw_data[col], nrows,
										width, key->sk_strategy,
										key->sk_argument);
				continue;
			}
		}

		/* Fallback: per-row evaluation using FunctionCall2Coll */
		{
			FmgrInfo   *cmp = &scan->zm_cmp_finfo[i];

			if (!OidIsValid(cmp->fn_oid) && !(key->sk_flags & SK_SEARCHARRAY))
				continue;

			for (uint32 row = 0; row < nrows; row++)
			{
				Datum		val;
				bool		isnull;
				int			cmp_result;

				if (!CS_ISSELECTED(scan->sel_bitmap, row))
					continue;

				val = cs_cache_get_value(cache, tupdesc, col, row, &isnull);
				if (isnull)
				{
					CS_DESELECT_ROW(scan->sel_bitmap, row);
					continue;
				}

				if (key->sk_flags & SK_SEARCHARRAY)
				{
					int			nelems = scan->sa_nelem[i];
					Datum	   *elem_values = scan->sa_elem_values[i];
					bool	   *elem_nulls = scan->sa_elem_nulls[i];
					bool		found = false;
					Oid			collation = scan->zm_cmp_collation[i];

					for (int e = 0; e < nelems; e++)
					{
						if (elem_nulls[e])
							continue;
						cmp_result = DatumGetInt32(
												   FunctionCall2Coll(cmp, collation,
																	 val, elem_values[e]));
						if (cmp_result == 0)
						{
							found = true;
							break;
						}
					}
					if (!found)
						CS_DESELECT_ROW(scan->sel_bitmap, row);
				}
				else
				{
					Oid			collation = scan->zm_cmp_collation[i];

					cmp_result = DatumGetInt32(
											   FunctionCall2Coll(cmp, collation,
																 val, key->sk_argument));
					switch (key->sk_strategy)
					{
						case BTLessStrategyNumber:
							if (cmp_result >= 0)
								CS_DESELECT_ROW(scan->sel_bitmap, row);
							break;
						case BTLessEqualStrategyNumber:
							if (cmp_result > 0)
								CS_DESELECT_ROW(scan->sel_bitmap, row);
							break;
						case BTEqualStrategyNumber:
							if (cmp_result != 0)
								CS_DESELECT_ROW(scan->sel_bitmap, row);
							break;
						case BTGreaterEqualStrategyNumber:
							if (cmp_result < 0)
								CS_DESELECT_ROW(scan->sel_bitmap, row);
							break;
						case BTGreaterStrategyNumber:
							if (cmp_result <= 0)
								CS_DESELECT_ROW(scan->sel_bitmap, row);
							break;
					}
				}
			}
		}
	}

	/*
	 * Apply bloom filter predicates pushed down from a hash join.
	 *
	 * Unlike the scan keys above, a bloom filter is a pure optimization: the
	 * producing hash join still rejects any non-matching outer tuple, so we
	 * are free to skip probing whenever it is not paying off.  Because we
	 * build the filter eagerly (HashJoin.bloom_eager), we cannot use the core
	 * executor's "wait 1000 lookups then decide" heuristic -- by the time we
	 * have probed that many rows we have already decompressed the row groups
	 * we hoped to skip.  Instead we measure the prune rate at row-group
	 * granularity and disable probing once it is clearly ineffective (e.g. a
	 * foreign-key join where every outer row matches), mirroring the core
	 * adaptive-disable idea without its per-tuple state.
	 *
	 * The disable is one-way (we do not re-enable and re-sample as the core
	 * heuristic does), so a filter that only becomes selective on later row
	 * groups -- matches clustered into the first few groups -- can be turned
	 * off prematurely.  Acceptable as a first cut; a rolling re-enable would
	 * refine it.
	 */
#define CS_BLOOM_MIN_RGS_BEFORE_DISABLE 4
#define CS_BLOOM_DISABLE_PRUNE_NUM 1	/* disable below 1/20 == 5% removed */
#define CS_BLOOM_DISABLE_PRUNE_DEN 20
	if (scan->bf_nfilters > 0 && !scan->bf_disabled)
	{
		uint32		sel_before = cs_sel_count(scan->sel_bitmap, nrows);
		uint32		sel_after;

		/* Apply bloom filter predicates */
		for (int f = 0; f < scan->bf_nfilters; f++)
		{
			int			bf_col = scan->bf_attnos[f] - 1;

			cs_ensure_column_loaded(cache, rel, bf_col);

			if (cache->col_dict && cache->col_dict[bf_col] != NULL)
			{
				CSDictColumn *dict = cache->col_dict[bf_col];

				/* Build bloom dict match array once per row group */
				if (scan->bf_dk_match_rg[f] != scan->cur_rowgroup)
				{
					if (scan->bf_dk_match[f])
						pfree(scan->bf_dk_match[f]);

					/* Rowgroup-lifetime: cache context, not a per-row one. */
					scan->bf_dk_match[f] =
						MemoryContextAllocZero(cache->cc_cxt ? cache->cc_cxt
											   : CurrentMemoryContext,
											   sizeof(bool) * (dict->dict_count + 1));
					for (int d = 0; d < dict->dict_count; d++)
					{
						if (!bloom_lacks_value(scan->bf_filters[f],
											   &scan->bf_hashfn[f],
											   scan->bf_hashcoll[f],
											   dict->dict_values[d]))
							scan->bf_dk_match[f][d] = true;
					}
					scan->bf_dk_match_rg[f] = scan->cur_rowgroup;
				}

				cs_sel_bitmap_apply_dict(scan->sel_bitmap, dict,
										 scan->bf_dk_match[f], nrows);
			}
			else
			{
				/* Non-dict column: per-row bloom filter check */
				for (uint32 row = 0; row < nrows; row++)
				{
					Datum		val;
					bool		isnull;

					if (!CS_ISSELECTED(scan->sel_bitmap, row))
						continue;

					val = cs_cache_get_value(cache, tupdesc, bf_col, row, &isnull);
					if (isnull ||
						bloom_lacks_value(scan->bf_filters[f], &scan->bf_hashfn[f],
										  scan->bf_hashcoll[f], val))
						CS_DESELECT_ROW(scan->sel_bitmap, row);
				}
			}
		}

		/* Account for what the bloom filters removed in this row group. */
		sel_after = cs_sel_count(scan->sel_bitmap, nrows);
		scan->instr_bloom_rg_probed++;
		scan->bf_rows_examined += sel_before;
		scan->instr_bloom_rows_removed += (sel_before - sel_after);
		if (sel_before > 0 && sel_after == 0)
			scan->instr_bloom_rg_skipped++;

		/*
		 * Adaptive disable: once we have probed a few row groups, give up on
		 * the filter if it is removing too few rows to be worth the probe and
		 * the extra payload columns it forces us to decompress.  This is
		 * always correct -- the hash join above still does the real matching.
		 */
		if (scan->instr_bloom_rg_probed >= CS_BLOOM_MIN_RGS_BEFORE_DISABLE &&
			scan->instr_bloom_rows_removed <
			scan->bf_rows_examined * CS_BLOOM_DISABLE_PRUNE_NUM /
			CS_BLOOM_DISABLE_PRUNE_DEN)
			scan->bf_disabled = true;
	}
#undef CS_BLOOM_MIN_RGS_BEFORE_DISABLE
#undef CS_BLOOM_DISABLE_PRUNE_NUM
#undef CS_BLOOM_DISABLE_PRUNE_DEN

	scan->sel_bitmap_valid = true;
}

/*
 * Check whether a row group can be skipped based on zone maps.
 *
 * Returns true if the row group can definitely be skipped (no matching
 * rows possible), false if it might contain matching rows.
 *
 * Supports four zone map modes (CS_ZM_BYVAL, CS_ZM_EXACT, CS_ZM_PREFIX,
 * CS_ZM_SORTKEY), IS NULL/NOT NULL pushdown, and SK_SEARCHARRAY evaluation.
 */
static bool
cs_zonemap_skip_rowgroup(CScanDesc scan, CSRowGroupDesc *rg_desc)
{
	CSZoneMap  *zonemaps = CSRowGroupGetZoneMaps(rg_desc);

	if (!scan->zm_cmp_initialized)
		cs_zonemap_init_cmp_cache(scan);

	for (int i = 0; i < scan->cs_nkeys; i++)
	{
		ScanKey		key = &scan->cs_keys[i];
		AttrNumber	attno = key->sk_attno;
		int			col;
		CSZoneMap  *zm;
		FmgrInfo   *cmp;
		Oid			collation;
		Datum		scanval;
		Datum		zm_min;
		Datum		zm_max;
		int			cmp_min;
		int			cmp_max;

		if (attno < 1 || attno > rg_desc->rg_natts)
			continue;

		col = attno - 1;
		zm = &zonemaps[col];

		/*
		 * IS NOT NULL: skip row group if all values are NULL (indicated by
		 * zm_has_minmax being false, meaning no non-null values exist).
		 *
		 * IS NULL: skip row group if no nulls exist.  We know there are no
		 * nulls when the column chunk has has_nulls=false.
		 */
		if (key->sk_flags & SK_SEARCHNOTNULL)
		{
			/*
			 * Only an explicit all-NULL marking allows the skip; a missing
			 * min/max can also mean the type has no btree opclass or the
			 * bounds were not storable, with plenty of non-null rows.
			 */
			if (zm->zm_all_null)
				return true;	/* no non-null rows in this group */
			continue;
		}
		if (key->sk_flags & SK_SEARCHNULL)
		{
			if (zm->zm_has_minmax &&
				!rg_desc->rg_columns[col].cc_has_nulls)
				return true;	/* no NULLs in this row group */
			continue;
		}

		if (!zm->zm_has_minmax)
			continue;
		if (key->sk_flags & SK_ISNULL)
			continue;

		/* Skip keys without a cached comparison function */
		cmp = &scan->zm_cmp_finfo[i];
		if (!OidIsValid(cmp->fn_oid))
			continue;

		collation = scan->zm_cmp_collation[i];
		scanval = key->sk_argument;

		/*
		 * Sort key zone maps store raw collation sort key bytes, not varlena
		 * values.  Compare by generating a sort key for the scan constant and
		 * using memcmp.
		 */
		if (zm->zm_mode == CS_ZM_SORTKEY)
		{
			pg_locale_t locale;
			char	   *str;
			ssize_t		slen;
			char		sk_buf[CS_ZONEMAP_INLINE_SIZE * 2];
			size_t		sk_len;

			if (key->sk_flags & SK_SEARCHARRAY)
			{
				bool		any_in_range = false;

				if (!scan->sa_initialized)
					cs_init_array_scan_keys(scan);

				locale = pg_newlocale_from_collation(collation);

				for (int e = 0; e < scan->sa_nelem[i]; e++)
				{
					int			cmp_lo,
								cmp_hi,
								minlen;

					if (scan->sa_elem_nulls[i][e])
						continue;

					str = VARDATA_ANY(DatumGetPointer(scan->sa_elem_values[i][e]));
					slen = VARSIZE_ANY_EXHDR(DatumGetPointer(scan->sa_elem_values[i][e]));

					sk_len = pg_strnxfrm_prefix(sk_buf, sizeof(sk_buf),
												str, slen, locale);

					minlen = Min(sk_len, zm->zm_min_len);
					cmp_lo = memcmp(sk_buf, zm->zm_min_data, minlen);
					if (cmp_lo == 0)
						cmp_lo = (sk_len < zm->zm_min_len) ? -1 :
							(sk_len > zm->zm_min_len) ? 1 : 0;
					if (cmp_lo < 0)
						continue;

					minlen = Min(sk_len, zm->zm_max_len);
					cmp_hi = memcmp(sk_buf, zm->zm_max_data, minlen);
					if (cmp_hi == 0)
						cmp_hi = (sk_len < zm->zm_max_len) ? -1 :
							(sk_len > zm->zm_max_len) ? 1 : 0;
					if (cmp_hi > 0)
						continue;

					any_in_range = true;
					break;
				}

				if (!any_in_range)
					return true;

				continue;
			}

			if (key->sk_flags & SK_ISNULL)
				continue;

			locale = pg_newlocale_from_collation(collation);
			str = VARDATA_ANY(DatumGetPointer(scanval));
			slen = VARSIZE_ANY_EXHDR(DatumGetPointer(scanval));

			sk_len = pg_strnxfrm_prefix(sk_buf, sizeof(sk_buf),
										str, slen, locale);

			switch (key->sk_strategy)
			{
				case BTLessStrategyNumber:
					{
						int			minlen = Min(sk_len, zm->zm_min_len);

						cmp_min = memcmp(zm->zm_min_data, sk_buf, minlen);
						if (cmp_min == 0)
							cmp_min = (zm->zm_min_len < sk_len) ? -1 :
								(zm->zm_min_len > sk_len) ? 1 : 0;
						if (cmp_min >= 0)
							return true;
						break;
					}
				case BTLessEqualStrategyNumber:
					{
						int			minlen = Min(sk_len, zm->zm_min_len);

						cmp_min = memcmp(zm->zm_min_data, sk_buf, minlen);
						if (cmp_min == 0)
							cmp_min = (zm->zm_min_len < sk_len) ? -1 :
								(zm->zm_min_len > sk_len) ? 1 : 0;
						if (cmp_min > 0)
							return true;
						break;
					}
				case BTEqualStrategyNumber:
					{
						int			minlen;

						minlen = Min(sk_len, zm->zm_min_len);
						cmp_min = memcmp(sk_buf, zm->zm_min_data, minlen);
						if (cmp_min == 0)
							cmp_min = (sk_len < zm->zm_min_len) ? -1 :
								(sk_len > zm->zm_min_len) ? 1 : 0;
						if (cmp_min < 0)
							return true;

						minlen = Min(sk_len, zm->zm_max_len);
						cmp_max = memcmp(sk_buf, zm->zm_max_data, minlen);
						if (cmp_max == 0)
							cmp_max = (sk_len < zm->zm_max_len) ? -1 :
								(sk_len > zm->zm_max_len) ? 1 : 0;
						if (cmp_max > 0)
							return true;
						break;
					}
				case BTGreaterEqualStrategyNumber:
					{
						int			minlen = Min(sk_len, zm->zm_max_len);

						cmp_max = memcmp(zm->zm_max_data, sk_buf, minlen);
						if (cmp_max == 0)
							cmp_max = (zm->zm_max_len < sk_len) ? -1 :
								(zm->zm_max_len > sk_len) ? 1 : 0;
						if (cmp_max < 0)
							return true;
						break;
					}
				case BTGreaterStrategyNumber:
					{
						int			minlen = Min(sk_len, zm->zm_max_len);

						cmp_max = memcmp(zm->zm_max_data, sk_buf, minlen);
						if (cmp_max == 0)
							cmp_max = (zm->zm_max_len < sk_len) ? -1 :
								(zm->zm_max_len > sk_len) ? 1 : 0;
						if (cmp_max <= 0)
							return true;
						break;
					}
				default:
					break;
			}

			continue;
		}

		/*
		 * Reconstruct Datum values.  For by-value types, zm_min/zm_max are
		 * stored directly.  For by-reference types (CS_ZM_EXACT or
		 * CS_ZM_PREFIX), point into the inline storage.
		 */
		if (zm->zm_mode == CS_ZM_EXACT || zm->zm_mode == CS_ZM_PREFIX)
		{
			zm_min = PointerGetDatum(zm->zm_min_data);
			zm_max = PointerGetDatum(zm->zm_max_data);
		}
		else
		{
			zm_min = zm->zm_min;
			zm_max = zm->zm_max;
		}

		/*
		 * For SK_SEARCHARRAY (= ANY), skip the row group only if every array
		 * element falls outside the [zm_min, zm_max] range.
		 */
		if (key->sk_flags & SK_SEARCHARRAY)
		{
			bool		any_in_range = false;

			if (!scan->sa_initialized)
				cs_init_array_scan_keys(scan);

			for (int e = 0; e < scan->sa_nelem[i]; e++)
			{
				if (scan->sa_elem_nulls[i][e])
					continue;

				cmp_min = DatumGetInt32(
										FunctionCall2Coll(cmp, collation,
														  zm_min,
														  scan->sa_elem_values[i][e]));
				if (cmp_min > 0)
					continue;
				cmp_max = DatumGetInt32(
										FunctionCall2Coll(cmp, collation,
														  zm_max,
														  scan->sa_elem_values[i][e]));
				if (cmp_max < 0)
					continue;

				any_in_range = true;
				break;
			}

			if (!any_in_range)
				return true;

			continue;
		}

		/*
		 * Compare zone map bounds against the scan value using the type's
		 * btree comparison function.
		 */
		switch (key->sk_strategy)
		{
			case BTLessStrategyNumber:
				cmp_min = DatumGetInt32(FunctionCall2Coll(cmp, collation,
														  zm_min, scanval));
				if (cmp_min >= 0)
					return true;
				break;

			case BTLessEqualStrategyNumber:
				cmp_min = DatumGetInt32(FunctionCall2Coll(cmp, collation,
														  zm_min, scanval));
				if (cmp_min > 0)
					return true;
				break;

			case BTEqualStrategyNumber:
				cmp_min = DatumGetInt32(FunctionCall2Coll(cmp, collation,
														  zm_min, scanval));
				if (cmp_min > 0)
					return true;
				cmp_max = DatumGetInt32(FunctionCall2Coll(cmp, collation,
														  zm_max, scanval));
				if (cmp_max < 0)
					return true;
				break;

			case BTGreaterEqualStrategyNumber:
				cmp_max = DatumGetInt32(FunctionCall2Coll(cmp, collation,
														  zm_max, scanval));
				if (cmp_max < 0)
					return true;
				break;

			case BTGreaterStrategyNumber:
				cmp_max = DatumGetInt32(FunctionCall2Coll(cmp, collation,
														  zm_max, scanval));
				if (cmp_max <= 0)
					return true;
				break;

			default:
				break;
		}
	}

	return false;
}

/*
 * MVCC visibility check that does not dirty the buffer page.
 *
 * HeapTupleSatisfiesVisibility() may write hint bits (HEAP_XMIN_COMMITTED,
 * HEAP_XMAX_COMMITTED, etc.) to the page tracked by the supplied buffer.
 * For heap tables that is fine, but the columnstore's delta pages are
 * managed via GenericXLog, which expects every page modification to be
 * paired with a matching GenericXLogStart/Finish.  A hint bit written
 * between two GenericXLog operations corrupts the delta computed for the
 * second one, causing standby WAL replay to detect an inconsistent page
 * and FATAL.
 *
 * Pass NoHintBitsBuffer, which the visibility code documents as "do not set
 * hint bits": the verdict is computed as usual, but the page is never
 * touched.  Delta pages are short-lived (VACUUM folds them into columnar
 * row groups), so losing the hint-bit cache on them is cheap.
 */
bool
cs_delta_satisfies_visibility(HeapTupleData *tuple, Snapshot snapshot)
{
	return HeapTupleSatisfiesVisibility(tuple, snapshot, NoHintBitsBuffer);
}

/*
 * Try to get the next visible tuple from the delta store.
 *
 * Uses page-mode scanning: pre-scans all visible tuple offsets on a page
 * under a single share lock, then yields them one at a time with just the
 * buffer pin held (no lock).  This reduces lock contention with concurrent
 * writers.
 */
static bool
cs_delta_getnext(CScanDesc scan, ScanDirection direction, TupleTableSlot *slot)
{
	Relation	rel = scan->rs_base.rs_rd;

	/*
	 * Only forward scans are implemented.  Erroring beats the silent
	 * wrong-rows alternative of treating a backward request as forward; plans
	 * over the CustomScan provider never ask (it does not set
	 * CUSTOMPATH_SUPPORT_BACKWARD_SCAN), so this guards stray callers.
	 */
	Snapshot	snapshot = scan->rs_base.rs_snapshot;

	if (!ScanDirectionIsForward(direction))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("columnstore does not support backward scans")));

	while (!scan->delta_done)
	{
		/* Return next visible tuple from pre-scanned page */
		while (scan->delta_visible_idx < scan->delta_nvisible)
		{
			CSTupleTableSlot *csslot = (CSTupleTableSlot *) slot;
			HeapTuple	tuple;

			/*
			 * Return the copy taken under the share lock in the pre-scan; the
			 * page itself is only pinned now and may be under concurrent
			 * rewrite by compaction.
			 */
			tuple = scan->delta_visible_tuples[scan->delta_visible_idx++];

			/*
			 * Deform with the SLOT's descriptor, not the relation's: during
			 * an ALTER TABLE rewrite the relcache already shows the new
			 * column types while these stored tuples (and the rewrite scan's
			 * slot) still have the old format.
			 */
			ExecClearTuple(slot);
			heap_deform_tuple(tuple, slot->tts_tupleDescriptor,
							  slot->tts_values, slot->tts_isnull);

			csslot->cs_scan = NULL;
			csslot->cs_colcache = NULL;
			csslot->cs_is_columnar = false;
			slot->tts_nvalid = slot->tts_tupleDescriptor->natts;
			slot->tts_flags &= ~TTS_FLAG_EMPTY;
			slot->tts_tid = tuple->t_self;
			slot->tts_tableOid = tuple->t_tableOid;
			return true;
		}

		/* Release current page if any */
		if (BufferIsValid(scan->delta_buf))
		{
			ReleaseBuffer(scan->delta_buf);
			scan->delta_buf = InvalidBuffer;
			scan->delta_cur_block++;
		}

		/* Check if we've scanned all delta pages */
		if (scan->delta_cur_block >= scan->delta_start + scan->delta_nblocks)
		{
			scan->delta_done = true;
			break;
		}

		/*
		 * Read the next page and pre-scan for all visible tuples under a
		 * single share lock, then release the lock.  The pin is kept so that
		 * tuple data remains valid while we return rows one at a time.
		 */
		scan->delta_buf = ReadBuffer(rel, scan->delta_cur_block);
		LockBuffer(scan->delta_buf, BUFFER_LOCK_SHARE);

		{
			Page		page = BufferGetPage(scan->delta_buf);
			OffsetNumber maxoff;

			scan->delta_nvisible = 0;
			scan->delta_visible_idx = 0;
			MemoryContextReset(scan->delta_tuple_cxt);

			/*
			 * Skip foreign pages: concurrent relation extension can
			 * interleave column data pages into the nominal delta range (see
			 * CSDeltaPageOpaque); their bytes are not heap tuples.
			 */
			maxoff = CSPageIsDelta(page) ?
				PageGetMaxOffsetNumber(page) : InvalidOffsetNumber;

			Assert(scan->delta_visible_tuples != NULL);

			for (OffsetNumber off = FirstOffsetNumber; off <= maxoff; off++)
			{
				ItemId		itemid;
				HeapTupleData tuple;
				MemoryContext oldcxt;

				itemid = PageGetItemId(page, off);
				if (!ItemIdIsNormal(itemid))
					continue;

				tuple.t_data = (HeapTupleHeader) PageGetItem(page, itemid);
				tuple.t_len = ItemIdGetLength(itemid);
				tuple.t_tableOid = RelationGetRelid(rel);
				ItemPointerSet(&tuple.t_self, scan->delta_cur_block, off);

				/* Skip tombstone tuples — they are not data rows */
				if (CS_IS_TOMBSTONE(tuple.t_data))
					continue;

				if (scan->rs_index_build)
				{
					/*
					 * Index everything except inserts known to have aborted:
					 * rows deleted-but-visible-to-someone and in-progress
					 * inserts must all be reachable through the index;
					 * per-snapshot filtering happens at fetch. Check
					 * in-progress before committed (see xact.c).
					 */
					TransactionId xmin = HeapTupleHeaderGetRawXmin(tuple.t_data);

					if (tuple.t_data->t_infomask & HEAP_XMIN_INVALID)
						continue;
					if (!(tuple.t_data->t_infomask & HEAP_XMIN_COMMITTED) &&
						!TransactionIdIsCurrentTransactionId(xmin) &&
						!TransactionIdIsInProgress(xmin) &&
						!TransactionIdDidCommit(xmin))
						continue;	/* aborted insert */
				}
				else if (!cs_delta_satisfies_visibility(&tuple, snapshot))
					continue;

				/*
				 * Copy the tuple into scan memory while we still hold the
				 * share lock; rows are returned from this copy, not by
				 * re-reading the (then unlocked) page.
				 */
				oldcxt = MemoryContextSwitchTo(scan->delta_tuple_cxt);
				scan->delta_visible_tuples[scan->delta_nvisible] =
					heap_copytuple(&tuple);
				MemoryContextSwitchTo(oldcxt);
				scan->delta_nvisible++;
			}
		}

		LockBuffer(scan->delta_buf, BUFFER_LOCK_UNLOCK);
	}

	return false;
}

/*
 * Read the row group catalog entry, which may span multiple pages.
 *
 * cs_write_rowgroup_catalog writes the entry across consecutive pages via
 * cs_write_column_data when header + per-column chunk descriptors +
 * per-column zone maps exceed CS_COLDATA_PER_PAGE; mirror that page-by-page
 * here.  Reading only the first page silently returned uninitialized memory
 * for any column whose zone map fell past the page boundary, which on tables
 * with roughly 73 or more columns causes pushed-down qual evaluation to
 * misjudge zone-map skip and drop matching rows.
 */
void
cs_read_rowgroup_catalog(Relation rel, BlockNumber catalog_block,
						 CSRowGroupDesc *rg_desc, int16 natts)
{
	Size		entry_size = CSRowGroupDescSize(natts);
	Size		offset = 0;
	BlockNumber blk = catalog_block;

	while (offset < entry_size)
	{
		Buffer		buf;
		Page		page;
		Size		chunk;

		buf = ReadBuffer(rel, blk++);
		LockBuffer(buf, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buf);

		chunk = Min(entry_size - offset, (Size) CS_COLDATA_PER_PAGE);
		memcpy((char *) rg_desc + offset, PageGetContents(page), chunk);

		UnlockReleaseBuffer(buf);
		offset += chunk;

		/*
		 * The first page contains the fixed header, including the column
		 * count this row group was written with.  If the relation has since
		 * grown columns (ALTER TABLE ADD COLUMN), the caller's entry_size
		 * exceeds what is stored; clamp so we do not read past the entry into
		 * unrelated pages.  The unread tail of the caller's buffer is zeroed;
		 * readers must not consult chunk descriptors at or beyond rg_natts
		 * anyway.
		 */
		if (rg_desc->rg_natts > 0 && rg_desc->rg_natts < natts)
		{
			Size		stored_size = CSRowGroupDescSize(rg_desc->rg_natts);

			if (stored_size < entry_size)
			{
				if (offset < entry_size)
					memset((char *) rg_desc + Max(offset, stored_size), 0,
						   entry_size - Max(offset, stored_size));
				entry_size = stored_size;
			}
		}
	}
}

/*
 * Read column data from pages into a buffer.
 */
char *
cs_read_column_pages(Relation rel, BlockNumber start_block,
					 uint32 npages, uint32 data_len)
{
	/*
	 * Over-allocate by a word: the FOR bit-unpacking decoder reads its input
	 * through a 64-bit shift buffer and may fetch up to seven bytes past the
	 * packed data while draining the final row.  The slack keeps that read
	 * inside the allocation instead of relying on allocator rounding (a
	 * dedicated small memory context places buffers right at the end of a
	 * block, where the overread faults).
	 */
	char	   *data = palloc(data_len + sizeof(uint64));
	uint32		offset = 0;

	for (uint32 i = 0; i < npages && offset < data_len; i++)
	{
		Buffer		buf;
		Page		page;
		uint32		chunk;

		buf = ReadBuffer(rel, start_block + i);
		LockBuffer(buf, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buf);

		chunk = Min(data_len - offset,
					(uint32) (BLCKSZ - MAXALIGN(SizeOfPageHeaderData)));
		memcpy(data + offset, PageGetContents(page), chunk);

		UnlockReleaseBuffer(buf);
		offset += chunk;
	}

	return data;
}


/*
 * Context-switching wrappers for the column-cache loaders.
 *
 * When the cache has a dedicated memory context, every allocation the
 * loaders make must land in it: these entry points are reached from the
 * custom slot's getsomeattrs callback and from raw table-AM scans, where
 * CurrentMemoryContext can be a per-row context that the caller resets
 * between rows (e.g. ATRewriteTable), leaving the cache dangling.
 */
bool
cs_load_rowgroup_into(CSColumnCache *cache, Relation rel,
					  BlockNumber catalog_block, int natts)
{
	MemoryContext oldcxt = NULL;
	bool		result;

	if (cache->cc_cxt)
		oldcxt = MemoryContextSwitchTo(cache->cc_cxt);
	result = cs_load_rowgroup_into_internal(cache, rel, catalog_block, natts);
	if (oldcxt)
		MemoryContextSwitchTo(oldcxt);
	return result;
}

void
cs_ensure_column_loaded(CSColumnCache *cache, Relation rel, int col)
{
	MemoryContext oldcxt = NULL;

	/* the common already-loaded case needs no context switch */
	if (cache->col_loaded[col])
		return;

	if (cache->cc_cxt)
		oldcxt = MemoryContextSwitchTo(cache->cc_cxt);
	cs_ensure_column_loaded_internal(cache, rel, col);
	if (oldcxt)
		MemoryContextSwitchTo(oldcxt);
}

void
cs_base_decompress_column(CSColumnCache *cache, Relation rel, int col)
{
	MemoryContext oldcxt = NULL;

	if (cache->cc_cxt)
		oldcxt = MemoryContextSwitchTo(cache->cc_cxt);
	cs_base_decompress_column_internal(cache, rel, col);
	if (oldcxt)
		MemoryContextSwitchTo(oldcxt);
}

/*
 * Allocate from the cache's dedicated context when it has one, else from
 * CurrentMemoryContext.  All cache-lifetime allocations made outside the
 * cs_load_rowgroup_into()/cs_ensure_column_loaded() wrappers must go
 * through this, since the caller's context can be a per-row context that
 * is reset while the cache lives on.
 */
static void *
cs_cache_alloc(CSColumnCache *cache, Size size)
{
	if (cache->cc_cxt)
		return MemoryContextAlloc(cache->cc_cxt, size);
	return palloc(size);
}

/*
 * Return a by-reference column value that is stored unaligned in the packed
 * columnar buffer as a Datum the executor can use safely.
 *
 * The columnar format packs values back-to-back with no alignment padding, so
 * a value pointer is generally not aligned to the type's natural boundary.
 * That is fine for cstrings (byte-aligned) and for short-header (1-byte)
 * varlenas, which are alignment-agnostic by construction.  But a long-header
 * (4-byte) varlena, and a fixed-length by-reference value, must be aligned:
 * some type functions dereference them through aligned struct types (e.g.
 * numeric_out casts to NumericData *, which requires 4-byte alignment), which
 * is undefined behavior -- and a hard trap under -fsanitize=alignment -- on a
 * misaligned address.  Copy those into MAXALIGN'd cache memory; leave the
 * alignment-safe cases zero-copy.
 */
static Datum
cs_byref_value(CSColumnCache *cache, int attlen, char *p, int32 len)
{
	char	   *aligned;

	if (attlen == -2)			/* cstring: byte-aligned, safe in place */
		return CStringGetDatum(p);
	if (attlen == -1 && VARATT_IS_1B(p))	/* short varlena: safe in place */
		return PointerGetDatum(p);

	/* Long varlena or fixed-length by-ref: must be aligned. */
	aligned = cs_cache_alloc(cache, len);
	memcpy(aligned, p, len);
	return PointerGetDatum(aligned);
}

/*
 * Reset per-column cache state for a new row group: allocate the
 * per-column pointer arrays on first use, then free data left over from
 * the previous row group and clear the per-column flags.  Allocations are
 * made in the cache's dedicated context when one exists.
 */
static void
cs_column_cache_reset_columns(CSColumnCache *cache, int natts)
{
	MemoryContext oldcxt = NULL;
	int			i;

	if (cache->cc_cxt)
		oldcxt = MemoryContextSwitchTo(cache->cc_cxt);

	/* Allocate column buffer arrays if not yet allocated */
	if (!cache->col_values)
	{
		cache->col_values = palloc0(sizeof(Datum *) * natts);
		cache->col_nulls = palloc0(sizeof(bool *) * natts);
		cache->col_loaded = palloc0(sizeof(bool) * natts);
		cache->col_raw_data = palloc0(sizeof(char *) * natts);
		cache->col_null_bitmap = palloc0(sizeof(char *) * natts);
		cache->col_dict = palloc0(sizeof(CSDictColumn *) * natts);
		cache->col_ni64_dscale = palloc0(sizeof(uint16) * natts);
		cache->col_ni64_buf = palloc0(sizeof(char *) * natts);
		cache->col_base_data = palloc0(sizeof(char *) * natts);
		cache->col_point_reads = palloc0(sizeof(uint16) * natts);
	}

	/* Free old column data from previous row group */
	for (i = 0; i < natts; i++)
	{
		if (cache->col_dict[i])
		{
			pfree(cache->col_dict[i]->dict_values);
			pfree(cache->col_dict[i]);
			cache->col_dict[i] = NULL;
		}
		if (cache->col_null_bitmap[i])
		{
			pfree(cache->col_null_bitmap[i]);
			cache->col_null_bitmap[i] = NULL;
		}
		if (cache->col_raw_data[i])
		{
			pfree(cache->col_raw_data[i]);
			cache->col_raw_data[i] = NULL;
		}
		if (cache->col_values[i])
		{
			pfree(cache->col_values[i]);
			cache->col_values[i] = NULL;
		}
		if (cache->col_nulls[i])
		{
			pfree(cache->col_nulls[i]);
			cache->col_nulls[i] = NULL;
		}

		/*
		 * col_ni64_buf doubles as the "this column is NI64" flag for
		 * cs_cache_get_value(), cs_scan_get_raw_attr() and
		 * cs_scan_column_encoding(); a stale entry would make a non-NI64 row
		 * group be misread as int64 data.
		 */
		if (cache->col_ni64_buf[i])
		{
			pfree(cache->col_ni64_buf[i]);
			cache->col_ni64_buf[i] = NULL;
		}
		cache->col_ni64_dscale[i] = 0;
		cache->col_loaded[i] = false;
		if (cache->col_base_data[i])
		{
			pfree(cache->col_base_data[i]);
			cache->col_base_data[i] = NULL;
		}
		cache->col_point_reads[i] = 0;
	}

	if (oldcxt)
		MemoryContextSwitchTo(oldcxt);
}

/*
 * Replace the cached deletion bitmap with the one for the row group
 * described by rg_desc (none if the row group has no deleted rows).
 */
static void
cs_cache_load_delbitmap(CSColumnCache *cache, Relation rel,
						CSRowGroupDesc *rg_desc)
{
	MemoryContext oldcxt = NULL;

	if (cache->cc_cxt)
		oldcxt = MemoryContextSwitchTo(cache->cc_cxt);

	if (cache->cur_delbitmap)
	{
		pfree(cache->cur_delbitmap);
		cache->cur_delbitmap = NULL;
	}

	if (rg_desc->rg_delbitmap_block != InvalidBlockNumber)
	{
		cache->cur_delbitmap = cs_read_delbitmap(rel,
												 rg_desc->rg_delbitmap_block,
												 rg_desc->rg_num_rows);
	}

	if (oldcxt)
		MemoryContextSwitchTo(oldcxt);
}

/*
 * Core row group loader: loads a row group into any CSColumnCache.
 *
 * Reads the catalog entry from the given catalog_block and allocates
 * (but does NOT populate) column buffers.  Actual column decompression
 * is deferred to cs_ensure_column_loaded().
 */
static bool
cs_load_rowgroup_into_internal(CSColumnCache *cache, Relation rel,
							   BlockNumber catalog_block, int natts)
{
	CSRowGroupDesc *rg_desc;
	Size		rg_size = CSRowGroupDescSize(natts);

	if (!cache->cur_rg_desc)
		cache->cur_rg_desc = cs_cache_alloc(cache, rg_size);

	rg_desc = cache->cur_rg_desc;
	cs_read_rowgroup_catalog(rel, catalog_block, rg_desc, natts);

	if (rg_desc->rg_num_rows == 0)
		return false;

	cs_column_cache_reset_columns(cache, natts);
	cs_cache_load_delbitmap(cache, rel, rg_desc);

	cache->col_nrows = rg_desc->rg_num_rows;
	return true;
}

/*
 * Decompress src_len bytes of column data using the given base
 * compression method (CS_COMPRESS_LZ4, CS_COMPRESS_PGLZ).  The caller
 * asserts that src_len is the compressed size and dst_len the
 * uncompressed size, and must skip the call entirely for
 * CS_COMPRESS_NONE.  Returns a freshly palloc'd buffer of dst_len
 * bytes; ereports if the data is corrupt or the method is unknown.
 */
static char *
cs_decompress_base(uint8 base_compression, const char *src,
				   uint32 src_len, uint32 dst_len)
{
	/*
	 * Over-allocate by a word for the same reason as cs_read_column_pages:
	 * the decompressed bytes can be FOR bit-packed data, whose decoder may
	 * read up to seven bytes past the end of its input.
	 */
	char	   *dst = palloc(dst_len + sizeof(uint64));

	switch (base_compression)
	{
#ifdef USE_LZ4
		case CS_COMPRESS_LZ4:
			if (LZ4_decompress_safe(src, dst, src_len, dst_len) < 0)
				ereport(ERROR,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("columnstore LZ4 decompression failed")));
			break;
#endif
		case CS_COMPRESS_PGLZ:
			if (pglz_decompress(src, src_len, dst, dst_len, true) < 0)
				ereport(ERROR,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("columnstore PGLZ decompression failed")));
			break;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("unknown columnstore compression method %d",
							base_compression)));
	}

	return dst;
}

/*
 * Ensure a column is fully decompressed and loaded into the cache.
 *
 * Handles all encoding layers: base compression (LZ4/PGLZ), dictionary,
 * NUMERIC_INT64, and frame-of-reference (FOR).  After this call,
 * col_loaded[col] is true and the column data is accessible through
 * col_values/col_nulls (varlen), col_raw_data (fixed-len by-val or NI64),
 * or col_dict (dictionary-encoded).
 */
static void
cs_ensure_column_loaded_internal(CSColumnCache *cache, Relation rel, int col)
{
	TupleDesc	tupdesc = cache->cc_tupdesc ? cache->cc_tupdesc :
		RelationGetDescr(rel);
	Form_pg_attribute attr = TupleDescAttr(tupdesc, col);
	CSRowGroupDesc *rg_desc = cache->cur_rg_desc;
	CSColumnChunkDesc *chunk;
	uint32		nrows = rg_desc->rg_num_rows;
	uint8		base_compression;
	uint8		preenc;
	bool		is_ni64;
	bool		is_for;
	bool		is_delta;
	bool		is_gorilla;
	bool		is_dict;
	bool		is_rle;
	char	   *raw_data;

	if (cache->col_loaded[col])
		return;

	/*
	 * A column added (ALTER TABLE ADD COLUMN) after this row group was
	 * written has no chunk here -- rg_columns[col] would index past the
	 * stored descriptors.  Materialize the attribute's missing value (the ADD
	 * COLUMN ... DEFAULT value, or NULL), as heap does for tuples whose natts
	 * predate the column.
	 */
	if (col >= rg_desc->rg_natts)
	{
		Datum		missing;
		bool		isnull;

		missing = getmissingattr(tupdesc, col + 1, &isnull);
		cache->col_values[col] = palloc(sizeof(Datum) * nrows);
		cache->col_nulls[col] = palloc(sizeof(bool) * nrows);
		for (uint32 row = 0; row < nrows; row++)
		{
			cache->col_values[col][row] = missing;
			cache->col_nulls[col][row] = isnull;
		}
		cache->col_loaded[col] = true;
		return;
	}

	chunk = &rg_desc->rg_columns[col];
	base_compression = CS_COMPRESS_BASE(chunk->cc_compression);
	preenc = CS_PREENC(chunk->cc_compression);
	is_ni64 = (preenc == CS_PREENC_NI64 ||
			   preenc == CS_PREENC_NI64_FOR ||
			   preenc == CS_PREENC_NI64_DELTA ||
			   preenc == CS_PREENC_NI64_DELTA_FOR);
	is_for = (preenc == CS_PREENC_FOR ||
			  preenc == CS_PREENC_NI64_FOR ||
			  preenc == CS_PREENC_DELTA_FOR ||
			  preenc == CS_PREENC_NI64_DELTA_FOR);
	is_delta = (preenc == CS_PREENC_DELTA ||
				preenc == CS_PREENC_DELTA_FOR ||
				preenc == CS_PREENC_NI64_DELTA ||
				preenc == CS_PREENC_NI64_DELTA_FOR);
	is_gorilla = (preenc == CS_PREENC_GORILLA);
	is_dict = (preenc == CS_PREENC_DICT);
	is_rle = (preenc == CS_PREENC_RLE);

	/*
	 * Reuse base-decompressed data from point reads if available, otherwise
	 * read and base-decompress from disk.
	 */

	/*
	 * Sanity-check the chunk descriptor before trusting it for sizing: these
	 * values come from disk, and a corrupted catalog page must surface as an
	 * error rather than wild reads.  The cheap invariants: the compressed
	 * bytes must fit the pages claimed, an NI64 chunk must at least hold its
	 * dscale header, and a base-compressed chunk must announce a plausible
	 * uncompressed size.
	 */
	if (chunk->cc_compressed_size >
		(uint64) chunk->cc_npages * CS_COLDATA_PER_PAGE ||
		(is_ni64 && chunk->cc_uncompressed_size < sizeof(uint16)) ||
		(base_compression != CS_COMPRESS_NONE &&
		 chunk->cc_uncompressed_size == 0))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("columnstore: corrupted column chunk descriptor for attribute %d", col + 1),
				 errdetail("Row group claims %u compressed bytes in %u pages (uncompressed %u).",
						   chunk->cc_compressed_size, chunk->cc_npages,
						   chunk->cc_uncompressed_size)));

	if (cache->col_base_data[col])
	{
		raw_data = cache->col_base_data[col];
		cache->col_base_data[col] = NULL;	/* transfer ownership */
	}
	else
	{
		raw_data = cs_read_column_pages(rel, chunk->cc_start_block,
										chunk->cc_npages,
										chunk->cc_compressed_size);

		if (base_compression != CS_COMPRESS_NONE)
		{
			char	   *decompressed = cs_decompress_base(base_compression,
														  raw_data,
														  chunk->cc_compressed_size,
														  chunk->cc_uncompressed_size);

			pfree(raw_data);
			raw_data = decompressed;
		}
	}

	/*
	 * If NUMERIC_INT64, strip the dscale header first so FOR/plain int64
	 * decode can work on the remainder.
	 */
	if (is_ni64)
	{
		uint16		ni64_dscale;
		uint32		remainder = chunk->cc_uncompressed_size - sizeof(uint16);
		char	   *shifted = palloc(remainder);

		memcpy(&ni64_dscale, raw_data, sizeof(uint16));
		memcpy(shifted, raw_data + sizeof(uint16), remainder);
		pfree(raw_data);
		raw_data = shifted;

		cache->col_ni64_dscale[col] = ni64_dscale;
	}

	/*
	 * If FOR-encoded, decode bit-packed values back into a flat integer
	 * array.  For DELTA+FOR, this produces the delta array which is then
	 * integrated below.
	 */
	if (is_for)
	{
		int			attlen = is_ni64 ? (int) sizeof(int64) : attr->attlen;
		uint32		bitmap_size = chunk->cc_has_nulls ? (nrows + 7) / 8 : 0;
		char	   *ptr = raw_data + bitmap_size;
		uint64		min_val;
		uint8		bits_per_value;
		char	   *decoded;
		uint32		decoded_size;

		/* Read FOR header */
		min_val = cs_fetch_att(ptr, true, attlen);
		ptr += attlen;
		bits_per_value = (uint8) *ptr++;

		/* Decode bit-packed values into flat array */
		decoded_size = (uint32) attlen * nrows;
		decoded = palloc(decoded_size);

		if (bits_per_value == 0)
		{
			/* All values identical: vectorizable fill */
			cs_for_fill(decoded, nrows, attlen, min_val);
		}
		else if (bits_per_value == attlen * 8)
		{
			/* Full-width: data is already a native array, memcpy + add */
			memcpy(decoded, ptr, (Size) attlen * nrows);
			if (min_val != 0)
				cs_for_add_min(decoded, nrows, attlen, min_val);
		}
		else if (bits_per_value == 8 || bits_per_value == 16 ||
				 bits_per_value == 32)
		{
			/* Byte-aligned sub-width: widen + add in vectorizable loop */
			cs_for_decode_aligned(decoded, ptr, nrows, attlen,
								  bits_per_value, min_val);
		}
		else
		{
			/* Generic bit-extraction for non-aligned widths */
			uint64		bit_buffer = 0;
			int			bits_in_buffer = 0;
			uint8	   *in = (uint8 *) ptr;
			uint64		mask = ((uint64) 1 << bits_per_value) - 1;

			for (uint32 row = 0; row < nrows; row++)
			{
				uint64		delta;

				while (bits_in_buffer < bits_per_value)
				{
					bit_buffer |= ((uint64) *in++) << bits_in_buffer;
					bits_in_buffer += 8;
				}

				delta = bit_buffer & mask;
				bit_buffer >>= bits_per_value;
				bits_in_buffer -= bits_per_value;

				cs_store_att_byval(decoded + (Size) row * attlen,
								   min_val + delta, attlen);
			}
		}

		/*
		 * Copy null bitmap separately so each allocation can be freed
		 * independently.
		 */
		if (chunk->cc_has_nulls)
		{
			char	   *bm_copy = palloc(bitmap_size);

			memcpy(bm_copy, raw_data, bitmap_size);
			cache->col_null_bitmap[col] = bm_copy;
		}
		pfree(raw_data);
		raw_data = NULL;

		/*
		 * If DELTA+FOR, the decoded array contains deltas.  Integrate them
		 * into absolute values by computing a prefix sum.
		 */
		if (is_delta)
		{
			uint64		current;
			char	   *null_bm = cache->col_null_bitmap[col];

			/*
			 * The base value is encoded as the FOR min_val for the first
			 * non-null row (since its delta is 0, FOR stored min_val for it).
			 * We use the first non-null decoded value as the starting point.
			 */
			current = 0;
			for (uint32 row = 0; row < nrows; row++)
			{
				uint64		d;
				uint64		v;

				if (null_bm && CS_ISNULL(null_bm, row))
					continue;

				d = cs_fetch_att(decoded + (Size) row * attlen, true, attlen);
				v = current + d;
				cs_store_att_byval(decoded + (Size) row * attlen, v, attlen);
				current = v;
			}
		}

		cache->col_raw_data[col] = decoded;

		if (is_ni64)
		{
			if (cache->col_ni64_buf[col] == NULL)
				cache->col_ni64_buf[col] = palloc(CS_NI64_BUF_SIZE);
		}

		cache->col_loaded[col] = true;
		return;
	}

	/*
	 * DELTA without FOR: raw_data is [null_bitmap] [base_value]
	 * [delta_array]. The base_value header is redundant (delta[0] for the
	 * first non-null row is the absolute value), so we skip it and just
	 * integrate the deltas.
	 */
	if (is_delta)
	{
		int			attlen = is_ni64 ? (int) sizeof(int64) : attr->attlen;
		uint32		bitmap_size = chunk->cc_has_nulls ? (nrows + 7) / 8 : 0;
		char	   *ptr = raw_data + bitmap_size;
		uint64		current;
		char	   *decoded;
		uint32		decoded_size;

		/* Skip base value header (redundant with delta[0]) */
		ptr += attlen;

		/* Read and integrate delta array */
		decoded_size = (uint32) attlen * nrows;
		decoded = palloc(decoded_size);
		current = 0;

		for (uint32 row = 0; row < nrows; row++)
		{
			uint64		d;
			uint64		v;

			if (chunk->cc_has_nulls && CS_ISNULL(raw_data, row))
			{
				cs_store_att_byval(decoded + (Size) row * attlen, (Datum) 0,
								   attlen);
				ptr += attlen;
				continue;
			}

			d = cs_fetch_att(ptr, true, attlen);
			ptr += attlen;
			v = current + d;

			cs_store_att_byval(decoded + (Size) row * attlen, v, attlen);
			current = v;
		}

		if (chunk->cc_has_nulls)
		{
			char	   *bm_copy = palloc(bitmap_size);

			memcpy(bm_copy, raw_data, bitmap_size);
			cache->col_null_bitmap[col] = bm_copy;
		}
		pfree(raw_data);

		cache->col_raw_data[col] = decoded;

		if (is_ni64)
		{
			if (cache->col_ni64_buf[col] == NULL)
				cache->col_ni64_buf[col] = palloc(CS_NI64_BUF_SIZE);
		}

		cache->col_loaded[col] = true;
		return;
	}

	/*
	 * Gorilla-encoded: delta-of-delta for integers, XOR for floats. Decode
	 * the variable-length bitstream back into a flat array.
	 */
	if (is_gorilla)
	{
		int			attlen = is_ni64 ? (int) sizeof(int64) : attr->attlen;
		uint32		bitmap_size = chunk->cc_has_nulls ? (nrows + 7) / 8 : 0;
		char	   *ptr = raw_data + bitmap_size;
		uint8		type_tag;
		char	   *decoded;
		uint32		decoded_size;
		uint8	   *in;
		uint64		bit_buffer = 0;
		int			bits_in_buffer = 0;

		type_tag = *ptr++;

		decoded_size = (uint32) attlen * nrows;
		decoded = palloc(decoded_size);

/* Helper: read N bits from the input stream */
#define GORILLA_GET_BITS(nbits) \
		({ \
			uint64 _val; \
			while (bits_in_buffer < (nbits)) { \
				bit_buffer |= ((uint64) *in++) << bits_in_buffer; \
				bits_in_buffer += 8; \
			} \
			_val = bit_buffer & (((uint64) 1 << (nbits)) - 1); \
			bit_buffer >>= (nbits); \
			bits_in_buffer -= (nbits); \
			_val; \
		})

/*
 * Read N (1..64) bits.  GORILLA_GET_BITS refills a 64-bit shift buffer
 * that can already hold up to 7 bits, so a single call is only safe
 * below 58 bits; read wider payloads (XOR windows reach the full 64 bits
 * of a float8) in two halves, low half first to match the encoder.
 */
#define GORILLA_GET_BITS_WIDE(nbits) \
		({ \
			uint64 _wval = GORILLA_GET_BITS((nbits) > 32 ? 32 : (nbits)); \
			if ((nbits) > 32) \
				_wval |= GORILLA_GET_BITS((nbits) - 32) << 32; \
			_wval; \
		})

		in = (uint8 *) ptr;

		if (type_tag == 0)
		{
			/* DoD: delta-of-delta for integers */
			int64		prev_val = 0;
			int64		prev_delta = 0;
			bool		found_first = false;

			/* Read first value raw */
			prev_val = cs_fetch_att((char *) in, true, attlen);
			in += attlen;

			for (uint32 row = 0; row < nrows; row++)
			{
				int64		val;

				if (chunk->cc_has_nulls && CS_ISNULL(raw_data, row))
				{
					cs_store_att_byval(decoded + (Size) row * attlen,
									   (Datum) 0, attlen);
					continue;
				}

				if (!found_first)
				{
					val = prev_val;
					found_first = true;
				}
				else
				{
					uint64		bit;
					int64		dod;

					bit = GORILLA_GET_BITS(1);
					if (bit == 0)
					{
						dod = 0;
					}
					else
					{
						bit = GORILLA_GET_BITS(1);
						if (bit == 0)
						{
							/* '10' + 7 bits */
							dod = (int64) GORILLA_GET_BITS(7) - 63;
						}
						else
						{
							bit = GORILLA_GET_BITS(1);
							if (bit == 0)
							{
								/* '110' + 10 bits */
								dod = (int64) GORILLA_GET_BITS(10) - 511;
							}
							else
							{
								bit = GORILLA_GET_BITS(1);
								if (bit == 0)
								{
									/* '1110' + 13 bits */
									dod = (int64) GORILLA_GET_BITS(13) - 4095;
								}
								else
								{
									/*
									 * '1111' + full absolute value.  The
									 * encoder flushed remaining bits to a
									 * byte boundary, then wrote raw bytes.
									 * Discard remaining bits to realign.
									 */
									if (bits_in_buffer > 0)
									{
										int			skip = bits_in_buffer % 8;

										if (skip > 0)
										{
											bit_buffer >>= skip;
											bits_in_buffer -= skip;
										}
										/* Push back whole unread bytes */
										in -= bits_in_buffer / 8;
										bit_buffer = 0;
										bits_in_buffer = 0;
									}
									val = (int64) cs_fetch_att((char *) in, true, attlen);
									in += attlen;
									cs_store_att_byval(decoded + (Size) row * attlen,
													   val, attlen);
									prev_delta = val - prev_val;
									prev_val = val;
									continue;
								}
							}
						}
					}

					prev_delta += dod;
					val = prev_val + prev_delta;
				}

				cs_store_att_byval(decoded + (Size) row * attlen,
								   val, attlen);
				prev_val = val;
			}
		}
		else
		{
			/* XOR: floating-point */
			uint64		prev_val = 0;
			uint8		prev_leading = 0;
			uint8		prev_trailing = 0;
			bool		found_first = false;
			int			val_bits = attlen * 8;

			/* Read first value raw */
			if (attlen == 4)
			{
				union
				{
					float		f;
					uint32		u;
				}			conv;

				conv.f = DatumGetFloat4(cs_fetch_att((char *) in, true, attlen));
				prev_val = (uint64) conv.u;
			}
			else
			{
				union
				{
					double		d;
					uint64		u;
				}			conv;

				conv.d = DatumGetFloat8(cs_fetch_att((char *) in, true, attlen));
				prev_val = conv.u;
			}
			in += attlen;

			for (uint32 row = 0; row < nrows; row++)
			{
				uint64		val;

				if (chunk->cc_has_nulls && CS_ISNULL(raw_data, row))
				{
					cs_store_att_byval(decoded + (Size) row * attlen,
									   (Datum) 0, attlen);
					continue;
				}

				if (!found_first)
				{
					val = prev_val;
					found_first = true;
				}
				else
				{
					uint64		bit = GORILLA_GET_BITS(1);

					if (bit == 0)
					{
						val = prev_val; /* XOR = 0, same value */
					}
					else
					{
						uint64		xor_val;

						bit = GORILLA_GET_BITS(1);
						if (bit == 0)
						{
							/* '10': use previous window */
							uint8		meaningful = val_bits - prev_leading - prev_trailing;
							uint64		bits = GORILLA_GET_BITS_WIDE(meaningful);

							xor_val = bits << prev_trailing;
						}
						else
						{
							/* '11': new window */
							uint8		leading = (uint8) GORILLA_GET_BITS(6);
							uint8		meaningful = (uint8) GORILLA_GET_BITS(6);
							uint8		trailing;
							uint64		bits;

							/*
							 * A full-width float8 window (meaningful = 64)
							 * does not fit the 6-bit length field; the
							 * encoder stores it as 0.
							 */
							if (meaningful == 0)
								meaningful = val_bits;
							trailing = val_bits - leading - meaningful;
							bits = GORILLA_GET_BITS_WIDE(meaningful);

							xor_val = bits << trailing;
							prev_leading = leading;
							prev_trailing = trailing;
						}

						val = prev_val ^ xor_val;
					}
				}

				/* Store reconstructed float/double */
				if (attlen == 4)
				{
					union
					{
						float		f;
						uint32		u;
					}			conv;

					conv.u = (uint32) val;
					cs_store_att_byval(decoded + (Size) row * attlen,
									   Float4GetDatum(conv.f), attlen);
				}
				else
				{
					union
					{
						double		d;
						uint64		u;
					}			conv;

					conv.u = val;
					cs_store_att_byval(decoded + (Size) row * attlen,
									   Float8GetDatum(conv.d), attlen);
				}

				prev_val = val;
			}
		}

#undef GORILLA_GET_BITS_WIDE
#undef GORILLA_GET_BITS

		if (chunk->cc_has_nulls)
		{
			char	   *bm_copy = palloc(bitmap_size);

			memcpy(bm_copy, raw_data, bitmap_size);
			cache->col_null_bitmap[col] = bm_copy;
		}
		pfree(raw_data);
		cache->col_raw_data[col] = decoded;

		if (is_ni64)
		{
			if (cache->col_ni64_buf[col] == NULL)
				cache->col_ni64_buf[col] = palloc(CS_NI64_BUF_SIZE);
		}

		cache->col_loaded[col] = true;
		return;
	}

	/*
	 * NUMERIC_INT64 without FOR, DELTA, or Gorilla: raw_data is [null_bitmap]
	 * [int64 array].  Store int64 array in col_raw_data for lazy per-row
	 * conversion.
	 */
	if (is_ni64)
	{
		uint32		bitmap_size = chunk->cc_has_nulls ? (nrows + 7) / 8 : 0;
		char	   *int64_start = raw_data + bitmap_size;
		uint32		int64_size = (uint32) sizeof(int64) * nrows;
		char	   *int64_copy = palloc(int64_size);

		memcpy(int64_copy, int64_start, int64_size);

		if (chunk->cc_has_nulls)
		{
			char	   *bm_copy = palloc(bitmap_size);

			memcpy(bm_copy, raw_data, bitmap_size);
			cache->col_null_bitmap[col] = bm_copy;
		}
		pfree(raw_data);

		cache->col_raw_data[col] = int64_copy;
		if (cache->col_ni64_buf[col] == NULL)
			cache->col_ni64_buf[col] = palloc(CS_NI64_BUF_SIZE);
		cache->col_loaded[col] = true;
		return;
	}

	/*
	 * If RLE-encoded, expand run-length data back into per-row arrays.
	 */
	if (is_rle)
	{
		uint32		bitmap_size = chunk->cc_has_nulls ? (nrows + 7) / 8 : 0;
		char	   *ptr = raw_data + bitmap_size;
		uint32		num_runs;
		uint32		out_row;
		bool		varlen_rle;

		memcpy(&num_runs, ptr, sizeof(uint32));
		ptr += sizeof(uint32);

		varlen_rle = !(attr->attbyval && attr->attlen > 0);

		/* Skip has_null flag byte for varlen types */
		if (varlen_rle)
			ptr += sizeof(uint8);

		if (!varlen_rle)
		{
			/* Fixed-length by-value: expand into col_raw_data */
			char	   *decoded = palloc((Size) attr->attlen * nrows);

			out_row = 0;
			for (uint32 r = 0; r < num_runs; r++)
			{
				Datum		val = cs_fetch_att(ptr, true, attr->attlen);
				uint32		count;

				ptr += attr->attlen;
				memcpy(&count, ptr, sizeof(uint32));
				ptr += sizeof(uint32);

				cs_fill_values(decoded, out_row, count,
							   attr->attlen, (uint64) val);
				out_row += count;
			}
			Assert(out_row == nrows);

			if (chunk->cc_has_nulls)
			{
				char	   *bm_copy = palloc(bitmap_size);

				memcpy(bm_copy, raw_data, bitmap_size);
				cache->col_null_bitmap[col] = bm_copy;
			}
			pfree(raw_data);
			cache->col_raw_data[col] = decoded;
		}
		else
		{
			/* Variable-length: expand into col_values/col_nulls arrays */
			char	   *null_bm = chunk->cc_has_nulls ? raw_data : NULL;

			cache->col_values[col] = palloc(sizeof(Datum) * nrows);
			cache->col_nulls[col] = palloc(sizeof(bool) * nrows);

			out_row = 0;
			for (uint32 r = 0; r < num_runs; r++)
			{
				uint32		count;
				bool		run_is_null = false;

				/*
				 * Check if this is a NULL run by looking at the null bitmap
				 * for the first row in the run.
				 */
				if (null_bm && CS_ISNULL(null_bm, out_row))
				{
					run_is_null = true;
					memcpy(&count, ptr, sizeof(uint32));
					ptr += sizeof(uint32);
				}
				else
				{
					int32		len;
					char	   *val_ptr;

					memcpy(&len, ptr, sizeof(int32));
					ptr += sizeof(int32);

					val_ptr = ptr;
					ptr += len;
					memcpy(&count, ptr, sizeof(uint32));
					ptr += sizeof(uint32);

					{
						Datum		dval = cs_byref_value(cache, attr->attlen,
														  val_ptr, len);
						Datum	   *vals = &cache->col_values[col][out_row];

						memset(&cache->col_nulls[col][out_row], 0, count);
						for (uint32 i = 0; i < count; i++)
							vals[i] = dval;
					}
				}

				if (run_is_null)
				{
					memset(&cache->col_nulls[col][out_row], true, count);
					memset(&cache->col_values[col][out_row], 0,
						   count * sizeof(Datum));
				}

				out_row += count;
			}
			Assert(out_row == nrows);

			/* Keep raw buffer alive for zero-copy varlen references */
			cache->col_raw_data[col] = raw_data;
		}

		cache->col_loaded[col] = true;
		return;
	}

	/*
	 * If dictionary-encoded, parse the dict header and index array.
	 */
	if (is_dict)
	{
		char	   *ptr = raw_data;
		uint16		dict_count;
		uint8		index_width;
		uint8		has_null_byte;
		CSDictColumn *dict;

		memcpy(&dict_count, ptr, sizeof(uint16));
		ptr += sizeof(uint16);
		index_width = *ptr++;
		has_null_byte = *ptr++;

		dict = palloc(sizeof(CSDictColumn));
		dict->dict_count = dict_count;
		dict->index_width = index_width;
		dict->has_null = (has_null_byte != 0);

		/* Parse dictionary values */
		dict->dict_values = palloc(sizeof(Datum) * dict_count);
		if (attr->attbyval && attr->attlen > 0)
		{
			for (int i = 0; i < dict_count; i++)
			{
				dict->dict_values[i] = cs_fetch_att(ptr, true, attr->attlen);
				ptr += attr->attlen;
			}
		}
		else
		{
			/* varlen: same {int32 len, data} format */
			for (int i = 0; i < dict_count; i++)
			{
				int32		len;

				memcpy(&len, ptr, sizeof(int32));
				ptr += sizeof(int32);

				dict->dict_values[i] = cs_byref_value(cache, attr->attlen,
													  ptr, len);
				ptr += len;
			}
		}

		dict->index_data = ptr; /* remaining bytes are the index array */

		cache->col_dict[col] = dict;
		cache->col_raw_data[col] = raw_data;	/* keep buffer alive */
		cache->col_loaded[col] = true;
		return;
	}

	/* Non-encoded column: deserialize based on type */
	if (attr->attbyval && attr->attlen > 0)
	{
		/*
		 * Fixed-length by-value: keep raw buffer for direct access.
		 * col_values[col] stays NULL as a sentinel for direct-access mode.
		 */
		if (chunk->cc_has_nulls)
		{
			uint32		bitmap_size = (nrows + 7) / 8;
			char	   *bm_copy = palloc(bitmap_size);
			uint32		vals_size = (uint32) attr->attlen * nrows;
			char	   *vals_copy = palloc(vals_size);

			memcpy(bm_copy, raw_data, bitmap_size);
			memcpy(vals_copy, raw_data + bitmap_size, vals_size);
			pfree(raw_data);

			cache->col_null_bitmap[col] = bm_copy;
			cache->col_raw_data[col] = vals_copy;
		}
		else
		{
			cache->col_raw_data[col] = raw_data;
		}
	}
	else
	{
		/*
		 * Variable-length or by-reference: point Datum values directly into
		 * the decompressed buffer (zero-copy).
		 */
		char	   *ptr = raw_data;
		uint32		row;

		cache->col_values[col] = palloc(sizeof(Datum) * nrows);
		cache->col_nulls[col] = palloc(sizeof(bool) * nrows);

		for (row = 0; row < nrows; row++)
		{
			int32		len;

			memcpy(&len, ptr, sizeof(int32));
			ptr += sizeof(int32);

			if (len == -1)
			{
				cache->col_nulls[col][row] = true;
				cache->col_values[col][row] = (Datum) 0;
			}
			else
			{
				cache->col_nulls[col][row] = false;
				cache->col_values[col][row] =
					cs_byref_value(cache, attr->attlen, ptr, len);
				ptr += len;
			}
		}

		/* Keep raw buffer alive for zero-copy references */
		cache->col_raw_data[col] = raw_data;
	}

	cache->col_loaded[col] = true;
}

/*
 * Base-decompress a column (LZ4/PGLZ) without decoding pre-encoding layers.
 *
 * Populates cache->col_base_data[col] with the decompressed data.
 * Subsequent point reads or full decode can use this buffer.
 */
static void
cs_base_decompress_column_internal(CSColumnCache *cache, Relation rel, int col)
{
	CSRowGroupDesc *rg_desc = cache->cur_rg_desc;
	CSColumnChunkDesc *chunk = &rg_desc->rg_columns[col];
	uint8		base_compression = CS_COMPRESS_BASE(chunk->cc_compression);
	char	   *raw_data;

	if (cache->col_base_data[col])
		return;

	raw_data = cs_read_column_pages(rel, chunk->cc_start_block,
									chunk->cc_npages,
									chunk->cc_compressed_size);

	if (base_compression != CS_COMPRESS_NONE)
	{
		char	   *decompressed = cs_decompress_base(base_compression,
													  raw_data,
													  chunk->cc_compressed_size,
													  chunk->cc_uncompressed_size);

		pfree(raw_data);
		raw_data = decompressed;
	}

	cache->col_base_data[col] = raw_data;
}

/*
 * Point-read a single value from a FOR or NI64 encoded column.
 *
 * Extracts just the value at the given row offset from the base-decompressed
 * data, avoiding the full FOR decode loop (which expands all 100K values).
 * col_base_data[col] must be populated via cs_base_decompress_column().
 */
Datum
cs_column_point_read(CSColumnCache *cache, Relation rel, int col,
					 uint32 row, bool *isnull)
{
	TupleDesc	tupdesc = cache->cc_tupdesc ? cache->cc_tupdesc :
		RelationGetDescr(rel);
	Form_pg_attribute attr = TupleDescAttr(tupdesc, col);
	CSRowGroupDesc *rg_desc = cache->cur_rg_desc;
	CSColumnChunkDesc *chunk = &rg_desc->rg_columns[col];
	uint32		nrows = rg_desc->rg_num_rows;
	uint8		preenc = CS_PREENC(chunk->cc_compression);
	bool		is_ni64 = (preenc == CS_PREENC_NI64 ||
						   preenc == CS_PREENC_NI64_FOR);
	bool		is_for = (preenc == CS_PREENC_FOR ||
						  preenc == CS_PREENC_NI64_FOR);
	char	   *ptr = cache->col_base_data[col];
	uint16		ni64_dscale = 0;
	uint32		bitmap_size;
	char	   *bitmap;
	int			attlen;

	Assert(is_for || is_ni64);

	/* Strip NI64 dscale header if present */
	if (is_ni64)
	{
		memcpy(&ni64_dscale, ptr, sizeof(uint16));
		ptr += sizeof(uint16);
		attlen = (int) sizeof(int64);
	}
	else
		attlen = attr->attlen;

	/* Locate and check null bitmap */
	bitmap_size = chunk->cc_has_nulls ? (nrows + 7) / 8 : 0;
	bitmap = chunk->cc_has_nulls ? ptr : NULL;
	ptr += bitmap_size;

	if (bitmap)
	{
		if (CS_ISNULL(bitmap, row))
		{
			*isnull = true;
			return (Datum) 0;
		}
	}

	*isnull = false;

	if (is_for)
	{
		uint64		min_val;
		uint8		bits_per_value;
		uint64		value;

		/* Read FOR header */
		min_val = cs_fetch_att(ptr, true, attlen);
		ptr += attlen;
		bits_per_value = (uint8) *ptr++;

		if (bits_per_value == 0)
		{
			value = min_val;
		}
		else
		{
			/* Extract single delta at bit position row * bits_per_value */
			uint64		bit_offset = (uint64) row * bits_per_value;
			uint32		byte_pos = (uint32) (bit_offset / 8);
			int			bit_pos = (int) (bit_offset % 8);
			uint8	   *in = (uint8 *) ptr + byte_pos;
			uint64		bit_buffer = 0;
			int			bytes_needed = (bit_pos + bits_per_value + 7) / 8;
			uint64		mask = ((uint64) 1 << bits_per_value) - 1;

			for (int i = 0; i < bytes_needed && i < 9; i++)
				bit_buffer |= ((uint64) in[i]) << (i * 8);

			value = min_val + ((bit_buffer >> bit_pos) & mask);
		}

		if (is_ni64)
		{
			if (cache->col_ni64_buf[col] == NULL)
				cache->col_ni64_buf[col] = cs_cache_alloc(cache, CS_NI64_BUF_SIZE);
			return cs_int64_to_numeric_buf((int64) value,
										   (int) ni64_dscale,
										   cache->col_ni64_buf[col]);
		}

		/* Return FOR-decoded value as Datum (by-value, truncated to width) */
		switch (attlen)
		{
			case 1:
				return (Datum) ((uint8) value);
			case 2:
				return (Datum) ((uint16) value);
			case 4:
				return (Datum) ((uint32) value);
			case 8:
				return (Datum) value;
			default:
				elog(ERROR, "unexpected attlen %d for FOR column", attlen);
				return (Datum) 0;
		}
	}

	/* NI64 without FOR: read int64 directly from the flat array */
	Assert(is_ni64);
	{
		int64		ival = cs_read_i64((const char *) ptr +
									   (Size) row * sizeof(int64));

		if (cache->col_ni64_buf[col] == NULL)
			cache->col_ni64_buf[col] = cs_cache_alloc(cache, CS_NI64_BUF_SIZE);
		return cs_int64_to_numeric_buf(ival, (int) ni64_dscale,
									   cache->col_ni64_buf[col]);
	}
}

/*
 * Load a row group for sequential/bitmap scan.
 *
 * Loads the row group catalog and deletion bitmap.  Column data is NOT
 * eagerly decompressed; the custom slot's getsomeattrs callback handles
 * lazy column loading on demand.
 */
bool
cs_load_rowgroup(CScanDesc scan, Relation rel, uint32 rg_id)
{
	int			natts = RelationGetDescr(rel)->natts;
	CSColumnCache *cache = &scan->colcache;

	/* Look up the catalog page block number for this row group */
	if (rg_id >= scan->nrowgroups ||
		scan->rg_catalog_blocks == NULL ||
		scan->rg_catalog_blocks[rg_id] == InvalidBlockNumber)
		return false;

	if (scan->rg_desc_preloaded)
	{
		CSRowGroupDesc *rg_desc;

		/*
		 * Catalog was already read for zone map checks.  Skip re-read but
		 * still allocate/reset column buffers and load delbitmap.
		 */
		scan->rg_desc_preloaded = false;

		rg_desc = cache->cur_rg_desc;
		if (rg_desc->rg_num_rows == 0)
			return false;

		cs_column_cache_reset_columns(cache, natts);
		cs_cache_load_delbitmap(cache, rel, rg_desc);

		cache->col_nrows = rg_desc->rg_num_rows;
		scan->sel_bitmap_valid = false;
		return true;
	}

	if (cs_load_rowgroup_into(cache, rel,
							  scan->rg_catalog_blocks[rg_id], natts))
	{
		scan->sel_bitmap_valid = false;
		return true;
	}
	return false;
}

/*
 * Load a row group for COUNT(*) fast path -- read only the catalog entry
 * and deletion bitmap, not the column data.  Per-row iteration checks the
 * deletion bitmap and tombstones to produce valid virtual TIDs.
 */
static bool
cs_load_rowgroup_countonly(CScanDesc scan, Relation rel, uint32 rg_id)
{
	TupleDesc	tupdesc = RelationGetDescr(rel);
	int			natts = tupdesc->natts;
	CSColumnCache *cache = &scan->colcache;
	CSRowGroupDesc *rg_desc;
	Size		rg_size = CSRowGroupDescSize(natts);

	if (rg_id >= scan->nrowgroups ||
		scan->rg_catalog_blocks == NULL ||
		scan->rg_catalog_blocks[rg_id] == InvalidBlockNumber)
		return false;

	if (!cache->cur_rg_desc)
		cache->cur_rg_desc = cs_cache_alloc(cache, rg_size);

	rg_desc = cache->cur_rg_desc;
	cs_read_rowgroup_catalog(rel, scan->rg_catalog_blocks[rg_id],
							 rg_desc, natts);

	if (rg_desc->rg_num_rows == 0)
		return false;

	/* Load deletion bitmap for per-row iteration */
	cs_cache_load_delbitmap(cache, rel, rg_desc);

	cache->col_nrows = rg_desc->rg_num_rows;

	return true;
}

/*
 * Fast-path columnar scan for COUNT(*) -- no columns needed.
 *
 * When no scan keys are present, uses a simple counter per row group
 * (rg_num_rows - rg_num_deleted) without reading the deletion bitmap
 * or iterating per-row.  With scan keys, falls back to per-row iteration
 * checking the deletion bitmap.
 */
static bool
cs_columnar_getnext_countonly(CScanDesc scan, TupleTableSlot *slot)
{
	Relation	rel = scan->rs_base.rs_rd;
	CSColumnCache *cache = &scan->colcache;
	bool		loaded;

	while (!scan->columnar_done)
	{
		/*
		 * Iterate per-row, checking the deletion bitmap and tombstones. No
		 * column data is decompressed (the slot stays all-null), but we
		 * produce valid virtual TIDs so that DML callers work correctly.
		 */
		if (cache->col_nrows > 0 &&
			scan->cur_row_in_group < cache->col_nrows)
		{
			uint32		row = scan->cur_row_in_group;
			uint32		rg_id = scan->cur_rowgroup - 1;
			BlockNumber vblk;
			OffsetNumber voff;

			scan->cur_row_in_group++;

			/* Skip deleted rows */
			if (cache->cur_delbitmap != NULL)
			{
				if (CS_ISDELETED(cache->cur_delbitmap, row))
					continue;
			}

			/* Skip rows with visible tombstones (pending MVCC deletes) */
			if (scan->tombstone_count > 0)
			{
				ItemPointerData vtid;

				cs_encode_virtual_tid(&vtid, rg_id, row);
				if (cs_tombstone_lookup(scan->tombstone_tids,
										scan->tombstone_count, &vtid))
					continue;
			}

			if (TTS_EMPTY(slot))
			{
				int			natts = slot->tts_tupleDescriptor->natts;

				memset(slot->tts_isnull, true, natts * sizeof(bool));
				memset(slot->tts_values, 0, natts * sizeof(Datum));
			}
			slot->tts_flags &= ~TTS_FLAG_EMPTY;
			slot->tts_nvalid = slot->tts_tupleDescriptor->natts;

			/* Set a valid virtual TID */
			vblk = CS_COLUMNAR_BLKNO_BASE +
				rg_id * CS_VIRTUAL_BLOCKS_PER_RG +
				row / CS_ROWS_PER_VIRTUAL_BLOCK;
			voff = (row % CS_ROWS_PER_VIRTUAL_BLOCK) + 1;
			ItemPointerSet(&slot->tts_tid, vblk, voff);

			slot->tts_tableOid = RelationGetRelid(rel);
			return true;
		}

		/* Need to load next row group */
		loaded = false;

		while (scan->cur_rowgroup < scan->nrowgroups)
		{
			if (cs_load_rowgroup_countonly(scan, rel, scan->cur_rowgroup))
			{
				scan->cur_row_in_group = 0;
				scan->cur_rowgroup++;
				loaded = true;
				break;
			}
			scan->cur_rowgroup++;
		}

		if (!loaded)
		{
			scan->columnar_done = true;
			break;
		}
	}

	return false;
}

/*
 * Get the next tuple from a columnar row group.
 */
static bool
cs_columnar_getnext(CScanDesc scan, TupleTableSlot *slot)
{
	Relation	rel = scan->rs_base.rs_rd;
	int			natts = RelationGetDescr(rel)->natts;
	CSColumnCache *cache = &scan->colcache;

	while (!scan->columnar_done)
	{
		/* Need to load a new row group? */
		if (cache->col_nrows == 0 || scan->cur_row_in_group >= cache->col_nrows)
		{
			bool		loaded = false;

			while (scan->cur_rowgroup < scan->nrowgroups)
			{
				/*
				 * Read the catalog entry to check zone maps before loading
				 * the full column data.
				 */
				if (scan->rg_catalog_blocks != NULL &&
					scan->rg_catalog_blocks[scan->cur_rowgroup] != InvalidBlockNumber &&
					scan->cs_nkeys > 0)
				{
					CSRowGroupDesc *rg_desc;
					Size		rg_size = CSRowGroupDescSize(natts);

					if (!cache->cur_rg_desc)
						cache->cur_rg_desc = cs_cache_alloc(cache, rg_size);
					rg_desc = cache->cur_rg_desc;

					cs_read_rowgroup_catalog(rel,
											 scan->rg_catalog_blocks[scan->cur_rowgroup],
											 rg_desc, natts);

					scan->instr_rg_examined++;
					if (cs_zonemap_skip_rowgroup(scan, rg_desc))
					{
						scan->instr_rg_zonemap_skipped++;
						scan->cur_rowgroup++;
						continue;
					}

					/*
					 * Catalog already loaded; skip re-read in
					 * cs_load_rowgroup
					 */
					scan->rg_desc_preloaded = true;
				}

				if (cs_load_rowgroup(scan, rel, scan->cur_rowgroup))
				{
					scan->cur_row_in_group = 0;
					scan->cur_rowgroup++;
					loaded = true;
					break;
				}
				scan->cur_rowgroup++;
			}

			if (!loaded)
			{
				scan->columnar_done = true;
				break;
			}

			/*
			 * When column projection is active, pre-fill all slot values to
			 * NULL at the start of each row group.  Since ExecClearTuple does
			 * not zero tts_values/tts_isnull, the NULLs for unneeded columns
			 * persist across rows.  The getsomeattrs callback then only
			 * writes needed columns.
			 */
			if (scan->needed_col_list != NULL)
			{
				int			total = slot->tts_tupleDescriptor->natts;

				memset(slot->tts_isnull, true, total * sizeof(bool));
				memset(slot->tts_values, 0, total * sizeof(Datum));
			}

			/*
			 * Late materialization: pre-load only the columns referenced by
			 * scan keys so that row-level filtering can evaluate them without
			 * decompressing the remaining columns.
			 */
			if (scan->cs_nkeys > 0 || scan->bf_nfilters > 0)
			{
				cs_preload_qual_columns(scan, cache, rel);
				cs_build_selection_bitmap(scan, cache, rel);
			}
		}

		/* Return the current row from the loaded row group */
		{
			CSTupleTableSlot *csslot = (CSTupleTableSlot *) slot;
			uint32		row = scan->cur_row_in_group;
			uint32		rg_id = scan->cur_rowgroup - 1;
			BlockNumber vblk;
			OffsetNumber voff;

			scan->cur_row_in_group++;

			if (scan->sel_bitmap_valid)
			{
				/*
				 * Selection bitmap incorporates deletion bitmap and all
				 * pushed-down predicates.  Only tombstones need per-row
				 * checking (they are rare MVCC pending deletes).
				 */
				if (!CS_ISSELECTED(scan->sel_bitmap, row))
					continue;

				if (scan->tombstone_count > 0)
				{
					ItemPointerData vtid;

					cs_encode_virtual_tid(&vtid, rg_id, row);
					if (cs_tombstone_lookup(scan->tombstone_tids,
											scan->tombstone_count, &vtid))
						continue;
				}
			}
			else
			{
				/* No selection bitmap: check deletion bitmap and quals */
				if (cache->cur_delbitmap != NULL)
				{
					if (CS_ISDELETED(cache->cur_delbitmap, row))
						continue;
				}

				if (scan->tombstone_count > 0)
				{
					ItemPointerData vtid;

					cs_encode_virtual_tid(&vtid, rg_id, row);
					if (cs_tombstone_lookup(scan->tombstone_tids,
											scan->tombstone_count, &vtid))
						continue;
				}

				if ((scan->cs_nkeys > 0 || scan->bf_nfilters > 0) &&
					!cs_row_passes_scan_keys(scan, cache, row))
					continue;
			}

			ExecClearTuple(slot);

			/*
			 * Set up lazy loading: store the scan back-pointer and row index.
			 * The custom slot's getsomeattrs callback will decompress columns
			 * on demand when the executor accesses them.
			 */
			csslot->cs_scan = scan;
			csslot->cs_colcache = &scan->colcache;
			csslot->cs_row = row;
			csslot->cs_is_columnar = true;

			/* Mark slot as non-empty with 0 valid attrs (lazy) */
			slot->tts_flags &= ~TTS_FLAG_EMPTY;
			slot->tts_nvalid = 0;

			/* Set a virtual TID for this columnar row */
			vblk = CS_COLUMNAR_BLKNO_BASE +
				rg_id * CS_VIRTUAL_BLOCKS_PER_RG +
				row / CS_ROWS_PER_VIRTUAL_BLOCK;
			voff = (row % CS_ROWS_PER_VIRTUAL_BLOCK) + 1;
			ItemPointerSet(&slot->tts_tid, vblk, voff);

			slot->tts_tableOid = RelationGetRelid(rel);
			return true;
		}
	}

	return false;
}

/*
 * Parallel scan: get the next tuple by claiming work units atomically.
 *
 * Work unit 0 = delta store, units 1..N = row groups 0..N-1.
 * When a worker exhausts its current unit, it atomically claims the next.
 */
static bool
cs_parallel_getnextslot(TableScanDesc sscan, ScanDirection direction,
						TupleTableSlot *slot)
{
	CScanDesc	scan = (CScanDesc) sscan;
	CSParallelScanDesc cpscan = (CSParallelScanDesc) sscan->rs_parallel;
	uint64		total_units = 1 + (uint64) cpscan->pcs_nrowgroups;
	uint64		unit;
	uint32		rg;

	for (;;)
	{
		CHECK_FOR_INTERRUPTS();

		/* Try to return a tuple from the current work unit */
		if (!scan->delta_done)
		{
			if (cs_delta_getnext(scan, direction, slot))
				return true;
			scan->delta_done = true;
		}

		if (!scan->columnar_done)
		{
			bool		got;

			/* Collect tombstones lazily before the first columnar row access */
			if (!scan->tombstones_collected)
				cs_collect_tombstones(scan);

			if (scan->count_optimized)
				got = cs_columnar_getnext_countonly(scan, slot);
			else
				got = cs_columnar_getnext(scan, slot);

			if (got)
				return true;
			scan->columnar_done = true;
		}

		/* Current work unit exhausted -- claim the next one */
		unit = pg_atomic_fetch_add_u64(&cpscan->pcs_nallocated, 1);

		if (unit >= total_units)
			return false;		/* no more work */

		if (unit == 0)
		{
			/* Work unit 0: delta store */
			scan->delta_done = false;
			scan->delta_cur_block = scan->delta_start;
			scan->delta_cur_offset = InvalidOffsetNumber;
			scan->delta_nvisible = 0;
			scan->delta_visible_idx = 0;
		}
		else
		{
			/* Work unit N: row group N-1 */
			rg = (uint32) (unit - 1);

			scan->columnar_done = false;
			scan->cur_rowgroup = rg;
			scan->nrowgroups = rg + 1;	/* bracket exactly one RG */
			scan->cur_row_in_group = 0;
			scan->colcache.col_nrows = 0;
			scan->rg_desc_preloaded = false;
		}
	}
}

/*
 * Get the next tuple from the scan (delta first, then columnar).
 */
bool
cs_scan_getnextslot(TableScanDesc sscan, ScanDirection direction,
					TupleTableSlot *slot)
{
	CScanDesc	scan = (CScanDesc) sscan;

	CHECK_FOR_INTERRUPTS();

	/* Parallel scan: dispatch to the parallel work-unit driver */
	if (sscan->rs_parallel != NULL)
		return cs_parallel_getnextslot(sscan, direction, slot);

	/*
	 * Both cs_delta_getnext and cs_columnar_getnext clear the slot before
	 * populating it, so don't waste a virtual call clearing it here.
	 */

	/* First: scan delta store */
	if (!scan->delta_done)
	{
		if (cs_delta_getnext(scan, direction, slot))
			return true;
	}

	/* Collect tombstones lazily before the first columnar row access */
	if (!scan->tombstones_collected)
		cs_collect_tombstones(scan);

	/* Second: scan columnar row groups */
	if (!scan->columnar_done)
	{
		if (scan->count_optimized)
		{
			if (cs_columnar_getnext_countonly(scan, slot))
				return true;
		}
		else
		{
			if (cs_columnar_getnext(scan, slot))
				return true;
		}
	}

	return false;
}

/*
 * cs_scan_set_tidrange -- restrict a scan to a TID range
 *
 * Sets the min/max TID bounds for a subsequent TID range scan.
 */
void
cs_scan_set_tidrange(TableScanDesc sscan, ItemPointer mintid,
					 ItemPointer maxtid)
{
	ItemPointerCopy(mintid, &sscan->st.tidrange.rs_mintid);
	ItemPointerCopy(maxtid, &sscan->st.tidrange.rs_maxtid);
}

/*
 * cs_scan_getnextslot_tidrange -- get next tuple within a TID range
 *
 * Wraps the normal sequential scan, filtering tuples to only return those
 * whose TID falls within [rs_mintid, rs_maxtid].
 */
bool
cs_scan_getnextslot_tidrange(TableScanDesc sscan, ScanDirection direction,
							 TupleTableSlot *slot)
{
	ItemPointer mintid = &sscan->st.tidrange.rs_mintid;
	ItemPointer maxtid = &sscan->st.tidrange.rs_maxtid;

	for (;;)
	{
		if (!cs_scan_getnextslot(sscan, direction, slot))
			return false;

		/*
		 * Filter out tuples outside the TID range, with early termination
		 * when the scan has passed beyond the range boundary.
		 */
		if (ItemPointerCompare(&slot->tts_tid, mintid) < 0)
		{
			ExecClearTuple(slot);
			if (ScanDirectionIsBackward(direction))
				return false;
			continue;
		}

		if (ItemPointerCompare(&slot->tts_tid, maxtid) > 0)
		{
			ExecClearTuple(slot);
			if (ScanDirectionIsForward(direction))
				return false;
			continue;
		}

		/* TID is within range */
		return true;
	}
}

/*
 * Report the physical storage encoding for a column.
 * For NUMERIC_INT64 columns, reports INT8OID and the decimal scale.
 *
 * If the column's data has been decompressed, the dscale is available
 * in the cache.  Otherwise, we check the row group catalog for the
 * NI64 compression flag and read the dscale header from the column
 * data pages.
 */
bool
cs_scan_column_encoding(TableScanDesc sscan, AttrNumber attno,
						Oid *phys_type, int32 *dscale)
{
	CScanDesc	scan = (CScanDesc) sscan;
	Relation	rel = sscan->rs_rd;
	TupleDesc	tupdesc = RelationGetDescr(rel);
	int			col = attno - 1;
	CSColumnCache *cache = &scan->colcache;

	if (col < 0 || col >= tupdesc->natts)
		return false;

	/* Only report for NUMERIC columns */
	if (TupleDescAttr(tupdesc, col)->atttypid != NUMERICOID)
		return false;

	/* Fast path: column already decompressed, dscale is known */
	if (cache->col_ni64_buf != NULL && cache->col_ni64_buf[col] != NULL)
	{
		*phys_type = INT8OID;
		*dscale = (int32) cache->col_ni64_dscale[col];
		return true;
	}

	/*
	 * Column not yet decompressed.  Check the current row group's catalog
	 * entry for NI64 encoding, and if found, load the column to get the
	 * dscale.  We must load rather than reading the raw page because the
	 * column data may have base compression (LZ4/PGLZ) on top.
	 */
	if (cache->cur_rg_desc != NULL)
	{
		CSColumnChunkDesc *chunk = &cache->cur_rg_desc->rg_columns[col];

		if (CS_PREENC(chunk->cc_compression) == CS_PREENC_NI64 ||
			CS_PREENC(chunk->cc_compression) == CS_PREENC_NI64_FOR ||
			CS_PREENC(chunk->cc_compression) == CS_PREENC_NI64_DELTA ||
			CS_PREENC(chunk->cc_compression) == CS_PREENC_NI64_DELTA_FOR)
		{
			/* Load the column, which decompresses and sets col_ni64_dscale */
			if (!cache->col_loaded[col])
				cs_ensure_column_loaded(cache, rel, col);

			if (cache->col_ni64_buf != NULL &&
				cache->col_ni64_buf[col] != NULL)
			{
				*phys_type = INT8OID;
				*dscale = (int32) cache->col_ni64_dscale[col];
				return true;
			}
		}
	}

	return false;
}

/*
 * Extract the raw physical value for a column from the current scan tuple,
 * bypassing NI64-to-Numeric conversion.  For NI64 columns, returns the raw
 * int64 value.  The slot must be a CSTupleTableSlot positioned on a columnar
 * row.
 */
bool
cs_scan_get_raw_attr(TableScanDesc sscan, TupleTableSlot *slot,
					 AttrNumber attno, Datum *value, bool *isnull)
{
	CSTupleTableSlot *csslot = (CSTupleTableSlot *) slot;
	CSColumnCache *cache;
	int			col = attno - 1;
	uint32		row;

	/* Only works for columnar rows */
	if (!csslot->cs_is_columnar || csslot->cs_colcache == NULL)
		return false;

	cache = csslot->cs_colcache;

	/* Ensure the column data is loaded before checking encoding */
	if (!cache->col_loaded[col])
		cs_ensure_column_loaded(cache, sscan->rs_rd, col);

	/* Only for NI64-encoded columns (dscale 0 is valid: whole numbers) */
	if (cache->col_ni64_buf == NULL || cache->col_ni64_buf[col] == NULL)
		return false;

	row = csslot->cs_row;

	/* Check null bitmap */
	if (cache->col_null_bitmap[col] != NULL)
	{
		if (CS_ISNULL(cache->col_null_bitmap[col], row))
		{
			*isnull = true;
			*value = (Datum) 0;
			return true;
		}
	}

	/* Return the raw int64 value */
	*isnull = false;
	*value = Int64GetDatum(((int64 *) cache->col_raw_data[col])[row]);
	return true;
}

/*
 * Register a bloom filter pushed down to the scan for hash join probe
 * filtering, or clear all filters.
 *
 * Each call registers one filter for one column, together with the hash
 * function (and collation) used to key it -- the hashjoin builds the filter on
 * the 32-bit join hash value, so the scan must probe it the same way (see
 * bloom_lacks_value).  Up to 8 filters can be active simultaneously.  Passing
 * filter == NULL clears all filters.
 *
 * This is no longer a table-AM callback: it is driven by
 * cs_apply_pushed_bloom_filters(), which translates the filters the planner
 * pushed onto the CustomScan into per-column entries here.
 */
void
cs_scan_set_bloom_filter(TableScanDesc sscan, AttrNumber attno,
						 bloom_filter *filter, Oid hashfn, Oid hashcoll)
{
	CScanDesc	scan = (CScanDesc) sscan;

	/* NULL filter means clear all filters */
	if (filter == NULL)
	{
		for (int i = 0; i < scan->bf_nfilters; i++)
		{
			if (scan->bf_dk_match && scan->bf_dk_match[i])
			{
				pfree(scan->bf_dk_match[i]);
				scan->bf_dk_match[i] = NULL;
			}
		}
		if (scan->bf_filters)
			pfree(scan->bf_filters);
		if (scan->bf_attnos)
			pfree(scan->bf_attnos);
		if (scan->bf_dk_match)
			pfree(scan->bf_dk_match);
		if (scan->bf_dk_match_rg)
			pfree(scan->bf_dk_match_rg);
		if (scan->bf_hashfn)
			pfree(scan->bf_hashfn);
		if (scan->bf_hashcoll)
			pfree(scan->bf_hashcoll);
		scan->bf_filters = NULL;
		scan->bf_attnos = NULL;
		scan->bf_dk_match = NULL;
		scan->bf_dk_match_rg = NULL;
		scan->bf_hashfn = NULL;
		scan->bf_hashcoll = NULL;
		scan->bf_nfilters = 0;
		/* Restart the adaptive-disable decision for any re-pushed filters. */
		scan->bf_disabled = false;
		scan->bf_rows_examined = 0;
		return;
	}

	/* Allocate arrays on first push */
	if (scan->bf_nfilters == 0)
	{
		scan->bf_filters = palloc(sizeof(bloom_filter *) * 8);
		scan->bf_attnos = palloc(sizeof(AttrNumber) * 8);
		scan->bf_dk_match = palloc0(sizeof(bool *) * 8);
		scan->bf_dk_match_rg = palloc(sizeof(uint32) * 8);
		scan->bf_hashfn = palloc0(sizeof(FmgrInfo) * 8);
		scan->bf_hashcoll = palloc(sizeof(Oid) * 8);
		for (int i = 0; i < 8; i++)
			scan->bf_dk_match_rg[i] = UINT32_MAX;
	}

	/* Append the new filter (limit to 8) */
	if (scan->bf_nfilters < 8)
	{
		int			idx = scan->bf_nfilters;

		scan->bf_filters[idx] = filter;
		scan->bf_attnos[idx] = attno;
		fmgr_info(hashfn, &scan->bf_hashfn[idx]);
		scan->bf_hashcoll[idx] = hashcoll;
		scan->bf_nfilters++;
	}
}
