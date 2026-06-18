/*-------------------------------------------------------------------------
 *
 * cs_internal.h
 *	  Private definitions for the columnstore table access method.
 *
 * Everything the AM's .c files need to talk to each other lives here:
 * on-disk layout, in-memory scan state, and cross-file prototypes.
 * Not installed; not intended for use outside contrib/columnstore/.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * contrib/columnstore/cs_internal.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CS_INTERNAL_H
#define CS_INTERNAL_H

#include "access/generic_xlog.h"
#include "access/reloptions.h"
#include "access/tableam.h"
#include "fmgr.h"
#include "port/atomics.h"
#include "storage/bufpage.h"
#include "utils/rel.h"

/*
 * Alignment-safe equivalents of fetch_att()/store_att_byval() for the
 * byte-packed columnar format.
 *
 * Column values in that format are not stored at type-aligned offsets -- a
 * 4-byte value may immediately follow a 1-byte type tag, for example -- so
 * the typed pointer dereferences in the core macros (e.g. *(int32 *) p) are
 * undefined behavior here.  Mainstream hardware (x86 and ARM alike) tolerates
 * the unaligned access, so this is latent in ordinary builds, but it is a
 * hard trap under -fsanitize=alignment.  These helpers go through memcpy,
 * which is well-defined for any alignment and lowers to the same instruction
 * the macros would emit on unaligned-tolerant targets.  Byte layout and sign
 * handling are otherwise identical to the core macros, so the on-disk format
 * is unchanged.
 */
static inline Datum
cs_fetch_att(const void *T, bool attbyval, int attlen)
{
	int16		v16;
	int32		v32;
	int64		v64;

	if (!attbyval)
		return PointerGetDatum(T);

	switch (attlen)
	{
		case sizeof(char):
			return CharGetDatum(*((const char *) T));
		case sizeof(int16):
			memcpy(&v16, T, sizeof(v16));
			return Int16GetDatum(v16);
		case sizeof(int32):
			memcpy(&v32, T, sizeof(v32));
			return Int32GetDatum(v32);
		case sizeof(int64):
			memcpy(&v64, T, sizeof(v64));
			return Int64GetDatum(v64);
		default:
			elog(ERROR, "unsupported byval length: %d", attlen);
			return 0;
	}
}

static inline void
cs_store_att_byval(void *T, Datum newdatum, int attlen)
{
	int16		v16;
	int32		v32;
	int64		v64;

	switch (attlen)
	{
		case sizeof(char):
			*((char *) T) = DatumGetChar(newdatum);
			break;
		case sizeof(int16):
			v16 = DatumGetInt16(newdatum);
			memcpy(T, &v16, sizeof(v16));
			break;
		case sizeof(int32):
			v32 = DatumGetInt32(newdatum);
			memcpy(T, &v32, sizeof(v32));
			break;
		case sizeof(int64):
			v64 = DatumGetInt64(newdatum);
			memcpy(T, &v64, sizeof(v64));
			break;
		default:
			elog(ERROR, "unsupported byval length: %d", attlen);
	}
}

/*
 * Alignment-safe scalar accessors for values stored at non-type-aligned
 * offsets in the byte-packed columnar buffers -- a uint16/uint32 dictionary
 * index array, or an int64 value array that follows a 1- or 2-byte header,
 * for example.  As with cs_fetch_att()/cs_store_att_byval() above, the memcpy
 * is well-defined for any alignment and lowers to a single load/store on
 * unaligned-tolerant targets.
 */
static inline uint16
cs_read_u16(const void *p)
{
	uint16		v;

	memcpy(&v, p, sizeof(v));
	return v;
}

static inline uint32
cs_read_u32(const void *p)
{
	uint32		v;

	memcpy(&v, p, sizeof(v));
	return v;
}

static inline int64
cs_read_i64(const void *p)
{
	int64		v;

	memcpy(&v, p, sizeof(v));
	return v;
}

static inline void
cs_write_u16(void *p, uint16 v)
{
	memcpy(p, &v, sizeof(v));
}

/*
 * The SUM/AVG aggregate accumulators use native 128-bit integers, so the
 * extension is only supported where the platform provides one.  The build
 * system already skips columnstore where PG_INT128_TYPE is unset (32-bit
 * builds, and MSVC, which has no __int128); this is a backstop for a forced
 * build (e.g. PGXS) on such a platform, turning the otherwise cryptic
 * "unknown type name 'int128'" errors into a clear message.
 */
#ifndef HAVE_INT128
#error "columnstore requires native 128-bit integer support (a 64-bit, non-MSVC build); it is unsupported on this platform"
#endif

/*
 * Per-relation options for columnstore tables.
 *
 * Layout begins with StdRdOptions so the standard accessor macros
 * (RelationGetFillFactor, RelationGetParallelWorkers, the autovacuum
 * helpers, etc.) read the right fields when they cast rd_options to
 * StdRdOptions *.  The trailing sort_key_offset is the columnstore's
 * private extension.  cs_reloptions() builds the bytea via
 * build_reloptions; the resulting struct is stored verbatim in
 * Relation->rd_options.
 */
typedef struct CSRdOptions
{
	StdRdOptions std;			/* must be first; mirrors heap layout */
	int			sort_key_offset;	/* offset of sort_key string, or 0 if
									 * unset */
} CSRdOptions;

#define CSRelationGetSortKey(relation) \
	((relation)->rd_options ? \
	 GET_STRING_RELOPTION((CSRdOptions *) (relation)->rd_options, sort_key_offset) : \
	 NULL)

extern void cs_register_reloptions(void);
extern bytea *cs_reloptions(Datum reloptions, bool validate);

/* Magic number for metapage validation */
#define CS_MAGIC			0x434F4C53	/* "COLS" */
#define CS_VERSION			1

/*
 * Row group sizing.  The delta store is frozen into columnar row groups
 * when it reaches this many rows (approximately).
 */
#define CS_ROWS_PER_ROWGROUP	100000

/*
 * Virtual TID encoding for columnar rows.
 *
 * Delta store rows use real page TIDs.  Columnar rows use virtual TIDs
 * so that we can decode them back to (row_group_id, row_offset).
 *
 * We reserve block numbers >= CS_COLUMNAR_BLKNO_BASE for virtual TIDs;
 * cs_delta_page_insert() errors out before a real delta page could ever
 * reach that block number.
 * Within that range:
 *   block = CS_COLUMNAR_BLKNO_BASE + rg_id * CS_VIRTUAL_BLOCKS_PER_RG
 *           + row_offset / CS_ROWS_PER_VIRTUAL_BLOCK
 *   offset = (row_offset % CS_ROWS_PER_VIRTUAL_BLOCK) + 1
 */
#define CS_COLUMNAR_BLKNO_BASE		((BlockNumber) 0x40000000)
#define CS_ROWS_PER_VIRTUAL_BLOCK	MaxHeapTuplesPerPage
#define CS_VIRTUAL_BLOCKS_PER_RG	\
	((CS_ROWS_PER_ROWGROUP + CS_ROWS_PER_VIRTUAL_BLOCK - 1) / CS_ROWS_PER_VIRTUAL_BLOCK)

/* Maximum column data bytes per page, after page header */
#define CS_COLDATA_PER_PAGE  (BLCKSZ - MAXALIGN(SizeOfPageHeaderData))

/* Number of BlockNumber entries that fit in one directory page */
#define CS_RGDIR_ENTRIES_PER_PAGE  \
	((int) (CS_COLDATA_PER_PAGE / sizeof(BlockNumber)))

/* Max columns we support per row group catalog entry */
#define CS_MAX_COLUMNS		MaxTupleAttributeNumber

/* Deletion bitmap sizing */
#define CS_DELBITMAP_BYTES(nrows) \
	(((nrows) + 7) / 8)
#define CS_DELBITMAP_NPAGES(nrows) \
	((CS_DELBITMAP_BYTES(nrows) + CS_COLDATA_PER_PAGE - 1) / CS_COLDATA_PER_PAGE)

/*
 * Null bitmap test: returns true if the given row is NULL.
 * Convention: bit 0 = NULL, bit 1 = present (same as heap att_isnull).
 */
#define CS_ISNULL(bitmap, row) \
	(!((bitmap)[(row) >> 3] & (1 << ((row) & 0x07))))

/*
 * Deletion bitmap test: returns true if the given row is deleted.
 * Convention: bit 1 = deleted, bit 0 = alive (opposite of null bitmap).
 */
#define CS_ISDELETED(bitmap, row) \
	(((bitmap)[(row) >> 3] & (1 << ((row) & 0x07))) != 0)

/*
 * Selection bitmap: bit 1 = selected (passes all quals), bit 0 = filtered.
 * Used by cs_build_selection_bitmap() to batch-evaluate predicates.
 */
#define CS_ISSELECTED(bitmap, row) \
	(((bitmap)[(row) >> 3] & (1 << ((row) & 0x07))) != 0)
#define CS_DESELECT_ROW(bitmap, row) \
	((bitmap)[(row) >> 3] &= ~(1 << ((row) & 0x07)))

/*
 * Tombstone identification: a delta store tuple is a tombstone if it has
 * zero data attributes and its t_ctid points into columnar virtual TID space.
 *
 * The validity check matters for zero-attribute DATA tuples (a table with
 * no columns): at insert time their t_ctid is still the all-zeroes invalid
 * pointer heap_form_tuple leaves behind, which must read as "not a
 * tombstone" rather than asserting in ItemPointerGetBlockNumber.  Once
 * placed, data tuples get t_ctid = self (below the columnar base).
 */
#define CS_IS_TOMBSTONE(htup) \
	(HeapTupleHeaderGetNatts(htup) == 0 && \
	 ItemPointerIsValid(&(htup)->t_ctid) && \
	 ItemPointerGetBlockNumber(&(htup)->t_ctid) >= CS_COLUMNAR_BLKNO_BASE)

/*
 * A lock-only tombstone records a row lock (cs_tuple_lock) on a columnar
 * row, not a delete: its xmin is the locker and HEAP_XMAX_LOCK_ONLY is
 * set in t_infomask, with the KEYSHR/EXCL bits encoding the lock mode as
 * heap encodes lock-only xmax modes.  Visibility paths must ignore it
 * (the target row stays live); deleters and other lockers treat an
 * in-progress one as a conflict.  VACUUM never materializes it and
 * reaps it once the locking transaction has ended.
 */
#define CS_TOMBSTONE_IS_LOCK_ONLY(htup) \
	(((htup)->t_infomask & HEAP_XMAX_LOCK_ONLY) != 0)

/*
 * Encode a (rg_id, row_offset) pair into a virtual TID.  Inverse of the
 * decode in cs_columnar_delete / cs_index_fetch_tuple.
 */
static inline void
cs_encode_virtual_tid(ItemPointer tid, uint32 rg_id, uint32 row_offset)
{
	BlockNumber blkno = CS_COLUMNAR_BLKNO_BASE
		+ rg_id * CS_VIRTUAL_BLOCKS_PER_RG
		+ row_offset / CS_ROWS_PER_VIRTUAL_BLOCK;
	OffsetNumber offnum = (row_offset % CS_ROWS_PER_VIRTUAL_BLOCK) + 1;

	ItemPointerSet(tid, blkno, offnum);
}

/*
 * Null bitmap write: mark a row as not-null (set bit to 1).
 * Bitmap must be pre-zeroed (palloc0 or memset).
 */
#define CS_SET_NOTNULL(bitmap, row) \
	((bitmap)[(row) >> 3] |= (1 << ((row) & 0x07)))

/* Number of pages needed to store data_len bytes of column data */
#define CS_PAGES_FOR_DATA(data_len) \
	(((data_len) + CS_COLDATA_PER_PAGE - 1) / CS_COLDATA_PER_PAGE)

/* Buffer size for cs_int64_to_numeric_buf() zero-alloc conversion */
#define CS_NI64_BUF_SIZE	24

/*
 * Free page range entry for space reclamation after compaction.
 */
typedef struct CSFreeRange
{
	BlockNumber fr_start;		/* first block of free range */
	uint32		fr_npages;		/* number of consecutive free pages */
} CSFreeRange;

/*
 * Column chunk compression byte layout (cc_compression):
 *
 *   Low nibble (bits 0-3):  base compression method
 *   High nibble (bits 4-7): pre-encoding scheme ID
 *
 * The high nibble is an enum, not independent flags.  Use CS_PREENC()
 * to extract it and compare against the CS_PREENC_* constants.
 */

/* Base compression methods (low nibble) */
#define CS_COMPRESS_NONE		0
#define CS_COMPRESS_PGLZ		1
#define CS_COMPRESS_LZ4			2

/* Extract the base compression method */
#define CS_COMPRESS_BASE(c)		((c) & 0x0F)

/* Pre-encoding scheme IDs (high nibble) */
#define CS_PREENC(c)			((c) & 0xF0)

#define CS_PREENC_NONE			0x00
#define CS_PREENC_DICT			0x10	/* dictionary encoding */
#define CS_PREENC_FOR			0x20	/* frame-of-reference + bit-packing */
#define CS_PREENC_GORILLA		0x30	/* Gorilla: DoD (int) or XOR (float) */
#define CS_PREENC_NI64			0x40	/* numeric-as-int64 */
#define CS_PREENC_NI64_GORILLA	0x50	/* NI64 + Gorilla DoD */
#define CS_PREENC_NI64_FOR		0x60	/* NI64 + FOR */
/* 0x70 free */
#define CS_PREENC_DELTA			0x80	/* delta encoding */
#define CS_PREENC_RLE			0x90	/* run-length encoding */
#define CS_PREENC_DELTA_FOR		0xA0	/* delta + FOR */
/* 0xB0 free */
#define CS_PREENC_NI64_DELTA	0xC0	/* NI64 + delta */
/* 0xD0 free */
#define CS_PREENC_NI64_DELTA_FOR 0xE0	/* NI64 + delta + FOR */
/* 0xF0 free */

/*
 * Backward-compatible aliases used by the encoding selection code in
 * cs_compact.c.  These are combined with | to build the pre-encoding
 * byte; the result must match one of the CS_PREENC_* constants above.
 */
#define CS_COMPRESS_DICT			CS_PREENC_DICT
#define CS_COMPRESS_FOR				CS_PREENC_FOR
#define CS_COMPRESS_NUMERIC_INT64	CS_PREENC_NI64
#define CS_COMPRESS_DELTA			CS_PREENC_DELTA
#define CS_COMPRESS_RLE				CS_PREENC_RLE
#define CS_COMPRESS_GORILLA			CS_PREENC_GORILLA

/*
 * Column chunk location in the relation file.
 */
typedef struct CSColumnChunkDesc
{
	BlockNumber cc_start_block; /* first page of this column's data */
	uint32		cc_npages;		/* number of pages used */
	uint32		cc_compressed_size; /* compressed data size in bytes */
	uint32		cc_uncompressed_size;	/* original data size */
	uint8		cc_compression; /* CS_COMPRESS_xxx */
	bool		cc_has_nulls;	/* does this chunk have any NULLs? */
	BlockNumber cc_nullbitmap_block;	/* block with null bitmap, if any */
} CSColumnChunkDesc;

/*
 * Maximum inline storage for by-reference zone map values (min/max).
 * Covers text up to ~28 chars, numeric up to ~14 digits, uuid, etc.
 */
#define CS_ZONEMAP_INLINE_SIZE	32

/* Zone map storage mode */
#define CS_ZM_BYVAL		0		/* Datum IS the value (by-value types) */
#define CS_ZM_EXACT		1		/* zm_*_data holds exact varlena value */
#define CS_ZM_PREFIX	2		/* truncated prefix bounds (C/POSIX/C.UTF-8) */
#define CS_ZM_SORTKEY	3		/* ICU collation sort key prefix */

/*
 * Zone map entry for one column in one row group.
 */
typedef struct CSZoneMap
{
	Datum		zm_min;			/* by-value: the value; by-ref: reconstructed */
	Datum		zm_max;
	bool		zm_has_minmax;	/* false if all NULLs or unsupported type */
	bool		zm_all_null;	/* every row in this chunk is NULL */
	uint8		zm_mode;		/* CS_ZM_BYVAL / CS_ZM_EXACT / CS_ZM_PREFIX /
								 * CS_ZM_SORTKEY */
	uint16		zm_min_len;		/* stored varlena length (0 for BYVAL) */
	uint16		zm_max_len;
	char		zm_min_data[CS_ZONEMAP_INLINE_SIZE];
	char		zm_max_data[CS_ZONEMAP_INLINE_SIZE];
} CSZoneMap;

/*
 * Row group catalog entry.  One per frozen row group.
 *
 * Stored in row group catalog pages.  For simplicity, we store a
 * fixed-size entry that supports up to CS_MAX_COLUMNS columns.
 * In practice, the variable-length portion is natts entries.
 */
typedef struct CSRowGroupDesc
{
	uint32		rg_id;			/* row group identifier */
	uint32		rg_num_rows;	/* number of rows in this group */
	uint32		rg_num_deleted; /* number of rows deleted */
	BlockNumber rg_delbitmap_block; /* deletion bitmap page, or
									 * InvalidBlockNumber */
	int16		rg_natts;		/* number of columns */
	/* variable length arrays follow -- we index into per-column arrays */
	CSColumnChunkDesc rg_columns[FLEXIBLE_ARRAY_MEMBER];
	/* CSZoneMap entries follow after the column descs */
} CSRowGroupDesc;

/*
 * Byte offset of the CSZoneMap array within a row group descriptor.  The
 * zone maps follow the variable-length rg_columns[] array, but CSZoneMap
 * contains Datum members and so needs MAXALIGN; MAXALIGN the offset (the
 * descriptor itself is always allocated MAXALIGN'd) so the entries are
 * properly aligned regardless of natts and of sizeof(CSColumnChunkDesc).
 */
#define CSRowGroupZoneMapsOffset(natts)	\
	MAXALIGN(offsetof(CSRowGroupDesc, rg_columns) + \
			 (natts) * sizeof(CSColumnChunkDesc))

#define CSRowGroupDescSize(natts)	\
	(CSRowGroupZoneMapsOffset(natts) + (natts) * sizeof(CSZoneMap))

/* Access zone maps from a row group desc */
#define CSRowGroupGetZoneMaps(rg)	\
	((CSZoneMap *) ((char *) (rg) + CSRowGroupZoneMapsOffset((rg)->rg_natts)))

/*
 * Metapage data, stored in the special space of page 0.
 *
 * The per-row-group catalog block numbers are stored in separate
 * "directory pages" (one page holds CS_RGDIR_ENTRIES_PER_PAGE entries).
 * cs_rgdir_start / cs_rgdir_npages locate the directory.
 */
typedef struct CSMetaPageData
{
	uint32		cs_magic;
	uint16		cs_version;
	uint16		cs_natts;		/* number of columns */
	uint32		cs_nrowgroups;	/* number of completed row groups */
	BlockNumber cs_delta_start; /* first delta store page */
	BlockNumber cs_delta_nblocks;	/* number of delta store blocks */
	uint64		cs_total_rows;	/* estimate of total live rows */
	BlockNumber cs_rgdir_start; /* first directory page */
	uint32		cs_rgdir_npages;	/* number of directory pages */
	BlockNumber cs_freelist_start;	/* first free range list page */
	uint32		cs_freelist_npages; /* pages storing free ranges */
	uint32		cs_freelist_nranges;	/* number of valid free ranges */
	BlockNumber cs_data_pages;	/* column data + delta pages (for planner) */
	uint32		cs_flags;		/* CS_META_* flags */
} CSMetaPageData;

/*
 * Compaction has republished rows under new TIDs but the index rebuild
 * has not completed: set atomically with the metapage flip, cleared
 * after reindexing succeeds.  If the rebuild is cancelled (autovacuum
 * is routinely auto-cancelled by conflicting DDL) the flag survives,
 * and the next VACUUM repairs the indexes before reclaiming any pages.
 * Index fetches remain correct in the interim because consumed delta
 * pages keep their contents until reused, and reuse requires
 * AccessExclusiveLock, which is only taken after the repair.
 */
#define CS_META_PENDING_REINDEX	0x0001

#define CS_METAPAGE_BLKNO	0

/*
 * Special-area opaque for delta store pages.
 *
 * The page id distinguishes delta pages (heap-format, scanned tuple by
 * tuple) from column data / bitmap / directory pages (raw byte streams,
 * pd_special == BLCKSZ).  The delta is described in the metapage as a
 * block range, but concurrent relation extension can interleave foreign
 * pages into that range (e.g. an INSERT extending the delta while VACUUM
 * writes column data); readers identify delta pages by this opaque and
 * skip everything else.
 *
 * CS_DELTA_FENCED marks a page whose contents a running compaction has
 * already collected: inserts must not add tuples to it (they extend
 * instead), because the compaction would consume the page without moving
 * the new tuple.  The flag is set as an unlogged hint; if the compaction
 * errors out the fence goes stale, costing only the page's remaining
 * free space until a later VACUUM consumes it.
 */
typedef struct CSDeltaPageOpaque
{
	uint16		cs_flags;
	uint16		cs_page_id;		/* CS_DELTA_PAGE_ID */
} CSDeltaPageOpaque;

#define CS_DELTA_PAGE_ID	0xC511
#define CS_DELTA_FENCED		0x0001

#define CSDeltaPageGetOpaque(page) \
	((CSDeltaPageOpaque *) PageGetSpecialPointer(page))

/* Is this initialized page a delta store page? */
static inline bool
CSPageIsDelta(Page page)
{
	return PageGetSpecialSize(page) == MAXALIGN(sizeof(CSDeltaPageOpaque)) &&
		CSDeltaPageGetOpaque(page)->cs_page_id == CS_DELTA_PAGE_ID;
}

static inline bool
CSDeltaPageIsFenced(Page page)
{
	return (CSDeltaPageGetOpaque(page)->cs_flags & CS_DELTA_FENCED) != 0;
}

/*
 * Dictionary-encoded column state for one loaded column.
 *
 * Populated by cs_ensure_column_loaded() when the column's compression
 * flags include CS_COMPRESS_DICT.  The index array maps each row to a
 * dictionary entry (or to dict_count for NULL).
 */
typedef struct CSDictColumn
{
	Datum	   *dict_values;	/* dict_count Datums into raw_data */
	uint16		dict_count;
	uint8		index_width;	/* 1, 2, or 4 bytes per index entry */
	bool		has_null;		/* idx == dict_count means NULL */
	char	   *index_data;		/* nrows * index_width bytes */
} CSDictColumn;

/*
 * Decompressed column state for one row group.
 *
 * Shared between sequential scans, bitmap scans (embedded in CScanDescData)
 * and index scans (embedded in CSRGCacheEntry).  The cs_load_rowgroup_into()
 * and cs_ensure_column_loaded() functions operate on this struct.
 */
typedef struct CSColumnCache
{
	/*
	 * All decompressed data and pointer arrays for this cache are allocated
	 * in cc_cxt when it is set.  Callers reached through the raw table-AM
	 * interface (ALTER TABLE rewrites, CREATE INDEX, ANALYZE) invoke the lazy
	 * loaders from short-lived per-row contexts that are reset between rows;
	 * without a dedicated context the cache would dangle.  A NULL cc_cxt
	 * means the owner manages lifetimes itself (single-row fetch uses and
	 * frees the cache immediately).
	 */
	MemoryContext cc_cxt;

	/*
	 * Descriptor describing the STORED format of the rows being decoded.
	 * Normally identical to RelationGetDescr(), which the loaders fall back
	 * to when this is NULL.  During an ALTER TABLE rewrite the relcache
	 * already shows the new column types while the data being scanned is
	 * still in the old format; ATRewriteTable's scan slot carries the old
	 * descriptor, and the slot callbacks store it here so the decoders use
	 * the correct attlen/attbyval.
	 */
	TupleDesc	cc_tupdesc;

	Datum	  **col_values;		/* [col][row] -- NULL for direct-access cols */
	bool	  **col_nulls;		/* [col][row] */
	bool	   *col_loaded;		/* per-column loaded flag */
	char	  **col_raw_data;	/* raw decompressed buffers */
	char	  **col_null_bitmap;	/* null bitmaps for fixed-len+nulls */
	CSDictColumn **col_dict;	/* [col] -- NULL if not dict-encoded */
	uint16	   *col_ni64_dscale;	/* [col] -- dscale for NUMERIC_INT64 cols */
	char	  **col_ni64_buf;	/* [col] -- scratch buffer for ni64 Numeric
								 * conversion */
	char	  **col_base_data;	/* [col] -- base-decompressed data (post-LZ4,
								 * pre-FOR decode) */
	uint16	   *col_point_reads;	/* [col] -- point read count this row
									 * group */
	uint32		col_nrows;		/* rows in this row group */
	CSRowGroupDesc *cur_rg_desc;	/* row group descriptor */
	char	   *cur_delbitmap;	/* deletion bitmap (NULL if none) */
} CSColumnCache;

/*
 * Scan descriptor for columnstore.
 */
typedef struct CScanDescData
{
	TableScanDescData rs_base;	/* base class — must be first */

	/* delta store scan state */
	BlockNumber delta_cur_block;	/* current delta block being scanned */
	OffsetNumber delta_cur_offset;	/* current offset on the page */
	Buffer		delta_buf;		/* currently pinned delta page buffer */
	bool		delta_done;		/* finished scanning delta? */

	/* page-mode: pre-scanned visible tuples for the current delta page */
	int			delta_nvisible; /* number of visible tuples */
	int			delta_visible_idx;	/* next index to return */

	/*
	 * Copies of the visible delta tuples for the current page, taken under
	 * the page's share lock.  Rows are returned from these copies, never by
	 * re-reading the page: the scan keeps only a pin between getnext() calls,
	 * and columnstore compaction rewrites delta pages in place (GenericXLog,
	 * exclusive lock -- not a cleanup lock), so a concurrent VACUUM could
	 * tear the page bytes out from under an unlocked read.  Held in
	 * delta_tuple_cxt, reset once per page.
	 */
	HeapTuple  *delta_visible_tuples;
	MemoryContext delta_tuple_cxt;

	/* columnar scan state */
	bool		columnar_done;	/* finished scanning columnar? */
	uint32		cur_rowgroup;	/* current row group being scanned */
	uint32		cur_row_in_group;	/* current row within the row group */

	/* cached metapage info */
	BlockNumber delta_start;
	BlockNumber delta_nblocks;
	uint32		nrowgroups;
	int16		natts;
	BlockNumber *rg_catalog_blocks; /* palloc'd per-RG catalog pages */

	/* column projection */
	Bitmapset  *needed_cols;	/* columns needed by the query (attno) */
	int		   *needed_col_list;	/* sorted array of needed column indices */
	int			needed_col_count;	/* number of entries in needed_col_list */
	bool		count_optimized;	/* true when no columns needed (COUNT) */
	bool		rg_desc_preloaded;	/* catalog already read for zone maps */

	/* scan keys for zone map filtering */
	int			cs_nkeys;		/* number of pushed-down scan keys */
	ScanKeyData *cs_keys;		/* pushed-down scan keys */
	FmgrInfo   *zm_cmp_finfo;	/* btree comparison functions per key */
	Oid		   *zm_cmp_collation;	/* collations per key */
	bool		zm_cmp_initialized; /* lazy init flag */

	/* Deconstructed SK_SEARCHARRAY elements for zone map evaluation */
	int		   *sa_nelem;		/* [key_idx] -> number of array elements */
	Datum	  **sa_elem_values; /* [key_idx] -> deconstructed array values */
	bool	  **sa_elem_nulls;	/* [key_idx] -> deconstructed array nulls */
	bool		sa_initialized; /* true after arrays are deconstructed */

	/*
	 * Per-scan-key cache for dictionary-aware evaluation. dk_match[i] is a
	 * bool array indexed by dict code; true means the dict entry matches the
	 * key's predicate.  sa_nelem[i] and sa_elem_values[i] hold deconstructed
	 * elements for SK_SEARCHARRAY keys. dk_match_rg tracks which row group
	 * the match arrays were built for.
	 */
	bool	  **dk_match;		/* [key_idx][dict_code] -> matches? */
	uint32		dk_match_rg;	/* row group ID for which dk_match is valid */

	/* Bloom filter pushdown (hash join probe values pushed to scan side) */
	struct bloom_filter **bf_filters;	/* [nfilters] pushed bloom filters */
	AttrNumber *bf_attnos;		/* [nfilters] 1-based column per filter */
	int			bf_nfilters;	/* number of pushed bloom filters */
	bool	  **bf_dk_match;	/* [nfilters][dict_code] bloom match? */
	uint32	   *bf_dk_match_rg; /* [nfilters] row group for which dk_match is
								 * valid */
	FmgrInfo   *bf_hashfn;		/* [nfilters] hash fn used to key the filter */
	Oid		   *bf_hashcoll;	/* [nfilters] collation for the hash fn */
	bool		bf_disabled;	/* probing turned off (ineffective filter) */
	uint64		bf_rows_examined;	/* rows that reached the bloom probe */

	/* bitmap scan state */
	bool		bm_started;		/* has TBM iteration begun? */
	OffsetNumber bm_offsets[MaxHeapTuplesPerPage];	/* current page's offsets */
	int			bm_noffsets;	/* number of offsets in current page */
	int			bm_curoff;		/* current offset index */
	bool		bm_lossy;		/* is current page lossy? */
	BlockNumber bm_block;		/* current TBM block number */
	bool		bm_recheck;		/* current page requires recheck */

	/* TABLESAMPLE state */
	bool		rs_index_build; /* CREATE INDEX scan: see
								 * cs_index_build_range_scan */
	bool		sample_inited;	/* true after first sample call */
	BlockNumber sample_nblocks; /* total virtual blocks */
	BlockNumber sample_cur_block;	/* current virtual block */
	bool		sample_is_delta;	/* current block is delta? */
	uint32		sample_row_base;	/* first row in current virtual block */
	uint32		sample_rg_loaded;	/* rg_id of currently loaded RG */

	/* decompressed column state for current row group */
	/* Tombstone state for MVCC columnar deletes */
	ItemPointerData *tombstone_tids;	/* sorted virtual TIDs of deleted rows */
	int			tombstone_count;
	bool		tombstones_collected;	/* lazy init flag */
	Snapshot	tombstone_snapshot; /* snapshot the tids were collected under */

	/* Selection bitmap: batch-evaluated predicate results per row group */
	char	   *sel_bitmap;		/* one bit per row, 1=passes all quals */
	uint32		sel_bitmap_nrows;	/* nrows when sel_bitmap was allocated */
	bool		sel_bitmap_valid;	/* true after cs_build_selection_bitmap */

	/*
	 * Instrumentation counters for EXPLAIN ANALYZE, incremented during the
	 * scan and reported by cs_explain_custom_scan.
	 */
	uint64		instr_rg_examined;	/* row groups whose catalog was read */
	uint64		instr_rg_zonemap_skipped;	/* skipped by
											 * cs_zonemap_skip_rowgroup */
	uint64		instr_bloom_rg_probed;	/* row groups a pushed bloom filter
										 * probed */
	uint64		instr_bloom_rg_skipped; /* row groups bloom eliminated
										 * entirely */
	uint64		instr_bloom_rows_removed;	/* rows a pushed bloom filter
											 * removed */

	CSColumnCache colcache;
} CScanDescData;

typedef CScanDescData *CScanDesc;

/*
 * Parallel scan descriptor for columnstore (shared memory).
 *
 * Work unit 0 = delta store, units 1..N = row groups 0..N-1.
 * Workers atomically increment pcs_nallocated to claim the next unit.
 */
typedef struct CSParallelScanDescData
{
	ParallelTableScanDescData base; /* must be first */
	uint32		pcs_nrowgroups;
	int16		pcs_natts;
	BlockNumber pcs_delta_start;
	BlockNumber pcs_delta_nblocks;

	/*
	 * Location of the row group directory.  The directory contents are
	 * deliberately NOT copied into shared memory: the struct must be
	 * fixed-size, because table_parallelscan_estimate and
	 * table_parallelscan_initialize read the metapage at different times and
	 * a VACUUM committing new row groups in between would make a flexible
	 * array overrun the DSM allocation.  Workers read the directory pages
	 * themselves from these captured coordinates, which is safe because a
	 * published directory is never modified in place: compaction writes a
	 * replacement and frees the old pages, and freed pages are only reused
	 * under AccessExclusiveLock (see cs_freelist_alloc), which cannot be
	 * taken while this scan runs.
	 */
	BlockNumber pcs_rgdir_start;
	uint32		pcs_rgdir_npages;

	pg_atomic_uint64 pcs_nallocated;	/* next work unit to hand out */

	/*
	 * EXPLAIN ANALYZE instrumentation, summed across all participants.  Each
	 * participant folds its private scan counters in here from
	 * cs_shutdown_custom_scan.  As a child of Gather that callback runs while
	 * this descriptor is still mapped, and a worker's runs before it signals
	 * EOF, so by the time the leader's runs (after Gather has consumed every
	 * worker's output) the workers' contributions are all present; the leader
	 * then folds the shared totals into its own scan for the EXPLAIN output.
	 */
	pg_atomic_uint64 pcs_instr_rg_examined;
	pg_atomic_uint64 pcs_instr_rg_zonemap_skipped;
} CSParallelScanDescData;

typedef CSParallelScanDescData *CSParallelScanDesc;

/*
 * LRU cache entry for one decompressed row group (index fetch path).
 */
typedef struct CSRGCacheEntry
{
	uint32		rg_id;			/* UINT32_MAX = empty slot */
	uint32		lru_counter;	/* for LRU eviction */
	CSColumnCache cache;		/* decompressed column data */
} CSRGCacheEntry;

#define CS_INDEX_CACHE_MAX	256

/*
 * Index fetch descriptor for columnstore.
 */
typedef struct CSIndexFetchData
{
	IndexFetchTableData base;	/* must be first */
	MemoryContext rg_cache_parent;	/* parent for per-entry cache contexts;
									 * captured at fetch-begin so entry caches
									 * outlive per-row contexts */
	Buffer		delta_buf;		/* buffer for delta page lookups */
	bool		meta_initialized;	/* true after first columnar lookup */
	int			natts;			/* relation natts (cached) */
	uint32		nrowgroups;		/* from metapage */
	BlockNumber *rg_catalog_blocks; /* row group directory */
	CSRGCacheEntry *rg_cache;	/* LRU cache array */
	int			rg_cache_size;	/* number of entries in rg_cache */
	uint32		lru_clock;		/* monotonic LRU counter */

	/* Tombstone state for MVCC columnar deletes */
	ItemPointerData *tombstone_tids;	/* sorted virtual TIDs of deleted rows */
	int			tombstone_count;
	bool		tombstones_collected;	/* lazy init flag */
} CSIndexFetchData;


/*
 * Custom TupleTableSlot for columnstore with lazy column decompression.
 *
 * For delta store rows, all attributes are filled eagerly (like virtual).
 * For columnar rows, columns are decompressed on demand via getsomeattrs.
 */
typedef struct CSTupleTableSlot
{
	TupleTableSlot base;
	char	   *data;			/* data for materialized slots */

	/* columnstore-specific lazy loading state */
	CScanDesc	cs_scan;		/* for projection pushdown info (may be NULL) */
	Relation	cs_rel;			/* for lazy column loading in index scans */
	CSColumnCache *cs_colcache; /* for column data access */
	uint32		cs_row;			/* row index in current row group */
	bool		cs_is_columnar; /* true = columnar row, false = delta row */
} CSTupleTableSlot;

/*
 * TTSOpsColumnStore is defined in and referenced only from within this
 * extension's own shared module, so it must not carry PGDLLIMPORT: on Windows
 * that would mark it __declspec(dllimport) and clash with the local definition
 * ("redeclared without dllimport attribute").  It is an internal symbol, not
 * one exported to other modules, so it needs no DLL decoration at all.
 */
extern const TupleTableSlotOps TTSOpsColumnStore;

/* Point-read threshold: above this, fully decode the column */
#define CS_POINT_READ_THRESHOLD		64

/* ---- Function prototypes ---- */

/* columnstore.c */
extern const TableAmRoutine *GetColumnstoreAmRoutine(void);

/* cs_storage.c */
extern void cs_init_metapage(Relation rel);
extern void cs_read_metapage(Relation rel, CSMetaPageData *meta);
extern void cs_write_metapage(Relation rel, CSMetaPageData *meta);
extern void cs_write_metapage_cond_clear_delta(Relation rel,
											   CSMetaPageData *meta,
											   uint32 orig_delta_nblocks);
extern void cs_write_metapage_advance_delta(Relation rel,
											CSMetaPageData *meta,
											uint32 consumed);
extern void cs_delta_page_init(Page page);
extern bool cs_metapage_merge_insert(Relation rel, BlockNumber new_blkno,
									 int64 rows_added);
extern void cs_metapage_update_stats(Relation rel, uint64 total_rows,
									 BlockNumber data_pages);
extern void cs_metapage_clear_flags(Relation rel, uint32 flags);
extern void cs_repair_pending_reindex(Relation rel);
extern BlockNumber *cs_read_rgdir(Relation rel, CSMetaPageData *meta);
extern uint32 cs_write_rgdir(Relation rel, BlockNumber *dir, uint32 nrowgroups,
							 BlockNumber *start_block_out);
extern uint32 cs_write_column_data(Relation rel, BlockNumber *start_block_inout,
								   const char *data, uint32 data_len);
extern BlockNumber cs_alloc_delbitmap(Relation rel, uint32 num_rows);
extern char *cs_read_delbitmap(Relation rel, BlockNumber start_block,
							   uint32 num_rows);
extern void cs_delbitmap_set_bit(Relation rel, BlockNumber start_block,
								 uint32 row_offset);
extern bool cs_delbitmap_test_bit(Relation rel, BlockNumber start_block,
								  uint32 row_offset);
extern BlockNumber *cs_read_rgdir_at(Relation rel, BlockNumber rgdir_start,
									 uint32 rgdir_npages, uint32 nrowgroups);
extern CSFreeRange *cs_read_freelist(Relation rel, CSMetaPageData *meta);
extern void cs_write_freelist(Relation rel, CSMetaPageData *meta,
							  CSFreeRange *ranges, uint32 nranges);
extern BlockNumber cs_freelist_alloc(CSFreeRange *ranges, uint32 *nranges,
									 uint32 npages);
extern void cs_freelist_add(CSFreeRange **ranges, uint32 *nranges,
							uint32 *max_ranges,
							BlockNumber start, uint32 npages);

/* cs_scan.c */
extern void cs_read_rowgroup_catalog(Relation rel, BlockNumber catalog_block,
									 CSRowGroupDesc *rg_desc, int16 natts);
extern char *cs_read_column_pages(Relation rel, BlockNumber start_block,
								  uint32 npages, uint32 data_len);

/* cs_insert.c */
extern void cs_tuple_insert(Relation rel, TupleTableSlot *slot,
							CommandId cid, uint32 options,
							BulkInsertStateData *bistate);
extern void cs_tuple_insert_speculative(Relation rel, TupleTableSlot *slot,
										CommandId cid, uint32 options,
										BulkInsertStateData *bistate,
										uint32 specToken);
extern void cs_tuple_complete_speculative(Relation rel, TupleTableSlot *slot,
										  uint32 specToken, bool succeeded);
extern void cs_multi_insert(Relation rel, TupleTableSlot **slots, int nslots,
							CommandId cid, uint32 options,
							BulkInsertStateData *bistate);
extern void cs_delta_insert_tombstone(Relation rel, ItemPointer target_tid,
									  uint16 extra_infomask);

/* cs_scan.c */
extern bool cs_delta_satisfies_visibility(HeapTupleData *tuple,
										  Snapshot snapshot);
extern TableScanDesc cs_scan_begin(Relation rel, Snapshot snapshot,
								   int nkeys, ScanKeyData *key,
								   ParallelTableScanDesc pscan,
								   uint32 flags);
extern void cs_scan_end(TableScanDesc scan);
extern void cs_scan_rescan(TableScanDesc scan, ScanKeyData *key,
						   bool set_params, bool allow_strat,
						   bool allow_sync, bool allow_pagemode);
extern void cs_scan_set_projection(TableScanDesc scan,
								   Bitmapset *needed_cols);
extern void cs_scan_set_qual_keys(TableScanDesc scan, int nkeys,
								  ScanKeyData *keys);
extern void cs_scan_set_bloom_filter(TableScanDesc scan, AttrNumber attno,
									 struct bloom_filter *filter,
									 Oid hashfn, Oid hashcoll);
extern bool cs_scan_column_encoding(TableScanDesc scan, AttrNumber attno,
									Oid *phys_type, int32 *dscale);
extern bool cs_scan_get_raw_attr(TableScanDesc scan, TupleTableSlot *slot,
								 AttrNumber attno, Datum *value, bool *isnull);
extern bool cs_scan_getnextslot(TableScanDesc scan, ScanDirection direction,
								TupleTableSlot *slot);
extern void cs_scan_set_tidrange(TableScanDesc scan,
								 ItemPointer mintid, ItemPointer maxtid);
extern bool cs_scan_getnextslot_tidrange(TableScanDesc scan,
										 ScanDirection direction,
										 TupleTableSlot *slot);
extern bool cs_load_rowgroup(CScanDesc scan, Relation rel, uint32 rg_id);
extern bool cs_load_rowgroup_into(CSColumnCache *cache, Relation rel,
								  BlockNumber catalog_block, int natts);
extern void cs_ensure_column_loaded(CSColumnCache *cache, Relation rel,
									int col);
extern void cs_base_decompress_column(CSColumnCache *cache, Relation rel,
									  int col);
extern Datum cs_column_point_read(CSColumnCache *cache, Relation rel,
								  int col, uint32 row, bool *isnull);
extern Datum cs_int64_to_numeric_buf(int64 val, int dscale, char *buf);
extern void cs_column_cache_free(CSColumnCache *cache, int natts);
extern void cs_collect_tombstones(CScanDesc scan);
extern bool cs_tombstone_lookup(ItemPointerData *tids, int count,
								ItemPointer target);

/* cs_vacuum.c */
extern double columnstore_rowgroup_compaction_threshold;
extern void cs_vacuum_rel(Relation rel, const VacuumParams *params,
						  BufferAccessStrategy bstrategy);

/* columnstore.c (cost-model helpers used by CustomPath) */
extern void cs_relation_cost_factors(Relation rel,
									 double *seq_page_cost_factor,
									 double *rand_page_cost_factor,
									 double *cpu_tuple_cost_factor,
									 BlockNumber *rand_io_pages,
									 bool *disk_cost_parallelizable,
									 double *index_fetch_cost);
extern int	cs_compute_parallel_workers(Relation rel);
extern const char *cs_get_sort_key(Relation rel);

/* cs_customscan.c */
extern void cs_register_custom_scan_methods(void);

/* cs_compact.c */
extern void cs_compact_delta(Relation rel);
extern void cs_freeze_delta(Relation rel, TransactionId horizon);
extern void cs_compact_rowgroups(Relation rel);
extern void cs_materialize_tombstones(Relation rel, TransactionId horizon);
extern BlockNumber cs_write_one_rowgroup(Relation rel, TupleDesc tupdesc,
										 int natts,
										 Datum **col_values, bool **col_nulls,
										 uint32 nrows, uint32 rg_id,
										 CSFreeRange *freelist,
										 uint32 *fl_nranges);

#endif							/* CS_INTERNAL_H */
