/*-------------------------------------------------------------------------
 *
 * cs_storage.c
 *	  Metapage initialization and page format helpers for columnstore.
 *
 * The metapage data is stored in the "special space" area of page 0
 * (via PageInit with a non-zero specialSize).  This ensures the data
 * survives GenericXLog's zeroing of the free-space hole between
 * pd_lower and pd_upper.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * contrib/columnstore/cs_storage.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cs_internal.h"
#include "access/generic_xlog.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "storage/smgr.h"

/*
 * Get metapage data pointer from a page initialized with special space.
 */
static inline CSMetaPageData *
cs_metapage_get_data(Page page)
{
	return (CSMetaPageData *) PageGetSpecialPointer(page);
}

/*
 * Initialize the metapage for a newly created columnstore relation.
 *
 * Called from the first INSERT when we detect the relation has no blocks.
 * We extend the relation and initialize the metapage and first delta page.
 */
void
cs_init_metapage(Relation rel)
{
	Buffer		metabuf;
	Buffer		deltabuf;
	Page		metapage;
	Page		deltapage;
	GenericXLogState *state;
	CSMetaPageData *meta;

	/*
	 * Use the relation extension lock to avoid races with concurrent
	 * inserters.  Extend to get 2 blocks: metapage (0) and first delta (1).
	 */
	LockRelationForExtension(rel, ExclusiveLock);

	/* Re-check under lock */
	if (RelationGetNumberOfBlocks(rel) > 0)
	{
		UnlockRelationForExtension(rel, ExclusiveLock);
		return;
	}

	metabuf = ReadBufferExtended(rel, MAIN_FORKNUM, P_NEW, RBM_ZERO_AND_LOCK,
								 NULL);
	deltabuf = ReadBufferExtended(rel, MAIN_FORKNUM, P_NEW, RBM_ZERO_AND_LOCK,
								  NULL);

	UnlockRelationForExtension(rel, ExclusiveLock);

	state = GenericXLogStart(rel);

	/*
	 * Initialize metapage with special space for CSMetaPageData. This puts
	 * the metadata in the pd_special..end area which is preserved by
	 * GenericXLog's page copy logic.
	 */
	metapage = GenericXLogRegisterBuffer(state, metabuf,
										 GENERIC_XLOG_FULL_IMAGE);
	PageInit(metapage, BLCKSZ, sizeof(CSMetaPageData));

	meta = cs_metapage_get_data(metapage);
	meta->cs_magic = CS_MAGIC;
	meta->cs_version = CS_VERSION;
	meta->cs_natts = RelationGetDescr(rel)->natts;
	meta->cs_nrowgroups = 0;
	meta->cs_delta_start = BufferGetBlockNumber(deltabuf);
	meta->cs_delta_nblocks = 1;
	meta->cs_total_rows = 0;
	meta->cs_rgdir_start = InvalidBlockNumber;
	meta->cs_rgdir_npages = 0;
	meta->cs_freelist_start = InvalidBlockNumber;
	meta->cs_freelist_npages = 0;
	meta->cs_freelist_nranges = 0;
	meta->cs_data_pages = 0;
	meta->cs_flags = 0;

	/* Initialize first delta page */
	deltapage = GenericXLogRegisterBuffer(state, deltabuf,
										  GENERIC_XLOG_FULL_IMAGE);
	cs_delta_page_init(deltapage);

	GenericXLogFinish(state);

	UnlockReleaseBuffer(deltabuf);
	UnlockReleaseBuffer(metabuf);
}

/*
 * Initialize a delta store page, stamping the identifying opaque.
 */
void
cs_delta_page_init(Page page)
{
	CSDeltaPageOpaque *opaque;

	PageInit(page, BLCKSZ, sizeof(CSDeltaPageOpaque));
	opaque = CSDeltaPageGetOpaque(page);
	opaque->cs_flags = 0;
	opaque->cs_page_id = CS_DELTA_PAGE_ID;
}

/*
 * Read the metapage data.  Caller should not hold any buffer lock.
 */
void
cs_read_metapage(Relation rel, CSMetaPageData *meta)
{
	Buffer		buf;
	Page		page;

	if (RelationGetNumberOfBlocks(rel) == 0)
	{
		/* Not yet initialized -- return empty state */
		memset(meta, 0, sizeof(CSMetaPageData));
		meta->cs_magic = CS_MAGIC;
		meta->cs_version = CS_VERSION;
		meta->cs_delta_start = InvalidBlockNumber;
		meta->cs_rgdir_start = InvalidBlockNumber;
		meta->cs_freelist_start = InvalidBlockNumber;
		return;
	}

	buf = ReadBuffer(rel, CS_METAPAGE_BLKNO);
	LockBuffer(buf, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buf);

	memcpy(meta, cs_metapage_get_data(page), sizeof(CSMetaPageData));

	if (meta->cs_magic != CS_MAGIC)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("columnstore metapage has wrong magic number")));

	if (meta->cs_version != CS_VERSION)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("columnstore metapage version %u is not supported",
						meta->cs_version),
				 errhint("This table was created with an older columnstore format. "
						 "Dump and reload the table to upgrade.")));

	UnlockReleaseBuffer(buf);
}

/*
 * Atomically merge an insert-side update into the on-disk metapage.
 *
 * new_blkno is the delta page this backend just created by extending the
 * relation, or InvalidBlockNumber if the tuple(s) went into an existing
 * page.  rows_added is the live-row count adjustment (0 for tombstones).
 *
 * All adjustments are applied to the CURRENT on-disk values under one
 * exclusive buffer lock.  The insert paths must never write back a
 * privately held copy of the whole metapage: between their read and
 * write another inserter may have extended the delta or VACUUM may have
 * advanced it, and a full-struct overwrite would silently revert that.
 *
 * The delta range is widened to cover new_blkno.  Pages between the old
 * range end and new_blkno may belong to a concurrent VACUUM's column
 * data; including them is fine because readers identify delta pages by
 * their special-area opaque and skip foreign pages.
 *
 * Returns true if the delta went from empty to non-empty (callers use
 * this to send a relcache invalidation; see cs_delta_do_insert).
 */
bool
cs_metapage_merge_insert(Relation rel, BlockNumber new_blkno, int64 rows_added)
{
	Buffer		buf;
	Page		page;
	GenericXLogState *state;
	CSMetaPageData *on_disk;
	bool		became_nonempty = false;

	if (new_blkno == InvalidBlockNumber && rows_added == 0)
		return false;			/* nothing to record */

	buf = ReadBuffer(rel, CS_METAPAGE_BLKNO);
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

	state = GenericXLogStart(rel);
	page = GenericXLogRegisterBuffer(state, buf, 0);
	on_disk = cs_metapage_get_data(page);

	if (new_blkno != InvalidBlockNumber)
	{
		if (on_disk->cs_delta_nblocks == 0 ||
			on_disk->cs_delta_start == InvalidBlockNumber)
		{
			on_disk->cs_delta_start = new_blkno;
			on_disk->cs_delta_nblocks = 1;
			became_nonempty = true;
		}
		else
		{
			/*
			 * New pages come from extending the relation, so they always lie
			 * at or beyond the current range end; VACUUM only ever moves the
			 * range start forward over already-consumed pages.
			 */
			Assert(new_blkno >= on_disk->cs_delta_start);
			if (new_blkno - on_disk->cs_delta_start + 1 >
				on_disk->cs_delta_nblocks)
				on_disk->cs_delta_nblocks =
					new_blkno - on_disk->cs_delta_start + 1;
		}
	}

	on_disk->cs_total_rows += rows_added;

	GenericXLogFinish(state);
	UnlockReleaseBuffer(buf);

	return became_nonempty;
}

/*
 * Update only the planner statistics fields of the metapage.
 *
 * Used by VACUUM after compaction: a full cs_write_metapage here would
 * race concurrent inserts (clobbering a delta extension that happened
 * after VACUUM re-read the metapage), so touch nothing but the stats.
 * The row count is itself an estimate; a concurrent insert's increment
 * lost to this overwrite is corrected by the next VACUUM.
 */
void
cs_metapage_update_stats(Relation rel, uint64 total_rows,
						 BlockNumber data_pages)
{
	Buffer		buf;
	Page		page;
	GenericXLogState *state;
	CSMetaPageData *on_disk;

	buf = ReadBuffer(rel, CS_METAPAGE_BLKNO);
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

	state = GenericXLogStart(rel);
	page = GenericXLogRegisterBuffer(state, buf, 0);
	on_disk = cs_metapage_get_data(page);

	on_disk->cs_total_rows = total_rows;
	on_disk->cs_data_pages = data_pages;

	GenericXLogFinish(state);
	UnlockReleaseBuffer(buf);
}

/*
 * Atomically clear metapage flag bits, leaving everything else as it
 * is on disk (so concurrent insert merges are not clobbered).
 */
void
cs_metapage_clear_flags(Relation rel, uint32 flags)
{
	Buffer		buf;
	Page		page;
	GenericXLogState *state;
	CSMetaPageData *on_disk;

	buf = ReadBuffer(rel, CS_METAPAGE_BLKNO);
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

	state = GenericXLogStart(rel);
	page = GenericXLogRegisterBuffer(state, buf, 0);
	on_disk = cs_metapage_get_data(page);

	on_disk->cs_flags &= ~flags;

	GenericXLogFinish(state);
	UnlockReleaseBuffer(buf);
}

/*
 * Write updated metapage data.
 */
void
cs_write_metapage(Relation rel, CSMetaPageData *meta)
{
	Buffer		buf;
	Page		page;
	GenericXLogState *state;

	buf = ReadBuffer(rel, CS_METAPAGE_BLKNO);
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

	state = GenericXLogStart(rel);
	page = GenericXLogRegisterBuffer(state, buf, 0);

	memcpy(cs_metapage_get_data(page), meta, sizeof(CSMetaPageData));

	GenericXLogFinish(state);
	UnlockReleaseBuffer(buf);
}

/*
 * Atomically update the metapage, adjusting the delta range if a concurrent
 * INSERT extended it since we started scanning.
 *
 * The caller supplies the desired metapage state in *meta (which may have
 * cs_delta_nblocks = 0, i.e. "clear the delta").  orig_delta_nblocks is the
 * delta size the caller observed at the start of its scan.
 *
 * Under a single exclusive buffer lock we read the on-disk metapage, check
 * whether the delta grew, and merge the caller's updates.  If the current
 * delta is larger than expected, we preserve the tail pages that were added
 * after our scan started.
 */
void
cs_write_metapage_cond_clear_delta(Relation rel, CSMetaPageData *meta,
								   uint32 orig_delta_nblocks)
{
	Buffer		buf;
	Page		page;
	GenericXLogState *state;
	CSMetaPageData *on_disk;

	buf = ReadBuffer(rel, CS_METAPAGE_BLKNO);
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

	state = GenericXLogStart(rel);
	page = GenericXLogRegisterBuffer(state, buf, 0);
	on_disk = cs_metapage_get_data(page);

	/*
	 * Check whether the delta grew since we started.  Delta inserts only ever
	 * append pages, so if the current size exceeds what we scanned, the tail
	 * pages belong to concurrent transactions and must be preserved.
	 */
	if (on_disk->cs_delta_nblocks > orig_delta_nblocks)
	{
		uint32		new_pages = on_disk->cs_delta_nblocks - orig_delta_nblocks;

		meta->cs_delta_start = on_disk->cs_delta_start + orig_delta_nblocks;
		meta->cs_delta_nblocks = new_pages;
	}

	/*
	 * Compaction moves rows, it doesn't add or remove live ones; keep the
	 * row-count estimate concurrent inserters have been merging into the
	 * on-disk metapage rather than the stale copy the caller read before the
	 * (long) compaction started.
	 */
	meta->cs_total_rows = on_disk->cs_total_rows;

	memcpy(on_disk, meta, sizeof(CSMetaPageData));

	GenericXLogFinish(state);
	UnlockReleaseBuffer(buf);
}

/*
 * Update the metapage, advancing the delta start past 'consumed' pages.
 *
 * Like cs_write_metapage_cond_clear_delta(), but the delta is only
 * consumed up to a prefix: pages still holding tuples that concurrent or
 * recent transactions may need (in-flight inserts, not-yet-all-visible
 * rows, pending tombstones) stay in place, with their TIDs intact.  Any
 * pages appended by concurrent inserts after the caller's scan are
 * preserved the same way.
 */
void
cs_write_metapage_advance_delta(Relation rel, CSMetaPageData *meta,
								uint32 consumed)
{
	Buffer		buf;
	Page		page;
	GenericXLogState *state;
	CSMetaPageData *on_disk;

	buf = ReadBuffer(rel, CS_METAPAGE_BLKNO);
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

	state = GenericXLogStart(rel);
	page = GenericXLogRegisterBuffer(state, buf, 0);
	on_disk = cs_metapage_get_data(page);

	Assert(consumed <= on_disk->cs_delta_nblocks);

	if (on_disk->cs_delta_nblocks > consumed)
	{
		meta->cs_delta_start = on_disk->cs_delta_start + consumed;
		meta->cs_delta_nblocks = on_disk->cs_delta_nblocks - consumed;
	}
	else
	{
		meta->cs_delta_start = InvalidBlockNumber;
		meta->cs_delta_nblocks = 0;
	}

	/* see cs_write_metapage_cond_clear_delta */
	meta->cs_total_rows = on_disk->cs_total_rows;

	memcpy(on_disk, meta, sizeof(CSMetaPageData));

	GenericXLogFinish(state);
	UnlockReleaseBuffer(buf);
}

/*
 * Write raw column data to relation pages.
 *
 * If *start_block_inout is a valid block number, write to existing pages
 * starting there (reusing previously freed pages).  If InvalidBlockNumber,
 * extend the relation with new pages.  Either way, *start_block_inout is
 * set to the actual first block written on return.
 *
 * Returns the number of pages written.
 */
uint32
cs_write_column_data(Relation rel, BlockNumber *start_block_inout,
					 const char *data, uint32 data_len)
{
	uint32		offset = 0;
	uint32		npages = 0;
	BlockNumber start_block = *start_block_inout;
	bool		reuse = (start_block != InvalidBlockNumber);

	if (!reuse)
		LockRelationForExtension(rel, ExclusiveLock);

	while (offset < data_len)
	{
		Buffer		buf;
		Page		page;
		GenericXLogState *state;
		uint32		chunk;

		chunk = Min(data_len - offset, CS_COLDATA_PER_PAGE);

		if (reuse)
		{
			/* Write to pre-allocated (freed) block */
			buf = ReadBuffer(rel, start_block + npages);
			LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
		}
		else
		{
			/*
			 * Extend the relation.  The extension lock is held across the
			 * whole loop (taken before the first page, released after the
			 * last): readers locate chunk pages as start_block + i, so a
			 * concurrent inserter extending the delta in between would punch
			 * a foreign page into the chunk and corrupt reads.
			 */
			buf = ReadBufferExtended(rel, MAIN_FORKNUM, P_NEW,
									 RBM_ZERO_AND_LOCK, NULL);
			if (npages == 0)
				start_block = BufferGetBlockNumber(buf);
		}

		state = GenericXLogStart(rel);
		page = GenericXLogRegisterBuffer(state, buf, GENERIC_XLOG_FULL_IMAGE);

		PageInit(page, BLCKSZ, 0);
		memcpy(PageGetContents(page), data + offset, chunk);
		/* Advance pd_lower past the data so GenericXLog preserves it */
		((PageHeader) page)->pd_lower = MAXALIGN(SizeOfPageHeaderData) + MAXALIGN(chunk);

		GenericXLogFinish(state);
		UnlockReleaseBuffer(buf);

		offset += chunk;
		npages++;
	}

	if (!reuse)
		UnlockRelationForExtension(rel, ExclusiveLock);

	*start_block_inout = start_block;
	return npages;
}

/*
 * Read the row group directory from directory pages.
 *
 * Returns a palloc'd array of BlockNumber[meta->cs_nrowgroups], or NULL
 * if there are no row groups.  Caller must pfree the result.
 */
BlockNumber *
cs_read_rgdir(Relation rel, CSMetaPageData *meta)
{
	return cs_read_rgdir_at(rel, meta->cs_rgdir_start,
							meta->cs_rgdir_npages, meta->cs_nrowgroups);
}

/*
 * As cs_read_rgdir, but from explicit directory coordinates (used by
 * parallel workers, which get them from the shared scan descriptor
 * rather than the live metapage).
 */
BlockNumber *
cs_read_rgdir_at(Relation rel, BlockNumber rgdir_start, uint32 rgdir_npages,
				 uint32 nrowgroups)
{
	uint32		data_len;

	if (nrowgroups == 0)
		return NULL;

	data_len = sizeof(BlockNumber) * nrowgroups;
	return (BlockNumber *) cs_read_column_pages(rel, rgdir_start,
												rgdir_npages, data_len);
}

/*
 * Write the row group directory to new pages.
 *
 * Returns the number of pages written and sets *start_block_inout to the
 * first directory page block number.  Returns 0 if nrowgroups == 0.
 */
uint32
cs_write_rgdir(Relation rel, BlockNumber *dir, uint32 nrowgroups,
			   BlockNumber *start_block_inout)
{
	uint32		data_len;
	uint32		npages;

	if (nrowgroups == 0)
	{
		*start_block_inout = InvalidBlockNumber;
		return 0;
	}

	data_len = sizeof(BlockNumber) * nrowgroups;

	npages = cs_write_column_data(rel, start_block_inout,
								  (const char *) dir, data_len);

	return npages;
}

/*
 * Allocate and initialize a deletion bitmap for a row group.
 *
 * The bitmap may span multiple pages for large row groups (>~65K rows).
 * All pages are initialized with zeros (no deletions).  Returns the
 * block number of the first bitmap page.
 */
BlockNumber
cs_alloc_delbitmap(Relation rel, uint32 num_rows)
{
	uint32		npages = CS_DELBITMAP_NPAGES(num_rows);
	uint32		bmsize = CS_DELBITMAP_BYTES(num_rows);
	BlockNumber start_block;

	LockRelationForExtension(rel, ExclusiveLock);

	/* Allocate consecutive pages */
	start_block = RelationGetNumberOfBlocks(rel);
	for (uint32 i = 0; i < npages; i++)
	{
		Buffer		buf;
		Page		page;
		GenericXLogState *state;
		uint32		chunk;

		buf = ReadBufferExtended(rel, MAIN_FORKNUM, P_NEW,
								 RBM_ZERO_AND_LOCK, NULL);
		if (i == 0)
			start_block = BufferGetBlockNumber(buf);

		chunk = Min(bmsize - i * CS_COLDATA_PER_PAGE, CS_COLDATA_PER_PAGE);

		state = GenericXLogStart(rel);
		page = GenericXLogRegisterBuffer(state, buf, GENERIC_XLOG_FULL_IMAGE);
		PageInit(page, BLCKSZ, 0);
		/* Advance pd_lower past bitmap data so GenericXLog preserves it */
		((PageHeader) page)->pd_lower =
			MAXALIGN(SizeOfPageHeaderData) + MAXALIGN(chunk);
		GenericXLogFinish(state);
		UnlockReleaseBuffer(buf);
	}

	UnlockRelationForExtension(rel, ExclusiveLock);

	return start_block;
}

/*
 * Read the full deletion bitmap into a palloc'd buffer.
 *
 * The bitmap may span multiple consecutive pages starting at start_block.
 * Returns a palloc'd buffer of CS_DELBITMAP_BYTES(num_rows) bytes.
 */
char *
cs_read_delbitmap(Relation rel, BlockNumber start_block, uint32 num_rows)
{
	uint32		bmsize = CS_DELBITMAP_BYTES(num_rows);
	uint32		npages = CS_DELBITMAP_NPAGES(num_rows);
	char	   *bitmap = palloc(bmsize);
	uint32		offset = 0;

	for (uint32 i = 0; i < npages && offset < bmsize; i++)
	{
		Buffer		buf;
		Page		page;
		uint32		chunk;

		buf = ReadBuffer(rel, start_block + i);
		LockBuffer(buf, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buf);

		chunk = Min(bmsize - offset, CS_COLDATA_PER_PAGE);
		memcpy(bitmap + offset, PageGetContents(page), chunk);

		UnlockReleaseBuffer(buf);
		offset += chunk;
	}

	return bitmap;
}

/*
 * Set a bit in the deletion bitmap, handling multi-page bitmaps.
 *
 * start_block is the first page of the bitmap.  row_offset is the
 * row index within the row group.
 */
void
cs_delbitmap_set_bit(Relation rel, BlockNumber start_block, uint32 row_offset)
{
	uint32		byte_offset = row_offset / 8;
	int			bit_offset = row_offset % 8;
	uint32		page_idx = byte_offset / CS_COLDATA_PER_PAGE;
	uint32		page_byte = byte_offset % CS_COLDATA_PER_PAGE;
	Buffer		buf;
	Page		page;
	char	   *bitmap;
	GenericXLogState *state;

	buf = ReadBuffer(rel, start_block + page_idx);
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

	state = GenericXLogStart(rel);
	page = GenericXLogRegisterBuffer(state, buf, 0);
	bitmap = PageGetContents(page);

	bitmap[page_byte] |= (1 << bit_offset);

	GenericXLogFinish(state);
	UnlockReleaseBuffer(buf);
}

/*
 * Check whether a bit is set in the deletion bitmap.
 *
 * start_block is the first page of the bitmap.  row_offset is the
 * row index within the row group.
 */
bool
cs_delbitmap_test_bit(Relation rel, BlockNumber start_block, uint32 row_offset)
{
	uint32		byte_offset = row_offset / 8;
	int			bit_offset = row_offset % 8;
	uint32		page_idx = byte_offset / CS_COLDATA_PER_PAGE;
	uint32		page_byte = byte_offset % CS_COLDATA_PER_PAGE;
	Buffer		buf;
	Page		page;
	char	   *bitmap;
	bool		result;

	buf = ReadBuffer(rel, start_block + page_idx);
	LockBuffer(buf, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buf);
	bitmap = PageGetContents(page);

	result = (bitmap[page_byte] & (1 << bit_offset)) != 0;

	UnlockReleaseBuffer(buf);
	return result;
}

/*
 * Read the free page range list from storage.
 *
 * Returns a palloc'd array of CSFreeRange[meta->cs_freelist_nranges],
 * or NULL if there are no free ranges.
 */
CSFreeRange *
cs_read_freelist(Relation rel, CSMetaPageData *meta)
{
	uint32		nranges = meta->cs_freelist_nranges;
	uint32		data_len;
	char	   *raw;

	if (nranges == 0 || meta->cs_freelist_start == InvalidBlockNumber)
		return NULL;

	data_len = sizeof(CSFreeRange) * nranges;
	raw = cs_read_column_pages(rel, meta->cs_freelist_start,
							   meta->cs_freelist_npages, data_len);
	return (CSFreeRange *) raw;
}

/*
 * Write the free page range list to new pages (always extends).
 *
 * Updates meta->cs_freelist_start/npages/nranges.  The caller must
 * write the metapage after calling this.
 */
void
cs_write_freelist(Relation rel, CSMetaPageData *meta,
				  CSFreeRange *ranges, uint32 nranges)
{
	BlockNumber start = InvalidBlockNumber;
	uint32		npages;

	if (nranges == 0)
	{
		meta->cs_freelist_start = InvalidBlockNumber;
		meta->cs_freelist_npages = 0;
		meta->cs_freelist_nranges = 0;
		return;
	}

	/* Always extend for free list pages (never allocate from free list) */
	npages = cs_write_column_data(rel, &start,
								  (const char *) ranges,
								  sizeof(CSFreeRange) * nranges);

	meta->cs_freelist_start = start;
	meta->cs_freelist_npages = npages;
	meta->cs_freelist_nranges = nranges;
}

/*
 * Allocate npages consecutive blocks from the free list.
 *
 * IMPORTANT: reusing a freed page overwrites its old contents, but
 * concurrent scans capture page references (delta range, row group
 * directory, column chunk locations) from the metapage at scan start
 * and may read those pages long after a compaction has freed them.
 * Allocating from the free list is therefore only safe while holding
 * AccessExclusiveLock on the relation, which proves no such scan
 * exists.  Today the only caller that passes a free list is row-group
 * recompaction (AEL-gated in cs_vacuum_rel); delta compaction and
 * deletion-bitmap allocation always extend the relation instead.
 * Physical reclamation of freed space happens in
 * cs_truncate_trailing_freespace, likewise under conditional AEL.
 *
 * Scans the free range array for a range with enough pages.  If found,
 * removes or shrinks that range and returns the start block.  Returns
 * InvalidBlockNumber if no suitable range exists.
 *
 * Uses best-fit: picks the smallest range that satisfies the request,
 * to minimize fragmentation.
 */
BlockNumber
cs_freelist_alloc(CSFreeRange *ranges, uint32 *nranges, uint32 npages)
{
	int			best = -1;
	BlockNumber result;

	if (ranges == NULL || *nranges == 0 || npages == 0)
		return InvalidBlockNumber;

	for (uint32 i = 0; i < *nranges; i++)
	{
		if (ranges[i].fr_npages >= npages)
		{
			if (best < 0 || ranges[i].fr_npages < ranges[best].fr_npages)
				best = i;
		}
	}

	if (best < 0)
		return InvalidBlockNumber;

	result = ranges[best].fr_start;

	if (ranges[best].fr_npages == npages)
	{
		/* Exact fit -- remove this entry */
		(*nranges)--;
		if ((uint32) best < *nranges)
			memmove(&ranges[best], &ranges[best + 1],
					sizeof(CSFreeRange) * (*nranges - best));
	}
	else
	{
		/* Shrink the range */
		ranges[best].fr_start += npages;
		ranges[best].fr_npages -= npages;
	}

	return result;
}

/*
 * Add a free page range, merging with adjacent ranges.
 *
 * The ranges array is reallocated if needed (tracked via max_ranges).
 */
void
cs_freelist_add(CSFreeRange **ranges, uint32 *nranges, uint32 *max_ranges,
				BlockNumber start, uint32 npages)
{
	uint32		end = start + npages;

	if (npages == 0)
		return;

	/* Try to merge with an existing range */
	for (uint32 i = 0; i < *nranges; i++)
	{
		BlockNumber rend = (*ranges)[i].fr_start + (*ranges)[i].fr_npages;

		if (end == (*ranges)[i].fr_start)
		{
			/* New range is immediately before this one */
			(*ranges)[i].fr_start = start;
			(*ranges)[i].fr_npages += npages;
			return;
		}
		if (start == rend)
		{
			/* New range is immediately after this one */
			(*ranges)[i].fr_npages += npages;

			/* Check if we can merge with the next range too */
			for (uint32 j = 0; j < *nranges; j++)
			{
				if (j != i &&
					(*ranges)[j].fr_start == (*ranges)[i].fr_start + (*ranges)[i].fr_npages)
				{
					(*ranges)[i].fr_npages += (*ranges)[j].fr_npages;
					(*nranges)--;
					if (j < *nranges)
						memmove(&(*ranges)[j], &(*ranges)[j + 1],
								sizeof(CSFreeRange) * (*nranges - j));
					break;
				}
			}
			return;
		}
	}

	/* No merge possible -- add a new entry */
	if (*nranges >= *max_ranges)
	{
		*max_ranges = (*max_ranges > 0) ? *max_ranges * 2 : 64;
		*ranges = repalloc(*ranges, sizeof(CSFreeRange) * *max_ranges);
	}

	(*ranges)[*nranges].fr_start = start;
	(*ranges)[*nranges].fr_npages = npages;
	(*nranges)++;
}
