/*-------------------------------------------------------------------------
 *
 * cs_insert.c
 *	  INSERT operations for columnstore tables.
 *
 * All inserts go into the delta store, which uses standard heap-format
 * pages with PageHeaderData + ItemIdData line pointers + HeapTupleHeader.
 * We use GenericXLog for crash safety.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * contrib/columnstore/cs_insert.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cs_internal.h"
#include "access/generic_xlog.h"
#include "access/heapam.h"
#include "access/heaptoast.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "utils/inval.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "utils/rel.h"

/* Forward declarations */
static HeapTuple cs_delta_form_tuple(Relation rel, TupleTableSlot *slot,
									 CommandId cid, uint32 options);
static void cs_delta_do_insert(Relation rel, TupleTableSlot *slot,
							   HeapTuple tuple);
static Buffer cs_delta_extend(Relation rel);
static void cs_delta_stamp_self_ctid(Page page, OffsetNumber offnum,
									 ItemPointer tid);

/*
 * Extend the delta store by one page and return it exclusively locked.
 *
 * ExtendBufferedRel takes the relation extension lock itself;
 * ReadBufferExtended(P_NEW) must not be used here, as it assumes the caller
 * already holds that lock and concurrent inserters would collide on the same
 * new block.
 *
 * This is also the one place a delta page's block number can grow, so it
 * enforces the invariant that a real delta page never lands at or above the
 * columnar virtual-TID space: a TID there would be decoded by readers as
 * (row_group_id, row_offset) coordinates.  An 8 TB delta store is far past
 * any plausible compaction backlog, so just error.
 */
static Buffer
cs_delta_extend(Relation rel)
{
	Buffer		buf = ExtendBufferedRel(BMR_REL(rel), MAIN_FORKNUM, NULL,
										EB_LOCK_FIRST);

	if (BufferGetBlockNumber(buf) >= CS_COLUMNAR_BLKNO_BASE)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("columnstore delta store cannot grow beyond %u blocks",
						CS_COLUMNAR_BLKNO_BASE)));

	return buf;
}

/*
 * Stamp a freshly placed delta data tuple's t_ctid with its own TID, as
 * heap's RelationPutHeapTuple does so HeapTupleSatisfiesUpdate can tell
 * "deleted" from "updated" (an uninitialized ctid trips its asserts).
 *
 * Tombstones keep their ctid (it carries the target virtual TID), and
 * speculative tuples keep theirs (it holds the token ON CONFLICT inserters
 * wait on, restored to the self pointer at confirmation).
 */
static void
cs_delta_stamp_self_ctid(Page page, OffsetNumber offnum, ItemPointer tid)
{
	HeapTupleHeader placed = (HeapTupleHeader)
		PageGetItem(page, PageGetItemId(page, offnum));

	if (!CS_IS_TOMBSTONE(placed) && !HeapTupleHeaderIsSpeculative(placed))
		placed->t_ctid = *tid;
}

/*
 * Try to insert a tuple into the given delta page.  Returns true if
 * successful, false if the page is full.
 */
static bool
cs_delta_page_insert(Relation rel, Buffer buf, HeapTuple tuple,
					 ItemPointer tid)
{
	Page		page;
	GenericXLogState *state;
	OffsetNumber offnum;
	Size		tupsize;

	tupsize = MAXALIGN(tuple->t_len);
	page = BufferGetPage(buf);

	/*
	 * The caller located this page through an unlocked metapage read, so it
	 * may be stale.  Refuse pages a running compaction has fenced (it has
	 * already collected their contents and will consume them without seeing a
	 * tuple added now), and anything that is not a delta page at all
	 * (concurrent extension can interleave column data pages into the nominal
	 * delta range).  The caller extends instead.
	 */
	if (!CSPageIsDelta(page) || CSDeltaPageIsFenced(page))
		return false;

	if (PageGetFreeSpace(page) < tupsize + sizeof(ItemIdData))
		return false;

	state = GenericXLogStart(rel);
	page = GenericXLogRegisterBuffer(state, buf, 0);

	offnum = PageAddItemExtended(page,
								 tuple->t_data,
								 tuple->t_len,
								 InvalidOffsetNumber,
								 0);

	if (offnum == InvalidOffsetNumber)
	{
		GenericXLogAbort(state);
		return false;
	}

	ItemPointerSet(tid, BufferGetBlockNumber(buf), offnum);
	cs_delta_stamp_self_ctid(page, offnum, tid);

	GenericXLogFinish(state);
	return true;
}

/*
 * Common delta-store insert logic shared by regular and speculative inserts.
 *
 * Forms a heap tuple from the slot, sets up MVCC headers, and handles TOAST.
 * The caller may set additional header flags on the tuple between this
 * function and cs_delta_do_insert().
 *
 * Returns the formed HeapTuple (caller must free it).
 */
static HeapTuple
cs_delta_form_tuple(Relation rel, TupleTableSlot *slot,
					CommandId cid, uint32 options)
{
	TupleDesc	tupdesc = RelationGetDescr(rel);
	HeapTuple	tuple;

	/* Materialize all attributes before forming the heap tuple */
	slot_getallattrs(slot);

	/* Form a heap tuple from the slot */
	slot->tts_tableOid = RelationGetRelid(rel);
	tuple = heap_form_tuple(tupdesc, slot->tts_values, slot->tts_isnull);

	/*
	 * Set up the heap tuple header with xmin/xmax for MVCC.
	 */
	HeapTupleHeaderSetXmin(tuple->t_data, GetCurrentTransactionId());
	HeapTupleHeaderSetCmin(tuple->t_data, cid);
	HeapTupleHeaderSetXmax(tuple->t_data, InvalidTransactionId);
	tuple->t_data->t_infomask |= HEAP_XMAX_INVALID;
	tuple->t_data->t_infomask2 &= ~HEAP_NATTS_MASK;
	tuple->t_data->t_infomask2 |= tupdesc->natts & HEAP_NATTS_MASK;
	tuple->t_tableOid = RelationGetRelid(rel);

	/*
	 * If the relation has a TOAST table, compress and/or move oversized
	 * varlena attributes out-of-line so the tuple fits in a delta page.
	 */
	if (OidIsValid(rel->rd_rel->reltoastrelid))
	{
		HeapTuple	toasted;

		toasted = heap_toast_insert_or_update(rel, tuple, NULL, 0);
		if (toasted != tuple)
		{
			heap_freetuple(tuple);
			tuple = toasted;
		}
	}

	return tuple;
}

/*
 * Insert a formed HeapTuple into the delta store, updating the metapage.
 * The TID is written back to slot->tts_tid.
 */
static void
cs_delta_do_insert(Relation rel, TupleTableSlot *slot, HeapTuple tuple)
{
	CSMetaPageData meta;
	ItemPointerData tid;
	Buffer		buf;
	bool		inserted = false;
	BlockNumber new_blkno = InvalidBlockNumber;

	/* Make sure the relation has a metapage */
	if (RelationGetNumberOfBlocks(rel) == 0)
		cs_init_metapage(rel);

	cs_read_metapage(rel, &meta);

	/*
	 * Try to insert into the last delta page only.  Earlier pages are already
	 * full (or fenced by a running compaction), so scanning backwards wastes
	 * buffer locks.
	 */
	if (meta.cs_delta_nblocks > 0)
	{
		BlockNumber last_blk = meta.cs_delta_start + meta.cs_delta_nblocks - 1;

		buf = ReadBuffer(rel, last_blk);
		LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

		if (cs_delta_page_insert(rel, buf, tuple, &tid))
			inserted = true;

		UnlockReleaseBuffer(buf);
	}

	/*
	 * If the last page was full (or no delta pages exist), extend.
	 */
	if (!inserted)
	{
		Buffer		new_buf;
		Page		new_page;
		GenericXLogState *init_state;

		new_buf = cs_delta_extend(rel);

		/* Initialize the new page */
		init_state = GenericXLogStart(rel);
		new_page = GenericXLogRegisterBuffer(init_state, new_buf,
											 GENERIC_XLOG_FULL_IMAGE);
		cs_delta_page_init(new_page);
		GenericXLogFinish(init_state);

		/* Now insert the tuple */
		if (!cs_delta_page_insert(rel, new_buf, tuple, &tid))
			elog(ERROR, "columnstore: failed to insert tuple into fresh page");

		new_blkno = BufferGetBlockNumber(new_buf);
		UnlockReleaseBuffer(new_buf);
	}

	/*
	 * Single metapage update for both the new page and the row count, merged
	 * into the current on-disk state (never a full write-back of our stale
	 * copy; see cs_metapage_merge_insert).
	 */
	if (cs_metapage_merge_insert(rel, new_blkno, 1))
	{
		/*
		 * The delta just became non-empty, which falsifies the "rows arrive
		 * in row-group order" property cached plans may rely on
		 * (cs_try_build_sort_pathkeys); make them replan.
		 */
		CacheInvalidateRelcache(rel);
	}

	/* Copy TID back to the slot */
	ItemPointerCopy(&tid, &slot->tts_tid);
}

/*
 * Insert a single tuple into the columnstore's delta store.
 */
void
cs_tuple_insert(Relation rel, TupleTableSlot *slot,
				CommandId cid, uint32 options,
				BulkInsertStateData *bistate)
{
	HeapTuple	tuple;

	tuple = cs_delta_form_tuple(rel, slot, cid, options);
	cs_delta_do_insert(rel, slot, tuple);
	heap_freetuple(tuple);
}

/*
 * Speculative insertion into the columnstore's delta store.
 *
 * Like cs_tuple_insert, but marks the tuple as speculative so it can be
 * confirmed or aborted by cs_tuple_complete_speculative.
 */
void
cs_tuple_insert_speculative(Relation rel, TupleTableSlot *slot,
							CommandId cid, uint32 options,
							BulkInsertStateData *bistate, uint32 specToken)
{
	HeapTuple	tuple;

	tuple = cs_delta_form_tuple(rel, slot, cid, options);
	HeapTupleHeaderSetSpeculativeToken(tuple->t_data, specToken);
	cs_delta_do_insert(rel, slot, tuple);
	heap_freetuple(tuple);
}

/*
 * Complete a speculative insertion.
 *
 * If succeeded, clear the speculative token so the tuple becomes a normal
 * committed tuple.  If not succeeded, mark the tuple dead so it is
 * immediately invisible to all transactions.
 */
void
cs_tuple_complete_speculative(Relation rel, TupleTableSlot *slot,
							  uint32 specToken, bool succeeded)
{
	ItemPointer tid = &slot->tts_tid;
	Buffer		buf;
	Page		page;
	OffsetNumber offnum;
	ItemId		lp;
	HeapTupleHeader htup;
	GenericXLogState *state;

	buf = ReadBuffer(rel, ItemPointerGetBlockNumber(tid));
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

	state = GenericXLogStart(rel);
	page = GenericXLogRegisterBuffer(state, buf, 0);

	offnum = ItemPointerGetOffsetNumber(tid);
	if (offnum < FirstOffsetNumber || offnum > PageGetMaxOffsetNumber(page))
		elog(ERROR, "columnstore: speculative TID out of range");

	lp = PageGetItemId(page, offnum);
	if (!ItemIdIsNormal(lp))
		elog(ERROR, "columnstore: speculative TID points to dead item");

	htup = (HeapTupleHeader) PageGetItem(page, lp);

	if (succeeded)
	{
		/*
		 * Clear the speculative token by pointing t_ctid back to the tuple
		 * itself, making it a normal tuple.
		 */
		htup->t_ctid = *tid;
	}
	else
	{
		/*
		 * Mark the tuple dead: set xmin to InvalidTransactionId so it is
		 * immediately invisible to everyone.  This mirrors
		 * heap_abort_speculative(), which kills a failed speculative
		 * insertion the same way; the visibility routines treat the invalid
		 * xid as an aborted insert.  (Heap additionally flags the page
		 * prunable, which has no analogue here -- dead delta tuples are
		 * reclaimed by compaction instead.)
		 */
		HeapTupleHeaderSetXmin(htup, InvalidTransactionId);
		htup->t_ctid = *tid;
	}

	GenericXLogFinish(state);
	UnlockReleaseBuffer(buf);
}

/*
 * Multi-insert: batch inserts into the delta store.
 *
 * Reads the metapage once, fills delta pages sequentially (extending as
 * needed), and writes the metapage once at the end.  All tuples that land
 * on the same page share a single GenericXLog WAL record, which cuts WAL
 * volume from one record per tuple to one record per page.
 */
void
cs_multi_insert(Relation rel, TupleTableSlot **slots, int nslots,
				CommandId cid, uint32 options,
				BulkInsertStateData *bistate)
{
	CSMetaPageData meta;
	TupleDesc	tupdesc = RelationGetDescr(rel);
	TransactionId xid = GetCurrentTransactionId();
	Buffer		cur_buf = InvalidBuffer;
	GenericXLogState *xlog_state = NULL;
	Page		cur_page = NULL;
	HeapTuple  *tuples;
	int			inserted = 0;
	BlockNumber last_new_blkno = InvalidBlockNumber;

	if (nslots <= 0)
		return;

	/* Make sure the relation has a metapage */
	if (RelationGetNumberOfBlocks(rel) == 0)
		cs_init_metapage(rel);

	cs_read_metapage(rel, &meta);

	/*
	 * Form (and if necessary TOAST) all tuples up front, before any page
	 * locks or WAL state are taken: toasting inserts into the TOAST table and
	 * must not run while a delta buffer is locked, and a row wider than a
	 * page can only be stored at all by moving its large attributes out of
	 * line, exactly as the single-row insert does.
	 */
	tuples = palloc(sizeof(HeapTuple) * nslots);
	for (int i = 0; i < nslots; i++)
	{
		HeapTuple	tuple;

		slot_getallattrs(slots[i]);
		slots[i]->tts_tableOid = RelationGetRelid(rel);
		tuple = heap_form_tuple(tupdesc,
								slots[i]->tts_values,
								slots[i]->tts_isnull);

		HeapTupleHeaderSetXmin(tuple->t_data, xid);
		HeapTupleHeaderSetCmin(tuple->t_data, cid);
		HeapTupleHeaderSetXmax(tuple->t_data, InvalidTransactionId);
		tuple->t_data->t_infomask |= HEAP_XMAX_INVALID;
		tuple->t_data->t_infomask2 &= ~HEAP_NATTS_MASK;
		tuple->t_data->t_infomask2 |= tupdesc->natts & HEAP_NATTS_MASK;
		tuple->t_tableOid = RelationGetRelid(rel);

		if (OidIsValid(rel->rd_rel->reltoastrelid))
		{
			HeapTuple	toasted;

			toasted = heap_toast_insert_or_update(rel, tuple, NULL, 0);
			if (toasted != tuple)
			{
				heap_freetuple(tuple);
				tuple = toasted;
			}
		}

		tuples[i] = tuple;
	}

	/* Pin the last existing delta page, if any */
	if (meta.cs_delta_nblocks > 0)
	{
		BlockNumber last_blk = meta.cs_delta_start + meta.cs_delta_nblocks - 1;

		cur_buf = ReadBuffer(rel, last_blk);
		LockBuffer(cur_buf, BUFFER_LOCK_EXCLUSIVE);

		/*
		 * Our metapage view is unlocked and may be stale: the page may be
		 * fenced by a running compaction (about to be consumed; adding tuples
		 * would lose them) or not be a delta page at all.  Fall through to
		 * extending in that case.  See cs_delta_page_insert.
		 */
		if (CSPageIsDelta(BufferGetPage(cur_buf)) &&
			!CSDeltaPageIsFenced(BufferGetPage(cur_buf)))
		{
			xlog_state = GenericXLogStart(rel);
			cur_page = GenericXLogRegisterBuffer(xlog_state, cur_buf, 0);
		}
		else
		{
			UnlockReleaseBuffer(cur_buf);
			cur_buf = InvalidBuffer;
		}
	}

	for (int i = 0; i < nslots; i++)
	{
		HeapTuple	tuple = tuples[i];
		ItemPointerData tid;
		OffsetNumber offnum;
		Size		tupsize;

		tupsize = MAXALIGN(tuple->t_len);

		/* If current page can't fit this tuple, commit and extend */
		if (cur_page == NULL ||
			PageGetFreeSpace(cur_page) < tupsize + sizeof(ItemIdData))
		{
			/* Commit the WAL record for the current page */
			if (xlog_state != NULL)
				GenericXLogFinish(xlog_state);
			if (BufferIsValid(cur_buf))
				UnlockReleaseBuffer(cur_buf);

			/* Allocate and initialize a new delta page */
			cur_buf = cs_delta_extend(rel);

			xlog_state = GenericXLogStart(rel);
			cur_page = GenericXLogRegisterBuffer(xlog_state, cur_buf,
												 GENERIC_XLOG_FULL_IMAGE);
			cs_delta_page_init(cur_page);

			last_new_blkno = BufferGetBlockNumber(cur_buf);
		}

		offnum = PageAddItemExtended(cur_page,
									 tuple->t_data,
									 tuple->t_len,
									 InvalidOffsetNumber,
									 0);
		if (offnum == InvalidOffsetNumber)
			elog(ERROR, "columnstore: failed to insert tuple into delta page");

		ItemPointerSet(&tid, BufferGetBlockNumber(cur_buf), offnum);
		cs_delta_stamp_self_ctid(cur_page, offnum, &tid);
		ItemPointerCopy(&tid, &slots[i]->tts_tid);
		heap_freetuple(tuple);
		inserted++;
	}
	pfree(tuples);

	/* Commit the last page's WAL record */
	if (xlog_state != NULL)
		GenericXLogFinish(xlog_state);
	if (BufferIsValid(cur_buf))
		UnlockReleaseBuffer(cur_buf);

	/*
	 * Single metapage update for the whole batch, merged into the current
	 * on-disk state (see cs_metapage_merge_insert).  Covering the highest new
	 * page covers all of them.
	 */
	if (cs_metapage_merge_insert(rel, last_new_blkno, inserted))
	{
		/* see cs_delta_do_insert */
		CacheInvalidateRelcache(rel);
	}
}

/*
 * Insert a tombstone tuple into the delta store for MVCC columnar deletes.
 *
 * A tombstone is a minimal heap tuple with zero data attributes (natts = 0)
 * whose t_ctid points to the target columnar virtual TID.  The tombstone's
 * xmin records the deleting transaction; visibility is checked via the
 * standard HeapTupleSatisfiesVisibility machinery.  VACUUM materializes
 * committed tombstones into the deletion bitmap and removes them.
 */
void
cs_delta_insert_tombstone(Relation rel, ItemPointer target_tid,
						  uint16 extra_infomask)
{
	HeapTupleHeaderData hdr;
	HeapTupleData tuple;
	CSMetaPageData meta;
	ItemPointerData tid;
	Buffer		buf;
	bool		inserted = false;
	BlockNumber new_blkno = InvalidBlockNumber;

	/* Form a minimal heap tuple header with no data attributes */
	memset(&hdr, 0, SizeofHeapTupleHeader);
	HeapTupleHeaderSetXmin(&hdr, GetCurrentTransactionId());
	HeapTupleHeaderSetCmin(&hdr, GetCurrentCommandId(true));
	HeapTupleHeaderSetXmax(&hdr, InvalidTransactionId);
	hdr.t_infomask |= HEAP_XMAX_INVALID;
	hdr.t_infomask |= extra_infomask;	/* lock-only tombstone bits, if any */
	hdr.t_infomask2 = 0;		/* natts = 0 */
	hdr.t_hoff = SizeofHeapTupleHeader;
	hdr.t_ctid = *target_tid;

	tuple.t_data = &hdr;
	tuple.t_len = SizeofHeapTupleHeader;
	tuple.t_tableOid = RelationGetRelid(rel);
	ItemPointerSetInvalid(&tuple.t_self);

	/* Make sure the relation has a metapage */
	if (RelationGetNumberOfBlocks(rel) == 0)
		cs_init_metapage(rel);

	cs_read_metapage(rel, &meta);

	/* Try inserting into the last delta page */
	if (meta.cs_delta_nblocks > 0)
	{
		BlockNumber last_blk = meta.cs_delta_start + meta.cs_delta_nblocks - 1;

		buf = ReadBuffer(rel, last_blk);
		LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

		if (cs_delta_page_insert(rel, buf, &tuple, &tid))
			inserted = true;

		UnlockReleaseBuffer(buf);
	}

	/* If the last page was full (or no delta pages exist), extend */
	if (!inserted)
	{
		Buffer		new_buf;
		Page		new_page;
		GenericXLogState *init_state;

		new_buf = cs_delta_extend(rel);

		new_blkno = BufferGetBlockNumber(new_buf);

		/* Initialize the new page */
		init_state = GenericXLogStart(rel);
		new_page = GenericXLogRegisterBuffer(init_state, new_buf,
											 GENERIC_XLOG_FULL_IMAGE);
		cs_delta_page_init(new_page);
		GenericXLogFinish(init_state);

		/* Now insert the tombstone */
		if (!cs_delta_page_insert(rel, new_buf, &tuple, &tid))
			elog(ERROR, "columnstore: failed to insert tombstone into fresh page");

		UnlockReleaseBuffer(new_buf);
	}

	/*
	 * Merge the new page (if any) into the metapage.  No cs_total_rows
	 * adjustment: a tombstone is a delete, not a new row.  When the tombstone
	 * fit into an existing page this writes nothing at all.
	 */
	if (cs_metapage_merge_insert(rel, new_blkno, 0))
	{
		/* see cs_delta_do_insert */
		CacheInvalidateRelcache(rel);
	}

	/*
	 * Invalidate the cached visibility info so that index-only scans see the
	 * new tombstone.  The cache stores delta_nblocks which may have changed.
	 */
	if (rel->rd_amcache)
	{
		pfree(rel->rd_amcache);
		rel->rd_amcache = NULL;
	}
}
