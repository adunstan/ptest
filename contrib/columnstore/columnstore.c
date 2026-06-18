/*-------------------------------------------------------------------------
 *
 * columnstore.c
 *	  Columnar storage table access method.
 *
 * This file contains the extension entry point (_PG_init), the table AM
 * handler function, and the static TableAmRoutine definition that wires
 * up all columnstore callbacks.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * contrib/columnstore/columnstore.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cs_internal.h"
#include "access/heapam.h"
#include "access/heaptoast.h"
#include "access/htup_details.h"
#include "access/reloptions.h"
#include "access/multixact.h"
#include "access/tableam.h"
#include "access/transam.h"
#include "access/tsmapi.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/index.h"
#include "catalog/pg_am.h"
#include "catalog/storage.h"
#include "catalog/storage_xlog.h"
#include "executor/executor.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "optimizer/cost.h"
#include "nodes/tidbitmap.h"
#include "storage/bufmgr.h"
#include "storage/procarray.h"
#include "storage/lmgr.h"
#include "storage/read_stream.h"
#include "storage/smgr.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/guc.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/tuplesort.h"

#ifdef USE_LZ4
#include <lz4.h>
#endif

PG_MODULE_MAGIC;

/* Forward declarations */
static const TupleTableSlotOps *cs_slot_callbacks(Relation rel);
static TM_Result cs_delta_delete(Relation rel, ItemPointer tid,
								 CommandId cid, TransactionId xid,
								 bool wait, TM_FailureData *tmfd);
static TM_Result cs_columnar_delete(Relation rel, ItemPointer tid,
									CommandId cid, bool wait,
									TM_FailureData *tmfd);
static TransactionId cs_find_lock_tombstone_for_tid(Relation rel,
													CSMetaPageData *meta,
													ItemPointer target_tid,
													uint16 *lock_infomask);
static TransactionId cs_find_tombstone_for_tid(Relation rel,
											   CSMetaPageData *meta,
											   ItemPointer target_tid,
											   CommandId *tomb_cmin);
static bool cs_has_visible_tombstone(Relation rel, CSMetaPageData *meta,
									 ItemPointer target_tid,
									 Snapshot snapshot);
static void cs_collect_tombstones_for_index(CSIndexFetchData *cscan,
											Relation rel,
											CSMetaPageData *meta,
											Snapshot snapshot);
static TM_Result cs_tuple_delete(Relation rel, ItemPointer tid,
								 CommandId cid, uint32 options,
								 Snapshot snapshot, Snapshot crosscheck,
								 bool wait, TM_FailureData *tmfd);
static TM_Result cs_tuple_update(Relation rel, ItemPointer otid,
								 TupleTableSlot *slot, CommandId cid,
								 uint32 options, Snapshot snapshot,
								 Snapshot crosscheck, bool wait,
								 TM_FailureData *tmfd,
								 LockTupleMode *lockmode,
								 TU_UpdateIndexes *update_indexes);
static TM_Result cs_columnar_lock_check(Relation rel, ItemPointer tid,
										TupleTableSlot *slot,
										LockTupleMode mode,
										LockWaitPolicy wait_policy,
										TM_FailureData *tmfd);
static TM_Result cs_delta_lock_check(Relation rel, ItemPointer tid,
									 TupleTableSlot *slot, CommandId cid,
									 LockTupleMode mode,
									 LockWaitPolicy wait_policy,
									 TM_FailureData *tmfd);
static TM_Result cs_tuple_lock(Relation rel, ItemPointer tid,
							   Snapshot snapshot, TupleTableSlot *slot,
							   CommandId cid, LockTupleMode mode,
							   LockWaitPolicy wait_policy, uint8 flags,
							   TM_FailureData *tmfd);
static void cs_fetch_columnar_row(Relation rel, BlockNumber rg_catalog_block,
								  uint32 row_offset, TupleDesc tupdesc,
								  Datum *values, bool *isnull);
static bool cs_fetch_row_version(Relation rel, ItemPointer tid,
								 Snapshot snapshot, TupleTableSlot *slot);
static bool cs_tuple_tid_valid(TableScanDesc scan, ItemPointer tid);
static void cs_get_latest_tid(TableScanDesc scan, ItemPointer tid);
static bool cs_tuple_satisfies_snapshot(Relation rel, TupleTableSlot *slot,
										Snapshot snapshot);
static TransactionId cs_index_delete_tuples(Relation rel,
											TM_IndexDeleteOp *delstate);
static IndexFetchTableData *cs_index_fetch_begin(Relation rel, uint32 flags);
static void cs_index_fetch_reset(IndexFetchTableData *data);
static void cs_index_fetch_end(IndexFetchTableData *data);
static bool cs_index_fetch_tuple(IndexFetchTableData *scan, ItemPointer tid,
								 Snapshot snapshot, TupleTableSlot *slot,
								 bool *call_again, bool *all_dead);
static CSRGCacheEntry *cs_index_cache_lookup(CSIndexFetchData *cscan,
											 uint32 rg_id);
static CSRGCacheEntry *cs_index_cache_evict(CSIndexFetchData *cscan);
static void cs_relation_set_new_filelocator(Relation rel,
											const RelFileLocator *newrlocator,
											char persistence,
											TransactionId *freezeXid,
											MultiXactId *minmulti);
static void cs_relation_nontransactional_truncate(Relation rel);
static void cs_relation_copy_data(Relation rel,
								  const RelFileLocator *newrlocator);
static void cs_relation_copy_for_cluster(Relation OldTable, Relation NewTable,
										 Relation OldIndex, bool use_sort,
										 TransactionId OldestXmin,
										 Snapshot snapshot,
										 TransactionId *xid_cutoff,
										 MultiXactId *multi_cutoff,
										 double *num_tuples,
										 double *tups_vacuumed,
										 double *tups_recently_dead);
static bool cs_scan_analyze_next_block(TableScanDesc scan, ReadStream *stream);
static bool cs_scan_analyze_next_tuple(TableScanDesc scan,
									   double *liverows, double *deadrows,
									   TupleTableSlot *slot);
static double cs_index_build_range_scan(Relation table_rel,
										Relation index_rel,
										IndexInfo *index_info,
										bool allow_sync,
										bool anyvisible,
										bool progress,
										BlockNumber start_blockno,
										BlockNumber numblocks,
										IndexBuildCallback callback,
										void *callback_state,
										TableScanDesc scan);
static void cs_index_validate_scan(Relation table_rel, Relation index_rel,
								   IndexInfo *index_info,
								   Snapshot snapshot,
								   ValidateIndexState *state);
static uint64 cs_relation_size(Relation rel, ForkNumber forkNumber);
static bool cs_relation_needs_toast_table(Relation rel);
static Oid	cs_relation_toast_am(Relation rel);
static void cs_relation_estimate_size(Relation rel, int32 *attr_widths,
									  BlockNumber *pages, double *tuples,
									  double *allvisfrac);
static bool cs_scan_bitmap_next_tuple(TableScanDesc scan,
									  TupleTableSlot *slot, bool *recheck,
									  uint64 *lossy_pages,
									  uint64 *exact_pages);
static bool cs_scan_sample_next_block(TableScanDesc scan,
									  SampleScanState *scanstate);
static bool cs_scan_sample_next_tuple(TableScanDesc scan,
									  SampleScanState *scanstate,
									  TupleTableSlot *slot);
static Size cs_parallelscan_estimate(Relation rel);
static Size cs_parallelscan_initialize(Relation rel,
									   ParallelTableScanDesc pscan);
static void cs_parallelscan_reinitialize(Relation rel,
										 ParallelTableScanDesc pscan);

/* ----------------------------------------------------------------
 * Slot callbacks
 * ----------------------------------------------------------------
 */
static const TupleTableSlotOps *
cs_slot_callbacks(Relation rel)
{
	return &TTSOpsColumnStore;
}

/* ----------------------------------------------------------------
 * Tuple deletion and update
 * ----------------------------------------------------------------
 */

/*
 * Delete a delta-store tuple by marking its xmax.
 *
 * Checks for concurrent modification and acquires a heavyweight tuple lock
 * for conflict detection.  If 'wait' is true, blocks on a conflicting
 * in-progress transaction; otherwise returns TM_WouldBlock.
 */
static TM_Result
cs_delta_delete(Relation rel, ItemPointer tid, CommandId cid,
				TransactionId xid, bool wait, TM_FailureData *tmfd)
{
	Buffer		buf;
	Page		page;
	ItemId		itemid;
	HeapTupleHeader htup;
	GenericXLogState *state;
	TransactionId xmax;
	CommandId	cmax;
	bool		iscombo;

	for (;;)
	{
		buf = ReadBuffer(rel, ItemPointerGetBlockNumber(tid));
		LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

		page = BufferGetPage(buf);

		/* a foreign page interleaved into the range is not delta content */
		if (!CSPageIsDelta(page))
		{
			UnlockReleaseBuffer(buf);
			return TM_Invisible;
		}

		itemid = PageGetItemId(page, ItemPointerGetOffsetNumber(tid));

		if (!ItemIdIsNormal(itemid))
		{
			UnlockReleaseBuffer(buf);
			return TM_Invisible;
		}

		htup = (HeapTupleHeader) PageGetItem(page, itemid);
		xmax = HeapTupleHeaderGetRawXmax(htup);

		if (!TransactionIdIsValid(xmax) ||
			(htup->t_infomask & HEAP_XMAX_INVALID))
		{
			/* No deleter -- row is live, proceed */
			break;
		}

		if (htup->t_infomask & HEAP_XMAX_LOCK_ONLY)
		{
			/*
			 * xmax is a row lock (cs_tuple_lock), not a delete.  Our own
			 * lock, or one whose transaction has ended, is simply
			 * overwritten; a live lock held by someone else blocks us until
			 * that transaction ends (we also contend on the heavyweight tuple
			 * lock below, but the locker holds that until commit, so waiting
			 * on the xid is equivalent and gives the deadlock detector the
			 * same edge).
			 */
			if (TransactionIdIsCurrentTransactionId(xmax))
				break;
			if (TransactionIdIsInProgress(xmax))
			{
				UnlockReleaseBuffer(buf);

				if (!wait)
					return TM_WouldBlock;

				XactLockTableWait(xmax, rel, tid, XLTW_Lock);
				CHECK_FOR_INTERRUPTS();
				continue;
			}
			/* locker committed or aborted: the row is live either way */
			break;
		}

		if (TransactionIdIsCurrentTransactionId(xmax))
		{
			tmfd->cmax = HeapTupleHeaderGetCmax(htup);
			tmfd->xmax = xmax;
			ItemPointerCopy(tid, &tmfd->ctid);
			UnlockReleaseBuffer(buf);
			return TM_SelfModified;
		}

		/*
		 * Check in-progress before committed: xact.c records the commit in
		 * pg_xact before clearing the proc array entry, so the reverse order
		 * could misread a just-committed deleter as aborted.
		 */
		if (TransactionIdIsInProgress(xmax))
		{
			/*
			 * Wait on the deleting transaction's xid (not the tuple lock,
			 * which the deleter does not hold while idle in its transaction),
			 * so we sleep instead of spinning and the deadlock detector sees
			 * the wait edge.
			 */
			UnlockReleaseBuffer(buf);

			if (!wait)
				return TM_WouldBlock;

			XactLockTableWait(xmax, rel, tid, XLTW_Delete);
			CHECK_FOR_INTERRUPTS();
			continue;			/* re-check after the deleter finishes */
		}

		if (TransactionIdDidCommit(xmax))
		{
			/*
			 * Columnstore rows have no update chains (UPDATE is delete +
			 * insert at an unrelated TID), so a committed deleter means the
			 * row is gone, not superseded: report TM_Deleted, as heap does
			 * when ctid would equal the tuple's own TID.  TM_Updated here
			 * would send the executor chasing a "next version" that does not
			 * exist.
			 */
			tmfd->xmax = xmax;
			tmfd->cmax = InvalidCommandId;
			ItemPointerCopy(tid, &tmfd->ctid);
			UnlockReleaseBuffer(buf);
			return TM_Deleted;
		}

		/*
		 * The deleter aborted (or crashed): the row is live.  Proceed; the
		 * new xmax simply overwrites the aborted one.
		 */
		break;
	}

	/*
	 * Acquire heavyweight lock before modifying.  Release buffer lock first
	 * to avoid lock ordering issues, then re-read and re-check.
	 */
	UnlockReleaseBuffer(buf);

	if (wait)
		LockTuple(rel, tid, AccessExclusiveLock);
	else
	{
		if (!ConditionalLockTuple(rel, tid, AccessExclusiveLock, false))
			return TM_WouldBlock;
	}

	/* Re-read and re-check under heavyweight lock */
	buf = ReadBuffer(rel, ItemPointerGetBlockNumber(tid));
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
	page = BufferGetPage(buf);

	/* see above */
	if (!CSPageIsDelta(page))
	{
		UnlockReleaseBuffer(buf);
		UnlockTuple(rel, tid, AccessExclusiveLock);
		return TM_Invisible;
	}

	itemid = PageGetItemId(page, ItemPointerGetOffsetNumber(tid));

	if (!ItemIdIsNormal(itemid))
	{
		UnlockReleaseBuffer(buf);
		UnlockTuple(rel, tid, AccessExclusiveLock);
		return TM_Invisible;
	}

	/*
	 * If a running compaction has fenced this page, it has already collected
	 * the page's rows and will consume it: an xmax stamped now would be
	 * silently discarded.  Wait for the compaction to either consume the page
	 * (the row has moved to columnar under a new TID we cannot chase; report
	 * it deleted) or discard its batch (retry).
	 */
	if (CSDeltaPageIsFenced(page))
	{
		CSMetaPageData fmeta;
		BlockNumber blkno = ItemPointerGetBlockNumber(tid);

		UnlockReleaseBuffer(buf);
		UnlockTuple(rel, tid, AccessExclusiveLock);

		if (!wait)
			return TM_WouldBlock;

		LockPage(rel, CS_METAPAGE_BLKNO, ShareLock);
		UnlockPage(rel, CS_METAPAGE_BLKNO, ShareLock);
		CHECK_FOR_INTERRUPTS();

		cs_read_metapage(rel, &fmeta);
		if (fmeta.cs_delta_nblocks == 0 ||
			blkno < fmeta.cs_delta_start ||
			blkno >= fmeta.cs_delta_start + fmeta.cs_delta_nblocks)
		{
			tmfd->xmax = InvalidTransactionId;
			tmfd->cmax = InvalidCommandId;
			ItemPointerCopy(tid, &tmfd->ctid);
			return TM_Deleted;
		}
		return cs_delta_delete(rel, tid, cid, xid, wait, tmfd);
	}

	htup = (HeapTupleHeader) PageGetItem(page, itemid);
	xmax = HeapTupleHeaderGetRawXmax(htup);

	if (TransactionIdIsValid(xmax) &&
		!(htup->t_infomask & HEAP_XMAX_INVALID))
	{
		if (htup->t_infomask & HEAP_XMAX_LOCK_ONLY)
		{
			/* see the first lock-only check above */
			if (!TransactionIdIsCurrentTransactionId(xmax) &&
				TransactionIdIsInProgress(xmax))
			{
				UnlockReleaseBuffer(buf);
				UnlockTuple(rel, tid, AccessExclusiveLock);

				if (!wait)
					return TM_WouldBlock;

				XactLockTableWait(xmax, rel, tid, XLTW_Lock);
				CHECK_FOR_INTERRUPTS();
				return cs_delta_delete(rel, tid, cid, xid, wait, tmfd);
			}
			/* own, committed, or aborted lock: overwrite below */
		}
		else

			/*
			 * Another deleter slipped in between our check and taking the
			 * tuple lock.  Same classification as above; an aborted xmax
			 * falls through and is overwritten.
			 */
		if (TransactionIdIsCurrentTransactionId(xmax))
		{
			tmfd->cmax = HeapTupleHeaderGetCmax(htup);
			tmfd->xmax = xmax;
			ItemPointerCopy(tid, &tmfd->ctid);
			UnlockReleaseBuffer(buf);
			UnlockTuple(rel, tid, AccessExclusiveLock);
			return TM_SelfModified;
		}
		else if (TransactionIdIsInProgress(xmax))
		{
			UnlockReleaseBuffer(buf);
			UnlockTuple(rel, tid, AccessExclusiveLock);

			if (!wait)
				return TM_WouldBlock;

			XactLockTableWait(xmax, rel, tid, XLTW_Delete);
			CHECK_FOR_INTERRUPTS();
			return cs_delta_delete(rel, tid, cid, xid, wait, tmfd);
		}
		else if (TransactionIdDidCommit(xmax))
		{
			/* no update chains; see the first committed-xmax site */
			tmfd->xmax = xmax;
			tmfd->cmax = InvalidCommandId;
			ItemPointerCopy(tid, &tmfd->ctid);
			UnlockReleaseBuffer(buf);
			UnlockTuple(rel, tid, AccessExclusiveLock);
			return TM_Deleted;
		}
	}

	/*
	 * If this transaction also inserted the row, cmin and cmax share the
	 * single t_cid field and a combo command id must be used, or an open
	 * cursor positioned between the insert and this delete would misread its
	 * cmin and skip the row.  Computed before the WAL operation starts
	 * because it can allocate (and so fail).
	 */
	cmax = cid;
	HeapTupleHeaderAdjustCmax(htup, &cmax, &iscombo);

	/* Set xmax via GenericXLog */
	state = GenericXLogStart(rel);
	page = GenericXLogRegisterBuffer(state, buf, 0);
	itemid = PageGetItemId(page, ItemPointerGetOffsetNumber(tid));
	htup = (HeapTupleHeader) PageGetItem(page, itemid);

	htup->t_infomask &= ~(HEAP_XMAX_COMMITTED | HEAP_XMAX_INVALID |
						  HEAP_XMAX_IS_MULTI | HEAP_XMAX_LOCK_ONLY |
						  HEAP_XMAX_KEYSHR_LOCK | HEAP_XMAX_EXCL_LOCK);
	HeapTupleHeaderSetXmax(htup, xid);
	HeapTupleHeaderSetCmax(htup, cmax, iscombo);

	GenericXLogFinish(state);
	UnlockReleaseBuffer(buf);

	/* Release the heavyweight tuple lock */
	UnlockTuple(rel, tid, AccessExclusiveLock);

	return TM_Ok;
}

/*
 * Scan delta pages for a tombstone targeting the given virtual TID.
 *
 * Returns the xmin of the tombstone if found, InvalidTransactionId otherwise.
 * Skips tombstones with aborted xmin.
 */
static TransactionId
cs_find_tombstone_for_tid(Relation rel, CSMetaPageData *meta,
						  ItemPointer target_tid, CommandId *tomb_cmin)
{
	BlockNumber blkno;
	BlockNumber end_blk;

	if (meta->cs_delta_nblocks == 0)
		return InvalidTransactionId;

	end_blk = meta->cs_delta_start + meta->cs_delta_nblocks;
	for (blkno = meta->cs_delta_start; blkno < end_blk; blkno++)
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
			TransactionId xmin;

			itemid = PageGetItemId(page, off);
			if (!ItemIdIsNormal(itemid))
				continue;

			htup = (HeapTupleHeader) PageGetItem(page, itemid);
			if (!CS_IS_TOMBSTONE(htup))
				continue;

			/* row locks are not deletes */
			if (CS_TOMBSTONE_IS_LOCK_ONLY(htup))
				continue;

			/* Check if this tombstone targets our TID */
			if (!ItemPointerEquals(&htup->t_ctid, target_tid))
				continue;

			xmin = HeapTupleHeaderGetRawXmin(htup);

			/* Skip aborted tombstones */
			if (TransactionIdDidAbort(xmin))
				continue;

			/*
			 * Report the deleting command id too: a caller hitting its own
			 * tombstone needs it to distinguish "deleted earlier in this
			 * command" from "deleted by an earlier command".
			 */
			if (tomb_cmin)
				*tomb_cmin = HeapTupleHeaderGetRawCommandId(htup);

			UnlockReleaseBuffer(buf);
			return xmin;
		}

		UnlockReleaseBuffer(buf);
	}

	return InvalidTransactionId;
}

/*
 * Check whether a visible tombstone exists for the given columnar virtual TID.
 *
 * Scans delta pages for tombstone tuples targeting target_tid and checks
 * their visibility against the given snapshot.  Returns true if at least
 * one visible tombstone is found, meaning the row should be treated as
 * deleted from this snapshot's perspective.
 */
static bool
cs_has_visible_tombstone(Relation rel, CSMetaPageData *meta,
						 ItemPointer target_tid, Snapshot snapshot)
{
	BlockNumber blkno;
	BlockNumber end_blk;

	if (meta->cs_delta_nblocks == 0)
		return false;

	end_blk = meta->cs_delta_start + meta->cs_delta_nblocks;
	for (blkno = meta->cs_delta_start; blkno < end_blk; blkno++)
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

			if (!ItemPointerEquals(&htup->t_ctid, target_tid))
				continue;

			/* Check visibility against snapshot */
			tuple.t_data = htup;
			tuple.t_len = ItemIdGetLength(itemid);
			tuple.t_tableOid = RelationGetRelid(rel);
			ItemPointerSet(&tuple.t_self, blkno, off);

			if (cs_delta_satisfies_visibility(&tuple, snapshot))
			{
				UnlockReleaseBuffer(buf);
				return true;
			}
		}

		UnlockReleaseBuffer(buf);
	}

	return false;
}

/*
 * Scan delta pages for a live lock-only tombstone on the given virtual
 * TID held by another transaction.
 *
 * Returns the locker's xid (for the caller to wait on) and, via
 * *lock_infomask, the lock-mode bits of that tombstone.  Spent lock
 * tombstones (transaction ended) and our own are ignored; our own lock
 * never conflicts with our own operations.
 */
static TransactionId
cs_find_lock_tombstone_for_tid(Relation rel, CSMetaPageData *meta,
							   ItemPointer target_tid, uint16 *lock_infomask)
{
	BlockNumber blkno;
	BlockNumber end_blk;

	if (meta->cs_delta_nblocks == 0)
		return InvalidTransactionId;

	end_blk = meta->cs_delta_start + meta->cs_delta_nblocks;
	for (blkno = meta->cs_delta_start; blkno < end_blk; blkno++)
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
			TransactionId xmin;

			itemid = PageGetItemId(page, off);
			if (!ItemIdIsNormal(itemid))
				continue;

			htup = (HeapTupleHeader) PageGetItem(page, itemid);
			if (!CS_IS_TOMBSTONE(htup) || !CS_TOMBSTONE_IS_LOCK_ONLY(htup))
				continue;

			if (!ItemPointerEquals(&htup->t_ctid, target_tid))
				continue;

			xmin = HeapTupleHeaderGetRawXmin(htup);
			if (!TransactionIdIsValid(xmin) ||
				TransactionIdIsCurrentTransactionId(xmin))
				continue;

			/* in-progress before committed; see xact.c on the ordering */
			if (TransactionIdIsInProgress(xmin))
			{
				if (lock_infomask)
					*lock_infomask = htup->t_infomask;
				UnlockReleaseBuffer(buf);
				return xmin;
			}

			/* transaction ended: the lock is spent */
		}

		UnlockReleaseBuffer(buf);
	}

	return InvalidTransactionId;
}

/*
 * Collect all visible tombstones into an index fetch descriptor.
 *
 * This mirrors cs_collect_tombstones() in cs_scan.c but operates on the
 * CSIndexFetchData rather than CScanDesc.
 */
static void
cs_collect_tombstones_for_index(CSIndexFetchData *cscan, Relation rel,
								CSMetaPageData *meta, Snapshot snapshot)
{
	/* arrays must outlive any per-row context we may be called from */
	MemoryContext oldcxt = MemoryContextSwitchTo(cscan->rg_cache_parent);

	BlockNumber blkno;
	BlockNumber end_blk;
	ItemPointerData *tids = NULL;
	int			count = 0;
	int			capacity = 0;

	cscan->tombstones_collected = true;
	cscan->tombstone_count = 0;
	cscan->tombstone_tids = NULL;

	if (meta->cs_delta_nblocks == 0)
	{
		MemoryContextSwitchTo(oldcxt);
		return;
	}

	end_blk = meta->cs_delta_start + meta->cs_delta_nblocks;

	for (blkno = meta->cs_delta_start; blkno < end_blk; blkno++)
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

			tuple.t_data = htup;
			tuple.t_len = ItemIdGetLength(itemid);
			tuple.t_tableOid = RelationGetRelid(rel);
			ItemPointerSet(&tuple.t_self, blkno, off);

			if (!cs_delta_satisfies_visibility(&tuple, snapshot))
				continue;

			if (count >= capacity)
			{
				if (capacity == 0)
				{
					capacity = 32;
					tids = palloc_array(ItemPointerData, capacity);
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
		qsort(tids, count, sizeof(ItemPointerData),
			  (int (*) (const void *, const void *)) ItemPointerCompare);
	}

	cscan->tombstone_tids = tids;
	cscan->tombstone_count = count;
	MemoryContextSwitchTo(oldcxt);
}

/*
 * Delete a columnar row by inserting a tombstone into the delta store.
 *
 * The tombstone is a minimal heap tuple with MVCC headers; its t_ctid
 * points to the target columnar virtual TID.  Scans check tombstone
 * visibility via HeapTupleSatisfiesVisibility, and VACUUM materializes
 * committed tombstones into the deletion bitmap.
 *
 * Acquires a heavyweight tuple lock for conflict detection.  If 'wait' is
 * true, blocks on a conflicting locker; otherwise returns TM_WouldBlock.
 */
static TM_Result
cs_columnar_delete(Relation rel, ItemPointer tid, CommandId cid,
				   bool wait, TM_FailureData *tmfd)
{
	BlockNumber blkno = ItemPointerGetBlockNumber(tid);
	OffsetNumber offnum = ItemPointerGetOffsetNumber(tid);
	uint32		rg_id;
	uint32		row_offset;
	CSMetaPageData meta;
	CSRowGroupDesc *rg_desc;
	Size		rg_size;
	BlockNumber rg_catalog_block;
	TransactionId tombstone_xmin;
	CommandId	tomb_cmin = InvalidCommandId;

	/* Decode virtual TID to (rg_id, row_offset) */
	rg_id = (blkno - CS_COLUMNAR_BLKNO_BASE) / CS_VIRTUAL_BLOCKS_PER_RG;
	row_offset = ((blkno - CS_COLUMNAR_BLKNO_BASE) % CS_VIRTUAL_BLOCKS_PER_RG)
		* CS_ROWS_PER_VIRTUAL_BLOCK + (offnum - 1);

retry:
	cs_read_metapage(rel, &meta);

	if (rg_id >= meta.cs_nrowgroups)
		return TM_Invisible;

	/* Read directory to find catalog block for this row group */
	{
		BlockNumber *rgdir = cs_read_rgdir(rel, &meta);

		if (rgdir == NULL)
			return TM_Invisible;
		rg_catalog_block = rgdir[rg_id];
		pfree(rgdir);
	}
	if (rg_catalog_block == InvalidBlockNumber)
		return TM_Invisible;

	/* Read the row group catalog entry */
	rg_size = CSRowGroupDescSize(meta.cs_natts);
	rg_desc = palloc(rg_size);
	cs_read_rowgroup_catalog(rel, rg_catalog_block, rg_desc, meta.cs_natts);

	if (row_offset >= rg_desc->rg_num_rows)
	{
		pfree(rg_desc);
		return TM_Invisible;
	}

	/*
	 * If already deleted, report TM_Deleted (no update chains; see
	 * cs_delta_delete)
	 */
	if (rg_desc->rg_delbitmap_block != InvalidBlockNumber)
	{
		if (cs_delbitmap_test_bit(rel, rg_desc->rg_delbitmap_block,
								  row_offset))
		{
			pfree(rg_desc);
			ItemPointerCopy(tid, &tmfd->ctid);
			tmfd->xmax = InvalidTransactionId;
			tmfd->cmax = InvalidCommandId;
			return TM_Deleted;
		}
	}

	pfree(rg_desc);

	/* Acquire heavyweight tuple lock for conflict detection */
	if (wait)
		LockTuple(rel, tid, AccessExclusiveLock);
	else
	{
		if (!ConditionalLockTuple(rel, tid, AccessExclusiveLock, false))
			return TM_WouldBlock;
	}

	/*
	 * Re-read metapage and re-check bitmap after lock — VACUUM may have
	 * materialized tombstones and set bitmap bits while we waited.
	 */
	cs_read_metapage(rel, &meta);
	rg_desc = palloc(CSRowGroupDescSize(meta.cs_natts));
	cs_read_rowgroup_catalog(rel, rg_catalog_block, rg_desc, meta.cs_natts);

	if (rg_desc->rg_delbitmap_block != InvalidBlockNumber)
	{
		if (cs_delbitmap_test_bit(rel, rg_desc->rg_delbitmap_block,
								  row_offset))
		{
			pfree(rg_desc);
			UnlockTuple(rel, tid, AccessExclusiveLock);
			ItemPointerCopy(tid, &tmfd->ctid);
			tmfd->xmax = InvalidTransactionId;
			tmfd->cmax = InvalidCommandId;
			return TM_Deleted;
		}
	}
	pfree(rg_desc);

	/* Check for existing tombstone targeting this TID */
	tombstone_xmin = cs_find_tombstone_for_tid(rel, &meta, tid, &tomb_cmin);

	if (TransactionIdIsValid(tombstone_xmin))
	{
		UnlockTuple(rel, tid, AccessExclusiveLock);

		if (TransactionIdIsCurrentTransactionId(tombstone_xmin))
		{
			ItemPointerCopy(tid, &tmfd->ctid);
			tmfd->xmax = tombstone_xmin;

			/*
			 * The deleting command id lets ExecDelete tell a legitimate
			 * duplicate hit (same row joined twice, earlier command) from
			 * self-modification within the current command, which must raise
			 * an error rather than be skipped.
			 */
			tmfd->cmax = tomb_cmin;
			return TM_SelfModified;
		}

		if (TransactionIdDidCommit(tombstone_xmin))
		{
			ItemPointerCopy(tid, &tmfd->ctid);
			tmfd->xmax = tombstone_xmin;
			tmfd->cmax = InvalidCommandId;
			return TM_Deleted;
		}

		/*
		 * Tombstone from in-progress transaction.  This shouldn't normally
		 * happen because we hold the tuple lock, but handle it by waiting for
		 * the other transaction and retrying.
		 */
		if (!wait)
			return TM_WouldBlock;

		XactLockTableWait(tombstone_xmin, rel, tid, XLTW_Delete);
		goto retry;
	}

	/*
	 * A live row lock (lock-only tombstone) from another transaction blocks
	 * the delete until that transaction ends, like a lock-only xmax does on a
	 * delta row.
	 */
	{
		TransactionId locker;

		locker = cs_find_lock_tombstone_for_tid(rel, &meta, tid, NULL);
		if (TransactionIdIsValid(locker))
		{
			UnlockTuple(rel, tid, AccessExclusiveLock);

			if (!wait)
				return TM_WouldBlock;

			XactLockTableWait(locker, rel, tid, XLTW_Delete);
			CHECK_FOR_INTERRUPTS();
			goto retry;
		}
	}

	/* Insert tombstone into the delta store */
	cs_delta_insert_tombstone(rel, tid, 0);

	UnlockTuple(rel, tid, AccessExclusiveLock);

	return TM_Ok;
}

static TM_Result
cs_tuple_delete(Relation rel, ItemPointer tid, CommandId cid,
				uint32 options, Snapshot snapshot, Snapshot crosscheck,
				bool wait, TM_FailureData *tmfd)
{
	BlockNumber blkno = ItemPointerGetBlockNumber(tid);

	/*
	 * Crosscheck snapshot (heap_delete equivalent): a referential action
	 * cascading under REPEATABLE READ must fail with a serialization error
	 * when the row is not visible to the transaction snapshot, even though
	 * its latest version is deletable.
	 */
	if (crosscheck != InvalidSnapshot)
	{
		TupleTableSlot *cslot = table_slot_create(rel, NULL);
		bool		visible;

		visible = cs_fetch_row_version(rel, tid, crosscheck, cslot);
		ExecDropSingleTupleTableSlot(cslot);
		if (!visible)
		{
			ItemPointerCopy(tid, &tmfd->ctid);
			tmfd->xmax = InvalidTransactionId;
			tmfd->cmax = InvalidCommandId;
			return TM_Updated;
		}
	}

	if (blkno >= CS_COLUMNAR_BLKNO_BASE)
		return cs_columnar_delete(rel, tid, cid, wait, tmfd);
	else
		return cs_delta_delete(rel, tid, cid, GetCurrentTransactionId(),
							   wait, tmfd);
}

static TM_Result
cs_tuple_update(Relation rel, ItemPointer otid, TupleTableSlot *slot,
				CommandId cid, uint32 options, Snapshot snapshot,
				Snapshot crosscheck, bool wait, TM_FailureData *tmfd,
				LockTupleMode *lockmode, TU_UpdateIndexes *update_indexes)
{
	TM_Result	result;

	/* DELETE the old row */
	result = cs_tuple_delete(rel, otid, cid, options, snapshot, crosscheck,
							 wait, tmfd);
	if (result != TM_Ok)
		return result;

	/* INSERT the new row into the delta store */
	cs_tuple_insert(rel, slot, cid, 0, NULL);

	*update_indexes = TU_All;
	return TM_Ok;
}

/*
 * Heavyweight lock mode for each tuple lock mode; the same mapping heap
 * uses (tupleLockExtraInfo), so the conflict matrix matches heap's tuple
 * lock semantics: key-share vs no-key-exclusive are compatible, share vs
 * exclusive conflict, and so on.
 */
static const LOCKMODE cs_tuplelock_hwlock[] = {
	[LockTupleKeyShare] = AccessShareLock,
	[LockTupleShare] = RowShareLock,
	[LockTupleNoKeyExclusive] = ExclusiveLock,
	[LockTupleExclusive] = AccessExclusiveLock,
};

/*
 * Lock a tuple, for SELECT FOR UPDATE/SHARE, EvalPlanQual, and the RI
 * trigger machinery.
 *
 * The lock is recorded durably in row state, as heap does: a delta row
 * gets a lock-only xmax stamped (HEAP_XMAX_LOCK_ONLY plus mode bits),
 * and a columnar row -- physically immutable -- gets a lock-only
 * tombstone in the delta carrying the locker's xid and mode.  Writers
 * find the record and wait on the locker's xid; the heavyweight tuple
 * lock below is only held transiently to serialize check-and-record,
 * mirroring heap_lock_tuple.  With a single xmax and no multixact
 * support, a delta row records one live locker at a time: concurrent
 * holders of compatible modes (e.g. two FOR KEY SHARE) serialize where
 * heap's would not (see README).
 *
 * There are no update chains (UPDATE is delete + insert at an unrelated
 * TID), so tmfd->traversed is always false and a committed deleter
 * yields TM_Deleted, never a followable TM_Updated.
 */
static TM_Result
cs_tuple_lock(Relation rel, ItemPointer tid, Snapshot snapshot,
			  TupleTableSlot *slot, CommandId cid, LockTupleMode mode,
			  LockWaitPolicy wait_policy, uint8 flags,
			  TM_FailureData *tmfd)
{
	ItemPointerData tid_copy;
	BlockNumber blkno = ItemPointerGetBlockNumber(tid);
	LOCKMODE	hwlock = cs_tuplelock_hwlock[mode];
	TM_Result	result;

	/*
	 * Callers may pass a pointer into the very slot we are asked to fill
	 * (ri_LockPKTuple passes &slot->tts_tid), and our fetch paths clear the
	 * slot before storing into it; work from a copy.
	 */
	tid_copy = *tid;
	tid = &tid_copy;

	tmfd->traversed = false;

	/* Acquire the heavyweight tuple lock per the caller's wait policy */
	switch (wait_policy)
	{
		case LockWaitBlock:
			LockTuple(rel, tid, hwlock);
			break;
		case LockWaitSkip:
			if (!ConditionalLockTuple(rel, tid, hwlock, false))
				return TM_WouldBlock;
			break;
		case LockWaitError:
			if (!ConditionalLockTuple(rel, tid, hwlock, false))
				ereport(ERROR,
						(errcode(ERRCODE_LOCK_NOT_AVAILABLE),
						 errmsg("could not obtain lock on row in relation \"%s\"",
								RelationGetRelationName(rel))));
			break;
	}

	if (blkno >= CS_COLUMNAR_BLKNO_BASE)
		result = cs_columnar_lock_check(rel, tid, slot, mode, wait_policy,
										tmfd);
	else
		result = cs_delta_lock_check(rel, tid, slot, cid, mode, wait_policy,
									 tmfd);

	/*
	 * The heavyweight lock only serializes the check-and-record sequence (the
	 * lock manager forbids holding tuple locks to transaction end); what
	 * persists is the record it protected: a lock-only xmax on a delta row,
	 * or a lock-only tombstone for a columnar row.
	 */
	UnlockTuple(rel, tid, hwlock);

	return result;
}

/*
 * Check the current state of a columnar row after the tuple lock has
 * been acquired, and load it into the slot on success.
 */
static TM_Result
cs_columnar_lock_check(Relation rel, ItemPointer tid, TupleTableSlot *slot,
					   LockTupleMode mode, LockWaitPolicy wait_policy,
					   TM_FailureData *tmfd)
{
	/* lock-only tombstone infomask bits per mode; see cs_delta_lock_check */
	static const uint16 lockonly_infomask[] = {
		[LockTupleKeyShare] = HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_KEYSHR_LOCK,
		[LockTupleShare] = HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_SHR_LOCK,
		[LockTupleNoKeyExclusive] = HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_EXCL_LOCK,
		[LockTupleExclusive] = HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_EXCL_LOCK,
	};

	for (;;)
	{
		CSMetaPageData meta;
		BlockNumber blkno = ItemPointerGetBlockNumber(tid);
		OffsetNumber offnum = ItemPointerGetOffsetNumber(tid);
		uint32		rg_id;
		uint32		row_offset;
		BlockNumber rg_catalog_block;
		CSRowGroupDesc *rg_desc;
		TransactionId tombstone_xmin;
		CommandId	tomb_cmin = InvalidCommandId;

		CHECK_FOR_INTERRUPTS();

		rg_id = (blkno - CS_COLUMNAR_BLKNO_BASE) / CS_VIRTUAL_BLOCKS_PER_RG;
		row_offset = ((blkno - CS_COLUMNAR_BLKNO_BASE) % CS_VIRTUAL_BLOCKS_PER_RG)
			* CS_ROWS_PER_VIRTUAL_BLOCK + (offnum - 1);

		cs_read_metapage(rel, &meta);

		if (rg_id >= meta.cs_nrowgroups)
			return TM_Invisible;

		{
			BlockNumber *rgdir = cs_read_rgdir(rel, &meta);

			if (rgdir == NULL)
				return TM_Invisible;
			rg_catalog_block = rgdir[rg_id];
			pfree(rgdir);
		}
		if (rg_catalog_block == InvalidBlockNumber)
			return TM_Invisible;

		rg_desc = palloc(CSRowGroupDescSize(meta.cs_natts));
		cs_read_rowgroup_catalog(rel, rg_catalog_block, rg_desc,
								 meta.cs_natts);

		if (row_offset >= rg_desc->rg_num_rows)
		{
			pfree(rg_desc);
			return TM_Invisible;
		}

		/* Materialized delete? */
		if (rg_desc->rg_delbitmap_block != InvalidBlockNumber &&
			cs_delbitmap_test_bit(rel, rg_desc->rg_delbitmap_block,
								  row_offset))
		{
			pfree(rg_desc);
			ItemPointerCopy(tid, &tmfd->ctid);
			tmfd->xmax = InvalidTransactionId;
			tmfd->cmax = InvalidCommandId;
			return TM_Deleted;
		}
		pfree(rg_desc);

		/* Pending (tombstoned) delete? */
		tombstone_xmin = cs_find_tombstone_for_tid(rel, &meta, tid,
												   &tomb_cmin);
		if (TransactionIdIsValid(tombstone_xmin))
		{
			if (TransactionIdIsCurrentTransactionId(tombstone_xmin))
			{
				ItemPointerCopy(tid, &tmfd->ctid);
				tmfd->xmax = tombstone_xmin;
				tmfd->cmax = tomb_cmin;
				return TM_SelfModified;
			}

			if (TransactionIdDidCommit(tombstone_xmin))
			{
				ItemPointerCopy(tid, &tmfd->ctid);
				tmfd->xmax = tombstone_xmin;
				tmfd->cmax = InvalidCommandId;
				return TM_Deleted;
			}

			/*
			 * In-progress deleter.  It inserted its tombstone before we
			 * acquired the tuple lock (deleters do not hold the tuple lock
			 * while their transaction idles).  Wait it out per the caller's
			 * policy, then re-check.
			 */
			if (TransactionIdIsInProgress(tombstone_xmin))
			{
				if (wait_policy == LockWaitSkip)
					return TM_WouldBlock;
				if (wait_policy == LockWaitError)
					ereport(ERROR,
							(errcode(ERRCODE_LOCK_NOT_AVAILABLE),
							 errmsg("could not obtain lock on row in relation \"%s\"",
									RelationGetRelationName(rel))));
				XactLockTableWait(tombstone_xmin, rel, tid, XLTW_Lock);
				continue;
			}

			/* aborted deleter: the row is live, fall through */
		}

		/*
		 * Another transaction's live row lock blocks us unless both locks are
		 * key-shares (the only compatible pairing we can recognize: an EXCL
		 * bit may mean either exclusive mode, so it is treated as conflicting
		 * with everything but is compatible with nothing).
		 */
		{
			uint16		their_mask = 0;
			TransactionId locker;

			locker = cs_find_lock_tombstone_for_tid(rel, &meta, tid,
													&their_mask);
			if (TransactionIdIsValid(locker) &&
				!(mode == LockTupleKeyShare &&
				  (their_mask & HEAP_XMAX_SHR_LOCK) == HEAP_XMAX_KEYSHR_LOCK))
			{
				if (wait_policy == LockWaitSkip)
					return TM_WouldBlock;
				if (wait_policy == LockWaitError)
					ereport(ERROR,
							(errcode(ERRCODE_LOCK_NOT_AVAILABLE),
							 errmsg("could not obtain lock on row in relation \"%s\"",
									RelationGetRelationName(rel))));
				XactLockTableWait(locker, rel, tid, XLTW_Lock);
				continue;
			}
		}

		/*
		 * Record the lock as a lock-only tombstone so it survives the
		 * transient heavyweight lock: deleters and conflicting lockers find
		 * it and wait on our xid, and compaction retains the page holding it
		 * until we finish.
		 */
		cs_delta_insert_tombstone(rel, tid, lockonly_infomask[mode]);

		/*
		 * Row is live.  Load it via SnapshotAny: visibility against the
		 * caller's snapshot is the executor's business (e.g. ON CONFLICT DO
		 * UPDATE locks rows its snapshot cannot see and then raises the
		 * serialization error itself); the lock layer reports the current
		 * version, exactly as heap_lock_tuple does.
		 */
		if (!cs_fetch_row_version(rel, tid, SnapshotAny, slot))
			return TM_Invisible;

		return TM_Ok;
	}
}

/*
 * Check the current state of a delta-store row after the tuple lock has
 * been acquired, and load it into the slot on success.
 *
 * Delta tuples are heap-format, so HeapTupleSatisfiesUpdate provides
 * the classification (without hint-bit maintenance: see the
 * NoHintBitsBuffer note at the call site).
 */
static TM_Result
cs_delta_lock_check(Relation rel, ItemPointer tid, TupleTableSlot *slot,
					CommandId cid, LockTupleMode mode,
					LockWaitPolicy wait_policy, TM_FailureData *tmfd)
{
	/* lock-only xmax infomask bits per lock mode, as heap sets them */
	static const uint16 lockonly_infomask[] = {
		[LockTupleKeyShare] = HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_KEYSHR_LOCK,
		[LockTupleShare] = HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_SHR_LOCK,
		[LockTupleNoKeyExclusive] = HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_EXCL_LOCK,
		[LockTupleExclusive] = HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_EXCL_LOCK,
	};

	for (;;)
	{
		Buffer		buf;
		Page		page;
		ItemId		itemid;
		HeapTupleData tuple;
		TM_Result	htsu;
		TransactionId xmax;
		GenericXLogState *state;
		HeapTupleHeader htup;

		CHECK_FOR_INTERRUPTS();

		buf = ReadBuffer(rel, ItemPointerGetBlockNumber(tid));
		LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
		page = BufferGetPage(buf);

		/* a foreign page interleaved into the range is not delta content */
		if (!CSPageIsDelta(page))
		{
			UnlockReleaseBuffer(buf);
			return TM_Invisible;
		}

		/*
		 * A fenced page is being moved to columnar by a running compaction;
		 * an xmax stamped now would be discarded with the page.  Wait for the
		 * compaction and re-evaluate (the row either moved -- gone from this
		 * TID -- or the batch was discarded). See the same dance in
		 * cs_delta_delete.
		 */
		if (CSDeltaPageIsFenced(page))
		{
			CSMetaPageData fmeta;
			BlockNumber blkno = ItemPointerGetBlockNumber(tid);

			UnlockReleaseBuffer(buf);

			if (wait_policy == LockWaitSkip)
				return TM_WouldBlock;
			if (wait_policy == LockWaitError)
				ereport(ERROR,
						(errcode(ERRCODE_LOCK_NOT_AVAILABLE),
						 errmsg("could not obtain lock on row in relation \"%s\"",
								RelationGetRelationName(rel))));

			LockPage(rel, CS_METAPAGE_BLKNO, ShareLock);
			UnlockPage(rel, CS_METAPAGE_BLKNO, ShareLock);

			cs_read_metapage(rel, &fmeta);
			if (fmeta.cs_delta_nblocks == 0 ||
				blkno < fmeta.cs_delta_start ||
				blkno >= fmeta.cs_delta_start + fmeta.cs_delta_nblocks)
			{
				tmfd->xmax = InvalidTransactionId;
				tmfd->cmax = InvalidCommandId;
				ItemPointerCopy(tid, &tmfd->ctid);
				return TM_Deleted;
			}
			continue;
		}

		itemid = PageGetItemId(page, ItemPointerGetOffsetNumber(tid));
		if (!ItemIdIsNormal(itemid))
		{
			UnlockReleaseBuffer(buf);
			return TM_Invisible;
		}

		tuple.t_data = (HeapTupleHeader) PageGetItem(page, itemid);
		tuple.t_len = ItemIdGetLength(itemid);
		tuple.t_tableOid = RelationGetRelid(rel);
		ItemPointerCopy(tid, &tuple.t_self);

		/*
		 * NoHintBitsBuffer: delta pages are modified exclusively through
		 * generic WAL, and the generic rmgr has no masking function, so an
		 * unlogged hint-bit write here would make subsequent WAL records for
		 * this page fail consistency checking on replay (and recovery itself
		 * with wal_consistency_checking enabled).
		 */
		htsu = HeapTupleSatisfiesUpdate(&tuple, cid, NoHintBitsBuffer);
		xmax = HeapTupleHeaderGetRawXmax(tuple.t_data);

		switch (htsu)
		{
			case TM_Ok:

				/*
				 * Stamp a lock-only xmax so that compaction's tuple
				 * classifier sees the row as pinned and will not move it to
				 * columnar (which would detach it from this lock's TID).  A
				 * previous lock whose transaction has ended is overwritten;
				 * t_cid is left alone so a tuple we inserted ourselves keeps
				 * its cmin for cursor visibility.  With a single xmax and no
				 * multixact support, only one live locker can be recorded:
				 * SatisfiesUpdate reports any other live locker as
				 * TM_BeingModified and we wait, so compatible lock modes
				 * serialize more than heap's would (documented in the
				 * README).
				 */
				state = GenericXLogStart(rel);
				page = GenericXLogRegisterBuffer(state, buf, 0);
				itemid = PageGetItemId(page,
									   ItemPointerGetOffsetNumber(tid));
				htup = (HeapTupleHeader) PageGetItem(page, itemid);
				htup->t_infomask &= ~(HEAP_XMAX_COMMITTED |
									  HEAP_XMAX_INVALID |
									  HEAP_XMAX_IS_MULTI |
									  HEAP_XMAX_LOCK_ONLY |
									  HEAP_XMAX_KEYSHR_LOCK |
									  HEAP_XMAX_EXCL_LOCK);
				htup->t_infomask |= lockonly_infomask[mode];
				HeapTupleHeaderSetXmax(htup, GetCurrentTransactionId());
				GenericXLogFinish(state);
				UnlockReleaseBuffer(buf);

				/* see cs_columnar_lock_check on SnapshotAny */
				if (!cs_fetch_row_version(rel, tid, SnapshotAny, slot))
					return TM_Invisible;
				return TM_Ok;

			case TM_SelfModified:
				tmfd->cmax = HeapTupleHeaderGetCmax(tuple.t_data);
				tmfd->xmax = xmax;
				ItemPointerCopy(tid, &tmfd->ctid);
				UnlockReleaseBuffer(buf);
				return TM_SelfModified;

			case TM_Invisible:
				UnlockReleaseBuffer(buf);
				return TM_Invisible;

			case TM_BeingModified:
				UnlockReleaseBuffer(buf);

				if (wait_policy == LockWaitSkip)
					return TM_WouldBlock;
				if (wait_policy == LockWaitError)
					ereport(ERROR,
							(errcode(ERRCODE_LOCK_NOT_AVAILABLE),
							 errmsg("could not obtain lock on row in relation \"%s\"",
									RelationGetRelationName(rel))));
				XactLockTableWait(xmax, rel, tid, XLTW_Lock);
				continue;

			case TM_Updated:
			case TM_Deleted:
				/* no update chains; see cs_delta_delete */
				tmfd->xmax = xmax;
				tmfd->cmax = InvalidCommandId;
				ItemPointerCopy(tid, &tmfd->ctid);
				UnlockReleaseBuffer(buf);
				return TM_Deleted;

			default:
				UnlockReleaseBuffer(buf);
				elog(ERROR, "unexpected HeapTupleSatisfiesUpdate result: %d",
					 (int) htsu);
		}
	}
}

/* ----------------------------------------------------------------
 * Tuple visibility callbacks
 * ----------------------------------------------------------------
 */
/*
 * Fetch a single row from a columnar row group.
 *
 * Uses cs_load_rowgroup_into + cs_ensure_column_loaded to handle all
 * encoding layers (NI64, FOR, DICT, base compression).  The loaded
 * column cache is freed before returning.
 */
static void
cs_fetch_columnar_row(Relation rel, BlockNumber rg_catalog_block,
					  uint32 row_offset, TupleDesc tupdesc,
					  Datum *values, bool *isnull)
{
	int			natts = tupdesc->natts;
	CSColumnCache cache;
	char		ni64_buf[CS_NI64_BUF_SIZE];

	memset(&cache, 0, sizeof(cache));

	if (!cs_load_rowgroup_into(&cache, rel, rg_catalog_block, natts))
	{
		/* Should not happen if caller verified the row group exists */
		for (int col = 0; col < natts; col++)
		{
			values[col] = (Datum) 0;
			isnull[col] = true;
		}
		return;
	}

	for (int col = 0; col < natts; col++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, col);

		cs_ensure_column_loaded(&cache, rel, col);

		/* Extract value from the loaded column */
		if (cache.col_dict[col])
		{
			CSDictColumn *dict = cache.col_dict[col];
			uint32		idx;

			switch (dict->index_width)
			{
				case 1:
					idx = ((uint8 *) dict->index_data)[row_offset];
					break;
				case 2:
					idx = cs_read_u16((const char *) dict->index_data + (Size) (row_offset) * sizeof(uint16));
					break;
				default:
					idx = cs_read_u32((const char *) dict->index_data + (Size) (row_offset) * sizeof(uint32));
					break;
			}

			if (dict->has_null && idx == dict->dict_count)
			{
				isnull[col] = true;
				values[col] = (Datum) 0;
			}
			else
			{
				isnull[col] = false;
				values[col] = datumCopy(dict->dict_values[idx],
										attr->attbyval, attr->attlen);
			}
		}
		else if (cache.col_values[col] != NULL)
		{
			/* varlen path: values point into raw_data buffer */
			values[col] = datumCopy(cache.col_values[col][row_offset],
									attr->attbyval, attr->attlen);
			isnull[col] = cache.col_nulls[col][row_offset];
		}
		else
		{
			/* Fixed-len by-val: direct access from raw_data */
			if (cache.col_null_bitmap[col] &&
				CS_ISNULL(cache.col_null_bitmap[col], row_offset))
			{
				isnull[col] = true;
				values[col] = (Datum) 0;
			}
			else if (cache.col_ni64_buf[col] != NULL)
			{
				/* NI64: dscale 0 (whole numbers) is a valid encoding too */
				int64		ival = ((int64 *) cache.col_raw_data[col])[row_offset];

				isnull[col] = false;
				values[col] = datumCopy(
										cs_int64_to_numeric_buf(ival,
																(int) cache.col_ni64_dscale[col],
																ni64_buf),
										false, -1);
			}
			else
			{
				isnull[col] = false;
				values[col] = cs_fetch_att(cache.col_raw_data[col] +
										   (Size) row_offset * attr->attlen,
										   true, attr->attlen);
			}
		}
	}

	cs_column_cache_free(&cache, natts);
}

static bool
cs_fetch_row_version(Relation rel, ItemPointer tid,
					 Snapshot snapshot, TupleTableSlot *slot)
{
	BlockNumber blkno = ItemPointerGetBlockNumber(tid);
	TupleDesc	tupdesc = RelationGetDescr(rel);

	ExecClearTuple(slot);

	if (blkno >= CS_COLUMNAR_BLKNO_BASE)
	{
		/* Columnar row: decode virtual TID and read column data */
		OffsetNumber offnum = ItemPointerGetOffsetNumber(tid);
		uint32		rg_id;
		uint32		row_offset;
		CSMetaPageData meta;
		CSRowGroupDesc *rg_desc;
		Size		rg_size;
		int			natts;
		BlockNumber rg_catalog_block;

		rg_id = (blkno - CS_COLUMNAR_BLKNO_BASE) / CS_VIRTUAL_BLOCKS_PER_RG;
		row_offset = ((blkno - CS_COLUMNAR_BLKNO_BASE) %
					  CS_VIRTUAL_BLOCKS_PER_RG)
			* CS_ROWS_PER_VIRTUAL_BLOCK + (offnum - 1);

		cs_read_metapage(rel, &meta);

		if (rg_id >= meta.cs_nrowgroups)
			return false;

		/* Read directory to find catalog block for this row group */
		{
			BlockNumber *rgdir = cs_read_rgdir(rel, &meta);

			if (rgdir == NULL)
				return false;
			rg_catalog_block = rgdir[rg_id];
			pfree(rgdir);
		}
		if (rg_catalog_block == InvalidBlockNumber)
			return false;

		natts = tupdesc->natts;
		rg_size = CSRowGroupDescSize(natts);
		rg_desc = palloc(rg_size);
		cs_read_rowgroup_catalog(rel, rg_catalog_block, rg_desc, natts);

		if (row_offset >= rg_desc->rg_num_rows)
		{
			pfree(rg_desc);
			return false;
		}

		/* Check deletion bitmap */
		if (rg_desc->rg_delbitmap_block != InvalidBlockNumber)
		{
			if (cs_delbitmap_test_bit(rel, rg_desc->rg_delbitmap_block,
									  row_offset))
			{
				pfree(rg_desc);
				return false;
			}
		}

		pfree(rg_desc);

		/*
		 * Check for visible tombstones (pending MVCC deletes).  SnapshotAny
		 * callers (AFTER ROW trigger and RI refetches) must see the row
		 * regardless of any delete, exactly as the delta branch below exempts
		 * SnapshotAny from its visibility test.
		 */
		if (snapshot->snapshot_type != SNAPSHOT_ANY &&
			cs_has_visible_tombstone(rel, &meta, tid, snapshot))
			return false;

		/* Fetch all columns for this row via the column cache */
		cs_fetch_columnar_row(rel, rg_catalog_block, row_offset,
							  tupdesc, slot->tts_values, slot->tts_isnull);

		ExecStoreVirtualTuple(slot);
		ItemPointerCopy(tid, &slot->tts_tid);
		slot->tts_tableOid = RelationGetRelid(rel);

		return true;
	}
	else
	{
		/* Delta store: read the page and check visibility */
		Buffer		buf;
		Page		page;
		ItemId		itemid;
		OffsetNumber offnum = ItemPointerGetOffsetNumber(tid);
		HeapTupleData tuple;

		/*
		 * Reject TIDs that cannot reference a delta page at all: the
		 * metapage, anything past EOF (a fabricated WHERE ctid = ... probe).
		 * TIDs below the current delta range are deliberately allowed
		 * through: an index entry may reference a page a compaction has
		 * consumed, whose contents remain intact and readable until the page
		 * is reused -- and reuse requires AccessExclusiveLock, taken only
		 * after indexes are rebuilt (see CS_META_PENDING_REINDEX).  The
		 * page-identity check below screens out everything that is not (or no
		 * longer) a delta page.
		 */
		if (blkno == CS_METAPAGE_BLKNO ||
			blkno >= RelationGetNumberOfBlocks(rel))
			return false;

		buf = ReadBuffer(rel, blkno);
		LockBuffer(buf, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buf);

		/* a foreign page interleaved into the range is not delta content */
		if (!CSPageIsDelta(page) ||
			offnum < FirstOffsetNumber ||
			offnum > PageGetMaxOffsetNumber(page))
		{
			UnlockReleaseBuffer(buf);
			return false;
		}

		itemid = PageGetItemId(page, offnum);
		if (!ItemIdIsNormal(itemid))
		{
			UnlockReleaseBuffer(buf);
			return false;
		}

		tuple.t_data = (HeapTupleHeader) PageGetItem(page, itemid);
		tuple.t_len = ItemIdGetLength(itemid);
		tuple.t_tableOid = RelationGetRelid(rel);
		ItemPointerCopy(tid, &tuple.t_self);

		/* Skip tombstone tuples — they are not data rows */
		if (CS_IS_TOMBSTONE(tuple.t_data))
		{
			UnlockReleaseBuffer(buf);
			return false;
		}

		if (snapshot != SnapshotAny &&
			!cs_delta_satisfies_visibility(&tuple, snapshot))
		{
			UnlockReleaseBuffer(buf);
			return false;
		}

		heap_deform_tuple(&tuple, slot->tts_tupleDescriptor,
						  slot->tts_values, slot->tts_isnull);
		ExecStoreVirtualTuple(slot);
		slot->tts_tid = tuple.t_self;
		slot->tts_tableOid = tuple.t_tableOid;

		UnlockReleaseBuffer(buf);
		return true;
	}
}

static bool
cs_tuple_tid_valid(TableScanDesc scan, ItemPointer tid)
{
	CScanDesc	cscan = (CScanDesc) scan;
	BlockNumber blkno = ItemPointerGetBlockNumber(tid);

	/* Columnar TIDs must point at an existing row group */
	if (blkno >= CS_COLUMNAR_BLKNO_BASE)
	{
		uint32		rg_id = (blkno - CS_COLUMNAR_BLKNO_BASE) /
			CS_VIRTUAL_BLOCKS_PER_RG;

		return rg_id < cscan->nrowgroups;
	}

	/*
	 * Delta TIDs must point into the delta page range; anything else
	 * (metapage, row-group catalog, column data) is not a row.
	 */
	return cscan->delta_nblocks > 0 &&
		blkno >= cscan->delta_start &&
		blkno < cscan->delta_start + cscan->delta_nblocks;
}

static void
cs_get_latest_tid(TableScanDesc scan, ItemPointer tid)
{
	/* no-op: columnstore doesn't do HOT chains */
}

static bool
cs_tuple_satisfies_snapshot(Relation rel, TupleTableSlot *slot,
							Snapshot snapshot)
{
	TupleTableSlot *testslot;
	bool		visible;

	/*
	 * Re-fetch the row at the slot's TID under the caller's snapshot.  The
	 * fetch path applies the delta-store MVCC test or, for columnar rows, the
	 * deletion-bitmap and pending-tombstone tests, which is exactly the
	 * verdict callers (ON CONFLICT DO UPDATE, EvalPlanQual) need. Always
	 * returning "visible" here would let an ON CONFLICT update modify a row
	 * its snapshot cannot see, instead of raising the serialization failure
	 * REPEATABLE READ requires.
	 */
	testslot = table_slot_create(rel, NULL);
	visible = cs_fetch_row_version(rel, &slot->tts_tid, snapshot, testslot);
	ExecDropSingleTupleTableSlot(testslot);

	return visible;
}

static TransactionId
cs_index_delete_tuples(Relation rel, TM_IndexDeleteOp *delstate)
{
	/* No bottom-up index deletion support yet */
	return InvalidTransactionId;
}

/* ----------------------------------------------------------------
 * Multi-RG index cache operations
 * ----------------------------------------------------------------
 */

/* Search cache for rg_id, return entry or NULL */
static CSRGCacheEntry *
cs_index_cache_lookup(CSIndexFetchData *cscan, uint32 rg_id)
{
	for (int i = 0; i < cscan->rg_cache_size; i++)
	{
		if (cscan->rg_cache[i].rg_id == rg_id)
			return &cscan->rg_cache[i];
	}
	return NULL;
}

/* Find LRU or empty slot, free its column data, return it */
static CSRGCacheEntry *
cs_index_cache_evict(CSIndexFetchData *cscan)
{
	CSRGCacheEntry *victim;
	uint32		min_lru;

	Assert(cscan->rg_cache_size > 0);
	victim = &cscan->rg_cache[0];
	min_lru = victim->lru_counter;

	for (int i = 0; i < cscan->rg_cache_size; i++)
	{
		if (cscan->rg_cache[i].rg_id == UINT32_MAX)
			return &cscan->rg_cache[i]; /* empty slot */

		if (cscan->rg_cache[i].lru_counter < min_lru)
		{
			min_lru = cscan->rg_cache[i].lru_counter;
			victim = &cscan->rg_cache[i];
		}
	}

	/* Evict: free column data */
	cs_column_cache_free(&victim->cache, cscan->natts);
	if (victim->cache.cur_rg_desc)
	{
		pfree(victim->cache.cur_rg_desc);
		victim->cache.cur_rg_desc = NULL;
	}
	victim->rg_id = UINT32_MAX;
	return victim;
}

/* ----------------------------------------------------------------
 * Index fetch callbacks
 * ----------------------------------------------------------------
 */
static IndexFetchTableData *
cs_index_fetch_begin(Relation rel, uint32 flags)
{
	CSIndexFetchData *cscan = palloc0(sizeof(CSIndexFetchData));

	cscan->base.rel = rel;
	cscan->delta_buf = InvalidBuffer;
	cscan->meta_initialized = false;
	cscan->lru_clock = 0;
	cscan->rg_cache = NULL;
	cscan->rg_cache_size = 0;
	cscan->rg_cache_parent = CurrentMemoryContext;

	return &cscan->base;
}

static void
cs_index_fetch_reset(IndexFetchTableData *data)
{
	CSIndexFetchData *cscan = (CSIndexFetchData *) data;

	if (BufferIsValid(cscan->delta_buf))
	{
		ReleaseBuffer(cscan->delta_buf);
		cscan->delta_buf = InvalidBuffer;
	}

	/*
	 * Preserve the LRU row-group cache across rescans.  The executor calls
	 * reset on every rescan (e.g. inner side of a nested loop), but the
	 * decompressed row groups are still valid and reusable.  The cache is
	 * freed in cs_index_fetch_end.
	 */
}

static void
cs_index_fetch_end(IndexFetchTableData *data)
{
	CSIndexFetchData *cscan = (CSIndexFetchData *) data;

	cs_index_fetch_reset(data);

	/*
	 * Free decompressed data and pointer arrays in each cache entry.
	 * cs_column_cache_free leaves every per-column field NULL on both of its
	 * paths, so the context delete is all that remains afterwards.
	 */
	for (int i = 0; i < cscan->rg_cache_size; i++)
	{
		CSRGCacheEntry *entry = &cscan->rg_cache[i];
		CSColumnCache *cache = &entry->cache;

		if (entry->rg_id != UINT32_MAX)
			cs_column_cache_free(cache, cscan->natts);

		if (cache->cc_cxt)
		{
			MemoryContextDelete(cache->cc_cxt);
			cache->cc_cxt = NULL;
		}
	}

	if (cscan->rg_cache)
		pfree(cscan->rg_cache);
	if (cscan->rg_catalog_blocks)
		pfree(cscan->rg_catalog_blocks);
	if (cscan->tombstone_tids)
		pfree(cscan->tombstone_tids);

	pfree(data);
}

static bool
cs_index_fetch_tuple(IndexFetchTableData *scan, ItemPointer tid,
					 Snapshot snapshot, TupleTableSlot *slot,
					 bool *call_again, bool *all_dead)
{
	CSIndexFetchData *cscan = (CSIndexFetchData *) scan;
	Relation	rel = scan->rel;
	BlockNumber blkno = ItemPointerGetBlockNumber(tid);
	TupleDesc	tupdesc = RelationGetDescr(rel);

	if (call_again)
		*call_again = false;

	ExecClearTuple(slot);

	if (blkno >= CS_COLUMNAR_BLKNO_BASE)
	{
		/*
		 * Columnar row: decode virtual TID, use multi-RG LRU cache.
		 */
		OffsetNumber offnum = ItemPointerGetOffsetNumber(tid);
		uint32		rg_id;
		uint32		row_offset;
		CSRGCacheEntry *entry;
		CSColumnCache *cache;
		CSTupleTableSlot *csslot;
		int			cache_size;

		rg_id = (blkno - CS_COLUMNAR_BLKNO_BASE) / CS_VIRTUAL_BLOCKS_PER_RG;
		row_offset = ((blkno - CS_COLUMNAR_BLKNO_BASE) % CS_VIRTUAL_BLOCKS_PER_RG)
			* CS_ROWS_PER_VIRTUAL_BLOCK + (offnum - 1);

		/* Initialize metadata and allocate cache on first columnar lookup */
		if (!cscan->meta_initialized)
		{
			CSMetaPageData meta;

			/*
			 * First-fetch initialization may run inside a short-lived per-row
			 * context; everything fetch-scoped must live in the context
			 * captured at fetch-begin.
			 */
			MemoryContext oldcxt =
				MemoryContextSwitchTo(cscan->rg_cache_parent);

			cs_read_metapage(rel, &meta);
			cscan->natts = tupdesc->natts;
			cscan->nrowgroups = meta.cs_nrowgroups;
			cscan->rg_catalog_blocks = cs_read_rgdir(rel, &meta);

			/* Size cache to fit the table, capped at CS_INDEX_CACHE_MAX */
			cache_size = Min(meta.cs_nrowgroups, CS_INDEX_CACHE_MAX);
			cache_size = Max(cache_size, 1);
			cscan->rg_cache = palloc0(sizeof(CSRGCacheEntry) * cache_size);
			cscan->rg_cache_size = cache_size;
			for (int i = 0; i < cache_size; i++)
				cscan->rg_cache[i].rg_id = UINT32_MAX;

			cscan->meta_initialized = true;

			/* Collect tombstones for MVCC columnar delete visibility */
			cs_collect_tombstones_for_index(cscan, rel, &meta, snapshot);

			MemoryContextSwitchTo(oldcxt);
		}

		/* Look up or load the row group via LRU cache */
		entry = cs_index_cache_lookup(cscan, rg_id);
		if (!entry)
		{
			if (rg_id >= cscan->nrowgroups ||
				cscan->rg_catalog_blocks == NULL ||
				cscan->rg_catalog_blocks[rg_id] == InvalidBlockNumber)
			{
				if (all_dead)
					*all_dead = false;
				return false;
			}

			entry = cs_index_cache_evict(cscan);
			if (entry->cache.cc_cxt == NULL)
				entry->cache.cc_cxt =
					AllocSetContextCreate(cscan->rg_cache_parent,
										  "columnstore index rowgroup cache",
										  ALLOCSET_DEFAULT_SIZES);
			if (!cs_load_rowgroup_into(&entry->cache, rel,
									   cscan->rg_catalog_blocks[rg_id],
									   cscan->natts))
			{
				if (all_dead)
					*all_dead = false;
				return false;
			}
			entry->rg_id = rg_id;
		}
		entry->lru_counter = ++cscan->lru_clock;
		cache = &entry->cache;

		/* Bounds check */
		if (row_offset >= cache->cur_rg_desc->rg_num_rows)
		{
			if (all_dead)
				*all_dead = false;
			return false;
		}

		/* Check deletion bitmap */
		if (cache->cur_delbitmap)
		{
			if (CS_ISDELETED(cache->cur_delbitmap, row_offset))
			{
				if (all_dead)
					*all_dead = true;
				return false;
			}
		}

		/*
		 * Check for pending MVCC deletes (tombstones).
		 *
		 * SnapshotDirty callers (uniqueness checks) get the real dirty
		 * contract: a row whose delete is still in progress is returned as
		 * visible with snapshot->xmax announcing the deleter, so
		 * _bt_check_unique waits for it instead of treating the row as
		 * already gone (which would let a conflicting insert succeed and
		 * leave two live rows if the delete then aborts).
		 *
		 * SnapshotAny callers (trigger/RI refetch) see the row no matter
		 * what.  Everyone else consults the tombstones collected under the
		 * scan snapshot.
		 */
		if (snapshot->snapshot_type == SNAPSHOT_DIRTY)
		{
			CSMetaPageData tomb_meta;
			TransactionId tomb_xmin;

			snapshot->xmin = InvalidTransactionId;
			snapshot->xmax = InvalidTransactionId;
			snapshot->speculativeToken = 0;

			cs_read_metapage(rel, &tomb_meta);
			tomb_xmin = cs_find_tombstone_for_tid(rel, &tomb_meta, tid, NULL);
			if (TransactionIdIsValid(tomb_xmin))
			{
				if (TransactionIdIsCurrentTransactionId(tomb_xmin))
				{
					/* deleted by our own transaction */
					if (all_dead)
						*all_dead = false;
					return false;
				}
				else if (TransactionIdIsInProgress(tomb_xmin))
				{
					/* pending delete: return the row, announce deleter */
					snapshot->xmax = tomb_xmin;
				}
				else if (TransactionIdDidCommit(tomb_xmin))
				{
					if (all_dead)
						*all_dead = false;
					return false;
				}
				/* else: aborted delete, row is live */
			}
		}
		else if (snapshot->snapshot_type != SNAPSHOT_ANY &&
				 cscan->tombstone_count > 0 &&
				 cs_tombstone_lookup(cscan->tombstone_tids,
									 cscan->tombstone_count, tid))
		{
			if (all_dead)
				*all_dead = false;
			return false;
		}

		/*
		 * Set up the slot for lazy column loading.  The actual column
		 * decompression happens in tts_columnstore_getsomeattrs when the
		 * executor accesses the slot values.
		 */
		csslot = (CSTupleTableSlot *) slot;
		csslot->cs_scan = NULL;
		csslot->cs_rel = rel;
		csslot->cs_colcache = cache;
		csslot->cs_row = row_offset;
		csslot->cs_is_columnar = true;

		if (all_dead)
			*all_dead = false;

		slot->tts_nvalid = 0;
		slot->tts_flags &= ~TTS_FLAG_EMPTY;
		slot->tts_tid = *tid;
		slot->tts_tableOid = RelationGetRelid(rel);

		return true;
	}
	else
	{
		/*
		 * Delta store row: read the page and check visibility.
		 */
		Page		page;
		ItemId		itemid;
		HeapTupleData tuple;
		bool		visible;

		OffsetNumber offnum = ItemPointerGetOffsetNumber(tid);

		/*
		 * Reject TIDs that cannot reference a delta page at all; see
		 * cs_fetch_row_version.  Index TIDs below the current delta range
		 * (consumed by compaction, contents still intact) stay readable, so
		 * index scans keep returning moved rows during the window between a
		 * compaction's metapage flip and the index rebuild.  The
		 * page-identity check below screens out recycled pages; their entries
		 * really are dead.
		 */
		if (blkno == CS_METAPAGE_BLKNO ||
			blkno >= RelationGetNumberOfBlocks(rel))
		{
			if (all_dead)
				*all_dead = true;
			return false;
		}

		/* Reuse buffer if same block, otherwise release and re-read */
		if (!BufferIsValid(cscan->delta_buf) ||
			BufferGetBlockNumber(cscan->delta_buf) != blkno)
		{
			if (BufferIsValid(cscan->delta_buf))
				ReleaseBuffer(cscan->delta_buf);
			cscan->delta_buf = ReadBuffer(rel, blkno);
		}

		LockBuffer(cscan->delta_buf, BUFFER_LOCK_SHARE);
		page = BufferGetPage(cscan->delta_buf);

		/* a foreign page interleaved into the range is not delta content */
		if (!CSPageIsDelta(page) ||
			offnum < FirstOffsetNumber ||
			offnum > PageGetMaxOffsetNumber(page))
		{
			LockBuffer(cscan->delta_buf, BUFFER_LOCK_UNLOCK);
			if (all_dead)
				*all_dead = true;
			return false;
		}

		itemid = PageGetItemId(page, offnum);
		if (!ItemIdIsNormal(itemid))
		{
			LockBuffer(cscan->delta_buf, BUFFER_LOCK_UNLOCK);
			if (all_dead)
				*all_dead = ItemIdIsDead(itemid);
			return false;
		}

		tuple.t_data = (HeapTupleHeader) PageGetItem(page, itemid);
		tuple.t_len = ItemIdGetLength(itemid);
		tuple.t_tableOid = RelationGetRelid(rel);
		tuple.t_self = *tid;

		/* Skip tombstone tuples — they are not data rows */
		if (CS_IS_TOMBSTONE(tuple.t_data))
		{
			LockBuffer(cscan->delta_buf, BUFFER_LOCK_UNLOCK);
			if (all_dead)
				*all_dead = false;
			return false;
		}

		visible = cs_delta_satisfies_visibility(&tuple, snapshot);
		if (!visible)
		{
			LockBuffer(cscan->delta_buf, BUFFER_LOCK_UNLOCK);
			if (all_dead)
				*all_dead = false;
			return false;
		}

		/* Tuple is visible -- deform into slot */
		heap_deform_tuple(&tuple, slot->tts_tupleDescriptor,
						  slot->tts_values, slot->tts_isnull);

		LockBuffer(cscan->delta_buf, BUFFER_LOCK_UNLOCK);

		if (all_dead)
			*all_dead = false;

		slot->tts_nvalid = slot->tts_tupleDescriptor->natts;
		slot->tts_flags &= ~TTS_FLAG_EMPTY;
		slot->tts_tid = *tid;
		slot->tts_tableOid = RelationGetRelid(rel);

		return true;
	}
}

/* ----------------------------------------------------------------
 * DDL callbacks
 * ----------------------------------------------------------------
 */
static void
cs_relation_set_new_filelocator(Relation rel,
								const RelFileLocator *newrlocator,
								char persistence,
								TransactionId *freezeXid,
								MultiXactId *minmulti)
{
	SMgrRelation srel;

	*freezeXid = RecentXmin;
	*minmulti = GetOldestMultiXactId();

	srel = RelationCreateStorage(*newrlocator, persistence, true);

	if (persistence == RELPERSISTENCE_UNLOGGED)
	{
		smgrcreate(srel, INIT_FORKNUM, false);
		log_smgrcreate(newrlocator, INIT_FORKNUM);
	}

	smgrclose(srel);

	/*
	 * Initialize the metapage.  We need to open the relation via its new
	 * filelocator to do this.  Since the relation cache entry still points to
	 * the old locator, we do this in cs_storage.c via a separate path that
	 * uses the smgr directly.
	 */
}

static void
cs_relation_nontransactional_truncate(Relation rel)
{
	RelationTruncate(rel, 0);
}

static void
cs_relation_copy_data(Relation rel, const RelFileLocator *newrlocator)
{
	SMgrRelation dstrel;

	FlushRelationBuffers(rel);

	dstrel = RelationCreateStorage(*newrlocator,
								   rel->rd_rel->relpersistence, true);

	RelationCopyStorage(RelationGetSmgr(rel), dstrel, MAIN_FORKNUM,
						rel->rd_rel->relpersistence);

	for (ForkNumber forkNum = MAIN_FORKNUM + 1;
		 forkNum <= MAX_FORKNUM; forkNum++)
	{
		if (smgrexists(RelationGetSmgr(rel), forkNum))
		{
			smgrcreate(dstrel, forkNum, false);
			if (RelationIsPermanent(rel) ||
				(rel->rd_rel->relpersistence == RELPERSISTENCE_UNLOGGED &&
				 forkNum == INIT_FORKNUM))
				log_smgrcreate(newrlocator, forkNum);
			RelationCopyStorage(RelationGetSmgr(rel), dstrel, forkNum,
								rel->rd_rel->relpersistence);
		}
	}

	RelationDropStorage(rel);
	smgrclose(dstrel);
}

static void
cs_relation_copy_for_cluster(Relation OldTable, Relation NewTable,
							 Relation OldIndex, bool use_sort,
							 TransactionId OldestXmin,
							 Snapshot snapshot,
							 TransactionId *xid_cutoff,
							 MultiXactId *multi_cutoff,
							 double *num_tuples,
							 double *tups_vacuumed,
							 double *tups_recently_dead)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("CLUSTER is not supported for columnstore tables")));
}

/* ----------------------------------------------------------------
 * ANALYZE callbacks
 * ----------------------------------------------------------------
 */
static bool
cs_scan_analyze_next_block(TableScanDesc scan, ReadStream *stream)
{
	CScanDesc	cscan = (CScanDesc) scan;

	/*
	 * Phase 1: consume blocks from the read stream, but only accept delta
	 * pages.  The stream samples all relation pages, so we skip the metapage,
	 * row group catalog pages, and column data pages.
	 */
	if (!cscan->delta_done)
	{
		for (;;)
		{
			BlockNumber blkno;

			cscan->delta_buf = read_stream_next_buffer(stream, NULL);
			if (!BufferIsValid(cscan->delta_buf))
			{
				cscan->delta_done = true;
				break;
			}

			blkno = BufferGetBlockNumber(cscan->delta_buf);

			/* Only process delta store pages */
			if (blkno >= cscan->delta_start &&
				blkno < cscan->delta_start + cscan->delta_nblocks)
			{
				LockBuffer(cscan->delta_buf, BUFFER_LOCK_SHARE);
				cscan->delta_cur_offset = FirstOffsetNumber;
				return true;
			}

			/* Not a delta page — release and try next */
			ReleaseBuffer(cscan->delta_buf);
			cscan->delta_buf = InvalidBuffer;
		}
	}

	/*
	 * Phase 2: iterate through columnar row groups.  Load the next unscanned
	 * row group so analyze_next_tuple can yield its rows.
	 */
	if (!cscan->columnar_done && cscan->cur_rowgroup < cscan->nrowgroups)
	{
		cs_load_rowgroup(cscan, scan->rs_rd, cscan->cur_rowgroup);
		cscan->cur_row_in_group = 0;
		return true;
	}

	cscan->columnar_done = true;
	return false;
}

static bool
cs_scan_analyze_next_tuple(TableScanDesc scan, double *liverows,
						   double *deadrows, TupleTableSlot *slot)
{
	CScanDesc	cscan = (CScanDesc) scan;

	/*
	 * If we have a delta buffer, yield tuples from it.
	 */
	if (BufferIsValid(cscan->delta_buf))
	{
		Page		page = BufferGetPage(cscan->delta_buf);
		OffsetNumber maxoff = PageGetMaxOffsetNumber(page);

		while (cscan->delta_cur_offset <= maxoff)
		{
			ItemId		itemid;
			HeapTupleData tuple;

			itemid = PageGetItemId(page, cscan->delta_cur_offset);
			cscan->delta_cur_offset++;

			if (!ItemIdIsNormal(itemid))
			{
				if (ItemIdIsDead(itemid))
					*deadrows += 1;
				continue;
			}

			tuple.t_data = (HeapTupleHeader) PageGetItem(page, itemid);
			tuple.t_len = ItemIdGetLength(itemid);
			tuple.t_tableOid = RelationGetRelid(scan->rs_rd);
			ItemPointerSet(&tuple.t_self,
						   BufferGetBlockNumber(cscan->delta_buf),
						   cscan->delta_cur_offset - 1);

			/* Skip tombstone tuples — they are not data rows */
			if (CS_IS_TOMBSTONE(tuple.t_data))
				continue;

			/*
			 * Only rows visible to ANALYZE's snapshot may enter the sample;
			 * aborted, uncommitted, and deleted rows otherwise pollute
			 * pg_statistic and reltuples.
			 */
			if (!cs_delta_satisfies_visibility(&tuple, GetActiveSnapshot()))
			{
				*deadrows += 1;
				continue;
			}

			*liverows += 1;

			ExecClearTuple(slot);
			heap_deform_tuple(&tuple, slot->tts_tupleDescriptor,
							  slot->tts_values, slot->tts_isnull);
			ExecStoreVirtualTuple(slot);
			return true;
		}

		/* Done with this delta block */
		UnlockReleaseBuffer(cscan->delta_buf);
		cscan->delta_buf = InvalidBuffer;
		ExecClearTuple(slot);
		return false;
	}

	/*
	 * Yield rows from the current columnar row group.
	 */
	if (cscan->colcache.col_nrows > 0)
	{
		/* Collect tombstones lazily before first columnar row access */
		if (!cscan->tombstones_collected)
			cs_collect_tombstones(cscan);

		while (cscan->cur_row_in_group < cscan->colcache.col_nrows)
		{
			uint32		row = cscan->cur_row_in_group++;

			/* Skip deleted rows */
			if (cscan->colcache.cur_delbitmap &&
				CS_ISDELETED(cscan->colcache.cur_delbitmap, row))
			{
				*deadrows += 1;
				continue;
			}

			/* Skip rows with visible tombstones (pending MVCC deletes) */
			if (cscan->tombstone_count > 0)
			{
				ItemPointerData vtid;

				cs_encode_virtual_tid(&vtid, cscan->cur_rowgroup, row);
				if (cs_tombstone_lookup(cscan->tombstone_tids,
										cscan->tombstone_count, &vtid))
				{
					*deadrows += 1;
					continue;
				}
			}

			*liverows += 1;

			ExecClearTuple(slot);

			/* Set up lazy loading via CSTupleTableSlot */
			{
				CSTupleTableSlot *csslot = (CSTupleTableSlot *) slot;
				BlockNumber vblk;
				OffsetNumber voff;

				csslot->cs_scan = cscan;
				csslot->cs_colcache = &cscan->colcache;
				csslot->cs_row = row;
				csslot->cs_is_columnar = true;

				slot->tts_flags &= ~TTS_FLAG_EMPTY;
				slot->tts_nvalid = 0;

				vblk = CS_COLUMNAR_BLKNO_BASE +
					cscan->cur_rowgroup * CS_VIRTUAL_BLOCKS_PER_RG +
					row / CS_ROWS_PER_VIRTUAL_BLOCK;
				voff = (row % CS_ROWS_PER_VIRTUAL_BLOCK) + 1;
				ItemPointerSet(&slot->tts_tid, vblk, voff);
			}
			slot->tts_tableOid = RelationGetRelid(scan->rs_rd);
			return true;
		}

		/* Done with this row group, advance to next */
		cscan->cur_rowgroup++;
		return false;
	}

	return false;
}

/* ----------------------------------------------------------------
 * Index build callback
 * ----------------------------------------------------------------
 */
static double
cs_index_build_range_scan(Relation table_rel, Relation index_rel,
						  IndexInfo *index_info, bool allow_sync,
						  bool anyvisible, bool progress,
						  BlockNumber start_blockno, BlockNumber numblocks,
						  IndexBuildCallback callback,
						  void *callback_state, TableScanDesc scan)
{
	TableScanDesc sscan;
	TupleTableSlot *slot;
	EState	   *estate;
	ExprContext *econtext;
	ExprState  *predicate;
	Snapshot	snapshot;
	bool		need_unregister_snapshot = false;
	double		reltuples = 0;

	/*
	 * Need an EState for evaluation of index expressions and partial-index
	 * predicates.  Also a slot to hold the current tuple.
	 */
	estate = CreateExecutorState();
	econtext = GetPerTupleExprContext(estate);

	slot = table_slot_create(table_rel, NULL);
	econtext->ecxt_scantuple = slot;

	/* Set up execution state for predicate, if any. */
	predicate = ExecPrepareQual(index_info->ii_Predicate, estate);

	if (scan)
	{
		sscan = scan;
		snapshot = scan->rs_snapshot;
	}
	else
	{
		snapshot = RegisterSnapshot(GetLatestSnapshot());
		need_unregister_snapshot = true;
		sscan = cs_scan_begin(table_rel, snapshot, 0, NULL, NULL,
							  SO_TYPE_SEQSCAN | SO_ALLOW_PAGEMODE);
	}

	/*
	 * Index-build mode: include rows any snapshot might still see (skip only
	 * known-aborted inserts) and ignore tombstones; see
	 * cs_collect_tombstones.  The registered snapshot above only provides
	 * xmin horizons for sysscans, not row filtering.
	 */
	((CScanDesc) sscan)->rs_index_build = true;

	while (cs_scan_getnextslot(sscan, ForwardScanDirection, slot))
	{
		Datum		values[INDEX_MAX_KEYS];
		bool		isnull[INDEX_MAX_KEYS];
		bool		tupleIsAlive = true;

		CHECK_FOR_INTERRUPTS();

		/*
		 * Honor the caller's block range (BRIN summarization passes one); our
		 * virtual blocks order rows by TID, so filtering on the emitted TID
		 * implements the range.
		 */
		if (numblocks != InvalidBlockNumber)
		{
			BlockNumber tidblk = ItemPointerGetBlockNumber(&slot->tts_tid);

			if (tidblk < start_blockno ||
				tidblk >= start_blockno + numblocks)
				continue;
		}

		MemoryContextReset(econtext->ecxt_per_tuple_memory);

		/*
		 * In a partial index, discard tuples that don't satisfy the
		 * predicate.
		 */
		if (predicate != NULL)
		{
			if (!ExecQual(predicate, econtext))
				continue;
		}

		FormIndexDatum(index_info, slot, estate, values, isnull);

		callback(index_rel, &slot->tts_tid, values, isnull,
				 tupleIsAlive, callback_state);

		reltuples++;
	}

	cs_scan_end(sscan);

	ExecDropSingleTupleTableSlot(slot);
	FreeExecutorState(estate);

	/* These may have been pointing to the now-gone estate */
	index_info->ii_ExpressionsState = NIL;
	index_info->ii_PredicateState = NULL;

	if (need_unregister_snapshot)
		UnregisterSnapshot(snapshot);

	return reltuples;
}

static void
cs_index_validate_scan(Relation table_rel, Relation index_rel,
					   IndexInfo *index_info, Snapshot snapshot,
					   ValidateIndexState *state)
{
	TableScanDesc scan;
	TupleTableSlot *slot;
	EState	   *estate;
	ExprContext *econtext;
	ExprState  *predicate;
	Datum		values[INDEX_MAX_KEYS];
	bool		isnull[INDEX_MAX_KEYS];
	ItemPointer indexcursor = NULL;
	ItemPointerData decoded;
	bool		tuplesort_empty = false;

	Assert(OidIsValid(index_rel->rd_rel->relam));

	/*
	 * Need an EState for evaluation of index expressions and partial-index
	 * predicates.  Also a slot to hold the current tuple.
	 */
	estate = CreateExecutorState();
	econtext = GetPerTupleExprContext(estate);
	slot = MakeSingleTupleTableSlot(RelationGetDescr(table_rel),
									&TTSOpsColumnStore);
	econtext->ecxt_scantuple = slot;

	/* Set up execution state for predicate, if any. */
	predicate = ExecPrepareQual(index_info->ii_Predicate, estate);

	/*
	 * Sequential scan of the table.  Syncscan must be disabled so that we
	 * read from block zero forward, matching the sorted TID order from the
	 * tuplesort.
	 */
	scan = table_beginscan_strat(table_rel, snapshot,
								 0, NULL, true, false);

	/*
	 * Merge-join the sequential scan against the sorted index TIDs.
	 *
	 * Columnstore has no HOT chains, so each tuple's TID is its own "root
	 * TID" -- the merge is a simple forward comparison without the per-page
	 * root_offsets / in_index bookkeeping that the heap needs.
	 */
	while (table_scan_getnextslot(scan, ForwardScanDirection, slot))
	{
		ItemPointer tablecursor = &slot->tts_tid;

		CHECK_FOR_INTERRUPTS();

		state->htups += 1;

		/*
		 * Advance the tuplesort cursor until it catches up to or passes the
		 * current table TID.
		 */
		while (!tuplesort_empty &&
			   (!indexcursor ||
				ItemPointerCompare(indexcursor, tablecursor) < 0))
		{
			Datum		ts_val;
			bool		ts_isnull;

			tuplesort_empty = !tuplesort_getdatum(state->tuplesort, true,
												  false, &ts_val, &ts_isnull,
												  NULL);
			Assert(tuplesort_empty || !ts_isnull);
			if (!tuplesort_empty)
			{
				itemptr_decode(&decoded, DatumGetInt64(ts_val));
				indexcursor = &decoded;
			}
			else
				indexcursor = NULL;
		}

		/*
		 * If the tuplesort has overshot (or is exhausted) then this tuple is
		 * missing from the index -- insert it.
		 */
		if (tuplesort_empty ||
			ItemPointerCompare(indexcursor, tablecursor) > 0)
		{
			MemoryContextReset(econtext->ecxt_per_tuple_memory);

			/*
			 * In a partial index, discard tuples that don't satisfy the
			 * predicate.
			 */
			if (predicate != NULL)
			{
				if (!ExecQual(predicate, econtext))
					continue;
			}

			FormIndexDatum(index_info, slot, estate, values, isnull);

			index_insert(index_rel,
						 values, isnull,
						 tablecursor,
						 table_rel,
						 index_info->ii_Unique ?
						 UNIQUE_CHECK_YES : UNIQUE_CHECK_NO,
						 false,
						 index_info);

			state->tups_inserted += 1;
		}
	}

	table_endscan(scan);

	ExecDropSingleTupleTableSlot(slot);
	FreeExecutorState(estate);

	/* These may have been pointing to the now-gone estate */
	index_info->ii_ExpressionsState = NIL;
	index_info->ii_PredicateState = NULL;
}

/* ----------------------------------------------------------------
 * Size / estimation callbacks
 * ----------------------------------------------------------------
 */
static uint64
cs_relation_size(Relation rel, ForkNumber forkNumber)
{
	return table_block_relation_size(rel, forkNumber);
}

/*
 * Decide whether a columnstore relation needs an associated TOAST table.
 *
 * Incoming inserts are buffered as ordinary 8 KB heap tuples in the delta
 * store, so the heap rule applies: if the maximum tuple length could
 * exceed TOAST_TUPLE_THRESHOLD, a TOAST table is required so
 * heap_toast_insert_or_update can push oversized varlena attributes out
 * of line.  The compactor later detoasts those attributes when it
 * serializes them into the column chunks.
 *
 * The logic mirrors heapam_relation_needs_toast_table().
 */
static bool
cs_relation_needs_toast_table(Relation rel)
{
	int32		data_length = 0;
	bool		maxlength_unknown = false;
	bool		has_toastable_attrs = false;
	TupleDesc	tupdesc = rel->rd_att;
	int32		tuple_length;
	int			i;

	for (i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(tupdesc, i);

		if (att->attisdropped)
			continue;
		if (att->attgenerated == ATTRIBUTE_GENERATED_VIRTUAL)
			continue;
		data_length = att_align_nominal(data_length, att->attalign);
		if (att->attlen > 0)
		{
			/* Fixed-length types are never toastable */
			data_length += att->attlen;
		}
		else
		{
			int32		maxlen = type_maximum_size(att->atttypid,
												   att->atttypmod);

			if (maxlen < 0)
				maxlength_unknown = true;
			else
				data_length += maxlen;
			if (att->attstorage != TYPSTORAGE_PLAIN)
				has_toastable_attrs = true;
		}
	}
	if (!has_toastable_attrs)
		return false;
	if (maxlength_unknown)
		return true;
	tuple_length = MAXALIGN(SizeofHeapTupleHeader +
							BITMAPLEN(tupdesc->natts)) +
		MAXALIGN(data_length);
	return (tuple_length > TOAST_TUPLE_THRESHOLD);
}

/*
 * TOAST tables for columnstore relations are plain heap tables; the
 * columnstore AM does not host TOAST chunks itself.
 */
static Oid
cs_relation_toast_am(Relation rel)
{
	return HEAP_TABLE_AM_OID;
}

/*
 * Compute scan-cost scaling factors for a columnstore relation.
 *
 * Of the six outputs, only *cpu_tuple_cost_factor is currently consumed
 * (by cs_set_rel_pathlist_hook, to scale the per-tuple portion of the
 * inherited baseline path's cost).  The remaining outputs --
 * seq_page_cost_factor, rand_page_cost_factor, rand_io_pages,
 * disk_cost_parallelizable, index_fetch_cost -- are computed for a
 * future cost-modifier hook in costsize.c that would let the AM scale
 * page costs and supply a per-row-group random-I/O unit; today nothing
 * reads them, so index-scan and bitmap-scan costing on columnstore
 * relations falls back to the heap defaults.
 */
void
cs_relation_cost_factors(Relation rel,
						 double *seq_page_cost_factor,
						 double *rand_page_cost_factor,
						 double *cpu_tuple_cost_factor,
						 BlockNumber *rand_io_pages,
						 bool *disk_cost_parallelizable,
						 double *index_fetch_cost)
{
	CSMetaPageData meta;

	/*
	 * Columnar scans decompress and project columns, which is cheaper
	 * per-tuple than heap's row-oriented processing when only a subset of
	 * columns is needed.  Use a lower CPU tuple cost factor.
	 */
	*seq_page_cost_factor = 1.0;
	*rand_page_cost_factor = 1.0;
	*cpu_tuple_cost_factor = 0.5;
	*disk_cost_parallelizable = false;
	*index_fetch_cost = 0.0;

	/*
	 * Below: rand_io_pages and index_fetch_cost are reported for a future
	 * cost-modifier hook; cost_index() does not consume them today.
	 */
	if (RelationGetNumberOfBlocks(rel) > 0)
	{
		cs_read_metapage(rel, &meta);

		/*
		 * Each index probe into a columnstore row group needs to decompress
		 * one chunk per projected column, not one heap-equivalent page per
		 * tuple, so the random-I/O unit is the row group rather than the
		 * page.
		 */
		*rand_io_pages = meta.cs_nrowgroups;

		/*
		 * Per-column per-tuple index fetch cost.
		 *
		 * Each index probe into a columnstore row group requires base-
		 * decompressing the column chunk: reading avg_pages_per_chunk pages
		 * from shared buffers and running LZ4/PGLZ decompression.  The
		 * decompression CPU dominates -- empirically ~10x the cost of a
		 * sequential page read per compressed page.
		 */
		if (meta.cs_nrowgroups > 0 && meta.cs_natts > 0 &&
			meta.cs_data_pages > 0)
		{
			double		avg_pages_per_chunk;

			avg_pages_per_chunk = (double) meta.cs_data_pages /
				((double) meta.cs_nrowgroups * meta.cs_natts);

			*index_fetch_cost = avg_pages_per_chunk * 10.0;
		}
	}
	else
		*rand_io_pages = 0;
}

int
cs_compute_parallel_workers(Relation rel)
{
	CSMetaPageData meta;

	if (RelationGetNumberOfBlocks(rel) == 0)
		return 0;

	cs_read_metapage(rel, &meta);

	/* One worker per row group, capped at a reasonable maximum */
	if (meta.cs_nrowgroups <= 1)
		return 0;

	/*
	 * Honor the user's limits: the parallel_workers reloption when set, and
	 * always max_parallel_workers_per_gather -- "Workers Planned" must never
	 * exceed what the user allowed.
	 */
	if (rel->rd_options != NULL &&
		((StdRdOptions *) rel->rd_options)->parallel_workers >= 0)
		return Min(((StdRdOptions *) rel->rd_options)->parallel_workers,
				   max_parallel_workers_per_gather);

	return Min(Min(meta.cs_nrowgroups, 4), max_parallel_workers_per_gather);
}

static void
cs_relation_estimate_size(Relation rel, int32 *attr_widths,
						  BlockNumber *pages, double *tuples,
						  double *allvisfrac)
{
	BlockNumber nblocks;
	CSMetaPageData meta;

	nblocks = RelationGetNumberOfBlocks(rel);

	if (nblocks == 0)
	{
		*pages = 0;
		*tuples = 0;
		*allvisfrac = 0;
		return;
	}

	/*
	 * Read the metapage to get cached stats.  cs_data_pages counts only
	 * column data and delta pages (set by VACUUM), giving the planner an
	 * accurate I/O cost without the metadata overhead that
	 * RelationGetNumberOfBlocks includes.  Fall back to the physical block
	 * count when cs_data_pages hasn't been set yet (pre-VACUUM).
	 */
	cs_read_metapage(rel, &meta);
	*tuples = (double) meta.cs_total_rows;
	*pages = (meta.cs_data_pages > 0) ? meta.cs_data_pages : nblocks;

	/*
	 * Report allvisfrac = 0.  An index-only scan needs the visibility map
	 * fork to be set for the row's page in order to skip the heap fetch, and
	 * this AM does not maintain a visibility map for the virtual TID range
	 * that columnar rows live in.  Reporting a non-zero allvisfrac would let
	 * the planner pick an IOS path whose execution still falls through to
	 * cs_index_fetch_tuple for every row -- a plan-shape lie with no actual
	 * heap-skip behind it.
	 */
	*allvisfrac = 0.0;
}

/* ----------------------------------------------------------------
 * Bitmap scan callback
 * ----------------------------------------------------------------
 */
static bool
cs_scan_bitmap_next_tuple(TableScanDesc scan, TupleTableSlot *slot,
						  bool *recheck, uint64 *lossy_pages,
						  uint64 *exact_pages)
{
	CScanDesc	cscan = (CScanDesc) scan;
	Relation	rel = scan->rs_rd;

	for (;;)
	{
		/* If we have offsets remaining in the current page, process them */
		while (cscan->bm_curoff < cscan->bm_noffsets)
		{
			ItemPointerData tid;
			OffsetNumber off = cscan->bm_offsets[cscan->bm_curoff++];

			ItemPointerSet(&tid, cscan->bm_block, off);

			if (cs_fetch_row_version(rel, &tid, scan->rs_snapshot, slot))
			{
				*recheck = cscan->bm_recheck;
				return true;
			}
		}

		/* For lossy pages, scan all offsets on the block */
		if (cscan->bm_lossy)
		{
			BlockNumber blkno = cscan->bm_block;

			cscan->bm_lossy = false;

			if (blkno >= CS_COLUMNAR_BLKNO_BASE)
			{
				/*
				 * Lossy columnar page: yield all rows in the virtual block's
				 * range that are alive.
				 */
				uint32		max_off = CS_ROWS_PER_VIRTUAL_BLOCK;

				cscan->bm_noffsets = 0;
				for (uint32 i = 0; i < max_off; i++)
				{
					cscan->bm_offsets[cscan->bm_noffsets++] = (OffsetNumber) (i + 1);
					if (cscan->bm_noffsets >= lengthof(cscan->bm_offsets))
						break;
				}
				cscan->bm_curoff = 0;
				cscan->bm_recheck = true;
				continue;
			}
			else if (blkno >= cscan->delta_start &&
					 blkno < cscan->delta_start + cscan->delta_nblocks)
			{
				/* Lossy delta page: scan all offsets */
				Buffer		buf;
				Page		page;
				OffsetNumber maxoff;

				buf = ReadBuffer(rel, blkno);
				LockBuffer(buf, BUFFER_LOCK_SHARE);
				page = BufferGetPage(buf);
				/* foreign pages in the range hold no delta tuples */
				maxoff = CSPageIsDelta(page) ?
					PageGetMaxOffsetNumber(page) : InvalidOffsetNumber;
				UnlockReleaseBuffer(buf);

				cscan->bm_noffsets = 0;
				for (OffsetNumber off = FirstOffsetNumber; off <= maxoff; off++)
				{
					cscan->bm_offsets[cscan->bm_noffsets++] = off;
					if (cscan->bm_noffsets >= lengthof(cscan->bm_offsets))
						break;
				}
				cscan->bm_curoff = 0;
				cscan->bm_recheck = true;
				continue;
			}
		}

		/* Need a new TBM page */
		{
			TBMIterateResult tbmres;

			if (!tbm_iterate(&scan->st.rs_tbmiterator, &tbmres))
				return false;

			cscan->bm_block = tbmres.blockno;
			cscan->bm_recheck = tbmres.recheck;

			if (tbmres.lossy)
			{
				*lossy_pages += 1;
				cscan->bm_lossy = true;
				cscan->bm_noffsets = 0;
				cscan->bm_curoff = 0;
				continue;
			}
			else
			{
				*exact_pages += 1;
				cscan->bm_noffsets = tbm_extract_page_tuple(&tbmres,
															cscan->bm_offsets,
															lengthof(cscan->bm_offsets));
				Assert(cscan->bm_noffsets <= lengthof(cscan->bm_offsets));
				cscan->bm_curoff = 0;
				cscan->bm_lossy = false;
			}
		}
	}
}

/* ----------------------------------------------------------------
 * TABLESAMPLE callbacks
 *
 * The columnstore presents a virtual block abstraction where delta store
 * pages map to the first delta_nblocks blocks, and each row group maps
 * to CS_VIRTUAL_BLOCKS_PER_RG blocks, each holding up to
 * CS_ROWS_PER_VIRTUAL_BLOCK rows.
 * ----------------------------------------------------------------
 */
static bool
cs_scan_sample_next_block(TableScanDesc scan, SampleScanState *scanstate)
{
	CScanDesc	cscan = (CScanDesc) scan;
	TsmRoutine *tsm = scanstate->tsmroutine;
	BlockNumber blockno;
	BlockNumber vblk;
	uint32		rg_id;
	uint32		block_in_rg;

	/* Compute total virtual blocks on first call */
	if (!cscan->sample_inited)
	{
		cscan->sample_nblocks = cscan->delta_nblocks +
			cscan->nrowgroups * CS_VIRTUAL_BLOCKS_PER_RG;
		cscan->sample_cur_block = InvalidBlockNumber;
		cscan->sample_inited = true;
		cscan->sample_rg_loaded = UINT32_MAX;
	}

	if (cscan->sample_nblocks == 0)
		return false;

	/* Release any previously pinned delta buffer */
	if (BufferIsValid(cscan->delta_buf))
	{
		ReleaseBuffer(cscan->delta_buf);
		cscan->delta_buf = InvalidBuffer;
	}

	if (tsm->NextSampleBlock)
	{
		blockno = tsm->NextSampleBlock(scanstate, cscan->sample_nblocks);
	}
	else
	{
		/* Sequential iteration (BERNOULLI method) */
		if (cscan->sample_cur_block == InvalidBlockNumber)
			blockno = 0;
		else
		{
			blockno = cscan->sample_cur_block + 1;
			if (blockno >= cscan->sample_nblocks)
				blockno = InvalidBlockNumber;
		}
	}

	if (!BlockNumberIsValid(blockno))
		return false;

	cscan->sample_cur_block = blockno;

	CHECK_FOR_INTERRUPTS();

	if (blockno < cscan->delta_nblocks)
	{
		/* Delta store page */
		BlockNumber real_blkno = cscan->delta_start + blockno;

		cscan->delta_buf = ReadBuffer(scan->rs_rd, real_blkno);
		cscan->sample_is_delta = true;
		return true;
	}

	/* Columnar virtual block */
	vblk = blockno - cscan->delta_nblocks;
	rg_id = vblk / CS_VIRTUAL_BLOCKS_PER_RG;
	block_in_rg = vblk % CS_VIRTUAL_BLOCKS_PER_RG;

	/* Load the row group if it's not the one we already have */
	if (cscan->sample_rg_loaded != rg_id)
	{
		if (!cs_load_rowgroup(cscan, scan->rs_rd, rg_id))
		{
			/*
			 * Invalid/empty row group.  Tell TSM there are no tuples on this
			 * block so it moves to the next.
			 */
			cscan->sample_is_delta = false;
			cscan->sample_row_base = 0;
			cscan->colcache.col_nrows = 0;
			return true;
		}
		cscan->sample_rg_loaded = rg_id;
	}

	cscan->sample_row_base = block_in_rg * CS_ROWS_PER_VIRTUAL_BLOCK;
	cscan->sample_is_delta = false;

	return true;
}

static bool
cs_scan_sample_next_tuple(TableScanDesc scan, SampleScanState *scanstate,
						  TupleTableSlot *slot)
{
	CScanDesc	cscan = (CScanDesc) scan;
	TsmRoutine *tsm = scanstate->tsmroutine;
	BlockNumber blockno = cscan->sample_cur_block;

	if (cscan->sample_is_delta)
	{
		/* Delta store: iterate tuples on the page */
		Page		page;
		OffsetNumber maxoffset;
		OffsetNumber tupoffset;
		ItemId		itemid;
		HeapTupleData tuple;
		bool		visible;

		LockBuffer(cscan->delta_buf, BUFFER_LOCK_SHARE);
		page = BufferGetPage(cscan->delta_buf);
		maxoffset = PageGetMaxOffsetNumber(page);

		for (;;)
		{
			CHECK_FOR_INTERRUPTS();

			tupoffset = tsm->NextSampleTuple(scanstate, blockno, maxoffset);
			if (!OffsetNumberIsValid(tupoffset))
			{
				LockBuffer(cscan->delta_buf, BUFFER_LOCK_UNLOCK);
				ExecClearTuple(slot);
				return false;
			}

			itemid = PageGetItemId(page, tupoffset);
			if (!ItemIdIsNormal(itemid))
				continue;

			tuple.t_data = (HeapTupleHeader) PageGetItem(page, itemid);
			tuple.t_len = ItemIdGetLength(itemid);
			tuple.t_tableOid = RelationGetRelid(scan->rs_rd);
			ItemPointerSet(&tuple.t_self,
						   cscan->delta_start + blockno, tupoffset);

			/* Skip tombstone tuples — they are not data rows */
			if (CS_IS_TOMBSTONE(tuple.t_data))
				continue;

			visible = cs_delta_satisfies_visibility(&tuple, scan->rs_snapshot);
			if (!visible)
				continue;

			LockBuffer(cscan->delta_buf, BUFFER_LOCK_UNLOCK);

			ExecClearTuple(slot);
			heap_deform_tuple(&tuple, slot->tts_tupleDescriptor,
							  slot->tts_values, slot->tts_isnull);
			ExecStoreVirtualTuple(slot);
			slot->tts_tid = tuple.t_self;
			slot->tts_tableOid = tuple.t_tableOid;
			return true;
		}
	}
	else
	{
		/* Columnar virtual block */
		CSColumnCache *cache = &cscan->colcache;
		uint32		nrows = cache->col_nrows;
		uint32		row_base = cscan->sample_row_base;
		OffsetNumber maxoffset;
		OffsetNumber tupoffset;
		uint32		row;
		CSTupleTableSlot *csslot;
		BlockNumber vblk_base;
		uint32		rg_id;
		BlockNumber vblk;
		OffsetNumber voff;

		/* Collect tombstones lazily before first columnar row access */
		if (!cscan->tombstones_collected)
			cs_collect_tombstones(cscan);

		/* Rows on this virtual block: capped by actual row group size */
		if (row_base >= nrows)
		{
			ExecClearTuple(slot);
			return false;
		}
		maxoffset = Min(CS_ROWS_PER_VIRTUAL_BLOCK, nrows - row_base);

		for (;;)
		{
			CHECK_FOR_INTERRUPTS();

			tupoffset = tsm->NextSampleTuple(scanstate, blockno, maxoffset);
			if (!OffsetNumberIsValid(tupoffset))
			{
				ExecClearTuple(slot);
				return false;
			}

			row = row_base + tupoffset - 1; /* 1-based to 0-based */

			/* Skip rows marked as deleted */
			if (cache->cur_delbitmap != NULL)
			{
				if (CS_ISDELETED(cache->cur_delbitmap, row))
					continue;
			}

			/* Skip rows with visible tombstones (pending MVCC deletes) */
			if (cscan->tombstone_count > 0)
			{
				ItemPointerData vtid;
				uint32		ts_rg_id;

				vblk_base = cscan->sample_cur_block - cscan->delta_nblocks;
				ts_rg_id = vblk_base / CS_VIRTUAL_BLOCKS_PER_RG;
				cs_encode_virtual_tid(&vtid, ts_rg_id, row);
				if (cs_tombstone_lookup(cscan->tombstone_tids,
										cscan->tombstone_count, &vtid))
					continue;
			}

			ExecClearTuple(slot);

			/* Set up lazy column loading */
			csslot = (CSTupleTableSlot *) slot;
			csslot->cs_scan = cscan;
			csslot->cs_colcache = cache;
			csslot->cs_row = row;
			csslot->cs_is_columnar = true;

			slot->tts_flags &= ~TTS_FLAG_EMPTY;
			slot->tts_nvalid = 0;

			/* Set the virtual TID */
			vblk_base = cscan->sample_cur_block - cscan->delta_nblocks;
			rg_id = vblk_base / CS_VIRTUAL_BLOCKS_PER_RG;
			vblk = CS_COLUMNAR_BLKNO_BASE +
				rg_id * CS_VIRTUAL_BLOCKS_PER_RG +
				row / CS_ROWS_PER_VIRTUAL_BLOCK;
			voff = (row % CS_ROWS_PER_VIRTUAL_BLOCK) + 1;

			ItemPointerSet(&slot->tts_tid, vblk, voff);

			return true;
		}
	}
}

/* ----------------------------------------------------------------
 * Parallel scan support
 * ----------------------------------------------------------------
 */
static Size
cs_parallelscan_estimate(Relation rel)
{
	return sizeof(CSParallelScanDescData);
}

static Size
cs_parallelscan_initialize(Relation rel, ParallelTableScanDesc pscan)
{
	CSParallelScanDesc cpscan = (CSParallelScanDesc) pscan;
	Size		size;

	pscan->phs_locator = rel->rd_locator;
	pscan->phs_syncscan = false;

	/* Read metapage and cache layout in shared memory */
	if (RelationGetNumberOfBlocks(rel) > 0)
	{
		CSMetaPageData meta;

		cs_read_metapage(rel, &meta);
		cpscan->pcs_nrowgroups = meta.cs_nrowgroups;
		cpscan->pcs_natts = meta.cs_natts;
		cpscan->pcs_delta_start = meta.cs_delta_start;
		cpscan->pcs_delta_nblocks = meta.cs_delta_nblocks;
		cpscan->pcs_rgdir_start = meta.cs_rgdir_start;
		cpscan->pcs_rgdir_npages = meta.cs_rgdir_npages;
	}
	else
	{
		cpscan->pcs_nrowgroups = 0;
		cpscan->pcs_natts = 0;
		cpscan->pcs_delta_start = InvalidBlockNumber;
		cpscan->pcs_delta_nblocks = 0;
		cpscan->pcs_rgdir_start = InvalidBlockNumber;
		cpscan->pcs_rgdir_npages = 0;
	}

	pg_atomic_init_u64(&cpscan->pcs_nallocated, 0);

	pg_atomic_init_u64(&cpscan->pcs_instr_rg_examined, 0);
	pg_atomic_init_u64(&cpscan->pcs_instr_rg_zonemap_skipped, 0);

	size = sizeof(CSParallelScanDescData);
	return size;
}

static void
cs_parallelscan_reinitialize(Relation rel, ParallelTableScanDesc pscan)
{
	CSParallelScanDesc cpscan = (CSParallelScanDesc) pscan;

	pg_atomic_write_u64(&cpscan->pcs_nallocated, 0);

	pg_atomic_write_u64(&cpscan->pcs_instr_rg_examined, 0);
	pg_atomic_write_u64(&cpscan->pcs_instr_rg_zonemap_skipped, 0);
}


/* ----------------------------------------------------------------
 * TableAmRoutine definition
 * ----------------------------------------------------------------
 */

static const TableAmRoutine columnstore_methods = {
	.type = T_TableAmRoutine,

	.amoptions = cs_reloptions,

	.slot_callbacks = cs_slot_callbacks,

	.scan_begin = cs_scan_begin,
	.scan_end = cs_scan_end,
	.scan_rescan = cs_scan_rescan,
	.scan_getnextslot = cs_scan_getnextslot,

	.scan_set_tidrange = cs_scan_set_tidrange,
	.scan_getnextslot_tidrange = cs_scan_getnextslot_tidrange,

	.parallelscan_estimate = cs_parallelscan_estimate,
	.parallelscan_initialize = cs_parallelscan_initialize,
	.parallelscan_reinitialize = cs_parallelscan_reinitialize,

	.index_fetch_begin = cs_index_fetch_begin,
	.index_fetch_reset = cs_index_fetch_reset,
	.index_fetch_end = cs_index_fetch_end,
	.index_fetch_tuple = cs_index_fetch_tuple,

	.tuple_insert = cs_tuple_insert,
	.tuple_insert_speculative = cs_tuple_insert_speculative,
	.tuple_complete_speculative = cs_tuple_complete_speculative,
	.multi_insert = cs_multi_insert,
	.tuple_delete = cs_tuple_delete,
	.tuple_update = cs_tuple_update,
	.tuple_lock = cs_tuple_lock,

	.tuple_fetch_row_version = cs_fetch_row_version,
	.tuple_get_latest_tid = cs_get_latest_tid,
	.tuple_tid_valid = cs_tuple_tid_valid,
	.tuple_satisfies_snapshot = cs_tuple_satisfies_snapshot,
	.index_delete_tuples = cs_index_delete_tuples,

	.relation_set_new_filelocator = cs_relation_set_new_filelocator,
	.relation_nontransactional_truncate = cs_relation_nontransactional_truncate,
	.relation_copy_data = cs_relation_copy_data,
	.relation_copy_for_cluster = cs_relation_copy_for_cluster,
	.relation_vacuum = cs_vacuum_rel,
	.scan_analyze_next_block = cs_scan_analyze_next_block,
	.scan_analyze_next_tuple = cs_scan_analyze_next_tuple,
	.index_build_range_scan = cs_index_build_range_scan,
	.index_validate_scan = cs_index_validate_scan,

	.relation_size = cs_relation_size,
	.relation_needs_toast_table = cs_relation_needs_toast_table,
	.relation_toast_am = cs_relation_toast_am,

	.relation_estimate_size = cs_relation_estimate_size,

	.scan_bitmap_next_tuple = cs_scan_bitmap_next_tuple,
	.scan_sample_next_block = cs_scan_sample_next_block,
	.scan_sample_next_tuple = cs_scan_sample_next_tuple,
};


const TableAmRoutine *
GetColumnstoreAmRoutine(void)
{
	return &columnstore_methods;
}

PG_FUNCTION_INFO_V1(columnstore_tableam_handler);

Datum
columnstore_tableam_handler(PG_FUNCTION_ARGS)
{
	PG_RETURN_POINTER(&columnstore_methods);
}

/*
 * Read the sort_key reloption from a columnstore relation.  The value
 * lives in our own CSRdOptions struct (registered by cs_reloptions);
 * NULL means no hint is set.
 */
const char *
cs_get_sort_key(Relation rel)
{
	return CSRelationGetSortKey(rel);
}

void
_PG_init(void)
{
	DefineCustomRealVariable("columnstore.rowgroup_compaction_threshold",
							 "Fraction of deleted rows that triggers row group compaction during VACUUM.",
							 "Row groups with a dead-row ratio at or above this value are rewritten "
							 "without the deleted rows.  Set to -1 to disable row group compaction.",
							 &columnstore_rowgroup_compaction_threshold,
							 -1.0,
							 -1.0, 1.0,
							 PGC_USERSET,
							 0,
							 NULL, NULL, NULL);

	MarkGUCPrefixReserved("columnstore");

	cs_register_reloptions();
	cs_register_custom_scan_methods();
}
