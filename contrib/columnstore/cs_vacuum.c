/*-------------------------------------------------------------------------
 *
 * cs_vacuum.c
 *	  VACUUM support for columnstore tables.
 *
 * VACUUM triggers the delta-to-columnar compaction, converting visible
 * delta store rows into compressed columnar row groups.  When the GUC
 * columnstore_rowgroup_compaction_threshold is non-negative, it also
 * rewrites row groups whose dead-row fraction exceeds the threshold,
 * reclaiming space.  After compaction, all indexes on the relation are
 * rebuilt automatically (via reindex_relation) so their entries match
 * the new virtual TIDs.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * contrib/columnstore/cs_vacuum.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cs_internal.h"
#include "access/multixact.h"
#include "catalog/index.h"
#include "catalog/storage.h"
#include "commands/progress.h"
#include "commands/vacuum.h"
#include "pgstat.h"
#include "postmaster/autovacuum.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "storage/procarray.h"
#include "utils/backend_progress.h"
#include "utils/inval.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/timestamp.h"

/* GUC variable */
double		columnstore_rowgroup_compaction_threshold = -1.0;

static void cs_count_live_stats(Relation rel, CSMetaPageData *meta,
								double *live_rows, double *dead_rows,
								BlockNumber *data_pages);
static void cs_truncate_trailing_freespace(Relation rel);

/*
 * Compute live-row count and data-page count from the row group catalog
 * and delta store.
 *
 * Columnar rows are counted precisely from the row group descriptors.
 * Delta rows are estimated from the delta page count (we don't scan pages
 * to check visibility, matching relation_estimate_size's approach).
 *
 * Data pages are the column data pages across all row groups plus delta
 * pages — i.e. the pages that a sequential scan actually reads.  Metadata
 * overhead (metapage, directory, catalog, free list) is excluded so that
 * the planner sees an accurate I/O cost.
 */
static void
cs_count_live_stats(Relation rel, CSMetaPageData *meta,
					double *live_rows, double *dead_rows,
					BlockNumber *data_pages)
{
	double		total_rows = 0;
	double		total_dead = 0;
	BlockNumber total_pages = 0;
	int			natts = RelationGetDescr(rel)->natts;

	if (meta->cs_nrowgroups > 0)
	{
		BlockNumber *dir = cs_read_rgdir(rel, meta);

		if (dir != NULL)
		{
			for (uint32 rg = 0; rg < meta->cs_nrowgroups; rg++)
			{
				CSRowGroupDesc *rg_desc;
				Size		rg_size;

				if (dir[rg] == InvalidBlockNumber)
					continue;

				rg_size = CSRowGroupDescSize(natts);
				rg_desc = palloc(rg_size);
				cs_read_rowgroup_catalog(rel, dir[rg], rg_desc, natts);
				total_rows += (double) (rg_desc->rg_num_rows -
										rg_desc->rg_num_deleted);
				total_dead += (double) rg_desc->rg_num_deleted;

				for (int col = 0; col < natts; col++)
					total_pages += rg_desc->rg_columns[col].cc_npages;

				pfree(rg_desc);
			}
			pfree(dir);
		}
	}

	/* Estimate delta rows from page count; delta pages are data too */
	if (meta->cs_delta_nblocks > 0)
	{
		double		usable = (double) (BLCKSZ - SizeOfPageHeaderData);
		int			tuple_width = 64;	/* conservative estimate */
		double		tup_per_page;

		tup_per_page = usable / tuple_width;
		if (tup_per_page < 1)
			tup_per_page = 1;
		total_rows += meta->cs_delta_nblocks * tup_per_page;
		total_pages += meta->cs_delta_nblocks;
	}

	*live_rows = total_rows;
	*dead_rows = total_dead;
	*data_pages = total_pages;
}

/*
 * Truncate trailing free space from the relation file.
 *
 * After compaction, freed pages land on the free list but the file never
 * shrinks.  Scan the free list for any range that reaches the end of the
 * relation; if found, truncate the file and remove (or shrink) that range.
 * Repeat until no trailing free range remains, then persist the updated
 * free list and metapage.
 */
static void
cs_truncate_trailing_freespace(Relation rel)
{
	CSMetaPageData meta;
	CSFreeRange *freelist;
	uint32		fl_nranges;
	BlockNumber nblocks;
	bool		truncated = false;

	/*
	 * Physical truncation requires AccessExclusiveLock, as in heap's lazy
	 * truncation: concurrent scans hold block numbers they expect to keep
	 * reading, and smgrtruncate under a weaker lock would yank pages out from
	 * under them.  Take the lock conditionally and skip truncation when
	 * anyone is using the table; the space remains on the free list for reuse
	 * and a later VACUUM can reclaim it.
	 */
	if (!ConditionalLockRelation(rel, AccessExclusiveLock))
		return;

	nblocks = RelationGetNumberOfBlocks(rel);
	if (nblocks <= 1)
	{
		UnlockRelation(rel, AccessExclusiveLock);
		return;					/* nothing beyond the metapage */
	}

	cs_read_metapage(rel, &meta);

	freelist = cs_read_freelist(rel, &meta);
	fl_nranges = meta.cs_freelist_nranges;
	if (freelist == NULL || fl_nranges == 0)
	{
		UnlockRelation(rel, AccessExclusiveLock);
		return;
	}

	/*
	 * Repeatedly look for a free range at the tail.  Merging in
	 * cs_freelist_add may have coalesced multiple ranges, so one pass
	 * suffices in practice, but loop for safety.
	 */
	for (;;)
	{
		bool		found = false;

		for (uint32 i = 0; i < fl_nranges; i++)
		{
			if (freelist[i].fr_start + freelist[i].fr_npages == nblocks)
			{
				nblocks = freelist[i].fr_start;

				/* Remove this entry */
				fl_nranges--;
				if (i < fl_nranges)
					memmove(&freelist[i], &freelist[i + 1],
							sizeof(CSFreeRange) * (fl_nranges - i));

				truncated = true;
				found = true;
				break;
			}
		}

		if (!found)
			break;
	}

	if (truncated)
	{
		/*
		 * Persist the reduced free list before truncating, so that a crash
		 * after the metapage write but before truncation leaves a consistent
		 * (slightly oversized) file rather than a truncated file with stale
		 * free-list entries pointing past EOF.
		 */
		meta.cs_freelist_nranges = fl_nranges;
		cs_write_freelist(rel, &meta, freelist, fl_nranges);
		cs_write_metapage(rel, &meta);

		RelationTruncate(rel, nblocks);
	}

	UnlockRelation(rel, AccessExclusiveLock);
	pfree(freelist);
}

void
cs_vacuum_rel(Relation rel, const VacuumParams *params,
			  BufferAccessStrategy bstrategy)
{
	CSMetaPageData meta;
	double		live_rows;
	double		dead_rows;
	BlockNumber num_pages;
	TransactionId frozenxid;
	TimestampTz starttime;

	starttime = GetCurrentTimestamp();

	pgstat_progress_start_command(PROGRESS_COMMAND_VACUUM,
								  RelationGetRelid(rel));
	if (AmAutoVacuumWorkerProcess())
		pgstat_progress_update_param(PROGRESS_VACUUM_STARTED_BY,
									 PROGRESS_VACUUM_STARTED_BY_AUTOVACUUM);
	else
		pgstat_progress_update_param(PROGRESS_VACUUM_STARTED_BY,
									 PROGRESS_VACUUM_STARTED_BY_MANUAL);

	/*
	 * If a previous compaction was cancelled between publishing moved rows
	 * and rebuilding the indexes, repair the indexes first: nothing may
	 * reclaim the consumed pages (which the stale index entries still resolve
	 * through) until this has succeeded, and both reclamation paths run later
	 * in this function.
	 */
	cs_repair_pending_reindex(rel);

	/* Compact the delta store into columnar row groups */
	cs_compact_delta(rel);

	/*
	 * Freeze what stays behind in the delta, so an honest relfrozenxid can be
	 * reported below.  The horizon is computed after compaction: everything
	 * it left in place either predates the horizon (and gets frozen or killed
	 * here) or carries xids the horizon already accounts for.
	 */
	frozenxid = GetOldestNonRemovableTransactionId(rel);
	cs_freeze_delta(rel, frozenxid);

	/* Compact row groups with too many deleted rows */

	/*
	 * Row-group re-compaction renumbers virtual TIDs, which invalidates
	 * concurrent scans' positions, pending tombstones, and index entries
	 * until the rebuild below completes.  That is only safe with no other
	 * users of the table, so it requires AccessExclusiveLock; skip the pass
	 * (it is opportunistic maintenance) when the lock is not immediately
	 * available.
	 */
	if (columnstore_rowgroup_compaction_threshold >= 0.0)
	{
		if (ConditionalLockRelation(rel, AccessExclusiveLock))
		{
			cs_compact_rowgroups(rel);
			UnlockRelation(rel, AccessExclusiveLock);
		}
	}

	/* Reclaim disk space by truncating trailing free pages */
	cs_truncate_trailing_freespace(rel);

	/*
	 * Update pg_class.reltuples and relpages so the planner has accurate
	 * estimates without requiring a separate ANALYZE.
	 *
	 * Use the data-page count (column data + delta) rather than
	 * RelationGetNumberOfBlocks, which includes metadata overhead and
	 * free-list space that a sequential scan never reads.
	 */
	if (RelationGetNumberOfBlocks(rel) == 0)
	{
		/* an empty relation holds no xids; still advance the horizons */
		vac_update_relstats(rel, 0, 0,
							0, 0,
							rel->rd_rel->relhasindex,
							frozenxid,
							ReadNextMultiXactId(),
							NULL, NULL,
							false);
		pgstat_progress_end_command();
		return;
	}

	cs_read_metapage(rel, &meta);
	cs_count_live_stats(rel, &meta, &live_rows, &dead_rows, &num_pages);

	/*
	 * Correct metapage stats so that relation_estimate_size is accurate. Only
	 * the stats fields are written: a full metapage write here would clobber
	 * a delta extension a concurrent insert merged in after we re-read the
	 * metapage above.
	 */
	if ((double) meta.cs_total_rows != live_rows ||
		meta.cs_data_pages != num_pages)
		cs_metapage_update_stats(rel, (uint64) live_rows, num_pages);

	/*
	 * Report the freeze horizon as the new relfrozenxid: cs_freeze_delta
	 * removed every older xid from the delta, and columnar rows carry no
	 * transaction information at all.  No multixacts are ever stored, so
	 * relminmxid can advance to the current value.
	 */
	vac_update_relstats(rel, num_pages, live_rows,
						0, 0,
						rel->rd_rel->relhasindex,
						frozenxid,
						ReadNextMultiXactId(),
						NULL, NULL,
						false);

	pgstat_report_vacuum(rel,
						 Max(live_rows, 0),
						 dead_rows,
						 starttime);
	pgstat_progress_end_command();
}
