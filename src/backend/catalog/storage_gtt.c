/*-------------------------------------------------------------------------
 *
 * storage_gtt.c
 *	  Per-session storage management for global temporary tables.
 *
 * Global temporary tables have a shared catalog definition but per-session
 * private data.  Each backend that accesses a GTT gets its own local
 * storage files, stored in local buffers like regular temp tables.
 *
 * The mapping from GTT OID to per-session RelFileLocator is maintained in
 * a backend-local hash table (gtt_storage_hash).  Storage is lazily
 * created on first access and cleaned up at session end.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/catalog/storage_gtt.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/amapi.h"
#include "access/parallel.h"
#include "access/relation.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "catalog/heap.h"
#include "catalog/index.h"
#include "catalog/pg_tablespace_d.h"
#include "catalog/storage.h"
#include "catalog/storage_gtt.h"
#include "commands/sequence.h"
#include "commands/tablecmds.h"
#include "common/hashfn.h"
#include "miscadmin.h"
#include "nodes/pg_list.h"
#include "storage/ipc.h"
#include "storage/bufmgr.h"
#include "storage/procnumber.h"
#include "storage/smgr.h"
#include "utils/hsearch.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/relcache.h"

/*
 * Per-session state for a single global temporary table.
 *
 * The SubTransactionId fields track which (sub)transaction most recently
 * performed an action that must be undone on abort:
 *   - create_subid: subxact that added this entry to the hash
 *     (InvalidSubTransactionId once the entry has survived to top-level
 *     commit)
 *   - storage_subid: subxact that most recently called RelationCreateStorage
 *   - index_subid: subxact that built the index for this session
 * On subxact or xact abort of a given subid, the corresponding state is
 * reverted.  On subxact commit, the subid is reparented.  See
 * gtt_subxact_callback / gtt_xact_callback.
 */
typedef struct GttStorageEntry
{
	Oid			relid;			/* GTT's pg_class OID (hash key) */
	Oid			heap_relid;		/* parent heap for indexes, InvalidOid for
								 * heap entries themselves */
	Oid			toast_relid;	/* toast relation for heap entries, InvalidOid
								 * if none / not a heap */
	RelFileLocator locator;		/* per-session physical storage location */
	bool		storage_created;	/* has smgr file been created? */
	bool		is_index;		/* is this an index relation? */
	bool		index_built;	/* has index been built in this session? */
	bool		build_deferred; /* index_build deferred the physical build
								 * because the parent heap was unmaterialized */
	bool		on_commit_delete;	/* truncate data on commit? */
	bool		drop_pending;	/* entry scheduled for drop at xact commit */
	SubTransactionId create_subid;	/* subxact that added this entry */
	SubTransactionId storage_subid; /* subxact that created current storage */
	SubTransactionId index_subid;	/* subxact that built the index */
} GttStorageEntry;

/* Backend-local hash table: GTT OID -> GttStorageEntry */
static HTAB *gtt_storage_hash = NULL;

/*
 * True when any entry carries rollback-sensitive state (a valid
 * create_subid/storage_subid/index_subid, or drop_pending), letting the
 * xact/subxact callbacks skip their full-hash scans in the common case of
 * a transaction that established no such state.  Conservative: it is only
 * cleared once a top-level transaction end has settled every entry.
 */
static bool gtt_xact_state_dirty = false;

/*
 * Undo log for transactional swaps of a GTT's session-local relfilenumber
 * (RelationSetNewRelfilenumber on a GTT; reached from TRUNCATE, ALTER
 * SEQUENCE ... RESTART, and the like).  The file-level work is rolled back
 * by the regular PendingRelDelete machinery; these records roll back the
 * session-local mapping and the per-entry state the swap reset.  One record
 * is pushed per swap, newest first; subxact commit reparents records to the
 * parent, abort restores and discards them, top-level commit discards them.
 */
typedef struct GttSwapUndo
{
	Oid			relid;			/* which GTT was swapped */
	SubTransactionId subid;		/* subxact that performed the swap */
	RelFileNumber prev_relnumber;	/* mapping to restore on abort */
	bool		prev_index_built;
	bool		prev_build_deferred;
} GttSwapUndo;

/* List of GttSwapUndo *, newest first, allocated in TopMemoryContext */
static List *gtt_swap_undo = NIL;

/* Guard against recursive index builds */
static bool gtt_building_index = false;

/* Local function prototypes */
static void gtt_session_cleanup(int code, Datum arg);
static void ensure_gtt_hash(void);
static void gtt_xact_callback(XactEvent event, void *arg);
static void gtt_subxact_callback(SubXactEvent event,
								 SubTransactionId mySubid,
								 SubTransactionId parentSubid,
								 void *arg);
static void gtt_remove_entry(GttStorageEntry *entry);
static void gtt_revert_storage(GttStorageEntry *entry);
static void gtt_remove_relids(List *to_remove);
static void gtt_init_entry(GttStorageEntry *entry, Relation relation);
static void gtt_build_index_internal(Relation indexRelation, bool force);
static void gtt_truncate_smgr(GttStorageEntry *entry);
static void gtt_swap_undo_apply(GttSwapUndo *undo);

/*
 * ensure_gtt_hash
 *		Create the backend-local hash table on first use.
 */
static void
ensure_gtt_hash(void)
{
	HASHCTL		hashctl;

	if (gtt_storage_hash != NULL)
		return;

	hashctl.keysize = sizeof(Oid);
	hashctl.entrysize = sizeof(GttStorageEntry);
	hashctl.hcxt = TopMemoryContext;
	gtt_storage_hash = hash_create("GTT storage hash",
								   32,	/* initial size */
								   &hashctl,
								   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	/*
	 * Register session cleanup to drop all per-session GTT storage files when
	 * the backend exits, and xact/subxact callbacks that keep the hash in
	 * sync with transaction state (see gtt_xact_callback).  The hash is
	 * created exactly once per backend and never destroyed, so this cannot
	 * register twice.
	 */
	before_shmem_exit(gtt_session_cleanup, (Datum) 0);
	RegisterXactCallback(gtt_xact_callback, NULL);
	RegisterSubXactCallback(gtt_subxact_callback, NULL);
}

/*
 * GttInitSessionStorage
 *		Ensure per-session local storage exists for the given GTT relation.
 *
 * This is called from RelationInitPhysicalAddr when a GTT is being set up
 * in the relcache.  It creates a per-session storage file if one doesn't
 * exist yet, and fills in the relation's rd_locator and rd_backend to
 * point to the session-local file.
 *
 * The per-session relfilenode is allocated from the local OID counter
 * (same mechanism as regular temp tables).
 */
void
GttInitSessionStorage(Relation relation)
{
	GttStorageEntry *entry;
	bool		found;
	Oid			relid = RelationGetRelid(relation);

	/*
	 * A parallel worker must never create or register per-session storage:
	 * the storage map is backend-local (a worker cannot see the leader's
	 * entries or dirty local buffers), so any worker-side materialization
	 * would corrupt or duplicate the leader's session state.  But a worker
	 * may legitimately need only the relation's catalog metadata -- e.g.
	 * pg_get_expr() opens the relation to deparse a column default while
	 * pg_dump runs under debug_parallel_query.  Point the relcache entry at
	 * the session-local file in the leader's temp namespace, exactly as an
	 * untouched GTT looks in the leader, but without recording anything in
	 * the storage map.  No file is created here; if the leader never
	 * materialized the relation, reads short-circuit to empty.  The planner
	 * never makes a GTT a parallel baserel and DML is never parallelized, so
	 * a worker never actually scans or writes one.
	 */
	if (IsParallelWorker())
	{
		if (OidIsValid(relation->rd_rel->reltablespace))
			relation->rd_locator.spcOid = relation->rd_rel->reltablespace;
		else
			relation->rd_locator.spcOid = MyDatabaseTableSpace;
		relation->rd_locator.dbOid = MyDatabaseId;
		relation->rd_locator.relNumber = relation->rd_rel->relfilenode;
		relation->rd_backend = ProcNumberForTempRelations();
		return;
	}

	ensure_gtt_hash();

	entry = (GttStorageEntry *) hash_search(gtt_storage_hash,
											&relid,
											HASH_ENTER,
											&found);

	if (!found)
		gtt_init_entry(entry, relation);

	/*
	 * Refresh on_commit_delete from the catalog reloption.  rd_options is not
	 * populated on the very first call from heap_create, so the CREATE path
	 * initially leaves this flag cleared; a subsequent relcache build (after
	 * CCI during the same CREATE) supplies the reloption.
	 *
	 * The truncation itself is done from PreCommit_gtt_on_commit -- we do not
	 * register an OnCommitItem because heap_truncate's AccessExclusiveLock
	 * would conflict with peer sessions' session-level AccessShareLock on the
	 * same GTT.
	 */
	if (relation->rd_options != NULL &&
		relation->rd_rel->relkind == RELKIND_RELATION)
	{
		/*
		 * The relkind check matters: rd_options is only StdRdOptions for
		 * plain tables -- for other relkinds it can be a smaller
		 * kind-specific struct, and reading on_commit_delete from it would
		 * run off the end of the allocation.
		 */
		StdRdOptions *opts = (StdRdOptions *) relation->rd_options;

		entry->on_commit_delete = opts->on_commit_delete;
	}

	/*
	 * Remember the toast relation for heap entries, so the commit-time
	 * on-commit-delete truncation can reach it without catalog access.  As
	 * with on_commit_delete, rd_rel is not fully populated on the very first
	 * call during CREATE; later relcache builds fill it in.
	 */
	if (relation->rd_rel->relkind == RELKIND_RELATION &&
		OidIsValid(relation->rd_rel->reltoastrelid))
		entry->toast_relid = relation->rd_rel->reltoastrelid;

	/*
	 * RelationBuildLocalRelation (the path used by index_create) leaves
	 * rd_index NULL: pg_index has not yet been inserted, so the index access
	 * info cannot be filled in.  Our first call therefore left heap_relid as
	 * InvalidOid for indexes.  Backfill it now that
	 * RelationInitIndexAccessInfo has supplied rd_index, so
	 * PreCommit_gtt_on_commit can find which heap each index belongs to.
	 */
	if (entry->is_index && !OidIsValid(entry->heap_relid) &&
		relation->rd_index != NULL)
		entry->heap_relid = relation->rd_index->indrelid;

	/*
	 * Our hash entry tracks this session's current storage for the GTT.  It
	 * starts out equal to the catalog relfilenode, but a transactional
	 * TRUNCATE swaps in a new session-local relfilenumber via
	 * GttSetNewSessionRelfilenumber without touching the shared catalog, so
	 * the two may legitimately diverge.  CLUSTER, REINDEX, SET TABLESPACE,
	 * SET LOGGED and heap rewrites (which would rotate the shared relfilenode
	 * itself) remain blocked for GTTs.
	 */
	Assert(RelFileNumberIsValid(entry->locator.relNumber));

	/* Point the relation at our per-session storage */
	relation->rd_locator = entry->locator;
	relation->rd_backend = ProcNumberForTempRelations();

	/*
	 * No physical file is created here.  Reads of unmaterialized storage
	 * complete without one (the zero-blocks short-circuits in
	 * bufmgr.c/tableam.c report the relation empty), so the file is deferred
	 * to GttEnsureSessionStorage at the first genuine data access.
	 */
}

/*
 * gtt_init_entry
 *		Initialize a newly created per-session map entry from the relation's
 *		current catalog state.
 */
static void
gtt_init_entry(GttStorageEntry *entry, Relation relation)
{
	/*
	 * Set up the locator using the catalog's tablespace and database, but in
	 * this backend's temp namespace.
	 */
	if (relation->rd_rel->reltablespace)
		entry->locator.spcOid = relation->rd_rel->reltablespace;
	else
		entry->locator.spcOid = MyDatabaseTableSpace;

	if (entry->locator.spcOid == GLOBALTABLESPACE_OID)
		entry->locator.dbOid = InvalidOid;
	else
		entry->locator.dbOid = MyDatabaseId;

	/*
	 * Use the catalog relfilenode as the per-session relfilenode. Since each
	 * backend uses its own proc number as the backend ID in the file path
	 * (t_<procnum>_<relfilenode>), the same relfilenode won't collide between
	 * sessions.
	 */
	entry->locator.relNumber = relation->rd_rel->relfilenode;
	entry->storage_created = false;
	entry->is_index = (relation->rd_rel->relkind == RELKIND_INDEX);
	if (entry->is_index && relation->rd_index != NULL)
		entry->heap_relid = relation->rd_index->indrelid;
	else
		entry->heap_relid = InvalidOid;
	entry->index_built = false;
	entry->build_deferred = false;
	entry->drop_pending = false;
	entry->create_subid = GetCurrentSubTransactionId();
	gtt_xact_state_dirty = true;
	entry->storage_subid = InvalidSubTransactionId;
	entry->index_subid = InvalidSubTransactionId;
	entry->on_commit_delete = false;
	entry->toast_relid = InvalidOid;
}

/*
 * GttEnsureSessionStorage
 *		Materialize this session's physical storage for a GTT.
 *
 * Called the first time a GTT is genuinely accessed for data: it creates the
 * per-session file (registered for delete-at-abort via RelationCreateStorage).
 * Reads never call this -- an unmaterialized GTT reads as empty.
 */
void
GttEnsureSessionStorage(Relation relation)
{
	GttStorageEntry *entry;
	Oid			relid = RelationGetRelid(relation);

	Assert(RelationIsGlobalTemp(relation));

	if (gtt_storage_hash == NULL)
		elog(ERROR, "no per-session storage map for global temporary table \"%s\"",
			 RelationGetRelationName(relation));

	entry = (GttStorageEntry *) hash_search(gtt_storage_hash, &relid,
											HASH_FIND, NULL);
	if (entry == NULL)
		elog(ERROR, "no per-session storage entry for global temporary table \"%s\"",
			 RelationGetRelationName(relation));

	if (entry->storage_created)
		return;

	/*
	 * RelationCreateStorage cannot run in parallel mode (it couldn't update
	 * pendingSyncHash), and a worker must never materialize state in the
	 * leader's temp namespace; relcache builds in workers are already
	 * rejected in GttInitSessionStorage.
	 */
	if (IsInParallelMode())
		ereport(ERROR,
				errcode(ERRCODE_INVALID_TRANSACTION_STATE),
				errmsg("cannot initialize global temporary table storage during a parallel operation"));

	RelationCreateStorage(entry->locator, RELPERSISTENCE_GLOBAL_TEMP, true);
	entry->storage_created = true;
	entry->storage_subid = GetCurrentSubTransactionId();
	gtt_xact_state_dirty = true;

	/*
	 * When a heap materializes -- typically at the top of the first
	 * heap_insert, before the row is written -- bring its indexes along while
	 * the heap is still empty.  Opening each index runs the relation_open
	 * build hook, which now fires because the heap has storage.  Building
	 * here, rather than when index_insert first touches an index, is what
	 * keeps a lazy build from indexing the very row whose insertion triggered
	 * it (which the subsequent aminsert would then insert a second time).
	 */
	if (!entry->is_index &&
		relation->rd_rel->relkind != RELKIND_SEQUENCE)
	{
		List	   *indexoids = RelationGetIndexList(relation);

		foreach_oid(idxoid, indexoids)
		{
			Relation	idxrel = index_open(idxoid, AccessShareLock);

			index_close(idxrel, NoLock);
		}
		list_free(indexoids);
	}
}

/*
 * GttSetNewSessionRelfilenumber
 *		Point this session's storage mapping for a GTT at a new, empty file.
 *
 * Called from RelationSetNewRelfilenumber after it has created the new
 * per-session file and scheduled the old one for unlink-at-commit.  The
 * shared pg_class row is deliberately left untouched: other sessions derive
 * their private storage paths from the catalog relfilenode, so only this
 * session's mapping changes.
 *
 * The swap is transactional: an undo record restores the previous mapping
 * and the per-entry state reset here if the (sub)transaction aborts, while
 * the file-level rollback is handled by the PendingRelDelete entries the
 * caller registered.
 */
void
GttSetNewSessionRelfilenumber(Relation relation, RelFileNumber newrelfilenumber)
{
	GttStorageEntry *entry;
	GttSwapUndo *undo;
	MemoryContext oldcxt;
	Oid			relid = RelationGetRelid(relation);

	Assert(RelationIsGlobalTemp(relation));

	if (gtt_storage_hash == NULL)
		elog(ERROR, "no per-session storage map for global temporary table \"%s\"",
			 RelationGetRelationName(relation));

	entry = (GttStorageEntry *) hash_search(gtt_storage_hash, &relid,
											HASH_FIND, NULL);
	if (entry == NULL)
		elog(ERROR, "no per-session storage entry for global temporary table \"%s\"",
			 RelationGetRelationName(relation));

	/*
	 * Swaps only ever apply to materialized storage: TRUNCATE skips
	 * unmaterialized relations, and sequences are materialized at open. This
	 * matters because the swap does not register in the sessions registry --
	 * it relies on GttEnsureSessionStorage having done so.
	 */
	Assert(entry->storage_created);

	/* Push the undo record before changing anything. */
	oldcxt = MemoryContextSwitchTo(TopMemoryContext);
	undo = palloc_object(GttSwapUndo);
	undo->relid = relid;
	undo->subid = GetCurrentSubTransactionId();
	undo->prev_relnumber = entry->locator.relNumber;
	undo->prev_index_built = entry->index_built;
	undo->prev_build_deferred = entry->build_deferred;
	gtt_swap_undo = lcons(undo, gtt_swap_undo);
	MemoryContextSwitchTo(oldcxt);

	entry->locator.relNumber = newrelfilenumber;
	entry->storage_created = true;

	/*
	 * The new file is empty: indexes must be lazily rebuilt on next access
	 * (GttBuildIndexIfNeeded).
	 */
	if (entry->is_index)
	{
		entry->index_built = false;

		/*
		 * For an index created earlier in this same transaction, the usual
		 * assumption that index_create() handles the initial build no longer
		 * holds: that build went into the file being swapped out.  Record the
		 * build as genuinely outstanding so gtt_build_index_internal rebuilds
		 * into the new empty file on next access.
		 */
		if (relation->rd_createSubid != InvalidSubTransactionId)
			entry->build_deferred = true;
	}

	/* Point the open relcache entry at the new storage. */
	relation->rd_locator = entry->locator;
	RelationCloseSmgr(relation);
}

/*
 * gtt_swap_undo_apply
 *		Restore the session-local state captured by one swap-undo record.
 *
 * The file created by the swap is unlinked, and the pre-swap file's
 * unlink-at-commit canceled, by the PendingRelDelete machinery; here we
 * restore the mapping and per-entry bookkeeping to match.
 */
static void
gtt_swap_undo_apply(GttSwapUndo *undo)
{
	GttStorageEntry *entry;

	entry = (GttStorageEntry *) hash_search(gtt_storage_hash, &undo->relid,
											HASH_FIND, NULL);
	if (entry == NULL)
		return;					/* entry itself is being removed by abort */

	entry->locator.relNumber = undo->prev_relnumber;
	entry->index_built = undo->prev_index_built;
	entry->build_deferred = undo->prev_build_deferred;

	/*
	 * Refresh the relcache entry so rd_locator points back at the surviving
	 * pre-swap file on next access.
	 */
	RelationCacheInvalidateEntry(undo->relid);
}

/*
 * GttHasSessionStorage
 *		Check if the current session has initialized storage for a GTT.
 *
 * Used by pg_relation_filepath to decide whether to surface the current
 * session's private file path for a GTT (or NULL when this session has
 * not yet accessed it).
 */
bool
GttHasSessionStorage(Oid relid)
{
	if (gtt_storage_hash == NULL)
		return false;

	return hash_search(gtt_storage_hash, &relid, HASH_FIND, NULL) != NULL;
}

/*
 * GttSessionIndexUsable
 *		Is this session's copy of a GTT index materialized AND built?
 *
 * Readers of index structure that bypass the index AM's own access paths
 * -- the plan-time metapage peeks (_bt_getrootheight, ginGetStats,
 * brinGetStats), amcanreturn, and diagnostic readers like pgstattuple --
 * must treat an index that is materialized but not built as empty rather
 * than read pages that may not exist.  That state is reachable: a swap
 * (TRUNCATE) points the mapping at a fresh zero-block file, and the abort
 * pass that reverts a heap's storage physically empties its surviving
 * indexes (gtt_truncate_dependents), both clearing index_built so the next
 * genuine index access rebuilds the structure.  Plan-time readers must not
 * be the ones to trigger that rebuild.
 */
bool
GttSessionIndexUsable(Oid relid)
{
	GttStorageEntry *entry;

	if (gtt_storage_hash == NULL)
		return false;

	entry = (GttStorageEntry *) hash_search(gtt_storage_hash, &relid,
											HASH_FIND, NULL);
	return entry != NULL && entry->storage_created && entry->index_built;
}

/*
 * gtt_remove_entry
 *		Release per-session state for a GTT and remove its hash entry.
 *
 * This is the common teardown path invoked from gtt_session_cleanup,
 * from the commit-side of a scheduled drop, and from abort handling when
 * an entry was created in the aborting (sub)transaction.  Physical file
 * removal via PendingRelDelete is handled separately (by smgr's abort
 * cleanup or RelationDropStorage).
 */
static void
gtt_remove_entry(GttStorageEntry *entry)
{
	Oid			relid = entry->relid;

	hash_search(gtt_storage_hash, &relid, HASH_REMOVE, NULL);
}

/*
 * GttScheduleDropSessionStorage
 *		Mark a GTT's per-session entry for cleanup at xact commit.
 *
 * Called from heap_drop_with_catalog when a DROP TABLE runs on a GTT.
 * The actual removal of the hash entry happens when the transaction
 * commits (see gtt_xact_callback).  If the transaction aborts, the drop
 * is abandoned and the entry stays.
 *
 * Physical file removal is handled by RelationDropStorage (via the
 * standard PendingRelDelete mechanism).  This function only coordinates
 * the session-local metadata.
 */
void
GttScheduleDropSessionStorage(Oid relid)
{
	GttStorageEntry *entry;

	if (gtt_storage_hash == NULL)
		return;

	entry = (GttStorageEntry *) hash_search(gtt_storage_hash,
											&relid,
											HASH_FIND,
											NULL);
	if (entry != NULL)
	{
		entry->drop_pending = true;
		gtt_xact_state_dirty = true;
	}
}

/*
 * gtt_revert_storage
 *		Undo lazily-created storage state on (sub)transaction abort.
 *
 * The files themselves have been unlinked by PendingRelDelete; reset the
 * bookkeeping so the next access re-creates the storage, and restart the
 * xid horizon tracking, since no data survives.  The caller must also
 * remove the relation from the shared sessions registry (we cannot take
 * the registry LWLock here, mid-hash-scan); with the storage gone there
 * is no live data left for peer DDL to respect.
 */
static void
gtt_revert_storage(GttStorageEntry *entry)
{
	entry->storage_created = false;
	entry->storage_subid = InvalidSubTransactionId;

	/*
	 * No storage means no index structure: any build this entry ever had
	 * lived in the files just unlinked.  This must be enforced here rather
	 * than left to the index_subid bookkeeping, because under nested aborts a
	 * swap-undo record from an outer subtransaction can re-restore
	 * index_built=true after an inner subtransaction's abort already cleared
	 * index_subid -- leaving a "built" index with no file behind it.
	 */
	entry->index_built = false;
	entry->index_subid = InvalidSubTransactionId;

	/*
	 * Reverting an index's storage also revives its outstanding-build mark:
	 * if the index was created in this same transaction, the build that
	 * index_create (or a deferred-build hook) performed went down with the
	 * reverted file, and gtt_build_index_internal's index_create skip would
	 * otherwise block the rebuild on the next materialization.  Harmless for
	 * pre-existing indexes, whose rebuild never consults the mark.
	 */
	if (entry->is_index)
		entry->build_deferred = true;
}

/*
 * gtt_remove_relids
 *		Remove the hash entries named by a list of relation OIDs.
 *
 * Shared tail of the xact and subxact callbacks: victims are collected
 * during the hash scan (hash_seq_search is fragile if the current entry is
 * deleted) and removed here afterwards.
 */
static void
gtt_remove_relids(List *to_remove)
{
	GttStorageEntry *entry;

	foreach_oid(relid, to_remove)
	{
		entry = (GttStorageEntry *) hash_search(gtt_storage_hash,
												&relid,
												HASH_FIND,
												NULL);
		if (entry != NULL)
			gtt_remove_entry(entry);
	}
	list_free(to_remove);
}

/*
 * gtt_xact_callback
 *		Reconcile gtt_storage_hash with transaction completion.
 *
 * On top-level commit: finalise any scheduled drops (remove entries) and
 * clear per-entry subxact state, since the surviving entries are now
 * permanent for the session.
 *
 * On top-level abort: roll back any state that was established by the
 * aborting transaction.  Entries created in this xact are removed;
 * entries whose storage was established in this xact have storage_created
 * cleared — PendingRelDelete has already unlinked the associated files.
 */
static void
gtt_xact_callback(XactEvent event, void *arg)
{
	HASH_SEQ_STATUS status;
	GttStorageEntry *entry;
	List	   *to_remove = NIL;
	List	   *to_invalidate = NIL;
	ListCell   *lc;

	if (gtt_storage_hash == NULL)
		return;

	if (event != XACT_EVENT_COMMIT && event != XACT_EVENT_ABORT &&
		event != XACT_EVENT_PARALLEL_COMMIT &&
		event != XACT_EVENT_PARALLEL_ABORT)
		return;

	/*
	 * The common case is a transaction that touched no GTT state at all;
	 * don't pay for a full-hash scan at every commit for the rest of the
	 * session's life just because a GTT was once used.
	 */
	if (!gtt_xact_state_dirty && gtt_swap_undo == NIL)
		return;

	/*
	 * Settle the relfilenumber-swap undo log first.  On abort, restore the
	 * pre-swap state, newest record first, so that the oldest record (the
	 * state from before the transaction's first swap) lands last; this must
	 * run before the storage_subid processing below so that storage created
	 * and then swapped within the aborting transaction still ends up with
	 * storage_created cleared.  On commit the swaps are final and the records
	 * are simply discarded.
	 */
	if (gtt_swap_undo != NIL)
	{
		if (event == XACT_EVENT_ABORT || event == XACT_EVENT_PARALLEL_ABORT)
		{
			foreach(lc, gtt_swap_undo)
				gtt_swap_undo_apply((GttSwapUndo *) lfirst(lc));
		}
		list_free_deep(gtt_swap_undo);
		gtt_swap_undo = NIL;
	}

	hash_seq_init(&status, gtt_storage_hash);
	while ((entry = (GttStorageEntry *) hash_seq_search(&status)) != NULL)
	{
		bool		remove = false;

		if (event == XACT_EVENT_COMMIT ||
			event == XACT_EVENT_PARALLEL_COMMIT)
		{
			if (entry->drop_pending)
				remove = true;
			else
			{
				entry->create_subid = InvalidSubTransactionId;
				entry->storage_subid = InvalidSubTransactionId;
				entry->index_subid = InvalidSubTransactionId;
			}
		}
		else
		{
			/*
			 * Top-level abort.  Either remove the entry (if it was created in
			 * this xact) or clear storage_created (if storage was created in
			 * this xact).  In both cases the per-session file has been
			 * unlinked by PendingRelDelete and the relcache still has a
			 * cached rd_locator pointing at it; force a relcache invalidation
			 * so the next access re-runs RelationInitPhysicalAddr ->
			 * GttInitSessionStorage and recreates the storage.
			 */
			if (entry->create_subid != InvalidSubTransactionId)
			{
				remove = true;
				to_invalidate = lappend_oid(to_invalidate, entry->relid);
			}
			else
			{
				if (entry->storage_subid != InvalidSubTransactionId)
				{
					gtt_revert_storage(entry);
					to_invalidate = lappend_oid(to_invalidate, entry->relid);
				}
				if (entry->index_subid != InvalidSubTransactionId)
				{
					entry->index_built = false;
					entry->index_subid = InvalidSubTransactionId;
				}
				entry->drop_pending = false;
			}
		}

		if (remove)
			to_remove = lappend_oid(to_remove, entry->relid);
	}

	gtt_remove_relids(to_remove);

	foreach(lc, to_invalidate)
		RelationCacheInvalidateEntry(lfirst_oid(lc));
	list_free(to_invalidate);

	/* every entry has now been settled */
	gtt_xact_state_dirty = false;
}

/*
 * gtt_subxact_callback
 *		Reconcile gtt_storage_hash with subtransaction completion.
 *
 * On subxact commit, reparent subxact-tagged state to the parent.  On
 * subxact abort, revert state established in the aborting subxact: whole
 * entry for newly-created ones, storage_created for older entries that
 * had new storage created in the aborting subxact.
 */
static void
gtt_subxact_callback(SubXactEvent event,
					 SubTransactionId mySubid,
					 SubTransactionId parentSubid,
					 void *arg)
{
	HASH_SEQ_STATUS status;
	GttStorageEntry *entry;
	List	   *to_remove = NIL;
	List	   *to_invalidate = NIL;
	ListCell   *lc;

	if (gtt_storage_hash == NULL)
		return;

	if (event != SUBXACT_EVENT_COMMIT_SUB && event != SUBXACT_EVENT_ABORT_SUB)
		return;

	/* As in gtt_xact_callback, skip the scans if nothing can need work. */
	if (!gtt_xact_state_dirty && gtt_swap_undo == NIL)
		return;

	/*
	 * Settle relfilenumber-swap undo records belonging to this subxact: on
	 * commit reparent them, on abort restore the pre-swap state and discard
	 * them.  As in gtt_xact_callback, restoring must precede the
	 * storage_subid processing below.
	 */
	foreach(lc, gtt_swap_undo)
	{
		GttSwapUndo *undo = (GttSwapUndo *) lfirst(lc);

		if (undo->subid != mySubid)
			continue;

		if (event == SUBXACT_EVENT_COMMIT_SUB)
			undo->subid = parentSubid;
		else
		{
			gtt_swap_undo_apply(undo);
			gtt_swap_undo = foreach_delete_current(gtt_swap_undo, lc);
			pfree(undo);
		}
	}

	hash_seq_init(&status, gtt_storage_hash);
	while ((entry = (GttStorageEntry *) hash_seq_search(&status)) != NULL)
	{
		if (event == SUBXACT_EVENT_COMMIT_SUB)
		{
			if (entry->create_subid == mySubid)
				entry->create_subid = parentSubid;
			if (entry->storage_subid == mySubid)
				entry->storage_subid = parentSubid;
			if (entry->index_subid == mySubid)
				entry->index_subid = parentSubid;
		}
		else					/* SUBXACT_EVENT_ABORT_SUB */
		{
			if (entry->create_subid == mySubid)
			{
				to_remove = lappend_oid(to_remove, entry->relid);
				to_invalidate = lappend_oid(to_invalidate, entry->relid);
				continue;
			}
			if (entry->storage_subid == mySubid)
			{
				gtt_revert_storage(entry);
				to_invalidate = lappend_oid(to_invalidate, entry->relid);
			}
			if (entry->index_subid == mySubid)
			{
				entry->index_built = false;
				entry->index_subid = InvalidSubTransactionId;
			}
		}
	}

	gtt_remove_relids(to_remove);

	/* See gtt_xact_callback: invalidate relcache for any killed storage. */
	foreach(lc, to_invalidate)
		RelationCacheInvalidateEntry(lfirst_oid(lc));
	list_free(to_invalidate);
}

/*
 * gtt_truncate_smgr
 *		Truncate one entry's per-session storage to zero blocks via smgr.
 *
 * We cannot call RelationTruncate (which requires a Relation) because
 * opening relations during commit-time hooks corrupts the relcache state
 * that subsequent xacts rely on for DROP TABLE.  Truncating directly via
 * smgr is sufficient: the storage is per-session and not visible to any
 * other backend, so neither the AccessExclusiveLock RelationTruncate
 * documents nor the relcache inval message it sends are needed for
 * correctness here.
 *
 * The btree _bt_getroot fast path keeps a copy of the metapage in
 * rd_amcache; that cache is dropped lazily by GttBuildIndexIfNeeded the
 * next time the index is opened (added in a later commit), so we do not
 * touch it here.
 */
static void
gtt_truncate_smgr(GttStorageEntry *entry)
{
	SMgrRelation reln;
	ForkNumber	forks[MAX_FORKNUM + 1];
	BlockNumber old_blocks[MAX_FORKNUM + 1];
	BlockNumber new_blocks[MAX_FORKNUM + 1];
	int			nforks = 0;

	if (!entry->storage_created)
		return;

	reln = smgropen(entry->locator, ProcNumberForTempRelations());

	/* tolerate an already-vanished file (defense in depth) */
	if (!smgrexists(reln, MAIN_FORKNUM))
		return;

	forks[nforks] = MAIN_FORKNUM;
	old_blocks[nforks] = smgrnblocks(reln, MAIN_FORKNUM);
	new_blocks[nforks] = 0;
	nforks++;

	if (smgrexists(reln, FSM_FORKNUM))
	{
		forks[nforks] = FSM_FORKNUM;
		old_blocks[nforks] = smgrnblocks(reln, FSM_FORKNUM);
		new_blocks[nforks] = 0;
		nforks++;
	}
	if (smgrexists(reln, VISIBILITYMAP_FORKNUM))
	{
		forks[nforks] = VISIBILITYMAP_FORKNUM;
		old_blocks[nforks] = smgrnblocks(reln, VISIBILITYMAP_FORKNUM);
		new_blocks[nforks] = 0;
		nforks++;
	}

	/*
	 * Skip the truncation entirely if every fork is already empty: there is
	 * then nothing in the local buffer pool for this relation either, so
	 * smgrtruncate's buffer-drop pass and sinval message would be pure
	 * overhead.  This matters because PreCommit_gtt_on_commit re-truncates
	 * every ON COMMIT DELETE ROWS GTT the session has opened, at every
	 * qualifying commit, written-to or not.
	 */
	while (nforks > 0 && old_blocks[nforks - 1] == 0)
		nforks--;
	if (nforks == 0)
		return;

	smgrtruncate(reln, forks, nforks, old_blocks, new_blocks);
}

/*
 * gtt_build_index_internal
 *		Build a GTT index for this session if it hasn't been built yet.
 *
 * Per-session index storage starts out unmaterialized; indexes additionally
 * need their internal structure initialized (e.g. btree metapage) before
 * the first access.  When "force" is true (index scans and index inserts),
 * the index storage is materialized and built unconditionally; when false
 * (relation_open of the index, for direct-readers like pgstattuple), the
 * build only proceeds if the parent heap already has materialized storage,
 * so that merely opening an index -- e.g. the planner's get_relation_info
 * during EXPLAIN -- materializes nothing.
 *
 * The build scans the heap through the ordinary table-AM path, so an
 * unmaterialized heap simply contributes zero rows (via the zero-blocks
 * short-circuits) and stays unmaterialized.
 */
static void
gtt_build_index_internal(Relation indexRelation, bool force)
{
	GttStorageEntry *entry;
	Oid			relid = RelationGetRelid(indexRelation);
	Relation	heapRelation = NULL;

	if (gtt_storage_hash == NULL)
		return;

	/* Prevent recursive builds (index_build may trigger index_open) */
	if (gtt_building_index)
		return;

	entry = (GttStorageEntry *) hash_search(gtt_storage_hash,
											&relid,
											HASH_FIND,
											NULL);

	if (entry == NULL || !entry->is_index)
		return;

	if (entry->index_built)
	{
		/* the structure this flag promises must actually exist */
		Assert(smgrexists(smgropen(entry->locator,
								   ProcNumberForTempRelations()),
						  MAIN_FORKNUM) &&
			   smgrnblocks(smgropen(entry->locator,
									ProcNumberForTempRelations()),
						   MAIN_FORKNUM) > 0);
		return;
	}

	/*
	 * If the index was created in the current transaction, index_create()
	 * normally handles the initial build via index_build(); skip the build
	 * here to avoid a "already contains data" error from btbuild (this hook
	 * fires from relation_open inside index_create, and again from the
	 * index_open in plan_create_index_workers after index_build has already
	 * materialized the storage).  The exception is an index whose build is
	 * recorded as genuinely outstanding (build_deferred): either index_build
	 * explicitly deferred it because the parent heap was unmaterialized, or a
	 * TRUNCATE swapped the already-built index to a fresh empty file after
	 * index_create finished.  In both cases this hook is the only thing that
	 * will ever (re)build the index.
	 */
	if (indexRelation->rd_createSubid != InvalidSubTransactionId &&
		!entry->build_deferred)
		return;

	if (!force)
	{
		GttStorageEntry *heap_entry;

		if (!OidIsValid(entry->heap_relid))
			return;
		heap_entry = (GttStorageEntry *) hash_search(gtt_storage_hash,
													 &entry->heap_relid,
													 HASH_FIND, NULL);
		if (heap_entry == NULL || !heap_entry->storage_created)
			return;
	}

	/*
	 * If the index already has blocks (e.g. it was created by this same
	 * session via CREATE INDEX), it's already been built — just mark it.
	 * Leave index_subid invalid: the file's content predates the current
	 * transaction and survives its abort, so this discovery must not be
	 * rolled back (else an abort of a transaction that merely opened the
	 * index would force a pointless rebuild).
	 */
	if (entry->storage_created &&
		RelationGetNumberOfBlocks(indexRelation) != 0)
	{
		entry->index_built = true;
		entry->build_deferred = false;
		return;
	}

	/* The build is about to write; materialize the index storage. */
	GttEnsureSessionStorage(indexRelation);

	/*
	 * Drop any AM-specific cache before rebuilding.  The btree _bt_getroot
	 * fast path keeps a copy of the metapage in rd_amcache and uses
	 * btm_fastroot without rereading; if PreCommit_gtt_on_commit truncated
	 * the index file to zero blocks, that cached block number now points past
	 * EOF and the next access would fail.  Clearing the cache forces the
	 * post-rebuild metapage to be reread.
	 */
	if (indexRelation->rd_amcache != NULL)
	{
		pfree(indexRelation->rd_amcache);
		indexRelation->rd_amcache = NULL;
	}

	/*
	 * Build the index.  Open the heap table, construct the IndexInfo, and
	 * call ambuild directly.  We use ambuild instead of index_build because
	 * index_build calls index_update_stats which would update the shared
	 * pg_class entry — inappropriate for a per-session lazy index build.
	 *
	 * For an empty heap, this just initializes the index structure (e.g.
	 * writes the btree metapage).
	 *
	 * Set the guard flag to prevent recursive index builds, since ambuild may
	 * trigger relcache invalidation that leads back to index_open.
	 */
	gtt_building_index = true;
	PG_TRY();
	{
		IndexInfo  *indexInfo;

		heapRelation = table_open(indexRelation->rd_index->indrelid,
								  AccessShareLock);
		indexInfo = BuildIndexInfo(indexRelation);
		indexRelation->rd_indam->ambuild(heapRelation, indexRelation,
										 indexInfo);
	}
	PG_FINALLY();
	{
		if (heapRelation != NULL)
			table_close(heapRelation, AccessShareLock);
		gtt_building_index = false;
	}
	PG_END_TRY();

	/*
	 * Re-fetch the hash entry after ambuild, because the hash table may have
	 * been resized during the build (e.g. if opening the heap triggered
	 * GttInitSessionStorage for other relations).  A concurrent relcache
	 * invalidation in ambuild could in principle have dropped the entry, so
	 * cope with NULL rather than asserting.
	 */
	entry = (GttStorageEntry *) hash_search(gtt_storage_hash,
											&relid,
											HASH_FIND,
											NULL);
	if (entry != NULL)
	{
		entry->index_built = true;
		entry->build_deferred = false;
		entry->index_subid = GetCurrentSubTransactionId();
		gtt_xact_state_dirty = true;
	}
}

/*
 * GttBuildIndexIfNeeded
 *		Opportunistically build a GTT index at relation open.
 *
 * Builds only when the parent heap already has materialized storage, so
 * opening an index (planning, EXPLAIN) never materializes anything by
 * itself, while direct readers such as pgstattuple still find a usable
 * index whenever there is data to inspect.
 */
void
GttBuildIndexIfNeeded(Relation indexRelation)
{
	gtt_build_index_internal(indexRelation, false);
}

/*
 * GttMarkIndexBuildDeferred
 *		Record that index_build deferred this index's physical build.
 *
 * Called when CREATE INDEX (or any other index_build) runs while the parent
 * heap is unmaterialized: the catalog work proceeds, but no per-session
 * structure is built.  The mark tells gtt_build_index_internal that the
 * build for this same-transaction-created index is genuinely outstanding,
 * overriding its usual assumption that index_create() will take care of a
 * just-created index.
 */
void
GttMarkIndexBuildDeferred(Relation indexRelation)
{
	GttStorageEntry *entry;
	Oid			relid = RelationGetRelid(indexRelation);

	if (gtt_storage_hash == NULL)
		return;

	entry = (GttStorageEntry *) hash_search(gtt_storage_hash, &relid,
											HASH_FIND, NULL);
	if (entry != NULL && entry->is_index && !entry->index_built)
		entry->build_deferred = true;
}

/*
 * GttPrepareIndexAccess
 *		Make a GTT index usable before a scan or insert.
 *
 * Index scans and index inserts genuinely access the index structure, so
 * the per-session index storage is materialized and built here if needed.
 * The parent heap is not touched: building over an unmaterialized heap
 * yields an empty (but structurally valid) index.
 */
void
GttPrepareIndexAccess(Relation indexRelation)
{
	gtt_build_index_internal(indexRelation, true);
}

/*
 * PreCommit_gtt_on_commit
 *		Truncate ON COMMIT DELETE ROWS GTTs at commit.
 *
 * Generic on-commit truncation in PreCommit_on_commit_actions cannot be
 * used for GTTs: heap_truncate's AccessExclusiveLock would block on peers'
 * ordinary transaction-level locks at every commit, and opening the relation
 * via table_open at commit-time -- even with NoLock -- destabilises the
 * relcache enough to break a subsequent DROP TABLE in the next xact.  So
 * we register no OnCommitItem for GTTs (heap_create_with_catalog
 * suppresses the generic registration; see register_on_commit_action()
 * callers in heap.c) and truncate each session's local storage here
 * directly through smgr, using the per-session locator that
 * GttInitSessionStorage already recorded in our hash.
 */
void
PreCommit_gtt_on_commit(void)
{
	HASH_SEQ_STATUS status;
	GttStorageEntry *entry;
	List	   *heap_relids = NIL;

	if (gtt_storage_hash == NULL)
		return;

	/*
	 * Match PreCommit_on_commit_actions's optimisation: skip when no temp
	 * namespace was accessed in this xact, since any GTT we have storage for
	 * is necessarily empty.
	 */
	if (!(MyXactFlags & XACT_FLAGS_ACCESSEDTEMPNAMESPACE))
		return;

	/* First pass: identify ON COMMIT DELETE ROWS heaps to truncate. */
	hash_seq_init(&status, gtt_storage_hash);
	while ((entry = (GttStorageEntry *) hash_seq_search(&status)) != NULL)
	{
		if (entry->is_index)
			continue;
		if (!entry->on_commit_delete || !entry->storage_created)
			continue;

		/*
		 * A heap whose main fork is already empty has not been written since
		 * its last truncation; skip it -- and thereby its indexes and toast
		 * -- so that an idle ON COMMIT DELETE ROWS table costs each commit no
		 * more than this block-count probe.  This also keeps an index that
		 * was lazily built against the empty heap intact, rather than
		 * truncating and rebuilding it at every commit.
		 */
		if (smgrnblocks(smgropen(entry->locator, ProcNumberForTempRelations()),
						MAIN_FORKNUM) == 0)
			continue;

		heap_relids = lappend_oid(heap_relids, entry->relid);

		/*
		 * Queue the toast relation too (if this session ever wrote toasted
		 * values, an entry for it exists); its index is then matched by the
		 * heap_relids check in the second pass like any other index.
		 */
		if (OidIsValid(entry->toast_relid))
			heap_relids = lappend_oid(heap_relids, entry->toast_relid);
	}

	if (heap_relids == NIL)
		return;

	/*
	 * Second pass: truncate the heap entries, plus every index entry whose
	 * parent heap is in heap_relids, and clear the per-session metadata tied
	 * to each.  Index AM caches (eg btree's rd_amcache) are dropped lazily by
	 * GttBuildIndexIfNeeded the next time the index is opened, so we don't
	 * have to invalidate the relcache here.
	 *
	 * Toast tables are truncated along with their parents: each heap entry
	 * records its toast relation's OID (captured from the relcache in
	 * GttInitSessionStorage), so the toast heap is in heap_relids and its
	 * index is caught by the matching below, all without any catalog access
	 * from this commit-time hook.
	 */
	hash_seq_init(&status, gtt_storage_hash);
	while ((entry = (GttStorageEntry *) hash_seq_search(&status)) != NULL)
	{
		if (entry->is_index)
		{
			if (OidIsValid(entry->heap_relid) &&
				list_member_oid(heap_relids, entry->heap_relid))
			{
				gtt_truncate_smgr(entry);
				entry->index_built = false;
			}
		}
		else if (list_member_oid(heap_relids, entry->relid))
			gtt_truncate_smgr(entry);
	}

	list_free(heap_relids);
}

/*
 * GttResetAllSessionData
 *		Clear this session's data in every global temporary table it has
 *		touched, for DISCARD TEMP / DISCARD ALL.
 *
 * Regular temporary tables are dropped outright by DISCARD TEMP; a GTT's
 * definition is shared and must survive, but its per-session contents are
 * session state and are cleared here.  This matters especially for
 * connection poolers, which rely on DISCARD ALL to prevent one client's
 * session state from leaking to the next.
 *
 * Tables are truncated with the transaction-safe session-storage swap
 * (GttTruncateInSession), and sequences are reset to their start value
 * (ResetSequence, which also swaps session storage for a GTT sequence), so
 * a DISCARD TEMP inside a transaction block is rolled back cleanly if the
 * transaction aborts -- matching the transactional drop of regular temp
 * tables.
 */
void
GttResetAllSessionData(void)
{
	HASH_SEQ_STATUS status;
	GttStorageEntry *entry;
	List	   *relids = NIL;
	Relation	rel;

	if (gtt_storage_hash == NULL)
		return;

	/* Collect first: truncation work must not run under an active scan. */
	hash_seq_init(&status, gtt_storage_hash);
	while ((entry = (GttStorageEntry *) hash_seq_search(&status)) != NULL)
	{
		if (entry->is_index || !entry->storage_created)
			continue;
		relids = lappend_oid(relids, entry->relid);
	}

	foreach_oid(relid, relids)
	{
		switch (get_rel_relkind(relid))
		{
			case RELKIND_RELATION:
				/*
				 * Same lock TRUNCATE takes; only this session's storage
				 * is affected, and peers hold no session-lifetime locks
				 * that could make this wait for their disconnect.
				 */
				rel = try_relation_open(relid, AccessExclusiveLock);
				if (rel == NULL)
					break;
				if (RelationIsGlobalTemp(rel))
					GttTruncateInSession(rel);

				/*
				 * hold the lock until end of transaction, as TRUNCATE
				 * does
				 */
				relation_close(rel, NoLock);
				break;
			case RELKIND_SEQUENCE:
				if (get_rel_persistence(relid) == RELPERSISTENCE_GLOBAL_TEMP)
					ResetSequence(relid);
				break;
			default:
				/* toast relations are reset along with their parents */
				break;
		}
	}
	list_free(relids);
}

/*
 * gtt_session_cleanup
 *		Drop all per-session GTT storage files at backend exit.
 *
 * This runs from before_shmem_exit.  Entries scheduled for drop by a
 * committed DROP TABLE have already been removed by gtt_xact_callback,
 * so anything still in the hash represents live per-session storage
 * that should be unlinked.
 */
static void
gtt_session_cleanup(int code, Datum arg)
{
	HASH_SEQ_STATUS status;
	GttStorageEntry *entry;

	if (gtt_storage_hash == NULL)
		return;

	hash_seq_init(&status, gtt_storage_hash);
	while ((entry = (GttStorageEntry *) hash_seq_search(&status)) != NULL)
	{
		if (entry->storage_created)
		{
			SMgrRelation srel;

			srel = smgropen(entry->locator, ProcNumberForTempRelations());
			smgrdounlinkall(&srel, 1, false);
		}
	}
}
