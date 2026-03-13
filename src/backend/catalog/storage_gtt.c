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

#include "access/table.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "catalog/heap.h"
#include "catalog/pg_tablespace_d.h"
#include "catalog/storage.h"
#include "catalog/storage_gtt.h"
#include "common/hashfn.h"
#include "miscadmin.h"
#include "nodes/pg_list.h"
#include "storage/ipc.h"
#include "storage/bufmgr.h"
#include "storage/procnumber.h"
#include "storage/smgr.h"
#include "utils/hsearch.h"
#include "utils/inval.h"
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
 * On subxact or xact abort of a given subid, the corresponding state is
 * reverted.  On subxact commit, the subid is reparented.  See
 * gtt_subxact_callback / gtt_xact_callback.
 */
typedef struct GttStorageEntry
{
	Oid			relid;			/* GTT's pg_class OID (hash key) */
	Oid			toast_relid;	/* toast relation for heap entries, InvalidOid
								 * if none / not a heap */
	RelFileLocator locator;		/* per-session physical storage location */
	bool		storage_created;	/* has smgr file been created? */
	bool		on_commit_delete;	/* truncate data on commit? */
	bool		drop_pending;	/* entry scheduled for drop at xact commit */
	SubTransactionId create_subid;	/* subxact that added this entry */
	SubTransactionId storage_subid; /* subxact that created current storage */
} GttStorageEntry;

/* Backend-local hash table: GTT OID -> GttStorageEntry */
static HTAB *gtt_storage_hash = NULL;

/*
 * True when any entry carries rollback-sensitive state (a valid
 * create_subid/storage_subid, or drop_pending), letting the xact/subxact
 * callbacks skip their full-hash scans in the common case of a transaction
 * that established no such state.  Conservative: it is only cleared once a
 * top-level transaction end has settled every entry.
 */
static bool gtt_xact_state_dirty = false;

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
static void gtt_truncate_smgr(GttStorageEntry *entry);

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
	entry->drop_pending = false;
	entry->create_subid = GetCurrentSubTransactionId();
	gtt_xact_state_dirty = true;
	entry->storage_subid = InvalidSubTransactionId;
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

	RelationCreateStorage(entry->locator, RELPERSISTENCE_GLOBAL_TEMP, true);
	entry->storage_created = true;
	entry->storage_subid = GetCurrentSubTransactionId();
	gtt_xact_state_dirty = true;
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
 * bookkeeping so the next access re-creates the storage.
 */
static void
gtt_revert_storage(GttStorageEntry *entry)
{
	entry->storage_created = false;
	entry->storage_subid = InvalidSubTransactionId;
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
	if (!gtt_xact_state_dirty)
		return;

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
	if (!gtt_xact_state_dirty)
		return;

	hash_seq_init(&status, gtt_storage_hash);
	while ((entry = (GttStorageEntry *) hash_seq_search(&status)) != NULL)
	{
		if (event == SUBXACT_EVENT_COMMIT_SUB)
		{
			if (entry->create_subid == mySubid)
				entry->create_subid = parentSubid;
			if (entry->storage_subid == mySubid)
				entry->storage_subid = parentSubid;
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
	List	   *toast_relids = NIL;
	ListCell   *lc;

	if (gtt_storage_hash == NULL)
		return;

	/*
	 * Match PreCommit_on_commit_actions's optimisation: skip when no temp
	 * namespace was accessed in this xact, since any GTT we have storage for
	 * is necessarily empty.
	 */
	if (!(MyXactFlags & XACT_FLAGS_ACCESSEDTEMPNAMESPACE))
		return;

	hash_seq_init(&status, gtt_storage_hash);
	while ((entry = (GttStorageEntry *) hash_seq_search(&status)) != NULL)
	{
		if (!entry->on_commit_delete || !entry->storage_created)
			continue;

		/*
		 * A heap whose main fork is already empty has not been written since
		 * its last truncation; skip it -- and thereby its toast -- so that an
		 * idle ON COMMIT DELETE ROWS table costs each commit no more than
		 * this block-count probe.
		 */
		if (smgrnblocks(smgropen(entry->locator, ProcNumberForTempRelations()),
						MAIN_FORKNUM) == 0)
			continue;

		gtt_truncate_smgr(entry);

		/*
		 * Queue the toast relation too (if this session ever wrote toasted
		 * values, an entry for it exists).  Truncating just the heap would
		 * orphan the toast rows for good: nothing else ever deletes them, and
		 * autovacuum never visits GTTs.
		 */
		if (OidIsValid(entry->toast_relid))
			toast_relids = lappend_oid(toast_relids, entry->toast_relid);
	}

	foreach(lc, toast_relids)
	{
		Oid			toast_relid = lfirst_oid(lc);

		entry = (GttStorageEntry *) hash_search(gtt_storage_hash,
												&toast_relid,
												HASH_FIND, NULL);
		if (entry != NULL && entry->storage_created)
			gtt_truncate_smgr(entry);
	}
	list_free(toast_relids);
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
