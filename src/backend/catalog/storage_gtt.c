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

#include "access/tableam.h"
#include "access/xact.h"
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
	RelFileLocator locator;		/* per-session physical storage location */
	bool		storage_created;	/* has smgr file been created? */
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
