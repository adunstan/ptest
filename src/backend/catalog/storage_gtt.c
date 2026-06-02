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
#include "access/htup_details.h"
#include "access/parallel.h"
#include "access/relation.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/multixact.h"
#include "access/transam.h"
#include "access/xact.h"
#include "catalog/heap.h"
#include "catalog/index.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_statistic.h"
#include "catalog/pg_tablespace_d.h"
#include "catalog/storage.h"
#include "catalog/storage_gtt.h"
#include "commands/sequence.h"
#include "commands/tablecmds.h"
#include "common/hashfn.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/pg_list.h"
#include "storage/ipc.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "storage/lwlock.h"
#include "storage/procnumber.h"
#include "storage/shmem.h"
#include "storage/smgr.h"
#include "utils/acl.h"
#include "utils/array.h"
#include "utils/fmgroids.h"
#include "utils/builtins.h"
#include "utils/tuplestore.h"
#include "utils/hsearch.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/syscache.h"

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
 *   - stats_subid: subxact that last wrote per-session statistics
 *   - demat_subid: subxact whose DISCARD scheduled dematerialization
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
	bool		is_sequence;	/* is this a sequence relation? */
	bool		index_built;	/* has index been built in this session? */
	bool		build_deferred; /* index_build deferred the physical build
								 * because the parent heap was unmaterialized */
	bool		on_commit_delete;	/* truncate data on commit? */
	SubTransactionId drop_subid;	/* subxact that scheduled this entry's
									 * drop (DROP TABLE/INDEX); acted on at
									 * top-level commit */
	SubTransactionId create_subid;	/* subxact that added this entry */
	SubTransactionId storage_subid; /* subxact that created current storage */
	SubTransactionId index_subid;	/* subxact that built the index */
	SubTransactionId stats_subid;	/* subxact that last wrote session stats */
	SubTransactionId demat_subid;	/* subxact whose DISCARD scheduled
									 * dematerialization at commit */

	/* Per-session relation statistics (set by ANALYZE) */
	bool		stats_valid;	/* has ANALYZE been run in this session? */
	BlockNumber relpages;		/* per-session page count */
	float4		reltuples;		/* per-session tuple count */
	BlockNumber relallvisible;	/* per-session all-visible pages */

	/*
	 * Transaction-ID horizon tracking (heap entries only).  oldest_xid is a
	 * conservative lower bound on the oldest unfrozen xmin in this session's
	 * data for the relation: it is set to the current XID on the first write
	 * into otherwise-empty storage and reset to InvalidTransactionId whenever
	 * the storage is emptied (truncate / on-commit-delete / abort-discard).
	 * Later writes only ever use newer XIDs, and a tuple's xmin cannot be
	 * older than the storage's first write, so this never overstates how
	 * recent the data is.  xid_warned throttles the approaching-horizon
	 * WARNING to once per relation per session.  See GttPrepareAccess().
	 *
	 * oldest_xid doubles as the session-local relfrozenxid: a VACUUM of the
	 * GTT freezes this session's storage in place and advances oldest_xid to
	 * the oldest unfrozen XID it leaves behind (see
	 * GttUpdateSessionFrozenXids). session_relminmxid is the matching
	 * session-local relminmxid; it is set to the current next-multixact on
	 * the first write and advanced by VACUUM. Neither value can live in the
	 * shared pg_class row, which is common to all sessions.
	 */
	TransactionId oldest_xid;
	bool		xid_warned;
	MultiXactId session_relminmxid;
} GttStorageEntry;

/* Backend-local hash table: GTT OID -> GttStorageEntry */
static HTAB *gtt_storage_hash = NULL;

/*
 * True when any entry carries rollback-sensitive state (a valid
 * create_subid/storage_subid/index_subid, or drop_subid), letting the
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
	bool		prev_stats_valid;
	BlockNumber prev_relpages;
	float4		prev_reltuples;
	BlockNumber prev_relallvisible;
	TransactionId prev_oldest_xid;
	bool		prev_xid_warned;
	MultiXactId prev_session_relminmxid;
} GttSwapUndo;

/* List of GttSwapUndo *, newest first, allocated in TopMemoryContext */
static List *gtt_swap_undo = NIL;

/*
 * Per-session column-level statistics for global temporary tables.
 *
 * Column stats (histograms, MCVs, distinct counts, etc.) are stored as
 * pg_statistic-format HeapTuples in a separate backend-local hash table,
 * keyed by (relid, attnum, inh).  This parallels the relation-level stats
 * (relpages/reltuples) stored in GttStorageEntry above.
 *
 * The key has trailing alignment padding that HASH_BLOBS hashes verbatim,
 * so all key instances must be zero-initialized before the fields are set.
 * Always build keys via init_colstats_key() rather than by hand.
 */
typedef struct GttColStatsKey
{
	Oid			relid;			/* relation OID */
	AttrNumber	attnum;			/* attribute number */
	bool		inh;			/* include inheritance children? */
} GttColStatsKey;

typedef struct GttColStatsEntry
{
	GttColStatsKey key;			/* hash key — must be first */
	HeapTuple	statsTuple;		/* pg_statistic-format tuple in
								 * TopMemoryContext */
} GttColStatsEntry;

/* Backend-local hash table: (relid, attnum, inh) -> GttColStatsEntry */
static HTAB *gtt_colstats_hash = NULL;

/*
 * GUC: how many transactions of head room to leave before the cluster
 * CLOG-truncation horizon when warning that a GTT's data is aging toward
 * transaction-ID wraparound.  The hard error fires at the horizon itself;
 * this only controls the earlier WARNING.  Generous by default so operators
 * get ample notice.  See GttPrepareAccess().
 */
int			global_temp_xid_warn_margin = 100000000;

/* Guard against recursive index builds */
static bool gtt_building_index = false;

/*
 * Shared-memory session registry: a hash of (dbOid, relid, ProcNumber)
 * entries recording which backends currently have live per-session storage
 * for each GTT.  This is the sole cross-session DDL-safety mechanism:
 * GttCheckDroppable / GttCheckAlterable, called while the DDL session holds
 * AccessExclusiveLock, error out when any other backend appears here.  No
 * session-level heavyweight lock is taken for a GTT -- it would make every
 * AccessExclusiveLock acquisition block until the registered backends
 * disconnect, instead of failing (or proceeding, for TRUNCATE) promptly.
 *
 * The hash is sized at postmaster startup (see GttSessionsShmemRequest) and
 * protected by the predefined GttSessionsLock LWLock.  Entries are added on
 * first per-session access and removed on session cleanup / explicit drop.
 */

/*
 * Initial-size hint for the shared GTT Sessions hash: entries per backend.
 * Only used once, in GttSessionsShmemRequest.  Dynahash grows past this
 * limit on demand; the value is just a sizing guess to avoid early splits
 * on common workloads.
 */
#define GTT_SESSIONS_ENTRIES_PER_BACKEND 16

typedef struct GttSessionsKey
{
	Oid			dbOid;
	Oid			relid;
	ProcNumber	procnum;
} GttSessionsKey;

typedef struct GttSessionsEntry
{
	GttSessionsKey key;			/* also the hash key -- must come first */
	Oid			table_relid;	/* owning table: self for heaps, the parent
								 * heap for indexes.  Lets DDL checks on a
								 * table see sessions whose only materialized
								 * storage is one of its indexes. */
} GttSessionsEntry;

static HTAB *GttSessionsHash = NULL;

/* Local function prototypes */
static void gtt_session_cleanup(int code, Datum arg);
static void ensure_gtt_hash(void);
static void ensure_gtt_colstats_hash(void);
static void init_colstats_key(GttColStatsKey *key, Oid relid,
							  AttrNumber attnum, bool inh);
static void init_sessions_key(GttSessionsKey *key, Oid relid);
static void gtt_reset_colstats_for_rel(Oid relid);
static char *format_stats_values_as_text(AttStatsSlot *sslot);
static void gtt_xact_callback(XactEvent event, void *arg);
static void gtt_subxact_callback(SubXactEvent event,
								 SubTransactionId mySubid,
								 SubTransactionId parentSubid,
								 void *arg);
static void gtt_remove_entry(GttStorageEntry *entry);
static void gtt_revert_storage(GttStorageEntry *entry);
static void gtt_remove_relids(List *to_remove);
static void gtt_truncate_dependents(List *heap_relids);
static void gtt_truncate_smgr(GttStorageEntry *entry);
static void gtt_swap_undo_apply(GttSwapUndo *undo);
static void gtt_init_entry(GttStorageEntry *entry, Relation relation);
static void gtt_build_index_internal(Relation indexRelation, bool force);
#ifdef USE_ASSERT_CHECKING
static bool gtt_session_registered(Oid relid);
static void gtt_check_invariants(void);
#endif
static void gtt_sessions_add(Oid relid, Oid table_relid);
static void gtt_sessions_remove(Oid relid);
static ProcNumber gtt_first_other_session_with_storage(Oid relid);
static ProcNumber gtt_wait_other_sessions_gone(Oid relid);
static void GttSessionsShmemRequest(void *arg);

const ShmemCallbacks GttSessionsShmemCallbacks = {
	.request_fn = GttSessionsShmemRequest,
	/* no init_fn: the hash is populated lazily by backends */
};

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
 * ensure_gtt_colstats_hash
 *		Create the backend-local column stats hash table on first use.
 *
 * This is separate from ensure_gtt_hash() so the column stats hash is only
 * created when actually needed (during ANALYZE or planner lookup).
 */
static void
ensure_gtt_colstats_hash(void)
{
	HASHCTL		hashctl;

	if (gtt_colstats_hash != NULL)
		return;

	hashctl.keysize = sizeof(GttColStatsKey);
	hashctl.entrysize = sizeof(GttColStatsEntry);
	hashctl.hcxt = TopMemoryContext;
	gtt_colstats_hash = hash_create("GTT column stats hash",
									64, /* initial size */
									&hashctl,
									HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

/*
 * init_colstats_key
 *		Build a GttColStatsKey with deterministic byte contents.
 *
 * HASH_BLOBS hashes the key byte-for-byte, including any trailing
 * alignment padding the compiler may insert after the last field.
 * Zero the whole struct first so equivalent (relid, attnum, inh) triples
 * always produce the same hash.
 */
static void
init_colstats_key(GttColStatsKey *key, Oid relid, AttrNumber attnum, bool inh)
{
	memset(key, 0, sizeof(*key));
	key->relid = relid;
	key->attnum = attnum;
	key->inh = inh;
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
	else if (!entry->storage_created &&
			 (entry->locator.relNumber != relation->rd_rel->relfilenode ||
			  entry->is_index !=
			  (relation->rd_rel->relkind == RELKIND_INDEX)))
	{
		/*
		 * Stale entry: it caches a locator for a catalog row that no longer
		 * exists.  This can only happen for an entry without storage -- the
		 * sessions registry blocks DROP while any session has materialized
		 * storage -- e.g. this session merely planned a query against a GTT
		 * that a peer then dropped, and the OID has since been reused for a
		 * new GTT.  The entry holds no resources (no file, no registry row),
		 * so simply reinitialize it from the current catalog state.
		 */
		gtt_init_entry(entry, relation);
	}

	/*
	 * Refresh on_commit_delete from the catalog reloption.  rd_options is not
	 * populated on the very first call from heap_create, so the CREATE path
	 * initially leaves this flag cleared; a subsequent relcache build (after
	 * CCI during the same CREATE) supplies the reloption.
	 *
	 * The truncation itself is done from PreCommit_gtt_on_commit -- we do not
	 * register an OnCommitItem because heap_truncate would escalate to
	 * AccessExclusiveLock at every commit, blocking on (and conflicting with)
	 * peers' ordinary transaction-level locks even though only this session's
	 * private storage is affected.
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
	 * the two may legitimately diverge -- though only once storage has been
	 * materialized (an entry without storage was either just created or just
	 * refreshed above, so it must match the catalog).  CLUSTER, REINDEX, SET
	 * TABLESPACE, SET LOGGED and heap rewrites (which would rotate the shared
	 * relfilenode itself) remain blocked for GTTs.
	 */
	Assert(entry->storage_created ||
		   entry->locator.relNumber == relation->rd_rel->relfilenode);

	/* Point the relation at our per-session storage */
	relation->rd_locator = entry->locator;
	relation->rd_backend = ProcNumberForTempRelations();

	/*
	 * Note: no physical file is created here.  Reads of unmaterialized
	 * storage complete without one (the zero-blocks short-circuits in
	 * bufmgr.c/tableam.c report the relation empty), so the file -- and the
	 * sessions-registry entry that makes peer DDL respect our data -- are
	 * deferred to GttEnsureSessionStorage at the first genuine data access.
	 */
}

/*
 * gtt_init_entry
 *		(Re)initialize a per-session map entry from the relation's current
 *		catalog state.
 *
 * Used for newly created entries and to refresh a stale resource-less
 * entry whose OID has been recycled (see GttInitSessionStorage).
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
	entry->is_sequence = (relation->rd_rel->relkind == RELKIND_SEQUENCE);
	if (entry->is_index && relation->rd_index != NULL)
		entry->heap_relid = relation->rd_index->indrelid;
	else
		entry->heap_relid = InvalidOid;
	entry->index_built = false;
	entry->build_deferred = false;
	entry->drop_subid = InvalidSubTransactionId;
	entry->create_subid = GetCurrentSubTransactionId();
	gtt_xact_state_dirty = true;
	entry->storage_subid = InvalidSubTransactionId;
	entry->index_subid = InvalidSubTransactionId;
	entry->stats_subid = InvalidSubTransactionId;
	entry->demat_subid = InvalidSubTransactionId;
	entry->stats_valid = false;
	entry->relpages = 0;
	entry->reltuples = 0;
	entry->relallvisible = 0;
	entry->oldest_xid = InvalidTransactionId;
	entry->xid_warned = false;
	entry->session_relminmxid = InvalidMultiXactId;
	entry->on_commit_delete = false;
	entry->toast_relid = InvalidOid;

	/*
	 * Discard any column statistics recorded under this OID: when the entry
	 * is being refreshed after OID recycling, they describe a different,
	 * dropped relation.  (For a brand-new entry this is a no-op.)
	 */
	gtt_reset_colstats_for_rel(entry->relid);
}

/*
 * GttEnsureSessionStorage
 *		Materialize this session's storage for a GTT: create the per-session
 *		file and register in the shared sessions registry.
 *
 * Called at the first genuine data access (heap inserts, index builds,
 * sequence seeding), not at relation open: sessions that merely plan or
 * read a never-written GTT hold no file and no registry entry, so they
 * neither pay for storage nor block peer DDL.
 *
 * Registration happens here, with the file: "another session has live
 * per-session data" (GttCheckDroppable/GttCheckAlterable) is then
 * literally true.  We deliberately take no session-level lock on the GTT
 * -- a session-lifetime AccessShareLock would make any AccessExclusiveLock
 * acquisition (a peer's TRUNCATE of its own private data, or a DROP that
 * the registry would reject with a clean error) block until this backend
 * disconnects.  In-flight windows are covered by the ordinary
 * transaction-level lock our caller holds while we're added to the
 * registry.
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

	/*
	 * Create the file, then register so peer DDL sees our live data.
	 *
	 * For tables and indexes the file carries a delete-at-abort registration
	 * and the materialization is recorded in storage_subid, making it fully
	 * transactional.  Storage of a pre-existing sequence is deliberately NOT
	 * transactional: sequence advancement must survive the abort of whichever
	 * transaction happened to first materialize the sequence, or rolled-back
	 * nextval calls would hand out the same values again -- regular and local
	 * temporary sequences never replay values.  Such a file persists until
	 * session end, DISCARD, or DROP, and its entry is made permanent for the
	 * session (create_subid cleared) so the abort paths leave both alone.
	 * Only when the sequence's own defining CREATE is still in flight
	 * (rd_createSubid) is the storage transactional like everything else: if
	 * that CREATE aborts, the catalog row vanishes and the file must go with
	 * it.
	 */
	if (entry->is_sequence &&
		relation->rd_createSubid == InvalidSubTransactionId)
	{
		RelationCreateStorage(entry->locator, RELPERSISTENCE_GLOBAL_TEMP,
							  false);
		entry->create_subid = InvalidSubTransactionId;
	}
	else
	{
		RelationCreateStorage(entry->locator, RELPERSISTENCE_GLOBAL_TEMP,
							  true);
		if (!entry->is_sequence)
			entry->storage_subid = GetCurrentSubTransactionId();
	}
	entry->storage_created = true;
	gtt_xact_state_dirty = true;

	gtt_sessions_add(relid,
					 entry->is_index ? entry->heap_relid : relid);

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
	undo->prev_stats_valid = entry->stats_valid;
	undo->prev_relpages = entry->relpages;
	undo->prev_reltuples = entry->reltuples;
	undo->prev_relallvisible = entry->relallvisible;
	undo->prev_oldest_xid = entry->oldest_xid;
	undo->prev_xid_warned = entry->xid_warned;
	undo->prev_session_relminmxid = entry->session_relminmxid;
	gtt_swap_undo = lcons(undo, gtt_swap_undo);
	MemoryContextSwitchTo(oldcxt);

	entry->locator.relNumber = newrelfilenumber;
	entry->storage_created = true;

	/*
	 * The new file is empty: indexes must be lazily rebuilt on next access
	 * (GttBuildIndexIfNeeded), previous ANALYZE results no longer apply, and
	 * there is no unfrozen data left to track for wraparound purposes.
	 * Column-level statistics are left alone, matching the behavior of
	 * TRUNCATE on regular tables, which does not clear pg_statistic.
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
	entry->stats_valid = false;
	entry->relpages = 0;
	entry->reltuples = 0;
	entry->relallvisible = 0;
	entry->oldest_xid = InvalidTransactionId;
	entry->xid_warned = false;
	entry->session_relminmxid = InvalidMultiXactId;

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
	entry->stats_valid = undo->prev_stats_valid;
	entry->relpages = undo->prev_relpages;
	entry->reltuples = undo->prev_reltuples;
	entry->relallvisible = undo->prev_relallvisible;
	entry->oldest_xid = undo->prev_oldest_xid;
	entry->xid_warned = undo->prev_xid_warned;
	entry->session_relminmxid = undo->prev_session_relminmxid;

	/*
	 * Refresh the relcache entry so rd_locator points back at the surviving
	 * pre-swap file on next access.
	 */
	RelationCacheInvalidateEntry(undo->relid);
}

/*
 * GttHasSessionStorage
 *		Check if the current session has materialized storage for a GTT.
 *
 * True only once a per-session file actually exists.  Sessions that have
 * merely opened a GTT (planning, EXPLAIN, reading a never-written table)
 * hold a backend-local map entry but no file; for them this returns false.
 *
 * Used by pg_relation_filepath to decide whether to surface the current
 * session's private file path, and by the zero-blocks short-circuits in
 * bufmgr.c/tableam.c that let reads of unmaterialized storage complete
 * without any file.
 */
bool
GttHasSessionStorage(Oid relid)
{
	GttStorageEntry *entry;

	if (gtt_storage_hash == NULL)
		return false;

	entry = (GttStorageEntry *) hash_search(gtt_storage_hash, &relid,
											HASH_FIND, NULL);
	return entry != NULL && entry->storage_created;
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
 * GttPrepareAccessGuts
 *		Materialize storage for writes, and fail-closed guard against accessing global temporary table data whose
 *		oldest unfrozen xmin has aged toward the transaction-ID wraparound /
 *		CLOG-truncation horizon.
 *
 * A GTT contributes nothing to datfrozenxid (its shared pg_class row carries
 * invalid relfrozenxid/relminmxid), so CLOG can be truncated, and XID
 * assignment can advance, past a still-live GTT tuple's xmin unless this
 * session freezes its own data.  A long-lived session that retains data in an
 * ON COMMIT PRESERVE ROWS GTT can therefore reach a point where reading that
 * data either fails with a "could not access status of transaction" error or
 * is silently mis-judged once the xmin is ~2^31 in the past.  To prevent wrong
 * results, refuse access once the relation's oldest tracked xmin reaches the
 * cluster CLOG-truncation horizon (TransamVariables->oldestXid), and warn as
 * it approaches.
 *
 * The remedy is to VACUUM the table: that freezes this session's storage in
 * place and advances oldest_xid (the session relfrozenxid) past the horizon,
 * after which the guard stays quiet.  VACUUM does not go through the guarded
 * entry points, so it remains available even once reads here have started to
 * fail -- though if a tuple's commit status was never hinted and its CLOG is
 * already gone, even freezing cannot read it, and only TRUNCATE / reconnect
 * recovers.  This is why we warn well ahead of the horizon: VACUUM during the
 * warning window is the reliable escape.
 *
 * Called from the heap read and write entry points (see heapam.c / indexam.c)
 * whenever the relation is a global temporary table.  For inserts, records the
 * current XID as the relation's oldest tracked xmin the first time data is
 * added to empty storage.  Only heap entries are tracked; indexes are checked
 * via their heap relation at scan begin.
 */
void
GttPrepareAccessGuts(Relation rel, bool is_insert)
{
	GttStorageEntry *entry;
	Oid			relid = RelationGetRelid(rel);
	TransactionId frontier;

	if (gtt_storage_hash == NULL)
		return;

	entry = (GttStorageEntry *) hash_search(gtt_storage_hash, &relid,
											HASH_FIND, NULL);
	if (entry == NULL)
		return;

	/* the file the mapping promises must actually exist */
	Assert(!entry->storage_created ||
		   smgrexists(smgropen(entry->locator, ProcNumberForTempRelations()),
					  MAIN_FORKNUM));

	/*
	 * Writes are the moment per-session storage springs into existence: reads
	 * of unmaterialized storage complete without a file via the zero-blocks
	 * short-circuits, but an insert is about to extend the relation and needs
	 * the file (and the registry entry that makes peer DDL respect our
	 * now-live data).
	 */
	if (is_insert && !entry->storage_created)
		GttEnsureSessionStorage(rel);

	/* Record the first write into otherwise-empty storage. */
	if (is_insert && !TransactionIdIsValid(entry->oldest_xid))
	{
		/*
		 * Seed with the top-level XID, not GetCurrentTransactionId(): inside
		 * a subtransaction the latter returns the subxact's XID, which is
		 * always newer than the parent's.  If the first write into empty
		 * storage happened under a savepoint, a later write at an outer
		 * nesting level would stamp tuples with an older xmin than the seed,
		 * and VACUUM (which uses oldest_xid as the relfrozenxid cutoff) would
		 * then see "xmin from before relfrozenxid".  The top-level XID is
		 * assigned before any of its children, so it is a valid lower bound
		 * for every xmin this transaction can write.
		 */
		entry->oldest_xid = GetTopTransactionId();
		entry->xid_warned = false;

		/*
		 * Seed the session relminmxid too.  No multixact older than the
		 * current next-multixact can appear in data written from here on, so
		 * this is a valid lower bound; a later VACUUM may advance it.
		 */
		entry->session_relminmxid = ReadNextMultiXactId();
		return;					/* freshly written data is current */
	}

	if (!TransactionIdIsValid(entry->oldest_xid))
		return;					/* no data tracked for this relation */

	/*
	 * Read the cluster-wide CLOG-truncation horizon.  A lock-free read is
	 * acceptable: oldestXid only ever advances, it changes only at CLOG
	 * truncation (infrequently), and we re-check on every access.  A slightly
	 * stale (older) value can only make us less aggressive, and the worst
	 * case is then the ordinary "could not access status of transaction"
	 * error rather than a wrong result.
	 */
	frontier = TransamVariables->oldestXid;
	if (!TransactionIdIsValid(frontier))
		return;

	/*
	 * Past the horizon: CLOG may be gone.  Refuse rather than risk
	 * corruption.
	 */
	if (TransactionIdPrecedes(entry->oldest_xid, frontier))
		ereport(ERROR,
				errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				errmsg("global temporary table \"%s\" contains data older than the transaction-ID horizon",
					   RelationGetRelationName(rel)),
				errdetail("The oldest row was written by transaction %u, which precedes the cluster commit-log truncation horizon (%u).",
						  entry->oldest_xid, frontier),
				errhint("VACUUM the table to freeze its data in place; if that fails because the data is already past the commit-log horizon, TRUNCATE the table or reconnect."));

	/*
	 * Approaching the horizon: warn once per relation per session.
	 *
	 * We know oldest_xid is at or after the frontier (it did not precede it
	 * above), so both the gap below the data and the cluster's total CLOG
	 * history are forward distances under 2^31.  Warn when the data sits
	 * within the configured head room of the frontier -- but only once the
	 * CLOG history itself is longer than that head room.  Otherwise (a young
	 * cluster, or one whose datfrozenxid is kept close to the next XID) the
	 * head-room band would cover even freshly written data, producing a
	 * warning that does not reflect any real risk.
	 */
	if (!entry->xid_warned && global_temp_xid_warn_margin > 0)
	{
		TransactionId next_xid = XidFromFullTransactionId(TransamVariables->nextXid);
		uint32		clog_history = (uint32) (next_xid - frontier);
		uint32		gap = (uint32) (entry->oldest_xid - frontier);
		uint32		margin = (uint32) global_temp_xid_warn_margin;

		if (clog_history > margin && gap <= margin)
		{
			ereport(WARNING,
					errmsg("global temporary table \"%s\" contains data approaching the transaction-ID horizon",
						   RelationGetRelationName(rel)),
					errhint("VACUUM the table to freeze its data; otherwise it is lost if its oldest row reaches transaction-ID wraparound."));
			entry->xid_warned = true;
		}
	}
}

/*
 * GttGetSessionFrozenXids
 *		Fetch the per-session freeze horizon for a global temporary table.
 *
 * VACUUM uses these in place of the shared pg_class relfrozenxid/relminmxid
 * (which are always invalid for a GTT) as the starting cutoffs for the
 * relation.  oldest_xid doubles as the session relfrozenxid; session_relminmxid
 * is its multixact counterpart.  Returns false, leaving the outputs untouched,
 * when this session has no tracked data for the relation -- there is then
 * nothing to vacuum.
 */
bool
GttGetSessionFrozenXids(Oid relid, TransactionId *relfrozenxid,
						MultiXactId *relminmxid)
{
	GttStorageEntry *entry;

	if (gtt_storage_hash == NULL)
		return false;

	entry = (GttStorageEntry *) hash_search(gtt_storage_hash, &relid,
											HASH_FIND, NULL);
	if (entry == NULL || !TransactionIdIsValid(entry->oldest_xid))
		return false;

	*relfrozenxid = entry->oldest_xid;
	*relminmxid = entry->session_relminmxid;
	return true;
}

/*
 * GttUpdateSessionFrozenXids
 *		Persist the freeze horizon left behind by a VACUUM of a global
 *		temporary table.
 *
 * The shared pg_class row cannot hold per-session freeze state, so VACUUM
 * stores its new relfrozenxid/relminmxid here instead.  The next VACUUM reads
 * them back via GttGetSessionFrozenXids, and the wraparound guard
 * (GttPrepareAccess) benefits immediately because oldest_xid is the same
 * field.  Advancing the horizon clears the approaching-wraparound warning
 * throttle.
 *
 * Invalid inputs (e.g. the index writeback, or a non-aggressive VACUUM that
 * skipped all-visible pages) leave the stored values unchanged.  *_updated, if
 * supplied, report whether the corresponding value advanced.
 */
void
GttUpdateSessionFrozenXids(Oid relid, TransactionId relfrozenxid,
						   MultiXactId relminmxid,
						   bool *frozenxid_updated, bool *minmulti_updated)
{
	GttStorageEntry *entry;

	if (frozenxid_updated)
		*frozenxid_updated = false;
	if (minmulti_updated)
		*minmulti_updated = false;

	if (gtt_storage_hash == NULL)
		return;

	entry = (GttStorageEntry *) hash_search(gtt_storage_hash, &relid,
											HASH_FIND, NULL);
	if (entry == NULL)
		return;

	if (TransactionIdIsNormal(relfrozenxid) &&
		(!TransactionIdIsValid(entry->oldest_xid) ||
		 TransactionIdPrecedes(entry->oldest_xid, relfrozenxid)))
	{
		entry->oldest_xid = relfrozenxid;
		entry->xid_warned = false;
		if (frozenxid_updated)
			*frozenxid_updated = true;
	}

	if (MultiXactIdIsValid(relminmxid) &&
		(!MultiXactIdIsValid(entry->session_relminmxid) ||
		 MultiXactIdPrecedes(entry->session_relminmxid, relminmxid)))
	{
		entry->session_relminmxid = relminmxid;
		if (minmulti_updated)
			*minmulti_updated = true;
	}
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

	/* Discard any per-session column statistics for this relation */
	gtt_reset_colstats_for_rel(relid);

	/*
	 * Drop the cross-session registry entry.  A peer's GttCheckDroppable /
	 * GttCheckAlterable would error out spuriously if it still found us in
	 * the registry.  (Same ordering as in gtt_session_cleanup; safe if we
	 * never added an entry.)
	 */
	gtt_sessions_remove(relid);

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
		entry->drop_subid = GetCurrentSubTransactionId();
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

	entry->oldest_xid = InvalidTransactionId;
	entry->xid_warned = false;
	entry->session_relminmxid = InvalidMultiXactId;
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
 * gtt_truncate_dependents
 *		Physically empty the still-materialized indexes (and toast) of
 *		heaps whose own storage was just reverted by an abort.
 *
 * When a heap's storage was created in the aborting (sub)transaction, its
 * file is unlinked by PendingRelDelete -- but an index materialized in an
 * EARLIER transaction (e.g. built empty by an index scan, or by CREATE
 * INDEX on a then-populated heap) keeps its committed file, now full of
 * entries whose TIDs point into the vanished heap file.  Those are not
 * MVCC-dead references that visibility checks would hide; they dangle
 * physically.  Truncate such indexes and clear index_built so the next
 * access rebuilds them against the (now-empty) heap; the same pass covers
 * toast relations via the heap's recorded toast_relid.
 *
 * Mirrors the heap_relids matching in PreCommit_gtt_on_commit.  Frees the
 * list.
 */
static void
gtt_truncate_dependents(List *heap_relids)
{
	HASH_SEQ_STATUS status;
	GttStorageEntry *entry;

	if (heap_relids == NIL)
		return;

	hash_seq_init(&status, gtt_storage_hash);
	while ((entry = (GttStorageEntry *) hash_seq_search(&status)) != NULL)
	{
		if (entry->is_index)
		{
			if (OidIsValid(entry->heap_relid) &&
				list_member_oid(heap_relids, entry->heap_relid) &&
				entry->storage_created)
			{
				gtt_truncate_smgr(entry);
				entry->index_built = false;
			}
		}
		else if (list_member_oid(heap_relids, entry->relid))
			gtt_truncate_smgr(entry);	/* toast heap; no-op if empty */
	}
	list_free(heap_relids);
}

#ifdef USE_ASSERT_CHECKING
/*
 * gtt_session_registered
 *		Does the shared sessions registry hold this backend's entry for relid?
 */
static bool
gtt_session_registered(Oid relid)
{
	GttSessionsKey key;
	bool		found;

	if (GttSessionsHash == NULL)
		return false;

	init_sessions_key(&key, relid);

	LWLockAcquire(GttSessionsLock, LW_SHARED);
	(void) hash_search(GttSessionsHash, &key, HASH_FIND, &found);
	LWLockRelease(GttSessionsLock);

	return found;
}

/*
 * gtt_check_invariants
 *		Cross-check the per-session storage map once a top-level transaction
 *		has settled.
 *
 * The map's fields interact in ways that scattered updates can silently
 * break (and have: every bug the randomized stress test found was a broken
 * invariant here that only surfaced as a read error several statements
 * later).  Catching the inconsistency at the transaction boundary that
 * produced it points directly at the cause.  Filesystem-level agreement is
 * asserted at the use sites instead (GttPrepareAccessGuts and
 * gtt_build_index_internal), because this callback runs before
 * smgrDoPendingDeletes and the files' fate is not yet settled here.
 */
static void
gtt_check_invariants(void)
{
	HASH_SEQ_STATUS status;
	GttStorageEntry *entry;

	/* every swap was either committed or undone */
	Assert(gtt_swap_undo == NIL);

	hash_seq_init(&status, gtt_storage_hash);
	while ((entry = (GttStorageEntry *) hash_seq_search(&status)) != NULL)
	{
		/* a top-level transaction end settles all subxact bookkeeping */
		Assert(entry->create_subid == InvalidSubTransactionId);
		Assert(entry->storage_subid == InvalidSubTransactionId);
		Assert(entry->index_subid == InvalidSubTransactionId);
		Assert(entry->stats_subid == InvalidSubTransactionId);
		Assert(entry->demat_subid == InvalidSubTransactionId);
		Assert(entry->drop_subid == InvalidSubTransactionId);

		/* the relkind flags are mutually exclusive ... */
		Assert(!(entry->is_index && entry->is_sequence));
		/* ... and only indexes carry build state */
		Assert(!entry->index_built || entry->is_index);
		Assert(!entry->build_deferred || entry->is_index);

		/* no storage => no index structure; a built index is not pending */
		Assert(!entry->index_built || entry->storage_created);
		Assert(!entry->index_built || !entry->build_deferred);

		/*
		 * The shared sessions registry mirrors materialization exactly --
		 * except during backend exit, where gtt_session_cleanup (a shmem-exit
		 * callback) deregisters everything before AbortOutOfAnyTransaction
		 * triggers this walker for a still-open transaction (pg_dump, for
		 * one, disconnects without closing its read-only transaction).
		 */
		Assert(proc_exit_inprogress ||
			   entry->storage_created == gtt_session_registered(entry->relid));
	}
}
#endif

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
	List	   *to_deregister = NIL;
	List	   *reverted_heaps = NIL;
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

	/*
	 * The traversal cannot remove entries inline: hash_seq_search is fragile
	 * if the current entry is deleted, and gtt_remove_entry may itself need
	 * to take an LWLock that we don't want to hold across the whole scan.
	 * Collect victims into to_remove and process them after the scan.
	 */
	hash_seq_init(&status, gtt_storage_hash);
	while ((entry = (GttStorageEntry *) hash_seq_search(&status)) != NULL)
	{
		bool		remove = false;

		if (event == XACT_EVENT_COMMIT ||
			event == XACT_EVENT_PARALLEL_COMMIT)
		{
			/*
			 * Top-level commit.  An entry with a scheduled drop is removed
			 * now: heap_drop_with_catalog scheduled the drop, the catalog
			 * change has just become visible, and the per-session bookkeeping
			 * must follow.  All other entries survive into subsequent
			 * transactions; clear the per-subxact bookkeeping since the state
			 * they reference is now committed and no longer
			 * rollback-sensitive.
			 */
			if (entry->drop_subid != InvalidSubTransactionId)
				remove = true;
			else
			{
				entry->create_subid = InvalidSubTransactionId;
				entry->storage_subid = InvalidSubTransactionId;
				entry->index_subid = InvalidSubTransactionId;
				entry->stats_subid = InvalidSubTransactionId;

				/*
				 * A committed DISCARD releases the (empty) storage and the
				 * registration; see GttResetAllSessionData.
				 */
				if (entry->demat_subid != InvalidSubTransactionId)
				{
					entry->demat_subid = InvalidSubTransactionId;
					if (entry->storage_created)
					{
						SMgrRelation srel;

						srel = smgropen(entry->locator,
										ProcNumberForTempRelations());
						if (!smgrexists(srel, MAIN_FORKNUM) ||
							entry->is_sequence ||
							smgrnblocks(srel, MAIN_FORKNUM) == 0)
						{
							smgrdounlinkall(&srel, 1, false);
							entry->storage_created = false;
							entry->index_built = false;
							to_deregister = lappend_oid(to_deregister,
														entry->relid);
							to_invalidate = lappend_oid(to_invalidate,
														entry->relid);
						}
					}
				}
			}
		}
		else
		{
			/*
			 * Top-level abort.  Three cases:
			 *
			 * 1) The entry was created in this aborting transaction
			 * (create_subid is set): the catalog row will not exist after
			 * rollback, so the entry must go away too.  Files for this entry
			 * are unlinked by the regular PendingRelDelete machinery.
			 *
			 * 2) The entry pre-dated this transaction but had its storage or
			 * index built in it (storage_subid/index_subid set): the catalog
			 * stays, but the lazily-created files have been unlinked by
			 * PendingRelDelete.  Reset the storage_created/index_built flags
			 * so the next access in a later transaction re-creates them.
			 * Force a relcache invalidation so the next access re-runs
			 * RelationInitPhysicalAddr -> GttInitSessionStorage; without it
			 * the cached rd_locator would still point at the now-deleted
			 * file.
			 *
			 * 3) drop_subid is cleared regardless, because any
			 * heap_drop_with_catalog from this xact is now reverted.
			 *
			 * Entries that match (2) or (3) survive the abort.
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
					to_deregister = lappend_oid(to_deregister, entry->relid);
					to_invalidate = lappend_oid(to_invalidate, entry->relid);
					if (!entry->is_index)
					{
						reverted_heaps = lappend_oid(reverted_heaps,
													 entry->relid);
						if (OidIsValid(entry->toast_relid))
							reverted_heaps = lappend_oid(reverted_heaps,
														 entry->toast_relid);
					}
				}
				if (entry->index_subid != InvalidSubTransactionId)
				{
					entry->index_built = false;
					entry->index_subid = InvalidSubTransactionId;
				}
				if (entry->stats_subid != InvalidSubTransactionId)
				{
					/*
					 * Session statistics written by the aborted transaction
					 * describe rolled-back data; throw them away (column
					 * stats too -- the previous tuples were freed when the
					 * aborted ANALYZE replaced them, so there is nothing to
					 * restore).  The planner falls back to size-based
					 * estimation, which is right for the surviving state.
					 */
					entry->stats_valid = false;
					entry->stats_subid = InvalidSubTransactionId;
					gtt_reset_colstats_for_rel(entry->relid);
				}

				/* an aborted DISCARD restores the data; cancel the release */
				entry->demat_subid = InvalidSubTransactionId;
				entry->drop_subid = InvalidSubTransactionId;
			}
		}

		if (remove)
			to_remove = lappend_oid(to_remove, entry->relid);
	}

	gtt_remove_relids(to_remove);
	gtt_truncate_dependents(reverted_heaps);

	foreach_oid(relid, to_deregister)
		gtt_sessions_remove(relid);
	list_free(to_deregister);

	foreach_oid(relid, to_invalidate)
		RelationCacheInvalidateEntry(relid);
	list_free(to_invalidate);

#ifdef USE_ASSERT_CHECKING
	gtt_check_invariants();
#endif

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
	List	   *to_deregister = NIL;
	List	   *reverted_heaps = NIL;
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
			if (entry->stats_subid == mySubid)
				entry->stats_subid = parentSubid;
			if (entry->demat_subid == mySubid)
				entry->demat_subid = parentSubid;
			if (entry->drop_subid == mySubid)
				entry->drop_subid = parentSubid;
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
				to_deregister = lappend_oid(to_deregister, entry->relid);
				to_invalidate = lappend_oid(to_invalidate, entry->relid);
				if (!entry->is_index)
				{
					reverted_heaps = lappend_oid(reverted_heaps,
												 entry->relid);
					if (OidIsValid(entry->toast_relid))
						reverted_heaps = lappend_oid(reverted_heaps,
													 entry->toast_relid);
				}
			}
			if (entry->index_subid == mySubid)
			{
				entry->index_built = false;
				entry->index_subid = InvalidSubTransactionId;
			}
			if (entry->stats_subid == mySubid)
			{
				/* see gtt_xact_callback */
				entry->stats_valid = false;
				entry->stats_subid = InvalidSubTransactionId;
				gtt_reset_colstats_for_rel(entry->relid);
			}
			if (entry->demat_subid == mySubid)
				entry->demat_subid = InvalidSubTransactionId;
			if (entry->drop_subid == mySubid)
				entry->drop_subid = InvalidSubTransactionId;
		}
	}

	gtt_remove_relids(to_remove);
	gtt_truncate_dependents(reverted_heaps);

	foreach_oid(relid, to_deregister)
		gtt_sessions_remove(relid);
	list_free(to_deregister);

	/* See gtt_xact_callback: invalidate relcache for any killed storage. */
	foreach_oid(relid, to_invalidate)
		RelationCacheInvalidateEntry(relid);
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

	/* Storage is being emptied; the xid horizon tracking restarts. */
	entry->oldest_xid = InvalidTransactionId;
	entry->xid_warned = false;
	entry->session_relminmxid = InvalidMultiXactId;

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
 * GttGetSessionStats
 *		Retrieve per-session relation statistics for a GTT.
 *
 * Returns true if per-session statistics are available (i.e. ANALYZE has
 * been run on this GTT in this session), filling in the output parameters.
 * Returns false if no per-session stats exist, in which case the planner
 * should fall back to default estimation.
 */
bool
GttGetSessionStats(Oid relid, BlockNumber *relpages, double *reltuples,
				   BlockNumber *relallvisible)
{
	GttStorageEntry *entry;

	if (gtt_storage_hash == NULL)
		return false;

	entry = (GttStorageEntry *) hash_search(gtt_storage_hash,
											&relid,
											HASH_FIND,
											NULL);
	if (entry == NULL || !entry->stats_valid)
		return false;

	*relpages = entry->relpages;
	*reltuples = (double) entry->reltuples;
	*relallvisible = entry->relallvisible;
	return true;
}

/*
 * GttUpdateSessionStats
 *		Store per-session relation statistics for a GTT.
 *
 * Called from ANALYZE to record relpages/reltuples/relallvisible in the
 * per-session hash instead of writing to the shared pg_class row.
 */
void
GttUpdateSessionStats(Oid relid, BlockNumber relpages, double reltuples,
					  BlockNumber relallvisible)
{
	GttStorageEntry *entry;

	if (gtt_storage_hash == NULL)
		return;

	entry = (GttStorageEntry *) hash_search(gtt_storage_hash,
											&relid,
											HASH_FIND,
											NULL);
	if (entry == NULL)
		return;

	entry->stats_valid = true;
	entry->relpages = relpages;
	entry->reltuples = (float4) reltuples;
	entry->relallvisible = relallvisible;

	/*
	 * Unlike pg_class/pg_statistic writes, these survive a transaction abort
	 * unless we act: remember the writing subxact so the abort paths can
	 * invalidate stats that describe rolled-back data.
	 */
	entry->stats_subid = GetCurrentSubTransactionId();
	gtt_xact_state_dirty = true;
}

/*
 * GttResetSessionStats
 *		Invalidate per-session stats for a GTT after TRUNCATE.
 *
 * After truncation, the previous ANALYZE statistics are no longer valid.
 * The planner will fall back to default estimation based on actual page
 * count until ANALYZE is run again.
 */
void
GttResetSessionStats(Oid relid)
{
	GttStorageEntry *entry;

	if (gtt_storage_hash == NULL)
		return;

	entry = (GttStorageEntry *) hash_search(gtt_storage_hash,
											&relid,
											HASH_FIND,
											NULL);
	if (entry != NULL)
		entry->stats_valid = false;

	/* Also clear any per-session column statistics */
	gtt_reset_colstats_for_rel(relid);
}

/*
 * GttStoreSessionColumnStats
 *		Store a per-session column statistics tuple for a GTT.
 *
 * The tuple must be a pg_statistic-format HeapTuple allocated in
 * TopMemoryContext.  If an entry already exists for this (relid, attnum, inh),
 * the old tuple is freed and replaced.
 */
void
GttStoreSessionColumnStats(Oid relid, AttrNumber attnum, bool inh,
						   HeapTuple tuple)
{
	GttColStatsKey key;
	GttColStatsEntry *entry;
	GttStorageEntry *rel_entry;
	bool		found;

	ensure_gtt_colstats_hash();

	init_colstats_key(&key, relid, attnum, inh);

	entry = (GttColStatsEntry *) hash_search(gtt_colstats_hash,
											 &key,
											 HASH_ENTER,
											 &found);
	if (found && entry->statsTuple != NULL)
		heap_freetuple(entry->statsTuple);

	entry->statsTuple = tuple;

	/*
	 * Mark the relation-level entry so an abort of the writing (sub)xact
	 * invalidates the column stats along with the relation stats; see
	 * GttUpdateSessionStats.
	 */
	if (gtt_storage_hash != NULL)
	{
		rel_entry = (GttStorageEntry *) hash_search(gtt_storage_hash, &relid,
													HASH_FIND, NULL);
		if (rel_entry != NULL)
		{
			rel_entry->stats_subid = GetCurrentSubTransactionId();
			gtt_xact_state_dirty = true;
		}
	}
}

/*
 * GttSearchColumnStats
 *		Look up per-session column statistics for a GTT column.
 *
 * Returns the stored pg_statistic-format HeapTuple, or NULL if no per-session
 * stats exist for this column.  The caller must NOT free the returned tuple;
 * it is owned by the hash table.
 */
HeapTuple
GttSearchColumnStats(Oid relid, AttrNumber attnum, bool inh)
{
	GttColStatsKey key;
	GttColStatsEntry *entry;

	if (gtt_colstats_hash == NULL)
		return NULL;

	init_colstats_key(&key, relid, attnum, inh);

	entry = (GttColStatsEntry *) hash_search(gtt_colstats_hash,
											 &key,
											 HASH_FIND,
											 NULL);
	if (entry != NULL)
		return entry->statsTuple;

	return NULL;
}

/*
 * GttReleaseColumnStats
 *		No-op freefunc for per-session GTT column statistics tuples.
 *
 * The tuple is owned by gtt_colstats_hash and must not be freed by the
 * planner.  This function is used as the VariableStatData.freefunc callback.
 */
void
GttReleaseColumnStats(HeapTuple tuple)
{
	/* No-op: tuple lives in gtt_colstats_hash in TopMemoryContext */
}

/*
 * SearchStats
 *		Look up column statistics, checking per-session GTT stats if requested.
 *
 * Checks the pg_statistic syscache first.  If include_gtt is true and no
 * shared stats are found, falls back to per-session GTT statistics.
 * Sets *freefunc to the appropriate release function for the returned tuple.
 */
HeapTuple
SearchStats(Oid relid, AttrNumber attnum, bool inh,
			bool include_gtt,
			void (**freefunc) (HeapTuple))
{
	HeapTuple	tuple;

	/*
	 * Check the shared pg_statistic catalog first.  A GTT never has rows
	 * there (ANALYZE diverts its stats to the per-session hash), so a
	 * syscache hit settles the lookup without touching the GTT hash: once any
	 * GTT has been ANALYZEd in this session, probing the hash first would
	 * cost every planner stats lookup for ordinary analyzed tables a
	 * guaranteed-miss hash search on this hot path.
	 */
	tuple = SearchSysCache3(STATRELATTINH,
							ObjectIdGetDatum(relid),
							Int16GetDatum(attnum),
							BoolGetDatum(inh));
	if (HeapTupleIsValid(tuple))
	{
		*freefunc = ReleaseSysCache;
		return tuple;
	}

	/* No shared stats: per-session GTT stats, or no stats at all. */
	if (include_gtt)
	{
		tuple = GttSearchColumnStats(relid, attnum, inh);
		if (HeapTupleIsValid(tuple))
		{
			*freefunc = GttReleaseColumnStats;
			return tuple;
		}
	}

	*freefunc = ReleaseSysCache;
	return NULL;
}

/*
 * gtt_reset_colstats_for_rel
 *		Remove all per-session column statistics for a given relation.
 *
 * Used when stats are invalidated (TRUNCATE, ON COMMIT DELETE ROWS, DROP).
 */
static void
gtt_reset_colstats_for_rel(Oid relid)
{
	HASH_SEQ_STATUS status;
	GttColStatsEntry *entry;
	List	   *keys_to_remove = NIL;
	ListCell   *lc;

	if (gtt_colstats_hash == NULL)
		return;

	/*
	 * Collect matching keys first; we can't remove hash entries during an
	 * active hash_seq_search scan.
	 */
	hash_seq_init(&status, gtt_colstats_hash);
	while ((entry = (GttColStatsEntry *) hash_seq_search(&status)) != NULL)
	{
		GttColStatsKey *keycopy;

		if (entry->key.relid != relid)
			continue;

		keycopy = (GttColStatsKey *) palloc(sizeof(*keycopy));
		*keycopy = entry->key;
		keys_to_remove = lappend(keys_to_remove, keycopy);
	}

	foreach(lc, keys_to_remove)
	{
		GttColStatsKey *key = (GttColStatsKey *) lfirst(lc);

		entry = (GttColStatsEntry *) hash_search(gtt_colstats_hash, key,
												 HASH_REMOVE, NULL);
		if (entry != NULL && entry->statsTuple != NULL)
			heap_freetuple(entry->statsTuple);
		pfree(key);
	}
	list_free(keys_to_remove);
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

	/* First pass: reset heap entries and remember which heaps were wiped. */
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

		entry->stats_valid = false;
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

	/* Column stats live in a second hash; clear them for each truncated rel. */
	foreach_oid(relid, heap_relids)
		gtt_reset_colstats_for_rel(relid);

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

	/*
	 * Schedule dematerialization at commit: once a DISCARD commits, the
	 * session holds no live data, so it should stop blocking peer DDL -- the
	 * registry entry, the (now empty) files, and the storage flag are all
	 * released by the commit callback.  The deferral keeps DISCARD TEMP
	 * transactional: an abort restores the data through the swap undo, and
	 * the schedule is simply cancelled.  At commit, an entry whose main fork
	 * is no longer empty (the same transaction wrote into it after the
	 * DISCARD) is kept materialized instead; sequences are always released,
	 * since rematerialization reseeds them to their start value, which is
	 * what DISCARD's reset means anyway.
	 */
	hash_seq_init(&status, gtt_storage_hash);
	while ((entry = (GttStorageEntry *) hash_seq_search(&status)) != NULL)
	{
		if (entry->storage_created)
		{
			entry->demat_subid = GetCurrentSubTransactionId();
			gtt_xact_state_dirty = true;
		}
	}
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

	/*
	 * The column-stats hash lives in TopMemoryContext and will be torn down
	 * with the rest of process memory shortly after we return; nothing to do
	 * here.  We only walk gtt_storage_hash because each entry owns
	 * externally-visible resources (on-disk files and a shared registry
	 * entry) that must be released explicitly.
	 */
	if (gtt_storage_hash == NULL)
		return;

	/*
	 * Drop registry entries in a separate first pass before any per-entry
	 * work that touches the disk.  A peer scanning the registry concurrently
	 * must not name this backend once we have effectively departed; without
	 * this ordering the per-entry unlink can stretch that visibility window
	 * long enough for a peer's GttCheckAlterable / Droppable to error out
	 * spuriously while we are merely finishing teardown.  See the matching
	 * ordering in gtt_remove_entry.
	 *
	 * This ordering is deliberately not covered by an automated regression
	 * test.  Reproducing the window requires pausing a backend between the
	 * registry pass and the lock release while a peer observes, and this code
	 * runs from before_shmem_exit during proc_exit: an injection-point wait
	 * placed here cannot be woken once the peer's CREATE INDEX delivers a
	 * sinval to the parked backend (its condition-variable wakeup is lost),
	 * so such a test cannot tear down cleanly.  The fix was instead verified
	 * by hand: with the old ordering and a temporary delay inserted here, a
	 * peer CREATE INDEX failed ~20/20 runs; with this ordering, 0/20.
	 */
	hash_seq_init(&status, gtt_storage_hash);
	while ((entry = (GttStorageEntry *) hash_seq_search(&status)) != NULL)
		gtt_sessions_remove(entry->relid);

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

/*
 * init_sessions_key
 *		Build a sessions-registry key with deterministic byte contents.
 *
 * Like init_colstats_key, we zero the full struct so that HASH_BLOBS hashing
 * over any trailing alignment padding is reproducible.
 */
static void
init_sessions_key(GttSessionsKey *key, Oid relid)
{
	memset(key, 0, sizeof(*key));
	key->dbOid = MyDatabaseId;
	key->relid = relid;
	key->procnum = MyProcNumber;
}

/*
 * gtt_sessions_add
 *		Record that this backend has live per-session storage for a GTT.
 *
 * table_relid is the owning table (self for a heap, the parent heap for an
 * index), so that a DDL check on the table also finds sessions whose only
 * materialized storage is one of its indexes.
 */
static void
gtt_sessions_add(Oid relid, Oid table_relid)
{
	GttSessionsKey key;
	GttSessionsEntry *entry;

	/* GttSessionsHash is NULL in bootstrap and single-user mode */
	if (GttSessionsHash == NULL)
		return;

	init_sessions_key(&key, relid);

	LWLockAcquire(GttSessionsLock, LW_EXCLUSIVE);
	entry = (GttSessionsEntry *) hash_search(GttSessionsHash, &key,
											 HASH_ENTER, NULL);
	entry->table_relid = table_relid;
	LWLockRelease(GttSessionsLock);
}

/*
 * gtt_sessions_remove
 *		Clear our registry entry for a GTT.  Safe to call if no entry exists.
 */
static void
gtt_sessions_remove(Oid relid)
{
	GttSessionsKey key;

	if (GttSessionsHash == NULL)
		return;

	init_sessions_key(&key, relid);

	LWLockAcquire(GttSessionsLock, LW_EXCLUSIVE);
	(void) hash_search(GttSessionsHash, &key, HASH_REMOVE, NULL);
	LWLockRelease(GttSessionsLock);
}

/*
 * gtt_first_other_session_with_storage
 *		Find any peer backend currently holding per-session storage for relid.
 *
 * Returns the ProcNumber of one such backend, or INVALID_PROC_NUMBER if none
 * exists.  Our own entry (if any) is skipped: a session is always allowed
 * to drop or alter its own live GTT.
 *
 * Callers must already hold AccessExclusiveLock on the relation, which keeps
 * new sessions out of GttInitSessionStorage for this relid; that makes the
 * shared-mode scan sufficient -- any backend listed here has already
 * committed to private per-session storage and will not clean it up until
 * its own exit.
 */
static ProcNumber
gtt_first_other_session_with_storage(Oid relid)
{
	HASH_SEQ_STATUS status;
	GttSessionsEntry *entry;
	ProcNumber	other_procnum = INVALID_PROC_NUMBER;

	if (GttSessionsHash == NULL)
		return INVALID_PROC_NUMBER;

	LWLockAcquire(GttSessionsLock, LW_SHARED);
	hash_seq_init(&status, GttSessionsHash);
	while ((entry = (GttSessionsEntry *) hash_seq_search(&status)) != NULL)
	{
		if (entry->key.dbOid == MyDatabaseId &&
			(entry->key.relid == relid || entry->table_relid == relid) &&
			entry->key.procnum != MyProcNumber)
		{
			other_procnum = entry->key.procnum;
			hash_seq_term(&status);
			break;
		}
	}
	LWLockRelease(GttSessionsLock);

	return other_procnum;
}

/*
 * gtt_wait_other_sessions_gone
 *		Wait briefly for other backends' registry entries on relid to clear.
 *
 * A backend that disconnects removes its registry entries from
 * before_shmem_exit (gtt_session_cleanup), but a client can reconnect and
 * issue DDL before its previous backend has finished exiting.  Erroring
 * out immediately on such an entry would make patterns like reconnect-
 * then-DROP fail spuriously.  As with DROP DATABASE's
 * CountOtherDBBackends(), retry for a few seconds to give exiting
 * backends time to finish; a backend that is genuinely retaining data
 * keeps its entry indefinitely, so we then report it.
 *
 * Returns INVALID_PROC_NUMBER if no other session has storage for relid
 * (possibly after waiting), else the proc number of one that does.
 */
static ProcNumber
gtt_wait_other_sessions_gone(Oid relid)
{
	ProcNumber	other_procnum = INVALID_PROC_NUMBER;
	int			tries;

	/* 50 tries with 100ms sleep between tries, i.e. 5 seconds in total */
	for (tries = 0; tries < 50; tries++)
	{
		CHECK_FOR_INTERRUPTS();

		other_procnum = gtt_first_other_session_with_storage(relid);
		if (other_procnum == INVALID_PROC_NUMBER)
			return INVALID_PROC_NUMBER;

		pg_usleep(100 * 1000L); /* 100ms */
	}

	return other_procnum;
}

/*
 * GttCheckDroppable
 *		Error out if any other backend has live per-session storage for relid.
 *
 * Called from heap_drop_with_catalog.  See
 * gtt_first_other_session_with_storage() for locking expectations.
 */
void
GttCheckDroppable(Oid relid)
{
	ProcNumber	other_procnum = gtt_wait_other_sessions_gone(relid);

	if (other_procnum != INVALID_PROC_NUMBER)
		ereport(ERROR,
				errcode(ERRCODE_OBJECT_IN_USE),
				errmsg("cannot drop global temporary table: another session has live per-session data"),
				errdetail("Backend with proc number %d has live per-session data.",
						  other_procnum));
}

/*
 * GttCheckAlterable
 *		Error out if any other backend has live per-session storage for relid.
 *
 * Used to gate ALTER TABLE and CREATE INDEX on a GTT.  Without this check,
 * a peer session with committed data but no transaction in progress holds
 * no heavyweight lock on the GTT, so nothing else would stop schema changes
 * that invalidate that session's data (e.g. SET NOT NULL with NULL rows,
 * ADD UNIQUE with duplicates).  See
 * gtt_first_other_session_with_storage() for locking expectations.
 */
void
GttCheckAlterable(Oid relid)
{
	ProcNumber	other_procnum = gtt_wait_other_sessions_gone(relid);

	if (other_procnum != INVALID_PROC_NUMBER)
		ereport(ERROR,
				errcode(ERRCODE_OBJECT_IN_USE),
				errmsg("cannot alter global temporary table: another session has live per-session data"),
				errdetail("Backend with proc number %d has live per-session data.",
						  other_procnum));
}

/*
 * GttSessionsShmemRequest
 *		Register the shared-memory hash at postmaster startup.
 *
 * The per-backend estimate is deliberately modest; extending it would only
 * matter for workloads that routinely touch a large number of GTTs per
 * session.  Hash growth beyond nelems is allowed by dynahash but forces
 * linear-probe segment splits, so oversizing slightly is cheap.
 */
static void
GttSessionsShmemRequest(void *arg)
{
	ShmemRequestHash(.name = "GTT Sessions",
					 .nelems = mul_size(MaxBackends,
										GTT_SESSIONS_ENTRIES_PER_BACKEND),
					 .ptr = &GttSessionsHash,
					 .hash_info.keysize = sizeof(GttSessionsKey),
					 .hash_info.entrysize = sizeof(GttSessionsEntry),
					 .hash_flags = HASH_ELEM | HASH_BLOBS);
}

/*
 * format_stats_values_as_text
 *		Convert the values from an AttStatsSlot into a text representation.
 *
 * We build a PostgreSQL array of the slot's element type and return its
 * array_out textual form.  That gives proper array-literal escaping for
 * values containing commas, braces, double quotes, backslashes, etc.,
 * matching what pg_stats produces via its anyarray columns.
 */
static char *
format_stats_values_as_text(AttStatsSlot *sslot)
{
	ArrayType  *arr;
	int16		typlen;
	bool		typbyval;
	char		typalign;

	get_typlenbyvalalign(sslot->valuetype, &typlen, &typbyval, &typalign);
	arr = construct_array(sslot->values, sslot->nvalues,
						  sslot->valuetype, typlen, typbyval, typalign);

	return OidOutputFunctionCall(F_ARRAY_OUT, PointerGetDatum(arr));
}

/*
 * pg_gtt_relstats
 *		Return per-session relation-level statistics for global temporary tables.
 *
 * If a regclass argument is provided, returns stats only for that table.
 * If NULL (the default), returns stats for all GTTs with valid session stats.
 */
Datum
pg_gtt_relstats(PG_FUNCTION_ARGS)
{
#define PG_GTT_SESSION_RELSTATS_COLS 5
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	Oid			filter_relid = InvalidOid;
	HASH_SEQ_STATUS status;
	GttStorageEntry *entry;
	Datum		values[PG_GTT_SESSION_RELSTATS_COLS];
	bool		nulls[PG_GTT_SESSION_RELSTATS_COLS];

	if (!PG_ARGISNULL(0))
		filter_relid = PG_GETARG_OID(0);

	InitMaterializedSRF(fcinfo, 0);

	if (gtt_storage_hash == NULL)
		return (Datum) 0;

	memset(nulls, 0, sizeof(nulls));

	hash_seq_init(&status, gtt_storage_hash);
	while ((entry = (GttStorageEntry *) hash_seq_search(&status)) != NULL)
	{
		char	   *relname;

		if (!entry->stats_valid)
			continue;
		if (OidIsValid(filter_relid) && entry->relid != filter_relid)
			continue;

		/*
		 * Respect SELECT privilege on the target relation so callers can't
		 * inspect stats for relations they can't see.  Mirrors pg_stats.
		 */
		if (pg_class_aclcheck(entry->relid, GetUserId(), ACL_SELECT) != ACLCHECK_OK)
			continue;

		relname = get_rel_name(entry->relid);
		if (relname == NULL)
			continue;

		values[0] = ObjectIdGetDatum(entry->relid);
		values[1] = CStringGetTextDatum(relname);
		values[2] = Int32GetDatum((int32) entry->relpages);
		values[3] = Float4GetDatum(entry->reltuples);
		values[4] = Int32GetDatum((int32) entry->relallvisible);

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc,
							 values, nulls);
	}

	return (Datum) 0;
}

/*
 * pg_gtt_colstats
 *		Return per-session column-level statistics for global temporary tables.
 *
 * Returns stats in a format similar to the pg_stats view: scalar stats
 * (null_frac, avg_width, n_distinct), MCVs, histograms, and correlation.
 * Array-typed values are converted to text representation.
 *
 * If a regclass argument is provided, returns stats only for that table.
 * If NULL (the default), returns stats for all GTTs with column stats.
 */
Datum
pg_gtt_colstats(PG_FUNCTION_ARGS)
{
#define PG_GTT_SESSION_COLSTATS_COLS 12
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	Oid			filter_relid = InvalidOid;
	HASH_SEQ_STATUS status;
	GttColStatsEntry *csentry;

	if (!PG_ARGISNULL(0))
		filter_relid = PG_GETARG_OID(0);

	InitMaterializedSRF(fcinfo, 0);

	if (gtt_colstats_hash == NULL)
		return (Datum) 0;

	hash_seq_init(&status, gtt_colstats_hash);
	while ((csentry = (GttColStatsEntry *) hash_seq_search(&status)) != NULL)
	{
		HeapTuple	statstuple = csentry->statsTuple;
		Form_pg_statistic stats;
		char	   *relname;
		char	   *attname;
		int			mcv_slot;
		int			hist_slot;
		int			corr_slot;
		int			k;
		Datum		values[PG_GTT_SESSION_COLSTATS_COLS];
		bool		nulls[PG_GTT_SESSION_COLSTATS_COLS];

		if (statstuple == NULL)
			continue;
		if (OidIsValid(filter_relid) && csentry->key.relid != filter_relid)
			continue;

		/*
		 * Require column-level SELECT privilege (or table-level) on the
		 * attribute to see its stats, matching pg_stats behavior.
		 */
		if (pg_class_aclcheck(csentry->key.relid, GetUserId(),
							  ACL_SELECT) != ACLCHECK_OK &&
			pg_attribute_aclcheck(csentry->key.relid, csentry->key.attnum,
								  GetUserId(), ACL_SELECT) != ACLCHECK_OK)
			continue;

		relname = get_rel_name(csentry->key.relid);
		if (relname == NULL)
			continue;

		stats = (Form_pg_statistic) GETSTRUCT(statstuple);

		/* Start with all nulls, then fill in non-null columns */
		memset(nulls, true, sizeof(nulls));

		values[0] = ObjectIdGetDatum(csentry->key.relid);
		nulls[0] = false;
		values[1] = CStringGetTextDatum(relname);
		nulls[1] = false;
		values[2] = Int16GetDatum(csentry->key.attnum);
		nulls[2] = false;

		attname = get_attname(csentry->key.relid, csentry->key.attnum, true);
		if (attname != NULL)
		{
			values[3] = CStringGetTextDatum(attname);
			nulls[3] = false;
		}

		values[4] = BoolGetDatum(csentry->key.inh);
		nulls[4] = false;
		values[5] = Float4GetDatum(stats->stanullfrac);
		nulls[5] = false;
		values[6] = Int32GetDatum(stats->stawidth);
		nulls[6] = false;
		values[7] = Float4GetDatum(stats->stadistinct);
		nulls[7] = false;

		/* Find which slots contain MCV, histogram, and correlation */
		mcv_slot = -1;
		hist_slot = -1;
		corr_slot = -1;
		for (k = 0; k < STATISTIC_NUM_SLOTS; k++)
		{
			int16		kind = (&stats->stakind1)[k];

			if (kind == STATISTIC_KIND_MCV)
				mcv_slot = k;
			else if (kind == STATISTIC_KIND_HISTOGRAM)
				hist_slot = k;
			else if (kind == STATISTIC_KIND_CORRELATION)
				corr_slot = k;
		}

		/* MCV values (as text) and frequencies (as float4[]) */
		if (mcv_slot >= 0)
		{
			AttStatsSlot sslot;

			if (get_attstatsslot(&sslot, statstuple, STATISTIC_KIND_MCV,
								 InvalidOid,
								 ATTSTATSSLOT_VALUES | ATTSTATSSLOT_NUMBERS))
			{
				values[8] = CStringGetTextDatum(
												format_stats_values_as_text(&sslot));
				nulls[8] = false;

				if (sslot.nnumbers > 0)
				{
					Datum	   *num_datums;
					int			j;

					num_datums = (Datum *) palloc(sslot.nnumbers * sizeof(Datum));
					for (j = 0; j < sslot.nnumbers; j++)
						num_datums[j] = Float4GetDatum(sslot.numbers[j]);
					values[9] = PointerGetDatum(
												construct_array_builtin(num_datums, sslot.nnumbers,
																		FLOAT4OID));
					nulls[9] = false;
					pfree(num_datums);
				}

				free_attstatsslot(&sslot);
			}
		}

		/* Histogram bounds (as text) */
		if (hist_slot >= 0)
		{
			AttStatsSlot sslot;

			if (get_attstatsslot(&sslot, statstuple, STATISTIC_KIND_HISTOGRAM,
								 InvalidOid, ATTSTATSSLOT_VALUES))
			{
				values[10] = CStringGetTextDatum(
												 format_stats_values_as_text(&sslot));
				nulls[10] = false;

				free_attstatsslot(&sslot);
			}
		}

		/* Correlation */
		if (corr_slot >= 0)
		{
			AttStatsSlot sslot;

			if (get_attstatsslot(&sslot, statstuple, STATISTIC_KIND_CORRELATION,
								 InvalidOid, ATTSTATSSLOT_NUMBERS))
			{
				if (sslot.nnumbers > 0)
				{
					values[11] = Float4GetDatum(sslot.numbers[0]);
					nulls[11] = false;
				}

				free_attstatsslot(&sslot);
			}
		}

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc,
							 values, nulls);
	}

	return (Datum) 0;
}

/*
 * pg_gtt_clear_stats
 *		Discard per-session relation- and column-level statistics for a GTT.
 *
 * If a regclass argument is provided, clears stats only for that table.  If
 * NULL (the default), clears stats for every GTT this session has touched.
 * Affects only the calling session's private state; the planner falls back
 * to default estimates until ANALYZE runs again in this session.
 *
 * Privilege rule mirrors the read-side SRFs (pg_gtt_relstats /
 * pg_gtt_colstats): SELECT on the relation is sufficient.  A user can only
 * affect stats they could already see, and the cleared state is private to
 * the calling backend, so a stricter check would not buy anything.
 */
Datum
pg_gtt_clear_stats(PG_FUNCTION_ARGS)
{
	HASH_SEQ_STATUS status;
	GttStorageEntry *entry;
	List	   *to_reset = NIL;
	Oid			filter_relid = InvalidOid;

	if (!PG_ARGISNULL(0))
	{
		filter_relid = PG_GETARG_OID(0);
		if (OidIsValid(filter_relid))
		{
			if (pg_class_aclcheck(filter_relid, GetUserId(),
								  ACL_SELECT) == ACLCHECK_OK)
				GttResetSessionStats(filter_relid);
		}
		PG_RETURN_VOID();
	}

	if (gtt_storage_hash == NULL)
		PG_RETURN_VOID();

	/*
	 * Collect the relids first.  GttResetSessionStats() calls
	 * gtt_reset_colstats_for_rel() which walks the column-stats hash, and
	 * mutating either hash inside an open hash_seq_search of the storage hash
	 * is fragile; deferring keeps the iteration simple.
	 */
	hash_seq_init(&status, gtt_storage_hash);
	while ((entry = (GttStorageEntry *) hash_seq_search(&status)) != NULL)
	{
		if (entry->is_index)
			continue;
		if (pg_class_aclcheck(entry->relid, GetUserId(),
							  ACL_SELECT) != ACLCHECK_OK)
			continue;
		to_reset = lappend_oid(to_reset, entry->relid);
	}

	foreach_oid(relid, to_reset)
		GttResetSessionStats(relid);
	list_free(to_reset);

	PG_RETURN_VOID();
}
