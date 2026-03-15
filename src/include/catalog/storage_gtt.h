/*-------------------------------------------------------------------------
 *
 * storage_gtt.h
 *	  Per-session storage management for global temporary tables.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/storage_gtt.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef STORAGE_GTT_H
#define STORAGE_GTT_H

#include "storage/shmem.h"
#include "utils/rel.h"

extern void GttInitSessionStorage(Relation relation);
extern void GttEnsureSessionStorage(Relation relation);
extern void GttSetNewSessionRelfilenumber(Relation relation,
										  RelFileNumber newrelfilenumber);
extern bool GttHasSessionStorage(Oid relid);
extern bool GttSessionIndexUsable(Oid relid);
extern void GttScheduleDropSessionStorage(Oid relid);
extern void GttBuildIndexIfNeeded(Relation indexRelation);
extern void GttMarkIndexBuildDeferred(Relation indexRelation);
extern void GttPrepareIndexAccess(Relation indexRelation);
extern void PreCommit_gtt_on_commit(void);
extern void GttResetAllSessionData(void);

/*
 * Cross-session sessions registry for DDL safety.  Backends that create
 * per-session GTT storage register themselves in a shared hash; DROP TABLE,
 * ALTER TABLE and CREATE INDEX consult it and error out if any other
 * session has live data.  No session-level heavyweight lock is taken for a
 * GTT, so the registry is the sole cross-session guard.
 */
extern PGDLLIMPORT const ShmemCallbacks GttSessionsShmemCallbacks;
extern void GttCheckDroppable(Oid relid);
extern void GttCheckAlterable(Oid relid);

/* Per-session relation-level statistics for planner */
extern bool GttGetSessionStats(Oid relid, BlockNumber *relpages,
							   double *reltuples, BlockNumber *relallvisible);
extern void GttUpdateSessionStats(Oid relid, BlockNumber relpages,
								  double reltuples, BlockNumber relallvisible);
extern void GttResetSessionStats(Oid relid);

/* Per-session column-level statistics for planner */
extern void GttStoreSessionColumnStats(Oid relid, AttrNumber attnum, bool inh,
									   HeapTuple tuple);
extern HeapTuple GttSearchColumnStats(Oid relid, AttrNumber attnum, bool inh);
extern void GttReleaseColumnStats(HeapTuple tuple);
extern HeapTuple SearchStats(Oid relid, AttrNumber attnum, bool inh,
							 bool include_gtt,
							 void (**freefunc) (HeapTuple));

#endif							/* STORAGE_GTT_H */
