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

#endif							/* STORAGE_GTT_H */
