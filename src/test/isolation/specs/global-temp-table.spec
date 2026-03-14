# Tests for global temporary tables across concurrent sessions.
#
# Two aspects are covered here:
#   1. Cross-session data isolation: the catalog definition is shared
#      but each session has its own rows.
#   2. DDL safety: while one session has materialized per-session data,
#      another session's DROP, ALTER, or CREATE INDEX must not succeed.
#      A shared-memory sessions registry keyed on (dboid, relid,
#      ProcNumber) provides this: GttCheckDroppable and
#      GttCheckAlterable consult it while the caller holds
#      AccessExclusiveLock and error out if any other backend has live
#      storage.  No session-level heavyweight lock is involved, so the
#      DDL fails promptly instead of blocking until the peer
#      disconnects.
#
#      Storage is created lazily: a session registers only when it first
#      writes (or index-scans) a GTT, and an abort of the materializing
#      transaction unlinks the files and deregisters.  So DDL against
#      peers that merely opened, planned, or read a GTT -- or whose only
#      writes were rolled back -- succeeds, while committed data still
#      blocks it.
#
# The DDL-safety permutations have s1 CREATE its own GTT rather than
# using the setup connection, so that only s1 can own a registry entry.
#
# Each DDL-safety permutation uses a distinct GTT name because session
# state persists across permutations and there is no teardown DROP;
# reusing a name would clash with the prior permutation's leftover
# relation.  The temp instance is destroyed after the test anyway.

setup
{
  CREATE GLOBAL TEMPORARY TABLE IF NOT EXISTS gtt_iso (a int);
  CREATE GLOBAL TEMPORARY TABLE IF NOT EXISTS gtt_ocdr (x int)
    ON COMMIT DELETE ROWS;
}

session s1
step s1_insert        { INSERT INTO gtt_iso VALUES (1), (2), (3); }
step s1_select        { SELECT a FROM gtt_iso ORDER BY a; }
# ON COMMIT DELETE ROWS in a non-creating session.  s1 inserts into a
# GTT created by the setup connection and commits; the per-session file
# must be truncated even though s1 was not the session that created the
# GTT.
step s1_ocdr_xact     { BEGIN; INSERT INTO gtt_ocdr VALUES (1), (2); COMMIT; }
step s1_ocdr_select   { SELECT x FROM gtt_ocdr ORDER BY x; }
# Stats-and-data interaction in a non-creating session.  Pre-fix, the
# data wasn't truncated at COMMIT in this session but PreCommit_gtt_on_commit
# still cleared the per-session ANALYZE results, leaving the planner to
# re-estimate against rows that were still on disk.  After the fix data
# and stats are wiped together at COMMIT.
step s1_ocdr_analyze    { BEGIN; INSERT INTO gtt_ocdr VALUES (5), (6), (7);
                          ANALYZE gtt_ocdr; }
step s1_ocdr_stats_in   { SELECT count(*) AS in_xact_stats
                          FROM pg_gtt_relstats()
                          WHERE table_name = 'gtt_ocdr'; }
step s1_ocdr_commit     { COMMIT; }
step s1_ocdr_stats_out  { SELECT count(*) AS post_commit_stats
                          FROM pg_gtt_relstats()
                          WHERE table_name = 'gtt_ocdr'; }
# s1 creates each DDL-test GTT so that only s1 holds the session lock.
step s1_create_drop   { CREATE GLOBAL TEMPORARY TABLE gtt_ddl (x int); }
step s1_create_alter  { CREATE GLOBAL TEMPORARY TABLE gtt_alter (a int); }
step s1_create_idx    { CREATE GLOBAL TEMPORARY TABLE gtt_idx (a int); }
step s1_create_drop_c { CREATE GLOBAL TEMPORARY TABLE gtt_ddl_c (x int); }
step s1_insert_drop_c { INSERT INTO gtt_ddl_c VALUES (1); }
step s1_create_lazy   { CREATE GLOBAL TEMPORARY TABLE gtt_lazy (a int); }
step s1_explain_lazy  { EXPLAIN (COSTS OFF) SELECT * FROM gtt_lazy; }
step s1_select_lazy   { SELECT count(*) FROM gtt_lazy; }
step s1_create_disc   { CREATE GLOBAL TEMPORARY TABLE gtt_disc_iso (a int); }
step s1_insert_disc   { INSERT INTO gtt_disc_iso VALUES (1); }
step s1_discard       { DISCARD ALL; }
step s1_create_idxscan { CREATE GLOBAL TEMPORARY TABLE gtt_idxscan (a int PRIMARY KEY); }
step s1_scan_idxscan  { SET enable_seqscan = off;
                        SELECT * FROM gtt_idxscan WHERE a = 1; }
step s1_begin         { BEGIN; }
step s1_access_drop   { INSERT INTO gtt_ddl VALUES (1); }
# Insert data that would invalidate a SET NOT NULL constraint on column a.
step s1_access_alter  { INSERT INTO gtt_alter VALUES (1), (NULL); }
# Insert data that would fail a UNIQUE-index build on column a.
step s1_access_idx    { INSERT INTO gtt_idx VALUES (1), (1); }
# ROLLBACK unlinks the files the aborted transaction materialized and
# removes s1's registry entries, so a waiting peer DDL then succeeds.
step s1_rollback      { ROLLBACK; }

session s2
step s2_insert        { INSERT INTO gtt_iso VALUES (10), (20), (30); }
step s2_select        { SELECT a FROM gtt_iso ORDER BY a; }
# Mirror of s1's ON COMMIT DELETE ROWS steps, run from a different
# session that also is not the GTT's creator.
step s2_ocdr_xact     { BEGIN; INSERT INTO gtt_ocdr VALUES (10), (20); COMMIT; }
step s2_ocdr_select   { SELECT x FROM gtt_ocdr ORDER BY x; }
# Peer-session DDL; each blocks on s1's transaction-level lock while
# s1's transaction is open, then either succeeds (s1 rolled back, so
# its registration is gone) or fails on the shared-memory
# sessions-registry check (s1 committed data).
step s2_drop          { DROP TABLE gtt_ddl; }
step s2_alter         { ALTER TABLE gtt_alter ALTER COLUMN a SET NOT NULL; }
step s2_create_idx    { CREATE UNIQUE INDEX ON gtt_idx (a); }
step s2_drop_c        { DROP TABLE gtt_ddl_c; }
step s2_drop_lazy     { DROP TABLE gtt_lazy; }
step s2_alter_lazy    { ALTER TABLE gtt_lazy ADD COLUMN b int; }
step s2_drop_disc     { DROP TABLE gtt_disc_iso; }
step s2_drop_idxscan  { DROP TABLE gtt_idxscan; }

# Data isolation: each session sees only its own rows.
permutation s1_insert s2_insert s1_select s2_select

# ON COMMIT DELETE ROWS must fire for sessions that did not create the
# GTT.  Each session registers its own OnCommitItem from
# GttInitSessionStorage, independently of the creating session's setup.
permutation s1_ocdr_xact s2_ocdr_xact s1_ocdr_select s2_ocdr_select

# In the non-creator session, stats and data must be cleared in lockstep
# at COMMIT.  Pre-fix the data lingered while the stats were wiped, so
# the planner re-estimated against rows that were still present.
permutation s1_ocdr_analyze s1_ocdr_stats_in s1_ocdr_commit
            s1_ocdr_stats_out s1_ocdr_select

# DDL safety - DROP after rollback: s2's DROP waits on s1's open
# transaction; s1's ROLLBACK discards the materialized storage and the
# registration, so the DROP then succeeds.
permutation s1_create_drop s1_begin s1_access_drop s2_drop s1_rollback

# DDL safety - ALTER after rollback: same shape; the rolled-back NULL
# rows no longer exist, so SET NOT NULL may proceed.
permutation s1_create_alter s1_begin s1_access_alter s2_alter s1_rollback

# DDL safety - CREATE UNIQUE INDEX after rollback: the rolled-back
# duplicates no longer exist, so the index may be created.
permutation s1_create_idx s1_begin s1_access_idx s2_create_idx s1_rollback

# DDL safety - committed data blocks DROP: s1's registration persists
# after commit, so s2's DROP fails on the registry check.
permutation s1_create_drop_c s1_insert_drop_c s2_drop_c

# Lazy materialization: merely planning or reading a GTT does not
# register the session, so peer DDL proceeds without delay.
permutation s1_create_lazy s1_explain_lazy s1_select_lazy s2_alter_lazy s2_drop_lazy

# A committed DISCARD releases the session's storage and registration:
# peer DROP succeeds without delay against an idle pooled session.
permutation s1_create_disc s1_insert_disc s1_discard s2_drop_disc

# An index materialized by a scan (even over an unmaterialized heap)
# counts as live storage for the owning table: peer DROP is refused, so
# no session can be left holding storage for a vanished catalog entry.
permutation s1_create_idxscan s1_scan_idxscan s2_drop_idxscan
