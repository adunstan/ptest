--
-- Tests for global temporary tables
--

-- Basic creation and data isolation
CREATE GLOBAL TEMPORARY TABLE gtt_basic (a int, b text);

-- Verify it exists in pg_class with correct persistence
SELECT relname, relpersistence FROM pg_class WHERE relname = 'gtt_basic';

-- Insert and query data
INSERT INTO gtt_basic VALUES (1, 'hello'), (2, 'world');
SELECT * FROM gtt_basic ORDER BY a;

-- Verify local buffer usage (no WAL)
SELECT count(*) > 0 AS has_data FROM gtt_basic;

-- GLOBAL TEMP shorthand
CREATE GLOBAL TEMP TABLE gtt_short (x int);
SELECT relname, relpersistence FROM pg_class WHERE relname = 'gtt_short';
DROP TABLE gtt_short;

-- Verify the table is visible after reconnect (definition persists)
\c -
SELECT relname, relpersistence FROM pg_class WHERE relname = 'gtt_basic';
-- But data should be gone (new session)
SELECT * FROM gtt_basic ORDER BY a;

-- Re-insert for further tests
INSERT INTO gtt_basic VALUES (10, 'new session');
SELECT * FROM gtt_basic ORDER BY a;

--
-- Indexes
--
CREATE INDEX gtt_basic_idx ON gtt_basic (a);

-- Verify index is usable
SELECT * FROM gtt_basic WHERE a = 10;

-- Index is in catalog with correct persistence
SELECT relname, relpersistence FROM pg_class WHERE relname = 'gtt_basic_idx';

-- Insert more and verify index scan still works
INSERT INTO gtt_basic VALUES (20, 'indexed');
SELECT * FROM gtt_basic WHERE a = 20;

--
-- ON COMMIT PRESERVE ROWS (default)
--
CREATE GLOBAL TEMPORARY TABLE gtt_preserve (x int);
BEGIN;
INSERT INTO gtt_preserve VALUES (1), (2), (3);
COMMIT;
-- Data should survive commit
SELECT * FROM gtt_preserve ORDER BY x;
DROP TABLE gtt_preserve;

-- Explicit ON COMMIT PRESERVE ROWS
CREATE GLOBAL TEMPORARY TABLE gtt_preserve2 (x int) ON COMMIT PRESERVE ROWS;
BEGIN;
INSERT INTO gtt_preserve2 VALUES (1), (2), (3);
COMMIT;
SELECT * FROM gtt_preserve2 ORDER BY x;
DROP TABLE gtt_preserve2;

--
-- ON COMMIT DELETE ROWS
--
CREATE GLOBAL TEMPORARY TABLE gtt_delete (x int) ON COMMIT DELETE ROWS;
BEGIN;
INSERT INTO gtt_delete VALUES (1), (2), (3);
-- Data visible within transaction
SELECT * FROM gtt_delete ORDER BY x;
COMMIT;
-- Data should be gone after commit
SELECT * FROM gtt_delete ORDER BY x;

-- Works across multiple transactions
BEGIN;
INSERT INTO gtt_delete VALUES (10);
COMMIT;
SELECT * FROM gtt_delete ORDER BY x;

-- ON COMMIT DELETE ROWS with indexes
CREATE INDEX gtt_delete_idx ON gtt_delete (x);
BEGIN;
INSERT INTO gtt_delete VALUES (100), (200);
SELECT * FROM gtt_delete ORDER BY x;
COMMIT;
-- Data gone, index still works for next transaction
SELECT * FROM gtt_delete ORDER BY x;
BEGIN;
INSERT INTO gtt_delete VALUES (300);
SELECT * FROM gtt_delete WHERE x = 300;
COMMIT;

DROP TABLE gtt_delete;

-- A first use that consists of INSERT followed by ROLLBACK must leave the
-- GTT usable in subsequent transactions: the abort path that removes the
-- per-session storage hash entry (and unlinks the file via PendingRelDelete)
-- has to invalidate the relcache so the next access re-runs
-- GttInitSessionStorage and re-creates the file.
CREATE GLOBAL TEMPORARY TABLE gtt_abort_recreate (x int) ON COMMIT DELETE ROWS;
BEGIN;
INSERT INTO gtt_abort_recreate VALUES (1);
ROLLBACK;
SELECT * FROM gtt_abort_recreate;
BEGIN;
INSERT INTO gtt_abort_recreate VALUES (99);
SELECT * FROM gtt_abort_recreate;
COMMIT;
SELECT * FROM gtt_abort_recreate;  -- empty (ON COMMIT DELETE ROWS)
DROP TABLE gtt_abort_recreate;

--
-- ON COMMIT DROP is not allowed for GTTs
--
CREATE GLOBAL TEMPORARY TABLE gtt_nodrop (x int) ON COMMIT DROP;  -- ERROR

--
-- DDL restrictions
--

-- Cannot ALTER to LOGGED or UNLOGGED
CREATE GLOBAL TEMPORARY TABLE gtt_noalter (x int);
ALTER TABLE gtt_noalter SET LOGGED;      -- ERROR
ALTER TABLE gtt_noalter SET UNLOGGED;    -- ERROR
DROP TABLE gtt_noalter;

-- CLUSTER, REPACK, and REINDEX are not supported (would change shared relfilenode)
CREATE GLOBAL TEMPORARY TABLE gtt_nocluster (x int);
CREATE INDEX gtt_nocluster_idx ON gtt_nocluster (x);
CLUSTER gtt_nocluster USING gtt_nocluster_idx;  -- ERROR
REPACK gtt_nocluster;                            -- ERROR
REPACK (CONCURRENTLY) gtt_nocluster;             -- ERROR
REINDEX TABLE gtt_nocluster;                     -- ERROR
REINDEX INDEX gtt_nocluster_idx;                 -- ERROR
REINDEX TABLE CONCURRENTLY gtt_nocluster;        -- NOTICE (falls back to non-concurrent), ERROR
REINDEX INDEX CONCURRENTLY gtt_nocluster_idx;    -- NOTICE (falls back to non-concurrent), ERROR
-- VACUUM FULL is rejected (it would reassign the shared relfilenode); plain
-- VACUUM is the supported way to maintain a GTT (see below)
VACUUM FULL gtt_nocluster;
DROP TABLE gtt_nocluster;

-- Database- and partitioned-wide bulk commands silently skip GTTs rather
-- than aborting on the first one.  We can't exercise REPACK; (no target)
-- here because it disallows running inside a transaction block, but we
-- can verify the partitioned-GTT and REINDEX SCHEMA paths.
CREATE SCHEMA gtt_bulk;
CREATE GLOBAL TEMPORARY TABLE gtt_bulk.gtt_part (x int) PARTITION BY RANGE (x);
CREATE GLOBAL TEMPORARY TABLE gtt_bulk.gtt_part_1 PARTITION OF gtt_bulk.gtt_part
    FOR VALUES FROM (0) TO (100);
REPACK gtt_bulk.gtt_part;                        -- ERROR (clean message at parent)
REINDEX SCHEMA gtt_bulk;                         -- no error: GTTs silently skipped
DROP SCHEMA gtt_bulk CASCADE;

-- CREATE STATISTICS is not supported (would require per-session extended stats)
CREATE GLOBAL TEMPORARY TABLE gtt_nostats (a int, b int);
CREATE STATISTICS gtt_nostats_stats ON a, b FROM gtt_nostats;  -- ERROR
DROP TABLE gtt_nostats;

-- CREATE TABLE ... (LIKE ...): a GTT cannot carry extended statistics, so
-- INCLUDING STATISTICS (and INCLUDING ALL) skips them with a warning rather
-- than failing the command.
CREATE TABLE gtt_like_src (a int, b int);
CREATE STATISTICS gtt_like_src_stats ON a, b FROM gtt_like_src;
CREATE GLOBAL TEMPORARY TABLE gtt_like_all (LIKE gtt_like_src INCLUDING ALL);  -- WARNING
SELECT count(*) AS extstats_on_gtt
    FROM pg_statistic_ext WHERE stxrelid = 'gtt_like_all'::regclass;
-- a plain LIKE (no statistics requested) is unaffected
CREATE GLOBAL TEMPORARY TABLE gtt_like_plain (LIKE gtt_like_src);
DROP TABLE gtt_like_all, gtt_like_plain, gtt_like_src;

-- CREATE INDEX CONCURRENTLY falls back to non-concurrent
CREATE GLOBAL TEMPORARY TABLE gtt_cic (x int);
INSERT INTO gtt_cic VALUES (1), (2), (3);
CREATE INDEX CONCURRENTLY gtt_cic_idx ON gtt_cic (x);
-- Verify it works
SELECT * FROM gtt_cic WHERE x = 2;

-- DROP INDEX CONCURRENTLY likewise falls back to non-concurrent: the
-- multi-transaction protocol would mark the index invalid in the shared
-- catalog while peers' per-session storage is unaffected.
DROP INDEX CONCURRENTLY gtt_cic_idx;
SELECT * FROM gtt_cic WHERE x = 2;
DROP TABLE gtt_cic;

-- VACUUM freezes a GTT's session-local data in place.  The new freeze
-- horizon is kept per session; the shared pg_class row keeps invalid
-- relfrozenxid/relminmxid (it cannot describe any one session's data).
CREATE GLOBAL TEMPORARY TABLE gtt_vacuum (x int);
-- Without session data there is nothing to freeze, so VACUUM is skipped.
VACUUM gtt_vacuum;
INSERT INTO gtt_vacuum VALUES (1), (2), (3);
-- With data, VACUUM and VACUUM FREEZE run against the session-local storage.
VACUUM gtt_vacuum;
VACUUM (FREEZE) gtt_vacuum;
-- Data still accessible after vacuuming/freezing.
SELECT count(*) FROM gtt_vacuum;
-- The shared catalog row is untouched: freeze state lives per session.
SELECT relfrozenxid, relminmxid FROM pg_class WHERE relname = 'gtt_vacuum';
-- VACUUM ANALYZE runs both the vacuum and the (session-local) analyze.
VACUUM ANALYZE gtt_vacuum;
SELECT count(*) FROM gtt_vacuum;
DROP TABLE gtt_vacuum;

-- FK: GTT can reference another GTT
CREATE GLOBAL TEMPORARY TABLE gtt_pk (id int PRIMARY KEY);
CREATE GLOBAL TEMPORARY TABLE gtt_fk (id int REFERENCES gtt_pk(id));
DROP TABLE gtt_fk;
DROP TABLE gtt_pk;

-- FK: GTT cannot reference permanent table
CREATE TABLE perm_pk (id int PRIMARY KEY);
CREATE GLOBAL TEMPORARY TABLE gtt_fk_bad (id int REFERENCES perm_pk(id));  -- ERROR
DROP TABLE perm_pk;

-- FK: permanent table cannot reference GTT
CREATE GLOBAL TEMPORARY TABLE gtt_pk2 (id int PRIMARY KEY);
CREATE TABLE perm_fk_bad (id int REFERENCES gtt_pk2(id));  -- ERROR
DROP TABLE gtt_pk2;

-- Inheritance restrictions: cannot mix GTT and local temp
CREATE GLOBAL TEMPORARY TABLE gtt_parent (x int);
CREATE TEMPORARY TABLE local_child () INHERITS (gtt_parent);  -- ERROR
CREATE TEMPORARY TABLE local_parent (x int);
CREATE GLOBAL TEMPORARY TABLE gtt_child () INHERITS (local_parent);  -- ERROR
DROP TABLE local_parent;
-- Inheritance restrictions: cannot mix GTT and permanent
CREATE TABLE perm_child () INHERITS (gtt_parent);  -- ERROR
-- GTT can inherit from another GTT
CREATE GLOBAL TEMPORARY TABLE gtt_child2 () INHERITS (gtt_parent);
INSERT INTO gtt_child2 VALUES (42);
SELECT * FROM gtt_parent;  -- should see child's data
DROP TABLE gtt_child2;
DROP TABLE gtt_parent;

-- Views on GTTs see per-session data
CREATE GLOBAL TEMPORARY TABLE gtt_viewtest (id int, val text);
CREATE VIEW gtt_view AS SELECT * FROM gtt_viewtest;
INSERT INTO gtt_viewtest VALUES (1, 'hello'), (2, 'world');
SELECT * FROM gtt_view ORDER BY id;
DROP VIEW gtt_view;
DROP TABLE gtt_viewtest;

-- Triggers on GTTs
CREATE GLOBAL TEMPORARY TABLE gtt_trigger (id int, val text);
CREATE FUNCTION gtt_trigger_fn() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    NEW.val := NEW.val || ' (triggered)';
    RETURN NEW;
END;
$$;
CREATE TRIGGER gtt_trg BEFORE INSERT ON gtt_trigger
    FOR EACH ROW EXECUTE FUNCTION gtt_trigger_fn();
INSERT INTO gtt_trigger VALUES (1, 'test');
SELECT * FROM gtt_trigger;
DROP TABLE gtt_trigger;
DROP FUNCTION gtt_trigger_fn();

-- SERIAL (sequence) columns on GTTs: the backing sequence is itself a
-- global temporary sequence so each session gets its own counter.
CREATE GLOBAL TEMPORARY TABLE gtt_serial (id serial, val text);
SELECT relname, relpersistence FROM pg_class
    WHERE relname LIKE 'gtt_serial%' ORDER BY relname;
INSERT INTO gtt_serial (val) VALUES ('a'), ('b'), ('c');
SELECT * FROM gtt_serial ORDER BY id;
-- currval/setval follow the per-session counter
SELECT currval('gtt_serial_id_seq');
SELECT setval('gtt_serial_id_seq', 100);
INSERT INTO gtt_serial (val) VALUES ('d');
SELECT * FROM gtt_serial ORDER BY id;
DROP TABLE gtt_serial;

-- DROP CASCADE with dependent view
CREATE GLOBAL TEMPORARY TABLE gtt_deptest (x int);
CREATE VIEW gtt_depview AS SELECT x FROM gtt_deptest;
DROP TABLE gtt_deptest;  -- ERROR: view depends on it
DROP TABLE gtt_deptest CASCADE;  -- OK: drops view too
-- Verify the view is gone
SELECT * FROM gtt_depview;  -- ERROR

-- Cannot create GTT in temporary schema
CREATE TEMPORARY TABLE force_temp_schema (x int);
CREATE GLOBAL TEMPORARY TABLE pg_temp.gtt_in_temp (x int);  -- ERROR
DROP TABLE force_temp_schema;

--
-- Toast tables
--
CREATE GLOBAL TEMPORARY TABLE gtt_toast (id int, data text);
INSERT INTO gtt_toast VALUES (1, repeat('x', 10000));
SELECT id, length(data) FROM gtt_toast;
DROP TABLE gtt_toast;

--
-- Multiple GTTs
--
CREATE GLOBAL TEMPORARY TABLE gtt_multi1 (a int);
CREATE GLOBAL TEMPORARY TABLE gtt_multi2 (b text);
INSERT INTO gtt_multi1 VALUES (1), (2);
INSERT INTO gtt_multi2 VALUES ('a'), ('b');
SELECT * FROM gtt_multi1 ORDER BY a;
SELECT * FROM gtt_multi2 ORDER BY b;
DROP TABLE gtt_multi1;
DROP TABLE gtt_multi2;

--
-- TRUNCATE
--
CREATE GLOBAL TEMPORARY TABLE gtt_trunc (x int);
INSERT INTO gtt_trunc VALUES (1), (2), (3);
SELECT count(*) FROM gtt_trunc;
TRUNCATE gtt_trunc;
SELECT count(*) FROM gtt_trunc;
-- Insert after truncate still works
INSERT INTO gtt_trunc VALUES (10);
SELECT * FROM gtt_trunc ORDER BY x;
DROP TABLE gtt_trunc;

-- TRUNCATE with indexes
CREATE GLOBAL TEMPORARY TABLE gtt_trunc2 (x int);
CREATE INDEX gtt_trunc2_idx ON gtt_trunc2 (x);
INSERT INTO gtt_trunc2 VALUES (1), (2), (3);
SELECT * FROM gtt_trunc2 ORDER BY x;
TRUNCATE gtt_trunc2;
SELECT * FROM gtt_trunc2 ORDER BY x;
-- Index works after truncate
INSERT INTO gtt_trunc2 VALUES (99);
SELECT * FROM gtt_trunc2 WHERE x = 99;
DROP TABLE gtt_trunc2;

--
-- COPY
--
CREATE GLOBAL TEMPORARY TABLE gtt_copy (a int, b text);
COPY gtt_copy FROM stdin;
1	alpha
2	beta
3	gamma
\.
SELECT * FROM gtt_copy ORDER BY a;
COPY gtt_copy TO stdout;
DROP TABLE gtt_copy;

--
-- UPDATE and DELETE
--
CREATE GLOBAL TEMPORARY TABLE gtt_dml (id int, val text);
INSERT INTO gtt_dml VALUES (1, 'one'), (2, 'two'), (3, 'three');
UPDATE gtt_dml SET val = 'TWO' WHERE id = 2;
DELETE FROM gtt_dml WHERE id = 3;
SELECT * FROM gtt_dml ORDER BY id;
DROP TABLE gtt_dml;

--
-- Data survives subtransactions
--
CREATE GLOBAL TEMPORARY TABLE gtt_subxact (x int);
BEGIN;
INSERT INTO gtt_subxact VALUES (1);
SAVEPOINT sp1;
INSERT INTO gtt_subxact VALUES (2);
ROLLBACK TO sp1;
INSERT INTO gtt_subxact VALUES (3);
COMMIT;
SELECT * FROM gtt_subxact ORDER BY x;
DROP TABLE gtt_subxact;

--
-- Verify data is session-private via reconnect
--
INSERT INTO gtt_basic VALUES (99, 'before reconnect');
SELECT * FROM gtt_basic ORDER BY a;
\c -
-- New session: should not see previous session's data
SELECT * FROM gtt_basic ORDER BY a;

--
-- Clean up
--
DROP TABLE gtt_basic;

--
-- GTT sequences: counter is per-session
--
CREATE GLOBAL TEMPORARY TABLE gtt_seq_tbl (id serial, v text);
INSERT INTO gtt_seq_tbl (v) VALUES ('a'), ('b'), ('c');
SELECT * FROM gtt_seq_tbl ORDER BY id;
\c -
-- Fresh session: table empty, counter restarts at 1.
SELECT * FROM gtt_seq_tbl ORDER BY id;
INSERT INTO gtt_seq_tbl (v) VALUES ('x'), ('y');
SELECT * FROM gtt_seq_tbl ORDER BY id;
DROP TABLE gtt_seq_tbl;

-- Standalone GLOBAL TEMPORARY SEQUENCE: definition persistent, counter per-session.
CREATE GLOBAL TEMPORARY SEQUENCE gtt_seq START 10 INCREMENT 5;
SELECT relname, relpersistence FROM pg_class WHERE relname = 'gtt_seq';
SELECT nextval('gtt_seq'), nextval('gtt_seq'), nextval('gtt_seq');
\c -
-- Fresh session sees the definition and gets its own counter from START.
SELECT nextval('gtt_seq'), nextval('gtt_seq');
DROP SEQUENCE gtt_seq;

--
-- Row Level Security on GTTs
--
CREATE GLOBAL TEMPORARY TABLE gtt_rls (id int, visible_to text);
ALTER TABLE gtt_rls ENABLE ROW LEVEL SECURITY;
CREATE ROLE regress_gtt_rls_user;
GRANT SELECT, INSERT ON gtt_rls TO regress_gtt_rls_user;
CREATE POLICY gtt_rls_policy ON gtt_rls
    USING (visible_to = current_user);
-- Insert rows: some for regress_gtt_rls_user, some not
INSERT INTO gtt_rls VALUES (1, 'regress_gtt_rls_user'), (2, 'other_user'), (3, 'regress_gtt_rls_user');
-- Table owner bypasses RLS by default
SELECT * FROM gtt_rls ORDER BY id;
SET ROLE regress_gtt_rls_user;
-- Non-owner should only see rows matching the policy
SELECT * FROM gtt_rls ORDER BY id;
-- Non-owner insert should work
INSERT INTO gtt_rls VALUES (4, 'regress_gtt_rls_user');
-- Should see only matching rows
SELECT * FROM gtt_rls ORDER BY id;
RESET ROLE;
-- Owner sees all rows again
SELECT * FROM gtt_rls ORDER BY id;
DROP TABLE gtt_rls;
DROP ROLE regress_gtt_rls_user;

-- A policy on a permanent table may reference a GTT: the GTT's definition is
-- permanent, so the reference is always resolvable (unlike a local temporary
-- table, whose definition exists only in its own session).  The subquery is
-- evaluated against the current session's per-session data.
CREATE TABLE gtt_rls_perm (a int);
CREATE GLOBAL TEMPORARY TABLE gtt_rls_ref (a int);
CREATE POLICY gtt_rls_perm_policy ON gtt_rls_perm AS RESTRICTIVE
    USING ((SELECT a IS NOT NULL FROM gtt_rls_ref WHERE a = 1));
ALTER TABLE gtt_rls_perm ENABLE ROW LEVEL SECURITY;
INSERT INTO gtt_rls_perm VALUES (1);
DROP TABLE gtt_rls_perm, gtt_rls_ref;

--
-- ANALYZE and per-session statistics
--
CREATE GLOBAL TEMPORARY TABLE gtt_analyze (id int, val text);
INSERT INTO gtt_analyze SELECT g, 'row ' || g FROM generate_series(1, 1000) g;

-- Before ANALYZE: pg_class has default values
SELECT relpages, reltuples FROM pg_class WHERE relname = 'gtt_analyze';

-- Run ANALYZE
ANALYZE gtt_analyze;

-- pg_class should NOT be updated (stats are per-session, not shared)
SELECT relpages, reltuples FROM pg_class WHERE relname = 'gtt_analyze';

-- Data is queryable after ANALYZE
SELECT count(*) FROM gtt_analyze WHERE id <= 500;

-- ANALYZE with indexes
CREATE INDEX gtt_analyze_idx ON gtt_analyze (id);
ANALYZE gtt_analyze;
SELECT count(*) FROM gtt_analyze WHERE id = 500;

-- Column stats should NOT be written to shared pg_statistic
SELECT count(*) FROM pg_statistic WHERE starelid = 'gtt_analyze'::regclass;

-- Column stats should be used by planner (distinct count for id should be ~1000)
-- Check that stadistinct is reflected in planner estimates
EXPLAIN (COSTS OFF) SELECT * FROM gtt_analyze WHERE id = 42;

-- TRUNCATE should reset per-session relation stats (column stats are
-- retained, as pg_statistic is for regular tables)
TRUNCATE gtt_analyze;
-- After truncate and re-insert, stats should reflect new data after ANALYZE
INSERT INTO gtt_analyze SELECT g, 'new ' || g FROM generate_series(1, 100) g;
ANALYZE gtt_analyze;
SELECT count(*) FROM gtt_analyze WHERE id <= 50;

-- Verify no column stats leaked to pg_statistic after re-ANALYZE
SELECT count(*) FROM pg_statistic WHERE starelid = 'gtt_analyze'::regclass;

-- Verify per-session stats don't survive reconnect
\c -
-- New session sees shared pg_class (unchanged by ANALYZE)
SELECT relpages, reltuples FROM pg_class WHERE relname = 'gtt_analyze';

-- No column stats in new session either
SELECT count(*) FROM pg_statistic WHERE starelid = 'gtt_analyze'::regclass;

DROP TABLE gtt_analyze;

--
-- Per-session column statistics: detailed tests
--
CREATE GLOBAL TEMPORARY TABLE gtt_colstats (id int, category text, val float);
INSERT INTO gtt_colstats
    SELECT g, 'cat_' || (g % 5), random() * 100
    FROM generate_series(1, 10000) g;
CREATE INDEX ON gtt_colstats (category);
ANALYZE gtt_colstats;

-- No column stats in shared catalog
SELECT count(*) FROM pg_statistic WHERE starelid = 'gtt_colstats'::regclass;

-- Planner should use per-session stats for category selectivity
-- With 5 distinct categories, ~2000 rows per category
EXPLAIN (COSTS OFF) SELECT * FROM gtt_colstats WHERE category = 'cat_0';

-- Index stats should also be collected per-session
SELECT count(*) FROM pg_statistic
    WHERE starelid = (SELECT oid FROM pg_class WHERE relname = 'gtt_colstats_category_idx');

-- ON COMMIT DELETE ROWS should reset column stats
CREATE GLOBAL TEMPORARY TABLE gtt_colstats_ocd (id int, cat text) ON COMMIT DELETE ROWS;
BEGIN;
INSERT INTO gtt_colstats_ocd SELECT g, 'c' || (g % 3) FROM generate_series(1, 1000) g;
ANALYZE gtt_colstats_ocd;
COMMIT;
-- After commit, data is gone and stats should be invalidated
-- New data with different distribution
BEGIN;
INSERT INTO gtt_colstats_ocd SELECT g, 'x' FROM generate_series(1, 100) g;
SELECT count(*) FROM gtt_colstats_ocd;
COMMIT;
DROP TABLE gtt_colstats_ocd;

-- Column stats should not survive reconnect
\c -
INSERT INTO gtt_colstats SELECT g, 'cat_' || (g % 5), random() * 100 FROM generate_series(1, 100) g;
-- Without ANALYZE in this session, no per-session column stats
SELECT count(*) FROM pg_statistic WHERE starelid = 'gtt_colstats'::regclass;

DROP TABLE gtt_colstats;

--
-- Per-session stats visibility via SRFs
--
CREATE GLOBAL TEMPORARY TABLE gtt_srf_test (id int, category text, val float);
INSERT INTO gtt_srf_test
    SELECT g, 'cat_' || (g % 5), random() * 100
    FROM generate_series(1, 1000) g;
ANALYZE gtt_srf_test;

-- pg_gtt_relstats should show relation-level stats
SELECT table_name, relpages > 0 AS has_pages, reltuples > 0 AS has_tuples
    FROM pg_gtt_relstats('gtt_srf_test'::regclass);

-- pg_gtt_relstats with NULL shows all GTTs (just this one)
SELECT table_name FROM pg_gtt_relstats() ORDER BY table_name;

-- pg_gtt_colstats should show column-level stats
SELECT attname, null_frac IS NOT NULL AS has_null_frac,
       avg_width > 0 AS has_avg_width,
       n_distinct != 0 AS has_n_distinct
    FROM pg_gtt_colstats('gtt_srf_test'::regclass)
    WHERE NOT inherited
    ORDER BY attnum;

-- Category column should have MCV data
SELECT attname, most_common_vals IS NOT NULL AS has_mcv,
       most_common_freqs IS NOT NULL AS has_mcf,
       histogram_bounds IS NOT NULL AS has_hist,
       correlation IS NOT NULL AS has_corr
    FROM pg_gtt_colstats('gtt_srf_test'::regclass)
    WHERE attname = 'category' AND NOT inherited;

-- id column should have histogram bounds
SELECT attname, histogram_bounds IS NOT NULL AS has_hist,
       correlation IS NOT NULL AS has_corr
    FROM pg_gtt_colstats('gtt_srf_test'::regclass)
    WHERE attname = 'id' AND NOT inherited;

-- Stats should not be in pg_statistic
SELECT count(*) FROM pg_statistic WHERE starelid = 'gtt_srf_test'::regclass;

-- After TRUNCATE, relation-level stats are invalidated; column-level
-- stats are retained, as pg_statistic is for regular tables
TRUNCATE gtt_srf_test;
SELECT count(*) FROM pg_gtt_relstats('gtt_srf_test'::regclass);
SELECT count(*) FROM pg_gtt_colstats('gtt_srf_test'::regclass);

-- Re-insert and re-analyze to get stats back
INSERT INTO gtt_srf_test SELECT g, 'x', g FROM generate_series(1, 100) g;
ANALYZE gtt_srf_test;
SELECT table_name, reltuples FROM pg_gtt_relstats('gtt_srf_test'::regclass);

-- After reconnect, stats should be gone
\c -
SELECT count(*) FROM pg_gtt_relstats('gtt_srf_test'::regclass);
SELECT count(*) FROM pg_gtt_colstats('gtt_srf_test'::regclass);

DROP TABLE gtt_srf_test;

--
-- pg_gtt_clear_stats: discard per-session stats explicitly
--
CREATE GLOBAL TEMPORARY TABLE gtt_clear_stats (id int, val text);
INSERT INTO gtt_clear_stats SELECT g, 'v' || g FROM generate_series(1, 50) g;
ANALYZE gtt_clear_stats;
-- Stats are present
SELECT count(*) FROM pg_gtt_relstats('gtt_clear_stats'::regclass);
SELECT count(*) > 0 AS has_colstats FROM pg_gtt_colstats('gtt_clear_stats'::regclass);

-- Clear and verify both rel- and col-level stats are gone
SELECT pg_gtt_clear_stats('gtt_clear_stats'::regclass);
SELECT count(*) FROM pg_gtt_relstats('gtt_clear_stats'::regclass);
SELECT count(*) FROM pg_gtt_colstats('gtt_clear_stats'::regclass);

-- A subsequent ANALYZE repopulates them
ANALYZE gtt_clear_stats;
SELECT count(*) FROM pg_gtt_relstats('gtt_clear_stats'::regclass);

-- NULL clears stats for every GTT this session has touched
CREATE GLOBAL TEMPORARY TABLE gtt_clear_stats2 (a int);
INSERT INTO gtt_clear_stats2 SELECT generate_series(1, 20);
ANALYZE gtt_clear_stats2;
SELECT count(*) FROM pg_gtt_relstats();
SELECT pg_gtt_clear_stats(NULL);
SELECT count(*) FROM pg_gtt_relstats();

DROP TABLE gtt_clear_stats, gtt_clear_stats2;

--
-- pg_restore_relation_stats / pg_restore_attribute_stats reject GTTs
--
-- The shared pg_class and pg_statistic rows for a GTT are never read by
-- the planner: GttGetSessionStats() / SearchStats() return
-- session-private values from the per-session hash, falling back on the
-- syscache only when the current session has not run ANALYZE.  Allowing
-- pg_restore_*_stats to write the shared values would surface them to
-- exactly that fallback path, undermining the per-session isolation.
--
CREATE GLOBAL TEMPORARY TABLE gtt_restore_stats (a int, b text);

SELECT pg_restore_relation_stats(
    'schemaname', 'public',
    'relname', 'gtt_restore_stats',
    'relpages', 42::integer,
    'reltuples', 1000::real);

SELECT pg_clear_relation_stats('public', 'gtt_restore_stats');

SELECT pg_restore_attribute_stats(
    'schemaname', 'public',
    'relname', 'gtt_restore_stats',
    'attname', 'a',
    'inherited', false,
    'null_frac', 0.0::real,
    'avg_width', 4::integer,
    'n_distinct', -1.0::real);

SELECT pg_clear_attribute_stats('public', 'gtt_restore_stats', 'a', false);

DROP TABLE gtt_restore_stats;

--
-- Subtransaction abort after first access to a GTT
-- The per-session storage file is unlinked by PendingRelDelete when the
-- subxact aborts; the hash entry must be reset so a subsequent access
-- in the outer transaction re-creates the storage, not error out.
--
CREATE GLOBAL TEMPORARY TABLE gtt_subxact (x int);
BEGIN;
SAVEPOINT s;
INSERT INTO gtt_subxact VALUES (1);  -- first access, creates storage
ROLLBACK TO SAVEPOINT s;             -- storage unlinked, hash entry cleaned
-- outer xact continues: next access must re-create storage cleanly
INSERT INTO gtt_subxact VALUES (2);
SELECT * FROM gtt_subxact;
COMMIT;
SELECT * FROM gtt_subxact;
DROP TABLE gtt_subxact;

--
-- DROP then recreate a same-named GTT in the same session
-- Exercises the commit-side cleanup in gtt_xact_callback: the hash
-- entry from the dropped table must be gone so the new CREATE uses
-- fresh per-session state.
--
CREATE GLOBAL TEMPORARY TABLE gtt_recreate (x int);
INSERT INTO gtt_recreate VALUES (1);
DROP TABLE gtt_recreate;
CREATE GLOBAL TEMPORARY TABLE gtt_recreate (y text);
INSERT INTO gtt_recreate VALUES ('fresh');
SELECT * FROM gtt_recreate;
DROP TABLE gtt_recreate;

--
-- ALTER TABLE SET TABLESPACE on a GTT is rejected
--
CREATE GLOBAL TEMPORARY TABLE gtt_notbs (x int);
ALTER TABLE gtt_notbs SET TABLESPACE pg_default;  -- ERROR
DROP TABLE gtt_notbs;

--
-- ALTER TABLE INHERIT / NO INHERIT with GTT persistence mixing
--
CREATE GLOBAL TEMPORARY TABLE gtt_parent2 (x int);
CREATE TABLE perm_child2 (x int);
ALTER TABLE perm_child2 INHERIT gtt_parent2;         -- ERROR (permanent into GTT)
CREATE TEMPORARY TABLE temp_child2 (x int);
ALTER TABLE temp_child2 INHERIT gtt_parent2;         -- ERROR (local temp into GTT)
CREATE GLOBAL TEMPORARY TABLE gtt_child2 (x int);
ALTER TABLE gtt_child2 INHERIT gtt_parent2;          -- OK: GTT→GTT
ALTER TABLE gtt_child2 NO INHERIT gtt_parent2;
DROP TABLE gtt_child2, gtt_parent2, perm_child2, temp_child2;

--
-- ATTACH PARTITION with GTT persistence mixing
--
CREATE GLOBAL TEMPORARY TABLE gtt_part (x int) PARTITION BY RANGE (x);
CREATE TABLE perm_part (x int);
ALTER TABLE gtt_part ATTACH PARTITION perm_part
    FOR VALUES FROM (0) TO (10);                     -- ERROR
CREATE TEMPORARY TABLE temp_part (x int);
ALTER TABLE gtt_part ATTACH PARTITION temp_part
    FOR VALUES FROM (0) TO (10);                     -- ERROR
CREATE GLOBAL TEMPORARY TABLE gtt_part_child (x int);
ALTER TABLE gtt_part ATTACH PARTITION gtt_part_child
    FOR VALUES FROM (0) TO (10);                     -- OK
DROP TABLE gtt_part, perm_part, temp_part;

--
-- CREATE TABLE ... PARTITION OF persistence mixing with a GTT
--
CREATE GLOBAL TEMPORARY TABLE gtt_prange (a int) PARTITION BY RANGE (a);
-- a permanent partition of a GTT parent is rejected
CREATE TABLE gtt_prange_perm PARTITION OF gtt_prange
    FOR VALUES FROM (0) TO (10);                     -- ERROR
-- a local temporary partition of a GTT parent is rejected
CREATE TEMPORARY TABLE gtt_prange_tmp PARTITION OF gtt_prange
    FOR VALUES FROM (0) TO (10);                     -- ERROR: cannot mix
-- a GTT partition of a permanent parent is rejected
CREATE TABLE perm_prange (a int) PARTITION BY RANGE (a);
CREATE GLOBAL TEMPORARY TABLE perm_prange_gtt PARTITION OF perm_prange
    FOR VALUES FROM (0) TO (10);                     -- ERROR
DROP TABLE perm_prange;
-- a GTT partition of a GTT parent is OK
CREATE GLOBAL TEMPORARY TABLE gtt_prange_1 PARTITION OF gtt_prange
    FOR VALUES FROM (0) TO (10);                     -- OK

--
-- SPLIT/MERGE PARTITION is not supported for global temporary tables: the new
-- partition would not inherit the parent's persistence and would wrongly get
-- permanent, cluster-wide storage under a per-session parent.
--
CREATE GLOBAL TEMPORARY TABLE gtt_prange_2 PARTITION OF gtt_prange
    FOR VALUES FROM (10) TO (20);
ALTER TABLE gtt_prange SPLIT PARTITION gtt_prange_2 INTO
    (PARTITION gtt_prange_2a FOR VALUES FROM (10) TO (15),
     PARTITION gtt_prange_2b FOR VALUES FROM (15) TO (20));  -- ERROR
ALTER TABLE gtt_prange MERGE PARTITIONS (gtt_prange_1, gtt_prange_2)
    INTO gtt_prange_m;                               -- ERROR
DROP TABLE gtt_prange;

--
-- Property graphs and GTTs
--
CREATE TABLE gtt_pg_perm_v (id int PRIMARY KEY);
CREATE GLOBAL TEMPORARY TABLE gtt_pg_gtt_v (id int PRIMARY KEY);
-- A property graph itself cannot be global temporary (it has no storage).
CREATE GLOBAL TEMPORARY PROPERTY GRAPH gtt_pg_self
    VERTEX TABLES (gtt_pg_perm_v KEY (id));           -- ERROR
-- But a GTT may serve as an element table of a permanent property graph,
-- because the GTT's definition is permanent; the graph stays permanent and a
-- dependency on the GTT is recorded.
CREATE PROPERTY GRAPH gtt_pg
    VERTEX TABLES (gtt_pg_perm_v KEY (id), gtt_pg_gtt_v KEY (id));
SELECT relpersistence FROM pg_class WHERE relname = 'gtt_pg';
DROP TABLE gtt_pg_gtt_v;                              -- ERROR: graph depends on it
DROP PROPERTY GRAPH gtt_pg;
DROP TABLE gtt_pg_perm_v, gtt_pg_gtt_v;

--
-- on_commit_delete reloption is internal: user cannot set it directly
--
CREATE TABLE perm_ocd (x int) WITH (on_commit_delete = true);  -- ERROR
CREATE GLOBAL TEMPORARY TABLE gtt_ocd (x int)
  WITH (on_commit_delete = true);                    -- ERROR: internal reloption
CREATE GLOBAL TEMPORARY TABLE gtt_ocd (x int) ON COMMIT DELETE ROWS;
ALTER TABLE gtt_ocd SET (on_commit_delete = false);  -- ERROR
ALTER TABLE gtt_ocd RESET (on_commit_delete);        -- ERROR
DROP TABLE gtt_ocd;

--
-- pg_relation_filepath on a GTT returns NULL before first access,
-- and a session-local path after.
--
CREATE GLOBAL TEMPORARY TABLE gtt_fp (x int);
-- Run from a fresh session so the table has no per-session storage
\c -
-- pg_relation_filepath/pg_relation_size read session-local GTT storage state
-- that a parallel worker cannot see, so they must run in the leader; force
-- that here so the checks are stable under debug_parallel_query.
SET debug_parallel_query = off;
SELECT pg_relation_filepath('gtt_fp') IS NULL AS no_storage_yet;
INSERT INTO gtt_fp VALUES (1);
-- After first access, the path is the session-local file name (t<proc>_<rel>)
SELECT pg_relation_filepath('gtt_fp') ~ '/t[0-9]+_[0-9]+$' AS got_session_path;
RESET debug_parallel_query;
DROP TABLE gtt_fp;

--
-- pg_gtt_relstats / pg_gtt_colstats honour SELECT privilege
--
CREATE ROLE regress_gtt_acl_role;
CREATE GLOBAL TEMPORARY TABLE gtt_acl (a int, b text);
INSERT INTO gtt_acl
SELECT i, repeat('x', i % 10)
FROM generate_series(1, 100) i;
ANALYZE gtt_acl;
-- Owner sees stats
SELECT count(*) > 0 FROM pg_gtt_relstats('gtt_acl'::regclass);
SELECT count(*) > 0 FROM pg_gtt_colstats('gtt_acl'::regclass);
-- Unprivileged role sees nothing
SET ROLE regress_gtt_acl_role;
SELECT count(*) FROM pg_gtt_relstats('gtt_acl'::regclass);
SELECT count(*) FROM pg_gtt_colstats('gtt_acl'::regclass);
RESET ROLE;
-- Grant table-level SELECT: both SRFs become visible
GRANT SELECT ON gtt_acl TO regress_gtt_acl_role;
SET ROLE regress_gtt_acl_role;
SELECT count(*) > 0 FROM pg_gtt_relstats('gtt_acl'::regclass);
SELECT count(*) > 0 FROM pg_gtt_colstats('gtt_acl'::regclass);
RESET ROLE;
REVOKE SELECT ON gtt_acl FROM regress_gtt_acl_role;
-- Grant column-level SELECT on just one column: colstats filters per-column
GRANT SELECT (a) ON gtt_acl TO regress_gtt_acl_role;
SET ROLE regress_gtt_acl_role;
SELECT attname FROM pg_gtt_colstats('gtt_acl'::regclass) ORDER BY attname;
RESET ROLE;
DROP TABLE gtt_acl;
DROP ROLE regress_gtt_acl_role;

--
-- ON COMMIT DELETE ROWS with FK between two GTTs
-- PreCommit_gtt_on_commit must batch the truncation so
-- heap_truncate_check_FKs validates FK integrity across the set,
-- matching regular-temp ON COMMIT DELETE ROWS behaviour.
--
CREATE GLOBAL TEMPORARY TABLE gtt_ocdr_pk (id int PRIMARY KEY)
    ON COMMIT DELETE ROWS;
CREATE GLOBAL TEMPORARY TABLE gtt_ocdr_fk (id int REFERENCES gtt_ocdr_pk(id))
    ON COMMIT DELETE ROWS;
BEGIN;
INSERT INTO gtt_ocdr_pk VALUES (1), (2);
INSERT INTO gtt_ocdr_fk VALUES (1);
COMMIT;
-- Both should be empty after commit; neither should have errored
-- during commit's truncation
SELECT count(*) FROM gtt_ocdr_pk;
SELECT count(*) FROM gtt_ocdr_fk;
-- Next transaction works cleanly
BEGIN;
INSERT INTO gtt_ocdr_pk VALUES (10);
INSERT INTO gtt_ocdr_fk VALUES (10);
SELECT count(*) FROM gtt_ocdr_pk;
SELECT count(*) FROM gtt_ocdr_fk;
COMMIT;
DROP TABLE gtt_ocdr_fk;
DROP TABLE gtt_ocdr_pk;

--
-- ALTER TABLE operations that would rewrite the heap are rejected,
-- because rewriting rotates the catalog relfilenode that every
-- session's per-session storage is keyed by.
--
CREATE GLOBAL TEMPORARY TABLE gtt_norewrite (a int, b varchar(10));
INSERT INTO gtt_norewrite VALUES (1, 'hi');
-- Type change that forces a rewrite: ERROR
ALTER TABLE gtt_norewrite ALTER COLUMN a TYPE bigint;  -- ERROR
-- Binary-coercible varchar length change (no rewrite): OK
ALTER TABLE gtt_norewrite ALTER COLUMN b TYPE varchar(20);
SELECT * FROM gtt_norewrite;
-- ADD COLUMN with volatile default (forces rewrite): ERROR
ALTER TABLE gtt_norewrite ADD COLUMN c int DEFAULT random()::int;  -- ERROR
-- ADD COLUMN with constant default (no rewrite on modern PG): OK
ALTER TABLE gtt_norewrite ADD COLUMN d int DEFAULT 42;
SELECT * FROM gtt_norewrite;
DROP TABLE gtt_norewrite;

--
-- CREATE TABLE AS and SELECT INTO create GTTs correctly
--
CREATE GLOBAL TEMPORARY TABLE gtt_cta AS
    SELECT i AS x, 'row_' || i::text AS y FROM generate_series(1, 10) i;
SELECT relpersistence FROM pg_class WHERE relname = 'gtt_cta';
SELECT count(*) FROM gtt_cta;
-- Data is per-session; reconnect sees no rows
\c -
SELECT count(*) FROM gtt_cta;
DROP TABLE gtt_cta;

-- SELECT INTO GLOBAL TEMPORARY should now produce a GTT, not a local TEMP
SELECT i AS x INTO GLOBAL TEMPORARY gtt_si FROM generate_series(1, 5) i;
SELECT relpersistence FROM pg_class WHERE relname = 'gtt_si';
SELECT count(*) FROM gtt_si;
\c -
SELECT count(*) FROM gtt_si;
DROP TABLE gtt_si;

--
-- WITH HOLD cursor on an ON COMMIT DELETE ROWS GTT.
-- PreCommit_Portals(false) materialises held portals before
-- PreCommit_gtt_on_commit truncates per-session data, so the
-- cursor's rows survive the commit.
--
CREATE GLOBAL TEMPORARY TABLE gtt_hold_ocdr (x int) ON COMMIT DELETE ROWS;
BEGIN;
INSERT INTO gtt_hold_ocdr SELECT i FROM generate_series(1, 5) i;
DECLARE gtt_hold_cur CURSOR WITH HOLD FOR
    SELECT * FROM gtt_hold_ocdr ORDER BY x;
COMMIT;
-- Table was truncated at commit, but the held cursor materialised first
SELECT count(*) FROM gtt_hold_ocdr;
FETCH ALL FROM gtt_hold_cur;
CLOSE gtt_hold_cur;
DROP TABLE gtt_hold_ocdr;

--
-- Subtransaction rollback
--
-- gtt_subxact_callback has to reconcile entries whose create/storage/index
-- state was established inside an aborted savepoint.  Verify that data
-- mutations in a rolled-back savepoint are not visible afterwards, and
-- that the GTT remains usable on the next top-level transaction.
--
CREATE GLOBAL TEMPORARY TABLE gtt_subxact (x int);
BEGIN;
INSERT INTO gtt_subxact VALUES (1);
SAVEPOINT sp;
INSERT INTO gtt_subxact VALUES (2), (3);
SELECT count(*) FROM gtt_subxact;  -- 3
ROLLBACK TO SAVEPOINT sp;
SELECT count(*) FROM gtt_subxact;  -- 1
COMMIT;
SELECT count(*) FROM gtt_subxact;  -- 1
-- Subxact that creates a GTT, then rolls back: the relation vanishes
BEGIN;
SAVEPOINT sp;
CREATE GLOBAL TEMPORARY TABLE gtt_subxact_new (y int);
INSERT INTO gtt_subxact_new VALUES (42);
ROLLBACK TO SAVEPOINT sp;
SELECT 1 FROM pg_class WHERE relname = 'gtt_subxact_new';  -- 0 rows
COMMIT;
DROP TABLE gtt_subxact;

--
-- Self-drop: a session that has touched a GTT must be able to DROP it.
-- GttCheckDroppable skips entries matching our own ProcNumber.
--
CREATE GLOBAL TEMPORARY TABLE gtt_selfdrop (x int);
INSERT INTO gtt_selfdrop VALUES (1), (2);
SELECT count(*) FROM gtt_selfdrop;
DROP TABLE gtt_selfdrop;

--
-- ON COMMIT DELETE ROWS resets per-session statistics.
-- PreCommit_gtt_on_commit calls GttResetSessionStats after truncating, so
-- after commit the planner should see no per-session stats until the next
-- ANALYZE.
--
CREATE GLOBAL TEMPORARY TABLE gtt_stats_ocdr (x int) ON COMMIT DELETE ROWS;
BEGIN;
INSERT INTO gtt_stats_ocdr SELECT g FROM generate_series(1, 100) g;
ANALYZE gtt_stats_ocdr;
-- Stats are visible within the transaction
SELECT table_name FROM pg_gtt_relstats()
 WHERE table_name = 'gtt_stats_ocdr';
COMMIT;
-- Commit truncated the data and cleared per-session stats
SELECT table_name FROM pg_gtt_relstats()
 WHERE table_name = 'gtt_stats_ocdr';
DROP TABLE gtt_stats_ocdr;

--
-- Subtransaction-abort counterpart of the gtt_abort_recreate test.  When
-- the per-session entry is FIRST CREATED inside a subxact, ROLLBACK TO
-- SAVEPOINT removes that entry and unlinks the file (PendingRelDelete is
-- subxact-aware).  Without the relcache invalidation in
-- gtt_subxact_callback, the outer xact's relcache entry would keep
-- pointing at the now-deleted file.  Force a fresh session with \c -
-- so this session has no hash entry going in; then the first INSERT in
-- SAVEPOINT s1 hits the !found branch with create_subid = s1.
--
CREATE GLOBAL TEMPORARY TABLE gtt_subxact_abort (x int) ON COMMIT DELETE ROWS;
\c -
BEGIN;
SAVEPOINT s1;
INSERT INTO gtt_subxact_abort VALUES (1);
ROLLBACK TO SAVEPOINT s1;
SELECT * FROM gtt_subxact_abort;        -- no error, 0 rows
INSERT INTO gtt_subxact_abort VALUES (99);
SELECT * FROM gtt_subxact_abort;
COMMIT;
SELECT * FROM gtt_subxact_abort;        -- empty after ON COMMIT DELETE ROWS
DROP TABLE gtt_subxact_abort;

--
-- Heap-only access method enforcement: a GTT may only use the heap table
-- access method (its per-session storage and wraparound handling are
-- heap-specific).  A non-heap access method is rejected at CREATE, whether
-- requested with USING or inherited from default_table_access_method.  Use a
-- second AM OID that reuses heap's handler to exercise the check without a
-- separate AM implementation.
--
CREATE ACCESS METHOD gtt_fake_heap TYPE TABLE HANDLER heap_tableam_handler;
CREATE GLOBAL TEMPORARY TABLE gtt_am_bad (a int) USING gtt_fake_heap;  -- error
SET default_table_access_method = gtt_fake_heap;
CREATE GLOBAL TEMPORARY TABLE gtt_am_bad (a int);                     -- error
RESET default_table_access_method;
CREATE GLOBAL TEMPORARY TABLE gtt_am_ok (a int) USING heap;           -- ok
DROP TABLE gtt_am_ok;
CREATE TABLE gtt_am_reg (a int) USING gtt_fake_heap;                  -- ok (not a GTT)
DROP TABLE gtt_am_reg;
DROP ACCESS METHOD gtt_fake_heap;

--
-- global_temp_xid_warn_margin GUC: controls the head room before the
-- transaction-ID horizon at which a warning is issued for aging GTT data.
-- The hard error is fixed at the horizon and is exercised by a TAP test
-- (the warning/error cannot be triggered deterministically here).
--
SHOW global_temp_xid_warn_margin;                 -- default 100000000
SET global_temp_xid_warn_margin = 0;              -- disables the warning
SHOW global_temp_xid_warn_margin;
SET global_temp_xid_warn_margin = 250000000;
SHOW global_temp_xid_warn_margin;
SET global_temp_xid_warn_margin = -1;             -- error: below minimum
SET global_temp_xid_warn_margin = 3000000000;     -- error: above maximum
RESET global_temp_xid_warn_margin;

--
-- TRUNCATE of a GTT is transaction-safe: this session's storage is swapped
-- for new, empty files (the shared catalog relfilenode never changes), so
-- ROLLBACK restores the rows, including across savepoints, and indexes
-- remain consistent afterwards.
--
CREATE GLOBAL TEMPORARY TABLE gtt_trunc_xact (id int PRIMARY KEY, t text);
INSERT INTO gtt_trunc_xact SELECT g, 'x' || g FROM generate_series(1, 100) g;
-- catalog relfilenode is stable across TRUNCATE (it equals the OID at
-- creation, and must still do so afterwards)
SELECT relfilenode = oid AS filenode_premise
  FROM pg_class WHERE relname = 'gtt_trunc_xact';
BEGIN;
TRUNCATE gtt_trunc_xact;
SELECT count(*) FROM gtt_trunc_xact;              -- 0 inside the transaction
ROLLBACK;
SELECT count(*) FROM gtt_trunc_xact;              -- 100 again
BEGIN;
SAVEPOINT s1;
TRUNCATE gtt_trunc_xact;
ROLLBACK TO s1;
SELECT count(*) FROM gtt_trunc_xact;              -- 100: subxact rollback
SAVEPOINT s2;
TRUNCATE gtt_trunc_xact;
RELEASE s2;
COMMIT;
SELECT count(*) FROM gtt_trunc_xact;              -- 0: released swap commits
-- index is rebuilt correctly after a rolled-back truncate
INSERT INTO gtt_trunc_xact SELECT g, 'y' || g FROM generate_series(1, 30) g;
BEGIN;
TRUNCATE gtt_trunc_xact;
INSERT INTO gtt_trunc_xact VALUES (7, 'seven');
ROLLBACK;
SET enable_seqscan = off;
SELECT t FROM gtt_trunc_xact WHERE id = 13;       -- index scan finds old row
RESET enable_seqscan;
SELECT relfilenode = oid AS filenode_unchanged
  FROM pg_class WHERE relname = 'gtt_trunc_xact';
DROP TABLE gtt_trunc_xact;

--
-- Sequence RESTART paths swap only the session-local storage as well.
--
CREATE GLOBAL TEMPORARY SEQUENCE gtt_seq_restart;
SELECT nextval('gtt_seq_restart'), nextval('gtt_seq_restart');
ALTER SEQUENCE gtt_seq_restart RESTART;
SELECT nextval('gtt_seq_restart');                -- 1 again
SELECT relfilenode = oid AS filenode_unchanged
  FROM pg_class WHERE relname = 'gtt_seq_restart';
DROP SEQUENCE gtt_seq_restart;
-- TRUNCATE ... RESTART IDENTITY, including rollback
CREATE GLOBAL TEMPORARY TABLE gtt_ident (id int GENERATED ALWAYS AS IDENTITY, v text);
INSERT INTO gtt_ident (v) VALUES ('a'), ('b'), ('c');
TRUNCATE gtt_ident RESTART IDENTITY;
INSERT INTO gtt_ident (v) VALUES ('d');
SELECT id, v FROM gtt_ident;                      -- id restarts at 1
BEGIN;
TRUNCATE gtt_ident RESTART IDENTITY;
ROLLBACK;
INSERT INTO gtt_ident (v) VALUES ('e');
SELECT count(*), max(id) FROM gtt_ident;          -- row back, id continues
DROP TABLE gtt_ident;

--
-- A GTT sequence presents its one mandatory row to any session, even via a
-- direct scan that bypasses the sequence functions (as psql's \d does).
--
CREATE GLOBAL TEMPORARY SEQUENCE gtt_seq_scan START 42;
SELECT last_value, is_called FROM gtt_seq_scan;   -- creator's view
\c -
SELECT last_value, is_called FROM gtt_seq_scan;   -- fresh session: same seed
SELECT nextval('gtt_seq_scan');
DROP SEQUENCE gtt_seq_scan;

--
-- Relations without storage cannot be global temporary.
--
CREATE GLOBAL TEMPORARY VIEW gtt_view_bad AS SELECT 1;          -- error
CREATE OR REPLACE GLOBAL TEMPORARY VIEW gtt_view_bad AS SELECT 1;  -- error
CREATE GLOBAL TEMPORARY RECURSIVE VIEW gtt_view_bad (n) AS SELECT 1;  -- error

--
-- ON COMMIT DELETE ROWS reclaims TOAST storage along with the heap.
--
CREATE GLOBAL TEMPORARY TABLE gtt_toast_ocdr (id int, blob text)
  ON COMMIT DELETE ROWS;
-- pg_relation_size reads session-local GTT storage; keep it in the leader.
SET debug_parallel_query = off;
BEGIN;
INSERT INTO gtt_toast_ocdr
  SELECT g, string_agg(md5(g::text || i::text), '')
  FROM generate_series(1, 5) g, generate_series(1, 200) i GROUP BY g;
SELECT pg_relation_size(reltoastrelid) > 0 AS toast_used_in_xact
  FROM pg_class WHERE relname = 'gtt_toast_ocdr';
COMMIT;
SELECT pg_relation_size(reltoastrelid) AS toast_size_after_commit
  FROM pg_class WHERE relname = 'gtt_toast_ocdr';
RESET debug_parallel_query;
SELECT count(*) FROM gtt_toast_ocdr;
DROP TABLE gtt_toast_ocdr;

--
-- Materialized views must not capture session-private GTT data into a
-- permanent relation: rejected both for direct references (at creation)
-- and for references through a view (at population time).
--
CREATE GLOBAL TEMPORARY TABLE gtt_mv (x int);
INSERT INTO gtt_mv VALUES (1), (2), (3);
CREATE MATERIALIZED VIEW gtt_mv_direct AS SELECT x FROM gtt_mv;     -- error
CREATE MATERIALIZED VIEW gtt_mv_nodata AS SELECT x FROM gtt_mv
  WITH NO DATA;                                                     -- error
-- a plain view over a GTT stays permanent and is fine
CREATE VIEW gtt_mv_view AS SELECT x FROM gtt_mv;
SELECT relpersistence FROM pg_class WHERE relname = 'gtt_mv_view';
SELECT count(*) FROM gtt_mv_view;
-- ... but materializing through the view is caught at population time
CREATE MATERIALIZED VIEW gtt_mv_indirect AS SELECT x FROM gtt_mv_view;  -- error
DROP VIEW gtt_mv_view;
DROP TABLE gtt_mv;

--
-- Per-session statistics roll back with the transaction that wrote them.
--
CREATE GLOBAL TEMPORARY TABLE gtt_stats_abort (id int, cat text);
BEGIN;
INSERT INTO gtt_stats_abort SELECT g, 'c' || (g % 4) FROM generate_series(1, 10000) g;
ANALYZE gtt_stats_abort;
SELECT reltuples FROM pg_gtt_relstats('gtt_stats_abort'::regclass);
ROLLBACK;
SELECT count(*) FROM gtt_stats_abort;
SELECT count(*) FROM pg_gtt_relstats('gtt_stats_abort'::regclass);  -- 0: stats gone
SELECT count(*) FROM pg_gtt_colstats('gtt_stats_abort'::regclass);  -- 0: colstats too
-- subtransaction variant
INSERT INTO gtt_stats_abort SELECT g, 'x' FROM generate_series(1, 100) g;
BEGIN;
SAVEPOINT s1;
ANALYZE gtt_stats_abort;
ROLLBACK TO s1;
COMMIT;
SELECT count(*) FROM pg_gtt_relstats('gtt_stats_abort'::regclass);  -- 0
-- committed ANALYZE still sticks
ANALYZE gtt_stats_abort;
SELECT reltuples FROM pg_gtt_relstats('gtt_stats_abort'::regclass);
DROP TABLE gtt_stats_abort;

--
-- VACUUM of a GTT whose toast table has no session data stays quiet about
-- the toast relation (only the named relation is reported when skipped).
--
CREATE GLOBAL TEMPORARY TABLE gtt_vac_toast (id int PRIMARY KEY, pad text);
INSERT INTO gtt_vac_toast SELECT g, repeat('y', 100) FROM generate_series(1, 100) g;
VACUUM gtt_vac_toast;       -- no INFO about pg_toast_NNN
DROP TABLE gtt_vac_toast;

--
-- DISCARD TEMP / DISCARD ALL clear per-session GTT data (and reset GTT
-- sequences), transactionally.
--
CREATE GLOBAL TEMPORARY TABLE gtt_disc (x int);
CREATE GLOBAL TEMPORARY SEQUENCE gtt_disc_seq;
INSERT INTO gtt_disc VALUES (1), (2), (3);
SELECT nextval('gtt_disc_seq'), nextval('gtt_disc_seq');
DISCARD TEMP;
SELECT count(*) AS rows_after_discard FROM gtt_disc;
SELECT nextval('gtt_disc_seq') AS seq_after_discard;      -- restarts at 1
-- inside a transaction block, DISCARD TEMP rolls back cleanly
INSERT INTO gtt_disc VALUES (4), (5);
BEGIN;
DISCARD TEMP;
SELECT count(*) FROM gtt_disc;                            -- 0 inside
ROLLBACK;
SELECT count(*) AS rows_restored FROM gtt_disc;           -- 2 again
DROP SEQUENCE gtt_disc_seq;
DROP TABLE gtt_disc;

--
-- Lazy storage creation: a session that merely opens, plans, or reads a
-- GTT holds no per-session file (and so does not block peer DDL); files
-- appear at the first genuine data access.
--
CREATE GLOBAL TEMPORARY TABLE gtt_lazy (id int PRIMARY KEY, t text);
INSERT INTO gtt_lazy VALUES (1, 'one');
\c -
-- The pg_relation_filepath/pg_relation_size probes below read session-local
-- GTT storage that a parallel worker cannot see, so force leader execution.
-- Each \c - starts a fresh session and resets this, so it is re-applied below.
SET debug_parallel_query = off;
-- reads and planning do not materialize
SELECT count(*) FROM gtt_lazy;
EXPLAIN (COSTS OFF) SELECT * FROM gtt_lazy WHERE id = 1;
SELECT pg_relation_filepath('gtt_lazy') IS NULL AS heap_unmaterialized,
       pg_relation_filepath('gtt_lazy_pkey') IS NULL AS index_unmaterialized;
SELECT pg_relation_size('gtt_lazy') AS size_unmaterialized;
-- an index scan materializes (and builds) only the index, not the heap
SET enable_seqscan = off;
SELECT * FROM gtt_lazy WHERE id = 1;
RESET enable_seqscan;
SELECT pg_relation_filepath('gtt_lazy') IS NULL AS heap_still_unmaterialized,
       pg_relation_filepath('gtt_lazy_pkey') IS NOT NULL AS index_materialized;
-- maintenance on unmaterialized storage is a no-op
TRUNCATE gtt_lazy;
ANALYZE gtt_lazy;
SELECT count(*) FROM pg_gtt_relstats('gtt_lazy'::regclass);
SELECT pg_relation_filepath('gtt_lazy') IS NULL AS still_unmaterialized;
-- the first write materializes the heap and its indexes together
INSERT INTO gtt_lazy VALUES (2, 'two');
SELECT pg_relation_filepath('gtt_lazy') IS NOT NULL AS heap_materialized;
SET enable_seqscan = off;
SELECT t FROM gtt_lazy WHERE id = 2;
RESET enable_seqscan;
-- rollback of the materializing transaction discards the storage again
\c -
SET debug_parallel_query = off;		-- GTT storage probes must run in the leader
BEGIN;
INSERT INTO gtt_lazy VALUES (3, 'three');
SELECT pg_relation_filepath('gtt_lazy') IS NOT NULL AS materialized_in_xact;
ROLLBACK;
SELECT pg_relation_filepath('gtt_lazy') IS NULL AS dematerialized_after_abort;
SELECT count(*) FROM gtt_lazy;
DROP TABLE gtt_lazy;

--
-- Lazy creation round 2: indexes are exactly as lazy as their heap, and
-- a rollback that dematerializes the heap leaves no stale index content.
--
CREATE GLOBAL TEMPORARY TABLE gtt_lz2 (id int PRIMARY KEY);
-- bare CREATE materializes nothing (and so blocks no peer DDL)
SELECT pg_relation_filepath('gtt_lz2') IS NULL AS heap_unmat,
       pg_relation_filepath('gtt_lz2_pkey') IS NULL AS pkey_unmat;
-- write + rollback returns to fully unmaterialized; no phantom index entries
BEGIN;
INSERT INTO gtt_lz2 SELECT generate_series(1, 5);
ROLLBACK;
SELECT pg_relation_filepath('gtt_lz2') IS NULL AS heap_unmat_after_abort,
       pg_relation_filepath('gtt_lz2_pkey') IS NULL AS pkey_unmat_after_abort;
INSERT INTO gtt_lz2 VALUES (1);                    -- no phantom duplicate
SET enable_seqscan = off;
SELECT * FROM gtt_lz2 WHERE id = 1;                -- index scan is consistent
RESET enable_seqscan;
-- variant: index materialized by a scan in an earlier transaction, then a
-- write is rolled back; the abort pass empties the stale index
TRUNCATE gtt_lz2;
DROP TABLE gtt_lz2;
CREATE GLOBAL TEMPORARY TABLE gtt_lz3 (id int PRIMARY KEY);
SET enable_seqscan = off;
SELECT * FROM gtt_lz3 WHERE id = 9;                -- builds the (empty) index
RESET enable_seqscan;
SELECT pg_relation_filepath('gtt_lz3_pkey') IS NOT NULL AS pkey_mat_by_scan;
BEGIN;
INSERT INTO gtt_lz3 SELECT generate_series(1, 5);
ROLLBACK;
INSERT INTO gtt_lz3 VALUES (2);
SET enable_seqscan = off;
SELECT * FROM gtt_lz3 WHERE id = 2;
RESET enable_seqscan;
DROP TABLE gtt_lz3;

--
-- All index access methods behave on unmaterialized GTTs, including the
-- AMs whose planner support reads index pages (SPGiST's amcanreturn).
--
CREATE GLOBAL TEMPORARY TABLE gtt_ams (
    id int,
    arr int[],
    p point,
    rng int4range
);
CREATE INDEX gtt_ams_btree ON gtt_ams (id);
CREATE INDEX gtt_ams_hash ON gtt_ams USING hash (id);
CREATE INDEX gtt_ams_gin ON gtt_ams USING gin (arr);
CREATE INDEX gtt_ams_gist ON gtt_ams USING gist (p);
CREATE INDEX gtt_ams_spgist ON gtt_ams USING spgist (p);
CREATE INDEX gtt_ams_brin ON gtt_ams USING brin (id);
\c -
SET debug_parallel_query = off;		-- GTT storage probes must run in the leader
SELECT count(*) FROM gtt_ams;                      -- plans fine, reads nothing
INSERT INTO gtt_ams VALUES (1, ARRAY[1,2], point(1,1), int4range(1,10));
SET enable_seqscan = off;
SELECT id FROM gtt_ams WHERE id = 1;
SELECT id FROM gtt_ams WHERE arr @> ARRAY[2];
SELECT id FROM gtt_ams WHERE p <@ box '((0,0),(2,2))';
RESET enable_seqscan;
DROP TABLE gtt_ams;

--
-- DISCARD releases the storage and the DDL hold once it commits: after a
-- committed DISCARD the session is back to the unmaterialized state.
--
CREATE GLOBAL TEMPORARY TABLE gtt_dd2 (x int);
INSERT INTO gtt_dd2 VALUES (1), (2);
DISCARD TEMP;
SELECT pg_relation_filepath('gtt_dd2') IS NULL AS dematerialized;
SELECT count(*) FROM gtt_dd2;
-- a rolled-back DISCARD keeps both the data and the storage
INSERT INTO gtt_dd2 VALUES (3);
BEGIN;
DISCARD TEMP;
ROLLBACK;
SELECT count(*) AS rows_kept FROM gtt_dd2;
SELECT pg_relation_filepath('gtt_dd2') IS NOT NULL AS still_materialized;
-- writing after DISCARD in the same transaction keeps the new data
BEGIN;
DISCARD TEMP;
INSERT INTO gtt_dd2 VALUES (4);
COMMIT;
SELECT * FROM gtt_dd2;
SELECT pg_relation_filepath('gtt_dd2') IS NOT NULL AS kept_for_new_data;
DROP TABLE gtt_dd2;
RESET debug_parallel_query;

--
-- Index lifecycle corner cases found by randomized stress testing
--

-- 1. CREATE INDEX deferred (heap unmaterialized) in the same transaction
--    that later materializes the heap: the deferred build must still happen.
CREATE GLOBAL TEMPORARY TABLE gtt_ilc (id int PRIMARY KEY, v int);
BEGIN;
CREATE INDEX gtt_ilc_v_idx ON gtt_ilc (v);
TRUNCATE gtt_ilc;
INSERT INTO gtt_ilc VALUES (1, 1);
COMMIT;
SELECT * FROM gtt_ilc;
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SELECT * FROM gtt_ilc WHERE v = 1;
RESET enable_seqscan;
RESET enable_bitmapscan;
DROP TABLE gtt_ilc;

-- 2. CREATE INDEX built for real (heap materialized), then TRUNCATE swaps
--    it to a fresh empty file in the same transaction: it must be rebuilt
--    on next access, not assumed handled by index_create.
CREATE GLOBAL TEMPORARY TABLE gtt_ilc2 (id int PRIMARY KEY, v int);
INSERT INTO gtt_ilc2 VALUES (1, 1);
BEGIN;
CREATE INDEX gtt_ilc2_v_idx ON gtt_ilc2 (v);
TRUNCATE gtt_ilc2;
INSERT INTO gtt_ilc2 VALUES (2, 2);
COMMIT;
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SELECT * FROM gtt_ilc2 WHERE v = 2;
RESET enable_seqscan;
RESET enable_bitmapscan;
DROP TABLE gtt_ilc2;

-- 3. Planning a query when an index is materialized but emptied by the
--    abort pass (heap storage reverted): plan-time metapage readers must
--    treat it as unbuilt rather than read a zero-block file.
CREATE GLOBAL TEMPORARY TABLE gtt_ilc3 (id int PRIMARY KEY, v int);
BEGIN;
INSERT INTO gtt_ilc3 VALUES (1, 1);
TRUNCATE gtt_ilc3;
ROLLBACK;
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SELECT count(*) FROM gtt_ilc3 WHERE id BETWEEN 1 AND 10;  -- scan-builds pkey
RESET enable_seqscan;
RESET enable_bitmapscan;
BEGIN;
INSERT INTO gtt_ilc3 VALUES (2, 2);
SAVEPOINT s1;
TRUNCATE gtt_ilc3;
ROLLBACK;
SELECT count(*) FROM gtt_ilc3;  -- planner must survive the emptied pkey
INSERT INTO gtt_ilc3 VALUES (3, 3);
SELECT * FROM gtt_ilc3;
DROP TABLE gtt_ilc3;

-- 4. Nested aborts: a swap-undo from an outer subtransaction must not
--    resurrect index_built over files the same abort unlinked.
CREATE GLOBAL TEMPORARY TABLE gtt_ilc4 (id int PRIMARY KEY, v int);
CREATE INDEX gtt_ilc4_v_idx ON gtt_ilc4 (v);
BEGIN;
SAVEPOINT s1;
INSERT INTO gtt_ilc4 VALUES (1, 1);
ROLLBACK TO SAVEPOINT s1;
INSERT INTO gtt_ilc4 VALUES (2, 2);
TRUNCATE gtt_ilc4;
SAVEPOINT s2;
INSERT INTO gtt_ilc4 VALUES (3, 3);
ROLLBACK;
BEGIN;
SAVEPOINT s3;
INSERT INTO gtt_ilc4 VALUES (4, 4);
COMMIT;
SELECT * FROM gtt_ilc4;
DROP TABLE gtt_ilc4;

-- 5. DROP INDEX inside a rolled-back subtransaction must not retire the
--    session entry at commit (the index is still live); a committed
--    subtransaction drop must.
CREATE GLOBAL TEMPORARY TABLE gtt_ilc5 (id int PRIMARY KEY, v int);
INSERT INTO gtt_ilc5 VALUES (1, 1);
BEGIN;
CREATE INDEX gtt_ilc5_v_idx ON gtt_ilc5 (v);
SAVEPOINT s1;
DROP INDEX gtt_ilc5_v_idx;
ROLLBACK TO SAVEPOINT s1;
INSERT INTO gtt_ilc5 VALUES (2, 2);
COMMIT;
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SELECT * FROM gtt_ilc5 WHERE v = 2;
RESET enable_seqscan;
RESET enable_bitmapscan;
BEGIN;
SAVEPOINT s2;
DROP INDEX gtt_ilc5_v_idx;
RELEASE SAVEPOINT s2;
COMMIT;
INSERT INTO gtt_ilc5 VALUES (3, 3);
SELECT * FROM gtt_ilc5 ORDER BY id;
DROP TABLE gtt_ilc5;

-- 6. Sequence advancement is non-transactional and must survive the abort
--    of the transaction that first materialized the per-session sequence;
--    rolled-back nextval values are not handed out again.
CREATE GLOBAL TEMPORARY TABLE gtt_ilc6
  (id int GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, v int);
BEGIN;
INSERT INTO gtt_ilc6 (v) VALUES (1);
ROLLBACK;
INSERT INTO gtt_ilc6 (v) VALUES (2) RETURNING id;  -- id 2, not 1
-- TRUNCATE RESTART IDENTITY stays transactional
BEGIN;
TRUNCATE gtt_ilc6 RESTART IDENTITY;
ROLLBACK;
INSERT INTO gtt_ilc6 (v) VALUES (3) RETURNING id;  -- id 3: restart rolled back
TRUNCATE gtt_ilc6 RESTART IDENTITY;
INSERT INTO gtt_ilc6 (v) VALUES (4) RETURNING id;  -- id 1: restart committed
-- an aborted CREATE still cleans up its sequence file
BEGIN;
CREATE GLOBAL TEMPORARY TABLE gtt_ilc6b
  (id int GENERATED BY DEFAULT AS IDENTITY, v int);
INSERT INTO gtt_ilc6b (v) VALUES (1);
ROLLBACK;
CREATE GLOBAL TEMPORARY TABLE gtt_ilc6b
  (id int GENERATED BY DEFAULT AS IDENTITY, v int);
INSERT INTO gtt_ilc6b (v) VALUES (1) RETURNING id;  -- id 1, fresh file
DROP TABLE gtt_ilc6;
DROP TABLE gtt_ilc6b;

-- 7. Repeated TRUNCATE in one transaction: the second TRUNCATE must not
--    take the in-place path, which would touch the (possibly
--    unmaterialized) toast relation's file directly.
CREATE GLOBAL TEMPORARY TABLE gtt_ilc7 (id int PRIMARY KEY, t text);
INSERT INTO gtt_ilc7 VALUES (1, 'x');
BEGIN;
TRUNCATE gtt_ilc7;
TRUNCATE gtt_ilc7;
INSERT INTO gtt_ilc7 VALUES (2, 'y');
COMMIT;
SELECT * FROM gtt_ilc7;
-- and TRUNCATE of a same-transaction-created GTT (rd_createSubid path)
BEGIN;
CREATE GLOBAL TEMPORARY TABLE gtt_ilc7b (id int PRIMARY KEY, t text);
TRUNCATE gtt_ilc7b;
INSERT INTO gtt_ilc7b VALUES (1, 'x');
COMMIT;
SELECT * FROM gtt_ilc7b;
DROP TABLE gtt_ilc7;
DROP TABLE gtt_ilc7b;

-- 8. A same-transaction-created index whose storage is reverted by a
--    subtransaction abort must still be rebuilt when the heap
--    rematerializes later in the transaction.
CREATE GLOBAL TEMPORARY TABLE gtt_ilc8 (id int PRIMARY KEY, v int);
BEGIN;
CREATE INDEX gtt_ilc8_v ON gtt_ilc8 (v);
SAVEPOINT s1;
INSERT INTO gtt_ilc8 VALUES (1, 1);
ROLLBACK TO SAVEPOINT s1;
INSERT INTO gtt_ilc8 VALUES (2, 2);
COMMIT;
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SELECT * FROM gtt_ilc8 WHERE v = 2;
RESET enable_seqscan;
RESET enable_bitmapscan;
DROP TABLE gtt_ilc8;

--
-- Prepared statements with GTT
--
CREATE GLOBAL TEMPORARY TABLE gtt_prep (a int, b text);
INSERT INTO gtt_prep VALUES (1, 'one'), (2, 'two'), (3, 'three');
PREPARE gtt_prep_q AS SELECT * FROM gtt_prep WHERE a = $1;
EXECUTE gtt_prep_q(2);
EXECUTE gtt_prep_q(1);
DEALLOCATE gtt_prep_q;

-- PREPARE/EXECUTE for CTAS
PREPARE ctas_prep AS SELECT g, g * 2 AS doubled FROM generate_series(1, 3) g;
CREATE GLOBAL TEMP TABLE gtt_ctas_exec AS EXECUTE ctas_prep;
SELECT * FROM gtt_ctas_exec ORDER BY g;
DROP TABLE gtt_ctas_exec;
DEALLOCATE ctas_prep;

DROP TABLE gtt_prep;

--
-- Typed GTT (OF composite type)
--
CREATE TYPE gtt_composite AS (a int, b int);
CREATE GLOBAL TEMP TABLE gtt_typed OF gtt_composite;
INSERT INTO gtt_typed VALUES (1, 2);
SELECT * FROM gtt_typed;
DROP TABLE gtt_typed;
DROP TYPE gtt_composite;
