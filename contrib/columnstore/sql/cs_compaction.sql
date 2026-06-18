--
-- Columnstore row group compaction tests
--

-- ===================================================================
-- Basic compaction: heavy deletion triggers row group rewrite
-- ===================================================================
CREATE TABLE cs_compact (id int, val text, num numeric(10,2)) USING columnstore;
INSERT INTO cs_compact SELECT i, 'row_' || i, (i * 1.5)::numeric(10,2)
    FROM generate_series(1, 500) i;
VACUUM cs_compact;

-- Delete 80% of rows
DELETE FROM cs_compact WHERE id <= 400;
SELECT count(*) AS before_compact FROM cs_compact;

-- VACUUM with compaction disabled (default)
VACUUM cs_compact;
SELECT count(*) AS after_vacuum_no_compact FROM cs_compact;

-- Enable compaction and VACUUM
SET columnstore.rowgroup_compaction_threshold = 0.5;
VACUUM cs_compact;
SELECT count(*) AS after_compact FROM cs_compact;
RESET columnstore.rowgroup_compaction_threshold;

-- Verify data integrity
SELECT min(id), max(id) FROM cs_compact;
SELECT id, val, num FROM cs_compact WHERE id = 401;
SELECT id, val, num FROM cs_compact WHERE id = 500;

DROP TABLE cs_compact;

-- ===================================================================
-- Compaction with multiple row groups
-- ===================================================================
CREATE TABLE cs_compact_multi (id int, val text) USING columnstore;
-- Create 3 row groups
INSERT INTO cs_compact_multi SELECT i, 'rg1_' || i FROM generate_series(1, 300) i;
VACUUM cs_compact_multi;
INSERT INTO cs_compact_multi SELECT i, 'rg2_' || i FROM generate_series(301, 600) i;
VACUUM cs_compact_multi;
INSERT INTO cs_compact_multi SELECT i, 'rg3_' || i FROM generate_series(601, 900) i;
VACUUM cs_compact_multi;

-- Heavy deletion in rg1 and rg3, light in rg2
DELETE FROM cs_compact_multi WHERE id <= 250;          -- 83% of rg1
DELETE FROM cs_compact_multi WHERE id BETWEEN 601 AND 850; -- 83% of rg3
DELETE FROM cs_compact_multi WHERE id = 450;           -- 1 row from rg2

SELECT count(*) AS before_compact FROM cs_compact_multi;

SET columnstore.rowgroup_compaction_threshold = 0.5;
VACUUM cs_compact_multi;
RESET columnstore.rowgroup_compaction_threshold;

SELECT count(*) AS after_compact FROM cs_compact_multi;

-- Verify data from all original row groups survived
SELECT count(*) FROM cs_compact_multi WHERE id BETWEEN 251 AND 300;
SELECT count(*) FROM cs_compact_multi WHERE id BETWEEN 301 AND 600 AND id != 450;
SELECT count(*) FROM cs_compact_multi WHERE id BETWEEN 851 AND 900;

DROP TABLE cs_compact_multi;

-- ===================================================================
-- Compaction with indexes
-- ===================================================================
CREATE TABLE cs_compact_idx (id int, val text, num int) USING columnstore;
INSERT INTO cs_compact_idx SELECT i, 'row_' || i, i * 10
    FROM generate_series(1, 500) i;
VACUUM cs_compact_idx;

CREATE INDEX cs_compact_idx_id ON cs_compact_idx (id);
CREATE INDEX cs_compact_idx_num ON cs_compact_idx (num);

-- Verify indexes are valid
SELECT indexrelid::regclass, indisvalid
    FROM pg_index WHERE indrelid = 'cs_compact_idx'::regclass ORDER BY 1;

-- Delete enough to trigger compaction
DELETE FROM cs_compact_idx WHERE id <= 400;

SET columnstore.rowgroup_compaction_threshold = 0.5;
VACUUM cs_compact_idx;
RESET columnstore.rowgroup_compaction_threshold;

-- Indexes should still be valid
SELECT indexrelid::regclass, indisvalid
    FROM pg_index WHERE indrelid = 'cs_compact_idx'::regclass ORDER BY 1;

-- Sequential scan correctness
SELECT count(*) AS seq_count FROM cs_compact_idx;
SELECT id, val, num FROM cs_compact_idx WHERE id = 450 ORDER BY id;

-- Index scan correctness
SET enable_seqscan = off;
SELECT id, val, num FROM cs_compact_idx WHERE id = 450;
SELECT id, num FROM cs_compact_idx WHERE num = 5000;
RESET enable_seqscan;

DROP TABLE cs_compact_idx;

-- ===================================================================
-- Compaction preserves data types correctly
-- ===================================================================
CREATE TABLE cs_compact_types (
    id int,
    i2 smallint,
    i8 bigint,
    t text,
    n numeric(10,2),
    d date,
    b bool
) USING columnstore;
INSERT INTO cs_compact_types SELECT
    i, (i % 100)::smallint, i::bigint * 1000000,
    'text_' || i, (i * 0.5)::numeric(10,2),
    '2020-01-01'::date + i, (i % 2 = 0)
    FROM generate_series(1, 500) i;
VACUUM cs_compact_types;

-- Save expected results
CREATE TABLE cs_compact_expected AS
    SELECT * FROM cs_compact_types WHERE id > 400;

-- Delete most rows and compact
DELETE FROM cs_compact_types WHERE id <= 400;
SET columnstore.rowgroup_compaction_threshold = 0.5;
VACUUM cs_compact_types;
RESET columnstore.rowgroup_compaction_threshold;

-- Verify all types survived compaction
SELECT count(*) FROM (
    SELECT * FROM cs_compact_types
    EXCEPT
    SELECT * FROM cs_compact_expected
) diff;

DROP TABLE cs_compact_types;
DROP TABLE cs_compact_expected;

-- ===================================================================
-- Delta page reclamation: freed delta pages go onto the free list
-- so the relation doesn't grow indefinitely with insert/vacuum cycles
-- ===================================================================
CREATE TABLE cs_delta_reclaim (id int, val text) USING columnstore;

-- Cycle 1: insert and compact
INSERT INTO cs_delta_reclaim SELECT i, 'cycle1_' || i FROM generate_series(1, 500) i;
VACUUM cs_delta_reclaim;
SELECT pg_relation_size('cs_delta_reclaim') AS size_after_cycle1
    \gset

-- Cycle 2: insert more rows and compact again
INSERT INTO cs_delta_reclaim SELECT i, 'cycle2_' || i FROM generate_series(501, 1000) i;
VACUUM cs_delta_reclaim;
SELECT pg_relation_size('cs_delta_reclaim') AS size_after_cycle2
    \gset

-- Cycle 3: insert more rows and compact again
INSERT INTO cs_delta_reclaim SELECT i, 'cycle3_' || i FROM generate_series(1001, 1500) i;
VACUUM cs_delta_reclaim;

-- Relation size after cycle 3 should not be much larger than after cycle 2,
-- since the old delta pages should be reclaimed by the free list.
-- Without reclamation each cycle adds ~8 delta pages that are never freed.
SELECT pg_relation_size('cs_delta_reclaim') <=
       :'size_after_cycle2'::bigint * 2 AS delta_pages_reclaimed;

-- Data integrity: all 1500 rows should survive
SELECT count(*) AS total_rows FROM cs_delta_reclaim;

DROP TABLE cs_delta_reclaim;

-- ===================================================================
-- Bare DELETE (no WHERE) on a compacted columnstore table
-- ===================================================================
-- Regression: bare DELETE needs valid TIDs from the scan, which the
-- count-optimized path did not provide.
CREATE TABLE cs_bare_delete (id int, val text) USING columnstore;
INSERT INTO cs_bare_delete SELECT i, 'row_' || i FROM generate_series(1, 500) i;
VACUUM cs_bare_delete;

DELETE FROM cs_bare_delete;
SELECT count(*) AS after_bare_delete FROM cs_bare_delete;

-- Verify the table is still functional after bare delete + vacuum
INSERT INTO cs_bare_delete SELECT i, 'new_' || i FROM generate_series(1, 100) i;
SELECT count(*) AS after_reinsert FROM cs_bare_delete;

DROP TABLE cs_bare_delete;

-- ===================================================================
-- VACUUM updates pg_class.reltuples and relallvisible
-- ===================================================================
CREATE TABLE cs_relstats (id int, val text) USING columnstore;
INSERT INTO cs_relstats SELECT i, 'row_' || i FROM generate_series(1, 500) i;

-- Before VACUUM, reltuples may be 0 or stale
VACUUM cs_relstats;

-- After VACUUM (which compacts delta to columnar), reltuples should
-- reflect the actual row count
SELECT reltuples > 0 AS has_reltuples FROM pg_class
    WHERE oid = 'cs_relstats'::regclass;

-- relallvisible should be 0 (columnstore does not track page visibility)
SELECT relallvisible = 0 AS allvis_zero FROM pg_class
    WHERE oid = 'cs_relstats'::regclass;

-- Delete some rows and VACUUM again with compaction
DELETE FROM cs_relstats WHERE id <= 400;
SET columnstore.rowgroup_compaction_threshold = 0.5;
VACUUM cs_relstats;
RESET columnstore.rowgroup_compaction_threshold;

-- reltuples should reflect the reduced count (~100 rows)
SELECT reltuples BETWEEN 50 AND 200 AS reltuples_reduced FROM pg_class
    WHERE oid = 'cs_relstats'::regclass;

-- relallvisible should still be 0 after compaction
SELECT relallvisible = 0 AS allvis_zero FROM pg_class
    WHERE oid = 'cs_relstats'::regclass;

DROP TABLE cs_relstats;

-- ===================================================================
-- VACUUM corrects cs_total_rows after deletes
-- ===================================================================
-- cs_total_rows (used by relation_estimate_size) should track live rows,
-- not physical delta rows.  Tombstones must not inflate the counter.
CREATE TABLE cs_totalrows (id int) USING columnstore;
INSERT INTO cs_totalrows SELECT i FROM generate_series(1, 500) i;
VACUUM cs_totalrows;

-- Planner estimate should be close to 500 after VACUUM
EXPLAIN (FORMAT TEXT) SELECT * FROM cs_totalrows;

-- Delete half the rows (creates tombstones, no cs_total_rows change)
DELETE FROM cs_totalrows WHERE id <= 250;

-- Before VACUUM, estimate is still ~500 (tombstones don't inflate it)
EXPLAIN (FORMAT TEXT) SELECT * FROM cs_totalrows;

-- VACUUM corrects cs_total_rows to the live count
SET columnstore.rowgroup_compaction_threshold = 0.5;
VACUUM cs_totalrows;
RESET columnstore.rowgroup_compaction_threshold;

-- Estimate should now be ~250
EXPLAIN (FORMAT TEXT) SELECT * FROM cs_totalrows;

DROP TABLE cs_totalrows;

-- ===================================================================
-- Row group merging: multiple partially-deleted RGs merge into fewer
-- ===================================================================
CREATE TABLE cs_merge (id int, val text) USING columnstore;

-- Create 3 separate row groups (300 rows each)
INSERT INTO cs_merge SELECT i, 'rg1_' || i FROM generate_series(1, 300) i;
VACUUM cs_merge;
INSERT INTO cs_merge SELECT i, 'rg2_' || i FROM generate_series(301, 600) i;
VACUUM cs_merge;
INSERT INTO cs_merge SELECT i, 'rg3_' || i FROM generate_series(601, 900) i;
VACUUM cs_merge;

-- Delete 80% from each row group (60 survivors each = 180 total)
DELETE FROM cs_merge WHERE id <= 240;
DELETE FROM cs_merge WHERE id BETWEEN 301 AND 540;
DELETE FROM cs_merge WHERE id BETWEEN 601 AND 840;

SELECT count(*) AS before_merge FROM cs_merge;

-- Compact with merging: 3 small RGs should merge into 1
SET columnstore.rowgroup_compaction_threshold = 0.5;
VACUUM cs_merge;
RESET columnstore.rowgroup_compaction_threshold;

SELECT count(*) AS after_merge FROM cs_merge;

-- Verify data from all original row groups survived
SELECT count(*) FROM cs_merge WHERE id BETWEEN 241 AND 300;
SELECT count(*) FROM cs_merge WHERE id BETWEEN 541 AND 600;
SELECT count(*) FROM cs_merge WHERE id BETWEEN 841 AND 900;

-- Verify specific values are intact
SELECT id, val FROM cs_merge WHERE id = 241;
SELECT id, val FROM cs_merge WHERE id = 600;
SELECT id, val FROM cs_merge WHERE id = 900;

-- Insert new data after merge to verify relation is healthy
INSERT INTO cs_merge SELECT i, 'new_' || i FROM generate_series(901, 1000) i;
VACUUM cs_merge;
SELECT count(*) AS after_new_insert FROM cs_merge;

DROP TABLE cs_merge;

-- ===================================================================
-- Row group merging pulls in undersized RGs from previous compaction
-- ===================================================================
CREATE TABLE cs_merge_undersized (id int, val text) USING columnstore;

-- Create 3 row groups, each undersized (100 rows)
INSERT INTO cs_merge_undersized SELECT i, 'a_' || i FROM generate_series(1, 100) i;
VACUUM cs_merge_undersized;
INSERT INTO cs_merge_undersized SELECT i, 'b_' || i FROM generate_series(101, 200) i;
VACUUM cs_merge_undersized;
INSERT INTO cs_merge_undersized SELECT i, 'c_' || i FROM generate_series(201, 300) i;
VACUUM cs_merge_undersized;

-- Delete 80% from just one RG (triggers compaction threshold)
-- The other two undersized RGs should also be pulled into the merge
DELETE FROM cs_merge_undersized WHERE id <= 80;

SET columnstore.rowgroup_compaction_threshold = 0.5;
VACUUM cs_merge_undersized;
RESET columnstore.rowgroup_compaction_threshold;

-- All 220 surviving rows should be present
SELECT count(*) AS total FROM cs_merge_undersized;
SELECT min(id), max(id) FROM cs_merge_undersized;

DROP TABLE cs_merge_undersized;

-- ===================================================================
-- Row group merging with indexes
-- ===================================================================
CREATE TABLE cs_merge_idx (id int, val text) USING columnstore;
INSERT INTO cs_merge_idx SELECT i, 'rg1_' || i FROM generate_series(1, 200) i;
VACUUM cs_merge_idx;
INSERT INTO cs_merge_idx SELECT i, 'rg2_' || i FROM generate_series(201, 400) i;
VACUUM cs_merge_idx;

CREATE INDEX cs_merge_idx_id ON cs_merge_idx (id);

-- Delete heavily from both RGs
DELETE FROM cs_merge_idx WHERE id <= 150;
DELETE FROM cs_merge_idx WHERE id BETWEEN 201 AND 350;

SET columnstore.rowgroup_compaction_threshold = 0.5;
VACUUM cs_merge_idx;
RESET columnstore.rowgroup_compaction_threshold;

-- Indexes should be rebuilt and valid
SELECT indexrelid::regclass, indisvalid
    FROM pg_index WHERE indrelid = 'cs_merge_idx'::regclass ORDER BY 1;

-- Index scan should work correctly
SET enable_seqscan = off;
SELECT id, val FROM cs_merge_idx WHERE id = 175;
SELECT id, val FROM cs_merge_idx WHERE id = 400;
RESET enable_seqscan;

-- Sequential scan correctness
SELECT count(*) AS seq_count FROM cs_merge_idx;

DROP TABLE cs_merge_idx;

-- ===================================================================
-- Rolling window simulation: repeated delete + insert + VACUUM cycles
-- ===================================================================
CREATE TABLE cs_rolling (id int, val text) USING columnstore;

-- Load initial "30 days" of data (3 batches)
INSERT INTO cs_rolling SELECT i, 'day1_' || i FROM generate_series(1, 200) i;
VACUUM cs_rolling;
INSERT INTO cs_rolling SELECT i, 'day2_' || i FROM generate_series(201, 400) i;
VACUUM cs_rolling;
INSERT INTO cs_rolling SELECT i, 'day3_' || i FROM generate_series(401, 600) i;
VACUUM cs_rolling;

-- Simulate rolling window: delete oldest, insert newest, VACUUM
SET columnstore.rowgroup_compaction_threshold = 0.5;

-- Cycle 1: delete day1, add day4
DELETE FROM cs_rolling WHERE id <= 200;
INSERT INTO cs_rolling SELECT i, 'day4_' || i FROM generate_series(601, 800) i;
VACUUM cs_rolling;
SELECT count(*) AS after_cycle1 FROM cs_rolling;

-- Cycle 2: delete day2, add day5
DELETE FROM cs_rolling WHERE id BETWEEN 201 AND 400;
INSERT INTO cs_rolling SELECT i, 'day5_' || i FROM generate_series(801, 1000) i;
VACUUM cs_rolling;
SELECT count(*) AS after_cycle2 FROM cs_rolling;

-- Cycle 3: delete day3, add day6
DELETE FROM cs_rolling WHERE id BETWEEN 401 AND 600;
INSERT INTO cs_rolling SELECT i, 'day6_' || i FROM generate_series(1001, 1200) i;
VACUUM cs_rolling;
SELECT count(*) AS after_cycle3 FROM cs_rolling;

RESET columnstore.rowgroup_compaction_threshold;

-- Should have exactly 600 rows (3 days worth)
SELECT count(*) AS final_count FROM cs_rolling;
SELECT min(id), max(id) FROM cs_rolling;

-- Verify all surviving data is correct
SELECT count(*) FROM cs_rolling WHERE id BETWEEN 601 AND 800;
SELECT count(*) FROM cs_rolling WHERE id BETWEEN 801 AND 1000;
SELECT count(*) FROM cs_rolling WHERE id BETWEEN 1001 AND 1200;

DROP TABLE cs_rolling;

-- ===================================================================
-- VACUUM reports stats to pg_stat_user_tables
-- ===================================================================
CREATE TABLE cs_vacstats (id int, val text) USING columnstore;
INSERT INTO cs_vacstats SELECT i, 'row_' || i FROM generate_series(1, 500) i;
VACUUM cs_vacstats;

-- last_vacuum should be set and n_live_tup should reflect row count
SELECT last_vacuum IS NOT NULL AS has_last_vacuum,
       n_live_tup > 0 AS has_live_tup
    FROM pg_stat_user_tables WHERE relname = 'cs_vacstats';

-- Delete some rows and vacuum again
DELETE FROM cs_vacstats WHERE id <= 400;
SET columnstore.rowgroup_compaction_threshold = 0.5;
VACUUM cs_vacstats;
RESET columnstore.rowgroup_compaction_threshold;

-- n_live_tup should reflect the reduced count
SELECT n_live_tup BETWEEN 50 AND 200 AS live_tup_reduced
    FROM pg_stat_user_tables WHERE relname = 'cs_vacstats';

DROP TABLE cs_vacstats;

-- ===================================================================
-- Freezing and relfrozenxid
-- ===================================================================
CREATE PROCEDURE cs_burn_xids(n int) LANGUAGE plpgsql AS $$
BEGIN
    FOR i IN 1..n LOOP
        PERFORM txid_current();
        COMMIT;
    END LOOP;
END $$;
-- relfrozenxid must advance across VACUUM (finding: pinned-at-creation
-- relfrozenxid eventually stalls the whole cluster at the wraparound
-- limit).  Burn xids so the pin would show as a large age, then check
-- that VACUUM's freeze pass plus an honest report brings the age back
-- close to the current xid.
CREATE TABLE cs_frozen (id int, val text) USING columnstore;
INSERT INTO cs_frozen SELECT i, 'f' || i FROM generate_series(1, 100) i;
CALL cs_burn_xids(5000);
SELECT age(relfrozenxid) > 4000 AS needs_freezing
  FROM pg_class WHERE relname = 'cs_frozen';
VACUUM cs_frozen;
SELECT age(relfrozenxid) < 1000 AS frozen_advanced
  FROM pg_class WHERE relname = 'cs_frozen';
-- rows survive freezing, in both storage forms
SELECT count(*), min(id), max(id) FROM cs_frozen;
INSERT INTO cs_frozen SELECT i, 'g' || i FROM generate_series(101, 150) i;
CALL cs_burn_xids(5000);
VACUUM cs_frozen;
SELECT age(relfrozenxid) < 1000 AS frozen_advanced_again
  FROM pg_class WHERE relname = 'cs_frozen';
SELECT count(*), min(id), max(id) FROM cs_frozen;
DROP TABLE cs_frozen;
DROP PROCEDURE cs_burn_xids(int);

-- compaction over delta pages too full to take even a minimal tuple:
-- ~2KB rows leave page remainders below the fence-worthiness threshold,
-- so the consumable prefix is processed without fencing those pages
CREATE TABLE cs_full_pages (id int, pad text) USING columnstore;
ALTER TABLE cs_full_pages ALTER COLUMN pad SET STORAGE PLAIN;
INSERT INTO cs_full_pages
    SELECT g, rpad(md5(g::text), 1996, 'x') FROM generate_series(1, 400) g;
VACUUM cs_full_pages;
SELECT count(*), min(id), max(id), max(length(pad)) FROM cs_full_pages;
-- the table keeps working for inserts and re-compaction afterwards
INSERT INTO cs_full_pages
    SELECT g, rpad(md5(g::text), 1996, 'x') FROM generate_series(401, 500) g;
VACUUM cs_full_pages;
SELECT count(*), max(id) FROM cs_full_pages;
DROP TABLE cs_full_pages;

-- Dropped columns: compaction must skip attisdropped attributes (their
-- atttypid is 0, so any type/opclass lookup would fail) and keep
-- compacting; subsequent DDL still works
CREATE TABLE cs_dropcol (a int, b text, c numeric(8,2)) USING columnstore;
INSERT INTO cs_dropcol SELECT g, 't' || g, g * 1.5 FROM generate_series(1, 500) g;
ALTER TABLE cs_dropcol DROP COLUMN b;
VACUUM cs_dropcol;
SELECT count(*), sum(a), sum(c) FROM cs_dropcol;
INSERT INTO cs_dropcol SELECT g, g * 2.5 FROM generate_series(501, 600) g;
VACUUM cs_dropcol;
ALTER TABLE cs_dropcol DROP COLUMN c;
VACUUM cs_dropcol;
SELECT count(*), sum(a) FROM cs_dropcol;
ALTER TABLE cs_dropcol ADD COLUMN d int DEFAULT 7;
SELECT count(*) FILTER (WHERE d = 7) AS defaults FROM cs_dropcol;
DROP TABLE cs_dropcol;

-- Zero-column rows through compaction
CREATE TABLE cs_zerocol_vac () USING columnstore;
INSERT INTO cs_zerocol_vac SELECT FROM generate_series(1, 200);
VACUUM cs_zerocol_vac;
SELECT count(*) FROM cs_zerocol_vac;
DELETE FROM cs_zerocol_vac;
VACUUM cs_zerocol_vac;
SELECT count(*) FROM cs_zerocol_vac;
DROP TABLE cs_zerocol_vac;

-- Subtransaction xids in delta tuples: released and rolled-back
-- savepoints (inserts and deletes) must classify correctly through
-- compaction
CREATE TABLE cs_subxact (v int) USING columnstore;
INSERT INTO cs_subxact SELECT g FROM generate_series(1, 500) g;
VACUUM cs_subxact;
BEGIN;
SAVEPOINT s1;
DELETE FROM cs_subxact WHERE v <= 100;
RELEASE s1;
SAVEPOINT s2;
DELETE FROM cs_subxact WHERE v > 400;
ROLLBACK TO s2;
INSERT INTO cs_subxact VALUES (1001);
SAVEPOINT s3;
INSERT INTO cs_subxact VALUES (1002);
ROLLBACK TO s3;
COMMIT;
SELECT count(*), min(v), max(v) FROM cs_subxact;
VACUUM cs_subxact;
SELECT count(*), min(v), max(v) FROM cs_subxact;
DROP TABLE cs_subxact;

-- A row group whose rows are all deleted, then compaction again, then
-- reuse of the table
CREATE TABLE cs_alldel (v int) USING columnstore;
INSERT INTO cs_alldel SELECT g FROM generate_series(1, 1000) g;
VACUUM cs_alldel;
DELETE FROM cs_alldel;
SELECT count(*) FROM cs_alldel;
VACUUM cs_alldel;
SELECT count(*) FROM cs_alldel;
INSERT INTO cs_alldel VALUES (42);
VACUUM cs_alldel;
SELECT * FROM cs_alldel;
DROP TABLE cs_alldel;

-- Wide table: the row group descriptor (per-column chunk descriptors + zone
-- maps) exceeds one catalog page beyond ~68 columns, so it is stored spanning
-- several consecutive pages.  DELETE + VACUUM materializes the tombstones into
-- the deletion bitmap and rewrites that multi-page descriptor; the rewrite must
-- update every page, not just the first.  Read columns from across the entry
-- (first, middle, last) after the rewrite to confirm it stayed intact.
SELECT format('CREATE TABLE cs_wide (%s) USING columnstore',
              string_agg(format('c%s int', g), ', '))
  FROM generate_series(1, 100) g \gexec
SELECT format('INSERT INTO cs_wide SELECT %s FROM generate_series(1, 2000) g',
              string_agg('g', ', '))
  FROM generate_series(1, 100) g \gexec
VACUUM cs_wide;
SELECT count(*), sum(c1), sum(c50), sum(c100) FROM cs_wide;
DELETE FROM cs_wide WHERE c1 % 7 = 0;
VACUUM cs_wide;
SELECT count(*), sum(c1), sum(c50), sum(c100) FROM cs_wide;
SELECT c1, c50, c100 FROM cs_wide WHERE c1 = 100;
DROP TABLE cs_wide;
