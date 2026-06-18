--
-- Columnstore scan path tests: point reads, column projection, COUNT(*) fast path
--

-- ===================================================================
-- Column projection on wide table
-- ===================================================================
CREATE TABLE cs_proj (
    c1 int, c2 int, c3 int, c4 int, c5 int,
    c6 int, c7 int, c8 int, c9 int, c10 int,
    c11 int, c12 int, c13 int, c14 int, c15 int,
    c16 int, c17 int, c18 int, c19 int, c20 int
) USING columnstore;
INSERT INTO cs_proj SELECT
    i, i+1, i+2, i+3, i+4, i+5, i+6, i+7, i+8, i+9,
    i+10, i+11, i+12, i+13, i+14, i+15, i+16, i+17, i+18, i+19
    FROM generate_series(1, 1000) i;
VACUUM cs_proj;

-- Access only first and last columns (should skip 18 in the middle)
SELECT c1, c20 FROM cs_proj WHERE c1 = 500;
-- Access only middle column
SELECT c10 FROM cs_proj WHERE c1 = 1;
-- Access all columns for one row
SELECT * FROM cs_proj WHERE c1 = 999;

DROP TABLE cs_proj;

-- ===================================================================
-- Point reads via index scan on FOR-encoded columns
-- ===================================================================
CREATE TABLE cs_point_for (id int, v int, padding text) USING columnstore;
INSERT INTO cs_point_for SELECT i, 1000 + (i % 100), 'pad_' || i
    FROM generate_series(1, 2000) i;
VACUUM cs_point_for;
CREATE INDEX ON cs_point_for (id);
ANALYZE cs_point_for;

SET enable_seqscan = off;
-- Single point read
SELECT id, v FROM cs_point_for WHERE id = 42;
SELECT id, v FROM cs_point_for WHERE id = 1999;
-- Range that stays within point-read threshold
SELECT count(*) FROM cs_point_for WHERE id BETWEEN 1 AND 50;
RESET enable_seqscan;
DROP TABLE cs_point_for;

-- ===================================================================
-- Point reads on NI64-encoded columns
-- ===================================================================
CREATE TABLE cs_point_ni64 (id int, v numeric(10,2)) USING columnstore;
INSERT INTO cs_point_ni64 SELECT i, (i * 1.23)::numeric(10,2)
    FROM generate_series(1, 2000) i;
VACUUM cs_point_ni64;
CREATE INDEX ON cs_point_ni64 (id);
ANALYZE cs_point_ni64;

SET enable_seqscan = off;
SELECT id, v FROM cs_point_ni64 WHERE id = 100;
SELECT id, v FROM cs_point_ni64 WHERE id = 1500;
-- Verify aggregate on index-scanned subset
SELECT sum(v) FROM cs_point_ni64 WHERE id BETWEEN 1 AND 10;
RESET enable_seqscan;
DROP TABLE cs_point_ni64;

-- ===================================================================
-- Sequential scan with filter on various encodings
-- ===================================================================
CREATE TABLE cs_scan_filter (
    id int,
    v_for int,      -- FOR-encoded
    v_ni64 numeric(10,2), -- NI64
    v_dict text,    -- dictionary
    v_plain text    -- plain
) USING columnstore;
INSERT INTO cs_scan_filter SELECT
    i,
    1000 + (i % 50),
    (i * 0.99)::numeric(10,2),
    'cat_' || (i % 20),
    'unique_' || i
    FROM generate_series(1, 2000) i;
VACUUM cs_scan_filter;

-- Filter on FOR column
SELECT count(*) FROM cs_scan_filter WHERE v_for = 1025;
-- Filter on NI64 column
SELECT count(*) FROM cs_scan_filter WHERE v_ni64 < 10.00;
-- Filter on dict column
SELECT count(*) FROM cs_scan_filter WHERE v_dict = 'cat_0';
-- Filter on plain column
SELECT id FROM cs_scan_filter WHERE v_plain = 'unique_1000';

DROP TABLE cs_scan_filter;

-- ===================================================================
-- COUNT(*) fast path correctness
-- ===================================================================

-- Pure columnar, no deletions
CREATE TABLE cs_cnt (id int) USING columnstore;
INSERT INTO cs_cnt SELECT i FROM generate_series(1, 2000) i;
VACUUM cs_cnt;
SELECT count(*) AS pure FROM cs_cnt;

-- After deletion
DELETE FROM cs_cnt WHERE id % 10 = 0;
SELECT count(*) AS after_del FROM cs_cnt;

-- Mixed delta + columnar
INSERT INTO cs_cnt SELECT i FROM generate_series(3000, 3100) i;
SELECT count(*) AS mixed FROM cs_cnt;

-- With filter (not pure COUNT(*) fast path)
SELECT count(*) AS filtered FROM cs_cnt WHERE id > 1000;

DROP TABLE cs_cnt;

-- ===================================================================
-- Scan with all-NULL column
-- ===================================================================
CREATE TABLE cs_scan_allnull (id int, v int) USING columnstore;
INSERT INTO cs_scan_allnull SELECT i, NULL FROM generate_series(1, 500) i;
VACUUM cs_scan_allnull;
SELECT count(*) FROM cs_scan_allnull;
SELECT count(v) FROM cs_scan_allnull;
SELECT count(*) FROM cs_scan_allnull WHERE v IS NULL;
SELECT count(*) FROM cs_scan_allnull WHERE v IS NOT NULL;
DROP TABLE cs_scan_allnull;

-- ===================================================================
-- Partitions get ColumnstoreScan paths and safe SCROLL cursors
--
-- Partition children are RELOPT_OTHER_MEMBER_REL; skipping them left
-- partitions on stock SeqScan paths, which lose the pushdowns and --
-- because ExecSupportsBackwardScan(SeqScan) is true -- let a SCROLL
-- cursor scan backward over an executor that cannot, returning wrong
-- rows.  With ColumnstoreScan the planner materializes for SCROLL.
-- ===================================================================
CREATE TABLE cs_part (a int, b text) PARTITION BY RANGE (a);
CREATE TABLE cs_part_1 PARTITION OF cs_part FOR VALUES FROM (1) TO (100)
    USING columnstore;
CREATE TABLE cs_part_2 PARTITION OF cs_part FOR VALUES FROM (100) TO (200)
    USING columnstore;
INSERT INTO cs_part SELECT i, 'p_' || i FROM generate_series(1, 199) i;
VACUUM cs_part_1;
VACUUM cs_part_2;
EXPLAIN (COSTS OFF) SELECT count(*) FROM cs_part WHERE a < 150;
SELECT count(*) FROM cs_part WHERE a < 150;
BEGIN;
DECLARE cs_scroll SCROLL CURSOR FOR SELECT a FROM cs_part ORDER BY a;
FETCH 5 FROM cs_scroll;
FETCH BACKWARD 3 FROM cs_scroll;
FETCH 4 FROM cs_scroll;
CLOSE cs_scroll;
COMMIT;
DROP TABLE cs_part;

-- ===================================================================
-- Rescans: TABLESAMPLE and parameterized bitmap inners under nestloop
--
-- Sample-scan and bitmap-scan per-page state must reset on rescan;
-- stale state once returned zero rows from the second TABLESAMPLE
-- execution and leftover offsets from the previous bitmap.
-- ===================================================================
CREATE TABLE cs_resc_outer (k int);
INSERT INTO cs_resc_outer VALUES (1), (2), (3);
CREATE TABLE cs_resc_inner (k int, v int) USING columnstore;
INSERT INTO cs_resc_inner SELECT i % 3 + 1, i FROM generate_series(1, 600) i;
VACUUM cs_resc_inner;
CREATE INDEX cs_resc_inner_v ON cs_resc_inner (v);
ANALYZE cs_resc_outer;
ANALYZE cs_resc_inner;
SET enable_hashjoin = off;
SET enable_mergejoin = off;
SET enable_memoize = off;
-- repeatable TABLESAMPLE rescanned once per outer row: identical
-- sample every time, so 3x the single-scan count
SELECT count(*) FROM cs_resc_outer o,
  LATERAL (SELECT v FROM cs_resc_inner TABLESAMPLE BERNOULLI (100) REPEATABLE (42)) s;
-- parameterized bitmap inner: each outer row sees its own matches
SET enable_seqscan = off;
SET enable_indexscan = off;
SET enable_bitmapscan = on;
SELECT o.k, (SELECT count(*) FROM cs_resc_inner i
             WHERE i.v BETWEEN o.k * 100 AND o.k * 100 + 50) AS hits
    FROM cs_resc_outer o ORDER BY o.k;
RESET enable_seqscan;
RESET enable_indexscan;
RESET enable_bitmapscan;
RESET enable_hashjoin;
RESET enable_mergejoin;
RESET enable_memoize;
DROP TABLE cs_resc_outer;
DROP TABLE cs_resc_inner;
