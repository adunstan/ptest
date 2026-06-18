--
-- Tests for the columnstore table access method
--

CREATE EXTENSION columnstore;

-- Basic table creation and insertion
CREATE TABLE cs_basic (a int, b text) USING columnstore;

-- Verify columnstore-optimized autovacuum defaults are set automatically
SELECT unnest(reloptions) FROM pg_class WHERE relname = 'cs_basic' ORDER BY 1;

INSERT INTO cs_basic SELECT i, 'row-' || i FROM generate_series(1, 20) i;
SELECT count(*) AS delta_count FROM cs_basic;
SELECT a, b FROM cs_basic WHERE a <= 5 ORDER BY a;

-- Delta-to-columnar freeze via VACUUM
VACUUM cs_basic;
SELECT count(*) AS columnar_count FROM cs_basic;
SELECT a, b FROM cs_basic WHERE a <= 5 ORDER BY a;
SELECT a, b FROM cs_basic WHERE a = 20;

-- Mixed delta + columnar scan
INSERT INTO cs_basic SELECT i, 'new-' || i FROM generate_series(100, 105) i;
SELECT count(*) AS mixed_count FROM cs_basic;
SELECT a, b FROM cs_basic WHERE a >= 100 ORDER BY a;

-- Multiple row groups
DROP TABLE cs_basic;
CREATE TABLE cs_basic (a int, b text) USING columnstore;
INSERT INTO cs_basic SELECT i, 'batch1-' || i FROM generate_series(1, 100) i;
VACUUM cs_basic;
INSERT INTO cs_basic SELECT i, 'batch2-' || i FROM generate_series(200, 250) i;
VACUUM cs_basic;
SELECT count(*) AS multi_rg_count FROM cs_basic;
SELECT a, b FROM cs_basic WHERE a = 1;
SELECT a, b FROM cs_basic WHERE a = 200;

-- Zone map filtering (row group 1: a=1..100, row group 2: a=200..250)
-- All btree strategies: <, <=, =, >=, >
SELECT count(*) AS zm_lt FROM cs_basic WHERE a < 50;
SELECT count(*) AS zm_le FROM cs_basic WHERE a <= 100;
SELECT count(*) AS zm_eq FROM cs_basic WHERE a = 200;
SELECT count(*) AS zm_ge FROM cs_basic WHERE a >= 200;
SELECT count(*) AS zm_gt FROM cs_basic WHERE a > 250;
-- Combined filter
SELECT count(*) AS zm_range FROM cs_basic WHERE a >= 50 AND a <= 100;
SELECT count(*) AS zone_all FROM cs_basic;

-- Zone maps with different by-value types
CREATE TABLE cs_zm_types (
    i int, f float4, d date, b bool
) USING columnstore;
INSERT INTO cs_zm_types
    SELECT i, i::float4, '2020-01-01'::date + i, (i % 2 = 0)
    FROM generate_series(1, 100) i;
VACUUM cs_zm_types;
INSERT INTO cs_zm_types
    SELECT i, i::float4, '2020-01-01'::date + i, (i % 2 = 0)
    FROM generate_series(200, 300) i;
VACUUM cs_zm_types;
-- These should benefit from zone map pruning
SELECT count(*) AS zm_int FROM cs_zm_types WHERE i > 150;
SELECT count(*) AS zm_float FROM cs_zm_types WHERE f < 50.0::float4;
SELECT count(*) AS zm_date FROM cs_zm_types WHERE d > '2020-07-20'::date;
-- NULLs in zone maps
CREATE TABLE cs_zm_nulls (a int, b int) USING columnstore;
INSERT INTO cs_zm_nulls SELECT i, CASE WHEN i <= 50 THEN NULL ELSE i END
    FROM generate_series(1, 100) i;
VACUUM cs_zm_nulls;
-- b has NULLs but zone map should still track non-NULL range (51..100)
SELECT count(*) AS zm_null_eq FROM cs_zm_nulls WHERE b = 75;
SELECT count(*) AS zm_null_gt FROM cs_zm_nulls WHERE b > 100;
DROP TABLE cs_zm_types;
DROP TABLE cs_zm_nulls;
-- Zone maps on by-reference types (text, numeric)
CREATE TABLE cs_zm_text (id int, name text, amount numeric) USING columnstore;
INSERT INTO cs_zm_text SELECT i, 'aaa-' || i, i * 1.5
    FROM generate_series(1, 100) i;
VACUUM cs_zm_text;
INSERT INTO cs_zm_text SELECT i, 'zzz-' || i, i * 1.5
    FROM generate_series(101, 200) i;
VACUUM cs_zm_text;
-- Text zone map: should skip RG2
SELECT count(*) AS zm_text_eq FROM cs_zm_text WHERE name = 'aaa-50';
-- Text zone map: should skip RG1
SELECT count(*) AS zm_text_gt FROM cs_zm_text WHERE name > 'yyy';
-- Numeric zone map: should skip RG2
SELECT count(*) AS zm_num_lt FROM cs_zm_text WHERE amount < 10;
-- Numeric zone map: should skip RG1
SELECT count(*) AS zm_num_gt FROM cs_zm_text WHERE amount > 200;
-- Long text (>32 bytes): prefix zone maps (C-locale byte truncation)
CREATE TABLE cs_zm_long (id int, val text COLLATE "C") USING columnstore;
INSERT INTO cs_zm_long SELECT i, 'aaa' || repeat('x', 40) || i
    FROM generate_series(1, 100) i;
VACUUM cs_zm_long;
INSERT INTO cs_zm_long SELECT i, 'zzz' || repeat('x', 40) || i
    FROM generate_series(101, 200) i;
VACUUM cs_zm_long;
-- Should skip RG2 (prefix min='zzz...' > 'bbb')
SELECT count(*) AS zm_prefix_lt FROM cs_zm_long WHERE val < 'bbb';
-- Should skip RG1 (prefix max='aaa...' < 'yyy')
SELECT count(*) AS zm_prefix_gt FROM cs_zm_long WHERE val > 'yyy';
-- Should match both row groups
SELECT count(*) AS zm_prefix_all FROM cs_zm_long WHERE val > 'aaa';
DROP TABLE cs_zm_text;
DROP TABLE cs_zm_long;

-- Compression
DROP TABLE cs_basic;
CREATE TABLE cs_basic (a int, b text) USING columnstore;
INSERT INTO cs_basic SELECT i, repeat('hello world ', 10) FROM generate_series(1, 1000) i;
VACUUM cs_basic;
SELECT count(*) AS compressed_count FROM cs_basic;
SELECT a, length(b) AS blen FROM cs_basic WHERE a = 500;
SELECT left(b, 11) AS b_prefix FROM cs_basic WHERE a = 42;

-- Column pruning: wide table
CREATE TABLE cs_wide (c1 int, c2 int, c3 int, c4 int, c5 int,
                      c6 int, c7 int, c8 int, c9 int, c10 int)
    USING columnstore;
INSERT INTO cs_wide SELECT i, i*2, i*3, i*4, i*5,
                            i*6, i*7, i*8, i*9, i*10
    FROM generate_series(1, 200) i;
VACUUM cs_wide;
-- Only access 2 out of 10 columns
SELECT c1, c10 FROM cs_wide WHERE c1 = 100;
SELECT count(*) FROM cs_wide WHERE c5 > 500;

-- DELETE from delta store
DROP TABLE cs_basic;
CREATE TABLE cs_basic (a int, b text) USING columnstore;
INSERT INTO cs_basic SELECT i, 'del-' || i FROM generate_series(1, 10) i;
DELETE FROM cs_basic WHERE a = 5;
SELECT count(*) AS after_delta_delete FROM cs_basic;
SELECT a FROM cs_basic ORDER BY a;

-- DELETE from columnar store
VACUUM cs_basic;
DELETE FROM cs_basic WHERE a = 3;
SELECT count(*) AS after_col_delete FROM cs_basic;
SELECT a FROM cs_basic ORDER BY a;

-- DELETE multiple columnar rows
DELETE FROM cs_basic WHERE a <= 2;
SELECT count(*) AS after_multi_delete FROM cs_basic;
SELECT a FROM cs_basic ORDER BY a;

-- UPDATE in delta store
DROP TABLE cs_basic;
CREATE TABLE cs_basic (a int, b text) USING columnstore;
INSERT INTO cs_basic SELECT i, 'orig-' || i FROM generate_series(1, 5) i;
UPDATE cs_basic SET b = 'updated' WHERE a = 3;
SELECT a, b FROM cs_basic WHERE a = 3;
SELECT count(*) AS after_delta_update FROM cs_basic;

-- UPDATE on columnar data
VACUUM cs_basic;
UPDATE cs_basic SET b = 'modified' WHERE a = 2;
SELECT a, b FROM cs_basic WHERE a = 2;
SELECT count(*) AS after_col_update FROM cs_basic;
SELECT a, b FROM cs_basic ORDER BY a;

-- Mixed operations: insert, freeze, delete, insert, update
DROP TABLE cs_basic;
CREATE TABLE cs_basic (a int, b text) USING columnstore;
INSERT INTO cs_basic SELECT i, 'v1-' || i FROM generate_series(1, 50) i;
VACUUM cs_basic;
DELETE FROM cs_basic WHERE a % 10 = 0;
INSERT INTO cs_basic SELECT i, 'v2-' || i FROM generate_series(100, 110) i;
UPDATE cs_basic SET b = 'changed' WHERE a = 25;
SELECT count(*) AS final_count FROM cs_basic;
SELECT a, b FROM cs_basic WHERE a = 25;
SELECT a, b FROM cs_basic WHERE a = 105;

-- INSERT...SELECT from columnstore source
DROP TABLE cs_basic;
CREATE TABLE cs_basic (a int NOT NULL, b text NOT NULL) USING columnstore;
INSERT INTO cs_basic SELECT i, 'is-' || i FROM generate_series(1, 10) i;
VACUUM cs_basic;

-- Self-insert from frozen columnar data
INSERT INTO cs_basic SELECT * FROM cs_basic;
SELECT count(*) AS self_insert_count FROM cs_basic;
SELECT a, b FROM cs_basic ORDER BY a, b;

-- Insert into a different columnstore table
CREATE TABLE cs_dest (a int, b text) USING columnstore;
INSERT INTO cs_dest SELECT * FROM cs_basic WHERE a <= 5;
SELECT count(*) AS dest_count FROM cs_dest;
SELECT a, b FROM cs_dest ORDER BY a, b;

-- Insert into a heap table from columnstore source
CREATE TABLE cs_heap_dest (a int, b text);
INSERT INTO cs_heap_dest SELECT * FROM cs_basic WHERE a <= 3;
SELECT count(*) AS heap_dest_count FROM cs_heap_dest;
SELECT a, b FROM cs_heap_dest ORDER BY a, b;

-- Insert from mixed delta + columnar
INSERT INTO cs_basic SELECT i, 'delta-' || i FROM generate_series(20, 22) i;
CREATE TABLE cs_dest2 (a int, b text) USING columnstore;
INSERT INTO cs_dest2 SELECT * FROM cs_basic WHERE a >= 9;
SELECT a, b FROM cs_dest2 ORDER BY a, b;

DROP TABLE cs_dest;
DROP TABLE cs_dest2;
DROP TABLE cs_heap_dest;

-- Multi-column types
CREATE TABLE cs_types (
    i int,
    b bigint,
    s smallint,
    t text,
    f float4
) USING columnstore;
INSERT INTO cs_types SELECT i, i * 1000000000::bigint, (i % 100)::smallint,
                            'text-' || i, i * 1.5
    FROM generate_series(1, 100) i;
VACUUM cs_types;
SELECT i, b, s, t, f FROM cs_types WHERE i = 50;
SELECT count(*) FROM cs_types WHERE s < 10;
SELECT count(*) FROM cs_types WHERE b > 50000000000;

-- Very wide table (tests multi-page row group catalog)
CREATE TABLE cs_wide200 (
    c1 int, c2 int, c3 int, c4 int, c5 int, c6 int, c7 int, c8 int, c9 int, c10 int,
    c11 int, c12 int, c13 int, c14 int, c15 int, c16 int, c17 int, c18 int, c19 int, c20 int,
    c21 int, c22 int, c23 int, c24 int, c25 int, c26 int, c27 int, c28 int, c29 int, c30 int,
    c31 int, c32 int, c33 int, c34 int, c35 int, c36 int, c37 int, c38 int, c39 int, c40 int,
    c41 int, c42 int, c43 int, c44 int, c45 int, c46 int, c47 int, c48 int, c49 int, c50 int,
    c51 int, c52 int, c53 int, c54 int, c55 int, c56 int, c57 int, c58 int, c59 int, c60 int,
    c61 int, c62 int, c63 int, c64 int, c65 int, c66 int, c67 int, c68 int, c69 int, c70 int,
    c71 int, c72 int, c73 int, c74 int, c75 int, c76 int, c77 int, c78 int, c79 int, c80 int,
    c81 int, c82 int, c83 int, c84 int, c85 int, c86 int, c87 int, c88 int, c89 int, c90 int,
    c91 int, c92 int, c93 int, c94 int, c95 int, c96 int, c97 int, c98 int, c99 int, c100 int,
    c101 int, c102 int, c103 int, c104 int, c105 int, c106 int, c107 int, c108 int, c109 int, c110 int,
    c111 int, c112 int, c113 int, c114 int, c115 int, c116 int, c117 int, c118 int, c119 int, c120 int,
    c121 int, c122 int, c123 int, c124 int, c125 int, c126 int, c127 int, c128 int, c129 int, c130 int,
    c131 int, c132 int, c133 int, c134 int, c135 int, c136 int, c137 int, c138 int, c139 int, c140 int,
    c141 int, c142 int, c143 int, c144 int, c145 int, c146 int, c147 int, c148 int, c149 int, c150 int,
    c151 int, c152 int, c153 int, c154 int, c155 int, c156 int, c157 int, c158 int, c159 int, c160 int,
    c161 int, c162 int, c163 int, c164 int, c165 int, c166 int, c167 int, c168 int, c169 int, c170 int,
    c171 int, c172 int, c173 int, c174 int, c175 int, c176 int, c177 int, c178 int, c179 int, c180 int,
    c181 int, c182 int, c183 int, c184 int, c185 int, c186 int, c187 int, c188 int, c189 int, c190 int,
    c191 int, c192 int, c193 int, c194 int, c195 int, c196 int, c197 int, c198 int, c199 int, c200 int
) USING columnstore;
INSERT INTO cs_wide200 SELECT
    i, i+1, i+2, i+3, i+4, i+5, i+6, i+7, i+8, i+9,
    i+10, i+11, i+12, i+13, i+14, i+15, i+16, i+17, i+18, i+19,
    i+20, i+21, i+22, i+23, i+24, i+25, i+26, i+27, i+28, i+29,
    i+30, i+31, i+32, i+33, i+34, i+35, i+36, i+37, i+38, i+39,
    i+40, i+41, i+42, i+43, i+44, i+45, i+46, i+47, i+48, i+49,
    i+50, i+51, i+52, i+53, i+54, i+55, i+56, i+57, i+58, i+59,
    i+60, i+61, i+62, i+63, i+64, i+65, i+66, i+67, i+68, i+69,
    i+70, i+71, i+72, i+73, i+74, i+75, i+76, i+77, i+78, i+79,
    i+80, i+81, i+82, i+83, i+84, i+85, i+86, i+87, i+88, i+89,
    i+90, i+91, i+92, i+93, i+94, i+95, i+96, i+97, i+98, i+99,
    i+100, i+101, i+102, i+103, i+104, i+105, i+106, i+107, i+108, i+109,
    i+110, i+111, i+112, i+113, i+114, i+115, i+116, i+117, i+118, i+119,
    i+120, i+121, i+122, i+123, i+124, i+125, i+126, i+127, i+128, i+129,
    i+130, i+131, i+132, i+133, i+134, i+135, i+136, i+137, i+138, i+139,
    i+140, i+141, i+142, i+143, i+144, i+145, i+146, i+147, i+148, i+149,
    i+150, i+151, i+152, i+153, i+154, i+155, i+156, i+157, i+158, i+159,
    i+160, i+161, i+162, i+163, i+164, i+165, i+166, i+167, i+168, i+169,
    i+170, i+171, i+172, i+173, i+174, i+175, i+176, i+177, i+178, i+179,
    i+180, i+181, i+182, i+183, i+184, i+185, i+186, i+187, i+188, i+189,
    i+190, i+191, i+192, i+193, i+194, i+195, i+196, i+197, i+198, i+199
    FROM generate_series(1, 100) i;
-- Freeze into columnar (catalog spans multiple pages for 200 columns)
VACUUM cs_wide200;
-- Verify data integrity
SELECT count(*) AS wide_count FROM cs_wide200;
SELECT c1, c100, c200 FROM cs_wide200 WHERE c1 = 50;
-- Column pruning on wide table: access only 2 of 200 columns
SELECT sum(c1), sum(c200) FROM cs_wide200;
-- Access all 200 columns
SELECT c1 + c50 + c100 + c150 + c200 AS sparse_sum FROM cs_wide200 WHERE c1 = 1;
-- Filtered count on a middle column (tests projection with count + qual)
SELECT count(*) AS filtered_count FROM cs_wide200 WHERE c100 = 149;
-- DELETE from wide columnar table
DELETE FROM cs_wide200 WHERE c1 <= 5;
SELECT count(*) AS wide_after_delete FROM cs_wide200;
SELECT c1, c200 FROM cs_wide200 WHERE c1 = 10;

-- Wide-table zone map across the page-1 boundary.  When a table has
-- enough columns that the row group catalog entry exceeds
-- CS_COLDATA_PER_PAGE, late columns' zone maps spill onto a second
-- catalog page; exercise the read path with a high-attno bigint
-- column whose value range spans negative-and-positive int64 (so the
-- zone map stores non-trivial bounds), and verify that the pushed-
-- down equality and the no-pushdown form return the same count.
CREATE TABLE cs_wide_zm (
    c1 int, c2 int, c3 int, c4 int, c5 int, c6 int, c7 int, c8 int, c9 int, c10 int,
    c11 int, c12 int, c13 int, c14 int, c15 int, c16 int, c17 int, c18 int, c19 int, c20 int,
    c21 int, c22 int, c23 int, c24 int, c25 int, c26 int, c27 int, c28 int, c29 int, c30 int,
    c31 int, c32 int, c33 int, c34 int, c35 int, c36 int, c37 int, c38 int, c39 int, c40 int,
    c41 int, c42 int, c43 int, c44 int, c45 int, c46 int, c47 int, c48 int, c49 int, c50 int,
    c51 int, c52 int, c53 int, c54 int, c55 int, c56 int, c57 int, c58 int, c59 int, c60 int,
    c61 int, c62 int, c63 int, c64 int, c65 int, c66 int, c67 int, c68 int, c69 int, c70 int,
    c71 int, c72 int, c73 int, c74 int, c75 int, c76 int, c77 int, c78 int, c79 int, c80 int,
    h bigint
) USING columnstore;
-- Mix of negative and positive bigint values, with one repeated target.
INSERT INTO cs_wide_zm
SELECT i, i+1, i+2, i+3, i+4, i+5, i+6, i+7, i+8, i+9,
       i+10, i+11, i+12, i+13, i+14, i+15, i+16, i+17, i+18, i+19,
       i+20, i+21, i+22, i+23, i+24, i+25, i+26, i+27, i+28, i+29,
       i+30, i+31, i+32, i+33, i+34, i+35, i+36, i+37, i+38, i+39,
       i+40, i+41, i+42, i+43, i+44, i+45, i+46, i+47, i+48, i+49,
       i+50, i+51, i+52, i+53, i+54, i+55, i+56, i+57, i+58, i+59,
       i+60, i+61, i+62, i+63, i+64, i+65, i+66, i+67, i+68, i+69,
       i+70, i+71, i+72, i+73, i+74, i+75, i+76, i+77, i+78, i+79,
       CASE WHEN i % 7 = 0 THEN  2868770270353813622::bigint
            WHEN i % 5 = 0 THEN -7512823426727949328::bigint
            ELSE (i::bigint * 9876543210987 - 4611686018427387904) END
FROM generate_series(1, 1000) i;
VACUUM cs_wide_zm;
-- Pushdown vs no-pushdown for the late bigint column must agree.
SELECT count(*) AS zm_pos_eq, sum(CASE WHEN h = 2868770270353813622 THEN 1 END) AS zm_pos_eq2
FROM cs_wide_zm WHERE h + 0 = 2868770270353813622;
SELECT count(*) AS zm_pos_eq_pushdown FROM cs_wide_zm WHERE h = 2868770270353813622;
SELECT count(*) AS zm_neg_eq_pushdown FROM cs_wide_zm WHERE h = -7512823426727949328;
SELECT count(*) AS zm_neg_eq_nopd     FROM cs_wide_zm WHERE h + 0 = -7512823426727949328;
DROP TABLE cs_wide_zm;

-- Parallel worker limiting: few row groups should suppress parallel scan
DROP TABLE cs_basic;
CREATE TABLE cs_basic (a int, b text) USING columnstore;
-- 1 row group only → 1 work unit < 2, no parallel
INSERT INTO cs_basic SELECT i, 'rg1-' || i FROM generate_series(1, 100) i;
VACUUM cs_basic;

SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;
SET min_parallel_table_scan_size = 0;
SET max_parallel_workers_per_gather = 2;

-- A bare count(*) is pushed down to a serial ColumnstoreAggregate, which
-- masks the scan's parallelism; fence a row-returning subquery with
-- OFFSET 0 so the plan exposes the ColumnstoreScan itself.
-- Should NOT show Gather node (only 1 work unit)
EXPLAIN (COSTS OFF)
    SELECT count(*) FROM (SELECT a FROM cs_basic WHERE b <> '' OFFSET 0) s;

-- Add second row group + delta → 3 work units >= 2, parallel enabled
INSERT INTO cs_basic SELECT i, 'rg2-' || i FROM generate_series(200, 250) i;
VACUUM cs_basic;
INSERT INTO cs_basic SELECT i, 'delta-' || i FROM generate_series(500, 510) i;

-- Should show Gather over a Parallel ColumnstoreScan (3 work units >= 2)
EXPLAIN (COSTS OFF)
    SELECT count(*) FROM (SELECT a FROM cs_basic WHERE b <> '' OFFSET 0) s;

RESET parallel_setup_cost;
RESET parallel_tuple_cost;
RESET min_parallel_table_scan_size;
RESET max_parallel_workers_per_gather;

-- Parallel scan EXPLAIN ANALYZE instrumentation must aggregate across all
-- participants, not report only the work units the leader happened to claim.
-- Pull a named counter out of EXPLAIN ANALYZE and confirm the parallel total
-- equals the serial total -- the value is data-determined and independent of
-- how many workers actually launch.
CREATE TABLE cs_instr (a int) USING columnstore;
INSERT INTO cs_instr SELECT g FROM generate_series(1, 120000) g;
VACUUM cs_instr;
CREATE FUNCTION cs_explain_counter(q text, workers int, label text)
    RETURNS bigint LANGUAGE plpgsql AS $fn$
DECLARE
    ln  text;
    tot bigint := 0;
BEGIN
    EXECUTE 'SET max_parallel_workers_per_gather = ' || workers;
    FOR ln IN
        EXECUTE 'EXPLAIN (ANALYZE, TIMING OFF, SUMMARY OFF, COSTS OFF) ' || q
    LOOP
        IF position(label IN ln) > 0 THEN
            tot := tot + substring(ln FROM '([0-9]+)\s*$')::bigint;
        END IF;
    END LOOP;
    RESET max_parallel_workers_per_gather;
    RETURN tot;
END $fn$;
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;
SET min_parallel_table_scan_size = 0;
-- fence the scan with OFFSET 0 so aggregate pushdown does not mask it; the
-- predicate prunes one of the two row groups by zone map
\set iq 'SELECT count(*) FROM (SELECT a FROM cs_instr WHERE a > 115000 OFFSET 0) s'
SELECT cs_explain_counter(:'iq', 0, 'Row Groups Examined') =
       cs_explain_counter(:'iq', 2, 'Row Groups Examined') AS examined_matches;
SELECT cs_explain_counter(:'iq', 0, 'Row Groups Skipped by Zone Map') =
       cs_explain_counter(:'iq', 2, 'Row Groups Skipped by Zone Map')
       AS zonemap_skipped_matches;
RESET parallel_setup_cost;
RESET parallel_tuple_cost;
RESET min_parallel_table_scan_size;
DROP FUNCTION cs_explain_counter(text, int, text);
DROP TABLE cs_instr;

-- Parallel sequential scan with enough row groups
DROP TABLE cs_basic;
CREATE TABLE cs_basic (a int, b text) USING columnstore;
-- Create 4 row groups + delta = 5 work units (enough for parallel)
INSERT INTO cs_basic SELECT i, 'rg1-' || i FROM generate_series(1, 100) i;
VACUUM cs_basic;
INSERT INTO cs_basic SELECT i, 'rg2-' || i FROM generate_series(200, 250) i;
VACUUM cs_basic;
INSERT INTO cs_basic SELECT i, 'rg3-' || i FROM generate_series(300, 350) i;
VACUUM cs_basic;
INSERT INTO cs_basic SELECT i, 'rg4-' || i FROM generate_series(400, 450) i;
VACUUM cs_basic;
-- Add delta rows to test mixed delta + columnar parallel scan
INSERT INTO cs_basic SELECT i, 'delta-' || i FROM generate_series(500, 510) i;

-- Force parallel query for this small table
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;
SET min_parallel_table_scan_size = 0;
SET max_parallel_workers_per_gather = 2;

-- Verify EXPLAIN shows Gather node with parallel workers
EXPLAIN (COSTS OFF) SELECT count(*) FROM cs_basic;
-- Verify correctness of parallel COUNT(*)
SELECT count(*) AS par_count FROM cs_basic;
-- Verify correctness of parallel SUM
SELECT sum(a) AS par_sum FROM cs_basic;
-- Verify correctness of parallel scan with filter
SELECT count(*) AS par_filtered FROM cs_basic WHERE a >= 200 AND a <= 250;
-- Verify correctness of parallel scan with ORDER BY (Gather Merge)
SELECT a FROM cs_basic WHERE a >= 500 ORDER BY a;

RESET parallel_setup_cost;
RESET parallel_tuple_cost;
RESET min_parallel_table_scan_size;
RESET max_parallel_workers_per_gather;

-- sort_key pathkeys: when row groups have non-overlapping ascending
-- ranges on the sort_key column, the CustomScan path advertises
-- pathkeys so an upper Sort can be elided on ORDER BY queries.
CREATE TABLE cs_sk_ordered (a int, b text)
    USING columnstore WITH (sort_key = 'a');
-- Append-ordered insert: row group 1 gets a=1..10, row group 2 gets a=11..20
INSERT INTO cs_sk_ordered SELECT i, 'sk-' || i FROM generate_series(1, 10) i;
VACUUM cs_sk_ordered;
INSERT INTO cs_sk_ordered SELECT i, 'sk-' || i FROM generate_series(11, 20) i;
VACUUM cs_sk_ordered;
INSERT INTO cs_sk_ordered SELECT i, 'sk-' || i FROM generate_series(21, 30) i;
VACUUM cs_sk_ordered;
-- Plan should have no upper Sort node.
EXPLAIN (COSTS OFF) SELECT a FROM cs_sk_ordered ORDER BY a LIMIT 5;
SELECT a FROM cs_sk_ordered ORDER BY a LIMIT 5;
DROP TABLE cs_sk_ordered;

-- sort_key with overlapping row groups: pathkeys must NOT be advertised,
-- planner must insert an upper Sort.  Inserts span the same value range,
-- so each row group's [min, max] overlaps the others.
CREATE TABLE cs_sk_overlap (a int, b text)
    USING columnstore WITH (sort_key = 'a');
INSERT INTO cs_sk_overlap SELECT i, 'sk-' || i FROM generate_series(1, 30) i;
VACUUM cs_sk_overlap;
INSERT INTO cs_sk_overlap SELECT i, 'sk-' || i FROM generate_series(1, 30) i;
VACUUM cs_sk_overlap;
INSERT INTO cs_sk_overlap SELECT i, 'sk-' || i FROM generate_series(1, 30) i;
VACUUM cs_sk_overlap;
-- Plan must have an upper Sort node.
EXPLAIN (COSTS OFF) SELECT a FROM cs_sk_overlap ORDER BY a LIMIT 5;
SELECT a FROM cs_sk_overlap ORDER BY a LIMIT 5;
DROP TABLE cs_sk_overlap;

-- Index scan support
DROP TABLE cs_basic;
CREATE TABLE cs_basic (a int, b text) USING columnstore;
INSERT INTO cs_basic SELECT i, 'idx-' || i FROM generate_series(1, 50) i;

-- Create index on delta rows
CREATE INDEX cs_basic_a_idx ON cs_basic (a);

-- Force index scan
SET enable_seqscan = off;

-- Index scan on delta rows
SELECT a, b FROM cs_basic WHERE a = 25;
SELECT a, b FROM cs_basic WHERE a = 1;
SELECT count(*) AS idx_delta_count FROM cs_basic WHERE a <= 10;

-- Freeze to columnar, then index scan on columnar rows
VACUUM cs_basic;
SELECT a, b FROM cs_basic WHERE a = 25;
SELECT a, b FROM cs_basic WHERE a = 50;
SELECT count(*) AS idx_columnar_count FROM cs_basic WHERE a <= 10;

-- Mixed: add more delta rows, scan should find both
INSERT INTO cs_basic SELECT i, 'new-' || i FROM generate_series(100, 110) i;
SELECT a, b FROM cs_basic WHERE a = 100;
SELECT a, b FROM cs_basic WHERE a = 25;
SELECT count(*) AS idx_mixed_count FROM cs_basic WHERE a <= 110;

-- Delete some columnar rows, index scan should respect deletion bitmap
DELETE FROM cs_basic WHERE a = 25;
DELETE FROM cs_basic WHERE a = 1;
SELECT a, b FROM cs_basic WHERE a = 25;
SELECT a, b FROM cs_basic WHERE a = 1;
SELECT count(*) AS idx_after_delete FROM cs_basic WHERE a <= 50;

-- Verify EXPLAIN shows index scan
EXPLAIN (COSTS OFF) SELECT a FROM cs_basic WHERE a = 42;

RESET enable_seqscan;

-- ===================================================================
-- ANALYZE on frozen columnstore tables
-- ===================================================================

-- Create and populate a table, then freeze to columnar format
CREATE TABLE cs_analyze (id int, val text, num numeric)
  USING columnstore;
INSERT INTO cs_analyze
  SELECT i, 'value_' || (i % 100)::text, (i * 1.1)::numeric
  FROM generate_series(1, 1000) i;
VACUUM cs_analyze;

-- Verify ANALYZE produces statistics on frozen columnar data
ANALYZE cs_analyze;

SELECT count(*) > 0 AS has_stats
  FROM pg_stats WHERE tablename = 'cs_analyze';

SELECT n_distinct IS NOT NULL AS has_ndistinct,
       most_common_vals IS NOT NULL AS has_mcv
  FROM pg_stats
  WHERE tablename = 'cs_analyze' AND attname = 'val';

-- Verify reltuples is sane (not double-counted)
SELECT reltuples BETWEEN 900 AND 1100 AS reltuples_sane
  FROM pg_class WHERE relname = 'cs_analyze';

DROP TABLE cs_analyze;

-- ===================================================================
-- Tuple locking (SELECT FOR UPDATE/SHARE)
-- ===================================================================

-- Test on delta store rows
DROP TABLE cs_basic;
CREATE TABLE cs_basic (a int, b text) USING columnstore;
INSERT INTO cs_basic SELECT i, 'lock-' || i FROM generate_series(1, 5) i;

-- FOR UPDATE on delta rows
SELECT a, b FROM cs_basic WHERE a = 3 FOR UPDATE;

-- FOR SHARE on delta rows
SELECT a, b FROM cs_basic WHERE a = 1 FOR SHARE;

-- FOR UPDATE SKIP LOCKED (no contention, should return the row)
SELECT a, b FROM cs_basic WHERE a = 2 FOR UPDATE SKIP LOCKED;

-- FOR UPDATE NOWAIT (no contention, should return the row)
SELECT a, b FROM cs_basic WHERE a = 4 FOR UPDATE NOWAIT;

-- Freeze to columnar, then test locking
VACUUM cs_basic;

-- FOR UPDATE on columnar rows
SELECT a, b FROM cs_basic WHERE a = 3 FOR UPDATE;

-- FOR SHARE on columnar rows
SELECT a, b FROM cs_basic WHERE a = 1 FOR SHARE;

-- FOR UPDATE SKIP LOCKED on columnar rows
SELECT a, b FROM cs_basic WHERE a = 2 FOR UPDATE SKIP LOCKED;

-- FOR UPDATE NOWAIT on columnar rows
SELECT a, b FROM cs_basic WHERE a = 4 FOR UPDATE NOWAIT;

-- After DELETE, locked rows should be excluded
DELETE FROM cs_basic WHERE a = 3;
SELECT a, b FROM cs_basic WHERE a = 3 FOR UPDATE;

-- Subquery with FOR UPDATE
SELECT a, b FROM cs_basic WHERE a IN (SELECT a FROM cs_basic WHERE a <= 2 FOR UPDATE) ORDER BY a;

-- Mixed delta + columnar locking
INSERT INTO cs_basic SELECT i, 'new-' || i FROM generate_series(10, 12) i;
SELECT a, b FROM cs_basic WHERE a IN (1, 10) ORDER BY a FOR UPDATE;

-- Index scan with row group caching
CREATE TABLE cs_indextest (id integer, val text) USING columnstore;
INSERT INTO cs_indextest SELECT i, 'value-' || i FROM generate_series(1, 2000) i;
VACUUM cs_indextest;
CREATE INDEX ON cs_indextest (id);
ANALYZE cs_indextest;
-- Point lookups (should hit cached row group)
SET enable_seqscan = off;
SELECT val FROM cs_indextest WHERE id = 42;
SELECT val FROM cs_indextest WHERE id = 1500;
-- Range scan (multiple rows from same row group)
SELECT count(*) FROM cs_indextest WHERE id BETWEEN 100 AND 200;
RESET enable_seqscan;
DROP TABLE cs_indextest;

-- ===================================================================
-- Row group compaction during VACUUM
-- ===================================================================

DROP TABLE cs_basic;
CREATE TABLE cs_basic (a int, b text) USING columnstore;
INSERT INTO cs_basic SELECT i, 'compact-' || i FROM generate_series(1, 20) i;
VACUUM cs_basic;
SELECT count(*) AS before_delete FROM cs_basic;

-- Delete more than half (threshold will be 0.5)
DELETE FROM cs_basic WHERE a <= 15;
SELECT count(*) AS after_delete FROM cs_basic;

-- VACUUM without compaction enabled (default: -1 = disabled)
VACUUM cs_basic;
SELECT count(*) AS after_vacuum_no_compact FROM cs_basic;

-- Enable compaction and VACUUM again
SET columnstore.rowgroup_compaction_threshold = 0.5;
VACUUM cs_basic;
SELECT count(*) AS after_compact FROM cs_basic;
SELECT a, b FROM cs_basic ORDER BY a;

-- Verify the data survived compaction correctly
SELECT count(*) AS final_check FROM cs_basic WHERE a > 15;

RESET columnstore.rowgroup_compaction_threshold;

-- Test compaction with index: index should be rebuilt automatically
DROP TABLE cs_basic;
CREATE TABLE cs_basic (a int, b text) USING columnstore;
INSERT INTO cs_basic SELECT i, 'idx-compact-' || i FROM generate_series(1, 20) i;
VACUUM cs_basic;
CREATE INDEX cs_compact_idx ON cs_basic (a);

-- Verify index is valid before compaction
SELECT indisvalid FROM pg_index WHERE indexrelid = 'cs_compact_idx'::regclass;

-- Delete enough rows and compact
DELETE FROM cs_basic WHERE a <= 15;
SET columnstore.rowgroup_compaction_threshold = 0.5;
VACUUM cs_basic;
RESET columnstore.rowgroup_compaction_threshold;

-- Index should still be valid (auto-rebuilt during VACUUM)
SELECT indisvalid FROM pg_index WHERE indexrelid = 'cs_compact_idx'::regclass;

-- Sequential scan works
SELECT count(*) AS after_idx_compact FROM cs_basic;
SELECT a FROM cs_basic ORDER BY a;

-- Index scan works with compacted data
SET enable_seqscan = off;
SELECT a, b FROM cs_basic WHERE a = 18;
RESET enable_seqscan;

DROP INDEX cs_compact_idx;

-- TOAST support: large varlena values
CREATE TABLE cs_toast (id int, big_text text, small_text text) USING columnstore;

-- Verify TOAST table was created
SELECT EXISTS (
  SELECT 1 FROM pg_class c WHERE c.relname = 'cs_toast' AND c.reltoastrelid <> 0
) AS has_toast_table;

-- Insert rows with large incompressible data (forces external TOAST storage)
INSERT INTO cs_toast VALUES (1,
  (SELECT string_agg(md5(s::text), '') FROM generate_series(1, 200) s), 'small1');
INSERT INTO cs_toast VALUES (2,
  (SELECT string_agg(md5(s::text), '') FROM generate_series(201, 400) s), 'small2');
INSERT INTO cs_toast VALUES (3, 'tiny', 'small3');

-- Verify delta read-back with toasted values
SELECT id, length(big_text), small_text FROM cs_toast ORDER BY id;

-- Compact to columnar (detoasts values during compaction)
VACUUM cs_toast;

-- Verify values survive compaction
SELECT id, length(big_text), small_text FROM cs_toast ORDER BY id;

-- Check that compaction released the TOAST chunks belonging to the
-- consumed delta rows.  cs_compact_delta calls heap_toast_delete on
-- every delta tuple with external TOAST references after the metapage
-- commit, so the TOAST relation should be empty once VACUUM returns.
DO $$
DECLARE
  rel_oid oid;
  toast_relname text;
  nchunks bigint;
BEGIN
  SELECT oid INTO rel_oid FROM pg_class WHERE relname = 'cs_toast';
  toast_relname := 'pg_toast_' || rel_oid;
  IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = toast_relname
                 AND relnamespace = 'pg_toast'::regnamespace) THEN
    RAISE EXCEPTION 'Expected TOAST table % to exist', toast_relname;
  END IF;
  EXECUTE format('SELECT count(*) FROM pg_toast.%I', toast_relname) INTO nchunks;
  IF nchunks <> 0 THEN
    RAISE EXCEPTION 'Expected 0 toast chunks after compaction, found %', nchunks;
  END IF;
END;
$$;

-- Test index scan on previously-toasted columnar data
CREATE INDEX cs_toast_idx ON cs_toast (id);
SET enable_seqscan = off;
SELECT id, length(big_text), small_text FROM cs_toast WHERE id = 2;
RESET enable_seqscan;
DROP INDEX cs_toast_idx;
DROP TABLE cs_toast;

-- TABLESAMPLE support
CREATE TABLE cs_sample (a int, b text) USING columnstore;
INSERT INTO cs_sample SELECT i, 'row-' || i FROM generate_series(1, 100) i;
VACUUM cs_sample;
INSERT INTO cs_sample SELECT i, 'delta-' || i FROM generate_series(101, 110) i;
-- BERNOULLI 100% should return all rows
SELECT count(*) AS bernoulli_all FROM cs_sample TABLESAMPLE BERNOULLI(100);
-- SYSTEM 100% should return all rows
SELECT count(*) AS system_all FROM cs_sample TABLESAMPLE SYSTEM(100);
-- BERNOULLI with REPEATABLE is deterministic
SELECT count(*) AS bernoulli_half FROM cs_sample TABLESAMPLE BERNOULLI(50) REPEATABLE(0);
-- Data integrity: all values present at 100%
SELECT min(a) AS smin, max(a) AS smax FROM cs_sample TABLESAMPLE BERNOULLI(100);
-- With WHERE clause
SELECT count(*) AS sample_filtered FROM cs_sample TABLESAMPLE BERNOULLI(100) WHERE a > 50;
-- Empty table
CREATE TABLE cs_sample_empty (a int) USING columnstore;
SELECT count(*) AS sample_empty FROM cs_sample_empty TABLESAMPLE BERNOULLI(100);
-- Columnar-only (no delta)
CREATE TABLE cs_sample_col (a int) USING columnstore;
INSERT INTO cs_sample_col SELECT i FROM generate_series(1, 50) i;
VACUUM cs_sample_col;
SELECT count(*) AS sample_col FROM cs_sample_col TABLESAMPLE BERNOULLI(100);
-- With deleted columnar rows
DELETE FROM cs_sample_col WHERE a <= 10;
SELECT count(*) AS sample_del FROM cs_sample_col TABLESAMPLE BERNOULLI(100);
DROP TABLE cs_sample;
DROP TABLE cs_sample_empty;
DROP TABLE cs_sample_col;

-- ===== CLUSTER and VACUUM FULL =====
-- Cluster on index: reorders rows by indexed column
CREATE TABLE cs_cluster (a int, b text) USING columnstore;
INSERT INTO cs_cluster SELECT i, 'row-' || i FROM generate_series(1, 200) i;
VACUUM cs_cluster;
-- Insert more rows in reverse order, then compact
INSERT INTO cs_cluster SELECT i, 'rev-' || i FROM generate_series(400, 201, -1) i;
VACUUM cs_cluster;
CREATE INDEX cs_cluster_a_idx ON cs_cluster (a);
SELECT count(*) AS before_cluster FROM cs_cluster;
CLUSTER cs_cluster USING cs_cluster_a_idx;
SELECT count(*) AS after_cluster FROM cs_cluster;
-- Verify ordering: first and last rows should be min/max of a
SELECT a, b FROM cs_cluster ORDER BY a LIMIT 3;
SELECT a, b FROM cs_cluster ORDER BY a DESC LIMIT 3;
-- Verify all data survived
SELECT count(*) AS cluster_total, min(a) AS cmin, max(a) AS cmax FROM cs_cluster;
-- VACUUM FULL (no reordering, just rewrites)
DELETE FROM cs_cluster WHERE a <= 50;
SELECT count(*) AS before_vf FROM cs_cluster;
VACUUM FULL cs_cluster;
SELECT count(*) AS after_vf FROM cs_cluster;
SELECT min(a) AS vf_min, max(a) AS vf_max FROM cs_cluster;
-- Cluster with mixed delta + columnar
INSERT INTO cs_cluster SELECT i, 'delta-' || i FROM generate_series(1, 10) i;
SELECT count(*) AS mixed_before FROM cs_cluster;
CLUSTER cs_cluster USING cs_cluster_a_idx;
SELECT count(*) AS mixed_after FROM cs_cluster;
SELECT a, b FROM cs_cluster ORDER BY a LIMIT 5;
DROP TABLE cs_cluster;

-- Cleanup
DROP TABLE cs_basic;
DROP TABLE cs_wide;
DROP TABLE cs_types;
DROP TABLE cs_wide200;

-- Zero-column tables: a zero-attribute data tuple must not be mistaken
-- for a tombstone (tombstones are zero-attribute tuples distinguished by
-- their columnar-space t_ctid)
CREATE TABLE cs_zerocol () USING columnstore;
INSERT INTO cs_zerocol DEFAULT VALUES;
INSERT INTO cs_zerocol SELECT FROM generate_series(1, 50);
SELECT count(*) FROM cs_zerocol;
DELETE FROM cs_zerocol;
SELECT count(*) FROM cs_zerocol;
DROP TABLE cs_zerocol;

-- Partitioned parents may carry the columnstore AM: the autovacuum
-- defaults event trigger must skip them (no storage parameters on 'p'
-- relations), and partitions inherit the AM
CREATE TABLE cs_part (k int, v int) PARTITION BY RANGE (k) USING columnstore;
CREATE TABLE cs_part1 PARTITION OF cs_part FOR VALUES FROM (0) TO (100);
CREATE TABLE cs_part2 PARTITION OF cs_part FOR VALUES FROM (100) TO (200);
SELECT c.relname, a.amname,
       c.reloptions IS NOT NULL AS has_av_defaults
    FROM pg_class c LEFT JOIN pg_am a ON a.oid = c.relam
    WHERE c.relname IN ('cs_part', 'cs_part1', 'cs_part2')
    ORDER BY c.relname;
INSERT INTO cs_part SELECT g % 200, g FROM generate_series(1, 400) g;
SELECT count(*), sum(v) FROM cs_part;
SELECT count(*) FROM cs_part WHERE k < 100;
DELETE FROM cs_part WHERE v <= 200;
SELECT count(*), sum(v) FROM cs_part;
DROP TABLE cs_part;

-- The defaults are applied at creation, including CREATE TABLE AS /
-- SELECT INTO.  A table converted with ALTER TABLE ... SET ACCESS METHOD
-- columnstore does NOT get them (the trigger fires only on creation, so
-- that a later ALTER TABLE ... RESET of these options is not re-imposed).
CREATE TABLE cs_avd_ctas USING columnstore AS SELECT 1 AS v;
CREATE TABLE cs_avd_heap (v int);
ALTER TABLE cs_avd_heap SET ACCESS METHOD columnstore;
SELECT relname, reloptions IS NOT NULL AS has_av_defaults
    FROM pg_class
    WHERE relname IN ('cs_avd_ctas', 'cs_avd_heap')
    ORDER BY relname;
DROP TABLE cs_avd_ctas, cs_avd_heap;

-- The autovacuum defaults are a creation-time convenience, not a lock-in:
-- a user can override them, and RESET must fall back to the GUC default
-- rather than being silently re-imposed by the event trigger.
CREATE TABLE cs_avd_reset (v int) USING columnstore;
SELECT reloptions FROM pg_class WHERE relname = 'cs_avd_reset';
ALTER TABLE cs_avd_reset SET (autovacuum_vacuum_insert_threshold = 5000);
SELECT reloptions FROM pg_class WHERE relname = 'cs_avd_reset';
ALTER TABLE cs_avd_reset RESET (autovacuum_vacuum_insert_threshold);
-- an unrelated ALTER afterwards must not bring the default back
ALTER TABLE cs_avd_reset ADD COLUMN w int;
SELECT reloptions FROM pg_class WHERE relname = 'cs_avd_reset';
DROP TABLE cs_avd_reset;

-- An option the user supplies at CREATE time is kept; the trigger only
-- fills in the one that is missing (here it must not overwrite the
-- explicit threshold, but must still add the scale_factor default).
CREATE TABLE cs_avd_explicit (v int) USING columnstore
    WITH (autovacuum_vacuum_insert_threshold = 5000);
SELECT unnest(reloptions) AS opt FROM pg_class
    WHERE relname = 'cs_avd_explicit' ORDER BY opt;
DROP TABLE cs_avd_explicit;
