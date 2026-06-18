--
-- Columnstore miscellaneous tests: late materialization, TOAST,
-- parallel scans, wide tables
--

-- ===================================================================
-- Late materialization: ORDER BY ... LIMIT with column projection
-- ===================================================================
CREATE TABLE cs_late_mat (id int, sort_key int, payload text) USING columnstore;
INSERT INTO cs_late_mat SELECT i, 1000 - i, repeat('x', 100) || i
    FROM generate_series(1, 2000) i;
VACUUM cs_late_mat;

-- Top-N query should benefit from late materialization
-- (only materializes LIMIT rows, not all 2000)
SELECT id, sort_key, length(payload) AS plen
    FROM cs_late_mat ORDER BY sort_key LIMIT 5;

-- Descending
SELECT id, sort_key
    FROM cs_late_mat ORDER BY sort_key DESC LIMIT 5;

-- With filter
SELECT id, sort_key
    FROM cs_late_mat WHERE id > 1000 ORDER BY sort_key LIMIT 3;

DROP TABLE cs_late_mat;

-- ===================================================================
-- TOAST: large text values through freeze and compaction
-- ===================================================================
CREATE TABLE cs_toast_stress (id int, big_text text, small_val int) USING columnstore;

-- Insert rows with large values that need TOAST
INSERT INTO cs_toast_stress SELECT i,
    (SELECT string_agg(md5(s::text), '') FROM generate_series(1, 100) s),
    i * 10
    FROM generate_series(1, 20) i;
VACUUM cs_toast_stress;

-- Verify data survived freeze
SELECT id, length(big_text) AS tlen, md5(big_text) AS thash, small_val
    FROM cs_toast_stress WHERE id IN (1, 10, 20) ORDER BY id;

-- Delete some and compact
DELETE FROM cs_toast_stress WHERE id <= 15;
SET columnstore.rowgroup_compaction_threshold = 0.5;
VACUUM cs_toast_stress;
RESET columnstore.rowgroup_compaction_threshold;

-- Verify data survived compaction
SELECT count(*) AS after_compact FROM cs_toast_stress;
SELECT id, length(big_text) AS tlen, small_val
    FROM cs_toast_stress WHERE id = 16;

DROP TABLE cs_toast_stress;

-- ===================================================================
-- Parallel scan correctness
-- ===================================================================
CREATE TABLE cs_parallel (id int, grp int, val numeric(10,2)) USING columnstore;
-- Create enough row groups for parallel scan (need >= 4 work units)
INSERT INTO cs_parallel SELECT i, i % 5, (i * 1.1)::numeric(10,2)
    FROM generate_series(1, 500) i;
VACUUM cs_parallel;
INSERT INTO cs_parallel SELECT i, i % 5, (i * 1.1)::numeric(10,2)
    FROM generate_series(501, 1000) i;
VACUUM cs_parallel;
INSERT INTO cs_parallel SELECT i, i % 5, (i * 1.1)::numeric(10,2)
    FROM generate_series(1001, 1500) i;
VACUUM cs_parallel;
INSERT INTO cs_parallel SELECT i, i % 5, (i * 1.1)::numeric(10,2)
    FROM generate_series(1501, 2000) i;
VACUUM cs_parallel;
-- Delta rows
INSERT INTO cs_parallel SELECT i, i % 5, (i * 1.1)::numeric(10,2)
    FROM generate_series(2001, 2100) i;

-- Save non-parallel results for comparison
SET max_parallel_workers_per_gather = 0;
SELECT count(*) AS seq_count FROM cs_parallel;
SELECT sum(val) AS seq_sum FROM cs_parallel;
SELECT grp, count(*), sum(val) FROM cs_parallel GROUP BY grp ORDER BY grp;
RESET max_parallel_workers_per_gather;

-- Force parallel
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;
SET min_parallel_table_scan_size = 0;
SET max_parallel_workers_per_gather = 2;

-- Verify parallel plan
EXPLAIN (COSTS OFF) SELECT count(*) FROM cs_parallel;

-- Compare results
SELECT count(*) AS par_count FROM cs_parallel;
SELECT sum(val) AS par_sum FROM cs_parallel;

-- Grouped aggregate in parallel
SELECT grp, count(*), sum(val) FROM cs_parallel GROUP BY grp ORDER BY grp;

-- Parallel with filter
SELECT count(*) AS par_filtered FROM cs_parallel WHERE id > 1500;

RESET parallel_setup_cost;
RESET parallel_tuple_cost;
RESET min_parallel_table_scan_size;
RESET max_parallel_workers_per_gather;

DROP TABLE cs_parallel;

-- ===================================================================
-- Very wide table: multi-page catalog, full lifecycle
-- ===================================================================
CREATE TABLE cs_wide_stress (
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

INSERT INTO cs_wide_stress SELECT
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
    FROM generate_series(1, 200) i;
VACUUM cs_wide_stress;

-- Verify data
SELECT count(*) AS wide_count FROM cs_wide_stress;
SELECT c1, c100, c200 FROM cs_wide_stress WHERE c1 = 1;
SELECT c1, c100, c200 FROM cs_wide_stress WHERE c1 = 200;

-- Column projection: access only 3 of 200 columns
SELECT sum(c1), sum(c100), sum(c200) FROM cs_wide_stress;

-- Delete and compact
DELETE FROM cs_wide_stress WHERE c1 <= 150;
SET columnstore.rowgroup_compaction_threshold = 0.5;
VACUUM cs_wide_stress;
RESET columnstore.rowgroup_compaction_threshold;

SELECT count(*) AS after_compact FROM cs_wide_stress;
SELECT c1, c100, c200 FROM cs_wide_stress WHERE c1 = 151;

DROP TABLE cs_wide_stress;

-- ===================================================================
-- Numeric precision edge cases through full pipeline
-- ===================================================================
CREATE TABLE cs_num_edge (id int, v numeric) USING columnstore;
INSERT INTO cs_num_edge VALUES (1, 0);
INSERT INTO cs_num_edge VALUES (2, 0.00);
INSERT INTO cs_num_edge VALUES (3, -0.00);
INSERT INTO cs_num_edge VALUES (4, 99999999999999999.99);
INSERT INTO cs_num_edge VALUES (5, -99999999999999999.99);
INSERT INTO cs_num_edge VALUES (6, 0.0000000001);
INSERT INTO cs_num_edge VALUES (7, 'NaN'::numeric);
-- Pad to trigger freeze
INSERT INTO cs_num_edge SELECT i, (i * 0.01)::numeric
    FROM generate_series(8, 500) i;
VACUUM cs_num_edge;

SELECT id, v FROM cs_num_edge WHERE id <= 7 ORDER BY id;
DROP TABLE cs_num_edge;

-- ===================================================================
-- TID range scan (WHERE ctid >= ... AND ctid < ...)
-- ===================================================================
CREATE TABLE cs_tidrange (id int, val text) USING columnstore;
INSERT INTO cs_tidrange SELECT i, 'row_' || i FROM generate_series(1, 500) i;
VACUUM cs_tidrange;
-- Delta rows
INSERT INTO cs_tidrange SELECT i, 'delta_' || i FROM generate_series(501, 600) i;

-- Unrestricted scan count for reference
SELECT count(*) AS total FROM cs_tidrange;

-- TID range that excludes columnar virtual blocks (only delta)
-- Delta blocks have block numbers below CS_COLUMNAR_BLKNO_BASE (0x40000000)
SELECT count(*) > 0 AS has_delta_rows
    FROM cs_tidrange WHERE ctid >= '(0,0)' AND ctid < '(1073741824,1)';

-- TID range that matches nothing (block 9999 doesn't exist)
SELECT count(*) FROM cs_tidrange WHERE ctid >= '(9999,1)' AND ctid < '(9999,100)';

-- Full range should return all rows
SELECT count(*) FROM cs_tidrange WHERE ctid >= '(0,0)' AND ctid <= '(4294967295,65535)';

DROP TABLE cs_tidrange;

-- ===================================================================
-- TABLESAMPLE on delta + columnar
-- ===================================================================
CREATE TABLE cs_sample (id int, val text) USING columnstore;
INSERT INTO cs_sample SELECT i, 'row-' || i FROM generate_series(1, 1000) i;
VACUUM cs_sample;
-- Delta rows
INSERT INTO cs_sample SELECT i, 'delta-' || i FROM generate_series(1001, 1200) i;

-- SYSTEM: block-level sampling, deterministic with REPEATABLE
SELECT count(*) > 0 AS system_returns_rows
    FROM cs_sample TABLESAMPLE SYSTEM (50) REPEATABLE (42);

-- BERNOULLI: row-level sampling
SELECT count(*) > 0 AS bernoulli_returns_rows
    FROM cs_sample TABLESAMPLE BERNOULLI (50) REPEATABLE (42);

-- 100% sample must return every live row
SELECT count(*) AS full_sample
    FROM cs_sample TABLESAMPLE BERNOULLI (100) REPEATABLE (42);

-- 0% sample must return nothing
SELECT count(*) AS empty_sample
    FROM cs_sample TABLESAMPLE BERNOULLI (0) REPEATABLE (42);

-- Same REPEATABLE seed must yield identical rows
SELECT (SELECT array_agg(id ORDER BY id)
          FROM cs_sample TABLESAMPLE BERNOULLI (30) REPEATABLE (7))
     = (SELECT array_agg(id ORDER BY id)
          FROM cs_sample TABLESAMPLE BERNOULLI (30) REPEATABLE (7))
    AS deterministic;

DROP TABLE cs_sample;

-- ===================================================================
-- INSERT ... ON CONFLICT (speculative insertion)
-- ===================================================================
-- Speculative insert flow hooks into tuple_insert_speculative /
-- tuple_complete_speculative so INSERT ... ON CONFLICT works on delta
-- store rows with a unique index.
CREATE TABLE cs_upsert (id int PRIMARY KEY, v text) USING columnstore;
INSERT INTO cs_upsert SELECT i, 'initial-' || i FROM generate_series(1, 20) i;

-- ON CONFLICT DO NOTHING: duplicate key leaves the existing row in place.
INSERT INTO cs_upsert VALUES (5, 'attempted') ON CONFLICT (id) DO NOTHING;
SELECT v FROM cs_upsert WHERE id = 5;

-- ON CONFLICT DO UPDATE: duplicate key is converted to an update.
INSERT INTO cs_upsert VALUES (7, 'new-7')
    ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v;
SELECT v FROM cs_upsert WHERE id = 7;

-- New key: inserts normally.
INSERT INTO cs_upsert VALUES (100, 'fresh')
    ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v || '-dup';
SELECT v FROM cs_upsert WHERE id = 100;

DROP TABLE cs_upsert;

-- ===================================================================
-- Very large individual datums (multi-MB single values)
-- ===================================================================
-- SET STORAGE EXTERNAL forces TOAST to push the value out of line
-- without compression, so this exercises the external-TOAST path
-- deterministically regardless of how compressible the bytes are.
-- The compactor must detoast every attribute when serializing it
-- inline into the column chunk; performance is not the goal here,
-- correctness through the full lifecycle is.
CREATE TABLE cs_big_datum (id int, payload text) USING columnstore;
ALTER TABLE cs_big_datum ALTER COLUMN payload SET STORAGE EXTERNAL;

-- ~4 MB per row, with a per-row prefix so the head is distinguishable.
INSERT INTO cs_big_datum
SELECT i, 'row-' || i || '-' || repeat('x', 4 * 1024 * 1024)
    FROM generate_series(1, 3) i;

-- Length, head and tail md5 cover truncation and corruption without
-- printing megabytes of output.
SELECT id, length(payload) AS plen,
       md5(substring(payload FOR 64)) AS head_md5,
       md5(substring(payload FROM length(payload) - 63)) AS tail_md5
    FROM cs_big_datum ORDER BY id;

-- Compact into a columnar stripe: detoast + inline serialization
-- + stripe-level compression.
VACUUM cs_big_datum;
SELECT id, length(payload) AS plen,
       md5(substring(payload FOR 64)) AS head_md5,
       md5(substring(payload FROM length(payload) - 63)) AS tail_md5
    FROM cs_big_datum ORDER BY id;

-- Delete the middle row and force compaction; the deleted row's
-- stripe-side bytes and any external TOAST chunks are released
-- after the metadata commit.
DELETE FROM cs_big_datum WHERE id = 2;
SET columnstore.rowgroup_compaction_threshold = 0.5;
VACUUM cs_big_datum;
RESET columnstore.rowgroup_compaction_threshold;
SELECT id, length(payload) AS plen,
       md5(substring(payload FOR 64)) AS head_md5,
       md5(substring(payload FROM length(payload) - 63)) AS tail_md5
    FROM cs_big_datum ORDER BY id;

DROP TABLE cs_big_datum;

-- ===================================================================
-- Very wide rows: many large attributes per tuple
-- ===================================================================
-- 20 text columns, each ~256 KB, every column EXTERNAL.  The delta-
-- store INSERT path must push all 20 attributes out of line; the
-- compactor must then walk that entire TOAST pointer set when
-- building the row group.
CREATE TABLE cs_wide_big (
    id int,
    t01 text, t02 text, t03 text, t04 text, t05 text,
    t06 text, t07 text, t08 text, t09 text, t10 text,
    t11 text, t12 text, t13 text, t14 text, t15 text,
    t16 text, t17 text, t18 text, t19 text, t20 text
) USING columnstore;

ALTER TABLE cs_wide_big
    ALTER COLUMN t01 SET STORAGE EXTERNAL,
    ALTER COLUMN t02 SET STORAGE EXTERNAL,
    ALTER COLUMN t03 SET STORAGE EXTERNAL,
    ALTER COLUMN t04 SET STORAGE EXTERNAL,
    ALTER COLUMN t05 SET STORAGE EXTERNAL,
    ALTER COLUMN t06 SET STORAGE EXTERNAL,
    ALTER COLUMN t07 SET STORAGE EXTERNAL,
    ALTER COLUMN t08 SET STORAGE EXTERNAL,
    ALTER COLUMN t09 SET STORAGE EXTERNAL,
    ALTER COLUMN t10 SET STORAGE EXTERNAL,
    ALTER COLUMN t11 SET STORAGE EXTERNAL,
    ALTER COLUMN t12 SET STORAGE EXTERNAL,
    ALTER COLUMN t13 SET STORAGE EXTERNAL,
    ALTER COLUMN t14 SET STORAGE EXTERNAL,
    ALTER COLUMN t15 SET STORAGE EXTERNAL,
    ALTER COLUMN t16 SET STORAGE EXTERNAL,
    ALTER COLUMN t17 SET STORAGE EXTERNAL,
    ALTER COLUMN t18 SET STORAGE EXTERNAL,
    ALTER COLUMN t19 SET STORAGE EXTERNAL,
    ALTER COLUMN t20 SET STORAGE EXTERNAL;

-- Each row is ~5 MB after detoast; columns differ by content char.
INSERT INTO cs_wide_big
SELECT i,
    repeat('A', 262144), repeat('B', 262144), repeat('C', 262144),
    repeat('D', 262144), repeat('E', 262144), repeat('F', 262144),
    repeat('G', 262144), repeat('H', 262144), repeat('I', 262144),
    repeat('J', 262144), repeat('K', 262144), repeat('L', 262144),
    repeat('M', 262144), repeat('N', 262144), repeat('O', 262144),
    repeat('P', 262144), repeat('Q', 262144), repeat('R', 262144),
    repeat('S', 262144), repeat('T', 262144)
    FROM generate_series(1, 2) i;

-- Round trip through delta store.
SELECT id,
       length(t01) AS l01, md5(t01) AS h01,
       length(t10) AS l10, md5(t10) AS h10,
       length(t20) AS l20, md5(t20) AS h20
    FROM cs_wide_big ORDER BY id;

VACUUM cs_wide_big;

-- Round trip through compaction into stripes.
SELECT id,
       length(t01) AS l01, md5(t01) AS h01,
       length(t10) AS l10, md5(t10) AS h10,
       length(t20) AS l20, md5(t20) AS h20
    FROM cs_wide_big ORDER BY id;

-- Touch every column so every chunk has to be read back.
SELECT id,
       length(t01) + length(t02) + length(t03) + length(t04) +
       length(t05) + length(t06) + length(t07) + length(t08) +
       length(t09) + length(t10) + length(t11) + length(t12) +
       length(t13) + length(t14) + length(t15) + length(t16) +
       length(t17) + length(t18) + length(t19) + length(t20)
       AS total_bytes
    FROM cs_wide_big ORDER BY id;

DROP TABLE cs_wide_big;

-- ===================================================================
-- Parallel worker snapshot
--
-- The DSM estimate/initialize callbacks must go through the
-- table_parallelscan_* wrappers so the scan snapshot is serialized
-- into the shared descriptor; bypassing them leaves the workers
-- restoring garbage as their snapshot, which (once enough xids have
-- been consumed) makes delta-store rows invisible to parallel scans.
-- Burn xids first so that failure mode is deterministic, not masked
-- by a young cluster's small xids.
-- ===================================================================
CREATE PROCEDURE cs_burn_xids(n int) LANGUAGE plpgsql AS $$
BEGIN
    FOR i IN 1..n LOOP
        PERFORM txid_current();
        COMMIT;
    END LOOP;
END $$;
CALL cs_burn_xids(30000);
CREATE TABLE cs_par_snap (a int) USING columnstore;
INSERT INTO cs_par_snap SELECT i FROM generate_series(1, 1000) i;
VACUUM cs_par_snap;
INSERT INTO cs_par_snap SELECT i FROM generate_series(1001, 2000) i;
VACUUM cs_par_snap;
INSERT INTO cs_par_snap SELECT i FROM generate_series(2001, 3000) i;
VACUUM cs_par_snap;
-- these stay in the delta store, with post-burn xmins
INSERT INTO cs_par_snap SELECT i FROM generate_series(3001, 3500) i;
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;
SET min_parallel_table_scan_size = 0;
SET max_parallel_workers_per_gather = 2;
-- parallel aggregate pushdown must count the delta rows
EXPLAIN (COSTS OFF) SELECT count(*) FROM cs_par_snap;
SELECT count(*) FROM cs_par_snap;
-- parallel plain scan too (the subquery fence keeps the aggregate
-- from being pushed into the scan)
SELECT count(a) FROM (SELECT a FROM cs_par_snap OFFSET 0) s;
-- rows inserted by our own open transaction must also be visible to
-- the workers
BEGIN;
INSERT INTO cs_par_snap SELECT i FROM generate_series(3501, 3600) i;
SELECT count(*) FROM cs_par_snap;
ROLLBACK;
RESET parallel_setup_cost;
RESET parallel_tuple_cost;
RESET min_parallel_table_scan_size;
RESET max_parallel_workers_per_gather;
DROP TABLE cs_par_snap;
DROP PROCEDURE cs_burn_xids(int);

-- ===================================================================
-- TID probes outside row space
--
-- WHERE ctid = ... may name any block: the metapage, row-group
-- catalog or column-data pages, or blocks beyond the delta.  None of
-- those are rows; the probes must return nothing rather than misread
-- page bytes as line pointers.
-- ===================================================================
CREATE TABLE cs_tidprobe (a int, b text) USING columnstore;
INSERT INTO cs_tidprobe SELECT i, 't_' || i FROM generate_series(1, 500) i;
VACUUM cs_tidprobe;
INSERT INTO cs_tidprobe VALUES (501, 'delta');
-- metapage and column-data pages are not rows
SELECT * FROM cs_tidprobe WHERE ctid = '(0,1)';
SELECT * FROM cs_tidprobe WHERE ctid = '(1,1)';
SELECT * FROM cs_tidprobe WHERE ctid = '(2,9999)';
-- nonexistent row group in virtual TID space
SELECT * FROM cs_tidprobe WHERE ctid = '(1073742824,1)';
-- a real row refetched by its own ctid
SELECT t2.a, t2.b FROM cs_tidprobe t1, cs_tidprobe t2
    WHERE t1.a = 501 AND t2.ctid = t1.ctid;
DROP TABLE cs_tidprobe;

-- ===================================================================
-- COPY with values wide enough to need TOAST
-- ===================================================================
CREATE TABLE cs_copy_toast (id int, big text) USING columnstore;
COPY cs_copy_toast FROM stdin;
1	aaaaaaaaaa
2	cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc
\.
SELECT id, length(big) FROM cs_copy_toast ORDER BY id;
DROP TABLE cs_copy_toast;

-- ===================================================================
-- ANALYZE must not count rolled-back rows
-- ===================================================================
CREATE TABLE cs_analyze_vis (a int) USING columnstore;
INSERT INTO cs_analyze_vis SELECT i FROM generate_series(1, 100) i;
BEGIN;
INSERT INTO cs_analyze_vis SELECT i FROM generate_series(101, 5000) i;
ROLLBACK;
ANALYZE cs_analyze_vis;
SELECT reltuples::int FROM pg_class WHERE relname = 'cs_analyze_vis';
DROP TABLE cs_analyze_vis;

-- Identity and generated stored columns through INSERT, COPY and
-- compaction
CREATE TABLE cs_gencols (id int GENERATED ALWAYS AS IDENTITY,
                         v int,
                         twice int GENERATED ALWAYS AS (v * 2) STORED)
    USING columnstore;
INSERT INTO cs_gencols (v) SELECT g FROM generate_series(1, 300) g;
COPY cs_gencols (v) FROM stdin;
9991
9992
\.
VACUUM cs_gencols;
SELECT count(*), max(id), sum(twice) = 2 * sum(v) AS gen_ok FROM cs_gencols;
DROP TABLE cs_gencols;

-- COPY BINARY round trip (server-side COPY; the scratch file goes to
-- the build directory like core's copy.sql, not into the data dir)
CREATE TABLE cs_copybin (a int, b text, c numeric(8,2), f float8)
    USING columnstore;
INSERT INTO cs_copybin SELECT g, 't' || g, g * 1.5, g * 0.25
    FROM generate_series(1, 200) g;
VACUUM cs_copybin;
\getenv abs_builddir PG_ABS_BUILDDIR
\set binpath :abs_builddir '/results/cs_copybin.data'
COPY cs_copybin TO :'binpath' WITH (FORMAT binary);
CREATE TABLE cs_copybin2 (LIKE cs_copybin) USING columnstore;
COPY cs_copybin2 FROM :'binpath' WITH (FORMAT binary);
SELECT (SELECT count(*) FROM cs_copybin2) = 200 AS count_ok,
       NOT EXISTS (TABLE cs_copybin EXCEPT TABLE cs_copybin2)
       AND NOT EXISTS (TABLE cs_copybin2 EXCEPT TABLE cs_copybin) AS same;
DROP TABLE cs_copybin, cs_copybin2;

-- Persistence and rewrite paths: unlogged tables, SET UNLOGGED/LOGGED,
-- CREATE TABLE AS, SET ACCESS METHOD round trip, transactional TRUNCATE
CREATE UNLOGGED TABLE cs_unlogged (v int) USING columnstore;
INSERT INTO cs_unlogged SELECT g FROM generate_series(1, 500) g;
VACUUM cs_unlogged;
SELECT count(*) FROM cs_unlogged;
DROP TABLE cs_unlogged;
CREATE TABLE cs_rewrites (k int, v text) USING columnstore;
INSERT INTO cs_rewrites SELECT g, 'x' || g FROM generate_series(1, 500) g;
VACUUM cs_rewrites;
ALTER TABLE cs_rewrites SET UNLOGGED;
SELECT count(*) FROM cs_rewrites;
ALTER TABLE cs_rewrites SET LOGGED;
SELECT count(*) FROM cs_rewrites;
CREATE TABLE cs_rewrites_ctas USING columnstore AS SELECT * FROM cs_rewrites;
SELECT count(*) FROM cs_rewrites_ctas;
ALTER TABLE cs_rewrites SET ACCESS METHOD heap;
ALTER TABLE cs_rewrites SET ACCESS METHOD columnstore;
SELECT count(*),
       NOT EXISTS (TABLE cs_rewrites EXCEPT TABLE cs_rewrites_ctas) AS same
    FROM cs_rewrites;
-- VACUUM FULL goes through the CLUSTER machinery and is rejected with
-- a clear error (direct CLUSTER is covered in columnstore.sql)
VACUUM FULL cs_rewrites;
BEGIN;
TRUNCATE cs_rewrites_ctas;
SELECT count(*) FROM cs_rewrites_ctas;
ROLLBACK;
SELECT count(*) FROM cs_rewrites_ctas;
DROP TABLE cs_rewrites, cs_rewrites_ctas;
