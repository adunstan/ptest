--
-- Columnstore aggregate correctness tests
-- Tests SUM/AVG/COUNT/MIN/MAX against heap for various encodings
--

-- ===================================================================
-- Basic aggregate correctness: NI64 columns with GROUP BY
-- ===================================================================
CREATE TABLE cs_agg (grp int, val numeric(10,2)) USING columnstore;
CREATE TABLE heap_agg (grp int, val numeric(10,2));

INSERT INTO cs_agg SELECT i % 10, (i * 1.23)::numeric(10,2)
    FROM generate_series(1, 2000) i;
INSERT INTO heap_agg SELECT i % 10, (i * 1.23)::numeric(10,2)
    FROM generate_series(1, 2000) i;
VACUUM cs_agg;

-- Compare grouped aggregates
SELECT count(*) FROM (
    SELECT grp, sum(val) AS s, avg(val) AS a, min(val) AS mn, max(val) AS mx, count(*) AS c
    FROM cs_agg GROUP BY grp
    EXCEPT
    SELECT grp, sum(val) AS s, avg(val) AS a, min(val) AS mn, max(val) AS mx, count(*) AS c
    FROM heap_agg GROUP BY grp
) diff;

-- Ungrouped aggregates (compare to heap without exposing numeric dscale)
SELECT (SELECT sum(val) FROM cs_agg) = (SELECT sum(val) FROM heap_agg) AS sum_match;
SELECT (SELECT avg(val) FROM cs_agg) = (SELECT avg(val) FROM heap_agg) AS avg_match;

DROP TABLE cs_agg;
DROP TABLE heap_agg;

-- ===================================================================
-- Aggregates with NULLs
-- ===================================================================
CREATE TABLE cs_agg_null (grp int, val numeric(10,2)) USING columnstore;
CREATE TABLE heap_agg_null (grp int, val numeric(10,2));

INSERT INTO cs_agg_null SELECT i % 5,
    CASE WHEN i % 3 = 0 THEN NULL ELSE (i * 0.5)::numeric(10,2) END
    FROM generate_series(1, 1500) i;
INSERT INTO heap_agg_null SELECT i % 5,
    CASE WHEN i % 3 = 0 THEN NULL ELSE (i * 0.5)::numeric(10,2) END
    FROM generate_series(1, 1500) i;
VACUUM cs_agg_null;

SELECT count(*) FROM (
    SELECT grp, sum(val), count(val), count(*)
    FROM cs_agg_null GROUP BY grp
    EXCEPT
    SELECT grp, sum(val), count(val), count(*)
    FROM heap_agg_null GROUP BY grp
) diff;

DROP TABLE cs_agg_null;
DROP TABLE heap_agg_null;

-- ===================================================================
-- Aggregates on integer types (FOR-encoded)
-- ===================================================================
CREATE TABLE cs_agg_int (grp int, v_i2 smallint, v_i4 int, v_i8 bigint) USING columnstore;
CREATE TABLE heap_agg_int (grp int, v_i2 smallint, v_i4 int, v_i8 bigint);

INSERT INTO cs_agg_int SELECT i % 7, (i % 100)::smallint, i, i::bigint * 100000
    FROM generate_series(1, 2000) i;
INSERT INTO heap_agg_int SELECT i % 7, (i % 100)::smallint, i, i::bigint * 100000
    FROM generate_series(1, 2000) i;
VACUUM cs_agg_int;

SELECT count(*) FROM (
    SELECT grp, sum(v_i2::int), sum(v_i4::bigint), sum(v_i8)
    FROM cs_agg_int GROUP BY grp
    EXCEPT
    SELECT grp, sum(v_i2::int), sum(v_i4::bigint), sum(v_i8)
    FROM heap_agg_int GROUP BY grp
) diff;

DROP TABLE cs_agg_int;
DROP TABLE heap_agg_int;

-- ===================================================================
-- Aggregates with deletions in columnar data
-- ===================================================================
CREATE TABLE cs_agg_del (id int, val numeric(10,2)) USING columnstore;
INSERT INTO cs_agg_del SELECT i, (i * 1.5)::numeric(10,2) FROM generate_series(1, 1000) i;
VACUUM cs_agg_del;
-- Delete even rows
DELETE FROM cs_agg_del WHERE id % 2 = 0;

-- Compare with heap that has only odd rows
CREATE TABLE heap_agg_del AS
    SELECT i AS id, (i * 1.5)::numeric(10,2) AS val
    FROM generate_series(1, 1000) i WHERE i % 2 = 1;

SELECT (SELECT sum(val) FROM cs_agg_del) = (SELECT sum(val) FROM heap_agg_del) AS sum_match;
SELECT (SELECT count(*) FROM cs_agg_del) = (SELECT count(*) FROM heap_agg_del) AS count_match;
SELECT (SELECT avg(val) FROM cs_agg_del) = (SELECT avg(val) FROM heap_agg_del) AS avg_match;

DROP TABLE cs_agg_del;
DROP TABLE heap_agg_del;

-- ===================================================================
-- COUNT(*) with various combinations
-- ===================================================================
CREATE TABLE cs_count (id int, v int) USING columnstore;
INSERT INTO cs_count SELECT i, i FROM generate_series(1, 1000) i;
VACUUM cs_count;

-- Pure columnar COUNT(*)
SELECT count(*) AS pure_columnar FROM cs_count;

-- After deletions
DELETE FROM cs_count WHERE id % 10 = 0;
SELECT count(*) AS after_delete FROM cs_count;

-- Mixed delta + columnar
INSERT INTO cs_count SELECT i, i FROM generate_series(2000, 2050) i;
SELECT count(*) AS mixed FROM cs_count;

-- COUNT with filter
SELECT count(*) AS filtered FROM cs_count WHERE v > 500;

DROP TABLE cs_count;

-- ===================================================================
-- Aggregate pushdown after UPDATE on columnar rows
--
-- An UPDATE on a columnar row creates one tombstone (the deleted
-- columnar row) plus one new delta tuple.  The COUNT(*) fast path
-- exercises both the deletion bitmap iteration and the delta-store
-- scan in the same query.  This is the case that caught a regression
-- where the agg pushdown returned 0 instead of the expected count.
-- ===================================================================
CREATE TABLE cs_agg_upd (id int, val int) USING columnstore;
CREATE TABLE heap_agg_upd (id int, val int);
INSERT INTO cs_agg_upd SELECT i, i FROM generate_series(1, 1000) i;
INSERT INTO heap_agg_upd SELECT i, i FROM generate_series(1, 1000) i;
VACUUM cs_agg_upd;

UPDATE cs_agg_upd   SET val = val + 100000 WHERE id BETWEEN 100 AND 110;
UPDATE heap_agg_upd SET val = val + 100000 WHERE id BETWEEN 100 AND 110;

SELECT (SELECT count(*) FROM cs_agg_upd) = (SELECT count(*) FROM heap_agg_upd) AS count_match;
SELECT (SELECT sum(val) FROM cs_agg_upd) = (SELECT sum(val) FROM heap_agg_upd) AS sum_match;
SELECT (SELECT min(val) FROM cs_agg_upd) = (SELECT min(val) FROM heap_agg_upd) AS min_match;
SELECT (SELECT max(val) FROM cs_agg_upd) = (SELECT max(val) FROM heap_agg_upd) AS max_match;
SELECT (SELECT avg(val) FROM cs_agg_upd) = (SELECT avg(val) FROM heap_agg_upd) AS avg_match;

DROP TABLE cs_agg_upd;
DROP TABLE heap_agg_upd;

-- ===================================================================
-- Aggregate pushdown edge safety
-- ===================================================================
CREATE TABLE cs_agg_edge (k int, g int, n numeric, t text) USING columnstore;
INSERT INTO cs_agg_edge
    SELECT i % 5, i % 3, (i % 100) / 10.0, 'x' || i FROM generate_series(1, 1000) i;
VACUUM cs_agg_edge;
ANALYZE cs_agg_edge;
-- whole-row references must materialize every column
SELECT (e).k, (e).t FROM (SELECT e FROM cs_agg_edge e WHERE (e).k = 2 LIMIT 1) s;
SELECT count(*) FROM cs_agg_edge e WHERE length(e::text) > 10;
-- correlated COUNT(DISTINCT) rescanned per outer row
CREATE TABLE cs_agg_outer (k int);
INSERT INTO cs_agg_outer VALUES (0), (1), (2);
SET enable_hashjoin = off;
SET enable_mergejoin = off;
SET enable_memoize = off;
SELECT o.k, (SELECT count(DISTINCT g) FROM cs_agg_edge e WHERE e.k = o.k) AS dg
    FROM cs_agg_outer o ORDER BY o.k;
RESET enable_hashjoin;
RESET enable_mergejoin;
RESET enable_memoize;
-- quals containing sublinks fall through to the standard plan
SELECT count(*) FROM cs_agg_edge
    WHERE g = 1 OR k IN (SELECT k FROM cs_agg_outer);
-- a user-defined zero-argument aggregate is not COUNT(*): this one
-- counts from 42, so the answer must be 1042, not the 1000 a
-- misclassification as COUNT(*) would give
CREATE AGGREGATE cs_count_from_42 (*) (
    SFUNC = int4inc, STYPE = int4, INITCOND = '42');
SELECT cs_count_from_42(*) FROM cs_agg_edge;
DROP AGGREGATE cs_count_from_42 (*);
-- ORDER BY ctid LIMIT must not go through late materialization
EXPLAIN (COSTS OFF) SELECT k FROM cs_agg_edge ORDER BY ctid LIMIT 3;
DROP TABLE cs_agg_outer;
DROP TABLE cs_agg_edge;

-- Numeric columns whose dscale changes between row groups.  The first
-- 100k rows are huge whole numbers (NI64 dscale 0), the next 100k are
-- tiny 20-decimal fractions, so the two batches straddle a row-group
-- boundary: one row group is pure dscale 0, the mixed one cannot use
-- NI64 at all, and the tail is pure dscale 20.  This exercises
-- (a) clearing NI64 state when the next row group is not NI64-encoded,
-- and (b) the int128 SUM/AVG fast path rescaling its accumulator across
-- dscales, including the overflow flush into the numeric accumulator
-- (9e18 * 100k * 10^20 vastly exceeds the int128 guard).
CREATE TABLE cs_agg_mixscale (v numeric) USING columnstore;
INSERT INTO cs_agg_mixscale
    SELECT 9000000000000000000::numeric FROM generate_series(1, 100000);
INSERT INTO cs_agg_mixscale
    SELECT 0.00000000000000000001 FROM generate_series(1, 100000);
VACUUM cs_agg_mixscale;
SELECT sum(v), avg(v), count(v) FROM cs_agg_mixscale;
-- must match the non-pushdown answer exactly
SELECT sum(v), avg(v), count(v) FROM (SELECT v FROM cs_agg_mixscale OFFSET 0) s;
-- row-level quals read per-value through the column cache; a stale NI64
-- flag would misdecode the non-NI64 row groups here
SELECT count(*) FROM cs_agg_mixscale WHERE v > 1;
SELECT sum(v) FROM cs_agg_mixscale WHERE v < 1;
-- reversed insert order: the accumulator starts at dscale 20 and then
-- meets dscale-0 values too large to upscale, forcing the per-value
-- numeric fallback path
CREATE TABLE cs_agg_mixscale_rev (v numeric) USING columnstore;
INSERT INTO cs_agg_mixscale_rev
    SELECT 0.00000000000000000001 FROM generate_series(1, 100000);
INSERT INTO cs_agg_mixscale_rev
    SELECT 9000000000000000000::numeric FROM generate_series(1, 100000);
VACUUM cs_agg_mixscale_rev;
SELECT sum(v), avg(v) FROM cs_agg_mixscale_rev;
SELECT sum(v), avg(v) FROM (SELECT v FROM cs_agg_mixscale_rev OFFSET 0) s;
DROP TABLE cs_agg_mixscale;
DROP TABLE cs_agg_mixscale_rev;

-- High-scale numeric.  Finalizing SUM/AVG over an NI64-encoded column builds
-- the result from the int128 accumulator using the column's stored dscale.
-- Here dscale is 50 -- far past any small fixed buffer -- with a tiny
-- magnitude, so the conversion must size its workspace to the scale rather
-- than overrunning it.  The pushed-down answer must match the non-pushdown one.
CREATE TABLE cs_agg_hiscale (v numeric) USING columnstore;
INSERT INTO cs_agg_hiscale
    SELECT '1e-50'::numeric FROM generate_series(1, 1000);
VACUUM cs_agg_hiscale;
SELECT sum(v), avg(v), count(v) FROM cs_agg_hiscale;
SELECT sum(v), avg(v), count(v) FROM (SELECT v FROM cs_agg_hiscale OFFSET 0) s;
DROP TABLE cs_agg_hiscale;

-- Inheritance parents: rows live in append children, so aggregate,
-- ORDER BY and late-materialization pushdown must all refuse the parent
-- (the plan must contain an Append, not a single pushdown node)
CREATE TABLE cs_agg_parent (v int) USING columnstore;
CREATE TABLE cs_agg_child (extra int) INHERITS (cs_agg_parent)
    USING columnstore;
INSERT INTO cs_agg_parent VALUES (1), (2);
INSERT INTO cs_agg_child VALUES (3, 30);
SELECT count(*), sum(v) FROM cs_agg_parent;
SELECT v FROM cs_agg_parent ORDER BY v DESC LIMIT 1;
EXPLAIN (COSTS OFF) SELECT count(*), sum(v) FROM cs_agg_parent;
SELECT count(*), sum(v) FROM ONLY cs_agg_parent;
DROP TABLE cs_agg_child;
DROP TABLE cs_agg_parent;

-- Partitioned tables: aggregate over the parent covers every partition
CREATE TABLE cs_agg_part (k int, v int) PARTITION BY RANGE (k)
    USING columnstore;
CREATE TABLE cs_agg_part1 PARTITION OF cs_agg_part
    FOR VALUES FROM (0) TO (100);
CREATE TABLE cs_agg_part2 PARTITION OF cs_agg_part
    FOR VALUES FROM (100) TO (200);
INSERT INTO cs_agg_part SELECT g % 200, g FROM generate_series(1, 400) g;
VACUUM cs_agg_part1;
VACUUM cs_agg_part2;
SELECT count(*), sum(v) FROM cs_agg_part;
SELECT count(*), sum(v) FROM (SELECT v FROM cs_agg_part OFFSET 0) s;
-- FROM ONLY on the partitioned parent clears rte->inh: the parent has
-- the columnstore relam but no storage, so every pushdown (and the
-- scan provider) must refuse it rather than scan a storage-less rel
SELECT count(*), sum(v) FROM ONLY cs_agg_part;
SELECT v FROM ONLY cs_agg_part ORDER BY v LIMIT 1;
DROP TABLE cs_agg_part;

-- Aggregate edges: empty input, all-NULL columns, NULL GROUP BY keys
CREATE TABLE cs_agg_null (k int, v int, n numeric) USING columnstore;
SELECT count(*), sum(v), avg(n), min(v), max(n) FROM cs_agg_null;
INSERT INTO cs_agg_null
    SELECT NULLIF(g % 3, 2), NULLIF(g, 5),
           CASE WHEN g % 4 = 0 THEN NULL ELSE g * 0.5 END
    FROM generate_series(1, 1000) g;
VACUUM cs_agg_null;
SELECT count(*), count(v), count(n), sum(v), sum(n) FROM cs_agg_null;
SELECT count(*), count(v), count(n), sum(v), sum(n)
    FROM (SELECT v, n FROM cs_agg_null OFFSET 0) s;
SELECT k, count(*), sum(v) FROM cs_agg_null
    GROUP BY k ORDER BY k NULLS FIRST;
CREATE TABLE cs_agg_allnull (v numeric) USING columnstore;
INSERT INTO cs_agg_allnull SELECT NULL FROM generate_series(1, 500);
VACUUM cs_agg_allnull;
SELECT count(*), count(v), sum(v), avg(v) FROM cs_agg_allnull;
DROP TABLE cs_agg_null, cs_agg_allnull;

-- Parallel-aware partial ColumnstoreAggregate.  A grouped aggregate over a
-- table with more than one row group, with the parallel cost knobs forced
-- on, plans a "Parallel Custom Scan (ColumnstoreAggregate)" under a
-- Gather that the Finalize aggregate combines.  Each worker (or the leader,
-- if no worker launches) runs the grouped emit path, which deforms the
-- per-group hash tuples through grp_slot's descriptor -- the descriptor
-- must be TupleDescFinalize()d or that deform asserts/misreads.  The serial
-- grouped query uses a plain HashAggregate, so this path is reachable only
-- under parallelism.
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;
SET min_parallel_table_scan_size = 0;
SET max_parallel_workers_per_gather = 2;
CREATE TABLE cs_agg_par (g int, i4 int, i8 bigint, n numeric) USING columnstore;
INSERT INTO cs_agg_par
    SELECT i % 5, i, i::bigint * 1000, (i % 50) * 0.5
    FROM generate_series(1, 120000) i;
VACUUM cs_agg_par;
EXPLAIN (COSTS OFF)
    SELECT g, count(*), sum(i4), sum(i8), avg(i4), min(i4), max(i4)
        FROM cs_agg_par GROUP BY g;
-- Pushed-down integer aggregates combined across workers; values are
-- deterministic regardless of how many workers actually launch.
SELECT g, count(*), sum(i4), sum(i8), avg(i4), min(i4), max(i4)
    FROM cs_agg_par GROUP BY g ORDER BY g;
-- SUM/AVG(numeric) are deliberately not partial-pushed; the query still
-- runs correctly under the same parallel knobs (via the stock aggregate).
SELECT g, sum(n), avg(n) FROM cs_agg_par GROUP BY g ORDER BY g;
RESET parallel_setup_cost;
RESET parallel_tuple_cost;
RESET min_parallel_table_scan_size;
RESET max_parallel_workers_per_gather;
DROP TABLE cs_agg_par;
