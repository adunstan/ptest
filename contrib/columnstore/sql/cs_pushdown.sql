--
-- Columnstore pushdown tests: scan key pushdown, bloom filter, ON CONFLICT
--

-- ===================================================================
-- IS NULL / IS NOT NULL scan key pushdown with zone map pruning
-- ===================================================================

-- Row group 1: all NULLs in v
CREATE TABLE cs_null_push (id int, v int) USING columnstore;
INSERT INTO cs_null_push SELECT i, NULL FROM generate_series(1, 500) i;
VACUUM cs_null_push;

-- Row group 2: no NULLs in v
INSERT INTO cs_null_push SELECT i, i FROM generate_series(501, 1000) i;
VACUUM cs_null_push;

-- IS NULL: should find only RG1's 500 rows
SELECT count(*) AS null_count FROM cs_null_push WHERE v IS NULL;

-- IS NOT NULL: should find only RG2's 500 rows
SELECT count(*) AS notnull_count FROM cs_null_push WHERE v IS NOT NULL;

-- Combined with value filter
SELECT count(*) FROM cs_null_push WHERE v IS NOT NULL AND v > 900;
SELECT count(*) FROM cs_null_push WHERE v IS NULL AND id < 100;

-- Row group 3: mixed NULLs and non-NULLs
INSERT INTO cs_null_push
    SELECT i, CASE WHEN i % 3 = 0 THEN NULL ELSE i END
    FROM generate_series(1001, 1500) i;
VACUUM cs_null_push;

SELECT count(*) AS null_count FROM cs_null_push WHERE v IS NULL;
SELECT count(*) AS notnull_count FROM cs_null_push WHERE v IS NOT NULL;

DROP TABLE cs_null_push;

-- ===================================================================
-- ScalarArrayOpExpr (IN-list) scan key pushdown
-- ===================================================================
CREATE TABLE cs_in_push (id int, cat text, val int) USING columnstore;
INSERT INTO cs_in_push SELECT i, 'cat_' || (i % 50), i
    FROM generate_series(1, 2000) i;
VACUUM cs_in_push;

-- IN-list on integer column — zone map should prune row groups
SELECT count(*) FROM cs_in_push WHERE val IN (1, 100, 500, 1000, 2000);

-- IN-list on dictionary-encoded text column
SELECT count(*) FROM cs_in_push WHERE cat IN ('cat_0', 'cat_1', 'cat_49');

-- IN-list combined with range filter
SELECT count(*) FROM cs_in_push WHERE val IN (1, 100, 200) AND id < 500;

-- Verify correctness against heap
CREATE TABLE cs_in_heap (id int, cat text, val int);
INSERT INTO cs_in_heap SELECT * FROM cs_in_push;

SELECT (SELECT count(*) FROM cs_in_push WHERE val IN (10, 20, 30)) =
       (SELECT count(*) FROM cs_in_heap WHERE val IN (10, 20, 30))
    AS in_matches;

SELECT (SELECT count(*) FROM cs_in_push WHERE cat IN ('cat_5', 'cat_10')) =
       (SELECT count(*) FROM cs_in_heap WHERE cat IN ('cat_5', 'cat_10'))
    AS in_text_matches;

DROP TABLE cs_in_push;
DROP TABLE cs_in_heap;

-- ===================================================================
-- Bloom filter pushdown from hash join
-- ===================================================================

-- Helper: run a query under EXPLAIN ANALYZE and return how many rows the
-- pushed-down bloom filter removed at the columnstore scan (or -1 if no
-- filter was pushed).  Reports only a count, not timings, so the output is
-- stable.  This lets the tests below assert that scan-side pruning actually
-- fires -- a guard against a future framework change silently turning the
-- pushdown into a no-op (which would stay correct but lose the speedup).
CREATE FUNCTION cs_bloom_removed(q text) RETURNS bigint
LANGUAGE plpgsql AS $$
DECLARE
	j jsonb;
	v bigint;
BEGIN
	EXECUTE 'EXPLAIN (ANALYZE, FORMAT JSON, TIMING OFF, COSTS OFF, '
			'BUFFERS OFF, SUMMARY OFF) ' || q INTO j;
	SELECT (x ->> 'Rows Removed by Bloom Filter')::bigint INTO v
		FROM jsonb_path_query(j, 'strict $.**') x
		WHERE x ? 'Rows Removed by Bloom Filter'
		LIMIT 1;
	RETURN COALESCE(v, -1);
END $$;

-- Pushdown is serial-only (the partial path does not advertise the flag), so
-- keep these joins non-parallel for stable, pushdown-exercising plans.
SET max_parallel_workers_per_gather = 0;

CREATE TABLE cs_bloom_fact (id int, fk int, val text) USING columnstore;
INSERT INTO cs_bloom_fact SELECT i, i % 1000, 'row_' || i
    FROM generate_series(1, 5000) i;
VACUUM cs_bloom_fact;

-- Small dimension table (heap) — hash join should push bloom filter
CREATE TABLE cs_bloom_dim (id int PRIMARY KEY, label text);
INSERT INTO cs_bloom_dim SELECT i, 'dim_' || i FROM generate_series(1, 10) i;
ANALYZE cs_bloom_dim;
ANALYZE cs_bloom_fact;

-- Force hash join
SET enable_nestloop = off;
SET enable_mergejoin = off;

-- The bloom filter should substantially reduce rows scanned from cs_bloom_fact
-- (only 50 of 5000 rows match fk IN 1..10)
SELECT count(*) FROM cs_bloom_fact f JOIN cs_bloom_dim d ON f.fk = d.id;

-- Guard: the single-key filter must actually prune rows at the scan.  This
-- also exercises the framework coupling -- the scan keys filters on the join
-- hash value, so it must probe with the join operator's outer hash function;
-- if a framework change broke that mapping the probe would match nothing and
-- this would report 0 (or -1) instead of a positive count.
SELECT cs_bloom_removed($$
    SELECT count(*) FROM cs_bloom_fact f JOIN cs_bloom_dim d ON f.fk = d.id
$$) > 0 AS singlekey_pruning_fired;

-- With additional filter on the fact table
SELECT count(*) FROM cs_bloom_fact f JOIN cs_bloom_dim d ON f.fk = d.id
    WHERE f.id < 1000;

RESET enable_nestloop;
RESET enable_mergejoin;

-- Bloom filter with text (by-reference) join key
CREATE TABLE cs_bloom_txtfact (id int, cat text, val int) USING columnstore;
INSERT INTO cs_bloom_txtfact SELECT i, 'cat_' || (i % 100), i
    FROM generate_series(1, 3000) i;
VACUUM cs_bloom_txtfact;

CREATE TABLE cs_bloom_txtdim (cat text PRIMARY KEY);
INSERT INTO cs_bloom_txtdim VALUES ('cat_0'), ('cat_1'), ('cat_2');
ANALYZE cs_bloom_txtdim;
ANALYZE cs_bloom_txtfact;

SET enable_nestloop = off;
SET enable_mergejoin = off;

SELECT count(*) FROM cs_bloom_txtfact f JOIN cs_bloom_txtdim d ON f.cat = d.cat;

RESET enable_nestloop;
RESET enable_mergejoin;

DROP TABLE cs_bloom_fact;
DROP TABLE cs_bloom_dim;
DROP TABLE cs_bloom_txtfact;
DROP TABLE cs_bloom_txtdim;

-- ===================================================================
-- Multi-key bloom filter (composite join keys)
-- ===================================================================
CREATE TABLE cs_bloom_multi (a int, b int, val text) USING columnstore;
INSERT INTO cs_bloom_multi SELECT i % 100, i % 50, 'v_' || i
    FROM generate_series(1, 3000) i;
VACUUM cs_bloom_multi;

CREATE TABLE cs_bloom_multi_dim (a int, b int, label text);
INSERT INTO cs_bloom_multi_dim VALUES (1, 1, 'match1'), (2, 2, 'match2');
ANALYZE cs_bloom_multi_dim;
ANALYZE cs_bloom_multi;

SET enable_nestloop = off;
SET enable_mergejoin = off;

SELECT count(*) FROM cs_bloom_multi f
    JOIN cs_bloom_multi_dim d ON f.a = d.a AND f.b = d.b;

-- Verify correctness
SELECT (SELECT count(*) FROM cs_bloom_multi WHERE a = 1 AND b = 1) +
       (SELECT count(*) FROM cs_bloom_multi WHERE a = 2 AND b = 2)
    AS expected;

RESET enable_nestloop;
RESET enable_mergejoin;

DROP TABLE cs_bloom_multi;
DROP TABLE cs_bloom_multi_dim;

-- ===================================================================
-- Multi-key per-key filter: false-accept correctness
--
-- For a multi-key join the scan uses one bloom filter per key column (the
-- combined-hash filter cannot be tested against a single column's
-- dictionary).  Per-key filters are strictly weaker: a row whose columns
-- each appear somewhere on the build side, but not together as a tuple,
-- passes every per-key filter.  Such a row must survive scan-side pruning
-- and be rejected by the real hash join, so the result is exactly the true
-- matches -- per-key filters prune row groups, they never stand in for the
-- join's own matching.
-- ===================================================================
CREATE TABLE cs_bloom_fa (a int, b int, val text) USING columnstore;
-- (1,1) and (20,20) are real matches; (1,20) and (20,1) are "cross" rows
-- that pass the per-key a-filter {1,20} and b-filter {1,20} but match no
-- build tuple.  The rest is non-matching noise so pruning has work to do.
INSERT INTO cs_bloom_fa VALUES
    (1, 1, 'hit'), (20, 20, 'hit'),
    (1, 20, 'cross'), (20, 1, 'cross');
INSERT INTO cs_bloom_fa
    SELECT 1000 + i, 2000 + i, 'noise' FROM generate_series(1, 5000) i;
VACUUM cs_bloom_fa;

CREATE TABLE cs_bloom_fa_dim (a int, b int);
INSERT INTO cs_bloom_fa_dim VALUES (1, 1), (20, 20);
ANALYZE cs_bloom_fa;
ANALYZE cs_bloom_fa_dim;

SET enable_nestloop = off;
SET enable_mergejoin = off;

-- Exactly the two true matches; the cross rows must not be counted.
SELECT count(*) AS true_matches
    FROM cs_bloom_fa f JOIN cs_bloom_fa_dim d ON f.a = d.a AND f.b = d.b;
SELECT f.a, f.b, f.val
    FROM cs_bloom_fa f JOIN cs_bloom_fa_dim d ON f.a = d.a AND f.b = d.b
    ORDER BY f.a;
-- And the per-key filters did fire (removed the noise rows at the scan).
SELECT cs_bloom_removed($$
    SELECT count(*) FROM cs_bloom_fa f
        JOIN cs_bloom_fa_dim d ON f.a = d.a AND f.b = d.b
$$) > 0 AS perkey_pruning_fired;

RESET enable_nestloop;
RESET enable_mergejoin;

DROP TABLE cs_bloom_fa;
DROP TABLE cs_bloom_fa_dim;

RESET max_parallel_workers_per_gather;
DROP FUNCTION cs_bloom_removed(text);

-- ===================================================================
-- ON CONFLICT (speculative insertion)
-- ===================================================================
CREATE TABLE cs_conflict (id int PRIMARY KEY, val text) USING columnstore;
INSERT INTO cs_conflict VALUES (1, 'first'), (2, 'second');

-- DO UPDATE: should update val for id=1
INSERT INTO cs_conflict VALUES (1, 'conflict')
    ON CONFLICT (id) DO UPDATE SET val = 'updated';
SELECT * FROM cs_conflict WHERE id = 1;

-- DO NOTHING: should silently skip
INSERT INTO cs_conflict VALUES (2, 'ignored')
    ON CONFLICT (id) DO NOTHING;
SELECT * FROM cs_conflict WHERE id = 2;

-- Non-conflicting insert should work normally
INSERT INTO cs_conflict VALUES (3, 'third')
    ON CONFLICT (id) DO NOTHING;
SELECT * FROM cs_conflict WHERE id = 3;

-- Batch ON CONFLICT
INSERT INTO cs_conflict VALUES (1, 'batch1'), (2, 'batch2'), (4, 'new4')
    ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val;
SELECT * FROM cs_conflict ORDER BY id;

-- ON CONFLICT with columnar rows (freeze first, then conflict)
VACUUM cs_conflict;
INSERT INTO cs_conflict VALUES (1, 'post_freeze')
    ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val;
SELECT * FROM cs_conflict WHERE id = 1;

DROP TABLE cs_conflict;

-- ON CONFLICT with expression index
CREATE TABLE cs_conflict_expr (id int, val text, lowval text GENERATED ALWAYS AS (lower(val)) STORED) USING columnstore;
CREATE UNIQUE INDEX ON cs_conflict_expr (lowval);
INSERT INTO cs_conflict_expr (id, val) VALUES (1, 'Hello');
INSERT INTO cs_conflict_expr (id, val) VALUES (2, 'HELLO')
    ON CONFLICT (lowval) DO UPDATE SET val = EXCLUDED.val;
SELECT id, val FROM cs_conflict_expr ORDER BY id;

DROP TABLE cs_conflict_expr;

-- ===================================================================
-- Qual pushdown safety: cross-type IN-lists, COLLATE overrides,
-- byte-image dictionary twins, composite IS NULL
-- ===================================================================
-- Prefer an ICU collation (exercises the sort-key zonemap and the
-- attcollation pushdown-safety path), but fall back to "C" where ICU is
-- unavailable -- notably a SQL_ASCII database, where ICU collations cannot be
-- used at all.  The column data below is pure ASCII, so the query results are
-- identical either way; only which pushdown path runs differs.
SELECT CASE
         WHEN current_setting('server_encoding') <> 'SQL_ASCII'
              AND EXISTS (SELECT 1 FROM pg_collation WHERE collname = 'en-x-icu')
         THEN 'en-x-icu' ELSE 'C'
       END AS qual_coll \gset
CREATE TABLE cs_qual_safety (
    d date, t text COLLATE :"qual_coll", f float8, r point,
    comp_a int, comp_b int) USING columnstore;
INSERT INTO cs_qual_safety
    SELECT '2026-01-01'::date + (i % 50),
           chr(65 + i % 26) || 'tail',
           CASE WHEN i % 100 = 0 THEN -0.0 ELSE (i % 7)::float8 END,
           point(i, i), i, CASE WHEN i % 3 = 0 THEN NULL ELSE i END
    FROM generate_series(1, 700) i;
VACUUM cs_qual_safety;
-- cross-type IN-list (date vs timestamp elements): stays in residual
-- qual, result must match the heap answer
SELECT count(*) FROM cs_qual_safety
    WHERE d = ANY (ARRAY['2026-01-10'::timestamp, '2026-01-20'::timestamp]);
-- explicit COLLATE override must not be pushed under attcollation
SELECT count(*) FROM cs_qual_safety WHERE t > 'Y' COLLATE "C";
SELECT count(*) FROM cs_qual_safety WHERE t > 'Y';
-- -0.0 equals 0.0 by btree order but is a distinct dictionary entry
SELECT count(*) FROM cs_qual_safety WHERE f = 0.0;
DROP TABLE cs_qual_safety;
