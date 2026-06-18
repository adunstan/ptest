--
-- Columnstore encoding stress tests: FOR, NI64, NI64+FOR stacking
--

-- ===================================================================
-- FOR encoding: integer boundary conditions
-- ===================================================================

-- All-same values (range=0, bits_per_value=0)
CREATE TABLE cs_for_same (a int, b bigint, c smallint) USING columnstore;
INSERT INTO cs_for_same SELECT 42, 9999999999::bigint, 7::smallint
    FROM generate_series(1, 500) i;
VACUUM cs_for_same;
SELECT count(*) AS cnt, min(a), max(a), min(b), max(b), min(c), max(c)
    FROM cs_for_same;
DROP TABLE cs_for_same;

-- int2 near MIN/MAX boundaries
CREATE TABLE cs_for_i2 (id int, v smallint) USING columnstore;
INSERT INTO cs_for_i2 VALUES (1, -32768), (2, -32767), (3, 0), (4, 32766), (5, 32767);
-- Pad to trigger freeze
INSERT INTO cs_for_i2 SELECT i, (i % 100)::smallint FROM generate_series(6, 500) i;
VACUUM cs_for_i2;
-- Verify boundary values survive encode/decode
SELECT id, v FROM cs_for_i2 WHERE id <= 5 ORDER BY id;
DROP TABLE cs_for_i2;

-- int4 near MIN/MAX boundaries
CREATE TABLE cs_for_i4 (id int, v int) USING columnstore;
INSERT INTO cs_for_i4 VALUES (1, -2147483648), (2, -2147483647),
    (3, 0), (4, 2147483646), (5, 2147483647);
INSERT INTO cs_for_i4 SELECT i, (i % 1000) FROM generate_series(6, 500) i;
VACUUM cs_for_i4;
SELECT id, v FROM cs_for_i4 WHERE id <= 5 ORDER BY id;
DROP TABLE cs_for_i4;

-- int8 near MIN/MAX boundaries
CREATE TABLE cs_for_i8 (id int, v bigint) USING columnstore;
INSERT INTO cs_for_i8 VALUES (1, -9223372036854775808),
    (2, -9223372036854775807), (3, 0),
    (4, 9223372036854775806), (5, 9223372036854775807);
INSERT INTO cs_for_i8 SELECT i, i::bigint FROM generate_series(6, 500) i;
VACUUM cs_for_i8;
SELECT id, v FROM cs_for_i8 WHERE id <= 5 ORDER BY id;
DROP TABLE cs_for_i8;

-- FOR with unsigned interpretation: mix extreme negative + positive int4
-- Range wraps around in unsigned representation
CREATE TABLE cs_for_unsigned (id int, v int) USING columnstore;
INSERT INTO cs_for_unsigned VALUES (1, -2147483648), (2, 2147483647);
INSERT INTO cs_for_unsigned SELECT i, (i % 100) - 50 FROM generate_series(3, 500) i;
VACUUM cs_for_unsigned;
SELECT id, v FROM cs_for_unsigned WHERE id <= 2 ORDER BY id;
-- Verify full range
SELECT min(v), max(v) FROM cs_for_unsigned;
DROP TABLE cs_for_unsigned;

-- FOR with narrow range (few bits per value)
CREATE TABLE cs_for_narrow (id int, v int) USING columnstore;
INSERT INTO cs_for_narrow SELECT i, 1000000 + (i % 4) FROM generate_series(1, 500) i;
VACUUM cs_for_narrow;
-- Range is only 0..3 = 2 bits per value
SELECT count(DISTINCT v), min(v), max(v) FROM cs_for_narrow;
-- Spot check
SELECT id, v FROM cs_for_narrow WHERE id = 1;
SELECT id, v FROM cs_for_narrow WHERE id = 500;
DROP TABLE cs_for_narrow;

-- FOR with NULLs interspersed
CREATE TABLE cs_for_nulls (id int, v int) USING columnstore;
INSERT INTO cs_for_nulls SELECT i, CASE WHEN i % 3 = 0 THEN NULL ELSE i END
    FROM generate_series(1, 500) i;
VACUUM cs_for_nulls;
SELECT count(*) AS total, count(v) AS nonnull, count(*) - count(v) AS nulls
    FROM cs_for_nulls;
-- Verify specific NULL/non-NULL rows
SELECT id, v FROM cs_for_nulls WHERE id IN (1, 2, 3, 4, 5, 6) ORDER BY id;
DROP TABLE cs_for_nulls;

-- ===================================================================
-- NI64 encoding: numeric to scaled int64
-- ===================================================================

-- Uniform dscale, narrow values (should trigger NI64)
CREATE TABLE cs_ni64_basic (id int, v numeric(10,2)) USING columnstore;
INSERT INTO cs_ni64_basic SELECT i, (i * 1.23)::numeric(10,2)
    FROM generate_series(1, 500) i;
VACUUM cs_ni64_basic;
SELECT count(*) AS cnt FROM cs_ni64_basic;
SELECT id, v FROM cs_ni64_basic WHERE id IN (1, 100, 250, 500) ORDER BY id;
-- Aggregate correctness
SELECT sum(v), avg(v), min(v), max(v) FROM cs_ni64_basic;
DROP TABLE cs_ni64_basic;

-- Mixed dscale should prevent NI64 (falls back to plain/dict)
CREATE TABLE cs_ni64_mixed (id int, v numeric) USING columnstore;
INSERT INTO cs_ni64_mixed SELECT i,
    CASE WHEN i % 2 = 0 THEN (i * 1.5)::numeric(10,2)
         ELSE (i * 1.555)::numeric(10,3)
    END FROM generate_series(1, 500) i;
VACUUM cs_ni64_mixed;
-- Should still return correct data even without NI64
SELECT count(*) AS cnt FROM cs_ni64_mixed;
SELECT id, v FROM cs_ni64_mixed WHERE id IN (1, 2, 3, 4) ORDER BY id;
DROP TABLE cs_ni64_mixed;

-- NI64 with near-overflow values (large numeric that fits in int64)
CREATE TABLE cs_ni64_large (id int, v numeric) USING columnstore;
INSERT INTO cs_ni64_large VALUES (1, 999999999999999999);   -- near INT64_MAX
INSERT INTO cs_ni64_large VALUES (2, -999999999999999999);  -- near INT64_MIN
INSERT INTO cs_ni64_large VALUES (3, 0);
INSERT INTO cs_ni64_large SELECT i, (i * 1000000)::numeric
    FROM generate_series(4, 500) i;
VACUUM cs_ni64_large;
SELECT id, v FROM cs_ni64_large WHERE id <= 3 ORDER BY id;
DROP TABLE cs_ni64_large;

-- NI64 with NaN (should prevent NI64 encoding)
CREATE TABLE cs_ni64_nan (id int, v numeric) USING columnstore;
INSERT INTO cs_ni64_nan SELECT i, CASE WHEN i = 250 THEN 'NaN'::numeric
    ELSE (i * 1.5)::numeric(10,2) END
    FROM generate_series(1, 500) i;
VACUUM cs_ni64_nan;
SELECT count(*) AS cnt FROM cs_ni64_nan;
SELECT id, v FROM cs_ni64_nan WHERE id = 250;
SELECT id, v FROM cs_ni64_nan WHERE id = 1;
DROP TABLE cs_ni64_nan;

-- NI64 with zero values
CREATE TABLE cs_ni64_zero (id int, v numeric(10,2)) USING columnstore;
INSERT INTO cs_ni64_zero SELECT i,
    CASE WHEN i % 2 = 0 THEN 0.00::numeric(10,2) ELSE (i * 0.01)::numeric(10,2) END
    FROM generate_series(1, 500) i;
VACUUM cs_ni64_zero;
SELECT count(*) AS cnt, count(v) AS nonnull FROM cs_ni64_zero;
SELECT id, v FROM cs_ni64_zero WHERE id IN (1, 2, 3, 4) ORDER BY id;
DROP TABLE cs_ni64_zero;

-- NI64 with NULLs
CREATE TABLE cs_ni64_nulls (id int, v numeric(10,2)) USING columnstore;
INSERT INTO cs_ni64_nulls SELECT i,
    CASE WHEN i % 5 = 0 THEN NULL ELSE (i * 1.11)::numeric(10,2) END
    FROM generate_series(1, 500) i;
VACUUM cs_ni64_nulls;
SELECT count(*) AS total, count(v) AS nonnull FROM cs_ni64_nulls;
SELECT id, v FROM cs_ni64_nulls WHERE id IN (4, 5, 6, 10, 11) ORDER BY id;
DROP TABLE cs_ni64_nulls;

-- ===================================================================
-- NI64 + FOR stacking: numeric(10,2) with narrow range
-- ===================================================================
CREATE TABLE cs_ni64_for (id int, v numeric(10,2)) USING columnstore;
-- Narrow range: 100.00 to 110.00 should trigger NI64 then FOR on the int64s
INSERT INTO cs_ni64_for SELECT i, (100.00 + (i % 11) * 1.00)::numeric(10,2)
    FROM generate_series(1, 500) i;
VACUUM cs_ni64_for;
SELECT count(*) AS cnt FROM cs_ni64_for;
SELECT count(DISTINCT v) AS ndistinct FROM cs_ni64_for;
SELECT id, v FROM cs_ni64_for WHERE id IN (1, 2, 11, 12) ORDER BY id;
-- Aggregates should be exact
SELECT sum(v), min(v), max(v) FROM cs_ni64_for;
DROP TABLE cs_ni64_for;

-- ===================================================================
-- Encoding combos: single table with columns designed to trigger each path
-- ===================================================================
CREATE TABLE cs_enc_combos (
    id int,
    -- plain int (no FOR if range too wide)
    i_wide int,
    -- FOR-encoded int (narrow range)
    i_narrow int,
    -- NI64 numeric
    n_ni64 numeric(10,2),
    -- NI64+FOR numeric (narrow range)
    n_ni64_for numeric(10,2),
    -- dictionary text (few distinct values)
    t_dict text,
    -- plain text (too many distinct values for dict)
    t_plain text,
    -- numeric with NaN (prevents NI64)
    n_nan numeric
) USING columnstore;

INSERT INTO cs_enc_combos SELECT
    i,
    -- i_wide: full int4 range prevents FOR
    CASE WHEN i = 1 THEN -2000000000 WHEN i = 2 THEN 2000000000 ELSE i END,
    -- i_narrow: only 0..9 range, ideal for FOR
    i % 10,
    -- n_ni64: uniform dscale, triggers NI64
    (i * 1.23)::numeric(10,2),
    -- n_ni64_for: narrow numeric range, triggers NI64+FOR
    (100.00 + (i % 5))::numeric(10,2),
    -- t_dict: 10 distinct values, should trigger dictionary
    'category_' || (i % 10),
    -- t_plain: all unique, no dictionary
    'unique_' || i || '_' || md5(i::text),
    -- n_nan: row 250 is NaN, prevents NI64
    CASE WHEN i = 250 THEN 'NaN'::numeric ELSE (i * 0.5)::numeric(10,1) END
FROM generate_series(1, 500) i;

VACUUM cs_enc_combos;

-- Verify all columns survived encode/decode
SELECT count(*) AS cnt FROM cs_enc_combos;

-- Spot check boundary values
SELECT id, i_wide, i_narrow, n_ni64, n_ni64_for, t_dict, left(t_plain, 20) AS t_plain_prefix, n_nan
    FROM cs_enc_combos WHERE id = 1;
SELECT id, i_wide, i_narrow, n_ni64, n_ni64_for, t_dict, left(t_plain, 20) AS t_plain_prefix, n_nan
    FROM cs_enc_combos WHERE id = 250;
SELECT id, i_wide, i_narrow, n_ni64, n_ni64_for, t_dict, left(t_plain, 20) AS t_plain_prefix, n_nan
    FROM cs_enc_combos WHERE id = 500;

-- Verify aggregates on FOR-encoded column
SELECT min(i_narrow), max(i_narrow), count(DISTINCT i_narrow) FROM cs_enc_combos;

-- Verify aggregates on NI64 column
SELECT sum(n_ni64), min(n_ni64), max(n_ni64) FROM cs_enc_combos;

-- Verify dictionary column
SELECT count(DISTINCT t_dict) FROM cs_enc_combos;

-- Cross-check: heap comparison for a subset
CREATE TABLE cs_enc_heap AS SELECT * FROM cs_enc_combos WHERE id <= 50;
SELECT count(*) FROM (
    SELECT id, i_wide, i_narrow, n_ni64, n_ni64_for, t_dict, t_plain, n_nan
    FROM cs_enc_combos WHERE id <= 50
    EXCEPT
    SELECT id, i_wide, i_narrow, n_ni64, n_ni64_for, t_dict, t_plain, n_nan
    FROM cs_enc_heap
) diff;
DROP TABLE cs_enc_heap;
DROP TABLE cs_enc_combos;

-- ===================================================================
-- NI64 numeric encoding: mixed dscales and whole numbers
--
-- Each row group picks its own dscale for an unconstrained numeric
-- column, so sums must rescale when row groups at different scales
-- meet; whole-number groups encode at dscale 0, which is a valid NI64
-- encoding, not "no encoding".
-- ===================================================================
CREATE TABLE cs_ni64_mix (id int, n numeric) USING columnstore;
-- first row group: dscale 2
INSERT INTO cs_ni64_mix SELECT i, (i || '.25')::numeric FROM generate_series(1, 500) i;
VACUUM cs_ni64_mix;
-- second row group: dscale 3
INSERT INTO cs_ni64_mix SELECT i, (i || '.125')::numeric FROM generate_series(501, 1000) i;
VACUUM cs_ni64_mix;
-- third row group: whole numbers, dscale 0
INSERT INTO cs_ni64_mix SELECT i, i::numeric FROM generate_series(1001, 1500) i;
VACUUM cs_ni64_mix;
SELECT sum(n), avg(n), count(n) FROM cs_ni64_mix;
-- same numbers through the row-at-a-time path for comparison
SELECT sum(n), avg(n), count(n) FROM (SELECT n FROM cs_ni64_mix OFFSET 0) s;
-- pushed qual and per-row fetch on a whole-number (dscale 0) group
SELECT id, n FROM cs_ni64_mix WHERE n = 1250 ORDER BY id;
SELECT id, n FROM cs_ni64_mix WHERE n = 750.125 ORDER BY id;
DROP TABLE cs_ni64_mix;

-- FOR bit-unpacking on top of base compression: cycling numeric(8,2)
-- values over ~100k rows produce NI64+FOR packed data that is large and
-- repetitive enough for LZ4/PGLZ to stack on top, so the FOR decoder
-- runs over a freshly decompressed buffer rather than page data.  The
-- decoder's 64-bit shift buffer may fetch a few bytes past the packed
-- input while draining the final rows, so reading the row group's tail
-- must stay within the buffer's padding.
CREATE TABLE cs_enc_forlz4 (n numeric(8,2)) USING columnstore;
INSERT INTO cs_enc_forlz4
    SELECT (g % 1000)::numeric / 100 FROM generate_series(1, 99892) g;
VACUUM cs_enc_forlz4;
SELECT count(*), sum(n), min(n), max(n) FROM cs_enc_forlz4;
SELECT n FROM cs_enc_forlz4 ORDER BY ctid DESC LIMIT 3;
DROP TABLE cs_enc_forlz4;

-- Gorilla XOR float compression with full-width windows: a sign flip
-- combined with a low mantissa bit makes the XOR of consecutive values
-- span all 64 bits (no leading or trailing zeros), so the window length
-- does not fit the 6-bit field and the payload exceeds what the bit
-- buffer can stage in one piece.  fm exercises the 58..63-bit band, f4
-- the float4 path.  Mostly-constant data keeps Gorilla profitable.
CREATE TABLE cs_enc_gorilla (g int, f float8, f4 float4, fm float8)
    USING columnstore;
INSERT INTO cs_enc_gorilla
    SELECT g,
           CASE WHEN g % 50 = 0 THEN -(1.0 + 2.0^(-52)) ELSE 1.0 END,
           CASE WHEN g % 50 = 0 THEN -(1.0 + 2.0^(-23))::float4
                ELSE 1.0::float4 END,
           CASE WHEN g % 50 = 0 THEN -(1.0 + 2.0^(-46)) ELSE 1.0 END
    FROM generate_series(1, 99892) g;
VACUUM cs_enc_gorilla;
SELECT count(*) FILTER (WHERE f <> CASE WHEN g % 50 = 0
                        THEN -(1.0 + 2.0^(-52)) ELSE 1.0 END) AS f8_wrong,
       count(*) FILTER (WHERE f4 <> CASE WHEN g % 50 = 0
                        THEN -(1.0 + 2.0^(-23))::float4
                        ELSE 1.0::float4 END) AS f4_wrong,
       count(*) FILTER (WHERE fm <> CASE WHEN g % 50 = 0
                        THEN -(1.0 + 2.0^(-46)) ELSE 1.0 END) AS fm_wrong,
       count(*) AS total
FROM cs_enc_gorilla;
DROP TABLE cs_enc_gorilla;

-- Integer extremes: both int8 extremes in one row group overflow any
-- frame-of-reference/delta range computation, forcing a fallback that
-- must still round-trip exactly; int4/int2 extremes likewise
CREATE TABLE cs_enc_extreme (b int8, i int4, s int2) USING columnstore;
INSERT INTO cs_enc_extreme VALUES
    (-9223372036854775808, -2147483648, -32768),
    ( 9223372036854775807,  2147483647,  32767),
    (0, 0, 0), (-1, -1, -1), (1, 1, 1);
INSERT INTO cs_enc_extreme SELECT 0, 0, 0 FROM generate_series(1, 300);
VACUUM cs_enc_extreme;
SELECT min(b), max(b), min(i), max(i), min(s), max(s), count(*)
    FROM cs_enc_extreme;
SELECT count(*) FROM cs_enc_extreme WHERE b = -9223372036854775808;
DROP TABLE cs_enc_extreme;

-- Float specials through the Gorilla XOR path: NaN, infinities,
-- negative zero and the smallest denormal are raw bit patterns and must
-- survive compaction bit-exactly
CREATE TABLE cs_enc_special (f float8, g float4) USING columnstore;
INSERT INTO cs_enc_special VALUES
    ('NaN', 'NaN'), ('Infinity', 'Infinity'), ('-Infinity', '-Infinity'),
    ('-0', '-0'), (0, 0), (4.9e-324, 1.4e-45);
INSERT INTO cs_enc_special SELECT 1.0, 1.0 FROM generate_series(1, 300);
VACUUM cs_enc_special;
SELECT count(*) FILTER (WHERE f = 'Infinity') AS pinf,
       count(*) FILTER (WHERE f = '-Infinity') AS ninf,
       count(*) FILTER (WHERE f::text = 'NaN') AS nan,
       count(*) FILTER (WHERE f::text = '-0') AS negzero,
       count(*) FILTER (WHERE f = 4.9e-324) AS denorm,
       count(*) FILTER (WHERE g::text = 'NaN') AS nan4,
       count(*) FILTER (WHERE g = '1.4e-45'::float4) AS denorm4
    FROM cs_enc_special;
DROP TABLE cs_enc_special;

-- Numeric dscales beyond the int64 fast path: dscale 100 values whose
-- scaled magnitude still fits an int64, and the dscale 63/64 boundary of
-- the short numeric header; varying values defeat RLE so the encoder's
-- own NI64 acceptance logic is what is being tested
CREATE TABLE cs_enc_dscale (a numeric, b numeric, c numeric) USING columnstore;
INSERT INTO cs_enc_dscale
    SELECT g * 1e-100::numeric,
           g * 1e-63::numeric,
           g * 1e-64::numeric
    FROM generate_series(1, 500) g;
VACUUM cs_enc_dscale;
SELECT count(*) FILTER (WHERE a <> g * 1e-100::numeric) AS a_wrong,
       count(*) FILTER (WHERE b <> g * 1e-63::numeric) AS b_wrong,
       count(*) FILTER (WHERE c <> g * 1e-64::numeric) AS c_wrong,
       count(*) AS total
    FROM (SELECT a, b, c, row_number() OVER (ORDER BY a) AS g FROM cs_enc_dscale) s;
SELECT max(a), max(b), max(c) FROM cs_enc_dscale;
DROP TABLE cs_enc_dscale;

-- Dictionary encoding cardinality edges: > 256 distinct values need
-- 2-byte dictionary indexes, and past 65536 distinct values the
-- encoder abandons dictionary encoding entirely (the rejection path)
CREATE TABLE cs_enc_dictw (t1 text, t2 text) USING columnstore;
INSERT INTO cs_enc_dictw
    SELECT 'v' || (g % 300), 'w' || (g % 70000) FROM generate_series(1, 100000) g;
VACUUM cs_enc_dictw;
SELECT count(DISTINCT t1) AS d1, count(DISTINCT t2) AS d2, count(*) FROM cs_enc_dictw;
SELECT count(*) FROM cs_enc_dictw WHERE t1 = 'v7';
SELECT count(*) FROM cs_enc_dictw WHERE t2 = 'w69999';
DROP TABLE cs_enc_dictw;

-- Exotic column types round-trip: enum, composite, array, domain, jsonb
CREATE TYPE cs_enc_color AS ENUM ('red', 'green', 'blue');
CREATE TYPE cs_enc_pair AS (x int, y text);
CREATE DOMAIN cs_enc_posint AS int CHECK (VALUE > 0);
CREATE TABLE cs_enc_exotic (c cs_enc_color, p cs_enc_pair, a int[],
                            d cs_enc_posint, j jsonb) USING columnstore;
INSERT INTO cs_enc_exotic
    SELECT (ARRAY['red', 'green', 'blue'])[1 + g % 3]::cs_enc_color,
           row(g, 't' || g)::cs_enc_pair,
           ARRAY[g, g + 1, g + 2],
           g + 1,
           jsonb_build_object('k', g)
    FROM generate_series(1, 500) g;
VACUUM cs_enc_exotic;
SELECT count(*) FILTER (WHERE c = 'green') AS greens,
       count(*) FILTER (WHERE (p).x = 7) AS px7,
       count(*) FILTER (WHERE a[2] = 100) AS a2,
       sum(d) AS dsum,
       count(*) FILTER (WHERE j->>'k' = '33') AS j33
    FROM cs_enc_exotic;
DROP TABLE cs_enc_exotic;
DROP TYPE cs_enc_color, cs_enc_pair;
DROP DOMAIN cs_enc_posint;

-- Gorilla float encoding: high-entropy float8 values exercise the XOR
-- "new window" path (the widest per-value output).  The encoder must size its
-- output buffer for that worst case (and bail to uncompressed rather than
-- overrun).  hashint4 gives deterministic pseudo-random values; verify they
-- round-trip through VACUUM compaction and that scanned aggregates agree.
CREATE TABLE cs_gorilla_float (id int, v float8) USING columnstore;
INSERT INTO cs_gorilla_float
    SELECT i, hashint4(i)::float8 / 7.0 FROM generate_series(1, 5000) i;
VACUUM cs_gorilla_float;
SELECT count(*) = 5000 AS n_ok, count(DISTINCT v) = 5000 AS all_distinct
    FROM cs_gorilla_float;
SELECT id, v FROM cs_gorilla_float WHERE id IN (1, 2500, 5000) ORDER BY id;
DROP TABLE cs_gorilla_float;
