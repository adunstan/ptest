--
-- Columnstore dictionary encoding edge cases
--

-- ===================================================================
-- Index width boundary: 1-byte vs 2-byte dictionary index
-- ===================================================================

-- 254 distinct values: should use 1-byte index (254 + possible NULL < 256)
CREATE TABLE cs_dict_254 (id int, v text) USING columnstore;
INSERT INTO cs_dict_254 SELECT i, 'val_' || lpad((i % 254)::text, 4, '0')
    FROM generate_series(1, 1000) i;
VACUUM cs_dict_254;
SELECT count(*) AS cnt FROM cs_dict_254;
SELECT count(DISTINCT v) AS ndistinct FROM cs_dict_254;
-- Verify first and last distinct values
SELECT DISTINCT v FROM cs_dict_254 ORDER BY v LIMIT 3;
SELECT DISTINCT v FROM cs_dict_254 ORDER BY v DESC LIMIT 3;
-- Point lookups
SELECT count(*) FROM cs_dict_254 WHERE v = 'val_0000';
SELECT count(*) FROM cs_dict_254 WHERE v = 'val_0253';
DROP TABLE cs_dict_254;

-- 256 distinct values: forces 2-byte index (256 > 255)
CREATE TABLE cs_dict_256 (id int, v text) USING columnstore;
INSERT INTO cs_dict_256 SELECT i, 'val_' || lpad((i % 256)::text, 4, '0')
    FROM generate_series(1, 1024) i;
VACUUM cs_dict_256;
SELECT count(*) AS cnt FROM cs_dict_256;
SELECT count(DISTINCT v) AS ndistinct FROM cs_dict_256;
SELECT DISTINCT v FROM cs_dict_256 ORDER BY v LIMIT 3;
SELECT DISTINCT v FROM cs_dict_256 ORDER BY v DESC LIMIT 3;
DROP TABLE cs_dict_256;

-- 255 distinct values with NULLs: NULL uses a sentinel index
-- 255 + NULL sentinel = 256 entries → forces 2-byte index
CREATE TABLE cs_dict_255_null (id int, v text) USING columnstore;
INSERT INTO cs_dict_255_null SELECT i,
    CASE WHEN i % 10 = 0 THEN NULL ELSE 'val_' || lpad((i % 255)::text, 4, '0') END
    FROM generate_series(1, 1000) i;
VACUUM cs_dict_255_null;
SELECT count(*) AS total, count(v) AS nonnull FROM cs_dict_255_null;
SELECT count(DISTINCT v) AS ndistinct FROM cs_dict_255_null;
-- Verify NULLs survived
SELECT count(*) AS null_count FROM cs_dict_255_null WHERE v IS NULL;
DROP TABLE cs_dict_255_null;

-- ===================================================================
-- Varlena with NULLs: text column, 50 distinct strings, 20% NULLs
-- ===================================================================
CREATE TABLE cs_dict_varlena (id int, v text) USING columnstore;
INSERT INTO cs_dict_varlena SELECT i,
    CASE WHEN i % 5 = 0 THEN NULL
         ELSE 'category_' || lpad((i % 50)::text, 3, '0')
    END FROM generate_series(1, 1000) i;
VACUUM cs_dict_varlena;
SELECT count(*) AS total, count(v) AS nonnull FROM cs_dict_varlena;
SELECT count(DISTINCT v) AS ndistinct FROM cs_dict_varlena;
-- Verify NULL and non-NULL values
SELECT id, v FROM cs_dict_varlena WHERE id IN (1, 5, 10, 50) ORDER BY id;
-- Aggregate on dictionary column
SELECT min(v), max(v) FROM cs_dict_varlena;
DROP TABLE cs_dict_varlena;

-- ===================================================================
-- Dictionary with fixed-width by-value types (int): should NOT use dict
-- (FOR encoding is preferred for integers)
-- ===================================================================
CREATE TABLE cs_dict_int (id int, v int) USING columnstore;
INSERT INTO cs_dict_int SELECT i, i % 10 FROM generate_series(1, 500) i;
VACUUM cs_dict_int;
-- Should still work correctly regardless of encoding chosen
SELECT count(*) AS cnt, count(DISTINCT v) AS ndistinct FROM cs_dict_int;
SELECT min(v), max(v) FROM cs_dict_int;
DROP TABLE cs_dict_int;

-- ===================================================================
-- Dictionary with long varlena values
-- ===================================================================
CREATE TABLE cs_dict_long (id int, v text) USING columnstore;
INSERT INTO cs_dict_long SELECT i,
    'long_prefix_' || repeat('x', 100) || '_suffix_' || lpad((i % 30)::text, 3, '0')
    FROM generate_series(1, 500) i;
VACUUM cs_dict_long;
SELECT count(*) AS cnt FROM cs_dict_long;
SELECT count(DISTINCT v) AS ndistinct FROM cs_dict_long;
-- Verify long values survive round-trip
SELECT id, length(v) AS vlen, left(v, 15) AS prefix, right(v, 10) AS suffix
    FROM cs_dict_long WHERE id IN (1, 30, 500) ORDER BY id;
DROP TABLE cs_dict_long;

-- ===================================================================
-- Too many distinct values: should fall back to plain/LZ4
-- ===================================================================
CREATE TABLE cs_dict_toomany (id int, v text) USING columnstore;
-- All unique strings → more than 25% distinct → no dictionary
INSERT INTO cs_dict_toomany SELECT i, md5(i::text) FROM generate_series(1, 500) i;
VACUUM cs_dict_toomany;
SELECT count(*) AS cnt FROM cs_dict_toomany;
-- Verify data integrity
SELECT id, v FROM cs_dict_toomany WHERE id = 1;
SELECT id, v FROM cs_dict_toomany WHERE id = 500;
DROP TABLE cs_dict_toomany;
