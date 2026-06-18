--
-- Columnstore index scan and index-only scan tests
--

-- ===================================================================
-- Index scan across multiple row groups
-- ===================================================================
CREATE TABLE cs_idx (id int, val text, num int) USING columnstore;
INSERT INTO cs_idx SELECT i, 'rg1_' || i, i * 10 FROM generate_series(1, 500) i;
VACUUM cs_idx;
INSERT INTO cs_idx SELECT i, 'rg2_' || i, i * 10 FROM generate_series(501, 1000) i;
VACUUM cs_idx;
-- Delta rows
INSERT INTO cs_idx SELECT i, 'delta_' || i, i * 10 FROM generate_series(1001, 1100) i;

CREATE INDEX cs_idx_id ON cs_idx (id);
ANALYZE cs_idx;

SET enable_seqscan = off;

-- Point lookups in each store
SELECT id, val FROM cs_idx WHERE id = 1;
SELECT id, val FROM cs_idx WHERE id = 500;
SELECT id, val FROM cs_idx WHERE id = 750;
SELECT id, val FROM cs_idx WHERE id = 1050;

-- Range scan spanning row groups
SELECT count(*) FROM cs_idx WHERE id BETWEEN 450 AND 550;

-- Range scan with deletions (re-enable seqscan for the DELETE)
RESET enable_seqscan;
DELETE FROM cs_idx WHERE id BETWEEN 490 AND 510;
SET enable_seqscan = off;
SELECT count(*) FROM cs_idx WHERE id BETWEEN 480 AND 520;

RESET enable_seqscan;

-- Partial index (predicate must be evaluated during build)
CREATE INDEX cs_idx_partial ON cs_idx (id) WHERE id > 500;
SET enable_seqscan = off;
SELECT count(*) FROM cs_idx WHERE id > 500 AND id <= 600;
RESET enable_seqscan;

DROP TABLE cs_idx;

-- ===================================================================
-- Index-only scan on frozen row groups
-- ===================================================================
CREATE TABLE cs_ios (id int, val int) USING columnstore;
INSERT INTO cs_ios SELECT i, i * 100 FROM generate_series(1, 1000) i;
VACUUM cs_ios;

CREATE INDEX cs_ios_id_val ON cs_ios (id, val);
ANALYZE cs_ios;

SET enable_seqscan = off;
SET enable_indexscan = off;

-- Force index-only scan
EXPLAIN (COSTS OFF) SELECT id, val FROM cs_ios WHERE id = 500;
SELECT id, val FROM cs_ios WHERE id = 500;

-- Range query via IOS
SELECT count(*) FROM cs_ios WHERE id BETWEEN 1 AND 100;

-- After deletion, IOS should still work
RESET enable_seqscan;
RESET enable_indexscan;
DELETE FROM cs_ios WHERE id = 500;
SET enable_seqscan = off;
SET enable_indexscan = off;
SELECT count(*) FROM cs_ios WHERE id BETWEEN 490 AND 510;

RESET enable_seqscan;
RESET enable_indexscan;
DROP TABLE cs_ios;

-- ===================================================================
-- Index scan with various predicate types
-- ===================================================================
CREATE TABLE cs_idx_pred (id int, category text, amount numeric(10,2)) USING columnstore;
INSERT INTO cs_idx_pred SELECT i, 'cat_' || (i % 20), (i * 0.5)::numeric(10,2)
    FROM generate_series(1, 2000) i;
VACUUM cs_idx_pred;

CREATE INDEX cs_idx_pred_cat ON cs_idx_pred (category);
CREATE INDEX cs_idx_pred_amt ON cs_idx_pred (amount);
ANALYZE cs_idx_pred;

SET enable_seqscan = off;

-- Equality on text
SELECT count(*) FROM cs_idx_pred WHERE category = 'cat_0';

-- Range on numeric
SELECT count(*) FROM cs_idx_pred WHERE amount BETWEEN 100.00 AND 200.00;

-- Verify actual values
SELECT id, category, amount FROM cs_idx_pred WHERE category = 'cat_0' AND id <= 20 ORDER BY id;

RESET enable_seqscan;
DROP TABLE cs_idx_pred;

-- ===================================================================
-- Index scan after UPDATE (mix of columnar + delta)
-- ===================================================================
CREATE TABLE cs_idx_upd (id int, val text) USING columnstore;
INSERT INTO cs_idx_upd SELECT i, 'orig_' || i FROM generate_series(1, 500) i;
VACUUM cs_idx_upd;
CREATE INDEX cs_idx_upd_id ON cs_idx_upd (id);

-- Update creates delta rows
UPDATE cs_idx_upd SET val = 'updated' WHERE id BETWEEN 100 AND 110;

SET enable_seqscan = off;

-- Old columnar rows should be gone (via deletion bitmap)
-- Updated rows should be found in delta store
SELECT id, val FROM cs_idx_upd WHERE id = 100;
SELECT id, val FROM cs_idx_upd WHERE id = 105;
-- Non-updated row still in columnar
SELECT id, val FROM cs_idx_upd WHERE id = 99;
SELECT count(*) AS total FROM cs_idx_upd WHERE id BETWEEN 95 AND 115;

RESET enable_seqscan;
DROP TABLE cs_idx_upd;

-- ===================================================================
-- CREATE INDEX CONCURRENTLY
-- ===================================================================
CREATE TABLE cs_idx_cic (id int, val text, num int) USING columnstore;
INSERT INTO cs_idx_cic SELECT i, 'rg1_' || i, i * 10 FROM generate_series(1, 500) i;
VACUUM cs_idx_cic;
INSERT INTO cs_idx_cic SELECT i, 'rg2_' || i, i * 10 FROM generate_series(501, 1000) i;
VACUUM cs_idx_cic;
-- Delta rows
INSERT INTO cs_idx_cic SELECT i, 'delta_' || i, i * 10 FROM generate_series(1001, 1100) i;

-- Build index concurrently (exercises cs_index_validate_scan)
CREATE INDEX CONCURRENTLY cs_idx_cic_id ON cs_idx_cic (id);
CREATE INDEX CONCURRENTLY cs_idx_cic_num ON cs_idx_cic (num);

-- Verify indexes are valid
SELECT indexrelid::regclass, indisvalid
    FROM pg_index WHERE indrelid = 'cs_idx_cic'::regclass ORDER BY 1;

-- Verify index scan correctness
SET enable_seqscan = off;
SELECT id, val FROM cs_idx_cic WHERE id = 1;
SELECT id, val FROM cs_idx_cic WHERE id = 750;
SELECT id, val FROM cs_idx_cic WHERE id = 1050;
SELECT count(*) FROM cs_idx_cic WHERE num BETWEEN 5000 AND 6000;
RESET enable_seqscan;

-- REINDEX CONCURRENTLY
REINDEX INDEX CONCURRENTLY cs_idx_cic_id;
SELECT indexrelid::regclass, indisvalid
    FROM pg_index WHERE indrelid = 'cs_idx_cic'::regclass ORDER BY 1;

-- Partial index concurrently
CREATE INDEX CONCURRENTLY cs_idx_cic_partial ON cs_idx_cic (id) WHERE id > 500;
SET enable_seqscan = off;
SELECT count(*) FROM cs_idx_cic WHERE id = 750;
SELECT count(*) FROM cs_idx_cic WHERE id > 500 AND id <= 600;
RESET enable_seqscan;

DROP TABLE cs_idx_cic;

-- ===================================================================
-- Dense bitmap pages
--
-- A virtual block holds MaxHeapTuplesPerPage (291) rows; a bitmap
-- scan matching more than 256 of them on one block used to overrun a
-- 256-entry offset buffer (exact pages) or silently drop offsets
-- 257..291 (lossy pages).  Match whole blocks and compare with the
-- seqscan count, then force lossy pages with a tiny work_mem.
-- ===================================================================
CREATE TABLE cs_bm_dense (a int, b text) USING columnstore;
INSERT INTO cs_bm_dense SELECT i, 'dense_' || i FROM generate_series(1, 2000) i;
VACUUM cs_bm_dense;
CREATE INDEX cs_bm_dense_a ON cs_bm_dense(a);
SET enable_seqscan = off;
SET enable_indexscan = off;
SET enable_bitmapscan = on;
EXPLAIN (COSTS OFF) SELECT count(*) FROM cs_bm_dense WHERE a BETWEEN 1 AND 1800;
SELECT count(*) FROM cs_bm_dense WHERE a BETWEEN 1 AND 1800;
SELECT sum(a) FROM cs_bm_dense WHERE a BETWEEN 1 AND 1800;
-- lossy pages
SET work_mem = '64kB';
SELECT count(*) FROM cs_bm_dense WHERE a BETWEEN 1 AND 1800;
RESET work_mem;
RESET enable_seqscan;
RESET enable_indexscan;
RESET enable_bitmapscan;
DROP TABLE cs_bm_dense;

-- index fetch of a whole-number (dscale 0) NI64 numeric column
CREATE TABLE cs_idx_ni64 (id int, n numeric) USING columnstore;
INSERT INTO cs_idx_ni64 SELECT i, i::numeric FROM generate_series(1, 500) i;
VACUUM cs_idx_ni64;
CREATE INDEX cs_idx_ni64_id ON cs_idx_ni64(id);
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SELECT id, n FROM cs_idx_ni64 WHERE id = 250;
RESET enable_seqscan;
RESET enable_bitmapscan;
DROP TABLE cs_idx_ni64;
-- NI64 point reads across row groups: the conversion buffer must live
-- with the column cache, not in the slot's per-tuple context, since the
-- row-group switch frees it long after the slot context is reset
CREATE TABLE cs_idx_ni64_multi (id int, n numeric(12,2)) USING columnstore;
INSERT INTO cs_idx_ni64_multi
    SELECT i, (i * 0.25)::numeric(12,2) FROM generate_series(1, 150000) i;
VACUUM cs_idx_ni64_multi;
CREATE INDEX cs_idx_ni64_multi_id ON cs_idx_ni64_multi(id);
SET enable_seqscan = off;
SET enable_bitmapscan = off;
-- fetch rows from both row groups through one index scan
SELECT id, n FROM cs_idx_ni64_multi
    WHERE id IN (1, 99892, 99893, 150000) ORDER BY id;
RESET enable_seqscan;
RESET enable_bitmapscan;
DROP TABLE cs_idx_ni64_multi;
