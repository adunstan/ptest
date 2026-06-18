--
-- Columnstore deletion bitmap patterns, UPDATE on columnar rows
--

-- ===================================================================
-- Deletion bitmap: checkerboard pattern
-- ===================================================================
CREATE TABLE cs_del_checker (id int, val text) USING columnstore;
INSERT INTO cs_del_checker SELECT i, 'row_' || i FROM generate_series(1, 1000) i;
VACUUM cs_del_checker;

-- Delete every other row
DELETE FROM cs_del_checker WHERE id % 2 = 0;
SELECT count(*) AS remaining FROM cs_del_checker;

-- Verify only odd rows remain
SELECT count(*) FROM cs_del_checker WHERE id % 2 = 1;
SELECT count(*) FROM cs_del_checker WHERE id % 2 = 0;

-- Spot check boundaries
SELECT id, val FROM cs_del_checker WHERE id IN (1, 2, 999, 1000) ORDER BY id;

DROP TABLE cs_del_checker;

-- ===================================================================
-- Deletion bitmap: delete all but one
-- ===================================================================
CREATE TABLE cs_del_one (id int, val text) USING columnstore;
INSERT INTO cs_del_one SELECT i, 'row_' || i FROM generate_series(1, 500) i;
VACUUM cs_del_one;

DELETE FROM cs_del_one WHERE id != 250;
SELECT count(*) AS remaining FROM cs_del_one;
SELECT id, val FROM cs_del_one;

DROP TABLE cs_del_one;

-- ===================================================================
-- Deletion bitmap: delete first and last rows
-- ===================================================================
CREATE TABLE cs_del_boundary (id int, val text) USING columnstore;
INSERT INTO cs_del_boundary SELECT i, 'row_' || i FROM generate_series(1, 500) i;
VACUUM cs_del_boundary;

DELETE FROM cs_del_boundary WHERE id = 1 OR id = 500;
SELECT count(*) AS remaining FROM cs_del_boundary;
-- Verify boundaries
SELECT min(id), max(id) FROM cs_del_boundary;

DROP TABLE cs_del_boundary;

-- ===================================================================
-- Delete all rows in a row group
-- ===================================================================
CREATE TABLE cs_del_all (id int, val text) USING columnstore;
INSERT INTO cs_del_all SELECT i, 'row_' || i FROM generate_series(1, 500) i;
VACUUM cs_del_all;

DELETE FROM cs_del_all WHERE id IS NOT NULL;
SELECT count(*) AS remaining FROM cs_del_all;

-- Insert new data after deleting everything
INSERT INTO cs_del_all SELECT i, 'new_' || i FROM generate_series(1, 100) i;
SELECT count(*) AS after_reinsert FROM cs_del_all;
SELECT id, val FROM cs_del_all WHERE id = 1;

DROP TABLE cs_del_all;

-- ===================================================================
-- UPDATE on columnar rows (creates delta row + bitmap entry)
-- ===================================================================
CREATE TABLE cs_upd (id int, val text, num int) USING columnstore;
INSERT INTO cs_upd SELECT i, 'orig_' || i, i * 10 FROM generate_series(1, 500) i;
VACUUM cs_upd;

-- Update a single row
UPDATE cs_upd SET val = 'updated_100', num = -1 WHERE id = 100;
SELECT id, val, num FROM cs_upd WHERE id = 100;
SELECT count(*) AS total FROM cs_upd;

-- Update multiple rows
UPDATE cs_upd SET val = 'batch_update' WHERE id BETWEEN 200 AND 210;
SELECT count(*) FROM cs_upd WHERE val = 'batch_update';
SELECT count(*) AS total FROM cs_upd;

-- Verify non-updated rows are untouched
SELECT id, val, num FROM cs_upd WHERE id = 199;
SELECT id, val, num FROM cs_upd WHERE id = 211;

DROP TABLE cs_upd;

-- ===================================================================
-- UPDATE then DELETE the updated row
-- ===================================================================
CREATE TABLE cs_upd_del (id int, val text) USING columnstore;
INSERT INTO cs_upd_del SELECT i, 'orig_' || i FROM generate_series(1, 500) i;
VACUUM cs_upd_del;

UPDATE cs_upd_del SET val = 'updated' WHERE id = 50;
DELETE FROM cs_upd_del WHERE id = 50;
SELECT count(*) FROM cs_upd_del WHERE id = 50;
SELECT count(*) AS total FROM cs_upd_del;

DROP TABLE cs_upd_del;

-- ===================================================================
-- Repeated UPDATEs on same row
-- ===================================================================
CREATE TABLE cs_upd_repeat (id int, val int) USING columnstore;
INSERT INTO cs_upd_repeat SELECT i, 0 FROM generate_series(1, 500) i;
VACUUM cs_upd_repeat;

UPDATE cs_upd_repeat SET val = 1 WHERE id = 1;
UPDATE cs_upd_repeat SET val = 2 WHERE id = 1;
UPDATE cs_upd_repeat SET val = 3 WHERE id = 1;
SELECT id, val FROM cs_upd_repeat WHERE id = 1;
SELECT count(*) AS total FROM cs_upd_repeat;

DROP TABLE cs_upd_repeat;

-- ===================================================================
-- Mixed DELETE + UPDATE + INSERT
-- ===================================================================
CREATE TABLE cs_mixed_ops (id int, val text) USING columnstore;
INSERT INTO cs_mixed_ops SELECT i, 'v1_' || i FROM generate_series(1, 500) i;
VACUUM cs_mixed_ops;

-- Various operations
DELETE FROM cs_mixed_ops WHERE id <= 50;
UPDATE cs_mixed_ops SET val = 'updated' WHERE id = 100;
INSERT INTO cs_mixed_ops SELECT i, 'new_' || i FROM generate_series(501, 550) i;

SELECT count(*) AS total FROM cs_mixed_ops;
SELECT count(*) FROM cs_mixed_ops WHERE val = 'updated';
SELECT count(*) FROM cs_mixed_ops WHERE id > 500;

-- Verify the operations compose correctly
SELECT id, val FROM cs_mixed_ops WHERE id IN (50, 51, 100, 501) ORDER BY id;

DROP TABLE cs_mixed_ops;

-- ===================================================================
-- MVCC: DELETE rollback restores visibility
-- ===================================================================
CREATE TABLE cs_del_rollback (id int, val text) USING columnstore;
INSERT INTO cs_del_rollback SELECT i, 'row_' || i FROM generate_series(1, 500) i;
VACUUM cs_del_rollback;

SELECT count(*) AS before_delete FROM cs_del_rollback;

BEGIN;
DELETE FROM cs_del_rollback WHERE id BETWEEN 100 AND 200;
-- Within transaction, rows should be gone
SELECT count(*) AS during_delete FROM cs_del_rollback;
ROLLBACK;

-- After rollback, all rows should be back
SELECT count(*) AS after_rollback FROM cs_del_rollback;

-- Verify the specific rows are visible again
SELECT count(*) FROM cs_del_rollback WHERE id BETWEEN 100 AND 200;

DROP TABLE cs_del_rollback;

-- ===================================================================
-- MVCC: VACUUM materializes tombstones into deletion bitmap
-- ===================================================================
CREATE TABLE cs_del_vacuum (id int, val text) USING columnstore;
INSERT INTO cs_del_vacuum SELECT i, 'row_' || i FROM generate_series(1, 500) i;
VACUUM cs_del_vacuum;

-- Delete some rows (creates tombstones)
DELETE FROM cs_del_vacuum WHERE id <= 10;
SELECT count(*) AS after_delete FROM cs_del_vacuum;

-- VACUUM should materialize committed tombstones into deletion bitmap
-- and clear the delta store
VACUUM cs_del_vacuum;
SELECT count(*) AS after_vacuum FROM cs_del_vacuum;

-- Verify specific rows are still gone
SELECT count(*) FROM cs_del_vacuum WHERE id <= 10;
SELECT count(*) FROM cs_del_vacuum WHERE id = 11;

DROP TABLE cs_del_vacuum;

-- ===================================================================
-- MVCC: DELETE + VACUUM + more deletes
-- ===================================================================
CREATE TABLE cs_del_multi_vacuum (id int, val text) USING columnstore;
INSERT INTO cs_del_multi_vacuum SELECT i, 'row_' || i FROM generate_series(1, 500) i;
VACUUM cs_del_multi_vacuum;

-- First round of deletes
DELETE FROM cs_del_multi_vacuum WHERE id <= 50;
VACUUM cs_del_multi_vacuum;
SELECT count(*) AS after_first_vacuum FROM cs_del_multi_vacuum;

-- Second round of deletes
DELETE FROM cs_del_multi_vacuum WHERE id BETWEEN 51 AND 100;
VACUUM cs_del_multi_vacuum;
SELECT count(*) AS after_second_vacuum FROM cs_del_multi_vacuum;

-- Verify correct count
SELECT count(*) FROM cs_del_multi_vacuum WHERE id <= 100;
SELECT min(id) FROM cs_del_multi_vacuum;

DROP TABLE cs_del_multi_vacuum;

-- ===================================================================
-- DELETE after an aborted DELETE of the same row
--
-- The aborted deleter leaves its xid in xmax with no hint bits; the
-- second DELETE must classify that as "aborted" and overwrite it, not
-- treat it as in-progress and wait (a bug here once spun forever,
-- uninterruptibly).  The timeout is a tripwire so a regression fails
-- rather than hangs the suite.
-- ===================================================================
CREATE TABLE cs_del_abort (a int, b text) USING columnstore;
INSERT INTO cs_del_abort VALUES (1, 'x'), (2, 'y');
SET statement_timeout = '60s';
BEGIN;
DELETE FROM cs_del_abort WHERE a = 1;
ROLLBACK;
DELETE FROM cs_del_abort WHERE a = 1;
RESET statement_timeout;
SELECT * FROM cs_del_abort ORDER BY a;
DROP TABLE cs_del_abort;

-- ===================================================================
-- Combo command ids: same-transaction INSERT + DELETE with a cursor
--
-- cmin and cmax share the tuple's single t_cid field; deleting a row
-- our own transaction inserted must install a combo cid, or a cursor
-- whose snapshot sits between the insert and the delete misreads the
-- cmin and skips rows it must see.
-- ===================================================================
CREATE TABLE cs_combocid (a int, b text) USING columnstore;
BEGIN;
INSERT INTO cs_combocid VALUES (1, 'one'), (2, 'two');
DECLARE cs_cc CURSOR FOR SELECT a, b FROM cs_combocid ORDER BY a;
DELETE FROM cs_combocid WHERE a = 1;
FETCH ALL FROM cs_cc;
CLOSE cs_cc;
COMMIT;
SELECT a, b FROM cs_combocid ORDER BY a;
-- UPDATE variant: same-transaction insert, cursor, update
BEGIN;
INSERT INTO cs_combocid VALUES (3, 'three');
DECLARE cs_cc CURSOR FOR SELECT a, b FROM cs_combocid ORDER BY a;
UPDATE cs_combocid SET b = 'THREE' WHERE a = 3;
FETCH ALL FROM cs_cc;
CLOSE cs_cc;
ROLLBACK;
DROP TABLE cs_combocid;

-- ===================================================================
-- AFTER ROW triggers on compacted rows (SnapshotAny refetch)
-- ===================================================================
CREATE TABLE cs_trig (a int, b text) USING columnstore;
INSERT INTO cs_trig SELECT i, 'r_' || i FROM generate_series(1, 300) i;
VACUUM cs_trig;
CREATE TABLE cs_trig_log (a int, op text);
CREATE FUNCTION cs_trig_fn() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        INSERT INTO cs_trig_log VALUES (OLD.a, 'D');
        RETURN OLD;
    ELSE
        INSERT INTO cs_trig_log VALUES (NEW.a, 'U');
        RETURN NEW;
    END IF;
END $$;
CREATE TRIGGER cs_trig_aft AFTER DELETE OR UPDATE ON cs_trig
    FOR EACH ROW EXECUTE FUNCTION cs_trig_fn();
DELETE FROM cs_trig WHERE a BETWEEN 10 AND 12;
UPDATE cs_trig SET b = 'upd' WHERE a = 20;
SELECT * FROM cs_trig_log ORDER BY a, op;
DROP TABLE cs_trig;
DROP TABLE cs_trig_log;
DROP FUNCTION cs_trig_fn();

-- ===================================================================
-- Duplicate-join DELETE hits the same columnar row twice
-- (TM_SelfModified with a valid cmax: skip, not error)
-- ===================================================================
CREATE TABLE cs_dupdel (a int, b text) USING columnstore;
INSERT INTO cs_dupdel SELECT i, 'd_' || i FROM generate_series(1, 100) i;
VACUUM cs_dupdel;
DELETE FROM cs_dupdel USING (VALUES (50), (50)) v(x) WHERE cs_dupdel.a = v.x;
SELECT count(*) FROM cs_dupdel;
DROP TABLE cs_dupdel;

-- tombstones over columnar rows must keep hiding deleted rows across
-- nestloop inner rescans (the visible set is cached per snapshot)
CREATE TABLE cs_resc (k int, v int) USING columnstore;
INSERT INTO cs_resc SELECT g % 5, g FROM generate_series(1, 1000) g;
VACUUM cs_resc;
DELETE FROM cs_resc WHERE v % 2 = 0;
CREATE TABLE cs_resc_outer (k int);
INSERT INTO cs_resc_outer VALUES (0), (1), (2);
SET enable_hashjoin = off;
SET enable_mergejoin = off;
SET enable_material = off;
SET enable_memoize = off;
SELECT o.k, count(*) AS visible, min(c.v) AS first_v
    FROM cs_resc_outer o JOIN cs_resc c ON c.k = o.k
    GROUP BY o.k ORDER BY o.k;
RESET enable_hashjoin;
RESET enable_mergejoin;
RESET enable_material;
RESET enable_memoize;
DROP TABLE cs_resc;
DROP TABLE cs_resc_outer;

-- WHERE CURRENT OF against columnar rows
CREATE TABLE cs_curof (v int) USING columnstore;
INSERT INTO cs_curof SELECT g FROM generate_series(1, 300) g;
VACUUM cs_curof;
BEGIN;
DECLARE cs_cur CURSOR FOR SELECT v FROM cs_curof WHERE v <= 3;
FETCH 1 FROM cs_cur;
DELETE FROM cs_curof WHERE CURRENT OF cs_cur;
FETCH 1 FROM cs_cur;
UPDATE cs_curof SET v = v + 9000 WHERE CURRENT OF cs_cur;
COMMIT;
SELECT count(*), min(v), max(v) FROM cs_curof;
DROP TABLE cs_curof;

-- MERGE driving both update and insert arms against columnar rows
CREATE TABLE cs_merge (k int, v int) USING columnstore;
INSERT INTO cs_merge SELECT g, g FROM generate_series(1, 100) g;
VACUUM cs_merge;
MERGE INTO cs_merge t USING (VALUES (5, 50), (200, 2000)) AS s(k, v)
    ON t.k = s.k
    WHEN MATCHED THEN UPDATE SET v = s.v
    WHEN NOT MATCHED THEN INSERT VALUES (s.k, s.v);
SELECT count(*), sum(v) FILTER (WHERE k IN (5, 200)) AS merged FROM cs_merge;
DROP TABLE cs_merge;

-- Triggers: AFTER statement trigger with a transition table sees the
-- deleted columnar rows; BEFORE row trigger rewrites inserted rows and
-- RETURNING reports the rewritten values
CREATE TABLE cs_trig (v int, note text) USING columnstore;
CREATE FUNCTION cs_trig_audit() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE NOTICE 'deleted % rows, sum %',
        (SELECT count(*) FROM trans), (SELECT sum(v) FROM trans);
    RETURN NULL;
END $$;
CREATE FUNCTION cs_trig_fix() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN NEW.note := 'fixed' || NEW.v; RETURN NEW; END $$;
CREATE TRIGGER cs_trig_bi BEFORE INSERT ON cs_trig
    FOR EACH ROW EXECUTE FUNCTION cs_trig_fix();
INSERT INTO cs_trig (v) SELECT g FROM generate_series(1, 300) g;
VACUUM cs_trig;
CREATE TRIGGER cs_trig_del AFTER DELETE ON cs_trig
    REFERENCING OLD TABLE AS trans
    FOR EACH STATEMENT EXECUTE FUNCTION cs_trig_audit();
DELETE FROM cs_trig WHERE v <= 3;
INSERT INTO cs_trig (v) VALUES (1000) RETURNING v, note;
DELETE FROM cs_trig WHERE v = 4 RETURNING v, note;
DROP TABLE cs_trig;
DROP FUNCTION cs_trig_audit, cs_trig_fix;

-- WITH HOLD cursors materialize at commit and support backward fetch
-- afterwards, over a mix of columnar and delta rows
CREATE TABLE cs_hold (v int) USING columnstore;
INSERT INTO cs_hold SELECT g FROM generate_series(1, 300) g;
VACUUM cs_hold;
INSERT INTO cs_hold VALUES (1000), (1001);
BEGIN;
DECLARE cs_holdc CURSOR WITH HOLD FOR SELECT v FROM cs_hold ORDER BY v;
FETCH 2 FROM cs_holdc;
COMMIT;
FETCH 3 FROM cs_holdc;
MOVE LAST cs_holdc;
FETCH BACKWARD 2 FROM cs_holdc;
CLOSE cs_holdc;
DROP TABLE cs_hold;

-- COPY routes through the bulk multi-insert path (cs_multi_insert), which
-- must stamp each data tuple's t_ctid at itself just as the single-row
-- path does, or HeapTupleSatisfiesUpdate cannot tell a live row from an
-- updated one.  DELETE/UPDATE the freshly COPY'd (still-in-delta) rows to
-- exercise that: a missing self-ctid would misclassify them.
CREATE TABLE cs_mi (k int, v int) USING columnstore;
COPY cs_mi (k, v) FROM stdin;
1	10
2	20
3	30
4	40
5	50
6	60
7	70
8	80
\.
DELETE FROM cs_mi WHERE k % 3 = 0;
UPDATE cs_mi SET v = v + 1 WHERE k = 1;
SELECT k, v FROM cs_mi ORDER BY k;
-- same again after compaction moves them to columnar storage
COPY cs_mi (k, v) FROM stdin;
9	90
10	100
\.
VACUUM cs_mi;
DELETE FROM cs_mi WHERE k = 10;
SELECT count(*), sum(v) FROM cs_mi;
DROP TABLE cs_mi;
