# CREATE INDEX must include rows that older snapshots can still see.
# A build filtered through a fresh snapshot would omit a row deleted
# after a REPEATABLE READ transaction took its snapshot; that
# transaction would then miss the row when scanning through the new
# index.  An aborted DELETE must likewise leave its row indexed.

setup
{
    CREATE EXTENSION IF NOT EXISTS columnstore;
    CREATE TABLE cs_idxvis (id int, val text) USING columnstore;
    INSERT INTO cs_idxvis SELECT i, 'v_' || i FROM generate_series(1, 200) i;
}

teardown
{
    DROP TABLE cs_idxvis;
}

session "s1"
step "s1_begin"   { BEGIN ISOLATION LEVEL REPEATABLE READ; }
step "s1_snap"    { SELECT count(*) FROM cs_idxvis; }
step "s1_idxscan" { SET LOCAL enable_seqscan = off;
                    SELECT count(*) FROM cs_idxvis WHERE id BETWEEN 1 AND 200; }
step "s1_commit"  { COMMIT; }

session "s2"
step "s2_delete"  { DELETE FROM cs_idxvis WHERE id = 100; }
step "s2_abort_delete" { BEGIN; DELETE FROM cs_idxvis WHERE id = 150; ROLLBACK; }
step "s2_index"   { CREATE INDEX cs_idxvis_id ON cs_idxvis (id); }
step "s2_count"   { SET LOCAL enable_seqscan = off;
                    SELECT count(*) FROM cs_idxvis WHERE id BETWEEN 1 AND 200; }

permutation "s1_begin" "s1_snap" "s2_delete" "s2_abort_delete" "s2_index" "s1_idxscan" "s1_commit" "s2_count"
