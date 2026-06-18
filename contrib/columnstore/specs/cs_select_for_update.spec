# Test SELECT FOR UPDATE on columnstore delta rows that are concurrently
# updated or deleted by another session.
#
# Without the TM_Deleted fix, READ COMMITTED sessions would hit:
#   elog(ERROR, "unexpected table_tuple_lock status: 7")
# because cs_tuple_lock returned TM_Updated (no update chain to follow).

setup
{
    CREATE EXTENSION IF NOT EXISTS columnstore;
    CREATE TABLE cs_lock_test (id int PRIMARY KEY, val text) USING columnstore;
    INSERT INTO cs_lock_test VALUES (1, 'original'), (2, 'keep');
}

teardown
{
    DROP TABLE cs_lock_test;
}

# Session 1: updates a row and commits
session "s1"
step "s1_begin"     { BEGIN; }
step "s1_update"    { UPDATE cs_lock_test SET val = 'updated' WHERE id = 1; }
step "s1_delete"    { DELETE FROM cs_lock_test WHERE id = 1; }
step "s1_commit"    { COMMIT; }

# Session 2: tries SELECT FOR UPDATE under READ COMMITTED
session "s2"
step "s2_begin"     { BEGIN ISOLATION LEVEL READ COMMITTED; }
step "s2_lock_one"  { SELECT * FROM cs_lock_test WHERE id = 1 FOR UPDATE; }
step "s2_lock_all"  { SELECT * FROM cs_lock_test ORDER BY id FOR UPDATE; }
step "s2_commit"    { COMMIT; }

# Scenario 1: s1 updates and commits before s2 tries FOR UPDATE.
# s2 should see the row as gone (TM_Deleted) and skip it.
permutation "s1_begin" "s1_update" "s1_commit" "s2_begin" "s2_lock_one" "s2_commit"

# Scenario 2: s1 deletes and commits before s2 tries FOR UPDATE.
permutation "s1_begin" "s1_delete" "s1_commit" "s2_begin" "s2_lock_one" "s2_commit"

# Scenario 3: s1 updates while s2 is blocking on FOR UPDATE.
# s2 should see the row disappear after s1 commits.
permutation "s2_begin" "s1_begin" "s1_update" "s2_lock_one" "s1_commit" "s2_commit"

# Scenario 4: FOR UPDATE on all rows -- surviving row should be returned.
permutation "s1_begin" "s1_update" "s1_commit" "s2_begin" "s2_lock_all" "s2_commit"
