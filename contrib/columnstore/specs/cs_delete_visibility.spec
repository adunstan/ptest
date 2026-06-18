# Test MVCC visibility of columnar DELETEs and UPDATEs via tombstones.
#
# When a session deletes or updates a columnar row, the old row should
# remain visible to other sessions with older snapshots until the
# modifying transaction commits.

setup
{
    CREATE EXTENSION IF NOT EXISTS columnstore;
    CREATE TABLE cs_vis (id int, val text) USING columnstore;
    INSERT INTO cs_vis SELECT i, 'row_' || i FROM generate_series(1, 500) i;
}

# Compact data into columnar row groups before testing
session "s0"
step "s0_vacuum"    { VACUUM cs_vis; }

teardown
{
    DROP TABLE cs_vis;
}

# Session 1: deletes or updates columnar rows
session "s1"
step "s1_begin"     { BEGIN; }
step "s1_delete"    { DELETE FROM cs_vis WHERE id BETWEEN 100 AND 110; }
step "s1_update"    { UPDATE cs_vis SET val = 'new_' || id WHERE id BETWEEN 100 AND 110; }
step "s1_commit"    { COMMIT; }
step "s1_rollback"  { ROLLBACK; }

# Session 2: reads with REPEATABLE READ snapshot
session "s2"
step "s2_begin"     { BEGIN ISOLATION LEVEL REPEATABLE READ; }
step "s2_count"     { SELECT count(*) AS visible FROM cs_vis WHERE id BETWEEN 100 AND 110; }
step "s2_count_all" { SELECT count(*) AS total FROM cs_vis; }
step "s2_read"      { SELECT id, val FROM cs_vis WHERE id IN (100, 105, 110) ORDER BY id; }
step "s2_commit"    { COMMIT; }

# Scenario 1: s2 BEGINs before s1 deletes, but s2's first query runs after
# s1 commits.  Under REPEATABLE READ the snapshot is taken at the first
# query, so s2 sees the delete.
permutation "s0_vacuum" "s2_begin" "s1_begin" "s1_delete" "s1_commit" "s2_count" "s2_count_all" "s2_commit"

# Scenario 2: s1 deletes and commits, then s2 starts.
# s2 should see the rows as deleted.
permutation "s0_vacuum" "s1_begin" "s1_delete" "s1_commit" "s2_begin" "s2_count" "s2_count_all" "s2_commit"

# Scenario 3: s1 deletes but rolls back.
# s2 should see all rows.
permutation "s0_vacuum" "s1_begin" "s1_delete" "s1_rollback" "s2_begin" "s2_count" "s2_count_all" "s2_commit"

# Scenario 4: s2 takes snapshot first, then s1 deletes and commits.
# Under REPEATABLE READ, s2 should still see the rows.
permutation "s0_vacuum" "s2_begin" "s2_count" "s1_begin" "s1_delete" "s1_commit" "s2_count" "s2_commit"

# ---- UPDATE scenarios ----

# Scenario 5: s2 takes snapshot, then s1 updates columnar rows and commits.
# Under REPEATABLE READ, s2 should still see the OLD values.
permutation "s0_vacuum" "s2_begin" "s2_read" "s1_begin" "s1_update" "s1_commit" "s2_read" "s2_count" "s2_commit"

# Scenario 6: s1 updates and commits, then s2 starts.
# s2 should see the NEW values.
permutation "s0_vacuum" "s1_begin" "s1_update" "s1_commit" "s2_begin" "s2_read" "s2_count" "s2_commit"

# Scenario 7: s1 updates but rolls back.
# s2 should see the original values.
permutation "s0_vacuum" "s1_begin" "s1_update" "s1_rollback" "s2_begin" "s2_read" "s2_count" "s2_commit"
