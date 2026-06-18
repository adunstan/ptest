# VACUUM must not break snapshot isolation: a REPEATABLE READ
# transaction's counts must not change when other sessions commit
# inserts/deletes and VACUUM materializes them, whether the rows start
# in the delta or in columnar row groups.

setup
{
    CREATE EXTENSION IF NOT EXISTS columnstore;
    CREATE TABLE cs_snapstable (id int, val text) USING columnstore;
    INSERT INTO cs_snapstable SELECT i, 'v_' || i FROM generate_series(1, 100) i;
}

teardown
{
    DROP TABLE cs_snapstable;
}

session "s0"
step "s0_vacuum"   { VACUUM cs_snapstable; }

session "s1"
step "s1_begin"    { BEGIN ISOLATION LEVEL REPEATABLE READ; }
step "s1_count"    { SELECT count(*) AS total,
                            count(*) FILTER (WHERE id <= 20) AS old_rows,
                            count(*) FILTER (WHERE id > 100) AS new_rows
                     FROM cs_snapstable; }
step "s1_commit"   { COMMIT; }

session "s2"
step "s2_insert"   { INSERT INTO cs_snapstable SELECT i, 'n_' || i
                     FROM generate_series(101, 120) i; }
step "s2_delete"   { DELETE FROM cs_snapstable WHERE id <= 20; }
step "s2_vacuum"   { VACUUM cs_snapstable; }
step "s2_count"    { SELECT count(*) AS total,
                            count(*) FILTER (WHERE id <= 20) AS old_rows,
                            count(*) FILTER (WHERE id > 100) AS new_rows
                     FROM cs_snapstable; }

# rows in the delta: RR count stays 100 across insert+delete+VACUUM
permutation "s1_begin" "s1_count" "s2_insert" "s2_delete" "s2_vacuum" "s1_count" "s1_commit" "s2_count"
# rows compacted first: same guarantees against columnar rows
permutation "s0_vacuum" "s1_begin" "s1_count" "s2_insert" "s2_delete" "s2_vacuum" "s1_count" "s1_commit" "s2_count"
