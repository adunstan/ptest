# VACUUM must not destroy in-flight delta state: an open INSERT's rows
# must survive a concurrent VACUUM and appear after commit; an open
# DELETE interrupted by VACUUM must still take effect when it commits.

setup
{
    CREATE EXTENSION IF NOT EXISTS columnstore;
    CREATE TABLE cs_inflight (id int, val text) USING columnstore;
    INSERT INTO cs_inflight SELECT i, 'v_' || i FROM generate_series(1, 100) i;
}

teardown
{
    DROP TABLE cs_inflight;
}

session "s1"
step "s1_begin"      { BEGIN; }
step "s1_insert"     { INSERT INTO cs_inflight SELECT i, 'new_' || i
                       FROM generate_series(101, 150) i; }
step "s1_delete"     { DELETE FROM cs_inflight WHERE id <= 10; }
step "s1_commit"     { COMMIT; }

session "s2"
step "s2_vacuum"     { VACUUM cs_inflight; }
step "s2_count"      { SELECT count(*) FROM cs_inflight; }

# open INSERT across VACUUM: all 150 rows must exist afterwards
permutation "s1_begin" "s1_insert" "s2_vacuum" "s1_commit" "s2_count"
# open DELETE across VACUUM: the delete must take effect
permutation "s1_begin" "s1_delete" "s2_vacuum" "s1_commit" "s2_count"
