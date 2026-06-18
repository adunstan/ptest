# An INSERT that conflicts with a row whose DELETE is still in
# progress must wait for the deleter, not treat the row as already
# gone.  If the delete then aborts, the insert must fail with a
# unique violation -- the alternative is two live rows under a
# unique index.

setup
{
    CREATE EXTENSION IF NOT EXISTS columnstore;
    CREATE TABLE cs_uniq (id int, val text) USING columnstore;
    CREATE UNIQUE INDEX cs_uniq_id ON cs_uniq (id);
    INSERT INTO cs_uniq SELECT i, 'v_' || i FROM generate_series(1, 200) i;
}

teardown
{
    DROP TABLE cs_uniq;
}

session "s0"
step "s0_vacuum"   { VACUUM cs_uniq; }

session "s1"
step "s1_begin"    { BEGIN; }
step "s1_delete"   { DELETE FROM cs_uniq WHERE id = 42; }
step "s1_abort"    { ROLLBACK; }
step "s1_commit"   { COMMIT; }

session "s2"
step "s2_insert"   { INSERT INTO cs_uniq VALUES (42, 'replacement'); }
step "s2_count"    { SELECT count(*) FROM cs_uniq WHERE id = 42; }

# deleter aborts: the blocked insert must fail with a unique violation
permutation "s0_vacuum" "s1_begin" "s1_delete" "s2_insert" "s1_abort" "s2_count"
# deleter commits: the blocked insert succeeds
permutation "s0_vacuum" "s1_begin" "s1_delete" "s2_insert" "s1_commit" "s2_count"
