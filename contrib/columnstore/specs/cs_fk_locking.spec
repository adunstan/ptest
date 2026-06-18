# Foreign keys referencing a columnstore parent: the RI trigger's
# FOR KEY SHARE lock on the parent row must block a concurrent parent
# DELETE until the child insert commits, for parents in the delta and
# in columnar form.

setup
{
    CREATE EXTENSION IF NOT EXISTS columnstore;
    CREATE TABLE cs_parent (id int PRIMARY KEY, val text) USING columnstore;
    CREATE TABLE heap_child (id int, parent_id int REFERENCES cs_parent (id));
    INSERT INTO cs_parent SELECT i, 'p' || i FROM generate_series(1, 5) i;
}

teardown
{
    DROP TABLE heap_child;
    DROP TABLE cs_parent;
}

session "s0"
step "s0_vacuum"     { VACUUM cs_parent; }

session "s1"
step "s1_begin"      { BEGIN; }
step "s1_child"      { INSERT INTO heap_child VALUES (100, 1); }
step "s1_commit"     { COMMIT; }

session "s2"
step "s2_del_parent" { DELETE FROM cs_parent WHERE id = 1; }
step "s2_children"   { SELECT count(*) FROM heap_child WHERE parent_id = 1; }

# the parent delete must wait for the child insert's key-share lock,
# then fail its RI check
permutation "s1_begin" "s1_child" "s2_del_parent" "s1_commit" "s2_children"
permutation "s0_vacuum" "s1_begin" "s1_child" "s2_del_parent" "s1_commit" "s2_children"
