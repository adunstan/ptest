# ON CONFLICT DO UPDATE under REPEATABLE READ must raise a
# serialization failure when the conflicting row is invisible to the
# updating transaction's snapshot, exactly as heap does.  A
# tuple_satisfies_snapshot callback that blindly answers "visible"
# would silently update the unseen row instead.

setup
{
    CREATE EXTENSION IF NOT EXISTS columnstore;
    CREATE TABLE cs_conflict_rr (id int, val text) USING columnstore;
    CREATE UNIQUE INDEX cs_conflict_rr_id ON cs_conflict_rr (id);
}

teardown
{
    DROP TABLE cs_conflict_rr;
}

session "s1"
step "s1_begin"    { BEGIN ISOLATION LEVEL REPEATABLE READ; }
step "s1_snap"     { SELECT count(*) FROM cs_conflict_rr; }
step "s1_upsert"   { INSERT INTO cs_conflict_rr VALUES (1, 'from_s1')
                     ON CONFLICT (id) DO UPDATE SET val = 'updated_by_s1'; }
step "s1_commit"   { COMMIT; }

session "s2"
step "s2_insert"   { INSERT INTO cs_conflict_rr VALUES (1, 'from_s2'); }

permutation "s1_begin" "s1_snap" "s2_insert" "s1_upsert" "s1_commit"
