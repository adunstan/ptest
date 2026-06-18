# Real tuple locking: FOR UPDATE must block concurrent lockers and
# deleters, NOWAIT must error instead of waiting, and the loser of a
# concurrent DELETE race must get a clean outcome (no hang, no assert).
# Run for both storage forms: rows in the delta and rows compacted to
# columnar (the second permutation set VACUUMs first).

setup
{
    CREATE EXTENSION IF NOT EXISTS columnstore;
    CREATE TABLE cs_locks (id int, val text) USING columnstore;
    INSERT INTO cs_locks SELECT i, 'v' || i FROM generate_series(1, 10) i;
}

teardown
{
    DROP TABLE cs_locks;
}

session "s0"
step "s0_vacuum"      { VACUUM cs_locks; }

session "s1"
step "s1_begin"       { BEGIN; }
step "s1_lock"        { SELECT id FROM cs_locks WHERE id = 1 FOR UPDATE; }
step "s1_keyshare"    { SELECT id FROM cs_locks WHERE id = 1 FOR KEY SHARE; }
step "s1_delete"      { DELETE FROM cs_locks WHERE id = 1; }
step "s1_commit"      { COMMIT; }

session "s2"
step "s2_begin"       { BEGIN; }
step "s2_lock_nowait" { SELECT id FROM cs_locks WHERE id = 1 FOR UPDATE NOWAIT; }
step "s2_lock_skip"   { SELECT id FROM cs_locks WHERE id = 1 FOR UPDATE SKIP LOCKED; }
step "s2_delete"      { DELETE FROM cs_locks WHERE id = 1; }
step "s2_keyshare"    { SELECT id FROM cs_locks WHERE id = 1 FOR KEY SHARE; }
step "s2_count"       { SELECT count(*) FROM cs_locks; }
step "s2_commit"      { COMMIT; }

# delta rows: NOWAIT errors, SKIP LOCKED skips, DELETE blocks until the
# locker commits, concurrent deletes leave exactly one winner
permutation "s1_begin" "s1_lock" "s2_lock_nowait" "s1_commit"
permutation "s1_begin" "s1_lock" "s2_lock_skip" "s1_commit"
permutation "s1_begin" "s1_lock" "s2_delete" "s1_commit" "s2_count"
permutation "s1_begin" "s1_delete" "s2_delete" "s1_commit" "s2_count"
# key share must not block a concurrent key share
permutation "s1_begin" "s1_keyshare" "s2_begin" "s2_keyshare" "s1_commit" "s2_commit"

# columnar rows: same guarantees after compaction
permutation "s0_vacuum" "s1_begin" "s1_lock" "s2_lock_nowait" "s1_commit"
permutation "s0_vacuum" "s1_begin" "s1_lock" "s2_lock_skip" "s1_commit"
permutation "s0_vacuum" "s1_begin" "s1_lock" "s2_delete" "s1_commit" "s2_count"
permutation "s0_vacuum" "s1_begin" "s1_delete" "s2_delete" "s1_commit" "s2_count"
permutation "s0_vacuum" "s1_begin" "s1_keyshare" "s2_begin" "s2_keyshare" "s1_commit" "s2_commit"
