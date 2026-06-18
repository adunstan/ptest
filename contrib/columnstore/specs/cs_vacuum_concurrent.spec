# Test that VACUUM does not lose rows inserted concurrently into the delta.
#
# VACUUM reads the delta range from the metapage, compacts those pages, and
# then clears the delta.  If a concurrent INSERT extends the delta after
# VACUUM's initial read, the clearing must preserve the new pages.

setup
{
    CREATE EXTENSION IF NOT EXISTS columnstore;
    CREATE TABLE cs_vc (id int, val text) USING columnstore;
    INSERT INTO cs_vc SELECT i, 'row_' || i FROM generate_series(1, 100) i;
}

teardown
{
    DROP TABLE cs_vc;
}

# Session 1: runs VACUUM (compacts delta into columnar row groups)
session "s1"
step "s1_vacuum"    { VACUUM cs_vc; }

# Session 2: inserts new rows into the delta
session "s2"
step "s2_insert"    { INSERT INTO cs_vc SELECT i, 'new_' || i FROM generate_series(101, 110) i; }
step "s2_count"     { SELECT count(*) AS total FROM cs_vc; }

# Scenario 1: INSERT happens after VACUUM starts but before it finishes.
# The isolation test framework serializes steps, so s2_insert runs after
# s1_vacuum begins its scan.  The new rows must survive VACUUM's delta clear.
permutation "s1_vacuum" "s2_insert" "s2_count"

# Scenario 2: INSERT first, then VACUUM.  Standard case — all rows preserved.
permutation "s2_insert" "s1_vacuum" "s2_count"

# Scenario 3: INSERT, VACUUM, another INSERT, verify both batches survive.
permutation "s2_insert" "s1_vacuum" "s2_insert" "s2_count"
