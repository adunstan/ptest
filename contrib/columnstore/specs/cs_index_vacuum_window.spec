# Index fetches during the compaction window.  VACUUM publishes moved
# rows (metapage flip) before it rebuilds indexes, and the rebuild
# blocks on locks a concurrent index-scan cursor holds, so the cursor's
# later fetches run exactly in the window where index entries still
# carry pre-move TIDs.  Those TIDs must keep resolving: consumed delta
# pages stay intact until reuse, and reuse cannot happen before the
# rebuild completes.

setup
{
    CREATE EXTENSION IF NOT EXISTS columnstore;
    CREATE TABLE cs_idxwin (id int, val text) USING columnstore;
    INSERT INTO cs_idxwin SELECT i, 'w' || i FROM generate_series(1, 30) i;
    CREATE INDEX cs_idxwin_id ON cs_idxwin (id);
}

teardown
{
    DROP TABLE cs_idxwin;
}

session "s1"
setup            { SET enable_seqscan = off; SET enable_bitmapscan = off; }
step "s1_begin"  { BEGIN; }
step "s1_open"   { DECLARE cur CURSOR FOR SELECT id FROM cs_idxwin ORDER BY id; }
step "s1_fetch3" { FETCH 3 FROM cur; }
step "s1_rest"   { FETCH ALL FROM cur; }
step "s1_commit" { COMMIT; }

session "s2"
step "s2_vacuum" { VACUUM cs_idxwin; }
step "s2_count"  { SELECT count(*) FROM cs_idxwin; }

# the VACUUM flips the metapage, then blocks rebuilding the index the
# cursor holds open; the remaining 27 rows must still be reachable
# through the old index entries
permutation "s1_begin" "s1_open" "s1_fetch3" "s2_vacuum" "s1_rest" "s1_commit" "s2_count"
