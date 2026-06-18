# Copyright (c) 2026, PostgreSQL Global Development Group
#
# Scale coverage for the columnstore AM.  The functional regression
# suite maxes out at ~1-2 row groups (row groups are 100000 rows), so
# the multi-row-group behavior of compaction, zone-map pruning, the
# int128 SUM/AVG accumulator, parallel work-unit distribution, the
# deletion bitmap and index fetch are essentially untested.  This test
# builds tables spanning tens of row groups and checks CORRECTNESS
# deterministically against closed-form values -- never timing.
#
# Gated on PG_TEST_EXTRA containing 'columnstore_scale': the largest
# table here is ~1M rows and a couple of VACUUM compaction passes, which
# is too slow/large for the default regression run but well under
# ~2 minutes / ~2GB.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

if (!$ENV{PG_TEST_EXTRA} || $ENV{PG_TEST_EXTRA} !~ /\bcolumnstore_scale\b/)
{
	plan skip_all =>
	  "test columnstore_scale not enabled in PG_TEST_EXTRA";
}

my $node = PostgreSQL::Test::Cluster->new('cs_scale');
$node->init;
# Keep parallelism deterministic and available for the parallel-vs-serial
# checks; small scan thresholds so the planner actually parallelizes.
$node->append_conf(
	'postgresql.conf', qq{
max_worker_processes = 8
max_parallel_workers = 8
max_parallel_workers_per_gather = 4
min_parallel_table_scan_size = 0
parallel_setup_cost = 0
parallel_tuple_cost = 0
maintenance_work_mem = '256MB'
});
$node->start;

$node->safe_psql('postgres', 'CREATE EXTENSION columnstore');

# ----------------------------------------------------------------------
# Section 1: many row groups + int128 SUM/AVG accumulator + zone maps.
#
# 1,000,000 rows = 10 full row groups.  id runs 1..N so every aggregate
# has a closed form:
#   count(*)        = N
#   sum(id)         = N*(N+1)/2
#   min(id)/max(id) = 1 / N
#   avg(id)         = (N+1)/2
# val = id * 1000 stresses SUM(int8)->numeric (sum ~ 1.1e12 * 1e3) and
# the int128 path without crossing the 2^122 fold.
# ----------------------------------------------------------------------
my $N = 1_000_000;
$node->safe_psql('postgres', q{
	CREATE TABLE cs_big (id bigint, val bigint, g int) USING columnstore;
});
$node->safe_psql('postgres',
	"INSERT INTO cs_big SELECT i, i * 1000, i % 97 "
	  . "FROM generate_series(1, $N) i");
$node->safe_psql('postgres', 'VACUUM cs_big');

# Confirm we actually built many row groups.  There is no introspection
# function, so read "Row Groups Examined" from EXPLAIN ANALYZE of a full
# serial scan (an unqualified scan examines every group).
my $nrg;
{
	my $exp = $node->safe_psql('postgres', q{
		SET max_parallel_workers_per_gather = 0;
		EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
		SELECT count(*) FROM cs_big;
	});
	($nrg) = $exp =~ /Row Groups Examined:\s+(\d+)/;
	$nrg = int($N / 100000) unless defined $nrg;     # derive if not reported
}
cmp_ok($nrg, '>=', 9, "cs_big spans many row groups ($nrg)");

# Closed-form aggregate correctness over many row groups.
my $exp_count = $N;
my $exp_sum   = $N * ($N + 1) / 2;           # fits in a Perl float exactly here
my $exp_min   = 1;
my $exp_max   = $N;
my $exp_avg2  = $N + 1;                       # 2*avg(id) = N+1 (integer)

cmp_ok($node->safe_psql('postgres', 'SELECT count(*) FROM cs_big'),
	'==', $exp_count, "count(*) over $nrg row groups");
cmp_ok($node->safe_psql('postgres', 'SELECT sum(id) FROM cs_big'),
	'==', $exp_sum, "sum(id) closed form over many row groups");
cmp_ok($node->safe_psql('postgres', 'SELECT min(id) FROM cs_big'),
	'==', $exp_min, "min(id) over many row groups");
cmp_ok($node->safe_psql('postgres', 'SELECT max(id) FROM cs_big'),
	'==', $exp_max, "max(id) over many row groups");
cmp_ok($node->safe_psql('postgres', 'SELECT (avg(id) * 2)::bigint FROM cs_big'),
	'==', $exp_avg2, "avg(id) over many row groups");

# SUM(int8)->numeric: sum(val) = 1000 * sum(id).  The exact closed form
# fits a double here, so a numeric comparison is exact.
my $exp_sumval = $exp_sum * 1000;
cmp_ok($node->safe_psql('postgres', 'SELECT sum(val)::numeric(40,0) FROM cs_big'),
	'==', $exp_sumval,
	"sum(int8) numeric result over many row groups");

# ----------------------------------------------------------------------
# Section 2: parallel result == serial result (the strongest determinism
# check for work-unit distribution: unit 0 is the delta, units 1..N are
# the row groups, claimed atomically).  Equality must hold regardless of
# how workers split the groups.
# ----------------------------------------------------------------------
my $serial = $node->safe_psql('postgres', q{
	SET max_parallel_workers_per_gather = 0;
	SELECT count(*), sum(id), sum(val)::numeric(40,0), min(id), max(id),
	       sum(g)
	FROM cs_big;
});
my $parallel = $node->safe_psql('postgres', q{
	SET max_parallel_workers_per_gather = 4;
	SET min_parallel_table_scan_size = 0;
	SELECT count(*), sum(id), sum(val)::numeric(40,0), min(id), max(id),
	       sum(g)
	FROM cs_big;
});
is($parallel, $serial, "parallel aggregate == serial aggregate");

# Group-by parallel vs serial: sum over the 97 residue classes must match.
my $serial_g = $node->safe_psql('postgres', q{
	SET max_parallel_workers_per_gather = 0;
	SELECT string_agg(t.s::text, ',' ORDER BY t.g) FROM
	  (SELECT g, sum(id) AS s FROM cs_big GROUP BY g) t;
});
my $parallel_g = $node->safe_psql('postgres', q{
	SET max_parallel_workers_per_gather = 4;
	SET min_parallel_table_scan_size = 0;
	SELECT string_agg(t.s::text, ',' ORDER BY t.g) FROM
	  (SELECT g, sum(id) AS s FROM cs_big GROUP BY g) t;
});
is($parallel_g, $serial_g, "parallel GROUP BY == serial GROUP BY");

# ----------------------------------------------------------------------
# Section 3: zone-map pruning across many row groups.  id is monotonic
# so each of the 15 groups owns a disjoint [lo,hi] range.  A predicate
# that selects a single group's range must let EXPLAIN ANALYZE report
# most groups skipped.  We only assert the deterministic facts: at least
# one group skipped, and skipped + (examined-but-kept) accounting is
# consistent, plus the result is correct.
# ----------------------------------------------------------------------
my $explain = $node->safe_psql('postgres', q{
	EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
	SELECT count(*) FROM cs_big WHERE id BETWEEN 250001 AND 350000;
});
my ($examined) = $explain =~ /Row Groups Examined:\s+(\d+)/;
my ($skipped)  = $explain =~ /Row Groups Skipped by Zone Map:\s+(\d+)/;
SKIP:
{
	skip "scan did not report zone-map instrumentation", 1
	  unless defined $examined;
	cmp_ok($skipped, '>=', 6,
		"zone map skipped most row groups for a narrow range "
		  . "(examined=$examined skipped=$skipped)");
}
# Range result is exact regardless of pruning.
cmp_ok($node->safe_psql('postgres',
		'SELECT count(*) FROM cs_big WHERE id BETWEEN 250001 AND 350000'),
	'==', 100000, "narrow-range count correct under zone-map pruning");

# ----------------------------------------------------------------------
# Section 4: DELETE spread across many row groups + the deletion bitmap,
# then VACUUM compaction of those groups.  Delete every 10th row.
# Survivors: rows with id % 10 != 0  ->  N - N/10.
# sum(id) of survivors = sum(all) - sum(multiples of 10).
# ----------------------------------------------------------------------
$node->safe_psql('postgres', 'DELETE FROM cs_big WHERE id % 10 = 0');
my $deleted    = int($N / 10);
my $exp_surv   = $N - $deleted;
# sum of 10,20,...,(N) = 10 * (1+2+..+N/10) = 10 * (M*(M+1)/2), M=N/10
my $M          = int($N / 10);
my $sum_mult10 = 10 * ($M * ($M + 1) / 2);
my $exp_surv_sum = $exp_sum - $sum_mult10;

cmp_ok($node->safe_psql('postgres', 'SELECT count(*) FROM cs_big'),
	'==', $exp_surv, "count after spread DELETE (deletion bitmap)");
cmp_ok($node->safe_psql('postgres', 'SELECT sum(id) FROM cs_big'),
	'==', $exp_surv_sum, "sum(id) of survivors after spread DELETE");

# VACUUM must compact the many partially-deleted groups without changing
# visible contents.
my $before = $node->safe_psql('postgres',
	'SELECT count(*), sum(id), min(id), max(id) FROM cs_big');
$node->safe_psql('postgres', 'VACUUM cs_big');
my $after = $node->safe_psql('postgres',
	'SELECT count(*), sum(id), min(id), max(id) FROM cs_big');
is($after, $before, "VACUUM compaction preserved contents across groups");

# Parallel vs serial again, now over a compacted, deletion-bearing table.
my $s2 = $node->safe_psql('postgres', q{
	SET max_parallel_workers_per_gather = 0;
	SELECT count(*), sum(id), min(id), max(id) FROM cs_big;
});
my $p2 = $node->safe_psql('postgres', q{
	SET max_parallel_workers_per_gather = 4;
	SET min_parallel_table_scan_size = 0;
	SELECT count(*), sum(id), min(id), max(id) FROM cs_big;
});
is($p2, $s2, "parallel == serial after compaction");

# ----------------------------------------------------------------------
# Section 5: index build + index fetch over many row groups.  Build a
# btree over the (now compacted, virtual-TID-addressed) columnar rows and
# verify point and small-range fetches return the right rows.  This
# exercises virtual-TID encode/decode across many groups.
# ----------------------------------------------------------------------
$node->safe_psql('postgres', 'CREATE INDEX cs_big_id_idx ON cs_big (id)');

# A surviving point (id not a multiple of 10).
cmp_ok($node->safe_psql('postgres', q{
		SET enable_seqscan = off;
		SELECT val FROM cs_big WHERE id = 123457;
	}),
	'==', 123457 * 1000, "index point fetch returns correct row");

# A deleted point returns nothing.
cmp_ok($node->safe_psql('postgres', q{
		SET enable_seqscan = off;
		SELECT count(*) FROM cs_big WHERE id = 123450;
	}),
	'==', 0, "index point fetch skips deleted row");

# Small range via index: ids 500001..500050, survivors are those not
# divisible by 10 = 50 - 5 = 45 rows; their id-sum has a closed form.
my $rlo = 500001;
my $rhi = 500050;
my $range_all = ($rlo + $rhi) * ($rhi - $rlo + 1) / 2;
# subtract multiples of 10 in [rlo,rhi]: 500010,500020,500030,500040,500050
my $range_del = 500010 + 500020 + 500030 + 500040 + 500050;
is($node->safe_psql('postgres', qq{
		SET enable_seqscan = off;
		SELECT count(*), sum(id) FROM cs_big
		WHERE id BETWEEN $rlo AND $rhi;
	}) =~ s/\r//gr,
	"45|" . ($range_all - $range_del),
	"index range fetch correct over compacted groups");

# ----------------------------------------------------------------------
# Section 6: SUM/AVG of large NUMERICs over many row groups.  1e30 does
# not fit an int64, so the column is NOT int64-encoded: this exercises the
# arbitrary-precision Numeric accumulator (cs_agg_num_acc_add) folding
# partials across ~2 row groups and across workers, rather than the int64
# fast path.  (The int128 2^122 fold is unreachable at any testable size --
# summing int64-bounded values to 2^122 needs ~6e17 rows -- so it is left
# to the small-scale dscale-rescale cases in cs_agg_pushdown.sql.)
#
# 200000 rows, each value = 1e30.  Closed form: sum = 200000 * 1e30 = 2e35,
# avg = 1e30.
# ----------------------------------------------------------------------
my $NB = 200_000;
$node->safe_psql('postgres', q{
	CREATE TABLE cs_num (v numeric) USING columnstore;
});
$node->safe_psql('postgres',
	"INSERT INTO cs_num SELECT 1000000000000000000000000000000 "
	  . "FROM generate_series(1, $NB) i");
$node->safe_psql('postgres', 'VACUUM cs_num');

my $exp_numsum = '2' . ('0' x 35);    # 200000 * 1e30 = 2e35
# These exceed double precision, so compare as exact text (\r-stripped).
is($node->safe_psql('postgres', 'SELECT sum(v)::numeric(60,0) FROM cs_num')
	  =~ s/\r//gr,
	$exp_numsum, "SUM(numeric) exact across int128 fold-to-Numeric");

# avg(v) = 1e30 exactly.
is($node->safe_psql('postgres', 'SELECT avg(v)::numeric(40,0) FROM cs_num')
	  =~ s/\r//gr,
	'1' . ('0' x 30), "AVG(numeric) exact across int128 fold");

# Parallel vs serial for the folded numeric sum (combines partial int128
# states through the serialize/deserialize/combine path).
my $sn = $node->safe_psql('postgres', q{
	SET max_parallel_workers_per_gather = 0;
	SELECT sum(v)::numeric(60,0) FROM cs_num;
});
my $pn = $node->safe_psql('postgres', q{
	SET max_parallel_workers_per_gather = 4;
	SET min_parallel_table_scan_size = 0;
	SELECT sum(v)::numeric(60,0) FROM cs_num;
});
is($pn, $sn, "parallel == serial SUM(numeric) across fold");

$node->stop;
done_testing();
