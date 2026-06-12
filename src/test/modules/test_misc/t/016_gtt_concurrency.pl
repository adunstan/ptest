# Copyright (c) 2022-2026, PostgreSQL Global Development Group

# Concurrent stress test for global temporary tables, in two phases.
#
# Phase 1 drives pgbench with a weighted mix of per-session DML (including
# savepoints, TRUNCATE, ON COMMIT DELETE ROWS, and DISCARD ALL), concurrent
# DDL against a busy table (expected to be refused with object_in_use), and
# whole-table create/drop churn.  Because GTT data is session-private, each
# script can assert its own session's invariants inline (the 1/(cond)::int
# idiom turns a violated invariant into a division-by-zero error, which
# aborts the pgbench client and fails the test).  This hammers the shared
# sessions registry, its 5-second DDL grace loop, OID recycling, and the
# lazy materialization machinery under real concurrency.
#
# Phase 2 simulates a connection pooler: a long-lived session repeatedly
# writes and then issues DISCARD ALL.  The DISCARD must dematerialize and
# deregister, so a peer's DROP TABLE succeeds while the pooled session is
# still connected and idle.
#
# After each phase the test verifies that no per-session relation files
# (t<proc>_<filenode>) linger in the database directory once their sessions
# are quiesced or gone.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $duration = $ENV{GTT_STRESS_DURATION} || 15;

my $node = PostgreSQL::Test::Cluster->new('gtt_concurrency');
$node->init;
$node->start;

$node->safe_psql(
	'postgres', q{
	CREATE GLOBAL TEMPORARY TABLE gtt_bench (id int PRIMARY KEY, v int);
	CREATE GLOBAL TEMPORARY TABLE gtt_ocdr (x int) ON COMMIT DELETE ROWS;
});

my $dboid = $node->safe_psql('postgres',
	"SELECT oid FROM pg_database WHERE datname = 'postgres'");

# Count leftover per-session relation files in the database directory.
sub session_file_count
{
	my $dbdir = $node->data_dir . "/base/$dboid";
	opendir(my $dh, $dbdir) || die "opendir $dbdir: $!";
	my @files = grep { /^t\d+_/ } readdir($dh);
	closedir($dh);
	return scalar(@files);
}

# pgbench scripts.  All expected errors are caught (object_in_use,
# duplicate_table for racing DDL); anything else aborts the client.
my $dir = PostgreSQL::Test::Utils::tempdir_short();

append_to_file(
	"$dir/dml.sql", q{\set k random(1, 100)
BEGIN;
INSERT INTO gtt_bench SELECT i, :k FROM generate_series(:k, :k + 9) i ON CONFLICT (id) DO NOTHING;
SELECT 1/((count(*) = 10)::int) FROM gtt_bench WHERE id BETWEEN :k AND :k + 9;
SAVEPOINT s1;
UPDATE gtt_bench SET v = v + 1 WHERE id = :k;
ROLLBACK TO SAVEPOINT s1;
DELETE FROM gtt_bench WHERE id = :k + 5;
COMMIT;
});

append_to_file(
	"$dir/ocdr.sql", q{BEGIN;
INSERT INTO gtt_ocdr VALUES (1), (2), (3);
SELECT 1/((count(*) = 3)::int) FROM gtt_ocdr;
COMMIT;
SELECT 1/((count(*) = 0)::int) FROM gtt_ocdr;
});

append_to_file(
	"$dir/truncate.sql", q{TRUNCATE gtt_bench;
SELECT 1/((count(*) = 0)::int) FROM gtt_bench;
INSERT INTO gtt_bench SELECT i, 0 FROM generate_series(1, 20) i ON CONFLICT (id) DO NOTHING;
});

append_to_file(
	"$dir/discard.sql",
	q{INSERT INTO gtt_bench VALUES (-1, 0) ON CONFLICT (id) DO NOTHING;
DISCARD ALL;
SELECT 1/((count(*) = 0)::int) FROM gtt_bench;
});

append_to_file(
	"$dir/read.sql", q{\set k random(1, 100)
SELECT count(*) FROM gtt_bench WHERE id = :k;
EXPLAIN (COSTS OFF) SELECT * FROM gtt_bench WHERE id = :k;
});

# DDL against the busy table: refused with object_in_use while any peer has
# session data (after the registry's grace period), so this mostly exercises
# the refusal/retry path; if it ever wins the race, the index is built and
# dropped for real.
append_to_file(
	"$dir/ddl_busy.sql", q{DO $$
BEGIN
	CREATE INDEX gtt_bench_v_idx ON gtt_bench (v);
	DROP INDEX gtt_bench_v_idx;
EXCEPTION
	WHEN object_in_use OR duplicate_table THEN NULL;
END $$;
});

# Whole-table churn: create, use, and drop a GTT under concurrency, cycling
# OIDs and registry entries.  Only the client that won the CREATE inserts
# and drops.
append_to_file(
	"$dir/churn.sql", q{DO $$
DECLARE
	created boolean := false;
BEGIN
	BEGIN
		CREATE GLOBAL TEMPORARY TABLE gtt_churn (a int PRIMARY KEY, b text);
		created := true;
	EXCEPTION
		WHEN duplicate_table THEN NULL;
	END;
	IF created THEN
		INSERT INTO gtt_churn VALUES (1, repeat('x', 10));
		BEGIN
			DROP TABLE gtt_churn;
		EXCEPTION
			WHEN object_in_use THEN NULL;
		END;
	END IF;
END $$;
});

# Phase 1: run the mix.  --max-tries retries deadlocks (possible between
# concurrent TRUNCATE/DDL lock acquisitions and ordinary writes); any other
# error aborts the client and fails the run.
$node->command_checks_all(
	[
		'pgbench', '-n',
		'-c', '8',
		'-j', '4',
		'-T', $duration,
		'--max-tries', '10',
		'-f', "$dir/dml.sql\@8",
		'-f', "$dir/ocdr.sql\@3",
		'-f', "$dir/truncate.sql\@2",
		'-f', "$dir/discard.sql\@2",
		'-f', "$dir/read.sql\@3",
		'-f', "$dir/ddl_busy.sql\@1",
		'-f', "$dir/churn.sql\@1",
		'postgres'
	],
	0,
	[qr/processed/],
	[qr/^(?!.*aborted)/s],
	'pgbench concurrent GTT workload runs to completion without aborts');

# Wait for the pgbench backends to fully exit, then verify their session
# files are gone and nothing blocks DDL.
$node->poll_query_until('postgres',
	"SELECT count(*) = 1 FROM pg_stat_activity WHERE backend_type = 'client backend'"
) or die 'pgbench backends did not exit';

is(session_file_count(), 0,
	'no per-session relation files survive the pgbench sessions');

$node->safe_psql('postgres',
	'DROP TABLE gtt_bench; DROP TABLE gtt_ocdr; DROP TABLE IF EXISTS gtt_churn;'
);
pass('DROP TABLE succeeds immediately once the stress sessions are gone');

# Phase 2: pooled-connection simulation.
$node->safe_psql('postgres',
	'CREATE GLOBAL TEMPORARY TABLE gtt_pool (a int PRIMARY KEY)');

my $pooled = $node->background_psql('postgres');
for my $i (1 .. 5)
{
	$pooled->query_safe(
		"INSERT INTO gtt_pool SELECT g FROM generate_series(1, 50) g");
	$pooled->query_safe('DISCARD ALL');
}

# The pooled session is connected but idle and discarded: a peer's DROP must
# succeed without waiting on it (a stale registration would error after the
# 5s grace and fail this safe_psql).
$node->safe_psql('postgres', 'DROP TABLE gtt_pool');
pass('DROP TABLE succeeds while a discarded pooled session is still connected'
);

is(session_file_count(), 0,
	'DISCARD ALL left no per-session relation files behind');

$pooled->quit;

# No crashes or assertion failures anywhere in the run.
my $log = slurp_file($node->logfile);
unlike($log, qr/PANIC|TRAP:/,
	'no PANIC or assertion failure in the server log');

$node->stop;
done_testing();
