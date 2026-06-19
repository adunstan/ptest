# Copyright (c) 2022-2026, PostgreSQL Global Development Group

# Crash-recovery behavior of global temporary tables.  Per-session GTT data
# is neither WAL-logged nor crash-safe by design; what crash recovery must
# guarantee is cleanup: orphaned per-session relation files are removed (they
# follow temporary-relation naming, so remove_temp_files_after_crash and
# startup cleanup cover them), the shared definition survives, the in-memory
# sessions registry is reset so no ghost registration blocks DDL, and the
# table is immediately usable and droppable.  Two crash shapes are covered:
# a single backend killed with SIGKILL (postmaster crash-restart cycle) and
# an immediate (simulated crash) shutdown of the whole cluster with an open
# transaction.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('gtt_crash');
$node->init;
# This test locates per-session GTT files with pg_relation_filepath(), which
# reports the session-local storage of the current backend.  A parallel worker
# cannot see that session-local storage, so under debug_parallel_query (which
# CI may bake into the initdb template) the probe would run in a worker and
# report the wrong path.  Parallelism is irrelevant to crash recovery here, so
# force it off for the whole test to keep the file-path probes authoritative.
$node->append_conf(
	'postgresql.conf', q{
remove_temp_files_after_crash = on
restart_after_crash = on
debug_parallel_query = off
});
$node->start;

$node->safe_psql('postgres',
	'CREATE GLOBAL TEMPORARY TABLE gtt_cr (a int PRIMARY KEY, b text)');

# --- Scenario 1: SIGKILL one backend with materialized GTT storage.

my $s = $node->background_psql('postgres');
$s->query_safe(
	"INSERT INTO gtt_cr SELECT g, repeat('x', 100) FROM generate_series(1, 1000) g"
);
my $pid = $s->query_safe('SELECT pg_backend_pid()');
my $heapfile = $s->query_safe("SELECT pg_relation_filepath('gtt_cr')");
my $idxfile = $s->query_safe("SELECT pg_relation_filepath('gtt_cr_pkey')");
my $datadir = $node->data_dir;

ok(-e "$datadir/$heapfile", 'per-session heap file exists while in use');
ok(-e "$datadir/$idxfile", 'per-session index file exists while in use');

kill 'KILL', $pid;
$s->{run}->finish;    # the killed session is gone; reap it

# The postmaster goes through a crash-restart cycle; wait it out.
$node->poll_query_until('postgres', 'SELECT true')
  or die 'node did not recover from backend crash';

ok(!-e "$datadir/$heapfile",
	'orphaned per-session heap file is removed after a backend crash');
ok(!-e "$datadir/$idxfile",
	'orphaned per-session index file is removed after a backend crash');

# The registry lives in shared memory and was reset: nothing may block DDL,
# and the surviving definition must be immediately usable.
is($node->safe_psql('postgres', 'SELECT count(*) FROM gtt_cr'),
	'0', 'GTT definition survives the crash with no data');
$node->safe_psql('postgres',
	'ALTER TABLE gtt_cr ADD COLUMN c int; INSERT INTO gtt_cr VALUES (1)');
pass('DDL and DML work immediately after the crash-restart cycle');

# --- Scenario 2: immediate shutdown with an open writing transaction.

$s = $node->background_psql('postgres');
$s->query_safe('INSERT INTO gtt_cr VALUES (2)');
$s->query_safe(
	'BEGIN; INSERT INTO gtt_cr SELECT g, NULL, NULL FROM generate_series(10, 500) g;'
);
$heapfile = $s->query_safe("SELECT pg_relation_filepath('gtt_cr')");
ok(-e "$datadir/$heapfile", 'per-session heap file exists mid-transaction');

$node->stop('immediate');
$s->{run}->finish;
$node->start;

ok( !-e "$datadir/$heapfile",
	'orphaned per-session heap file is removed by startup after a hard stop');
is($node->safe_psql('postgres', 'SELECT count(*) FROM gtt_cr'),
	'0', 'no GTT data survives a cluster crash');

$node->safe_psql('postgres', 'DROP TABLE gtt_cr');
pass('DROP TABLE succeeds immediately after recovery');

$node->stop;
done_testing();
