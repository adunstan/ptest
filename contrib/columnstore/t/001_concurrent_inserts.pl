# Copyright (c) 2026, PostgreSQL Global Development Group

# Concurrency stress for the columnstore delta store: parallel insert
# streams, inserts racing VACUUM's delta compaction, and a bulk DELETE
# racing repeated VACUUMs.  Each phase has a deterministic expected
# row count; lost metapage updates (a full-struct read-modify-write
# overwriting a concurrent extension) or pages consumed with unseen
# tuples show up as missing or duplicated rows.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use IPC::Run qw(start);
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('cs_concurrent');
$node->init;
$node->start;

$node->safe_psql('postgres', 'CREATE EXTENSION columnstore');
$node->safe_psql('postgres',
	'CREATE TABLE t (id bigint, val text) USING columnstore');

my $clients = 8;
my $txns = 20;
my $rows_per_txn = 100;

my $insert_script = $PostgreSQL::Test::Utils::tmp_check . '/cs_insert.sql';
append_to_file($insert_script,
	"INSERT INTO t SELECT g, 'x' || g FROM generate_series(1, $rows_per_txn) g;\n");

my $vacuum_script = $PostgreSQL::Test::Utils::tmp_check . '/cs_vacuum.sql';
append_to_file($vacuum_script, "VACUUM t;\n");

# Phase A: parallel insert streams with no VACUUM.  Exercises the
# insert-vs-insert metapage merge: every stream's delta extension must
# survive every other stream's metapage update.
$node->command_ok(
	[
		'pgbench', '-n', '-c', $clients, '-t', $txns,
		'-f', $insert_script,
		'-h', $node->host, '-p', $node->port, 'postgres'
	],
	'parallel insert streams');

my $expected = $clients * $txns * $rows_per_txn;
my $count = $node->safe_psql('postgres', 'SELECT count(*) FROM t');
cmp_ok($count, '==', $expected, "no rows lost by concurrent inserts");

# Phase B: insert streams racing VACUUM.  Exercises the compaction
# fence (no tuple added to an already-collected page) and the
# delta-advance metapage flip preserving concurrent extensions.
$node->command_ok(
	[
		'pgbench', '-n', '-c', $clients + 1, '-t', $txns,
		'-f', "$insert_script\@$clients",
		'-f', "$vacuum_script\@1",
		'-h', $node->host, '-p', $node->port, 'postgres'
	],
	'insert streams racing VACUUM');

# weights are probabilistic, so derive the expectation from the actual
# number of insert transactions pgbench ran
my $insert_txns = $node->safe_psql('postgres',
	'SELECT (count(*) - ' . $expected . ') / ' . $rows_per_txn . ' FROM t');
$count = $node->safe_psql('postgres', 'SELECT count(*) FROM t');
cmp_ok(($count - $expected) % $rows_per_txn,
	'==', 0, "rows lost to a VACUUM race would break txn granularity");
ok($count > $expected, "insert phase B added rows");
$expected = $count;

# Phase C: one bulk DELETE racing repeated VACUUMs.  Exercises
# tombstone insertion and materialization racing compaction.
my $deleted = $node->safe_psql('postgres',
	'SELECT count(*) FROM t WHERE id % 7 = 0');
my $h = $node->background_psql('postgres');
$h->query_until(qr/start/, "\\echo start\n");

# Drive the concurrent VACUUMs with IPC::Run::start, not fork()+exec.  perl's
# fork is emulated on Windows (ithreads); a pseudo-fork that then exec()s
# corrupts the parent's open file handles -- including the background_psql
# pipes -- so the DELETE's psql "ends prematurely" (and worse on some MinGW
# builds).  IPC::Run::start is the portable way to run a child concurrently.
my @vacuum_cmd = (
	'pgbench', '-n', '-c', 1, '-t', 10,
	'-f', $vacuum_script,
	'-h', $node->host, '-p', $node->port, 'postgres');
my $vacuum_run = start(\@vacuum_cmd);
$h->query_safe('DELETE FROM t WHERE id % 7 = 0');
$h->quit;
$vacuum_run->finish;

$node->safe_psql('postgres', 'VACUUM t');
$count = $node->safe_psql('postgres', 'SELECT count(*) FROM t');
cmp_ok($count, '==', $expected - $deleted,
	"DELETE racing VACUUM kept exact count");

# Final integrity pass: another VACUUM must not change visible contents.
my $sum_before = $node->safe_psql('postgres', 'SELECT sum(id) FROM t');
$node->safe_psql('postgres', 'VACUUM t');
my $sum_after = $node->safe_psql('postgres', 'SELECT sum(id) FROM t');
cmp_ok($sum_after, '==', $sum_before, "VACUUM preserved table contents");

$node->stop;
done_testing();
