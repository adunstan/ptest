# Copyright (c) 2022-2026, PostgreSQL Global Development Group

# Verify the transaction-ID horizon guard for global temporary tables:
# session-local GTT data contributes nothing to datfrozenxid, so unless the
# session freezes it (by VACUUMing the table) its oldest row can fall behind
# the cluster commit-log truncation horizon and no longer be readable.
# Accessing such a table must be refused with an error (fail closed) rather
# than risk a wrong result, and a warning must be issued as the data approaches
# the horizon.  VACUUM is the escape hatch: it freezes the data in place and
# lifts the block without losing it.  The hard error and warning cannot be
# triggered deterministically in the standard regression suite, so they are
# exercised here on a dedicated cluster whose frozen horizon we advance by hand.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('gtt_xid_horizon');
$node->init;
# Disable autovacuum so the frozen horizon only moves when we say so, and make
# VACUUM FREEZE advance relfrozenxid all the way to the current xmin.
$node->append_conf(
	'postgresql.conf', q{
autovacuum = off
vacuum_freeze_min_age = 0
vacuum_freeze_table_age = 0
});
$node->start;

# Advance the cluster-wide CLOG-truncation horizon (TransamVariables->oldestXid,
# the minimum datfrozenxid across all databases) past all current data by
# freezing every database, including template0.
sub freeze_all_databases
{
	$node->safe_psql('postgres',
		"UPDATE pg_database SET datallowconn = true WHERE datname = 'template0'"
	);
	$node->safe_psql($_, 'VACUUM (FREEZE)')
	  for (qw(template0 template1 postgres));
	$node->safe_psql('postgres',
		"UPDATE pg_database SET datallowconn = false WHERE datname = 'template0'"
	);
}

# A persistent session.  GTT data and its horizon tracking are per-session, so
# everything that observes them must run on this one connection.
my $s = $node->background_psql('postgres', on_error_stop => 0);

# Server-side helper to consume transaction IDs (advance the next XID) without
# needing a fresh connection per XID.
$s->query_safe(
	q{CREATE PROCEDURE burn(n int) LANGUAGE plpgsql AS $$
	  BEGIN FOR i IN 1..n LOOP PERFORM pg_current_xact_id(); COMMIT; END LOOP; END $$});

$s->query_safe("CREATE GLOBAL TEMPORARY TABLE g (a int)");
$s->query_safe("INSERT INTO g VALUES (1)");

# 1. Default margin: freshly written data must not warn.
$s->{stderr} = '';
$s->query("SELECT count(*) FROM g");
is($s->{stderr}, '',
	'no false-positive warning at default margin on fresh data');

# 2. As the data ages toward the horizon (small margin, many XIDs burned so
#    the CLOG history exceeds it), a warning is issued -- once.
$s->query_safe("SET global_temp_xid_warn_margin = 100");
$s->query_safe("CALL burn(500)");
$s->{stderr} = '';
$s->query("SELECT count(*) FROM g");
like(
	$s->{stderr},
	qr/approaching the transaction-ID horizon/,
	'warning fires as GTT data approaches the horizon');

$s->{stderr} = '';
$s->query("SELECT count(*) FROM g");
is($s->{stderr}, '',
	'approaching-horizon warning is throttled to once per relation');

# 3. Once the horizon is advanced past the data, access is refused.
freeze_all_databases();
$s->{stderr} = '';
$s->query("SELECT count(*) FROM g");
like(
	$s->{stderr},
	qr/older than the transaction-ID horizon/,
	'reading GTT data older than the horizon is refused with an error');

# The write path is guarded too.
$s->{stderr} = '';
$s->query("INSERT INTO g VALUES (2)");
like(
	$s->{stderr},
	qr/older than the transaction-ID horizon/,
	'writing to a GTT whose data is older than the horizon is refused');

# 4. VACUUM is the escape hatch.  It does not go through the guarded read/write
#    entry points, so it runs even while plain access is being refused; it
#    freezes this session's data in place (the rows were read above, so their
#    commit-status hint bits are set and freezing needs no truncated CLOG) and
#    advances the per-session relfrozenxid past the horizon.  The table is then
#    readable and writable again with no data lost.  (Reset the accumulated
#    stderr from the expected errors above first.)
$s->{stderr} = '';
$s->query_safe("VACUUM g");
$s->{stderr} = '';
is($s->query("SELECT count(*) FROM g"),
	'1', 'VACUUM freezes a trapped GTT in place; its single row survives');
is($s->{stderr}, '',
	'reading the GTT no longer errors once VACUUM has frozen its data');
$s->{stderr} = '';
$s->query_safe("INSERT INTO g VALUES (2)");
is($s->{stderr}, '',
	'writing to the GTT no longer errors once VACUUM has frozen its data');

# 5. TRUNCATE clears the per-session tracking; fresh data is accessible again
#    even though the horizon has advanced.
$s->{stderr} = '';
$s->query_safe("TRUNCATE g");
$s->query_safe("INSERT INTO g VALUES (3)");
$s->{stderr} = '';
$s->query("SELECT count(*) FROM g");
is($s->{stderr}, '',
	'TRUNCATE resets horizon tracking; fresh data is accessible again');

$s->quit;
$node->stop;

done_testing();
