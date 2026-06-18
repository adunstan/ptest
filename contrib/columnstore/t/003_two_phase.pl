# Copyright (c) 2026, PostgreSQL Global Development Group

# Two-phase commit against columnstore tables: a prepared transaction's
# inserts, deletes and row locks pin the delta store's visibility
# horizon, so VACUUM compaction must leave its work untouched; the
# prepared state must survive a server restart; and both commit and
# rollback outcomes must read correctly afterwards (including through
# another compaction).

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('cs_twophase');
$node->init;
$node->append_conf('postgresql.conf', 'max_prepared_transactions = 5');
$node->start;

$node->safe_psql('postgres', 'CREATE EXTENSION columnstore');
$node->safe_psql('postgres',
	'CREATE TABLE tp (v int) USING columnstore');
$node->safe_psql('postgres',
	'INSERT INTO tp SELECT g FROM generate_series(1, 1000) g');
$node->safe_psql('postgres', 'VACUUM tp');

# Prepare one transaction that inserts and deletes, and another that
# only inserts (it will be rolled back).
$node->safe_psql(
	'postgres', q{
BEGIN;
INSERT INTO tp VALUES (5001);
DELETE FROM tp WHERE v <= 10;
PREPARE TRANSACTION 'cs_commit_me';
});
$node->safe_psql(
	'postgres', q{
BEGIN;
INSERT INTO tp SELECT g FROM generate_series(6001, 6100) g;
DELETE FROM tp WHERE v > 990;
PREPARE TRANSACTION 'cs_abort_me';
});

# Compaction with both transactions pending must not consume or
# materialize their work.
$node->safe_psql('postgres', 'VACUUM tp');
cmp_ok($node->safe_psql('postgres', 'SELECT count(*) FROM tp'),
	'==', 1000, 'pending prepared work invisible after VACUUM');

# Prepared state survives a restart.
$node->restart;

$node->safe_psql('postgres', "COMMIT PREPARED 'cs_commit_me'");
$node->safe_psql('postgres', "ROLLBACK PREPARED 'cs_abort_me'");

is( $node->safe_psql('postgres',
		'SELECT count(*), min(v), max(v) FROM tp') =~ s/\r//gr,
	'991|11|5001',
	'committed prepared work visible, rolled-back work gone');

$node->safe_psql('postgres', 'VACUUM tp');
is( $node->safe_psql('postgres',
		'SELECT count(*), min(v), max(v) FROM tp') =~ s/\r//gr,
	'991|11|5001',
	'unchanged after compaction');

done_testing();
