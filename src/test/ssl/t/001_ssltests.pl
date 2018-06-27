use strict;
use warnings;
<<<<<<< HEAD
use PostgresNode;
use TestLib;
use Test::More tests => 29;
use ServerSetup;
use File::Copy;

=======
use TestLib;
use Test::More tests => 38;
use ServerSetup;
use File::Copy;

# Like TestLib.pm, we use IPC::Run
BEGIN
{
	eval {
		require IPC::Run;
		import IPC::Run qw(run start);
		1;
	} or do
	{
		plan skip_all => "IPC::Run not available";
	  }
}

>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
#### Some configuration

# This is the hostname used to connect to the server. This cannot be a
# hostname, because the server certificate is always for the domain
# postgresql-ssl-regression.test.
my $SERVERHOSTADDR = '127.0.0.1';

<<<<<<< HEAD
=======
my $tempdir = TestLib::tempdir;

#my $tempdir = "tmp_check";


>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
# Define a couple of helper functions to test connecting to the server.

my $common_connstr;

sub run_test_psql
{
	my $connstr   = $_[0];
	my $logstring = $_[1];

	my $cmd = [
<<<<<<< HEAD
		'psql', '-X', '-A', '-t', '-c', "'SELECT version();'",
		'-d', "'" . "$connstr" . "'" ];

	my $result = run_log($cmd);
=======
		'psql', '-A', '-t', '-c', "SELECT 'connected with $connstr'",
		'-d', "$connstr" ];

	open CLIENTLOG, ">>$tempdir/client-log"
	  or die "Could not open client-log file";
	print CLIENTLOG "\n# Running test: $connstr $logstring\n";
	close CLIENTLOG;

	my $result = run $cmd, '>>', "$tempdir/client-log", '2>&1';
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
	return $result;
}

#
# The first argument is a (part of a) connection string, and it's also printed
# out as the test case name. It is appended to $common_connstr global variable,
# which also contains a libpq connection string.
<<<<<<< HEAD
=======
#
# The second argument is a hostname to connect to.
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
sub test_connect_ok
{
	my $connstr = $_[0];

	my $result =
	  run_test_psql("$common_connstr $connstr", "(should succeed)");
	ok($result, $connstr);
}

sub test_connect_fails
{
	my $connstr = $_[0];

	my $result = run_test_psql("$common_connstr $connstr", "(should fail)");
	ok(!$result, "$connstr (should fail)");
}

<<<<<<< HEAD
# The client's private key must not be world-readable, so take a copy
# of the key stored in the code tree and update its permissions.
copy("ssl/client.key", "ssl/client_tmp.key");
chmod 0600, "ssl/client_tmp.key";

#### Part 0. Set up the server.

note "setting up data directory";
my $node = get_new_demo_node();

$node->init;
configure_test_server_for_ssl($node, $SERVERHOSTADDR);
switch_server_cert($node, 'server-cn-only');
=======
# The client's private key must not be world-readable. Git doesn't track
# permissions (except for the executable bit), so they might be wrong after
# a checkout.
chmod 0600, "ssl/client.key";

#### Part 0. Set up the server.

diag "setting up data directory in \"$tempdir\"...";
start_test_server($tempdir);
configure_test_server_for_ssl($tempdir);
switch_server_cert($tempdir, 'server-cn-only');
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

### Part 1. Run client-side tests.
###
### Test that libpq accepts/rejects the connection correctly, depending
### on sslmode and whether the server's certificate looks correct. No
### client certificate is used in these tests.

<<<<<<< HEAD
note "running client tests";
=======
diag "running client tests...";
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

$common_connstr =
"user=ssltestuser dbname=trustdb sslcert=invalid hostaddr=$SERVERHOSTADDR host=common-name.pg-ssltest.test";

# The server should not accept non-SSL connections
<<<<<<< HEAD
note "test that the server doesn't accept non-SSL connections";
=======
diag "test that the server doesn't accept non-SSL connections";
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
test_connect_fails("sslmode=disable");

# Try without a root cert. In sslmode=require, this should work. In verify-ca
# or verify-full mode it should fail
<<<<<<< HEAD
note "connect without server root cert";
=======
diag "connect without server root cert";
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
test_connect_ok("sslrootcert=invalid sslmode=require");
test_connect_fails("sslrootcert=invalid sslmode=verify-ca");
test_connect_fails("sslrootcert=invalid sslmode=verify-full");

# Try with wrong root cert, should fail. (we're using the client CA as the
# root, but the server's key is signed by the server CA)
<<<<<<< HEAD
note "connect without wrong server root cert";
=======
diag "connect without wrong server root cert";
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
test_connect_fails("sslrootcert=ssl/client_ca.crt sslmode=require");
test_connect_fails("sslrootcert=ssl/client_ca.crt sslmode=verify-ca");
test_connect_fails("sslrootcert=ssl/client_ca.crt sslmode=verify-full");

# Try with just the server CA's cert. This fails because the root file
# must contain the whole chain up to the root CA.
<<<<<<< HEAD
note "connect with server CA cert, without root CA";
test_connect_fails("sslrootcert=ssl/server_ca.crt sslmode=verify-ca");

# And finally, with the correct root cert.
note "connect with correct server CA cert file";
=======
diag "connect with server CA cert, without root CA";
test_connect_fails("sslrootcert=ssl/server_ca.crt sslmode=verify-ca");

# And finally, with the correct root cert.
diag "connect with correct server CA cert file";
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
test_connect_ok("sslrootcert=ssl/root+server_ca.crt sslmode=require");
test_connect_ok("sslrootcert=ssl/root+server_ca.crt sslmode=verify-ca");
test_connect_ok("sslrootcert=ssl/root+server_ca.crt sslmode=verify-full");

# Test with cert root file that contains two certificates. The client should
# be able to pick the right one, regardless of the order in the file.
test_connect_ok("sslrootcert=ssl/both-cas-1.crt sslmode=verify-ca");
test_connect_ok("sslrootcert=ssl/both-cas-2.crt sslmode=verify-ca");

<<<<<<< HEAD
note "testing sslcrl option with a non-revoked cert";
=======
diag "testing sslcrl option with a non-revoked cert";
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

# Invalid CRL filename is the same as no CRL, succeeds
test_connect_ok(
	"sslrootcert=ssl/root+server_ca.crt sslmode=verify-ca sslcrl=invalid");

# A CRL belonging to a different CA is not accepted, fails
test_connect_fails(
"sslrootcert=ssl/root+server_ca.crt sslmode=verify-ca sslcrl=ssl/client.crl");

# With the correct CRL, succeeds (this cert is not revoked)
test_connect_ok(
"sslrootcert=ssl/root+server_ca.crt sslmode=verify-ca sslcrl=ssl/root+server.crl"
);

# Check that connecting with verify-full fails, when the hostname doesn't
# match the hostname in the server's certificate.
<<<<<<< HEAD
note "test mismatch between hostname and server certificate";
=======
diag "test mismatch between hostname and server certificate";
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
$common_connstr =
"user=ssltestuser dbname=trustdb sslcert=invalid sslrootcert=ssl/root+server_ca.crt hostaddr=$SERVERHOSTADDR sslmode=verify-full";

test_connect_ok("sslmode=require host=wronghost.test");
test_connect_ok("sslmode=verify-ca host=wronghost.test");
test_connect_fails("sslmode=verify-full host=wronghost.test");

# Test Subject Alternative Names.
<<<<<<< HEAD
#switch_server_cert($node, 'server-multiple-alt-names');
#
#note "test hostname matching with X.509 Subject Alternative Names";
#print "test hostname matching with X.509 Subject Alternative Names";
#$common_connstr =
#"user=ssltestuser dbname=trustdb sslcert=invalid sslrootcert=ssl/root+server_ca.crt hostaddr=$SERVERHOSTADDR sslmode=verify-full";
#
#test_connect_ok("host=dns1.alt-name.pg-ssltest.test");
#test_connect_ok("host=dns2.alt-name.pg-ssltest.test");
#test_connect_ok("host=foo.wildcard.pg-ssltest.test");
#
#test_connect_fails("host=wronghost.alt-name.pg-ssltest.test");
#test_connect_fails("host=deep.subdomain.wildcard.pg-ssltest.test");
#
## Test certificate with a single Subject Alternative Name. (this gives a
## slightly different error message, that's all)
#switch_server_cert($node, 'server-single-alt-name');

#note "test hostname matching with a single X.509 Subject Alternative Name";
#print "test hostname matching with a single X.509 Subject Alternative Name";
#$common_connstr =
#"user=ssltestuser dbname=trustdb sslcert=invalid sslrootcert=ssl/root+server_ca.crt hostaddr=$SERVERHOSTADDR sslmode=verify-full";
#
#test_connect_ok("host=single.alt-name.pg-ssltest.test");
#
#test_connect_fails("host=wronghost.alt-name.pg-ssltest.test");
#test_connect_fails("host=deep.subdomain.wildcard.pg-ssltest.test");

# Test server certificate with a CN and SANs. Per RFCs 2818 and 6125, the CN
# should be ignored when the certificate has both.
#switch_server_cert($node, 'server-cn-and-alt-names');
#
#note "test certificate with both a CN and SANs";
#print "test certificate with both a CN and SANs";
#$common_connstr =
#"user=ssltestuser dbname=trustdb sslcert=invalid sslrootcert=ssl/root+server_ca.crt hostaddr=$SERVERHOSTADDR sslmode=verify-full";
#
#test_connect_ok("host=dns1.alt-name.pg-ssltest.test");
#test_connect_ok("host=dns2.alt-name.pg-ssltest.test");
#test_connect_fails("host=common-name.pg-ssltest.test");

# Finally, test a server certificate that has no CN or SANs. Of course, that's
# not a very sensible certificate, but libpq should handle it gracefully.
switch_server_cert($node, 'server-no-names');
=======
switch_server_cert($tempdir, 'server-multiple-alt-names');

diag "test hostname matching with X509 Subject Alternative Names";
$common_connstr =
"user=ssltestuser dbname=trustdb sslcert=invalid sslrootcert=ssl/root+server_ca.crt hostaddr=$SERVERHOSTADDR sslmode=verify-full";

test_connect_ok("host=dns1.alt-name.pg-ssltest.test");
test_connect_ok("host=dns2.alt-name.pg-ssltest.test");
test_connect_ok("host=foo.wildcard.pg-ssltest.test");

test_connect_fails("host=wronghost.alt-name.pg-ssltest.test");
test_connect_fails("host=deep.subdomain.wildcard.pg-ssltest.test");

# Test certificate with a single Subject Alternative Name. (this gives a
# slightly different error message, that's all)
switch_server_cert($tempdir, 'server-single-alt-name');

diag "test hostname matching with a single X509 Subject Alternative Name";
$common_connstr =
"user=ssltestuser dbname=trustdb sslcert=invalid sslrootcert=ssl/root+server_ca.crt hostaddr=$SERVERHOSTADDR sslmode=verify-full";

test_connect_ok("host=single.alt-name.pg-ssltest.test");

test_connect_fails("host=wronghost.alt-name.pg-ssltest.test");
test_connect_fails("host=deep.subdomain.wildcard.pg-ssltest.test");

# Test server certificate with a CN and SANs. Per RFCs 2818 and 6125, the CN
# should be ignored when the certificate has both.
switch_server_cert($tempdir, 'server-cn-and-alt-names');

diag "test certificate with both a CN and SANs";
$common_connstr =
"user=ssltestuser dbname=trustdb sslcert=invalid sslrootcert=ssl/root+server_ca.crt hostaddr=$SERVERHOSTADDR sslmode=verify-full";

test_connect_ok("host=dns1.alt-name.pg-ssltest.test");
test_connect_ok("host=dns2.alt-name.pg-ssltest.test");
test_connect_fails("host=common-name.pg-ssltest.test");

# Finally, test a server certificate that has no CN or SANs. Of course, that's
# not a very sensible certificate, but libpq should handle it gracefully.
switch_server_cert($tempdir, 'server-no-names');
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
$common_connstr =
"user=ssltestuser dbname=trustdb sslcert=invalid sslrootcert=ssl/root+server_ca.crt hostaddr=$SERVERHOSTADDR";

test_connect_ok("sslmode=verify-ca host=common-name.pg-ssltest.test");
test_connect_fails("sslmode=verify-full host=common-name.pg-ssltest.test");

# Test that the CRL works
<<<<<<< HEAD
note "testing client-side CRL";
switch_server_cert($node, 'server-revoked');
=======
diag "Testing client-side CRL";
switch_server_cert($tempdir, 'server-revoked');
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

$common_connstr =
"user=ssltestuser dbname=trustdb sslcert=invalid hostaddr=$SERVERHOSTADDR host=common-name.pg-ssltest.test";

# Without the CRL, succeeds. With it, fails.
test_connect_ok("sslrootcert=ssl/root+server_ca.crt sslmode=verify-ca");
test_connect_fails(
"sslrootcert=ssl/root+server_ca.crt sslmode=verify-ca sslcrl=ssl/root+server.crl"
);

### Part 2. Server-side tests.
###
### Test certificate authorization.

<<<<<<< HEAD
note "testing certificate authorization";
=======
diag "Testing certificate authorization...";
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
$common_connstr =
"sslrootcert=ssl/root+server_ca.crt sslmode=require dbname=certdb hostaddr=$SERVERHOSTADDR";

# no client cert
test_connect_fails("user=ssltestuser sslcert=invalid");

# correct client cert
test_connect_ok(
<<<<<<< HEAD
	"user=ssltestuser sslcert=ssl/client.crt sslkey=ssl/client_tmp.key");

# client cert belonging to another user
test_connect_fails(
	"user=anotheruser sslcert=ssl/client.crt sslkey=ssl/client_tmp.key");
=======
	"user=ssltestuser sslcert=ssl/client.crt sslkey=ssl/client.key");

# client cert belonging to another user
test_connect_fails(
	"user=anotheruser sslcert=ssl/client.crt sslkey=ssl/client.key");
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

# revoked client cert
test_connect_fails(
"user=ssltestuser sslcert=ssl/client-revoked.crt sslkey=ssl/client-revoked.key"
);

<<<<<<< HEAD
# intermediate client_ca.crt is provided by client, and isn't in server's ssl_ca_file
switch_server_cert($node, 'server-cn-only', 'root_ca');
$common_connstr =
"user=ssltestuser dbname=certdb sslkey=ssl/client_tmp.key sslrootcert=ssl/root+server_ca.crt hostaddr=$SERVERHOSTADDR";

test_connect_ok("sslmode=require sslcert=ssl/client+client_ca.crt");
test_connect_fails("sslmode=require sslcert=ssl/client.crt");

# clean up
unlink "ssl/client_tmp.key";
=======

# All done! Save the log, before the temporary installation is deleted
copy("$tempdir/client-log", "./client-log");
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
