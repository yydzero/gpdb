# This module sets up a test server, for the SSL regression tests.
#
# The server is configured as follows:
#
# - SSL enabled, with the server certificate specified by argument to
#   switch_server_cert function.
# - ssl/root+client_ca.crt as the CA root for validating client certs.
# - reject non-SSL connections
# - a database called trustdb that lets anyone in
<<<<<<< HEAD
# - another database called certdb that uses certificate authentication, ie.
=======
# - another database called certdb that uses certificate authentiction, ie.
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
#   the client must present a valid certificate signed by the client CA
# - two users, called ssltestuser and anotheruser.
#
# The server is configured to only accept connections from localhost. If you
# want to run the client from another host, you'll have to configure that
# manually.
package ServerSetup;

use strict;
use warnings;
<<<<<<< HEAD
use PostgresNode;
=======
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
use TestLib;
use File::Basename;
use File::Copy;
use Test::More;

use Exporter 'import';
our @EXPORT = qw(
  configure_test_server_for_ssl switch_server_cert
);

# Copy a set of files, taking into account wildcards
sub copy_files
{
	my $orig = shift;
	my $dest = shift;

	my @orig_files = glob $orig;
	foreach my $orig_file (@orig_files)
	{
		my $base_file = basename($orig_file);
		copy($orig_file, "$dest/$base_file")
		  or die "Could not copy $orig_file to $dest";
	}
}

<<<<<<< HEAD
sub configure_test_server_for_ssl
{
	my $node       = $_[0];
	my $serverhost = $_[1];

	my $pgdata = $ENV{MASTER_DATA_DIRECTORY};

	# Prevent duplicate config lines in postgresql.conf
	my $exist = `grep sslconfig $pgdata/postgresql.conf`;

	if (!$exist)
	{
		# Create test users and databases
		system_or_bail('createuser ssltestuser -s');
		system_or_bail('createuser anotheruser -s');
		system_or_bail('createdb trustdb');
		system_or_bail('createdb certdb');

		# enable logging etc.
		open my $conf, '>>', "$pgdata/postgresql.conf";

		# enable SSL and set up server key
		print $conf "include 'sslconfig.conf'";

		close $conf;

		# ssl configuration will be placed here
		open my $sslconf, '>', "$pgdata/sslconfig.conf";
		print $sslconf "ssl=on\n";
		close $sslconf;

		# Copy all server certificates and keys, and client root cert, to the data dir
		copy_files("ssl/server.crt", $pgdata);
		copy_files("ssl/server.key", $pgdata);
		copy_files("ssl/server-*.crt", $pgdata);
		copy_files("ssl/server-*.key", $pgdata);
		chmod(0600, glob "$pgdata/server*.key") or die $!;
		copy_files("ssl/root+client_ca.crt", $pgdata);
		copy_files("ssl/root_ca.crt",        $pgdata);
		copy_files("ssl/root+client.crl",    $pgdata);

		# Stop and restart server to load ssl configs.
		$node->restart;
	}

	# Change pg_hba after restart because hostssl requires ssl=on
	configure_hba_for_ssl($node, $serverhost);
}

# Change the configuration to use given server cert file, and reload
# the server so that the configuration takes effect.
sub switch_server_cert
{
	my $node     = $_[0];
	my $certfile = $_[1];
	my $cafile   = $_[2] || "root+client_ca";
	my $pgdata   = $ENV{MASTER_DATA_DIRECTORY};

	note
	  "reloading server with certfile \"$certfile\" and cafile \"$cafile\"";

	system_or_bail("cp $pgdata/$certfile.crt $pgdata/server.crt");
	system_or_bail("cp $pgdata/$certfile.key $pgdata/server.key");
	system_or_bail("cp $pgdata/$cafile.crt $pgdata/root.crt");
	system_or_bail("cp $pgdata/root+client.crl $pgdata/root.crl");
#	print $sslconf "ssl_ca_file='$cafile.crt'\n";
#	print $sslconf "ssl_cert_file='$certfile.crt'\n";
#	print $sslconf "ssl_key_file='$certfile.key'\n";
#	print $sslconf "ssl_crl_file='root+client.crl'\n";

	$node->restart_qd;
}

sub configure_hba_for_ssl
{
	my $node       = $_[0];
	my $serverhost = $_[1];
	my $pgdata   = $ENV{MASTER_DATA_DIRECTORY};
=======
# Perform chmod on a set of files, taking into account wildcards
sub chmod_files
{
	my $mode = shift;
	my $file_expr = shift;

	my @all_files = glob $file_expr;
	foreach my $file_entry (@all_files)
	{
		chmod $mode, $file_entry
		  or die "Could not run chmod with mode $mode on $file_entry";
	}
}

sub configure_test_server_for_ssl
{
	my $tempdir = $_[0];

	# Create test users and databases
	psql 'postgres', "CREATE USER ssltestuser";
	psql 'postgres', "CREATE USER anotheruser";
	psql 'postgres', "CREATE DATABASE trustdb";
	psql 'postgres', "CREATE DATABASE certdb";

	# enable logging etc.
	open CONF, ">>$tempdir/pgdata/postgresql.conf";
	print CONF "fsync=off\n";
	print CONF "log_connections=on\n";
	print CONF "log_hostname=on\n";
	print CONF "log_statement=all\n";

	# enable SSL and set up server key
	print CONF "include 'sslconfig.conf'";

	close CONF;

# Copy all server certificates and keys, and client root cert, to the data dir
	copy_files("ssl/server-*.crt", "$tempdir/pgdata");
	copy_files("ssl/server-*.key", "$tempdir/pgdata");
	chmod_files(0600, "$tempdir/pgdata/server-*.key");
	copy_files("ssl/root+client_ca.crt", "$tempdir/pgdata");
	copy_files("ssl/root+client.crl",    "$tempdir/pgdata");
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

  # Only accept SSL connections from localhost. Our tests don't depend on this
  # but seems best to keep it as narrow as possible for security reasons.
  #
  # When connecting to certdb, also check the client certificate.
<<<<<<< HEAD

	open my $hba, '>>', "$pgdata/pg_hba.conf";

	print $hba
"hostssl trustdb         ssltestuser     $serverhost/32            trust\n";
	print $hba
"hostssl trustdb         ssltestuser     ::1/128                 trust\n";
	print $hba
"hostssl certdb          ssltestuser     $serverhost/32            cert\n";
	print $hba
"hostssl certdb          ssltestuser     ::1/128                 cert\n";
	close $hba;
=======
	open HBA, ">$tempdir/pgdata/pg_hba.conf";
	print HBA
"# TYPE  DATABASE        USER            ADDRESS                 METHOD\n";
	print HBA
"hostssl trustdb         ssltestuser     127.0.0.1/32            trust\n";
	print HBA
"hostssl trustdb         ssltestuser     ::1/128                 trust\n";
	print HBA
"hostssl certdb          ssltestuser     127.0.0.1/32            cert\n";
	print HBA
"hostssl certdb          ssltestuser     ::1/128                 cert\n";
	close HBA;
}

# Change the configuration to use given server cert file, and restart
# the server so that the configuration takes effect.
sub switch_server_cert
{
	my $tempdir  = $_[0];
	my $certfile = $_[1];

	diag "Restarting server with certfile \"$certfile\"...";

	open SSLCONF, ">$tempdir/pgdata/sslconfig.conf";
	print SSLCONF "ssl=on\n";
	print SSLCONF "ssl_ca_file='root+client_ca.crt'\n";
	print SSLCONF "ssl_cert_file='$certfile.crt'\n";
	print SSLCONF "ssl_key_file='$certfile.key'\n";
	print SSLCONF "ssl_crl_file='root+client.crl'\n";
	close SSLCONF;

   # Stop and restart server to reload the new config. We cannot use
   # restart_test_server() because that overrides listen_addresses to only all
   # Unix domain socket connections.

	system_or_bail 'pg_ctl', 'stop',  '-s', '-D', "$tempdir/pgdata", '-w';
	system_or_bail 'pg_ctl', 'start', '-s', '-D', "$tempdir/pgdata", '-w',
	  '-l',
	  "$tempdir/logfile";
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
}
