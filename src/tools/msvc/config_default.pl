# Configuration arguments for vcbuild.
use strict;
use warnings;

our $config = {
<<<<<<< HEAD
<<<<<<< HEAD
    asserts=>1,			# --enable-cassert
    # integer_datetimes=>1,   # --enable-integer-datetimes - on is now default
    # float4byval=>1,         # --disable-float4-byval, on by default
    # float8byval=>1,         # --disable-float8-byval, on by default
    # blocksize => 32,        # --with-blocksize, 8kB by default
    # wal_blocksize => 8,     # --with-wal-blocksize, 8kB by default
    # wal_segsize => 16,      # --with-wal-segsize, 16MB by default
    ldap=>1,				# --with-ldap
    nls=>undef,				# --enable-nls=<path>
    tcl=>undef,				# --with-tls=<path>
    perl=>undef, 			# --with-perl
    python=>undef,			# --with-python=<path>
    krb5=>undef,			# --with-krb5=<path>
    openssl=>undef,			# --with-ssl=<path>
    uuid=>undef,			# --with-ossp-uuid
    xml=>undef,				# --with-libxml=<path>
    xslt=>undef,			# --with-libxslt=<path>
    iconv=>undef,			# (not in configure, path to iconv)
    zlib=>'c:\zlib64',			# --with-zlib=<path>  (GPDB needs zlib)
    pthread=>'c:\pthreads',  		# gpdb needs pthreads 
    curl=>'c:\zlib', 			# gpdb needs libcurl
    bz2=>'c:\pgbuild\bzlib'
    #readline=>'c:\progra~1\GnuWin32' 	# readline for windows?
=======
	asserts=>0,			# --enable-cassert
	# integer_datetimes=>1,   # --enable-integer-datetimes - on is now default
	# float4byval=>1,         # --disable-float4-byval, on by default
	# float8byval=>0,         # --disable-float8-byval, off by default
	# blocksize => 8,         # --with-blocksize, 8kB by default
	# wal_blocksize => 8,     # --with-wal-blocksize, 8kB by default
	# wal_segsize => 16,      # --with-wal-segsize, 16MB by default
	ldap=>1,				# --with-ldap
	nls=>undef,				# --enable-nls=<path>
	tcl=>undef,				# --with-tls=<path>
	perl=>undef, 			# --with-perl
	python=>undef,			# --with-python=<path>
	krb5=>undef,			# --with-krb5=<path>
	openssl=>undef,			# --with-ssl=<path>
	uuid=>undef,			# --with-ossp-uuid
	xml=>undef,				# --with-libxml=<path>
	xslt=>undef,			# --with-libxslt=<path>
	iconv=>undef,			# (not in configure, path to iconv)
	zlib=>undef				# --with-zlib=<path>
>>>>>>> 80edfd76591fdb9beec061de3c05ef4e9d96ce56
=======
	asserts => 0,    # --enable-cassert
	  # integer_datetimes=>1,   # --enable-integer-datetimes - on is now default
	  # float4byval=>1,         # --disable-float4-byval, on by default

	# float8byval=> $platformbits == 64, # --disable-float8-byval,
	# off by default on 32 bit platforms, on by default on 64 bit platforms

	# blocksize => 8,         # --with-blocksize, 8kB by default
	# wal_blocksize => 8,     # --with-wal-blocksize, 8kB by default
	# wal_segsize => 16,      # --with-wal-segsize, 16MB by default
	ldap     => 1,        # --with-ldap
	extraver => undef,    # --with-extra-version=<string>
	nls      => undef,    # --enable-nls=<path>
	tcl      => undef,    # --with-tls=<path>
	perl     => undef,    # --with-perl
	python   => undef,    # --with-python=<path>
	openssl  => undef,    # --with-openssl=<path>
	uuid     => undef,    # --with-ossp-uuid
	xml      => undef,    # --with-libxml=<path>
	xslt     => undef,    # --with-libxslt=<path>
	iconv    => undef,    # (not in configure, path to iconv)
	zlib     => undef     # --with-zlib=<path>
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
};

1;
