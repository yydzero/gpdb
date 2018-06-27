# contrib/Makefile

subdir = contrib
top_builddir = ..
include $(top_builddir)/src/Makefile.global

SUBDIRS = \
		auth_delay	\
		auto_explain	\
		btree_gin	\
		citext		\
		dblink		\
		dict_int	\
		dict_xsyn	\
<<<<<<< HEAD
		dummy_seclabel	\
=======
		earthdistance	\
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
		file_fdw	\
		fuzzystrmatch	\
		hstore		\
		intarray	\
		pageinspect	\
		passwordcheck	\
		pg_buffercache	\
		pg_freespacemap \
		pg_prewarm	\
		pg_standby	\
		pg_stat_statements \
		pg_trgm		\
		pgcrypto	\
		pgrowlocks	\
		pgstattuple	\
<<<<<<< HEAD
=======
		postgres_fdw	\
		seg		\
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
		spi		\
		tcn		\
		test_decoding	\
		tsm_system_rows \
		tsm_system_time \
		tsearch2	\
		unaccent


# GPDB_92_MERGE_FIXME: Why some files (especially Makefile) are missing under
# these directories?
#       seg         \
		tablefunc	\
		vacuumlo

# Greenplum-specific additions (to ease merge pain).
SUBDIRS += \
		xlogdump \
		formatter \
		formatter_fixedwidth \
		extprotocol \
		gp_distribution_policy \
		gp_internal_tools \
		gp_inject_fault \
		gp_cancel_query \
		gp_sparse_vector \
		indexscan \

ifeq ($(with_openssl),yes)
SUBDIRS += sslinfo
else
ALWAYS_SUBDIRS += sslinfo
endif

ifneq ($(with_uuid),no)
SUBDIRS += uuid-ossp
else
ALWAYS_SUBDIRS += uuid-ossp
endif

ifeq ($(with_libxml),yes)
SUBDIRS += xml2
else
ALWAYS_SUBDIRS += xml2
endif

ifeq ($(with_selinux),yes)
SUBDIRS += sepgsql
else
ALWAYS_SUBDIRS += sepgsql
endif

<<<<<<< HEAD
ifeq ($(enable_orca),yes)
SUBDIRS += orca_debug
endif

SUBDIRS += gp_replica_check
=======
ifeq ($(with_perl),yes)
SUBDIRS += hstore_plperl
else
ALWAYS_SUBDIRS += hstore_plperl
endif

ifeq ($(with_python),yes)
SUBDIRS += hstore_plpython ltree_plpython
else
ALWAYS_SUBDIRS += hstore_plpython ltree_plpython
endif
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

# Missing:
#		start-scripts	\ (does not have a makefile)


$(recurse)
$(recurse_always)
