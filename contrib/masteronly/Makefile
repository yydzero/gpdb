# contrib/masteronly/Makefile

MODULE_big = masteronly

OBJS = masteronly.o init.o

EXTENSION = masteronly
DATA = masteronly--1.0.sql
PGFILEDESC = "masteronly - master only table support"

REGRESS = masteronly

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/masteronly
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif