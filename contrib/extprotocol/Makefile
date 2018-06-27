<<<<<<< HEAD:contrib/extprotocol/Makefile
MODULE_big = gpextprotocol
OBJS       = gpextprotocol.o
REGRESS = setup exttableext
=======
# contrib/oid2name/Makefile

PGFILEDESC = "oid2name - examine the file structure"
PGAPPICON = win32

PROGRAM = oid2name
OBJS	= oid2name.o $(WIN32RES)
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8:contrib/oid2name/Makefile

PG_CPPFLAGS = -I$(libpq_srcdir)
PG_LIBS = $(libpq_pgport)

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/extprotocol
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
