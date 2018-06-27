<<<<<<< HEAD:contrib/gp_internal_tools/Makefile
MODULES    = gp_ao_co_diagnostics gp_workfile_mgr gp_session_state_memory_stats gp_instrument_shmem
DATA       = gp_session_state.sql uninstall_gp_session_state.sql
=======
# contrib/vacuumlo/Makefile

PGFILEDESC = "vacuumlo - removes orphaned large objects"
PGAPPICON = win32

PROGRAM = vacuumlo
OBJS	= vacuumlo.o $(WIN32RES)
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8:contrib/vacuumlo/Makefile

PG_CPPFLAGS = -I$(libpq_srcdir)

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/gp_diagnostic_libraries
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
