#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for backend/regex
#
# IDENTIFICATION
#    src/backend/regex/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/regex
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

<<<<<<< HEAD
OBJS = regcomp.o regerror.o regexec.o regfree.o regprefix.o
=======
OBJS = regcomp.o regerror.o regexec.o regfree.o regprefix.o regexport.o
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

include $(top_srcdir)/src/backend/common.mk

# mark inclusion dependencies between .c files explicitly
regcomp.o: regcomp.c regc_lex.c regc_color.c regc_nfa.c regc_cvec.c \
        regc_locale.c regc_pg_locale.c

regexec.o: regexec.c rege_dfa.c
