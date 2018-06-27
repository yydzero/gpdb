/*-------------------------------------------------------------------------
 *
 * checksum.c
 *	  Checksum implementation for data pages.
 *
<<<<<<< HEAD
 * Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
=======
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/storage/page/checksum.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "storage/checksum.h"

/*
 * The actual code is in storage/checksum_impl.h.  This is done so that
 * external programs can incorporate the checksum code by #include'ing
 * that file from the exported Postgres headers.  (Compare our CRC code.)
 */
#include "storage/checksum_impl.h"
