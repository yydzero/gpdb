/*-------------------------------------------------------------------------
 *
 * checksum.h
 *	  Checksum implementation for data pages.
 *
<<<<<<< HEAD
 * Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
=======
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/checksum.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CHECKSUM_H
#define CHECKSUM_H

#include "storage/block.h"

/*
 * Compute the checksum for a Postgres page.  The page must be aligned on a
 * 4-byte boundary.
 */
extern uint16 pg_checksum_page(char *page, BlockNumber blkno);

#endif   /* CHECKSUM_H */
