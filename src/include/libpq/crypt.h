/*-------------------------------------------------------------------------
 *
 * crypt.h
 *	  Interface to libpq/crypt.c
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/libpq/crypt.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_CRYPT_H
#define PG_CRYPT_H

#include "c.h"

#include "libpq/libpq-be.h"
#include "libpq/md5.h"

<<<<<<< HEAD
extern int hashed_passwd_verify(const Port *port, const char *user,
								char *client_pass);
=======
extern int md5_crypt_verify(const Port *port, const char *role,
				 char *client_pass, char **logdetail);

>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
#endif
