/*-------------------------------------------------------------------------
 *
 * pgtar.h
 *	  Functions for manipulating tarfile datastructures (src/port/tar.c)
 *
 *
<<<<<<< HEAD
 * Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
=======
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/pgtar.h
 *
 *-------------------------------------------------------------------------
 */
<<<<<<< HEAD
extern void tarCreateHeader(char *h, const char *filename, const char *linktarget, size_t size, mode_t mode, uid_t uid, gid_t gid, time_t mtime);
=======

enum tarError
{
	TAR_OK = 0,
	TAR_NAME_TOO_LONG,
	TAR_SYMLINK_TOO_LONG
};

extern enum tarError tarCreateHeader(char *h, const char *filename, const char *linktarget, size_t size, mode_t mode, uid_t uid, gid_t gid, time_t mtime);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
extern int	tarChecksum(char *header);
