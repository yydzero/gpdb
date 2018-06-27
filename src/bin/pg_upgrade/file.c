/*
 *	file.c
 *
 *	file system operations
 *
 *	Copyright (c) 2010-2015, PostgreSQL Global Development Group
 *	src/bin/pg_upgrade/file.c
 */

#include "postgres_fe.h"

#include "pg_upgrade.h"

#include <fcntl.h>

#include "storage/bufpage.h"
#include "storage/checksum.h"
#include "storage/checksum_impl.h"


#ifndef WIN32
static int	copy_file(const char *fromfile, const char *tofile, bool force);
#else
static int	win32_pghardlink(const char *src, const char *dst);
#endif


/*
 * copyAndUpdateFile()
 *
 *	Copies a relation file from src to dst.  If pageConverter is non-NULL, this function
 *	uses that pageConverter to do a page-by-page conversion.
 */
const char *
copyAndUpdateFile(pageCnvCtx *pageConverter,
				  const char *src, const char *dst, bool force)
{
	report_progress(NULL, FILE_COPY, "Copy \"%s\" to \"%s\"", src, dst);

	if (pageConverter == NULL)
	{
		if (pg_copy_file(src, dst, force) == -1)
			return getErrorText(errno);
		else
			return NULL;
	}
	else
	{
		/*
		 * We have a pageConverter object - that implies that the
		 * PageLayoutVersion differs between the two clusters so we have to
		 * perform a page-by-page conversion.
		 *
		 * If the pageConverter can convert the entire file at once, invoke
		 * that plugin function, otherwise, read each page in the relation
		 * file and call the convertPage plugin function.
		 */

#ifdef PAGE_CONVERSION
		if (pageConverter->convertFile)
			return pageConverter->convertFile(pageConverter->pluginData,
											  dst, src);
		else
#endif
		{
			int			src_fd;
			int			dstfd;
			char		buf[BLCKSZ];
			ssize_t		bytesRead;
			const char *msg = NULL;

			if ((src_fd = open(src, O_RDONLY, 0)) < 0)
				return "could not open source file";

			if ((dstfd = open(dst, O_RDWR | O_CREAT | O_EXCL, S_IRUSR | S_IWUSR)) < 0)
			{
				close(src_fd);
				return "could not create destination file";
			}

			while ((bytesRead = read(src_fd, buf, BLCKSZ)) == BLCKSZ)
			{
#ifdef PAGE_CONVERSION
				if ((msg = pageConverter->convertPage(pageConverter->pluginData, buf, buf)) != NULL)
					break;
#endif
				if (write(dstfd, buf, BLCKSZ) != BLCKSZ)
				{
					msg = "could not write new page to destination";
					break;
				}
			}

			close(src_fd);
			close(dstfd);

			if (msg)
				return msg;
			else if (bytesRead != 0)
				return "found partial page in source file";
			else
				return NULL;
		}
	}
}

/*
 * rewriteHeapPageWithChecksum
 *
 * Copies a relation file from src to dst and sets the data checksum in the
 * page headers in the process. We are not using a pageConverter, even though
 * that would make sense, since pageConverter are deprecated and removed in
 * upstream and would give us merge headaches.
 */
void
rewriteHeapPageChecksum(const char *fromfile, const char *tofile,
						const char *schemaName, const char *relName)
{
	int			src_fd;
	int			dst_fd;
	int			blkno;
	int			bytesRead;
	int			totalBytesRead;
	char	   *buf;
	ssize_t		writesize;
	struct stat statbuf;

	/*
	 * transfer_relfile() should never call us unless requested by the data
	 * checksum option but better doublecheck before we start rewriting data.
	 */
	if (user_opts.checksum_mode == CHECKSUM_NONE)
		pg_log(PG_FATAL, "error, incorrect checksum configuration detected.\n");

	if ((src_fd = open(fromfile, O_RDONLY | PG_BINARY, 0)) < 0)
		pg_log(PG_FATAL,
			   "error while rewriting relation \"%s.%s\": could not open file \"%s\": %s\n",
			   schemaName, relName, fromfile, strerror(errno));

	if (fstat(src_fd, &statbuf) != 0)
		pg_log(PG_FATAL,
			   "error while rewriting relation \"%s.%s\": could not stat file \"%s\": %s\n",
			   schemaName, relName, fromfile, strerror(errno));

	if ((dst_fd = open(tofile, O_RDWR | O_CREAT | O_EXCL | PG_BINARY, S_IRUSR | S_IWUSR)) < 0)
		pg_log(PG_FATAL,
			   "error while rewriting relation \"%s.%s\": could not create file \"%s\": %s\n",
			   schemaName, relName, tofile, strerror(errno));

	blkno = 0;
	totalBytesRead = 0;
	buf = (char *) pg_malloc(BLCKSZ);

	while ((bytesRead = read(src_fd, buf, BLCKSZ)) == BLCKSZ)
	{
		Size		page_size;

		page_size = PageGetPageSize((PageHeader) buf);

		if (!PageSizeIsValid(page_size) && page_size != 0)
			pg_log(PG_FATAL,
				   "error while rewriting relation \"%s.%s\": invalid page size detected (%zd)\n",
				   schemaName, relName, page_size);

		if (!PageIsNew(buf))
		{
			if (user_opts.checksum_mode == CHECKSUM_ADD)
				((PageHeader) buf)->pd_checksum = pg_checksum_page(buf, blkno);
			else
				memset(&(((PageHeader) buf)->pd_checksum), 0, sizeof(uint16));
		}

		writesize = write(dst_fd, buf, BLCKSZ);

		if (writesize != BLCKSZ)
			pg_log(PG_FATAL, "error when rewriting relation \"%s.%s\": %s",
				   schemaName, relName, strerror(errno));

		blkno++;
		totalBytesRead += BLCKSZ;
	}

	if (totalBytesRead != statbuf.st_size)
		pg_log(PG_FATAL,
			   "error when rewriting relation \"%s.%s\": torn read on file \"%s\"%c %s\n",
			   schemaName, relName, fromfile,
			   (errno != 0 ? ':' : ' '), strerror(errno));

	pg_free(buf);
	close(dst_fd);
	close(src_fd);
}

/*
 * linkAndUpdateFile()
 *
 * Creates a hard link between the given relation files. We use
 * this function to perform a true in-place update. If the on-disk
 * format of the new cluster is bit-for-bit compatible with the on-disk
 * format of the old cluster, we can simply link each relation
 * instead of copying the data from the old cluster to the new cluster.
 */
const char *
linkAndUpdateFile(pageCnvCtx *pageConverter,
				  const char *src, const char *dst)
{
	if (pageConverter != NULL)
		return "Cannot in-place update this cluster, page-by-page conversion is required";

	if (pg_link_file(src, dst) == -1)
		return getErrorText(errno);
	else
		return NULL;
}


#ifndef WIN32
static int
copy_file(const char *srcfile, const char *dstfile, bool force)
{
#define COPY_BUF_SIZE (50 * BLCKSZ)

	int			src_fd;
	int			dest_fd;
	char	   *buffer;
	int			ret = 0;
	int			save_errno = 0;

	if ((srcfile == NULL) || (dstfile == NULL))
	{
		errno = EINVAL;
		return -1;
	}

	if ((src_fd = open(srcfile, O_RDONLY, 0)) < 0)
		return -1;

	if ((dest_fd = open(dstfile, O_RDWR | O_CREAT | (force ? 0 : O_EXCL), S_IRUSR | S_IWUSR)) < 0)
	{
		save_errno = errno;

		if (src_fd != 0)
			close(src_fd);

		errno = save_errno;
		return -1;
	}

	buffer = (char *) pg_malloc(COPY_BUF_SIZE);

	/* perform data copying i.e read src source, write to destination */
	while (true)
	{
		ssize_t		nbytes = read(src_fd, buffer, COPY_BUF_SIZE);

		if (nbytes < 0)
		{
			save_errno = errno;
			ret = -1;
			break;
		}

		if (nbytes == 0)
			break;

		errno = 0;

		if (write(dest_fd, buffer, nbytes) != nbytes)
		{
			/* if write didn't set errno, assume problem is no disk space */
			if (errno == 0)
				errno = ENOSPC;
			save_errno = errno;
			ret = -1;
			break;
		}
	}

	pg_free(buffer);

	if (src_fd != 0)
		close(src_fd);

	if (dest_fd != 0)
		close(dest_fd);

	if (save_errno != 0)
		errno = save_errno;

	return ret;
}
#endif


<<<<<<< HEAD:contrib/pg_upgrade/file.c
/*
 * load_directory()
 *
 * Returns count of files that meet the selection criteria coded in
 * the function pointed to by selector.  Creates an array of pointers
 * to dirent structures.  Address of array returned in namelist.
 *
 * Note that the number of dirent structures needed is dynamically
 * allocated using realloc.  Realloc can be inefficient if invoked a
 * large number of times.
 */
int
load_directory(const char *dirname, struct dirent *** namelist)
{
	DIR		   *dirdesc;
	struct dirent *direntry;
	int			count = 0;
	int			name_num = 0;
	size_t		entrysize;

	if ((dirdesc = opendir(dirname)) == NULL)
		pg_log(PG_FATAL, "could not open directory \"%s\": %s\n", dirname, getErrorText(errno));

	*namelist = NULL;

	while (errno = 0, (direntry = readdir(dirdesc)) != NULL)
	{
		count++;

		*namelist = (struct dirent **) realloc((void *) (*namelist),
						(size_t) ((name_num + 1) * sizeof(struct dirent *)));

		if (*namelist == NULL)
		{
			closedir(dirdesc);
			return -1;
		}

		entrysize = sizeof(struct dirent) - sizeof(direntry->d_name) +
			strlen(direntry->d_name) + 1;

		(*namelist)[name_num] = (struct dirent *) malloc(entrysize);

		if ((*namelist)[name_num] == NULL)
		{
			closedir(dirdesc);
			return -1;
		}

		memcpy((*namelist)[name_num], direntry, entrysize);

		name_num++;
	}

#ifdef WIN32
	/* Bug in old Mingw dirent.c;  fixed in mingw-runtime-3.2, 2003-10-10 */
	if (GetLastError() == ERROR_NO_MORE_FILES)
		errno = 0;
#endif

	if (errno)
		pg_log(PG_FATAL, "Could not read directory \"%s\": %s\n", dirname, getErrorText(errno));

	if (closedir(dirdesc))
		pg_log(PG_FATAL, "Could not close directory \"%s\": %s\n", dirname, getErrorText(errno));

	return count;
}


=======
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8:src/bin/pg_upgrade/file.c
void
check_hard_link(void)
{
	char		existing_file[MAXPGPATH];
	char		new_link_file[MAXPGPATH];

	snprintf(existing_file, sizeof(existing_file), "%s/PG_VERSION", old_cluster.pgdata);
	snprintf(new_link_file, sizeof(new_link_file), "%s/PG_VERSION.linktest", new_cluster.pgdata);
	unlink(new_link_file);		/* might fail */

	if (pg_link_file(existing_file, new_link_file) == -1)
	{
		pg_fatal("Could not create hard link between old and new data directories: %s\n"
				 "In link mode the old and new data directories must be on the same file system volume.\n",
				 getErrorText(errno));
	}
	unlink(new_link_file);
}

#ifdef WIN32
static int
win32_pghardlink(const char *src, const char *dst)
{
	/*
	 * CreateHardLinkA returns zero for failure
	 * http://msdn.microsoft.com/en-us/library/aa363860(VS.85).aspx
	 */
	if (CreateHardLinkA(dst, src, NULL) == 0)
		return -1;
	else
		return 0;
}
#endif


/* fopen() file with no group/other permissions */
FILE *
fopen_priv(const char *path, const char *mode)
{
	mode_t		old_umask = umask(S_IRWXG | S_IRWXO);
	FILE	   *fp;

	fp = fopen(path, mode);
	umask(old_umask);

	return fp;
}
