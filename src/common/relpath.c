/*-------------------------------------------------------------------------
 * relpath.c
 *		Shared frontend/backend code to compute pathnames of relation files
 *
 * This module also contains some logic associated with fork names.
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/common/relpath.c
 *
 *-------------------------------------------------------------------------
 */
#ifndef FRONTEND
#include "postgres.h"
#else
#include "postgres_fe.h"
#endif

#include "catalog/catalog.h"
#include "catalog/pg_tablespace.h"
#include "common/relpath.h"
#include "storage/backendid.h"


/*
 * Lookup table of fork name by fork number.
 *
 * If you add a new entry, remember to update the errhint in
 * forkname_to_number() below, and update the SGML documentation for
 * pg_relation_size().
 */
const char *const forkNames[] = {
	"main",						/* MAIN_FORKNUM */
	"fsm",						/* FSM_FORKNUM */
	"vm",						/* VISIBILITYMAP_FORKNUM */
	"init"						/* INIT_FORKNUM */
};

/*
 * forkname_to_number - look up fork number by name
 *
 * In backend, we throw an error for no match; in frontend, we just
 * return InvalidForkNumber.
 */
ForkNumber
forkname_to_number(const char *forkName)
{
	ForkNumber	forkNum;

	for (forkNum = 0; forkNum <= MAX_FORKNUM; forkNum++)
		if (strcmp(forkNames[forkNum], forkName) == 0)
			return forkNum;

#ifndef FRONTEND
	ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("invalid fork name"),
			 errhint("Valid fork names are \"main\", \"fsm\", "
					 "\"vm\", and \"init\".")));
#endif

	return InvalidForkNumber;
}

/*
 * forkname_chars
 *		We use this to figure out whether a filename could be a relation
 *		fork (as opposed to an oddly named stray file that somehow ended
 *		up in the database directory).  If the passed string begins with
 *		a fork name (other than the main fork name), we return its length,
 *		and set *fork (if not NULL) to the fork number.  If not, we return 0.
 *
 * Note that the present coding assumes that there are no fork names which
 * are prefixes of other fork names.
 */
int
forkname_chars(const char *str, ForkNumber *fork)
{
	ForkNumber	forkNum;

	for (forkNum = 1; forkNum <= MAX_FORKNUM; forkNum++)
	{
		int			len = strlen(forkNames[forkNum]);

		if (strncmp(forkNames[forkNum], str, len) == 0)
		{
			if (fork)
				*fork = forkNum;
			return len;
		}
	}
	if (fork)
		*fork = InvalidForkNumber;
	return 0;
}


/*
 * GetDatabasePath - construct path to a database directory
 *
 * Result is a palloc'd string.
 *
 * XXX this must agree with GetRelationPath()!
 */
char *
GetDatabasePath(Oid dbNode, Oid spcNode)
{
	if (spcNode == GLOBALTABLESPACE_OID)
	{
		/* Shared system relations live in {datadir}/global */
		Assert(dbNode == 0);
		return pstrdup("global");
	}
	else if (spcNode == DEFAULTTABLESPACE_OID)
	{
		/* The default tablespace is {datadir}/base */
		return psprintf("base/%u", dbNode);
	}
	else
	{
		/* All other tablespaces are accessed via symlinks */
		return psprintf("pg_tblspc/%u/%s/%u",
						spcNode, tablespace_version_directory(), dbNode);
	}
}

/*
 * GetRelationPath - construct path to a relation's file
 *
 * Result is a palloc'd string.
 *
 * Note: ideally, backendId would be declared as type BackendId, but relpath.h
 * would have to include a backend-only header to do that; doesn't seem worth
 * the trouble considering BackendId is just int anyway.
 *
 * In PostgreSQL, the 'backendid' is embedded in the filename of temporary
 * relations. In GPDB, however, temporary relations are just prefixed with
 * "t_*", without the backend id. For compatibility with upstream code, this
 * function still takes 'backendid' as argument, but we only care whether
 * it's InvalidBackendId or not. If you need to construct the path of a
 * temporary relation, but don't know the real backend ID, pass
 * TempRelBackendId.
 */
char *
GetRelationPath(Oid dbNode, Oid spcNode, Oid relNode,
				int backendId, ForkNumber forkNumber)
{
	char	   *path;

	if (spcNode == GLOBALTABLESPACE_OID)
	{
		/* Shared system relations live in {datadir}/global */
		Assert(dbNode == 0);
		Assert(backendId == InvalidBackendId);
		if (forkNumber != MAIN_FORKNUM)
			path = psprintf("global/%u_%s",
							relNode, forkNames[forkNumber]);
		else
			path = psprintf("global/%u", relNode);
	}
	else if (spcNode == DEFAULTTABLESPACE_OID)
	{
		/* The default tablespace is {datadir}/base */
		if (backendId == InvalidBackendId)
		{
			if (forkNumber != MAIN_FORKNUM)
				path = psprintf("base/%u/%u_%s",
								dbNode, relNode,
								forkNames[forkNumber]);
			else
				path = psprintf("base/%u/%u",
								dbNode, relNode);
		}
		else
		{
			if (forkNumber != MAIN_FORKNUM)
				path = psprintf("base/%u/t%d_%u_%s",
								dbNode, backendId, relNode,
								forkNames[forkNumber]);
			else
				path = psprintf("base/%u/t%d_%u",
								dbNode, backendId, relNode);
		}
	}
	else
	{
		/* All other tablespaces are accessed via symlinks */
		if (backendId == InvalidBackendId)
		{
			if (forkNumber != MAIN_FORKNUM)
				path = psprintf("pg_tblspc/%u/%s/%u/%u_%s",
								spcNode, tablespace_version_directory(),
								dbNode, relNode,
								forkNames[forkNumber]);
			else
				path = psprintf("pg_tblspc/%u/%s/%u/%u",
								spcNode, tablespace_version_directory(),
								dbNode, relNode);
		}
		else
		{
			if (forkNumber != MAIN_FORKNUM)
				path = psprintf("pg_tblspc/%u/%s/%u/t%d_%u_%s",
								spcNode, tablespace_version_directory(),
								dbNode, backendId, relNode,
								forkNames[forkNumber]);
			else
				path = psprintf("pg_tblspc/%u/%s/%u/t%d_%u",
								spcNode, tablespace_version_directory(),
								dbNode, backendId, relNode);
		}
	}
	return path;
}

/*
 * Like relpath(), but gets the directory containing the data file
 * and the filename separately.
 */
void
aoreldir_and_filename(Oid dbNode, Oid spcNode, Oid relNode, BackendId backend,
					ForkNumber forknum, char **dir, char **filename)
{
	char	   *path;
	int			i;

	path = GetRelationPath(dbNode, spcNode, relNode, backend, forknum);

	/*
	 * The base path is like "<path>/<rnode>". Split it into
	 * path and filename parts.
	 */
	for (i = strlen(path) - 1; i >= 0; i--)
	{
		if (path[i] == '/')
			break;
	}
#ifndef FRONTEND
	/* MERGE_95_FIXME: move these ao related functions to better place */
	if (i <= 0 || path[i] != '/')
		elog(ERROR, "unexpected path: \"%s\"", path);
#endif

	path[i] = '\0';	// to use pstrdup for FRONTEND.
	*dir = pstrdup(path);
	*filename = pstrdup(&path[i + 1]);

	pfree(path);
}

/*
 * Like relpathbackend(), but more convenient when dealing with
 * AO relations. The filename pattern is the same as for heap
 * tables, but this variant takes also 'segno' as argument.
 */
char *
aorelpathbackend(Oid dbNode, Oid spcNode, Oid relNode, int backend,
				 int32 segno)
{
	char	   *fullpath;
	char	   *path;

	path = GetRelationPath(dbNode, spcNode, relNode, backend, MAIN_FORKNUM);
	if (segno == 0)
		fullpath = path;
	else
	{
		fullpath = psprintf("%s.%u", path, segno);
		pfree(path);
	}
	return fullpath;
}