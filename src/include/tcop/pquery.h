/*-------------------------------------------------------------------------
 *
 * pquery.h
 *	  prototypes for pquery.c.
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/tcop/pquery.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PQUERY_H
#define PQUERY_H

#include "nodes/parsenodes.h"
#include "utils/portal.h"


extern PGDLLIMPORT Portal ActivePortal;


extern PortalStrategy ChoosePortalStrategy(List *stmts);

extern List *FetchPortalTargetList(Portal portal);

extern List *FetchStatementTargetList(Node *stmt);

/* GPDB_92_MERGE_FIXME: ddesc is needed? */
extern void PortalStart(Portal portal, ParamListInfo params,
<<<<<<< HEAD
						int eflags, bool use_active_snapshot,
						QueryDispatchDesc *ddesc);
=======
			int eflags, Snapshot snapshot);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

extern void PortalSetResultFormat(Portal portal, int nFormats,
					  int16 *formats);

extern bool PortalRun(Portal portal, int64 count, bool isTopLevel,
		  DestReceiver *dest, DestReceiver *altdest,
		  char *completionTag);

extern uint64 PortalRunFetch(Portal portal,
			   FetchDirection fdirection,
			   int64 count,
			   DestReceiver *dest);

#endif   /* PQUERY_H */
