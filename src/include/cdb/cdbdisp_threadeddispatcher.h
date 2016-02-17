/*-------------------------------------------------------------------------
 *
 * cdbdisp_threadeddispatcher.h
 * routines for dispatching commands from the dispatcher process
 * to the qExec processes.
 *
 * Copyright (c) 2005-2016, Pivotal inc
 *
 *-------------------------------------------------------------------------
 */

#ifndef CDBDISP_THREADEDDISPATCHER_H
#define CDBDISP_THREADEDDISPATCHER_H

#include "lib/stringinfo.h"         /* StringInfo */

#include "cdb/cdbtm.h"
#include <pthread.h>


struct CdbDispatchResult;           /* #include "cdb/cdbdispatchresult.h" */
struct CdbDispatchResults;          /* #include "cdb/cdbdispatchresult.h" */
struct Gang;                        /* #include "cdb/cdbgang.h" */
struct Node;                        /* #include "nodes/nodes.h" */
struct QueryDesc;                   /* #include "executor/execdesc.h" */
struct SegmentDatabaseDescriptor;   /* #include "cdb/cdbconn.h" */
struct pollfd;

/*
 * Types of message to QE when we wait for it.
 */
typedef enum DispatchWaitMode
{
	DISPATCH_WAIT_NONE = 0,			/* wait until QE fully completes */
	DISPATCH_WAIT_FINISH,			/* send query finish */
	DISPATCH_WAIT_CANCEL			/* send query cancel */
} DispatchWaitMode;

/*
 * Parameter structure for the DispatchCommand threads
 */
typedef struct DispatchCommandParms
{
	GpDispatchCommandType			mppDispatchCommandType;
	DispatchCommandQueryParms		queryParms;
	DispatchCommandDtxProtocolParms	dtxProtocolParms;

	char		*query_text;
	int			query_text_len;

	int			localSlice;

	/*
	 * options for controlling certain behavior
	 * on the remote backend before query processing. Like
	 * explicitly opening a transaction.
	 */
	int			txnOptions;

	/*
	 * store the command here, so that we make sure that every
	 * thread gets the same command-id
	 */
	int			cmdID;

	/*
	 * db_count: The number of segdbs that this thread is responsible
	 * for dispatching the command to.
	 * Equals the count of segdbDescPtrArray below.
	 */
	int			db_count;


	/*
	 * Session auth info
	 */
	Oid			sessUserId;
	Oid			outerUserId;
	Oid			currUserId;
	bool		sessUserId_is_super;
	bool		outerUserId_is_super;

	/*
	 * dispatchResultPtrArray: Array[0..db_count-1] of CdbDispatchResult*
	 * Each CdbDispatchResult object points to a SegmentDatabaseDescriptor
	 * that this thread is responsible for dispatching the command to.
	 */
	struct CdbDispatchResult **dispatchResultPtrArray;

	/*
	 * Depending on this mode, we may send query cancel or query finish
	 * message to QE while we are waiting it to complete.  NONE means
	 * we expect QE to complete without any instruction.
	 */
	volatile DispatchWaitMode waitMode;

	/*
	 * pollfd supports for libpq
	 */
	int				nfds;
	struct pollfd	*fds;

	/*
	 * The pthread_t thread handle.
	 */
	pthread_t	thread;
	bool		thread_valid;

}	DispatchCommandParms;

/*
 * Keeps state of all the dispatch command threads.
 */
typedef struct CdbDispatchCmdThreads
{
	struct DispatchCommandParms *dispatchCommandParmsAr;
	int	dispatchCommandParmsArSize;
	int	threadCount;

}   CdbDispatchCmdThreads;

/*--------------------------------------------------------------------*/

/*
 * cdbdisp_dispatchToGang:
 * Send the strCommand SQL statement to the subset of all segdbs in the cluster
 * specified by the gang parameter.  cancelOnError indicates whether an error
 * occurring on one of the qExec segdbs should cause all still-executing commands to cancel
 * on other qExecs. Normally this would be true.  The commands are sent over the libpq
 * connections that were established during cdblink_setup.	They are run inside of threads.
 * The number of segdbs handled by any one thread is determined by the
 * guc variable gp_connections_per_thread.
 *
 * The caller must also provide a serialized Snapshot string to be used to
 * set the distributed snapshot for the dispatched statement.
 *
 * The caller must provide a CdbDispatchResults object having available
 * resultArray slots sufficient for the number of QEs to be dispatched:
 * i.e., resultCapacity - resultCount >= gp->size.  This function will
 * assign one resultArray slot per QE of the Gang, paralleling the Gang's
 * db_descriptors array.  Success or failure of each QE will be noted in
 * the QE's CdbDispatchResult entry; but before examining the results, the
 * caller must wait for execution to end by calling CdbCheckDispatchResult().
 *
 * The CdbDispatchResults object owns some malloc'ed storage, so the caller
 * must make certain to free it by calling cdbdisp_destroyDispatchResults().
 *
 * When dispatchResults->cancelOnError is false, strCommand is to be
 * dispatched to every connected gang member if possible, despite any
 * cancellation requests, QE errors, connection failures, etc.
 *
 * NB: This function should return normally even if there is an error.
 * It should not longjmp out via elog(ERROR, ...), ereport(ERROR, ...),
 * PG_THROW, CHECK_FOR_INTERRUPTS, etc.
 */
void
cdbdisp_dispatchToGang(struct CdbDispatcherState *ds,
					   GpDispatchCommandType		mppDispatchCommandType,
					   void							*commandTypeParms,
                       struct Gang					*gp,
                       int							sliceIndex,
                       unsigned int					maxSlices,
                       CdbDispatchDirectDesc		*direct);


/*
 * create a CdbDispatchCmdThreads object that holds the dispatch
 * threads state, and an array of dispatch command params.
 */
CdbDispatchCmdThreads *
cdbdisp_makeDispatchThreads(int paramCount);

/*
 * free all memory allocated for a CdbDispatchCmdThreads object.
 */
void
cdbdisp_destroyDispatchThreads(CdbDispatchCmdThreads *dThreads);


void
cdbdisp_waitThreads(void);

void
thread_DispatchWait(DispatchCommandParms *pParms);
/*--------------------------------------------------------------------*/

#endif   /* CDBDISP_THREADEDDISPATCHER_H */
