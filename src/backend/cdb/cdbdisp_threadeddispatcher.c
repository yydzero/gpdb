/*-------------------------------------------------------------------------
 *
 * cdbdisp_threadeddispatch.c
 *
 *		Dispatching using multiple threads.
 *
 * Copyright (c) 2005-2016, Pivotal inc
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include <pthread.h>
#include <limits.h>
#ifdef HAVE_POLL_H
#include <poll.h>
#endif
#ifdef HAVE_SYS_POLL_H
#include <sys/poll.h>
#endif

#include "catalog/catquery.h"
#include "executor/execdesc.h"	/* QueryDesc */
#include "storage/ipc.h"		/* For proc_exit_inprogress  */
#include "miscadmin.h"
#include "utils/memutils.h"

#include "utils/tqual.h" 			/*for the snapshot */
#include "storage/proc.h"  			/* MyProc */
#include "storage/procarray.h"      /* updateSharedLocalSnapshot */
#include "access/xact.h"  			/*for GetCurrentTransactionId */


#include "utils/syscache.h"
#include "utils/lsyscache.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "nodes/makefuncs.h"
#include "utils/datum.h"
#include "utils/guc.h"
#include "executor/executor.h"
#include "optimizer/clauses.h"
#include "optimizer/planmain.h"
#include "tcop/tcopprot.h"
#include "cdb/cdbplan.h"
#include "postmaster/syslogger.h"

#include "cdb/cdbselect.h"
#include "cdb/cdbdisp.h"
#include "cdb/cdbdisp_threadeddispatcher.h"
#include "cdb/cdbdispmgr.h"
#include "cdb/cdbdispatchresult.h"
#include "cdb/cdbfts.h"
#include "cdb/cdbgang.h"
#include "cdb/cdbgangmgr.h"
#include "cdb/cdbslicetable.h"
#include "cdb/cdbsrlz.h"
#include "cdb/cdbsubplan.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbtm.h"
#include "cdb/cdbdtxcontextinfo.h"
#include "cdb/cdbllize.h"
#include "cdb/cdbsreh.h"
#include "cdb/cdbrelsize.h"
#include "gp-libpq-fe.h"
#include "libpq/libpq-be.h"
#include "cdb/cdbutil.h"

#include "parser/parsetree.h"
#include "parser/parse_oper.h"
#include "parser/parse_relation.h"
#include "utils/gp_atomic.h"
#include "utils/builtins.h"
#include "utils/portal.h"


extern pthread_t main_tid;
#ifndef _WIN32
#define mythread() ((unsigned long) pthread_self())
#else
#define mythread() ((unsigned long) pthread_self().p)
#endif


static MemoryContext DispatchContext = NULL;


/*
 * Counter to indicate there are some dispatch threads running.  This will
 * be incremented at the beginning of dispatch threads and decremented at
 * the end of them.
 */
volatile int32 RunningThreadCount = 0;

static bool thread_DispatchOut(DispatchCommandParms *pParms);
static void * thread_DispatchCommand(void *arg);
static void handlePollError(DispatchCommandParms *pParms,
				  int db_count,
				  int sock_errno);
static void handlePollTimeout(DispatchCommandParms * pParms,
					int db_count,
					int *timeoutCounter, bool useSampling);
static void addSegDBToDispatchThreadPool(DispatchCommandParms  *ParmsAr,
                             int                    segdbs_in_thread_pool,
						     GpDispatchCommandType	mppDispatchCommandType,
						     void				   *commandTypeParms,
                             int					sliceId,
                             CdbDispatchResult     *dispatchResult);
static bool	processResults(CdbDispatchResult *dispatchResult);
static void CollectQEWriterTransactionInformation(SegmentDatabaseDescriptor *segdbDesc,
					CdbDispatchResult *dispatchResult);
static bool shouldStillDispatchCommand(DispatchCommandParms *pParms, CdbDispatchResult * dispatchResult);

static void dispatchCommandQuery(CdbDispatchResult	*dispatchResult,
					 const char			*query_text,
					 int				query_text_len,
					 const char			*strCommand);
static void dispatchCommandDtxProtocol(CdbDispatchResult	*dispatchResult,
						   const char			*query_text,
						   int					query_text_len,
						   DtxProtocolCommand	dtxProtocolCommand);

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
 * The caller must provide a CdbDispatchResults object having available
 * resultArray slots sufficient for the number of QEs to be dispatched:
 * i.e., resultCapacity - resultCount >= gp->size.	This function will
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
 * This function is passing out a pointer to the newly allocated
 * CdbDispatchCmdThreads object. It holds the dispatch thread information
 * including an array of dispatch thread commands. It gets destroyed later
 * on when the command is finished, along with the DispatchResults objects.
 *
 * NB: This function should return normally even if there is an error.
 * It should not longjmp out via elog(ERROR, ...), ereport(ERROR, ...),
 * PG_THROW, CHECK_FOR_INTERRUPTS, etc.
 *
 * Note: the maxSlices argument is used to allocate the parameter
 * blocks for dispatch, it should be set to the maximum number of
 * slices in a plan. For dispatch of single commands (ie most uses of
 * cdbdisp_dispatchToGang()), setting it to 1 is fine.
 */
void
cdbdisp_dispatchToGang(struct CdbDispatcherState *ds,
					   GpDispatchCommandType		mppDispatchCommandType,
					   void						   *commandTypeParms,
                       struct Gang                 *gp,
                       int                          sliceIndex,
                       unsigned int                 maxSlices,
                       CdbDispatchDirectDesc		*disp_direct)
{
	struct CdbDispatchResults	*dispatchResults = ds->primaryResults;
	SegmentDatabaseDescriptor	*segdbDesc;
	int	i,
		max_threads,
		segdbs_in_thread_pool = 0,
		x,
		newThreads = 0;
	int db_descriptors_size;
	SegmentDatabaseDescriptor *db_descriptors;

	MemoryContext oldContext;

	Assert(Gp_role == GP_ROLE_DISPATCH);
	Assert(gp && gp->size > 0);
	Assert(dispatchResults && dispatchResults->resultArray);

	if (dispatchResults->writer_gang)
	{
		/* Are we dispatching to the writer-gang when it is already busy ? */
		if (gp == dispatchResults->writer_gang)
		{
			if (dispatchResults->writer_gang->dispatcherActive)
			{
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("query plan with multiple segworker groups is not supported"),
						 errhint("likely caused by a function that reads or modifies data in a distributed table")));
			}

			dispatchResults->writer_gang->dispatcherActive = true;
		}
	}

	switch (mppDispatchCommandType)
	{
		case GP_DISPATCH_COMMAND_TYPE_QUERY:
		{
			DispatchCommandQueryParms *pQueryParms = (DispatchCommandQueryParms *) commandTypeParms;

			Assert(pQueryParms->strCommand != NULL);

			if (DEBUG2 >= log_min_messages)
			{
				if (sliceIndex >= 0)
					elog(DEBUG2, "dispatchToGang: sliceIndex=%d gangsize=%d  %.100s",
						 sliceIndex, gp->size, pQueryParms->strCommand);
				else
					elog(DEBUG2, "dispatchToGang: gangsize=%d  %.100s",
						 gp->size, pQueryParms->strCommand);
			}
		}
		break;

		case GP_DISPATCH_COMMAND_TYPE_DTX_PROTOCOL:
		{
			DispatchCommandDtxProtocolParms *pDtxProtocolParms = (DispatchCommandDtxProtocolParms *) commandTypeParms;

			Assert(pDtxProtocolParms->dtxProtocolCommandLoggingStr != NULL);
			Assert(pDtxProtocolParms->gid != NULL);

			if (DEBUG2 >= log_min_messages)
			{
				if (sliceIndex >= 0)
					elog(DEBUG2, "dispatchToGang: sliceIndex=%d gangsize=%d  dtxProtocol = %s, gid = %s",
						 sliceIndex, gp->size, pDtxProtocolParms->dtxProtocolCommandLoggingStr, pDtxProtocolParms->gid);
				else
					elog(DEBUG2, "dispatchToGang: gangsize=%d  dtxProtocol = %s, gid = %s",
						 gp->size, pDtxProtocolParms->dtxProtocolCommandLoggingStr, pDtxProtocolParms->gid);
			}
		}
		break;

		default:
			elog(FATAL, "Unrecognized MPP dispatch command type: %d",
				 (int) mppDispatchCommandType);
	}

	db_descriptors_size = gp->size;
	db_descriptors = gp->db_descriptors;

	/*
	 * The most threads we could have is segdb_count / gp_connections_per_thread, rounded up.
	 * This is equivalent to 1 + (segdb_count-1) / gp_connections_per_thread.
	 * We allocate enough memory for this many DispatchCommandParms structures,
	 * even though we may not use them all.
	 *
	 * We can only use gp->size here if we're not dealing with a
	 * singleton gang. It is safer to always use the max number of segments we are
	 * controlling (largestGangsize).
	 */
	Assert(gp_connections_per_thread >= 0);

	segdbs_in_thread_pool = 0;

	Assert(db_descriptors_size <= GetGangMgr().largestGangsize());

	if (gp_connections_per_thread == 0)
		max_threads = 1;	/* one, not zero, because we need to allocate one param block */
	else
		max_threads = 1 + (GetGangMgr().largestGangsize() - 1) / gp_connections_per_thread;

	if (DispatchContext == NULL)
	{
		DispatchContext = AllocSetContextCreate(TopMemoryContext,
												"Dispatch Context",
												ALLOCSET_DEFAULT_MINSIZE,
												ALLOCSET_DEFAULT_INITSIZE,
												ALLOCSET_DEFAULT_MAXSIZE);
	}
	Assert(DispatchContext != NULL);

	oldContext = MemoryContextSwitchTo(DispatchContext);

	if (ds->dispatchThreads == NULL)
	{
		/* the maximum number of command parameter blocks we'll possibly need is
		 * one for each slice on the primary gang. Max sure that we
		 * have enough -- once we've created the command block we're stuck with it
		 * for the duration of this statement (including CDB-DTM ).
		 * 1 * maxthreads * slices for each primary
		 * X 2 for good measure ? */
		int paramCount = max_threads * 4 * Max(maxSlices, 5);

		elog(DEBUG4, "dispatcher: allocating command array with maxslices %d paramCount %d", maxSlices, paramCount);

		ds->dispatchThreads = cdbdisp_makeDispatchThreads(paramCount);
	}
	else
	{
		/*
		 * If we attempt to reallocate, there is a race here: we
		 * know that we have threads running using the
		 * dispatchCommandParamsAr! If we reallocate we
		 * potentially yank it out from under them! Don't do
		 * it!
		 */
		if (ds->dispatchThreads->dispatchCommandParmsArSize < (ds->dispatchThreads->threadCount + max_threads))
		{
			elog(ERROR, "Attempted to reallocate dispatchCommandParmsAr while other threads still running size %d new threadcount %d",
				 ds->dispatchThreads->dispatchCommandParmsArSize, ds->dispatchThreads->threadCount + max_threads);
		}
	}

	MemoryContextSwitchTo(oldContext);

	x = 0;
	/*
	 * Create the thread parms structures based targetSet parameter.
	 * This will add the segdbDesc pointers appropriate to the
	 * targetSet into the thread Parms structures, making sure that each thread
	 * handles gp_connections_per_thread segdbs.
	 */
	for (i = 0; i < db_descriptors_size; i++)
	{
		CdbDispatchResult *qeResult;

		segdbDesc = &db_descriptors[i];

		Assert(segdbDesc != NULL);

		if (disp_direct->directed_dispatch)
		{
			Assert (disp_direct->count == 1); /* currently we allow direct-to-one dispatch, only */

			if (disp_direct->content[0] != segdbDesc->segment_database_info->segindex)
				continue;
		}

		/* Initialize the QE's CdbDispatchResult object. */
		qeResult = cdbdisp_makeResult(dispatchResults, segdbDesc, sliceIndex);

		if (qeResult == NULL)
		{
			/* writer_gang could be NULL if this is an extended query. */
			if (dispatchResults->writer_gang)
				dispatchResults->writer_gang->dispatcherActive = true;
			elog(FATAL, "could not allocate resources for segworker communication");
		}

		/* Transfer any connection errors from segdbDesc. */
		if (segdbDesc->errcode ||
			segdbDesc->error_message.len)
			cdbdisp_mergeConnectionErrors(qeResult, segdbDesc);

		addSegDBToDispatchThreadPool(ds->dispatchThreads->dispatchCommandParmsAr + ds->dispatchThreads->threadCount,
									 x,
					   				 mppDispatchCommandType,
					   				 commandTypeParms,
									 sliceIndex,
                                     qeResult);

		/*
		 * This CdbDispatchResult/SegmentDatabaseDescriptor pair will be
		 * dispatched and monitored by a thread to be started below. Only that
		 * thread should touch them until the thread is finished with them and
		 * resets the stillRunning flag. Caller must CdbCheckDispatchResult()
		 * to wait for completion.
		 */
		qeResult->stillRunning = true;

		x++;
	}
	segdbs_in_thread_pool = x;

	/*
	 * Compute the thread count based on how many segdbs were added into the
	 * thread pool, knowing that each thread handles gp_connections_per_thread
	 * segdbs.
	 */
	if (segdbs_in_thread_pool == 0)
		newThreads += 0;
	else
		if (gp_connections_per_thread == 0)
			newThreads += 1;
		else
			newThreads += 1 + (segdbs_in_thread_pool - 1) / gp_connections_per_thread;

	oldContext = MemoryContextSwitchTo(DispatchContext);
	for (i = 0; i < newThreads; i++)
	{
		DispatchCommandParms *pParms = &(ds->dispatchThreads->dispatchCommandParmsAr + ds->dispatchThreads->threadCount)[i];

		pParms->fds = (struct pollfd *) palloc0(sizeof(struct pollfd) * pParms->db_count);
		pParms->nfds = pParms->db_count;
	}
	MemoryContextSwitchTo(oldContext);

	/*
	 * Create the threads. (which also starts the dispatching).
	 */
	for (i = 0; i < newThreads; i++)
	{
		DispatchCommandParms *pParms = &(ds->dispatchThreads->dispatchCommandParmsAr + ds->dispatchThreads->threadCount)[i];

		Assert(pParms != NULL);

	    if (gp_connections_per_thread==0)
	    {
	    	Assert(newThreads <= 1);
	    	thread_DispatchOut(pParms);
	    }
	    else
	    {
	    	int		pthread_err = 0;

			pParms->thread_valid = true;
			pthread_err = gp_pthread_create(&pParms->thread, thread_DispatchCommand, pParms, "dispatchToGang");

			if (pthread_err != 0)
			{
				int j;

				pParms->thread_valid = false;

				/*
				 * Error during thread create (this should be caused by
				 * resource constraints). If we leave the threads running,
				 * they'll immediately have some problems -- so we need to
				 * join them, and *then* we can issue our FATAL error
				 */
				pParms->waitMode = DISPATCH_WAIT_CANCEL;

				for (j = 0; j < ds->dispatchThreads->threadCount + (i - 1); j++)
				{
					DispatchCommandParms *pParms;

					pParms = &ds->dispatchThreads->dispatchCommandParmsAr[j];

					pParms->waitMode = DISPATCH_WAIT_CANCEL;
					pParms->thread_valid = false;
					pthread_join(pParms->thread, NULL);
				}

				ereport(FATAL, (errcode(ERRCODE_INTERNAL_ERROR),
								errmsg("could not create thread %d of %d", i + 1, newThreads),
								errdetail("pthread_create() failed with err %d", pthread_err)));
			}
	    }

	}

	ds->dispatchThreads->threadCount += newThreads;
	elog(DEBUG4, "dispatchToGang: Total threads now %d", ds->dispatchThreads->threadCount);
}	/* cdbdisp_dispatchToGang */


/*
 * addSegDBToDispatchThreadPool
 * Helper function used to add a segdb's segdbDesc to the thread pool to have commands dispatched to.
 * It figures out which thread will handle it, based on the setting of
 * gp_connections_per_thread.
 */
static void
addSegDBToDispatchThreadPool(DispatchCommandParms  *ParmsAr,
                             int                    segdbs_in_thread_pool,
						     GpDispatchCommandType	mppDispatchCommandType,
						     void				   *commandTypeParms,
                             int					sliceId,
                             CdbDispatchResult     *dispatchResult)
{
	DispatchCommandParms *pParms;
	int			ParmsIndex;
	bool 		firsttime = false;

	/*
	 * The proper index into the DispatchCommandParms array is computed, based on
	 * having gp_connections_per_thread segdbDesc's in each thread.
	 * If it's the first access to an array location, determined
	 * by (*segdbCount) % gp_connections_per_thread == 0,
	 * then we initialize the struct members for that array location first.
	 */
	if (gp_connections_per_thread == 0)
		ParmsIndex = 0;
	else
		ParmsIndex = segdbs_in_thread_pool / gp_connections_per_thread;
	pParms = &ParmsAr[ParmsIndex];

	/*
	 * First time through?
	 */

	if (gp_connections_per_thread==0)
		firsttime = segdbs_in_thread_pool == 0;
	else
		firsttime = segdbs_in_thread_pool % gp_connections_per_thread == 0;
	if (firsttime)
	{
		pParms->mppDispatchCommandType = mppDispatchCommandType;

		switch (mppDispatchCommandType)
		{
			case GP_DISPATCH_COMMAND_TYPE_QUERY:
			{
				DispatchCommandQueryParms *pQueryParms = (DispatchCommandQueryParms *) commandTypeParms;

				if (pQueryParms->strCommand == NULL || strlen(pQueryParms->strCommand) == 0)
				{
					pParms->queryParms.strCommand = NULL;
					pParms->queryParms.strCommandlen = 0;
				}
				else
				{
					pParms->queryParms.strCommand = pQueryParms->strCommand;
					pParms->queryParms.strCommandlen = strlen(pQueryParms->strCommand) + 1;
				}

				if (pQueryParms->serializedQuerytree == NULL || pQueryParms->serializedQuerytreelen == 0)
				{
					pParms->queryParms.serializedQuerytree = NULL;
					pParms->queryParms.serializedQuerytreelen = 0;
				}
				else
				{
					pParms->queryParms.serializedQuerytree = pQueryParms->serializedQuerytree;
					pParms->queryParms.serializedQuerytreelen = pQueryParms->serializedQuerytreelen;
				}

				if (pQueryParms->serializedPlantree == NULL || pQueryParms->serializedPlantreelen == 0)
				{
					pParms->queryParms.serializedPlantree = NULL;
					pParms->queryParms.serializedPlantreelen = 0;
				}
				else
				{
					pParms->queryParms.serializedPlantree = pQueryParms->serializedPlantree;
					pParms->queryParms.serializedPlantreelen = pQueryParms->serializedPlantreelen;
				}

				if (pQueryParms->serializedParams == NULL || pQueryParms->serializedParamslen == 0)
				{
					pParms->queryParms.serializedParams = NULL;
					pParms->queryParms.serializedParamslen = 0;
				}
				else
				{
					pParms->queryParms.serializedParams = pQueryParms->serializedParams;
					pParms->queryParms.serializedParamslen = pQueryParms->serializedParamslen;
				}

				if (pQueryParms->serializedSliceInfo == NULL || pQueryParms->serializedSliceInfolen == 0)
				{
					pParms->queryParms.serializedSliceInfo = NULL;
					pParms->queryParms.serializedSliceInfolen = 0;
				}
				else
				{
					pParms->queryParms.serializedSliceInfo = pQueryParms->serializedSliceInfo;
					pParms->queryParms.serializedSliceInfolen = pQueryParms->serializedSliceInfolen;
				}

				if (pQueryParms->serializedDtxContextInfo == NULL || pQueryParms->serializedDtxContextInfolen == 0)
				{
					pParms->queryParms.serializedDtxContextInfo = NULL;
					pParms->queryParms.serializedDtxContextInfolen = 0;
				}
				else
				{
					pParms->queryParms.serializedDtxContextInfo = pQueryParms->serializedDtxContextInfo;
					pParms->queryParms.serializedDtxContextInfolen = pQueryParms->serializedDtxContextInfolen;
				}

				pParms->queryParms.rootIdx = pQueryParms->rootIdx;

				if (pQueryParms->seqServerHost == NULL || pQueryParms->seqServerHostlen == 0)
				{
					pParms->queryParms.seqServerHost = NULL;
					pParms->queryParms.seqServerHostlen = 0;
					pParms->queryParms.seqServerPort = -1;
				}

				else
				{
					pParms->queryParms.seqServerHost = pQueryParms->seqServerHost;
					pParms->queryParms.seqServerHostlen = pQueryParms->seqServerHostlen;
					pParms->queryParms.seqServerPort = pQueryParms->seqServerPort;
				}

				pParms->queryParms.primary_gang_id = pQueryParms->primary_gang_id;
			}
			break;

			case GP_DISPATCH_COMMAND_TYPE_DTX_PROTOCOL:
			{
				DispatchCommandDtxProtocolParms *pDtxProtocolParms = (DispatchCommandDtxProtocolParms *) commandTypeParms;

				pParms->dtxProtocolParms.dtxProtocolCommand = pDtxProtocolParms->dtxProtocolCommand;
				pParms->dtxProtocolParms.flags = pDtxProtocolParms->flags;
				pParms->dtxProtocolParms.dtxProtocolCommandLoggingStr = pDtxProtocolParms->dtxProtocolCommandLoggingStr;
				if (strlen(pDtxProtocolParms->gid) >= TMGIDSIZE)
					elog(PANIC, "Distribute transaction identifier too long (%d)",
						 (int)strlen(pDtxProtocolParms->gid));
				memcpy(pParms->dtxProtocolParms.gid, pDtxProtocolParms->gid, TMGIDSIZE);
				pParms->dtxProtocolParms.gxid = pDtxProtocolParms->gxid;
				pParms->dtxProtocolParms.primary_gang_id = pDtxProtocolParms->primary_gang_id;
				pParms->dtxProtocolParms.argument = pDtxProtocolParms->argument;
				pParms->dtxProtocolParms.argumentLength = pDtxProtocolParms->argumentLength;
			}
			break;

			default:
				elog(FATAL, "Unrecognized MPP dispatch command type: %d",
					 (int) mppDispatchCommandType);
		}

		pParms->sessUserId = GetSessionUserId();
		pParms->outerUserId = GetOuterUserId();
		pParms->currUserId = GetUserId();
		pParms->sessUserId_is_super = superuser_arg(GetSessionUserId());
		pParms->outerUserId_is_super = superuser_arg(GetOuterUserId());

		pParms->cmdID = gp_command_count;
		pParms->localSlice = sliceId;
		Assert(DispatchContext != NULL);
		pParms->dispatchResultPtrArray =
			(CdbDispatchResult **) palloc0((gp_connections_per_thread == 0 ? GetGangMgr().largestGangsize() : gp_connections_per_thread)*
										   sizeof(CdbDispatchResult *));
		MemSet(&pParms->thread, 0, sizeof(pthread_t));
		pParms->db_count = 0;
	}

	/*
	 * Just add to the end of the used portion of the dispatchResultPtrArray
	 * and bump the count of members
	 */
	pParms->dispatchResultPtrArray[pParms->db_count++] = dispatchResult;

}	/* addSegDBToDispatchThreadPool */



static bool
thread_DispatchOut(DispatchCommandParms *pParms)
{
	CdbDispatchResult			*dispatchResult;
	int							i, db_count = pParms->db_count;

	switch (pParms->mppDispatchCommandType)
	{
		case GP_DISPATCH_COMMAND_TYPE_QUERY:
		{
			DispatchCommandQueryParms *pQueryParms = &pParms->queryParms;

			pParms->query_text = PQbuildGpQueryString(
				pQueryParms->strCommand, pQueryParms->strCommandlen,
				pQueryParms->serializedQuerytree, pQueryParms->serializedQuerytreelen,
				pQueryParms->serializedPlantree, pQueryParms->serializedPlantreelen,
				pQueryParms->serializedParams, pQueryParms->serializedParamslen,
				pQueryParms->serializedSliceInfo, pQueryParms->serializedSliceInfolen,
				pQueryParms->serializedDtxContextInfo, pQueryParms->serializedDtxContextInfolen,
				0 /* unused flags*/, pParms->cmdID, pParms->localSlice, pQueryParms->rootIdx,
				pQueryParms->seqServerHost, pQueryParms->seqServerHostlen, pQueryParms->seqServerPort,
				pQueryParms->primary_gang_id,
				GetCurrentStatementStartTimestamp(),
				pParms->sessUserId, pParms->sessUserId_is_super,
				pParms->outerUserId, pParms->outerUserId_is_super, pParms->currUserId,
				&pParms->query_text_len);
		}
		break;

		case GP_DISPATCH_COMMAND_TYPE_DTX_PROTOCOL:
		{
			pParms->query_text = PQbuildGpDtxProtocolCommand(
				(int)pParms->dtxProtocolParms.dtxProtocolCommand,
				pParms->dtxProtocolParms.flags,
				pParms->dtxProtocolParms.dtxProtocolCommandLoggingStr,
				pParms->dtxProtocolParms.gid,
				pParms->dtxProtocolParms.gxid,
				pParms->dtxProtocolParms.primary_gang_id,
				pParms->dtxProtocolParms.argument,
				pParms->dtxProtocolParms.argumentLength,
				&pParms->query_text_len);
		}
		break;

		default:
			write_log("bad dispatch command type %d", (int) pParms->mppDispatchCommandType);
			pParms->query_text_len = 0;
			return false;
	}

	if (pParms->query_text == NULL)
	{
		write_log("could not build query string, total length %d", pParms->query_text_len);
		pParms->query_text_len = 0;
		return false;
	}

	/*
	 * The pParms contains an array of SegmentDatabaseDescriptors
	 * to send commands through to.
	 */
	for (i = 0; i < db_count; i++)
	{
		dispatchResult = pParms->dispatchResultPtrArray[i];

		/* Don't use elog, it's not thread-safe */
		if (DEBUG5 >= log_min_messages)
		{
			if (dispatchResult->segdbDesc->conn)
			{
				write_log("thread_DispatchCommand working on %d of %d commands.  asyncStatus %d",
						  i + 1, db_count, dispatchResult->segdbDesc->conn->asyncStatus);
			}
		}

        dispatchResult->hasDispatched = false;
        dispatchResult->sentSignal = DISPATCH_WAIT_NONE;
        dispatchResult->wasCanceled = false;

        if (!shouldStillDispatchCommand(pParms, dispatchResult))
        {
            /* Don't dispatch if cancellation pending or no connection. */
            dispatchResult->stillRunning = false;
            if (PQisBusy(dispatchResult->segdbDesc->conn))
				write_log(" We thought we were done, because !shouldStillDispatchCommand(), but libpq says we are still busy");
			if (PQstatus(dispatchResult->segdbDesc->conn) == CONNECTION_BAD)
				write_log(" We thought we were done, because !shouldStillDispatchCommand(), but libpq says the connection died?");
        }
        else
        {
            /* Kick off the command over the libpq connection.
             * If unsuccessful, proceed anyway, and check for lost connection below.
             */
			if (PQisBusy(dispatchResult->segdbDesc->conn))
			{
				write_log("Trying to send to busy connection %s  %d %d asyncStatus %d",
					dispatchResult->segdbDesc->whoami, i, db_count, dispatchResult->segdbDesc->conn->asyncStatus);
			}

			if (PQstatus(dispatchResult->segdbDesc->conn) == CONNECTION_BAD)
			{
				char *msg;

				msg = PQerrorMessage(dispatchResult->segdbDesc->conn);

				write_log("Dispatcher noticed a problem before query transmit: %s (%s)", msg ? msg : "unknown error", dispatchResult->segdbDesc->whoami);

				/* Save error info for later. */
				cdbdisp_appendMessage(dispatchResult, LOG,
									  ERRCODE_GP_INTERCONNECTION_ERROR,
									  "Error before transmit from %s: %s",
									  dispatchResult->segdbDesc->whoami,
									  msg ? msg : "unknown error");

				PQfinish(dispatchResult->segdbDesc->conn);
				dispatchResult->segdbDesc->conn = NULL;
				dispatchResult->stillRunning = false;

				continue;
			}
#ifdef USE_NONBLOCKING
			/*
			 * In 2000, Tom Lane said:
			 * "I believe that the nonblocking-mode code is pretty buggy, and don't
			 *  recommend using it unless you really need it and want to help debug
			 *  it.."
			 *
			 * Reading through the code, I'm not convinced the situation has
			 * improved in 2007... I still see some very questionable things
			 * about nonblocking mode, so for now, I'm disabling it.
			 */
			PQsetnonblocking(dispatchResult->segdbDesc->conn, TRUE);
#endif

			switch (pParms->mppDispatchCommandType)
			{
				case GP_DISPATCH_COMMAND_TYPE_QUERY:
					dispatchCommandQuery(dispatchResult, pParms->query_text, pParms->query_text_len, pParms->queryParms.strCommand);
					break;

				case GP_DISPATCH_COMMAND_TYPE_DTX_PROTOCOL:

					if (Debug_dtm_action == DEBUG_DTM_ACTION_DELAY &&
						Debug_dtm_action_target == DEBUG_DTM_ACTION_TARGET_PROTOCOL &&
						Debug_dtm_action_protocol == pParms->dtxProtocolParms.dtxProtocolCommand &&
						Debug_dtm_action_segment == dispatchResult->segdbDesc->segment_database_info->segindex)
					{
						write_log("Delaying '%s' broadcast for segment %d by %d milliseconds.",
								  DtxProtocolCommandToString(Debug_dtm_action_protocol),
								  Debug_dtm_action_segment,
								  Debug_dtm_action_delay_ms);
						pg_usleep(Debug_dtm_action_delay_ms * 1000);
					}

					dispatchCommandDtxProtocol(dispatchResult, pParms->query_text, pParms->query_text_len,
											   pParms->dtxProtocolParms.dtxProtocolCommand);
					break;

				default:
					write_log("bad dispatch command type %d", (int) pParms->mppDispatchCommandType);
					pParms->query_text_len = 0;
					return false;
			}

            dispatchResult->hasDispatched = true;
        }
	}

#ifdef USE_NONBLOCKING

    /*
     * Is everything sent?  Well, if the network stack was too busy, and we are using
     * nonblocking mode, some of the sends
     * might not have completed.  We can't use SELECT to wait unless they have
     * received their work, or we will wait forever.    Make sure they do.
     */

	{
		bool allsent=true;

		/*
		 * debug loop to check to see if this really is needed
		 */
		for (i = 0; i < db_count; i++)
    	{
    		dispatchResult = pParms->dispatchResultPtrArray[i];
    		if (!dispatchResult->stillRunning || !dispatchResult->hasDispatched)
    			continue;
    		if (PQstatus(dispatchResult->segdbDesc->conn) == CONNECTION_BAD)
    			continue;
    		if (dispatchResult->segdbDesc->conn->outCount > 0)
    		{
    			write_log("Yes, extra flushing is necessary %d",i);
    			break;
    		}
    	}

		/*
		 * Check to see if any needed extra flushing.
		 */
		for (i = 0; i < db_count; i++)
    	{
        	int			flushResult;

    		dispatchResult = pParms->dispatchResultPtrArray[i];
    		if (!dispatchResult->stillRunning || !dispatchResult->hasDispatched)
    			continue;
    		if (PQstatus(dispatchResult->segdbDesc->conn) == CONNECTION_BAD)
    			continue;
    		/*
			 * If data remains unsent, send it.  Else we might be waiting for the
			 * result of a command the backend hasn't even got yet.
			 */
    		flushResult = PQflush(dispatchResult->segdbDesc->conn);
    		/*
    		 * First time, go through the loop without waiting if we can't
    		 * flush, in case we are using multiple network adapters, and
    		 * other connections might be able to flush
    		 */
    		if (flushResult > 0)
    		{
    			allsent=false;
    			write_log("flushing didn't finish the work %d",i);
    		}

    	}

        /*
         * our first attempt at doing more flushes didn't get everything out,
         * so we need to continue to try.
         */

		for (i = 0; i < db_count; i++)
    	{
    		dispatchResult = pParms->dispatchResultPtrArray[i];
    		while (PQisnonblocking(dispatchResult->segdbDesc->conn))
    		{
    			PQflush(dispatchResult->segdbDesc->conn);
    			PQsetnonblocking(dispatchResult->segdbDesc->conn, FALSE);
    		}
		}

	}
#endif

	return true;
}



void
thread_DispatchWait(DispatchCommandParms		*pParms)
{
	SegmentDatabaseDescriptor	*segdbDesc;
	CdbDispatchResult			*dispatchResult;
	int							i, db_count = pParms->db_count;
	int							timeoutCounter = 0;

	/*
	 * OK, we are finished submitting the command to the segdbs.
	 * Now, we have to wait for them to finish.
	 */
	for (;;)
	{							/* some QEs running */
		int			sock;
		int			n;
		int			nfds = 0;
		int			cur_fds_num = 0;

		/*
		 * Which QEs are still running and could send results to us?
		 */

		for (i = 0; i < db_count; i++)
		{						/* loop to check connection status */
			dispatchResult = pParms->dispatchResultPtrArray[i];
			segdbDesc = dispatchResult->segdbDesc;

			/* Already finished with this QE? */
			if (!dispatchResult->stillRunning)
				continue;

			/* Add socket to fd_set if still connected. */
			sock = PQsocket(segdbDesc->conn);
			if (sock >= 0 &&
				PQstatus(segdbDesc->conn) != CONNECTION_BAD)
			{
				pParms->fds[nfds].fd = sock;
				pParms->fds[nfds].events = POLLIN;
				nfds++;
				Assert(nfds <= pParms->nfds);
			}

			/* Lost the connection. */
			else
			{
				char	   *msg = PQerrorMessage(segdbDesc->conn);

				/* Save error info for later. */
				cdbdisp_appendMessage(dispatchResult, DEBUG1,
									  ERRCODE_GP_INTERCONNECTION_ERROR,
									  "Lost connection to %s.  %s",
									  segdbDesc->whoami,
									  msg ? msg : "");

				/* Free the PGconn object. */
				PQfinish(segdbDesc->conn);
				segdbDesc->conn = NULL;
				dispatchResult->stillRunning = false;	/* he's dead, Jim */
			}
		}						/* loop to check connection status */

		/* Break out when no QEs still running. */
		if (nfds <= 0)
			break;

		/*
		 * bail-out if we are dying.  We should not do much of cleanup
		 * as the main thread is waiting on this thread to finish.  Once
		 * QD dies, QE will recognize it shortly anyway.
		 */
		if (proc_exit_inprogress)
			break;

		/*
		 * Wait for results from QEs.
		 */

		/* Block here until input is available. */
		n = poll(pParms->fds, nfds, DISPATCH_WAIT_TIMEOUT_SEC * 1000);

		if (n < 0)
		{
			int			sock_errno = SOCK_ERRNO;

			if (sock_errno == EINTR)
				continue;

			handlePollError(pParms, db_count, sock_errno);
			continue;
		}

		if (n == 0)
		{
			handlePollTimeout(pParms, db_count, &timeoutCounter, true);
			continue;
		}

		cur_fds_num = 0;
		/*
		 * We have data waiting on one or more of the connections.
		 */
		for (i = 0; i < db_count; i++)
		{						/* input available; receive and process it */
			bool		finished;

			dispatchResult = pParms->dispatchResultPtrArray[i];
			segdbDesc = dispatchResult->segdbDesc;

			/* Skip if already finished or didn't dispatch. */
			if (!dispatchResult->stillRunning)
				continue;

			if (DEBUG4 >= log_min_messages)
				write_log("looking for results from %d of %d",i+1,db_count);

			/* Skip this connection if it has no input available. */
			sock = PQsocket(segdbDesc->conn);
			if (sock >= 0)
				/*
				 * The fds array is shorter than conn array, so the following
				 * match method will use this assumtion.
				 */
				Assert(sock == pParms->fds[cur_fds_num].fd);
			if (sock >= 0 && (sock == pParms->fds[cur_fds_num].fd))
			{
				cur_fds_num++;
				if (!(pParms->fds[cur_fds_num - 1].revents & POLLIN))
					continue;
			}

			if (DEBUG4 >= log_min_messages)
				write_log("PQsocket says there are results from %d",i+1);
			/* Receive and process results from this QE. */
			finished = processResults(dispatchResult);

			/* Are we through with this QE now? */
			if (finished)
			{
				if (DEBUG4 >= log_min_messages)
					write_log("processResults says we are finished with %d:  %s",i+1,segdbDesc->whoami);
				dispatchResult->stillRunning = false;
				if (DEBUG1 >= log_min_messages)
				{
					char		msec_str[32];
					switch (check_log_duration(msec_str, false))
					{
						case 1:
						case 2:
							write_log("duration to dispatch result received from thread %d (seg %d): %s ms", i+1 ,dispatchResult->segdbDesc->segindex,msec_str);
							break;
					}
				}
				if (PQisBusy(dispatchResult->segdbDesc->conn))
					write_log("We thought we were done, because finished==true, but libpq says we are still busy");

			}
			else
				if (DEBUG4 >= log_min_messages)
					write_log("processResults says we have more to do with %d: %s",i+1,segdbDesc->whoami);
		}						/* input available; receive and process it */

	}							/* some QEs running */

}

static void
thread_DispatchWaitSingle(DispatchCommandParms		*pParms)
{
	SegmentDatabaseDescriptor	*segdbDesc;
	CdbDispatchResult			*dispatchResult;
	char * msg = NULL;

	/* Assert() cannot be used in threads */
	if (pParms->db_count != 1)
		write_log("Bug... thread_dispatchWaitSingle called with db_count %d",pParms->db_count);

	dispatchResult = pParms->dispatchResultPtrArray[0];
	segdbDesc = dispatchResult->segdbDesc;

	if (PQstatus(segdbDesc->conn) == CONNECTION_BAD)
		/* Lost the connection. */
	{
		msg = PQerrorMessage(segdbDesc->conn);

		/* Save error info for later. */
		cdbdisp_appendMessage(dispatchResult, DEBUG1,
							  ERRCODE_GP_INTERCONNECTION_ERROR,
							  "Lost connection to %s.  %s",
							  segdbDesc->whoami,
							  msg ? msg : "");

		/* Free the PGconn object. */
		PQfinish(segdbDesc->conn);
		segdbDesc->conn = NULL;
		dispatchResult->stillRunning = false;	/* he's dead, Jim */
	}
	else
	{

		PQsetnonblocking(segdbDesc->conn,FALSE);  /* Not necessary, I think */

		for(;;)
		{							/* loop to call PQgetResult; will block */
			PGresult   *pRes;
			ExecStatusType resultStatus;
			int			resultIndex = cdbdisp_numPGresult(dispatchResult);

			if (DEBUG4 >= log_min_messages)
				write_log("PQgetResult, resultIndex = %d",resultIndex);
			/* Get one message. */
			pRes = PQgetResult(segdbDesc->conn);

			CollectQEWriterTransactionInformation(segdbDesc, dispatchResult);

			/*
			 * Command is complete when PGgetResult() returns NULL. It is critical
			 * that for any connection that had an asynchronous command sent thru
			 * it, we call PQgetResult until it returns NULL. Otherwise, the next
			 * time a command is sent to that connection, it will return an error
			 * that there's a command pending.
			 */
			if (!pRes)
			{						/* end of results */
				if (DEBUG4 >= log_min_messages)
				{
					/* Don't use elog, it's not thread-safe */
					write_log("%s -> idle", segdbDesc->whoami);
				}
				break;		/* this is normal end of command */
			}						/* end of results */

			/* Attach the PGresult object to the CdbDispatchResult object. */
			cdbdisp_appendResult(dispatchResult, pRes);

			/* Did a command complete successfully? */
			resultStatus = PQresultStatus(pRes);
			if (resultStatus == PGRES_COMMAND_OK ||
				resultStatus == PGRES_TUPLES_OK ||
				resultStatus == PGRES_COPY_IN ||
				resultStatus == PGRES_COPY_OUT)
			{						/* QE reported success */

				/*
				 * Save the index of the last successful PGresult. Can be given to
				 * cdbdisp_getPGresult() to get tuple count, etc.
				 */
				dispatchResult->okindex = resultIndex;

				if (DEBUG3 >= log_min_messages)
				{
					/* Don't use elog, it's not thread-safe */
					char	   *cmdStatus = PQcmdStatus(pRes);

					write_log("%s -> ok %s",
							  segdbDesc->whoami,
							  cmdStatus ? cmdStatus : "(no cmdStatus)");
				}

				if (resultStatus == PGRES_COPY_IN ||
					resultStatus == PGRES_COPY_OUT)
					return;
			}						/* QE reported success */

			/* Note QE error.  Cancel the whole statement if requested. */
			else
			{						/* QE reported an error */
				char	   *sqlstate = PQresultErrorField(pRes, PG_DIAG_SQLSTATE);
				int			errcode = 0;

				msg = PQresultErrorMessage(pRes);

				if (DEBUG2 >= log_min_messages)
				{
					/* Don't use elog, it's not thread-safe */
					write_log("%s -> %s %s  %s",
							  segdbDesc->whoami,
							  PQresStatus(resultStatus),
							  sqlstate ? sqlstate : "(no SQLSTATE)",
							  msg ? msg : "");
				}

				/*
				 * Convert SQLSTATE to an error code (ERRCODE_xxx). Use a generic
				 * nonzero error code if no SQLSTATE.
				 */
				if (sqlstate &&
					strlen(sqlstate) == 5)
					errcode = cdbdisp_sqlstate_to_errcode(sqlstate);

				/*
				 * Save first error code and the index of its PGresult buffer
				 * entry.
				 */
				cdbdisp_seterrcode(errcode, resultIndex, dispatchResult);
			}						/* QE reported an error */
		}							/* loop to call PQgetResult; won't block */


		if (DEBUG4 >= log_min_messages)
			write_log("processResultsSingle says we are finished with :  %s",segdbDesc->whoami);
		dispatchResult->stillRunning = false;
		if (DEBUG1 >= log_min_messages)
		{
			char		msec_str[32];
			switch (check_log_duration(msec_str, false))
			{
				case 1:
				case 2:
					write_log("duration to dispatch result received from thread (seg %d): %s ms", dispatchResult->segdbDesc->segindex,msec_str);
					break;
			}
		}
		if (PQisBusy(dispatchResult->segdbDesc->conn))
			write_log("We thought we were done, because finished==true, but libpq says we are still busy");
	}
}

/*
 * Cleanup routine for the dispatching thread.  This will indicate the thread
 * is not running any longer.
 */
static void
DecrementRunningCount(void *arg)
{
	pg_atomic_sub_fetch_u32((pg_atomic_uint32 *)&RunningThreadCount, 1);
}

/*
 * thread_DispatchCommand is the thread proc used to dispatch the command to one or more of the qExecs.
 *
 * NOTE: This function MUST NOT contain elog or ereport statements. (or most any other backend code)
 *		 elog is NOT thread-safe.  Developers should instead use something like:
 *
 *	if (DEBUG3 >= log_min_messages)
 *			write_log("my brilliant log statement here.");
 *
 * NOTE: In threads, we cannot use palloc, because it's not thread safe.
 */
void *
thread_DispatchCommand(void *arg)
{
	DispatchCommandParms		*pParms = (DispatchCommandParms *) arg;

	gp_set_thread_sigmasks();

	/*
	 * Mark that we are runnig a new thread.  The main thread will check
	 * it to see if there is still alive one.  Let's do this after we block
	 * signals so that nobody will intervent and mess up the value.
	 * (should we actually block signals before spawning a thread, as much
	 * like we do in fork??)
	 */
	pg_atomic_add_fetch_u32((pg_atomic_uint32 *)&RunningThreadCount, 1);

	/*
	 * We need to make sure the value will be decremented once the thread
	 * finishes.  Currently there is not such case but potentially we could
	 * have pthread_exit or thread cancellation in the middle of code, in
	 * which case we would miss to decrement value if we tried to do this
	 * without the cleanup callback facility.
	 */
	pthread_cleanup_push(DecrementRunningCount, NULL);
	{
		if (thread_DispatchOut(pParms))
		{
			/*
			 * thread_DispatchWaitSingle might have a problem with interupts
			 */
			if (pParms->db_count == 1 && false)
				thread_DispatchWaitSingle(pParms);
			else
				thread_DispatchWait(pParms);
		}
	}
	pthread_cleanup_pop(1);

	return (NULL);
}	/* thread_DispatchCommand */


/* Helper function to thread_DispatchCommand that decides if we should dispatch
 * to this segment database.
 *
 * (1) don't dispatch if there is already a query cancel notice pending.
 * (2) make sure our libpq connection is still good.
 *
 * NOTE: since this is called via a thread, the same rules apply as to
 *		 thread_DispatchCommand absolutely no elog'ing.
 */
bool
shouldStillDispatchCommand(DispatchCommandParms *pParms, CdbDispatchResult * dispatchResult)
{
	SegmentDatabaseDescriptor *segdbDesc = dispatchResult->segdbDesc;
	CdbDispatchResults *gangResults = dispatchResult->meleeResults;

	/* Don't dispatch to a QE that is not connected. Note, that PQstatus() correctly
	 * handles the case where segdbDesc->conn is NULL, and we *definitely* want to
	 * produce an error for that case. */
	if (PQstatus(segdbDesc->conn) == CONNECTION_BAD)
	{
		char	   *msg = PQerrorMessage(segdbDesc->conn);

		/* Save error info for later. */
		cdbdisp_appendMessage(dispatchResult, LOG,
							  ERRCODE_GP_INTERCONNECTION_ERROR,
							  "Lost connection to %s.  %s",
							  segdbDesc->whoami,
							  msg ? msg : "");

		if (DEBUG4 >= log_min_messages)
		{
			/* Don't use elog, it's not thread-safe */
			write_log("Lost connection: %s", segdbDesc->whoami);
		}

		/* Free the PGconn object at once whenever we notice it's gone bad. */
		PQfinish(segdbDesc->conn);
		segdbDesc->conn = NULL;
		dispatchResult->stillRunning = false;

		return false;
	}

	/*
	 * Don't submit if already encountered an error. The error has already
	 * been noted, so just keep quiet.
	 */
	if (pParms->waitMode == DISPATCH_WAIT_CANCEL || gangResults->errcode)
	{
		if (gangResults->cancelOnError)
		{
			dispatchResult->wasCanceled = true;

			if (Debug_cancel_print || DEBUG4 >= log_min_messages)
			{
				/* Don't use elog, it's not thread-safe */
				write_log("Error cleanup in progress; command not sent to %s",
						  segdbDesc->whoami);
			}
			return false;
		}
	}

	/*
	 * Don't submit if client told us to cancel. The cancellation request has
	 * already been noted, so hush.
	 */
	if (InterruptPending &&
		gangResults->cancelOnError)
	{
		dispatchResult->wasCanceled = true;
		if (Debug_cancel_print || DEBUG4 >= log_min_messages)
			write_log("Cancellation request pending; command not sent to %s",
					  segdbDesc->whoami);
		return false;
	}

	return true;
}	/* shouldStillDispatchCommand */


/* Helper function to thread_DispatchCommand that actually kicks off the
 * command on the libpq connection.
 *
 * NOTE: since this is called via a thread, the same rules apply as to
 *		 thread_DispatchCommand absolutely no elog'ing.
 */
static void
dispatchCommandQuery(CdbDispatchResult	*dispatchResult,
					 const char			*query_text,
					 int				query_text_len,
					 const char			*strCommand)
{
	SegmentDatabaseDescriptor *segdbDesc = dispatchResult->segdbDesc;
	PGconn	   *conn = segdbDesc->conn;
	TimestampTz beforeSend = 0;
	long		secs;
	int			usecs;

	/* Don't use elog, it's not thread-safe */
	if (DEBUG3 >= log_min_messages)
		write_log("%s <- %.120s", segdbDesc->whoami, strCommand);

	if (DEBUG1 >= log_min_messages)
		beforeSend = GetCurrentTimestamp();

	/*
	 * Submit the command asynchronously.
	 */
	if (PQsendGpQuery_shared(conn, (char *)query_text, query_text_len) == 0)
	{
		char	   *msg = PQerrorMessage(segdbDesc->conn);

		if (DEBUG3 >= log_min_messages)
			write_log("PQsendMPPQuery_shared error %s %s",
					  segdbDesc->whoami, msg ? msg : "");

		/* Note the error. */
		cdbdisp_appendMessage(dispatchResult, LOG,
							  ERRCODE_GP_INTERCONNECTION_ERROR,
							  "Command could not be sent to segment db %s;  %s",
							  segdbDesc->whoami, msg ? msg : "");
		PQfinish(conn);
		segdbDesc->conn = NULL;
		dispatchResult->stillRunning = false;
	}

	if (DEBUG1 >= log_min_messages)
	{
		TimestampDifference(beforeSend,
							GetCurrentTimestamp(),
							&secs, &usecs);

		if (secs != 0 || usecs > 1000) /* Time > 1ms? */
			write_log("time for PQsendGpQuery_shared %ld.%06d", secs, usecs);
	}


	/*
	 * We'll keep monitoring this QE -- whether or not the command
	 * was dispatched -- in order to check for a lost connection
	 * or any other errors that libpq might have in store for us.
	 */
}	/* dispatchCommand */



/* Helper function to thread_DispatchCommand that actually kicks off the
 * command on the libpq connection.
 *
 * NOTE: since this is called via a thread, the same rules apply as to
 *		 thread_DispatchCommand absolutely no elog'ing.
 */
static void
dispatchCommandDtxProtocol(CdbDispatchResult	*dispatchResult,
						   const char			*query_text,
						   int					query_text_len,
						   DtxProtocolCommand	dtxProtocolCommand)
{
	SegmentDatabaseDescriptor *segdbDesc = dispatchResult->segdbDesc;
	PGconn	   *conn = segdbDesc->conn;

	/* Don't use elog, it's not thread-safe */
	if (DEBUG3 >= log_min_messages)
		write_log("%s <- dtx protocol command %d", segdbDesc->whoami, (int)dtxProtocolCommand);

	/*
	 * Submit the command asynchronously.
	 */
	if (PQsendGpQuery_shared(conn, (char *)query_text, query_text_len) == 0)
	{
		char *msg = PQerrorMessage(segdbDesc->conn);

		if (DEBUG3 >= log_min_messages)
			write_log("PQsendMPPQuery_shared error %s %s",segdbDesc->whoami,
						 msg ? msg : "");
		/* Note the error. */
		cdbdisp_appendMessage(dispatchResult, LOG,
							  ERRCODE_GP_INTERCONNECTION_ERROR,
							  "Command could not be sent to segment db %s;  %s",
							  segdbDesc->whoami,
							  msg ? msg : "");
		PQfinish(conn);
		segdbDesc->conn = NULL;
		dispatchResult->stillRunning = false;
	}

	/*
	 * We'll keep monitoring this QE -- whether or not the command
	 * was dispatched -- in order to check for a lost connection
	 * or any other errors that libpq might have in store for us.
	 */
}	/* dispatchCommand */


/* Helper function to thread_DispatchCommand that handles errors that occur
 * during the poll() call.
 *
 * NOTE: since this is called via a thread, the same rules apply as to
 *		 thread_DispatchCommand absolutely no elog'ing.
 *
 * NOTE: The cleanup of the connections will be performed by handlePollTimeout().
 */
void
handlePollError(DispatchCommandParms *pParms,
				  int db_count,
				  int sock_errno)
{
	int			i;
	int			forceTimeoutCount;

	if (LOG >= log_min_messages)
	{
		/* Don't use elog, it's not thread-safe */
		write_log("handlePollError poll() failed; errno=%d", sock_errno);
	}

	/*
	 * Based on the select man page, we could get here with
	 * errno == EBADF (bad descriptor), EINVAL (highest descriptor negative or negative timeout)
	 * or ENOMEM (out of memory).
	 * This is most likely a programming error or a bad system failure, but we'll try to
	 * clean up a bit anyhow.
	 *
	 * MPP-3551: We *can* get here as a result of some hardware issues. the timeout code
	 * knows how to clean up if we've lost contact with one of our peers.
	 *
	 * We should check a connection's integrity before calling PQisBusy().
	 */
	for (i = 0; i < db_count; i++)
	{
		CdbDispatchResult *dispatchResult = pParms->dispatchResultPtrArray[i];

		/* Skip if already finished or didn't dispatch. */
		if (!dispatchResult->stillRunning)
			continue;

		/* We're done with this QE, sadly. */
		if (PQstatus(dispatchResult->segdbDesc->conn) == CONNECTION_BAD)
		{
			char *msg;

			msg = PQerrorMessage(dispatchResult->segdbDesc->conn);
			if (msg)
				write_log("Dispatcher encountered connection error on %s: %s",
						  dispatchResult->segdbDesc->whoami, msg);

			write_log("Dispatcher noticed bad connection in handlePollError()");

			/* Save error info for later. */
			cdbdisp_appendMessage(dispatchResult, LOG,
								  ERRCODE_GP_INTERCONNECTION_ERROR,
								  "Error after dispatch from %s: %s",
								  dispatchResult->segdbDesc->whoami,
								  msg ? msg : "unknown error");

			PQfinish(dispatchResult->segdbDesc->conn);
			dispatchResult->segdbDesc->conn = NULL;
			dispatchResult->stillRunning = false;
		}
	}

	forceTimeoutCount = 60; /* anything bigger than 30 */
	handlePollTimeout(pParms, db_count, &forceTimeoutCount, false);

	return;

	/* No point in trying to cancel the other QEs with select() broken. */
}	/* handlePollError */


/*
 * Send cancel/finish signal to still-running QE through libpq.
 * waitMode is either CANCEL or FINISH.  Returns true if we successfully
 * sent a signal (not necessarily received by the target process).
 */
static DispatchWaitMode
cdbdisp_signalQE(SegmentDatabaseDescriptor *segdbDesc,
				 DispatchWaitMode waitMode)
{
	char errbuf[256];
	PGcancel *cn = PQgetCancel(segdbDesc->conn);
	int		ret = 0;

	if (cn == NULL)
		return DISPATCH_WAIT_NONE;

	/*
	 * PQcancel uses some strcpy/strcat functions; let's
	 * clear this for safety.
	 */
	MemSet(errbuf, 0, sizeof(errbuf));

	if (Debug_cancel_print || DEBUG4 >= log_min_messages)
		write_log("Calling PQcancel for %s", segdbDesc->whoami);

	/*
	 * Send query-finish, unless the client really wants to cancel the
	 * query.  This could happen if cancel comes after we sent finish.
	 */
	if (waitMode == DISPATCH_WAIT_CANCEL)
		ret = PQcancel(cn, errbuf, 256);
	else if (waitMode == DISPATCH_WAIT_FINISH)
		ret = PQrequestFinish(cn, errbuf, 256);
	else
		write_log("unknown waitMode: %d", waitMode);

	if (ret == 0 && (Debug_cancel_print || LOG >= log_min_messages))
		write_log("Unable to cancel: %s", errbuf);

	PQfreeCancel(cn);

	return (ret != 0 ? waitMode : DISPATCH_WAIT_NONE);
}

/* Helper function to thread_DispatchCommand that handles timeouts that occur
 * during the poll() call.
 *
 * NOTE: since this is called via a thread, the same rules apply as to
 *		 thread_DispatchCommand absolutely no elog'ing.
 */
void
handlePollTimeout(DispatchCommandParms * pParms,
					int db_count,
					int *timeoutCounter, bool useSampling)
{
	CdbDispatchResult *dispatchResult;
	CdbDispatchResults *meleeResults;
	SegmentDatabaseDescriptor *segdbDesc;
	int			i;

	/*
	 * Are there any QEs that should be canceled?
	 *
	 * CDB TODO: PQcancel() is expensive, and we do them
	 *			 serially.	Just do a few each time; save some
	 *			 for the next timeout.
	 */
	for (i = 0; i < db_count; i++)
	{							/* loop to check connection status */
		DispatchWaitMode		waitMode;

		dispatchResult = pParms->dispatchResultPtrArray[i];
		if (dispatchResult == NULL)
			continue;
		segdbDesc = dispatchResult->segdbDesc;
		meleeResults = dispatchResult->meleeResults;

		/* Already finished with this QE? */
		if (!dispatchResult->stillRunning)
			continue;

		waitMode = DISPATCH_WAIT_NONE;

		/*
		 * Send query finish to this QE if QD is already done.
		 */
		if (pParms->waitMode == DISPATCH_WAIT_FINISH)
			waitMode = DISPATCH_WAIT_FINISH;

		/*
		 * However, escalate it to cancel if:
		 *   - user interrupt has occurred,
		 *   - or I'm told to send cancel,
		 *   - or an error has been reported by another QE,
		 *   - in case the caller wants cancelOnError and it was not canceled
		 */
		if ((InterruptPending ||
			pParms->waitMode == DISPATCH_WAIT_CANCEL ||
			meleeResults->errcode) &&
				(meleeResults->cancelOnError &&
				 !dispatchResult->wasCanceled))
			waitMode = DISPATCH_WAIT_CANCEL;

		/*
		 * Finally, don't send the signal if
		 *   - no action needed (NONE)
		 *   - the signal was already sent
		 *   - connection is dead
		 */
		if (waitMode != DISPATCH_WAIT_NONE &&
			waitMode != dispatchResult->sentSignal &&
			PQstatus(segdbDesc->conn) != CONNECTION_BAD)
		{
			dispatchResult->sentSignal =
				cdbdisp_signalQE(segdbDesc, waitMode);
		}
	}

	/*
	 * check the connection still valid, set 1 min time interval
	 * this may affect performance, should turn it off if required.
	 */
	if ((*timeoutCounter)++ > 30)
	{
		*timeoutCounter = 0;

		for (i = 0; i < db_count; i++)
		{
			dispatchResult = pParms->dispatchResultPtrArray[i];
			segdbDesc = dispatchResult->segdbDesc;

			if (DEBUG5 >= log_min_messages)
				write_log("checking status %d of %d     %s stillRunning %d",
						  i+1, db_count, segdbDesc->whoami, dispatchResult->stillRunning);

			/* Skip if already finished or didn't dispatch. */
			if (!dispatchResult->stillRunning)
				continue;

			/*
			 * If we hit the timeout, and the query has already been
			 * cancelled we'll try to re-cancel here.
			 *
			 * XXX we may not need this anymore.  It might be harmful
			 * rather than helpful, as it creates another connection.
			 */
			if (dispatchResult->sentSignal == DISPATCH_WAIT_CANCEL &&
				PQstatus(segdbDesc->conn) != CONNECTION_BAD)
			{
				dispatchResult->sentSignal =
					cdbdisp_signalQE(segdbDesc, DISPATCH_WAIT_CANCEL);
			}

			/* Skip the entry db. */
			if (segdbDesc->segindex < 0)
				continue;

			if (DEBUG5 >= log_min_messages)
				write_log("testing connection %d of %d     %s stillRunning %d",
						  i+1, db_count, segdbDesc->whoami, dispatchResult->stillRunning);

			if (!FtsTestConnection(segdbDesc->segment_database_info, false))
			{
				/* Note the error. */
				cdbdisp_appendMessage(dispatchResult, DEBUG1,
									  ERRCODE_GP_INTERCONNECTION_ERROR,
									  "Lost connection to one or more segments - fault detector checking for segment failures. (%s)",
									  segdbDesc->whoami);

				/*
				 * Not a good idea to store into the PGconn object. Instead,
				 * just close it.
				 */
				PQfinish(segdbDesc->conn);
				segdbDesc->conn = NULL;

				/* This connection is hosed. */
				dispatchResult->stillRunning = false;
			}
		}
	}

}	/* handlePollTimeout */



void
CollectQEWriterTransactionInformation(SegmentDatabaseDescriptor *segdbDesc, CdbDispatchResult *dispatchResult)
{
	PGconn *conn = segdbDesc->conn;

	if (conn && conn->QEWriter_HaveInfo)
	{
		dispatchResult->QEIsPrimary = true;
		dispatchResult->QEWriter_HaveInfo = true;
		dispatchResult->QEWriter_DistributedTransactionId = conn->QEWriter_DistributedTransactionId;
		dispatchResult->QEWriter_CommandId = conn->QEWriter_CommandId;
		if (conn && conn->QEWriter_Dirty)
		{
			dispatchResult->QEWriter_Dirty = true;
		}
	}
}

bool							/* returns true if command complete */
processResults(CdbDispatchResult *dispatchResult)
{
	SegmentDatabaseDescriptor *segdbDesc = dispatchResult->segdbDesc;
	char	   *msg;
	int			rc;

	/* MPP-2518: PQisBusy() has side-effects */
	if (DEBUG5 >= log_min_messages)
	{
		write_log("processResults.  isBusy = %d", PQisBusy(segdbDesc->conn));

		if (PQstatus(segdbDesc->conn) == CONNECTION_BAD)
			goto connection_error;
	}

	/* Receive input from QE. */
	rc = PQconsumeInput(segdbDesc->conn);

	/* If PQconsumeInput fails, we're hosed. */
	if (rc == 0)
	{ /* handle PQconsumeInput error */
		goto connection_error;
	}

	/* MPP-2518: PQisBusy() has side-effects */
	if (DEBUG4 >= log_min_messages && PQisBusy(segdbDesc->conn))
		write_log("PQisBusy");

	/* If we have received one or more complete messages, process them. */
	while (!PQisBusy(segdbDesc->conn))
	{							/* loop to call PQgetResult; won't block */
		PGresult   *pRes;
		ExecStatusType resultStatus;
		int			resultIndex;

		/* MPP-2518: PQisBusy() does some error handling, which can
		 * cause the connection to die -- we can't just continue on as
		 * if the connection is happy without checking first.
		 *
		 * For example, cdbdisp_numPGresult() will return a completely
		 * bogus value! */
		if (PQstatus(segdbDesc->conn) == CONNECTION_BAD || segdbDesc->conn->sock == -1)
		{ /* connection is dead. */
			goto connection_error;
		}

		resultIndex = cdbdisp_numPGresult(dispatchResult);

		if (DEBUG4 >= log_min_messages)
			write_log("PQgetResult");
		/* Get one message. */
		pRes = PQgetResult(segdbDesc->conn);

		CollectQEWriterTransactionInformation(segdbDesc, dispatchResult);

		/*
		 * Command is complete when PGgetResult() returns NULL. It is critical
		 * that for any connection that had an asynchronous command sent thru
		 * it, we call PQgetResult until it returns NULL. Otherwise, the next
		 * time a command is sent to that connection, it will return an error
		 * that there's a command pending.
		 */
		if (!pRes)
		{						/* end of results */
			if (DEBUG4 >= log_min_messages)
			{
				/* Don't use elog, it's not thread-safe */
				write_log("%s -> idle", segdbDesc->whoami);
			}

			return true;		/* this is normal end of command */
		}						/* end of results */


		/* Attach the PGresult object to the CdbDispatchResult object. */
		cdbdisp_appendResult(dispatchResult, pRes);

		/* Did a command complete successfully? */
		resultStatus = PQresultStatus(pRes);
		if (resultStatus == PGRES_COMMAND_OK ||
			resultStatus == PGRES_TUPLES_OK ||
			resultStatus == PGRES_COPY_IN ||
			resultStatus == PGRES_COPY_OUT)
		{						/* QE reported success */

			/*
			 * Save the index of the last successful PGresult. Can be given to
			 * cdbdisp_getPGresult() to get tuple count, etc.
			 */
			dispatchResult->okindex = resultIndex;

			if (DEBUG3 >= log_min_messages)
			{
				/* Don't use elog, it's not thread-safe */
				char	   *cmdStatus = PQcmdStatus(pRes);

				write_log("%s -> ok %s",
						  segdbDesc->whoami,
						  cmdStatus ? cmdStatus : "(no cmdStatus)");
			}

			/* SREH - get number of rows rejected from QE if any */
			if(pRes->numRejected > 0)
				dispatchResult->numrowsrejected += pRes->numRejected;

			if (resultStatus == PGRES_COPY_IN ||
				resultStatus == PGRES_COPY_OUT)
				return true;
		}						/* QE reported success */

		/* Note QE error.  Cancel the whole statement if requested. */
		else
		{						/* QE reported an error */
			char	   *sqlstate = PQresultErrorField(pRes, PG_DIAG_SQLSTATE);
			int			errcode = 0;

			msg = PQresultErrorMessage(pRes);

			if (DEBUG2 >= log_min_messages)
			{
				/* Don't use elog, it's not thread-safe */
				write_log("%s -> %s %s  %s",
						  segdbDesc->whoami,
						  PQresStatus(resultStatus),
						  sqlstate ? sqlstate : "(no SQLSTATE)",
						  msg ? msg : "");
			}

			/*
			 * Convert SQLSTATE to an error code (ERRCODE_xxx). Use a generic
			 * nonzero error code if no SQLSTATE.
			 */
			if (sqlstate &&
				strlen(sqlstate) == 5)
				errcode = cdbdisp_sqlstate_to_errcode(sqlstate);

			/*
			 * Save first error code and the index of its PGresult buffer
			 * entry.
			 */
			cdbdisp_seterrcode(errcode, resultIndex, dispatchResult);
		}						/* QE reported an error */
	}							/* loop to call PQgetResult; won't block */

	return false;				/* we must keep on monitoring this socket */

connection_error:
	msg = PQerrorMessage(segdbDesc->conn);

	if (msg)
		write_log("Dispatcher encountered connection error on %s: %s", segdbDesc->whoami, msg);

	/* Save error info for later. */
	cdbdisp_appendMessage(dispatchResult, LOG,
						  ERRCODE_GP_INTERCONNECTION_ERROR,
						  "Error on receive from %s: %s",
						  segdbDesc->whoami,
						  msg ? msg : "unknown error");

	/* Can't recover, so drop the connection. */
	PQfinish(segdbDesc->conn);
	segdbDesc->conn = NULL;
	dispatchResult->stillRunning = false;

	return true; /* connection is gone! */
}	/* processResults */



/*
 * cdbdisp_makeDispatchThreads:
 * Allocates memory for a CdbDispatchCmdThreads struct that holds
 * the thread count and array of dispatch command parameters (which
 * is being allocated here as well).
 */
CdbDispatchCmdThreads *
cdbdisp_makeDispatchThreads(int paramCount)
{
	CdbDispatchCmdThreads *dThreads = palloc0(sizeof(*dThreads));

	dThreads->dispatchCommandParmsAr =
		(DispatchCommandParms *)palloc0(paramCount * sizeof(DispatchCommandParms));

	dThreads->dispatchCommandParmsArSize = paramCount;

    dThreads->threadCount = 0;

    return dThreads;
}                               /* cdbdisp_makeDispatchThreads */

/*
 * cdbdisp_destroyDispatchThreads:
 * Frees all memory allocated in CdbDispatchCmdThreads struct.
 */
void
cdbdisp_destroyDispatchThreads(CdbDispatchCmdThreads *dThreads)
{

	DispatchCommandParms *pParms;
	int i;

	if (!dThreads)
        return;

	/*
	 * pfree the memory allocated for the dispatchCommandParmsAr
	 */
	elog(DEBUG3, "destroydispatchthreads: threadcount %d array size %d", dThreads->threadCount, dThreads->dispatchCommandParmsArSize);
	for (i = 0; i < dThreads->dispatchCommandParmsArSize; i++)
	{
		pParms = &(dThreads->dispatchCommandParmsAr[i]);
		if (pParms->dispatchResultPtrArray)
		{
			pfree(pParms->dispatchResultPtrArray);
			pParms->dispatchResultPtrArray = NULL;
		}
		if (pParms->query_text)
		{
			/* NOTE: query_text gets malloc()ed by the pqlib code, use
			 * free() not pfree() */
			free(pParms->query_text);
			pParms->query_text = NULL;
		}

		if (pParms->nfds != 0)
		{
			if (pParms->fds != NULL)
				pfree(pParms->fds);
			pParms->fds = NULL;
			pParms->nfds = 0;
		}

		switch (pParms->mppDispatchCommandType)
		{
			case GP_DISPATCH_COMMAND_TYPE_QUERY:
			{
				DispatchCommandQueryParms *pQueryParms = &pParms->queryParms;

				if (pQueryParms->strCommand)
				{
					/* Caller frees if desired */
					pQueryParms->strCommand = NULL;
				}

				if (pQueryParms->serializedDtxContextInfo)
				{
					if (i==0)
						pfree(pQueryParms->serializedDtxContextInfo);
					pQueryParms->serializedDtxContextInfo = NULL;
				}

				if (pQueryParms->serializedSliceInfo)
				{
					if (i==0)
						pfree(pQueryParms->serializedSliceInfo);
					pQueryParms->serializedSliceInfo = NULL;
				}

				if (pQueryParms->serializedQuerytree)
				{
					if (i==0)
						pfree(pQueryParms->serializedQuerytree);
					pQueryParms->serializedQuerytree = NULL;
				}

				if (pQueryParms->serializedPlantree)
				{
					if (i==0)
						pfree(pQueryParms->serializedPlantree);
					pQueryParms->serializedPlantree = NULL;
				}

				if (pQueryParms->serializedParams)
				{
					if (i==0)
						pfree(pQueryParms->serializedParams);
					pQueryParms->serializedParams = NULL;
				}
			}
			break;

			case GP_DISPATCH_COMMAND_TYPE_DTX_PROTOCOL:
			{
				DispatchCommandDtxProtocolParms *pDtxProtocolParms = &pParms->dtxProtocolParms;

				pDtxProtocolParms->dtxProtocolCommand = 0;
			}
			break;

			default:
				elog(FATAL, "Unrecognized MPP dispatch command type: %d",
					 (int) pParms->mppDispatchCommandType);
		}
	}

	pfree(dThreads->dispatchCommandParmsAr);
	dThreads->dispatchCommandParmsAr = NULL;

    dThreads->dispatchCommandParmsArSize = 0;
    dThreads->threadCount = 0;

	pfree(dThreads);
}                               /* cdbdisp_destroyDispatchThreads */
