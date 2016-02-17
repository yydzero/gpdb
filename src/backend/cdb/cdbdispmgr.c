/*-------------------------------------------------------------------------
 *
 * cdbdispmgr.c
 *	  Manager for dispatcher on how to dispatch request and check response.
 *
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

extern bool Test_print_direct_dispatch_info;
extern volatile int32 RunningThreadCount;

static DtxContextInfo TempQDDtxContextInfo = DtxContextInfo_StaticInit;

/*
 * We need an array describing the relationship between a slice and
 * the number of "child" slices which depend on it.
 */
typedef struct {
	int sliceIndex;
	int children;
	Slice *slice;
} sliceVec;

static int fillSliceVector(SliceTable * sliceTable, int sliceIndex, sliceVec *sliceVector, int len);


static struct pg_result ** cdbdisp_dispatchDtxProtocolCommand(DtxProtocolCommand dtxProtocolCommand, int flags, char *dtxProtocolCommandLoggingStr,
						char *gid, DistributedTransactionId gxid, StringInfo errmsgbuf, int	*numresults, bool *badGangs,
						CdbDispatchDirectDesc *direct, char	*argument, int argumentLength );
static void cdbdisp_dispatchX(DispatchCommandQueryParms *pQueryParms, bool cancelOnError, struct SliceTable *sliceTbl,
						struct CdbDispatcherState *ds);
static void cdbdisp_dispatchCommand(const char *strCommand, char *serializedQuerytree, int serializedQuerytreelen, bool cancelOnError,
						bool needTwoPhase, bool withSnapshot, struct CdbDispatcherState *ds);
static void cdbdisp_dispatchSetCommandToAllGangs(const char	*strCommand, char *serializedQuerytree, int serializedQuerytreelen,
						char *serializedPlantree, int serializedPlantreelen, bool cancelOnError, bool needTwoPhase, struct CdbDispatcherState *ds);

static void cdbdisp_finishCommand(struct CdbDispatcherState *ds, void (*handle_results_callback)(struct CdbDispatchResults *primaryResults, void *ctx),
						void *ctx);
static void CdbCheckDispatchResult(struct CdbDispatcherState *ds, DispatchWaitMode waitMode);
static void cdbdisp_handleError(struct CdbDispatcherState *ds);
static char * qdSerializeDtxContextInfo(int * size, bool wantSnapshot, bool inCursor, int txnOptions, char *debugCaller);
static bool cdbdisp_check_estate_for_cancel(struct EState *estate);
static int generateTxnOptions(bool needTwoPhase);


static DispatcherMgrMethods dispatcherMgrMethods = {
		cdbdisp_dispatchDtxProtocolCommand,
		cdbdisp_dispatchX,
		cdbdisp_dispatchCommand,
		cdbdisp_dispatchSetCommandToAllGangs,

		cdbdisp_finishCommand,
		CdbCheckDispatchResult,
		cdbdisp_destroyDispatchResults,
		cdbdisp_handleError,
		qdSerializeDtxContextInfo,
		cdbdisp_check_estate_for_cancel,

		generateTxnOptions,

		// cleanup
};

inline DispatcherMgrMethods GetDispatcherMgr()
{
	return dispatcherMgrMethods;
}

/* internal functions */
static void CdbCheckDispatchResultInt(struct CdbDispatcherState *ds,
						  struct SegmentDatabaseDescriptor *** failedSegDB,
						  int *numOfFailed,
						  DispatchWaitMode waitMode);
static void cdbdisp_clearGangActiveFlag(CdbDispatcherState *ds);

/*
 * cdbdisp_dispatchDtxProtocolCommand:
 * Sends a non-cancelable command to all segment dbs
 *
 * Returns a malloc'ed array containing the struct pg_result objects thus
 * produced; the caller must PQclear() them and free() the array.
 * A NULL entry follows the last used entry in the array.
 *
 * Any error messages - whether or not they are associated with
 * struct pg_result objects - are appended to a StringInfo buffer provided
 * by the caller.
 */
static struct pg_result **				/* returns ptr to array of struct pg_result ptrs */
cdbdisp_dispatchDtxProtocolCommand(DtxProtocolCommand dtxProtocolCommand,
								   int	flags,
								   char	*dtxProtocolCommandLoggingStr,
								   char	*gid,
								   DistributedTransactionId	gxid,
								   StringInfo errmsgbuf,
								   int *numresults,
								   bool *badGangs,
								   CdbDispatchDirectDesc *direct,
								   char *argument, int argumentLength)
{
	CdbDispatcherState ds = {NULL, NULL};

	struct pg_result  **resultSets = NULL;

	DispatchCommandDtxProtocolParms dtxProtocolParms;
	Gang	*primaryGang;
	int		nsegdb = getgpsegmentCount();

	elog((Debug_print_full_dtm ? LOG : DEBUG5), "cdbdisp_dispatchDtxProtocolCommand: %s for gid = %s, direct content #: %d",
		 dtxProtocolCommandLoggingStr, gid, direct->directed_dispatch ? direct->content[0] : -1);

	*badGangs = false;

	MemSet(&dtxProtocolParms, 0, sizeof(dtxProtocolParms));
	dtxProtocolParms.dtxProtocolCommand = dtxProtocolCommand;
	dtxProtocolParms.flags = flags;
	dtxProtocolParms.dtxProtocolCommandLoggingStr = dtxProtocolCommandLoggingStr;
	if (strlen(gid) >= TMGIDSIZE)
		elog(PANIC, "Distribute transaction identifier too long (%d)",
			 (int)strlen(gid));
	memcpy(dtxProtocolParms.gid, gid, TMGIDSIZE);
	dtxProtocolParms.gxid = gxid;
	dtxProtocolParms.argument = argument;
	dtxProtocolParms.argumentLength = argumentLength;

	/*
	 * Allocate a primary QE for every available segDB in the system.
	 */
	primaryGang = GetGangMgr().allocateWriterGang();

	Assert(primaryGang);

	if (primaryGang->dispatcherActive)
	{
		elog(LOG, "cdbdisp_dispatchDtxProtocolCommand: primary gang marked active re-marking");
		primaryGang->dispatcherActive = false;
	}

	dtxProtocolParms.primary_gang_id = primaryGang->gang_id;

	/*
     * Dispatch the command.
     */
    ds.dispatchThreads = NULL;

	ds.primaryResults = cdbdisp_makeDispatchResults(nsegdb, 0, /* cancelOnError */ false);

	ds.primaryResults->writer_gang = primaryGang;

	cdbdisp_dispatchToGang(&ds, GP_DISPATCH_COMMAND_TYPE_DTX_PROTOCOL,
						   &dtxProtocolParms,
						   primaryGang, -1, 1, direct);

	/* Wait for all QEs to finish.	Don't cancel. */
	CdbCheckDispatchResult(&ds, DISPATCH_WAIT_NONE);

	if (! GetGangMgr().gangOK(primaryGang))
	{
		*badGangs = true;

		elog((Debug_print_full_dtm ? LOG : DEBUG5),
			 "cdbdisp_dispatchDtxProtocolCommand: Bad gang from dispatch of %s for gid = %s",
			 dtxProtocolCommandLoggingStr, gid);
	}

	resultSets = cdbdisp_returnResults(ds.primaryResults, errmsgbuf, numresults);

	/* free memory allocated for the dispatch threads struct */
	cdbdisp_destroyDispatchThreads(ds.dispatchThreads);

	return resultSets;
}	/* cdbdisp_dispatchDtxProtocolCommand */


/*
 * This code was refactored out of cdbdisp_dispatchPlan.  It's
 * used both for dispatching plans when we are using normal gangs,
 * and for dispatching all statements from Query Dispatch Agents
 * when we are using dispatch agents.
 */
static void
cdbdisp_dispatchX(DispatchCommandQueryParms *pQueryParms,
				  bool cancelOnError,
				  struct SliceTable *sliceTbl,
				  struct CdbDispatcherState *ds)
{
	int			oldLocalSlice = 0;
	sliceVec	*sliceVector = NULL;
	int			nSlices = 1;
	int			sliceLim = 1;
	int			iSlice;
	int			rootIdx = pQueryParms->rootIdx;

	if (log_dispatch_stats)
		ResetUsage();

	ds->primaryResults = NULL;
	ds->dispatchThreads = NULL;

	Assert(Gp_role == GP_ROLE_DISPATCH);

	if (sliceTbl)
	{
		Assert(rootIdx == 0 ||
			   (rootIdx > sliceTbl->nMotions &&
				rootIdx <= sliceTbl->nMotions + sliceTbl->nInitPlans));

		/*
		 * Keep old value so we can restore it.  We use this field as a parameter.
		 */
		oldLocalSlice = sliceTbl->localSlice;

		/*
		 * Traverse the slice tree in sliceTbl rooted at rootIdx and build a
		 * vector of slice indexes specifying the order of [potential] dispatch.
		 */
		sliceLim = list_length(sliceTbl->slices);
		sliceVector = palloc0(sliceLim * sizeof(sliceVec));

		nSlices = fillSliceVector(sliceTbl, rootIdx, sliceVector, sliceLim);
	}

	/* Allocate result array with enough slots for QEs of primary gangs. */
	ds->primaryResults = cdbdisp_makeDispatchResults(nSlices * GetGangMgr().largestGangsize(),
												   sliceLim,
												   cancelOnError);

	cdb_total_plans++;
	cdb_total_slices += nSlices;
	if (nSlices > cdb_max_slices)
		cdb_max_slices = nSlices;

	/* must have somebody to dispatch to. */
	Assert(sliceTbl != NULL || pQueryParms->primary_gang_id > 0);

	if (DEBUG1 >= log_min_messages)
	{
		char		msec_str[32];
		switch (check_log_duration(msec_str, false))
		{
			case 1:
			case 2:
				ereport(LOG, (errmsg("duration to start of dispatch send (root %d): %s ms", pQueryParms->rootIdx, msec_str)));
				break;
		}
	}

	/*
	 * Now we need to call CDBDispatchCommand once per slice.  Each such
	 * call dispatches a MPPEXEC command to each of the QEs assigned to
	 * the slice.
	 *
	 * The QE information goes in two places: (1) in the argument to the
	 * function CDBDispatchCommand, and (2) in the serialized
	 * command sent to the QEs.
	 *
	 * So, for each slice in the tree...
	 */
	for (iSlice = 0; iSlice < nSlices; iSlice++)
	{
		CdbDispatchDirectDesc direct;
		Gang	   *primaryGang = NULL;
		Slice *slice = NULL;
		int si = -1;

		if (sliceVector)
		{
			/*
			 * Sliced dispatch, and we are either the dispatch agent, or we are
			 * the QD and are not using Dispatch Agents.
			 *
			 * So, dispatch to each slice.
			 */
			slice = sliceVector[iSlice].slice;
			si = slice->sliceIndex;

			/*
			 * Is this a slice we should dispatch?
			 */
			if (slice && slice->gangType == GANGTYPE_UNALLOCATED)
			{
				/*
				 * Most slices are dispatched, however, in many  cases the
				 * root runs only on the QD and is not dispatched to the QEs.
				 */
				continue;
			}

			primaryGang = slice->primaryGang;

			/*
			 * If we are on the dispatch agent, the gang pointers aren't filled in.
			 * We must look them up by ID
			 */
			if (primaryGang == NULL)
			{
				elog(DEBUG2,"Dispatch %d, Gangs are %d, type=%d",iSlice, slice->primary_gang_id, slice->gangType);
				primaryGang = GetGangMgr().findGangById(slice->primary_gang_id);

				Assert(primaryGang != NULL);
				if (primaryGang != NULL)
					Assert(primaryGang->type == slice->gangType || primaryGang->type == GANGTYPE_PRIMARY_WRITER);

				if (primaryGang == NULL)
					continue;
			}

			if (slice->directDispatch.isDirectDispatch)
			{
				direct.directed_dispatch = true;
				direct.count = list_length(slice->directDispatch.contentIds);
				Assert(direct.count == 1); /* only support to single content right now.  If this changes then we need to change from a list to another structure to avoid n^2 cases */
				direct.content[0] = linitial_int(slice->directDispatch.contentIds);

				if (Test_print_direct_dispatch_info)
				{
					elog(INFO, "Dispatch command to SINGLE content");
				}
			}
			else
			{
				direct.directed_dispatch = false;
				direct.count = 0;

				if (Test_print_direct_dispatch_info)
				{
					elog(INFO, "Dispatch command to ALL contents");
				}
			}
		}
		else
		{
			direct.directed_dispatch = false;
			direct.count = 0;

			if (Test_print_direct_dispatch_info)
			{
				elog(INFO, "Dispatch command to ALL contents");
			}

			/*
			 *  Non-sliced, used specified gangs
			 */
			elog(DEBUG2,"primary %d",pQueryParms->primary_gang_id);
			if (pQueryParms->primary_gang_id > 0)
				primaryGang = GetGangMgr().findGangById(pQueryParms->primary_gang_id);
		}

		Assert(primaryGang != NULL);	/* Must have a gang to dispatch to */
		if (primaryGang)
		{
			Assert(ds->primaryResults && ds->primaryResults->resultArray);
		}

		/* Bail out if already got an error or cancellation request. */
		if (cancelOnError)
		{
			if (ds->primaryResults->errcode)
				break;
			if (InterruptPending)
				break;
		}

		/*
		 * Dispatch the plan to our primaryGang.
		 * Doesn't wait for it to finish.
		 */
		if (primaryGang != NULL)
		{
			if (primaryGang->type == GANGTYPE_PRIMARY_WRITER)
				ds->primaryResults->writer_gang = primaryGang;

			cdbdisp_dispatchToGang(ds,
								   GP_DISPATCH_COMMAND_TYPE_QUERY,
								   pQueryParms, primaryGang,
								   si, sliceLim, &direct);
		}
	}

	if (sliceVector)
		pfree(sliceVector);

	if (sliceTbl)
		sliceTbl->localSlice = oldLocalSlice;

	/*
	 * If bailed before completely dispatched, stop QEs and throw error.
	 */
	if (iSlice < nSlices)
	{
		elog(Debug_cancel_print ? LOG : DEBUG2, "Plan dispatch canceled; dispatched %d of %d slices", iSlice, nSlices);

		/* Cancel any QEs still running, and wait for them to terminate. */
		CdbCheckDispatchResult(ds, DISPATCH_WAIT_CANCEL);

		/*
		 * Check and free the results of all gangs. If any QE had an
		 * error, report it and exit via PG_THROW.
		 */
		cdbdisp_finishCommand(ds, NULL, NULL);

		/* Wasn't an error, must have been an interrupt. */
		CHECK_FOR_INTERRUPTS();

		/* Strange!  Not an interrupt either. */
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg_internal("Unable to dispatch plan.")));
	}

	if (DEBUG1 >= log_min_messages)
	{
		char		msec_str[32];
		switch (check_log_duration(msec_str, false))
		{
			case 1:
			case 2:
				ereport(LOG, (errmsg("duration to dispatch out (root %d): %s ms", pQueryParms->rootIdx,msec_str)));
				break;
		}
	}

}	/* cdbdisp_dispatchX */



/*
 * cdbdisp_dispatchCommand:
 * Send the strCommand SQL statement to all segdbs in the cluster
 * cancelOnError indicates whether an error
 * occurring on one of the qExec segdbs should cause all still-executing commands to cancel
 * on other qExecs. Normally this would be true.  The commands are sent over the libpq
 * connections that were established during gang creation.	They are run inside of threads.
 * The number of segdbs handled by any one thread is determined by the
 * guc variable gp_connections_per_thread.
 *
 * The CdbDispatchResults objects allocated for the command
 * are returned in *pPrimaryResults
 * The caller, after calling CdbCheckDispatchResult(), can
 * examine the CdbDispatchResults objects, can keep them as
 * long as needed, and ultimately must free them with
 * cdbdisp_destroyDispatchResults() prior to deallocation
 * of the memory context from which they were allocated.
 *
 * NB: Callers should use PG_TRY()/PG_CATCH() if needed to make
 * certain that the CdbDispatchResults objects are destroyed by
 * cdbdisp_destroyDispatchResults() in case of error.
 * To wait for completion, check for errors, and clean up, it is
 * suggested that the caller use cdbdisp_finishCommand().
 */
static void
cdbdisp_dispatchCommand(const char                 *strCommand,
						char			   		   *serializedQuerytree,
						int							serializedQuerytreelen,
                        bool                        cancelOnError,
                        bool						needTwoPhase,
                        bool						withSnapshot,
						CdbDispatcherState		*ds)
{
	DispatchCommandQueryParms queryParms;
	Gang	*primaryGang;
	int		nsegdb = getgpsegmentCount();
	CdbComponentDatabaseInfo *qdinfo;

	if (log_dispatch_stats)
		ResetUsage();

	if (DEBUG5 >= log_min_messages)
    	elog(DEBUG3, "cdbdisp_dispatchCommand: %s (needTwoPhase = %s)",
	    	 strCommand, (needTwoPhase ? "true" : "false"));
    else
    	elog((Debug_print_full_dtm ? LOG : DEBUG3), "cdbdisp_dispatchCommand: %.50s (needTwoPhase = %s)",
    	     strCommand, (needTwoPhase ? "true" : "false"));

    ds->primaryResults = NULL;
	ds->dispatchThreads = NULL;

	MemSet(&queryParms, 0, sizeof(queryParms));
	queryParms.strCommand = strCommand;
	queryParms.serializedQuerytree = serializedQuerytree;
	queryParms.serializedQuerytreelen = serializedQuerytreelen;

	/*
	 * Allocate a primary QE for every available segDB in the system.
	 */
	primaryGang = GetGangMgr().allocateWriterGang();

	Assert(primaryGang);

	queryParms.primary_gang_id = primaryGang->gang_id;

	/* Serialize a version of our DTX Context Info */
	queryParms.serializedDtxContextInfo =
		qdSerializeDtxContextInfo(&queryParms.serializedDtxContextInfolen, withSnapshot, false, generateTxnOptions(needTwoPhase), "cdbdisp_dispatchCommand");

	/* sequence server info */
	qdinfo = &(getComponentDatabases()->entry_db_info[0]);
	Assert(qdinfo != NULL && qdinfo->hostip != NULL);
	queryParms.seqServerHost = pstrdup(qdinfo->hostip);
	queryParms.seqServerHostlen = strlen(qdinfo->hostip) + 1;
	queryParms.seqServerPort = seqServerCtl->seqServerPort;

	/*
	 * Dispatch the command.
	 */
	ds->primaryResults = cdbdisp_makeDispatchResults(nsegdb, 0, cancelOnError);
	ds->primaryResults->writer_gang = primaryGang;

	cdbdisp_dispatchToGang(ds,
						   GP_DISPATCH_COMMAND_TYPE_QUERY,
						   &queryParms,
						   primaryGang, -1, 1, DEFAULT_DISP_DIRECT);

	/*
	 * don't pfree serializedShapshot here, it will be pfree'd when
	 * the first thread is destroyed.
	 */

}	/* cdbdisp_dispatchCommand */

/*
 * Dispatch SET command to all gangs.
 *
 * Can not dispatch SET commands to busy reader gangs (allocated by cursors) directly because another command is already in progress.
 * Cursors only allocate reader gangs, so primary writer and idle reader gangs can be dispatched to.
 */
static void
cdbdisp_dispatchSetCommandToAllGangs(const char	*strCommand,
								  char			*serializedQuerytree,
								  int			serializedQuerytreelen,
								  char			*serializedPlantree,
								  int			serializedPlantreelen,
								  bool			cancelOnError,
								  bool			needTwoPhase,
								  struct CdbDispatcherState *ds)
{
	DispatchCommandQueryParms queryParms;

	Gang		*primaryGang;
	List		*idleReaderGangs;
	List		*busyReaderGangs;
	ListCell	*le;

	int			nsegdb = getgpsegmentCount();
	int			gangCount;

	ds->primaryResults = NULL;
	ds->dispatchThreads = NULL;

	MemSet(&queryParms, 0, sizeof(queryParms));
	queryParms.strCommand = strCommand;
	queryParms.serializedQuerytree = serializedQuerytree;
	queryParms.serializedQuerytreelen = serializedQuerytreelen;
	queryParms.serializedPlantree = serializedPlantree;
	queryParms.serializedPlantreelen = serializedPlantreelen;

	/*
	 * Allocate a primary QE for every available segDB in the system.
	 */
	primaryGang = GetGangMgr().allocateWriterGang();

	Assert(primaryGang);

	queryParms.primary_gang_id = primaryGang->gang_id;

	/* serialized a version of our snapshot */
	queryParms.serializedDtxContextInfo =
		qdSerializeDtxContextInfo(&queryParms.serializedDtxContextInfolen, true /* withSnapshot */, false /* cursor*/,
								  generateTxnOptions(needTwoPhase), "cdbdisp_dispatchSetCommandToAllGangs");

	idleReaderGangs = GetGangMgr().getAllIdleReaderGangs();
	busyReaderGangs = GetGangMgr().getAllBusyReaderGangs();

	/*
	 * Dispatch the command.
	 */
	gangCount = 1 + list_length(idleReaderGangs);
	ds->primaryResults = cdbdisp_makeDispatchResults(nsegdb * gangCount, 0, cancelOnError);

	ds->primaryResults->writer_gang = primaryGang;
	cdbdisp_dispatchToGang(ds,
						   GP_DISPATCH_COMMAND_TYPE_QUERY,
						   &queryParms,
						   primaryGang, -1, gangCount, DEFAULT_DISP_DIRECT);

	foreach(le, idleReaderGangs)
	{
		Gang  *rg = lfirst(le);
		cdbdisp_dispatchToGang(ds,
							   GP_DISPATCH_COMMAND_TYPE_QUERY,
							   &queryParms,
							   rg, -1, gangCount, DEFAULT_DISP_DIRECT);
	}

	/*
	 *Can not send set command to busy gangs, so those gangs
	 *can not be reused because their GUC is not set.
	 */
	foreach(le, busyReaderGangs)
	{
		Gang  *rg = lfirst(le);
		rg->noReuse = true;
	}
}	/* cdbdisp_dispatchSetCommandToAllGangs */


/* Wait for all QEs to finish, then report any errors from the given
 * CdbDispatchResults objects and free them.  If not all QEs in the
 * associated gang(s) executed the command successfully, throws an
 * error and does not return.  No-op if both CdbDispatchResults ptrs are NULL.
 * This is a convenience function; callers with unusual requirements may
 * instead call CdbCheckDispatchResult(), etc., directly.
 */
static void
cdbdisp_finishCommand(struct CdbDispatcherState *ds,
					  void (*handle_results_callback)(CdbDispatchResults *primaryResults, void *ctx),
					  void *ctx)
{
	StringInfoData buf;
	int			errorcode = 0;

	/* If cdbdisp_dispatchToGang() wasn't called, don't wait. */
	if (!ds || !ds->primaryResults)
		return;

	/*
	 * If we are called in the dying sequence, don't touch QE connections.
	 * Anything below could cause ERROR in which case we would miss a chance
	 * to clean up shared memory as this is from AbortTransaction.
	 * QE may stay a bit longer, but since we can consider QD as libpq
	 * client to QE, they will notice that we as a client do not
	 * appear anymore and will finish soon.  Also ERROR report doesn't
	 * go to the client anyway since we are in proc_exit.
	 */
	if (proc_exit_inprogress)
		return;

	/* Wait for all QEs to finish. Don't cancel them. */
	CdbCheckDispatchResult(ds, DISPATCH_WAIT_NONE);

	/* If no errors, free the CdbDispatchResults objects and return. */
	if (ds->primaryResults)
		errorcode = ds->primaryResults->errcode;

	if (!errorcode)
	{
		/* Call the callback function to handle the results */
		if (handle_results_callback != NULL)
			handle_results_callback(ds->primaryResults, ctx);

		cdbdisp_destroyDispatchResults(ds->primaryResults);
		ds->primaryResults = NULL;
		cdbdisp_destroyDispatchThreads(ds->dispatchThreads);
		ds->dispatchThreads = NULL;
		return;
	}

	/* Format error messages from the primary gang. */
	initStringInfo(&buf);
	cdbdisp_dumpDispatchResults(ds->primaryResults, &buf, false);

	cdbdisp_destroyDispatchResults(ds->primaryResults);
	ds->primaryResults = NULL;

	cdbdisp_destroyDispatchThreads(ds->dispatchThreads);
	ds->dispatchThreads = NULL;

	/* Too bad, our gang got an error. */
	PG_TRY();
	{
		ereport(ERROR, (errcode(errorcode),
                        errOmitLocation(true),
						errmsg("%s", buf.data)));
	}
	PG_CATCH();
	{
		pfree(buf.data);
		PG_RE_THROW();
	}
	PG_END_TRY();

	/* not reached */
}	/* cdbdisp_finishCommand */

/*
 * CdbCheckDispatchResult:
 *
 * Waits for completion of threads launched by cdbdisp_dispatchToGang().
 *
 * QEs that were dispatched with 'cancelOnError' true and are not yet idle
 * will be canceled/finished according to waitMode.
 */
static void
CdbCheckDispatchResult(struct CdbDispatcherState *ds,
					   DispatchWaitMode waitMode)
{
	PG_TRY();
	{
		CdbCheckDispatchResultInt(ds, NULL, NULL, waitMode);
	}
	PG_CATCH();
	{
		cdbdisp_clearGangActiveFlag(ds);
		PG_RE_THROW();
	}
	PG_END_TRY();

	cdbdisp_clearGangActiveFlag(ds);

	if (log_dispatch_stats)
		ShowUsage("DISPATCH STATISTICS");

	if (DEBUG1 >= log_min_messages)
	{
		char		msec_str[32];

		switch (check_log_duration(msec_str, false))
		{
			case 1:
			case 2:
				ereport(LOG,  (errmsg("duration to dispatch result received from all QEs: %s ms", msec_str)));
				break;
		}
	}
}

static void
CdbCheckDispatchResultInt(struct CdbDispatcherState *ds,
						  struct SegmentDatabaseDescriptor *** failedSegDB,
						  int *numOfFailed,
						  DispatchWaitMode waitMode)
{
	int			i;
	int			j;
	int			nFailed = 0;
	DispatchCommandParms *pParms;
	CdbDispatchResult *dispatchResult;
	SegmentDatabaseDescriptor *segdbDesc;

	Assert(ds != NULL);

	if (failedSegDB)
		*failedSegDB = NULL;
	if (numOfFailed)
		*numOfFailed = 0;

	/* No-op if no work was dispatched since the last time we were called.	*/
	if (!ds->dispatchThreads || ds->dispatchThreads->threadCount == 0)
	{
		elog(DEBUG5, "CheckDispatchResult: no threads active");
		return;
	}

	/*
	 * Wait for threads to finish.
	 */
	for (i = 0; i < ds->dispatchThreads->threadCount; i++)
	{							/* loop over threads */
		pParms = &ds->dispatchThreads->dispatchCommandParmsAr[i];
		Assert(pParms != NULL);

		/* Does caller want to stop short? */
		switch (waitMode)
		{
		case DISPATCH_WAIT_CANCEL:
		case DISPATCH_WAIT_FINISH:
			pParms->waitMode = waitMode;
			break;
		default:
			/* none */
			break;
		}

		if (gp_connections_per_thread==0)
		{
			thread_DispatchWait(pParms);
		}
		else
		{
			elog(DEBUG4, "CheckDispatchResult: Joining to thread %d of %d",
				 i + 1, ds->dispatchThreads->threadCount);

			if (pParms->thread_valid)
			{
				int			pthread_err = 0;
				pthread_err = pthread_join(pParms->thread, NULL);
				if (pthread_err != 0)
					elog(FATAL, "CheckDispatchResult: pthread_join failed on thread %d (%lu) of %d (returned %d attempting to join to %lu)",
						 i + 1,
#ifndef _WIN32
						 (unsigned long) pParms->thread,
#else
						 (unsigned long) pParms->thread.p,
#endif
						 ds->dispatchThreads->threadCount, pthread_err, (unsigned long)mythread());
			}
		}
		HOLD_INTERRUPTS();
		pParms->thread_valid = false;
		MemSet(&pParms->thread, 0, sizeof(pParms->thread));
		RESUME_INTERRUPTS();

		/*
		 * Examine the CdbDispatchResult objects containing the results
		 * from this thread's QEs.
		 */
		for (j = 0; j < pParms->db_count; j++)
		{						/* loop over QEs managed by one thread */
			dispatchResult = pParms->dispatchResultPtrArray[j];

			if (dispatchResult == NULL)
			{
				elog(LOG, "CheckDispatchResult: result object is NULL ? skipping.");
				continue;
			}

			if (dispatchResult->segdbDesc == NULL)
			{
				elog(LOG, "CheckDispatchResult: result object segment descriptor is NULL ? skipping.");
				continue;
			}

			segdbDesc = dispatchResult->segdbDesc;

			/* segdbDesc error message is unlikely here, but check anyway. */
			if (segdbDesc->errcode ||
				segdbDesc->error_message.len)
				cdbdisp_mergeConnectionErrors(dispatchResult, segdbDesc);

			/* Log the result */
			if (DEBUG2 >= log_min_messages)
				cdbdisp_debugDispatchResult(dispatchResult, DEBUG2, DEBUG3);

			/* Notify FTS to reconnect if connection lost or never connected. */
			if (failedSegDB &&
				PQstatus(segdbDesc->conn) == CONNECTION_BAD)
			{
				/* Allocate storage.  Caller should pfree() it. */
				if (!*failedSegDB)
					*failedSegDB = palloc(sizeof(**failedSegDB) *
										  (2 * getgpsegmentCount() + 1));

				/* Append to broken connection list. */
				(*failedSegDB)[nFailed++] = segdbDesc;
				(*failedSegDB)[nFailed] = NULL;

				if (numOfFailed)
					*numOfFailed = nFailed;
			}

			/*
			 * Zap our SegmentDatabaseDescriptor ptr because it may be
			 * invalidated by the call to FtsHandleNetFailure() below.
			 * Anything we need from there, we should get before this.
			 */
			dispatchResult->segdbDesc = NULL;

		}						/* loop over QEs managed by one thread */
	}							/* loop over threads */

	/* reset thread state (will be destroyed later on in finishCommand) */
    ds->dispatchThreads->threadCount = 0;

	/*
	 * It looks like everything went fine, make sure we don't miss a
	 * user cancellation?
	 *
	 * The waitMode argument is NONE when we are doing "normal work".
	 */
	if (waitMode == DISPATCH_WAIT_NONE || waitMode == DISPATCH_WAIT_FINISH)
		CHECK_FOR_INTERRUPTS();
}	/* cdbdisp_checkDispatchResult */

/*
 * Clear our "active" flags; so that we know that the writer gangs are busy -- and don't stomp on
 * internal dispatcher structures. See MPP-6253 and MPP-6579.
 */
static void
cdbdisp_clearGangActiveFlag(CdbDispatcherState *ds)
{
	if (ds && ds->primaryResults && ds->primaryResults->writer_gang)
	{
		ds->primaryResults->writer_gang->dispatcherActive = false;
	}
}


/*
 * cdbdisp_handleError
 *
 * When caller catches an error, the PG_CATCH handler can use this
 * function instead of cdbdisp_finishCommand to wait for all QEs
 * to finish, clean up, and report QE errors if appropriate.
 * This function should be called only from PG_CATCH handlers.
 *
 * This function destroys and frees the given CdbDispatchResults objects.
 * It is a no-op if both CdbDispatchResults ptrs are NULL.
 *
 * On return, the caller is expected to finish its own cleanup and
 * exit via PG_RE_THROW().
 */
static void
cdbdisp_handleError(struct CdbDispatcherState *ds)
{
	int     qderrcode;
	bool    useQeError = false;

	qderrcode = elog_geterrcode();

	/* If cdbdisp_dispatchToGang() wasn't called, don't wait. */
	if (!ds || !ds->primaryResults)
		return;

	/*
	 * Request any remaining commands executing on qExecs to stop.
	 * We need to wait for the threads to finish.  This allows for proper
	 * cleanup of the results from the async command executions.
	 * Cancel any QEs still running.
	 */
	CdbCheckDispatchResult(ds, DISPATCH_WAIT_CANCEL);

    /*
     * When a QE stops executing a command due to an error, as a
     * consequence there can be a cascade of interconnect errors
     * (usually "sender closed connection prematurely") thrown in
     * downstream processes (QEs and QD).  So if we are handling
     * an interconnect error, and a QE hit a more interesting error,
     * we'll let the QE's error report take precedence.
     */
	if (qderrcode == ERRCODE_GP_INTERCONNECTION_ERROR)
	{
		bool qd_lost_flag = false;
		char *qderrtext = elog_message();

		if (qderrtext && strcmp(qderrtext, CDB_MOTION_LOST_CONTACT_STRING) == 0)
			qd_lost_flag = true;

		if (ds->primaryResults && ds->primaryResults->errcode)
		{
			if (qd_lost_flag && ds->primaryResults->errcode == ERRCODE_GP_INTERCONNECTION_ERROR)
				useQeError = true;
			else if (ds->primaryResults->errcode != ERRCODE_GP_INTERCONNECTION_ERROR)
				useQeError = true;
		}
	}

    if (useQeError)
    {
        /*
         * Throw the QE's error, catch it, and fall thru to return
         * normally so caller can finish cleaning up.  Afterwards
         * caller must exit via PG_RE_THROW().
         */
        PG_TRY();
        {
            cdbdisp_finishCommand(ds, NULL, NULL);
        }
        PG_CATCH();
        {}                      /* nop; fall thru */
        PG_END_TRY();
    }
    else
    {
        /*
         * Discard any remaining results from QEs; don't confuse matters by
         * throwing a new error.  Any results of interest presumably should
         * have been examined before raising the error that the caller is
         * currently handling.
         */
        cdbdisp_destroyDispatchResults(ds->primaryResults);
		ds->primaryResults = NULL;
		cdbdisp_destroyDispatchThreads(ds->dispatchThreads);
		ds->dispatchThreads = NULL;
    }
}


static char *
qdSerializeDtxContextInfo(int *size, bool wantSnapshot, bool inCursor, int txnOptions, char *debugCaller)
{
	char *serializedDtxContextInfo;

	Snapshot snapshot;
	int serializedLen;
	DtxContextInfo *pDtxContextInfo = NULL;

	/* If we already have a LatestSnapshot set then no reason to try
	 * and get a new one.  just use that one.  But... there is one important
	 * reason why this HAS to be here.  ROLLBACK stmts get dispatched to QEs
	 * in the abort transaction code.  This code tears down enough stuff such
	 * that you can't call GetTransactionSnapshot() within that code. So we
	 * need to use the LatestSnapshot since we can't re-gen a new one.
	 *
	 * It is also very possible that for a single user statement which may
	 * only generate a single snapshot that we will dispatch multiple statements
	 * to our qExecs.  Something like:
	 *
	 *							QD				QEs
	 *							|  				|
	 * User SQL Statement ----->|	  BEGIN		|
	 *  						|-------------->|
	 *						    | 	  STMT		|
	 *							|-------------->|
	 *						 	|    PREPARE	|
	 *							|-------------->|
	 *							|    COMMIT		|
	 *							|-------------->|
	 *							|				|
	 *
	 * This may seem like a problem because all four of those will dispatch
	 * the same snapshot with the same curcid.  But... this is OK because
	 * BEGIN, PREPARE, and COMMIT don't need Snapshots on the QEs.
	 *
	 * NOTE: This will be a problem if we ever need to dispatch more than one
	 *  	 statement to the qExecs and more than one needs a snapshot!
	 */
	*size = 0;
	snapshot = NULL;

	if (wantSnapshot)
	{

		if (LatestSnapshot == NULL &&
			SerializableSnapshot == NULL &&
			!IsAbortInProgress() )
		{
			/* unfortunately, the dtm issues a select for prepared xacts at the
			 * beginning and this is before a snapshot has been set up.  so we need
			 * one for that but not for when we dont have a valid XID.
			 *
			 * but we CANT do this if an ABORT is in progress... instead we'll send
			 * a NONE since the qExecs dont need the information to do a ROLLBACK.
			 *
			 */
			elog((Debug_print_full_dtm ? LOG : DEBUG5),"qdSerializeDtxContextInfo calling GetTransactionSnapshot to make snapshot");

			GetTransactionSnapshot();
		}

		if (LatestSnapshot != NULL)
		{
			elog((Debug_print_full_dtm ? LOG : DEBUG5),"qdSerializeDtxContextInfo using LatestSnapshot");

			snapshot = LatestSnapshot;
			elog((Debug_print_snapshot_dtm ? LOG : DEBUG5),"[Distributed Snapshot #%u] *QD Use Latest* currcid = %d (gxid = %u, '%s')",
				 LatestSnapshot->distribSnapshotWithLocalMapping.header.distribSnapshotId,
				 LatestSnapshot->curcid,
				 getDistributedTransactionId(),
				 DtxContextToString(DistributedTransactionContext));
		}
		else if (SerializableSnapshot != NULL)
		{
			elog((Debug_print_full_dtm ? LOG : DEBUG5),"qdSerializeDtxContextInfo using SerializableSnapshot");

			snapshot = SerializableSnapshot;
			elog((Debug_print_snapshot_dtm ? LOG : DEBUG5),"[Distributed Snapshot #%u] *QD Use Serializable* currcid = %d (gxid = %u, '%s')",
				 SerializableSnapshot->distribSnapshotWithLocalMapping.header.distribSnapshotId,
				 SerializableSnapshot->curcid,
				 getDistributedTransactionId(),
				 DtxContextToString(DistributedTransactionContext));

		}
	}


	switch (DistributedTransactionContext)
	{
		case DTX_CONTEXT_QD_DISTRIBUTED_CAPABLE:
		case DTX_CONTEXT_LOCAL_ONLY:
			if (snapshot != NULL)
			{
				DtxContextInfo_CreateOnMaster(&TempQDDtxContextInfo,
											  &snapshot->distribSnapshotWithLocalMapping,
											  snapshot->curcid, txnOptions);
			}
			else
			{
				DtxContextInfo_CreateOnMaster(&TempQDDtxContextInfo,
											  NULL, 0, txnOptions);
			}

			TempQDDtxContextInfo.cursorContext = inCursor;

			if (DistributedTransactionContext == DTX_CONTEXT_QD_DISTRIBUTED_CAPABLE &&
				snapshot != NULL)
			{
				updateSharedLocalSnapshot(&TempQDDtxContextInfo, snapshot, "qdSerializeDtxContextInfo");
			}

			pDtxContextInfo = &TempQDDtxContextInfo;
			break;

		case DTX_CONTEXT_QD_RETRY_PHASE_2:
		case DTX_CONTEXT_QE_ENTRY_DB_SINGLETON:
		case DTX_CONTEXT_QE_AUTO_COMMIT_IMPLICIT:
		case DTX_CONTEXT_QE_TWO_PHASE_EXPLICIT_WRITER:
		case DTX_CONTEXT_QE_TWO_PHASE_IMPLICIT_WRITER:
		case DTX_CONTEXT_QE_READER:
		case DTX_CONTEXT_QE_PREPARED:
		case DTX_CONTEXT_QE_FINISH_PREPARED:
			elog(FATAL, "Unexpected distribute transaction context: '%s'",
				 DtxContextToString(DistributedTransactionContext));

		default:
			elog(FATAL, "Unrecognized DTX transaction context: %d",
				 (int)DistributedTransactionContext);
	}

	serializedLen = DtxContextInfo_SerializeSize(pDtxContextInfo);
	Assert (serializedLen > 0);

	*size = serializedLen;
	serializedDtxContextInfo = palloc(*size);

	DtxContextInfo_Serialize(serializedDtxContextInfo, pDtxContextInfo);

	elog((Debug_print_full_dtm ? LOG : DEBUG5),"qdSerializeDtxContextInfo (called by %s) returning a snapshot of %d bytes (ptr is %s)",
	     debugCaller, *size, (serializedDtxContextInfo != NULL ? "Non-NULL" : "NULL"));
	return serializedDtxContextInfo;
}


/* cdbdisp_handleError */

static bool
cdbdisp_check_estate_for_cancel(struct EState *estate)
{
	struct CdbDispatchResults  *meleeResults;

	Assert(estate);
	Assert(estate->dispatcherState);

	meleeResults = estate->dispatcherState->primaryResults;

	if (meleeResults == NULL) /* cleanup ? */
	{
		return false;
	}

	Assert(meleeResults);

	//	if (pleaseCancel || meleeResults->errcode)
	if (meleeResults->errcode)
	{
		return true;
	}

	return false;
}



/*
 * Three Helper functions for CdbDispatchPlan:
 *
 * Used to figure out the dispatch order for the sliceTable by
 * counting the number of dependent child slices for each slice; and
 * then sorting based on the count (all indepenedent slices get
 * dispatched first, then the slice above them and so on).
 *
 * fillSliceVector: figure out the number of slices we're dispatching,
 * and order them.
 *
 * count_dependent_children(): walk tree counting up children.
 *
 * compare_slice_order(): comparison function for qsort(): order the
 * slices by the number of dependent children. Empty slices are
 * sorted last (to make this work with initPlans).
 *
 */
static int
compare_slice_order(const void *aa, const void *bb)
{
	sliceVec *a = (sliceVec *)aa;
	sliceVec *b = (sliceVec *)bb;

	if (a->slice == NULL)
		return 1;
	if (b->slice == NULL)
		return -1;

	/* sort the writer gang slice first, because he sets the shared snapshot */
	if (a->slice->primary_gang_id == 1 && b->slice->primary_gang_id != 1)
		return -1;
	else if (b->slice->primary_gang_id == 1 && a->slice->primary_gang_id != 1)
		return 1;

	if (a->children == b->children)
		return 0;
	else if (a->children > b->children)
		return 1;
	else
		return -1;
}

/* Quick and dirty bit mask operations */
static void
mark_bit(char *bits, int nth)
{
	int nthbyte = nth >> 3;
	char nthbit  = 1 << (nth & 7);
	bits[nthbyte] |= nthbit;
}
static void
or_bits(char* dest, char* src, int n)
{
	int i;

	for(i=0; i<n; i++)
		dest[i] |= src[i];
}

static int
count_bits(char* bits, int nbyte)
{
	int i;
	int nbit = 0;
	int bitcount[] = {
		0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4
	};

	for(i=0; i<nbyte; i++)
	{
		nbit += bitcount[bits[i] & 0x0F];
		nbit += bitcount[(bits[i] >> 4) & 0x0F];
	}

	return nbit;
}

/* We use a bitmask to count the dep. childrens.
 * Because of input sharing, the slices now are DAG.  We cannot simply go down the
 * tree and add up number of children, which will return too big number.
 */
static int markbit_dep_children(SliceTable *sliceTable, int sliceIdx, sliceVec *sliceVec, int bitmasklen, char* bits)
{
	ListCell *sublist;
	Slice *slice = (Slice *) list_nth(sliceTable->slices, sliceIdx);

	foreach(sublist, slice->children)
	{
		int childIndex = lfirst_int(sublist);
		char *newbits = palloc0(bitmasklen);

		markbit_dep_children(sliceTable, childIndex, sliceVec, bitmasklen, newbits);
		or_bits(bits, newbits, bitmasklen);
		mark_bit(bits, childIndex);
		pfree(newbits);
	}

	sliceVec[sliceIdx].sliceIndex = sliceIdx;
	sliceVec[sliceIdx].children = count_bits(bits, bitmasklen);
	sliceVec[sliceIdx].slice = slice;

	return sliceVec[sliceIdx].children;
}

/* Count how many dependent childrens and fill in the sliceVector of dependent childrens. */
static int
count_dependent_children(SliceTable * sliceTable, int sliceIndex, sliceVec *sliceVector, int len)
{
	int 		ret = 0;
	int			bitmasklen = (len+7) >> 3;
	char 	   *bitmask = palloc0(bitmasklen);

	ret = markbit_dep_children(sliceTable, sliceIndex, sliceVector, bitmasklen, bitmask);
	pfree(bitmask);

	return ret;
}

int
fillSliceVector(SliceTable *sliceTbl, int rootIdx, sliceVec *sliceVector, int sliceLim)
{
	int top_count;

	/* count doesn't include top slice add 1 */
	top_count = 1 + count_dependent_children(sliceTbl, rootIdx, sliceVector, sliceLim);

	qsort(sliceVector, sliceLim, sizeof(sliceVec), compare_slice_order);

	return top_count;
}


/*
 * Synchronize threads to finish for this process to die.  Dispatching
 * threads need to acknowledge that we are dying, otherwise the main
 * thread will cleanup memory contexts which could cause process crash
 * while the threads are touching stale pointers.  Threads will check
 * proc_exit_inprogress and immediately stops once it's found to be true.
 *
 * Only called by proc_exit_prepare() for now.
 */
void
cdbdisp_waitThreads(void)
{
	int i, max_retry;
	long interval = 10 * 1000; /* 10 msec */

	/*
	 * Just in case to avoid to be stuck in the final stage of process
	 * lifecycle, insure by setting time limit.  If it exceeds, it probably
	 * means some threads are stuck and not progressing, in which case
	 * we can go ahead and cleanup things anyway.  The duration should be
	 * longer than the select timeout in thread_DispatchWait.
	 */
	max_retry = (DISPATCH_WAIT_TIMEOUT_SEC + 10) * 1000000L / interval;

	/* This is supposed to be called after the flag is set. */
	Assert(proc_exit_inprogress);

	for (i = 0; i < max_retry; i++)
	{
		if (RunningThreadCount == 0)
			break;
		pg_usleep(interval);
	}
}

/* generateTxnOptions:
 * Generates an int containing the appropriate flags to direct the remote
 * segdb QE process to perform any needed transaction commands before or
 * after the statement.
 *
 * needTwoPhase - specifies whether this statement even wants a transaction to
 *				 be started.  Certain utility statements dont want to be in a
 *				 distributed transaction.
 */
static int
generateTxnOptions(bool needTwoPhase)
{
	int options;

	options = mppTxnOptions(needTwoPhase);

	return options;
}
