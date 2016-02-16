/*-------------------------------------------------------------------------
 *
 * cdbdispmgr.h
 *
 * dispatch manager API
 *
 * Copyright (c) 2005-2016, Pivotal inc
 *
 *-------------------------------------------------------------------------
 */

#ifndef CDBDISPMGR_H
#define CDBDISPMGR_H

#include "lib/stringinfo.h"         /* StringInfo */

#include "cdb/cdbtm.h"

PGresult;


/*
 * DispatcherMgrMethods encapsulate how dispatcher works under lower level.
 *
 * 		- What to dispatch?  A bunch of serialized string?
 * 		- What is result?
 * 		- How to process error?
 * 		- Dispatch state?
 *
 * Related files:
 * 		- cdbconn.c: SegmentDatabaseDescriptor
 * 		- cdbdisp.c
 * 		- cdbgang.c
 * 		- cdbdispatchresult.c
 * 		- cdbfts.c
 * 		- cdbtm.c
 */
typedef struct DispatcherMgrMethods
{
	/*
	 * Sends a non-cancelable command to all segment dbs, primary
	 *
	 * Returns a malloc'ed array containing the PGresult objects thus
	 * produced; the caller must PQclear() them and free() the array.
	 * A NULL entry follows the last used entry in the array.
	 *
	 * Any error messages - whether or not they are associated with
	 * PGresult objects - are appended to a StringInfo buffer provided
	 * by the caller.
	 *
	 * returns ptr to array of PGresult ptrs.
	 *
	 * Used only by cdbtm.c#doDispatchDtxProtocolCommand() function.
	 */
	PGresult ** (*dispatchDtxProtocolCommand) (DtxProtocolCommand		 dtxProtocolCommand,
									   	   	   int						 flags,
											   char						*dtxProtocolCommandLoggingStr,
											   char						*gid,
											   DistributedTransactionId	 gxid,
											   StringInfo    			 errmsgbuf,
											   int						*numresults,
											   bool 					*badGangs,
											   CdbDispatchDirectDesc    *direct,
											   char						*argument,
											   int						 argumentLength );


	/*
	 * This code was refactored out of cdbdisp_dispatchPlan.  It's
	 * used both for dispatching plans when we are using normal gangs,
	 * and for dispatching all statements from Query Dispatch Agents
	 * when we are using dispatch agents.
	 */
	void (*cdbdisp_dispatchX) (DispatchCommandQueryParms *pQueryParms,
					  bool cancelOnError,
					  struct SliceTable *sliceTbl,
					  struct CdbDispatcherState *ds);

	/*
	 * Send the strCommand SQL statement to all segdbs in the cluster
	 * cancelOnError indicates whether an error
	 * occurring on one of the qExec segdbs should cause all still-executing commands to cancel
	 * on other qExecs. Normally this would be true.  The commands are sent over the libpq
	 * connections that were established during gang creation.
	 *
	 * The needTwoPhase flag is used to express intent on whether the command to
	 * be dispatched should be done inside of a global transaction or not.
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
	 * suggested that the caller use finishCommand().
	 */
	void (*dispatchCommand) (const char        *strCommand,
							 char			   *serializedQuerytree,
							 int				serializedQuerytreelen,
							 bool               cancelOnError,
							 bool				needTwoPhase,
							 bool				withSnapshot,
							 struct CdbDispatcherState *ds); /* OUT */

	/*
	 * Dispatch SET command to all gangs.
	 *
	 * Can not dispatch SET commands to busy reader gangs (allocated by cursors) directly because another command is already in progress.
	 * Cursors only allocate reader gangs, so primary writer and idle reader gangs can be dispatched to.
	 */
	void (*dispatchSetCommandToAllGangs) (const char	*strCommand,
									  char			*serializedQuerytree,
									  int			serializedQuerytreelen,
									  char			*serializedPlantree,
									  int			serializedPlantreelen,
									  bool			cancelOnError,
									  bool			needTwoPhase,
									  struct CdbDispatcherState *ds);

	/* Wait for all QEs to finish, then report any errors from the given
	 * CdbDispatchResults objects and free them.  If not all QEs in the
	 * associated gang(s) executed the command successfully, throws an
	 * error and does not return.  No-op if both CdbDispatchResults ptrs are NULL.
	 *
	 * This is a convenience function; callers with unusual requirements may
	 * instead call CdbCheckDispatchResult(), etc., directly.
	 */
	void (*finishCommand) (struct CdbDispatcherState *ds,
						   void (*handle_results_callback)(struct CdbDispatchResults *primaryResults, void *ctx),
						   void *ctx);

	/*
	 * CdbCheckDispatchResult:
	 *
	 * Waits for completion of tasks launched by cdbdisp_dispatchToGang().
	 *
	 * QEs that were dispatched with 'cancelOnError' true and are not yet idle
	 * will be canceled/finished according to waitMode.
	 */
	void (*CdbCheckDispatchResult) (struct CdbDispatcherState *ds, DispatchWaitMode waitMode);

	void (*cdbdisp_destroyDispatchResults) (CdbDispatchResults * results);

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
	void (*cdbdisp_handleError) (struct CdbDispatcherState *ds);

	/* used to take the current Transaction Snapshot and serialized a version of it
	 * into the static variable serializedDtxContextInfo */
	char * (*qdSerializeDtxContextInfo) (int * size, bool wantSnapshot, bool inCursor, int txnOptions, char *debugCaller);

	/* used in the interconnect on the dispatcher to avoid error-cleanup deadlocks. */
	bool (*cdbdisp_check_estate_for_cancel) (struct EState *estate);

	bool (*cleanup)(CdbDispatcherState *ds);
} DispatcherMgrMethods;

DispatcherMgrMethods GetDispatcherMgr();

#endif   /* CDBDISPMGR_H */
