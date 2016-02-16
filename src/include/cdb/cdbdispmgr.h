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


typedef struct DispatcherMgrMethods
{
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
	void (*cdbdisp_dispatchToGang) (struct CdbDispatcherState *ds,
						   GpDispatchCommandType		mppDispatchCommandType,
						   void							*commandTypeParms,
	                       struct Gang					*gp,
	                       int							sliceIndex,
	                       unsigned int					maxSlices,
	                       CdbDispatchDirectDesc		*direct);

	/*
	 * CdbCheckDispatchResult:
	 *
	 * Waits for completion of threads launched by cdbdisp_dispatchToGang().
	 *
	 * QEs that were dispatched with 'cancelOnError' true and are not yet idle
	 * will be canceled/finished according to waitMode.
	 */
	void (*CdbCheckDispatchResult) (struct CdbDispatcherState *ds, DispatchWaitMode waitMode);

	struct pg_result ** (*cdbdisp_returnResults) (struct CdbDispatchResults *primaryResults,
												  StringInfo errmsgbuf,
												  int *numresults);

	/*
	 * cdbdisp_dispatchRMCommand:
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
	 * returns ptr to array of PGresult ptrs
	 */
	struct pg_result ** (*cdbdisp_dispatchRMCommand) (const char   *strCommand,
							  bool			withSnapshot,
	                          StringInfo    errmsgbuf,
	                          int			*numresults);

	/*
	 * cdbdisp_dispatchDtxProtocolCommand:
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
	 * returns ptr to array of PGresult ptrs
	 */
	struct pg_result ** (*cdbdisp_dispatchDtxProtocolCommand) (DtxProtocolCommand		dtxProtocolCommand,
									   int						flags,
									   char						*dtxProtocolCommandLoggingStr,
									   char						*gid,
									   DistributedTransactionId	gxid,
									   StringInfo    			errmsgbuf,
									   int						*numresults,
									   bool 					*badGangs,
									   CdbDispatchDirectDesc *direct,
									   char *argument, int argumentLength );

	/*
	 * cdbdisp_dispatchCommand:
	 * Send ths strCommand SQL statement to all segdbs in the cluster
	 * cancelOnError indicates whether an error
	 * occurring on one of the qExec segdbs should cause all still-executing commands to cancel
	 * on other qExecs. Normally this would be true.  The commands are sent over the libpq
	 * connections that were established during gang creation.	They are run inside of threads.
	 * The number of segdbs handled by any one thread is determined by the
	 * guc variable gp_connections_per_thread.
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
	 * suggested that the caller use cdbdisp_finishCommand().
	 */
	void (*cdbdisp_dispatchCommand) (const char                 *strCommand,
							char				   	   *serializedQuerytree,
							int							serializedQuerytreelen,
	                        bool                        cancelOnError,
	                        bool						needTwoPhase,
	                        bool						withSnapshot,
							struct CdbDispatcherState *ds); /* OUT */

	/* Wait for all QEs to finish, then report any errors from the given
	 * CdbDispatchResults objects and free them.  If not all QEs in the
	 * associated gang(s) executed the command successfully, throws an
	 * error and does not return.  No-op if both CdbDispatchResults ptrs are NULL.
	 * This is a convenience function; callers with unusual requirements may
	 * instead call CdbCheckDispatchResult(), etc., directly.
	 */
	void (*cdbdisp_finishCommand) (struct CdbDispatcherState *ds,
						  void (*handle_results_callback)(struct CdbDispatchResults *primaryResults, void *ctx),
						  void *ctx);

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

	/*
	 * CdbDoCommand:
	 * Combination of cdbdisp_dispatchCommand and cdbdisp_finishCommand.
	 * Called by general users, this method includes global transaction control.
	 * If not all QEs execute the command successfully, throws an error and
	 * does not return.
	 *
	 * needTwoPhase specifies whether to dispatch within a distributed
	 * transaction or not.
	 */
	void (*CdbDoCommand) (const char *strCommand, bool cancelOnError, bool needTwoPhase);

	/*
	 * Special for sending SET commands that change GUC variables, so they go to all
	 * gangs, both reader and writer
	 */
	void (*CdbSetGucOnAllGangs) (const char *strCommand, bool cancelOnError, bool needTwoPhase);

	void (*cdbdisp_dispatchX) (DispatchCommandQueryParms *pQueryParms,
					  bool cancelOnError,
					  struct SliceTable *sliceTbl,
					  struct CdbDispatcherState *ds); /* OUT: fields filled in */

	/* Compose and dispatch the MPPEXEC commands corresponding to a plan tree
	 * within a complete parallel plan.
	 *
	 * The CdbDispatchResults objects allocated for the plan are
	 * returned in *pPrimaryResults
	 * The caller, after calling CdbCheckDispatchResult(), can
	 * examine the CdbDispatchResults objects, can keep them as
	 * long as needed, and ultimately must free them with
	 * cdbdisp_destroyDispatchResults() prior to deallocation
	 * of the caller's memory context.
	 *
	 * NB: Callers should use PG_TRY()/PG_CATCH() if needed to make
	 * certain that the CdbDispatchResults objects are destroyed by
	 * cdbdisp_destroyDispatchResults() in case of error.
	 * To wait for completion, check for errors, and clean up, it is
	 * suggested that the caller use cdbdisp_finishCommand().
	 */
	void (*cdbdisp_dispatchPlan) (struct QueryDesc              *queryDesc,
	                     bool                           planRequiresTxn,
	                     bool                           cancelOnError,
						 struct CdbDispatcherState *ds); /* OUT: fields filled in */

	/* Dispatch a command - already parsed and in the form of a Node
	 * tree - to all primary segdbs.  Does not wait for
	 * completion.  Does not start a global transaction.
	 *
	 *
	 * The needTwoPhase flag indicates whether you want the dispatched
	 * statement to participate in a distributed transaction or not.
	 *
	 * NB: Callers should use PG_TRY()/PG_CATCH() if needed to make
	 * certain that the CdbDispatchResults objects are destroyed by
	 * cdbdisp_destroyDispatchResults() in case of error.
	 * To wait for completion, check for errors, and clean up, it is
	 * suggested that the caller use cdbdisp_finishCommand().
	 */
	void (*cdbdisp_dispatchUtilityStatement) (struct Node   *stmt,
	                                 bool           cancelOnError,
	                                 bool			needTwoPhase,
	                                 bool			withSnapshot,
									 struct CdbDispatcherState *ds,
									 char* debugCaller __attribute__((unused)) );

	/* Dispatch a command - already parsed and in the form of a Node
	 * tree - to all primary segdbs, and wait for completion.
	 * Starts a global transaction first, if not already started.
	 * If not all QEs in the given gang(s) executed the command successfully,
	 * throws an error and does not return.
	 */
	void (*CdbDispatchUtilityStatement) (struct Node *stmt, char* debugCaller __attribute__((unused)) );

	void (*CdbDispatchUtilityStatement_NoTwoPhase) (struct Node *stmt, char* debugCaller __attribute__((unused)) );

	/*
	 * create a CdbDispatchCmdThreads object that holds the dispatch
	 * threads state, and an array of dispatch command params.
	 */
	CdbDispatchCmdThreads * (*cdbdisp_makeDispatchThreads) (int paramCount);

	/*
	 * free all memory allocated for a CdbDispatchCmdThreads object.
	 */
	void (*cdbdisp_destroyDispatchThreads) (CdbDispatchCmdThreads *dThreads);

	/* used to take the current Transaction Snapshot and serialized a version of it
	 * into the static variable serializedDtxContextInfo */
	char * (*qdSerializeDtxContextInfo) (int * size, bool wantSnapshot, bool inCursor, int txnOptions, char *debugCaller);

	/* used in the interconnect on the dispatcher to avoid error-cleanup deadlocks. */
	bool (*cdbdisp_check_estate_for_cancel) (struct EState *estate);
};


#endif   /* CDBDISPMGR_H */
