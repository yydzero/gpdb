/*-------------------------------------------------------------------------
 *
 * cdbdisp.h
 * routines for dispatching commands from the dispatcher process
 * to the qExec processes.
 *
 * Copyright (c) 2005-2008, Greenplum inc
 *
 *-------------------------------------------------------------------------
 */
#ifndef CDBDISP_H
#define CDBDISP_H

#include "lib/stringinfo.h"         /* StringInfo */

#include "cdb/cdbtm.h"
#include <pthread.h>

struct pg_result;					/* #include "gp-libpq-int.h" */
struct CdbDispatchResult;           /* #include "cdb/cdbdispatchresult.h" */
struct CdbDispatchResults;          /* #include "cdb/cdbdispatchresult.h" */
struct Gang;                        /* #include "cdb/cdbgang.h" */
struct Node;                        /* #include "nodes/nodes.h" */
struct QueryDesc;                   /* #include "executor/execdesc.h" */
struct SegmentDatabaseDescriptor;   /* #include "cdb/cdbconn.h" */
struct pollfd;

#define CDB_MOTION_LOST_CONTACT_STRING "Interconnect error master lost contact with segment."
#define DISPATCH_WAIT_TIMEOUT_SEC 2


typedef enum
{
	GP_DISPATCH_COMMAND_TYPE_QUERY,
	GP_DISPATCH_COMMAND_TYPE_DTX_PROTOCOL
} GpDispatchCommandType;

/*
 * Parameter structure for Greenplum Database Queries
 */
typedef struct DispatchCommandQueryParms
{
	/*
	 * The SQL command
	 */
	const char	*strCommand;
	int			strCommandlen;
	char		*serializedQuerytree;
	int			serializedQuerytreelen;
	char		*serializedPlantree;
	int			serializedPlantreelen;
	char		*serializedParams;
	int			serializedParamslen;
	char		*serializedSliceInfo;
	int			serializedSliceInfolen;
	
	/*
	 * serialized DTX context string
	 */
	char		*serializedDtxContextInfo;
	int			serializedDtxContextInfolen;
	
	int			rootIdx;

	/*
	 * the sequence server info.
	 */
	char *seqServerHost;		/* If non-null, sequence server host name. */
	int seqServerHostlen;
	int seqServerPort;			/* If seqServerHost non-null, sequence server port. */
	
	/*
	 * Used by dispatch agent if NOT using sliced execution
	 */
	int			primary_gang_id;

} DispatchCommandQueryParms;

/*
 * Parameter structure for DTX protocol commands
 */
typedef struct DispatchCommandDtxProtocolParms
{
	DtxProtocolCommand			dtxProtocolCommand;
	int							flags;
	char*						dtxProtocolCommandLoggingStr;
	char						gid[TMGIDSIZE];
	DistributedTransactionId	gxid;
	int							primary_gang_id;
	char *argument;
	int argumentLength;
} DispatchCommandDtxProtocolParms;



typedef struct CdbDispatchDirectDesc
{
	bool directed_dispatch;
	uint16 count;
	uint16 content[1];
} CdbDispatchDirectDesc;

extern CdbDispatchDirectDesc default_dispatch_direct_desc;
#define DEFAULT_DISP_DIRECT (&default_dispatch_direct_desc)

typedef struct CdbDispatcherState
{
	struct CdbDispatchResults    *primaryResults;
	struct CdbDispatchCmdThreads *dispatchThreads;
} CdbDispatcherState;

/*--------------------------------------------------------------------*/

/*
 * cdbdisp_dispatchRMCommand:
 * Sends a non-cancelable command to all segment dbs, primary
 *
 * Returns a malloc'ed array containing the struct pg_result objects thus
 * produced; the caller must PQclear() them and free() the array.
 * A NULL entry follows the last used entry in the array.
 *
 * Any error messages - whether or not they are associated with
 * struct pg_result objects - are appended to a StringInfo buffer provided
 * by the caller.
 */
struct pg_result **             /* returns ptr to array of struct pg_result ptrs */
cdbdisp_dispatchRMCommand(const char   *strCommand,
						  bool			withSnapshot,
                          StringInfo    errmsgbuf,
                          int			*numresults);



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
void
CdbDoCommand(const char *strCommand, bool cancelOnError, bool needTwoPhase);

/*
 * Special for sending SET commands that change GUC variables, so they go to all
 * gangs, both reader and writer
 */
void
CdbSetGucOnAllGangs(const char *strCommand, bool cancelOnError, bool needTwoPhase);


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
void
cdbdisp_dispatchPlan(struct QueryDesc              *queryDesc,
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
void
cdbdisp_dispatchUtilityStatement(struct Node   *stmt,
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
void
CdbDispatchUtilityStatement(struct Node *stmt, char* debugCaller __attribute__((unused)) );

void
CdbDispatchUtilityStatement_NoTwoPhase(struct Node *stmt, char* debugCaller __attribute__((unused)) );

struct EState;
struct PlannedStmt;
struct PlannerInfo;

/* 
 * make a plan constant, if possible. Call must say if we're doing single row
 * inserts.
 */
Node *planner_make_plan_constant(struct PlannerInfo *root, Node *n, bool is_SRI);

/*--------------------------------------------------------------------*/

#endif   /* CDBDISP_H */
