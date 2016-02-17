/*-------------------------------------------------------------------------
 *
 * cdbdisp.c
 *	  Functions to dispatch commands to QExecutors.
 *
 *
 * Copyright (c) 2005-2008, Greenplum inc
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

#define GP_PARTITION_SELECTION_OID 6084
#define GP_PARTITION_EXPANSION_OID 6085
#define GP_PARTITION_INVERSE_OID 6086

/*
 * default directed-dispatch parameters: don't direct anything.
 */
CdbDispatchDirectDesc default_dispatch_direct_desc = {false, 0, {0}};


typedef struct
{
	plan_tree_base_prefix base; /* Required prefix for plan_tree_walker/mutator */
	bool		single_row_insert;
}	pre_dispatch_function_evaluation_context;

/*
 * Static Helper functions
 */
static void bindCurrentOfParams(char *cursor_name,
						Oid target_relid,
						ItemPointer ctid,
						int *gp_segment_id,
						Oid *tableoid);
static Node *pre_dispatch_function_evaluation_mutator(Node *node,
						pre_dispatch_function_evaluation_context * context);
static void CdbDispatchUtilityStatement_Internal(struct Node *stmt, bool needTwoPhase,
						char* debugCaller);

/*--------------------------------------------------------------------*/


/*
 * cdbdisp_dispatchRMCommand:
 * Sends a non-cancelable command to all segment dbs.
 *
 * Returns a malloc'ed array containing the PGresult objects thus
 * produced; the caller must PQclear() them and free() the array.
 * A NULL entry follows the last used entry in the array.
 *
 * Any error messages - whether or not they are associated with
 * PGresult objects - are appended to a StringInfo buffer provided
 * by the caller.
 */
PGresult **				/* returns ptr to array of PGresult ptrs */
cdbdisp_dispatchRMCommand(const char *strCommand,
						  bool withSnapshot,
						  StringInfo errmsgbuf,
						  int *numresults)
{
	volatile struct CdbDispatcherState ds = {NULL, NULL};
	
	PGresult  **resultSets = NULL;

	/* never want to start a global transaction for these */
	bool		needTwoPhase = false;

	elog(((Debug_print_full_dtm || Debug_print_snapshot_dtm) ? LOG : DEBUG5),
		 "cdbdisp_dispatchRMCommand for command = '%s', withSnapshot = %s",
	     strCommand, (withSnapshot ? "true" : "false"));

	PG_TRY();
	{
		/* Launch the command.	Don't cancel on error. */
		GetDispatcherMgr().dispatchCommand(strCommand, NULL, 0,
								/* cancelOnError */false,
								needTwoPhase, withSnapshot,
								(struct CdbDispatcherState *)&ds);

		/* Wait for all QEs to finish.	Don't cancel. */
		GetDispatcherMgr().CdbCheckDispatchResult((struct CdbDispatcherState *)&ds,
							   DISPATCH_WAIT_NONE);
	}
	PG_CATCH();
	{
		/* Something happend, clean up after ourselves */
		GetDispatcherMgr().CdbCheckDispatchResult((struct CdbDispatcherState *)&ds,
							   DISPATCH_WAIT_NONE);

		GetDispatcherMgr().cdbdisp_destroyDispatchResults(ds.primaryResults);
		cdbdisp_destroyDispatchThreads(ds.dispatchThreads);

		PG_RE_THROW();
		/* not reached */
	}
	PG_END_TRY();

	resultSets = cdbdisp_returnResults(ds.primaryResults, errmsgbuf, numresults);
	
	/* free memory allocated for the dispatch threads struct */
	cdbdisp_destroyDispatchThreads(ds.dispatchThreads);

	return resultSets;
}	/* cdbdisp_dispatchRMCommand */




/*--------------------------------------------------------------------*/


/*
 * CdbDoCommand:
 * Combination of cdbdisp_dispatchCommand and cdbdisp_finishCommand.
 * If not all QEs execute the command successfully, throws an error and
 * does not return.
 *
 * needTwoPhase specifies a desire to include global transaction control
 *  before dispatch.
 */
void
CdbDoCommand(const char *strCommand,
			 bool cancelOnError,
			 bool needTwoPhase)
{
	CdbDispatcherState ds = {NULL, NULL};
	const bool withSnapshot = true;

	elog((Debug_print_full_dtm ? LOG : DEBUG5), "CdbDoCommand for command = '%s', needTwoPhase = %s",
		 strCommand, (needTwoPhase ? "true" : "false"));

	dtmPreCommand("CdbDoCommand", strCommand, NULL, needTwoPhase, withSnapshot, false /* inCursor */);

	GetDispatcherMgr().dispatchCommand(strCommand, NULL, 0, cancelOnError, needTwoPhase,
							/* withSnapshot */true, &ds);

	/*
	 * Wait for all QEs to finish. If not all of our QEs were successful,
	 * report the error and throw up.
	 */
	GetDispatcherMgr().finishCommand(&ds, NULL, NULL);
}	/* CdbDoCommand */


void
CdbSetGucOnAllGangs(const char *strCommand,
					   bool cancelOnError,
					   bool needTwoPhase)
{
	volatile CdbDispatcherState ds = {NULL, NULL};
	const bool withSnapshot = true;

	elog((Debug_print_full_dtm ? LOG : DEBUG5), "CdbSetGucOnAllGangs for command = '%s', needTwoPhase = %s",
		 strCommand, (needTwoPhase ? "true" : "false"));

	dtmPreCommand("CdbSetGucOnAllGangs", strCommand, NULL, needTwoPhase, withSnapshot, false /* inCursor */ );

	PG_TRY();
	{
		GetDispatcherMgr().dispatchSetCommandToAllGangs(strCommand, NULL, 0, NULL, 0, cancelOnError, needTwoPhase, (struct CdbDispatcherState *)&ds);

		/*
		 * Wait for all QEs to finish. If not all of our QEs were successful,
		 * report the error and throw up.
		 */
		GetDispatcherMgr().finishCommand((struct CdbDispatcherState *)&ds, NULL, NULL);
	}
	PG_CATCH();
	{
		/* Something happend, clean up after ourselves */
		GetDispatcherMgr().CdbCheckDispatchResult((struct CdbDispatcherState *)&ds,
							   DISPATCH_WAIT_CANCEL);

		cdbdisp_destroyDispatchResults(ds.primaryResults);
		cdbdisp_destroyDispatchThreads(ds.dispatchThreads);

		PG_RE_THROW();
		/* not reached */
	}
	PG_END_TRY();
}	

/*--------------------------------------------------------------------*/



/*
 * Let's evaluate all STABLE functions that have constant args before dispatch, so we get a consistent
 * view across QEs
 *
 * Also, if this is a single_row insert, let's evaluate nextval() and currval() before dispatching
 *
 */

static Node *
pre_dispatch_function_evaluation_mutator(Node *node,
										 pre_dispatch_function_evaluation_context * context)
{
	Node * new_node = 0;
	
	if (node == NULL)
		return NULL;

	if (IsA(node, Param))
	{
		Param	   *param = (Param *) node;

		/* Not replaceable, so just copy the Param (no need to recurse) */
		return (Node *) copyObject(param);
	}
	else if (IsA(node, FuncExpr))
	{
		FuncExpr   *expr = (FuncExpr *) node;
		List	   *args;
		ListCell   *arg;
		Expr	   *simple;
		FuncExpr   *newexpr;
		bool		has_nonconst_input;

		Form_pg_proc funcform;
		EState	   *estate;
		ExprState  *exprstate;
		MemoryContext oldcontext;
		Datum		const_val;
		bool		const_is_null;
		int16		resultTypLen;
		bool		resultTypByVal;

		Oid			funcid;
		HeapTuple	func_tuple;


		/*
		 * Reduce constants in the FuncExpr's arguments.  We know args is
		 * either NIL or a List node, so we can call expression_tree_mutator
		 * directly rather than recursing to self.
		 */
		args = (List *) expression_tree_mutator((Node *) expr->args,
												pre_dispatch_function_evaluation_mutator,
												(void *) context);
										
		funcid = expr->funcid;

		newexpr = makeNode(FuncExpr);
		newexpr->funcid = expr->funcid;
		newexpr->funcresulttype = expr->funcresulttype;
		newexpr->funcretset = expr->funcretset;
		newexpr->funcformat = expr->funcformat;
		newexpr->args = args;

		/*
		 * Check for constant inputs
		 */
		has_nonconst_input = false;
		
		foreach(arg, args)
		{
			if (!IsA(lfirst(arg), Const))
			{
				has_nonconst_input = true;
				break;
			}
		}
		
		if (!has_nonconst_input)
		{
			bool is_seq_func = false;
			bool tup_or_set;
			cqContext	*pcqCtx;

			pcqCtx = caql_beginscan(
					NULL,
					cql("SELECT * FROM pg_proc "
						" WHERE oid = :1 ",
						ObjectIdGetDatum(funcid)));

			func_tuple = caql_getnext(pcqCtx);

			if (!HeapTupleIsValid(func_tuple))
				elog(ERROR, "cache lookup failed for function %u", funcid);

			funcform = (Form_pg_proc) GETSTRUCT(func_tuple);

			/* can't handle set returning or row returning functions */
			tup_or_set = (funcform->proretset || 
						  type_is_rowtype(funcform->prorettype));

			caql_endscan(pcqCtx);
			
			/* can't handle it */
			if (tup_or_set)
			{
				/* 
				 * We haven't mutated this node, but we still return the
				 * mutated arguments.
				 *
				 * If we don't do this, we'll miss out on transforming function
				 * arguments which are themselves functions we need to mutated.
				 * For example, select foo(now()).
				 *
				 * See MPP-3022 for what happened when we didn't do this.
				 */
				return (Node *)newexpr;
			}

			/* 
			 * Ignored evaluation of gp_partition stable functions.
			 * TODO: garcic12 - May 30, 2013, refactor gp_partition stable functions to be truly
			 * stable (JIRA: MPP-19541).
			 */
			if (funcid == GP_PARTITION_SELECTION_OID 
				|| funcid == GP_PARTITION_EXPANSION_OID 
				|| funcid == GP_PARTITION_INVERSE_OID)
			{
				return (Node *)newexpr;
			}

			/* 
			 * Related to MPP-1429.  Here we want to mark any statement that is
			 * going to use a sequence as dirty.  Doing this means that the
			 * QD will flush the xlog which will also flush any xlog writes that
			 * the sequence server might do. 
			 */
			if (funcid == NEXTVAL_FUNC_OID || funcid == CURRVAL_FUNC_OID ||
				funcid == SETVAL_FUNC_OID)
			{
				ExecutorMarkTransactionUsesSequences();
				is_seq_func = true;
			}

			if (funcform->provolatile == PROVOLATILE_IMMUTABLE)
				/* okay */ ;
			else if (funcform->provolatile == PROVOLATILE_STABLE)
				/* okay */ ;
			else if (context->single_row_insert && is_seq_func)
				;				/* Volatile, but special sequence function */
			else
				return (Node *)newexpr;

			/*
			 * Ok, we have a function that is STABLE (or IMMUTABLE), with
			 * constant args. Let's try to evaluate it.
			 */

			/*
			 * To use the executor, we need an EState.
			 */
			estate = CreateExecutorState();

			/* We can use the estate's working context to avoid memory leaks. */
			oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);

			/*
			 * Prepare expr for execution.
			 */
			exprstate = ExecPrepareExpr((Expr *) newexpr, estate);

			/*
			 * And evaluate it.
			 *
			 * It is OK to use a default econtext because none of the
			 * ExecEvalExpr() code used in this situation will use econtext.
			 * That might seem fortuitous, but it's not so unreasonable --- a
			 * constant expression does not depend on context, by definition,
			 * n'est-ce pas?
			 */
			const_val =
				ExecEvalExprSwitchContext(exprstate,
										  GetPerTupleExprContext(estate),
										  &const_is_null, NULL);

			/* Get info needed about result datatype */
			get_typlenbyval(expr->funcresulttype, &resultTypLen, &resultTypByVal);

			/* Get back to outer memory context */
			MemoryContextSwitchTo(oldcontext);

			/* Must copy result out of sub-context used by expression eval */
			if (!const_is_null)
				const_val = datumCopy(const_val, resultTypByVal, resultTypLen);

			/* Release all the junk we just created */
			FreeExecutorState(estate);

			/*
			 * Make the constant result node.
			 */
			simple = (Expr *) makeConst(expr->funcresulttype, -1, resultTypLen,
										const_val, const_is_null,
										resultTypByVal);

			if (simple)			/* successfully simplified it */
				return (Node *) simple;
		}

		/*
		 * The expression cannot be simplified any further, so build and
		 * return a replacement FuncExpr node using the possibly-simplified
		 * arguments.
		 */
		return (Node *) newexpr;
	}
	else if (IsA(node, OpExpr))
	{
		OpExpr	   *expr = (OpExpr *) node;
		List	   *args;

		OpExpr	   *newexpr;

		/*
		 * Reduce constants in the OpExpr's arguments.  We know args is either
		 * NIL or a List node, so we can call expression_tree_mutator directly
		 * rather than recursing to self.
		 */
		args = (List *) expression_tree_mutator((Node *) expr->args,
												pre_dispatch_function_evaluation_mutator,
												(void *) context);

		/*
		 * Need to get OID of underlying function.	Okay to scribble on input
		 * to this extent.
		 */
		set_opfuncid(expr);

		newexpr = makeNode(OpExpr);
		newexpr->opno = expr->opno;
		newexpr->opfuncid = expr->opfuncid;
		newexpr->opresulttype = expr->opresulttype;
		newexpr->opretset = expr->opretset;
		newexpr->args = args;

		return (Node *) newexpr;
	}
	else if (IsA(node, CurrentOfExpr))
	{
		/*
		 * updatable cursors 
		 *
		 * During constant folding, the CurrentOfExpr's gp_segment_id, ctid, 
		 * and tableoid fields are filled in with observed values from the 
		 * referenced cursor. For more detail, see bindCurrentOfParams below.
		 */
		CurrentOfExpr *expr = (CurrentOfExpr *) node,
					  *newexpr = copyObject(expr);

		bindCurrentOfParams(newexpr->cursor_name,
							newexpr->target_relid,
			   		   		&newexpr->ctid,
					  	   	&newexpr->gp_segment_id,
					  	   	&newexpr->tableoid);
		return (Node *) newexpr;
	}
	
	/*
	 * For any node type not handled above, we recurse using
	 * plan_tree_mutator, which will copy the node unchanged but try to
	 * simplify its arguments (if any) using this routine.
	 */
	new_node =  plan_tree_mutator(node, pre_dispatch_function_evaluation_mutator,
								  (void *) context);

	return new_node;
}

/*
 * bindCurrentOfParams
 *
 * During constant folding, we evaluate STABLE functions to give QEs a consistent view
 * of the query. At this stage, we will also bind observed values of 
 * gp_segment_id/ctid/tableoid into the CurrentOfExpr.
 * This binding must happen only after planning, otherwise we disrupt prepared statements.
 * Furthermore, this binding must occur before dispatch, because a QE lacks the 
 * the information needed to discern whether it's responsible for the currently 
 * positioned tuple.
 *
 * The design of this parameter binding is very tightly bound to the parse/analyze
 * and subsequent planning of DECLARE CURSOR. We depend on the "is_simply_updatable"
 * calculation of parse/analyze to decide whether CURRENT OF makes sense for the
 * referenced cursor. Moreover, we depend on the ensuing planning of DECLARE CURSOR
 * to provide the junk metadata of gp_segment_id/ctid/tableoid (per tuple).
 *
 * This function will lookup the portal given by "cursor_name". If it's simply updatable,
 * we'll glean gp_segment_id/ctid/tableoid from the portal's most recently fetched 
 * (raw) tuple. We bind this information into the CurrentOfExpr to precisely identify
 * the currently scanned tuple, ultimately for consumption of TidScan/execQual by the QEs.
 */
static void
bindCurrentOfParams(char *cursor_name, Oid target_relid, ItemPointer ctid, int *gp_segment_id, Oid *tableoid)
{
	char 			*table_name;
	Portal			portal;
	QueryDesc		*queryDesc;
	AttrNumber		gp_segment_id_attno;
	AttrNumber		ctid_attno;
	AttrNumber		tableoid_attno;
	bool			isnull;
	Datum			value;

	portal = GetPortalByName(cursor_name);
	if (!PortalIsValid(portal))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_CURSOR_STATE),
				 errmsg("cursor \"%s\" does not exist", cursor_name)));

	queryDesc = PortalGetQueryDesc(portal);
	if (queryDesc == NULL)
		ereport(ERROR, 
				(errcode(ERRCODE_INVALID_CURSOR_STATE),
				 errmsg("cursor \"%s\" is held from a previous transaction", cursor_name)));

	/* obtain table_name for potential error messages */
	table_name = get_rel_name(target_relid);

	/* 
	 * The referenced cursor must be simply updatable. This has already
	 * been discerned by parse/analyze for the DECLARE CURSOR of the given
	 * cursor. This flag assures us that gp_segment_id, ctid, and tableoid (if necessary)
 	 * will be available as junk metadata, courtesy of preprocess_targetlist.
	 */
	if (!portal->is_simply_updatable)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_CURSOR_STATE),
				 errmsg("cursor \"%s\" is not a simply updatable scan of table \"%s\"",
						cursor_name, table_name)));

	/* 
	 * The target relation must directly match the cursor's relation. This throws out
	 * the simple case in which a cursor is declared against table X and the update is
	 * issued against Y. Moreover, this disallows some subtler inheritance cases where
	 * Y inherits from X. While such cases could be implemented, it seems wiser to
	 * simply error out cleanly.
	 */
	Index varno = extractSimplyUpdatableRTEIndex(queryDesc->plannedstmt->rtable);
	Oid cursor_relid = getrelid(varno, queryDesc->plannedstmt->rtable);
	if (target_relid != cursor_relid)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_CURSOR_STATE),
				 errmsg("cursor \"%s\" is not a simply updatable scan of table \"%s\"",
						cursor_name, table_name)));
	/* 
	 * The cursor must have a current result row: per the SQL spec, it's 
	 * an error if not.
	 */
	if (portal->atStart || portal->atEnd)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_CURSOR_STATE),
				 errmsg("cursor \"%s\" is not positioned on a row", cursor_name)));

	/*
	 * As mentioned above, if parse/analyze recognized this cursor as simply
	 * updatable during DECLARE CURSOR, then its subsequent planning must have
	 * made gp_segment_id, ctid, and tableoid available as junk for each tuple.
	 *
	 * To retrieve this junk metadeta, we leverage the EState's junkfilter against
	 * the raw tuple yielded by the highest most node in the plan.
	 */
	TupleTableSlot *slot = queryDesc->planstate->ps_ResultTupleSlot;
	Insist(!TupIsNull(slot));
	Assert(queryDesc->estate->es_junkFilter);

	/* extract gp_segment_id metadata */
	gp_segment_id_attno = ExecFindJunkAttribute(queryDesc->estate->es_junkFilter, "gp_segment_id");
	if (!AttributeNumberIsValid(gp_segment_id_attno))
		elog(ERROR, "could not find junk gp_segment_id column");
	value = ExecGetJunkAttribute(slot, gp_segment_id_attno, &isnull);
	if (isnull)
		elog(ERROR, "gp_segment_id is NULL");
	*gp_segment_id = DatumGetInt32(value);

	/* extract ctid metadata */
	ctid_attno = ExecFindJunkAttribute(queryDesc->estate->es_junkFilter, "ctid");
	if (!AttributeNumberIsValid(ctid_attno))
		elog(ERROR, "could not find junk ctid column");
	value = ExecGetJunkAttribute(slot, ctid_attno, &isnull);
	if (isnull)
		elog(ERROR, "ctid is NULL");
	ItemPointerCopy(DatumGetItemPointer(value), ctid);

	/* 
	 * extract tableoid metadata
	 *
	 * DECLARE CURSOR planning only includes tableoid metadata when
	 * scrolling a partitioned table, as this is the only case in which
	 * gp_segment_id/ctid alone do not suffice to uniquely identify a tuple.
	 */
	tableoid_attno = ExecFindJunkAttribute(queryDesc->estate->es_junkFilter,
										   "tableoid");
	if (AttributeNumberIsValid(tableoid_attno))
	{
		value = ExecGetJunkAttribute(slot, tableoid_attno, &isnull);
		if (isnull)
			elog(ERROR, "tableoid is NULL");
		*tableoid = DatumGetObjectId(value);

		/*
		 * This is our last opportunity to verify that the physical table given
		 * by tableoid is, indeed, simply updatable.
		 */
		if (!isSimplyUpdatableRelation(*tableoid))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("%s is not updatable",
							get_rel_name_partition(*tableoid))));
	} else
		*tableoid = InvalidOid;

	pfree(table_name);
}


/*
 * Evaluate functions to constants.
 */
static Node *
exec_make_plan_constant(struct PlannedStmt *stmt, bool is_SRI)
{
	pre_dispatch_function_evaluation_context pcontext;

	Assert(stmt);
	exec_init_plan_tree_base(&pcontext.base, stmt);
	pcontext.single_row_insert = is_SRI;

	return plan_tree_mutator((Node *)stmt->planTree, pre_dispatch_function_evaluation_mutator, &pcontext);
}

/*
 * Called by apply_motion code
 */
Node *
planner_make_plan_constant(struct PlannerInfo *root, Node *n, bool is_SRI)
{
	pre_dispatch_function_evaluation_context pcontext;

	planner_init_plan_tree_base(&pcontext.base, root);
	pcontext.single_row_insert = is_SRI;

	return plan_tree_mutator(n, pre_dispatch_function_evaluation_mutator, &pcontext);
}


/*
 *
 * Remove subquery field in RTE's with subquery kind
 * This is an optimization used to reduce plan size before serialization
 *
 */
static void
remove_subquery_in_RTEs(Node *node)
{
	if (node == NULL)
	{
		return;
	}

 	if (IsA(node, RangeTblEntry))
 	{
 		RangeTblEntry *rte = (RangeTblEntry *)node;
 		if (RTE_SUBQUERY == rte->rtekind && NULL != rte->subquery)
 		{
 			/*
 			 * replace subquery with a dummy subquery
 			 */
 			rte->subquery = makeNode(Query);
 		}

 		return;
 	}

 	if (IsA(node, List))
 	{
 		List *list = (List *) node;
 		ListCell   *lc = NULL;
 		foreach(lc, list)
 		{
 			remove_subquery_in_RTEs((Node *) lfirst(lc));
 		}
 	}
}


/*
 * Compose and dispatch the MPPEXEC commands corresponding to a plan tree
 * within a complete parallel plan.  (A plan tree will correspond either
 * to an initPlan or to the main plan.)
 *
 * If cancelOnError is true, then any dispatching error, a cancellation
 * request from the client, or an error from any of the associated QEs,
 * may cause the unfinished portion of the plan to be abandoned or canceled;
 * and in the event this occurs before all gangs have been dispatched, this
 * function does not return, but waits for all QEs to stop and exits to
 * the caller's error catcher via ereport(ERROR,...).  Otherwise this
 * function returns normally and errors are not reported until later.
 *
 * If cancelOnError is false, the plan is to be dispatched as fully as
 * possible and the QEs allowed to proceed regardless of cancellation
 * requests, errors or connection failures from other QEs, etc.
 *
 * The CdbDispatchResults objects allocated for the plan are returned
 * in *pPrimaryResults.  The caller, after calling
 * CdbCheckDispatchResult(), can examine the CdbDispatchResults
 * objects, can keep them as long as needed, and ultimately must free
 * them with cdbdisp_destroyDispatchResults() prior to deallocation of
 * the caller's memory context.  Callers should use PG_TRY/PG_CATCH to
 * ensure proper cleanup.
 *
 * To wait for completion, check for errors, and clean up, it is
 * suggested that the caller use cdbdisp_finishCommand().
 *
 * Note that the slice tree dispatched is the one specified in the EState
 * of the argument QueryDesc as es_cur__slice.
 *
 * Note that the QueryDesc params must include PARAM_EXEC_REMOTE parameters
 * containing the values of any initplans required by the slice to be run.
 * (This is handled by calls to addRemoteExecParamsToParamList() from the
 * functions preprocess_initplans() and ExecutorRun().)
 *
 * Each QE receives its assignment as a message of type 'M' in PostgresMain().
 * The message is deserialized and processed by exec_mpp_query() in postgres.c.
 */
void
cdbdisp_dispatchPlan(struct QueryDesc *queryDesc,
					 bool planRequiresTxn,
					 bool cancelOnError,
					 struct CdbDispatcherState *ds)
{
	char 	*splan,
		*ssliceinfo,
		*sparams;

	int 	splan_len,
		splan_len_uncompressed,
		ssliceinfo_len,
		sparams_len;

	SliceTable *sliceTbl;
	int			rootIdx;
	int			oldLocalSlice;
	PlannedStmt	   *stmt;
	bool		is_SRI;
	
	DispatchCommandQueryParms queryParms;
	CdbComponentDatabaseInfo *qdinfo;
 
	ds->primaryResults = NULL;
	ds->dispatchThreads = NULL;

	Assert(Gp_role == GP_ROLE_DISPATCH);
	Assert(queryDesc != NULL && queryDesc->estate != NULL);

	/*
	 * Later we'll need to operate with the slice table provided via the
	 * EState structure in the argument QueryDesc.	Cache this information
	 * locally and assert our expectations about it.
	 */
	sliceTbl = queryDesc->estate->es_sliceTable;
	rootIdx = RootSliceIndex(queryDesc->estate);

	Assert(sliceTbl != NULL);
	Assert(rootIdx == 0 ||
		   (rootIdx > sliceTbl->nMotions && rootIdx <= sliceTbl->nMotions + sliceTbl->nInitPlans));

	/*
	 * Keep old value so we can restore it.  We use this field as a parameter.
	 */
	oldLocalSlice = sliceTbl->localSlice;

	/*
	 * This function is called only for planned statements.
	 */
	stmt = queryDesc->plannedstmt;
	Assert(stmt);


	/*
	 * Let's evaluate STABLE functions now, so we get consistent values on the QEs
	 *
	 * Also, if this is a single-row INSERT statement, let's evaluate
	 * nextval() and currval() now, so that we get the QD's values, and a
	 * consistent value for everyone
	 *
	 */

	is_SRI = false;

	if (queryDesc->operation == CMD_INSERT)
	{
		Assert(stmt->commandType == CMD_INSERT);

		/* We might look for constant input relation (instead of SRI), but I'm afraid
		 * that wouldn't scale.
		 */
		is_SRI = IsA(stmt->planTree, Result) && stmt->planTree->lefttree == NULL;
	}

	if (!is_SRI)
		clear_relsize_cache();

	if (queryDesc->operation == CMD_INSERT ||
		queryDesc->operation == CMD_SELECT ||
		queryDesc->operation == CMD_UPDATE ||
		queryDesc->operation == CMD_DELETE)
	{

		MemoryContext oldContext;
		
		oldContext = CurrentMemoryContext;
		if ( stmt->qdContext ) /* Temporary! See comment in PlannedStmt. */
		{
			oldContext = MemoryContextSwitchTo(stmt->qdContext);
		}
		else /* MPP-8382: memory context of plan tree should not change */
		{
			MemoryContext mc = GetMemoryChunkContext(stmt->planTree);
			oldContext = MemoryContextSwitchTo(mc);
		}

		stmt->planTree = (Plan *) exec_make_plan_constant(stmt, is_SRI);
		
		MemoryContextSwitchTo(oldContext);
	}

	/*
	 * Cursor queries and bind/execute path queries don't run on the
	 * writer-gang QEs; but they require snapshot-synchronization to
	 * get started.
	 *
	 * initPlans, and other work (see the function pre-evaluation
	 * above) may advance the snapshot "segmateSync" value, so we're
	 * best off setting the shared-snapshot-ready value here. This
	 * will dispatch to the writer gang and force it to set its
	 * snapshot; we'll then be able to serialize the same snapshot
	 * version (see qdSerializeDtxContextInfo() below).
	 *
	 * For details see MPP-6533/MPP-5805. There are a large number of
	 * interesting test cases for segmate-sync.
	 */
	if (queryDesc->extended_query)
	{
		verify_shared_snapshot_ready();
	}

	/*
	 *	MPP-20785:
	 *	remove subquery field from RTE's since it is not needed during query
	 *	execution,
	 *	this is an optimization to reduce size of serialized plan before dispatching
	 */
	remove_subquery_in_RTEs((Node *) (queryDesc->plannedstmt->rtable));

	/*
	 * serialized plan tree. Note that we're called for a single
	 * slice tree (corresponding to an initPlan or the main plan), so the
	 * parameters are fixed and we can include them in the prefix.
	 */
	splan = serializeNode((Node *) queryDesc->plannedstmt, &splan_len, &splan_len_uncompressed);

	/* compute the total uncompressed size of the query plan for all slices */
	int num_slices = queryDesc->plannedstmt->planTree->nMotionNodes + 1;
	uint64 plan_size_in_kb = ((uint64)splan_len_uncompressed * (uint64)num_slices) / (uint64)1024;
	
	elog(((gp_log_gang >= GPVARS_VERBOSITY_VERBOSE) ? LOG : DEBUG1), 
		"Query plan size to dispatch: " UINT64_FORMAT "KB", plan_size_in_kb);

	if (0 < gp_max_plan_size && plan_size_in_kb > gp_max_plan_size)
	{
		ereport(ERROR,
			(errcode(ERRCODE_STATEMENT_TOO_COMPLEX),
				(errmsg("Query plan size limit exceeded, current size: " UINT64_FORMAT "KB, max allowed size: %dKB", plan_size_in_kb, gp_max_plan_size),
				 errhint("Size controlled by gp_max_plan_size"))));
	}

	Assert(splan != NULL && splan_len > 0 && splan_len_uncompressed > 0);
	
	if (queryDesc->params != NULL && queryDesc->params->numParams > 0)
	{		
        ParamListInfoData  *pli;
        ParamExternData    *pxd;
        StringInfoData      parambuf;
		Size                length;
        int                 plioff;
		int32               iparam;

        /* Allocate buffer for params */
        initStringInfo(&parambuf);

        /* Copy ParamListInfoData header and ParamExternData array */
        pli = queryDesc->params;
        length = (char *)&pli->params[pli->numParams] - (char *)pli;
        plioff = parambuf.len;
        Assert(plioff == MAXALIGN(plioff));
        appendBinaryStringInfo(&parambuf, pli, length);

        /* Copy pass-by-reference param values. */
        for (iparam = 0; iparam < queryDesc->params->numParams; iparam++)
		{
			int16   typlen;
			bool    typbyval;

            /* Recompute pli each time in case parambuf.data is repalloc'ed */
            pli = (ParamListInfoData *)(parambuf.data + plioff);
			pxd = &pli->params[iparam];

            /* Does pxd->value contain the value itself, or a pointer? */
			get_typlenbyval(pxd->ptype, &typlen, &typbyval);
            if (!typbyval)
            {
				char   *s = DatumGetPointer(pxd->value);

				if (pxd->isnull ||
                    !PointerIsValid(s))
                {
                    pxd->isnull = true;
                    pxd->value = 0;
                }
				else
				{
			        length = datumGetSize(pxd->value, typbyval, typlen);

					/* MPP-1637: we *must* set this before we
					 * append. Appending may realloc, which will
					 * invalidate our pxd ptr. (obviously we could
					 * append first if we recalculate pxd from the new
					 * base address) */
                    pxd->value = Int32GetDatum(length);

                    appendBinaryStringInfo(&parambuf, &iparam, sizeof(iparam));
                    appendBinaryStringInfo(&parambuf, s, length);
				}
            }
		}
        sparams = parambuf.data;
        sparams_len = parambuf.len;
	}
	else
	{
		sparams = NULL;
		sparams_len = 0;
	}

	ssliceinfo = serializeNode((Node *) sliceTbl, &ssliceinfo_len, NULL /*uncompressed_size*/);
	
	MemSet(&queryParms, 0, sizeof(queryParms));
	queryParms.strCommand = queryDesc->sourceText;
	queryParms.serializedQuerytree = NULL;
	queryParms.serializedQuerytreelen = 0;
	queryParms.serializedPlantree = splan;
	queryParms.serializedPlantreelen = splan_len;
	queryParms.serializedParams = sparams;
	queryParms.serializedParamslen = sparams_len;
	queryParms.serializedSliceInfo = ssliceinfo;
	queryParms.serializedSliceInfolen= ssliceinfo_len;
	queryParms.rootIdx = rootIdx;

	/* sequence server info */
	qdinfo = &(getComponentDatabases()->entry_db_info[0]);
	Assert(qdinfo != NULL && qdinfo->hostip != NULL);
	queryParms.seqServerHost = pstrdup(qdinfo->hostip);
	queryParms.seqServerHostlen = strlen(qdinfo->hostip) + 1;
	queryParms.seqServerPort = seqServerCtl->seqServerPort;

	queryParms.primary_gang_id = 0;	/* We are relying on the slice table to provide gang ids */

	/* serialized a version of our snapshot */
	/* 
	 * Generate our transction isolations.  We generally want Plan
	 * based dispatch to be in a global transaction. The executor gets
	 * to decide if the special circumstances exist which allow us to
	 * dispatch without starting a global xact.
	 */
	queryParms.serializedDtxContextInfo = 
		GetDispatcherMgr().qdSerializeDtxContextInfo(&queryParms.serializedDtxContextInfolen, true /* wantSnapshot */, queryDesc->extended_query,
								  GetDispatcherMgr().generateTxnOptions(planRequiresTxn), "cdbdisp_dispatchPlan");

	Assert(sliceTbl);
	Assert(sliceTbl->slices != NIL);

	GetDispatcherMgr().cdbdisp_dispatchX(&queryParms, cancelOnError, sliceTbl, ds);

	sliceTbl->localSlice = oldLocalSlice;
}	/* cdbdisp_dispatchPlan */


/*
 * Dispatch a command - already parsed and in the form of a Node tree
 * - to all primary segdbs.  Does not wait for completion. Does not
 * start a global transaction.
 *
 * NB: Callers should use PG_TRY()/PG_CATCH() if needed to make
 * certain that the CdbDispatchResults objects are destroyed by
 * cdbdisp_destroyDispatchResults() in case of error.
 * To wait for completion, check for errors, and clean up, it is
 * suggested that the caller use cdbdisp_finishCommand().
 */
void
cdbdisp_dispatchUtilityStatement(struct Node *stmt,
								 bool cancelOnError,
								 bool needTwoPhase,
								 bool withSnapshot,
								 struct CdbDispatcherState *ds,
								 char *debugCaller)
{
	char	   *serializedQuerytree;
	int			serializedQuerytree_len;
	Query	   *q = makeNode(Query);
	StringInfoData buffer;

	elog((Debug_print_full_dtm ? LOG : DEBUG5),"cdbdisp_dispatchUtilityStatement debug_query_string = %s (needTwoPhase = %s, debugCaller = %s)",
	     debug_query_string, (needTwoPhase ? "true" : "false"), debugCaller);

	dtmPreCommand("cdbdisp_dispatchUtilityStatement", "(none)", NULL, needTwoPhase,
			withSnapshot, false /* inCursor */ );
	
	initStringInfo(&buffer);

	q->commandType = CMD_UTILITY;

	Assert(stmt != NULL);
	Assert(stmt->type < 1000);
	Assert(stmt->type > 0);

	q->utilityStmt = stmt;

	q->querySource = QSRC_ORIGINAL;

	/*
	 * We must set q->canSetTag = true.  False would be used to hide a command
	 * introduced by rule expansion which is not allowed to return its
	 * completion status in the command tag (PQcmdStatus/PQcmdTuples). For
	 * example, if the original unexpanded command was SELECT, the status
	 * should come back as "SELECT n" and should not reflect other commands
	 * inserted by rewrite rules.  True means we want the status.
	 */
	q->canSetTag = true;		/* ? */

	/*
	 * serialized the stmt tree, and create the sql statement: mppexec ....
	 */
	serializedQuerytree = serializeNode((Node *) q, &serializedQuerytree_len, NULL /*uncompressed_size*/);

	Assert(serializedQuerytree != NULL);

	GetDispatcherMgr().dispatchCommand(debug_query_string, serializedQuerytree, serializedQuerytree_len, cancelOnError, needTwoPhase,
							withSnapshot, ds);

}	/* cdbdisp_dispatchUtilityStatement */


/*
 * Dispatch a command - already parsed and in the form of a Node tree
 * - to all primary segdbs, and wait for completion.  Starts a global
 * transaction first, if not already started.  If not all QEs in the
 * given gang(s) executed the command successfully, throws an error
 * and does not return.
 */
void
CdbDispatchUtilityStatement(struct Node *stmt, char *debugCaller __attribute__((unused)) )
{
	CdbDispatchUtilityStatement_Internal(stmt, /* needTwoPhase */ true, "CdbDispatchUtilityStatement");
}

void
CdbDispatchUtilityStatement_NoTwoPhase(struct Node *stmt, char *debugCaller __attribute__((unused)) )
{
	CdbDispatchUtilityStatement_Internal(stmt, /* needTwoPhase */ false, "CdbDispatchUtilityStatement_NoTwoPhase");
}

static void
CdbDispatchUtilityStatement_Internal(struct Node *stmt, bool needTwoPhase, char *debugCaller)
{
	volatile struct CdbDispatcherState ds = {NULL, NULL};
	
	elog((Debug_print_full_dtm ? LOG : DEBUG5),
		 "cdbdisp_dispatchUtilityStatement called (needTwoPhase = %s, debugCaller = %s)",
		 (needTwoPhase ? "true" : "false"), debugCaller);

	PG_TRY();
	{
		cdbdisp_dispatchUtilityStatement(stmt, 
										 true /* cancelOnError */, 
										 needTwoPhase, 
										 true /* withSnapshot */,
										 (struct CdbDispatcherState *)&ds,
										 debugCaller);

		/* Wait for all QEs to finish.	Throw up if error. */
		GetDispatcherMgr().finishCommand((struct CdbDispatcherState *)&ds, NULL, NULL);
	}
	PG_CATCH();
	{
		/* Something happend, clean up after ourselves */
		GetDispatcherMgr().CdbCheckDispatchResult((struct CdbDispatcherState *)&ds,
							   DISPATCH_WAIT_CANCEL);

		cdbdisp_destroyDispatchResults(ds.primaryResults);
		cdbdisp_destroyDispatchThreads(ds.dispatchThreads);

		PG_RE_THROW();
		/* not reached */
	}
	PG_END_TRY();

}	/* CdbDispatchUtilityStatement */
