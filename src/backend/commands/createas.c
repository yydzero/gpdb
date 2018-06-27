/*-------------------------------------------------------------------------
 *
 * createas.c
 *	  Execution of CREATE TABLE ... AS, a/k/a SELECT INTO.
 *	  Since CREATE MATERIALIZED VIEW shares syntax and most behaviors,
 *	  we implement that here, too.
 *
 * We implement this by diverting the query's normal output to a
 * specialized DestReceiver type.
 *
 * Formerly, CTAS was implemented as a variant of SELECT, which led
 * to assorted legacy behaviors that we still try to preserve, notably that
 * we must return a tuples-processed count in the completionTag.
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/createas.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/reloptions.h"
#include "access/htup_details.h"
#include "access/sysattr.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "catalog/namespace.h"
#include "catalog/toasting.h"
#include "commands/createas.h"
#include "commands/matview.h"
#include "commands/prepare.h"
#include "commands/tablecmds.h"
#include "commands/view.h"
#include "miscadmin.h"
#include "parser/parse_clause.h"
#include "rewrite/rewriteHandler.h"
#include "storage/smgr.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/rls.h"
#include "utils/snapmgr.h"

#include "access/appendonlywriter.h"
#include "catalog/aoseg.h"
#include "catalog/aovisimap.h"
#include "catalog/oid_dispatch.h"
#include "catalog/pg_attribute_encoding.h"
#include "cdb/cdbappendonlyam.h"
#include "cdb/cdbaocsam.h"
#include "cdb/cdbdisp_query.h"
#include "cdb/cdbvars.h"

typedef struct
{
	DestReceiver pub;			/* publicly-known function pointers */
	IntoClause *into;			/* target relation specification */
	/* These fields are filled by intorel_startup: */
	Relation	rel;			/* relation to write to */
	CommandId	output_cid;		/* cmin to insert in output tuples */
	int			hi_options;		/* heap_insert performance options */
	BulkInsertState bistate;	/* bulk insert state */

	struct AppendOnlyInsertDescData *ao_insertDesc; /* descriptor to AO tables */
	struct AOCSInsertDescData *aocs_insertDes;      /* descriptor for aocs */

	/* GPDB_92_MERGE_FIXME: Seems to be useless? */
	ItemPointerData last_heap_tid;
} DR_intorel;

<<<<<<< HEAD
static void intorel_startup_dummy(DestReceiver *self, int operation, TupleDesc typeinfo);
=======
/* the address of the created table, for ExecCreateTableAs consumption */
static ObjectAddress CreateAsReladdr = {InvalidOid, InvalidOid, 0};

static void intorel_startup(DestReceiver *self, int operation, TupleDesc typeinfo);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
static void intorel_receive(TupleTableSlot *slot, DestReceiver *self);
static void intorel_shutdown(DestReceiver *self);
static void intorel_destroy(DestReceiver *self);

/*
 * ExecCreateTableAs -- execute a CREATE TABLE AS command
 */
ObjectAddress
ExecCreateTableAs(CreateTableAsStmt *stmt, const char *queryString,
				  ParamListInfo params, char *completionTag)
{
	Query	   *query = (Query *) stmt->query;
	IntoClause *into = stmt->into;
	bool		is_matview = (into->viewQuery != NULL);
	DestReceiver *dest;
	Oid			save_userid = InvalidOid;
	int			save_sec_context = 0;
	int			save_nestlevel = 0;
	ObjectAddress address;
	List	   *rewritten;
	PlannedStmt *plan;
	QueryDesc  *queryDesc;
	ScanDirection dir;

<<<<<<< HEAD
	Assert(Gp_role == GP_ROLE_DISPATCH);
=======
	if (stmt->if_not_exists)
	{
		Oid			nspid;

		nspid = RangeVarGetCreationNamespace(stmt->into->rel);

		if (get_relname_relid(stmt->into->rel->relname, nspid))
		{
			ereport(NOTICE,
					(errcode(ERRCODE_DUPLICATE_TABLE),
					 errmsg("relation \"%s\" already exists, skipping",
							stmt->into->rel->relname)));
			return InvalidObjectAddress;
		}
	}
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

	/*
	 * Create the tuple receiver object and insert info it will need
	 */
	dest = CreateIntoRelDestReceiver(into);

	/*
	 * The contained Query could be a SELECT, or an EXECUTE utility command.
	 * If the latter, we just pass it off to ExecuteQuery.
	 */
	Assert(IsA(query, Query));
	if (query->commandType == CMD_UTILITY &&
		IsA(query->utilityStmt, ExecuteStmt))
	{
		ExecuteStmt *estmt = (ExecuteStmt *) query->utilityStmt;

<<<<<<< HEAD
		elog(ERROR, "Create Table As Execute is known to need some work. Error out temporarily to avoid panic which affects other tests sometimes.");

=======
		Assert(!is_matview);	/* excluded by syntax */
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
		ExecuteQuery(estmt, into, queryString, params, dest, completionTag);

		address = CreateAsReladdr;
		CreateAsReladdr = InvalidObjectAddress;
		return address;
	}
	Assert(query->commandType == CMD_SELECT);

	/*
	 * For materialized views, lock down security-restricted operations and
	 * arrange to make GUC variable changes local to this command.  This is
	 * not necessary for security, but this keeps the behavior similar to
	 * REFRESH MATERIALIZED VIEW.  Otherwise, one could create a materialized
	 * view not possible to refresh.
	 */
	if (is_matview)
	{
		GetUserIdAndSecContext(&save_userid, &save_sec_context);
		SetUserIdAndSecContext(save_userid,
						   save_sec_context | SECURITY_RESTRICTED_OPERATION);
		save_nestlevel = NewGUCNestLevel();
	}

	/*
	 * Parse analysis was done already, but we still have to run the rule
	 * rewriter.  We do not do AcquireRewriteLocks: we assume the query either
	 * came straight from the parser, or suitable locks were acquired by
	 * plancache.c.
	 *
	 * Because the rewriter and planner tend to scribble on the input, we make
	 * a preliminary copy of the source querytree.  This prevents problems in
	 * the case that CTAS is in a portal or plpgsql function and is executed
	 * repeatedly.  (See also the same hack in EXPLAIN and PREPARE.)
	 */
	rewritten = QueryRewrite((Query *) copyObject(query));

	/* SELECT should never rewrite to more or less than one SELECT query */
	if (list_length(rewritten) != 1)
		elog(ERROR, "unexpected rewrite result for CREATE TABLE AS SELECT");
	query = (Query *) linitial(rewritten);
	Assert(query->commandType == CMD_SELECT);

	/* plan the query */
	plan = pg_plan_query(query, 0, params);

	/*
	 * Use a snapshot with an updated command ID to ensure this query sees
	 * results of any previously executed queries.  (This could only matter if
	 * the planner executed an allegedly-stable function that changed the
	 * database contents, but let's do it anyway to be parallel to the EXPLAIN
	 * code path.)
	 */
	PushCopiedSnapshot(GetActiveSnapshot());
	UpdateActiveSnapshotCommandId();

	/* Create a QueryDesc, redirecting output to our tuple receiver */
	queryDesc = CreateQueryDesc(plan, queryString,
								GetActiveSnapshot(), InvalidSnapshot,
								dest, params, 0);

	/* call ExecutorStart to prepare the plan for execution */
	ExecutorStart(queryDesc, GetIntoRelEFlags(into));

	/*
	 * Normally, we run the plan to completion; but if skipData is specified,
	 * just do tuple receiver startup and shutdown.
	 */
	if (into->skipData)
		dir = NoMovementScanDirection;
	else
		dir = ForwardScanDirection;

	/* run the plan */
	ExecutorRun(queryDesc, dir, 0L);

	dest->rDestroy(dest);

	/* and clean up */
	ExecutorFinish(queryDesc);
	ExecutorEnd(queryDesc);

	if (into->distributedBy &&
		((DistributedBy *)(into->distributedBy))->ptype == POLICYTYPE_REPLICATED)
		queryDesc->es_processed /= getgpsegmentCount();

	/* save the rowcount if we're given a completionTag to fill */
	if (completionTag)
		snprintf(completionTag, COMPLETION_TAG_BUFSIZE,
				 "SELECT " UINT64_FORMAT, queryDesc->es_processed);

	FreeQueryDesc(queryDesc);

	PopActiveSnapshot();

	if (is_matview)
	{
		/* Roll back any GUC changes */
		AtEOXact_GUC(false, save_nestlevel);

		/* Restore userid and security context */
		SetUserIdAndSecContext(save_userid, save_sec_context);
	}

	address = CreateAsReladdr;
	CreateAsReladdr = InvalidObjectAddress;

	return address;
}

/*
 * GetIntoRelEFlags --- compute executor flags needed for CREATE TABLE AS
 *
 * This is exported because EXPLAIN and PREPARE need it too.  (Note: those
 * callers still need to deal explicitly with the skipData flag; since they
 * use different methods for suppressing execution, it doesn't seem worth
 * trying to encapsulate that part.)
 */
int
GetIntoRelEFlags(IntoClause *intoClause)
{
	int			flags;

	/*
	 * We need to tell the executor whether it has to produce OIDs or not,
	 * because it doesn't have enough information to do so itself (since we
	 * can't build the target relation until after ExecutorStart).
	 *
	 * Disallow the OIDS option for materialized views.
	 */
	if (interpretOidsOption(intoClause->options,
							(intoClause->viewQuery == NULL)))
		flags = EXEC_FLAG_WITH_OIDS;
	else
		flags = EXEC_FLAG_WITHOUT_OIDS;

	if (intoClause->skipData)
		flags |= EXEC_FLAG_WITH_NO_DATA;

	return flags;
}

/*
 * CreateIntoRelDestReceiver -- create a suitable DestReceiver object
 *
 * intoClause will be NULL if called from CreateDestReceiver(), in which
 * case it has to be provided later.  However, it is convenient to allow
 * self->into to be filled in immediately for other callers.
 */
DestReceiver *
CreateIntoRelDestReceiver(IntoClause *intoClause)
{
	DR_intorel *self = (DR_intorel *) palloc0(sizeof(DR_intorel));

	self->pub.receiveSlot = intorel_receive;
	self->pub.rStartup = intorel_startup_dummy;
	self->pub.rShutdown = intorel_shutdown;
	self->pub.rDestroy = intorel_destroy;
	self->pub.mydest = DestIntoRel;
	self->into = intoClause;

	self->ao_insertDesc = NULL;
	self->aocs_insertDes = NULL;

	return (DestReceiver *) self;
}

/*
 * intorel_startup_dummy --- executor startup
 */
static void
intorel_startup_dummy(DestReceiver *self, int operation, TupleDesc typeinfo)
{
<<<<<<< HEAD
	/* no-op */

	/* See intorel_initplan() for explanation */
}

/*
 * intorel_initplan --- Based on PG intorel_startup().
 * Parameters are different. We need to run the code earlier before the
 * executor runs since we want the relation to be created earlier else current
 * MPP framework will fail. This could be called in InitPlan() as before, but
 * we could call it just before ExecutorRun() in ExecCreateTableAs(). In the
 * future if the requirment is general we could add an interface into
 * DestReceiver but so far that is not needed (Based on PG 11 code.)
 */
void
intorel_initplan(QueryDesc *queryDesc, int eflags)
{
	DR_intorel *myState;
	/* Get 'into' from the dispatched plan */
	IntoClause *into = queryDesc->plannedstmt->intoClause;
=======
	DR_intorel *myState = (DR_intorel *) self;
	IntoClause *into = myState->into;
	bool		is_matview;
	char		relkind;
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
	CreateStmt *create;
	ObjectAddress intoRelationAddr;
	Relation	intoRelationDesc;
	RangeTblEntry *rte;
	Datum		toast_options;
	ListCell   *lc;
	int			attnum;
	static char *validnsps[] = HEAP_RELOPT_NAMESPACES;
	StdRdOptions *stdRdOptions;
	Datum       reloptions;
	int         relstorage;
	bool		validate_reloptions;
	TupleDesc   typeinfo = queryDesc->tupDesc;

	/* If EXPLAIN/QE, skip creating the "into" relation. */
	if ((eflags & EXEC_FLAG_EXPLAIN_ONLY) ||
		(Gp_role == GP_ROLE_EXECUTE && !Gp_is_writer))
		return;

	/* This code supports both CREATE TABLE AS and CREATE MATERIALIZED VIEW */
	is_matview = (into->viewQuery != NULL);
	relkind = is_matview ? RELKIND_MATVIEW : RELKIND_RELATION;

	/*
	 * Create the target relation by faking up a CREATE TABLE parsetree and
	 * passing it to DefineRelation.
	 */
	create = makeNode(CreateStmt);
	create->relation = into->rel;
	create->tableElts = NIL;	/* will fill below */
	create->inhRelations = NIL;
	create->ofTypename = NULL;
	create->constraints = NIL;
	create->options = into->options;
	create->oncommit = into->onCommit;
	/*
	 * Select tablespace to use.  If not specified, use default tablespace
	 * (which may in turn default to database's default).
	 *
	 * In PostgreSQL, we resolve default tablespace here. In GPDB, that's
	 * done earlier, because we need to dispatch the final tablespace name,
	 * after resolving any defaults, to the segments. (Otherwise, we would
	 * rely on the assumption that default_tablespace GUC is kept in sync
	 * in all segment connections. That actually seems to be the case, as of
	 * this writing, but better to not rely on it.) So usually, we already
	 * have the fully-resolved tablespace name stashed in queryDesc->ddesc->
	 * intoTableSpaceName. In the dispatcher, we filled it in earlier, and
	 * in executor nodes, we received it from the dispatcher along with the
	 * query. In utility mode, however, queryDesc->ddesc is not set at all,
	 * and we follow the PostgreSQL codepath, resolving the defaults here.
	 */

	if (queryDesc->ddesc)
		create->tablespacename = queryDesc->ddesc->intoTableSpaceName;
	else
		create->tablespacename = into->tableSpaceName;
	create->if_not_exists = false;

	/*
	 * Build column definitions using "pre-cooked" type and collation info. If
	 * a column name list was specified in CREATE TABLE AS, override the
	 * column names derived from the query.  (Too few column names are OK, too
	 * many are not.)
	 */
	lc = list_head(into->colNames);
	for (attnum = 0; attnum < typeinfo->natts; attnum++)
	{
		Form_pg_attribute attribute = typeinfo->attrs[attnum];
		ColumnDef  *col = makeNode(ColumnDef);
		TypeName   *coltype = makeNode(TypeName);

		if (lc)
		{
			col->colname = strVal(lfirst(lc));
			lc = lnext(lc);
		}
		else
			col->colname = NameStr(attribute->attname);
		col->typeName = coltype;
		col->inhcount = 0;
		col->is_local = true;
		col->is_not_null = false;
		col->is_from_type = false;
		col->storage = 0;
		col->raw_default = NULL;
		col->cooked_default = NULL;
		col->collClause = NULL;
		col->collOid = attribute->attcollation;
		col->constraints = NIL;
		col->fdwoptions = NIL;
		col->location = -1;

		coltype->names = NIL;
		coltype->typeOid = attribute->atttypid;
		coltype->setof = false;
		coltype->pct_type = false;
		coltype->typmods = NIL;
		coltype->typemod = attribute->atttypmod;
		coltype->arrayBounds = NIL;
		coltype->location = -1;

		/*
		 * It's possible that the column is of a collatable type but the
		 * collation could not be resolved, so double-check.  (We must check
		 * this here because DefineRelation would adopt the type's default
		 * collation rather than complaining.)
		 */
		if (!OidIsValid(col->collOid) &&
			type_is_collatable(coltype->typeOid))
			ereport(ERROR,
					(errcode(ERRCODE_INDETERMINATE_COLLATION),
					 errmsg("no collation was derived for column \"%s\" with collatable type %s",
							col->colname, format_type_be(coltype->typeOid)),
					 errhint("Use the COLLATE clause to set the collation explicitly.")));

		create->tableElts = lappend(create->tableElts, col);
	}

	if (lc != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("too many column names were specified")));

	/* Parse and validate any reloptions */
	reloptions = transformRelOptions((Datum) 0,
									 into->options,
									 NULL,
									 validnsps,
									 true,
									 false);

	/* get the relstorage (heap or AO tables) */
	if (queryDesc->ddesc)
		validate_reloptions = queryDesc->ddesc->validate_reloptions;
	else
		validate_reloptions = true;

	stdRdOptions = (StdRdOptions*) heap_reloptions(RELKIND_RELATION, reloptions, validate_reloptions);
	if(stdRdOptions->appendonly)
		relstorage = stdRdOptions->columnstore ? RELSTORAGE_AOCOLS : RELSTORAGE_AOROWS;
	else
		relstorage = RELSTORAGE_HEAP;

	create->distributedBy = into->distributedBy; /* Seems to be not needed? */
	create->partitionBy = NULL; /* CTAS does not not support partition. */

    create->policy = queryDesc->plannedstmt->intoPolicy;
	create->postCreate = NULL;
	create->deferredStmts = NULL;
	create->is_part_child = false;
	create->is_add_part = false;
	create->is_split_part = false;
	create->buildAoBlkdir = false;
	create->attr_encodings = NULL; /* Handle by AddDefaultRelationAttributeOptions() */

	/* Save them in CreateStmt for dispatching. */
	create->relKind = RELKIND_RELATION;
	create->relStorage = relstorage;
	create->ownerid = GetUserId();

	/*
	 * Actually create the target table.
	 * Don't dispatch it yet, as we haven't created the toast and other
	 * auxiliary tables yet.
	 */
<<<<<<< HEAD
	intoRelationId = DefineRelation(create, RELKIND_RELATION, InvalidOid, relstorage, false);
=======
	intoRelationAddr = DefineRelation(create, relkind, InvalidOid, NULL);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

	/*
	 * If necessary, create a TOAST table for the target table.  Note that
	 * NewRelationCreateToastTable ends with CommandCounterIncrement(), so
	 * that the TOAST table will be visible for insertion.
	 */
	CommandCounterIncrement();

	/* parse and validate reloptions for the toast table */
	toast_options = transformRelOptions((Datum) 0,
										create->options,
										"toast",
										validnsps,
										true, false);

	(void) heap_reloptions(RELKIND_TOASTVALUE, toast_options, true);

<<<<<<< HEAD
	AlterTableCreateToastTable(intoRelationId, toast_options, false, true);
	AlterTableCreateAoSegTable(intoRelationId, false);
	/* don't create AO block directory here, it'll be created when needed. */
	AlterTableCreateAoVisimapTable(intoRelationId, false);
=======
	NewRelationCreateToastTable(intoRelationAddr.objectId, toast_options);

	/* Create the "view" part of a materialized view. */
	if (is_matview)
	{
		/* StoreViewQuery scribbles on tree, so make a copy */
		Query	   *query = (Query *) copyObject(into->viewQuery);

		StoreViewQuery(intoRelationAddr.objectId, query, false);
		CommandCounterIncrement();
	}
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

	/*
	 * Finally we can open the target table
	 */
	intoRelationDesc = heap_open(intoRelationAddr.objectId, AccessExclusiveLock);

	/*
	 * Add column encoding entries based on the WITH clause.
	 *
	 * NOTE:  we could also do this expansion during parse analysis, by
	 * expanding the IntoClause options field into some attr_encodings field
	 * (cf. CreateStmt and transformCreateStmt()). As it stands, there's no real
	 * benefit for doing that from a code complexity POV. In fact, it would mean
	 * more code. If, however, we supported column encoding syntax during CTAS,
	 * it would be a good time to relocate this code.
	 */
	AddDefaultRelationAttributeOptions(intoRelationDesc,
									   into->options);

	/*
	 * Check INSERT permission on the constructed table.
	 *
	 * XXX: It would arguably make sense to skip this check if into->skipData
	 * is true.
	 */
	rte = makeNode(RangeTblEntry);
	rte->rtekind = RTE_RELATION;
	rte->relid = intoRelationAddr.objectId;
	rte->relkind = relkind;
	rte->requiredPerms = ACL_INSERT;

	for (attnum = 1; attnum <= intoRelationDesc->rd_att->natts; attnum++)
		rte->insertedCols = bms_add_member(rte->insertedCols,
								attnum - FirstLowInvalidHeapAttributeNumber);

	ExecCheckRTPerms(list_make1(rte), true);

	/*
	 * Make sure the constructed table does not have RLS enabled.
	 *
	 * check_enable_rls() will ereport(ERROR) itself if the user has requested
	 * something invalid, and otherwise will return RLS_ENABLED if RLS should
	 * be enabled here.  We don't actually support that currently, so throw
	 * our own ereport(ERROR) if that happens.
	 */
	if (check_enable_rls(intoRelationAddr.objectId, InvalidOid, false) == RLS_ENABLED)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 (errmsg("policies not yet implemented for this command"))));

	/*
	 * Tentatively mark the target as populated, if it's a matview and we're
	 * going to fill it; otherwise, no change needed.
	 */
	if (is_matview && !into->skipData)
		SetMatViewPopulatedState(intoRelationDesc, true);

	/*
	 * Fill private fields of myState for use by later routines
	 */

	if (queryDesc->dest->mydest != DestIntoRel)
		queryDesc->dest = CreateIntoRelDestReceiver(into);
	myState = (DR_intorel *) queryDesc->dest;
	myState->rel = intoRelationDesc;
	myState->output_cid = GetCurrentCommandId(true);

	/* and remember the new relation's address for ExecCreateTableAs */
	CreateAsReladdr = intoRelationAddr;

	/*
	 * We can skip WAL-logging the insertions, unless PITR or streaming
	 * replication is in use. We can skip the FSM in any case.
	 */
	myState->hi_options = HEAP_INSERT_SKIP_FSM |
		(XLogIsNeeded() ? 0 : HEAP_INSERT_SKIP_WAL);
	myState->bistate = GetBulkInsertState();

	/* Not using WAL requires smgr_targblock be initially invalid */
	Assert(RelationGetTargetBlock(intoRelationDesc) == InvalidBlockNumber);
}

/*
 * intorel_receive --- receive one tuple
 */
static void
intorel_receive(TupleTableSlot *slot, DestReceiver *self)
{
	DR_intorel *myState = (DR_intorel *) self;
	Relation    into_rel = myState->rel;

	if (RelationIsAoRows(into_rel))
	{
		AOTupleId	aoTupleId;
		MemTuple	tuple;

		tuple = ExecCopySlotMemTuple(slot);
		if (myState->ao_insertDesc == NULL)
			myState->ao_insertDesc = appendonly_insert_init(into_rel, RESERVED_SEGNO, false);

		appendonly_insert(myState->ao_insertDesc, tuple, InvalidOid, &aoTupleId);
		pfree(tuple);
	}
	else if (RelationIsAoCols(into_rel))
	{
		if(myState->aocs_insertDes == NULL)
			myState->aocs_insertDes = aocs_insert_init(into_rel, RESERVED_SEGNO, false);

		aocs_insert(myState->aocs_insertDes, slot);
	}
	else
	{
		HeapTuple	tuple;

		/*
		 * get the heap tuple out of the tuple table slot, making sure we have a
		 * writable copy
		 */
		tuple = ExecMaterializeSlot(slot);

		/*
		 * force assignment of new OID (see comments in ExecInsert)
		 */
		if (myState->rel->rd_rel->relhasoids)
			HeapTupleSetOid(tuple, InvalidOid);

		heap_insert(myState->rel,
					tuple,
					myState->output_cid,
					myState->hi_options,
					myState->bistate,
					GetCurrentTransactionId());

		/* We know this is a newly created relation, so there are no indexes */
	}
}

/*
 * intorel_shutdown --- executor end
 */
static void
intorel_shutdown(DestReceiver *self)
{
	DR_intorel *myState = (DR_intorel *) self;
	Relation	into_rel = myState->rel;

	if (into_rel == NULL)
		return;

	FreeBulkInsertState(myState->bistate);

	/* If we skipped using WAL, must heap_sync before commit */
	if (myState->hi_options & HEAP_INSERT_SKIP_WAL)
		heap_sync(myState->rel);

	if (RelationIsAoRows(into_rel) && myState->ao_insertDesc)
		appendonly_insert_finish(myState->ao_insertDesc);
	else if (RelationIsAoCols(into_rel) && myState->aocs_insertDes)
        aocs_insert_finish(myState->aocs_insertDes);

	/* close rel, but keep lock until commit */
	heap_close(into_rel, NoLock);
	myState->rel = NULL;

}

/*
 * intorel_destroy --- release DestReceiver object
 */
static void
intorel_destroy(DestReceiver *self)
{
	pfree(self);
}
