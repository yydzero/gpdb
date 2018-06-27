/*-------------------------------------------------------------------------
 *
 * orderedsetaggs.c
 *		Ordered-set aggregate functions.
 *
<<<<<<< HEAD
 * Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
=======
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/orderedsetaggs.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "catalog/pg_aggregate.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_type.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/tlist.h"
<<<<<<< HEAD
#include "parser/parse_expr.h"
=======
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/timestamp.h"
#include "utils/tuplesort.h"


/*
 * Generic support for ordered-set aggregates
<<<<<<< HEAD
 */

typedef struct OrderedSetAggState
{
	/* Aggref for this aggregate: */
	Aggref	   *aggref;
	/* Sort object we're accumulating data in: */
	Tuplesortstate *sortstate;
	/* Number of normal rows inserted into sortstate: */
	int64		number_of_rows;
=======
 *
 * The state for an ordered-set aggregate is divided into a per-group struct
 * (which is the internal-type transition state datum returned to nodeAgg.c)
 * and a per-query struct, which contains data and sub-objects that we can
 * create just once per query because they will not change across groups.
 * The per-query struct and subsidiary data live in the executor's per-query
 * memory context, and go away implicitly at ExecutorEnd().
 */

typedef struct OSAPerQueryState
{
	/* Aggref for this aggregate: */
	Aggref	   *aggref;
	/* Memory context containing this struct and other per-query data: */
	MemoryContext qcontext;
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

	/* These fields are used only when accumulating tuples: */

	/* Tuple descriptor for tuples inserted into sortstate: */
	TupleDesc	tupdesc;
	/* Tuple slot we can use for inserting/extracting tuples: */
	TupleTableSlot *tupslot;
<<<<<<< HEAD
=======
	/* Per-sort-column sorting information */
	int			numSortCols;
	AttrNumber *sortColIdx;
	Oid		   *sortOperators;
	Oid		   *eqOperators;
	Oid		   *sortCollations;
	bool	   *sortNullsFirsts;
	/* Equality operator call info, created only if needed: */
	FmgrInfo   *equalfns;
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

	/* These fields are used only when accumulating datums: */

	/* Info about datatype of datums being sorted: */
<<<<<<< HEAD
	Oid			datumtype;
	int16		typLen;
	bool		typByVal;
	char		typAlign;
	/* Info about equality operator associated with sort operator: */
	Oid			eqOperator;
} OrderedSetAggState;
=======
	Oid			sortColType;
	int16		typLen;
	bool		typByVal;
	char		typAlign;
	/* Info about sort ordering: */
	Oid			sortOperator;
	Oid			eqOperator;
	Oid			sortCollation;
	bool		sortNullsFirst;
	/* Equality operator call info, created only if needed: */
	FmgrInfo	equalfn;
} OSAPerQueryState;

typedef struct OSAPerGroupState
{
	/* Link to the per-query state for this aggregate: */
	OSAPerQueryState *qstate;
	/* Memory context containing per-group data: */
	MemoryContext gcontext;
	/* Sort object we're accumulating data in: */
	Tuplesortstate *sortstate;
	/* Number of normal rows inserted into sortstate: */
	int64		number_of_rows;
} OSAPerGroupState;
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

static void ordered_set_shutdown(Datum arg);


/*
 * Set up working state for an ordered-set aggregate
 */
<<<<<<< HEAD
static OrderedSetAggState *
ordered_set_startup(FunctionCallInfo fcinfo, bool use_tuples)
{
	OrderedSetAggState *osastate;
	Aggref	   *aggref;
	ExprContext *peraggecontext;
	MemoryContext aggcontext;
	MemoryContext oldcontext;
	List	   *sortlist;
	int			numSortCols;

	/* Must be called as aggregate; get the Agg node's query-lifespan context */
	if (AggCheckCallContext(fcinfo, &aggcontext) != AGG_CONTEXT_AGGREGATE)
		elog(ERROR, "ordered-set aggregate called in non-aggregate context");
	/* Need the Aggref as well */
	aggref = AggGetAggref(fcinfo);
	if (!aggref)
		elog(ERROR, "ordered-set aggregate called in non-aggregate context");
	if (!AGGKIND_IS_ORDERED_SET(aggref->aggkind))
		elog(ERROR, "ordered-set aggregate support function called for non-ordered-set aggregate");
	/* Also get output exprcontext so we can register shutdown callback */
	peraggecontext = AggGetPerAggEContext(fcinfo);
	if (!peraggecontext)
		elog(ERROR, "ordered-set aggregate called in non-aggregate context");

	/* Initialize working-state object in the aggregate-lifespan context */
	osastate = (OrderedSetAggState *)
		MemoryContextAllocZero(aggcontext, sizeof(OrderedSetAggState));
	osastate->aggref = aggref;

	/* Extract the sort information */
	sortlist = aggref->aggorder;
	numSortCols = list_length(sortlist);

	if (use_tuples)
	{
		bool		ishypothetical = (aggref->aggkind == AGGKIND_HYPOTHETICAL);
		AttrNumber *sortColIdx;
		Oid		   *sortOperators;
		Oid		   *sortCollations;
		bool	   *sortNullsFirst;
		ListCell   *lc;
		int			i;

		if (ishypothetical)
			numSortCols++;		/* make space for flag column */
		/* these arrays are made in short-lived context */
		sortColIdx = (AttrNumber *) palloc(numSortCols * sizeof(AttrNumber));
		sortOperators = (Oid *) palloc(numSortCols * sizeof(Oid));
		sortCollations = (Oid *) palloc(numSortCols * sizeof(Oid));
		sortNullsFirst = (bool *) palloc(numSortCols * sizeof(bool));

		i = 0;
		foreach(lc, sortlist)
		{
			SortGroupClause *sortcl = (SortGroupClause *) lfirst(lc);
			TargetEntry *tle = get_sortgroupclause_tle(sortcl, aggref->args);

			/* the parser should have made sure of this */
			Assert(OidIsValid(sortcl->sortop));

			sortColIdx[i] = tle->resno;
			sortOperators[i] = sortcl->sortop;
			sortCollations[i] = exprCollation((Node *) tle->expr);
			sortNullsFirst[i] = sortcl->nulls_first;
			i++;
		}

		if (ishypothetical)
		{
			/* Add an integer flag column as the last sort column */
			sortColIdx[i] = list_length(aggref->args) + 1;
			sortOperators[i] = Int4LessOperator;
			sortCollations[i] = InvalidOid;
			sortNullsFirst[i] = false;
			i++;
		}

		Assert(i == numSortCols);

		/* Now build the stuff we need in aggregate-lifespan context */
		oldcontext = MemoryContextSwitchTo(aggcontext);

		/*
		 * Get a tupledesc corresponding to the aggregated inputs (including
		 * sort expressions) of the agg.
		 */
		osastate->tupdesc = ExecTypeFromTL(aggref->args, false);

		/* If we need a flag column, hack the tupledesc to include that */
		if (ishypothetical)
		{
			TupleDesc	newdesc;
			int			natts = osastate->tupdesc->natts;

			newdesc = CreateTemplateTupleDesc(natts + 1, false);
			for (i = 1; i <= natts; i++)
				TupleDescCopyEntry(newdesc, i, osastate->tupdesc, i);

			TupleDescInitEntry(newdesc,
							   (AttrNumber) ++natts,
							   "flag",
							   INT4OID,
							   -1,
							   0);

			FreeTupleDesc(osastate->tupdesc);
			osastate->tupdesc = newdesc;
		}

		/* Initialize tuplesort object */
		osastate->sortstate = tuplesort_begin_heap(NULL,
												   osastate->tupdesc,
												   numSortCols,
												   sortColIdx,
												   sortOperators,
												   sortCollations,
												   sortNullsFirst,
												   work_mem, false);

		/* Create slot we'll use to store/retrieve rows */
		osastate->tupslot = MakeSingleTupleTableSlot(osastate->tupdesc);
	}
	else
	{
		/* Sort single datums */
		SortGroupClause *sortcl;
		TargetEntry *tle;
		Oid			sortColType;
		Oid			sortOperator;
		Oid			sortCollation;
		Oid			eqOperator;
		bool		sortNullsFirst;

		if (numSortCols != 1 || aggref->aggkind == AGGKIND_HYPOTHETICAL)
			elog(ERROR, "ordered-set aggregate support function does not support multiple aggregated columns");

		sortcl = (SortGroupClause *) linitial(sortlist);
		tle = get_sortgroupclause_tle(sortcl, aggref->args);

		/* the parser should have made sure of this */
		Assert(OidIsValid(sortcl->sortop));

		sortColType = exprType((Node *) tle->expr);
		sortOperator = sortcl->sortop;
		eqOperator = sortcl->eqop;
		sortCollation = exprCollation((Node *) tle->expr);
		sortNullsFirst = sortcl->nulls_first;

		/* Save datatype info */
		osastate->datumtype = sortColType;
		get_typlenbyvalalign(sortColType,
							 &osastate->typLen,
							 &osastate->typByVal,
							 &osastate->typAlign);
		osastate->eqOperator = eqOperator;

		/* Now build the stuff we need in aggregate-lifespan context */
		oldcontext = MemoryContextSwitchTo(aggcontext);

		/* Initialize tuplesort object */
		osastate->sortstate = tuplesort_begin_datum(NULL,
													sortColType,
													sortOperator,
													sortCollation,
													sortNullsFirst,
													work_mem, false);
	}

	/* Now register a shutdown callback to clean it all up */
	RegisterExprContextCallback(peraggecontext,
								ordered_set_shutdown,
								PointerGetDatum(osastate));
=======
static OSAPerGroupState *
ordered_set_startup(FunctionCallInfo fcinfo, bool use_tuples)
{
	OSAPerGroupState *osastate;
	OSAPerQueryState *qstate;
	MemoryContext gcontext;
	MemoryContext oldcontext;

	/*
	 * Check we're called as aggregate (and not a window function), and get
	 * the Agg node's group-lifespan context (which might change from group to
	 * group, so we shouldn't cache it in the per-query state).
	 */
	if (AggCheckCallContext(fcinfo, &gcontext) != AGG_CONTEXT_AGGREGATE)
		elog(ERROR, "ordered-set aggregate called in non-aggregate context");

	/*
	 * We keep a link to the per-query state in fn_extra; if it's not there,
	 * create it, and do the per-query setup we need.
	 */
	qstate = (OSAPerQueryState *) fcinfo->flinfo->fn_extra;
	if (qstate == NULL)
	{
		Aggref	   *aggref;
		MemoryContext qcontext;
		List	   *sortlist;
		int			numSortCols;

		/* Get the Aggref so we can examine aggregate's arguments */
		aggref = AggGetAggref(fcinfo);
		if (!aggref)
			elog(ERROR, "ordered-set aggregate called in non-aggregate context");
		if (!AGGKIND_IS_ORDERED_SET(aggref->aggkind))
			elog(ERROR, "ordered-set aggregate support function called for non-ordered-set aggregate");

		/*
		 * Prepare per-query structures in the fn_mcxt, which we assume is the
		 * executor's per-query context; in any case it's the right place to
		 * keep anything found via fn_extra.
		 */
		qcontext = fcinfo->flinfo->fn_mcxt;
		oldcontext = MemoryContextSwitchTo(qcontext);

		qstate = (OSAPerQueryState *) palloc0(sizeof(OSAPerQueryState));
		qstate->aggref = aggref;
		qstate->qcontext = qcontext;

		/* Extract the sort information */
		sortlist = aggref->aggorder;
		numSortCols = list_length(sortlist);

		if (use_tuples)
		{
			bool		ishypothetical = (aggref->aggkind == AGGKIND_HYPOTHETICAL);
			ListCell   *lc;
			int			i;

			if (ishypothetical)
				numSortCols++;	/* make space for flag column */
			qstate->numSortCols = numSortCols;
			qstate->sortColIdx = (AttrNumber *) palloc(numSortCols * sizeof(AttrNumber));
			qstate->sortOperators = (Oid *) palloc(numSortCols * sizeof(Oid));
			qstate->eqOperators = (Oid *) palloc(numSortCols * sizeof(Oid));
			qstate->sortCollations = (Oid *) palloc(numSortCols * sizeof(Oid));
			qstate->sortNullsFirsts = (bool *) palloc(numSortCols * sizeof(bool));

			i = 0;
			foreach(lc, sortlist)
			{
				SortGroupClause *sortcl = (SortGroupClause *) lfirst(lc);
				TargetEntry *tle = get_sortgroupclause_tle(sortcl,
														   aggref->args);

				/* the parser should have made sure of this */
				Assert(OidIsValid(sortcl->sortop));

				qstate->sortColIdx[i] = tle->resno;
				qstate->sortOperators[i] = sortcl->sortop;
				qstate->eqOperators[i] = sortcl->eqop;
				qstate->sortCollations[i] = exprCollation((Node *) tle->expr);
				qstate->sortNullsFirsts[i] = sortcl->nulls_first;
				i++;
			}

			if (ishypothetical)
			{
				/* Add an integer flag column as the last sort column */
				qstate->sortColIdx[i] = list_length(aggref->args) + 1;
				qstate->sortOperators[i] = Int4LessOperator;
				qstate->eqOperators[i] = Int4EqualOperator;
				qstate->sortCollations[i] = InvalidOid;
				qstate->sortNullsFirsts[i] = false;
				i++;
			}

			Assert(i == numSortCols);

			/*
			 * Get a tupledesc corresponding to the aggregated inputs
			 * (including sort expressions) of the agg.
			 */
			qstate->tupdesc = ExecTypeFromTL(aggref->args, false);

			/* If we need a flag column, hack the tupledesc to include that */
			if (ishypothetical)
			{
				TupleDesc	newdesc;
				int			natts = qstate->tupdesc->natts;

				newdesc = CreateTemplateTupleDesc(natts + 1, false);
				for (i = 1; i <= natts; i++)
					TupleDescCopyEntry(newdesc, i, qstate->tupdesc, i);

				TupleDescInitEntry(newdesc,
								   (AttrNumber) ++natts,
								   "flag",
								   INT4OID,
								   -1,
								   0);

				FreeTupleDesc(qstate->tupdesc);
				qstate->tupdesc = newdesc;
			}

			/* Create slot we'll use to store/retrieve rows */
			qstate->tupslot = MakeSingleTupleTableSlot(qstate->tupdesc);
		}
		else
		{
			/* Sort single datums */
			SortGroupClause *sortcl;
			TargetEntry *tle;

			if (numSortCols != 1 || aggref->aggkind == AGGKIND_HYPOTHETICAL)
				elog(ERROR, "ordered-set aggregate support function does not support multiple aggregated columns");

			sortcl = (SortGroupClause *) linitial(sortlist);
			tle = get_sortgroupclause_tle(sortcl, aggref->args);

			/* the parser should have made sure of this */
			Assert(OidIsValid(sortcl->sortop));

			/* Save sort ordering info */
			qstate->sortColType = exprType((Node *) tle->expr);
			qstate->sortOperator = sortcl->sortop;
			qstate->eqOperator = sortcl->eqop;
			qstate->sortCollation = exprCollation((Node *) tle->expr);
			qstate->sortNullsFirst = sortcl->nulls_first;

			/* Save datatype info */
			get_typlenbyvalalign(qstate->sortColType,
								 &qstate->typLen,
								 &qstate->typByVal,
								 &qstate->typAlign);
		}

		fcinfo->flinfo->fn_extra = (void *) qstate;

		MemoryContextSwitchTo(oldcontext);
	}

	/* Now build the stuff we need in group-lifespan context */
	oldcontext = MemoryContextSwitchTo(gcontext);

	osastate = (OSAPerGroupState *) palloc(sizeof(OSAPerGroupState));
	osastate->qstate = qstate;
	osastate->gcontext = gcontext;

	/*
	 * Initialize tuplesort object.
	 */
	if (use_tuples)
		osastate->sortstate = tuplesort_begin_heap(qstate->tupdesc,
												   qstate->numSortCols,
												   qstate->sortColIdx,
												   qstate->sortOperators,
												   qstate->sortCollations,
												   qstate->sortNullsFirsts,
												   work_mem, false);
	else
		osastate->sortstate = tuplesort_begin_datum(qstate->sortColType,
													qstate->sortOperator,
													qstate->sortCollation,
													qstate->sortNullsFirst,
													work_mem, false);

	osastate->number_of_rows = 0;

	/* Now register a shutdown callback to clean things up at end of group */
	AggRegisterCallback(fcinfo,
						ordered_set_shutdown,
						PointerGetDatum(osastate));
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

	MemoryContextSwitchTo(oldcontext);

	return osastate;
}

/*
 * Clean up when evaluation of an ordered-set aggregate is complete.
 *
<<<<<<< HEAD
 * We don't need to bother freeing objects in the aggcontext memory context,
 * since that will get reset anyway by nodeAgg.c, but we should take care to
 * release any potential non-memory resources.
=======
 * We don't need to bother freeing objects in the per-group memory context,
 * since that will get reset anyway by nodeAgg.c; nor should we free anything
 * in the per-query context, which will get cleared (if this was the last
 * group) by ExecutorEnd.  But we must take care to release any potential
 * non-memory resources.
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
 *
 * This callback is arguably unnecessary, since we don't support use of
 * ordered-set aggs in AGG_HASHED mode and there is currently no non-error
 * code path in non-hashed modes wherein nodeAgg.c won't call the finalfn
 * after calling the transfn one or more times.  So in principle we could rely
 * on the finalfn to delete the tuplestore etc.  However, it's possible that
 * such a code path might exist in future, and in any case it'd be
 * notationally tedious and sometimes require extra data copying to ensure
 * we always delete the tuplestore in the finalfn.
 */
static void
ordered_set_shutdown(Datum arg)
{
<<<<<<< HEAD
	OrderedSetAggState *osastate = (OrderedSetAggState *) DatumGetPointer(arg);
=======
	OSAPerGroupState *osastate = (OSAPerGroupState *) DatumGetPointer(arg);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

	/* Tuplesort object might have temp files. */
	if (osastate->sortstate)
		tuplesort_end(osastate->sortstate);
	osastate->sortstate = NULL;
	/* The tupleslot probably can't be holding a pin, but let's be safe. */
<<<<<<< HEAD
	if (osastate->tupslot)
		ExecDropSingleTupleTableSlot(osastate->tupslot);
	osastate->tupslot = NULL;
=======
	if (osastate->qstate->tupslot)
		ExecClearTuple(osastate->qstate->tupslot);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
}


/*
 * Generic transition function for ordered-set aggregates
 * with a single input column in which we want to suppress nulls
 */
Datum
ordered_set_transition(PG_FUNCTION_ARGS)
{
<<<<<<< HEAD
	OrderedSetAggState *osastate;
=======
	OSAPerGroupState *osastate;
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

	/* If first call, create the transition state workspace */
	if (PG_ARGISNULL(0))
		osastate = ordered_set_startup(fcinfo, false);
	else
<<<<<<< HEAD
	{
		/* safety check */
		if (AggCheckCallContext(fcinfo, NULL) != AGG_CONTEXT_AGGREGATE)
			elog(ERROR, "ordered-set aggregate called in non-aggregate context");
		osastate = (OrderedSetAggState *) PG_GETARG_POINTER(0);
	}
=======
		osastate = (OSAPerGroupState *) PG_GETARG_POINTER(0);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

	/* Load the datum into the tuplesort object, but only if it's not null */
	if (!PG_ARGISNULL(1))
	{
		tuplesort_putdatum(osastate->sortstate, PG_GETARG_DATUM(1), false);
		osastate->number_of_rows++;
	}

	PG_RETURN_POINTER(osastate);
}

/*
 * Generic transition function for ordered-set aggregates
 * with (potentially) multiple aggregated input columns
 */
Datum
ordered_set_transition_multi(PG_FUNCTION_ARGS)
{
<<<<<<< HEAD
	OrderedSetAggState *osastate;
=======
	OSAPerGroupState *osastate;
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
	TupleTableSlot *slot;
	int			nargs;
	int			i;

	/* If first call, create the transition state workspace */
	if (PG_ARGISNULL(0))
		osastate = ordered_set_startup(fcinfo, true);
	else
<<<<<<< HEAD
	{
		/* safety check */
		if (AggCheckCallContext(fcinfo, NULL) != AGG_CONTEXT_AGGREGATE)
			elog(ERROR, "ordered-set aggregate called in non-aggregate context");
		osastate = (OrderedSetAggState *) PG_GETARG_POINTER(0);
	}

	/* Form a tuple from all the other inputs besides the transition value */
	slot = osastate->tupslot;
=======
		osastate = (OSAPerGroupState *) PG_GETARG_POINTER(0);

	/* Form a tuple from all the other inputs besides the transition value */
	slot = osastate->qstate->tupslot;
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
	ExecClearTuple(slot);
	nargs = PG_NARGS() - 1;
	for (i = 0; i < nargs; i++)
	{
<<<<<<< HEAD
		slot->PRIVATE_tts_values[i] = PG_GETARG_DATUM(i + 1);
		slot->PRIVATE_tts_isnull[i] = PG_ARGISNULL(i + 1);
	}
	if (osastate->aggref->aggkind == AGGKIND_HYPOTHETICAL)
	{
		/* Add a zero flag value to mark this row as a normal input row */
		slot->PRIVATE_tts_values[i] = Int32GetDatum(0);
		slot->PRIVATE_tts_isnull[i] = false;
=======
		slot->tts_values[i] = PG_GETARG_DATUM(i + 1);
		slot->tts_isnull[i] = PG_ARGISNULL(i + 1);
	}
	if (osastate->qstate->aggref->aggkind == AGGKIND_HYPOTHETICAL)
	{
		/* Add a zero flag value to mark this row as a normal input row */
		slot->tts_values[i] = Int32GetDatum(0);
		slot->tts_isnull[i] = false;
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
		i++;
	}
	Assert(i == slot->tts_tupleDescriptor->natts);
	ExecStoreVirtualTuple(slot);

	/* Load the row into the tuplesort object */
	tuplesort_puttupleslot(osastate->sortstate, slot);
	osastate->number_of_rows++;

	PG_RETURN_POINTER(osastate);
}


/*
 * percentile_disc(float8) within group(anyelement) - discrete percentile
 */
Datum
percentile_disc_final(PG_FUNCTION_ARGS)
{
<<<<<<< HEAD
	OrderedSetAggState *osastate;
=======
	OSAPerGroupState *osastate;
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
	double		percentile;
	Datum		val;
	bool		isnull;
	int64		rownum;

<<<<<<< HEAD
	/* safety check */
	if (AggCheckCallContext(fcinfo, NULL) != AGG_CONTEXT_AGGREGATE)
		elog(ERROR, "ordered-set aggregate called in non-aggregate context");
=======
	Assert(AggCheckCallContext(fcinfo, NULL) == AGG_CONTEXT_AGGREGATE);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

	/* Get and check the percentile argument */
	if (PG_ARGISNULL(1))
		PG_RETURN_NULL();

	percentile = PG_GETARG_FLOAT8(1);

	if (percentile < 0 || percentile > 1 || isnan(percentile))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("percentile value %g is not between 0 and 1",
						percentile)));

	/* If there were no regular rows, the result is NULL */
	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

<<<<<<< HEAD
	osastate = (OrderedSetAggState *) PG_GETARG_POINTER(0);
=======
	osastate = (OSAPerGroupState *) PG_GETARG_POINTER(0);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

	/* number_of_rows could be zero if we only saw NULL input values */
	if (osastate->number_of_rows == 0)
		PG_RETURN_NULL();

	/* Finish the sort */
	tuplesort_performsort(osastate->sortstate);

	/*----------
	 * We need the smallest K such that (K/N) >= percentile.
	 * N>0, therefore K >= N*percentile, therefore K = ceil(N*percentile).
	 * So we skip K-1 rows (if K>0) and return the next row fetched.
	 *----------
	 */
	rownum = (int64) ceil(percentile * osastate->number_of_rows);
	Assert(rownum <= osastate->number_of_rows);

	if (rownum > 1)
	{
		if (!tuplesort_skiptuples(osastate->sortstate, rownum - 1, true))
			elog(ERROR, "missing row in percentile_disc");
	}

	if (!tuplesort_getdatum(osastate->sortstate, true, &val, &isnull))
		elog(ERROR, "missing row in percentile_disc");

	/*
	 * Note: we *cannot* clean up the tuplesort object here, because the value
<<<<<<< HEAD
	 * to be returned is allocated inside its sortcontext.	We could use
=======
	 * to be returned is allocated inside its sortcontext.  We could use
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
	 * datumCopy to copy it out of there, but it doesn't seem worth the
	 * trouble, since the cleanup callback will clear the tuplesort later.
	 */

	/* We shouldn't have stored any nulls, but do the right thing anyway */
	if (isnull)
		PG_RETURN_NULL();
	else
		PG_RETURN_DATUM(val);
}


/*
 * For percentile_cont, we need a way to interpolate between consecutive
 * values. Use a helper function for that, so that we can share the rest
 * of the code between types.
 */
typedef Datum (*LerpFunc) (Datum lo, Datum hi, double pct);

static Datum
float8_lerp(Datum lo, Datum hi, double pct)
{
	double		loval = DatumGetFloat8(lo);
	double		hival = DatumGetFloat8(hi);

	return Float8GetDatum(loval + (pct * (hival - loval)));
}

static Datum
interval_lerp(Datum lo, Datum hi, double pct)
{
	Datum		diff_result = DirectFunctionCall2(interval_mi, hi, lo);
	Datum		mul_result = DirectFunctionCall2(interval_mul,
												 diff_result,
												 Float8GetDatumFast(pct));

	return DirectFunctionCall2(interval_pl, mul_result, lo);
}

<<<<<<< HEAD
static Datum
timestamp_lerp(Datum lo, Datum hi, double pct)
{
	Timestamp	lots = DatumGetTimestamp(lo);
	Timestamp	hits = DatumGetTimestamp(hi);
	Timestamp	diff_result;
	Timestamp	mul_result;

	diff_result = hits - lots;
#ifdef HAVE_INT64_TIMESTAMP
	mul_result = (Timestamp) round(diff_result * pct);
#else
	mul_result = (Timestamp) diff_result * pct;
#endif

	return TimestampGetDatum(lo + mul_result);
}

static Datum
timestamptz_lerp(Datum lo, Datum hi, double pct)
{
	TimestampTz lots = DatumGetTimestampTz(lo);
	TimestampTz hits = DatumGetTimestampTz(hi);
	TimestampTz diff_result;
	TimestampTz mul_result;

	diff_result = hits - lots;
#ifdef HAVE_INT64_TIMESTAMP
	mul_result = (TimestampTz) round(diff_result * pct);
#else
	mul_result = (TimestampTz) diff_result * pct;
#endif

	return TimestampTzGetDatum(lo + mul_result);
}

=======
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
/*
 * Continuous percentile
 */
static Datum
percentile_cont_final_common(FunctionCallInfo fcinfo,
							 Oid expect_type,
							 LerpFunc lerpfunc)
{
<<<<<<< HEAD
	OrderedSetAggState *osastate;
=======
	OSAPerGroupState *osastate;
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
	double		percentile;
	int64		first_row = 0;
	int64		second_row = 0;
	Datum		val;
	Datum		first_val;
	Datum		second_val;
	double		proportion;
	bool		isnull;

<<<<<<< HEAD
	/* safety check */
	if (AggCheckCallContext(fcinfo, NULL) != AGG_CONTEXT_AGGREGATE)
		elog(ERROR, "ordered-set aggregate called in non-aggregate context");
=======
	Assert(AggCheckCallContext(fcinfo, NULL) == AGG_CONTEXT_AGGREGATE);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

	/* Get and check the percentile argument */
	if (PG_ARGISNULL(1))
		PG_RETURN_NULL();

	percentile = PG_GETARG_FLOAT8(1);

	if (percentile < 0 || percentile > 1 || isnan(percentile))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("percentile value %g is not between 0 and 1",
						percentile)));

	/* If there were no regular rows, the result is NULL */
	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

<<<<<<< HEAD
	osastate = (OrderedSetAggState *) PG_GETARG_POINTER(0);
=======
	osastate = (OSAPerGroupState *) PG_GETARG_POINTER(0);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

	/* number_of_rows could be zero if we only saw NULL input values */
	if (osastate->number_of_rows == 0)
		PG_RETURN_NULL();

<<<<<<< HEAD
	Assert(expect_type == osastate->datumtype);
=======
	Assert(expect_type == osastate->qstate->sortColType);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

	/* Finish the sort */
	tuplesort_performsort(osastate->sortstate);

	first_row = floor(percentile * (osastate->number_of_rows - 1));
	second_row = ceil(percentile * (osastate->number_of_rows - 1));

	Assert(first_row < osastate->number_of_rows);

	if (!tuplesort_skiptuples(osastate->sortstate, first_row, true))
		elog(ERROR, "missing row in percentile_cont");

	if (!tuplesort_getdatum(osastate->sortstate, true, &first_val, &isnull))
		elog(ERROR, "missing row in percentile_cont");
	if (isnull)
		PG_RETURN_NULL();

	if (first_row == second_row)
	{
		val = first_val;
	}
	else
	{
		if (!tuplesort_getdatum(osastate->sortstate, true, &second_val, &isnull))
			elog(ERROR, "missing row in percentile_cont");

		if (isnull)
			PG_RETURN_NULL();

		proportion = (percentile * (osastate->number_of_rows - 1)) - first_row;
		val = lerpfunc(first_val, second_val, proportion);
	}

	/*
	 * Note: we *cannot* clean up the tuplesort object here, because the value
<<<<<<< HEAD
	 * to be returned may be allocated inside its sortcontext.	We could use
=======
	 * to be returned may be allocated inside its sortcontext.  We could use
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
	 * datumCopy to copy it out of there, but it doesn't seem worth the
	 * trouble, since the cleanup callback will clear the tuplesort later.
	 */

<<<<<<< HEAD
	if (isnull)
		PG_RETURN_NULL();
	else
		PG_RETURN_DATUM(val);
=======
	PG_RETURN_DATUM(val);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
}

/*
 * percentile_cont(float8) within group (float8)	- continuous percentile
 */
Datum
percentile_cont_float8_final(PG_FUNCTION_ARGS)
{
	return percentile_cont_final_common(fcinfo, FLOAT8OID, float8_lerp);
}

/*
 * percentile_cont(float8) within group (interval)	- continuous percentile
 */
Datum
percentile_cont_interval_final(PG_FUNCTION_ARGS)
{
	return percentile_cont_final_common(fcinfo, INTERVALOID, interval_lerp);
}

<<<<<<< HEAD
/*
 * percentile_cont(float8) within group (timestamp)	- continuous percentile
 */
Datum
percentile_cont_timestamp_final(PG_FUNCTION_ARGS)
{
	return percentile_cont_final_common(fcinfo, TIMESTAMPOID, timestamp_lerp);
}

/*
 * percentile_cont(float8) within group (timestamptz)	- continuous percentile
 */
Datum
percentile_cont_timestamptz_final(PG_FUNCTION_ARGS)
{
	return percentile_cont_final_common(fcinfo, TIMESTAMPTZOID, timestamptz_lerp);
}

=======
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

/*
 * Support code for handling arrays of percentiles
 *
 * Note: in each pct_info entry, second_row should be equal to or
 * exactly one more than first_row.
 */
struct pct_info
{
	int64		first_row;		/* first row to sample */
	int64		second_row;		/* possible second row to sample */
	double		proportion;		/* interpolation fraction */
	int			idx;			/* index of this item in original array */
};

/*
 * Sort comparator to sort pct_infos by first_row then second_row
 */
static int
pct_info_cmp(const void *pa, const void *pb)
{
	const struct pct_info *a = (const struct pct_info *) pa;
	const struct pct_info *b = (const struct pct_info *) pb;

	if (a->first_row != b->first_row)
		return (a->first_row < b->first_row) ? -1 : 1;
	if (a->second_row != b->second_row)
		return (a->second_row < b->second_row) ? -1 : 1;
	return 0;
}

/*
 * Construct array showing which rows to sample for percentiles.
 */
static struct pct_info *
setup_pct_info(int num_percentiles,
			   Datum *percentiles_datum,
			   bool *percentiles_null,
			   int64 rowcount,
			   bool continuous)
{
	struct pct_info *pct_info;
	int			i;

	pct_info = (struct pct_info *) palloc(num_percentiles * sizeof(struct pct_info));

	for (i = 0; i < num_percentiles; i++)
	{
		pct_info[i].idx = i;

		if (percentiles_null[i])
		{
			/* dummy entry for any NULL in array */
			pct_info[i].first_row = 0;
			pct_info[i].second_row = 0;
			pct_info[i].proportion = 0;
		}
		else
		{
			double		p = DatumGetFloat8(percentiles_datum[i]);

			if (p < 0 || p > 1 || isnan(p))
				ereport(ERROR,
						(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
						 errmsg("percentile value %g is not between 0 and 1",
								p)));

			if (continuous)
			{
				pct_info[i].first_row = 1 + floor(p * (rowcount - 1));
				pct_info[i].second_row = 1 + ceil(p * (rowcount - 1));
				pct_info[i].proportion = (p * (rowcount - 1)) - floor(p * (rowcount - 1));
			}
			else
			{
				/*----------
				 * We need the smallest K such that (K/N) >= percentile.
				 * N>0, therefore K >= N*percentile, therefore
				 * K = ceil(N*percentile); but not less than 1.
				 *----------
				 */
				int64		row = (int64) ceil(p * rowcount);

				row = Max(1, row);
				pct_info[i].first_row = row;
				pct_info[i].second_row = row;
				pct_info[i].proportion = 0;
			}
		}
	}

	/*
	 * The parameter array wasn't necessarily in sorted order, but we need to
	 * visit the rows in order, so sort by first_row/second_row.
	 */
	qsort(pct_info, num_percentiles, sizeof(struct pct_info), pct_info_cmp);

	return pct_info;
}

/*
 * percentile_disc(float8[]) within group (anyelement)	- discrete percentiles
 */
Datum
percentile_disc_multi_final(PG_FUNCTION_ARGS)
{
<<<<<<< HEAD
	OrderedSetAggState *osastate;
=======
	OSAPerGroupState *osastate;
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
	ArrayType  *param;
	Datum	   *percentiles_datum;
	bool	   *percentiles_null;
	int			num_percentiles;
	struct pct_info *pct_info;
	Datum	   *result_datum;
	bool	   *result_isnull;
	int64		rownum = 0;
	Datum		val = (Datum) 0;
	bool		isnull = true;
	int			i;

<<<<<<< HEAD
	/* safety check */
	if (AggCheckCallContext(fcinfo, NULL) != AGG_CONTEXT_AGGREGATE)
		elog(ERROR, "ordered-set aggregate called in non-aggregate context");
=======
	Assert(AggCheckCallContext(fcinfo, NULL) == AGG_CONTEXT_AGGREGATE);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

	/* If there were no regular rows, the result is NULL */
	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

<<<<<<< HEAD
	osastate = (OrderedSetAggState *) PG_GETARG_POINTER(0);
=======
	osastate = (OSAPerGroupState *) PG_GETARG_POINTER(0);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

	/* number_of_rows could be zero if we only saw NULL input values */
	if (osastate->number_of_rows == 0)
		PG_RETURN_NULL();

	/* Deconstruct the percentile-array input */
	if (PG_ARGISNULL(1))
		PG_RETURN_NULL();
	param = PG_GETARG_ARRAYTYPE_P(1);

	deconstruct_array(param, FLOAT8OID,
	/* hard-wired info on type float8 */
					  8, FLOAT8PASSBYVAL, 'd',
					  &percentiles_datum,
					  &percentiles_null,
					  &num_percentiles);

	if (num_percentiles == 0)
<<<<<<< HEAD
		PG_RETURN_POINTER(construct_empty_array(osastate->datumtype));
=======
		PG_RETURN_POINTER(construct_empty_array(osastate->qstate->sortColType));
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

	pct_info = setup_pct_info(num_percentiles,
							  percentiles_datum,
							  percentiles_null,
							  osastate->number_of_rows,
							  false);

	result_datum = (Datum *) palloc(num_percentiles * sizeof(Datum));
	result_isnull = (bool *) palloc(num_percentiles * sizeof(bool));

	/*
	 * Start by dealing with any nulls in the param array - those are sorted
	 * to the front on row=0, so set the corresponding result indexes to null
	 */
	for (i = 0; i < num_percentiles; i++)
	{
		int			idx = pct_info[i].idx;

		if (pct_info[i].first_row > 0)
			break;

		result_datum[idx] = (Datum) 0;
		result_isnull[idx] = true;
	}

	/*
	 * If there's anything left after doing the nulls, then grind the input
	 * and extract the needed values
	 */
	if (i < num_percentiles)
	{
		/* Finish the sort */
		tuplesort_performsort(osastate->sortstate);

		for (; i < num_percentiles; i++)
		{
			int64		target_row = pct_info[i].first_row;
			int			idx = pct_info[i].idx;

			/* Advance to target row, if not already there */
			if (target_row > rownum)
			{
				if (!tuplesort_skiptuples(osastate->sortstate, target_row - rownum - 1, true))
					elog(ERROR, "missing row in percentile_disc");

				if (!tuplesort_getdatum(osastate->sortstate, true, &val, &isnull))
					elog(ERROR, "missing row in percentile_disc");

				rownum = target_row;
			}

			result_datum[idx] = val;
			result_isnull[idx] = isnull;
		}
	}

	/*
	 * We could clean up the tuplesort object after forming the array, but
	 * probably not worth the trouble.
	 */

	/* We make the output array the same shape as the input */
	PG_RETURN_POINTER(construct_md_array(result_datum, result_isnull,
										 ARR_NDIM(param),
										 ARR_DIMS(param),
										 ARR_LBOUND(param),
<<<<<<< HEAD
										 osastate->datumtype,
										 osastate->typLen,
										 osastate->typByVal,
										 osastate->typAlign));
=======
										 osastate->qstate->sortColType,
										 osastate->qstate->typLen,
										 osastate->qstate->typByVal,
										 osastate->qstate->typAlign));
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
}

/*
 * percentile_cont(float8[]) within group ()	- continuous percentiles
 */
static Datum
percentile_cont_multi_final_common(FunctionCallInfo fcinfo,
								   Oid expect_type,
								   int16 typLen, bool typByVal, char typAlign,
								   LerpFunc lerpfunc)
{
<<<<<<< HEAD
	OrderedSetAggState *osastate;
=======
	OSAPerGroupState *osastate;
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
	ArrayType  *param;
	Datum	   *percentiles_datum;
	bool	   *percentiles_null;
	int			num_percentiles;
	struct pct_info *pct_info;
	Datum	   *result_datum;
	bool	   *result_isnull;
	int64		rownum = 0;
	Datum		first_val = (Datum) 0;
	Datum		second_val = (Datum) 0;
	bool		isnull;
	int			i;

<<<<<<< HEAD
	/* safety check */
	if (AggCheckCallContext(fcinfo, NULL) != AGG_CONTEXT_AGGREGATE)
		elog(ERROR, "ordered-set aggregate called in non-aggregate context");
=======
	Assert(AggCheckCallContext(fcinfo, NULL) == AGG_CONTEXT_AGGREGATE);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

	/* If there were no regular rows, the result is NULL */
	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

<<<<<<< HEAD
	osastate = (OrderedSetAggState *) PG_GETARG_POINTER(0);
=======
	osastate = (OSAPerGroupState *) PG_GETARG_POINTER(0);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

	/* number_of_rows could be zero if we only saw NULL input values */
	if (osastate->number_of_rows == 0)
		PG_RETURN_NULL();

<<<<<<< HEAD
	Assert(expect_type == osastate->datumtype);
=======
	Assert(expect_type == osastate->qstate->sortColType);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

	/* Deconstruct the percentile-array input */
	if (PG_ARGISNULL(1))
		PG_RETURN_NULL();
	param = PG_GETARG_ARRAYTYPE_P(1);

	deconstruct_array(param, FLOAT8OID,
	/* hard-wired info on type float8 */
					  8, FLOAT8PASSBYVAL, 'd',
					  &percentiles_datum,
					  &percentiles_null,
					  &num_percentiles);

	if (num_percentiles == 0)
<<<<<<< HEAD
		PG_RETURN_POINTER(construct_empty_array(osastate->datumtype));
=======
		PG_RETURN_POINTER(construct_empty_array(osastate->qstate->sortColType));
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

	pct_info = setup_pct_info(num_percentiles,
							  percentiles_datum,
							  percentiles_null,
							  osastate->number_of_rows,
							  true);

	result_datum = (Datum *) palloc(num_percentiles * sizeof(Datum));
	result_isnull = (bool *) palloc(num_percentiles * sizeof(bool));

	/*
	 * Start by dealing with any nulls in the param array - those are sorted
	 * to the front on row=0, so set the corresponding result indexes to null
	 */
	for (i = 0; i < num_percentiles; i++)
	{
		int			idx = pct_info[i].idx;

		if (pct_info[i].first_row > 0)
			break;

		result_datum[idx] = (Datum) 0;
		result_isnull[idx] = true;
	}

	/*
	 * If there's anything left after doing the nulls, then grind the input
	 * and extract the needed values
	 */
	if (i < num_percentiles)
	{
		/* Finish the sort */
		tuplesort_performsort(osastate->sortstate);

		for (; i < num_percentiles; i++)
		{
<<<<<<< HEAD
			int64		target_row = pct_info[i].first_row;
			bool		need_lerp = (pct_info[i].second_row > target_row);
			int			idx = pct_info[i].idx;

			/* Advance to first_row, if not already there */
			if (target_row > rownum)
			{
				if (!tuplesort_skiptuples(osastate->sortstate, target_row - rownum - 1, true))
=======
			int64		first_row = pct_info[i].first_row;
			int64		second_row = pct_info[i].second_row;
			int			idx = pct_info[i].idx;

			/*
			 * Advance to first_row, if not already there.  Note that we might
			 * already have rownum beyond first_row, in which case first_val
			 * is already correct.  (This occurs when interpolating between
			 * the same two input rows as for the previous percentile.)
			 */
			if (first_row > rownum)
			{
				if (!tuplesort_skiptuples(osastate->sortstate, first_row - rownum - 1, true))
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
					elog(ERROR, "missing row in percentile_cont");

				if (!tuplesort_getdatum(osastate->sortstate, true, &first_val, &isnull) || isnull)
					elog(ERROR, "missing row in percentile_cont");

<<<<<<< HEAD
				rownum = target_row;
			}
			else
			{
				/*
				 * We are already at the target row, so we must previously
				 * have read its value into second_val.
=======
				rownum = first_row;
				/* Always advance second_val to be latest input value */
				second_val = first_val;
			}
			else if (first_row == rownum)
			{
				/*
				 * We are already at the desired row, so we must previously
				 * have read its value into second_val (and perhaps first_val
				 * as well, but this assignment is harmless in that case).
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
				 */
				first_val = second_val;
			}

			/* Fetch second_row if needed */
<<<<<<< HEAD
			if (need_lerp)
=======
			if (second_row > rownum)
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
			{
				if (!tuplesort_getdatum(osastate->sortstate, true, &second_val, &isnull) || isnull)
					elog(ERROR, "missing row in percentile_cont");
				rownum++;
			}
<<<<<<< HEAD
			else
				second_val = first_val;

			/* Compute appropriate result */
			if (need_lerp)
=======
			/* We should now certainly be on second_row exactly */
			Assert(second_row == rownum);

			/* Compute appropriate result */
			if (second_row > first_row)
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
				result_datum[idx] = lerpfunc(first_val, second_val,
											 pct_info[i].proportion);
			else
				result_datum[idx] = first_val;

			result_isnull[idx] = false;
		}
	}

	/*
	 * We could clean up the tuplesort object after forming the array, but
	 * probably not worth the trouble.
	 */

	/* We make the output array the same shape as the input */
	PG_RETURN_POINTER(construct_md_array(result_datum, result_isnull,
										 ARR_NDIM(param),
										 ARR_DIMS(param), ARR_LBOUND(param),
										 expect_type,
										 typLen,
										 typByVal,
										 typAlign));
}

/*
 * percentile_cont(float8[]) within group (float8)	- continuous percentiles
 */
Datum
percentile_cont_float8_multi_final(PG_FUNCTION_ARGS)
{
	return percentile_cont_multi_final_common(fcinfo,
											  FLOAT8OID,
	/* hard-wired info on type float8 */
											  8, FLOAT8PASSBYVAL, 'd',
											  float8_lerp);
}

/*
 * percentile_cont(float8[]) within group (interval)  - continuous percentiles
 */
Datum
percentile_cont_interval_multi_final(PG_FUNCTION_ARGS)
{
	return percentile_cont_multi_final_common(fcinfo,
											  INTERVALOID,
	/* hard-wired info on type interval */
											  16, false, 'd',
											  interval_lerp);
}

<<<<<<< HEAD
/*
 * percentile_cont(float8[]) within group (timestamp)  - continuous percentiles
 */
Datum
percentile_cont_timestamp_multi_final(PG_FUNCTION_ARGS)
{
	return percentile_cont_multi_final_common(fcinfo,
											  TIMESTAMPOID,
	/* hard-wired info on type interval */
											  8, FLOAT8PASSBYVAL, 'd',
											  timestamp_lerp);
}

/*
 * percentile_cont(float8[]) within group (timestamptz)  - continuous percentiles
 */
Datum
percentile_cont_timestamptz_multi_final(PG_FUNCTION_ARGS)
{
	return percentile_cont_multi_final_common(fcinfo,
											  TIMESTAMPTZOID,
	/* hard-wired info on type interval */
											  8, FLOAT8PASSBYVAL, 'd',
											  timestamptz_lerp);
}

=======
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

/*
 * mode() within group (anyelement) - most common value
 */
Datum
mode_final(PG_FUNCTION_ARGS)
{
<<<<<<< HEAD
	OrderedSetAggState *osastate;
=======
	OSAPerGroupState *osastate;
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
	Datum		val;
	bool		isnull;
	Datum		mode_val = (Datum) 0;
	int64		mode_freq = 0;
	Datum		last_val = (Datum) 0;
	int64		last_val_freq = 0;
	bool		last_val_is_mode = false;
<<<<<<< HEAD
	FmgrInfo	equalfn;
	bool		shouldfree;

	/* safety check */
	if (AggCheckCallContext(fcinfo, NULL) != AGG_CONTEXT_AGGREGATE)
		elog(ERROR, "ordered-set aggregate called in non-aggregate context");
=======
	FmgrInfo   *equalfn;
	bool		shouldfree;

	Assert(AggCheckCallContext(fcinfo, NULL) == AGG_CONTEXT_AGGREGATE);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

	/* If there were no regular rows, the result is NULL */
	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

<<<<<<< HEAD
	osastate = (OrderedSetAggState *) PG_GETARG_POINTER(0);
=======
	osastate = (OSAPerGroupState *) PG_GETARG_POINTER(0);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

	/* number_of_rows could be zero if we only saw NULL input values */
	if (osastate->number_of_rows == 0)
		PG_RETURN_NULL();

<<<<<<< HEAD
	/* Look up the equality function for the datatype */
	fmgr_info(get_opcode(osastate->eqOperator), &equalfn);

	shouldfree = !(osastate->typByVal);
=======
	/* Look up the equality function for the datatype, if we didn't already */
	equalfn = &(osastate->qstate->equalfn);
	if (!OidIsValid(equalfn->fn_oid))
		fmgr_info_cxt(get_opcode(osastate->qstate->eqOperator), equalfn,
					  osastate->qstate->qcontext);

	shouldfree = !(osastate->qstate->typByVal);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

	/* Finish the sort */
	tuplesort_performsort(osastate->sortstate);

	/* Scan tuples and count frequencies */
	while (tuplesort_getdatum(osastate->sortstate, true, &val, &isnull))
	{
		/* we don't expect any nulls, but ignore them if found */
		if (isnull)
			continue;

		if (last_val_freq == 0)
		{
			/* first nonnull value - it's the mode for now */
			mode_val = last_val = val;
			mode_freq = last_val_freq = 1;
			last_val_is_mode = true;
		}
<<<<<<< HEAD
		else if (DatumGetBool(FunctionCall2(&equalfn, val, last_val)))
=======
		else if (DatumGetBool(FunctionCall2(equalfn, val, last_val)))
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
		{
			/* value equal to previous value, count it */
			if (last_val_is_mode)
				mode_freq++;	/* needn't maintain last_val_freq */
			else if (++last_val_freq > mode_freq)
			{
				/* last_val becomes new mode */
				if (shouldfree)
					pfree(DatumGetPointer(mode_val));
				mode_val = last_val;
				mode_freq = last_val_freq;
				last_val_is_mode = true;
			}
			if (shouldfree)
				pfree(DatumGetPointer(val));
		}
		else
		{
			/* val should replace last_val */
			if (shouldfree && !last_val_is_mode)
				pfree(DatumGetPointer(last_val));
			last_val = val;
			last_val_freq = 1;
			last_val_is_mode = false;
		}

		CHECK_FOR_INTERRUPTS();
	}

	if (shouldfree && !last_val_is_mode)
		pfree(DatumGetPointer(last_val));

	/*
	 * Note: we *cannot* clean up the tuplesort object here, because the value
<<<<<<< HEAD
	 * to be returned is allocated inside its sortcontext.	We could use
=======
	 * to be returned is allocated inside its sortcontext.  We could use
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
	 * datumCopy to copy it out of there, but it doesn't seem worth the
	 * trouble, since the cleanup callback will clear the tuplesort later.
	 */

	if (mode_freq)
		PG_RETURN_DATUM(mode_val);
	else
		PG_RETURN_NULL();
}


/*
 * Common code to sanity-check args for hypothetical-set functions. No need
 * for friendly errors, these can only happen if someone's messing up the
 * aggregate definitions. The checks are needed for security, however.
 */
static void
hypothetical_check_argtypes(FunctionCallInfo fcinfo, int nargs,
							TupleDesc tupdesc)
{
	int			i;

	/* check that we have an int4 flag column */
	if (!tupdesc ||
		(nargs + 1) != tupdesc->natts ||
		tupdesc->attrs[nargs]->atttypid != INT4OID)
		elog(ERROR, "type mismatch in hypothetical-set function");

	/* check that direct args match in type with aggregated args */
	for (i = 0; i < nargs; i++)
	{
		if (get_fn_expr_argtype(fcinfo->flinfo, i + 1) != tupdesc->attrs[i]->atttypid)
			elog(ERROR, "type mismatch in hypothetical-set function");
	}
}

/*
 * compute rank of hypothetical row
 *
 * flag should be -1 to sort hypothetical row ahead of its peers, or +1
 * to sort behind.
 * total number of regular rows is returned into *number_of_rows.
 */
static int64
hypothetical_rank_common(FunctionCallInfo fcinfo, int flag,
						 int64 *number_of_rows)
{
	int			nargs = PG_NARGS() - 1;
	int64		rank = 1;
<<<<<<< HEAD
	OrderedSetAggState *osastate;
	TupleTableSlot *slot;
	int			i;

	/* safety check */
	if (AggCheckCallContext(fcinfo, NULL) != AGG_CONTEXT_AGGREGATE)
		elog(ERROR, "ordered-set aggregate called in non-aggregate context");
=======
	OSAPerGroupState *osastate;
	TupleTableSlot *slot;
	int			i;

	Assert(AggCheckCallContext(fcinfo, NULL) == AGG_CONTEXT_AGGREGATE);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

	/* If there were no regular rows, the rank is always 1 */
	if (PG_ARGISNULL(0))
	{
		*number_of_rows = 0;
		return 1;
	}

<<<<<<< HEAD
	osastate = (OrderedSetAggState *) PG_GETARG_POINTER(0);
=======
	osastate = (OSAPerGroupState *) PG_GETARG_POINTER(0);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
	*number_of_rows = osastate->number_of_rows;

	/* Adjust nargs to be the number of direct (or aggregated) args */
	if (nargs % 2 != 0)
		elog(ERROR, "wrong number of arguments in hypothetical-set function");
	nargs /= 2;

<<<<<<< HEAD
	hypothetical_check_argtypes(fcinfo, nargs, osastate->tupdesc);

	/* insert the hypothetical row into the sort */
	slot = osastate->tupslot;
	ExecClearTuple(slot);
	for (i = 0; i < nargs; i++)
	{
		slot->PRIVATE_tts_values[i] = PG_GETARG_DATUM(i + 1);
		slot->PRIVATE_tts_isnull[i] = PG_ARGISNULL(i + 1);
	}
	slot->PRIVATE_tts_values[i] = Int32GetDatum(flag);
	slot->PRIVATE_tts_isnull[i] = false;
=======
	hypothetical_check_argtypes(fcinfo, nargs, osastate->qstate->tupdesc);

	/* insert the hypothetical row into the sort */
	slot = osastate->qstate->tupslot;
	ExecClearTuple(slot);
	for (i = 0; i < nargs; i++)
	{
		slot->tts_values[i] = PG_GETARG_DATUM(i + 1);
		slot->tts_isnull[i] = PG_ARGISNULL(i + 1);
	}
	slot->tts_values[i] = Int32GetDatum(flag);
	slot->tts_isnull[i] = false;
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
	ExecStoreVirtualTuple(slot);

	tuplesort_puttupleslot(osastate->sortstate, slot);

	/* finish the sort */
	tuplesort_performsort(osastate->sortstate);

	/* iterate till we find the hypothetical row */
	while (tuplesort_gettupleslot(osastate->sortstate, true, slot))
	{
		bool		isnull;
		Datum		d = slot_getattr(slot, nargs + 1, &isnull);

		if (!isnull && DatumGetInt32(d) != 0)
			break;

		rank++;

		CHECK_FOR_INTERRUPTS();
	}

	ExecClearTuple(slot);

	/* Might as well clean up the tuplesort object immediately */
	tuplesort_end(osastate->sortstate);
	osastate->sortstate = NULL;

	return rank;
}


/*
 * rank()  - rank of hypothetical row
 */
Datum
hypothetical_rank_final(PG_FUNCTION_ARGS)
{
	int64		rank;
	int64		rowcount;

	rank = hypothetical_rank_common(fcinfo, -1, &rowcount);

	PG_RETURN_INT64(rank);
}

/*
 * percent_rank()	- percentile rank of hypothetical row
 */
Datum
hypothetical_percent_rank_final(PG_FUNCTION_ARGS)
{
	int64		rank;
	int64		rowcount;
	double		result_val;

	rank = hypothetical_rank_common(fcinfo, -1, &rowcount);

	if (rowcount == 0)
		PG_RETURN_FLOAT8(0);

	result_val = (double) (rank - 1) / (double) (rowcount);

	PG_RETURN_FLOAT8(result_val);
}

/*
 * cume_dist()	- cumulative distribution of hypothetical row
 */
Datum
hypothetical_cume_dist_final(PG_FUNCTION_ARGS)
{
	int64		rank;
	int64		rowcount;
	double		result_val;

	rank = hypothetical_rank_common(fcinfo, 1, &rowcount);

	result_val = (double) (rank) / (double) (rowcount + 1);

	PG_RETURN_FLOAT8(result_val);
}

/*
 * dense_rank() - rank of hypothetical row without gaps in ranking
 */
Datum
hypothetical_dense_rank_final(PG_FUNCTION_ARGS)
{
	int			nargs = PG_NARGS() - 1;
	int64		rank = 1;
	int64		duplicate_count = 0;
<<<<<<< HEAD
	OrderedSetAggState *osastate;
	List	   *sortlist;
=======
	OSAPerGroupState *osastate;
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
	int			numDistinctCols;
	AttrNumber *sortColIdx;
	FmgrInfo   *equalfns;
	TupleTableSlot *slot;
	TupleTableSlot *extraslot;
	TupleTableSlot *slot2;
	MemoryContext tmpcontext;
<<<<<<< HEAD
	ListCell   *lc;
	int			i;

	/* safety check */
	if (AggCheckCallContext(fcinfo, NULL) != AGG_CONTEXT_AGGREGATE)
		elog(ERROR, "ordered-set aggregate called in non-aggregate context");
=======
	int			i;

	Assert(AggCheckCallContext(fcinfo, NULL) == AGG_CONTEXT_AGGREGATE);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

	/* If there were no regular rows, the rank is always 1 */
	if (PG_ARGISNULL(0))
		PG_RETURN_INT64(rank);

<<<<<<< HEAD
	osastate = (OrderedSetAggState *) PG_GETARG_POINTER(0);
=======
	osastate = (OSAPerGroupState *) PG_GETARG_POINTER(0);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

	/* Adjust nargs to be the number of direct (or aggregated) args */
	if (nargs % 2 != 0)
		elog(ERROR, "wrong number of arguments in hypothetical-set function");
	nargs /= 2;

<<<<<<< HEAD
	hypothetical_check_argtypes(fcinfo, nargs, osastate->tupdesc);

	/*
	 * Construct list of columns to compare for uniqueness.  We can omit the
	 * flag column since we will only compare rows with flag == 0.
	 */
	sortlist = osastate->aggref->aggorder;
	numDistinctCols = list_length(sortlist);
	sortColIdx = (AttrNumber *) palloc(numDistinctCols * sizeof(AttrNumber));
	equalfns = (FmgrInfo *) palloc(numDistinctCols * sizeof(FmgrInfo));

	i = 0;
	foreach(lc, sortlist)
	{
		SortGroupClause *sortcl = (SortGroupClause *) lfirst(lc);
		TargetEntry *tle = get_sortgroupclause_tle(sortcl,
												   osastate->aggref->args);

		sortColIdx[i] = tle->resno;
		fmgr_info(get_opcode(sortcl->eqop), &equalfns[i]);
		i++;
	}

	/* Get short-term context we can use for execTuplesMatch */
	tmpcontext = AggGetPerTupleEContext(fcinfo)->ecxt_per_tuple_memory;

	/* insert the hypothetical row into the sort */
	slot = osastate->tupslot;
	ExecClearTuple(slot);
	for (i = 0; i < nargs; i++)
	{
		slot->PRIVATE_tts_values[i] = PG_GETARG_DATUM(i + 1);
		slot->PRIVATE_tts_isnull[i] = PG_ARGISNULL(i + 1);
	}
	slot->PRIVATE_tts_values[i] = Int32GetDatum(-1);
	slot->PRIVATE_tts_isnull[i] = false;
=======
	hypothetical_check_argtypes(fcinfo, nargs, osastate->qstate->tupdesc);

	/*
	 * When comparing tuples, we can omit the flag column since we will only
	 * compare rows with flag == 0.
	 */
	numDistinctCols = osastate->qstate->numSortCols - 1;

	/* Look up the equality function(s), if we didn't already */
	equalfns = osastate->qstate->equalfns;
	if (equalfns == NULL)
	{
		MemoryContext qcontext = osastate->qstate->qcontext;

		equalfns = (FmgrInfo *)
			MemoryContextAlloc(qcontext, numDistinctCols * sizeof(FmgrInfo));
		for (i = 0; i < numDistinctCols; i++)
		{
			fmgr_info_cxt(get_opcode(osastate->qstate->eqOperators[i]),
						  &equalfns[i],
						  qcontext);
		}
		osastate->qstate->equalfns = equalfns;
	}
	sortColIdx = osastate->qstate->sortColIdx;

	/* Get short-term context we can use for execTuplesMatch */
	tmpcontext = AggGetTempMemoryContext(fcinfo);

	/* insert the hypothetical row into the sort */
	slot = osastate->qstate->tupslot;
	ExecClearTuple(slot);
	for (i = 0; i < nargs; i++)
	{
		slot->tts_values[i] = PG_GETARG_DATUM(i + 1);
		slot->tts_isnull[i] = PG_ARGISNULL(i + 1);
	}
	slot->tts_values[i] = Int32GetDatum(-1);
	slot->tts_isnull[i] = false;
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
	ExecStoreVirtualTuple(slot);

	tuplesort_puttupleslot(osastate->sortstate, slot);

	/* finish the sort */
	tuplesort_performsort(osastate->sortstate);

	/*
<<<<<<< HEAD
	 * We alternate fetching into osastate->tupslot and extraslot so that we
	 * have the previous row available for comparisons.  This is accomplished
	 * by swapping the slot pointer variables after each row.
	 */
	extraslot = MakeSingleTupleTableSlot(osastate->tupdesc);
=======
	 * We alternate fetching into tupslot and extraslot so that we have the
	 * previous row available for comparisons.  This is accomplished by
	 * swapping the slot pointer variables after each row.
	 */
	extraslot = MakeSingleTupleTableSlot(osastate->qstate->tupdesc);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
	slot2 = extraslot;

	/* iterate till we find the hypothetical row */
	while (tuplesort_gettupleslot(osastate->sortstate, true, slot))
	{
		bool		isnull;
		Datum		d = slot_getattr(slot, nargs + 1, &isnull);
		TupleTableSlot *tmpslot;

		if (!isnull && DatumGetInt32(d) != 0)
			break;

		/* count non-distinct tuples */
		if (!TupIsNull(slot2) &&
			execTuplesMatch(slot, slot2,
							numDistinctCols,
							sortColIdx,
							equalfns,
							tmpcontext))
			duplicate_count++;

		tmpslot = slot2;
		slot2 = slot;
		slot = tmpslot;

		rank++;

		CHECK_FOR_INTERRUPTS();
	}

	ExecClearTuple(slot);
	ExecClearTuple(slot2);

	ExecDropSingleTupleTableSlot(extraslot);

	/* Might as well clean up the tuplesort object immediately */
	tuplesort_end(osastate->sortstate);
	osastate->sortstate = NULL;

	rank = rank - duplicate_count;

	PG_RETURN_INT64(rank);
}
