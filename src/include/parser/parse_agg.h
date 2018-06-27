/*-------------------------------------------------------------------------
 *
 * parse_agg.h
 *	  handle aggregates and window functions in parser
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/parser/parse_agg.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PARSE_AGG_H
#define PARSE_AGG_H

#include "parser/parse_node.h"

extern void transformAggregateCall(ParseState *pstate, Aggref *agg,
					   List *args, List *aggorder,
					   bool agg_distinct);

extern Node *transformGroupingFunc(ParseState *pstate, GroupingFunc *g);

extern void transformWindowFuncCall(ParseState *pstate, WindowFunc *wfunc,
						WindowDef *windef);

extern void parseCheckAggregates(ParseState *pstate, Query *qry);

<<<<<<< HEAD
=======
extern List *expand_grouping_sets(List *groupingSets, int limit);

>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
extern int	get_aggregate_argtypes(Aggref *aggref, Oid *inputTypes);

extern Oid resolve_aggregate_transtype(Oid aggfuncid,
							Oid aggtranstype,
							Oid *inputTypes,
							int numArguments);

extern void build_aggregate_fnexprs(Oid *agg_input_types,
						int agg_num_inputs,
						int agg_num_direct_inputs,
						int num_finalfn_inputs,
						bool agg_variadic,
						Oid agg_state_type,
						Oid agg_result_type,
						Oid agg_input_collation,
						Oid transfn_oid,
						Oid invtransfn_oid,
						Oid finalfn_oid,
						Oid prelimfn_oid,
						Oid invtransfn_oid,
						Oid invprelimfn_oid,
						Expr **transfnexpr,
<<<<<<< HEAD
						Expr **finalfnexpr,
						Expr **prelimfnexpr,
						Expr **invtransfnexpr,
						Expr **invprelimfnexpr);

extern bool checkExprHasGroupExtFuncs(Node *node);
=======
						Expr **invtransfnexpr,
						Expr **finalfnexpr);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

#endif   /* PARSE_AGG_H */
