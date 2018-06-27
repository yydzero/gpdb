/*-------------------------------------------------------------------------
 *
 * prep.h
 *	  prototypes for files in optimizer/prep/
 *
 *
<<<<<<< HEAD
 * Portions Copyright (c) 2006-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
=======
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/optimizer/prep.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PREP_H
#define PREP_H

#include "nodes/plannodes.h"
#include "nodes/relation.h"


/*
 * prototypes for prepjointree.c
 */
extern void pull_up_sublinks(PlannerInfo *root);
extern void inline_set_returning_functions(PlannerInfo *root);
<<<<<<< HEAD
extern void pull_up_sublinks(PlannerInfo *root);
extern Node *pull_up_subqueries(PlannerInfo *root, Node *jtnode,
				   JoinExpr *lowest_outer_join,
				   AppendRelInfo *containing_appendrel);
=======
extern void pull_up_subqueries(PlannerInfo *root);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
extern void flatten_simple_union_all(PlannerInfo *root);
extern void reduce_outer_joins(PlannerInfo *root);
extern Relids get_relids_in_jointree(Node *jtnode, bool include_joins);
extern Relids get_relids_for_join(PlannerInfo *root, int joinrelid);

extern List *init_list_cteplaninfo(int numCtes);

/*
 * prototypes for prepqual.c
 */
extern Node *negate_clause(Node *node);
extern Expr *canonicalize_qual(Expr *qual);

/*
 * prototypes for prepsecurity.c
 */
extern void expand_security_quals(PlannerInfo *root, List *tlist);

/*
 * prototypes for preptlist.c
 */
extern List *preprocess_targetlist(PlannerInfo *root, List *tlist);

extern List *preprocess_onconflict_targetlist(List *tlist,
								 int result_relation, List *range_table);

extern PlanRowMark *get_plan_rowmark(List *rowmarks, Index rtindex);

/*
 * prototypes for prepunion.c
 */
extern Plan *plan_set_operations(PlannerInfo *root, double tuple_fraction,
					List **sortClauses);

extern void expand_inherited_tables(PlannerInfo *root);

extern Node *adjust_appendrel_attrs(PlannerInfo *root, Node *node,
					   AppendRelInfo *appinfo);

extern Node *adjust_appendrel_attrs_multilevel(PlannerInfo *root, Node *node,
								  RelOptInfo *child_rel);

#endif   /* PREP_H */
