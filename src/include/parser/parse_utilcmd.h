/*-------------------------------------------------------------------------
 *
 * parse_utilcmd.h
 *		parse analysis for utility commands
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/parser/parse_utilcmd.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PARSE_UTILCMD_H
#define PARSE_UTILCMD_H

#include "nodes/parsenodes.h"
#include "parser/analyze.h"
#include "parser/parse_node.h"


extern List *transformCreateStmt(CreateStmt *stmt, const char *queryString, bool createPartition);
extern List *transformAlterTableStmt(Oid relid, AlterTableStmt *stmt,
						const char *queryString);
extern IndexStmt *transformIndexStmt(Oid relid, IndexStmt *stmt,
				   const char *queryString);
extern void transformRuleStmt(RuleStmt *stmt, const char *queryString,
				  List **actions, Node **whereClause);
extern List *transformCreateSchemaStmt(CreateSchemaStmt *stmt);

/* gpdb specific */
extern List *transformCreateExternalStmt(CreateExternalStmt *stmt, const char *queryString);

#endif   /* PARSE_UTILCMD_H */
