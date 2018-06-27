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


<<<<<<< HEAD
extern List *transformCreateStmt(CreateStmt *stmt, const char *queryString, bool createPartition);
extern List *transformCreateExternalStmt(CreateExternalStmt *stmt, const char *queryString);
extern List *transformAlterTableStmt(AlterTableStmt *stmt,
						const char *queryString);
extern List *transformIndexStmt(IndexStmt *stmt, const char *queryString);
=======
extern List *transformCreateStmt(CreateStmt *stmt, const char *queryString);
extern List *transformAlterTableStmt(Oid relid, AlterTableStmt *stmt,
						const char *queryString);
extern IndexStmt *transformIndexStmt(Oid relid, IndexStmt *stmt,
				   const char *queryString);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
extern void transformRuleStmt(RuleStmt *stmt, const char *queryString,
				  List **actions, Node **whereClause);
extern List *transformCreateSchemaStmt(CreateSchemaStmt *stmt);

#endif   /* PARSE_UTILCMD_H */
