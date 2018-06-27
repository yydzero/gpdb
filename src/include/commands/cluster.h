/*-------------------------------------------------------------------------
 *
 * cluster.h
 *	  header file for postgres cluster command stuff
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994-5, Regents of the University of California
 *
 * src/include/commands/cluster.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_H
#define CLUSTER_H

#include "nodes/parsenodes.h"
#include "storage/lock.h"
#include "utils/relcache.h"


extern void cluster(ClusterStmt *stmt, bool isTopLevel);
<<<<<<< HEAD
extern bool cluster_rel(Oid tableOid, Oid indexOid, bool recheck,
			bool verbose, bool printError, int freeze_min_age, int freeze_table_age);
=======
extern void cluster_rel(Oid tableOid, Oid indexOid, bool recheck,
			bool verbose);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
extern void check_index_is_clusterable(Relation OldHeap, Oid indexOid,
						   bool recheck, LOCKMODE lockmode);
extern void mark_index_clustered(Relation rel, Oid indexOid, bool is_internal);

<<<<<<< HEAD
extern Oid	make_new_heap(Oid OIDOldHeap, Oid NewTableSpace,
			  bool createAoBlockDirectory);
=======
extern Oid make_new_heap(Oid OIDOldHeap, Oid NewTableSpace, char relpersistence,
			  LOCKMODE lockmode);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
extern void finish_heap_swap(Oid OIDOldHeap, Oid OIDNewHeap,
				 bool is_system_catalog,
				 bool swap_toast_by_content,
				 bool swap_stats,
				 bool check_constraints,
				 bool is_internal,
				 TransactionId frozenXid,
				 MultiXactId minMulti,
				 char newrelpersistence);

extern void swap_relation_files(Oid r1, Oid r2, bool target_is_pg_class,
					bool swap_toast_by_content,
					bool swap_stats,
					TransactionId frozenXid,
					Oid *mapped_tables);

#endif   /* CLUSTER_H */
