/*-------------------------------------------------------------------------
 *
 * index.h
 *	  prototypes for catalog/index.c.
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/index.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef INDEX_H
#define INDEX_H

<<<<<<< HEAD
#include "access/relscan.h"     /* Relation, Snapshot */
#include "executor/tuptable.h"  /* TupTableSlot */
=======
#include "catalog/objectaddress.h"
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
#include "nodes/execnodes.h"

struct EState;                  /* #include "nodes/execnodes.h" */

#define DEFAULT_INDEX_TYPE	"btree"

/* Typedef for callback function for IndexBuildScan */
typedef void (*IndexBuildCallback) (Relation index,
									ItemPointer tupleId,
									Datum *values,
									bool *isnull,
									bool tupleIsAlive,
									void *state);

/* Action code for index_set_state_flags */
typedef enum
{
	INDEX_CREATE_SET_READY,
<<<<<<< HEAD
	INDEX_CREATE_SET_VALID
} IndexStateFlagsAction;
=======
	INDEX_CREATE_SET_VALID,
	INDEX_DROP_CLEAR_VALID,
	INDEX_DROP_SET_DEAD
} IndexStateFlagsAction;

>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8


extern bool relationHasPrimaryKey(Relation rel);
extern void index_check_primary_key(Relation heapRel,
						IndexInfo *indexInfo,
						bool is_alter_table);

extern Oid index_create(Relation heapRelation,
			 const char *indexRelationName,
			 Oid indexRelationId,
			 Oid relFileNode,
			 IndexInfo *indexInfo,
			 List *indexColNames,
			 Oid accessMethodObjectId,
			 Oid tableSpaceId,
			 Oid *collationObjectId,
			 Oid *classObjectId,
			 int16 *coloptions,
			 Datum reloptions,
			 bool isprimary,
			 bool isconstraint,
			 bool deferrable,
			 bool initdeferred,
			 bool allow_system_table_mods,
			 bool skip_build,
			 bool concurrent,
<<<<<<< HEAD
			 const char *altConName);
=======
			 bool is_internal,
			 bool if_not_exists);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

extern ObjectAddress index_constraint_create(Relation heapRelation,
						Oid indexRelationId,
						IndexInfo *indexInfo,
						const char *constraintName,
						char constraintType,
						bool deferrable,
						bool initdeferred,
						bool mark_as_primary,
						bool update_pgindex,
						bool remove_old_dependencies,
						bool allow_system_table_mods,
						bool is_internal);

extern void index_drop(Oid indexId, bool concurrent);

extern IndexInfo *BuildIndexInfo(Relation index);

extern void BuildSpeculativeIndexInfo(Relation index, IndexInfo *ii);

extern void FormIndexDatum(IndexInfo *indexInfo,
			   TupleTableSlot *slot,
			   struct EState *estate,
			   Datum *values,
			   bool *isnull);

extern Oid setNewRelfilenodeToOid(Relation relation, TransactionId freezeXid,
					   Oid newrelfilenode);

extern void index_build(Relation heapRelation,
			Relation indexRelation,
			IndexInfo *indexInfo,
			bool isprimary,
			bool isreindex);

<<<<<<< HEAD
extern double IndexBuildScan(Relation parentRelation,
					Relation indexRelation,
					IndexInfo *indexInfo,
					bool allow_sync,
					IndexBuildCallback callback,
					void *callback_state);
=======
extern double IndexBuildHeapScan(Relation heapRelation,
				   Relation indexRelation,
				   IndexInfo *indexInfo,
				   bool allow_sync,
				   IndexBuildCallback callback,
				   void *callback_state);
extern double IndexBuildHeapRangeScan(Relation heapRelation,
						Relation indexRelation,
						IndexInfo *indexInfo,
						bool allow_sync,
						BlockNumber start_blockno,
						BlockNumber end_blockno,
						IndexBuildCallback callback,
						void *callback_state);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

extern void validate_index(Oid heapId, Oid indexId, Snapshot snapshot);

extern void index_set_state_flags(Oid indexId, IndexStateFlagsAction action);

<<<<<<< HEAD
extern void reindex_index(Oid indexId, bool skip_constraint_checks);
=======
extern void reindex_index(Oid indexId, bool skip_constraint_checks,
			  char relpersistence, int options);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

/* Flag bits for reindex_relation(): */
#define REINDEX_REL_PROCESS_TOAST			0x01
#define REINDEX_REL_SUPPRESS_INDEX_USE		0x02
#define REINDEX_REL_CHECK_CONSTRAINTS		0x04
#define REINDEX_REL_FORCE_INDEXES_UNLOGGED	0x08
#define REINDEX_REL_FORCE_INDEXES_PERMANENT 0x10

extern bool reindex_relation(Oid relid, int flags, int options);

extern bool ReindexIsProcessingHeap(Oid heapOid);
extern bool ReindexIsProcessingIndex(Oid indexOid);
extern Oid	IndexGetRelation(Oid indexId, bool missing_ok);

#endif   /* INDEX_H */
