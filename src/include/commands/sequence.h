/*-------------------------------------------------------------------------
 *
 * sequence.h
 *	  prototypes for sequence.c.
 *
<<<<<<< HEAD
 * Portions Copyright (c) 2006-2008, Greenplum inc.
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
=======
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/sequence.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SEQUENCE_H
#define SEQUENCE_H

#include "access/xlogreader.h"
#include "catalog/objectaddress.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "nodes/parsenodes.h"
#include "storage/relfilenode.h"


typedef struct FormData_pg_sequence
{
	NameData	sequence_name;
	int64		last_value;
	int64		start_value;
	int64		increment_by;
	int64		max_value;
	int64		min_value;
	int64		cache_value;
	int64		log_cnt;
	bool		is_cycled;
	bool		is_called;
} FormData_pg_sequence;

typedef FormData_pg_sequence *Form_pg_sequence;

/*
 * Columns of a sequence relation
 */

#define SEQ_COL_NAME			1
#define SEQ_COL_LASTVAL			2
#define SEQ_COL_STARTVAL		3
#define SEQ_COL_INCBY			4
#define SEQ_COL_MAXVALUE		5
#define SEQ_COL_MINVALUE		6
#define SEQ_COL_CACHE			7
#define SEQ_COL_LOG				8
#define SEQ_COL_CYCLE			9
#define SEQ_COL_CALLED			10

#define SEQ_COL_FIRSTCOL		SEQ_COL_NAME
#define SEQ_COL_LASTCOL			SEQ_COL_CALLED

/* XLOG stuff */
#define XLOG_SEQ_LOG			0x00

typedef struct xl_seq_rec
{
	RelFileNode 	node;

	/* SEQUENCE TUPLE DATA FOLLOWS AT THE END */
} xl_seq_rec;

extern Datum nextval(PG_FUNCTION_ARGS);
extern Datum nextval_oid(PG_FUNCTION_ARGS);
extern Datum currval_oid(PG_FUNCTION_ARGS);
extern Datum setval_oid(PG_FUNCTION_ARGS);
extern Datum setval3_oid(PG_FUNCTION_ARGS);
extern Datum lastval(PG_FUNCTION_ARGS);

extern Datum pg_sequence_parameters(PG_FUNCTION_ARGS);

extern ObjectAddress DefineSequence(CreateSeqStmt *stmt);
extern ObjectAddress AlterSequence(AlterSeqStmt *stmt);
extern void ResetSequence(Oid seq_relid);
extern void ResetSequenceCaches(void);

<<<<<<< HEAD
extern void seq_redo(XLogRecPtr beginLoc, XLogRecPtr lsn, XLogRecord *rptr);
extern void seq_desc(StringInfo buf, XLogRecPtr beginLoc, XLogRecord *record);

/*
 * CDB: nextval entry point called by sequence server
 */
void
cdb_sequence_nextval_server(Oid    tablespaceid,
                            Oid    dbid,
                            Oid    relid,
                            bool   istemp,
                            int64 *plast,
                            int64 *pcached,
                            int64 *pincrement,
                            bool  *poverflow);

extern void seq_mask(char *pagedata, BlockNumber blkno);
=======
extern void seq_redo(XLogReaderState *rptr);
extern void seq_desc(StringInfo buf, XLogReaderState *rptr);
extern const char *seq_identify(uint8 info);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

#endif   /* SEQUENCE_H */
