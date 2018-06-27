/*
 * xlog.h
 *
 * PostgreSQL transaction log manager
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/xlog.h
 */
#ifndef XLOG_H
#define XLOG_H

#include "access/rmgr.h"
#include "access/xlogdefs.h"
<<<<<<< HEAD
#include "catalog/gp_segment_config.h"
#include "catalog/pg_control.h"
#include "lib/stringinfo.h"
#include "storage/buf.h"
#include "utils/pg_crc.h"
#include "utils/relcache.h"
#include "utils/timestamp.h"
#include "cdb/cdbpublic.h"
#include "replication/walsender.h"
#include "datatype/timestamp.h"

/*
 * The overall layout of an XLOG record is:
 *		Fixed-size header (XLogRecord struct)
 *		rmgr-specific data
 *		BkpBlock
 *		backup block data
 *		BkpBlock
 *		backup block data
 *		...
 *
 * where there can be zero to four backup blocks (as signaled by xl_info flag
 * bits).  XLogRecord structs always start on MAXALIGN boundaries in the WAL
 * files, and we round up SizeOfXLogRecord so that the rmgr data is also
 * guaranteed to begin on a MAXALIGN boundary.	However, no padding is added
 * to align BkpBlock structs or backup block data.
 *
 * NOTE: xl_len counts only the rmgr data, not the XLogRecord header,
 * and also not any backup blocks.	xl_tot_len counts everything.  Neither
 * length field is rounded up to an alignment boundary.
 */
typedef struct XLogRecord
{
	pg_crc32	xl_crc;			/* CRC for this record */
	XLogRecPtr	xl_prev;		/* ptr to previous record in log */
	TransactionId xl_xid;		/* xact id */
	uint32		xl_tot_len;		/* total len of entire record */
	uint32		xl_len;			/* total len of rmgr data */
	uint8		xl_info;		/* flag bits, see below */
	RmgrId		xl_rmid;		/* resource manager for this record */
	uint8       xl_extended_info; /* flag bits, see below */

	/* Depending on MAXALIGN, there are either 2 or 6 wasted bytes here */

	/* ACTUAL LOG DATA FOLLOWS AT END OF STRUCT */

} XLogRecord;

#define SizeOfXLogRecord	MAXALIGN(sizeof(XLogRecord))

#define XLogRecGetData(record)	((char*) (record) + SizeOfXLogRecord)

/*
 * XLOG uses only low 4 bits of xl_info. High 4 bits may be used by rmgr.
 * XLR_CHECK_CONSISTENCY bits can be passed by XLogInsert caller.
 */
#define XLR_INFO_MASK			0x0F

/*
 * Enforces consistency checks of replayed WAL at recovery. If enabled,
 * each record will log a full-page write for each block modified by the
 * record and will reuse it afterwards for consistency checks. The caller
 * of XLogInsert can use this value if necessary, but if
 * wal_consistency_checking is enabled for a rmgr this is set unconditionally.
 */
#define XLR_CHECK_CONSISTENCY 0x02

/*
 * If we backed up any disk blocks with the XLOG record, we use flag bits in
 * xl_info to signal it.  We support backup of up to 4 disk blocks per XLOG
 * record.
 */
#define XLR_BKP_BLOCK_MASK		0x0F	/* all info bits used for bkp blocks */
#define XLR_MAX_BKP_BLOCKS		4
#define XLR_BKP_BLOCK(iblk)		(0x08 >> (iblk))		/* iblk in 0..3 */

/* These macros are deprecated and will be removed in 9.3; use XLR_BKP_BLOCK */
#define XLR_SET_BKP_BLOCK(iblk) (0x08 >> (iblk))
#define XLR_BKP_BLOCK_1			XLR_SET_BKP_BLOCK(0)	/* 0x08 */
#define XLR_BKP_BLOCK_2			XLR_SET_BKP_BLOCK(1)	/* 0x04 */
#define XLR_BKP_BLOCK_3			XLR_SET_BKP_BLOCK(2)	/* 0x02 */
#define XLR_BKP_BLOCK_4			XLR_SET_BKP_BLOCK(3)	/* 0x01 */
=======
#include "access/xloginsert.h"
#include "access/xlogreader.h"
#include "datatype/timestamp.h"
#include "lib/stringinfo.h"
#include "nodes/pg_list.h"
#include "storage/fd.h"

>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

/* Sync methods */
#define SYNC_METHOD_FSYNC		0
#define SYNC_METHOD_FDATASYNC	1
#define SYNC_METHOD_OPEN		2		/* for O_SYNC */
#define SYNC_METHOD_FSYNC_WRITETHROUGH	3
#define SYNC_METHOD_OPEN_DSYNC	4		/* for O_DSYNC */
extern int	sync_method;

<<<<<<< HEAD
/*
 * The rmgr data to be written by XLogInsert() is defined by a chain of
 * one or more XLogRecData structs.  (Multiple structs would be used when
 * parts of the source data aren't physically adjacent in memory, or when
 * multiple associated buffers need to be specified.)
 *
 * If buffer is valid then XLOG will check if buffer must be backed up
 * (ie, whether this is first change of that page since last checkpoint).
 * If so, the whole page contents are attached to the XLOG record, and XLOG
 * sets XLR_BKP_BLOCK(N) bit in xl_info.  Note that the buffer must be pinned
 * and exclusive-locked by the caller, so that it won't change under us.
 * NB: when the buffer is backed up, we DO NOT insert the data pointed to by
 * this XLogRecData struct into the XLOG record, since we assume it's present
 * in the buffer.  Therefore, rmgr redo routines MUST pay attention to
 * XLR_BKP_BLOCK(N) to know what is actually stored in the XLOG record.
 * The N'th XLR_BKP_BLOCK bit corresponds to the N'th distinct buffer
 * value (ignoring InvalidBuffer) appearing in the rdata chain.
 *
 * When buffer is valid, caller must set buffer_std to indicate whether the
 * page uses standard pd_lower/pd_upper header fields.	If this is true, then
 * XLOG is allowed to omit the free space between pd_lower and pd_upper from
 * the backed-up page image.  Note that even when buffer_std is false, the
 * page MUST have an LSN field as its first eight bytes!
 *
 * Note: data can be NULL to indicate no rmgr data associated with this chain
 * entry.  This can be sensible (ie, not a wasted entry) if buffer is valid.
 * The implication is that the buffer has been changed by the operation being
 * logged, and so may need to be backed up, but the change can be redone using
 * only information already present elsewhere in the XLOG entry.
 */
typedef struct XLogRecData
{
	char	   *data;			/* start of rmgr data to include */
	uint32		len;			/* length of rmgr data to include */
	Buffer		buffer;			/* buffer associated with data, if any */
	bool		buffer_std;		/* buffer has standard pd_lower/pd_upper */
	struct XLogRecData *next;	/* next struct in chain, or NULL */
} XLogRecData;

=======
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
extern PGDLLIMPORT TimeLineID ThisTimeLineID;	/* current TLI */

/*
 * Prior to 8.4, all activity during recovery was carried out by the startup
 * process. This local variable continues to be used in many parts of the
 * code to indicate actions taken by RecoveryManagers. Other processes that
 * potentially perform work during recovery should check RecoveryInProgress().
 * See XLogCtl notes in xlog.c.
 */
extern bool InRecovery;

/*
 * Like InRecovery, standbyState is only valid in the startup process.
 * In all other processes it will have the value STANDBY_DISABLED (so
 * InHotStandby will read as FALSE).
 *
 * In DISABLED state, we're performing crash recovery or hot standby was
 * disabled in postgresql.conf.
 *
 * In INITIALIZED state, we've run InitRecoveryTransactionEnvironment, but
 * we haven't yet processed a RUNNING_XACTS or shutdown-checkpoint WAL record
 * to initialize our master-transaction tracking system.
 *
 * When the transaction tracking is initialized, we enter the SNAPSHOT_PENDING
 * state. The tracked information might still be incomplete, so we can't allow
 * connections yet, but redo functions must update the in-memory state when
 * appropriate.
 *
 * In SNAPSHOT_READY mode, we have full knowledge of transactions that are
 * (or were) running in the master at the current WAL location. Snapshots
 * can be taken, and read-only queries can be run.
 */
typedef enum
{
	STANDBY_DISABLED,
	STANDBY_INITIALIZED,
	STANDBY_SNAPSHOT_PENDING,
	STANDBY_SNAPSHOT_READY
} HotStandbyState;

extern HotStandbyState standbyState;

#define InHotStandby (standbyState >= STANDBY_SNAPSHOT_PENDING)

/*
 * Recovery target type.
 * Only set during a Point in Time recovery, not when standby_mode = on
 */
typedef enum
{
	RECOVERY_TARGET_UNSET,
	RECOVERY_TARGET_XID,
	RECOVERY_TARGET_TIME,
	RECOVERY_TARGET_NAME,
	RECOVERY_TARGET_IMMEDIATE
} RecoveryTargetType;

extern XLogRecPtr XactLastRecEnd;
extern PGDLLIMPORT XLogRecPtr XactLastCommitEnd;

extern bool reachedConsistency;

/* these variables are GUC parameters related to XLOG */
extern int	min_wal_size;
extern int	max_wal_size;
extern int	wal_keep_segments;
extern int	XLOGbuffers;
extern int	XLogArchiveTimeout;
extern int	wal_retrieve_retry_interval;
extern char *XLogArchiveCommand;
extern bool EnableHotStandby;
extern bool gp_keep_all_xlog;
extern int keep_wal_segments;

extern bool *wal_consistency_checking;
extern char *wal_consistency_checking_string;

extern bool fullPageWrites;
extern bool wal_log_hints;
extern bool wal_compression;
extern bool log_checkpoints;

extern int	CheckPointSegments;

/* Archive modes */
typedef enum ArchiveMode
{
	ARCHIVE_MODE_OFF = 0,		/* disabled */
	ARCHIVE_MODE_ON,			/* enabled while server is running normally */
	ARCHIVE_MODE_ALWAYS			/* enabled always (even during recovery) */
} ArchiveMode;
extern int	XLogArchiveMode;

/* WAL levels */
typedef enum WalLevel
{
	WAL_LEVEL_MINIMAL = 0,
	WAL_LEVEL_ARCHIVE,
	WAL_LEVEL_HOT_STANDBY,
	WAL_LEVEL_LOGICAL
} WalLevel;
extern int	wal_level;

/* Is WAL archiving enabled (always or only while server is running normally)? */
#define XLogArchivingActive() \
	(XLogArchiveMode > ARCHIVE_MODE_OFF && wal_level >= WAL_LEVEL_ARCHIVE)
/* Is WAL archiving enabled always (even during recovery)? */
#define XLogArchivingAlways() \
	(XLogArchiveMode == ARCHIVE_MODE_ALWAYS && wal_level >= WAL_LEVEL_ARCHIVE)
#define XLogArchiveCommandSet() (XLogArchiveCommand[0] != '\0')

/*
 * Is WAL-logging necessary for archival or log-shipping, or can we skip
 * WAL-logging if we fsync() the data before committing instead?
 */
#define XLogIsNeeded() (wal_level >= WAL_LEVEL_ARCHIVE)

/*
 * Is a full-page image needed for hint bit updates?
 *
 * Normally, we don't WAL-log hint bit updates, but if checksums are enabled,
 * we have to protect them against torn page writes.  When you only set
 * individual bits on a page, it's still consistent no matter what combination
 * of the bits make it to disk, but the checksum wouldn't match.  Also WAL-log
 * them if forced by wal_log_hints=on.
 */
#define XLogHintBitIsNeeded() (DataChecksumsEnabled() || wal_log_hints)

/* Do we need to WAL-log information required only for Hot Standby and logical replication? */
#define XLogStandbyInfoActive() (wal_level >= WAL_LEVEL_HOT_STANDBY)

<<<<<<< HEAD
extern bool am_startup;
=======
/* Do we need to WAL-log information required only for logical replication? */
#define XLogLogicalInfoActive() (wal_level >= WAL_LEVEL_LOGICAL)
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

#ifdef WAL_DEBUG
extern bool XLOG_DEBUG;
#endif

/*
 * OR-able request flag bits for checkpoints.  The "cause" bits are used only
 * for logging purposes.  Note: the flags must be defined so that it's
 * sensible to OR together request flags arising from different requestors.
 */

/* These directly affect the behavior of CreateCheckPoint and subsidiaries */
#define CHECKPOINT_IS_SHUTDOWN	0x0001	/* Checkpoint is for shutdown */
#define CHECKPOINT_END_OF_RECOVERY	0x0002		/* Like shutdown checkpoint,
												 * but issued at end of WAL
												 * recovery */
#define CHECKPOINT_IMMEDIATE	0x0004	/* Do it without delays */
#define CHECKPOINT_FORCE		0x0008	/* Force even if no activity */
#define CHECKPOINT_FLUSH_ALL	0x0010	/* Flush all pages, including those
										 * belonging to unlogged tables */
/* These are important to RequestCheckpoint */
#define CHECKPOINT_WAIT			0x0020	/* Wait for completion */
/* These indicate the cause of a checkpoint request */
#define CHECKPOINT_CAUSE_XLOG	0x0040	/* XLOG consumption */
#define CHECKPOINT_CAUSE_TIME	0x0080	/* Elapsed time */

/* Checkpoint statistics */
typedef struct CheckpointStatsData
{
	TimestampTz ckpt_start_t;	/* start of checkpoint */
	TimestampTz ckpt_write_t;	/* start of flushing buffers */
	TimestampTz ckpt_sync_t;	/* start of fsyncs */
	TimestampTz ckpt_sync_end_t;	/* end of fsyncs */
	TimestampTz ckpt_end_t;		/* end of checkpoint */

	int			ckpt_bufs_written;		/* # of buffers written */

	int			ckpt_segs_added;	/* # of new xlog segments created */
	int			ckpt_segs_removed;		/* # of xlog segments deleted */
	int			ckpt_segs_recycled;		/* # of xlog segments recycled */

	int			ckpt_sync_rels; /* # of relations synced */
	uint64		ckpt_longest_sync;		/* Longest sync for one relation */
	uint64		ckpt_agg_sync_time;		/* The sum of all the individual sync
										 * times, which is not necessarily the
										 * same as the total elapsed time for
										 * the entire sync phase. */
} CheckpointStatsData;

extern CheckpointStatsData CheckpointStats;

<<<<<<< HEAD
/* File path names (all relative to $PGDATA) */
#define RECOVERY_COMMAND_FILE	"recovery.conf"
#define RECOVERY_COMMAND_DONE	"recovery.done"
#define PROMOTE_SIGNAL_FILE "promote"

extern XLogRecPtr XLogInsert(RmgrId rmid, uint8 info, XLogRecData *rdata);
extern XLogRecPtr XLogInsert_OverrideXid(RmgrId rmid, uint8 info, XLogRecData *rdata, TransactionId overrideXid);
extern XLogRecPtr XLogLastInsertBeginLoc(void);
=======
struct XLogRecData;

extern XLogRecPtr XLogInsertRecord(struct XLogRecData *rdata, XLogRecPtr fpw_lsn);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
extern void XLogFlush(XLogRecPtr RecPtr);
extern bool XLogBackgroundFlush(void);
extern bool XLogNeedsFlush(XLogRecPtr RecPtr);
extern int	XLogFileInit(XLogSegNo segno, bool *use_existent, bool use_lock);
extern int	XLogFileOpen(XLogSegNo segno);

<<<<<<< HEAD
extern void XLogGetLastRemoved(uint32 *log, uint32 *seg);
extern void XLogSetAsyncXactLSN(XLogRecPtr record);
extern XLogRecPtr XLogSaveBufferForHint(Buffer buffer);

extern Buffer RestoreBackupBlock(XLogRecPtr lsn, XLogRecord *record,
				   int block_index,
				   bool get_cleanup_lock, bool keep_buffer);

extern void xlog_redo(XLogRecPtr beginLoc __attribute__((unused)), XLogRecPtr lsn __attribute__((unused)), XLogRecord *record);
extern void xlog_desc(StringInfo buf, XLogRecPtr beginLoc, XLogRecord *record);

extern void issue_xlog_fsync(int fd, uint32 log, uint32 seg);
=======
extern void CheckXLogRemoved(XLogSegNo segno, TimeLineID tli);
extern XLogSegNo XLogGetLastRemovedSegno(void);
extern void XLogSetAsyncXactLSN(XLogRecPtr record);
extern void XLogSetReplicationSlotMinimumLSN(XLogRecPtr lsn);

extern void xlog_redo(XLogReaderState *record);
extern void xlog_desc(StringInfo buf, XLogReaderState *record);
extern const char *xlog_identify(uint8 info);

extern void issue_xlog_fsync(int fd, XLogSegNo segno);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8


extern bool RecoveryInProgress(void);
extern bool HotStandbyActive(void);
extern bool HotStandbyActiveInReplay(void);
extern bool XLogInsertAllowed(void);
extern void GetXLogReceiptTime(TimestampTz *rtime, bool *fromStream);
<<<<<<< HEAD

extern XLogRecPtr GetXLogReplayRecPtr(TimeLineID *targetTLI);
extern XLogRecPtr GetStandbyFlushRecPtr(TimeLineID *targetTLI);
=======
extern XLogRecPtr GetXLogReplayRecPtr(TimeLineID *replayTLI);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
extern XLogRecPtr GetXLogInsertRecPtr(void);
extern XLogRecPtr GetXLogWriteRecPtr(void);
extern bool RecoveryIsPaused(void);
extern void SetRecoveryPause(bool recoveryPause);
extern TimestampTz GetLatestXTime(void);
extern TimestampTz GetCurrentChunkReplayStartTime(void);
extern char *XLogFileNameP(TimeLineID tli, XLogSegNo segno);

extern void UpdateControlFile(void);
extern uint64 GetSystemIdentifier(void);
extern bool DataChecksumsEnabled(void);
<<<<<<< HEAD
=======
extern XLogRecPtr GetFakeLSNForUnloggedRel(void);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
extern Size XLOGShmemSize(void);
extern void XLOGShmemInit(void);
extern void BootStrapXLOG(void);
extern void StartupXLOG(void);
extern bool XLogStartupMultipleRecoveryPassesNeeded(void);
extern bool XLogStartupIntegrityCheckNeeded(void);
extern void ShutdownXLOG(int code, Datum arg);
extern void InitXLOGAccess(void);
extern void CreateCheckPoint(int flags);
extern bool CreateRestartPoint(int flags);
extern void XLogPutNextOid(Oid nextOid);
extern void XLogPutNextRelfilenode(Oid nextRelfilenode);
extern XLogRecPtr XLogRestorePoint(const char *rpName);
extern void UpdateFullPageWrites(void);
extern void GetFullPageWriteInfo(XLogRecPtr *RedoRecPtr_p, bool *doPageWrites_p);
extern XLogRecPtr GetRedoRecPtr(void);
extern XLogRecPtr GetInsertRecPtr(void);
extern XLogRecPtr GetFlushRecPtr(void);
extern void GetNextXidAndEpoch(TransactionId *xid, uint32 *epoch);

extern void XLogGetRecoveryStart(char *callerStr, char *reasonStr, XLogRecPtr *redoCheckPointLoc, CheckPoint *redoCheckPoint);
extern char *XLogLocationToString(XLogRecPtr *loc);
extern char *XLogLocationToString2(XLogRecPtr *loc);
extern char *XLogLocationToString3(XLogRecPtr *loc);
extern char *XLogLocationToString4(XLogRecPtr *loc);
extern char *XLogLocationToString5(XLogRecPtr *loc);
extern char *XLogLocationToString_Long(XLogRecPtr *loc);
extern char *XLogLocationToString2_Long(XLogRecPtr *loc);
extern char *XLogLocationToString3_Long(XLogRecPtr *loc);
extern char *XLogLocationToString4_Long(XLogRecPtr *loc);
extern char *XLogLocationToString5_Long(XLogRecPtr *loc);

extern void HandleStartupProcInterrupts(void);
extern void StartupProcessMain(void);
extern bool CheckPromoteSignal(bool do_unlink);
extern void WakeupRecovery(void);
extern void SetWalWriterSleeping(bool sleeping);

extern void assign_max_wal_size(int newval, void *extra);
extern void assign_checkpoint_completion_target(double newval, void *extra);

/*
 * Starting/stopping a base backup
 */
extern XLogRecPtr do_pg_start_backup(const char *backupidstr, bool fast,
				   TimeLineID *starttli_p, char **labelfile, DIR *tblspcdir,
				   List **tablespaces, char **tblspcmapfile, bool infotbssize,
				   bool needtblspcmapfile);
extern XLogRecPtr do_pg_stop_backup(char *labelfile, bool waitforarchive,
				  TimeLineID *stoptli_p);
extern void do_pg_abort_backup(void);

/* File path names (all relative to $PGDATA) */
#define BACKUP_LABEL_FILE		"backup_label"
#define BACKUP_LABEL_OLD		"backup_label.old"

<<<<<<< HEAD
extern void xlog_print_redo_lsn_application(
		RelFileNode		*rnode,
		BlockNumber 	blkno,
		void			*page,
		XLogRecPtr		lsn,
		const char		*funcName);

extern XLogRecord *XLogReadRecord(XLogRecPtr *RecPtr, int emode, bool fetching_ckpt);

extern void XLogCloseReadRecord(void);

extern void XLogReadRecoveryCommandFile(int emode);

extern List *XLogReadTimeLineHistory(TimeLineID targetTLI);

extern TimeLineID GetRecoveryTargetTLI(void);

extern bool IsStandbyMode(void);
extern DBState GetCurrentDBState(void);
extern bool IsRoleMirror(void);

extern bool IsBkpBlockApplied(XLogRecord *record, uint8 block_id);

extern XLogRecPtr
last_xlog_replay_location(void);
=======
#define TABLESPACE_MAP			"tablespace_map"
#define TABLESPACE_MAP_OLD		"tablespace_map.old"
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

#endif   /* XLOG_H */
