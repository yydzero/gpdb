/*-------------------------------------------------------------------------
 *
 * proc.c
 *	  routines to manage per-process shared memory data structure
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
 *
 * IDENTIFICATION
 *	  src/backend/storage/lmgr/proc.c
 *
 *-------------------------------------------------------------------------
 */
/*
 * Interface (a):
 *		ProcSleep(), ProcWakeup(),
 *		ProcQueueAlloc() -- create a shm queue for sleeping processes
 *		ProcQueueInit() -- create a queue without allocing memory
 *
 * Waiting for a lock causes the backend to be put to sleep.  Whoever releases
 * the lock wakes the process up again (and gives it an error code so it knows
 * whether it was awoken on an error condition).
 *
 * Interface (b):
 *
 * ProcReleaseLocks -- frees the locks associated with current transaction
 *
 * ProcKill -- destroys the shared memory state (and locks)
 * associated with the process.
 */
#include "postgres.h"

#include <signal.h>
#include <unistd.h>
#include <sys/time.h>

#include "access/transam.h"
#include "access/twophase.h"
#include "access/xact.h"
#include "catalog/namespace.h" /* TempNamespaceOidIsValid */
#include "commands/async.h"
#include "miscadmin.h"
#include "postmaster/autovacuum.h"
<<<<<<< HEAD
#include "postmaster/fts.h"
=======
#include "replication/slot.h"
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
#include "replication/syncrep.h"
#include "replication/walsender.h"
#include "storage/ipc.h"
#include "storage/spin.h"
#include "storage/sinval.h"
#include "storage/lmgr.h"
#include "storage/pmsignal.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/procsignal.h"
#include "storage/spin.h"
#include "utils/timeout.h"
#include "utils/timestamp.h"

#include "utils/sharedsnapshot.h"  /*SharedLocalSnapshotSlot*/

#include "cdb/cdblocaldistribxact.h"
#include "cdb/cdbtm.h"
#include "cdb/cdbvars.h"  /*Gp_is_writer*/
#include "port/atomics.h"
#include "utils/session_state.h"
#include "tcop/idle_resource_cleaner.h"
#include "utils/resscheduler.h"

/* GUC variables */
int			DeadlockTimeout = 1000;
int			StatementTimeout = 0;
int			LockTimeout = 0;
bool		log_lock_waits = false;

/* Pointer to this process's PGPROC and PGXACT structs, if any */
PGPROC	   *MyProc = NULL;
PGXACT	   *MyPgXact = NULL;

/* Special for MPP reader gangs */
PGPROC	   *lockHolderProcPtr = NULL;

/*
 * This spinlock protects the freelist of recycled PGPROC structures.
 * We cannot use an LWLock because the LWLock manager depends on already
 * having a PGPROC and a wait semaphore!  But these structures are touched
 * relatively infrequently (only at backend startup or shutdown) and not for
 * very long, so a spinlock is okay.
 */
NON_EXEC_STATIC slock_t *ProcStructLock = NULL;

/* Pointers to shared-memory structures */
PROC_HDR   *ProcGlobal = NULL;
NON_EXEC_STATIC PGPROC *AuxiliaryProcs = NULL;
PGPROC	   *PreparedXactProcs = NULL;

/* If we are waiting for a lock, this points to the associated LOCALLOCK */
static LOCALLOCK *lockAwaited = NULL;

<<<<<<< HEAD
/* Mark these volatile because they can be changed by signal handler */
static volatile bool standby_timeout_active = false;
static volatile bool statement_timeout_active = false;
static volatile bool deadlock_timeout_active = false;
static volatile DeadLockState deadlock_state = DS_NOT_YET_CHECKED;
static volatile sig_atomic_t clientWaitTimeoutInterruptEnabled = 0;
static volatile sig_atomic_t clientWaitTimeoutInterruptOccurred = 0;
volatile bool cancel_from_timeout = false;

/* timeout_start_time is set when log_lock_waits is true */
static TimestampTz timeout_start_time;

/* statement_fin_time is valid only if statement_timeout_active is true */
static TimestampTz statement_fin_time;
static TimestampTz statement_fin_time2; /* valid only in recovery */

static void RemoveProcFromArray(int code, Datum arg);
static void ProcKill(int code, Datum arg);
static void AuxiliaryProcKill(int code, Datum arg);
static bool CheckStatementTimeout(void);
static bool CheckStandbyTimeout(void);
static void ClientWaitTimeoutInterruptHandler(void);
static void ProcessClientWaitTimeout(void);
=======
static DeadLockState deadlock_state = DS_NOT_YET_CHECKED;

/* Is a deadlock check pending? */
static volatile sig_atomic_t got_deadlock_timeout;

static void RemoveProcFromArray(int code, Datum arg);
static void ProcKill(int code, Datum arg);
static void AuxiliaryProcKill(int code, Datum arg);
static void CheckDeadLock(void);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8


/*
 * Report shared-memory space needed by InitProcGlobal.
 */
Size
ProcGlobalShmemSize(void)
{
	Size		size = 0;

	/* ProcGlobal */
	size = add_size(size, sizeof(PROC_HDR));
	/* MyProcs, including autovacuum workers and launcher */
	size = add_size(size, mul_size(MaxBackends, sizeof(PGPROC)));
	/* AuxiliaryProcs */
	size = add_size(size, mul_size(NUM_AUXILIARY_PROCS, sizeof(PGPROC)));
	/* Prepared xacts */
	size = add_size(size, mul_size(max_prepared_xacts, sizeof(PGPROC)));
	/* ProcStructLock */
	size = add_size(size, sizeof(slock_t));

	size = add_size(size, mul_size(MaxBackends, sizeof(PGXACT)));
	size = add_size(size, mul_size(NUM_AUXILIARY_PROCS, sizeof(PGXACT)));
	size = add_size(size, mul_size(max_prepared_xacts, sizeof(PGXACT)));

	return size;
}

/*
 * Report number of semaphores needed by InitProcGlobal.
 */
int
ProcGlobalSemas(void)
{
	/*
	 * We need a sema per backend (including autovacuum), plus one for each
	 * auxiliary process.
	 */
	return MaxBackends + NUM_AUXILIARY_PROCS;
}

/*
 * InitProcGlobal -
 *	  Initialize the global process table during postmaster or standalone
 *	  backend startup.
 *
 *	  We also create all the per-process semaphores we will need to support
 *	  the requested number of backends.  We used to allocate semaphores
 *	  only when backends were actually started up, but that is bad because
 *	  it lets Postgres fail under load --- a lot of Unix systems are
 *	  (mis)configured with small limits on the number of semaphores, and
 *	  running out when trying to start another backend is a common failure.
 *	  So, now we grab enough semaphores to support the desired max number
 *	  of backends immediately at initialization --- if the sysadmin has set
 *	  MaxConnections, max_worker_processes, or autovacuum_max_workers higher
 *	  than his kernel will support, he'll find out sooner rather than later.
 *
 *	  Another reason for creating semaphores here is that the semaphore
 *	  implementation typically requires us to create semaphores in the
 *	  postmaster, not in backends.
 *
 * Note: this is NOT called by individual backends under a postmaster,
 * not even in the EXEC_BACKEND case.  The ProcGlobal and AuxiliaryProcs
 * pointers must be propagated specially for EXEC_BACKEND operation.
 */
void
InitProcGlobal(void)
{
	PGPROC	   *procs;
	PGXACT	   *pgxacts;
	int			i,
				j;
	bool		found;
	uint32		TotalProcs = MaxBackends + NUM_AUXILIARY_PROCS + max_prepared_xacts;

	/* Create the ProcGlobal shared structure */
	ProcGlobal = (PROC_HDR *)
		ShmemInitStruct("Proc Header", sizeof(PROC_HDR), &found);
	Assert(!found);

	/*
	 * Initialize the data structures.
	 */
	ProcGlobal->spins_per_delay = DEFAULT_SPINS_PER_DELAY;
	ProcGlobal->freeProcs = NULL;
	ProcGlobal->autovacFreeProcs = NULL;
	ProcGlobal->bgworkerFreeProcs = NULL;
	ProcGlobal->startupProc = NULL;
	ProcGlobal->startupProcPid = 0;
	ProcGlobal->startupBufferPinWaitBufId = -1;
	ProcGlobal->walwriterLatch = NULL;
	ProcGlobal->checkpointerLatch = NULL;

	ProcGlobal->mppLocalProcessCounter = 0;

	/*
	 * Create and initialize all the PGPROC structures we'll need.  There are
	 * five separate consumers: (1) normal backends, (2) autovacuum workers
	 * and the autovacuum launcher, (3) background workers, (4) auxiliary
	 * processes, and (5) prepared transactions.  Each PGPROC structure is
	 * dedicated to exactly one of these purposes, and they do not move
	 * between groups.
	 */
	procs = (PGPROC *) ShmemAlloc(TotalProcs * sizeof(PGPROC));
	ProcGlobal->allProcs = procs;
	/* XXX allProcCount isn't really all of them; it excludes prepared xacts */
	ProcGlobal->allProcCount = MaxBackends + NUM_AUXILIARY_PROCS;
	if (!procs)
		ereport(FATAL,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of shared memory")));
	MemSet(procs, 0, TotalProcs * sizeof(PGPROC));

	/*
	 * Also allocate a separate array of PGXACT structures.  This is separate
	 * from the main PGPROC array so that the most heavily accessed data is
	 * stored contiguously in memory in as few cache lines as possible. This
	 * provides significant performance benefits, especially on a
	 * multiprocessor system.  There is one PGXACT structure for every PGPROC
	 * structure.
	 */
	pgxacts = (PGXACT *) ShmemAlloc(TotalProcs * sizeof(PGXACT));
	MemSet(pgxacts, 0, TotalProcs * sizeof(PGXACT));
	ProcGlobal->allPgXact = pgxacts;

	for (i = 0; i < TotalProcs; i++)
	{
		/* Common initialization for all PGPROCs, regardless of type. */

		/*
		 * Set up per-PGPROC semaphore, latch, and backendLock. Prepared xact
		 * dummy PGPROCs don't need these though - they're never associated
		 * with a real process
		 */
		if (i < MaxBackends + NUM_AUXILIARY_PROCS)
		{
			PGSemaphoreCreate(&(procs[i].sem));
			InitSharedLatch(&(procs[i].procLatch));
			procs[i].backendLock = LWLockAssign();
		}
		procs[i].pgprocno = i;

		/*
		 * Newly created PGPROCs for normal backends, autovacuum and bgworkers
		 * must be queued up on the appropriate free list.  Because there can
		 * only ever be a small, fixed number of auxiliary processes, no free
		 * list is used in that case; InitAuxiliaryProcess() instead uses a
		 * linear search.   PGPROCs for prepared transactions are added to a
		 * free list by TwoPhaseShmemInit().
		 */
		if (i < MaxConnections)
		{
			/* PGPROC for normal backend, add to freeProcs list */
			procs[i].links.next = (SHM_QUEUE *) ProcGlobal->freeProcs;
			ProcGlobal->freeProcs = &procs[i];
		}
		else if (i < MaxConnections + autovacuum_max_workers + 1)
		{
			/* PGPROC for AV launcher/worker, add to autovacFreeProcs list */
			procs[i].links.next = (SHM_QUEUE *) ProcGlobal->autovacFreeProcs;
			ProcGlobal->autovacFreeProcs = &procs[i];
		}
		else if (i < MaxBackends)
		{
			/* PGPROC for bgworker, add to bgworkerFreeProcs list */
			procs[i].links.next = (SHM_QUEUE *) ProcGlobal->bgworkerFreeProcs;
			ProcGlobal->bgworkerFreeProcs = &procs[i];
		}

		/* Initialize myProcLocks[] shared memory queues. */
		for (j = 0; j < NUM_LOCK_PARTITIONS; j++)
			SHMQueueInit(&(procs[i].myProcLocks[j]));
	}

	/*
	 * Save pointers to the blocks of PGPROC structures reserved for auxiliary
	 * processes and prepared transactions.
	 */
	AuxiliaryProcs = &procs[MaxBackends];
	PreparedXactProcs = &procs[MaxBackends + NUM_AUXILIARY_PROCS];

	/* Create ProcStructLock spinlock, too */
	ProcStructLock = (slock_t *) ShmemAlloc(sizeof(slock_t));
	SpinLockInit(ProcStructLock);

	/*
	 * GPDB: numFreeProcs is used to keep track of the
	 * number of free PGPROC entries in freeProcs list,
	 * and will accelerate HaveNFreeProcs().
	 */
	ProcGlobal->numFreeProcs = MaxConnections;
}

/*
 * InitProcess -- initialize a per-process data structure for this backend
 */
void
InitProcess(void)
{
	/* use volatile pointer to prevent code rearrangement */
	volatile PROC_HDR *procglobal = ProcGlobal;

	/*
	 * Autovacuum, WAL sender, FTS handler and FTS daemon processes are marked
	 * as GP_ROLE_UTILITY to prevent unwanted GP_ROLE_DISPATCH MyProc settings
	 * such as mppSessionId being valid and mppIsWriter set to true.
	 */
	if (IsAutoVacuumWorkerProcess() || am_walsender || am_ftshandler ||
		am_ftsprobe)
		Gp_role = GP_ROLE_UTILITY;

	/*
	 * ProcGlobal should be set up already (if we are a backend, we inherit
	 * this by fork() or EXEC_BACKEND mechanism from the postmaster).
	 */
	if (procglobal == NULL)
		elog(PANIC, "proc header uninitialized");

	if (MyProc != NULL)
		elog(ERROR, "you already exist");

	/*
	 * Try to get a proc struct from the free list.  If this fails, we must be
	 * out of PGPROC structures (not to mention semaphores).
	 *
	 * While we are holding the ProcStructLock, also copy the current shared
	 * estimate of spins_per_delay to local storage.
	 */
	SpinLockAcquire(ProcStructLock);

	set_spins_per_delay(procglobal->spins_per_delay);

	if (IsAnyAutoVacuumProcess())
		MyProc = procglobal->autovacFreeProcs;
	else if (IsBackgroundWorker)
		MyProc = procglobal->bgworkerFreeProcs;
	else
		MyProc = procglobal->freeProcs;

	if (MyProc != NULL)
	{
		if (IsAnyAutoVacuumProcess())
		{
			procglobal->autovacFreeProcs = (PGPROC *) MyProc->links.next;
<<<<<<< HEAD
		}
=======
		else if (IsBackgroundWorker)
			procglobal->bgworkerFreeProcs = (PGPROC *) MyProc->links.next;
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
		else
		{
			procglobal->freeProcs = (PGPROC *) MyProc->links.next;

			procglobal->numFreeProcs--;     /* we removed an entry from the list. */
			Assert(procglobal->numFreeProcs >= 0);
		}

		SpinLockRelease(ProcStructLock);
	}
	else
	{
		/*
		 * If we reach here, all the PGPROCs are in use.  This is one of the
		 * possible places to detect "too many backends", so give the standard
		 * error message.  XXX do we need to give a different failure message
		 * in the autovacuum case?
		 */
		SpinLockRelease(ProcStructLock);
		ereport(FATAL,
				(errcode(ERRCODE_TOO_MANY_CONNECTIONS),
				 errmsg("sorry, too many clients already")));
	}
	MyPgXact = &ProcGlobal->allPgXact[MyProc->pgprocno];

	if (gp_debug_pgproc)
	{
		elog(LOG, "allocating PGPROC entry for pid %d, freeProcs (prev ptr, new ptr): (%p, %p)",
			 MyProcPid, MyProc, MyProc->links.next);
	}

	int mppLocalProcessSerial = pg_atomic_add_fetch_u32((pg_atomic_uint32 *)&procglobal->mppLocalProcessCounter, 1);

	lockHolderProcPtr = MyProc;

	/* Set the next pointer to NULL */
	MyProc->links.next = NULL;

	/*
	 * Now that we have a PGPROC, mark ourselves as an active postmaster
	 * child; this is so that the postmaster can detect it if we exit without
	 * cleaning up.  (XXX autovac launcher currently doesn't participate in
	 * this; it probably should.)
	 *
	 * Ideally, we should create functions similar to IsAutoVacuumLauncherProcess()
	 * for ftsProber, SeqServer etc who call InitProcess().
	 * But MyPMChildSlot helps to get away with it.
	 */
	if (IsUnderPostmaster && !IsAutoVacuumLauncherProcess()
		&& MyPMChildSlot > 0)
		MarkPostmasterChildActive();

	/*
	 * Initialize all fields of MyProc, except for those previously
	 * initialized by InitProcGlobal.
	 */
	SHMQueueElemInit(&(MyProc->links));
	MyProc->waitStatus = STATUS_OK;
	MyProc->lxid = InvalidLocalTransactionId;
	MyProc->fpVXIDLock = false;
	MyProc->fpLocalTransactionId = InvalidLocalTransactionId;
	MyPgXact->xid = InvalidTransactionId;
	MyPgXact->xmin = InvalidTransactionId;
	MyProc->localDistribXactData.state = LOCALDISTRIBXACT_STATE_NONE;
	MyProc->serializableIsoLevel = false;
	MyProc->inDropTransaction = false;
	MyProc->pid = MyProcPid;
	/* backendId, databaseId and roleId will be filled in later */
	MyProc->backendId = InvalidBackendId;
	MyProc->databaseId = InvalidOid;
	MyProc->roleId = InvalidOid;
	MyPgXact->delayChkpt = false;
	MyPgXact->vacuumFlags = 0;
	/* NB -- autovac launcher intentionally does not set IS_AUTOVACUUM */
	if (IsAutoVacuumWorkerProcess())
		MyPgXact->vacuumFlags |= PROC_IS_AUTOVACUUM;
	MyProc->lwWaiting = false;
	MyProc->lwWaitMode = 0;
	MyProc->waitLock = NULL;
	MyProc->waitProcLock = NULL;
	MyProc->resSlot = NULL;

    /* 
     * mppLocalProcessSerial uniquely identifies this backend process among
     * all those that our parent postmaster process creates over its lifetime. 
     *
  	 * Since we use the process serial number to decide if we should
	 * deliver a response from a server under this spin, we need to 
	 * assign it under the spin lock.
	 */
    MyProc->mppLocalProcessSerial = mppLocalProcessSerial;

    /* 
     * A nonzero gp_session_id uniquely identifies an MPP client session 
     * over the lifetime of the entry postmaster process. A qDisp passes
     * its gp_session_id down to all of its qExecs. If this is a qExec,
     * we have already received the gp_session_id from the qDisp.
     */
    if (Gp_role == GP_ROLE_DISPATCH && gp_session_id == -1)
        gp_session_id = mppLocalProcessSerial;
    MyProc->mppSessionId = gp_session_id;
    elog(DEBUG1,"InitProcess(): gp_session_id %d, Gp_role %d",gp_session_id, Gp_role);
    
    MyProc->mppIsWriter = Gp_is_writer;

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		MyProc->mppIsWriter = true;
	}
    
	/* Initialise for sync rep */
#ifdef USE_ASSERT_CHECKING
	{
		int			i;

		/* Last process should have released all locks. */
		for (i = 0; i < NUM_LOCK_PARTITIONS; i++)
			Assert(SHMQueueEmpty(&(MyProc->myProcLocks[i])));
	}
#endif
	MyProc->recoveryConflictPending = false;

	/* Initialize fields for sync rep */
	MyProc->waitLSN = 0;
	MyProc->syncRepState = SYNC_REP_NOT_WAITING;
	SHMQueueElemInit(&(MyProc->syncRepLinks));

	/*
	 * Acquire ownership of the PGPROC's latch, so that we can use WaitLatch
	 * on it.  That allows us to repoint the process latch, which so far
	 * points to process local one, to the shared one.
	 */
	OwnLatch(&MyProc->procLatch);
	SwitchToSharedLatch();

	/*
	 * We might be reusing a semaphore that belonged to a failed process. So
	 * be careful and reinitialize its value here.  (This is not strictly
	 * necessary anymore, but seems like a good idea for cleanliness.)
	 */
	PGSemaphoreReset(&MyProc->sem);

	/* Set wait portal (do not check if resource scheduling is enabled) */
	MyProc->waitPortalId = INVALID_PORTALID;

	MyProc->queryCommandId = -1;

	/* Init gxact */
	initGxact(&MyProc->gxact);

	/*
	 * Arrange to clean up at backend exit.
	 */
	on_shmem_exit(ProcKill, 0);

	/*
	 * Now that we have a PGPROC, we could try to acquire locks, so initialize
	 * local state needed for LWLocks, and the deadlock checker.
	 */
	InitLWLockAccess();
	InitDeadLockChecking();
}

/*
 * InitProcessPhase2 -- make MyProc visible in the shared ProcArray.
 *
 * This is separate from InitProcess because we can't acquire LWLocks until
 * we've created a PGPROC, but in the EXEC_BACKEND case ProcArrayAdd won't
 * work until after we've done CreateSharedMemoryAndSemaphores.
 */
void
InitProcessPhase2(void)
{
	Assert(MyProc != NULL);

	/*
	 * Add our PGPROC to the PGPROC array in shared memory.
	 */
	ProcArrayAdd(MyProc);

	/*
	 * Arrange to clean that up at backend exit.
	 */
	on_shmem_exit(RemoveProcFromArray, 0);
}

/*
 * InitAuxiliaryProcess -- create a per-auxiliary-process data structure
 *
 * This is called by bgwriter and similar processes so that they will have a
 * MyProc value that's real enough to let them wait for LWLocks.  The PGPROC
 * and sema that are assigned are one of the extra ones created during
 * InitProcGlobal.
 *
 * Auxiliary processes are presently not expected to wait for real (lockmgr)
 * locks, so we need not set up the deadlock checker.  They are never added
 * to the ProcArray or the sinval messaging mechanism, either.  They also
 * don't get a VXID assigned, since this is only useful when we actually
 * hold lockmgr locks.
 *
 * Startup process however uses locks but never waits for them in the
 * normal backend sense. Startup process also takes part in sinval messaging
 * as a sendOnly process, so never reads messages from sinval queue. So
 * Startup process does have a VXID and does show up in pg_locks.
 */
void
InitAuxiliaryProcess(void)
{
	PGPROC	   *auxproc;
	int			proctype;

	/*
	 * ProcGlobal should be set up already (if we are a backend, we inherit
	 * this by fork() or EXEC_BACKEND mechanism from the postmaster).
	 */
	if (ProcGlobal == NULL || AuxiliaryProcs == NULL)
		elog(PANIC, "proc header uninitialized");

	if (MyProc != NULL)
		elog(ERROR, "you already exist");

	/*
	 * We use the ProcStructLock to protect assignment and releasing of
	 * AuxiliaryProcs entries.
	 *
	 * While we are holding the ProcStructLock, also copy the current shared
	 * estimate of spins_per_delay to local storage.
	 */
	SpinLockAcquire(ProcStructLock);

	set_spins_per_delay(ProcGlobal->spins_per_delay);

	/*
	 * Find a free auxproc ... *big* trouble if there isn't one ...
	 */
	for (proctype = 0; proctype < NUM_AUXILIARY_PROCS; proctype++)
	{
		auxproc = &AuxiliaryProcs[proctype];
		if (auxproc->pid == 0)
			break;
	}
	if (proctype >= NUM_AUXILIARY_PROCS)
	{
		SpinLockRelease(ProcStructLock);
		elog(FATAL, "all AuxiliaryProcs are in use");
	}

	/* Mark auxiliary proc as in use by me */
	/* use volatile pointer to prevent code rearrangement */
	((volatile PGPROC *) auxproc)->pid = MyProcPid;

	MyProc = auxproc;
	lockHolderProcPtr = auxproc;
	MyPgXact = &ProcGlobal->allPgXact[auxproc->pgprocno];

	SpinLockRelease(ProcStructLock);

	/*
	 * Initialize all fields of MyProc, except for those previously
	 * initialized by InitProcGlobal.
	 */
	SHMQueueElemInit(&(MyProc->links));
	MyProc->waitStatus = STATUS_OK;
	MyProc->lxid = InvalidLocalTransactionId;
	MyProc->fpVXIDLock = false;
	MyProc->fpLocalTransactionId = InvalidLocalTransactionId;
	MyPgXact->xid = InvalidTransactionId;
	MyPgXact->xmin = InvalidTransactionId;
	MyProc->localDistribXactData.state = LOCALDISTRIBXACT_STATE_NONE;
	MyProc->serializableIsoLevel = false;
	MyProc->inDropTransaction = false;
	MyProc->backendId = InvalidBackendId;
	MyProc->databaseId = InvalidOid;
	MyProc->roleId = InvalidOid;
<<<<<<< HEAD
    MyProc->mppLocalProcessSerial = 0;
    MyProc->mppSessionId = 0;
    MyProc->mppIsWriter = false;
	MyPgXact->inCommit = false;
=======
	MyPgXact->delayChkpt = false;
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
	MyPgXact->vacuumFlags = 0;
	MyProc->lwWaiting = false;
	MyProc->lwWaitMode = 0;
	MyProc->waitLock = NULL;
	MyProc->waitProcLock = NULL;
#ifdef USE_ASSERT_CHECKING
	{
		int			i;

		/* Last process should have released all locks. */
		for (i = 0; i < NUM_LOCK_PARTITIONS; i++)
			Assert(SHMQueueEmpty(&(MyProc->myProcLocks[i])));
	}
#endif

	/*
	 * Acquire ownership of the PGPROC's latch, so that we can use WaitLatch
	 * on it.  That allows us to repoint the process latch, which so far
	 * points to process local one, to the shared one.
	 */
	OwnLatch(&MyProc->procLatch);
	SwitchToSharedLatch();

	/*
	 * We might be reusing a semaphore that belonged to a failed process. So
	 * be careful and reinitialize its value here.  (This is not strictly
	 * necessary anymore, but seems like a good idea for cleanliness.)
	 */
	PGSemaphoreReset(&MyProc->sem);

	MyProc->queryCommandId = -1;

	/*
	 * Arrange to clean up at process exit.
	 */
	on_shmem_exit(AuxiliaryProcKill, Int32GetDatum(proctype));
}

/*
 * Record the PID and PGPROC structures for the Startup process, for use in
 * ProcSendSignal().  See comments there for further explanation.
 */
void
PublishStartupProcessInformation(void)
{
	/* use volatile pointer to prevent code rearrangement */
	volatile PROC_HDR *procglobal = ProcGlobal;

	SpinLockAcquire(ProcStructLock);

	procglobal->startupProc = MyProc;
	procglobal->startupProcPid = MyProcPid;

	SpinLockRelease(ProcStructLock);
}

/*
 * Used from bufgr to share the value of the buffer that Startup waits on,
 * or to reset the value to "not waiting" (-1). This allows processing
 * of recovery conflicts for buffer pins. Set is made before backends look
 * at this value, so locking not required, especially since the set is
 * an atomic integer set operation.
 */
void
SetStartupBufferPinWaitBufId(int bufid)
{
	/* use volatile pointer to prevent code rearrangement */
	volatile PROC_HDR *procglobal = ProcGlobal;

	procglobal->startupBufferPinWaitBufId = bufid;
}

/*
 * Used by backends when they receive a request to check for buffer pin waits.
 */
int
GetStartupBufferPinWaitBufId(void)
{
	/* use volatile pointer to prevent code rearrangement */
	volatile PROC_HDR *procglobal = ProcGlobal;

	return procglobal->startupBufferPinWaitBufId;
}

/*
 * Check whether there are at least N free PGPROC objects.
 */
bool
HaveNFreeProcs(int n)
{
	Assert(n >= 0);

	return (ProcGlobal->numFreeProcs >= n);
}

/*
 * Check if the current process is awaiting a lock.
 */
bool
IsWaitingForLock(void)
{
	if (lockAwaited == NULL)
		return false;

	return true;
}

/*
 * Cancel any pending wait for lock, when aborting a transaction, and revert
 * any strong lock count acquisition for a lock being acquired.
 *
 * (Normally, this would only happen if we accept a cancel/die
 * interrupt while waiting; but an ereport(ERROR) before or during the lock
 * wait is within the realm of possibility, too.)
 */
void
LockErrorCleanup(void)
{
	LWLock	   *partitionLock;
	DisableTimeoutParams timeouts[2];

	HOLD_INTERRUPTS();

	AbortStrongLockAcquire();

	/* Nothing to do if we weren't waiting for a lock */
	if (lockAwaited == NULL)
	{
		RESUME_INTERRUPTS();
		return;
	}

<<<<<<< HEAD
	/* Don't try to cancel resource locks.*/
	if (Gp_role == GP_ROLE_DISPATCH && IsResQueueEnabled() &&
		LOCALLOCK_LOCKMETHOD(*lockAwaited) == RESOURCE_LOCKMETHOD)
		return;

	/* Turn off the deadlock timer, if it's still running (see ProcSleep) */
	disable_sig_alarm(false);
=======
	/*
	 * Turn off the deadlock and lock timeout timers, if they are still
	 * running (see ProcSleep).  Note we must preserve the LOCK_TIMEOUT
	 * indicator flag, since this function is executed before
	 * ProcessInterrupts when responding to SIGINT; else we'd lose the
	 * knowledge that the SIGINT came from a lock timeout and not an external
	 * source.
	 */
	timeouts[0].id = DEADLOCK_TIMEOUT;
	timeouts[0].keep_indicator = false;
	timeouts[1].id = LOCK_TIMEOUT;
	timeouts[1].keep_indicator = true;
	disable_timeouts(timeouts, 2);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

	/* Unlink myself from the wait queue, if on it (might not be anymore!) */
	partitionLock = LockHashPartitionLock(lockAwaited->hashcode);
	LWLockAcquire(partitionLock, LW_EXCLUSIVE);

	if (MyProc->links.next != NULL)
	{
		/* We could not have been granted the lock yet */
		RemoveFromWaitQueue(MyProc, lockAwaited->hashcode);
	}
	else
	{
		/*
		 * Somebody kicked us off the lock queue already.  Perhaps they
		 * granted us the lock, or perhaps they detected a deadlock. If they
		 * did grant us the lock, we'd better remember it in our local lock
		 * table.
		 */
		if (MyProc->waitStatus == STATUS_OK)
			GrantAwaitedLock();
	}

	lockAwaited = NULL;

	LWLockRelease(partitionLock);

	RESUME_INTERRUPTS();
}


/*
 * ProcReleaseLocks() -- release locks associated with current transaction
 *			at main transaction commit or abort
 *
 * At main transaction commit, we release standard locks except session locks.
 * At main transaction abort, we release all locks including session locks.
 *
 * Advisory locks are released only if they are transaction-level;
 * session-level holds remain, whether this is a commit or not.
 *
 * At subtransaction commit, we don't release any locks (so this func is not
 * needed at all); we will defer the releasing to the parent transaction.
 * At subtransaction abort, we release all locks held by the subtransaction;
 * this is implemented by retail releasing of the locks under control of
 * the ResourceOwner mechanism.
 */
void
ProcReleaseLocks(bool isCommit)
{
	if (!MyProc)
		return;
	/* If waiting, get off wait queue (should only be needed after error) */
	LockErrorCleanup();
	/* Release standard locks, including session-level if aborting */
	LockReleaseAll(DEFAULT_LOCKMETHOD, !isCommit);
	/* Release transaction-level advisory locks */
	LockReleaseAll(USER_LOCKMETHOD, false);
}


/*
 * RemoveProcFromArray() -- Remove this process from the shared ProcArray.
 */
static void
RemoveProcFromArray(int code, Datum arg)
{
	Assert(MyProc != NULL);
	ProcArrayRemove(MyProc, InvalidTransactionId);
}

/*
 * update_spins_per_delay
 *   Update spins_per_delay value in ProcGlobal.
 */
static void
update_spins_per_delay(void)
{
	volatile PROC_HDR *procglobal = ProcGlobal;
	bool casResult = false;

	while (!casResult)
	{
		int old_spins_per_delay = procglobal->spins_per_delay;
		int new_spins_per_delay = recompute_spins_per_delay(old_spins_per_delay);
		casResult = pg_atomic_compare_exchange_u32((pg_atomic_uint32 *)&procglobal->spins_per_delay,
										(uint32 *)&old_spins_per_delay,
										new_spins_per_delay);
	}
}

/*
 * ProcKill() -- Destroy the per-proc data structure for
 *		this process. Release any of its held LW locks.
 */
static void
ProcKill(int code, Datum arg)
{
	/* use volatile pointer to prevent code rearrangement */
	volatile PROC_HDR *procglobal = ProcGlobal;
	PGPROC	   *proc;
<<<<<<< HEAD
=======

>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
	Assert(MyProc != NULL);

	/* Make sure we're out of the sync rep lists */
	SyncRepCleanupAtProcExit();

	/* 
	 * Cleanup for any resource locks on portals - from holdable cursors or
	 * unclean process abort (assertion failures).
	 */
	if (Gp_role == GP_ROLE_DISPATCH && IsResQueueEnabled())
		AtExitCleanup_ResPortals();

	/*
	 * Remove the shared snapshot slot.
	 */
	if (SharedLocalSnapshotSlot != NULL)
	{
		if (Gp_role == GP_ROLE_DISPATCH)
		{
			SharedSnapshotRemove(SharedLocalSnapshotSlot,
								 "Query Dispatcher");
		}
	    else if (IS_QUERY_DISPATCHER() && Gp_role == GP_ROLE_EXECUTE && !Gp_is_writer)
	    {
			/* 
			 * Entry db singleton QE is a user of the shared snapshot -- not a creator.
			 */	
	    }
		else if (Gp_role == GP_ROLE_EXECUTE && Gp_is_writer)
		{
			SharedSnapshotRemove(SharedLocalSnapshotSlot,
								 "Writer qExec");
		}
		SharedLocalSnapshotSlot = NULL;
	}

	SyncRepCleanupAtProcExit();

#ifdef USE_ASSERT_CHECKING
	{
		int			i;

		/* Last process should have released all locks. */
		for (i = 0; i < NUM_LOCK_PARTITIONS; i++)
			Assert(SHMQueueEmpty(&(MyProc->myProcLocks[i])));
	}
#endif

	/*
	 * Release any LW locks I am holding.  There really shouldn't be any, but
	 * it's cheap to check again before we cut the knees off the LWLock
	 * facility by releasing our PGPROC ...
	 */
	LWLockReleaseAll();

<<<<<<< HEAD
	MyProc->localDistribXactData.state = LOCALDISTRIBXACT_STATE_NONE;
    MyProc->mppLocalProcessSerial = 0;
    MyProc->mppSessionId = 0;
    MyProc->mppIsWriter = false;
	MyProc->pid = 0;

	/*
	 * Clear MyProc first; then disown the process latch.  This is so that
	 * signal handlers won't try to clear the process latch after it's no
	 * longer ours.
	 */
=======
	/* Make sure active replication slots are released */
	if (MyReplicationSlot != NULL)
		ReplicationSlotRelease();

	/*
	 * Reset MyLatch to the process local one.  This is so that signal
	 * handlers et al can continue using the latch after the shared latch
	 * isn't ours anymore. After that clear MyProc and disown the shared
	 * latch.
	 */
	SwitchBackToLocalLatch();
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
	proc = MyProc;
	MyProc = NULL;
	DisownLatch(&proc->procLatch);

	SpinLockAcquire(ProcStructLock);

	/* Return PGPROC structure (and semaphore) to appropriate freelist */
	if (IsAnyAutoVacuumProcess())
	{
		proc->links.next = (SHM_QUEUE *) procglobal->autovacFreeProcs;
		procglobal->autovacFreeProcs = proc;
<<<<<<< HEAD
=======
	}
	else if (IsBackgroundWorker)
	{
		proc->links.next = (SHM_QUEUE *) procglobal->bgworkerFreeProcs;
		procglobal->bgworkerFreeProcs = proc;
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
	}
	else
	{
		proc->links.next = (SHM_QUEUE *) procglobal->freeProcs;
		procglobal->freeProcs = proc;
<<<<<<< HEAD

		procglobal->numFreeProcs++;	/* we added an entry */
	}


=======
	}

>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
	/* Update shared estimate of spins_per_delay */
	update_spins_per_delay();

	SpinLockRelease(ProcStructLock);

	/*
	 * This process is no longer present in shared memory in any meaningful
	 * way, so tell the postmaster we've cleaned up acceptably well. (XXX
	 * autovac launcher should be included here someday)
	 */
	if (IsUnderPostmaster && !IsAutoVacuumLauncherProcess()
		&& MyPMChildSlot > 0)
		MarkPostmasterChildInactive();

	/* wake autovac launcher if needed -- see comments in FreeWorkerInfo */
	if (AutovacuumLauncherPid != 0)
		kill(AutovacuumLauncherPid, SIGUSR2);
}

/*
 * AuxiliaryProcKill() -- Cut-down version of ProcKill for auxiliary
 *		processes (bgwriter, etc).  The PGPROC and sema are not released, only
 *		marked as not-in-use.
 */
static void
AuxiliaryProcKill(int code, Datum arg)
{
	int			proctype = DatumGetInt32(arg);
	PGPROC	   *auxproc PG_USED_FOR_ASSERTS_ONLY;
	PGPROC	   *proc;

	Assert(proctype >= 0 && proctype < NUM_AUXILIARY_PROCS);

	auxproc = &AuxiliaryProcs[proctype];

	Assert(MyProc == auxproc);

	/* Release any LW locks I am holding (see notes above) */
	LWLockReleaseAll();

	/*
	 * Reset MyLatch to the process local one.  This is so that signal
	 * handlers et al can continue using the latch after the shared latch
	 * isn't ours anymore. After that clear MyProc and disown the shared
	 * latch.
	 */
	SwitchBackToLocalLatch();
	proc = MyProc;
	MyProc = NULL;
	DisownLatch(&proc->procLatch);

	SpinLockAcquire(ProcStructLock);

	/* Mark auxiliary proc no longer in use */
<<<<<<< HEAD
	MyProc->pid = 0;
=======
	proc->pid = 0;
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

	/* Update shared estimate of spins_per_delay */
	update_spins_per_delay();

	SpinLockRelease(ProcStructLock);

	/*
	 * If the parent process of this auxiliary process does not exist, we
	 * have trouble. Besides the obvious case that the postmaster is gone,
	 * this could happen to filerep subprocesses when the filerep main
	 * process dies unexpectedly. The postmaster will receive the SIGCHLD
	 * signal when we exit in that case. Make sure we exit with non-zero (and
	 * not 1 either) exit status, to force the postmaster to reset the system
	 * if that happens.
	 */
	if (!ParentProcIsAlive())
		proc_exit(10);

	/* PGPROC struct isn't mine anymore */
	MyProc = NULL;
	lockHolderProcPtr = NULL;
}


/*
 * ProcQueue package: routines for putting processes to sleep
 *		and  waking them up
 */

/*
 * ProcQueueAlloc -- alloc/attach to a shared memory process queue
 *
 * Returns: a pointer to the queue
 * Side Effects: Initializes the queue if it wasn't there before
 */
#ifdef NOT_USED
PROC_QUEUE *
ProcQueueAlloc(const char *name)
{
	PROC_QUEUE *queue;
	bool		found;

	queue = (PROC_QUEUE *)
		ShmemInitStruct(name, sizeof(PROC_QUEUE), &found);

	if (!found)
		ProcQueueInit(queue);

	return queue;
}
#endif

/*
 * ProcQueueInit -- initialize a shared memory process queue
 */
void
ProcQueueInit(PROC_QUEUE *queue)
{
	SHMQueueInit(&(queue->links));
	queue->size = 0;
}


/*
 * ProcSleep -- put a process to sleep on the specified lock
 *
 * Caller must have set MyProc->heldLocks to reflect locks already held
 * on the lockable object by this process (under all XIDs).
 *
 * The lock table's partition lock must be held at entry, and will be held
 * at exit.
 *
 * Result: STATUS_OK if we acquired the lock, STATUS_ERROR if not (deadlock).
 *
 * ASSUME: that no one will fiddle with the queue until after
 *		we release the partition lock.
 *
 * NOTES: The process queue is now a priority queue for locking.
 */
int
ProcSleep(LOCALLOCK *locallock, LockMethod lockMethodTable)
{
	LOCKMODE	lockmode = locallock->tag.mode;
	LOCK	   *lock = locallock->lock;
	PROCLOCK   *proclock = locallock->proclock;
	uint32		hashcode = locallock->hashcode;
	LWLock	   *partitionLock = LockHashPartitionLock(hashcode);
	PROC_QUEUE *waitQueue = &(lock->waitProcs);
	LOCKMASK	myHeldLocks = MyProc->heldLocks;
	bool		early_deadlock = false;
	bool		allow_autovacuum_cancel = true;
	int			myWaitStatus;
	PGPROC	   *proc;
	int			i;

	/*
	 * Determine where to add myself in the wait queue.
	 *
	 * Normally I should go at the end of the queue.  However, if I already
	 * hold locks that conflict with the request of any previous waiter, put
	 * myself in the queue just in front of the first such waiter. This is not
	 * a necessary step, since deadlock detection would move me to before that
	 * waiter anyway; but it's relatively cheap to detect such a conflict
	 * immediately, and avoid delaying till deadlock timeout.
	 *
	 * Special case: if I find I should go in front of some waiter, check to
	 * see if I conflict with already-held locks or the requests before that
	 * waiter.  If not, then just grant myself the requested lock immediately.
	 * This is the same as the test for immediate grant in LockAcquire, except
	 * we are only considering the part of the wait queue before my insertion
	 * point.
	 */
	if (myHeldLocks != 0)
	{
		LOCKMASK	aheadRequests = 0;

		proc = (PGPROC *) waitQueue->links.next;
		for (i = 0; i < waitQueue->size; i++)
		{
			/* Must he wait for me? */
			if (lockMethodTable->conflictTab[proc->waitLockMode] & myHeldLocks)
			{
				/* Must I wait for him ? */
				if (lockMethodTable->conflictTab[lockmode] & proc->heldLocks)
				{
					/*
					 * Yes, so we have a deadlock.  Easiest way to clean up
					 * correctly is to call RemoveFromWaitQueue(), but we
					 * can't do that until we are *on* the wait queue. So, set
					 * a flag to check below, and break out of loop.  Also,
					 * record deadlock info for later message.
					 */
					RememberSimpleDeadLock(MyProc, lockmode, lock, proc);
					early_deadlock = true;
					break;
				}
				/* I must go before this waiter.  Check special case. */
				if ((lockMethodTable->conflictTab[lockmode] & aheadRequests) == 0 &&
					LockCheckConflicts(lockMethodTable,
									   lockmode,
									   lock,
									   proclock) == STATUS_OK)
				{
					/* Skip the wait and just grant myself the lock. */
					GrantLock(lock, proclock, lockmode);
					GrantAwaitedLock();
					return STATUS_OK;
				}
				/* Break out of loop to put myself before him */
				break;
			}
			/* Nope, so advance to next waiter */
			aheadRequests |= LOCKBIT_ON(proc->waitLockMode);
			proc = (PGPROC *) proc->links.next;
		}

		/*
		 * If we fall out of loop normally, proc points to waitQueue head, so
		 * we will insert at tail of queue as desired.
		 */
	}
	else
	{
		/* I hold no locks, so I can't push in front of anyone. */
		proc = (PGPROC *) &(waitQueue->links);
	}

	/*
	 * Insert self into queue, ahead of the given proc (or at tail of queue).
	 */
	SHMQueueInsertBefore(&(proc->links), &(MyProc->links));
	waitQueue->size++;

	lock->waitMask |= LOCKBIT_ON(lockmode);

	/* Set up wait information in PGPROC object, too */
	MyProc->waitLock = lock;
	MyProc->waitProcLock = proclock;
	MyProc->waitLockMode = lockmode;

	MyProc->waitStatus = STATUS_WAITING;

	/*
	 * If we detected deadlock, give up without waiting.  This must agree with
	 * CheckDeadLock's recovery code, except that we shouldn't release the
	 * semaphore since we haven't tried to lock it yet.
	 */
	if (early_deadlock)
	{
		RemoveFromWaitQueue(MyProc, hashcode);
		return STATUS_ERROR;
	}

	/* mark that we are waiting for a lock */
	lockAwaited = locallock;

	/*
	 * Release the lock table's partition lock.
	 *
	 * NOTE: this may also cause us to exit critical-section state, possibly
	 * allowing a cancel/die interrupt to be accepted. This is OK because we
	 * have recorded the fact that we are waiting for a lock, and so
	 * LockErrorCleanup will clean up if cancel/die happens.
	 */
	LWLockRelease(partitionLock);

	/*
	 * Also, now that we will successfully clean up after an ereport, it's
	 * safe to check to see if there's a buffer pin deadlock against the
	 * Startup process.  Of course, that's only necessary if we're doing Hot
	 * Standby and are not the Startup process ourselves.
	 */
	if (RecoveryInProgress() && !InRecovery)
		CheckRecoveryConflictDeadlock();

	/* Reset deadlock_state before enabling the timeout handler */
	deadlock_state = DS_NOT_YET_CHECKED;
	got_deadlock_timeout = false;

	/*
	 * Set timer so we can wake up after awhile and check for a deadlock. If a
	 * deadlock is detected, the handler releases the process's semaphore and
	 * sets MyProc->waitStatus = STATUS_ERROR, allowing us to know that we
	 * must report failure rather than success.
	 *
	 * By delaying the check until we've waited for a bit, we can avoid
	 * running the rather expensive deadlock-check code in most cases.
	 *
	 * If LockTimeout is set, also enable the timeout for that.  We can save a
	 * few cycles by enabling both timeout sources in one call.
	 */
	if (LockTimeout > 0)
	{
		EnableTimeoutParams timeouts[2];

		timeouts[0].id = DEADLOCK_TIMEOUT;
		timeouts[0].type = TMPARAM_AFTER;
		timeouts[0].delay_ms = DeadlockTimeout;
		timeouts[1].id = LOCK_TIMEOUT;
		timeouts[1].type = TMPARAM_AFTER;
		timeouts[1].delay_ms = LockTimeout;
		enable_timeouts(timeouts, 2);
	}
	else
		enable_timeout_after(DEADLOCK_TIMEOUT, DeadlockTimeout);

	/*
	 * If somebody wakes us between LWLockRelease and WaitLatch, the latch
	 * will not wait. But a set latch does not necessarily mean that the lock
	 * is free now, as there are many other sources for latch sets than
	 * somebody releasing the lock.
	 *
	 * We process interrupts whenever the latch has been set, so cancel/die
	 * interrupts are processed quickly. This means we must not mind losing
	 * control to a cancel/die interrupt here.  We don't, because we have no
	 * shared-state-change work to do after being granted the lock (the
	 * grantor did it all).  We do have to worry about canceling the deadlock
	 * timeout and updating the locallock table, but if we lose control to an
	 * error, LockErrorCleanup will fix that up.
	 */
	do
	{
		WaitLatch(MyLatch, WL_LATCH_SET, 0);
		ResetLatch(MyLatch);
		/* check for deadlocks first, as that's probably log-worthy */
		if (got_deadlock_timeout)
		{
			CheckDeadLock();
			got_deadlock_timeout = false;
		}
		CHECK_FOR_INTERRUPTS();

		/*
		 * waitStatus could change from STATUS_WAITING to something else
		 * asynchronously.  Read it just once per loop to prevent surprising
		 * behavior (such as missing log messages).
		 */
		myWaitStatus = *((volatile int *) &MyProc->waitStatus);

		/*
		 * If we are not deadlocked, but are waiting on an autovacuum-induced
		 * task, send a signal to interrupt it.
		 */
		if (deadlock_state == DS_BLOCKED_BY_AUTOVACUUM && allow_autovacuum_cancel)
		{
			PGPROC	   *autovac = GetBlockingAutoVacuumPgproc();
			PGXACT	   *autovac_pgxact = &ProcGlobal->allPgXact[autovac->pgprocno];

			LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);

			/*
			 * Only do it if the worker is not working to protect against Xid
			 * wraparound.
			 */
			if ((autovac_pgxact->vacuumFlags & PROC_IS_AUTOVACUUM) &&
				!(autovac_pgxact->vacuumFlags & PROC_VACUUM_FOR_WRAPAROUND))
			{
				int			pid = autovac->pid;
				StringInfoData locktagbuf;
<<<<<<< HEAD
				StringInfoData logbuf;		/* errdetail for server log */
=======
				StringInfoData logbuf;	/* errdetail for server log */
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

				initStringInfo(&locktagbuf);
				initStringInfo(&logbuf);
				DescribeLockTag(&locktagbuf, &lock->tag);
				appendStringInfo(&logbuf,
<<<<<<< HEAD
					  _("Process %d waits for %s on %s."),
						 MyProcPid,
						 GetLockmodeName(lock->tag.locktag_lockmethodid,
										 lockmode),
						 locktagbuf.data);
=======
								 _("Process %d waits for %s on %s."),
								 MyProcPid,
							  GetLockmodeName(lock->tag.locktag_lockmethodid,
											  lockmode),
								 locktagbuf.data);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

				/* release lock as quickly as possible */
				LWLockRelease(ProcArrayLock);

				ereport(LOG,
<<<<<<< HEAD
						(errmsg("sending cancel to blocking autovacuum PID %d",
							pid),
						 errdetail("%s", logbuf.data)));
=======
					  (errmsg("sending cancel to blocking autovacuum PID %d",
							  pid),
					   errdetail_log("%s", logbuf.data)));
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

				pfree(logbuf.data);
				pfree(locktagbuf.data);

				/* send the autovacuum worker Back to Old Kent Road */
				if (kill(pid, SIGINT) < 0)
				{
					/* Just a warning to allow multiple callers */
					ereport(WARNING,
							(errmsg("could not send signal to process %d: %m",
									pid)));
				}
			}
			else
				LWLockRelease(ProcArrayLock);

			/* prevent signal from being resent more than once */
			allow_autovacuum_cancel = false;
		}

		/*
		 * If awoken after the deadlock check interrupt has run, and
		 * log_lock_waits is on, then report about the wait.
		 */
		if (log_lock_waits && deadlock_state != DS_NOT_YET_CHECKED)
		{
			StringInfoData buf,
						lock_waiters_sbuf,
						lock_holders_sbuf;
			const char *modename;
			long		secs;
			int			usecs;
			long		msecs;
			SHM_QUEUE  *procLocks;
			PROCLOCK   *proclock;
			bool		first_holder = true,
						first_waiter = true;
			int			lockHoldersNum = 0;

			initStringInfo(&buf);
			initStringInfo(&lock_waiters_sbuf);
			initStringInfo(&lock_holders_sbuf);

			DescribeLockTag(&buf, &locallock->tag.lock);
			modename = GetLockmodeName(locallock->tag.lock.locktag_lockmethodid,
									   lockmode);
			TimestampDifference(get_timeout_start_time(DEADLOCK_TIMEOUT),
								GetCurrentTimestamp(),
								&secs, &usecs);
			msecs = secs * 1000 + usecs / 1000;
			usecs = usecs % 1000;

			/*
			 * we loop over the lock's procLocks to gather a list of all
			 * holders and waiters. Thus we will be able to provide more
			 * detailed information for lock debugging purposes.
			 *
			 * lock->procLocks contains all processes which hold or wait for
			 * this lock.
			 */

			LWLockAcquire(partitionLock, LW_SHARED);

			procLocks = &(lock->procLocks);
			proclock = (PROCLOCK *) SHMQueueNext(procLocks, procLocks,
											   offsetof(PROCLOCK, lockLink));

			while (proclock)
			{
				/*
				 * we are a waiter if myProc->waitProcLock == proclock; we are
				 * a holder if it is NULL or something different
				 */
				if (proclock->tag.myProc->waitProcLock == proclock)
				{
					if (first_waiter)
					{
						appendStringInfo(&lock_waiters_sbuf, "%d",
										 proclock->tag.myProc->pid);
						first_waiter = false;
					}
					else
						appendStringInfo(&lock_waiters_sbuf, ", %d",
										 proclock->tag.myProc->pid);
				}
				else
				{
					if (first_holder)
					{
						appendStringInfo(&lock_holders_sbuf, "%d",
										 proclock->tag.myProc->pid);
						first_holder = false;
					}
					else
						appendStringInfo(&lock_holders_sbuf, ", %d",
										 proclock->tag.myProc->pid);

					lockHoldersNum++;
				}

				proclock = (PROCLOCK *) SHMQueueNext(procLocks, &proclock->lockLink,
											   offsetof(PROCLOCK, lockLink));
			}

			LWLockRelease(partitionLock);

			if (deadlock_state == DS_SOFT_DEADLOCK)
				ereport(LOG,
						(errmsg("process %d avoided deadlock for %s on %s by rearranging queue order after %ld.%03d ms",
								MyProcPid, modename, buf.data, msecs, usecs),
						 (errdetail_log_plural("Process holding the lock: %s. Wait queue: %s.",
						   "Processes holding the lock: %s. Wait queue: %s.",
											   lockHoldersNum, lock_holders_sbuf.data, lock_waiters_sbuf.data))));
			else if (deadlock_state == DS_HARD_DEADLOCK)
			{
				/*
				 * This message is a bit redundant with the error that will be
				 * reported subsequently, but in some cases the error report
				 * might not make it to the log (eg, if it's caught by an
				 * exception handler), and we want to ensure all long-wait
				 * events get logged.
				 */
				ereport(LOG,
						(errmsg("process %d detected deadlock while waiting for %s on %s after %ld.%03d ms",
								MyProcPid, modename, buf.data, msecs, usecs),
						 (errdetail_log_plural("Process holding the lock: %s. Wait queue: %s.",
						   "Processes holding the lock: %s. Wait queue: %s.",
											   lockHoldersNum, lock_holders_sbuf.data, lock_waiters_sbuf.data))));
			}

			if (myWaitStatus == STATUS_WAITING)
				ereport(LOG,
						(errmsg("process %d still waiting for %s on %s after %ld.%03d ms",
								MyProcPid, modename, buf.data, msecs, usecs),
						 (errdetail_log_plural("Process holding the lock: %s. Wait queue: %s.",
						   "Processes holding the lock: %s. Wait queue: %s.",
											   lockHoldersNum, lock_holders_sbuf.data, lock_waiters_sbuf.data))));
			else if (myWaitStatus == STATUS_OK)
				ereport(LOG,
					(errmsg("process %d acquired %s on %s after %ld.%03d ms",
							MyProcPid, modename, buf.data, msecs, usecs)));
			else
			{
				Assert(myWaitStatus == STATUS_ERROR);

				/*
				 * Currently, the deadlock checker always kicks its own
				 * process, which means that we'll only see STATUS_ERROR when
				 * deadlock_state == DS_HARD_DEADLOCK, and there's no need to
				 * print redundant messages.  But for completeness and
				 * future-proofing, print a message if it looks like someone
				 * else kicked us off the lock.
				 */
				if (deadlock_state != DS_HARD_DEADLOCK)
					ereport(LOG,
							(errmsg("process %d failed to acquire %s on %s after %ld.%03d ms",
								MyProcPid, modename, buf.data, msecs, usecs),
							 (errdetail_log_plural("Process holding the lock: %s. Wait queue: %s.",
						   "Processes holding the lock: %s. Wait queue: %s.",
												   lockHoldersNum, lock_holders_sbuf.data, lock_waiters_sbuf.data))));
			}

			/*
			 * At this point we might still need to wait for the lock. Reset
			 * state so we don't print the above messages again.
			 */
			deadlock_state = DS_NO_DEADLOCK;

			pfree(buf.data);
			pfree(lock_holders_sbuf.data);
			pfree(lock_waiters_sbuf.data);
		}
	} while (myWaitStatus == STATUS_WAITING);

	/*
	 * Disable the timers, if they are still running.  As in LockErrorCleanup,
	 * we must preserve the LOCK_TIMEOUT indicator flag: if a lock timeout has
	 * already caused QueryCancelPending to become set, we want the cancel to
	 * be reported as a lock timeout, not a user cancel.
	 */
	if (LockTimeout > 0)
	{
		DisableTimeoutParams timeouts[2];

		timeouts[0].id = DEADLOCK_TIMEOUT;
		timeouts[0].keep_indicator = false;
		timeouts[1].id = LOCK_TIMEOUT;
		timeouts[1].keep_indicator = true;
		disable_timeouts(timeouts, 2);
	}
	else
		disable_timeout(DEADLOCK_TIMEOUT, false);

	/*
	 * Re-acquire the lock table's partition lock.  We have to do this to hold
	 * off cancel/die interrupts before we can mess with lockAwaited (else we
	 * might have a missed or duplicated locallock update).
	 */
	LWLockAcquire(partitionLock, LW_EXCLUSIVE);

	/*
	 * We no longer want LockErrorCleanup to do anything.
	 */
	lockAwaited = NULL;

	/*
	 * If we got the lock, be sure to remember it in the locallock table.
	 */
	if (MyProc->waitStatus == STATUS_OK)
		GrantAwaitedLock();

	/*
	 * We don't have to do anything else, because the awaker did all the
	 * necessary update of the lock table and MyProc.
	 */
	return MyProc->waitStatus;
}


/*
 * ProcWakeup -- wake up a process by releasing its private semaphore.
 *
 *	 Also remove the process from the wait queue and set its links invalid.
 *	 RETURN: the next process in the wait queue.
 *
 * The appropriate lock partition lock must be held by caller.
 *
 * XXX: presently, this code is only used for the "success" case, and only
 * works correctly for that case.  To clean up in failure case, would need
 * to twiddle the lock's request counts too --- see RemoveFromWaitQueue.
 * Hence, in practice the waitStatus parameter must be STATUS_OK.
 */
PGPROC *
ProcWakeup(PGPROC *proc, int waitStatus)
{
	PGPROC	   *retProc;

	/* Proc should be sleeping ... */
	if (proc->links.prev == NULL ||
		proc->links.next == NULL)
		return NULL;
	Assert(proc->waitStatus == STATUS_WAITING);

	/* Save next process before we zap the list link */
	retProc = (PGPROC *) proc->links.next;

	/* Remove process from wait queue */
	SHMQueueDelete(&(proc->links));
	(proc->waitLock->waitProcs.size)--;

	/* Clean up process' state and pass it the ok/fail signal */
	proc->waitLock = NULL;
	proc->waitProcLock = NULL;
	proc->waitStatus = waitStatus;

	/* And awaken it */
	SetLatch(&proc->procLatch);

	return retProc;
}

/*
 * ProcLockWakeup -- routine for waking up processes when a lock is
 *		released (or a prior waiter is aborted).  Scan all waiters
 *		for lock, waken any that are no longer blocked.
 *
 * The appropriate lock partition lock must be held by caller.
 */
void
ProcLockWakeup(LockMethod lockMethodTable, LOCK *lock)
{
	PROC_QUEUE *waitQueue = &(lock->waitProcs);
	int			queue_size = waitQueue->size;
	PGPROC	   *proc;
	LOCKMASK	aheadRequests = 0;

	Assert(queue_size >= 0);

	if (queue_size == 0)
		return;

	proc = (PGPROC *) waitQueue->links.next;

	while (queue_size-- > 0)
	{
		LOCKMODE	lockmode = proc->waitLockMode;

		/*
		 * Waken if (a) doesn't conflict with requests of earlier waiters, and
		 * (b) doesn't conflict with already-held locks.
		 */
		if ((lockMethodTable->conflictTab[lockmode] & aheadRequests) == 0 &&
			LockCheckConflicts(lockMethodTable,
							   lockmode,
							   lock,
							   proc->waitProcLock) == STATUS_OK)
		{
			/* OK to waken */
			GrantLock(lock, proc->waitProcLock, lockmode);
			proc = ProcWakeup(proc, STATUS_OK);

			/*
			 * ProcWakeup removes proc from the lock's waiting process queue
			 * and returns the next proc in chain; don't use proc's next-link,
			 * because it's been cleared.
			 */
		}
		else
		{
			/*
			 * Cannot wake this guy. Remember his request for later checks.
			 */
			aheadRequests |= LOCKBIT_ON(lockmode);
			proc = (PGPROC *) proc->links.next;
		}
	}

	Assert(waitQueue->size >= 0);
}

/*
 * CheckDeadLock
 *
 * We only get to this routine, if DEADLOCK_TIMEOUT fired while waiting for a
 * lock to be released by some other process.  Check if there's a deadlock; if
 * not, just return.  (But signal ProcSleep to log a message, if
 * log_lock_waits is true.)  If we have a real deadlock, remove ourselves from
 * the lock's wait queue and signal an error to ProcSleep.
 */
static void
CheckDeadLock(void)
{
	int			i;

	/*
	 * Acquire exclusive lock on the entire shared lock data structures. Must
	 * grab LWLocks in partition-number order to avoid LWLock deadlock.
	 *
	 * Note that the deadlock check interrupt had better not be enabled
	 * anywhere that this process itself holds lock partition locks, else this
	 * will wait forever.  Also note that LWLockAcquire creates a critical
	 * section, so that this routine cannot be interrupted by cancel/die
	 * interrupts.
	 */
	for (i = 0; i < NUM_LOCK_PARTITIONS; i++)
		LWLockAcquire(LockHashPartitionLockByIndex(i), LW_EXCLUSIVE);

	/*
	 * Check to see if we've been awoken by anyone in the interim.
	 *
	 * If we have, we can return and resume our transaction -- happy day.
	 * Before we are awoken the process releasing the lock grants it to us so
	 * we know that we don't have to wait anymore.
	 *
	 * We check by looking to see if we've been unlinked from the wait queue.
	 * This is quicker than checking our semaphore's state, since no kernel
	 * call is needed, and it is safe because we hold the lock partition lock.
	 */
	if (MyProc->links.prev == NULL ||
		MyProc->links.next == NULL)
		goto check_done;

#ifdef LOCK_DEBUG
	if (Debug_deadlocks)
		DumpAllLocks();
#endif

	/* Run the deadlock check, and set deadlock_state for use by ProcSleep */
	deadlock_state = DeadLockCheck(MyProc);

	if (deadlock_state == DS_HARD_DEADLOCK)
	{
		/*
		 * Oops.  We have a deadlock.
		 *
		 * Get this process out of wait state.	(Note: we could do this more
		 * efficiently by relying on lockAwaited, but use this coding to
		 * preserve the flexibility to kill some other transaction than the
		 * one detecting the deadlock.)
		 *
		 * RemoveFromWaitQueue sets MyProc->waitStatus to STATUS_ERROR, so
		 * ProcSleep will report an error after we return from the signal
		 * handler.
		 */
		Assert(MyProc->waitLock != NULL);
		if (Gp_role == GP_ROLE_DISPATCH && IsResQueueEnabled() &&
			LOCK_LOCKMETHOD(*(MyProc->waitLock)) == RESOURCE_LOCKMETHOD)
		{
			ResRemoveFromWaitQueue(MyProc, 
								   LockTagHashCode(&(MyProc->waitLock->tag)));
		}
		else
		{
			RemoveFromWaitQueue(MyProc, LockTagHashCode(&(MyProc->waitLock->tag)));
		}

		/*
		 * We're done here.  Transaction abort caused by the error that
		 * ProcSleep will raise will cause any other locks we hold to be
		 * released, thus allowing other processes to wake up; we don't need
		 * to do that here.  NOTE: an exception is that releasing locks we
		 * hold doesn't consider the possibility of waiters that were blocked
		 * behind us on the lock we just failed to get, and might now be
		 * wakable because we're not in front of them anymore.  However,
		 * RemoveFromWaitQueue took care of waking up any such processes.
		 */
	}

	/*
	 * And release locks.  We do this in reverse order for two reasons: (1)
	 * Anyone else who needs more than one of the locks will be trying to lock
	 * them in increasing order; we don't want to release the other process
	 * until it can get all the locks it needs. (2) This avoids O(N^2)
	 * behavior inside LWLockRelease.
	 */
check_done:
	for (i = NUM_LOCK_PARTITIONS; --i >= 0;)
		LWLockRelease(LockHashPartitionLockByIndex(i));
}

/*
 * CheckDeadLockAlert - Handle the expiry of deadlock_timeout.
 *
 * NB: Runs inside a signal handler, be careful.
 */
void
CheckDeadLockAlert(void)
{
	int			save_errno = errno;

	got_deadlock_timeout = true;

	/*
	 * Have to set the latch again, even if handle_sig_alarm already did. Back
	 * then got_deadlock_timeout wasn't yet set... It's unlikely that this
	 * ever would be a problem, but setting a set latch again is cheap.
	 */
	SetLatch(MyLatch);
	errno = save_errno;
}

/*
 * ProcWaitForSignal - wait for a signal from another backend.
 *
 * As this uses the generic process latch the caller has to be robust against
 * unrelated wakeups: Always check that the desired state has occurred, and
 * wait again if not.
 */
void
ProcWaitForSignal(void)
{
	WaitLatch(MyLatch, WL_LATCH_SET, 0);
	ResetLatch(MyLatch);
	CHECK_FOR_INTERRUPTS();
}

/*
 * ProcSendSignal - send a signal to a backend identified by PID
 */
void
ProcSendSignal(int pid)
{
	PGPROC	   *proc = NULL;

	if (RecoveryInProgress())
	{
		/* use volatile pointer to prevent code rearrangement */
		volatile PROC_HDR *procglobal = ProcGlobal;

		SpinLockAcquire(ProcStructLock);

		/*
		 * Check to see whether it is the Startup process we wish to signal.
		 * This call is made by the buffer manager when it wishes to wake up a
		 * process that has been waiting for a pin in so it can obtain a
		 * cleanup lock using LockBufferForCleanup(). Startup is not a normal
		 * backend, so BackendPidGetProc() will not return any pid at all. So
		 * we remember the information for this special case.
		 */
		if (pid == procglobal->startupProcPid)
			proc = procglobal->startupProc;

		SpinLockRelease(ProcStructLock);
	}

	if (proc == NULL)
		proc = BackendPidGetProc(pid);

	if (proc != NULL)
<<<<<<< HEAD
		PGSemaphoreUnlock(&proc->sem);
}


/*****************************************************************************
 * SIGALRM interrupt support
 *
 * Maybe these should be in pqsignal.c?
 *****************************************************************************/

/*
 * Enable the SIGALRM interrupt to fire after the specified delay
 *
 * Delay is given in milliseconds.	Caller should be sure a SIGALRM
 * signal handler is installed before this is called.
 *
 * This code properly handles nesting of deadlock timeout alarms within
 * statement timeout alarms.
 *
 * Returns TRUE if okay, FALSE on failure.
 */
bool
enable_sig_alarm(int delayms, bool is_statement_timeout)
{
	TimestampTz fin_time;
	struct itimerval timeval;

	if (is_statement_timeout)
	{
		/*
		 * Begin statement-level timeout
		 *
		 * Note that we compute statement_fin_time with reference to the
		 * statement_timestamp, but apply the specified delay without any
		 * correction; that is, we ignore whatever time has elapsed since
		 * statement_timestamp was set.  In the normal case only a small
		 * interval will have elapsed and so this doesn't matter, but there
		 * are corner cases (involving multi-statement query strings with
		 * embedded COMMIT or ROLLBACK) where we might re-initialize the
		 * statement timeout long after initial receipt of the message. In
		 * such cases the enforcement of the statement timeout will be a bit
		 * inconsistent.  This annoyance is judged not worth the cost of
		 * performing an additional gettimeofday() here.
		 */
		Assert(!deadlock_timeout_active);
		fin_time = GetCurrentStatementStartTimestamp();
		fin_time = TimestampTzPlusMilliseconds(fin_time, delayms);
		statement_fin_time = fin_time;
		cancel_from_timeout = false;
		statement_timeout_active = true;
	}
	else if (statement_timeout_active)
	{
		/*
		 * Begin deadlock timeout with statement-level timeout active
		 *
		 * Here, we want to interrupt at the closer of the two timeout times.
		 * If fin_time >= statement_fin_time then we need not touch the
		 * existing timer setting; else set up to interrupt at the deadlock
		 * timeout time.
		 *
		 * NOTE: in this case it is possible that this routine will be
		 * interrupted by the previously-set timer alarm.  This is okay
		 * because the signal handler will do only what it should do according
		 * to the state variables.	The deadlock checker may get run earlier
		 * than normal, but that does no harm.
		 */
		timeout_start_time = GetCurrentTimestamp();
		fin_time = TimestampTzPlusMilliseconds(timeout_start_time, delayms);
		deadlock_timeout_active = true;
		if (fin_time >= statement_fin_time)
			return true;
	}
	else
	{
		/* Begin deadlock timeout with no statement-level timeout */
		deadlock_timeout_active = true;
		/* GetCurrentTimestamp can be expensive, so only do it if we must */
		if (log_lock_waits)
			timeout_start_time = GetCurrentTimestamp();
	}

	/* If we reach here, okay to set the timer interrupt */
	MemSet(&timeval, 0, sizeof(struct itimerval));
	timeval.it_value.tv_sec = delayms / 1000;
	timeval.it_value.tv_usec = (delayms % 1000) * 1000;
	if (setitimer(ITIMER_REAL, &timeval, NULL))
		return false;
	return true;
}

/*
 * Cancel the SIGALRM timer, either for a deadlock timeout or a statement
 * timeout.  If a deadlock timeout is canceled, any active statement timeout
 * remains in force.
 *
 * Returns TRUE if okay, FALSE on failure.
 */
bool
disable_sig_alarm(bool is_statement_timeout)
{
	/*
	 * Always disable the interrupt if it is active; this avoids being
	 * interrupted by the signal handler and thereby possibly getting
	 * confused.
	 *
	 * We will re-enable the interrupt if necessary in CheckStatementTimeout.
	 */
	if (statement_timeout_active || deadlock_timeout_active)
	{
		struct itimerval timeval;

		MemSet(&timeval, 0, sizeof(struct itimerval));
		if (setitimer(ITIMER_REAL, &timeval, NULL))
		{
			statement_timeout_active = false;
			cancel_from_timeout = false;
			deadlock_timeout_active = false;
			return false;
		}
	}

	/* Always cancel deadlock timeout, in case this is error cleanup */
	deadlock_timeout_active = false;

	/* Cancel or reschedule statement timeout */
	if (is_statement_timeout)
	{
		statement_timeout_active = false;
		cancel_from_timeout = false;
	}
	else if (statement_timeout_active)
	{
		if (!CheckStatementTimeout())
			return false;
	}
	return true;
}

/*
 * Check for statement timeout.  If the timeout time has come,
 * trigger a query-cancel interrupt; if not, reschedule the SIGALRM
 * interrupt to occur at the right time.
 *
 * Returns true if okay, false if failed to set the interrupt.
 */
static bool
CheckStatementTimeout(void)
{
	TimestampTz now;

	if (!statement_timeout_active)
		return true;			/* do nothing if not active */

	/* QD takes care of timeouts for QE. */
	if (Gp_role == GP_ROLE_EXECUTE)
		return true;

	now = GetCurrentTimestamp();

	if (now >= statement_fin_time)
	{
		/* Time to die */
		statement_timeout_active = false;
		cancel_from_timeout = true;
#ifdef HAVE_SETSID
		/* try to signal whole process group */
		kill(-MyProcPid, SIGINT);
#endif
		kill(MyProcPid, SIGINT);
	}
	else
	{
		/* Not time yet, so (re)schedule the interrupt */
		long		secs;
		int			usecs;
		struct itimerval timeval;

		TimestampDifference(now, statement_fin_time,
							&secs, &usecs);

		/*
		 * It's possible that the difference is less than a microsecond;
		 * ensure we don't cancel, rather than set, the interrupt.
		 */
		if (secs == 0 && usecs == 0)
			usecs = 1;
		MemSet(&timeval, 0, sizeof(struct itimerval));
		timeval.it_value.tv_sec = secs;
		timeval.it_value.tv_usec = usecs;
		if (setitimer(ITIMER_REAL, &timeval, NULL))
			return false;
	}

	return true;
}

/*
 * need DoingCommandRead to be extern so we can test it here.
 * Or would it be better to have some routine to call to get the
 * value of the bool?  This is simpler.
 */
extern bool DoingCommandRead;

/*
 * Signal handler for SIGALRM for normal user backends
 *
 * Process deadlock check and/or statement timeout check, as needed.
 * To avoid various edge cases, we must be careful to do nothing
 * when there is nothing to be done.  We also need to be able to
 * reschedule the timer interrupt if called before end of statement.
 */
void
handle_sig_alarm(SIGNAL_ARGS)
{
	int			save_errno = errno;

	/* SIGALRM is cause for waking anything waiting on the process latch */
	if (MyProc)
		SetLatch(&MyProc->procLatch);

	/* don't joggle the elbow of proc_exit */
	if (!proc_exit_inprogress)
	{
		/*
		 * Idle session timeout shares with the deadlock timeout.
		 * If DoingCommandRead is true, we are deciding the session is idle
		 * In that case, we can't possibly be in a deadlock, so no point
		 * in running the deadlock detection.
		 */

		if (deadlock_timeout_active && !DoingCommandRead)
		{
			deadlock_timeout_active = false;
			CheckDeadLock();
		}

		if (statement_timeout_active)
			(void) CheckStatementTimeout();

		/*
		 * If we are DoingCommandRead, it means we are sitting idle waiting for
		 * the user to send us some SQL.
		 */
		if (DoingCommandRead)
		{
			(void) ClientWaitTimeoutInterruptHandler();
			deadlock_timeout_active = false;
		}
	}

	errno = save_errno;
}

static void
ClientWaitTimeoutInterruptHandler(void)
{
	int save_errno = errno;

	/* Don't joggle the elbow of proc_exit */
	if (proc_exit_inprogress)
		return;

	if (clientWaitTimeoutInterruptEnabled)
	{
		bool save_ImmediateInterruptOK = ImmediateInterruptOK;

		/*
		 * We may be called while ImmediateInterruptOK is true; turn it off
		 * while messing with the client wait timeout state.
		 */
		ImmediateInterruptOK = false;

		/*
		 * I'm not sure whether some flavors of Unix might allow another
		 * SIGALRM occurrence to recursively interrupt this routine. To cope
		 * with the possibility, we do the same sort of dance that
		 * EnableNotifyInterrupt must do -- see that routine for comments.
		 */
		clientWaitTimeoutInterruptEnabled = 0; /* disable any recursive signal */
		clientWaitTimeoutInterruptOccurred = 1; /* do at least one iteration */
		for (;;)
		{
			clientWaitTimeoutInterruptEnabled = 1;
			if (!clientWaitTimeoutInterruptOccurred)
				break;
			clientWaitTimeoutInterruptEnabled = 0;
			if (clientWaitTimeoutInterruptOccurred)
			{
				ProcessClientWaitTimeout();
			}
		}

		/*
		 * Restore ImmediateInterruptOK, and check for interrupts if needed.
		 */
		ImmediateInterruptOK = save_ImmediateInterruptOK;
		if (save_ImmediateInterruptOK)
			CHECK_FOR_INTERRUPTS();
	}
	else
	{
		/*
		 * In this path it is NOT SAFE to do much of anything, except this:
		 */
		clientWaitTimeoutInterruptOccurred = 1;
	}

	errno = save_errno;
}

void
EnableClientWaitTimeoutInterrupt(void)
{
	for (;;)
	{
		clientWaitTimeoutInterruptEnabled = 1;
		if (!clientWaitTimeoutInterruptOccurred)
			break;
		clientWaitTimeoutInterruptEnabled = 0;
		if (clientWaitTimeoutInterruptOccurred)
		{
			ProcessClientWaitTimeout();
		}
	}
}

bool
DisableClientWaitTimeoutInterrupt(void)
{
	bool result = (clientWaitTimeoutInterruptEnabled != 0);

	clientWaitTimeoutInterruptEnabled = 0;

	return result;
}

static void
ProcessClientWaitTimeout(void)
{
	bool notify_enabled;
	bool catchup_enabled;

	/* Must prevent SIGUSR1 and SIGUSR2 interrupt while I am running */
	notify_enabled = DisableNotifyInterrupt();
	catchup_enabled = DisableCatchupInterrupt();

	clientWaitTimeoutInterruptOccurred = 0;

	DoIdleResourceCleanup();

	if (notify_enabled)
		EnableNotifyInterrupt();
	if (catchup_enabled)
		EnableCatchupInterrupt();
}

/*
 * ResProcSleep -- put a process to sleep (that is waiting for a resource lock).
 *
 * Notes:
 * 	Locktable's masterLock must be held at entry, and will be held
 * 	at exit.
 *
 *	This is merely a version of ProcSleep modified for resource locks.
 *	The logic here could have been merged into ProcSleep, however it was
 *	requested to keep as much as possible of this resource lock code 
 *	seperate from its standard lock relatives - in the interest of not
 *	introducing new bugs or performance regressions into the lock code.
 */
int
ResProcSleep(LOCKMODE lockmode, LOCALLOCK *locallock, void *incrementSet)
{
	LOCK	   *lock = locallock->lock;
	PROCLOCK   *proclock = locallock->proclock;
	PROC_QUEUE	*waitQueue = &(lock->waitProcs);
	PGPROC		*proc;
	uint32		hashcode = locallock->hashcode;
	LWLockId	partitionLock = LockHashPartitionLock(hashcode);

	bool		selflock = true;		/* initialize result for error. */

	/*
	 * Don't check my held locks, as we just add at the end of the queue.
	 */
	proc = (PGPROC *) &(waitQueue->links);
	SHMQueueInsertBefore(&(proc->links), &(MyProc->links));
	waitQueue->size++;

	lock->waitMask |= LOCKBIT_ON(lockmode);

	/*
	 * reflect this in PGPROC object, too.
	 */
	MyProc->waitLock = lock;
	MyProc->waitProcLock = (PROCLOCK *) proclock;
	MyProc->waitLockMode = lockmode;

	MyProc->waitStatus = STATUS_ERROR;	/* initialize result for error */

	/* Now check the status of the self lock footgun. */
	selflock = ResCheckSelfDeadLock(lock, proclock, incrementSet);
	if (selflock)
	{
		LWLockRelease(partitionLock);
		ereport(ERROR,
				(errcode(ERRCODE_T_R_DEADLOCK_DETECTED),
				 errmsg("deadlock detected, locking against self")));
	}

	/* Mark that we are waiting for a lock */
	lockAwaited = locallock;

	/* Ok to wait.*/
	LWLockRelease(partitionLock);

	if (!enable_sig_alarm(DeadlockTimeout, false))
   		elog(FATAL, "could not set timer for (resource lock) process wakeup");

	/*
	 * Sleep on the semaphore.
	 */
	PGSemaphoreLock(&MyProc->sem, true);

	if (!disable_sig_alarm(false))
		elog(FATAL, "could not disable timer for (resource lock) process wakeup");

	/*
	 * Have been awakened, so continue.
	 */
	LWLockAcquire(partitionLock, LW_EXCLUSIVE);

	/*
	 * We no longer want (Res)LockWaitCancel to do anything.
	 */
	lockAwaited = NULL;

	return MyProc->waitStatus;
}


/*
 * ResLockWaitCancel -- Cancel any pending wait for a resource lock, when 
 *	aborting a transaction.
 */
void
ResLockWaitCancel(void)
{
	LWLockId	partitionLock;

	if (lockAwaited != NULL)
	{
		/* Unlink myself from the wait queue, if on it  */
		partitionLock = LockHashPartitionLock(lockAwaited->hashcode);
		LWLockAcquire(partitionLock, LW_EXCLUSIVE);

		if (MyProc->links.next != NULL)
		{
			/* We could not have been granted the lock yet */
			Assert(MyProc->waitStatus == STATUS_ERROR);

			/* We should only be trying to cancel resource locks. */
			Assert(LOCALLOCK_LOCKMETHOD(*lockAwaited) == RESOURCE_LOCKMETHOD);

			ResRemoveFromWaitQueue(MyProc, lockAwaited->hashcode);
		}

		lockAwaited = NULL;

		LWLockRelease(partitionLock);
	}

	/*
	 * Reset the proc wait semaphore to zero. This is necessary in the
	 * scenario where someone else granted us the lock we wanted before we
	 * were able to remove ourselves from the wait-list.
	 */
	PGSemaphoreReset(&MyProc->sem);

	return;
}

bool ProcCanSetMppSessionId(void)
{
	if (ProcGlobal == NULL || MyProc == NULL)
		return false;

	return true;
}


void ProcNewMppSessionId(int *newSessionId)
{
	Assert(newSessionId != NULL);

    *newSessionId = MyProc->mppSessionId =
		pg_atomic_add_fetch_u32((pg_atomic_uint32 *)&ProcGlobal->mppLocalProcessCounter, 1);

    /*
     * Make sure that our SessionState entry correctly records our
     * new session id.
     */
    if (NULL != MySessionState)
    {
    	/* This should not happen outside of dispatcher on the master */
    	Assert(IS_QUERY_DISPATCHER() && Gp_role == GP_ROLE_DISPATCH);

    	ereport(gp_sessionstate_loglevel, (errmsg("ProcNewMppSessionId: changing session id (old: %d, new: %d), pinCount: %d, activeProcessCount: %d",
    			MySessionState->sessionId, *newSessionId, MySessionState->pinCount, MySessionState->activeProcessCount), errprintstack(true)));

#ifdef USE_ASSERT_CHECKING
    	MySessionState->isModifiedSessionId = true;
#endif

    	MySessionState->sessionId = *newSessionId;
    }
}

/*
 * Signal handler for SIGALRM in Startup process
 *
 * To avoid various edge cases, we must be careful to do nothing
 * when there is nothing to be done.  We also need to be able to
 * reschedule the timer interrupt if called before end of statement.
 *
 * We set either deadlock_timeout_active or statement_timeout_active
 * or both. Interrupts are enabled if standby_timeout_active.
 */
bool
enable_standby_sig_alarm(TimestampTz now, TimestampTz fin_time, bool deadlock_only)
{
	TimestampTz deadlock_time = TimestampTzPlusMilliseconds(now,
															DeadlockTimeout);

	if (deadlock_only)
	{
		/*
		 * Wake up at deadlock_time only, then wait forever
		 */
		statement_fin_time = deadlock_time;
		deadlock_timeout_active = true;
		statement_timeout_active = false;
	}
	else if (fin_time > deadlock_time)
	{
		/*
		 * Wake up at deadlock_time, then again at fin_time
		 */
		statement_fin_time = deadlock_time;
		statement_fin_time2 = fin_time;
		deadlock_timeout_active = true;
		statement_timeout_active = true;
	}
	else
	{
		/*
		 * Wake only at fin_time because its fairly soon
		 */
		statement_fin_time = fin_time;
		deadlock_timeout_active = false;
		statement_timeout_active = true;
	}

	if (deadlock_timeout_active || statement_timeout_active)
=======
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
	{
		SetLatch(&proc->procLatch);
	}
}
