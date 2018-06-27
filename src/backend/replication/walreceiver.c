/*-------------------------------------------------------------------------
 *
 * walreceiver.c
 *
 * The WAL receiver process (walreceiver) is new as of Postgres 9.0. It
 * is the process in the standby server that takes charge of receiving
 * XLOG records from a primary server during streaming replication.
 *
 * When the startup process determines that it's time to start streaming,
 * it instructs postmaster to start walreceiver. Walreceiver first connects
 * to the primary server (it will be served by a walsender process
 * in the primary server), and then keeps receiving XLOG records and
 * writing them to the disk as long as the connection is alive. As XLOG
 * records are received and flushed to disk, it updates the
 * WalRcv->receivedUpto variable in shared memory, to inform the startup
 * process of how far it can proceed with XLOG replay.
 *
 * If the primary server ends streaming, but doesn't disconnect, walreceiver
 * goes into "waiting" mode, and waits for the startup process to give new
 * instructions. The startup process will treat that the same as
 * disconnection, and will rescan the archive/pg_xlog directory. But when the
 * startup process wants to try streaming replication again, it will just
 * nudge the existing walreceiver process that's waiting, instead of launching
 * a new one.
 *
 * Normal termination is by SIGTERM, which instructs the walreceiver to
 * exit(0). Emergency termination is by SIGQUIT; like any postmaster child
 * process, the walreceiver will simply abort and exit on SIGQUIT. A close
 * of the connection and a FATAL error are treated not as a crash but as
 * normal operation.
 *
 * This file contains the server-facing parts of walreceiver. The libpq-
 * specific parts are in the libpqwalreceiver module. It's loaded
 * dynamically to avoid linking the server with libpq.
 *
 * Portions Copyright (c) 2010-2015, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/replication/walreceiver.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <signal.h>
#include <unistd.h>

#include "access/timeline.h"
#include "access/transam.h"
#include "access/xlog_internal.h"
#include "libpq/pqformat.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "replication/walreceiver.h"
#include "replication/walsender.h"
#include "storage/ipc.h"
#include "storage/pmsignal.h"
#include "storage/procarray.h"
#include "utils/guc.h"
#include "utils/ps_status.h"
#include "utils/resowner.h"
#include "utils/timestamp.h"
#include "utils/faultinjector.h"


/* GUC variables */
<<<<<<< HEAD
int			wal_receiver_status_interval = 10;
bool		hot_standby_feedback;

=======
int			wal_receiver_status_interval;
int			wal_receiver_timeout;
bool		hot_standby_feedback;

/* libpqreceiver hooks to these when loaded */
walrcv_connect_type walrcv_connect = NULL;
walrcv_identify_system_type walrcv_identify_system = NULL;
walrcv_startstreaming_type walrcv_startstreaming = NULL;
walrcv_endstreaming_type walrcv_endstreaming = NULL;
walrcv_readtimelinehistoryfile_type walrcv_readtimelinehistoryfile = NULL;
walrcv_receive_type walrcv_receive = NULL;
walrcv_send_type walrcv_send = NULL;
walrcv_disconnect_type walrcv_disconnect = NULL;
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

#define NAPTIME_PER_CYCLE 100	/* max sleep time between cycles (100ms) */

/*
 * These variables are used similarly to openLogFile/SegNo/Off,
 * but for walreceiver to write the XLOG. recvFileTLI is the TimeLineID
 * corresponding the filename of recvFile.
 */
static int	recvFile = -1;
<<<<<<< HEAD
static TimeLineID	recvFileTLI = 0;
static uint32 recvId = 0;
static uint32 recvSeg = 0;
=======
static TimeLineID recvFileTLI = 0;
static XLogSegNo recvSegNo = 0;
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
static uint32 recvOff = 0;

/*
 * Flags set by interrupt handlers of walreceiver for later service in the
 * main loop.
 */
static volatile sig_atomic_t got_SIGHUP = false;
static volatile sig_atomic_t got_SIGTERM = false;

/* Following flags should be strictly used for testing purposes ONLY */
static volatile sig_atomic_t wait_before_rcv = false;
static volatile sig_atomic_t wait_before_send = false;
static volatile sig_atomic_t wait_before_send_ack = false;
static volatile sig_atomic_t resume = false;

/*
 * LogstreamResult indicates the byte positions that we have already
 * written/fsynced.
 */
static struct
{
	XLogRecPtr	Write;			/* last byte + 1 written out in the standby */
	XLogRecPtr	Flush;			/* last byte + 1 flushed in the standby */
}	LogstreamResult;

static StringInfoData reply_message;
static StringInfoData incoming_message;

/*
 * About SIGTERM handling:
 *
 * We can't just exit(1) within SIGTERM signal handler, because the signal
 * might arrive in the middle of some critical operation, like while we're
 * holding a spinlock. We also can't just set a flag in signal handler and
 * check it in the main loop, because we perform some blocking operations
 * like libpqrcv_PQexec(), which can take a long time to finish.
 *
 * We use a combined approach: When WalRcvImmediateInterruptOK is true, it's
 * safe for the signal handler to elog(FATAL) immediately. Otherwise it just
 * sets got_SIGTERM flag, which is checked in the main loop when convenient.
 *
 * This is very much like what regular backends do with ImmediateInterruptOK,
 * ProcessInterrupts() etc.
 */
static volatile bool WalRcvImmediateInterruptOK = false;

/* Prototypes for private functions */
static void ProcessWalRcvInterrupts(void);
static void EnableWalRcvImmediateExit(void);
static void DisableWalRcvImmediateExit(void);
static void WalRcvFetchTimeLineHistoryFiles(TimeLineID first, TimeLineID last);
static void WalRcvWaitForStartPosition(XLogRecPtr *startpoint, TimeLineID *startpointTLI);
static void WalRcvDie(int code, Datum arg);
static void XLogWalRcvProcessMsg(unsigned char type, char *buf, Size len);
static void XLogWalRcvWrite(char *buf, Size nbytes, XLogRecPtr recptr);
static void XLogWalRcvFlush(bool dying);
static void XLogWalRcvSendReply(bool force, bool requestReply);
static void XLogWalRcvSendHSFeedback(bool immed);
static void ProcessWalSndrMessage(XLogRecPtr walEnd, TimestampTz sendTime);

/* Signal handlers */
static void WalRcvSigHupHandler(SIGNAL_ARGS);
static void WalRcvSigUsr1Handler(SIGNAL_ARGS);
static void WalRcvShutdownHandler(SIGNAL_ARGS);
static void WalRcvQuickDieHandler(SIGNAL_ARGS);
static void WalRcvUsr2Handler(SIGNAL_ARGS);
static void WalRcvCrashHandler(SIGNAL_ARGS);


static void
ProcessWalRcvInterrupts(void)
{
	/*
	 * Although walreceiver interrupt handling doesn't use the same scheme as
	 * regular backends, call CHECK_FOR_INTERRUPTS() to make sure we receive
	 * any incoming signals on Win32.
	 */
	CHECK_FOR_INTERRUPTS();

	if (got_SIGTERM)
	{
		WalRcvImmediateInterruptOK = false;
		ereport(FATAL,
				(errcode(ERRCODE_ADMIN_SHUTDOWN),
				 errmsg("terminating walreceiver process due to administrator command")));
	}
}

static void
EnableWalRcvImmediateExit(void)
{
	WalRcvImmediateInterruptOK = true;
	ProcessWalRcvInterrupts();
}

static void
DisableWalRcvImmediateExit(void)
{
	WalRcvImmediateInterruptOK = false;
	ProcessWalRcvInterrupts();
}

/* Main entry point for walreceiver process */
void
WalReceiverMain(void)
{
	char		conninfo[MAXCONNINFO];
	char		slotname[NAMEDATALEN];
	XLogRecPtr	startpoint;
<<<<<<< HEAD
	File		pid_file;

	/* use volatile pointer to prevent code rearrangement */
	volatile WalRcvData *walrcv = WalRcv;
	sigjmp_buf	local_sigjmp_buf;
=======
	TimeLineID	startpointTLI;
	TimeLineID	primaryTLI;
	bool		first_stream;

	/* use volatile pointer to prevent code rearrangement */
	volatile WalRcvData *walrcv = WalRcv;
	TimestampTz last_recv_timestamp;
	bool		ping_sent;
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

	/*
	 * WalRcv should be set up already (if we are a backend, we inherit this
	 * by fork() or EXEC_BACKEND mechanism from the postmaster).
	 */
	Assert(walrcv != NULL);

	/*
	 * Mark walreceiver as running in shared memory.
	 *
	 * Do this as early as possible, so that if we fail later on, we'll set
	 * state to STOPPED. If we die before this, the startup process will keep
	 * waiting for us to start up, until it times out.
	 */
	SpinLockAcquire(&walrcv->mutex);
	Assert(walrcv->pid == 0);
	switch (walrcv->walRcvState)
	{
		case WALRCV_STOPPING:
			/* If we've already been requested to stop, don't start up. */
			walrcv->walRcvState = WALRCV_STOPPED;
			/* fall through */

		case WALRCV_STOPPED:
			SpinLockRelease(&walrcv->mutex);
			proc_exit(1);
			break;

		case WALRCV_STARTING:
			/* The usual case */
			break;

		case WALRCV_WAITING:
		case WALRCV_STREAMING:
		case WALRCV_RESTARTING:
		default:
			/* Shouldn't happen */
			elog(PANIC, "walreceiver still running according to shared memory state");
	}
	/* Advertise our PID so that the startup process can kill us */
	walrcv->pid = MyProcPid;
	walrcv->walRcvState = WALRCV_STREAMING;

	elogif(debug_walrepl_rcv, LOG,
			"WAL receiver state is set to '%s'",
			WalRcvGetStateString(walrcv->walRcvState));

	/* Fetch information required to start streaming */
	strlcpy(conninfo, (char *) walrcv->conninfo, MAXCONNINFO);
	strlcpy(slotname, (char *) walrcv->slotname, NAMEDATALEN);
	startpoint = walrcv->receiveStart;
	startpointTLI = walrcv->receiveStartTLI;

	/* Initialise to a sanish value */
	walrcv->lastMsgSendTime = walrcv->lastMsgReceiptTime = walrcv->latestWalEndTime = GetCurrentTimestamp();

	SpinLockRelease(&walrcv->mutex);

	/* Arrange to clean up at walreceiver exit */
	on_shmem_exit(WalRcvDie, 0);

	OwnLatch(&walrcv->latch);

	/* Properly accept or ignore signals the postmaster might send us */
	pqsignal(SIGHUP, WalRcvSigHupHandler);		/* set flag to read config
												 * file */
	pqsignal(SIGINT, SIG_IGN);
	pqsignal(SIGTERM, WalRcvShutdownHandler);	/* request shutdown */
	pqsignal(SIGQUIT, WalRcvQuickDieHandler);	/* hard crash time */
	pqsignal(SIGALRM, SIG_IGN);
	pqsignal(SIGPIPE, SIG_IGN);
<<<<<<< HEAD
	pqsignal(SIGUSR1, SIG_IGN);
	pqsignal(SIGUSR2, WalRcvUsr2Handler);
=======
	pqsignal(SIGUSR1, WalRcvSigUsr1Handler);
	pqsignal(SIGUSR2, SIG_IGN);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

	/* Reset some signals that are accepted by postmaster but not here */
	pqsignal(SIGCHLD, SIG_DFL);
	pqsignal(SIGTTIN, SIG_DFL);
	pqsignal(SIGTTOU, SIG_DFL);
	pqsignal(SIGCONT, SIG_DFL);
	pqsignal(SIGWINCH, SIG_DFL);

#ifdef SIGILL
	pqsignal(SIGILL, WalRcvCrashHandler);
#endif
#ifdef SIGSEGV
	pqsignal(SIGSEGV, WalRcvCrashHandler);
#endif
#ifdef SIGBUS
	pqsignal(SIGBUS, WalRcvCrashHandler);
#endif

	/* We allow SIGQUIT (quickdie) at all times */
	sigdelset(&BlockSig, SIGQUIT);

<<<<<<< HEAD
=======
	/* Load the libpq-specific functions */
	load_file("libpqwalreceiver", false);
	if (walrcv_connect == NULL || walrcv_startstreaming == NULL ||
		walrcv_endstreaming == NULL ||
		walrcv_identify_system == NULL ||
		walrcv_readtimelinehistoryfile == NULL ||
		walrcv_receive == NULL || walrcv_send == NULL ||
		walrcv_disconnect == NULL)
		elog(ERROR, "libpqwalreceiver didn't initialize correctly");

>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
	/*
	 * Create a resource owner to keep track of our resources (not clear that
	 * we need this, but may as well have one).
	 */
	CurrentResourceOwner = ResourceOwnerCreate(NULL, "Wal Receiver");

	/*
	 * In case of ERROR, walreceiver just dies cleanly. Startup process
	 * will invoke another one if necessary.
	 */
	if (sigsetjmp(local_sigjmp_buf, 1) != 0)
	{
		EmitErrorReport();

		proc_exit(0);
	}

	/* We can now handle ereport(ERROR) */
	PG_exception_stack = &local_sigjmp_buf;

	/* Unblock signals (they were blocked when the postmaster forked us) */
	PG_SETMASK(&UnBlockSig);

	/* Establish the connection to the primary for XLOG streaming */
	EnableWalRcvImmediateExit();
<<<<<<< HEAD

	/*
	 * (Testing Purpose) - Create a PID file under $PGDATA directory
	 * This file existence is very temporary and will be removed once the WAL receiver
	 * sets up successful connection. Verifying the presence of the PID file
	 * helps to find if the WAL Receiver was actually started or not.
	 * Again, This is just for testing purpose. With enhancements in future,
	 * this change can/will go away.
	 */
	if ((pid_file = PathNameOpenFile("wal_rcv.pid", O_RDWR | O_CREAT| PG_BINARY, 0600)) < 0)
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\" for reading: %m",
						"wal_rcv.pid")));
	}
	else
		FileClose(pid_file);

	walrcv_connect(conninfo, startpoint);

	/* (Testing Purpose) - Now drop the PID file */
	unlink("wal_rcv.pid");

	DisableWalRcvImmediateExit();

	/* Initialize LogstreamResult, reply_message */
	LogstreamResult.Write = LogstreamResult.Flush = GetXLogReplayRecPtr(NULL);
	MemSet(&reply_message, 0, sizeof(reply_message));

	/* Loop until end-of-streaming or error */
=======
	walrcv_connect(conninfo);
	DisableWalRcvImmediateExit();

	first_stream = true;
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
	for (;;)
	{
		/*
		 * Check that we're connected to a valid server using the
		 * IDENTIFY_SYSTEM replication command,
		 */
		EnableWalRcvImmediateExit();
		walrcv_identify_system(&primaryTLI);
		DisableWalRcvImmediateExit();

		/*
		 * Confirm that the current timeline of the primary is the same or
		 * ahead of ours.
		 */
		if (primaryTLI < startpointTLI)
			ereport(ERROR,
					(errmsg("highest timeline %u of the primary is behind recovery timeline %u",
							primaryTLI, startpointTLI)));

		/*
		 * Get any missing history files. We do this always, even when we're
		 * not interested in that timeline, so that if we're promoted to
		 * become the master later on, we don't select the same timeline that
		 * was already used in the current master. This isn't bullet-proof -
		 * you'll need some external software to manage your cluster if you
		 * need to ensure that a unique timeline id is chosen in every case,
		 * but let's avoid the confusion of timeline id collisions where we
		 * can.
		 */
		WalRcvFetchTimeLineHistoryFiles(startpointTLI, primaryTLI);

		/*
		 * Start streaming.
		 *
		 * We'll try to start at the requested starting point and timeline,
		 * even if it's different from the server's latest timeline. In case
		 * we've already reached the end of the old timeline, the server will
		 * finish the streaming immediately, and we will go back to await
		 * orders from the startup process. If recovery_target_timeline is
		 * 'latest', the startup process will scan pg_xlog and find the new
		 * history file, bump recovery target timeline, and ask us to restart
		 * on the new timeline.
		 */
		ThisTimeLineID = startpointTLI;
		if (walrcv_startstreaming(startpointTLI, startpoint,
								  slotname[0] != '\0' ? slotname : NULL))
		{
			bool		endofwal = false;

			if (first_stream)
				ereport(LOG,
						(errmsg("started streaming WAL from primary at %X/%X on timeline %u",
							(uint32) (startpoint >> 32), (uint32) startpoint,
								startpointTLI)));
			else
				ereport(LOG,
				   (errmsg("restarted WAL streaming at %X/%X on timeline %u",
						   (uint32) (startpoint >> 32), (uint32) startpoint,
						   startpointTLI)));
			first_stream = false;

			/* Initialize LogstreamResult and buffers for processing messages */
			LogstreamResult.Write = LogstreamResult.Flush = GetXLogReplayRecPtr(NULL);
			initStringInfo(&reply_message);
			initStringInfo(&incoming_message);

			/* Initialize the last recv timestamp */
			last_recv_timestamp = GetCurrentTimestamp();
			ping_sent = false;

			/* Loop until end-of-streaming or error */
			while (!endofwal)
			{
				char	   *buf;
				int			len;

				/*
				 * Emergency bailout if postmaster has died.  This is to avoid
				 * the necessity for manual cleanup of all postmaster
				 * children.
				 */
				if (!PostmasterIsAlive())
					exit(1);

				/*
				 * Exit walreceiver if we're not in recovery. This should not
				 * happen, but cross-check the status here.
				 */
				if (!RecoveryInProgress())
					ereport(FATAL,
							(errmsg("cannot continue WAL streaming, recovery has already ended")));

				/* Process any requests or signals received recently */
				ProcessWalRcvInterrupts();

				if (got_SIGHUP)
				{
					got_SIGHUP = false;
					ProcessConfigFile(PGC_SIGHUP);
					XLogWalRcvSendHSFeedback(true);
				}

				/* Wait a while for data to arrive */
				len = walrcv_receive(NAPTIME_PER_CYCLE, &buf);
				if (len != 0)
				{
					/*
					 * Process the received data, and any subsequent data we
					 * can read without blocking.
					 */
					for (;;)
					{
						if (len > 0)
						{
							/*
							 * Something was received from master, so reset
							 * timeout
							 */
							last_recv_timestamp = GetCurrentTimestamp();
							ping_sent = false;
							XLogWalRcvProcessMsg(buf[0], &buf[1], len - 1);
						}
						else if (len == 0)
							break;
						else if (len < 0)
						{
							ereport(LOG,
									(errmsg("replication terminated by primary server"),
									 errdetail("End of WAL reached on timeline %u at %X/%X.",
											   startpointTLI,
											   (uint32) (LogstreamResult.Write >> 32), (uint32) LogstreamResult.Write)));
							endofwal = true;
							break;
						}
						len = walrcv_receive(0, &buf);
					}

					/* Let the master know that we received some data. */
					XLogWalRcvSendReply(false, false);

					/*
					 * If we've written some records, flush them to disk and
					 * let the startup process and primary server know about
					 * them.
					 */
					XLogWalRcvFlush(false);
				}
				else
				{
					/*
					 * We didn't receive anything new. If we haven't heard
					 * anything from the server for more than
					 * wal_receiver_timeout / 2, ping the server. Also, if
					 * it's been longer than wal_receiver_status_interval
					 * since the last update we sent, send a status update to
					 * the master anyway, to report any progress in applying
					 * WAL.
					 */
					bool		requestReply = false;

					/*
					 * Check if time since last receive from standby has
					 * reached the configured limit.
					 */
					if (wal_receiver_timeout > 0)
					{
						TimestampTz now = GetCurrentTimestamp();
						TimestampTz timeout;

						timeout =
							TimestampTzPlusMilliseconds(last_recv_timestamp,
														wal_receiver_timeout);

						if (now >= timeout)
							ereport(ERROR,
									(errmsg("terminating walreceiver due to timeout")));

						/*
						 * We didn't receive anything new, for half of
						 * receiver replication timeout. Ping the server.
						 */
						if (!ping_sent)
						{
							timeout = TimestampTzPlusMilliseconds(last_recv_timestamp,
												 (wal_receiver_timeout / 2));
							if (now >= timeout)
							{
								requestReply = true;
								ping_sent = true;
							}
						}
					}

					XLogWalRcvSendReply(requestReply, requestReply);
					XLogWalRcvSendHSFeedback(false);
				}
			}

			/*
			 * The backend finished streaming. Exit streaming COPY-mode from
			 * our side, too.
			 */
			EnableWalRcvImmediateExit();
			walrcv_endstreaming(&primaryTLI);
			DisableWalRcvImmediateExit();

			/*
			 * If the server had switched to a new timeline that we didn't
			 * know about when we began streaming, fetch its timeline history
			 * file now.
			 */
			WalRcvFetchTimeLineHistoryFiles(startpointTLI, primaryTLI);
		}
		else
			ereport(LOG,
					(errmsg("primary server contains no more WAL on requested timeline %u",
							startpointTLI)));

		/*
		 * End of WAL reached on the requested timeline. Close the last
		 * segment, and await for new orders from the startup process.
		 */
		if (recvFile >= 0)
		{
			char		xlogfname[MAXFNAMELEN];

			XLogWalRcvFlush(false);
			if (close(recvFile) != 0)
				ereport(PANIC,
						(errcode_for_file_access(),
						 errmsg("could not close log segment %s: %m",
								XLogFileNameP(recvFileTLI, recvSegNo))));

			/*
			 * Create .done file forcibly to prevent the streamed segment from
			 * being archived later.
			 */
			XLogFileName(xlogfname, recvFileTLI, recvSegNo);
			if (XLogArchiveMode != ARCHIVE_MODE_ALWAYS)
				XLogArchiveForceDone(xlogfname);
			else
				XLogArchiveNotify(xlogfname);
		}
		recvFile = -1;

		elog(DEBUG1, "walreceiver ended streaming and awaits new instructions");
		WalRcvWaitForStartPosition(&startpoint, &startpointTLI);
	}
	/* not reached */
}

/*
 * Wait for startup process to set receiveStart and receiveStartTLI.
 */
static void
WalRcvWaitForStartPosition(XLogRecPtr *startpoint, TimeLineID *startpointTLI)
{
	/* use volatile pointer to prevent code rearrangement */
	volatile WalRcvData *walrcv = WalRcv;
	int			state;

	SpinLockAcquire(&walrcv->mutex);
	state = walrcv->walRcvState;
	if (state != WALRCV_STREAMING)
	{
		SpinLockRelease(&walrcv->mutex);
		if (state == WALRCV_STOPPING)
			proc_exit(0);
		else
			elog(FATAL, "unexpected walreceiver state");
	}
	walrcv->walRcvState = WALRCV_WAITING;
	walrcv->receiveStart = InvalidXLogRecPtr;
	walrcv->receiveStartTLI = 0;
	SpinLockRelease(&walrcv->mutex);

	if (update_process_title)
		set_ps_display("idle", false);

	/*
	 * nudge startup process to notice that we've stopped streaming and are
	 * now waiting for instructions.
	 */
	WakeupRecovery();
	for (;;)
	{
		ResetLatch(&walrcv->latch);

		/*
		 * Emergency bailout if postmaster has died.  This is to avoid the
		 * necessity for manual cleanup of all postmaster children.
		 */
		if (!PostmasterIsAlive())
			exit(1);

<<<<<<< HEAD
		/*
		 * Exit walreceiver if we're not in recovery. This should not happen,
		 * but cross-check the status here.
		 */
		if (!RecoveryInProgress())
			ereport(FATAL,
					(errmsg("cannot continue WAL streaming, recovery has already ended"),
					errSendAlert(true)));

		/* Process any requests or signals received recently */
=======
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
		ProcessWalRcvInterrupts();

		SpinLockAcquire(&walrcv->mutex);
		Assert(walrcv->walRcvState == WALRCV_RESTARTING ||
			   walrcv->walRcvState == WALRCV_WAITING ||
			   walrcv->walRcvState == WALRCV_STOPPING);
		if (walrcv->walRcvState == WALRCV_RESTARTING)
		{
			/* we don't expect primary_conninfo to change */
			*startpoint = walrcv->receiveStart;
			*startpointTLI = walrcv->receiveStartTLI;
			walrcv->walRcvState = WALRCV_STREAMING;
			SpinLockRelease(&walrcv->mutex);
			break;
		}
		if (walrcv->walRcvState == WALRCV_STOPPING)
		{
			/*
			 * We should've received SIGTERM if the startup process wants us
			 * to die, but might as well check it here too.
			 */
			SpinLockRelease(&walrcv->mutex);
			exit(1);
		}
		SpinLockRelease(&walrcv->mutex);

		WaitLatch(&walrcv->latch, WL_LATCH_SET | WL_POSTMASTER_DEATH, 0);
	}

	if (update_process_title)
	{
		char		activitymsg[50];

<<<<<<< HEAD
		/* Perform suspend if signaled by an external entity (Testing Purpose) */
		if (wait_before_rcv)
		{
			while(true)
			{
				if (resume)
				{
					resume = false;
					break;
				}
				pg_usleep(5000);
			}
			wait_before_rcv = false;
		}

		/* Wait a while for data to arrive */
		if (walrcv_receive(NAPTIME_PER_CYCLE, &type, &buf, &len))
=======
		snprintf(activitymsg, sizeof(activitymsg), "restarting at %X/%X",
				 (uint32) (*startpoint >> 32),
				 (uint32) *startpoint);
		set_ps_display(activitymsg, false);
	}
}

/*
 * Fetch any missing timeline history files between 'first' and 'last'
 * (inclusive) from the server.
 */
static void
WalRcvFetchTimeLineHistoryFiles(TimeLineID first, TimeLineID last)
{
	TimeLineID	tli;

	for (tli = first; tli <= last; tli++)
	{
		/* there's no history file for timeline 1 */
		if (tli != 1 && !existsTimeLineHistory(tli))
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
		{
			char	   *fname;
			char	   *content;
			int			len;
			char		expectedfname[MAXFNAMELEN];

			ereport(LOG,
					(errmsg("fetching timeline history file for timeline %u from primary server",
							tli)));

			EnableWalRcvImmediateExit();
			walrcv_readtimelinehistoryfile(tli, &fname, &content, &len);
			DisableWalRcvImmediateExit();

			/*
			 * Check that the filename on the master matches what we
			 * calculated ourselves. This is just a sanity check, it should
			 * always match.
			 */
			TLHistoryFileName(expectedfname, tli);
			if (strcmp(fname, expectedfname) != 0)
				ereport(ERROR,
						(errcode(ERRCODE_PROTOCOL_VIOLATION),
						 errmsg_internal("primary reported unexpected file name for timeline history file of timeline %u",
										 tli)));

			/*
			 * Write the file to pg_xlog.
			 */
			writeTimeLineHistoryFile(tli, content, len);

			pfree(fname);
			pfree(content);
		}
	}
}

/*
 * This is the handler for USR2 signal. Currently, it is used for testing
 * purposes. Normally, an external entity signals (USR2) the WAL receiver process
 * and based on the input from an external file on disk the WAL Receiver acts
 * upon that.
 */
static void
WalRcvUsr2Handler(SIGNAL_ARGS)
{
#define BUF_SIZE 80
	File file;
	int nbytes = 0;
	char buf[BUF_SIZE];

	/* Read the type of action to take later from the file in the $PGDATA */
	if ((file = PathNameOpenFile("wal_rcv_test", O_RDONLY | PG_BINARY, 0600)) < 0)
	{
		/* Do not error out inside signal handler. Ignore it.*/
		return;
	}
	else
	{
		nbytes = FileRead(file, buf, BUF_SIZE - 1);
		if (nbytes <= 0)
		{
			/* Cleanup */
			FileClose(file);

			/* Don't error out. Ignore. */
			return;
		}
		FileClose(file);

		Assert(nbytes < BUF_SIZE);
		buf[nbytes] = '\0';

		if (strcmp(buf,"wait_before_send_ack") == 0)
			wait_before_send_ack = true;
		else if (strcmp(buf,"wait_before_rcv") == 0)
			wait_before_rcv = true;
		else if (strcmp(buf,"wait_before_send") == 0)
			wait_before_send = true;
		else if (strcmp(buf,"resume") == 0)
			resume = true;
		else
			return;
	}
}

/*
 * Mark us as STOPPED in shared memory at exit.
 */
static void
WalRcvDie(int code, Datum arg)
{
	/* use volatile pointer to prevent code rearrangement */
	volatile WalRcvData *walrcv = WalRcv;

	/* Ensure that all WAL records received are flushed to disk */
	XLogWalRcvFlush(true);

	DisownLatch(&walrcv->latch);

	SpinLockAcquire(&walrcv->mutex);
	Assert(walrcv->walRcvState == WALRCV_STREAMING ||
		   walrcv->walRcvState == WALRCV_RESTARTING ||
		   walrcv->walRcvState == WALRCV_STARTING ||
		   walrcv->walRcvState == WALRCV_WAITING ||
		   walrcv->walRcvState == WALRCV_STOPPING);
	Assert(walrcv->pid == MyProcPid);
	walrcv->walRcvState = WALRCV_STOPPED;
	walrcv->pid = 0;
	SpinLockRelease(&walrcv->mutex);

	/* Terminate the connection gracefully. */
<<<<<<< HEAD
	walrcv_disconnect();
=======
	if (walrcv_disconnect != NULL)
		walrcv_disconnect();

	/* Wake up the startup process to notice promptly that we're gone */
	WakeupRecovery();
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
}

/* SIGHUP: set flag to re-read config file at next convenient time */
static void
WalRcvSigHupHandler(SIGNAL_ARGS)
{
	got_SIGHUP = true;
}


/* SIGUSR1: used by latch mechanism */
static void
WalRcvSigUsr1Handler(SIGNAL_ARGS)
{
	int			save_errno = errno;

	latch_sigusr1_handler();

	errno = save_errno;
}

/* SIGTERM: set flag for main loop, or shutdown immediately if safe */
static void
WalRcvShutdownHandler(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_SIGTERM = true;

	SetLatch(&WalRcv->latch);

	/* Don't joggle the elbow of proc_exit */
	if (!proc_exit_inprogress && WalRcvImmediateInterruptOK)
		ProcessWalRcvInterrupts();

	errno = save_errno;
}

/*
 * WalRcvQuickDieHandler() occurs when signalled SIGQUIT by the postmaster.
 *
 * Some backend has bought the farm, so we need to stop what we're doing and
 * exit.
 */
static void
WalRcvQuickDieHandler(SIGNAL_ARGS)
{
	PG_SETMASK(&BlockSig);

	/*
	 * We DO NOT want to run proc_exit() callbacks -- we're here because
	 * shared memory may be corrupted, so we don't want to try to clean up our
	 * transaction.  Just nail the windows shut and get out of town.  Now that
	 * there's an atexit callback to prevent third-party code from breaking
	 * things by calling exit() directly, we have to reset the callbacks
	 * explicitly to make this work as intended.
	 */
	on_exit_reset();

	/*
	 * Note we do exit(2) not exit(0).  This is to force the postmaster into a
	 * system reset cycle if some idiot DBA sends a manual SIGQUIT to a random
	 * backend.  This is necessary precisely because we don't clean up our
	 * shared memory state.  (The "dead man switch" mechanism in pmsignal.c
	 * should ensure the postmaster sees this as a crash, too, but no harm in
	 * being doubly sure.)
	 */
	exit(2);
}

static void
WalRcvCrashHandler(SIGNAL_ARGS)
{
	StandardHandlerForSigillSigsegvSigbus_OnMainThread("walreceiver",
														PASS_SIGNAL_ARGS);
}

/*
 * Accept the message from XLOG stream, and process it.
 */
static void
XLogWalRcvProcessMsg(unsigned char type, char *buf, Size len)
{
	int			hdrlen;
	XLogRecPtr	dataStart;
	XLogRecPtr	walEnd;
	TimestampTz sendTime;
	bool		replyRequested;

	resetStringInfo(&incoming_message);

	switch (type)
	{
		case 'w':				/* WAL records */
			{
				/* copy message to StringInfo */
				hdrlen = sizeof(int64) + sizeof(int64) + sizeof(int64);
				if (len < hdrlen)
					ereport(ERROR,
							(errcode(ERRCODE_PROTOCOL_VIOLATION),
<<<<<<< HEAD
							 errmsg_internal("invalid WAL message received from primary"),
							 errSendAlert(true)));
				/* memcpy is required here for alignment reasons */
				memcpy(&msghdr, buf, sizeof(WalDataMessageHeader));

				elogif(debug_walrepl_rcv, LOG,
					   "walrcv msg metadata -- datastart %s, buflen %d",
					    XLogLocationToString(&(msghdr.dataStart)),(int)len);

				ProcessWalSndrMessage(msghdr.walEnd, msghdr.sendTime);
=======
							 errmsg_internal("invalid WAL message received from primary")));
				appendBinaryStringInfo(&incoming_message, buf, hdrlen);

				/* read the fields */
				dataStart = pq_getmsgint64(&incoming_message);
				walEnd = pq_getmsgint64(&incoming_message);
				sendTime = IntegerTimestampToTimestampTz(
										  pq_getmsgint64(&incoming_message));
				ProcessWalSndrMessage(walEnd, sendTime);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

				buf += hdrlen;
				len -= hdrlen;
				XLogWalRcvWrite(buf, len, dataStart);
				break;
			}
		case 'k':				/* Keepalive */
			{
				/* copy message to StringInfo */
				hdrlen = sizeof(int64) + sizeof(int64) + sizeof(char);
				if (len != hdrlen)
					ereport(ERROR,
							(errcode(ERRCODE_PROTOCOL_VIOLATION),
<<<<<<< HEAD
							 errmsg_internal("invalid keepalive message received from primary"),
							 errSendAlert(true)));

				/* memcpy is required here for alignment reasons */
				memcpy(&keepalive, buf, sizeof(PrimaryKeepaliveMessage));
=======
							 errmsg_internal("invalid keepalive message received from primary")));
				appendBinaryStringInfo(&incoming_message, buf, hdrlen);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

				/* read the fields */
				walEnd = pq_getmsgint64(&incoming_message);
				sendTime = IntegerTimestampToTimestampTz(
										  pq_getmsgint64(&incoming_message));
				replyRequested = pq_getmsgbyte(&incoming_message);

				ProcessWalSndrMessage(walEnd, sendTime);

				/* If the primary requested a reply, send one immediately */
				if (replyRequested)
					XLogWalRcvSendReply(true, false);
				break;
			}
		default:
			ereport(ERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg_internal("invalid replication message type %d",
									 type),	errSendAlert(true)));
	}
}

/*
 * Write XLOG data to disk.
 */
static void
XLogWalRcvWrite(char *buf, Size nbytes, XLogRecPtr recptr)
{
	int			startoff;
	int			byteswritten;

	while (nbytes > 0)
	{
		int			segbytes;

		if (recvFile < 0 || !XLByteInSeg(recptr, recvSegNo))
		{
			bool		use_existent;

			/*
			 * fsync() and close current file before we switch to next one. We
			 * would otherwise have to reopen this file to fsync it later
			 */
			if (recvFile >= 0)
			{
				char		xlogfname[MAXFNAMELEN];

				XLogWalRcvFlush(false);

				/*
				 * XLOG segment files will be re-read by recovery in startup
				 * process soon, so we don't advise the OS to release cache
				 * pages associated with the file like XLogFileClose() does.
				 */
				if (close(recvFile) != 0)
					ereport(PANIC,
							(errcode_for_file_access(),
<<<<<<< HEAD
						errmsg("could not close log file %u, segment %u: %m",
							   recvId, recvSeg)));

				/*
				 * Create .done file forcibly to prevent the restored segment from
				 * being archived again later.
				 */
				XLogFileName(xlogfname, recvFileTLI, recvId, recvSeg);
=======
							 errmsg("could not close log segment %s: %m",
									XLogFileNameP(recvFileTLI, recvSegNo))));

				/*
				 * Create .done file forcibly to prevent the streamed segment
				 * from being archived later.
				 */
				XLogFileName(xlogfname, recvFileTLI, recvSegNo);
				if (XLogArchiveMode != ARCHIVE_MODE_ALWAYS)
					XLogArchiveForceDone(xlogfname);
				else
					XLogArchiveNotify(xlogfname);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
			}
			recvFile = -1;

			/* Create/use new log file */
			XLByteToSeg(recptr, recvSegNo);
			use_existent = true;
<<<<<<< HEAD
			/* For now this only happens on master, so don't care mirror */
			recvFile = XLogFileInit(recvId, recvSeg, &use_existent, true);
=======
			recvFile = XLogFileInit(recvSegNo, &use_existent, true);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
			recvFileTLI = ThisTimeLineID;
			recvOff = 0;
		}

		/* Calculate the start offset of the received logs */
		startoff = recptr % XLogSegSize;

		if (startoff + nbytes > XLogSegSize)
			segbytes = XLogSegSize - startoff;
		else
			segbytes = nbytes;

		/* Need to seek in the file? */
		if (recvOff != startoff)
		{
			if (lseek(recvFile, (off_t) startoff, SEEK_SET) < 0)
				ereport(PANIC,
						(errcode_for_file_access(),
<<<<<<< HEAD
						 errmsg("could not seek in log file %u, "
								"segment %u to offset %u: %m",
								recvId, recvSeg, startoff),
								errSendAlert(true)));
=======
				  errmsg("could not seek in log segment %s to offset %u: %m",
						 XLogFileNameP(recvFileTLI, recvSegNo),
						 startoff)));
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
			recvOff = startoff;
		}

		/* OK to write the logs */
		errno = 0;

		byteswritten = write(recvFile, buf, segbytes);
		if (byteswritten <= 0)
		{
			/* if write didn't set errno, assume no disk space */
			if (errno == 0)
				errno = ENOSPC;
			ereport(PANIC,
					(errcode_for_file_access(),
					 errmsg("could not write to log segment %s "
							"at offset %u, length %lu: %m",
<<<<<<< HEAD
							recvId, recvSeg,
							recvOff, (unsigned long) segbytes),
							errSendAlert(true)));
=======
							XLogFileNameP(recvFileTLI, recvSegNo),
							recvOff, (unsigned long) segbytes)));
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
		}

		/* Update state for write */
		recptr += byteswritten;

		recvOff += byteswritten;
		nbytes -= byteswritten;
		buf += byteswritten;

		LogstreamResult.Write = recptr;

		elogif(debug_walrepl_rcv, LOG,
			   "walrcv write -- Wrote %d bytes in file (logid %u, seg %u, offset %d)."
			   "Latest write location is %X/%X.",
			   byteswritten, recvId, recvSeg, startoff,
			   LogstreamResult.Write.xlogid, LogstreamResult.Write.xrecoff);
	}
}

/*
 * Flush the log to disk.
 *
 * If we're in the midst of dying, it's unwise to do anything that might throw
 * an error, so we skip sending a reply in that case.
 */
static void
XLogWalRcvFlush(bool dying)
{
	if (LogstreamResult.Flush < LogstreamResult.Write)
	{
		/* use volatile pointer to prevent code rearrangement */
		volatile WalRcvData *walrcv = WalRcv;

		issue_xlog_fsync(recvFile, recvSegNo);

		LogstreamResult.Flush = LogstreamResult.Write;

		/* Update shared-memory status */
		SpinLockAcquire(&walrcv->mutex);
		if (walrcv->receivedUpto < LogstreamResult.Flush)
		{
			walrcv->latestChunkStart = walrcv->receivedUpto;
			walrcv->receivedUpto = LogstreamResult.Flush;
			walrcv->receivedTLI = ThisTimeLineID;
		}
		SpinLockRelease(&walrcv->mutex);

		/* Signal the startup process and walsender that new WAL has arrived */
		WakeupRecovery();
		if (AllowCascadeReplication())
			WalSndWakeup();

		/* Report XLOG streaming progress in PS display */
		if (update_process_title)
		{
			char		activitymsg[50];

			snprintf(activitymsg, sizeof(activitymsg), "streaming %X/%X",
					 (uint32) (LogstreamResult.Write >> 32),
					 (uint32) LogstreamResult.Write);
			set_ps_display(activitymsg, false);
		}

		/* Also let the master know that we made some progress */
		if (!dying)
		{
<<<<<<< HEAD
			/* Perform suspend if signaled by an external entity (Testing Purpose) */
			if (wait_before_send_ack)
			{
				while(true)
				{
					if (resume)
					{
						resume = false;
						break;
					}
					pg_usleep(5000);
				}
				wait_before_send_ack = false;
			}
			XLogWalRcvSendReply();
			XLogWalRcvSendHSFeedback();
=======
			XLogWalRcvSendReply(false, false);
			XLogWalRcvSendHSFeedback(false);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
		}

		elogif(debug_walrepl_rcv, LOG,
			   "walrcv fsync -- Fsync'd xlog file (logid %u seg %u). "
			   "Latest flush location is %X/%X with latestchunkstart(%X/%X) receivedupto(%X/%X).",
			   recvId, recvSeg,
			   LogstreamResult.Flush.xlogid, LogstreamResult.Flush.xrecoff,
			   walrcv->latestChunkStart.xlogid, walrcv->latestChunkStart.xrecoff,
			   walrcv->receivedUpto.xlogid, walrcv->receivedUpto.xrecoff);
	}
}

/*
 * Send reply message to primary, indicating our current XLOG positions, oldest
 * xmin and the current time.
 *
 * If 'force' is not set, the message is only sent if enough time has
 * passed since last status update to reach wal_receiver_status_interval.
 * If wal_receiver_status_interval is disabled altogether and 'force' is
 * false, this is a no-op.
 *
 * If 'requestReply' is true, requests the server to reply immediately upon
 * receiving this message. This is used for heartbearts, when approaching
 * wal_receiver_timeout.
 */
static void
XLogWalRcvSendReply(bool force, bool requestReply)
{
	static XLogRecPtr writePtr = 0;
	static XLogRecPtr flushPtr = 0;
	XLogRecPtr	applyPtr;
	static TimestampTz sendTime = 0;
	TimestampTz now;

	/*
	 * If the user doesn't want status to be reported to the master, be sure
	 * to exit before doing anything at all.
	 */
	if (!force && wal_receiver_status_interval <= 0)
		return;

	/* Get current timestamp. */
	now = GetCurrentTimestamp();

	/*
	 * We can compare the write and flush positions to the last message we
	 * sent without taking any lock, but the apply position requires a spin
	 * lock, so we don't check that unless something else has changed or 10
	 * seconds have passed.  This means that the apply log position will
	 * appear, from the master's point of view, to lag slightly, but since
	 * this is only for reporting purposes and only on idle systems, that's
	 * probably OK.
	 */
	if (!force
		&& writePtr == LogstreamResult.Write
		&& flushPtr == LogstreamResult.Flush
		&& !TimestampDifferenceExceeds(sendTime, now,
									   wal_receiver_status_interval * 1000))
		return;
	sendTime = now;

	/* Construct a new message */
	writePtr = LogstreamResult.Write;
	flushPtr = LogstreamResult.Flush;
	applyPtr = GetXLogReplayRecPtr(NULL);

<<<<<<< HEAD
	/* Prepend with the message type and send it. */
	buf[0] = 'r';
	memcpy(&buf[1], &reply_message, sizeof(StandbyReplyMessage));

	/* Perform suspend if signaled by an external entity (Testing Purpose) */
	if (wait_before_send)
	{
		while(true)
		{
			if (resume)
			{
				resume = false;
				break;
			}
			pg_usleep(5000);
		}
		wait_before_send = false;
	}

	walrcv_send(buf, sizeof(StandbyReplyMessage) + 1);

	elogif(debug_walrepl_rcv, LOG,
		   "walrcv reply -- Sent write %X/%X flush %X/%X apply %X/%X",
		   reply_message.write.xlogid, reply_message.write.xrecoff,
		   reply_message.flush.xlogid, reply_message.flush.xrecoff,
		   reply_message.apply.xlogid, reply_message.apply.xrecoff);
=======
	resetStringInfo(&reply_message);
	pq_sendbyte(&reply_message, 'r');
	pq_sendint64(&reply_message, writePtr);
	pq_sendint64(&reply_message, flushPtr);
	pq_sendint64(&reply_message, applyPtr);
	pq_sendint64(&reply_message, GetCurrentIntegerTimestamp());
	pq_sendbyte(&reply_message, requestReply ? 1 : 0);

	/* Send it */
	elog(DEBUG2, "sending write %X/%X flush %X/%X apply %X/%X%s",
		 (uint32) (writePtr >> 32), (uint32) writePtr,
		 (uint32) (flushPtr >> 32), (uint32) flushPtr,
		 (uint32) (applyPtr >> 32), (uint32) applyPtr,
		 requestReply ? " (reply requested)" : "");

	walrcv_send(reply_message.data, reply_message.len);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
}

/*
 * Send hot standby feedback message to primary, plus the current time,
 * in case they don't have a watch.
 *
 * If the user disables feedback, send one final message to tell sender
 * to forget about the xmin on this standby.
 */
static void
XLogWalRcvSendHSFeedback(bool immed)
{
	TimestampTz now;
	TransactionId nextXid;
	uint32		nextEpoch;
	TransactionId xmin;
	static TimestampTz sendTime = 0;
	static bool master_has_standby_xmin = false;

	/*
	 * If the user doesn't want status to be reported to the master, be sure
	 * to exit before doing anything at all.
	 */
	if ((wal_receiver_status_interval <= 0 || !hot_standby_feedback) &&
		!master_has_standby_xmin)
		return;

	/* Get current timestamp. */
	now = GetCurrentTimestamp();

	if (!immed)
	{
		/*
		 * Send feedback at most once per wal_receiver_status_interval.
		 */
		if (!TimestampDifferenceExceeds(sendTime, now,
										wal_receiver_status_interval * 1000))
			return;
		sendTime = now;
	}

	/*
	 * If Hot Standby is not yet active there is nothing to send. Check this
	 * after the interval has expired to reduce number of calls.
	 */
	if (!HotStandbyActive())
	{
		Assert(!master_has_standby_xmin);
		return;
	}

	/*
	 * Make the expensive call to get the oldest xmin once we are certain
	 * everything else has been checked.
	 */
	if (hot_standby_feedback)
		xmin = GetOldestXmin(NULL, false);
	else
		xmin = InvalidTransactionId;

	/*
	 * Get epoch and adjust if nextXid and oldestXmin are different sides of
	 * the epoch boundary.
	 */
	GetNextXidAndEpoch(&nextXid, &nextEpoch);
	if (nextXid < xmin)
		nextEpoch--;

	elog(DEBUG2, "sending hot standby feedback xmin %u epoch %u",
		 xmin, nextEpoch);

	/* Construct the message and send it. */
	resetStringInfo(&reply_message);
	pq_sendbyte(&reply_message, 'h');
	pq_sendint64(&reply_message, GetCurrentIntegerTimestamp());
	pq_sendint(&reply_message, xmin, 4);
	pq_sendint(&reply_message, nextEpoch, 4);
	walrcv_send(reply_message.data, reply_message.len);
	if (TransactionIdIsValid(xmin))
		master_has_standby_xmin = true;
	else
		master_has_standby_xmin = false;
}

/*
<<<<<<< HEAD
 * Return a string constant representing the state.
 */
const char *
WalRcvGetStateString(WalRcvState state)
{
	switch (state)
	{
		case WALRCV_STOPPED:
			return "stopped";
		case WALRCV_STARTING:
			return "starting";
		case WALRCV_RUNNING:
			return "running";
		case WALRCV_STOPPING:
			return "stopping";
	}
	return "UNKNOWN";
}

/*
 * Keep track of important messages from primary.
=======
 * Update shared memory status upon receiving a message from primary.
 *
 * 'walEnd' and 'sendTime' are the end-of-WAL and timestamp of the latest
 * message, reported by primary.
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
 */
static void
ProcessWalSndrMessage(XLogRecPtr walEnd, TimestampTz sendTime)
{
	/* use volatile pointer to prevent code rearrangement */
	volatile WalRcvData *walrcv = WalRcv;

	TimestampTz lastMsgReceiptTime = GetCurrentTimestamp();

	/* Update shared-memory status */
	SpinLockAcquire(&walrcv->mutex);
	if (walrcv->latestWalEnd < walEnd)
		walrcv->latestWalEndTime = sendTime;
	walrcv->latestWalEnd = walEnd;
	walrcv->lastMsgSendTime = sendTime;
	walrcv->lastMsgReceiptTime = lastMsgReceiptTime;
	SpinLockRelease(&walrcv->mutex);

	if (log_min_messages <= DEBUG2)
	{
		char	   *sendtime;
		char	   *receipttime;
		int			applyDelay;

		/* Copy because timestamptz_to_str returns a static buffer */
		sendtime = pstrdup(timestamptz_to_str(sendTime));
		receipttime = pstrdup(timestamptz_to_str(lastMsgReceiptTime));
		applyDelay = GetReplicationApplyDelay();

		/* apply delay is not available */
		if (applyDelay == -1)
			elog(DEBUG2, "sendtime %s receipttime %s replication apply delay (N/A) transfer latency %d ms",
				 sendtime,
				 receipttime,
				 GetReplicationTransferLatency());
		else
			elog(DEBUG2, "sendtime %s receipttime %s replication apply delay %d ms transfer latency %d ms",
				 sendtime,
				 receipttime,
				 applyDelay,
				 GetReplicationTransferLatency());

		pfree(sendtime);
		pfree(receipttime);
	}
}
