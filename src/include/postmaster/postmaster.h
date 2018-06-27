/*-------------------------------------------------------------------------
 *
 * postmaster.h
 *	  Exports from postmaster/postmaster.c.
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/postmaster/postmaster.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef _POSTMASTER_H
#define _POSTMASTER_H

/* GUC options */
extern bool EnableSSL;
extern int	ReservedBackends;
extern int	PostPortNumber;
extern int	Unix_socket_permissions;
extern char *Unix_socket_group;
extern char *Unix_socket_directories;
extern char *ListenAddresses;
extern char *BackendListenAddress;
extern bool ClientAuthInProgress;
extern int	PreAuthDelay;
extern int	AuthenticationTimeout;
extern bool Log_connections;
extern bool log_hostname;
extern bool enable_bonjour;
extern char *bonjour_name;
extern bool restart_after_crash;

#ifdef WIN32
extern HANDLE PostmasterHandle;
#else
extern int	postmaster_alive_fds[2];

/*
 * Constants that represent which of postmaster_alive_fds is held by
 * postmaster, and which is used in children to check for postmaster death.
 */
#define POSTMASTER_FD_WATCH		0		/* used in children to check for
										 * postmaster death */
#define POSTMASTER_FD_OWN		1		/* kept open by postmaster only */
#endif

#define POSTMASTER_IN_STARTUP_MSG "the database system is starting up"
#define POSTMASTER_IN_RECOVERY_MSG "the database system is in recovery mode"
#define POSTMASTER_IN_RECOVERY_DETAIL_MSG "last replayed record at"

extern const char *progname;

<<<<<<< HEAD
/* stack base pointer, defined in postgres.c */
extern char *stack_base_ptr;

extern int	PostmasterMain(int argc, char *argv[]);
=======
extern void PostmasterMain(int argc, char *argv[]) pg_attribute_noreturn();
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
extern void ClosePostmasterPorts(bool am_syslogger);

extern int	MaxLivePostmasterChildren(void);

<<<<<<< HEAD
extern void SignalPromote(void);
extern void ResetMirrorReadyFlag(void);
=======
extern int	GetNumShmemAttachedBgworkers(void);
extern bool PostmasterMarkPIDForWorkerNotify(int);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

#ifdef EXEC_BACKEND
extern pid_t postmaster_forkexec(int argc, char *argv[]);
extern void SubPostmasterMain(int argc, char *argv[]) pg_attribute_noreturn();

extern Size ShmemBackendArraySize(void);
extern void ShmemBackendArrayAllocation(void);
#endif

<<<<<<< HEAD
/* CDB */
typedef int (PMSubStartCallback)(void);
=======
/*
 * Note: MAX_BACKENDS is limited to 2^23-1 because inval.c stores the
 * backend ID as a 3-byte signed integer.  Even if that limitation were
 * removed, we still could not exceed INT_MAX/4 because some places compute
 * 4*MaxBackends without any overflow check.  This is rechecked in the relevant
 * GUC check hooks and in RegisterBackgroundWorker().
 */
#define MAX_BACKENDS	0x7fffff
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

#endif   /* _POSTMASTER_H */
