/*
 *	pg_upgrade.h
 *
<<<<<<< HEAD:contrib/pg_upgrade/pg_upgrade.h
<<<<<<< HEAD
 *	Portions Copyright (c) 2016, Pivotal Software Inc
 *	Copyright (c) 2010-2011, PostgreSQL Global Development Group
=======
 *	Copyright (c) 2010-2012, PostgreSQL Global Development Group
>>>>>>> 80edfd76591fdb9beec061de3c05ef4e9d96ce56
 *	contrib/pg_upgrade/pg_upgrade.h
=======
 *	Copyright (c) 2010-2015, PostgreSQL Global Development Group
 *	src/bin/pg_upgrade/pg_upgrade.h
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8:src/bin/pg_upgrade/pg_upgrade.h
 */

#include <unistd.h>
#include <assert.h>
#include <sys/stat.h>
#include <sys/time.h>

#include "libpq-fe.h"

/* Use port in the private/dynamic port number range */
#define DEF_PGUPORT			50432

/* Allocate for null byte */
#define USER_NAME_SIZE		128

#define MAX_STRING			1024
#define LINE_ALLOC			4096
#define QUERY_ALLOC			8192

#define NUMERIC_ALLOC 100

#define MIGRATOR_API_VERSION	1

#define MESSAGE_WIDTH		60

#define GET_MAJOR_VERSION(v)	((v) / 100)

/* contains both global db information and CREATE DATABASE commands */
#define GLOBALS_DUMP_FILE	"pg_upgrade_dump_globals.sql"
#define DB_DUMP_FILE_MASK	"pg_upgrade_dump_%u.custom"

<<<<<<< HEAD:contrib/pg_upgrade/pg_upgrade.h
<<<<<<< HEAD
#define GLOBAL_OIDS_DUMP_FILE "pg_upgrade_dump_global_oids.sql"
#define DB_OIDS_DUMP_FILE_MASK	"pg_upgrade_dump_%u_oids.sql"


/* needs to be kept in sync with pg_class.h */
#define RELSTORAGE_EXTERNAL	'x'
=======
=======
#define DB_DUMP_LOG_FILE_MASK	"pg_upgrade_dump_%u.log"
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8:src/bin/pg_upgrade/pg_upgrade.h
#define SERVER_LOG_FILE		"pg_upgrade_server.log"
#define UTILITY_LOG_FILE	"pg_upgrade_utility.log"
#define INTERNAL_LOG_FILE	"pg_upgrade_internal.log"

extern char *output_files[];

/*
 * WIN32 files do not accept writes from multiple processes
 *
 * On Win32, we can't send both pg_upgrade output and command output to the
 * same file because we get the error: "The process cannot access the file
 * because it is being used by another process." so send the pg_ctl
 * command-line output to a new file, rather than into the server log file.
 * Ideally we could use UTILITY_LOG_FILE for this, but some Windows platforms
 * keep the pg_ctl output file open by the running postmaster, even after
 * pg_ctl exits.
 *
 * We could use the Windows pgwin32_open() flags to allow shared file
 * writes but is unclear how all other tools would use those flags, so
 * we just avoid it and log a little differently on Windows;  we adjust
 * the error message appropriately.
 */
#ifndef WIN32
#define SERVER_START_LOG_FILE	SERVER_LOG_FILE
#define SERVER_STOP_LOG_FILE	SERVER_LOG_FILE
#else
#define SERVER_START_LOG_FILE	"pg_upgrade_server_start.log"
/*
 *	"pg_ctl start" keeps SERVER_START_LOG_FILE and SERVER_LOG_FILE open
 *	while the server is running, so we use UTILITY_LOG_FILE for "pg_ctl
 *	stop".
 */
#define SERVER_STOP_LOG_FILE	UTILITY_LOG_FILE
#endif

>>>>>>> 80edfd76591fdb9beec061de3c05ef4e9d96ce56

#ifndef WIN32
#define pg_copy_file		copy_file
#define pg_mv_file			rename
#define pg_link_file		link
<<<<<<< HEAD:contrib/pg_upgrade/pg_upgrade.h
#define PATH_SEPARATOR      '/'
=======
#define PATH_SEPARATOR		'/'
#define PATH_QUOTE	'\''
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8:src/bin/pg_upgrade/pg_upgrade.h
#define RM_CMD				"rm -f"
#define RMDIR_CMD			"rm -rf"
#define SCRIPT_PREFIX		"./"
#define SCRIPT_EXT			"sh"
#define ECHO_QUOTE	"'"
#define ECHO_BLANK	""
#else
#define pg_copy_file		CopyFile
#define pg_mv_file			pgrename
#define pg_link_file		win32_pghardlink
<<<<<<< HEAD:contrib/pg_upgrade/pg_upgrade.h
#define sleep(x)			Sleep(x * 1000)
#define PATH_SEPARATOR      '\\'
=======
#define PATH_SEPARATOR		'\\'
#define PATH_QUOTE	'"'
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8:src/bin/pg_upgrade/pg_upgrade.h
#define RM_CMD				"DEL /q"
#define RMDIR_CMD			"RMDIR /s/q"
#define SCRIPT_PREFIX		""
#define SCRIPT_EXT			"bat"
#define EXE_EXT				".exe"
#define ECHO_QUOTE	""
#define ECHO_BLANK	"."
#endif

#if defined(WIN32) && !defined(__CYGWIN__)

		/*
		 * XXX This does not work for all terminal environments or for output
		 * containing non-ASCII characters; see comments in simple_prompt().
		 */
#define DEVTTY	"con"
#else
#define DEVTTY	"/dev/tty"
#endif

#define CLUSTER_NAME(cluster)	((cluster) == &old_cluster ? "old" : \
								 (cluster) == &new_cluster ? "new" : "none")

#define atooid(x)  ((Oid) strtoul((x), NULL, 10))

/* OID system catalog preservation added during PG 9.0 development */
#define TABLE_SPACE_SUBDIRS_CAT_VER 201001111
/* postmaster/postgres -b (binary_upgrade) flag added during PG 9.1 development */
<<<<<<< HEAD
/* In GPDB, it was introduced during GPDB 5.0 development. */
#define BINARY_UPGRADE_SERVER_FLAG_CAT_VER 301607301

/*
 * Extra information stored for each Append-only table.
 * This is used to transfer the information from the auxiliary
 * AO table to the new cluster.
 */

/* To hold contents of pg_visimap_<oid> */
typedef struct
{
	int			segno;
	int64		first_row_no;
	char	   *visimap;		/* text representation of the "bit varying" field */
} AOVisiMapInfo;

typedef struct
{
	int			segno;
	int			columngroup_no;
	int64		first_row_no;
	char	   *minipage;		/* text representation of the "bit varying" field */
} AOBlkDir;

/* To hold contents of pg_aoseg_<oid> */
typedef struct
{
	int			segno;
	int64		eof;
	int64		tupcount;
	int64		varblockcount;
	int64		eofuncompressed;
	int64		modcount;
	int16		version;
	int16		state;
} AOSegInfo;

/* To hold contents of pf_aocsseg_<oid> */
typedef struct
{
	int         segno;
	int64		tupcount;
	int64		varblockcount;
	char       *vpinfo;
	int64		modcount;
	int16		state;
	int16		version;
} AOCSSegInfo;

typedef struct
{
	int16		attlen;
	char		attalign;
	bool		is_numeric;
} AttInfo;
=======
#define BINARY_UPGRADE_SERVER_FLAG_CAT_VER 201104251
/*
 *	Visibility map changed with this 9.2 commit,
 *	8f9fe6edce358f7904e0db119416b4d1080a83aa; pick later catalog version.
 */
#define VISIBILITY_MAP_CRASHSAFE_CAT_VER 201107031

<<<<<<< HEAD:contrib/pg_upgrade/pg_upgrade.h
>>>>>>> 80edfd76591fdb9beec061de3c05ef4e9d96ce56
=======
/*
 * pg_multixact format changed in 9.3 commit 0ac5ad5134f2769ccbaefec73844f85,
 * ("Improve concurrency of foreign key locking") which also updated catalog
 * version to this value.  pg_upgrade behavior depends on whether old and new
 * server versions are both newer than this, or only the new one is.
 */
#define MULTIXACT_FORMATCHANGE_CAT_VER 201301231

/*
 * large object chunk size added to pg_controldata,
 * commit 5f93c37805e7485488480916b4585e098d3cc883
 */
#define LARGE_OBJECT_SIZE_PG_CONTROL_VER 942

/*
 * change in JSONB format during 9.4 beta
 */
#define JSONB_FORMAT_CHANGE_CAT_VER 201409291
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8:src/bin/pg_upgrade/pg_upgrade.h

/*
 * Each relation is represented by a relinfo structure.
 */
typedef struct
{
	/* Can't use NAMEDATALEN;  not guaranteed to fit on client */
	char	   *nspname;		/* namespace name */
	char	   *relname;		/* relation name */
	Oid			reloid;			/* relation oid */
	char		relstorage;
	Oid			relfilenode;	/* relation relfile node */
	/* relation tablespace path, or "" for the cluster default */
<<<<<<< HEAD:contrib/pg_upgrade/pg_upgrade.h
	char		tablespace[MAXPGPATH];
<<<<<<< HEAD

	/* Extra information for append-only tables */
	AOSegInfo  *aosegments;
	AOCSSegInfo *aocssegments;
	int			naosegments;
	AOVisiMapInfo *aovisimaps;
	int			naovisimaps;
	AOBlkDir   *aoblkdirs;
	int			naoblkdirs;

	/* Extra information for heap tables */
	bool		gpdb4_heap_conversion_needed;
	bool		has_numerics;
	AttInfo	   *atts;
	int			natts;
=======
>>>>>>> 80edfd76591fdb9beec061de3c05ef4e9d96ce56
=======
	char	   *tablespace;
	bool		nsp_alloc;
	bool		tblsp_alloc;
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8:src/bin/pg_upgrade/pg_upgrade.h
} RelInfo;

typedef struct
{
	RelInfo    *rels;
	int			nrels;
} RelInfoArr;

typedef enum
{
	HEAP,
	AO,
	AOCS,
	FSM
} RelType;

/*
 * The following structure represents a relation mapping.
 */
typedef struct
{
	const char *old_tablespace;
	const char *new_tablespace;
	const char *old_tablespace_suffix;
	const char *new_tablespace_suffix;
	Oid			old_db_oid;
	Oid			new_db_oid;

	/*
	 * old/new relfilenodes might differ for pg_largeobject(_metadata) indexes
	 * due to VACUUM FULL or REINDEX.  Other relfilenodes are preserved.
	 */
	Oid			old_relfilenode;
	Oid			new_relfilenode;
	/* the rest are used only for logging and error reporting */
<<<<<<< HEAD:contrib/pg_upgrade/pg_upgrade.h
	char		nspname[NAMEDATALEN];	/* namespaces */
	char		relname[NAMEDATALEN];

	/* GPDB */
	bool		missing_seg0_ok;

	RelType		type;			/* Type of relation */

	/* Extra information for heap tables */
	bool		gpdb4_heap_conversion_needed;
	bool		has_numerics;
	AttInfo	   *atts;
	int			natts;
=======
	char	   *nspname;		/* namespaces */
	char	   *relname;
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8:src/bin/pg_upgrade/pg_upgrade.h
} FileNameMap;

/*
 * Structure to store database information
 */
typedef struct
{
	Oid			db_oid;			/* oid of the database */
	char	   *db_name;		/* database name */
	char		db_tablespace[MAXPGPATH];		/* database default tablespace
												 * path */
	char	   *db_collate;
	char	   *db_ctype;
	int			db_encoding;
	RelInfoArr	rel_arr;		/* array of all user relinfos */

	char	   *reserved_oids;	/* as a string */
} DbInfo;

typedef struct
{
	DbInfo	   *dbs;			/* array of db infos */
	int			ndbs;			/* number of db infos */
} DbInfoArr;

/*
 * The following structure is used to hold pg_control information.
 * Rather than using the backend's control structure we use our own
 * structure to avoid pg_control version issues between releases.
 */
typedef struct
{
	uint32		ctrl_ver;
	uint32		cat_ver;
	char		nextxlogfile[25];
	uint32		chkpnt_nxtxid;
	uint32		chkpnt_nxtepoch;
	uint32		chkpnt_nxtoid;
	uint32		chkpnt_nxtmulti;
	uint32		chkpnt_nxtmxoff;
	uint32		chkpnt_oldstMulti;
	uint32		align;
	uint32		blocksz;
	uint32		largesz;
	uint32		walsz;
	uint32		walseg;
	uint32		ident;
	uint32		index;
	uint32		toast;
	uint32		large_object;
	bool		date_is_int;
	bool		float8_pass_by_value;
	bool		data_checksum_version;
<<<<<<< HEAD:contrib/pg_upgrade/pg_upgrade.h
	char	   *lc_collate;
	char	   *lc_ctype;
	char	   *encoding;
=======
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8:src/bin/pg_upgrade/pg_upgrade.h
} ControlData;

/*
 * Enumeration to denote link modes
 */
typedef enum
{
	TRANSFER_MODE_COPY,
	TRANSFER_MODE_LINK
} transferMode;

/*
 * Enumeration to denote checksum modes
 */
typedef enum
{
	CHECKSUM_NONE = 0,
	CHECKSUM_ADD,
	CHECKSUM_REMOVE
} checksumMode;

/*
 * Enumeration to denote pg_log modes
 */
typedef enum
{
	PG_VERBOSE,
	PG_STATUS,
	PG_REPORT,
	PG_WARNING,
	PG_FATAL
} eLogType;


typedef long pgpid_t;

/*
 * Enumeration for operations in the progress report
 */
typedef enum
{
	CHECK,
	SCHEMA_DUMP,
	SCHEMA_RESTORE,
	FILE_MAP,
	FILE_COPY,
	FIXUP,
	ABORT,
	DONE
} progress_type;

/*
 * cluster
 *
 *	information about each cluster
 */
typedef struct
{
	ControlData controldata;	/* pg_control information */
	DbInfoArr	dbarr;			/* dbinfos array */
	char	   *pgdata;			/* pathname for cluster's $PGDATA directory */
	char	   *pgconfig;		/* pathname for cluster's config file
								 * directory */
	char	   *bindir;			/* pathname for cluster's executable directory */
	char	   *pgopts;			/* options to pass to the server, like pg_ctl
								 * -o */
	char	   *sockdir;		/* directory for Unix Domain socket, if any */
	unsigned short port;		/* port number where postmaster is waiting */
	uint32		major_version;	/* PG_VERSION of cluster */
	char		major_version_str[64];	/* string PG_VERSION of cluster */
	uint32		bin_version;	/* version returned from pg_ctl */
	Oid			pg_database_oid;	/* OID of pg_database relation */
<<<<<<< HEAD:contrib/pg_upgrade/pg_upgrade.h
	Oid			install_role_oid;	/* OID of connected role */
	Oid			role_count;			/* number of roles defined in the cluster */
	char	   *tablespace_suffix;		/* directory specification */

	char	   *global_reserved_oids; /* OID preassign calls for shared objects */
=======
	const char *tablespace_suffix;		/* directory specification */
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8:src/bin/pg_upgrade/pg_upgrade.h
} ClusterInfo;


/*
 *	LogOpts
*/
typedef struct
{
	FILE	   *internal;		/* internal log FILE */
	bool		verbose;		/* TRUE -> be verbose in messages */
<<<<<<< HEAD

	/* GPDB */
	bool		progress;		/* TRUE -> file based progress queue */
=======
	bool		retain;			/* retain log files on success */
>>>>>>> 80edfd76591fdb9beec061de3c05ef4e9d96ce56
} LogOpts;


/*
 *	UserOpts
*/
typedef struct
{
	bool		check;			/* TRUE -> ask user for permission to make
								 * changes */
	transferMode transfer_mode; /* copy files or link them? */
<<<<<<< HEAD:contrib/pg_upgrade/pg_upgrade.h

	/* GPDB */
	bool		dispatcher_mode; /* TRUE -> upgrading QD node */
	checksumMode checksum_mode; /* TRUE -> calculate and add checksums to
								 * data pages */
=======
	int			jobs;
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8:src/bin/pg_upgrade/pg_upgrade.h
} UserOpts;


/*
 * OSInfo
 */
typedef struct
{
	const char *progname;		/* complete pathname for this program */
	char	   *exec_path;		/* full path to my executable */
	char	   *user;			/* username for clusters */
	bool		user_specified; /* user specified on command-line */
	char	  **old_tablespaces;	/* tablespaces */
	int			num_old_tablespaces;
	char	  **libraries;		/* loadable libraries */
	int			num_libraries;
	ClusterInfo *running_cluster;
} OSInfo;


/*
 * Global variables
 */
extern LogOpts log_opts;
extern UserOpts user_opts;
extern ClusterInfo old_cluster,
			new_cluster;
extern OSInfo os_info;


/* check.c */

void		output_check_banner(bool live_check);
void		check_and_dump_old_cluster(bool live_check);
void		check_new_cluster(void);
void		report_clusters_compatible(void);
void		issue_warnings(void);
void output_completion_banner(char *analyze_script_file_name,
						 char *deletion_script_file_name);
void		check_cluster_versions(void);
void		check_cluster_compatibility(bool live_check);
void		create_script_for_old_cluster_deletion(char **deletion_script_file_name);
void		create_script_for_cluster_analyze(char **analyze_script_file_name);


/* controldata.c */

void		get_control_data(ClusterInfo *cluster, bool live_check);
void		check_control_data(ControlData *oldctrl, ControlData *newctrl);
void		disable_old_cluster(void);


/* dump.c */

void		generate_old_dump(void);
void		optionally_create_toast_tables(void);


/* exec.c */

#define EXEC_PSQL_ARGS "--echo-queries --set ON_ERROR_STOP=on --no-psqlrc --dbname=template1"

bool exec_prog(const char *log_file, const char *opt_log_file,
		  bool throw_error, const char *fmt,...) pg_attribute_printf(4, 5);
void		verify_directories(void);
bool		pid_lock_file_exists(const char *datadir);


/* file.c */

#ifdef PAGE_CONVERSION
typedef const char *(*pluginStartup) (uint16 migratorVersion,
								uint16 *pluginVersion, uint16 newPageVersion,
								   uint16 oldPageVersion, void **pluginData);
typedef const char *(*pluginConvertFile) (void *pluginData,
								   const char *dstName, const char *srcName);
typedef const char *(*pluginConvertPage) (void *pluginData,
								   const char *dstPage, const char *srcPage);
typedef const char *(*pluginShutdown) (void *pluginData);

typedef struct
{
	uint16		oldPageVersion; /* Page layout version of the old cluster		*/
	uint16		newPageVersion; /* Page layout version of the new cluster		*/
	uint16		pluginVersion;	/* API version of converter plugin */
	void	   *pluginData;		/* Plugin data (set by plugin) */
	pluginStartup startup;		/* Pointer to plugin's startup function */
	pluginConvertFile convertFile;		/* Pointer to plugin's file converter
										 * function */
	pluginConvertPage convertPage;		/* Pointer to plugin's page converter
										 * function */
	pluginShutdown shutdown;	/* Pointer to plugin's shutdown function */
} pageCnvCtx;

const pageCnvCtx *setupPageConverter(void);
#else
/* dummy */
typedef void *pageCnvCtx;
#endif

const char *copyAndUpdateFile(pageCnvCtx *pageConverter, const char *src,
				  const char *dst, bool force);
const char *linkAndUpdateFile(pageCnvCtx *pageConverter, const char *src,
				  const char *dst);

void		check_hard_link(void);
<<<<<<< HEAD
void rewriteHeapPageChecksum(const char *fromfile, const char *tofile,
							 const char *schemaName, const char *relName);
=======
FILE	   *fopen_priv(const char *path, const char *mode);
>>>>>>> 80edfd76591fdb9beec061de3c05ef4e9d96ce56

/* function.c */

void		get_loadable_libraries(void);
void		check_loadable_libraries(void);

/* info.c */

FileNameMap *gen_db_file_maps(DbInfo *old_db,
				 DbInfo *new_db, int *nmaps, const char *old_pgdata,
				 const char *new_pgdata);
void		get_db_and_rel_infos(ClusterInfo *cluster);
void print_maps(FileNameMap *maps, int n,
		   const char *db_name);

/* option.c */

void		parseCommandLine(int argc, char *argv[]);
void		adjust_data_dir(ClusterInfo *cluster);
void		get_sock_dir(ClusterInfo *cluster, bool live_check);

/* relfilenode.c */

void		get_pg_database_relfilenode(ClusterInfo *cluster);
<<<<<<< HEAD:contrib/pg_upgrade/pg_upgrade.h
const char *transfer_all_new_dbs(DbInfoArr *olddb_arr,
				   DbInfoArr *newdb_arr, char *old_pgdata, char *new_pgdata);

/* aotable.c */
void		restore_aosegment_tables(void);

/* gpdb4_heap_convert.c */
const char *convert_gpdb4_heap_file(const char *src, const char *dst,
									bool has_numerics, AttInfo *atts, int natts);
=======
void transfer_all_new_tablespaces(DbInfoArr *old_db_arr,
				  DbInfoArr *new_db_arr, char *old_pgdata, char *new_pgdata);
void transfer_all_new_dbs(DbInfoArr *old_db_arr,
				   DbInfoArr *new_db_arr, char *old_pgdata, char *new_pgdata,
					 char *old_tablespace);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8:src/bin/pg_upgrade/pg_upgrade.h

/* tablespace.c */

void		init_tablespaces(void);


/* server.c */

PGconn	   *connectToServer(ClusterInfo *cluster, const char *db_name);
PGresult   *executeQueryOrDie(PGconn *conn, const char *fmt,...) pg_attribute_printf(2, 3);

char	   *cluster_conn_opts(ClusterInfo *cluster);

bool		start_postmaster(ClusterInfo *cluster, bool throw_error);
void		stop_postmaster(bool fast);
uint32		get_major_server_version(ClusterInfo *cluster);
void		check_pghost_envvar(void);


/* util.c */

char	   *quote_identifier(const char *s);
int			get_user_info(char **user_name_p);
void		check_ok(void);
void		report_status(eLogType type, const char *fmt,...) pg_attribute_printf(2, 3);
void		pg_log(eLogType type, const char *fmt,...) pg_attribute_printf(2, 3);
void		pg_fatal(const char *fmt,...) pg_attribute_printf(1, 2) pg_attribute_noreturn();
void		end_progress_output(void);
void		prep_status(const char *fmt,...) pg_attribute_printf(1, 2);
void		check_ok(void);
const char *getErrorText(int errNum);
unsigned int str2uint(const char *str);
void		pg_putenv(const char *var, const char *val);
void 		report_progress(ClusterInfo *cluster, progress_type op, char *fmt,...);
void		close_progress(void);


/* version.c */

void new_9_0_populate_pg_largeobject_metadata(ClusterInfo *cluster,
										 bool check_mode);
<<<<<<< HEAD:contrib/pg_upgrade/pg_upgrade.h
void new_gpdb5_0_invalidate_indexes(bool check_mode);
void new_gpdb_invalidate_bitmap_indexes(bool check_mode);

/* version_old_8_3.c */

void		old_8_3_check_for_name_data_type_usage(ClusterInfo *cluster);
void		old_8_3_check_for_tsquery_usage(ClusterInfo *cluster);
void		old_8_3_check_ltree_usage(ClusterInfo *cluster);
void		old_8_3_rebuild_tsvector_tables(ClusterInfo *cluster, bool check_mode);
void		old_8_3_invalidate_hash_gin_indexes(ClusterInfo *cluster, bool check_mode);
void old_8_3_invalidate_bpchar_pattern_ops_indexes(ClusterInfo *cluster,
											  bool check_mode);
char	   *old_8_3_create_sequence_script(ClusterInfo *cluster);

/* version_old_gpdb4.c */
void old_GPDB4_check_for_money_data_type_usage(ClusterInfo *cluster);
void old_GPDB4_check_no_free_aoseg(ClusterInfo *cluster);

/* oid_dump.c */
void dump_new_oids(void);
void get_old_oids(void);
void slurp_oid_files(void);

/*
 * Hack to make backend macros that check for assertions to work.
 */
#ifdef AssertMacro
#undef AssertMacro
#endif
#define AssertMacro(condition) ((void) true)
#ifdef Assert
#undef Assert
#endif
#define Assert(condition) ((void) (true || (condition)))
=======
void		old_9_3_check_for_line_data_type_usage(ClusterInfo *cluster);

/* parallel.c */
void parallel_exec_prog(const char *log_file, const char *opt_log_file,
				   const char *fmt,...) pg_attribute_printf(3, 4);
void parallel_transfer_all_new_dbs(DbInfoArr *old_db_arr, DbInfoArr *new_db_arr,
							  char *old_pgdata, char *new_pgdata,
							  char *old_tablespace);
bool		reap_child(bool wait_for_child);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8:src/bin/pg_upgrade/pg_upgrade.h
