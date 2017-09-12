/*-------------------------------------------------------------------------
 *
 * resgroup.h
 *	  Greenplum database resource group definitions.
 *
 * Copyright (c) 2016-Present Pivotal Software, Inc
 *
 *-------------------------------------------------------------------------
 */
#ifndef RES_GROUP_H
#define RES_GROUP_H

// #include "cdb/memquota.h"           // Do we really need this?
#include "catalog/pg_resgroup.h"

/*
 * Resource group capability.
 */
typedef struct ResGroupCap
{
	int		value;
	int		proposed;
} ResGroupCap;

/*
 * Resource group capabilities.
 *
 * These are usually a snapshot of the pg_resgroupcapability table
 * for a resource group.
 *
 * The properties must be in the same order as ResGroupLimitType.
 */
typedef struct ResGroupCaps
{
	ResGroupCap		__unknown;			// Why do we need a unknown field? A good place for comment
	ResGroupCap		concurrency;
	ResGroupCap		cpuRateLimit;
	ResGroupCap		memLimit;
	ResGroupCap		memSharedQuota;
	ResGroupCap		memSpillRatio;
} ResGroupCaps;

/*
 * Resource group setting options.
 *
 * These can represent the effective settings of a resource group,
 * or the new settings from ALTER RESOURCE GROUP syntax.
 *
 * The properties must be in the same order as ResGroupLimitType.
 */
typedef struct ResGroupOpts
{
	int32			__unknown;
	int32			concurrency;
	int32			cpuRateLimit;
	int32			memLimit;
	int32			memSharedQuota;
	int32			memSpillRatio;
} ResGroupOpts;

/*
 * GUC variables.
 */
extern char                		*gp_resgroup_memory_policy_str;
extern bool						gp_log_resgroup_memory;
extern int						gp_resgroup_memory_policy_auto_fixed_mem;
extern bool						gp_resgroup_print_operator_memory_limits;
extern int						memory_spill_ratio;			// Do we want to use same prefix 'gp_resgroup_'?

extern int MaxResourceGroups;		// by default it is 9?? too small?
extern double gp_resource_group_cpu_limit;
extern double gp_resource_group_memory_limit;

struct ResGroupConfigSnapshot;		// Who is using this guy?

/* Type of statistic infomation */
typedef enum
{
	RES_GROUP_STAT_UNKNOWN = -1,

	RES_GROUP_STAT_NRUNNING = 0,
	RES_GROUP_STAT_NQUEUEING,
	RES_GROUP_STAT_TOTAL_EXECUTED,
	RES_GROUP_STAT_TOTAL_QUEUED,
	RES_GROUP_STAT_TOTAL_QUEUE_TIME,
	RES_GROUP_STAT_CPU_USAGE,
	RES_GROUP_STAT_MEM_USAGE,
} ResGroupStatType;

/*
 * Functions in resgroup.c
 */

// For pulbic methods, strongly suggest to add comment for each function, similar as dispatcher functions.

/* Shared memory and semaphores */
extern Size ResGroupShmemSize(void);
extern void ResGroupControlInit(void);

/* Load resource group information from catalog */
extern void	InitResGroups(void);

extern void AllocResGroupEntry(Oid groupId, const ResGroupOpts *opts);
extern void FreeResGroupEntry(Oid groupId);

extern void SerializeResGroupInfo(StringInfo str);
extern void DeserializeResGroupInfo(struct ResGroupCaps *capsOut,
									const char *buf, int len);

extern bool ShouldAssignResGroupOnMaster(void);
extern void AssignResGroupOnMaster(void);
extern void UnassignResGroupOnMaster(void);
extern void SwitchResGroupOnSegment(const char *buf, int len);

/* Retrieve statistic information of type from resource group */
extern Datum ResGroupGetStat(Oid groupId, ResGroupStatType type);

// There is no implementation for those functions?!
extern int32 ResGroupCalcMemStocksExpected(const ResGroupCaps *caps);
extern int32 ResGroupCalcMemQuotaStocks(const ResGroupCaps *caps);
extern int32 ResGroupCalcMemSharedStocks(const ResGroupCaps *caps);
extern int32 ResGroupCalcMemSpillStocks(const ResGroupCaps *caps);

extern void ResGroupOptsToCaps(const ResGroupOpts *optsIn, ResGroupCaps *capsOut);
extern void ResGroupCapsToOpts(const ResGroupCaps *capsIn, ResGroupOpts *optsOut);
extern void ResGroupDumpMemoryInfo(void);

/* Check the memory limit of resource group */
extern bool ResGroupReserveMemory(int32 memoryChunks, int32 overuseChunks, bool *waiverUsed);
/* Update the memory usage of resource group */
extern void ResGroupReleaseMemory(int32 memoryChunks);

extern void ResGroupAlterOnCommit(Oid groupId,
								  ResGroupLimitType limittype,
								  const ResGroupCaps *caps);
extern void ResGroupDropCheckForWakeup(Oid groupId, bool isCommit);
extern void ResGroupCheckForDrop(Oid groupId, char *name);
extern int32 ResGroupAllocStocks(Oid groupId, int32 stocks);        // No implementation ?!
extern void ResGroupFreeStocks(Oid groupId, int32 stocks);
extern void ResGroupDecideMemoryCaps(int groupId,
									 ResGroupCaps *caps,
									 const ResGroupOpts *opts);
extern void ResGroupDecideConcurrencyCaps(Oid groupId,
										  ResGroupCaps *caps,
										  const ResGroupOpts *opts);

/* test helper function */
extern void ResGroupGetMemInfo(int *memLimit, int *slotQuota, int *sharedQuota);

extern int64 ResourceGroupGetQueryMemoryLimit(void);
extern int32 ResGroupGetMemStocks(Oid groupId);

#define LOG_RESGROUP_DEBUG(...) \
	do {if (Debug_resource_group) elog(__VA_ARGS__); } while(false);

#endif   /* RES_GROUP_H */
