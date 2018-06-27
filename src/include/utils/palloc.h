/*-------------------------------------------------------------------------
 *
 * palloc.h
 *	  POSTGRES memory allocator definitions.
 *
 * This file contains the basic memory allocation interface that is
 * needed by almost every backend module.  It is included directly by
 * postgres.h, so the definitions here are automatically available
 * everywhere.  Keep it lean!
 *
 * Memory allocation occurs within "contexts".  Every chunk obtained from
 * palloc()/MemoryContextAlloc() is allocated within a specific context.
 * The entire contents of a context can be freed easily and quickly by
 * resetting or deleting the context --- this is both faster and less
 * prone to memory-leakage bugs than releasing chunks individually.
 * We organize contexts into context trees to allow fine-grain control
 * over chunk lifetime while preserving the certainty that we will free
 * everything that should be freed.  See utils/mmgr/README for more info.
 *
 *
<<<<<<< HEAD
 * Portions Copyright (c) 2007-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
=======
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/palloc.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PALLOC_H
#define PALLOC_H

/*
<<<<<<< HEAD
 * Optional #defines for debugging...
 *
 * If CDB_PALLOC_CALLER_ID is defined, MemoryContext error and warning
 * messages (such as "out of memory" and "invalid memory alloc request
 * size") will include the caller's source file name and line number.
 * This can be useful in optimized builds where the error handler's
 * stack trace doesn't accurately identify the call site.  Overhead
 * is minimal: two extra parameters to memory allocation functions,
 * and 8 to 16 bytes per context.
 *
 * If CDB_PALLOC_TAGS is defined, every allocation from a standard
 * memory context (aset.c) is tagged with an extra 16 to 32 bytes of
 * debugging info preceding the first byte of the area.  The added
 * header fields identify the allocation call site (source file name
 * and line number).  Also each context keeps a linked list of all
 * of its allocated areas.  The dump_memory_allocation() and
 * dump_memory_allocation_ctxt() functions in aset.c may be called 
 * from a debugger to write the area headers to a file.
 */

/*
#define CDB_PALLOC_CALLER_ID
*/

#ifdef USE_ASSERT_CHECKING
#define CDB_PALLOC_TAGS
#endif

/* CDB_PALLOC_TAGS implies CDB_PALLOC_CALLER_ID */
#if defined(CDB_PALLOC_TAGS) && !defined(CDB_PALLOC_CALLER_ID)
#define CDB_PALLOC_CALLER_ID
#endif

#include "utils/memaccounting.h"

/*
 * We track last OOM time to identify culprit processes that
 * consume too much memory. For 64-bit platform we have high
 * precision time variable based on TimeStampTz. However, on
 * 32-bit platform we only have "per-second" precision.
 * OOMTimeType abstracts this different types.
 */
#if defined(__x86_64__)
typedef int64 OOMTimeType;
#else
typedef uint32 OOMTimeType;
#endif

/*
 * Type MemoryContextData is declared in nodes/memnodes.h.	Most users
=======
 * Type MemoryContextData is declared in nodes/memnodes.h.  Most users
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
 * of memory allocation should just treat it as an abstract type, so we
 * do not provide the struct contents here.
 */
typedef struct MemoryContextData *MemoryContext;

/*
 * A memory context can have callback functions registered on it.  Any such
 * function will be called once just before the context is next reset or
 * deleted.  The MemoryContextCallback struct describing such a callback
 * typically would be allocated within the context itself, thereby avoiding
 * any need to manage it explicitly (the reset/delete action will free it).
 */
typedef void (*MemoryContextCallbackFunction) (void *arg);

typedef struct MemoryContextCallback
{
	MemoryContextCallbackFunction func; /* function to call */
	void	   *arg;			/* argument to pass it */
	struct MemoryContextCallback *next; /* next in list of callbacks */
} MemoryContextCallback;

/*
 * CurrentMemoryContext is the default allocation context for palloc().
 * Avoid accessing it directly!  Instead, use MemoryContextSwitchTo()
 * to change the setting.
 */
extern PGDLLIMPORT MemoryContext CurrentMemoryContext;

extern bool gp_mp_inited;
extern volatile OOMTimeType* segmentOOMTime;
extern volatile OOMTimeType oomTrackerStartTime;
extern volatile OOMTimeType alreadyReportedOOMTime;

/*
 * Flags for MemoryContextAllocExtended.
 */
#define MCXT_ALLOC_HUGE			0x01	/* allow huge allocation (> 1 GB) */
#define MCXT_ALLOC_NO_OOM		0x02	/* no failure if out-of-memory */
#define MCXT_ALLOC_ZERO			0x04	/* zero allocated memory */

/*
 * Fundamental memory-allocation operations (more are in utils/memutils.h)
 */
<<<<<<< HEAD
extern void * __attribute__((malloc)) MemoryContextAllocImpl(MemoryContext context, Size size, const char* file, const char *func, int line);
extern void * __attribute__((malloc)) MemoryContextAllocZeroImpl(MemoryContext context, Size size, const char* file, const char *func, int line);
extern void * __attribute__((malloc)) MemoryContextAllocZeroAlignedImpl(MemoryContext context, Size size, const char* file, const char *func, int line);
extern void * __attribute__((malloc)) MemoryContextReallocImpl(void *pointer, Size size, const char* file, const char *func, int line);
extern void MemoryContextFreeImpl(void *pointer, const char* file, const char *func, int sline);

#define MemoryContextAlloc(ctxt, sz) MemoryContextAllocImpl((ctxt), (sz), __FILE__, PG_FUNCNAME_MACRO, __LINE__)
#define palloc(sz) MemoryContextAlloc(CurrentMemoryContext, (sz)) 
#define ctxt_alloc(ctxt, sz) MemoryContextAlloc((ctxt), (sz)) 

#define MemoryContextAllocZero(ctxt, sz) MemoryContextAllocZeroImpl((ctxt), (sz), __FILE__, PG_FUNCNAME_MACRO, __LINE__)
#define palloc0(sz) MemoryContextAllocZero(CurrentMemoryContext, (sz))
=======
extern void *MemoryContextAlloc(MemoryContext context, Size size);
extern void *MemoryContextAllocZero(MemoryContext context, Size size);
extern void *MemoryContextAllocZeroAligned(MemoryContext context, Size size);
extern void *MemoryContextAllocExtended(MemoryContext context,
						   Size size, int flags);

extern void *palloc(Size size);
extern void *palloc0(Size size);
extern void *palloc_extended(Size size, int flags);
extern void *repalloc(void *pointer, Size size);
extern void pfree(void *pointer);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

#define repalloc(ptr, sz) MemoryContextReallocImpl(ptr, (sz), __FILE__, PG_FUNCNAME_MACRO, __LINE__)
#define pfree(ptr) MemoryContextFreeImpl(ptr, __FILE__, PG_FUNCNAME_MACRO, __LINE__)

/*
 * The result of palloc() is always word-aligned, so we can skip testing
 * alignment of the pointer when deciding which MemSet variant to use.
 * Note that this variant does not offer any advantage, and should not be
 * used, unless its "sz" argument is a compile-time constant; therefore, the
 * issue that it evaluates the argument multiple times isn't a problem in
 * practice.
 */
#define palloc0fast(sz) \
<<<<<<< HEAD
	( MemSetTest(0, (sz)) ? \
		MemoryContextAllocZeroAlignedImpl(CurrentMemoryContext, (sz), __FILE__, PG_FUNCNAME_MACRO, __LINE__) : \
		MemoryContextAllocZeroImpl(CurrentMemoryContext, (sz), __FILE__, PG_FUNCNAME_MACRO, __LINE__))

/*
=======
	( MemSetTest(0, sz) ? \
		MemoryContextAllocZeroAligned(CurrentMemoryContext, sz) : \
		MemoryContextAllocZero(CurrentMemoryContext, sz) )

/* Higher-limit allocators. */
extern void *MemoryContextAllocHuge(MemoryContext context, Size size);
extern void *repalloc_huge(void *pointer, Size size);

/*
 * MemoryContextSwitchTo can't be a macro in standard C compilers.
 * But we can make it an inline function if the compiler supports it.
 * See STATIC_IF_INLINE in c.h.
 *
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
 * Although this header file is nominally backend-only, certain frontend
 * programs like pg_controldata include it via postgres.h.  For some compilers
 * it's necessary to hide the inline definition of MemoryContextSwitchTo in
 * this scenario; hence the #ifndef FRONTEND.
 */

#ifndef FRONTEND
<<<<<<< HEAD
static inline MemoryContext
=======
#ifndef PG_USE_INLINE
extern MemoryContext MemoryContextSwitchTo(MemoryContext context);
#endif   /* !PG_USE_INLINE */
#if defined(PG_USE_INLINE) || defined(MCXT_INCLUDE_DEFINITIONS)
STATIC_IF_INLINE MemoryContext
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
MemoryContextSwitchTo(MemoryContext context)
{
	MemoryContext old = CurrentMemoryContext;

	CurrentMemoryContext = context;
	return old;
}
<<<<<<< HEAD
#endif   /* FRONTEND */
=======
#endif   /* PG_USE_INLINE || MCXT_INCLUDE_DEFINITIONS */
#endif   /* FRONTEND */

/* Registration of memory context reset/delete callbacks */
extern void MemoryContextRegisterResetCallback(MemoryContext context,
								   MemoryContextCallback *cb);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

/*
 * These are like standard strdup() except the copied string is
 * allocated in a context, not with malloc().
 */
<<<<<<< HEAD
extern char * __attribute__((malloc)) MemoryContextStrdup(MemoryContext context, const char *string);
extern void MemoryContextStats(MemoryContext context);

#define pstrdup(str)  MemoryContextStrdup(CurrentMemoryContext, (str))

extern char *pnstrdup(const char *in, Size len);

/* sprintf into a palloc'd buffer --- these are in psprintf.c */
extern char *psprintf(const char *fmt,...) __attribute__((format(printf, 1, 2)));
extern size_t pvsnprintf(char *buf, size_t len, const char *fmt, va_list args)  __attribute__((format(printf, 3, 0)));

#if defined(WIN32) || defined(__CYGWIN__)
extern void *pgport_palloc(Size sz);
extern char *pgport_pstrdup(const char *str);
extern void pgport_pfree(void *pointer);
#endif
=======
extern char *MemoryContextStrdup(MemoryContext context, const char *string);
extern char *pstrdup(const char *in);
extern char *pnstrdup(const char *in, Size len);

/* sprintf into a palloc'd buffer --- these are in psprintf.c */
extern char *psprintf(const char *fmt,...) pg_attribute_printf(1, 2);
extern size_t pvsnprintf(char *buf, size_t len, const char *fmt, va_list args) pg_attribute_printf(3, 0);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

/* Mem Protection */
extern int max_chunks_per_query;

extern PGDLLIMPORT MemoryContext TopMemoryContext;

extern bool MemoryProtection_IsOwnerThread(void);
extern void InitPerProcessOOMTracking(void);
extern void GPMemoryProtect_ShmemInit(void);
extern void GPMemoryProtect_Init(void);
extern void GPMemoryProtect_Shutdown(void);
extern void UpdateTimeAtomically(volatile OOMTimeType* time_var);

/*
 * ReportOOMConsumption
 *
 * Checks if there was any new OOM event in this segment.
 * In case of a new OOM, it reports the memory consumption
 * of the current process.
 */
#define ReportOOMConsumption()\
{\
	if (gp_mp_inited && *segmentOOMTime >= oomTrackerStartTime && *segmentOOMTime > alreadyReportedOOMTime)\
	{\
		Assert(MemoryProtection_IsOwnerThread());\
		UpdateTimeAtomically(&alreadyReportedOOMTime);\
		write_stderr("One or more query execution processes ran out of memory on this segment. Logging memory usage.");\
		MemoryAccounting_SaveToLog();\
		MemoryContextStats(TopMemoryContext);\
	}\
}

#endif   /* PALLOC_H */
