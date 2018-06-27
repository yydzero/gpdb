/*-------------------------------------------------------------------------
 *
 * generic-xlc.h
 *	  Atomic operations for IBM's CC
 *
 * Portions Copyright (c) 2013-2015, PostgreSQL Global Development Group
 *
 * NOTES:
 *
 * Documentation:
 * * Synchronization and atomic built-in functions
 *   http://publib.boulder.ibm.com/infocenter/lnxpcomp/v8v101/topic/com.ibm.xlcpp8l.doc/compiler/ref/bif_sync.htm
 *
 * src/include/port/atomics/generic-xlc.h
 *
 * -------------------------------------------------------------------------
 */

#if defined(HAVE_ATOMICS)

<<<<<<< HEAD
=======
#include <atomic.h>

>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
#define PG_HAVE_ATOMIC_U32_SUPPORT
typedef struct pg_atomic_uint32
{
	volatile uint32 value;
} pg_atomic_uint32;


/* 64bit atomics are only supported in 64bit mode */
#ifdef __64BIT__
#define PG_HAVE_ATOMIC_U64_SUPPORT
typedef struct pg_atomic_uint64
{
	volatile uint64 value pg_attribute_aligned(8);
} pg_atomic_uint64;

#endif /* __64BIT__ */

#endif /* defined(HAVE_ATOMICS) */

#if defined(PG_USE_INLINE) || defined(ATOMICS_INCLUDE_DEFINITIONS)

#if defined(HAVE_ATOMICS)

#define PG_HAVE_ATOMIC_COMPARE_EXCHANGE_U32
static inline bool
pg_atomic_compare_exchange_u32_impl(volatile pg_atomic_uint32 *ptr,
									uint32 *expected, uint32 newval)
{
<<<<<<< HEAD
=======
	bool	ret;
	uint64	current;

>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
	/*
	 * xlc's documentation tells us:
	 * "If __compare_and_swap is used as a locking primitive, insert a call to
	 * the __isync built-in function at the start of any critical sections."
	 */
	__isync();

	/*
	 * XXX: __compare_and_swap is defined to take signed parameters, but that
	 * shouldn't matter since we don't perform any arithmetic operations.
	 */
<<<<<<< HEAD
	return __compare_and_swap((volatile int*)&ptr->value,
							  (int *)expected, (int)newval);
=======
	current = (uint32)__compare_and_swap((volatile int*)ptr->value,
										 (int)*expected, (int)newval);
	ret = current == *expected;
	*expected = current;
	return ret;
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
}

#define PG_HAVE_ATOMIC_FETCH_ADD_U32
static inline uint32
pg_atomic_fetch_add_u32_impl(volatile pg_atomic_uint32 *ptr, int32 add_)
{
<<<<<<< HEAD
	return __fetch_and_add((volatile int *)&ptr->value, add_);
=======
	return __fetch_and_add(&ptr->value, add_);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
}

#ifdef PG_HAVE_ATOMIC_U64_SUPPORT

#define PG_HAVE_ATOMIC_COMPARE_EXCHANGE_U64
static inline bool
pg_atomic_compare_exchange_u64_impl(volatile pg_atomic_uint64 *ptr,
									uint64 *expected, uint64 newval)
{
<<<<<<< HEAD
	__isync();

	return __compare_and_swaplp((volatile long*)&ptr->value,
								(long *)expected, (long)newval);;
=======
	bool	ret;
	uint64	current;

	__isync();

	current = (uint64)__compare_and_swaplp((volatile long*)ptr->value,
										   (long)*expected, (long)newval);
	ret = current == *expected;
	*expected = current;
	return ret;
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
}

#define PG_HAVE_ATOMIC_FETCH_ADD_U64
static inline uint64
pg_atomic_fetch_add_u64_impl(volatile pg_atomic_uint64 *ptr, int64 add_)
{
<<<<<<< HEAD
	return __fetch_and_addlp((volatile long *)&ptr->value, add_);
=======
	return __fetch_and_addlp(&ptr->value, add_);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
}

#endif /* PG_HAVE_ATOMIC_U64_SUPPORT */

#endif /* defined(HAVE_ATOMICS) */

#endif /* defined(PG_USE_INLINE) || defined(ATOMICS_INCLUDE_DEFINITIONS) */
