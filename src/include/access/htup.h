/*-------------------------------------------------------------------------
 *
 * htup.h
 *	  POSTGRES heap tuple definitions.
 *
 *
<<<<<<< HEAD
 * Portions Copyright (c) 2006-2009, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
=======
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/htup.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef HTUP_H
#define HTUP_H

<<<<<<< HEAD
#include "access/tupdesc.h"
#include "access/tupmacs.h"
#include "storage/itemptr.h"
#include "storage/relfilenode.h"
#include "access/sysattr.h"

#include "access/memtup.h"
=======
#include "storage/itemptr.h"
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

/* typedefs and forward declarations for structs defined in htup_details.h */

<<<<<<< HEAD
/*
 * MaxHeapAttributeNumber limits the number of (user) columns in a table.
 * This should be somewhat less than MaxTupleAttributeNumber.  It must be
 * at least one less, else we will fail to do UPDATEs on a maximal-width
 * table (because UPDATE has to form working tuples that include CTID).
 * In practice we want some additional daylight so that we can gracefully
 * support operations that add hidden "resjunk" columns, for example
 * SELECT * FROM wide_table ORDER BY foo, bar, baz.
 * In any case, depending on column data types you will likely be running
 * into the disk-block-based limit on overall tuple size if you have more
 * than a thousand or so columns.  TOAST won't help.
 */
#define MaxHeapAttributeNumber	1600	/* 8 * 200 */
#define MaxPolicyAttributeNumber MaxHeapAttributeNumber

/*
 * Heap tuple header.  To avoid wasting space, the fields should be
 * laid out in such a way as to avoid structure padding.
 *
 * Datums of composite types (row types) share the same general structure
 * as on-disk tuples, so that the same routines can be used to build and
 * examine them.  However the requirements are slightly different: a Datum
 * does not need any transaction visibility information, and it does need
 * a length word and some embedded type information.  We can achieve this
 * by overlaying the xmin/cmin/xmax/cmax/xvac fields of a heap tuple
 * with the fields needed in the Datum case.  Typically, all tuples built
 * in-memory will be initialized with the Datum fields; but when a tuple is
 * about to be inserted in a table, the transaction fields will be filled,
 * overwriting the datum fields.
 *
 * The overall structure of a heap tuple looks like:
 *			fixed fields (HeapTupleHeaderData struct)
 *			nulls bitmap (if HEAP_HASNULL is set in t_infomask)
 *			alignment padding (as needed to make user data MAXALIGN'd)
 *			object ID (if HEAP_HASOID is set in t_infomask)
 *			user data fields
 *
 * We store five "virtual" fields Xmin, Cmin, Xmax, Cmax, and Xvac in three
 * physical fields.  Xmin and Xmax are always really stored, but Cmin, Cmax
 * and Xvac share a field.	This works because we know that Cmin and Cmax
 * are only interesting for the lifetime of the inserting and deleting
 * transaction respectively.  If a tuple is inserted and deleted in the same
 * transaction, we store a "combo" command id that can be mapped to the real
 * cmin and cmax, but only by use of local state within the originating
 * backend.  See combocid.c for more details.  Meanwhile, Xvac is only set by
 * old-style VACUUM FULL, which does not have any command sub-structure and so
 * does not need either Cmin or Cmax.  (This requires that old-style VACUUM
 * FULL never try to move a tuple whose Cmin or Cmax is still interesting,
 * ie, an insert-in-progress or delete-in-progress tuple.)
 *
 * A word about t_ctid: whenever a new tuple is stored on disk, its t_ctid
 * is initialized with its own TID (location).	If the tuple is ever updated,
 * its t_ctid is changed to point to the replacement version of the tuple.
 * Thus, a tuple is the latest version of its row iff XMAX is invalid or
 * t_ctid points to itself (in which case, if XMAX is valid, the tuple is
 * either locked or deleted).  One can follow the chain of t_ctid links
 * to find the newest version of the row.  Beware however that VACUUM might
 * erase the pointed-to (newer) tuple before erasing the pointing (older)
 * tuple.  Hence, when following a t_ctid link, it is necessary to check
 * to see if the referenced slot is empty or contains an unrelated tuple.
 * Check that the referenced tuple has XMIN equal to the referencing tuple's
 * XMAX to verify that it is actually the descendant version and not an
 * unrelated tuple stored into a slot recently freed by VACUUM.  If either
 * check fails, one may assume that there is no live descendant version.
 *
 * Following the fixed header fields, the nulls bitmap is stored (beginning
 * at t_bits).	The bitmap is *not* stored if t_infomask shows that there
 * are no nulls in the tuple.  If an OID field is present (as indicated by
 * t_infomask), then it is stored just before the user data, which begins at
 * the offset shown by t_hoff.	Note that t_hoff must be a multiple of
 * MAXALIGN.
 */

typedef struct HeapTupleFields
{
	TransactionId t_xmin;		/* inserting xact ID */
	TransactionId t_xmax;		/* deleting or locking xact ID */

	union
	{
		CommandId	t_cid;		/* inserting or deleting command ID, or both */
		TransactionId t_xvac;	/* old-style VACUUM FULL xact ID */
	}			t_field3;
} HeapTupleFields;

typedef struct DatumTupleFields
{
	int32		datum_len_;		/* varlena header (do not touch directly!) */

	int32		datum_typmod;	/* -1, or identifier of a record type */

	Oid			datum_typeid;	/* composite type OID, or RECORDOID */

	/*
	 * Note: field ordering is chosen with thought that Oid might someday
	 * widen to 64 bits.
	 */
} DatumTupleFields;

typedef struct HeapTupleHeaderData
{
	union
	{
		HeapTupleFields t_heap;
		DatumTupleFields t_datum;
	}			t_choice;

	ItemPointerData t_ctid;		/* current TID of this or newer tuple */

	/* Fields below here must match MinimalTupleData! */

	uint16		t_infomask2;	/* number of attributes + various flags */

	uint16		t_infomask;		/* various flag bits, see below */

	uint8		t_hoff;			/* sizeof header incl. bitmap, padding */

	/* ^ - 23 bytes - ^ */

	bits8		t_bits[1];		/* bitmap of NULLs -- VARIABLE LENGTH */

	/* MORE DATA FOLLOWS AT END OF STRUCT */
} HeapTupleHeaderData;

typedef HeapTupleHeaderData *HeapTupleHeader;

/*
 * information stored in t_infomask:
 */
#define HEAP_HASNULL			0x0001	/* has null attribute(s) */
#define HEAP_HASVARWIDTH		0x0002	/* has variable-width attribute(s) */
#define HEAP_HASEXTERNAL		0x0004	/* has external stored attribute(s) */
#define HEAP_HASOID				0x0008	/* has an object-id field */
/* bit 0x0010 is available */
#define HEAP_COMBOCID			0x0020	/* t_cid is a combo cid */
#define HEAP_XMAX_EXCL_LOCK		0x0040	/* xmax is exclusive locker */
#define HEAP_XMAX_SHARED_LOCK	0x0080	/* xmax is shared locker */
/* if either LOCK bit is set, xmax hasn't deleted the tuple, only locked it */
#define HEAP_IS_LOCKED	(HEAP_XMAX_EXCL_LOCK | HEAP_XMAX_SHARED_LOCK)
#define HEAP_XMIN_COMMITTED		0x0100	/* t_xmin committed */
#define HEAP_XMIN_INVALID		0x0200	/* t_xmin invalid/aborted */
#define HEAP_XMIN_FROZEN        (HEAP_XMIN_COMMITTED|HEAP_XMIN_INVALID)
#define HEAP_XMAX_COMMITTED		0x0400	/* t_xmax committed */
#define HEAP_XMAX_INVALID		0x0800	/* t_xmax invalid/aborted */
#define HEAP_XMAX_IS_MULTI		0x1000	/* t_xmax is a MultiXactId */
#define HEAP_UPDATED			0x2000	/* this is UPDATEd version of row */
#define HEAP_MOVED_OFF			0x4000	/* moved to another place by pre-9.0
										 * VACUUM FULL; kept for binary
										 * upgrade support */
#define HEAP_MOVED_IN			0x8000	/* moved from another place by pre-9.0
										 * VACUUM FULL; kept for binary
										 * upgrade support */
#define HEAP_MOVED (HEAP_MOVED_OFF | HEAP_MOVED_IN)

#define HEAP_XACT_MASK			0xFFE0	/* visibility-related bits */

/*
 * information stored in t_infomask2:
 */
#define HEAP_NATTS_MASK			0x07FF	/* 11 bits for number of attributes */
#define HEAP_XMIN_DISTRIBUTED_SNAPSHOT_IGNORE		0x0800	
										/* t_xmin is either an old committed 
										 * distributed transaction or local.
										 * They can be ignored for distributed
										 * snapshots.
										 */
#define HEAP_XMAX_DISTRIBUTED_SNAPSHOT_IGNORE		0x1000
										/* t_xmax same as above. */
#define HEAP_HOT_UPDATED		0x4000	/* tuple was HOT-updated */
#define HEAP_ONLY_TUPLE			0x8000	/* this is heap-only tuple */

#define HEAP2_XACT_MASK			0xC000	/* visibility-related bits */

/*
 * HEAP_TUPLE_HAS_MATCH is a temporary flag used during hash joins.  It is
 * only used in tuples that are in the hash table, and those don't need
 * any visibility information, so we can overlay it on a visibility flag
 * instead of using up a dedicated bit.
 */
#define HEAP_TUPLE_HAS_MATCH	HEAP_ONLY_TUPLE /* tuple has a join match */

/*
 * HeapTupleHeader accessor macros
 *
 * Note: beware of multiple evaluations of "tup" argument.	But the Set
 * macros evaluate their other argument only once.
 */

#define HeapTupleHeaderGetXmin(tup) \
( \
	(tup)->t_choice.t_heap.t_xmin \
)

#define HeapTupleHeaderSetXmin(tup, xid) \
( \
	(tup)->t_choice.t_heap.t_xmin = (xid) \
)

#define HeapTupleHeaderXminInvalid(tup) \
( \
    ((tup)->t_infomask & (HEAP_XMIN_COMMITTED|HEAP_XMIN_INVALID)) == \
        HEAP_XMIN_INVALID \
)

#define HeapTupleHeaderGetXmax(tup) \
( \
	(tup)->t_choice.t_heap.t_xmax \
)

#define HeapTupleHeaderSetXmax(tup, xid) \
( \
	(tup)->t_choice.t_heap.t_xmax = (xid) \
)

#define HeapTupleHeaderSetXminFrozen(tup) \
( \
    AssertMacro(!HeapTupleHeaderXminInvalid(tup)), \
    ((tup)->t_infomask |= HEAP_XMIN_FROZEN) \
)

/*
 * HeapTupleHeaderGetRawCommandId will give you what's in the header whether
 * it is useful or not.  Most code should use HeapTupleHeaderGetCmin or
 * HeapTupleHeaderGetCmax instead, but note that those Assert that you can
 * get a legitimate result, ie you are in the originating transaction!
 */
#define HeapTupleHeaderGetRawCommandId(tup) \
( \
	(tup)->t_choice.t_heap.t_field3.t_cid \
)

/* SetCmin is reasonably simple since we never need a combo CID */
#define HeapTupleHeaderSetCmin(tup, cid) \
do { \
	Assert(!((tup)->t_infomask & HEAP_MOVED)); \
	(tup)->t_choice.t_heap.t_field3.t_cid = (cid); \
	(tup)->t_infomask &= ~HEAP_COMBOCID; \
} while (0)

/* SetCmax must be used after HeapTupleHeaderAdjustCmax; see combocid.c */
#define HeapTupleHeaderSetCmax(tup, cid, iscombo) \
do { \
	Assert(!((tup)->t_infomask & HEAP_MOVED)); \
	(tup)->t_choice.t_heap.t_field3.t_cid = (cid); \
	if (iscombo) \
		(tup)->t_infomask |= HEAP_COMBOCID; \
	else \
		(tup)->t_infomask &= ~HEAP_COMBOCID; \
} while (0)

#define HeapTupleHeaderGetXvac(tup) \
( \
	((tup)->t_infomask & HEAP_MOVED) ? \
		(tup)->t_choice.t_heap.t_field3.t_xvac \
	: \
		InvalidTransactionId \
)

#define HeapTupleHeaderSetXvac(tup, xid) \
do { \
	Assert((tup)->t_infomask & HEAP_MOVED); \
	(tup)->t_choice.t_heap.t_field3.t_xvac = (xid); \
} while (0)

#define HeapTupleHeaderGetDatumLength(tup) \
	VARSIZE(tup)

#define HeapTupleHeaderSetDatumLength(tup, len) \
	SET_VARSIZE(tup, len)

#define HeapTupleHeaderGetTypeId(tup) \
( \
	(tup)->t_choice.t_datum.datum_typeid \
)

#define HeapTupleHeaderSetTypeId(tup, typeid) \
( \
	(tup)->t_choice.t_datum.datum_typeid = (typeid) \
)

#define HeapTupleHeaderGetTypMod(tup) \
( \
	(tup)->t_choice.t_datum.datum_typmod \
)

#define HeapTupleHeaderSetTypMod(tup, typmod) \
( \
	(tup)->t_choice.t_datum.datum_typmod = (typmod) \
)

#define HeapTupleHeaderGetOid(tup) \
( \
	((tup)->t_infomask & HEAP_HASOID) ? \
		*((Oid *) ((char *)(tup) + (tup)->t_hoff - sizeof(Oid))) \
	: \
		InvalidOid \
)

#define HeapTupleHeaderSetOid(tup, oid) \
do { \
	Assert((tup)->t_infomask & HEAP_HASOID); \
	*((Oid *) ((char *)(tup) + (tup)->t_hoff - sizeof(Oid))) = (oid); \
} while (0)

/*
 * Note that we stop considering a tuple HOT-updated as soon as it is known
 * aborted or the would-be updating transaction is known aborted.  For best
 * efficiency, check tuple visibility before using this macro, so that the
 * INVALID bits will be as up to date as possible.
 */
#define HeapTupleHeaderIsHotUpdated(tup) \
( \
	((tup)->t_infomask2 & HEAP_HOT_UPDATED) != 0 && \
	((tup)->t_infomask & (HEAP_XMIN_INVALID | HEAP_XMAX_INVALID)) == 0 \
)

#define HeapTupleHeaderSetHotUpdated(tup) \
( \
	(tup)->t_infomask2 |= HEAP_HOT_UPDATED \
)

#define HeapTupleHeaderClearHotUpdated(tup) \
( \
	(tup)->t_infomask2 &= ~HEAP_HOT_UPDATED \
)

#define HeapTupleHeaderIsHeapOnly(tup) \
( \
  (tup)->t_infomask2 & HEAP_ONLY_TUPLE \
)

#define HeapTupleHeaderSetHeapOnly(tup) \
( \
  (tup)->t_infomask2 |= HEAP_ONLY_TUPLE \
)

#define HeapTupleHeaderClearHeapOnly(tup) \
( \
  (tup)->t_infomask2 &= ~HEAP_ONLY_TUPLE \
)

#define HeapTupleHeaderHasMatch(tup) \
( \
  (tup)->t_infomask2 & HEAP_TUPLE_HAS_MATCH \
)

#define HeapTupleHeaderSetMatch(tup) \
( \
  (tup)->t_infomask2 |= HEAP_TUPLE_HAS_MATCH \
)

#define HeapTupleHeaderClearMatch(tup) \
( \
  (tup)->t_infomask2 &= ~HEAP_TUPLE_HAS_MATCH \
)

#define HeapTupleHeaderGetNatts(tup) \
	((tup)->t_infomask2 & HEAP_NATTS_MASK)

#define HeapTupleHeaderSetNatts(tup, natts) \
( \
	(tup)->t_infomask2 = ((tup)->t_infomask2 & ~HEAP_NATTS_MASK) | (natts) \
)


/*
 * BITMAPLEN(NATTS) -
 *		Computes size of null bitmap given number of data columns.
 */
#define BITMAPLEN(NATTS)	(((int)(NATTS) + 7) / 8)

/*
 * MaxHeapTupleSize is the maximum allowed size of a heap tuple, including
 * header and MAXALIGN alignment padding.  Basically it's BLCKSZ minus the
 * other stuff that has to be on a disk page.  Since heap pages use no
 * "special space", there's no deduction for that.
 *
 * NOTE: we allow for the ItemId that must point to the tuple, ensuring that
 * an otherwise-empty page can indeed hold a tuple of this size.  Because
 * ItemIds and tuples have different alignment requirements, don't assume that
 * you can, say, fit 2 tuples of size MaxHeapTupleSize/2 on the same page.
 */
#define MaxHeapTupleSize  (BLCKSZ - MAXALIGN(SizeOfPageHeaderData + sizeof(ItemIdData)))

/*
 * MaxHeapTuplesPerPage is an upper bound on the number of tuples that can
 * fit on one heap page.  (Note that indexes could have more, because they
 * use a smaller tuple header.)  We arrive at the divisor because each tuple
 * must be maxaligned, and it must have an associated item pointer.
 *
 * Note: with HOT, there could theoretically be more line pointers (not actual
 * tuples) than this on a heap page.  However we constrain the number of line
 * pointers to this anyway, to avoid excessive line-pointer bloat and not
 * require increases in the size of work arrays.
 */
#define MaxHeapTuplesPerPage	\
	((int) ((BLCKSZ - SizeOfPageHeaderData) / \
			(MAXALIGN(offsetof(HeapTupleHeaderData, t_bits)) + sizeof(ItemIdData))))

/*
 * MaxAttrSize is a somewhat arbitrary upper limit on the declared size of
 * data fields of char(n) and similar types.  It need not have anything
 * directly to do with the *actual* upper limit of varlena values, which
 * is currently 1Gb (see TOAST structures in postgres.h).  I've set it
 * at 10Mb which seems like a reasonable number --- tgl 8/6/00.
 */
#define MaxAttrSize		(10 * 1024 * 1024)


/*
 * MinimalTuple is an alternative representation that is used for transient
 * tuples inside the executor, in places where transaction status information
 * is not required, the tuple rowtype is known, and shaving off a few bytes
 * is worthwhile because we need to store many tuples.	The representation
 * is chosen so that tuple access routines can work with either full or
 * minimal tuples via a HeapTupleData pointer structure.  The access routines
 * see no difference, except that they must not access the transaction status
 * or t_ctid fields because those aren't there.
 *
 * For the most part, MinimalTuples should be accessed via TupleTableSlot
 * routines.  These routines will prevent access to the "system columns"
 * and thereby prevent accidental use of the nonexistent fields.
 *
 * MinimalTupleData contains a length word, some padding, and fields matching
 * HeapTupleHeaderData beginning with t_infomask2. The padding is chosen so
 * that offsetof(t_infomask2) is the same modulo MAXIMUM_ALIGNOF in both
 * structs.   This makes data alignment rules equivalent in both cases.
 *
 * When a minimal tuple is accessed via a HeapTupleData pointer, t_data is
 * set to point MINIMAL_TUPLE_OFFSET bytes before the actual start of the
 * minimal tuple --- that is, where a full tuple matching the minimal tuple's
 * data would start.  This trick is what makes the structs seem equivalent.
 *
 * Note that t_hoff is computed the same as in a full tuple, hence it includes
 * the MINIMAL_TUPLE_OFFSET distance.  t_len does not include that, however.
 *
 * MINIMAL_TUPLE_DATA_OFFSET is the offset to the first useful (non-pad) data
 * other than the length word.	tuplesort.c and tuplestore.c use this to avoid
 * writing the padding to disk.
 */
#define MINIMAL_TUPLE_OFFSET \
	((offsetof(HeapTupleHeaderData, t_infomask2) - sizeof(uint32)) / MAXIMUM_ALIGNOF * MAXIMUM_ALIGNOF)
#define MINIMAL_TUPLE_PADDING \
	((offsetof(HeapTupleHeaderData, t_infomask2) - sizeof(uint32)) % MAXIMUM_ALIGNOF)
#define MINIMAL_TUPLE_DATA_OFFSET \
	offsetof(MinimalTupleData, t_infomask2)

typedef struct MinimalTupleData
{
	uint32		t_len;			/* actual length of minimal tuple */

	char		mt_padding[MINIMAL_TUPLE_PADDING];

	/* Fields below here must match HeapTupleHeaderData! */

	uint16		t_infomask2;	/* number of attributes + various flags */

	uint16		t_infomask;		/* various flag bits, see below */

	uint8		t_hoff;			/* sizeof header incl. bitmap, padding */

	/* ^ - 23 bytes - ^ */

	bits8		t_bits[1];		/* bitmap of NULLs -- VARIABLE LENGTH */

	/* MORE DATA FOLLOWS AT END OF STRUCT */
} MinimalTupleData;
=======
typedef struct HeapTupleHeaderData HeapTupleHeaderData;

typedef HeapTupleHeaderData *HeapTupleHeader;

typedef struct MinimalTupleData MinimalTupleData;
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

typedef MinimalTupleData *MinimalTuple;


/*
 * HeapTupleData is an in-memory data structure that points to a tuple.
 *
 * There are several ways in which this data structure is used:
 *
 * * Pointer to a tuple in a disk buffer: t_data points directly into the
 *	 buffer (which the code had better be holding a pin on, but this is not
 *	 reflected in HeapTupleData itself).
 *
 * * Pointer to nothing: t_data is NULL.  This is used as a failure indication
 *	 in some functions.
 *
 * * Part of a palloc'd tuple: the HeapTupleData itself and the tuple
 *	 form a single palloc'd chunk.  t_data points to the memory location
 *	 immediately following the HeapTupleData struct (at offset HEAPTUPLESIZE).
 *	 This is the output format of heap_form_tuple and related routines.
 *
 * * Separately allocated tuple: t_data points to a palloc'd chunk that
 *	 is not adjacent to the HeapTupleData.  (This case is deprecated since
 *	 it's difficult to tell apart from case #1.  It should be used only in
 *	 limited contexts where the code knows that case #1 will never apply.)
 *
 * * Separately allocated minimal tuple: t_data points MINIMAL_TUPLE_OFFSET
 *	 bytes before the start of a MinimalTuple.  As with the previous case,
 *	 this can't be told apart from case #1 by inspection; code setting up
 *	 or destroying this representation has to know what it's doing.
 *
 * t_len should always be valid, except in the pointer-to-nothing case.
 * t_self and t_tableOid should be valid if the HeapTupleData points to
 * a disk buffer, or if it represents a copy of a tuple on disk.  They
 * should be explicitly set invalid in manufactured tuples.
 *
 * CDB: t_tableOid deleted.  Instead, use tts_tableOid in TupleTableSlot.
 */
typedef struct HeapTupleData
{
	uint32		t_len;			/* length of *t_data */
	ItemPointerData t_self;		/* SelfItemPointer */
	HeapTupleHeader t_data;		/* -> tuple header and data */
} HeapTupleData;

typedef HeapTupleData *HeapTuple;

#define HEAPTUPLESIZE	MAXALIGN(sizeof(HeapTupleData))

/*
<<<<<<< HEAD
 * GenericTuple is a pointer that can point to either a HeapTuple or a
 * MemTuple. Use is_memtuple() to check which one it is.
 *
 * GenericTupleData has no definition; this is a fake "supertype".
 */
struct GenericTupleData;
typedef struct GenericTupleData *GenericTuple;

/* XXX Hack Hack Hack 
 * heaptuple, or memtuple, cannot be more than 2G, so, if
 * the first bit is ever set, it is really a memtuple
 */
static inline bool is_memtuple(GenericTuple tup)
{
	return ((((HeapTuple) tup)->t_len & 0x80000000) != 0);
}

static inline bool is_heaptuple_splitter(HeapTuple htup)
{
	return ((char *) htup->t_data) != ((char *) htup + HEAPTUPLESIZE);
}
static inline uint32 heaptuple_get_size(HeapTuple htup)
{
	return htup->t_len + HEAPTUPLESIZE;
}

/*
 * GETSTRUCT - given a HeapTuple pointer, return address of the user data
 */
#define GETSTRUCT(TUP) ((char *) ((TUP)->t_data) + (TUP)->t_data->t_hoff)

/*
=======
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
 * Accessor macros to be used with HeapTuple pointers.
 */
#define HeapTupleIsValid(tuple) PointerIsValid(tuple)

/* HeapTupleHeader functions implemented in utils/time/combocid.c */
extern CommandId HeapTupleHeaderGetCmin(HeapTupleHeader tup);
extern CommandId HeapTupleHeaderGetCmax(HeapTupleHeader tup);
extern void HeapTupleHeaderAdjustCmax(HeapTupleHeader tup,
						  CommandId *cmax, bool *iscombo);

<<<<<<< HEAD
extern Datum nocachegetattr(HeapTuple tup, int attnum, TupleDesc att);
extern Datum heap_getsysattr(HeapTuple tup, int attnum, bool *isnull);

/* ----------------
 *		fastgetattr
 *
 *		Fetch a user attribute's value as a Datum (might be either a
 *		value, or a pointer into the data area of the tuple).
 *
 *		This must not be used when a system attribute might be requested.
 *		Furthermore, the passed attnum MUST be valid.  Use heap_getattr()
 *		instead, if in doubt.
 *
 *		This gets called many times, so we macro the cacheable and NULL
 *		lookups, and call nocachegetattr() for the rest.
 *
 *      CDB:  Implemented as inline function instead of macro.
 * ----------------
 */
static inline Datum
fastgetattr(HeapTuple tup, int attnum, TupleDesc tupleDesc, bool *isnull)
{
    Datum               result;
    Form_pg_attribute   att = tupleDesc->attrs[attnum-1];

    Assert(attnum > 0);

	*isnull = false;

    if (HeapTupleNoNulls(tup))
    {
        if (att->attcacheoff >= 0)
			result = fetchatt(att,
				              (char *)tup->t_data + tup->t_data->t_hoff +
					            att->attcacheoff);
        else
            result = nocachegetattr(tup, attnum, tupleDesc);
    }
    else if (att_isnull(attnum-1, tup->t_data->t_bits))
    {
        result = Int32GetDatum(0);
		*isnull = true;
    }
    else
        result = nocachegetattr(tup, attnum, tupleDesc);

    return result;
}                               /* fastgetattr */


/* ----------------
 *		heap_getattr
 *
 *		Extract an attribute of a heap tuple and return it as a Datum.
 *		This works for either system or user attributes.  The given attnum
 *		is properly range-checked.
 *
 *		If the field in question has a NULL value, we return a zero Datum
 *		and set *isnull == true.  Otherwise, we set *isnull == false.
 *
 *		<tup> is the pointer to the heap tuple.  <attnum> is the attribute
 *		number of the column (field) caller wants.	<tupleDesc> is a
 *		pointer to the structure describing the row and all its fields.
 *
 *      GPDB:  Implemented as inline function instead of macro.
 * ----------------
 */
static inline Datum
heap_getattr(HeapTuple tup, int attnum, TupleDesc tupleDesc, bool *isnull)
{
    Datum       result;

    Assert(tup != NULL);

    if (attnum > (int)HeapTupleHeaderGetNatts(tup->t_data))
    {
        result = DatumGetInt32(0);
		*isnull = true;
    }
    else if (attnum > 0)
        result = fastgetattr(tup, attnum, tupleDesc, isnull);
    else
        result = heap_getsysattr(tup, attnum, isnull);

    return result;
}                               /* heap_getattr */

/* prototypes for functions in common/heaptuple.c */
extern Size heap_compute_data_size(TupleDesc tupleDesc,
					   Datum *values, bool *isnull);
extern Size heap_fill_tuple(TupleDesc tupleDesc,
				Datum *values, bool *isnull,
				char *data, Size data_size,
				uint16 *infomask, bits8 *bit);
extern Datum nocachegetattr(HeapTuple tup, int attnum, TupleDesc att);
extern Datum heap_getsysattr(HeapTuple tup, int attnum, bool *isnull);
extern bool heap_attisnull(HeapTuple tup, int attnum);
extern bool heap_attisnull_normalattr(HeapTuple tup, int attnum);

extern HeapTuple heaptuple_copy_to(HeapTuple tup, HeapTuple result, uint32 *len);

static inline HeapTuple heap_copytuple(HeapTuple tuple)
{
	return heaptuple_copy_to(tuple, NULL, NULL);
}

extern void heap_copytuple_with_tuple(HeapTuple src, HeapTuple dest);

extern HeapTuple heaptuple_form_to(TupleDesc tupdesc, Datum* values,
								   bool *isnull, HeapTuple tup, uint32 *len);
static inline HeapTuple heap_form_tuple(TupleDesc tupleDescriptor,
				Datum *values, bool *isnull)
{
	return heaptuple_form_to(tupleDescriptor, values, isnull, NULL, NULL);
}

extern HeapTuple heap_modify_tuple(HeapTuple tuple,
				  TupleDesc tupleDesc,
				  Datum *replValues,
				  bool *replIsnull,
				  bool *doReplace);
extern void heap_deform_tuple(HeapTuple tuple, TupleDesc tupleDesc,
				  Datum *values, bool *isnull);

/* these three are deprecated versions of the three above: */
extern HeapTuple heap_formtuple(TupleDesc tupleDescriptor,
			   Datum *values, char *nulls) __attribute__ ((deprecated));
extern HeapTuple heap_modifytuple(HeapTuple tuple,
				 TupleDesc tupleDesc,
				 Datum *replValues,
				 char *replNulls,
				 char *replActions) __attribute__ ((deprecated));
extern void heap_deformtuple(HeapTuple tuple, TupleDesc tupleDesc,
				 Datum *values, char *nulls) __attribute__ ((deprecated));
extern void heap_freetuple(HeapTuple htup);
extern MinimalTuple heap_form_minimal_tuple(TupleDesc tupleDescriptor,
						Datum *values, bool *isnull);
extern void heap_free_minimal_tuple(MinimalTuple mtup);
extern MinimalTuple heap_copy_minimal_tuple(MinimalTuple mtup);
extern HeapTuple heap_tuple_from_minimal_tuple(MinimalTuple mtup);
extern MinimalTuple minimal_tuple_from_heap_tuple(HeapTuple htup);
=======
/* Prototype for HeapTupleHeader accessors in heapam.c */
extern TransactionId HeapTupleGetUpdateXid(HeapTupleHeader tuple);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

#endif   /* HTUP_H */
