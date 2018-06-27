/*-------------------------------------------------------------------------
 *
 * pg_class.h
 *	  definition of the system "relation" relation (pg_class)
 *	  along with the relation's initial contents.
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_class.h
 *
 * NOTES
 *	  the genbki.pl script reads this file and generates .bki
 *	  information from the DATA() statements.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_CLASS_H
#define PG_CLASS_H

#include "catalog/genbki.h"

/* ----------------
 *		postgres.h contains the system type definitions and the
 *		CATALOG(), BKI_BOOTSTRAP and DATA() sugar words so this file
 *		can be read by both genbki.sh and the C compiler.
 * ----------------
 */
#define RelationRelationId	1259
#define RelationRelation_Rowtype_Id  83

CATALOG(pg_class,1259) BKI_BOOTSTRAP BKI_ROWTYPE_OID(83) BKI_SCHEMA_MACRO
{
	NameData	relname;		/* class name */
	Oid			relnamespace;	/* OID of namespace containing this class */
	Oid			reltype;		/* OID of entry in pg_type for table's
								 * implicit row type */
	Oid			reloftype;		/* OID of entry in pg_type for underlying
								 * composite type */
	Oid			relowner;		/* class owner */
	Oid			relam;			/* index access method; 0 if not an index */
	Oid			relfilenode;	/* identifier of physical storage file */

	/* relfilenode == 0 means it is a "mapped" relation, see relmapper.c */
	Oid			reltablespace;	/* identifier of table space for relation */
	int32		relpages;		/* # of blocks (not always up-to-date) */
	float4		reltuples;		/* # of tuples (not always up-to-date) */
	int32		relallvisible;	/* # of all-visible blocks (not always
								 * up-to-date) */
	Oid			reltoastrelid;	/* OID of toast table; 0 if none */
	bool		relhasindex;	/* T if has (or has had) any indexes */
	bool		relisshared;	/* T if shared across databases */
	char		relpersistence; /* see RELPERSISTENCE_xxx constants below */
	char		relkind;		/* see RELKIND_xxx constants below */
<<<<<<< HEAD
	char		relstorage;		/* see RELSTORAGE_xxx constants below */
	int2		relnatts;		/* number of user attributes */
=======
	int16		relnatts;		/* number of user attributes */
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

	/*
	 * Class pg_attribute must contain exactly "relnatts" user attributes
	 * (with attnums ranging from 1 to relnatts) for this class.  It may also
	 * contain entries with negative attnums for system attributes.
	 */
	int16		relchecks;		/* # of CHECK constraints for class */
	bool		relhasoids;		/* T if we generate OIDs for rows of rel */
	bool		relhaspkey;		/* has (or has had) PRIMARY KEY index */
	bool		relhasrules;	/* has (or has had) any rules */
	bool		relhastriggers; /* has (or has had) any TRIGGERs */
	bool		relhassubclass; /* has (or has had) derived classes */
	bool		relrowsecurity; /* row security is enabled or not */
	bool		relispopulated; /* matview currently holds query results */
	char		relreplident;	/* see REPLICA_IDENTITY_xxx constants  */
	TransactionId relfrozenxid; /* all Xids < this are frozen in this rel */
	TransactionId relminmxid;	/* all multixacts in this rel are >= this.
								 * this is really a MultiXactId */

#ifdef CATALOG_VARLEN			/* variable-length fields start here */
	/* NOTE: These fields are not present in a relcache entry's rd_rel field. */
	aclitem		relacl[1];		/* access permissions */
	text		reloptions[1];	/* access-method-specific options */
#endif
} FormData_pg_class;

/* GPDB added foreign key definitions for gpcheckcat. */
FOREIGN_KEY(relnamespace REFERENCES pg_namespace(oid));
FOREIGN_KEY(reltype REFERENCES pg_type(oid));
FOREIGN_KEY(relowner REFERENCES pg_authid(oid));
FOREIGN_KEY(relam REFERENCES pg_am(oid));
FOREIGN_KEY(reltablespace REFERENCES pg_tablespace(oid));
FOREIGN_KEY(reltoastrelid REFERENCES pg_class(oid));
FOREIGN_KEY(reltoastidxid REFERENCES pg_class(oid));

/* Size of fixed part of pg_class tuples, not counting var-length fields */
#define CLASS_TUPLE_SIZE \
	 (offsetof(FormData_pg_class,relminmxid) + sizeof(TransactionId))

/* ----------------
 *		Form_pg_class corresponds to a pointer to a tuple with
 *		the format of pg_class relation.
 * ----------------
 */
typedef FormData_pg_class *Form_pg_class;

/* ----------------
 *		compiler constants for pg_class
 * ----------------
 */

<<<<<<< HEAD
#define Natts_pg_class					28
=======
#define Natts_pg_class					30
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
#define Anum_pg_class_relname			1
#define Anum_pg_class_relnamespace		2
#define Anum_pg_class_reltype			3
#define Anum_pg_class_reloftype			4
#define Anum_pg_class_relowner			5
#define Anum_pg_class_relam				6
#define Anum_pg_class_relfilenode		7
#define Anum_pg_class_reltablespace		8
#define Anum_pg_class_relpages			9
#define Anum_pg_class_reltuples			10
#define Anum_pg_class_relallvisible		11
#define Anum_pg_class_reltoastrelid		12
<<<<<<< HEAD
#define Anum_pg_class_reltoastidxid		13
#define Anum_pg_class_relhasindex		14
#define Anum_pg_class_relisshared		15
#define Anum_pg_class_relpersistence	16
#define Anum_pg_class_relkind			17
#define Anum_pg_class_relstorage		18 /* GPDB specific */
#define Anum_pg_class_relnatts			19
#define Anum_pg_class_relchecks			20
#define Anum_pg_class_relhasoids		21
#define Anum_pg_class_relhaspkey		22
#define Anum_pg_class_relhasrules		23
#define Anum_pg_class_relhastriggers	24
#define Anum_pg_class_relhassubclass	25
#define Anum_pg_class_relfrozenxid		26
#define Anum_pg_class_relacl			27
#define Anum_pg_class_reloptions		28
=======
#define Anum_pg_class_relhasindex		13
#define Anum_pg_class_relisshared		14
#define Anum_pg_class_relpersistence	15
#define Anum_pg_class_relkind			16
#define Anum_pg_class_relnatts			17
#define Anum_pg_class_relchecks			18
#define Anum_pg_class_relhasoids		19
#define Anum_pg_class_relhaspkey		20
#define Anum_pg_class_relhasrules		21
#define Anum_pg_class_relhastriggers	22
#define Anum_pg_class_relhassubclass	23
#define Anum_pg_class_relrowsecurity	24
#define Anum_pg_class_relispopulated	25
#define Anum_pg_class_relreplident		26
#define Anum_pg_class_relfrozenxid		27
#define Anum_pg_class_relminmxid		28
#define Anum_pg_class_relacl			29
#define Anum_pg_class_reloptions		30
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

/* ----------------
 *		initial contents of pg_class
 *
 * NOTE: only "bootstrapped" relations need to be declared here.  Be sure that
 * the OIDs listed here match those given in their CATALOG macros, and that
 * the relnatts values are correct.
 * ----------------
 */

<<<<<<< HEAD
/* Note: "3" in the relfrozenxid column stands for FirstNormalTransactionId */
DATA(insert OID = 1247 (  pg_type		PGNSP 71 0 PGUID 0 0 0 0 0 0 0 0 f f p r h 30 0 t f f f f 3 _null_ _null_ ));
DESCR("");
DATA(insert OID = 1249 (  pg_attribute	PGNSP 75 0 PGUID 0 0 0 0 0 0 0 0 f f p r h 21 0 f f f f f 3 _null_ _null_ ));
DESCR("");
DATA(insert OID = 1255 (  pg_proc		PGNSP 81 0 PGUID 0 0 0 0 0 0 0 0 f f p r h 29 0 t f f f f 3 _null_ _null_ ));
DESCR("");
DATA(insert OID = 1259 (  pg_class		PGNSP 83 0 PGUID 0 0 0 0 0 0 0 0 f f p r h 28 0 t f f f f 3 _null_ _null_ ));
=======
/*
 * Note: "3" in the relfrozenxid column stands for FirstNormalTransactionId;
 * similarly, "1" in relminmxid stands for FirstMultiXactId
 */
DATA(insert OID = 1247 (  pg_type		PGNSP 71 0 PGUID 0 0 0 0 0 0 0 f f p r 30 0 t f f f f f t n 3 1 _null_ _null_ ));
DESCR("");
DATA(insert OID = 1249 (  pg_attribute	PGNSP 75 0 PGUID 0 0 0 0 0 0 0 f f p r 21 0 f f f f f f t n 3 1 _null_ _null_ ));
DESCR("");
DATA(insert OID = 1255 (  pg_proc		PGNSP 81 0 PGUID 0 0 0 0 0 0 0 f f p r 28 0 t f f f f f t n 3 1 _null_ _null_ ));
DESCR("");
DATA(insert OID = 1259 (  pg_class		PGNSP 83 0 PGUID 0 0 0 0 0 0 0 f f p r 30 0 t f f f f f t n 3 1 _null_ _null_ ));
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
DESCR("");


#define		  RELKIND_RELATION		  'r'		/* ordinary table */
#define		  RELKIND_INDEX			  'i'		/* secondary index */
#define		  RELKIND_SEQUENCE		  'S'		/* sequence object */
#define		  RELKIND_TOASTVALUE	  't'		/* for out-of-line values */
#define		  RELKIND_VIEW			  'v'		/* view */
#define		  RELKIND_COMPOSITE_TYPE  'c'		/* composite type */
#define		  RELKIND_FOREIGN_TABLE   'f'		/* foreign table */
<<<<<<< HEAD
#define		  RELKIND_UNCATALOGED	  'u'		/* not yet cataloged */
#define		  RELKIND_AOSEGMENTS	  'o'		/* AO segment files and eof's */
#define		  RELKIND_AOBLOCKDIR	  'b'		/* AO block directory */
#define		  RELKIND_AOVISIMAP		  'm'		/* AO visibility map */
=======
#define		  RELKIND_MATVIEW		  'm'		/* materialized view */
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

#define		  RELPERSISTENCE_PERMANENT	'p'		/* regular table */
#define		  RELPERSISTENCE_UNLOGGED	'u'		/* unlogged permanent table */
#define		  RELPERSISTENCE_TEMP		't'		/* temporary table */

<<<<<<< HEAD
/*
 * relstorage describes how a relkind is physically stored in the database.
 *
 * RELSTORAGE_HEAP    - stored on disk using heap storage.
 * RELSTORAGE_AOROWS  - stored on disk using append only storage.
 * RELSTORAGE_AOCOLS  - stored on dist using append only column storage.
 * RELSTORAGE_VIRTUAL - has virtual storage, meaning, relation has no
 *						data directly stored forit  (right now this
 *						relates to views and comp types).
 * RELSTORAGE_EXTERNAL-	stored externally using external tables.
 * RELSTORAGE_FOREIGN - stored in another server.  
 */
#define		  RELSTORAGE_HEAP	'h'
#define		  RELSTORAGE_AOROWS	'a'
#define 	  RELSTORAGE_AOCOLS	'c'
#define		  RELSTORAGE_PARQUET 'p'
#define		  RELSTORAGE_VIRTUAL	'v'
#define		  RELSTORAGE_EXTERNAL 'x'
#define		  RELSTORAGE_FOREIGN 'f'

static inline bool relstorage_is_heap(char c)
{
	return (c == RELSTORAGE_HEAP);
}

static inline bool relstorage_is_ao(char c)
{
	return (c == RELSTORAGE_AOROWS || c == RELSTORAGE_AOCOLS);
}

static inline bool relstorage_is_external(char c)
{
	return (c == RELSTORAGE_EXTERNAL);
}

static inline bool relstorage_is_foreign(char c)
{
	return (c == RELSTORAGE_FOREIGN);
}
=======
/* default selection for replica identity (primary key or nothing) */
#define		  REPLICA_IDENTITY_DEFAULT	'd'
/* no replica identity is logged for this relation */
#define		  REPLICA_IDENTITY_NOTHING	'n'
/* all columns are logged as replica identity */
#define		  REPLICA_IDENTITY_FULL		'f'
/*
 * an explicitly chosen candidate key's columns are used as identity;
 * will still be set if the index has been dropped, in that case it
 * has the same meaning as 'd'
 */
#define		  REPLICA_IDENTITY_INDEX	'i'
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
#endif   /* PG_CLASS_H */
