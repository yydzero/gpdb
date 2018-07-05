/*-------------------------------------------------------------------------
 *
 * catalog.h
 *	  prototypes for functions in backend/catalog/catalog.c
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/catalog.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CATALOG_H
#define CATALOG_H

/*
 *	'pgrminclude ignore' needed here because CppAsString2() does not throw
 *	an error if the symbol is not defined.
 */
#include "catalog/catversion.h" /* pgrminclude ignore */
#include "catalog/pg_class.h"
#include "utils/relcache.h"

#define OIDCHARS		10		/* max chars printed by %u */
/*
 * In PostgreSQL, this is called just TABLESPACE_VERSION_DIRECTORY. But in 
 * GPDB, you should use tablespace_version_directory() function instead.
 * This constant has been renamed so that we catch and know to modify all
 * upstream uses of TABLESPACE_VERSION_DIRECTORY.
 */
#define GP_TABLESPACE_VERSION_DIRECTORY	"GPDB_" GP_MAJORVERSION "_" \
									CppAsString2(CATALOG_VERSION_NO)

extern const char *tablespace_version_directory(void);

extern bool IsSystemRelation(Relation relation);
extern bool IsToastRelation(Relation relation);
extern bool IsCatalogRelation(Relation relation);

extern bool IsSystemClass(Oid relid, Form_pg_class reltuple);
extern bool IsToastClass(Form_pg_class reltuple);
extern bool IsCatalogClass(Oid relid, Form_pg_class reltuple);

extern bool IsSystemNamespace(Oid namespaceId);
extern bool IsToastNamespace(Oid namespaceId);
extern bool IsAoSegmentNamespace(Oid namespaceId);

extern bool IsReservedName(const char *name);
extern char* GetReservedPrefix(const char *name);

extern bool IsSharedRelation(Oid relationId);

extern Oid GetNewOid(Relation relation);
extern Oid GetNewOidWithIndex(Relation relation, Oid indexId,
				   AttrNumber oidcolumn);
extern Oid GetNewSequenceRelationOid(Relation relation);
extern Oid GetNewRelFileNode(Oid reltablespace, Relation pg_class,
				  char relpersistence);

const char *tablespace_version_directory(void);

#endif   /* CATALOG_H */
