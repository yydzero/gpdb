/*-------------------------------------------------------------------------
 *
 * catalog.c
 *		routines concerned with catalog naming conventions and other
 *		bits of hard-wired knowledge
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/catalog/catalog.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <fcntl.h>
#include <unistd.h>

#include "access/genam.h"
#include "access/sysattr.h"
#include "access/transam.h"
#include "catalog/catalog.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_amop.h"
#include "catalog/pg_amproc.h"
#include "catalog/pg_auth_members.h"
#include "catalog/pg_auth_time_constraint.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_database.h"
#include "catalog/pg_exttable.h"
#include "catalog/pg_largeobject.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_pltemplate.h"
#include "catalog/pg_resourcetype.h"
#include "catalog/pg_resqueue.h"
#include "catalog/pg_resqueuecapability.h"
#include "catalog/pg_resgroup.h"
#include "catalog/pg_db_role_setting.h"
#include "catalog/pg_replication_origin.h"
#include "catalog/pg_shdepend.h"
#include "catalog/pg_shdescription.h"
#include "catalog/pg_shseclabel.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_rewrite.h"
#include "catalog/pg_statistic.h"
#include "catalog/pg_trigger.h"

#include "catalog/gp_configuration_history.h"
#include "catalog/gp_segment_config.h"
#include "catalog/pg_stat_last_operation.h"
#include "catalog/pg_stat_last_shoperation.h"

#include "catalog/gp_id.h"
#include "catalog/gp_version.h"
#include "catalog/toasting.h"

#include "miscadmin.h"
#include "storage/fd.h"
#include "utils/fmgroids.h"
#include "utils/rel.h"
#include "utils/tqual.h"

#include "cdb/cdbvars.h"

/*
 * Return directory name within tablespace location to use, for this server.
 * This is the GDPB replacement for PostgreSQL's TABLESPACE_VERSION_DIRECTORY
 * constant.
 */
const char *
tablespace_version_directory(void)
{
	return psprintf("%s_db%d", GP_TABLESPACE_VERSION_DIRECTORY, GpIdentity.dbid);
}

/*
 * IsSystemRelation
 *		True iff the relation is either a system catalog or toast table.
 *		By a system catalog, we mean one that created in the pg_catalog schema
 *		during initdb.  User-created relations in pg_catalog don't count as
 *		system catalogs.
 *
 *		NB: TOAST relations are considered system relations by this test
 *		for compatibility with the old IsSystemRelationName function.
 *		This is appropriate in many places but not all.  Where it's not,
 *		also check IsToastRelation or use IsCatalogRelation().
 */
bool
IsSystemRelation(Relation relation)
{
<<<<<<< HEAD
	return IsSystemNamespace(RelationGetNamespace(relation)) ||
		   IsToastNamespace(RelationGetNamespace(relation)) ||
		   IsAoSegmentNamespace(RelationGetNamespace(relation));
=======
	return IsSystemClass(RelationGetRelid(relation), relation->rd_rel);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
}

/*
 * IsSystemClass
 *		Like the above, but takes a Form_pg_class as argument.
 *		Used when we do not want to open the relation and have to
 *		search pg_class directly.
 */
bool
IsSystemClass(Oid relid, Form_pg_class reltuple)
{
	return IsToastClass(reltuple) || IsCatalogClass(relid, reltuple);
}

/*
 * IsCatalogRelation
 *		True iff the relation is a system catalog, or the toast table for
 *		a system catalog.  By a system catalog, we mean one that created
 *		in the pg_catalog schema during initdb.  As with IsSystemRelation(),
 *		user-created relations in pg_catalog don't count as system catalogs.
 *
 *		Note that IsSystemRelation() returns true for ALL toast relations,
 *		but this function returns true only for toast relations of system
 *		catalogs.
 */
bool
IsCatalogRelation(Relation relation)
{
	return IsCatalogClass(RelationGetRelid(relation), relation->rd_rel);
}

/*
 * IsCatalogClass
 *		True iff the relation is a system catalog relation.
 *
 * Check IsCatalogRelation() for details.
 */
bool
IsCatalogClass(Oid relid, Form_pg_class reltuple)
{
	Oid			relnamespace = reltuple->relnamespace;

<<<<<<< HEAD
	return IsSystemNamespace(relnamespace) ||
		IsToastNamespace(relnamespace) ||
		IsAoSegmentNamespace(relnamespace);
=======
	/*
	 * Never consider relations outside pg_catalog/pg_toast to be catalog
	 * relations.
	 */
	if (!IsSystemNamespace(relnamespace) && !IsToastNamespace(relnamespace))
		return false;

	/* ----
	 * Check whether the oid was assigned during initdb, when creating the
	 * initial template database. Minus the relations in information_schema
	 * excluded above, these are integral part of the system.
	 * We could instead check whether the relation is pinned in pg_depend, but
	 * this is noticeably cheaper and doesn't require catalog access.
	 *
	 * This test is safe since even an oid wraparound will preserve this
	 * property (c.f. GetNewObjectId()) and it has the advantage that it works
	 * correctly even if a user decides to create a relation in the pg_catalog
	 * namespace.
	 * ----
	 */
	return relid < FirstNormalObjectId;
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
}

/*
 * IsToastRelation
 *		True iff relation is a TOAST support relation (or index).
 */
bool
IsToastRelation(Relation relation)
{
	return IsToastNamespace(RelationGetNamespace(relation));
}

/*
 * IsToastClass
 *		Like the above, but takes a Form_pg_class as argument.
 *		Used when we do not want to open the relation and have to
 *		search pg_class directly.
 */
bool
IsToastClass(Form_pg_class reltuple)
{
	Oid			relnamespace = reltuple->relnamespace;

	return IsToastNamespace(relnamespace);
}

/*
 * IsSystemNamespace
 *		True iff namespace is pg_catalog.
 *
 * NOTE: the reason this isn't a macro is to avoid having to include
 * catalog/pg_namespace.h in a lot of places.
 */
bool
IsSystemNamespace(Oid namespaceId)
{
	return namespaceId == PG_CATALOG_NAMESPACE;
}

/*
 * IsToastNamespace
 *		True iff namespace is pg_toast or my temporary-toast-table namespace.
 *
 * Note: this will return false for temporary-toast-table namespaces belonging
 * to other backends.  Those are treated the same as other backends' regular
 * temp table namespaces, and access is prevented where appropriate.
 */
bool
IsToastNamespace(Oid namespaceId)
{
	return (namespaceId == PG_TOAST_NAMESPACE) ||
		isTempToastNamespace(namespaceId);
}

/*
 * IsAoSegmentNamespace
 *		True iff namespace is pg_aoseg.
 *
 * NOTE: the reason this isn't a macro is to avoid having to include
 * catalog/pg_namespace.h in a lot of places.
 */
bool
IsAoSegmentNamespace(Oid namespaceId)
{
	return namespaceId == PG_AOSEGMENT_NAMESPACE;
}

/*
 * IsReservedName
 *		True iff name starts with the pg_ prefix.
 *
 *		For some classes of objects, the prefix pg_ is reserved for
 *		system objects only.  As of 8.0, this is only true for
 *		schema and tablespace names.
 *
 *      As of Greenplum 4.0 we also reserve the prefix gp_
 */
bool
IsReservedName(const char *name)
{
	/* ugly coding for speed */
	return ((name[0] == 'p' && name[1] == 'g' && name[2] == '_') ||
			(name[0] == 'g' && name[1] == 'p' && name[2] == '_'));
}

/*
 * GetReservedPrefix
 *		Given a string that is a reserved name return the portion of
 *      the name that makes it reserved - the reserved prefix.
 *
 *      Current return values include "pg_" and "gp_"
 */
char *
GetReservedPrefix(const char *name)
{
	char		*prefix = NULL;

	if (IsReservedName(name))
	{
		prefix = palloc(4);
		memcpy(prefix, name, 3);
		prefix[3] = '\0';
	}

	return prefix;
}

/*
 * IsSharedRelation
 *		Given the OID of a relation, determine whether it's supposed to be
 *		shared across an entire database cluster.
 *
 * In older releases, this had to be hard-wired so that we could compute the
 * locktag for a relation and lock it before examining its catalog entry.
 * Since we now have MVCC catalog access, the race conditions that made that
 * a hard requirement are gone, so we could look at relaxing this restriction.
 * However, if we scanned the pg_class entry to find relisshared, and only
 * then locked the relation, pg_class could get updated in the meantime,
 * forcing us to scan the relation again, which would definitely be complex
 * and might have undesirable performance consequences.  Fortunately, the set
 * of shared relations is fairly static, so a hand-maintained list of their
 * OIDs isn't completely impractical.
 */
bool
IsSharedRelation(Oid relationId)
{
	/* These are the shared catalogs (look for BKI_SHARED_RELATION) */
	if (relationId == AuthIdRelationId ||
		relationId == AuthMemRelationId ||
		relationId == DatabaseRelationId ||
		relationId == PLTemplateRelationId ||
		relationId == SharedDescriptionRelationId ||
		relationId == SharedDependRelationId ||
		relationId == SharedSecLabelRelationId ||
		relationId == TableSpaceRelationId ||
		relationId == DbRoleSettingRelationId ||
		relationId == ReplicationOriginRelationId)
		return true;

	/* GPDB additions */
	if (relationId == GpIdRelationId ||
		relationId == GpVersionRelationId ||

		/* MPP-6929: metadata tracking */
		relationId == StatLastShOpRelationId ||

		relationId == ResQueueRelationId ||
		relationId == ResourceTypeRelationId ||
		relationId == ResQueueCapabilityRelationId ||
		relationId == ResGroupRelationId ||
		relationId == ResGroupCapabilityRelationId ||
		relationId == GpConfigHistoryRelationId ||
		relationId == GpSegmentConfigRelationId ||

		relationId == AuthTimeConstraintRelationId)
		return true;

	/* These are their indexes (see indexing.h) */
	if (relationId == AuthIdRolnameIndexId ||
		relationId == AuthIdOidIndexId ||
		relationId == AuthMemRoleMemIndexId ||
		relationId == AuthMemMemRoleIndexId ||
		relationId == DatabaseNameIndexId ||
		relationId == DatabaseOidIndexId ||
		relationId == PLTemplateNameIndexId ||
		relationId == SharedDescriptionObjIndexId ||
		relationId == SharedDependDependerIndexId ||
		relationId == SharedDependReferenceIndexId ||
		relationId == SharedSecLabelObjectIndexId ||
		relationId == TablespaceOidIndexId ||
		relationId == TablespaceNameIndexId ||
		relationId == DbRoleSettingDatidRolidIndexId ||
		relationId == ReplicationOriginIdentIndex ||
		relationId == ReplicationOriginNameIndex)
		return true;

	/* GPDB added indexes */
	if (/* MPP-6929: metadata tracking */
		relationId == StatLastShOpClassidObjidIndexId ||
		relationId == StatLastShOpClassidObjidStaactionnameIndexId ||

		relationId == ResQueueOidIndexId ||
		relationId == ResQueueRsqnameIndexId ||
		relationId == ResourceTypeOidIndexId ||
		relationId == ResourceTypeRestypidIndexId ||
		relationId == ResourceTypeResnameIndexId ||
		relationId == ResQueueCapabilityOidIndexId ||
		relationId == ResQueueCapabilityResqueueidIndexId ||
		relationId == ResQueueCapabilityRestypidIndexId ||
		relationId == ResGroupOidIndexId ||
		relationId == ResGroupRsgnameIndexId ||
		relationId == ResGroupCapabilityOidIndexId ||
		relationId == ResGroupCapabilityResgroupidIndexId ||
		relationId == ResGroupCapabilityResgroupidResLimittypeIndexId ||
		relationId == AuthIdRolResQueueIndexId ||
		relationId == AuthIdRolResGroupIndexId ||
		relationId == GpSegmentConfigContentPreferred_roleIndexId ||
		relationId == GpSegmentConfigDbidIndexId ||
		relationId == AuthTimeConstraintAuthIdIndexId)
	{
		return true;
	}

	/* These are their toast tables and toast indexes (see toasting.h) */
	if (relationId == PgShdescriptionToastTable ||
		relationId == PgShdescriptionToastIndex ||
		relationId == PgDbRoleSettingToastTable ||
		relationId == PgDbRoleSettingToastIndex ||
		relationId == PgShseclabelToastTable ||
		relationId == PgShseclabelToastIndex)
		return true;

	/* GPDB added toast tables and their indexes */
	if (relationId == GpSegmentConfigToastTable ||
		relationId == GpSegmentConfigToastIndex)
	{
		return true;
	}
	return false;
}

/*
 * OIDs for catalog object are normally allocated in the master, and
 * executor nodes should just use the OIDs passed by the master. But
 * there are some exceptions.
 */
static bool
RelationNeedsSynchronizedOIDs(Relation relation)
{
	if (IsSystemNamespace(RelationGetNamespace(relation)))
	{
		switch(RelationGetRelid(relation))
		{
			/*
			 * pg_largeobject is more like a user table, and has
			 * different contents in each segment and master.
			 */
			case LargeObjectRelationId:
				return false;

			/*
			 * We don't currently synchronize the OIDs of these catalogs.
			 * It's a bit sketchy that we don't, but we get away with it
			 * because these OIDs don't appear in any of the Node structs
			 * that are dispatched from master to segments. (Except for the
			 * OIDs, the contents of these tables should be in sync.)
			 */
			case RewriteRelationId:
			case TriggerRelationId:
			case AccessMethodOperatorRelationId:
			case AccessMethodProcedureRelationId:
				return false;
		}

		/*
		 * All other system catalogs are assumed to need synchronized
		 * OIDs.
		 */
		return true;
	}
	return false;
}

/*
 * GetNewOid
 *		Generate a new OID that is unique within the given relation.
 *
 * Caller must have a suitable lock on the relation.
 *
 * Uniqueness is promised only if the relation has a unique index on OID.
 * This is true for all system catalogs that have OIDs, but might not be
 * true for user tables.  Note that we are effectively assuming that the
 * table has a relatively small number of entries (much less than 2^32)
 * and there aren't very long runs of consecutive existing OIDs.  Again,
 * this is reasonable for system catalogs but less so for user tables.
 *
 * Since the OID is not immediately inserted into the table, there is a
 * race condition here; but a problem could occur only if someone else
 * managed to cycle through 2^32 OIDs and generate the same OID before we
 * finish inserting our row.  This seems unlikely to be a problem.  Note
 * that if we had to *commit* the row to end the race condition, the risk
 * would be rather higher; therefore we use SnapshotDirty in the test,
 * so that we will see uncommitted rows.
 */
Oid
GetNewOid(Relation relation)
{
	Oid			newOid;
	Oid			oidIndex;

	/* If relation doesn't have OIDs at all, caller is confused */
	Assert(relation->rd_rel->relhasoids);

	/* In bootstrap mode, we don't have any indexes to use */
	if (IsBootstrapProcessingMode())
		return GetNewObjectId();

	/* The relcache will cache the identity of the OID index for us */
	oidIndex = RelationGetOidIndex(relation);

	/* If no OID index, just hand back the next OID counter value */
	if (!OidIsValid(oidIndex))
	{
		/*
		 * System catalogs that have OIDs should *always* have a unique OID
		 * index; we should only take this path for user tables. Give a
		 * warning if it looks like somebody forgot an index.
		 */
		if (IsSystemRelation(relation))
			elog(WARNING, "generating possibly-non-unique OID for \"%s\"",
				 RelationGetRelationName(relation));

		return GetNewObjectId();
	}

	/* Otherwise, use the index to find a nonconflicting OID */
	do {
		newOid = GetNewOidWithIndex(relation, oidIndex, ObjectIdAttributeNumber);
	} while(!IsOidAcceptable(newOid));

	/*
	 * Most catalog objects need to have the same OID in the master and all
	 * segments. When creating a new object, the master should allocate the
	 * OID and tell the segments to use the same, so segments should have no
	 * need to ever allocate OIDs on their own. Therefore, give a WARNING if
	 * GetNewOid() is called in a segment. (There are a few exceptions, see
	 * RelationNeedsSynchronizedOIDs()).
	 */
	if (Gp_role == GP_ROLE_EXECUTE && RelationNeedsSynchronizedOIDs(relation))
		elog(PANIC, "allocated OID %u for relation \"%s\" in segment",
			 newOid, RelationGetRelationName(relation));

	return newOid;
}

/*
 * GetNewOidWithIndex
 *		Guts of GetNewOid: use the supplied index
 *
 * This is exported separately because there are cases where we want to use
 * an index that will not be recognized by RelationGetOidIndex: TOAST tables
 * have indexes that are usable, but have multiple columns and are on
 * ordinary columns rather than a true OID column.  This code will work
 * anyway, so long as the OID is the index's first column.  The caller must
 * pass in the actual heap attnum of the OID column, however.
 *
 * Caller must have a suitable lock on the relation.
 */
Oid
GetNewOidWithIndex(Relation relation, Oid indexId, AttrNumber oidcolumn)
{
	Oid			newOid;
	SnapshotData SnapshotDirty;
	SysScanDesc scan;
	ScanKeyData key;
	bool		collides;

	InitDirtySnapshot(SnapshotDirty);

	/* Generate new OIDs until we find one not in the table */
	do
	{
		CHECK_FOR_INTERRUPTS();

		newOid = GetNewObjectId();

		ScanKeyInit(&key,
					oidcolumn,
					BTEqualStrategyNumber, F_OIDEQ,
					ObjectIdGetDatum(newOid));

		/* see notes above about using SnapshotDirty */
		scan = systable_beginscan(relation, indexId, true,
								  &SnapshotDirty, 1, &key);

		collides = HeapTupleIsValid(systable_getnext(scan));

		systable_endscan(scan);
	} while (collides);

	return newOid;
}

/*
 * GetNewSequenceRelationOid
 *		Get a sequence relation Oid and verify it is valid against
 *		the pg_class relation by doing an index lookup. The caller
 *		should have a suitable lock on pg_class.
 */
Oid
GetNewSequenceRelationOid(Relation relation)
{
	Oid			newOid;
	Oid			oidIndex;
	Relation	indexrel;
	SnapshotData SnapshotDirty;
	IndexScanDesc scan;
	ScanKeyData key;
	bool		collides;
	RelFileNode rnode;
	char	   *rpath;
	int			fd;

	/* This should match RelationInitPhysicalAddr */
	rnode.spcNode = relation->rd_rel->reltablespace ? relation->rd_rel->reltablespace : MyDatabaseTableSpace;
	rnode.dbNode = relation->rd_rel->relisshared ? InvalidOid : MyDatabaseId;

	/* We should only be using pg_class */
	Assert(RelationGetRelid(relation) == RelationRelationId);

	/* The relcache will cache the identity of the OID index for us */
	oidIndex = RelationGetOidIndex(relation);

	/* Otherwise, use the index to find a nonconflicting OID */
	indexrel = index_open(oidIndex, AccessShareLock);

	InitDirtySnapshot(SnapshotDirty);

	/* Generate new sequence relation OIDs until we find one not in the table */
	do
	{
		CHECK_FOR_INTERRUPTS();

		newOid = GetNewSequenceRelationObjectId();

		ScanKeyInit(&key,
					(AttrNumber) 1,
					BTEqualStrategyNumber, F_OIDEQ,
					ObjectIdGetDatum(newOid));

		/* see notes above about using SnapshotDirty */
		scan = index_beginscan(relation, indexrel,
							   &SnapshotDirty, 1, 0);
		index_rescan(scan, &key, 1, NULL, 0);

		collides = HeapTupleIsValid(index_getnext(scan, ForwardScanDirection));

		index_endscan(scan);

		if (!collides)
		{
			/* Check for existing file of same name */
			rpath = relpathbackend(rnode, relation->rd_backend, MAIN_FORKNUM);
			fd = BasicOpenFile(rpath, O_RDONLY | PG_BINARY, 0);

			if (fd >= 0)
			{
				/* definite collision */
				gp_retry_close(fd);
				collides = true;
			}
			else
			{
				/*
				 * Here we have a little bit of a dilemma: if errno is something
				 * other than ENOENT, should we declare a collision and loop? In
				 * particular one might think this advisable for, say, EPERM.
				 * However there really shouldn't be any unreadable files in a
				 * tablespace directory, and if the EPERM is actually complaining
				 * that we can't read the directory itself, we'd be in an infinite
				 * loop.  In practice it seems best to go ahead regardless of the
				 * errno.  If there is a colliding file we will get an smgr
				 * failure when we attempt to create the new relation file.
				 */
				collides = false;
			}
		}

		/*
		 * Also check that the OID hasn't been pre-assigned for a different
		 * relation.
		 *
		 * We're a bit sloppy between OIDs and relfilenodes here; it would be
		 * OK to use a value that's been reserved for use as a type or
		 * relation OID here, as long as the relfilenode is free. But there's
		 * no harm in skipping over those too, so we don't bother to
		 * distinguish them.
		 */
		if (!collides && !IsOidAcceptable(newOid))
			collides = true;

	} while (collides);

	index_close(indexrel, AccessShareLock);

	return newOid;
}

static bool
GpCheckRelFileCollision(RelFileNodeBackend rnode)
{
	char	   *rpath;
	bool		collides;

	/* Check for existing file of same name */
	rpath = relpath(rnode, MAIN_FORKNUM);
	if (access(rpath, F_OK) == 0)
		collides = true;
	else
	{
		/*
		 * Here we have a little bit of a dilemma: if errno is something
		 * other than ENOENT, should we declare a collision and loop? In
		 * particular one might think this advisable for, say, EPERM.
		 * However there really shouldn't be any unreadable files in a
		 * tablespace directory, and if the EPERM is actually complaining
		 * that we can't read the directory itself, we'd be in an infinite
		 * loop.  In practice it seems best to go ahead regardless of the
		 * errno.  If there is a colliding file we will get an smgr
		 * failure when we attempt to create the new relation file.
		 */
		collides = false;
	}

	pfree(rpath);

	return collides;
}

/*
 * GetNewRelFileNode
 *		Generate a new relfilenode number that is unique within the
 *		database of the given tablespace.
 *
 * If the relfilenode will also be used as the relation's OID, pass the
 * opened pg_class catalog, and this routine will guarantee that the result
 * is also an unused OID within pg_class.  If the result is to be used only
 * as a relfilenode for an existing relation, pass NULL for pg_class.
 * (in GPDB, 'pg_class' is unused, there is a different mechanism to avoid
 * clashes, across the whole cluster.)
 *
 * As with GetNewOid, there is some theoretical risk of a race condition,
 * but it doesn't seem worth worrying about.
 *
 * Note: we don't support using this in bootstrap mode.  All relations
 * created by bootstrap have preassigned OIDs, so there's no need.
 */
Oid
GetNewRelFileNode(Oid reltablespace, Relation pg_class, char relpersistence)
{
	RelFileNodeBackend rnode;
	bool		collides = true;
	BackendId	backend;

	switch (relpersistence)
	{
		case RELPERSISTENCE_TEMP:
			backend = TempRelBackendId;
			break;
		case RELPERSISTENCE_UNLOGGED:
		case RELPERSISTENCE_PERMANENT:
			backend = InvalidBackendId;
			break;
		default:
			elog(ERROR, "invalid relpersistence: %c", relpersistence);
			return InvalidOid;	/* placate compiler */
	}

	/* This logic should match RelationInitPhysicalAddr */
	rnode.node.spcNode = reltablespace ? reltablespace : MyDatabaseTableSpace;
	rnode.node.dbNode = (rnode.node.spcNode == GLOBALTABLESPACE_OID) ? InvalidOid : MyDatabaseId;

	/*
	 * The relpath will vary based on the backend ID, so we must initialize
	 * that properly here to make sure that any collisions based on filename
	 * are properly detected.
	 */
	rnode.backend = backend;

	do
	{
		CHECK_FOR_INTERRUPTS();

		/* Generate the Relfilenode */
		rnode.node.relNode = GetNewSegRelfilenode();

		if (!IsOidAcceptable(rnode.node.relNode))
			continue;

		collides = GpCheckRelFileCollision(rnode);

		if (!collides && rnode.node.spcNode != GLOBALTABLESPACE_OID)
		{
			/*
			 * GPDB_91_MERGE_FIXME: check again for a collision with a temp
			 * table (if this is a normal relation) or a normal table (if this
			 * is a temp relation).
			 *
			 * The shared buffer manager currently assumes that relfilenodes of
			 * relations stored in shared buffers can't conflict, which is
			 * trivially true in upstream because temp tables don't use shared
			 * buffers at all. We have to make this additional check to make
			 * sure of that.
			 */
			rnode.backend = (backend == InvalidBackendId) ? TempRelBackendId
														  : InvalidBackendId;
			collides = GpCheckRelFileCollision(rnode);
		}
	} while (collides);

	elog(DEBUG1, "Calling GetNewRelFileNode returns new relfilenode = %d", rnode.node.relNode);

	return rnode.node.relNode;
}
