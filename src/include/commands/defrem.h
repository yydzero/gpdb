/*-------------------------------------------------------------------------
 *
 * defrem.h
 *	  POSTGRES define and remove utility definitions.
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/defrem.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef DEFREM_H
#define DEFREM_H

#include "catalog/objectaddress.h"
#include "nodes/parsenodes.h"
#include "utils/array.h"
<<<<<<< HEAD

struct HTAB;  /* utils/hsearch.h */
=======
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

/* commands/dropcmds.c */
extern void RemoveObjects(DropStmt *stmt);

/* commands/indexcmds.c */
extern ObjectAddress DefineIndex(Oid relationId,
			IndexStmt *stmt,
			Oid indexRelationId,
			bool is_alter_table,
			bool check_rights,
			bool skip_build,
<<<<<<< HEAD
			bool quiet,
			bool concurrent,
			IndexStmt *stmt /* MPP */);
extern void ReindexIndex(ReindexStmt *stmt);
extern void ReindexTable(ReindexStmt *stmt);
extern void ReindexDatabase(ReindexStmt *stmt);
=======
			bool quiet);
extern Oid	ReindexIndex(RangeVar *indexRelation, int options);
extern Oid	ReindexTable(RangeVar *relation, int options);
extern void ReindexMultipleTables(const char *objectName, ReindexObjectType objectKind,
					  int options);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
extern char *makeObjectName(const char *name1, const char *name2,
			   const char *label);
extern char *ChooseRelationName(const char *name1, const char *name2,
				   const char *label, Oid namespaceid);
<<<<<<< HEAD
extern char *ChooseRelationNameWithCache(const char *name1, const char *name2,
								const char *label, Oid namespaceid, 
								struct HTAB *cache);
extern char *ChooseIndexName(const char *tabname, Oid namespaceId,
				List *colnames, List *exclusionOpNames,
				bool primary, bool isconstraint);
extern List *ChooseIndexColumnNames(List *indexElems);
=======
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
extern bool CheckIndexCompatible(Oid oldId,
					 char *accessMethodName,
					 List *attributeList,
					 List *exclusionOpNames);
extern Oid	GetDefaultOpClass(Oid type_id, Oid am_id);

/* commands/functioncmds.c */
extern ObjectAddress CreateFunction(CreateFunctionStmt *stmt, const char *queryString);
extern void RemoveFunctionById(Oid funcOid);
extern void SetFunctionReturnType(Oid funcOid, Oid newRetType);
extern void SetFunctionArgType(Oid funcOid, int argIndex, Oid newArgType);
extern ObjectAddress AlterFunction(AlterFunctionStmt *stmt);
extern ObjectAddress CreateCast(CreateCastStmt *stmt);
extern void DropCastById(Oid castOid);
<<<<<<< HEAD
extern Oid  get_cast_oid(Oid sourcetypeid, Oid targettypeid, bool missing_ok);
extern void interpret_function_parameter_list(List *parameters,
								  Oid languageOid,
								  bool is_aggregate,
								  const char *queryString,
								  oidvector **parameterTypes,
								  ArrayType **allParameterTypes,
								  ArrayType **parameterModes,
								  ArrayType **parameterNames,
								  List **parameterDefaults,
								  Oid *variadicArgType,
								  Oid *requiredResultType);
extern void AlterFunctionNamespace(List *name, List *argtypes, bool isagg,
					   const char *newschema);
extern Oid	AlterFunctionNamespace_oid(Oid procOid, Oid nspOid);
=======
extern ObjectAddress CreateTransform(CreateTransformStmt *stmt);
extern void DropTransformById(Oid transformOid);
extern void IsThereFunctionInNamespace(const char *proname, int pronargs,
						   oidvector *proargtypes, Oid nspOid);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
extern void ExecuteDoStmt(DoStmt *stmt);
extern Oid	get_cast_oid(Oid sourcetypeid, Oid targettypeid, bool missing_ok);
extern Oid	get_transform_oid(Oid type_id, Oid lang_id, bool missing_ok);
extern void interpret_function_parameter_list(List *parameters,
								  Oid languageOid,
								  bool is_aggregate,
								  const char *queryString,
								  oidvector **parameterTypes,
								  ArrayType **allParameterTypes,
								  ArrayType **parameterModes,
								  ArrayType **parameterNames,
								  List **parameterDefaults,
								  Oid *variadicArgType,
								  Oid *requiredResultType);

/* commands/operatorcmds.c */
extern ObjectAddress DefineOperator(List *names, List *parameters);
extern void RemoveOperatorById(Oid operOid);

/* commands/aggregatecmds.c */
<<<<<<< HEAD
extern void DefineAggregate(List *name, List *args, bool oldstyle,
							List *parameters, bool ordered, const char *queryString);
extern void RenameAggregate(List *name, List *args, const char *newname);
extern void AlterAggregateOwner(List *name, List *args, Oid newOwnerId);
=======
extern ObjectAddress DefineAggregate(List *name, List *args, bool oldstyle,
				List *parameters, const char *queryString);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

/* commands/opclasscmds.c */
extern ObjectAddress DefineOpClass(CreateOpClassStmt *stmt);
extern ObjectAddress DefineOpFamily(CreateOpFamilyStmt *stmt);
extern Oid	AlterOpFamily(AlterOpFamilyStmt *stmt);
extern void RemoveOpClassById(Oid opclassOid);
extern void RemoveOpFamilyById(Oid opfamilyOid);
extern void RemoveAmOpEntryById(Oid entryOid);
extern void RemoveAmProcEntryById(Oid entryOid);
extern void IsThereOpClassInNamespace(const char *opcname, Oid opcmethod,
						  Oid opcnamespace);
extern void IsThereOpFamilyInNamespace(const char *opfname, Oid opfmethod,
						   Oid opfnamespace);
extern Oid	get_am_oid(const char *amname, bool missing_ok);
extern char *get_am_name(Oid amOid);
extern Oid	get_opclass_oid(Oid amID, List *opclassname, bool missing_ok);
extern Oid	get_opfamily_oid(Oid amID, List *opfamilyname, bool missing_ok);

/* commands/tsearchcmds.c */
extern ObjectAddress DefineTSParser(List *names, List *parameters);
extern void RemoveTSParserById(Oid prsId);

extern ObjectAddress DefineTSDictionary(List *names, List *parameters);
extern void RemoveTSDictionaryById(Oid dictId);
extern ObjectAddress AlterTSDictionary(AlterTSDictionaryStmt *stmt);

extern ObjectAddress DefineTSTemplate(List *names, List *parameters);
extern void RemoveTSTemplateById(Oid tmplId);

extern ObjectAddress DefineTSConfiguration(List *names, List *parameters,
					  ObjectAddress *copied);
extern void RemoveTSConfigurationById(Oid cfgId);
extern ObjectAddress AlterTSConfiguration(AlterTSConfigurationStmt *stmt);

extern text *serialize_deflist(List *deflist);
extern List *deserialize_deflist(Datum txt);

/* commands/foreigncmds.c */
extern ObjectAddress AlterForeignServerOwner(const char *name, Oid newOwnerId);
extern void AlterForeignServerOwner_oid(Oid, Oid newOwnerId);
extern ObjectAddress AlterForeignDataWrapperOwner(const char *name, Oid newOwnerId);
extern void AlterForeignDataWrapperOwner_oid(Oid fwdId, Oid newOwnerId);
extern ObjectAddress CreateForeignDataWrapper(CreateFdwStmt *stmt);
extern ObjectAddress AlterForeignDataWrapper(AlterFdwStmt *stmt);
extern void RemoveForeignDataWrapperById(Oid fdwId);
extern ObjectAddress CreateForeignServer(CreateForeignServerStmt *stmt);
extern ObjectAddress AlterForeignServer(AlterForeignServerStmt *stmt);
extern void RemoveForeignServerById(Oid srvId);
extern ObjectAddress CreateUserMapping(CreateUserMappingStmt *stmt);
extern ObjectAddress AlterUserMapping(AlterUserMappingStmt *stmt);
extern Oid	RemoveUserMapping(DropUserMappingStmt *stmt);
extern void RemoveUserMappingById(Oid umId);
extern void CreateForeignTable(CreateForeignTableStmt *stmt, Oid relid);
extern void ImportForeignSchema(ImportForeignSchemaStmt *stmt);
extern Datum transformGenericOptions(Oid catalogId,
						Datum oldOptions,
						List *options,
						Oid fdwvalidator);

/* support routines in commands/define.c */

extern char *defGetString(DefElem *def);
extern double defGetNumeric(DefElem *def);
extern bool defGetBoolean(DefElem *def);
extern int32 defGetInt32(DefElem *def);
extern int64 defGetInt64(DefElem *def);
extern List *defGetQualifiedName(DefElem *def);
extern TypeName *defGetTypeName(DefElem *def);
extern int	defGetTypeLength(DefElem *def);
extern DefElem *defWithOids(bool value);

#endif   /* DEFREM_H */
