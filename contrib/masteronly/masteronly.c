

/*
 * masteronly extension provides master only table, which store data only on
 * master. Segment has no user data.
 *
 * Major work:
 *
 * 1) Create a table which store all the data on master.
 * 2) Run query with master only table
 *
 * Throw error if one query contains both master only table and other table.
 *
 * Usage:
 *
 * CREATE TABLE t1 (id int, name text);
 * SELECT masteronly.set_master_only_table('public.t1');
 * INSERT INTO t1 VALUES (1, 'abc'), (2, 'def');
 * SELECT * FROM t1 where id = 1;
 *
 *
 *
 */


#include "postgres.h"

#include "utils/builtins.h"
#include "utils/formatting.h"
#include "utils/lsyscache.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

PG_FUNCTION_INFO_V1(set_master_only_table);

Datum
set_master_only_table(PG_FUNCTION_ARGS)
{
	Oid     table_relid = PG_GETARG_OID(0);
	char   *result;

	result = get_rel_name(table_relid);

	PG_RETURN_NAME(result);
}