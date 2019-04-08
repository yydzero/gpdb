

#include "postgres.h"

#include "optimizer/planner.h"
#include "optimizer/plancat.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

/*
 * Define planner hook to support singleton table.
 *
 * To install the hook, add following lines to postgres.conf file and restart:
 *
 *      shared_preload_libraries = 'masteronly'
 */

extern void _PG_init(void);
extern void _PG_fini(void);

static planner_hook_type prev_planner_hook = NULL;
// Since PostgreSQL 9.5
// static set_rel_pathlist_hook_type prev_set_rel_pathlist_hook;
static get_relation_info_hook_type prev_get_relation_info_hook;
// Since PostgreSQL 9.6
// static create_upper_paths_hook_type prev_create_upper_paths_hook;


static PlannedStmt *
singleton_planner_hook(Query *parse, int cursorOptions, ParamListInfo boundParams)
{
	PlannedStmt *stmt;

	if (prev_planner_hook)
		stmt = (*prev_planner_hook) (parse, cursorOptions, boundParams);
	else
		stmt = standard_planner(parse, cursorOptions, boundParams);

	return stmt;
}

void _PG_init(void)
{
	prev_planner_hook = planner_hook;
	planner_hook = singleton_planner_hook;
}

void _PG_fini(void)
{
	planner_hook = prev_planner_hook;
}