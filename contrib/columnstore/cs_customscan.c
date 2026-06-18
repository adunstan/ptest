/*-------------------------------------------------------------------------
 *
 * cs_customscan.c
 *	  CustomScan integration for columnstore tables.
 *
 * This file contains every plan-time path the columnstore extension
 * registers, and the executor methods that run them:
 *
 *   - ColumnstoreScan.  Replaces the planner-supplied SeqScan paths
 *     (serial and partial) for columnstore base relations, opens a
 *     TableScanDesc, and drives ExecScan against a virtual result
 *     slot containing only the projected columns.  Side-benefit: the
 *     projected target list also shrinks the MinimalTuple HashAgg
 *     would otherwise spill, which carries a null bitmap sized for
 *     every column of the (potentially very wide) table.
 *
 *   - ColumnstoreAggregate.  Recognises COUNT / SUM / AVG / MIN / MAX
 *     (with FILTER, GROUP BY, HAVING, WHERE) above a columnstore
 *     base relation and emits an upper-rel CustomScan that
 *     accumulates per-aggregate state directly inside the scan,
 *     finalising once per group.  Two stages dispatch: a sequential
 *     path on UPPERREL_GROUP_AGG for ungrouped queries, and a
 *     parallel-aware partial path on UPPERREL_PARTIAL_GROUP_AGG that
 *     each worker drives independently for the upstream Finalize
 *     Aggregate to combine.
 *
 *   - ColumnstoreLateMat.  At UPPERREL_ORDERED, defers reading
 *     non-qual / non-sort-key columns until after Sort + Limit has
 *     selected the surviving rows; the bottom probe scan emits
 *     (sort_key columns, packed_tid) and a top CustomScan refetches
 *     the payload columns via table_tuple_fetch_row_version.
 *
 * Path-level scaffolding (cs_set_rel_pathlist_hook,
 * cs_create_upper_paths_hook) and plan-tree walkers that derive
 * projection columns and scan keys from the surrounding plan also
 * live here, since they have no useful home in the table-AM file.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * contrib/columnstore/cs_customscan.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cs_internal.h"

#include "access/nbtree.h"
#include "access/parallel.h"
#include "access/relation.h"
#include "access/reloptions.h"
#include "access/relscan.h"
#include "access/tableam.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "commands/explain_format.h"
#include "executor/executor.h"
#include "executor/nodeSeqscan.h"
#include "lib/stringinfo.h"
#include "nodes/extensible.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/pathnodes.h"
#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/planner.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/tlist.h"
#include "parser/parse_oper.h"
#include "storage/shm_toc.h"
#include "utils/datum.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/selfuncs.h"
#include "utils/syscache.h"

/*
 * State carried between BeginCustomScan and EndCustomScan.
 */
typedef struct CSCustomScanState
{
	CustomScanState css;		/* must be first */
	TableScanDesc scandesc;
	bool		bloom_applied;	/* pushed-down bloom filters consumed yet? */
	bool		instr_flushed;	/* parallel instrumentation folded already? */
} CSCustomScanState;

/* Forward declarations */
static Plan *cs_find_plan_parent(Plan *root, Plan *target, bool *is_inner);
static Bitmapset *cs_extract_scan_needed_cols(Plan *root_plan,
											  Scan *scan_plan);
static int	cs_extract_scan_qual_keys(List *quals, Index scanrelid,
									  ScanKeyData **keys_out);
static void cs_am_oid_invalidate(Datum arg, int cacheid, uint32 hashvalue);
static Oid	cs_get_am_oid(void);
static bool rel_is_columnstore(Oid relid);
static bool cs_upper_input_is_columnstore_rel(PlannerInfo *root,
											  RelOptInfo *input_rel,
											  RangeTblEntry **rte_out);
static List *cs_try_build_sort_pathkeys(PlannerInfo *root, RelOptInfo *rel,
										RangeTblEntry *rte);
static void cs_set_rel_pathlist_hook(PlannerInfo *root, RelOptInfo *rel,
									 Index rti, RangeTblEntry *rte);
static void cs_open_scan(CSCustomScanState *state, EState *estate,
						 ParallelTableScanDesc pscan);
static Plan *cs_path_to_plan(PlannerInfo *root, RelOptInfo *rel,
							 struct CustomPath *best_path, List *tlist,
							 List *clauses, List *custom_plans);
static Node *cs_create_custom_scan_state(CustomScan *cscan);
static void cs_begin_custom_scan(CustomScanState *node, EState *estate,
								 int eflags);
static TupleTableSlot *cs_exec_custom_scan(CustomScanState *node);
static void cs_end_custom_scan(CustomScanState *node);
static void cs_rescan_custom_scan(CustomScanState *node);
static void cs_shutdown_custom_scan(CustomScanState *node);
static void cs_explain_custom_scan(CustomScanState *node, List *ancestors,
								   ExplainState *es);
static Size cs_estimate_dsm_custom_scan(CustomScanState *node,
										ParallelContext *pcxt);
static void cs_initialize_dsm_custom_scan(CustomScanState *node,
										  ParallelContext *pcxt,
										  void *coordinate);
static void cs_reinitialize_dsm_custom_scan(CustomScanState *node,
											ParallelContext *pcxt,
											void *coordinate);
static void cs_initialize_worker_custom_scan(CustomScanState *node,
											 shm_toc *toc, void *coordinate);

static TupleTableSlot *cs_custom_scan_next(CustomScanState *node);
static bool cs_custom_scan_recheck(CustomScanState *node,
								   TupleTableSlot *slot);
static void cs_apply_pushed_bloom_filters(CSCustomScanState *state);

/* Saved upstream hook chain pointers */
static set_rel_pathlist_hook_type prev_set_rel_pathlist_hook = NULL;
static create_upper_paths_hook_type prev_create_upper_paths_hook = NULL;

static const struct CustomPathMethods cs_path_methods = {
	.CustomName = "ColumnstoreScan",
	.PlanCustomPath = cs_path_to_plan,
};

static const struct CustomScanMethods cs_scan_methods = {
	.CustomName = "ColumnstoreScan",
	.CreateCustomScanState = cs_create_custom_scan_state,
};

static const struct CustomExecMethods cs_exec_methods = {
	.CustomName = "ColumnstoreScan",
	.BeginCustomScan = cs_begin_custom_scan,
	.ExecCustomScan = cs_exec_custom_scan,
	.EndCustomScan = cs_end_custom_scan,
	.ReScanCustomScan = cs_rescan_custom_scan,
	.EstimateDSMCustomScan = cs_estimate_dsm_custom_scan,
	.InitializeDSMCustomScan = cs_initialize_dsm_custom_scan,
	.ReInitializeDSMCustomScan = cs_reinitialize_dsm_custom_scan,
	.InitializeWorkerCustomScan = cs_initialize_worker_custom_scan,
	.ShutdownCustomScan = cs_shutdown_custom_scan,
	.ExplainCustomScan = cs_explain_custom_scan,
};

/* Forward decls for the aggregate-pushdown CustomScan ("ColumnstoreAggregate"). */
static Plan *cs_agg_path_to_plan(PlannerInfo *root, RelOptInfo *rel,
								 struct CustomPath *best_path, List *tlist,
								 List *clauses, List *custom_plans);
static bool cs_contains_param_walker(Node *node, void *context);
static bool cs_contains_param(Node *node);
static Node *cs_agg_create_state(CustomScan *cscan);
static void cs_agg_begin(CustomScanState *node, EState *estate, int eflags);
static TupleTableSlot *cs_agg_exec(CustomScanState *node);
static void cs_agg_end(CustomScanState *node);
static void cs_agg_open_scan(CustomScanState *node, EState *estate,
							 ParallelTableScanDesc pscan);
static Size cs_agg_estimate_dsm(CustomScanState *node, ParallelContext *pcxt);
static void cs_agg_initialize_dsm(CustomScanState *node,
								  ParallelContext *pcxt, void *coordinate);
static void cs_agg_reinitialize_dsm(CustomScanState *node,
									ParallelContext *pcxt, void *coordinate);
static void cs_agg_initialize_worker(CustomScanState *node, shm_toc *toc,
									 void *coordinate);
static void cs_agg_rescan(CustomScanState *node);
static void cs_agg_explain(CustomScanState *node, List *ancestors,
						   ExplainState *es);
static void cs_create_upper_paths_hook(PlannerInfo *root,
									   UpperRelationKind stage,
									   RelOptInfo *input_rel,
									   RelOptInfo *output_rel,
									   void *extra);

/* Forward decls for the late-mat CustomScan ("ColumnstoreLateMat"). */
static Plan *cs_lm_path_to_plan(PlannerInfo *root, RelOptInfo *rel,
								struct CustomPath *best_path, List *tlist,
								List *clauses, List *custom_plans);
static Node *cs_lm_create_state(CustomScan *cscan);
static void cs_lm_begin(CustomScanState *node, EState *estate, int eflags);
static TupleTableSlot *cs_lm_exec(CustomScanState *node);
static void cs_lm_end(CustomScanState *node);
static void cs_lm_rescan(CustomScanState *node);
static void cs_lm_explain(CustomScanState *node, List *ancestors,
						  ExplainState *es);
static void cs_try_add_latemat_path(PlannerInfo *root, RelOptInfo *input_rel,
									RelOptInfo *output_rel);


/*
 * Aggregate-pushdown internal forward decls live just above the agg
 * implementation block (after the CSAggOne typedef) so they can reference
 * the types those callees use.  See the CSAggOne forward decls below.
 */

static const struct CustomPathMethods cs_agg_path_methods = {
	.CustomName = "ColumnstoreAggregate",
	.PlanCustomPath = cs_agg_path_to_plan,
};

static const struct CustomScanMethods cs_agg_scan_methods = {
	.CustomName = "ColumnstoreAggregate",
	.CreateCustomScanState = cs_agg_create_state,
};

static const struct CustomExecMethods cs_agg_exec_methods = {
	.CustomName = "ColumnstoreAggregate",
	.BeginCustomScan = cs_agg_begin,
	.ExecCustomScan = cs_agg_exec,
	.EndCustomScan = cs_agg_end,
	.ReScanCustomScan = cs_agg_rescan,
	.EstimateDSMCustomScan = cs_agg_estimate_dsm,
	.InitializeDSMCustomScan = cs_agg_initialize_dsm,
	.ReInitializeDSMCustomScan = cs_agg_reinitialize_dsm,
	.InitializeWorkerCustomScan = cs_agg_initialize_worker,
	.ExplainCustomScan = cs_agg_explain,
};

static const struct CustomPathMethods cs_lm_path_methods = {
	.CustomName = "ColumnstoreLateMat",
	.PlanCustomPath = cs_lm_path_to_plan,
};

static const struct CustomScanMethods cs_lm_scan_methods = {
	.CustomName = "ColumnstoreLateMat",
	.CreateCustomScanState = cs_lm_create_state,
};

static const struct CustomExecMethods cs_lm_exec_methods = {
	.CustomName = "ColumnstoreLateMat",
	.BeginCustomScan = cs_lm_begin,
	.ExecCustomScan = cs_lm_exec,
	.EndCustomScan = cs_lm_end,
	.ReScanCustomScan = cs_lm_rescan,
	.ExplainCustomScan = cs_lm_explain,
};

/*
 * cs_register_custom_scan_methods
 *
 * Idempotent registration of the CustomScan integration.  Called from
 * the extension's _PG_init.  When the extension is listed in
 * shared_preload_libraries (the recommended deployment for any setup
 * that runs parallel queries against columnstore tables), _PG_init
 * runs in the postmaster at server start, and every forked backend
 * -- regular client, parallel worker, autovacuum -- inherits the
 * registered methods table.  Without shared_preload_libraries,
 * _PG_init runs only when CREATE EXTENSION executes or when an SQL
 * function from the extension is first invoked, which can be after
 * parallel workers have already attempted to deserialise plans that
 * reference our CustomScan node tags.
 */
void
cs_register_custom_scan_methods(void)
{
	static bool registered = false;

	if (registered)
		return;

	RegisterCustomScanMethods(&cs_scan_methods);
	RegisterCustomScanMethods(&cs_agg_scan_methods);
	RegisterCustomScanMethods(&cs_lm_scan_methods);

	prev_set_rel_pathlist_hook = set_rel_pathlist_hook;
	set_rel_pathlist_hook = cs_set_rel_pathlist_hook;

	prev_create_upper_paths_hook = create_upper_paths_hook;
	create_upper_paths_hook = cs_create_upper_paths_hook;

	registered = true;
}

/* ------------------------------------------------------------------------
 * Path generation
 * ------------------------------------------------------------------------
 */

/*
 * Cached OID of the columnstore access method.  Looked up lazily on first
 * use because the AM does not exist until CREATE EXTENSION columnstore
 * has run, and re-resolved when pg_am changes (DROP EXTENSION /
 * CREATE EXTENSION cycles).
 */
static Oid	cs_am_oid_cache = InvalidOid;
static bool cs_am_oid_callback_registered = false;

static void
cs_am_oid_invalidate(Datum arg, int cacheid, uint32 hashvalue)
{
	cs_am_oid_cache = InvalidOid;
}

static Oid
cs_get_am_oid(void)
{
	if (!cs_am_oid_callback_registered)
	{
		CacheRegisterSyscacheCallback(AMOID, cs_am_oid_invalidate, (Datum) 0);
		cs_am_oid_callback_registered = true;
	}

	if (!OidIsValid(cs_am_oid_cache))
		cs_am_oid_cache = get_am_oid("columnstore", true);

	return cs_am_oid_cache;
}

/*
 * Test whether a relation oid refers to a table using the columnstore AM.
 * Uses the relation cache so the lookup is cheap; the planner already
 * holds these entries.
 */
static bool
rel_is_columnstore(Oid relid)
{
	HeapTuple	tuple;
	Form_pg_class pg_class;
	Oid			am_oid;
	bool		result;

	if (!OidIsValid(relid))
		return false;

	am_oid = cs_get_am_oid();
	if (!OidIsValid(am_oid))
		return false;

	tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(tuple))
		return false;

	pg_class = (Form_pg_class) GETSTRUCT(tuple);

	/*
	 * The relkind check matters for partitioned tables: they may carry the
	 * columnstore relam (inherited by future partitions) but have no storage
	 * of their own, so no columnstore plan may ever scan one.  The planner
	 * usually keeps them away from us as append parents, but FROM ONLY
	 * produces a childless base rel that would otherwise pass.
	 */
	result = (pg_class->relam == am_oid &&
			  RELKIND_HAS_STORAGE(pg_class->relkind));
	ReleaseSysCache(tuple);
	return result;
}

/*
 * Common gate for the upper-path hooks (aggregate and ORDER BY/LIMIT
 * pushdown): the input must be a single plain columnstore base relation.
 *
 * An inheritance or partitioned parent is a base rel too, but its rows
 * live in the append children; pushing the upper step onto just the
 * parent would silently drop them.  Sampled scans must keep the stock
 * TABLESAMPLE machinery.  On success, *rte_out is set to the relation's
 * RTE.
 */
static bool
cs_upper_input_is_columnstore_rel(PlannerInfo *root, RelOptInfo *input_rel,
								  RangeTblEntry **rte_out)
{
	RangeTblEntry *rte;

	if (input_rel->reloptkind != RELOPT_BASEREL)
		return false;
	rte = root->simple_rte_array[input_rel->relid];
	if (rte == NULL || rte->rtekind != RTE_RELATION)
		return false;
	if (rte->inh)
		return false;
	if (rte->tablesample != NULL)
		return false;
	if (!rel_is_columnstore(rte->relid))
		return false;

	*rte_out = rte;
	return true;
}

/*
 * Build pathkeys describing the columnstore relation's natural row order
 * when its sort_key reloption guarantees globally sorted output.
 *
 * Returns NIL when the relation is not provably sorted across row groups.
 * The check is conservative: we only declare pathkeys when every row
 * group's zone-map [min, max] for the sort_key column is non-overlapping
 * and ascending across the row group directory.  This is the case when
 * data was inserted in sort order (the natural workload for sort_key,
 * e.g. time-series append-only), but not when random inserts produced
 * row groups with overlapping ranges.  Without the non-overlap property
 * the scan output is sorted only within each row group, not globally,
 * and declaring pathkeys would be a correctness bug.
 *
 * Limitations of this first cut: only the leading column of a
 * comma-separated sort_key is considered; only ASC NULLS LAST ordering
 * is recognised (matching cs_compact's sort).
 */
static List *
cs_try_build_sort_pathkeys(PlannerInfo *root, RelOptInfo *rel,
						   RangeTblEntry *rte)
{
	Relation	relation;
	const char *sort_key_str;
	char	   *first_col;
	char	   *comma;
	int			first_col_len;
	AttrNumber	sort_attno;
	Oid			sort_type;
	Oid			opclass;
	Oid			opfamily;
	Oid			lt_op;
	CSMetaPageData meta;
	BlockNumber *rg_blocks = NULL;
	bool		sorted = true;
	bool		any_seen = false;
	Datum		prev_max = (Datum) 0;
	bool		have_prev = false;
	FmgrInfo	cmp_finfo;
	Oid			cmp_proc;
	Oid			collation;
	List	   *pathkeys = NIL;
	int			natts;
	uint32		i;

	relation = relation_open(rte->relid, NoLock);

	sort_key_str = cs_get_sort_key(relation);
	if (sort_key_str == NULL || sort_key_str[0] == '\0')
	{
		relation_close(relation, NoLock);
		return NIL;
	}

	/* Take only the leading column of a comma-separated sort_key */
	first_col = pstrdup(sort_key_str);
	while (*first_col == ' ')
		first_col++;
	comma = strchr(first_col, ',');
	if (comma)
		*comma = '\0';
	first_col_len = strlen(first_col);
	while (first_col_len > 0 && first_col[first_col_len - 1] == ' ')
		first_col[--first_col_len] = '\0';

	sort_attno = get_attnum(rte->relid, first_col);
	if (sort_attno == InvalidAttrNumber)
	{
		relation_close(relation, NoLock);
		return NIL;
	}

	sort_type = TupleDescAttr(RelationGetDescr(relation),
							  sort_attno - 1)->atttypid;
	collation = TupleDescAttr(RelationGetDescr(relation),
							  sort_attno - 1)->attcollation;

	/* Default btree opfamily for the sort column's type */
	opclass = GetDefaultOpClass(sort_type, BTREE_AM_OID);
	if (!OidIsValid(opclass))
	{
		relation_close(relation, NoLock);
		return NIL;
	}
	opfamily = get_opclass_family(opclass);

	lt_op = get_opfamily_member(opfamily, sort_type, sort_type,
								BTLessStrategyNumber);
	cmp_proc = get_opfamily_proc(opfamily, sort_type, sort_type,
								 BTORDER_PROC);
	if (!OidIsValid(lt_op) || !OidIsValid(cmp_proc))
	{
		relation_close(relation, NoLock);
		return NIL;
	}
	fmgr_info(cmp_proc, &cmp_finfo);

	/*
	 * Walk the row group directory, reading each row group's zone map for the
	 * sort column.  Bail out as soon as we see a row group with no zone map
	 * (e.g. all NULLs) or a min that does not exceed the previous row group's
	 * max.
	 */
	natts = RelationGetDescr(relation)->natts;

	if (RelationGetNumberOfBlocks(relation) > 0)
	{
		cs_read_metapage(relation, &meta);

		/*
		 * Any delta pages defeat the ordering claim: the scan emits delta
		 * rows first, in insertion order, before the (ordered) row groups.
		 * The insert paths send a relcache invalidation when the delta
		 * transitions from empty to non-empty, so cached plans that relied on
		 * an empty delta are replanned rather than silently misordered.
		 */
		if (meta.cs_delta_nblocks > 0)
		{
			relation_close(relation, NoLock);
			return NIL;
		}
		if (meta.cs_nrowgroups > 0)
			rg_blocks = cs_read_rgdir(relation, &meta);
	}
	if (rg_blocks == NULL)
	{
		relation_close(relation, NoLock);
		return NIL;
	}

	{
		Size		rg_size = CSRowGroupDescSize(natts);
		CSRowGroupDesc *rg_desc = palloc(rg_size);

		for (i = 0; i < meta.cs_nrowgroups && sorted; i++)
		{
			CSZoneMap  *zm;

			if (rg_blocks[i] == InvalidBlockNumber)
				continue;

			cs_read_rowgroup_catalog(relation, rg_blocks[i], rg_desc, natts);

			if (rg_desc->rg_num_rows == 0)
				continue;

			zm = &CSRowGroupGetZoneMaps(rg_desc)[sort_attno - 1];
			if (!zm->zm_has_minmax || zm->zm_mode != CS_ZM_BYVAL)
			{
				/*
				 * No zone map (all NULLs) or non-by-value mode (text /
				 * sort-key encoded).  We do not currently extend the
				 * non-overlap check to byref types, so bail.
				 */
				sorted = false;
				break;
			}

			if (have_prev)
			{
				int			cmp;

				/*
				 * Need this row group's min strictly greater than the
				 * previous row group's max.  Equality is not enough -- row
				 * groups with equal boundary values violate strict
				 * non-overlap, which the planner relies on to skip an upper
				 * sort.
				 */
				cmp = DatumGetInt32(FunctionCall2Coll(&cmp_finfo, collation,
													  prev_max, zm->zm_min));
				if (cmp >= 0)
					sorted = false;
			}

			prev_max = zm->zm_max;
			have_prev = true;
			any_seen = true;
		}
		pfree(rg_desc);
	}

	if (sorted && any_seen)
	{
		Var		   *var;

		var = makeVar(rel->relid, sort_attno, sort_type, -1, collation, 0);
		pathkeys = build_expression_pathkey(root, (Expr *) var,
											lt_op, rel->relids, false);
	}

	relation_close(relation, NoLock);
	return pathkeys;
}

/*
 * Hook entry point.  For each base relation that uses the columnstore AM,
 * REPLACE the planner-supplied SeqScan paths (both serial and parallel)
 * with a single CustomScan path.  We have to replace rather than just
 * add-alongside because there is no AM-level cost-factor callback that
 * could scale the standard scan-path costs, so the planner can't be
 * relied on to pick our CustomScan over the built-in SeqScan paths.
 *
 * The CustomPath inherits the standard rel->reltarget so the
 * planner-supplied targetlist passed to cs_path_to_plan reflects only
 * the columns referenced upstream.  Costs are inherited from the
 * cheapest standard path; the per-tuple portion (total - startup) is
 * scaled by the AM's cpu_tuple_cost_factor (0.5 today) to reflect the
 * cheaper per-tuple work of a column-projected scan.  Other factors
 * cs_relation_cost_factors returns -- page-cost scalars, random-I/O
 * page count, index-fetch cost -- have no consumer in this hook and
 * are ignored.  When the relation has a sort_key reloption and every
 * row group's zone map is non-overlapping ascending, the path also
 * advertises pathkeys so upper plan nodes can skip an explicit Sort.
 */
static void
cs_set_rel_pathlist_hook(PlannerInfo *root, RelOptInfo *rel,
						 Index rti, RangeTblEntry *rte)
{
	CustomPath *cpath;
	Path	   *baseline = NULL;
	List	   *new_pathlist = NIL;
	List	   *new_partial_pathlist = NIL;
	ListCell   *lc;
	double		seq_factor = 1.0;
	double		rand_factor = 1.0;
	double		cpu_factor = 1.0;
	BlockNumber rand_io_pages = 0;
	bool		disk_par = false;
	double		index_fetch_cost = 0.0;
	Relation	relation;
	Relation	rel_open;
	int			num_workers;

	if (prev_set_rel_pathlist_hook)
		prev_set_rel_pathlist_hook(root, rel, rti, rte);

	if (rte->rtekind != RTE_RELATION)
		return;

	/*
	 * Handle plain base rels and partition/inheritance children alike.
	 * Skipping children would leave partitions on the planner's stock SeqScan
	 * paths -- losing projection and qual pushdown, and, worse, letting
	 * ExecSupportsBackwardScan() approve a SCROLL cursor over a path whose
	 * executor cannot actually scan backwards.
	 */
	if (rel->reloptkind != RELOPT_BASEREL &&
		rel->reloptkind != RELOPT_OTHER_MEMBER_REL)
		return;
	if (!rel_is_columnstore(rte->relid))
		return;

	/*
	 * Walk rel->pathlist: find the cheapest SeqScan path to use as our
	 * row-estimate baseline, and rebuild the list without the SeqScan paths.
	 * Keep IndexScan / BitmapHeap / TidScan paths as-is.
	 */
	foreach(lc, rel->pathlist)
	{
		Path	   *p = (Path *) lfirst(lc);

		if (p->pathtype == T_SeqScan)
		{
			if (p->param_info == NULL &&
				(baseline == NULL || p->total_cost < baseline->total_cost))
				baseline = p;
			continue;
		}
		new_pathlist = lappend(new_pathlist, p);
	}
	if (baseline == NULL)
		return;

	rel->pathlist = new_pathlist;

	/* Same for partial_pathlist (parallel SeqScan candidates). */
	foreach(lc, rel->partial_pathlist)
	{
		Path	   *p = (Path *) lfirst(lc);

		if (p->pathtype != T_SeqScan)
			new_partial_pathlist = lappend(new_partial_pathlist, p);
	}
	rel->partial_pathlist = new_partial_pathlist;

	/* Apply the AM's cost-factor helper on top of baseline. */
	relation = relation_open(rte->relid, NoLock);
	cs_relation_cost_factors(relation,
							 &seq_factor, &rand_factor, &cpu_factor,
							 &rand_io_pages, &disk_par,
							 &index_fetch_cost);
	relation_close(relation, NoLock);

	cpath = makeNode(CustomPath);
	cpath->path.pathtype = T_CustomScan;
	cpath->path.parent = rel;
	cpath->path.pathtarget = rel->reltarget;
	cpath->path.param_info = NULL;
	cpath->path.parallel_aware = false;
	cpath->path.parallel_safe = baseline->parallel_safe;
	cpath->path.parallel_workers = 0;
	cpath->path.rows = baseline->rows;
	cpath->path.startup_cost = baseline->startup_cost;
	cpath->path.total_cost = baseline->startup_cost +
		(baseline->total_cost - baseline->startup_cost) * cpu_factor;
	cpath->path.pathkeys = cs_try_build_sort_pathkeys(root, rel, rte);

	cpath->custom_paths = NIL;
	cpath->custom_private = NIL;
	cpath->methods = &cs_path_methods;

	/*
	 * Advertise that this scan can consume a hash-join bloom filter pushed
	 * down to it.  The planner (find_bloom_filter_recipient) only targets a
	 * CustomScan that sets this, and the executor applies the filter inside
	 * the scan loop -- at row-group / dictionary granularity -- via
	 * cs_apply_pushed_bloom_filters().  Left off the partial (parallel) path
	 * below, since filter pushdown is serial-only for now.
	 */
	cpath->flags = CUSTOMPATH_SUPPORT_BLOOM_FILTERS;

	add_path(rel, &cpath->path);

	/*
	 * Also generate a partial (parallel-aware) CustomPath when the relation
	 * is large enough to benefit and the AM allows parallelism.  The planner
	 * will wrap this in a Gather above any aggregate or join that sits over
	 * the scan; without it we'd lose every parallel-scan win.
	 */
	rel_open = relation_open(rte->relid, NoLock);
	num_workers = cs_compute_parallel_workers(rel_open);
	relation_close(rel_open, NoLock);

	if (num_workers > 0 && baseline->parallel_safe)
	{
		CustomPath *ppath = makeNode(CustomPath);
		double		divisor = (double) num_workers;

		ppath->path.pathtype = T_CustomScan;
		ppath->path.parent = rel;
		ppath->path.pathtarget = rel->reltarget;
		ppath->path.param_info = NULL;
		ppath->path.parallel_aware = true;
		ppath->path.parallel_safe = true;
		ppath->path.parallel_workers = num_workers;
		ppath->path.rows = clamp_row_est(baseline->rows / divisor);
		ppath->path.startup_cost = baseline->startup_cost;
		ppath->path.total_cost = baseline->startup_cost +
			((baseline->total_cost - baseline->startup_cost) * cpu_factor)
			/ divisor;
		ppath->path.pathkeys = NIL;

		ppath->custom_paths = NIL;
		ppath->custom_private = NIL;
		ppath->methods = &cs_path_methods;
		ppath->flags = 0;

		add_partial_path(rel, &ppath->path);
	}
}

/*
 * Convert a CustomPath into a CustomScan plan node.  The planner has
 * already projected the tlist down to upstream-referenced columns
 * (use_physical_tlist is bypassed for CustomPath, see createplan.c), so
 * we simply install it as the scan's output and let the standard
 * executor projection machinery deliver a narrow virtual slot upstream.
 */
static Plan *
cs_path_to_plan(PlannerInfo *root, RelOptInfo *rel,
				struct CustomPath *best_path, List *tlist,
				List *clauses, List *custom_plans)
{
	CustomScan *cscan = makeNode(CustomScan);
	List	   *quals;

	quals = extract_actual_clauses(clauses, false);

	cscan->scan.plan.targetlist = tlist;
	cscan->scan.plan.qual = quals;
	cscan->scan.plan.lefttree = NULL;
	cscan->scan.plan.righttree = NULL;
	cscan->scan.plan.parallel_aware = best_path->path.parallel_aware;
	cscan->scan.plan.parallel_safe = best_path->path.parallel_safe;
	cscan->scan.scanrelid = rel->relid;

	cscan->flags = best_path->flags;
	cscan->custom_plans = NIL;
	cscan->custom_exprs = NIL;
	cscan->custom_private = NIL;
	cscan->custom_scan_tlist = NIL;
	cscan->custom_relids = bms_make_singleton(rel->relid);
	cscan->methods = &cs_scan_methods;

	return (Plan *) cscan;
}

/* ------------------------------------------------------------------------
 * Execution callbacks
 * ------------------------------------------------------------------------
 */

static Node *
cs_create_custom_scan_state(CustomScan *cscan)
{
	CSCustomScanState *state = palloc0(sizeof(CSCustomScanState));

	NodeSetTag(state, T_CustomScanState);
	state->css.methods = &cs_exec_methods;
	state->css.slotOps = &TTSOpsColumnStore;
	state->scandesc = NULL;
	return (Node *) state;
}

/*
 * Helper: open the AM-level TableScanDesc and forward both projection and
 * scan-key information.  Used both by serial cs_begin_custom_scan and by
 * the parallel-worker initializer.  pscan==NULL means a non-parallel scan.
 */
static void
cs_open_scan(CSCustomScanState *state, EState *estate,
			 ParallelTableScanDesc pscan)
{
	Relation	rel = state->css.ss.ss_currentRelation;
	CustomScan *cscan = (CustomScan *) state->css.ss.ps.plan;
	Bitmapset  *needed_cols;
	ScanKeyData *qual_keys;
	int			nqual_keys;

	/*
	 * No user flags: table_beginscan() and table_beginscan_parallel() both
	 * supply the seqscan-style internal flags (SO_TYPE_SEQSCAN and the
	 * SO_ALLOW_* set) themselves.
	 */
	if (pscan != NULL)
		state->scandesc = table_beginscan_parallel(rel, pscan, SO_NONE);
	else
		state->scandesc = table_beginscan(rel, estate->es_snapshot,
										  0, NULL, SO_NONE);

	/*
	 * Publish the scan in the embedded ScanState so generic code paths that
	 * look up a base-rel ScanState's TableScanDesc -- such as the hash-join
	 * bloom-filter pushdown machinery -- can find it.
	 */
	state->css.ss.ss_currentScanDesc = state->scandesc;

	needed_cols = cs_extract_scan_needed_cols(estate->es_plannedstmt->planTree,
											  &cscan->scan);
	if (needed_cols != NULL)
	{
		cs_scan_set_projection(state->scandesc, needed_cols);
		bms_free(needed_cols);
	}

	nqual_keys = cs_extract_scan_qual_keys(cscan->scan.plan.qual,
										   cscan->scan.scanrelid,
										   &qual_keys);
	if (nqual_keys > 0)
	{
		cs_scan_set_qual_keys(state->scandesc, nqual_keys, qual_keys);
		pfree(qual_keys);
	}
}

static void
cs_begin_custom_scan(CustomScanState *node, EState *estate, int eflags)
{
	CSCustomScanState *state = (CSCustomScanState *) node;

	Assert(node->ss.ss_currentRelation != NULL);

	/*
	 * ExecInitCustomScan opened the relation, set up ss_ScanTupleSlot using
	 * TTSOpsColumnStore, and assigned ProjInfo because the plan targetlist
	 * differs from the scan tupledesc (use_physical_tlist is bypassed for
	 * CustomPath).  ps_ResultTupleSlot is a TTSOpsVirtual slot with the
	 * projected tupledesc; that is the slot that flows upstream from
	 * ExecCustomScan -> ExecScan.
	 *
	 * For non-parallel scans, open the underlying TableScanDesc here.  For
	 * parallel scans, the leader's InitializeDSMCustomScan and each worker's
	 * InitializeWorkerCustomScan open their own scan over a shared
	 * ParallelTableScanDesc; we leave state->scandesc NULL here and let the
	 * DSM callbacks fill it in.
	 */
	state->scandesc = NULL;

	/* plain EXPLAIN never reads the table; don't open the scan */
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	if (node->ss.ps.state->es_use_parallel_mode &&
		node->ss.ps.plan->parallel_aware)
	{
		/* DSM callbacks will open the scan */
		return;
	}

	cs_open_scan(state, estate, NULL);
}

static Size
cs_estimate_dsm_custom_scan(CustomScanState *node, ParallelContext *pcxt)
{
	Relation	rel = node->ss.ss_currentRelation;
	EState	   *estate = node->ss.ps.state;

	/*
	 * Go through the table_parallelscan_* wrappers, not the bare AM callback:
	 * they account for and serialize the scan snapshot into the shared
	 * descriptor, which table_beginscan_parallel() in each worker restores.
	 * Calling the AM callback directly leaves phs_snapshot_off zero, and the
	 * workers would "restore" the descriptor header itself as their snapshot.
	 */
	return table_parallelscan_estimate(rel, estate->es_snapshot);
}

static void
cs_initialize_dsm_custom_scan(CustomScanState *node, ParallelContext *pcxt,
							  void *coordinate)
{
	CSCustomScanState *state = (CSCustomScanState *) node;
	Relation	rel = node->ss.ss_currentRelation;
	ParallelTableScanDesc pscan = (ParallelTableScanDesc) coordinate;
	EState	   *estate = node->ss.ps.state;

	/* see cs_estimate_dsm_custom_scan for why the wrapper is required */
	table_parallelscan_initialize(rel, pscan, estate->es_snapshot);

	/* defensive: never leak a serial scan opened before the DSM phase */
	if (state->scandesc != NULL)
		cs_scan_end(state->scandesc);

	cs_open_scan(state, estate, pscan);
}

static void
cs_reinitialize_dsm_custom_scan(CustomScanState *node, ParallelContext *pcxt,
								void *coordinate)
{
	Relation	rel = node->ss.ss_currentRelation;
	ParallelTableScanDesc pscan = (ParallelTableScanDesc) coordinate;

	table_parallelscan_reinitialize(rel, pscan);
}

static void
cs_initialize_worker_custom_scan(CustomScanState *node, shm_toc *toc,
								 void *coordinate)
{
	CSCustomScanState *state = (CSCustomScanState *) node;
	ParallelTableScanDesc pscan = (ParallelTableScanDesc) coordinate;

	if (state->scandesc != NULL)
		cs_scan_end(state->scandesc);

	cs_open_scan(state, node->ss.ps.state, pscan);
}

static TupleTableSlot *
cs_exec_custom_scan(CustomScanState *node)
{
	return ExecScan(&node->ss,
					(ExecScanAccessMtd) cs_custom_scan_next,
					(ExecScanRecheckMtd) cs_custom_scan_recheck);
}

/*
 * Translate the bloom filters the planner pushed onto this CustomScan into
 * per-column entries on the TableScanDesc, so the scan loop can prune row
 * groups / dictionary entries.
 *
 * The filters live on the producing HashJoin's Hash node and are built lazily;
 * HashJoin.bloom_eager (set because we advertised CUSTOMPATH_SUPPORT_BLOOM_FILTERS)
 * guarantees the build happens before our first getnextslot, so they are ready
 * here.  We map each join key to a column and to its outer hash function, and
 * register it with cs_scan_set_bloom_filter() for hash32 probing.
 *
 * For a single-key join the combined filter is exactly that key's hash filter,
 * so we use it directly.  For a multi-key join we use the per-key filters
 * (HashJoin.bloom_perkey, also implied by our flag), since the combined filter
 * mixes the keys and cannot be tested against one column's dictionary.  A
 * producer whose filter is not yet built is skipped (no pruning, still
 * correct).
 */
static void
cs_apply_pushed_bloom_filters(CSCustomScanState *state)
{
	CustomScanState *node = &state->css;
	EState	   *estate = node->ss.ps.state;
	MemoryContext oldcxt;
	ListCell   *lc;

	if (node->ss.ps.bloom_filters == NIL || state->scandesc == NULL)
		return;

	oldcxt = MemoryContextSwitchTo(estate->es_query_cxt);

	foreach(lc, node->ss.ps.bloom_filters)
	{
		BloomFilterState *bfs = (BloomFilterState *) lfirst(lc);
		HashState  *hash;
		BloomFilter *bf = bfs->filter;
		int			nkeys = list_length(bf->filter_exprs);

		if (bfs->producer == NULL)
			continue;
		hash = castNode(HashState, innerPlanState(&bfs->producer->js.ps));

		if (nkeys == 1)
		{
			Var		   *var = (Var *) linitial(bf->filter_exprs);
			Oid			outfn,
						innfn;

			/* combined filter == the single key's hash filter */
			if (hash->bloom_filter == NULL || !IsA(var, Var))
				continue;
			get_op_hash_functions(linitial_oid(bf->hashops), &outfn, &innfn);
			cs_scan_set_bloom_filter(state->scandesc, var->varattno,
									 hash->bloom_filter, outfn,
									 linitial_oid(bf->hashcollations));
		}
		else if (hash->perkey_filters != NULL &&
				 hash->perkey_nfilters == nkeys)
		{
			for (int k = 0; k < nkeys; k++)
			{
				Var		   *var = (Var *) list_nth(bf->filter_exprs, k);
				Oid			outfn,
							innfn;

				if (!IsA(var, Var))
					continue;
				get_op_hash_functions(list_nth_oid(bf->hashops, k),
									  &outfn, &innfn);
				cs_scan_set_bloom_filter(state->scandesc, var->varattno,
										 hash->perkey_filters[k], outfn,
										 list_nth_oid(bf->hashcollations, k));
			}
		}
	}

	MemoryContextSwitchTo(oldcxt);
}

static TupleTableSlot *
cs_custom_scan_next(CustomScanState *node)
{
	CSCustomScanState *state = (CSCustomScanState *) node;
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;

	if (state->scandesc == NULL)
		return NULL;

	/*
	 * On the first tuple request, consume any bloom filters the planner
	 * pushed down (by now the producing hash table, and thus the filters, are
	 * built).
	 */
	if (!state->bloom_applied)
	{
		cs_apply_pushed_bloom_filters(state);
		state->bloom_applied = true;
	}

	if (table_scan_getnextslot(state->scandesc, ForwardScanDirection, slot))
		return slot;
	return NULL;
}

static bool
cs_custom_scan_recheck(CustomScanState *node, TupleTableSlot *slot)
{
	/*
	 * No EPQ rechecks: cstore is not subject to concurrent updates that could
	 * change visibility mid-scan.
	 */
	return true;
}

static void
cs_end_custom_scan(CustomScanState *node)
{
	CSCustomScanState *state = (CSCustomScanState *) node;

	if (state->scandesc != NULL)
	{
		table_endscan(state->scandesc);
		state->scandesc = NULL;
	}
}

static void
cs_rescan_custom_scan(CustomScanState *node)
{
	CSCustomScanState *state = (CSCustomScanState *) node;

	/* re-arm parallel instrumentation folding (shared totals reset in DSM) */
	state->instr_flushed = false;

	if (state->scandesc != NULL)
	{
		/*
		 * Drop the pushed-down bloom filters: the producing hash table (and
		 * thus the filter) is rebuilt across a rescan, so the pointers we
		 * registered are stale.  cs_apply_pushed_bloom_filters() re-pulls the
		 * fresh ones on the next getnextslot.
		 */
		cs_scan_set_bloom_filter(state->scandesc, 0, NULL,
								 InvalidOid, InvalidOid);
		state->bloom_applied = false;

		table_rescan(state->scandesc, NULL);
	}
}

/*
 * Shutdown callback: fold this participant's zone-map pruning counters into
 * the shared totals so the leader's EXPLAIN reports the whole scan, not just
 * the work units it personally claimed.
 *
 * ExecShutdownNode visits this node (a child of Gather) before it tears the
 * Gather's parallel context down, and a worker's shutdown runs before it
 * signals EOF -- so by the time the leader's shutdown runs, after Gather has
 * consumed every worker's output, all workers have already contributed.
 * Workers only add; the leader additionally folds the shared total into its
 * own scan, which cs_explain_custom_scan then reads.
 *
 * Only the zone-map counters need this: a hash-join bloom filter is never
 * pushed to a parallel-aware scan (see try_push_bloom_filter), so the bloom
 * counters are zero in every participant under parallelism.
 */
static void
cs_shutdown_custom_scan(CustomScanState *node)
{
	CSCustomScanState *state = (CSCustomScanState *) node;
	CScanDesc	scan;
	CSParallelScanDesc cpscan;

	if (state->scandesc == NULL || state->instr_flushed)
		return;
	scan = (CScanDesc) state->scandesc;
	cpscan = (CSParallelScanDesc) scan->rs_base.rs_parallel;
	if (cpscan == NULL)
		return;					/* serial scan: private counters suffice */

	state->instr_flushed = true;

	if (IsParallelWorker())
	{
		pg_atomic_fetch_add_u64(&cpscan->pcs_instr_rg_examined,
								scan->instr_rg_examined);
		pg_atomic_fetch_add_u64(&cpscan->pcs_instr_rg_zonemap_skipped,
								scan->instr_rg_zonemap_skipped);
	}
	else
	{
		/* leader: add the workers' totals to its own for the EXPLAIN lines */
		scan->instr_rg_examined +=
			pg_atomic_read_u64(&cpscan->pcs_instr_rg_examined);
		scan->instr_rg_zonemap_skipped +=
			pg_atomic_read_u64(&cpscan->pcs_instr_rg_zonemap_skipped);
	}
}

static void
cs_explain_custom_scan(CustomScanState *node, List *ancestors,
					   ExplainState *es)
{
	CSCustomScanState *state = (CSCustomScanState *) node;
	CustomScan *cscan = (CustomScan *) node->ss.ps.plan;
	Relation	rel = node->ss.ss_currentRelation;
	EState	   *estate = node->ss.ps.state;
	Bitmapset  *needed_cols;
	StringInfoData buf;
	int			attno = -1;
	bool		first = true;

	/*
	 * Report row-group pruning effectiveness when EXPLAIN ANALYZE is in use
	 * and at least one row-group catalog was examined.  Both counters live on
	 * the underlying TableScanDesc and remain accessible until ExecEnd runs;
	 * ExecutorEnd is called after ExplainPrintPlan finishes.
	 */
	if (es->analyze && state->scandesc != NULL)
	{
		CScanDesc	scan = (CScanDesc) state->scandesc;

		if (scan->instr_rg_examined > 0)
		{
			ExplainPropertyInteger("Row Groups Examined", NULL,
								   scan->instr_rg_examined, es);
			ExplainPropertyInteger("Row Groups Skipped by Zone Map", NULL,
								   scan->instr_rg_zonemap_skipped, es);
		}

		/*
		 * Report the effect of any hash-join bloom filter pushed down to this
		 * scan.  These lines account for the gap between the planner's
		 * estimated row count (computed before the filter was attached) and
		 * the actual rows emitted: rows the filter removed never reach an
		 * upstream node, and whole row groups it eliminated are never
		 * decompressed.  When the filter proved ineffective we disabled it
		 * mid-scan and say so, so the estimate/actual divergence is not
		 * misread as a planner misestimate.
		 */
		if (scan->instr_bloom_rg_probed > 0)
		{
			ExplainPropertyInteger("Rows Removed by Bloom Filter", NULL,
								   scan->instr_bloom_rows_removed, es);
			ExplainPropertyInteger("Row Groups Skipped by Bloom Filter", NULL,
								   scan->instr_bloom_rg_skipped, es);
			if (scan->bf_disabled)
				ExplainPropertyText("Bloom Filter Probing",
									"disabled (ineffective)", es);
		}
	}

	if (rel == NULL)
		return;

	needed_cols = cs_extract_scan_needed_cols(estate->es_plannedstmt->planTree,
											  &cscan->scan);
	if (needed_cols == NULL)
		return;

	initStringInfo(&buf);
	while ((attno = bms_next_member(needed_cols, attno)) >= 0)
	{
		Form_pg_attribute attr;

		if (attno < 0 || attno >= RelationGetDescr(rel)->natts)
			continue;
		attr = TupleDescAttr(RelationGetDescr(rel), attno);
		if (!first)
			appendStringInfoString(&buf, ", ");
		appendStringInfoString(&buf, NameStr(attr->attname));
		first = false;
	}

	if (buf.len > 0)
		ExplainPropertyText("Columnstore Projected Columns", buf.data, es);

	pfree(buf.data);
	bms_free(needed_cols);
}

/* ================================================================
 * Plan-tree walkers used to derive projection columns and scan keys
 * for a columnstore base relation from the surrounding plan, since
 * the AM has no direct planner-side hooks for either.
 * ==============================================================
 */

/*
 * find_plan_parent
 *		Walk the plan tree to find the immediate parent of `target`.
 *
 * Returns the parent Plan node and sets *is_inner to true if `target`
 * is the inner (right) child.  Returns NULL if `target` is the root
 * or not found.
 */
static Plan *
cs_find_plan_parent(Plan *root, Plan *target, bool *is_inner)
{
	Plan	   *result;

	if (root == NULL)
		return NULL;
	if (root->lefttree == target)
	{
		*is_inner = false;
		return root;
	}
	if (root->righttree == target)
	{
		*is_inner = true;
		return root;
	}

	/* Check Append/MergeAppend/SubqueryScan children */
	if (IsA(root, Append))
	{
		ListCell   *lc;

		foreach(lc, ((Append *) root)->appendplans)
		{
			if ((Plan *) lfirst(lc) == target)
			{
				*is_inner = false;
				return root;
			}
		}
	}
	else if (IsA(root, MergeAppend))
	{
		ListCell   *lc;

		foreach(lc, ((MergeAppend *) root)->mergeplans)
		{
			if ((Plan *) lfirst(lc) == target)
			{
				*is_inner = false;
				return root;
			}
		}
	}
	else if (IsA(root, SubqueryScan))
	{
		if (((SubqueryScan *) root)->subplan == target)
		{
			*is_inner = false;
			return root;
		}
	}

	/* Recurse into lefttree / righttree */
	result = cs_find_plan_parent(root->lefttree, target, is_inner);
	if (result != NULL)
		return result;
	result = cs_find_plan_parent(root->righttree, target, is_inner);
	if (result != NULL)
		return result;

	/* Recurse into Append/MergeAppend/SubqueryScan children */
	if (IsA(root, Append))
	{
		ListCell   *lc;

		foreach(lc, ((Append *) root)->appendplans)
		{
			result = cs_find_plan_parent((Plan *) lfirst(lc), target, is_inner);
			if (result != NULL)
				return result;
		}
	}
	else if (IsA(root, MergeAppend))
	{
		ListCell   *lc;

		foreach(lc, ((MergeAppend *) root)->mergeplans)
		{
			result = cs_find_plan_parent((Plan *) lfirst(lc), target, is_inner);
			if (result != NULL)
				return result;
		}
	}
	else if (IsA(root, SubqueryScan))
	{
		result = cs_find_plan_parent(((SubqueryScan *) root)->subplan,
									 target, is_inner);
		if (result != NULL)
			return result;
	}

	return NULL;
}

/*
 * extract_scan_needed_cols
 *		Determine which table columns are needed by the scan and its parent.
 *
 * Returns a Bitmapset of 0-indexed column numbers, or NULL if no columns
 * are needed (e.g. COUNT(*)).  The caller is responsible for freeing the
 * returned Bitmapset.
 *
 * The algorithm:
 * 1. Collect columns referenced in the scan's own qual.
 * 2. Find the parent plan node (via find_plan_parent).
 * 3. Determine which output positions the parent actually uses.
 * 4. Map those positions back to base-table column numbers via the
 *    scan's targetlist.
 * 5. Convert from pull_varattnos encoding to 0-indexed column numbers.
 */
static Bitmapset *
cs_extract_scan_needed_cols(Plan *root_plan, Scan *scan_plan)
{
	Plan	   *parent;
	bool		is_inner = false;
	Index		ref_varno;
	Bitmapset  *ref_attrs = NULL;
	Bitmapset  *needed_cols = NULL;
	int			attno;

	/* Include columns referenced in the scan's own qual */
	if (scan_plan->plan.qual != NIL)
		pull_varattnos((Node *) scan_plan->plan.qual,
					   scan_plan->scanrelid, &ref_attrs);

	/* Find the parent node to discover which output positions it uses */
	parent = cs_find_plan_parent(root_plan, &scan_plan->plan, &is_inner);
	if (parent == NULL)
	{
		/*
		 * Scan is the root plan.  Include the scan's own targetlist which
		 * contains the output column Vars (these still use the real varno,
		 * unlike parent nodes where setrefs.c rewrites them to OUTER_VAR).
		 */
		pull_varattnos((Node *) scan_plan->plan.targetlist,
					   scan_plan->scanrelid, &ref_attrs);
		if (ref_attrs == NULL)
			return NULL;
	}
	else
	{
		Bitmapset  *outer_refs = NULL;
		List	   *scan_tlist = scan_plan->plan.targetlist;
		int			pos_encoded;

		ref_varno = is_inner ? INNER_VAR : OUTER_VAR;

		/*
		 * ModifyTable (INSERT/UPDATE/DELETE) consumes all columns from its
		 * input but doesn't express this through Var references.  Return all
		 * scan columns as needed.
		 */
		if (IsA(parent, ModifyTable))
		{
			ListCell   *lc;

			foreach(lc, scan_plan->plan.targetlist)
			{
				TargetEntry *tle = (TargetEntry *) lfirst(lc);

				pull_varattnos((Node *) tle->expr,
							   scan_plan->scanrelid, &ref_attrs);
			}
			goto convert_attrs;
		}

		pull_varattnos((Node *) parent->targetlist,
					   ref_varno, &outer_refs);
		pull_varattnos((Node *) parent->qual,
					   ref_varno, &outer_refs);

		/*
		 * Join nodes store condition expressions in separate fields that
		 * aren't in plan.targetlist or plan.qual.  Extract column references
		 * from those too.
		 */
		if (IsA(parent, NestLoop) || IsA(parent, HashJoin) ||
			IsA(parent, MergeJoin))
		{
			Join	   *join = (Join *) parent;

			pull_varattnos((Node *) join->joinqual,
						   ref_varno, &outer_refs);
		}
		if (IsA(parent, NestLoop))
		{
			NestLoop   *nl = (NestLoop *) parent;
			ListCell   *lc;

			foreach(lc, nl->nestParams)
			{
				NestLoopParam *nlp = (NestLoopParam *) lfirst(lc);

				pull_varattnos((Node *) nlp->paramval,
							   ref_varno, &outer_refs);
			}
		}
		else if (IsA(parent, HashJoin))
		{
			HashJoin   *hj = (HashJoin *) parent;

			pull_varattnos((Node *) hj->hashclauses,
						   ref_varno, &outer_refs);
		}
		else if (IsA(parent, MergeJoin))
		{
			MergeJoin  *mj = (MergeJoin *) parent;

			pull_varattnos((Node *) mj->mergeclauses,
						   ref_varno, &outer_refs);
		}

		/*
		 * Some nodes reference input columns by positional index arrays
		 * rather than Var expressions.  Add those too.
		 */
		if (IsA(parent, Agg))
		{
			Agg		   *agg = (Agg *) parent;

			for (int i = 0; i < agg->numCols; i++)
				outer_refs = bms_add_member(outer_refs,
											agg->grpColIdx[i] -
											FirstLowInvalidHeapAttributeNumber);
		}
		else if (IsA(parent, Group))
		{
			Group	   *grp = (Group *) parent;

			for (int i = 0; i < grp->numCols; i++)
				outer_refs = bms_add_member(outer_refs,
											grp->grpColIdx[i] -
											FirstLowInvalidHeapAttributeNumber);
		}
		else if (IsA(parent, WindowAgg))
		{
			WindowAgg  *wag = (WindowAgg *) parent;

			for (int i = 0; i < wag->partNumCols; i++)
				outer_refs = bms_add_member(outer_refs,
											wag->partColIdx[i] -
											FirstLowInvalidHeapAttributeNumber);
			for (int i = 0; i < wag->ordNumCols; i++)
				outer_refs = bms_add_member(outer_refs,
											wag->ordColIdx[i] -
											FirstLowInvalidHeapAttributeNumber);
		}
		else if (IsA(parent, Unique))
		{
			Unique	   *uniq = (Unique *) parent;

			for (int i = 0; i < uniq->numCols; i++)
				outer_refs = bms_add_member(outer_refs,
											uniq->uniqColIdx[i] -
											FirstLowInvalidHeapAttributeNumber);
		}
		else if (IsA(parent, SetOp))
		{
			SetOp	   *setop = (SetOp *) parent;

			for (int i = 0; i < setop->numCols; i++)
				outer_refs = bms_add_member(outer_refs,
											setop->cmpColIdx[i] -
											FirstLowInvalidHeapAttributeNumber);
		}

		/*
		 * Map parent's output-position references back to base table columns
		 * via the scan's targetlist.  For a physical tlist (output position
		 * == attribute number) this is an identity mapping.  For a
		 * non-physical tlist (e.g. SELECT col3 FROM t) the parent's OUTER_VAR
		 * attno 1 maps to the scan's first target entry, whose Var gives the
		 * real base table column.
		 */
		pos_encoded = -1;
		while ((pos_encoded = bms_next_member(outer_refs, pos_encoded)) >= 0)
		{
			int			pos = pos_encoded + FirstLowInvalidHeapAttributeNumber;

			if (pos > 0 && pos <= list_length(scan_tlist))
			{
				TargetEntry *tle = (TargetEntry *) list_nth(scan_tlist,
															pos - 1);

				pull_varattnos((Node *) tle->expr,
							   scan_plan->scanrelid, &ref_attrs);
			}
		}
		bms_free(outer_refs);
	}

convert_attrs:
	if (ref_attrs == NULL)
		return NULL;

	/*
	 * Convert from pull_varattnos offset encoding to 0-indexed column
	 * numbers.
	 */
	attno = -1;
	while ((attno = bms_next_member(ref_attrs, attno)) >= 0)
	{
		int			real_attno = attno + FirstLowInvalidHeapAttributeNumber;

		/*
		 * A whole-row Var (attno 0) references every column; projection
		 * pushdown must be skipped or the unreferenced fields of the
		 * composite come back NULL for columnar rows.
		 */
		if (real_attno == 0)
		{
			bms_free(ref_attrs);
			bms_free(needed_cols);
			return NULL;
		}
		if (real_attno > 0)
			needed_cols = bms_add_member(needed_cols, real_attno - 1);
	}
	bms_free(ref_attrs);

	return needed_cols;
}


/* ----------------------------------------------------------------
 *		Scan qual key extraction support
 * ----------------------------------------------------------------
 */


/*
 * Does the expression reference any Param?  Quals captured for the
 * pushdown CustomScans are compiled and evaluated in a private executor
 * state with no access to the outer plan's parameter values, so
 * parameterized clauses (correlated references, nestloop params) must
 * stay with the standard plan.
 */
static bool
cs_contains_param_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;
	if (IsA(node, Param))
		return true;
	return expression_tree_walker(node, cs_contains_param_walker, context);
}

static bool
cs_contains_param(Node *node)
{
	return cs_contains_param_walker(node, NULL);
}

/*
 * extract_scan_qual_keys
 *		Extract btree-strategy scan keys from a list of qual expressions.
 *
 * Walks the qual list looking for simple "Var op Const" comparisons,
 * ScalarArrayOpExpr (IN-list), and NullTest expressions.  For each one
 * that can be mapped to a btree strategy number, fills in a ScanKeyData
 * entry.
 *
 * Returns the number of extracted keys.  If > 0, *keys_out is set to
 * a palloc'd array of ScanKeyData entries.  Caller must pfree it.
 */
static int
cs_extract_scan_qual_keys(List *quals, Index scanrelid, ScanKeyData **keys_out)
{
	ScanKeyData *keys;
	int			nkeys = 0;
	int			max_keys;
	ListCell   *lc;

	*keys_out = NULL;

	if (quals == NIL)
		return 0;

	max_keys = list_length(quals);
	keys = palloc(sizeof(ScanKeyData) * max_keys);

	foreach(lc, quals)
	{
		Node	   *qual = (Node *) lfirst(lc);
		OpExpr	   *opexpr;
		Var		   *var;
		Const	   *constval;
		Oid			opclass;
		Oid			opfamily;
		int			strategy;
		bool		var_on_left;

		/*
		 * Handle ScalarArrayOpExpr: "Var op ANY(ARRAY[...])" where the
		 * operator is an equality check.  We emit a single ScanKey with
		 * SK_SEARCHARRAY so the table AM can match against any element.
		 */
		if (IsA(qual, ScalarArrayOpExpr))
		{
			ScalarArrayOpExpr *saop = (ScalarArrayOpExpr *) qual;

			/* Only handle ANY (useOr), not ALL */
			if (!saop->useOr)
				continue;
			if (list_length(saop->args) != 2)
				continue;
			if (!IsA(linitial(saop->args), Var) ||
				!IsA(lsecond(saop->args), Const))
				continue;

			var = (Var *) linitial(saop->args);
			constval = (Const *) lsecond(saop->args);

			if (var->varno != scanrelid)
				continue;
			if (constval->constisnull)
				continue;

			/*
			 * Only same-type elements: a cross-type IN-list (e.g. datecol =
			 * ANY(timestamp[])) would be compared by the column type's own
			 * comparator against differently-typed datums.  The clause stays
			 * in the residual qual.
			 */
			if (get_base_element_type(constval->consttype) != var->vartype)
				continue;

			/*
			 * Only the column's own collation: comparisons run under
			 * attcollation, so a qual with an explicit COLLATE override must
			 * not be pushed.
			 */
			if (OidIsValid(saop->inputcollid) &&
				saop->inputcollid != var->varcollid)
				continue;

			/* Must be an equality operator */
			opclass = GetDefaultOpClass(var->vartype, BTREE_AM_OID);
			if (!OidIsValid(opclass))
				continue;
			opfamily = get_opclass_family(opclass);
			strategy = get_op_opfamily_strategy(saop->opno, opfamily);
			if (strategy != BTEqualStrategyNumber)
				continue;

			/* May need to grow the keys array */
			if (nkeys >= max_keys)
			{
				max_keys *= 2;
				keys = repalloc(keys, sizeof(ScanKeyData) * max_keys);
			}

			memset(&keys[nkeys], 0, sizeof(ScanKeyData));
			keys[nkeys].sk_flags = SK_SEARCHARRAY;
			keys[nkeys].sk_attno = var->varattno;
			keys[nkeys].sk_strategy = BTEqualStrategyNumber;
			keys[nkeys].sk_subtype = InvalidOid;
			keys[nkeys].sk_collation = saop->inputcollid;
			keys[nkeys].sk_argument = constval->constvalue;

			nkeys++;
			continue;
		}

		/*
		 * Handle NullTest: "Var IS NULL" or "Var IS NOT NULL".  These use
		 * SK_SEARCHNULL/SK_SEARCHNOTNULL flags with a NULL argument,
		 * mirroring how btree index scans represent these conditions.
		 */
		if (IsA(qual, NullTest))
		{
			NullTest   *ntest = (NullTest *) qual;
			Var		   *ntvar;

			if (!IsA(ntest->arg, Var))
				continue;

			/*
			 * ROW(...) IS NULL has per-field semantics that a storage
			 * nullness test cannot answer; leave it to the residual qual.
			 */
			if (ntest->argisrow)
				continue;

			ntvar = (Var *) ntest->arg;
			if (ntvar->varno != scanrelid)
				continue;

			if (nkeys >= max_keys)
			{
				max_keys *= 2;
				keys = repalloc(keys, sizeof(ScanKeyData) * max_keys);
			}

			memset(&keys[nkeys], 0, sizeof(ScanKeyData));
			keys[nkeys].sk_flags = (ntest->nulltesttype == IS_NULL) ?
				SK_SEARCHNULL : SK_SEARCHNOTNULL;
			keys[nkeys].sk_attno = ntvar->varattno;
			keys[nkeys].sk_strategy = InvalidStrategy;
			keys[nkeys].sk_subtype = InvalidOid;
			keys[nkeys].sk_collation = InvalidOid;
			keys[nkeys].sk_argument = (Datum) 0;

			nkeys++;
			continue;
		}

		if (!IsA(qual, OpExpr))
			continue;

		opexpr = (OpExpr *) qual;
		if (list_length(opexpr->args) != 2)
			continue;

		/* Check for (Var op Const) or (Const op Var) */
		if (IsA(linitial(opexpr->args), Var) &&
			IsA(lsecond(opexpr->args), Const))
		{
			var = (Var *) linitial(opexpr->args);
			constval = (Const *) lsecond(opexpr->args);
			var_on_left = true;
		}
		else if (IsA(linitial(opexpr->args), Const) &&
				 IsA(lsecond(opexpr->args), Var))
		{
			constval = (Const *) linitial(opexpr->args);
			var = (Var *) lsecond(opexpr->args);
			var_on_left = false;
		}
		else
			continue;

		/* Must reference this scan relation */
		if (var->varno != scanrelid)
			continue;

		/* Skip NULL constants */
		if (constval->constisnull)
			continue;

		/* Only the column's own collation; see the IN-list case above. */
		if (OidIsValid(opexpr->inputcollid) &&
			opexpr->inputcollid != var->varcollid)
			continue;

		/*
		 * Look up the btree strategy for this operator.  We need the default
		 * btree opclass for the variable's type.
		 */
		opclass = GetDefaultOpClass(var->vartype, BTREE_AM_OID);
		if (!OidIsValid(opclass))
			continue;

		opfamily = get_opclass_family(opclass);
		strategy = get_op_opfamily_strategy(opexpr->opno, opfamily);
		if (strategy == 0)
			continue;

		/*
		 * If Const is on the left, commute the strategy so it reads as
		 * "column op value".
		 */
		if (!var_on_left)
		{
			switch (strategy)
			{
				case BTLessStrategyNumber:
					strategy = BTGreaterStrategyNumber;
					break;
				case BTLessEqualStrategyNumber:
					strategy = BTGreaterEqualStrategyNumber;
					break;
				case BTGreaterEqualStrategyNumber:
					strategy = BTLessEqualStrategyNumber;
					break;
				case BTGreaterStrategyNumber:
					strategy = BTLessStrategyNumber;
					break;
				case BTEqualStrategyNumber:
					break;
				default:
					continue;
			}
		}

		/*
		 * Fill the ScanKeyData manually rather than using ScanKeyInit(),
		 * because we don't need sk_func populated — the table AM will use
		 * its own comparison functions.
		 */
		memset(&keys[nkeys], 0, sizeof(ScanKeyData));
		keys[nkeys].sk_flags = 0;
		keys[nkeys].sk_attno = var->varattno;
		keys[nkeys].sk_strategy = strategy;
		keys[nkeys].sk_subtype = (constval->consttype != var->vartype) ?
			constval->consttype : InvalidOid;
		keys[nkeys].sk_collation = opexpr->inputcollid;
		keys[nkeys].sk_argument = constval->constvalue;

		nkeys++;
	}

	if (nkeys == 0)
	{
		pfree(keys);
		return 0;
	}

	*keys_out = keys;
	return nkeys;
}

/* ========================================================================
 * Aggregate pushdown via create_upper_paths_hook (the
 * "ColumnstoreAggregate" CustomScan).
 *
 * Replaces an Aggregate node sitting directly above a columnstore base
 * relation when every aggregate in the targetlist is recognised by
 * cs_aggref_pushdown_kind.  The recognised set is:
 *
 *   COUNT(*), COUNT(col), COUNT(DISTINCT col)
 *      -> int8 row counter; DISTINCT uses a TupleHashTable dedup pass
 *
 *   SUM(int2|int4)             -> int128 accumulator, returns int8
 *   SUM(int8)                  -> int128 accumulator, returns numeric
 *   SUM(numeric)               -> int128 accumulator on NI64-encoded
 *                                 columns, fallback to numeric_add
 *   AVG(int2|int4|int8)        -> int128 sum + int8 count
 *   AVG(numeric)               -> numeric sum + int8 count
 *
 *   MIN/MAX over any type with a btree cmp_proc
 *      (enumerated explicitly for int2/int4/int8/float4/float8/date/
 *      time/timetz/timestamp/timestamptz/text/numeric/bpchar/interval/
 *      bytea, plus user-defined MIN/MAX following the same shape)
 *      -> Datum compare via the typcache cmp_proc with first-row
 *      tracking.
 *
 * Two stages dispatch into this code:
 *   - UPPERREL_GROUP_AGG (ungrouped queries): a sequential
 *     ColumnstoreAggregate that emits one row of finalised values.
 *   - UPPERREL_PARTIAL_GROUP_AGG (grouped queries): a parallel-aware
 *     partial path; each worker accumulates its own grouphash and
 *     emits per-group transition state for the upstream Finalize
 *     Aggregate to combine.  See the partial-path block further down
 *     in cs_create_upper_paths_hook.
 *
 * The CustomScan's executor opens a TableScanDesc, runs through every
 * row, accumulating per-aggregate state; on the second EXEC call it
 * emits a single tuple (or one tuple per group) containing the
 * finalized aggregate values, and returns NULL on the next call (EOF).
 *
 * The path is added to output_rel (the post-aggregate upper rel), not
 * input_rel.  Standard Aggregate paths are already on output_rel; we
 * add ours alongside, costed cheaper so the planner picks it.
 * ========================================================================
 */

#include "access/htup_details.h"
#include "catalog/pg_aggregate.h"
#include "common/int128.h"
#include "fmgr.h"
#include "libpq/pqformat.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/numeric.h"
#include "utils/typcache.h"

/* Per-aggregate runtime state. */
typedef enum CSAggKind
{
	CS_AGG_COUNT,				/* COUNT(*) or COUNT(col): int8 counter */
	CS_AGG_COUNT_DISTINCT,		/* COUNT(DISTINCT col): hash-set dedup */
	CS_AGG_SUM_NUMERIC,			/* SUM(numeric): int128 (NI64) or numeric_add */
	CS_AGG_AVG_NUMERIC,			/* AVG(numeric): SUM via numeric_add + count */
	CS_AGG_SUM_INT,				/* SUM(int2|int4): int8 result via int128 acc */
	CS_AGG_SUM_INT8,			/* SUM(int8): numeric result via int128 acc */
	CS_AGG_AVG_INT,				/* AVG(int2|int4|int8): numeric via int128 +
								 * count */
	CS_AGG_MIN,					/* MIN via type's btree cmp_proc */
	CS_AGG_MAX,					/* MAX via type's btree cmp_proc */
	CS_AGG_GROUP_KEY,			/* not an aggregate: a group-by column */
	CS_AGG_PASSTHROUGH,			/* constant in target list */
} CSAggKind;

typedef struct CSAggOne
{
	CSAggKind	kind;
	AttrNumber	col_attno;		/* 1-based, 0 for COUNT(*); for GROUP_KEY: the
								 * input rel attno being grouped on */
	bool		count_skips_null;
	int64		count;
	int128		sumX;			/* int128 accumulator for NI64 fast path */
	int32		dscale;			/* dscale of the NUMERIC column */
	bool		ni64_active;	/* sumX has accumulated >= 1 NI64 raw value */
	bool		saw_value;		/* any non-null row contributed to the
								 * aggregate via either the int128 or the
								 * numeric_add fallback path */
	Numeric		num_acc;		/* fallback Numeric accumulator */
	bool		num_acc_valid;
	Datum		minmax;
	bool		minmax_isnull;
	bool		first_row;
	Oid			col_type;
	int16		col_typlen;		/* MIN/MAX: typlen for datumCopy/pfree */
	bool		col_typbyval;	/* MIN/MAX: typbyval */
	Oid			col_collation;	/* MIN/MAX: collation for cmp_proc */
	FmgrInfo   *cmp_finfo;		/* MIN/MAX: btree cmp_proc fmgrinfo (or NULL) */
	Expr	   *filter_clause;	/* FILTER (WHERE ...) raw expr or NULL */
	ExprState  *filter_state;	/* compiled filter, built in cs_agg_begin */

	/*
	 * COUNT(DISTINCT col) state.  The first three fields (the desc/slot/
	 * eq+hash funcs) are template metadata, set up once in cs_agg_begin and
	 * copied to each per-group CSAggOne by reference -- they're read-only at
	 * run time so sharing is safe.  distinct_hash itself is per-group:
	 * state->aggs[i] owns the ungrouped table; gstate->aggs[i] owns its
	 * group's table, built lazily on isnew.
	 */
	TupleDesc	distinct_desc;
	TupleTableSlot *distinct_slot;
	Oid		   *distinct_eqfn;
	FmgrInfo   *distinct_hashfn;
	Oid		   *distinct_coll;
	TupleHashTable distinct_hash;
} CSAggOne;

/*
 * Per-group payload stored in the hash table's per-entry additional area.
 * Reset for each new group; cs_agg_exec walks state->aggs as the
 * "template" and uses these per-group copies for per-row accumulation.
 */
typedef struct CSAggGroupState
{
	CSAggOne   *aggs;			/* nagg entries, indexed by output position */
} CSAggGroupState;

typedef struct CSAggState
{
	CustomScanState css;		/* must be first */
	TableScanDesc scandesc;
	int			nagg;			/* output positions (group keys + aggs +
								 * consts) */
	CSAggOne   *aggs;			/* template per output position */
	bool		emitted;
	bool		exhausted;
	bool		is_partial;		/* emit AGGSPLIT_INITIAL_SERIAL transition
								 * state */
	TupleTableSlot *probe_slot; /* slot used to read input rows */
	Bitmapset  *needed_cols;	/* projection mask applied at scan-open time */
	ScanKeyData *qual_keys;		/* zone-map qual keys applied at scan-open */
	int			nqual_keys;
	ExprState  *qual_state;		/* per-row residual filter, or NULL */

	/* GROUP BY support */
	int			ngrpkeys;		/* number of group-key output positions */
	int		   *grpkey_outpos;	/* output position for each group key */
	AttrNumber *grpkey_attnos;	/* input rel attno for each group key */
	TupleHashTable grouphash;	/* group hash table; NULL for ungrouped */
	TupleTableSlot *grp_slot;	/* slot for hash key lookup, sized to grp keys */
	TupleHashIterator grp_iter;
	bool		grp_iter_started;

	/*
	 * Contexts backing all of this node's tuple hash tables (grouphash and
	 * the COUNT(DISTINCT) sets).  Created lazily by
	 * cs_agg_ensure_hash_cxts(); NULL until then.  hash_tuplescxt holds the
	 * hashed tuples and per-entry payloads and lives until shutdown;
	 * hash_tempcxt is per-lookup scratch, reset once per input row.
	 */
	MemoryContext hash_tuplescxt;
	MemoryContext hash_tempcxt;
} CSAggState;

/*
 * Magnitude bound for the NI64 int128 fast-path accumulator.  int128
 * holds ~1.7e38 (~2^127); values and sums are kept at or below 2^122 so
 * that one more multiply-by-ten (~2^125.4) plus one addition still fits
 * comfortably.  Crossing the bound folds the running sum into the
 * unbounded-precision Numeric accumulator (cs_agg_ni64_flush).
 */
#define CS_NI64_RESCALE_LIMIT	(((int128) 1) << 122)

/* Forward decls for aggregate-pushdown internals. */
static Numeric cs_int128_to_numeric(int128 val, int dscale);
static void cs_agg_num_acc_add(CSAggOne *a, Numeric n);
static void cs_agg_ni64_flush(CSAggOne *a);
static void cs_agg_ensure_hash_cxts(CSAggState *state, EState *estate);
static bool cs_aggref_pushdown_kind(Aggref *agg, Relation rel, CSAggOne *out);
static void cs_agg_accum_row(CSAggOne *a, TableScanDesc scandesc,
							 TupleTableSlot *slot, ExprContext *econtext);
static void cs_agg_emit_one(CSAggOne *a, TupleTableSlot *out, int outpos,
							bool partial);

/*
 * Convert a (possibly negative) int128 numerator with the given decimal
 * scale to a Numeric.  Used to finalize SUM(numeric)/AVG(numeric) when
 * the column was read via the NI64 fast path.
 *
 * Implementation builds a decimal text representation and feeds it to
 * numeric_in.  This runs once per group at finalize time, so the
 * conversion overhead is amortized over millions of accumulated rows.
 */
static Numeric
cs_int128_to_numeric(int128 val, int dscale)
{
	char		digits[40];		/* int128 magnitude: at most 39 decimal digits */
	int			ndigits = 0;
	bool		neg = (val < 0);
	uint128		uval = neg ? (uint128) (-val) : (uint128) val;
	int			intlen;
	char	   *out;
	char	   *p;
	Numeric		result;

	/* Decimal digits of the magnitude, least-significant first. */
	if (uval == 0)
		digits[ndigits++] = '0';
	else
	{
		while (uval > 0)
		{
			digits[ndigits++] = (char) ('0' + (int) (uval % 10));
			uval /= 10;
		}
	}

	/*
	 * Build "[-]<int>.<frac>", where the low dscale digits are the fraction.
	 * The integer part has max(ndigits - dscale, 1) digits; the fraction has
	 * exactly dscale digits, with leading zeros when dscale >= ndigits.
	 *
	 * dscale is the column's stored scale and can be as large as a numeric
	 * permits (up to NUMERIC_DSCALE_MASK = 16383), so the text does not fit a
	 * fixed stack buffer: size it to dscale.  digits[] itself is never
	 * indexed past the int128 width.  This runs once per group at finalize,
	 * so the allocation is negligible.
	 */
	intlen = (ndigits > dscale) ? (ndigits - dscale) : 1;
	out = palloc((Size) 1 /* sign */ + intlen + 1 /* dot */ + dscale + 1);
	p = out;

	if (neg)
		*p++ = '-';

	/* Integer part. */
	if (ndigits > dscale)
	{
		for (int i = ndigits - 1; i >= dscale; i--)
			*p++ = digits[i];
	}
	else
		*p++ = '0';

	/* Fraction: low dscale digits, zero-filled above the value's width. */
	if (dscale > 0)
	{
		*p++ = '.';
		for (int i = dscale - 1; i >= 0; i--)
			*p++ = (i < ndigits) ? digits[i] : '0';
	}
	*p = '\0';

	result = DatumGetNumeric(DirectFunctionCall3(numeric_in,
												 CStringGetDatum(out),
												 ObjectIdGetDatum(InvalidOid),
												 Int32GetDatum(-1)));
	pfree(out);
	return result;
}

/*
 * Fold the NI64 int128 accumulator into the Numeric accumulator and zero
 * it.  Used when rescaling to a higher dscale would risk overflowing
 * int128: the running sum moves to unbounded-precision Numeric (the same
 * accumulator the non-NI64 fallback path uses; the emit path already
 * merges both), and int128 accumulation restarts from zero.
 */
/*
 * Add a freshly allocated Numeric into the aggregate's Numeric
 * accumulator, taking ownership of (and ultimately freeing) 'n'.
 * Superseded intermediates are freed eagerly: they would otherwise pile
 * up once per row for the whole scan.
 */
static void
cs_agg_num_acc_add(CSAggOne *a, Numeric n)
{
	Datum		add;
	Numeric		prev;

	if (!a->num_acc_valid)
	{
		a->num_acc = n;
		a->num_acc_valid = true;
		return;
	}

	add = DirectFunctionCall2(numeric_add,
							  NumericGetDatum(a->num_acc),
							  NumericGetDatum(n));
	prev = a->num_acc;
	/* numeric_add returns a fresh palloc'd result; adopt it directly */
	a->num_acc = DatumGetNumeric(add);
	pfree(prev);
	pfree(n);
}

static void
cs_agg_ni64_flush(CSAggOne *a)
{
	cs_agg_num_acc_add(a, cs_int128_to_numeric(a->sumX, a->dscale));
	a->sumX = 0;
}

/*
 * Decide whether an Aggref can be pushed down.  If yes, fill in `out`
 * with the kind and column attno; return true.
 *
 * Dispatch is by aggfnoid -- the fmgroids.h F_* constants identify both
 * the aggregate function and its argument type unambiguously, so no
 * runtime catalog lookup or string comparison is needed.
 */
static bool
cs_aggref_pushdown_kind(Aggref *agg, Relation rel, CSAggOne *out)
{
	Oid			fnoid = agg->aggfnoid;
	TargetEntry *arg;
	Var		   *v;
	bool		is_min = false;
	bool		is_max = false;
	TypeCacheEntry *tcache;

	/*
	 * No ORDER BY inside the aggregate.  FILTER and DISTINCT are allowed:
	 * FILTER is evaluated per row before accumulating; DISTINCT is
	 * implemented for COUNT(DISTINCT col) only via a hash-set dedup -- other
	 * DISTINCT shapes (SUM/AVG DISTINCT) bail to the standard Agg path
	 * further down.
	 */
	if (agg->aggorder != NIL)
		return false;
	if (agg->aggvariadic)
		return false;
	if (agg->agglevelsup != 0)
		return false;

	/*
	 * Stash the FILTER clause unprocessed by setrefs, so its Vars retain
	 * their relid-based varno and evaluate against probe_slot at runtime.
	 */
	out->filter_clause = (Expr *) copyObject(agg->aggfilter);

	/* COUNT(*) -- aggref args is an empty list. */
	if (list_length(agg->args) == 0 && agg->aggstar &&
		fnoid == F_COUNT_)
	{
		out->kind = CS_AGG_COUNT;
		out->col_attno = 0;
		out->count_skips_null = false;
		return true;
	}

	/* COUNT(col) and COUNT(DISTINCT col). */
	if (fnoid == F_COUNT_ANY)
	{
		if (list_length(agg->args) != 1)
			return false;
		arg = linitial_node(TargetEntry, agg->args);
		if (!IsA(arg->expr, Var))
			return false;
		v = (Var *) arg->expr;
		if (v->varlevelsup != 0)
			return false;
		out->col_attno = v->varattno;
		out->col_type = v->vartype;
		if (agg->aggdistinct != NIL)
		{
			out->kind = CS_AGG_COUNT_DISTINCT;
			out->col_collation = agg->inputcollid;
			return true;
		}
		out->kind = CS_AGG_COUNT;
		out->count_skips_null = true;
		return true;
	}

	/*
	 * SUM/AVG/MIN/MAX with DISTINCT: bail.  The hash-dedup machinery here is
	 * for COUNT only; rare enough to leave to the standard Agg path.
	 */
	if (agg->aggdistinct != NIL)
		return false;

	/* All remaining cases require a single Var argument. */
	if (list_length(agg->args) != 1)
		return false;
	arg = linitial_node(TargetEntry, agg->args);
	if (!IsA(arg->expr, Var))
		return false;
	v = (Var *) arg->expr;
	if (v->varlevelsup != 0)
		return false;
	out->col_attno = v->varattno;
	out->col_type = v->vartype;

	switch (fnoid)
	{
		case F_SUM_NUMERIC:
			out->kind = CS_AGG_SUM_NUMERIC;
			return true;
		case F_AVG_NUMERIC:
			out->kind = CS_AGG_AVG_NUMERIC;
			return true;
		case F_SUM_INT2:
		case F_SUM_INT4:
			out->kind = CS_AGG_SUM_INT;
			return true;
		case F_SUM_INT8:
			out->kind = CS_AGG_SUM_INT8;
			return true;
		case F_AVG_INT2:
		case F_AVG_INT4:
		case F_AVG_INT8:
			out->kind = CS_AGG_AVG_INT;
			return true;
		default:
			break;
	}

	/*
	 * MIN/MAX over any type with a btree cmp_proc.  The fmgroids.h F_MIN_* /
	 * F_MAX_* set is enumerated, but we also accept user-defined MIN/MAX
	 * aggregates that follow the same shape (single Var, return type = input
	 * type, type has cmp_proc).  Runtime uses the typcache's cached FmgrInfo,
	 * so the per-row cost is one FunctionCall2Coll.
	 */
	switch (fnoid)
	{
		case F_MIN_INT2:
		case F_MIN_INT4:
		case F_MIN_INT8:
		case F_MIN_FLOAT4:
		case F_MIN_FLOAT8:
		case F_MIN_DATE:
		case F_MIN_TIME:
		case F_MIN_TIMETZ:
		case F_MIN_TIMESTAMP:
		case F_MIN_TIMESTAMPTZ:
		case F_MIN_TEXT:
		case F_MIN_NUMERIC:
		case F_MIN_BPCHAR:
		case F_MIN_INTERVAL:
		case F_MIN_BYTEA:
			is_min = true;
			break;
		case F_MAX_INT2:
		case F_MAX_INT4:
		case F_MAX_INT8:
		case F_MAX_FLOAT4:
		case F_MAX_FLOAT8:
		case F_MAX_DATE:
		case F_MAX_TIME:
		case F_MAX_TIMETZ:
		case F_MAX_TIMESTAMP:
		case F_MAX_TIMESTAMPTZ:
		case F_MAX_TEXT:
		case F_MAX_NUMERIC:
		case F_MAX_BPCHAR:
		case F_MAX_INTERVAL:
		case F_MAX_BYTEA:
			is_max = true;
			break;
		default:
			return false;
	}
	(void) is_max;				/* set in switch but only is_min is read */

	tcache = lookup_type_cache(out->col_type, TYPECACHE_CMP_PROC_FINFO);
	if (!OidIsValid(tcache->cmp_proc) ||
		tcache->cmp_proc_finfo.fn_oid == InvalidOid)
		return false;

	out->kind = is_min ? CS_AGG_MIN : CS_AGG_MAX;
	out->col_typlen = tcache->typlen;
	out->col_typbyval = tcache->typbyval;
	out->col_collation = agg->inputcollid;
	out->cmp_finfo = &tcache->cmp_proc_finfo;
	return true;
}

static void
cs_create_upper_paths_hook(PlannerInfo *root, UpperRelationKind stage,
						   RelOptInfo *input_rel, RelOptInfo *output_rel,
						   void *extra)
{
	Query	   *parse = root->parse;
	RangeTblEntry *rte;
	Relation	rel;
	List	   *agg_descs = NIL;
	List	   *having_aggrefs_extra = NIL;
	List	   *having_aggrefs = NIL;
	List	   *baseclauses = NIL;
	List	   *priv;
	ListCell   *lc;
	ListCell   *lc2;
	CustomPath *cpath;
	Path	   *cheapest;

	if (prev_create_upper_paths_hook)
		prev_create_upper_paths_hook(root, stage, input_rel, output_rel, extra);

	if (stage == UPPERREL_ORDERED)
	{
		cs_try_add_latemat_path(root, input_rel, output_rel);
		return;
	}

	if (stage != UPPERREL_GROUP_AGG &&
		stage != UPPERREL_PARTIAL_GROUP_AGG)
		return;

	/* GROUPING SETS, ROLLUP, CUBE not supported. */
	if (parse->groupingSets != NIL ||
		parse->hasWindowFuncs || parse->hasTargetSRFs)
		return;
	if (!parse->hasAggs && parse->groupClause == NIL)
		return;

	/* Single plain columnstore base relation only. */
	if (!cs_upper_input_is_columnstore_rel(root, input_rel, &rte))
		return;

	/* WHERE clauses are forwarded as scan-key pushdown + residual filter. */

	rel = relation_open(rte->relid, NoLock);

	/*
	 * Each targetlist entry must be one of: * Const                 -
	 * passthrough constant * Var on input rel      - GROUP BY column (we
	 * accept any Var here since it must come from somewhere; the planner has
	 * already validated GROUP BY coverage) * Aggref pushdownable   - a
	 * recognized agg pattern
	 */
	foreach(lc, parse->targetList)
	{
		TargetEntry *tle = lfirst_node(TargetEntry, lc);
		CSAggOne	desc = {0};

		if (IsA(tle->expr, Const))
		{
			agg_descs = lappend(agg_descs, NULL);
			continue;
		}
		if (IsA(tle->expr, Var))
		{
			Var		   *v = (Var *) tle->expr;
			CSAggOne   *copy;

			if (v->varno != input_rel->relid)
			{
				relation_close(rel, NoLock);
				return;
			}
			copy = palloc(sizeof(CSAggOne));
			memset(copy, 0, sizeof(*copy));
			copy->kind = CS_AGG_GROUP_KEY;
			copy->col_attno = v->varattno;
			copy->col_type = v->vartype;
			agg_descs = lappend(agg_descs, copy);
			continue;
		}
		if (!IsA(tle->expr, Aggref))
		{
			relation_close(rel, NoLock);
			return;
		}
		if (!cs_aggref_pushdown_kind((Aggref *) tle->expr, rel, &desc))
		{
			relation_close(rel, NoLock);
			return;
		}
		{
			CSAggOne   *copy = palloc(sizeof(CSAggOne));

			*copy = desc;
			agg_descs = lappend(agg_descs, copy);
		}
	}

	/*
	 * HAVING may reference Aggrefs that are not in the SELECT target list
	 * (e.g. SELECT grp FROM ... HAVING count(*) > 100).  Augment our
	 * custom_scan_tlist with these so setrefs.c's fix_upper_expr can rewrite
	 * them to slot positions; they are emitted by cs_agg_exec alongside the
	 * SELECT-list aggregates and dropped by the plan-level targetlist after
	 * HAVING evaluation.  Bail if any HAVING-only Aggref isn't pushdownable.
	 */
	if (parse->havingQual != NULL)
		having_aggrefs = pull_var_clause((Node *) parse->havingQual,
										 PVC_INCLUDE_AGGREGATES);
	foreach(lc2, having_aggrefs)
	{
		Node	   *node = (Node *) lfirst(lc2);
		Aggref	   *hagg;
		ListCell   *lc3;
		bool		found = false;
		CSAggOne	desc = {0};
		CSAggOne   *copy;

		if (!IsA(node, Aggref))
			continue;			/* group-key Var: already in tlist */
		hagg = (Aggref *) node;

		foreach(lc3, parse->targetList)
		{
			TargetEntry *tle = lfirst_node(TargetEntry, lc3);

			if (IsA(tle->expr, Aggref) && equal(tle->expr, hagg))
			{
				found = true;
				break;
			}
		}
		if (found)
			continue;

		if (!cs_aggref_pushdown_kind(hagg, rel, &desc))
		{
			relation_close(rel, NoLock);
			return;
		}
		copy = palloc(sizeof(CSAggOne));
		*copy = desc;
		agg_descs = lappend(agg_descs, copy);
		having_aggrefs_extra = lappend(having_aggrefs_extra, hagg);
	}

	relation_close(rel, NoLock);

	/*
	 * If GROUP BY is present, every key expression must be a simple Var
	 * referencing the input rel, or a Const (e.g. GROUP BY 1 where the
	 * targetlist position is a constant -- the planner treats this as a
	 * trivial group key and we just emit it from the targetlist Const).
	 */
	if (parse->groupClause != NIL)
	{
		foreach(lc2, parse->groupClause)
		{
			SortGroupClause *sgc = lfirst_node(SortGroupClause, lc2);
			TargetEntry *tle = get_sortgroupclause_tle(sgc, parse->targetList);

			if (IsA(tle->expr, Const))
				continue;
			if (!IsA(tle->expr, Var))
				return;
			if (((Var *) tle->expr)->varno != input_rel->relid)
				return;
		}
	}

	if (agg_descs == NIL)
		return;

	/* Use the cheapest input path's row count as our scan-cost reference. */
	cheapest = input_rel->cheapest_total_path;
	if (cheapest == NULL)
		return;

	cpath = makeNode(CustomPath);
	cpath->path.pathtype = T_CustomScan;
	cpath->path.parent = output_rel;
	cpath->path.pathtarget = output_rel->reltarget;
	cpath->path.parallel_aware = false;
	cpath->path.parallel_safe = false;
	cpath->path.parallel_workers = 0;
	cpath->path.rows = 1;
	cpath->path.startup_cost = cheapest->total_cost;
	cpath->path.total_cost = cheapest->total_cost +
		cpu_tuple_cost * cheapest->rows * 0.1;
	cpath->path.pathkeys = NIL;
	cpath->custom_paths = NIL;

	/*
	 * Stash for cs_agg_begin / cs_agg_path_to_plan: 1. agg_descs       --
	 * per-Aggref descriptors 2. relid (Integer) -- input scan rti 3.
	 * baseclauses     -- restrict clauses to apply at scan time 4. havingQual
	 * -- HAVING clauses to apply per group at emit 5. having_extra    --
	 * HAVING-only Aggrefs to append to scan tlist
	 */
	foreach(lc2, input_rel->baserestrictinfo)
	{
		RestrictInfo *ri = lfirst_node(RestrictInfo, lc2);

		/*
		 * SubPlans cannot be compiled by the executor-side
		 * ExecPrepareQual(parent = NULL); leave such queries to the standard
		 * Aggregate plan.
		 */
		if (contain_subplans((Node *) ri->clause) ||
			cs_contains_param((Node *) ri->clause))
			return;

		baseclauses = lappend(baseclauses, copyObject(ri->clause));
	}
	priv = list_make4(agg_descs,
					  makeInteger(input_rel->relid),
					  baseclauses,
					  copyObject(parse->havingQual));
	priv = lappend(priv, having_aggrefs_extra);
	priv = lappend(priv, makeInteger(0));	/* not partial */
	cpath->custom_private = priv;
	cpath->methods = &cs_agg_path_methods;
	cpath->flags = 0;

	/*
	 * Sequential ColumnstoreAggregate is added on UPPERREL_GROUP_AGG only for
	 * ungrouped queries.  GROUP BY queries use the partial path on
	 * UPPERREL_PARTIAL_GROUP_AGG below; this avoids OOM risk under bad
	 * row-count estimates and lets the planner pick a parallel-aware Finalize
	 * Aggregate above us.
	 */
	if (stage == UPPERREL_GROUP_AGG && parse->groupClause == NIL)
		add_path(output_rel, &cpath->path);

	/*
	 * Partial-aware ColumnstoreAggregate on UPPERREL_PARTIAL_GROUP_AGG. Each
	 * worker accumulates its own grouphash; the upstream Finalize Aggregate
	 * that gather_grouping_paths will install on top of Gather combines
	 * partial states.  Bail for COUNT_DISTINCT (per-group dedup hash can't
	 * combine across workers) and for the numeric INTERNAL- state aggs
	 * (SUM_NUMERIC, AVG_NUMERIC) which need a NumericVar serializer that
	 * lives static in numeric.c.  AVG_INT and SUM_INT8 also use INTERNAL
	 * transition state, but their bytea format (int8_avg_serialize: N + sumX
	 * as 24 bytes) is trivial enough that cs_agg_emit_one can produce it
	 * directly.  HAVING is also unsupported here: the Finalize step lives
	 * upstream and can't see our partial- state Aggrefs.
	 */
	if (stage == UPPERREL_PARTIAL_GROUP_AGG && parse->groupClause != NIL)
	{
		ListCell   *adlc;
		bool		partial_safe = true;
		Relation	rel_open;
		int			parallel_workers;

		if (parse->havingQual != NULL || having_aggrefs_extra != NIL)
			partial_safe = false;
		foreach(adlc, agg_descs)
		{
			CSAggOne   *desc = (CSAggOne *) lfirst(adlc);

			if (desc == NULL)
				continue;
			switch (desc->kind)
			{
				case CS_AGG_COUNT:
				case CS_AGG_SUM_INT:
				case CS_AGG_SUM_INT8:
				case CS_AGG_AVG_INT:
				case CS_AGG_MIN:
				case CS_AGG_MAX:
				case CS_AGG_GROUP_KEY:
				case CS_AGG_PASSTHROUGH:
					break;
				default:
					partial_safe = false;
					break;
			}
		}
		if (!partial_safe)
			return;

		rel_open = relation_open(rte->relid, NoLock);
		parallel_workers = cs_compute_parallel_workers(rel_open);
		relation_close(rel_open, NoLock);
		if (parallel_workers <= 0)
			return;

		{
			CustomPath *ppath = makeNode(CustomPath);
			List	   *ppriv;
			List	   *group_exprs = NIL;
			ListCell   *gc;
			double		est_groups;
			double		divisor = (double) parallel_workers;

			foreach(gc, parse->groupClause)
			{
				SortGroupClause *sgc = lfirst_node(SortGroupClause, gc);
				TargetEntry *tle = get_sortgroupclause_tle(sgc, parse->targetList);

				group_exprs = lappend(group_exprs, tle->expr);
			}
			est_groups = estimate_num_groups(root, group_exprs,
											 input_rel->rows, NULL, NULL);

			/*
			 * Bail when the grouping keys barely compress.  Each worker still
			 * has to emit one row per distinct key it sees, and with high
			 * cardinality the per-worker output is close to the total
			 * distinct count, so we just shovel the same data through Gather
			 * and the leader's Finalize HashAggregate spills heavily.  The
			 * stock plan (single HashAgg over our scan) wins in that regime.
			 */
			if (est_groups > input_rel->rows / (divisor * 2.0))
				return;

			ppath->path.pathtype = T_CustomScan;
			ppath->path.parent = output_rel;
			ppath->path.pathtarget = output_rel->reltarget;
			ppath->path.param_info = NULL;
			ppath->path.parallel_aware = true;
			ppath->path.parallel_safe = true;
			ppath->path.parallel_workers = parallel_workers;
			ppath->path.rows = clamp_row_est(est_groups / divisor);
			ppath->path.startup_cost = cheapest->total_cost / divisor;
			ppath->path.total_cost = cheapest->total_cost / divisor +
				cpu_tuple_cost * cheapest->rows / divisor * 0.1;
			ppath->path.pathkeys = NIL;
			ppath->custom_paths = NIL;

			ppriv = list_make4(agg_descs,
							   makeInteger(input_rel->relid),
							   baseclauses,
							   copyObject(parse->havingQual));
			ppriv = lappend(ppriv, having_aggrefs_extra);
			ppriv = lappend(ppriv, makeInteger(1)); /* partial */
			ppath->custom_private = ppriv;
			ppath->methods = &cs_agg_path_methods;
			ppath->flags = 0;

			add_partial_path(output_rel, &ppath->path);
		}
	}
}

/*
 * Plan-time conversion: build a CustomScan node carrying the agg
 * descriptors and the underlying scan's relid in custom_private.
 */
static Plan *
cs_agg_path_to_plan(PlannerInfo *root, RelOptInfo *rel,
					struct CustomPath *best_path, List *tlist,
					List *clauses, List *custom_plans)
{
	CustomScan *cscan = makeNode(CustomScan);
	int			scanrelid = intVal(lsecond(best_path->custom_private));
	Node	   *having = (list_length(best_path->custom_private) > 3
						  ? (Node *) list_nth(best_path->custom_private, 3)
						  : NULL);
	List	   *having_extra = (list_length(best_path->custom_private) > 4
								? (List *) list_nth(best_path->custom_private, 4)
								: NIL);
	bool		is_partial = (list_length(best_path->custom_private) > 5
							  && intVal(list_nth(best_path->custom_private,
												 5)) != 0);
	List	   *augmented_tlist = list_copy(tlist);
	int			next_resno = list_length(tlist) + 1;
	ListCell   *lc;

	/*
	 * Append HAVING-only Aggrefs to the scan tlist so fix_upper_expr can
	 * rewrite plan.qual against them.  These positions show up in the scan
	 * slot but are dropped by plan.targetlist projection.
	 */
	foreach(lc, having_extra)
	{
		Aggref	   *agg = (Aggref *) lfirst(lc);
		TargetEntry *tle = makeTargetEntry((Expr *) agg, next_resno++,
										   NULL, false);

		augmented_tlist = lappend(augmented_tlist, tle);
	}

	/*
	 * Both plan.targetlist and custom_scan_tlist hold the same Aggref-
	 * bearing expressions.  setrefs.c's fix_upper_expr will replace each
	 * Aggref in plan.targetlist with a Var(INDEX_VAR, resno) pointing into
	 * custom_scan_tlist; the executor reads our synthesized scan tuple at
	 * those slot positions without evaluating the Aggref itself.
	 *
	 * HAVING is plumbed through plan.qual the same way: setrefs's
	 * fix_upper_expr rewrites Aggref / group-Var references against
	 * custom_scan_tlist so that at runtime ExecQual reads slot positions
	 * directly without touching the Aggref evaluator.  cs_agg_exec applies
	 * ps.qual after each group's projection.
	 */
	cscan->scan.plan.targetlist = tlist;
	if (having != NULL && IsA(having, List))
		cscan->scan.plan.qual = (List *) having;
	else if (having != NULL)
		cscan->scan.plan.qual = list_make1(having);
	else
		cscan->scan.plan.qual = NIL;
	cscan->scan.plan.lefttree = NULL;
	cscan->scan.plan.righttree = NULL;
	cscan->scan.plan.parallel_aware = is_partial;
	cscan->scan.plan.parallel_safe = is_partial;
	cscan->scan.scanrelid = scanrelid;

	cscan->flags = best_path->flags;
	cscan->custom_plans = NIL;
	cscan->custom_exprs = NIL;

	/*
	 * custom_private carries Node-only values so the executor's plan
	 * serializer (ExecSerializePlan, used for parallel workers) can copy /
	 * round-trip it via nodeToString: - linitial: base-rel restriction
	 * clauses (Vars retain their input-rel varnos; setrefs.c does not process
	 * custom_private, so we evaluate them ourselves at execution time with
	 * ecxt_scantuple=probe_slot). - lsecond:  is_partial flag (Integer 0 or
	 * 1) so cs_agg_begin knows whether to emit AGGSPLIT_INITIAL_SERIAL
	 * transition values rather than final values.
	 *
	 * The agg descriptors (CSAggOne, not Nodes) are NOT stored here;
	 * cs_agg_begin rebuilds them by walking the Aggref / Var / Const entries
	 * in cscan->custom_scan_tlist.
	 */
	cscan->custom_private = list_make2(lthird(best_path->custom_private),
									   makeInteger(is_partial ? 1 : 0));
	cscan->custom_scan_tlist = (List *) copyObject(augmented_tlist);
	cscan->custom_relids = bms_make_singleton(scanrelid);
	cscan->methods = &cs_agg_scan_methods;

	return (Plan *) cscan;
}

/*
 * Create the memory contexts backing this node's tuple hash tables, once.
 *
 * BuildTupleHashTable() requires the tuples context to be distinct from the
 * metadata context (resetting the former must not destroy the latter).  The
 * temp context is scratch space for hash/equality evaluation during lookups;
 * the accumulation loops reset it once per input row.  All hash tables of
 * one CSAggState share both contexts.
 */
static void
cs_agg_ensure_hash_cxts(CSAggState *state, EState *estate)
{
	if (state->hash_tuplescxt != NULL)
		return;
	state->hash_tuplescxt =
		AllocSetContextCreate(estate->es_query_cxt,
							  "ColumnstoreAgg hash tuples",
							  ALLOCSET_DEFAULT_SIZES);
	state->hash_tempcxt =
		AllocSetContextCreate(estate->es_query_cxt,
							  "ColumnstoreAgg hash temp",
							  ALLOCSET_SMALL_SIZES);
}

static Node *
cs_agg_create_state(CustomScan *cscan)
{
	CSAggState *state = palloc0(sizeof(CSAggState));

	NodeSetTag(state, T_CustomScanState);
	state->css.methods = &cs_agg_exec_methods;

	/*
	 * We synthesize a single virtual tuple per query.  ss_ScanTupleSlot's
	 * tupledesc is built from custom_scan_tlist; using TTSOpsVirtual lets us
	 * fill tts_values/tts_isnull directly.
	 */
	state->css.slotOps = &TTSOpsVirtual;
	return (Node *) state;
}

static void
cs_agg_begin(CustomScanState *node, EState *estate, int eflags)
{
	CSAggState *state = (CSAggState *) node;
	CustomScan *cscan = (CustomScan *) node->ss.ps.plan;
	List	   *baseclauses = (list_length(cscan->custom_private) > 0
							   ? (List *) linitial(cscan->custom_private)
							   : NIL);
	List	   *agg_descs = NIL;
	Relation	rel = node->ss.ss_currentRelation;
	Bitmapset  *needed_cols = NULL;
	ScanKeyData *qual_keys = NULL;
	int			nqual_keys = 0;
	ListCell   *lc;
	int			i;

	Assert(rel != NULL);

	/*
	 * Defer scan_begin to cs_agg_open_scan; for parallel-aware paths the DSM
	 * init / worker init callbacks open the scan with a shared
	 * ParallelTableScanDesc, and we don't want to allocate-then-discard a
	 * sequential scandesc here.
	 */
	state->scandesc = NULL;
	state->is_partial = (list_length(cscan->custom_private) > 1
						 && intVal(list_nth(cscan->custom_private, 1)) != 0);

	/*
	 * Rebuild the per-output-position CSAggOne descriptors by walking
	 * cscan->custom_scan_tlist.  We don't stash the agg_descs list in
	 * custom_private because CSAggOne is not a Node and would not survive the
	 * parallel-plan serializer (ExecSerializePlan).  The Aggref nodes in
	 * custom_scan_tlist round-trip through nodeToString correctly, so we
	 * re-derive descriptors from them here at begin time.
	 */
	foreach(lc, cscan->custom_scan_tlist)
	{
		TargetEntry *tle = lfirst_node(TargetEntry, lc);
		CSAggOne	desc = {0};
		CSAggOne   *copy;

		if (IsA(tle->expr, Const))
		{
			agg_descs = lappend(agg_descs, NULL);
			continue;
		}
		if (IsA(tle->expr, Var))
		{
			Var		   *v = (Var *) tle->expr;

			copy = palloc0(sizeof(CSAggOne));
			copy->kind = CS_AGG_GROUP_KEY;
			copy->col_attno = v->varattno;
			copy->col_type = v->vartype;
			agg_descs = lappend(agg_descs, copy);
			continue;
		}
		if (!IsA(tle->expr, Aggref))
			elog(ERROR, "ColumnstoreAggregate: unexpected tlist node tag %d",
				 nodeTag(tle->expr));
		if (!cs_aggref_pushdown_kind((Aggref *) tle->expr, rel, &desc))
			elog(ERROR, "ColumnstoreAggregate: Aggref no longer pushdownable");
		copy = palloc(sizeof(CSAggOne));
		*copy = desc;
		agg_descs = lappend(agg_descs, copy);
	}

	state->nagg = list_length(agg_descs);
	state->aggs = palloc0(sizeof(CSAggOne) * state->nagg);
	i = 0;
	foreach(lc, agg_descs)
	{
		CSAggOne   *src = (CSAggOne *) lfirst(lc);

		if (src == NULL)		/* constant pass-through */
		{
			state->aggs[i].col_attno = -1;	/* sentinel */
			i++;
			continue;
		}
		state->aggs[i] = *src;
		state->aggs[i].first_row = true;
		if (src->col_attno > 0)
			needed_cols = bms_add_member(needed_cols, src->col_attno - 1);

		/*
		 * Compile the FILTER (if any) into an ExprState.  Vars in the filter
		 * reference the relation's column numbers (we copyObject'd the filter
		 * from the parser-level Aggref before setrefs touched it), so we
		 * evaluate against probe_slot at runtime.  We also add
		 * filter-referenced columns to needed_cols so the AM materializes
		 * them.  parent=NULL keeps EEOP_SCAN_FETCHSOME from chasing into our
		 * CustomScanState's scan-output slot (which has the agg tupledesc,
		 * not the relation tupledesc).
		 */
		if (src->filter_clause != NULL)
		{
			List	   *flist = list_make1(src->filter_clause);
			List	   *fvars;
			ListCell   *fc;

			state->aggs[i].filter_state = ExecPrepareQual(flist, estate);
			fvars = pull_var_clause((Node *) src->filter_clause, 0);
			foreach(fc, fvars)
			{
				Var		   *v = (Var *) lfirst(fc);

				if (v->varno == cscan->scan.scanrelid && v->varattno > 0)
					needed_cols = bms_add_member(needed_cols, v->varattno - 1);
			}
		}

		/*
		 * COUNT(DISTINCT col) setup.  Build a 1-column TupleHashTable
		 * descriptor + slot + eq/hash funcs once per CSAggOne; per-group hash
		 * tables (in gstate->aggs[i].distinct_hash) reuse this metadata.  For
		 * ungrouped queries the hash table itself lives on state->aggs[i] and
		 * is built here too.
		 */
		if (state->aggs[i].kind == CS_AGG_COUNT_DISTINCT)
		{
			TupleDesc	d_desc;
			Form_pg_attribute relatt = TupleDescAttr(RelationGetDescr(rel),
													 src->col_attno - 1);
			AttrNumber	keyColIdx[1] = {1};
			Oid		   *eqOps = palloc(sizeof(Oid) * 1);
			Oid		   *eqfuncoids;
			FmgrInfo   *hashfunctions;
			Oid		   *colls = palloc(sizeof(Oid) * 1);
			TypeCacheEntry *tcache;

			d_desc = CreateTemplateTupleDesc(1);
			TupleDescInitEntry(d_desc, 1, "k", relatt->atttypid,
							   relatt->atttypmod, 0);
			TupleDescInitEntryCollation(d_desc, 1, relatt->attcollation);
			TupleDescFinalize(d_desc);

			tcache = lookup_type_cache(relatt->atttypid,
									   TYPECACHE_EQ_OPR);
			if (!OidIsValid(tcache->eq_opr))
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_FUNCTION),
						 errmsg("type %s has no default equality operator",
								format_type_be(relatt->atttypid))));
			eqOps[0] = tcache->eq_opr;
			execTuplesHashPrepare(1, eqOps, &eqfuncoids, &hashfunctions);
			colls[0] = src->col_collation;

			state->aggs[i].distinct_desc = d_desc;
			state->aggs[i].distinct_slot =
				MakeSingleTupleTableSlot(d_desc, &TTSOpsVirtual);
			state->aggs[i].distinct_eqfn = eqfuncoids;
			state->aggs[i].distinct_hashfn = hashfunctions;
			state->aggs[i].distinct_coll = colls;
			cs_agg_ensure_hash_cxts(state, estate);
			state->aggs[i].distinct_hash =
				BuildTupleHashTable(&node->ss.ps,
									d_desc, &TTSOpsVirtual,
									1, keyColIdx,
									eqfuncoids, hashfunctions,
									colls, 1024,
									0,	/* no per-entry payload needed */
									estate->es_query_cxt,
									state->hash_tuplescxt,
									state->hash_tempcxt,
									false);
			pfree(eqOps);
		}

		i++;
	}

	/*
	 * Forward the WHERE clauses both as zone-map / qual-key pushdown to the
	 * AM (cheap row-group skipping) and as a residual ExprState for any
	 * clauses the qual-key extractor couldn't translate.
	 */
	if (baseclauses != NIL)
	{
		List	   *vars;

		nqual_keys = cs_extract_scan_qual_keys(baseclauses,
											   cscan->scan.scanrelid,
											   &qual_keys);

		/*
		 * Mark every column referenced by the WHERE quals as needed so
		 * scan_set_projection lets the AM materialize them for the residual
		 * ExprState evaluation below.
		 */
		vars = pull_var_clause((Node *) baseclauses, 0);
		foreach(lc, vars)
		{
			Var		   *v = (Var *) lfirst(lc);

			if (v->varno == cscan->scan.scanrelid && v->varattno > 0)
				needed_cols = bms_add_member(needed_cols, v->varattno - 1);
		}

		/*
		 * ExecPrepareQual compiles with parent=NULL, so the
		 * EEOP_SCAN_FETCHSOME setup step doesn't try to optimize against our
		 * CustomScanState's scandesc (which describes the agg output
		 * tupledesc, not the columnstore relation tupledesc that the Vars in
		 * baseclauses reference).  At runtime ExecQual reads column values
		 * via slot_getsomeattrs on whatever slot we put in
		 * econtext->ecxt_scantuple -- that's probe_slot.
		 */
		state->qual_state = ExecPrepareQual(baseclauses, estate);
	}

	state->needed_cols = needed_cols;
	state->qual_keys = qual_keys;
	state->nqual_keys = nqual_keys;

	state->probe_slot = MakeSingleTupleTableSlot(RelationGetDescr(rel),
												 &TTSOpsColumnStore);
	state->emitted = false;
	state->exhausted = false;

	/*
	 * If any output positions are GROUP_KEY, set up a TupleHashTable keyed on
	 * those columns.  Per-entry additional area carries CSAggGroupState with
	 * one CSAggOne per output position (group keys are unused there but the
	 * array indices line up with state->aggs for simplicity).
	 */
	state->ngrpkeys = 0;
	for (i = 0; i < state->nagg; i++)
		if (state->aggs[i].kind == CS_AGG_GROUP_KEY)
			state->ngrpkeys++;

	if (state->ngrpkeys > 0)
	{
		TupleDesc	grpdesc;
		AttrNumber *keyColIdx;
		Oid		   *eqfuncoids;
		Oid		   *collations;
		Oid		   *eqOps;
		FmgrInfo   *hashfunctions;
		int			k = 0;

		state->grpkey_outpos = palloc(sizeof(int) * state->ngrpkeys);
		state->grpkey_attnos = palloc(sizeof(AttrNumber) * state->ngrpkeys);
		grpdesc = CreateTemplateTupleDesc(state->ngrpkeys);
		keyColIdx = palloc(sizeof(AttrNumber) * state->ngrpkeys);
		collations = palloc(sizeof(Oid) * state->ngrpkeys);

		for (i = 0; i < state->nagg; i++)
		{
			CSAggOne   *a = &state->aggs[i];
			Form_pg_attribute relatt;

			if (a->kind != CS_AGG_GROUP_KEY)
				continue;

			state->grpkey_outpos[k] = i;
			state->grpkey_attnos[k] = a->col_attno;
			relatt = TupleDescAttr(RelationGetDescr(rel), a->col_attno - 1);
			TupleDescInitEntry(grpdesc, (AttrNumber) (k + 1),
							   NameStr(relatt->attname),
							   relatt->atttypid, relatt->atttypmod, 0);
			TupleDescInitEntryCollation(grpdesc, (AttrNumber) (k + 1),
										relatt->attcollation);
			keyColIdx[k] = (AttrNumber) (k + 1);
			collations[k] = relatt->attcollation;
			k++;
		}

		/*
		 * Finalize before the desc backs a slot: the grouped emit path
		 * deforms the hash table's minimal tuples through grp_slot's
		 * descriptor (ExecForceStoreMinimalTuple), and heap_deform_tuple
		 * asserts the offset cache was computed.  Matches the other
		 * hand-built descriptors (d_desc, the late-mat probe_desc).
		 */
		TupleDescFinalize(grpdesc);

		state->grp_slot = MakeSingleTupleTableSlot(grpdesc, &TTSOpsVirtual);

		/*
		 * Equality and hash functions per group-key column.  Pull the default
		 * eq operator out of the type cache directly -- avoids the
		 * operator-by-name lookup compatible_oper_opid would do.
		 */
		eqOps = palloc(sizeof(Oid) * state->ngrpkeys);
		for (k = 0; k < state->ngrpkeys; k++)
		{
			Oid			typid = TupleDescAttr(grpdesc, k)->atttypid;
			TypeCacheEntry *tcache;

			tcache = lookup_type_cache(typid, TYPECACHE_EQ_OPR);
			if (!OidIsValid(tcache->eq_opr))
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_FUNCTION),
						 errmsg("type %s has no default equality operator",
								format_type_be(typid))));
			eqOps[k] = tcache->eq_opr;
		}
		execTuplesHashPrepare(state->ngrpkeys, eqOps,
							  &eqfuncoids, &hashfunctions);
		pfree(eqOps);

		cs_agg_ensure_hash_cxts(state, estate);
		state->grouphash = BuildTupleHashTable(&node->ss.ps,
											   grpdesc, &TTSOpsVirtual,
											   state->ngrpkeys,
											   keyColIdx,
											   eqfuncoids,
											   hashfunctions,
											   collations,
											   1024,
											   sizeof(CSAggGroupState),
											   estate->es_query_cxt,
											   state->hash_tuplescxt,
											   state->hash_tempcxt,
											   false);
		state->grp_iter_started = false;
	}

	/*
	 * Open the scan now for non-parallel execution.  For parallel-aware paths
	 * the leader's InitializeDSMCustomScan and each worker's
	 * InitializeWorkerCustomScan call cs_agg_open_scan with a shared
	 * ParallelTableScanDesc.
	 */
	if (!(node->ss.ps.state->es_use_parallel_mode &&
		  node->ss.ps.plan->parallel_aware))
		cs_agg_open_scan(node, estate, NULL);
}

static void
cs_agg_open_scan(CustomScanState *node, EState *estate,
				 ParallelTableScanDesc pscan)
{
	CSAggState *state = (CSAggState *) node;
	Relation	rel = node->ss.ss_currentRelation;

	/* No user flags; see the comment in cs_open_scan(). */
	if (pscan != NULL)
		state->scandesc = table_beginscan_parallel(rel, pscan, SO_NONE);
	else
		state->scandesc = table_beginscan(rel, estate->es_snapshot,
										  0, NULL, SO_NONE);
	state->css.ss.ss_currentScanDesc = state->scandesc;
	if (state->needed_cols != NULL)
		cs_scan_set_projection(state->scandesc, state->needed_cols);
	if (state->nqual_keys > 0)
		cs_scan_set_qual_keys(state->scandesc,
							  state->nqual_keys, state->qual_keys);
}

static Size
cs_agg_estimate_dsm(CustomScanState *node, ParallelContext *pcxt)
{
	Relation	rel = node->ss.ss_currentRelation;
	EState	   *estate = node->ss.ps.state;

	/* see cs_estimate_dsm_custom_scan for why the wrapper is required */
	return table_parallelscan_estimate(rel, estate->es_snapshot);
}

static void
cs_agg_initialize_dsm(CustomScanState *node, ParallelContext *pcxt,
					  void *coordinate)
{
	Relation	rel = node->ss.ss_currentRelation;
	ParallelTableScanDesc pscan = (ParallelTableScanDesc) coordinate;
	EState	   *estate = node->ss.ps.state;

	/* see cs_estimate_dsm_custom_scan for why the wrapper is required */
	table_parallelscan_initialize(rel, pscan, estate->es_snapshot);
	cs_agg_open_scan(node, estate, pscan);
}

static void
cs_agg_reinitialize_dsm(CustomScanState *node, ParallelContext *pcxt,
						void *coordinate)
{
	Relation	rel = node->ss.ss_currentRelation;
	ParallelTableScanDesc pscan = (ParallelTableScanDesc) coordinate;

	table_parallelscan_reinitialize(rel, pscan);
}

static void
cs_agg_initialize_worker(CustomScanState *node, shm_toc *toc, void *coordinate)
{
	ParallelTableScanDesc pscan = (ParallelTableScanDesc) coordinate;

	cs_agg_open_scan(node, node->ss.ps.state, pscan);
}

/*
 * Apply per-row accumulation against a CSAggOne descriptor + slot.
 *
 * `scan` is the columnstore TableScanDesc used to drive `slot`; it is
 * only consulted by the SUM/AVG numeric fast path which needs to query
 * the column's NI64 encoding via cs_scan_get_raw_attr.
 *
 * `econtext` is consulted only when the aggregate has a FILTER clause;
 * in that case the caller is expected to have set ecxt_scantuple = slot
 * (which the WHERE-residual path already does for the same row).
 */
static void
cs_agg_accum_row(CSAggOne *a, TableScanDesc scan, TupleTableSlot *slot,
				 ExprContext *econtext)
{
	Datum		val;
	bool		isnull;

	if (a->kind == CS_AGG_GROUP_KEY ||
		a->kind == CS_AGG_PASSTHROUGH ||
		a->col_attno < 0)
		return;

	/*
	 * Aggregate-level FILTER (WHERE ...) -- skip the row for THIS aggregate
	 * if the filter is false.  The filter's Vars reference the relation's
	 * tupledesc; the caller has already set econtext->ecxt_scantuple to the
	 * probe slot.
	 */
	if (a->filter_state != NULL && econtext != NULL)
	{
		if (!ExecQual(a->filter_state, econtext))
			return;
	}

	/*
	 * COUNT(*) -- a->col_attno == 0 means the aggregate references no column.
	 * Increment unconditionally and bypass slot_getattr, which would do
	 * nothing useful for attno=0 anyway.
	 */
	if (a->kind == CS_AGG_COUNT && a->col_attno == 0)
	{
		a->count++;
		return;
	}

	/*
	 * COUNT(DISTINCT col) -- look up the value in the per-group hash set;
	 * increment count only when isnew=true.  NULL inputs are skipped (SQL
	 * standard: NULL not counted by DISTINCT).
	 */
	if (a->kind == CS_AGG_COUNT_DISTINCT)
	{
		Datum		dv;
		bool		dnull;
		bool		isnew;

		dv = slot_getattr(slot, a->col_attno, &dnull);
		if (dnull)
			return;

		ExecClearTuple(a->distinct_slot);
		a->distinct_slot->tts_values[0] = dv;
		a->distinct_slot->tts_isnull[0] = false;
		ExecStoreVirtualTuple(a->distinct_slot);

		(void) LookupTupleHashEntry(a->distinct_hash, a->distinct_slot,
									&isnew, NULL);
		if (isnew)
			a->count++;
		return;
	}

	if (a->kind == CS_AGG_SUM_NUMERIC || a->kind == CS_AGG_AVG_NUMERIC)
	{
		Datum		raw_val;
		bool		raw_isnull;

		/*
		 * NI64 fast path: when the numeric column is stored as a scaled
		 * int64, accumulate raw int64 values into an int128 sum.  The per-row
		 * int64 -> Numeric conversion (~50ns) is replaced by an int128 +=
		 * int64 operation (~1ns), an order-of-magnitude win on
		 * SUM/AVG-over-numeric workloads where it matters most.
		 */
		if (cs_scan_get_raw_attr(scan, slot, a->col_attno,
								 &raw_val, &raw_isnull))
		{
			Oid			phys_type = InvalidOid;
			int32		dscale = -1;

			/*
			 * The dscale belongs to the current row group -- the encoder
			 * picks it per row group, so an unconstrained numeric column can
			 * change scale as the scan moves between groups.  Refetch it
			 * every time, and rescale whichever of the running sum and the
			 * incoming value has the smaller scale when they differ.
			 */
			(void) cs_scan_column_encoding(scan, a->col_attno,
										   &phys_type, &dscale);
			if (dscale < 0)
				dscale = 0;
			if (!a->ni64_active)
			{
				a->dscale = dscale;
				a->ni64_active = true;
			}
			if (!raw_isnull)
			{
				int128		v = (int128) DatumGetInt64(raw_val);

				if (unlikely(dscale != a->dscale))
				{
					if (dscale > a->dscale)
					{
						/*
						 * Scale the running sum up to the incoming chunk's
						 * dscale.  a->dscale is advanced in step with each
						 * multiplication so that a mid-loop flush converts
						 * the sum at its true scale, not the original one.
						 */
						while (a->dscale < dscale)
						{
							if (a->sumX > CS_NI64_RESCALE_LIMIT ||
								a->sumX < -CS_NI64_RESCALE_LIMIT)
							{
								cs_agg_ni64_flush(a);
								/* the sum is zero now; it scales freely */
								a->dscale = dscale;
								break;
							}
							a->sumX *= 10;
							a->dscale++;
						}
					}
					else
					{
						int32		vscale = dscale;

						while (vscale < a->dscale)
						{
							if (v > CS_NI64_RESCALE_LIMIT ||
								v < -CS_NI64_RESCALE_LIMIT)
								break;
							v *= 10;
							vscale++;
						}
						if (vscale < a->dscale)
						{
							/*
							 * The incoming value cannot reach the
							 * accumulator's scale in int128.  Add it through
							 * the Numeric accumulator at the scale it is
							 * actually at; the emit path merges both sides.
							 */
							cs_agg_num_acc_add(a,
											   cs_int128_to_numeric(v,
																	vscale));
							a->saw_value = true;
							if (a->kind == CS_AGG_AVG_NUMERIC)
								a->count++;
							return;
						}
					}
				}

				/*
				 * Guard the addition too: after a rescale either side can sit
				 * just past the limit (one multiply beyond it), and a run of
				 * such rows would overflow int128 within a few additions.
				 * Flushing first keeps every sum reachable here below ~2^126.
				 */
				if (a->sumX > CS_NI64_RESCALE_LIMIT ||
					a->sumX < -CS_NI64_RESCALE_LIMIT ||
					v > CS_NI64_RESCALE_LIMIT ||
					v < -CS_NI64_RESCALE_LIMIT)
					cs_agg_ni64_flush(a);
				a->sumX += v;
				a->saw_value = true;
				if (a->kind == CS_AGG_AVG_NUMERIC)
					a->count++;
			}
			return;
		}
		/* Fall through to the generic numeric_add path below. */
	}

	val = slot_getattr(slot, a->col_attno, &isnull);
	if (isnull && a->count_skips_null)
		return;

	switch (a->kind)
	{
		case CS_AGG_COUNT:
			if (!isnull)
				a->count++;
			break;
		case CS_AGG_SUM_NUMERIC:
		case CS_AGG_AVG_NUMERIC:
			if (!isnull)
			{
				cs_agg_num_acc_add(a, DatumGetNumericCopy(val));
				a->saw_value = true;
				if (a->kind == CS_AGG_AVG_NUMERIC)
					a->count++;
			}
			break;
		case CS_AGG_SUM_INT:
		case CS_AGG_SUM_INT8:
		case CS_AGG_AVG_INT:
			if (!isnull)
			{
				int64		ival;

				switch (a->col_type)
				{
					case INT2OID:
						ival = (int64) DatumGetInt16(val);
						break;
					case INT4OID:
						ival = (int64) DatumGetInt32(val);
						break;
					case INT8OID:
						ival = DatumGetInt64(val);
						break;
					default:
						ival = 0;
						break;
				}
				a->sumX += (int128) ival;
				a->saw_value = true;

				/*
				 * count is the partial-aggregate N: it must be tracked for
				 * AVG (we divide by it in the final emit) and for SUM_INT8
				 * (the upstream numeric_poly_sum / int8_avg_combine relies on
				 * N > 0 to distinguish "no rows" from "all-NULL rows").
				 */
				if (a->kind == CS_AGG_AVG_INT || a->kind == CS_AGG_SUM_INT8)
					a->count++;
			}
			break;
		case CS_AGG_MIN:
		case CS_AGG_MAX:
			if (isnull)
				break;
			if (a->first_row)
			{
				a->minmax = datumCopy(val, a->col_typbyval, a->col_typlen);
				a->minmax_isnull = false;
				a->first_row = false;
			}
			else
			{
				int32		cmp_int;
				bool		replace;

				cmp_int = DatumGetInt32(FunctionCall2Coll(a->cmp_finfo,
														  a->col_collation,
														  val, a->minmax));
				replace = (a->kind == CS_AGG_MIN)
					? cmp_int < 0 : cmp_int > 0;
				if (replace)
				{
					if (!a->col_typbyval && DatumGetPointer(a->minmax) != NULL)
						pfree(DatumGetPointer(a->minmax));
					a->minmax = datumCopy(val, a->col_typbyval, a->col_typlen);
				}
			}
			break;
		default:
			break;
	}
}

/*
 * Build the bytea that PG's int8_avg_serialize would have produced for an
 * Int128AggState with the given N and sumX.  This is the partial transition
 * value expected by the deserialize/combine functions of SUM(int8) and
 * AVG(int8): both aggregates declare aggtranstype = INTERNAL with
 * aggserialfn = int8_avg_serialize, so producing this bytea directly lets a
 * Finalize Aggregate above us complete the aggregation.
 */
static bytea *
cs_int128_aggstate_serialize(int64 N, int128 sumX)
{
	StringInfoData buf;

	pq_begintypsend(&buf);
	pq_sendint64(&buf, N);
	pq_sendint64(&buf, PG_INT128_HI_INT64(sumX));
	pq_sendint64(&buf, (int64) PG_INT128_LO_UINT64(sumX));
	return pq_endtypsend(&buf);
}

/*
 * Build a 2-element bigint[] {count, sum} that matches Int8TransTypeData,
 * the partial transition value expected by AVG(int2)/AVG(int4) (aggtranstype
 * = bigint[], no aggserialfn).  Their aggcombinefn = int4_avg_combine reads
 * the array and validates ARR_SIZE strictly, so we emit the canonical 1-D
 * non-null INT8OID array shape.
 */
static ArrayType *
cs_int8_transtype_array(int64 count, int64 sum)
{
	Datum		elems[2];

	elems[0] = Int64GetDatum(count);
	elems[1] = Int64GetDatum(sum);
	return construct_array_builtin(elems, 2, INT8OID);
}

/*
 * Materialize one CSAggOne's output value into a slot position.  When
 * partial=true and the kind is INTERNAL-stated (CS_AGG_AVG_INT,
 * CS_AGG_SUM_INT8), emit the serialized partial transition state (bytea)
 * for an upstream Finalize Aggregate.  Otherwise emit the final value.
 */
static void
cs_agg_emit_one(CSAggOne *a, TupleTableSlot *out, int outpos, bool partial)
{
	switch (a->kind)
	{
		case CS_AGG_COUNT:
		case CS_AGG_COUNT_DISTINCT:
			out->tts_values[outpos] = Int64GetDatum(a->count);
			out->tts_isnull[outpos] = false;
			break;
		case CS_AGG_SUM_NUMERIC:
			if (a->saw_value)
			{
				Numeric		sum;

				if (a->ni64_active && a->num_acc_valid)
				{
					Numeric		ni_part = cs_int128_to_numeric(a->sumX,
															   a->dscale);
					Datum		merged = DirectFunctionCall2(numeric_add,
															 NumericGetDatum(a->num_acc),
															 NumericGetDatum(ni_part));

					sum = DatumGetNumericCopy(merged);
				}
				else if (a->ni64_active)
					sum = cs_int128_to_numeric(a->sumX, a->dscale);
				else
					sum = a->num_acc;

				out->tts_values[outpos] = NumericGetDatum(sum);
				out->tts_isnull[outpos] = false;
			}
			else
				out->tts_isnull[outpos] = true;
			break;
		case CS_AGG_AVG_NUMERIC:
			if (a->saw_value && a->count > 0)
			{
				Numeric		sum;
				Datum		count_d;
				Datum		avg;

				if (a->ni64_active && a->num_acc_valid)
				{
					Numeric		ni_part = cs_int128_to_numeric(a->sumX,
															   a->dscale);
					Datum		merged = DirectFunctionCall2(numeric_add,
															 NumericGetDatum(a->num_acc),
															 NumericGetDatum(ni_part));

					sum = DatumGetNumericCopy(merged);
				}
				else if (a->ni64_active)
					sum = cs_int128_to_numeric(a->sumX, a->dscale);
				else
					sum = a->num_acc;

				count_d = DirectFunctionCall1(int8_numeric,
											  Int64GetDatum(a->count));
				avg = DirectFunctionCall2(numeric_div,
										  NumericGetDatum(sum), count_d);

				out->tts_values[outpos] = avg;
				out->tts_isnull[outpos] = false;
			}
			else
				out->tts_isnull[outpos] = true;
			break;
		case CS_AGG_SUM_INT:
			if (a->saw_value)
			{
				/*
				 * SUM(int2)/SUM(int4) returns int8.  Range-check the int128
				 * accumulator to match heap behaviour: PG's int4_sum throws
				 * "bigint out of range" on overflow.
				 */
				if (a->sumX > (int128) PG_INT64_MAX ||
					a->sumX < (int128) PG_INT64_MIN)
					ereport(ERROR,
							(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
							 errmsg("bigint out of range")));
				out->tts_values[outpos] = Int64GetDatum((int64) a->sumX);
				out->tts_isnull[outpos] = false;
			}
			else
				out->tts_isnull[outpos] = true;
			break;
		case CS_AGG_SUM_INT8:
			if (partial)
			{
				/*
				 * SUM(int8): aggtransfn=int8_avg_accum (Int128AggState),
				 * aggserialfn=int8_avg_serialize.  Even when we never saw a
				 * value, partial output must still match the serialized empty
				 * state so combine + finalfn produce NULL upstream.
				 */
				bytea	   *b = cs_int128_aggstate_serialize(a->saw_value ? a->count : 0,
															 a->saw_value ? a->sumX : (int128) 0);

				out->tts_values[outpos] = PointerGetDatum(b);
				out->tts_isnull[outpos] = false;
			}
			else if (a->saw_value)
			{
				out->tts_values[outpos] = NumericGetDatum(
														  cs_int128_to_numeric(a->sumX, 0));
				out->tts_isnull[outpos] = false;
			}
			else
				out->tts_isnull[outpos] = true;
			break;
		case CS_AGG_AVG_INT:
			if (partial)
			{
				int64		N = a->saw_value ? a->count : 0;
				int128		sumX = a->saw_value ? a->sumX : (int128) 0;

				/*
				 * AVG output type differs by input type: AVG(int8):
				 * aggtranstype = INTERNAL (Int128AggState) with aggserialfn =
				 * int8_avg_serialize -> bytea. AVG(int2|int4): aggtranstype =
				 * bigint[] (Int8TransTypeData packed as a 2-element int8
				 * array, count + sum), no serialize step.  The upstream
				 * int4_avg_combine validates ARR_SIZE strictly, so we must
				 * emit the canonical 1-D non-null array shape.
				 */
				if (a->col_type == INT8OID)
				{
					bytea	   *b = cs_int128_aggstate_serialize(N, sumX);

					out->tts_values[outpos] = PointerGetDatum(b);
				}
				else
				{
					ArrayType  *arr = cs_int8_transtype_array(N, (int64) sumX);

					out->tts_values[outpos] = PointerGetDatum(arr);
				}
				out->tts_isnull[outpos] = false;
			}
			else if (a->saw_value && a->count > 0)
			{
				Numeric		sum = cs_int128_to_numeric(a->sumX, 0);
				Datum		count_d = DirectFunctionCall1(int8_numeric,
														  Int64GetDatum(a->count));
				Datum		avg = DirectFunctionCall2(numeric_div,
													  NumericGetDatum(sum),
													  count_d);

				out->tts_values[outpos] = avg;
				out->tts_isnull[outpos] = false;
			}
			else
				out->tts_isnull[outpos] = true;
			break;
		case CS_AGG_MIN:
		case CS_AGG_MAX:
			if (a->first_row)
				out->tts_isnull[outpos] = true;
			else
			{
				out->tts_values[outpos] = a->minmax;
				out->tts_isnull[outpos] = false;
			}
			break;
		default:
			out->tts_isnull[outpos] = true;
			break;
	}
}

static TupleTableSlot *
cs_agg_exec(CustomScanState *node)
{
	CSAggState *state = (CSAggState *) node;
	TupleTableSlot *scan_slot = node->ss.ss_ScanTupleSlot;
	int			i;

	/*
	 * GROUP BY path: drain the input and bucket per group.  After scan
	 * completes, iterate the hash table emitting one row per group.
	 */
	if (state->grouphash != NULL)
	{
		ExprContext *econtext = node->ss.ps.ps_ExprContext;

		if (!state->exhausted)
		{
			while (true)
			{
				TupleTableSlot *slot = state->probe_slot;
				TupleHashEntry entry;
				CSAggGroupState *gstate;
				bool		isnew;
				int			k;

				if (!table_scan_getnextslot(state->scandesc,
											ForwardScanDirection, slot))
				{
					state->exhausted = true;
					break;
				}
				if (state->qual_state != NULL)
				{
					econtext->ecxt_scantuple = slot;
					if (!ExecQual(state->qual_state, econtext))
						continue;
				}

				/*
				 * Build the lookup tuple: copy group-key columns into
				 * grp_slot.
				 */
				ExecClearTuple(state->grp_slot);
				for (k = 0; k < state->ngrpkeys; k++)
				{
					bool		isnull;
					Datum		v = slot_getattr(slot,
												 state->grpkey_attnos[k],
												 &isnull);

					state->grp_slot->tts_values[k] = v;
					state->grp_slot->tts_isnull[k] = isnull;
				}
				ExecStoreVirtualTuple(state->grp_slot);

				entry = LookupTupleHashEntry(state->grouphash,
											 state->grp_slot,
											 &isnew, NULL);
				gstate = (CSAggGroupState *) TupleHashEntryGetAdditional(state->grouphash, entry);

				if (isnew)
				{
					/* Initialize per-group accumulators from the template. */
					gstate->aggs = palloc(sizeof(CSAggOne) * state->nagg);
					for (i = 0; i < state->nagg; i++)
					{
						AttrNumber	d_keyColIdx[1] = {1};

						gstate->aggs[i] = state->aggs[i];
						gstate->aggs[i].count = 0;
						gstate->aggs[i].num_acc_valid = false;
						gstate->aggs[i].sumX = 0;
						gstate->aggs[i].ni64_active = false;
						gstate->aggs[i].saw_value = false;
						gstate->aggs[i].first_row = true;
						gstate->aggs[i].minmax_isnull = true;

						/*
						 * COUNT(DISTINCT col): each group gets its own hash
						 * set.  Reuse the desc/eq/hash funcs from the
						 * template (set up once in cs_agg_begin); a smaller
						 * initial bucket count is fine -- many groups means
						 * the dedup set per group is small.
						 */
						if (state->aggs[i].kind == CS_AGG_COUNT_DISTINCT)
						{
							gstate->aggs[i].distinct_hash =
								BuildTupleHashTable(&node->ss.ps,
													state->aggs[i].distinct_desc,
													&TTSOpsVirtual,
													1, d_keyColIdx,
													state->aggs[i].distinct_eqfn,
													state->aggs[i].distinct_hashfn,
													state->aggs[i].distinct_coll,
													64, 0,
													node->ss.ps.state->es_query_cxt,
													state->hash_tuplescxt,
													state->hash_tempcxt,
													false);
						}
					}
				}

				/*
				 * Set ecxt_scantuple so per-aggregate FILTER ExprStates can
				 * fetch column values from probe_slot.
				 */
				econtext->ecxt_scantuple = slot;
				for (i = 0; i < state->nagg; i++)
					cs_agg_accum_row(&gstate->aggs[i],
									 state->scandesc, slot, econtext);

				/* Discard per-lookup scratch from this row's hash probes. */
				MemoryContextReset(state->hash_tempcxt);
			}
		}

		/* Iterate hash table to emit results. */
		if (!state->grp_iter_started)
		{
			InitTupleHashIterator(state->grouphash, &state->grp_iter);
			state->grp_iter_started = true;
		}
		while (true)
		{
			TupleHashEntry entry = ScanTupleHashTable(state->grouphash,
													  &state->grp_iter);
			MinimalTuple mtup;
			CSAggGroupState *gstate;

			if (entry == NULL)
				return NULL;

			mtup = TupleHashEntryGetTuple(entry);
			gstate = (CSAggGroupState *) TupleHashEntryGetAdditional(state->grouphash, entry);

			ExecClearTuple(scan_slot);

			/*
			 * grp_slot is a TTSOpsVirtual slot, so use the force variant to
			 * materialize the hash entry's minimal tuple's values into
			 * tts_values/isnull.
			 */
			ExecForceStoreMinimalTuple(mtup, state->grp_slot, false);
			slot_getallattrs(state->grp_slot);

			for (i = 0; i < state->nagg; i++)
			{
				CSAggOne   *a = &state->aggs[i];

				if (a->kind == CS_AGG_GROUP_KEY)
				{
					/* Find this output position in grpkey_outpos. */
					int			k;

					for (k = 0; k < state->ngrpkeys; k++)
					{
						if (state->grpkey_outpos[k] == i)
						{
							scan_slot->tts_values[i] = state->grp_slot->tts_values[k];
							scan_slot->tts_isnull[i] = state->grp_slot->tts_isnull[k];
							break;
						}
					}
				}
				else
				{
					cs_agg_emit_one(&gstate->aggs[i], scan_slot, i,
									state->is_partial);
				}
			}
			ExecStoreVirtualTuple(scan_slot);

			/*
			 * Apply HAVING.  setrefs.c rewrote any Aggref / group-Var
			 * references in plan->qual against custom_scan_tlist with
			 * INDEX_VAR, so ExecQual reads slot positions from scan_slot
			 * directly without re-evaluating aggregates.  Groups that fail
			 * are skipped and the next hash entry is fetched.
			 */
			if (node->ss.ps.qual != NULL)
			{
				econtext->ecxt_scantuple = scan_slot;
				if (!ExecQual(node->ss.ps.qual, econtext))
					continue;
			}

			if (node->ss.ps.ps_ProjInfo != NULL)
			{
				econtext->ecxt_scantuple = scan_slot;
				return ExecProject(node->ss.ps.ps_ProjInfo);
			}
			return scan_slot;
		}
	}

	if (state->emitted)
		return NULL;

	/* Drain the underlying scan, accumulating per-aggregate state. */
	while (!state->exhausted)
	{
		TupleTableSlot *slot = state->probe_slot;
		ExprContext *econtext = node->ss.ps.ps_ExprContext;

		if (!table_scan_getnextslot(state->scandesc, ForwardScanDirection,
									slot))
		{
			state->exhausted = true;
			break;
		}

		/* Apply residual WHERE clauses; skip non-matching rows. */
		if (state->qual_state != NULL)
		{
			econtext->ecxt_scantuple = slot;
			if (!ExecQual(state->qual_state, econtext))
				continue;
		}

		econtext->ecxt_scantuple = slot;
		for (i = 0; i < state->nagg; i++)
			cs_agg_accum_row(&state->aggs[i], state->scandesc, slot,
							 econtext);

		/*
		 * Discard per-lookup scratch from this row's COUNT(DISTINCT) hash
		 * probes; NULL when no such aggregate is present.
		 */
		if (state->hash_tempcxt != NULL)
			MemoryContextReset(state->hash_tempcxt);
	}

	/*
	 * Build the single output tuple in ss_ScanTupleSlot.  The slot's
	 * tupledesc has one attribute per custom_scan_tlist entry, in order. The
	 * plan-tlist Vars (varno=INDEX_VAR) reference those attributes, and the
	 * executor's standard projection (set up by ExecInitCustomScan because
	 * plan tlist != custom_scan_tlist) maps from the scan slot to the result
	 * slot.
	 */
	ExecClearTuple(scan_slot);
	for (i = 0; i < state->nagg; i++)
	{
		CSAggOne   *a = &state->aggs[i];

		if (a->col_attno < 0)
		{
			scan_slot->tts_isnull[i] = true;
			continue;
		}
		cs_agg_emit_one(a, scan_slot, i, state->is_partial);
	}
	ExecStoreVirtualTuple(scan_slot);

	state->emitted = true;

	/* HAVING applies even in the ungrouped case (e.g., HAVING count(*) > 0). */
	if (node->ss.ps.qual != NULL)
	{
		ExprContext *econtext = node->ss.ps.ps_ExprContext;

		econtext->ecxt_scantuple = scan_slot;
		if (!ExecQual(node->ss.ps.qual, econtext))
			return NULL;
	}

	/*
	 * If the executor set up a projection, run it; otherwise return scan
	 * slot.
	 */
	if (node->ss.ps.ps_ProjInfo != NULL)
	{
		ExprContext *econtext = node->ss.ps.ps_ExprContext;

		econtext->ecxt_scantuple = scan_slot;
		return ExecProject(node->ss.ps.ps_ProjInfo);
	}
	return scan_slot;
}

static void
cs_agg_end(CustomScanState *node)
{
	CSAggState *state = (CSAggState *) node;

	if (state->scandesc != NULL)
	{
		table_endscan(state->scandesc);
		state->scandesc = NULL;
	}
	if (state->probe_slot != NULL)
	{
		ExecDropSingleTupleTableSlot(state->probe_slot);
		state->probe_slot = NULL;
	}
}

static void
cs_agg_rescan(CustomScanState *node)
{
	CSAggState *state = (CSAggState *) node;

	if (state->scandesc != NULL)
		table_rescan(state->scandesc, NULL);
	state->emitted = false;
	state->exhausted = false;
	for (int i = 0; i < state->nagg; i++)
	{
		state->aggs[i].count = 0;
		state->aggs[i].num_acc_valid = false;
		state->aggs[i].sumX = 0;
		state->aggs[i].ni64_active = false;
		state->aggs[i].saw_value = false;
		state->aggs[i].first_row = true;
		state->aggs[i].minmax_isnull = true;

		/* DISTINCT sets must restart with the new parameter values */
		if (state->aggs[i].distinct_hash != NULL)
			ResetTupleHashTable(state->aggs[i].distinct_hash);
	}

	/*
	 * Grouped state: the hash of groups and the emit iterator belong to the
	 * previous execution; correlated rescans would otherwise emit stale
	 * groups (or nothing, with the iterator already exhausted).
	 */
	state->grp_iter_started = false;
	if (state->grouphash != NULL)
		ResetTupleHashTable(state->grouphash);
	if (state->hash_tempcxt != NULL)
		MemoryContextReset(state->hash_tempcxt);
}

/*
 * Display labels for EXPLAIN output, indexed by CSAggKind.
 * NULL entries (e.g. CS_AGG_PASSTHROUGH) cause the explain loop to skip
 * the aggregate descriptor.
 */
static const char *const cs_agg_kind_names[] = {
	[CS_AGG_COUNT] = "count",
	[CS_AGG_COUNT_DISTINCT] = "count_distinct",
	[CS_AGG_SUM_NUMERIC] = "sum_numeric",
	[CS_AGG_AVG_NUMERIC] = "avg_numeric",
	[CS_AGG_SUM_INT] = "sum_int",
	[CS_AGG_SUM_INT8] = "sum_int8",
	[CS_AGG_AVG_INT] = "avg_int",
	[CS_AGG_MIN] = "min",
	[CS_AGG_MAX] = "max",
	[CS_AGG_GROUP_KEY] = "group_by",
	[CS_AGG_PASSTHROUGH] = NULL,
};

static void
cs_agg_explain(CustomScanState *node, List *ancestors, ExplainState *es)
{
	CSAggState *state = (CSAggState *) node;
	StringInfoData buf;
	bool		first = true;

	initStringInfo(&buf);
	for (int i = 0; i < state->nagg; i++)
	{
		const char *kind;

		if (state->aggs[i].col_attno < 0)
			continue;
		if (state->aggs[i].kind >= lengthof(cs_agg_kind_names))
			continue;
		kind = cs_agg_kind_names[state->aggs[i].kind];
		if (kind == NULL)
			continue;
		if (!first)
			appendStringInfoString(&buf, ", ");
		appendStringInfo(&buf, "%s(att=%d)", kind, state->aggs[i].col_attno);
		first = false;
	}
	if (buf.len > 0)
		ExplainPropertyText("Columnstore Aggregates", buf.data, es);
	pfree(buf.data);
}

/* ========================================================================
 * Late materialization for ORDER BY + LIMIT (the "ColumnstoreLateMat"
 * CustomScan).
 *
 * For a query of the shape
 *
 *     SELECT cols... FROM cs_table
 *     ORDER BY sort_col [, ...] LIMIT N
 *
 * the standard plan materializes EVERY column for EVERY row before
 * sorting, then keeps only N rows.  The late-mat plan emits only
 * (sort_col_values, ItemPointer) into a top-N tuplesort, then for each
 * of the N surviving tuples refetches the projected columns by TID.
 * Wide tables with selective LIMIT see large speedups: the heavy column
 * decompression cost is paid for only N rows, not for the whole table.
 *
 * Implementation: tuplesort_begin_heap with bound = LIMIT keeps a
 * top-N min-heap.  After scan completes, we drain the sorted output and
 * for each (sort_col..., tid) tuple we call table_tuple_fetch_row_version
 * to materialize the full row, then ExecProject to deliver the
 * caller's targetlist.
 * ========================================================================
 */

#include "access/heapam.h"
#include "access/sysattr.h"
#include "miscadmin.h"
#include "utils/guc.h"
#include "utils/tuplesort.h"


typedef struct CSLatemat
{
	int			scanrelid;		/* base rel rti */
	int			n_sort;			/* number of ORDER BY keys */
	AttrNumber *sort_attnos;	/* input rel attnos for each sort key */
	Oid		   *sort_collations;
	Oid		   *sort_operators;
	bool	   *sort_nulls_first;
	int64		limit_n;
} CSLatemat;

typedef struct CSLatematState
{
	CustomScanState css;		/* must be first */
	TableScanDesc scandesc;
	Relation	rel;
	CSLatemat	plan_info;
	Tuplesortstate *sortstate;
	TupleDesc	probe_desc;		/* narrow desc: sort_keys + tid */
	TupleTableSlot *probe_slot; /* slot pushed into tuplesort */
	TupleTableSlot *sort_slot;	/* slot tuplesort hands back */
	TupleTableSlot *scan_slot;	/* slot for raw scan output (full row) */
	ExprState  *qual_state;		/* per-row residual filter, or NULL */
	int64		emitted;
	bool		populated;
} CSLatematState;

static void
cs_try_add_latemat_path(PlannerInfo *root, RelOptInfo *input_rel,
						RelOptInfo *output_rel)
{
	Query	   *parse = root->parse;
	RangeTblEntry *rte;
	List	   *tlist;
	CustomPath *cpath;
	CSLatemat  *info;
	Path	   *cheapest;
	Const	   *limit_const;
	List	   *baseclauses;
	ListCell   *lc2;
	int			n_sort;
	int			i;
	ListCell   *lc;
	int64		limit_n;

	/* Need ORDER BY + LIMIT.  No GROUP BY / aggregation / windows here. */
	if (parse->sortClause == NIL || parse->limitCount == NULL)
		return;
	if (parse->groupClause != NIL || parse->groupingSets != NIL ||
		parse->hasAggs || parse->hasWindowFuncs || parse->hasTargetSRFs ||
		parse->havingQual != NULL)
		return;
	if (parse->limitOption != LIMIT_OPTION_COUNT)
		return;
	if (!IsA(parse->limitCount, Const))
		return;
	limit_const = (Const *) parse->limitCount;
	if (limit_const->constisnull || limit_const->consttype != INT8OID)
		return;
	limit_n = DatumGetInt64(limit_const->constvalue);
	if (limit_n <= 0 || limit_n > 100000)
		return;					/* sanity guard */
	if (parse->limitOffset != NULL)
		return;					/* don't bother for now */
	if (parse->rowMarks != NIL)
		return;					/* FOR UPDATE et al. need real ctids */

	if (!cs_upper_input_is_columnstore_rel(root, input_rel, &rte))
		return;

	cheapest = input_rel->cheapest_total_path;
	if (cheapest == NULL)
		return;

	/* Every ORDER BY key must be a Var on the input rel. */
	tlist = parse->targetList;
	n_sort = list_length(parse->sortClause);
	info = palloc(sizeof(CSLatemat));
	info->scanrelid = input_rel->relid;
	info->n_sort = n_sort;
	info->sort_attnos = palloc(sizeof(AttrNumber) * n_sort);
	info->sort_collations = palloc(sizeof(Oid) * n_sort);
	info->sort_operators = palloc(sizeof(Oid) * n_sort);
	info->sort_nulls_first = palloc(sizeof(bool) * n_sort);
	info->limit_n = limit_n;

	i = 0;
	foreach(lc, parse->sortClause)
	{
		SortGroupClause *sgc = lfirst_node(SortGroupClause, lc);
		TargetEntry *tle = get_sortgroupclause_tle(sgc, tlist);
		Var		   *v;

		if (!IsA(tle->expr, Var))
			return;
		v = (Var *) tle->expr;
		if (v->varno != input_rel->relid)
			return;

		/* only user columns: no system attributes or whole-row refs */
		if (v->varattno <= 0)
			return;

		info->sort_attnos[i] = v->varattno;
		info->sort_collations[i] = exprCollation((Node *) v);
		info->sort_operators[i] = sgc->sortop;
		info->sort_nulls_first[i] = sgc->nulls_first;
		i++;
	}

	cpath = makeNode(CustomPath);
	cpath->path.pathtype = T_CustomScan;
	cpath->path.parent = output_rel;

	/*
	 * At UPPERREL_ORDERED-stage hook time, output_rel->reltarget is still
	 * empty; the actual target shape lives in root->upper_targets[stage]. Use
	 * that so PlanCustomPath gets a tlist derived from the right pathtarget.
	 */
	cpath->path.pathtarget = root->upper_targets[UPPERREL_ORDERED];
	if (cpath->path.pathtarget == NULL)
		cpath->path.pathtarget = output_rel->reltarget;
	cpath->path.parallel_aware = false;
	cpath->path.parallel_safe = false;
	cpath->path.parallel_workers = 0;
	cpath->path.rows = limit_n;
	cpath->path.startup_cost = cheapest->startup_cost;

	/*
	 * Late-mat avoids materializing payload columns for all but limit_n rows,
	 * saving O(N_table * (width - sort_cols)) of column decompression and
	 * projection work.  The standard Sort+Limit path doesn't model that
	 * AM-internal cost so by the planner's view both paths look similar. Cost
	 * ours roughly at the input scan's cost reduced by the per-tuple work we
	 * save outside the top N (one cpu_tuple_cost per non-top-N tuple).
	 */
	cpath->path.total_cost = cheapest->total_cost
		- cpu_tuple_cost * Max(cheapest->rows - limit_n, 0);
	/* never let the discount go below the startup cost */
	if (cpath->path.total_cost < cpath->path.startup_cost)
		cpath->path.total_cost = cpath->path.startup_cost;
	cpath->path.pathkeys = NIL; /* output is already in sort order */
	cpath->custom_paths = NIL;

	/*
	 * Forward the WHERE clauses to exec.  They are evaluated against the
	 * projected probe slot (sort keys + tid) before pushing into the bounded
	 * tuplesort, so the LIMIT survives only rows that pass the qual.  The
	 * corresponding qual-key extraction also lets the AM prune row groups via
	 * zone maps cheaply.
	 *
	 * We stash them in custom_private to bypass setrefs.c, which would try to
	 * rewrite Var references against the (empty) custom_scan_tlist; the
	 * late-mat probe slot is shaped from the relation's tupledesc instead.
	 */
	baseclauses = NIL;
	foreach(lc2, input_rel->baserestrictinfo)
	{
		RestrictInfo *ri = lfirst_node(RestrictInfo, lc2);

		/*
		 * SubPlans cannot be compiled in the executor-side
		 * ExecPrepareQual(parent = NULL); leave such queries to the stock
		 * plan.
		 */
		if (contain_subplans((Node *) ri->clause) ||
			cs_contains_param((Node *) ri->clause))
			return;

		baseclauses = lappend(baseclauses, ri->clause);
	}
	cpath->custom_private = list_make2(info, baseclauses);
	cpath->methods = &cs_lm_path_methods;
	cpath->flags = 0;

	add_path(output_rel, &cpath->path);
}

static Plan *
cs_lm_path_to_plan(PlannerInfo *root, RelOptInfo *rel,
				   struct CustomPath *best_path, List *tlist,
				   List *clauses, List *custom_plans)
{
	CustomScan *cscan = makeNode(CustomScan);
	CSLatemat  *info = (CSLatemat *) linitial(best_path->custom_private);
	List	   *baseclauses = (list_length(best_path->custom_private) > 1
							   ? (List *) lsecond(best_path->custom_private)
							   : NIL);
	List	   *attnos = NIL;
	List	   *colls = NIL;
	List	   *ops = NIL;
	List	   *nfs = NIL;
	List	   *priv;
	int			i;

	cscan->scan.plan.targetlist = tlist;
	cscan->scan.plan.qual = NIL;
	cscan->scan.plan.lefttree = NULL;
	cscan->scan.plan.righttree = NULL;
	cscan->scan.plan.parallel_aware = false;
	cscan->scan.plan.parallel_safe = false;
	cscan->scan.scanrelid = info->scanrelid;

	cscan->flags = best_path->flags;
	cscan->custom_plans = NIL;
	cscan->custom_exprs = NIL;
	for (i = 0; i < info->n_sort; i++)
	{
		attnos = lappend_int(attnos, info->sort_attnos[i]);
		colls = lappend_oid(colls, info->sort_collations[i]);
		ops = lappend_oid(ops, info->sort_operators[i]);
		nfs = lappend_int(nfs, info->sort_nulls_first[i] ? 1 : 0);
	}
	priv = list_make4(attnos, colls, ops, nfs);
	priv = lappend(priv, makeInteger((int) info->limit_n));
	priv = lappend(priv, baseclauses);
	cscan->custom_private = priv;
	cscan->custom_scan_tlist = NIL;
	cscan->custom_relids = bms_make_singleton(info->scanrelid);
	cscan->methods = &cs_lm_scan_methods;

	return (Plan *) cscan;
}

static Node *
cs_lm_create_state(CustomScan *cscan)
{
	CSLatematState *state = palloc0(sizeof(CSLatematState));

	NodeSetTag(state, T_CustomScanState);
	state->css.methods = &cs_lm_exec_methods;
	/* Output is a virtual tuple synthesized from sort_slot's values. */
	state->css.slotOps = &TTSOpsVirtual;
	return (Node *) state;
}

static void
cs_lm_begin(CustomScanState *node, EState *estate, int eflags)
{
	CSLatematState *state = (CSLatematState *) node;
	CustomScan *cscan = (CustomScan *) node->ss.ps.plan;
	List	   *attnos_l = (List *) linitial(cscan->custom_private);
	List	   *colls_l = (List *) lsecond(cscan->custom_private);
	List	   *ops_l = (List *) lthird(cscan->custom_private);
	List	   *nfs_l = (List *) lfourth(cscan->custom_private);
	int64		limit_n = (int64) intVal(list_nth(cscan->custom_private, 4));
	List	   *baseclauses = (list_length(cscan->custom_private) > 5
							   ? (List *) list_nth(cscan->custom_private, 5)
							   : NIL);
	Relation	rel = node->ss.ss_currentRelation;
	int			n_sort = list_length(attnos_l);
	int			i;
	TupleDesc	probe_desc;
	TupleDesc	rel_desc = RelationGetDescr(rel);
	AttrNumber *attnos;
	Oid		   *collations;
	Oid		   *operators;
	bool	   *nulls_first;
	AttrNumber *sort_keys;
	Bitmapset  *needed_cols = NULL;
	ScanKeyData *qual_keys = NULL;
	int			nqual_keys = 0;

	state->rel = rel;
	state->plan_info.n_sort = n_sort;
	state->plan_info.limit_n = limit_n;

	state->plan_info.sort_attnos = palloc(sizeof(AttrNumber) * n_sort);
	state->plan_info.sort_collations = palloc(sizeof(Oid) * n_sort);
	state->plan_info.sort_operators = palloc(sizeof(Oid) * n_sort);
	state->plan_info.sort_nulls_first = palloc(sizeof(bool) * n_sort);

	attnos = state->plan_info.sort_attnos;
	collations = state->plan_info.sort_collations;
	operators = state->plan_info.sort_operators;
	nulls_first = state->plan_info.sort_nulls_first;

	for (i = 0; i < n_sort; i++)
	{
		attnos[i] = (AttrNumber) list_nth_int(attnos_l, i);
		collations[i] = list_nth_oid(colls_l, i);
		operators[i] = list_nth_oid(ops_l, i);
		nulls_first[i] = (list_nth_int(nfs_l, i) != 0);
	}

	state->scandesc = rel->rd_tableam->scan_begin(rel, estate->es_snapshot,
												  0, NULL, NULL,
												  SO_TYPE_SEQSCAN |
												  SO_ALLOW_STRAT |
												  SO_ALLOW_SYNC |
												  SO_ALLOW_PAGEMODE);

	/*
	 * Build the narrow probe tupledesc: one column per sort key, plus a
	 * trailing int8 column carrying the row's ItemPointer packed into 64 bits
	 * (block << 16 | offset).  Encoding the TID as an int8 keeps the probe
	 * attribute by-value and avoids the tupdesc/slot complications around the
	 * 6-byte TID datatype.  The probe shape sets the upper bound on per-row
	 * work for the input loop -- only sort_keys + 8 bytes are paid for every
	 * input row, regardless of the relation's width.
	 */
	probe_desc = CreateTemplateTupleDesc(n_sort + 1);
	for (i = 0; i < n_sort; i++)
	{
		Form_pg_attribute relatt = TupleDescAttr(rel_desc, attnos[i] - 1);

		TupleDescInitEntry(probe_desc, (AttrNumber) (i + 1),
						   NameStr(relatt->attname),
						   relatt->atttypid, relatt->atttypmod, 0);
		TupleDescInitEntryCollation(probe_desc, (AttrNumber) (i + 1),
									relatt->attcollation);
	}
	TupleDescInitEntry(probe_desc, (AttrNumber) (n_sort + 1), "_tid",
					   INT8OID, -1, 0);
	state->probe_desc = probe_desc;

	TupleDescFinalize(probe_desc);

	state->probe_slot = MakeSingleTupleTableSlot(probe_desc, &TTSOpsVirtual);
	state->sort_slot = MakeSingleTupleTableSlot(probe_desc, &TTSOpsMinimalTuple);

	/* Slot for the raw scan output (relation's full tupledesc). */
	state->scan_slot = MakeSingleTupleTableSlot(rel_desc, &TTSOpsColumnStore);

	/* Sort keys are the first n_sort columns of probe_desc. */
	sort_keys = palloc(sizeof(AttrNumber) * n_sort);
	for (i = 0; i < n_sort; i++)
		sort_keys[i] = (AttrNumber) (i + 1);

	/*
	 * TUPLESORT_ALLOWBOUNDED tells tuplesort to use an AllocSet tuple context
	 * instead of a Bump allocator.  Bump can't free individual tuples, but
	 * bounded sorts free the largest tuple every time the heap exceeds the
	 * bound.  Without this flag, tuplesort_set_bound still works for the
	 * bounded heap but free_sort_tuple's pfree on a Bump-allocated chunk
	 * crashes inside SlabGetChunkSpace.
	 */
	state->sortstate = tuplesort_begin_heap(probe_desc, n_sort,
											sort_keys, operators,
											collations, nulls_first,
											work_mem, NULL,
											TUPLESORT_ALLOWBOUNDED);
	tuplesort_set_bound(state->sortstate, limit_n);
	state->emitted = 0;
	state->populated = false;

	/*
	 * Forward needed_cols and qual_keys to the AM: - sort_attnos must be
	 * loaded so we can build the probe tuple. - any column referenced by
	 * WHERE quals must be loaded so the residual ExprState can evaluate
	 * against the scan slot.
	 *
	 * The full payload columns are NOT marked needed: those are loaded on
	 * demand at refetch time via table_tuple_fetch_row_version, which runs
	 * only against the LIMIT survivors.
	 */
	for (i = 0; i < n_sort; i++)
		needed_cols = bms_add_member(needed_cols, attnos[i] - 1);

	if (baseclauses != NIL)
	{
		List	   *vars;
		ListCell   *lc;

		nqual_keys = cs_extract_scan_qual_keys(baseclauses,
											   cscan->scan.scanrelid,
											   &qual_keys);

		vars = pull_var_clause((Node *) baseclauses, 0);
		foreach(lc, vars)
		{
			Var		   *v = (Var *) lfirst(lc);

			if (v->varno == cscan->scan.scanrelid && v->varattno > 0)
				needed_cols = bms_add_member(needed_cols, v->varattno - 1);
		}

		state->qual_state = ExecPrepareQual(baseclauses, estate);
	}

	if (needed_cols != NULL)
	{
		cs_scan_set_projection(state->scandesc, needed_cols);
		bms_free(needed_cols);
	}
	if (nqual_keys > 0)
	{
		cs_scan_set_qual_keys(state->scandesc, nqual_keys, qual_keys);
		pfree(qual_keys);
	}
}

static TupleTableSlot *
cs_lm_exec(CustomScanState *node)
{
	CSLatematState *state = (CSLatematState *) node;
	ExprContext *econtext = node->ss.ps.ps_ExprContext;
	TupleTableSlot *fetch_slot = node->ss.ss_ScanTupleSlot;
	int			n_sort = state->plan_info.n_sort;
	AttrNumber	tid_attno = (AttrNumber) (n_sort + 1);

	/* First call: drain input into the bounded tuplesort. */
	if (!state->populated)
	{
		while (table_scan_getnextslot(state->scandesc,
									  ForwardScanDirection, state->scan_slot))
		{
			int			i;

			/* Apply residual WHERE quals before paying tuplesort overhead. */
			if (state->qual_state != NULL)
			{
				econtext->ecxt_scantuple = state->scan_slot;
				if (!ExecQual(state->qual_state, econtext))
					continue;
			}

			ExecClearTuple(state->probe_slot);
			for (i = 0; i < n_sort; i++)
			{
				bool		isnull;
				Datum		v = slot_getattr(state->scan_slot,
											 state->plan_info.sort_attnos[i],
											 &isnull);

				state->probe_slot->tts_values[i] = v;
				state->probe_slot->tts_isnull[i] = isnull;
			}
			{
				ItemPointer src = &state->scan_slot->tts_tid;
				int64		packed = ((int64) ItemPointerGetBlockNumber(src) << 16)
					| (int64) ItemPointerGetOffsetNumber(src);

				state->probe_slot->tts_values[n_sort] = Int64GetDatum(packed);
				state->probe_slot->tts_isnull[n_sort] = false;
			}
			ExecStoreVirtualTuple(state->probe_slot);

			tuplesort_puttupleslot(state->sortstate, state->probe_slot);
		}
		tuplesort_performsort(state->sortstate);
		state->populated = true;
	}

	while (state->emitted < state->plan_info.limit_n)
	{
		Datum		tid_d;
		bool		tid_isnull;
		int64		packed;
		ItemPointerData tid;
		Snapshot	snap = node->ss.ps.state->es_snapshot;

		if (!tuplesort_gettupleslot(state->sortstate, true, true,
									state->sort_slot, NULL))
			return NULL;

		state->emitted++;

		tid_d = slot_getattr(state->sort_slot, tid_attno, &tid_isnull);
		if (tid_isnull)
			continue;
		packed = DatumGetInt64(tid_d);
		ItemPointerSet(&tid,
					   (BlockNumber) (packed >> 16),
					   (OffsetNumber) (packed & 0xffff));

		/*
		 * Refetch the full row.  The same snapshot is in effect as during the
		 * seq scan, so visibility shouldn't change; if the row is gone
		 * anyway, skip it (we may return < LIMIT rows on heavy concurrent
		 * vacuum, matching what a Sort+Limit would see).
		 */
		ExecClearTuple(fetch_slot);
		if (!table_tuple_fetch_row_version(state->rel, &tid, snap, fetch_slot))
			continue;

		econtext->ecxt_scantuple = fetch_slot;

		if (node->ss.ps.ps_ProjInfo != NULL)
		{
			TupleTableSlot *out_slot = ExecProject(node->ss.ps.ps_ProjInfo);

			ExecMaterializeSlot(out_slot);
			return out_slot;
		}
		ExecMaterializeSlot(fetch_slot);
		return fetch_slot;
	}
	return NULL;
}

static void
cs_lm_end(CustomScanState *node)
{
	CSLatematState *state = (CSLatematState *) node;

	if (state->sortstate != NULL)
	{
		tuplesort_end(state->sortstate);
		state->sortstate = NULL;
	}
	if (state->probe_slot != NULL)
	{
		ExecDropSingleTupleTableSlot(state->probe_slot);
		state->probe_slot = NULL;
	}
	if (state->sort_slot != NULL)
	{
		ExecDropSingleTupleTableSlot(state->sort_slot);
		state->sort_slot = NULL;
	}
	if (state->scan_slot != NULL)
	{
		ExecDropSingleTupleTableSlot(state->scan_slot);
		state->scan_slot = NULL;
	}
	if (state->scandesc != NULL)
	{
		table_endscan(state->scandesc);
		state->scandesc = NULL;
	}
}

static void
cs_lm_rescan(CustomScanState *node)
{
	/* Conservative: re-init state. */
	cs_lm_end(node);
	cs_lm_begin(node, node->ss.ps.state, 0);
}

static void
cs_lm_explain(CustomScanState *node, List *ancestors, ExplainState *es)
{
	CSLatematState *state = (CSLatematState *) node;
	StringInfoData buf;

	initStringInfo(&buf);
	for (int i = 0; i < state->plan_info.n_sort; i++)
	{
		if (i > 0)
			appendStringInfoString(&buf, ", ");
		appendStringInfo(&buf, "att=%d", state->plan_info.sort_attnos[i]);
	}
	ExplainPropertyText("Columnstore LateMat Sort Keys", buf.data, es);
	ExplainPropertyInteger("Columnstore LateMat Limit", NULL,
						   state->plan_info.limit_n, es);
	pfree(buf.data);
}
