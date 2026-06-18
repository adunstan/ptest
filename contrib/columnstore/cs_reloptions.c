/*-------------------------------------------------------------------------
 *
 * cs_reloptions.c
 *	  Reloption registration and parser for the columnstore table AM.
 *
 * The columnstore uses its own RELOPT_KIND_* so that columnstore-specific
 * options (sort_key) are not visible on standard heap tables.  A subset
 * of the standard heap options is opted in by name so the columnstore
 * accepts the autovacuum, parallel-worker, and fillfactor options that
 * its users expect to be able to set.
 *
 * Wired into the AM dispatch via TableAmRoutine.amoptions.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * contrib/columnstore/cs_reloptions.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cs_internal.h"
#include "access/reloptions.h"

static void cs_validate_sort_key(const char *value);

static relopt_kind cs_relopt_kind = (relopt_kind) 0;

/*
 * Register the columnstore reloption kind and opt the standard heap
 * options into it.  Called from _PG_init.
 */
void
cs_register_reloptions(void)
{
	cs_relopt_kind = add_reloption_kind();

	/*
	 * sort_key is columnstore-only.  Register it under our kind only;
	 * standard heap tables should reject it as unknown.
	 */
	add_string_reloption(cs_relopt_kind, "sort_key",
						 "Comma-separated column list used to order rows "
						 "during delta-to-columnar compaction",
						 NULL, cs_validate_sort_key,
						 AccessExclusiveLock);

	/*
	 * Standard options the columnstore accepts.  The list mirrors what
	 * default_reloptions() recognises minus heap-specific options that
	 * columnstore does not honour (user_catalog_table, vacuum_index_cleanup,
	 * vacuum_max_eager_freeze_failure_rate). toast_tuple_target is honoured:
	 * incoming inserts pass through heap_toast_insert_or_update against the
	 * delta-store heap pages, which reads the target via
	 * RelationGetToastTupleTarget.  Each call extends the named option's
	 * kinds bitmask so cs_reloptions can parse it.
	 */
	add_reloption_to_kind("fillfactor", RELOPT_KIND_HEAP, cs_relopt_kind);
	add_reloption_to_kind("parallel_workers", RELOPT_KIND_HEAP, cs_relopt_kind);
	add_reloption_to_kind("toast_tuple_target", RELOPT_KIND_HEAP, cs_relopt_kind);
	add_reloption_to_kind("vacuum_truncate", RELOPT_KIND_HEAP, cs_relopt_kind);

	add_reloption_to_kind("autovacuum_enabled", RELOPT_KIND_HEAP, cs_relopt_kind);
	add_reloption_to_kind("autovacuum_parallel_workers", RELOPT_KIND_HEAP, cs_relopt_kind);
	add_reloption_to_kind("autovacuum_vacuum_threshold", RELOPT_KIND_HEAP, cs_relopt_kind);
	add_reloption_to_kind("autovacuum_vacuum_max_threshold", RELOPT_KIND_HEAP, cs_relopt_kind);
	add_reloption_to_kind("autovacuum_vacuum_insert_threshold", RELOPT_KIND_HEAP, cs_relopt_kind);
	add_reloption_to_kind("autovacuum_analyze_threshold", RELOPT_KIND_HEAP, cs_relopt_kind);
	add_reloption_to_kind("autovacuum_vacuum_cost_limit", RELOPT_KIND_HEAP, cs_relopt_kind);
	add_reloption_to_kind("autovacuum_freeze_min_age", RELOPT_KIND_HEAP, cs_relopt_kind);
	add_reloption_to_kind("autovacuum_freeze_max_age", RELOPT_KIND_HEAP, cs_relopt_kind);
	add_reloption_to_kind("autovacuum_freeze_table_age", RELOPT_KIND_HEAP, cs_relopt_kind);
	add_reloption_to_kind("autovacuum_multixact_freeze_min_age", RELOPT_KIND_HEAP, cs_relopt_kind);
	add_reloption_to_kind("autovacuum_multixact_freeze_max_age", RELOPT_KIND_HEAP, cs_relopt_kind);
	add_reloption_to_kind("autovacuum_multixact_freeze_table_age", RELOPT_KIND_HEAP, cs_relopt_kind);
	add_reloption_to_kind("log_autovacuum_min_duration", RELOPT_KIND_HEAP, cs_relopt_kind);
	add_reloption_to_kind("log_autoanalyze_min_duration", RELOPT_KIND_HEAP, cs_relopt_kind);
	add_reloption_to_kind("autovacuum_vacuum_cost_delay", RELOPT_KIND_HEAP, cs_relopt_kind);
	add_reloption_to_kind("autovacuum_vacuum_scale_factor", RELOPT_KIND_HEAP, cs_relopt_kind);
	add_reloption_to_kind("autovacuum_vacuum_insert_scale_factor", RELOPT_KIND_HEAP, cs_relopt_kind);
	add_reloption_to_kind("autovacuum_analyze_scale_factor", RELOPT_KIND_HEAP, cs_relopt_kind);
}

/*
 * Parse reloptions for a columnstore relation.  Wired into
 * TableAmRoutine.amoptions on the columnstore AM handler.
 */
bytea *
cs_reloptions(Datum reloptions, bool validate)
{
	static const relopt_parse_elt tab[] = {
		{"fillfactor", RELOPT_TYPE_INT, offsetof(StdRdOptions, fillfactor)},
		{"parallel_workers", RELOPT_TYPE_INT,
		offsetof(StdRdOptions, parallel_workers)},
		{"toast_tuple_target", RELOPT_TYPE_INT,
		offsetof(StdRdOptions, toast_tuple_target)},
		{"vacuum_truncate", RELOPT_TYPE_TERNARY,
		offsetof(StdRdOptions, vacuum_truncate)},

		{"autovacuum_enabled", RELOPT_TYPE_BOOL,
		offsetof(StdRdOptions, autovacuum) + offsetof(AutoVacOpts, enabled)},
		{"autovacuum_parallel_workers", RELOPT_TYPE_INT,
		offsetof(StdRdOptions, autovacuum) + offsetof(AutoVacOpts, autovacuum_parallel_workers)},
		{"autovacuum_vacuum_threshold", RELOPT_TYPE_INT,
		offsetof(StdRdOptions, autovacuum) + offsetof(AutoVacOpts, vacuum_threshold)},
		{"autovacuum_vacuum_max_threshold", RELOPT_TYPE_INT,
		offsetof(StdRdOptions, autovacuum) + offsetof(AutoVacOpts, vacuum_max_threshold)},
		{"autovacuum_vacuum_insert_threshold", RELOPT_TYPE_INT,
		offsetof(StdRdOptions, autovacuum) + offsetof(AutoVacOpts, vacuum_ins_threshold)},
		{"autovacuum_analyze_threshold", RELOPT_TYPE_INT,
		offsetof(StdRdOptions, autovacuum) + offsetof(AutoVacOpts, analyze_threshold)},
		{"autovacuum_vacuum_cost_limit", RELOPT_TYPE_INT,
		offsetof(StdRdOptions, autovacuum) + offsetof(AutoVacOpts, vacuum_cost_limit)},
		{"autovacuum_freeze_min_age", RELOPT_TYPE_INT,
		offsetof(StdRdOptions, autovacuum) + offsetof(AutoVacOpts, freeze_min_age)},
		{"autovacuum_freeze_max_age", RELOPT_TYPE_INT,
		offsetof(StdRdOptions, autovacuum) + offsetof(AutoVacOpts, freeze_max_age)},
		{"autovacuum_freeze_table_age", RELOPT_TYPE_INT,
		offsetof(StdRdOptions, autovacuum) + offsetof(AutoVacOpts, freeze_table_age)},
		{"autovacuum_multixact_freeze_min_age", RELOPT_TYPE_INT,
		offsetof(StdRdOptions, autovacuum) + offsetof(AutoVacOpts, multixact_freeze_min_age)},
		{"autovacuum_multixact_freeze_max_age", RELOPT_TYPE_INT,
		offsetof(StdRdOptions, autovacuum) + offsetof(AutoVacOpts, multixact_freeze_max_age)},
		{"autovacuum_multixact_freeze_table_age", RELOPT_TYPE_INT,
		offsetof(StdRdOptions, autovacuum) + offsetof(AutoVacOpts, multixact_freeze_table_age)},
		{"log_autovacuum_min_duration", RELOPT_TYPE_INT,
		offsetof(StdRdOptions, autovacuum) + offsetof(AutoVacOpts, log_vacuum_min_duration)},
		{"log_autoanalyze_min_duration", RELOPT_TYPE_INT,
		offsetof(StdRdOptions, autovacuum) + offsetof(AutoVacOpts, log_analyze_min_duration)},
		{"autovacuum_vacuum_cost_delay", RELOPT_TYPE_REAL,
		offsetof(StdRdOptions, autovacuum) + offsetof(AutoVacOpts, vacuum_cost_delay)},
		{"autovacuum_vacuum_scale_factor", RELOPT_TYPE_REAL,
		offsetof(StdRdOptions, autovacuum) + offsetof(AutoVacOpts, vacuum_scale_factor)},
		{"autovacuum_vacuum_insert_scale_factor", RELOPT_TYPE_REAL,
		offsetof(StdRdOptions, autovacuum) + offsetof(AutoVacOpts, vacuum_ins_scale_factor)},
		{"autovacuum_analyze_scale_factor", RELOPT_TYPE_REAL,
		offsetof(StdRdOptions, autovacuum) + offsetof(AutoVacOpts, analyze_scale_factor)},

		{"sort_key", RELOPT_TYPE_STRING,
		offsetof(CSRdOptions, sort_key_offset)},
	};

	return (bytea *) build_reloptions(reloptions, validate,
									  cs_relopt_kind,
									  sizeof(CSRdOptions),
									  tab, lengthof(tab));
}

/*
 * Syntactic validator for sort_key: comma-separated SQL identifiers.
 * Accepts both unquoted ([A-Za-z_][A-Za-z0-9_$]*) and double-quoted
 * identifiers (with embedded double-quotes doubled).  Whitespace around
 * commas is tolerated.  An empty/whitespace-only string clears the hint.
 *
 * Semantic validation against the relation's column list is left to the
 * consumer (cs_compact.c reads the hint at compaction time); this
 * function only enforces parsability.
 */
static void
cs_validate_sort_key(const char *value)
{
	const char *p;
	bool		quoted_ident_unterminated;

	if (value == NULL)
		return;

	p = value;

	while (*p == ' ' || *p == '\t')
		p++;
	if (*p == '\0')
		return;

	for (;;)
	{
		if (*p == '"')
		{
			quoted_ident_unterminated = true;
			p++;
			while (*p != '\0')
			{
				if (*p == '"')
				{
					if (p[1] == '"')
						p += 2;
					else
					{
						p++;
						quoted_ident_unterminated = false;
						break;
					}
				}
				else
					p++;
			}
			if (quoted_ident_unterminated)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("invalid sort_key value: unterminated quoted identifier"),
						 errdetail("sort_key was \"%s\"", value)));
		}
		else if ((*p >= 'A' && *p <= 'Z') ||
				 (*p >= 'a' && *p <= 'z') ||
				 *p == '_')
		{
			p++;
			while ((*p >= 'A' && *p <= 'Z') ||
				   (*p >= 'a' && *p <= 'z') ||
				   (*p >= '0' && *p <= '9') ||
				   *p == '_' || *p == '$')
				p++;
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid sort_key value: expected column name"),
					 errdetail("sort_key was \"%s\"", value)));
		}

		while (*p == ' ' || *p == '\t')
			p++;

		if (*p == '\0')
			return;
		if (*p != ',')
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid sort_key value: expected ',' between column names"),
					 errdetail("sort_key was \"%s\"", value)));
		p++;

		while (*p == ' ' || *p == '\t')
			p++;
	}
}
