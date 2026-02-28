/* tee_adaptive_selector.c: Adaptive meta-optimizer for TEE environments. */
#include "postgres.h"
#include "fmgr.h"
#include "utils/elog.h"
#include "optimizer/planner.h"
#include "parser/parsetree.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "catalog/pg_class.h"
#include "catalog/pg_constraint.h"
#include "nodes/nodes.h"
#include "utils/timestamp.h"
#include "storage/fd.h"			/* AllocateFile / FreeFile */
#include "tcop/tcopprot.h"		/* debug_query_string */
#include "access/heapam.h"
#include "access/hash.h"
#include "access/table.h"
#include "executor/executor.h"
#include "miscadmin.h"			/* is_absolute_path */
#include "lib/stringinfo.h"
#include "common/pg_prng.h"
#include "storage/ipc.h"
#include <ctype.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>

PG_MODULE_MAGIC;

/* GUC variable to enable/disable the adaptive selector itself */
static bool tee_adaptive_enable = true;
/* GUC to control per-query decision logging */
static bool tee_adaptive_log_decisions = true;
/* GUC: use cached hash->scenario mapping */
static bool tee_adaptive_use_cache = true;
/* GUC: keep cache in memory unless populating */
static bool tee_adaptive_cache_populating = false;
/* GUC: workload selector (job/ceb/stack/tpcds) */
static char *tee_adaptive_workload = "tpcds";

/* Saved hook pointer */
static planner_hook_type prev_planner_hook = NULL;

/* SQL-callable activation helper */
PG_FUNCTION_INFO_V1(tee_adaptive_selector_activate);
Datum	tee_adaptive_selector_activate(PG_FUNCTION_ARGS);

/* Structure to hold extracted query features */
typedef struct QueryFeatures
{
	int			num_relations;			/* Number of base relations joined */
	double		estimated_total_rows;	/* Rough estimate of total input rows */
	double		max_rel_rows;			/* Largest single relation row estimate */
	int			small_rel_count;		/* Count of small relations */
	int			large_rel_count;		/* Count of large relations */
	int			indexed_rel_count;		/* Relations with indexes (IMDB only) */
	int			index_total_count;		/* Total indexes across relations */
	double		avg_index_per_rel;		/* Avg indexes per relation */
	int			num_quals;				/* Total number of quals in jointree */
	int			num_and_quals;			/* Count of AND branches */
	int			num_or_quals;			/* Count of OR branches */
	bool		has_aggregates;
	bool		has_group_by;
	bool		has_distinct;
	bool		has_sort;
	bool		has_limit;
	bool		has_sublinks;
	bool		has_window_funcs;
	bool		has_like;
	bool		has_in;
	bool		has_between;
	/* Metrics used for weighted voting */
	int			join_count;
	int			subquery_count;
	int			has_having;
	int			has_union;
	int			has_exists;
	int			has_case;
	int			agg_func_count;
	int			window_func_count;
	int			table_count_est;
	int			where_terms_est;
	double		or_ratio;
	int			table_mentioned_count;
	double		table_rows_sum;
	double		table_rows_mean;
	double		table_rows_max;
	double		table_rows_min;
	double		table_index_count_sum;
	double		table_index_count_mean;
	double		pct_tables_with_index;
} QueryFeatures;

/* GUC Names for underlying extensions (must match each extension's _PG_init) */
static const char *GUC_JN_ENABLE = "tee_join_enumerator.jn_enabled";
static const char *GUC_CE_ENABLE = "tee_cardinality_estimation.enable_sev_snp_ce";
static const char *GUC_CM_ENABLE = "tee_cost_model.enable";

/* Cache inputs/outputs (configurable via GUCs). */
static char *tee_source_csv = "postgresql-16.4/contrib/tee_adaptive_selector/best_combination_job_no_geqo.csv";
static char *tee_cache_csv = "postgresql-16.4/contrib/tee_adaptive_selector/sql_speedup_hash_cache.csv";
static char *tee_query_dir = "postgresql-16.4/contrib/tee_adaptive_selector/job_queries";

#define MAX_CACHE_ENTRIES 256
#define CACHE_SLOT_CAPACITY 8

typedef enum Scenario
{
	SC_NONE = 0,
	SC_CM,
	SC_CE,
	SC_JN,
	SC_CE_CM,
	SC_CE_JN,
	SC_CM_JN,
	SC_ALL
} Scenario;

typedef enum WorkloadType
{
	WL_JOB = 0,
	WL_CEB,
	WL_STACK,
	WL_TPCDS
} WorkloadType;

typedef struct CacheSlot
{
	uint8		v;      /* version 0..7 */
	double		t;      /* execution time */
	uint32		sh;     /* similarity hash */
	uint8		cb;     /* component combination bits */
	bool		in_use;
} CacheSlot;

typedef struct CacheBucket
{
	uint32		hash;   /* H */
	int			count;  /* number of valid slots */
	CacheSlot	slots[CACHE_SLOT_CAPACITY];
} CacheBucket;

typedef struct DataProfile
{
	bool		initialized;
	int			table_count;
	int			fk_count;
	double		total_rows;
	double		max_rows;
	int			large_table_count;
	int			huge_table_count;
	double		max_ratio;
	double		fk_per_table;
	int			index_count;
	double		index_per_table;
	bool		join_dense;
	bool		skewed;
	bool		large_db;
	bool		index_dense;
	bool		imdb_mode;
	WorkloadType workload;
} DataProfile;

static CacheBucket cache_buckets[MAX_CACHE_ENTRIES];
static int cache_bucket_count = 0;
static bool cache_loaded = false;
static DataProfile data_profile;

static ExecutorStart_hook_type prev_executor_start_hook = NULL;
static ExecutorEnd_hook_type prev_executor_end_hook = NULL;

static pg_prng_state cache_prng_state;
static bool cache_prng_seeded = false;

static bool cache_feedback_pending = false;
static bool cache_feedback_active = false;
static TimestampTz cache_feedback_start;
static uint32 cache_feedback_hash;
static uint32 cache_feedback_sh;
static uint8 cache_feedback_cb;

static void tee_adaptive_on_proc_exit(int code, Datum arg);
static void sanitize_sql(const char *sql, StringInfo buf);

typedef struct TableRowsIndex
{
	const char *name;
	double		rows;
	int			indexes;
} TableRowsIndex;

/* IMDB table row counts (hardcoded). */
#define IMDB_FK_COUNT 19
#define IMDB_INDEX_COUNT 46
static const TableRowsIndex imdb_table_rows_index[] = {
	{"aka_name", 901343.0, 2},
	{"aka_title", 361472.0, 3},
	{"cast_info", 36244344.0, 5},
	{"char_name", 3140339.0, 1},
	{"comp_cast_type", 4.0, 1},
	{"company_name", 234997.0, 1},
	{"company_type", 4.0, 1},
	{"complete_cast", 135086.0, 4},
	{"info_type", 113.0, 1},
	{"keyword", 134170.0, 1},
	{"kind_type", 7.0, 1},
	{"link_type", 18.0, 1},
	{"movie_companies", 2609129.0, 4},
	{"movie_info", 14835720.0, 3},
	{"movie_info_idx", 1380035.0, 3},
	{"movie_keyword", 4523930.0, 3},
	{"movie_link", 29997.0, 4},
	{"name", 4167491.0, 1},
	{"person_info", 2963664.0, 3},
	{"role_type", 12.0, 1},
	{"title", 2528312.0, 2},
};

static const TableRowsIndex stack_table_rows_index[] = {
	{"account", 13872153.0, 1},
	{"answer", 6347553.0, 5},
	{"badge", 51236903.0, 1},
	{"comment", 103459956.0, 3},
	{"post_link", 2264333.0, 1},
	{"question", 12666441.0, 4},
	{"site", 173.0, 1},
	{"so_user", 21097302.0, 3},
	{"tag", 186770.0, 1},
	{"tag_question", 36883819.0, 2},
};

static const TableRowsIndex tpcds_table_rows_index[] = {
	{"call_center", 24.0, 3},
	{"catalog_page", 12000.0, 3},
	{"catalog_returns", 1439749.0, 18},
	{"catalog_sales", 14401261.0, 19},
	{"customer", 500000.0, 6},
	{"customer_address", 250000.0, 2},
	{"customer_demographics", 1920800.0, 2},
	{"date_dim", 73049.0, 1},
	{"household_demographics", 7200.0, 2},
	{"income_band", 20.0, 1},
	{"inventory", 133110000.0, 4},
	{"item", 102000.0, 3},
	{"promotion", 500.0, 4},
	{"reason", 45.0, 1},
	{"ship_mode", 20.0, 1},
	{"store", 102.0, 2},
	{"store_returns", 2875432.0, 11},
	{"store_sales", 28800991.0, 15},
	{"time_dim", 86400.0, 1},
	{"warehouse", 10.0, 1},
	{"web_page", 200.0, 4},
	{"web_returns", 719217.0, 15},
	{"web_sales", 7197566.0, 19},
	{"web_site", 42.0, 3},
};

typedef enum MetricId
{
	MET_JOIN_COUNT = 0,
	MET_SUBQUERY_COUNT,
	MET_HAS_GROUP_BY,
	MET_HAS_ORDER_BY,
	MET_HAS_HAVING,
	MET_HAS_DISTINCT,
	MET_HAS_LIMIT,
	MET_HAS_UNION,
	MET_HAS_EXISTS,
	MET_HAS_IN,
	MET_HAS_LIKE,
	MET_HAS_BETWEEN,
	MET_HAS_CASE,
	MET_AGG_FUNC_COUNT,
	MET_WINDOW_FUNC_COUNT,
	MET_TABLE_COUNT_EST,
	MET_WHERE_TERMS_EST,
	MET_OR_COUNT,
	MET_AND_COUNT,
	MET_OR_RATIO,
	MET_TABLE_MENTIONED_COUNT,
	MET_TABLE_ROWS_SUM,
	MET_TABLE_ROWS_MEAN,
	MET_TABLE_ROWS_MAX,
	MET_TABLE_ROWS_MIN,
	MET_TABLE_INDEX_SUM,
	MET_TABLE_INDEX_MEAN,
	MET_PCT_TABLES_WITH_INDEX
} MetricId;

typedef struct MetricRule
{
	MetricId	id;
	double		threshold;
	int			direction;
	double		weight;
} MetricRule;

typedef enum ComponentId
{
	COMP_CE = 0,
	COMP_CM,
	COMP_JN
} ComponentId;

static const MetricRule ce_job_rules[] = {
	{MET_AND_COUNT, 17.2500, 1, 1.0},
	{MET_HAS_IN, 0.5987, 1, 1.0},
	{MET_TABLE_COUNT_EST, 8.5000, 1, 1.0},
	{MET_TABLE_INDEX_SUM, 17.5000, 1, 1.0},
	{MET_TABLE_MENTIONED_COUNT, 7.5000, 1, 1.0},
	{MET_TABLE_ROWS_MAX, 25540032.0000, 1, 1.0},
	{MET_TABLE_ROWS_MEAN, 5164715.2474, 1, 1.0},
	{MET_TABLE_ROWS_MIN, 8.0000, -1, 1.0},
	{MET_TABLE_ROWS_SUM, 36570981.0000, 1, 1.0},
	{MET_WHERE_TERMS_EST, 19.0000, 1, 1.0},
};

static const MetricRule ce_ceb_rules[] = {
	{MET_AND_COUNT, 19.5000, 1, 1.2},
	{MET_HAS_BETWEEN, 0.0109, -1, 0.8},
	{MET_HAS_CASE, 0.0054, -1, 0.8},
	{MET_HAS_GROUP_BY, 0.3500, 1, 0.9},
	{MET_HAS_ORDER_BY, 0.1800, 1, 0.9},
	{MET_HAS_UNION, 0.0250, 1, 0.7},
	{MET_TABLE_MENTIONED_COUNT, 9.0000, 1, 1.2},
	{MET_TABLE_ROWS_MAX, 35000000.0000, 1, 1.0},
	{MET_TABLE_ROWS_MEAN, 6400000.0000, -1, 0.9},
	{MET_TABLE_ROWS_MIN, 6.0000, 1, 0.8},
	{MET_TABLE_ROWS_SUM, 57000000.0000, 1, 1.1},
	{MET_WHERE_TERMS_EST, 20.5000, 1, 1.2},
};

static const MetricRule ce_stack_rules[] = {
	{MET_AND_COUNT, 14.5000, -1, 1.0},
	{MET_HAS_CASE, 0.0146, -1, 1.0},
	{MET_HAS_DISTINCT, 0.6055, -1, 1.0},
	{MET_HAS_EXISTS, 0.1018, 1, 1.0},
	{MET_HAS_GROUP_BY, 0.2127, -1, 1.0},
	{MET_JOIN_COUNT, 0.0000, 1, 1.0},
	{MET_SUBQUERY_COUNT, 0.0000, 1, 1.0},
	{MET_TABLE_COUNT_EST, 7.0000, 1, 1.0},
	{MET_TABLE_INDEX_SUM, 14.5000, -1, 1.0},
	{MET_TABLE_MENTIONED_COUNT, 7.0000, 1, 1.0},
	{MET_TABLE_ROWS_MAX, 51236903.0000, 1, 1.0},
	{MET_TABLE_ROWS_MEAN, 17786389.2500, 1, 1.0},
	{MET_TABLE_ROWS_MIN, 173.0000, 1, 1.0},
	{MET_TABLE_ROWS_SUM, 110242888.0000, -1, 1.0},
	{MET_WHERE_TERMS_EST, 15.5000, -1, 1.0},
};

static const MetricRule ce_tpc_ds_rules[] = {
	{MET_AGG_FUNC_COUNT, 3.0000, 1, 1.0},
	{MET_AND_COUNT, 6.0000, 1, 1.0},
	{MET_HAS_CASE, 0.3277, 1, 1.0},
	{MET_HAS_DISTINCT, 0.0795, -1, 1.0},
	{MET_HAS_EXISTS, 0.0459, -1, 1.0},
	{MET_HAS_IN, 0.4017, 1, 1.0},
	{MET_HAS_LIKE, 0.0071, -1, 1.0},
	{MET_HAS_UNION, 0.1784, -1, 1.0},
	{MET_OR_COUNT, 0.0000, 1, 1.0},
	{MET_SUBQUERY_COUNT, 1.5000, -1, 1.0},
	{MET_TABLE_INDEX_MEAN, 7.8333, -1, 1.0},
	{MET_TABLE_INDEX_SUM, 24.5000, -1, 1.0},
	{MET_TABLE_ROWS_MAX, 28800991.0000, 1, 1.0},
	{MET_TABLE_ROWS_MEAN, 8473920.9167, -1, 1.0},
	{MET_TABLE_ROWS_MIN, 87524.5000, -1, 1.0},
	{MET_TABLE_ROWS_SUM, 28888515.5000, -1, 1.0},
	{MET_WHERE_TERMS_EST, 7.5000, -1, 1.0},
	{MET_WINDOW_FUNC_COUNT, 0.0000, 1, 1.0},
};

static const MetricRule cm_job_rules[] = {
	{MET_TABLE_MENTIONED_COUNT, 8.0000, 1, 1.0},
	{MET_TABLE_ROWS_MAX, 25540032.0000, 1, 1.0},
	{MET_TABLE_ROWS_MEAN, 4750094.3000, 1, 1.0},
	{MET_TABLE_ROWS_MIN, 6.2500, -1, 1.0},
	{MET_TABLE_ROWS_SUM, 36051444.7500, 1, 1.0},
};

static const MetricRule cm_ceb_rules[] = {
	{MET_AND_COUNT, 19.5000, -1, 1.1},
	{MET_HAS_BETWEEN, 0.0120, 1, 1.0},
	{MET_HAS_CASE, 0.0050, -1, 0.8},
	{MET_HAS_LIKE, 0.0120, 1, 1.0},
	{MET_OR_COUNT, 0.3000, 1, 1.1},
	{MET_TABLE_COUNT_EST, 10.0000, -1, 1.2},
	{MET_TABLE_ROWS_MAX, 35000000.0000, 1, 1.0},
	{MET_TABLE_ROWS_MEAN, 6500000.0000, 1, 1.1},
	{MET_TABLE_ROWS_SUM, 57000000.0000, -1, 1.0},
	{MET_WHERE_TERMS_EST, 21.0000, -1, 1.2},
};

static const MetricRule cm_stack_rules[] = {
	{MET_HAS_GROUP_BY, 0.1875, 1, 1.0},
	{MET_HAS_LIMIT, 0.1313, 1, 1.0},
	{MET_HAS_ORDER_BY, 0.1250, 1, 1.0},
	{MET_JOIN_COUNT, 0.0000, 1, 1.0},
	{MET_TABLE_COUNT_EST, 7.0000, 1, 1.0},
	{MET_TABLE_INDEX_SUM, 14.0000, 1, 1.0},
	{MET_TABLE_MENTIONED_COUNT, 6.5000, 1, 1.0},
	{MET_TABLE_ROWS_MAX, 51236903.0000, 1, 1.0},
	{MET_TABLE_ROWS_MEAN, 17786389.2500, 1, 1.0},
	{MET_TABLE_ROWS_MIN, 173.0000, 1, 1.0},
	{MET_TABLE_ROWS_SUM, 105854723.5000, -1, 1.0},
};

static const MetricRule cm_tpc_ds_rules[] = {
	{MET_AGG_FUNC_COUNT, 3.0000, 1, 1.0},
	{MET_AND_COUNT, 6.0000, 1, 1.0},
	{MET_HAS_CASE, 0.2962, -1, 1.0},
	{MET_HAS_DISTINCT, 0.0972, 1, 1.0},
	{MET_HAS_HAVING, 0.0657, -1, 1.0},
	{MET_HAS_LIKE, 0.0086, -1, 1.0},
	{MET_HAS_UNION, 0.2099, 1, 1.0},
	{MET_JOIN_COUNT, 0.0000, 1, 1.0},
	{MET_OR_COUNT, 0.0000, 1, 1.0},
	{MET_SUBQUERY_COUNT, 1.2500, 1, 1.0},
	{MET_TABLE_COUNT_EST, 4.0000, 1, 1.0},
	{MET_TABLE_INDEX_SUM, 25.5000, 1, 1.0},
	{MET_TABLE_ROWS_MAX, 28800991.0000, 1, 1.0},
	{MET_TABLE_ROWS_MEAN, 9237321.0833, 1, 1.0},
	{MET_TABLE_ROWS_MIN, 87524.5000, -1, 1.0},
	{MET_TABLE_ROWS_SUM, 28937441.0000, 1, 1.0},
	{MET_WHERE_TERMS_EST, 7.5000, 1, 1.0},
};

static const MetricRule jn_job_rules[] = {
	{MET_AND_COUNT, 16.0000, -1, 1.0},
	{MET_HAS_BETWEEN, 0.1860, -1, 1.0},
	{MET_HAS_IN, 0.5566, -1, 1.0},
	{MET_HAS_LIKE, 0.6828, -1, 1.0},
	{MET_OR_COUNT, 0.0000, 1, 1.0},
	{MET_TABLE_COUNT_EST, 8.5000, -1, 1.0},
	{MET_TABLE_INDEX_SUM, 17.0000, 1, 1.0},
	{MET_TABLE_MENTIONED_COUNT, 8.0000, 1, 1.0},
	{MET_TABLE_ROWS_MAX, 25540032.0000, 1, 1.0},
	{MET_TABLE_ROWS_MEAN, 4868530.2857, 1, 1.0},
	{MET_TABLE_ROWS_MIN, 9.5000, 1, 1.0},
	{MET_TABLE_ROWS_SUM, 36922332.0000, 1, 1.0},
	{MET_WHERE_TERMS_EST, 17.5000, -1, 1.0},
};

static const MetricRule jn_ceb_rules[] = {
	{MET_AND_COUNT, 19.5000, -1, 1.2},
	{MET_HAS_BETWEEN, 0.0150, 1, 1.0},
	{MET_HAS_CASE, 0.0040, -1, 0.8},
	{MET_HAS_GROUP_BY, 0.3000, -1, 1.1},
	{MET_HAS_LIKE, 0.0080, -1, 0.9},
	{MET_HAS_ORDER_BY, 0.1500, -1, 1.1},
	{MET_TABLE_COUNT_EST, 10.0000, -1, 1.3},
	{MET_TABLE_INDEX_SUM, 17.5000, -1, 1.0},
	{MET_TABLE_MENTIONED_COUNT, 8.8000, -1, 1.2},
	{MET_TABLE_ROWS_MAX, 33000000.0000, 1, 1.0},
	{MET_TABLE_ROWS_MEAN, 6200000.0000, -1, 1.0},
	{MET_TABLE_ROWS_MIN, 7.0000, 1, 0.9},
	{MET_TABLE_ROWS_SUM, 55000000.0000, -1, 1.1},
	{MET_WHERE_TERMS_EST, 21.0000, -1, 1.2},
};

static const MetricRule jn_stack_rules[] = {
	{MET_AND_COUNT, 14.7500, -1, 1.0},
	{MET_HAS_CASE, 0.0162, 1, 1.0},
	{MET_HAS_EXISTS, 0.1614, 1, 1.0},
	{MET_HAS_GROUP_BY, 0.2115, 1, 1.0},
	{MET_HAS_IN, 0.5347, -1, 1.0},
	{MET_JOIN_COUNT, 0.0000, 1, 1.0},
	{MET_SUBQUERY_COUNT, 0.0000, 1, 1.0},
	{MET_TABLE_COUNT_EST, 6.5000, -1, 1.0},
	{MET_TABLE_INDEX_SUM, 12.0000, -1, 1.0},
	{MET_TABLE_MENTIONED_COUNT, 5.5000, -1, 1.0},
	{MET_TABLE_ROWS_MAX, 47648632.0000, 1, 1.0},
	{MET_TABLE_ROWS_MEAN, 17686295.7530, 1, 1.0},
	{MET_TABLE_ROWS_MIN, 173.0000, 1, 1.0},
	{MET_TABLE_ROWS_SUM, 105854723.5000, 1, 1.0},
	{MET_WHERE_TERMS_EST, 15.7500, -1, 1.0},
};

static const MetricRule jn_tpc_ds_rules[] = {
	{MET_AGG_FUNC_COUNT, 3.0000, 1, 1.0},
	{MET_AND_COUNT, 6.2500, 1, 1.0},
	{MET_HAS_HAVING, 0.0645, -1, 1.0},
	{MET_HAS_LIKE, 0.0075, -1, 1.0},
	{MET_HAS_ORDER_BY, 0.9165, 1, 1.0},
	{MET_JOIN_COUNT, 0.0000, 1, 1.0},
	{MET_OR_COUNT, 0.0000, 1, 1.0},
	{MET_SUBQUERY_COUNT, 1.2500, -1, 1.0},
	{MET_TABLE_INDEX_SUM, 24.7500, -1, 1.0},
	{MET_TABLE_ROWS_MAX, 28800991.0000, 1, 1.0},
	{MET_TABLE_ROWS_MEAN, 8856478.4583, -1, 1.0},
	{MET_TABLE_ROWS_MIN, 87524.5000, -1, 1.0},
	{MET_TABLE_ROWS_SUM, 28914041.0000, -1, 1.0},
	{MET_WHERE_TERMS_EST, 7.5000, 1, 1.0},
	{MET_WINDOW_FUNC_COUNT, 0.0000, 1, 1.0},
};

static const TableRowsIndex *
get_workload_tables(WorkloadType workload, int *count)
{
	switch (workload)
	{
		case WL_JOB:
		case WL_CEB:
			*count = (int) (sizeof(imdb_table_rows_index) / sizeof(imdb_table_rows_index[0]));
			return imdb_table_rows_index;
		case WL_STACK:
			*count = (int) (sizeof(stack_table_rows_index) / sizeof(stack_table_rows_index[0]));
			return stack_table_rows_index;
		case WL_TPCDS:
			*count = (int) (sizeof(tpcds_table_rows_index) / sizeof(tpcds_table_rows_index[0]));
			return tpcds_table_rows_index;
		default:
			*count = 0;
			return NULL;
	}
}

static int
workload_table_index(WorkloadType workload, const char *relname)
{
	const TableRowsIndex *tables;
	int			table_count;
	int			i;

	tables = get_workload_tables(workload, &table_count);
	if (tables == NULL)
		return -1;

	for (i = 0; i < table_count; i++)
	{
		if (strcmp(relname, tables[i].name) == 0)
			return i;
	}
	return -1;
}

static bool
lookup_table_rows_index(WorkloadType workload, const char *relname, double *rows, int *indexes)
{
	const TableRowsIndex *tables;
	int			table_count;
	int			i;

	tables = get_workload_tables(workload, &table_count);
	if (tables == NULL)
		return false;
	for (i = 0; i < table_count; i++)
	{
		if (strcmp(relname, tables[i].name) == 0)
		{
			if (rows)
				*rows = tables[i].rows;
			if (indexes)
				*indexes = tables[i].indexes;
			return true;
		}
	}
	return false;
}

/* Helper to log decisions when enabled */
static void log_strategy_decision(const char *strategy, const char *jn, const char *ce, const char *cm)
{
	TimestampTz now;

	if (!tee_adaptive_log_decisions)
		return;

	now = GetCurrentTimestamp();
	elog(LOG, "TEE Adaptive: %s at %s (jn=%s, ce=%s, cm=%s)",
		 strategy, timestamptz_to_str(now), jn, ce, cm);
}

static FILE *
open_with_fallback(const char *path, const char *mode)
{
	FILE	   *f;

	f = AllocateFile(path, mode);
	if (f)
		return f;
	if (!is_absolute_path(path))
	{
		const char *home = getenv("HOME");

		if (home && *home)
		{
			StringInfoData buf;

			initStringInfo(&buf);
			appendStringInfo(&buf, "%s/%s", home, path);
			f = AllocateFile(buf.data, mode);
			pfree(buf.data);
			if (f)
				return f;
		}
	}
	return NULL;
}

static const char *
strip_explain_prefix(const char *sql)
{
	const char *p = sql;

	while (*p && isspace((unsigned char) *p))
		p++;
	if (pg_strncasecmp(p, "explain", 7) != 0)
		return sql;
	p += 7;
	while (*p && isspace((unsigned char) *p))
		p++;
	if (*p == '(')
	{
		int depth = 1;

		p++;
		while (*p && depth > 0)
		{
			if (*p == '(')
				depth++;
			else if (*p == ')')
				depth--;
			p++;
		}
		while (*p && isspace((unsigned char) *p))
			p++;
	}
	else
	{
		while (*p)
		{
			if (pg_strncasecmp(p, "analyze", 7) == 0 ||
				pg_strncasecmp(p, "verbose", 7) == 0 ||
				pg_strncasecmp(p, "costs", 5) == 0 ||
				pg_strncasecmp(p, "buffers", 7) == 0 ||
				pg_strncasecmp(p, "timing", 6) == 0 ||
				pg_strncasecmp(p, "summary", 7) == 0 ||
				pg_strncasecmp(p, "settings", 8) == 0 ||
				pg_strncasecmp(p, "wal", 3) == 0)
			{
				while (*p && !isspace((unsigned char) *p))
					p++;
				while (*p && isspace((unsigned char) *p))
					p++;
				continue;
			}
			break;
		}
	}
	while (*p)
	{
		if (pg_strncasecmp(p, "select", 6) == 0 ||
			pg_strncasecmp(p, "with", 4) == 0 ||
			pg_strncasecmp(p, "insert", 6) == 0 ||
			pg_strncasecmp(p, "update", 6) == 0 ||
			pg_strncasecmp(p, "delete", 6) == 0)
			return p;
		p++;
	}
	return sql;
}

static void
normalize_sql(const char *sql, StringInfo buf)
{
	const char *p;
	const char *start = strip_explain_prefix(sql);

	initStringInfo(buf);
	for (p = start; *p; p++)
	{
		if (isspace((unsigned char) *p))
			continue;
		appendStringInfoChar(buf, pg_tolower((unsigned char) *p));
	}
}

static void
trim_whitespace(char *s)
{
	char	   *end;
	char	   *start = s;

	while (*start && isspace((unsigned char) *start))
		start++;
	if (start != s)
		memmove(s, start, strlen(start) + 1);
	if (*s == '\0')
		return;
	end = s + strlen(s) - 1;
	while (end >= s && isspace((unsigned char) *end))
	{
		*end = '\0';
		end--;
	}
}

static Scenario
scenario_from_string(const char *s)
{
	if (s == NULL || s[0] == '\0')
		return SC_NONE;
	if (pg_strcasecmp(s, "CM") == 0)
		return SC_CM;
	if (pg_strcasecmp(s, "CE") == 0)
		return SC_CE;
	if (pg_strcasecmp(s, "JN") == 0)
		return SC_JN;
	if (pg_strcasecmp(s, "CE+CM") == 0)
		return SC_CE_CM;
	if (pg_strcasecmp(s, "CE+JN") == 0)
		return SC_CE_JN;
	if (pg_strcasecmp(s, "CM+JN") == 0)
		return SC_CM_JN;
	if (pg_strcasecmp(s, "ALL") == 0 || pg_strcasecmp(s, "CE+CM+JN") == 0)
		return SC_ALL;
	if (pg_strcasecmp(s, "BASELINE") == 0 || pg_strcasecmp(s, "NONE") == 0)
		return SC_NONE;
	return SC_NONE;
}

static uint8
scenario_to_cb(Scenario s)
{
	switch (s)
	{
		case SC_CE: return 1;
		case SC_CM: return 2;
		case SC_JN: return 4;
		case SC_CE_CM: return 3;
		case SC_CE_JN: return 5;
		case SC_CM_JN: return 6;
		case SC_ALL: return 7;
		case SC_NONE:
		default:
			return 0;
	}
}

static Scenario
cb_to_scenario(uint8 cb)
{
	switch (cb & 7)
	{
		case 1: return SC_CE;
		case 2: return SC_CM;
		case 3: return SC_CE_CM;
		case 4: return SC_JN;
		case 5: return SC_CE_JN;
		case 6: return SC_CM_JN;
		case 7: return SC_ALL;
		case 0:
		default:
			return SC_NONE;
	}
}

static void
strip_round_suffix(char *sql_file)
{
	const char *suffix = "_round1";
	size_t		len = strlen(sql_file);
	size_t		suffix_len = strlen(suffix);

	if (len > suffix_len && strcmp(sql_file + len - suffix_len, suffix) == 0)
		sql_file[len - suffix_len] = '\0';
}

static uint32
cache_distance(uint32 a, uint32 b)
{
	uint64 diff = (a >= b) ? (uint64) (a - b) : (uint64) (b - a);
	return (uint32) diff;
}

static CacheBucket *
cache_find_bucket(uint32 hash)
{
	int i;

	for (i = 0; i < cache_bucket_count; i++)
	{
		if (cache_buckets[i].hash == hash)
			return &cache_buckets[i];
	}
	return NULL;
}

static CacheBucket *
cache_get_or_create_bucket(uint32 hash, bool *created)
{
	CacheBucket *bucket;

	if (created)
		*created = false;
	bucket = cache_find_bucket(hash);
	if (bucket)
		return bucket;
	if (cache_bucket_count >= MAX_CACHE_ENTRIES)
		return NULL;
	bucket = &cache_buckets[cache_bucket_count++];
	memset(bucket, 0, sizeof(*bucket));
	bucket->hash = hash;
	if (created)
		*created = true;
	return bucket;
}

static bool
cache_bucket_has_cb(const CacheBucket *bucket, uint8 cb)
{
	int i;

	if (bucket == NULL)
		return false;
	for (i = 0; i < bucket->count; i++)
	{
		if (bucket->slots[i].in_use && bucket->slots[i].cb == cb)
			return true;
	}
	return false;
}

static int
cache_slot_cmp(const void *a, const void *b)
{
	const CacheSlot *sa = (const CacheSlot *) a;
	const CacheSlot *sb = (const CacheSlot *) b;

	if (!sa->in_use && !sb->in_use)
		return 0;
	if (!sa->in_use)
		return 1;
	if (!sb->in_use)
		return -1;
	if (sa->t < sb->t)
		return -1;
	if (sa->t > sb->t)
		return 1;
	return (int) sa->v - (int) sb->v;
}

static void
cache_sort_bucket(CacheBucket *bucket)
{
	if (bucket == NULL || bucket->count <= 1)
		return;
	qsort(bucket->slots, bucket->count, sizeof(CacheSlot), cache_slot_cmp);
}

static void
cache_insert_slot(CacheBucket *bucket, const CacheSlot *slot)
{
	if (bucket == NULL || slot == NULL)
		return;
	if (bucket->count >= CACHE_SLOT_CAPACITY)
		return;
	bucket->slots[bucket->count] = *slot;
	bucket->slots[bucket->count].in_use = true;
	bucket->count++;
	cache_sort_bucket(bucket);
}

static uint8
cache_random_cb(void)
{
	if (!cache_prng_seeded)
	{
		pg_prng_seed(&cache_prng_state, (uint64) GetCurrentTimestamp());
		cache_prng_seeded = true;
	}
	return (uint8) pg_prng_uint64_range(&cache_prng_state, 0, 7);
}

static uint8
cache_random_cb_not_in_bucket(const CacheBucket *bucket)
{
	bool used[8] = {false};
	int i;
	int available[8];
	int count = 0;

	if (!cache_prng_seeded)
	{
		pg_prng_seed(&cache_prng_state, (uint64) GetCurrentTimestamp());
		cache_prng_seeded = true;
	}
	if (bucket)
	{
		for (i = 0; i < bucket->count; i++)
		{
			if (bucket->slots[i].in_use && bucket->slots[i].cb < 8)
				used[bucket->slots[i].cb] = true;
		}
	}
	for (i = 0; i < 8; i++)
	{
		if (!used[i])
			available[count++] = i;
	}
	if (count == 0)
		return cache_random_cb();
	return (uint8) available[pg_prng_uint64_range(&cache_prng_state, 0, count - 1)];
}

static int
collect_neighbors_bucket(const CacheBucket *bucket, uint32 sh, int k, const CacheSlot **out, bool skip_best)
{
	uint32 best_dist[3] = {UINT32_MAX, UINT32_MAX, UINT32_MAX};
	const CacheSlot *best_slots[3] = {NULL, NULL, NULL};
	int i;
	int limit = (k > 3) ? 3 : k;

	if (bucket == NULL || limit <= 0)
		return 0;
	for (i = 0; i < bucket->count; i++)
	{
		const CacheSlot *slot = &bucket->slots[i];
		uint32 dist;
		int j;

		if (!slot->in_use)
			continue;
		if (skip_best && i == 0)
			continue;
		dist = cache_distance(sh, slot->sh);
		for (j = 0; j < limit; j++)
		{
			if (dist < best_dist[j])
			{
				int m;
				for (m = limit - 1; m > j; m--)
				{
					best_dist[m] = best_dist[m - 1];
					best_slots[m] = best_slots[m - 1];
				}
				best_dist[j] = dist;
				best_slots[j] = slot;
				break;
			}
		}
	}
	for (i = 0; i < limit; i++)
		out[i] = best_slots[i];
	return limit;
}

static int
collect_neighbors_global(uint32 sh, int k, const CacheSlot **out)
{
	uint32 best_dist[3] = {UINT32_MAX, UINT32_MAX, UINT32_MAX};
	const CacheSlot *best_slots[3] = {NULL, NULL, NULL};
	int limit = (k > 3) ? 3 : k;
	int i;
	int j;

	if (limit <= 0)
		return 0;
	for (i = 0; i < cache_bucket_count; i++)
	{
		const CacheBucket *bucket = &cache_buckets[i];
		int s;
		for (s = 0; s < bucket->count; s++)
		{
			const CacheSlot *slot = &bucket->slots[s];
			uint32 dist;

			if (!slot->in_use)
				continue;
			dist = cache_distance(sh, slot->sh);
			for (j = 0; j < limit; j++)
			{
				if (dist < best_dist[j])
				{
					int m;
					for (m = limit - 1; m > j; m--)
					{
						best_dist[m] = best_dist[m - 1];
						best_slots[m] = best_slots[m - 1];
					}
					best_dist[j] = dist;
					best_slots[j] = slot;
					break;
				}
			}
		}
	}
	for (i = 0; i < limit; i++)
		out[i] = best_slots[i];
	return limit;
}

static uint8
vote_cb(const CacheSlot **slots, int nslots, const CacheBucket *bucket, bool avoid_duplicates)
{
	int counts[8] = {0};
	int i;
	int max = -1;
	int candidates[8];
	int cand_count = 0;

	if (!cache_prng_seeded)
	{
		pg_prng_seed(&cache_prng_state, (uint64) GetCurrentTimestamp());
		cache_prng_seeded = true;
	}
	for (i = 0; i < nslots; i++)
	{
		if (slots[i] && slots[i]->cb < 8)
			counts[slots[i]->cb]++;
	}
	for (i = 0; i < 8; i++)
	{
		if (avoid_duplicates && cache_bucket_has_cb(bucket, (uint8) i))
			continue;
		if (counts[i] > max)
		{
			max = counts[i];
			cand_count = 0;
			candidates[cand_count++] = i;
		}
		else if (counts[i] == max && max >= 0)
		{
			candidates[cand_count++] = i;
		}
	}
	if (cand_count == 0)
		return cache_random_cb_not_in_bucket(bucket);
	return (uint8) candidates[pg_prng_uint64_range(&cache_prng_state, 0, cand_count - 1)];
}

static uint8
propose_cb_initial(uint32 sh)
{
	const CacheSlot *neighbors[3] = {NULL, NULL, NULL};
	int n = collect_neighbors_global(sh, 3, neighbors);

	if (n <= 0)
		return cache_random_cb();
	return vote_cb(neighbors, n, NULL, false);
}

static uint8
propose_cb_best_biased(const CacheBucket *bucket, uint32 sh)
{
	const CacheSlot *neighbors[3] = {NULL, NULL, NULL};
	const CacheSlot *votes[4] = {NULL, NULL, NULL, NULL};
	int k = (bucket && bucket->count > 4) ? 1 : 3;
	int n = collect_neighbors_bucket(bucket, sh, k, neighbors, true);
	int i;

	if (bucket && bucket->count > 0)
		votes[0] = &bucket->slots[0];
	for (i = 0; i < n && i + 1 < 4; i++)
		votes[i + 1] = neighbors[i];
	return vote_cb(votes, 1 + n, bucket, true);
}

static void
compute_query_hashes(const char *query_string, uint32 *out_hash, uint32 *out_sh)
{
	StringInfoData norm;
	StringInfoData clean;

	*out_hash = 0;
	*out_sh = 0;
	if (query_string == NULL || query_string[0] == '\0')
		return;

	normalize_sql(query_string, &norm);
	sanitize_sql(query_string, &clean);
	*out_hash = hash_any((const unsigned char *) norm.data, norm.len);
	*out_sh = hash_any((const unsigned char *) clean.data, clean.len);
	pfree(norm.data);
	pfree(clean.data);
}

static bool
hash_sql_file(const char *filename, uint32 *out_hash, uint32 *out_sh)
{
	StringInfoData path;
	StringInfoData content;
	FILE	   *f;

	initStringInfo(&path);
	appendStringInfo(&path, "%s/%s", tee_query_dir, filename);
	f = open_with_fallback(path.data, "r");
	if (f == NULL)
	{
		pfree(path.data);
		return false;
	}
	initStringInfo(&content);
	while (!feof(f))
	{
		char		tmp[4096];
		size_t		nread = fread(tmp, 1, sizeof(tmp), f);

		if (nread > 0)
			appendBinaryStringInfo(&content, tmp, (int) nread);
	}
	FreeFile(f);
	if (content.len == 0)
	{
		pfree(path.data);
		pfree(content.data);
		return false;
	}
	compute_query_hashes(content.data, out_hash, out_sh);
	pfree(path.data);
	pfree(content.data);
	return true;
}

static void
generate_cache_file(void)
{
	FILE	   *src;
	FILE	   *dst;
	char		line[4096];
	int			line_no = 0;

	cache_bucket_count = 0;
	src = open_with_fallback(tee_source_csv, "r");
	if (src == NULL)
		return;
	dst = open_with_fallback(tee_cache_csv, "w");
	if (dst == NULL)
	{
		FreeFile(src);
		return;
	}
	fprintf(dst, "hash,version,time,sh,cb\n");
	while (fgets(line, sizeof(line), src) != NULL && cache_bucket_count < MAX_CACHE_ENTRIES)
	{
		char	   *sql_file = NULL;
		char	   *best = NULL;
		char	   *saveptr = NULL;
		uint32		h;
		uint32		sh;
		CacheBucket *bucket;
		CacheSlot	 slot;
		uint8		cb;

		line_no++;
		if (line_no == 1)
			continue;
		(void) strtok_r(line, ",", &saveptr);
		sql_file = strtok_r(NULL, ",", &saveptr);
		best = strtok_r(NULL, ",", &saveptr);
		if (!sql_file || !best)
			continue;
		trim_whitespace(sql_file);
		trim_whitespace(best);
		if (sql_file[0] == '\0' || best[0] == '\0')
			continue;
		strip_round_suffix(sql_file);
		if (!hash_sql_file(sql_file, &h, &sh))
			continue;
		cb = scenario_to_cb(scenario_from_string(best));
		bucket = cache_get_or_create_bucket(h, NULL);
		if (bucket == NULL)
			continue;
		if (cache_bucket_has_cb(bucket, cb))
			continue;
		memset(&slot, 0, sizeof(slot));
		slot.v = (uint8) bucket->count;
		slot.t = 0.0;
		slot.sh = sh;
		slot.cb = cb;
		slot.in_use = true;
		cache_insert_slot(bucket, &slot);
		fprintf(dst, "%u,%u,%.3f,%u,%u\n", h, slot.v, slot.t, sh, cb);
	}
	FreeFile(src);
	FreeFile(dst);
	cache_loaded = true;
}

static void
load_cache_if_needed(void)
{
	FILE	   *f;
	char		line[256];
	int			line_no = 0;

	if (cache_loaded)
		return;
	f = open_with_fallback(tee_cache_csv, "r");
	if (f == NULL)
	{
		generate_cache_file();
		return;
	}
	cache_bucket_count = 0;
	while (fgets(line, sizeof(line), f) != NULL && cache_bucket_count < MAX_CACHE_ENTRIES)
	{
		char	   *hash_s;
		char	   *col2;
		char	   *col3;
		char	   *col4;
		char	   *col5;
		char	   *saveptr = NULL;
		uint32		h;
		uint32		sh = 0;
		double		t = 0.0;
		uint8		v = 0;
		uint8		cb = 0;
		CacheBucket *bucket;
		CacheSlot	 slot;

		line_no++;
		if (line_no == 1)
			continue;
		hash_s = strtok_r(line, ",", &saveptr);
		col2 = strtok_r(NULL, ",", &saveptr);
		col3 = strtok_r(NULL, ",", &saveptr);
		col4 = strtok_r(NULL, ",", &saveptr);
		col5 = strtok_r(NULL, ",", &saveptr);
		if (!hash_s || !col2)
			continue;
		h = (uint32) strtoll(hash_s, NULL, 10);
		trim_whitespace(col2);
		if (col5 == NULL)
		{
			cb = scenario_to_cb(scenario_from_string(col2));
			sh = h;
			t = 0.0;
			v = 0;
		}
		else
		{
			trim_whitespace(col3);
			trim_whitespace(col4);
			trim_whitespace(col5);
			v = (uint8) strtoul(col2, NULL, 10);
			t = strtod(col3, NULL);
			sh = (uint32) strtoll(col4, NULL, 10);
			if (col5 && *col5)
			{
				if (isdigit((unsigned char) col5[0]))
					cb = (uint8) strtoul(col5, NULL, 10);
				else
					cb = scenario_to_cb(scenario_from_string(col5));
			}
		}
		bucket = cache_get_or_create_bucket(h, NULL);
		if (bucket == NULL)
			continue;
		if (bucket->count >= CACHE_SLOT_CAPACITY)
			continue;
		if (cache_bucket_has_cb(bucket, cb))
			continue;
		memset(&slot, 0, sizeof(slot));
		slot.v = v;
		slot.t = t;
		slot.sh = sh;
		slot.cb = cb & 7;
		slot.in_use = true;
		cache_insert_slot(bucket, &slot);
	}
	FreeFile(f);
	cache_loaded = true;
}

static bool
lookup_cache_for_query(const char *query_string, Scenario *out, bool *should_record,
						   uint32 *out_hash, uint32 *out_sh, uint8 *out_cb)
{
	uint32		h;
	uint32		sh;
	CacheBucket *bucket;
	uint8		cb;

	if (should_record)
		*should_record = false;
	if (!tee_adaptive_use_cache)
		return false;
	load_cache_if_needed();
	if (!cache_loaded)
		return false;
	compute_query_hashes(query_string, &h, &sh);
	if (h == 0 && sh == 0)
		return false;
	bucket = cache_find_bucket(h);
	if (bucket == NULL || bucket->count == 0)
	{
		if (!tee_adaptive_cache_populating)
			return false;
		bucket = cache_get_or_create_bucket(h, NULL);
		if (bucket == NULL)
			return false;
		cb = propose_cb_initial(sh);
		*out = cb_to_scenario(cb);
		if (should_record)
			*should_record = true;
		if (out_hash)
			*out_hash = h;
		if (out_sh)
			*out_sh = sh;
		if (out_cb)
			*out_cb = cb;
		return true;
	}

	cache_sort_bucket(bucket);
	if (bucket->count >= CACHE_SLOT_CAPACITY)
	{
		*out = cb_to_scenario(bucket->slots[0].cb);
		return true;
	}

	if (!tee_adaptive_cache_populating)
	{
		*out = cb_to_scenario(bucket->slots[0].cb);
		return true;
	}

	cb = propose_cb_best_biased(bucket, sh);
	*out = cb_to_scenario(cb);
	if (should_record)
		*should_record = true;
	if (out_hash)
		*out_hash = h;
	if (out_sh)
		*out_sh = sh;
	if (out_cb)
		*out_cb = cb;
	return true;
}

static void
cache_record_feedback(uint32 hash, uint32 sh, uint8 cb, double t)
{
	CacheBucket *bucket;
	CacheSlot	 slot;

	bucket = cache_get_or_create_bucket(hash, NULL);
	if (bucket == NULL)
		return;
	if (bucket->count >= CACHE_SLOT_CAPACITY)
		return;
	if (cache_bucket_has_cb(bucket, cb))
		return;
	memset(&slot, 0, sizeof(slot));
	slot.v = (uint8) bucket->count;
	slot.t = t;
	slot.sh = sh;
	slot.cb = cb & 7;
	slot.in_use = true;
	cache_insert_slot(bucket, &slot);
}

static void
cache_clear(void)
{
	memset(cache_buckets, 0, sizeof(cache_buckets));
	cache_bucket_count = 0;
	cache_loaded = false;
}

static void
cache_write_to_csv(void)
{
	FILE	   *f;
	int			i;

	if (!cache_loaded || cache_bucket_count == 0)
		return;

	f = open_with_fallback(tee_cache_csv, "w");
	if (f == NULL)
		return;

	fprintf(f, "hash,version,time,sh,cb\n");
	for (i = 0; i < cache_bucket_count; i++)
	{
		CacheBucket *bucket = &cache_buckets[i];
		int s;

		for (s = 0; s < bucket->count; s++)
		{
			CacheSlot *slot = &bucket->slots[s];

			if (!slot->in_use)
				continue;
			fprintf(f, "%u,%u,%.3f,%u,%u\n",
					bucket->hash,
					slot->v,
					slot->t,
					slot->sh,
					slot->cb);
		}
	}
	FreeFile(f);
}

static void
tee_adaptive_on_proc_exit(int code, Datum arg)
{
	if (tee_adaptive_use_cache && tee_adaptive_cache_populating)
	{
		cache_write_to_csv();
		cache_clear();
	}
}

static void
tee_adaptive_executor_start(QueryDesc *queryDesc, int eflags)
{
	if (cache_feedback_pending)
	{
		cache_feedback_start = GetCurrentTimestamp();
		cache_feedback_active = true;
	}

	if (prev_executor_start_hook)
		prev_executor_start_hook(queryDesc, eflags);
	else
		standard_ExecutorStart(queryDesc, eflags);
}

static void
tee_adaptive_executor_end(QueryDesc *queryDesc)
{
	if (prev_executor_end_hook)
		prev_executor_end_hook(queryDesc);
	else
		standard_ExecutorEnd(queryDesc);

	if (cache_feedback_active)
	{
		TimestampTz end = GetCurrentTimestamp();
		long		secs = 0;
		int			usecs = 0;
		double		elapsed_ms = 0.0;

		TimestampDifference(cache_feedback_start, end, &secs, &usecs);
		elapsed_ms = ((double) secs) * 1000.0 + ((double) usecs) / 1000.0;
		cache_record_feedback(cache_feedback_hash, cache_feedback_sh, cache_feedback_cb, elapsed_ms);
		cache_feedback_active = false;
		cache_feedback_pending = false;
	}
}

static bool
is_word_boundary(char c)
{
	return !isalnum((unsigned char) c) && c != '_';
}

static bool
contains_keyword_ci(const char *s, const char *kw)
{
	size_t		klen;
	size_t		slen;
	const char *p;
	const char *end;

	if (s == NULL || kw == NULL || *kw == '\0')
		return false;

	klen = strlen(kw);
	slen = strlen(s);
	if (slen < klen)
		return false;
	end = s + slen;

	for (p = s; p + klen <= end; p++)
	{
		char next = (p + klen < end) ? p[klen] : '\0';

		if (pg_strncasecmp(p, kw, klen) == 0 &&
			(p == s || is_word_boundary(p[-1])) &&
			is_word_boundary(next))
			return true;
	}
	return false;
}

static bool
contains_in_operator_ci(const char *s)
{
	const char *p;

	if (s == NULL)
		return false;

	for (p = s; *p; p++)
	{
		if (pg_strncasecmp(p, "in", 2) == 0 &&
			(p == s || is_word_boundary(p[-1])))
		{
			const char *q = p + 2;

			while (*q && isspace((unsigned char) *q))
				q++;
			if (*q == '(')
				return true;
		}
	}
	return false;
}

static void
sanitize_sql(const char *sql, StringInfo buf)
{
	bool		in_line_comment = false;
	bool		in_block_comment = false;
	bool		in_string = false;
	bool		last_space = true;
	const char *p;

	initStringInfo(buf);
	if (sql == NULL)
		return;

	for (p = sql; *p; p++)
	{
		char c = *p;

		if (in_line_comment)
		{
			if (c == '\n')
				in_line_comment = false;
			continue;
		}
		if (in_block_comment)
		{
			if (c == '*' && p[1] == '/')
			{
				in_block_comment = false;
				p++;
			}
			continue;
		}
		if (in_string)
		{
			if (c == '\'' && p[1] == '\'')
			{
				p++;
				continue;
			}
			if (c == '\'')
				in_string = false;
			continue;
		}
		if (c == '-' && p[1] == '-')
		{
			if (!last_space)
			{
				appendStringInfoChar(buf, ' ');
				last_space = true;
			}
			in_line_comment = true;
			p++;
			continue;
		}
		if (c == '/' && p[1] == '*')
		{
			if (!last_space)
			{
				appendStringInfoChar(buf, ' ');
				last_space = true;
			}
			in_block_comment = true;
			p++;
			continue;
		}
		if (c == '\'')
		{
			in_string = true;
			if (!last_space)
			{
				appendStringInfoChar(buf, ' ');
				last_space = true;
			}
			continue;
		}

		if (isspace((unsigned char) c))
		{
			if (!last_space)
			{
				appendStringInfoChar(buf, ' ');
				last_space = true;
			}
			continue;
		}

		appendStringInfoChar(buf, pg_tolower((unsigned char) c));
		last_space = false;
	}
}

static int
count_keyword(const char *s, const char *kw)
{
	size_t		klen;
	const char *p;
	const char *end;
	int			count = 0;

	if (s == NULL || kw == NULL || *kw == '\0')
		return 0;

	klen = strlen(kw);
	end = s + strlen(s);
	for (p = s; p + klen <= end; p++)
	{
		char next = (p + klen < end) ? p[klen] : '\0';

		if (strncmp(p, kw, klen) == 0 &&
			(p == s || is_word_boundary(p[-1])) &&
			is_word_boundary(next))
		{
			count++;
			p += klen - 1;
		}
	}
	return count;
}

static bool
has_keyword_pair(const char *s, const char *first, const char *second)
{
	size_t		flen;
	size_t		slen;
	const char *p;
	const char *end;

	if (s == NULL)
		return false;
	flen = strlen(first);
	slen = strlen(second);
	end = s + strlen(s);

	for (p = s; p + flen <= end; p++)
	{
		char next = (p + flen < end) ? p[flen] : '\0';

		if (strncmp(p, first, flen) != 0)
			continue;
		if (p != s && !is_word_boundary(p[-1]))
			continue;
		if (!is_word_boundary(next))
			continue;
		{
			const char *q = p + flen;

			while (*q && isspace((unsigned char) *q))
				q++;
			if (strncmp(q, second, slen) == 0 &&
				(is_word_boundary(q[slen]) || q[slen] == '\0'))
				return true;
		}
	}
	return false;
}

static int
count_subquery(const char *s)
{
	const char *p;
	int			count = 0;

	if (s == NULL)
		return 0;
	for (p = s; *p; p++)
	{
		if (*p == '(')
		{
			const char *q = p + 1;

			while (*q && isspace((unsigned char) *q))
				q++;
			if (strncmp(q, "select", 6) == 0 &&
				(is_word_boundary(q[6]) || q[6] == '\0'))
				count++;
		}
	}
	return count;
}

static int
count_function_calls(const char *s, const char *fname)
{
	size_t		flen;
	const char *p;
	const char *end;
	int			count = 0;

	if (s == NULL || fname == NULL || *fname == '\0')
		return 0;
	flen = strlen(fname);
	end = s + strlen(s);
	for (p = s; p + flen <= end; p++)
	{
		if (strncmp(p, fname, flen) != 0)
			continue;
		if (p != s && !is_word_boundary(p[-1]))
			continue;
		{
			const char *q = p + flen;

			if (!is_word_boundary(*q))
				continue;
			while (*q && isspace((unsigned char) *q))
				q++;
			if (*q == '(')
				count++;
		}
	}
	return count;
}

static void
normalize_table_token(char *token)
{
	char	   *src;
	char	   *dst;
	char	   *dot;

	dst = token;
	for (src = token; *src; src++)
	{
		if (*src == '"')
			continue;
		*dst++ = *src;
	}
	*dst = '\0';

	dot = strrchr(token, '.');
	if (dot && dot[1] != '\0')
		memmove(token, dot + 1, strlen(dot + 1) + 1);
}

static void
mark_table_if_known(WorkloadType workload, const char *token, bool *seen, int table_count)
{
	char		buf[128];
	int			idx;

	if (token == NULL || token[0] == '\0')
		return;
	strlcpy(buf, token, sizeof(buf));
	normalize_table_token(buf);
	idx = workload_table_index(workload, buf);
	if (idx >= 0 && idx < table_count)
		seen[idx] = true;
}

static void
collect_tables_top_level(const char *s, WorkloadType workload, bool *seen, int table_count)
{
	static const char *stop_tokens[] = {
		"where", "group", "order", "having", "limit", "union", "intersect", "except"
	};
	static const char *join_tokens[] = {
		"join", "inner", "left", "right", "full", "cross"
	};
	static const char *skip_tokens[] = {
		"select", "on", "as"
	};
	int			depth = 0;
	bool		in_from = false;
	bool		expect_table = false;
	bool		done = false;
	char		token[128];
	int			token_len = 0;
	const char *p;

	if (s == NULL)
		return;

	for (p = s; ; p++)
	{
		char c = *p;

		if (c == '(')
		{
			depth++;
			if (token_len > 0)
			{
				token[token_len] = '\0';
				token_len = 0;
			}
			continue;
		}
		if (c == ')')
		{
			if (depth > 0)
				depth--;
			if (token_len > 0)
			{
				token[token_len] = '\0';
				token_len = 0;
			}
			continue;
		}

		if (depth > 0)
		{
			if (c == '\0')
				break;
			continue;
		}

		if (isalnum((unsigned char) c) || c == '_' || c == '.' || c == '"')
		{
			if (token_len < (int) sizeof(token) - 1)
				token[token_len++] = c;
			continue;
		}

		if (token_len > 0)
		{
			int i;
			bool is_stop = false;
			bool is_join = false;
			bool is_skip = false;

			token[token_len] = '\0';
			token_len = 0;

			for (i = 0; i < (int) (sizeof(stop_tokens) / sizeof(stop_tokens[0])); i++)
			{
				if (strcmp(token, stop_tokens[i]) == 0)
				{
					is_stop = true;
					break;
				}
			}
			if (is_stop && in_from)
			{
				done = true;
				break;
			}

			if (strcmp(token, "from") == 0)
			{
				in_from = true;
				expect_table = true;
				goto token_done;
			}

			if (!in_from)
				goto token_done;

			for (i = 0; i < (int) (sizeof(join_tokens) / sizeof(join_tokens[0])); i++)
			{
				if (strcmp(token, join_tokens[i]) == 0)
				{
					is_join = true;
					break;
				}
			}
			if (is_join)
			{
				if (strcmp(token, "join") == 0)
					expect_table = true;
				goto token_done;
			}

			for (i = 0; i < (int) (sizeof(skip_tokens) / sizeof(skip_tokens[0])); i++)
			{
				if (strcmp(token, skip_tokens[i]) == 0)
				{
					is_skip = true;
					break;
				}
			}

			if (expect_table && !is_skip)
			{
				mark_table_if_known(workload, token, seen, table_count);
				expect_table = false;
			}
		}

token_done:
		if (c == ',')
			expect_table = in_from;
		if (c == '\0')
			break;
	}
	if (done)
		return;
}

static bool
match_keyword_at(const char *s, const char *p, const char *kw)
{
	size_t		klen = strlen(kw);
	size_t		remain = strlen(p);
	char		next;

	if (remain < klen)
		return false;
	if (strncmp(p, kw, klen) != 0)
		return false;
	if (p != s && !is_word_boundary(p[-1]))
		return false;
	next = (remain > klen) ? p[klen] : '\0';
	if (!is_word_boundary(next))
		return false;
	return true;
}

static void
collect_tables_global(const char *s, WorkloadType workload, bool *seen, int table_count)
{
	const char *p;

	if (s == NULL)
		return;

	for (p = s; *p; p++)
	{
		const char *kw = NULL;

		if (match_keyword_at(s, p, "from"))
			kw = "from";
		else if (match_keyword_at(s, p, "join"))
			kw = "join";

		if (kw)
		{
			const char *q = p + strlen(kw);
			char		token[128];
			int			token_len = 0;

			while (*q && isspace((unsigned char) *q))
				q++;
			if (*q == '(')
				continue;

			while (*q && (isalnum((unsigned char) *q) || *q == '_' || *q == '.' || *q == '"'))
			{
				if (token_len < (int) sizeof(token) - 1)
					token[token_len++] = *q;
				q++;
			}
			token[token_len] = '\0';
			if (token_len > 0)
				mark_table_if_known(workload, token, seen, table_count);
			p = q;
		}
	}
}

static void
extract_text_metrics(const char *query_string, QueryFeatures *feats)
{
	StringInfoData clean;
	const TableRowsIndex *tables;
	int			table_count;
	bool	   *seen = NULL;
	int			i;

	feats->join_count = 0;
	feats->subquery_count = 0;
	feats->has_having = 0;
	feats->has_union = 0;
	feats->has_exists = 0;
	feats->has_case = 0;
	feats->agg_func_count = 0;
	feats->window_func_count = 0;
	feats->table_count_est = 0;
	feats->where_terms_est = 0;
	feats->or_ratio = 0.0;
	feats->table_mentioned_count = 0;
	feats->table_rows_sum = 0.0;
	feats->table_rows_mean = 0.0;
	feats->table_rows_max = 0.0;
	feats->table_rows_min = 0.0;
	feats->table_index_count_sum = 0.0;
	feats->table_index_count_mean = 0.0;
	feats->pct_tables_with_index = 0.0;

	feats->has_group_by = false;
	feats->has_sort = false;
	feats->has_distinct = false;
	feats->has_limit = false;
	feats->has_like = false;
	feats->has_in = false;
	feats->has_between = false;
	feats->has_window_funcs = false;
	feats->has_aggregates = false;

	if (query_string == NULL || query_string[0] == '\0')
		return;

	sanitize_sql(query_string, &clean);

	feats->join_count = count_keyword(clean.data, "join");
	feats->subquery_count = count_subquery(clean.data);
	feats->has_group_by = has_keyword_pair(clean.data, "group", "by");
	feats->has_sort = has_keyword_pair(clean.data, "order", "by");
	feats->has_having = contains_keyword_ci(clean.data, "having") ? 1 : 0;
	feats->has_distinct = contains_keyword_ci(clean.data, "distinct") ? 1 : 0;
	feats->has_limit = contains_keyword_ci(clean.data, "limit") ? 1 : 0;
	feats->has_union = contains_keyword_ci(clean.data, "union") ? 1 : 0;
	feats->has_exists = contains_keyword_ci(clean.data, "exists") ? 1 : 0;
	feats->has_in = contains_in_operator_ci(clean.data) ? 1 : 0;
	feats->has_like = contains_keyword_ci(clean.data, "like") ? 1 : 0;
	feats->has_between = contains_keyword_ci(clean.data, "between") ? 1 : 0;
	feats->has_case = contains_keyword_ci(clean.data, "case") ? 1 : 0;

	feats->agg_func_count =
		count_function_calls(clean.data, "sum") +
		count_function_calls(clean.data, "avg") +
		count_function_calls(clean.data, "min") +
		count_function_calls(clean.data, "max") +
		count_function_calls(clean.data, "count");
	feats->window_func_count = count_function_calls(clean.data, "over");
	feats->has_window_funcs = (feats->window_func_count > 0);
	feats->has_aggregates = (feats->agg_func_count > 0);

	{
		const char *from_ptr = NULL;
		const char *p;
		const char *stop_ptr = NULL;
		static const char *stop_tokens[] = {
			"where", "group", "order", "having", "limit", "union", "intersect", "except"
		};

		for (p = clean.data; *p; p++)
		{
			if (match_keyword_at(clean.data, p, "from"))
			{
				from_ptr = p + 4;
				break;
			}
		}

		if (from_ptr)
		{
			int comma_count = 0;
			bool has_token = false;
			int depth = 0;
			int j;

			for (p = from_ptr; *p; p++)
			{
				for (j = 0; j < (int) (sizeof(stop_tokens) / sizeof(stop_tokens[0])); j++)
				{
					if (match_keyword_at(clean.data, p, stop_tokens[j]))
					{
						stop_ptr = p;
						break;
					}
				}
				if (stop_ptr)
					break;
			}
			if (stop_ptr == NULL)
				stop_ptr = clean.data + strlen(clean.data);

			for (p = from_ptr; p < stop_ptr; p++)
			{
				if (*p == '(')
				{
					depth++;
					continue;
				}
				if (*p == ')' && depth > 0)
				{
					depth--;
					continue;
				}
				if (depth > 0)
					continue;
				if (*p == ',')
					comma_count++;
				if (!isspace((unsigned char) *p))
					has_token = true;
			}
			feats->table_count_est = has_token ? comma_count + feats->join_count + 1 : 0;
		}
	}

	{
		const char *where_ptr = NULL;
		const char *p;
		const char *stop_ptr = NULL;
		static const char *stop_tokens[] = {
			"group", "order", "having", "limit", "union", "intersect", "except"
		};

		for (p = clean.data; *p; p++)
		{
			if (match_keyword_at(clean.data, p, "where"))
			{
				where_ptr = p + 5;
				break;
			}
		}
		if (where_ptr)
		{
			int ands = 0;
			int ors = 0;

			for (p = where_ptr; *p; p++)
			{
				int j;

				for (j = 0; j < (int) (sizeof(stop_tokens) / sizeof(stop_tokens[0])); j++)
				{
					if (match_keyword_at(clean.data, p, stop_tokens[j]))
					{
						stop_ptr = p;
						break;
					}
				}
				if (stop_ptr)
					break;
			}
			if (stop_ptr == NULL)
				stop_ptr = clean.data + strlen(clean.data);

			{
				char *where_buf;
				size_t len = stop_ptr - where_ptr;

				where_buf = (char *) palloc(len + 1);
				memcpy(where_buf, where_ptr, len);
				where_buf[len] = '\0';
				ands = count_keyword(where_buf, "and");
				ors = count_keyword(where_buf, "or");
				pfree(where_buf);
			}

			feats->num_and_quals = ands;
			feats->num_or_quals = ors;
			feats->where_terms_est = ands + ors + 1;
			feats->or_ratio = (ands + ors) > 0 ? ((double) ors / (double) (ands + ors)) : 0.0;
		}
		else
		{
			feats->where_terms_est = 0;
			feats->num_and_quals = 0;
			feats->num_or_quals = 0;
			feats->or_ratio = 0.0;
		}
	}

	tables = get_workload_tables(data_profile.workload, &table_count);
	if (tables && table_count > 0)
	{
		int with_index = 0;

		seen = (bool *) palloc0(sizeof(bool) * table_count);
		collect_tables_top_level(clean.data, data_profile.workload, seen, table_count);
		collect_tables_global(clean.data, data_profile.workload, seen, table_count);

		for (i = 0; i < table_count; i++)
		{
			if (!seen[i])
				continue;
			feats->table_mentioned_count++;
			feats->table_rows_sum += tables[i].rows;
			feats->table_index_count_sum += tables[i].indexes;
			if (tables[i].rows > feats->table_rows_max)
				feats->table_rows_max = tables[i].rows;
			if (feats->table_rows_min == 0.0 || tables[i].rows < feats->table_rows_min)
				feats->table_rows_min = tables[i].rows;
			if (tables[i].indexes > 0)
				with_index++;
		}

		if (feats->table_mentioned_count > 0)
		{
			feats->table_rows_mean = feats->table_rows_sum /
				(double) feats->table_mentioned_count;
			feats->table_index_count_mean = feats->table_index_count_sum /
				(double) feats->table_mentioned_count;
			feats->pct_tables_with_index = (double) with_index /
				(double) feats->table_mentioned_count;
		}
		pfree(seen);
	}

	pfree(clean.data);
}

static double
metric_value(const QueryFeatures *feats, MetricId id)
{
	switch (id)
	{
		case MET_JOIN_COUNT: return feats->join_count;
		case MET_SUBQUERY_COUNT: return feats->subquery_count;
		case MET_HAS_GROUP_BY: return feats->has_group_by ? 1.0 : 0.0;
		case MET_HAS_ORDER_BY: return feats->has_sort ? 1.0 : 0.0;
		case MET_HAS_HAVING: return feats->has_having ? 1.0 : 0.0;
		case MET_HAS_DISTINCT: return feats->has_distinct ? 1.0 : 0.0;
		case MET_HAS_LIMIT: return feats->has_limit ? 1.0 : 0.0;
		case MET_HAS_UNION: return feats->has_union ? 1.0 : 0.0;
		case MET_HAS_EXISTS: return feats->has_exists ? 1.0 : 0.0;
		case MET_HAS_IN: return feats->has_in ? 1.0 : 0.0;
		case MET_HAS_LIKE: return feats->has_like ? 1.0 : 0.0;
		case MET_HAS_BETWEEN: return feats->has_between ? 1.0 : 0.0;
		case MET_HAS_CASE: return feats->has_case ? 1.0 : 0.0;
		case MET_AGG_FUNC_COUNT: return feats->agg_func_count;
		case MET_WINDOW_FUNC_COUNT: return feats->window_func_count;
		case MET_TABLE_COUNT_EST: return feats->table_count_est;
		case MET_WHERE_TERMS_EST: return feats->where_terms_est;
		case MET_OR_COUNT: return feats->num_or_quals;
		case MET_AND_COUNT: return feats->num_and_quals;
		case MET_OR_RATIO: return feats->or_ratio;
		case MET_TABLE_MENTIONED_COUNT: return feats->table_mentioned_count;
		case MET_TABLE_ROWS_SUM: return feats->table_rows_sum;
		case MET_TABLE_ROWS_MEAN: return feats->table_rows_mean;
		case MET_TABLE_ROWS_MAX: return feats->table_rows_max;
		case MET_TABLE_ROWS_MIN: return feats->table_rows_min;
		case MET_TABLE_INDEX_SUM: return feats->table_index_count_sum;
		case MET_TABLE_INDEX_MEAN: return feats->table_index_count_mean;
		case MET_PCT_TABLES_WITH_INDEX: return feats->pct_tables_with_index;
	}
	return 0.0;
}

static const MetricRule *
get_component_rules(ComponentId comp, WorkloadType workload, int *rule_count)
{
	switch (comp)
	{
		case COMP_CE:
			switch (workload)
			{
				case WL_JOB:
					*rule_count = (int) (sizeof(ce_job_rules) / sizeof(ce_job_rules[0]));
					return ce_job_rules;
				case WL_CEB:
					*rule_count = (int) (sizeof(ce_ceb_rules) / sizeof(ce_ceb_rules[0]));
					return ce_ceb_rules;
				case WL_STACK:
					*rule_count = (int) (sizeof(ce_stack_rules) / sizeof(ce_stack_rules[0]));
					return ce_stack_rules;
				case WL_TPCDS:
					*rule_count = (int) (sizeof(ce_tpc_ds_rules) / sizeof(ce_tpc_ds_rules[0]));
					return ce_tpc_ds_rules;
			}
			break;
		case COMP_CM:
			switch (workload)
			{
				case WL_JOB:
					*rule_count = (int) (sizeof(cm_job_rules) / sizeof(cm_job_rules[0]));
					return cm_job_rules;
				case WL_CEB:
					*rule_count = (int) (sizeof(cm_ceb_rules) / sizeof(cm_ceb_rules[0]));
					return cm_ceb_rules;
				case WL_STACK:
					*rule_count = (int) (sizeof(cm_stack_rules) / sizeof(cm_stack_rules[0]));
					return cm_stack_rules;
				case WL_TPCDS:
					*rule_count = (int) (sizeof(cm_tpc_ds_rules) / sizeof(cm_tpc_ds_rules[0]));
					return cm_tpc_ds_rules;
			}
			break;
		case COMP_JN:
			switch (workload)
			{
				case WL_JOB:
					*rule_count = (int) (sizeof(jn_job_rules) / sizeof(jn_job_rules[0]));
					return jn_job_rules;
				case WL_CEB:
					*rule_count = (int) (sizeof(jn_ceb_rules) / sizeof(jn_ceb_rules[0]));
					return jn_ceb_rules;
				case WL_STACK:
					*rule_count = (int) (sizeof(jn_stack_rules) / sizeof(jn_stack_rules[0]));
					return jn_stack_rules;
				case WL_TPCDS:
					*rule_count = (int) (sizeof(jn_tpc_ds_rules) / sizeof(jn_tpc_ds_rules[0]));
					return jn_tpc_ds_rules;
			}
			break;
	}

	*rule_count = 0;
	return NULL;
}

static double
component_threshold(ComponentId comp, WorkloadType workload)
{
	double ce_thresh[] = {0.55, 0.80, 1.00, 0.00};
	double cm_thresh[] = {0.55, 0.65, 0.00, 1.00};
	double jn_thresh[] = {0.65, 0.75, 1.00, 0.00};
	int			idx = (int) workload;

	if (idx < 0 || idx >= 4)
		idx = 0;
	switch (comp)
	{
		case COMP_CE: return ce_thresh[idx];
		case COMP_CM: return cm_thresh[idx];
		case COMP_JN: return jn_thresh[idx];
	}
	return 1.0;
}

static double
score_component(const MetricRule *rules, int rule_count, const QueryFeatures *feats)
{
	double score = 0.0;
	double total = 0.0;
	int			i;

	if (rules == NULL || rule_count <= 0)
		return 0.0;

	for (i = 0; i < rule_count; i++)
	{
		double val;
		bool	pass = false;

		if (rules[i].weight <= 0.0)
			continue;
		total += rules[i].weight;
		val = metric_value(feats, rules[i].id);
		if (rules[i].direction > 0)
			pass = (val >= rules[i].threshold);
		else if (rules[i].direction < 0)
			pass = (val <= rules[i].threshold);

		if (pass)
			score += rules[i].weight;
	}

	if (total <= 0.0)
		return 0.0;
	return score / total;
}

/* Thresholds for strategy decision */
#define THRESHOLD_SIMPLE_RELATIONS 1
#define THRESHOLD_MODERATE_RELATIONS 4
#define THRESHOLD_COMPLEX_RELATIONS 7
#define THRESHOLD_HUGE_RELATIONS 14

#define THRESHOLD_SMALL_ROWS 120000.0
#define THRESHOLD_MEDIUM_ROWS 1500000.0
#define THRESHOLD_LARGE_ROWS 5000000.0
#define THRESHOLD_HUGE_DATA_ROWS 20000000.0 /* IMDB: very large tables */
#define THRESHOLD_INDEX_PER_TABLE 2.0

static WorkloadType
parse_workload(const char *name)
{
	if (name == NULL)
		return WL_JOB;
	if (pg_strcasecmp(name, "job") == 0)
		return WL_JOB;
	if (pg_strcasecmp(name, "ceb") == 0)
		return WL_CEB;
	if (pg_strcasecmp(name, "stack") == 0)
		return WL_STACK;
	if (pg_strcasecmp(name, "tpcds") == 0 ||
		pg_strcasecmp(name, "tpc-ds") == 0 ||
		pg_strcasecmp(name, "tpc_ds") == 0)
		return WL_TPCDS;
	return WL_JOB;
}

static void
load_profile_for_workload(DataProfile *profile, WorkloadType workload)
{
	const TableRowsIndex *tables;
	int			table_count;
	int			i;

	memset(profile, 0, sizeof(*profile));
	tables = get_workload_tables(workload, &table_count);
	profile->table_count = table_count;
	profile->workload = workload;
	profile->imdb_mode = (workload == WL_JOB || workload == WL_CEB);
	profile->fk_count = profile->imdb_mode ? IMDB_FK_COUNT : 0;

	for (i = 0; i < table_count; i++)
	{
		double rel_rows = tables[i].rows;

		if (rel_rows <= 0)
			continue;
		profile->total_rows += rel_rows;
		if (rel_rows > profile->max_rows)
			profile->max_rows = rel_rows;
		if (rel_rows >= THRESHOLD_MEDIUM_ROWS)
			profile->large_table_count++;
		if (rel_rows >= THRESHOLD_HUGE_DATA_ROWS)
			profile->huge_table_count++;
		profile->index_count += tables[i].indexes;
	}

	if (profile->total_rows > 0)
		profile->max_ratio = profile->max_rows / profile->total_rows;
	if (profile->table_count > 0)
		profile->fk_per_table = (double) profile->fk_count /
			(double) profile->table_count;
	if (profile->table_count > 0)
		profile->index_per_table = (double) profile->index_count /
			(double) profile->table_count;

	profile->join_dense = (profile->fk_per_table >= 0.9);
	profile->skewed = (profile->max_ratio >= 0.60);
	profile->large_db = (profile->total_rows >= 100000000.0);
	profile->index_dense = (profile->index_per_table >= THRESHOLD_INDEX_PER_TABLE);
	profile->initialized = true;
}

static void
load_data_profile_if_needed(void)
{
	WorkloadType workload;

	workload = parse_workload(tee_adaptive_workload);
	if (data_profile.initialized && data_profile.workload == workload)
		return;

	load_profile_for_workload(&data_profile, workload);

	elog(DEBUG1,
		 "TEE Adaptive: Data profile tables=%d fks=%d idx=%d total_rows=%.0f max_ratio=%.2f join_dense=%s skewed=%s large_db=%s index_dense=%s",
		 data_profile.table_count, data_profile.fk_count, data_profile.index_count, data_profile.total_rows,
		 data_profile.max_ratio,
		 data_profile.join_dense ? "true" : "false",
		 data_profile.skewed ? "true" : "false",
		 data_profile.large_db ? "true" : "false",
		 data_profile.index_dense ? "true" : "false");
}


/*
 * extract_query_features
 *		Traverses the query range table to extract basic features.
 *		Keeps overhead low by using system caches instead of robust statistics.
 */
static void
count_quals(Node *node, int *quals, int *ands, int *ors)
{
	ListCell   *lc;

	if (node == NULL)
		return;

	switch (nodeTag(node))
	{
		case T_BoolExpr:
			{
				BoolExpr   *b = (BoolExpr *) node;

				if (b->boolop == AND_EXPR)
					(*ands)++;
				else if (b->boolop == OR_EXPR)
					(*ors)++;

				foreach(lc, b->args)
					count_quals((Node *) lfirst(lc), quals, ands, ors);
			}
			break;
		case T_OpExpr:
		case T_FuncExpr:
		case T_NullTest:
		case T_BooleanTest:
		case T_RelabelType:
		case T_DistinctExpr:
		case T_ScalarArrayOpExpr:
			(*quals)++;
			break;
		default:
			/* Recurse into generic expressions if they have children */
			break;
	}
}

static void
extract_query_features(Query *parse, QueryFeatures *feats)
{
	ListCell   *lc;

	load_data_profile_if_needed();

	feats->num_relations = 0;
	feats->estimated_total_rows = 0;
	feats->max_rel_rows = 0;
	feats->small_rel_count = 0;
	feats->large_rel_count = 0;
	feats->indexed_rel_count = 0;
	feats->index_total_count = 0;
	feats->avg_index_per_rel = 0.0;
	feats->num_quals = 0;
	feats->num_and_quals = 0;
	feats->num_or_quals = 0;
	feats->has_aggregates = false;
	feats->has_group_by = false;
	feats->has_distinct = false;
	feats->has_sort = false;
	feats->has_limit = false;
	feats->has_sublinks = false;
	feats->has_window_funcs = false;
	feats->has_like = false;
	feats->has_in = false;
	feats->has_between = false;

	feats->has_aggregates = parse->hasAggs;
	feats->has_group_by = (parse->groupClause != NIL);
	feats->has_distinct = (parse->distinctClause != NIL);
	feats->has_sort = (parse->sortClause != NIL);
	feats->has_limit = (parse->limitCount != NULL || parse->limitOffset != NULL);
	feats->has_sublinks = parse->hasSubLinks;
	feats->has_window_funcs = parse->hasWindowFuncs;

	/*
	 * We only count RTE_RELATION entries in the main query's rtable.
	 * This gives a good proxy for Join complexity.
	 */
	foreach(lc, parse->rtable)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);

		if (rte->rtekind == RTE_RELATION)
		{
			Oid			relid;
			HeapTuple	tuple;

			feats->num_relations++;

			/*
			 * Quickly fetch rough row count from pg_class system cache.
			 * This is much faster than calling the full planner statistics.
			 */
			relid = rte->relid;
			tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));

			if (HeapTupleIsValid(tuple))
			{
				Form_pg_class classForm = (Form_pg_class) GETSTRUCT(tuple);

				/* reltuples might be -1 if the table hasn't been analyzed yet */
				{
					double rel_rows = 0.0;
					int index_count = 0;

					if (lookup_table_rows_index(data_profile.workload,
												NameStr(classForm->relname),
												&rel_rows,
												&index_count))
					{
						if (rel_rows > 0)
						{
							feats->estimated_total_rows += rel_rows;
							if (rel_rows > feats->max_rel_rows)
								feats->max_rel_rows = rel_rows;
							if (rel_rows <= THRESHOLD_SMALL_ROWS)
								feats->small_rel_count++;
							if (rel_rows >= THRESHOLD_MEDIUM_ROWS)
								feats->large_rel_count++;
						}
						feats->index_total_count += index_count;
						if (index_count > 0)
							feats->indexed_rel_count++;
					}
				}

				ReleaseSysCache(tuple);
			}
		}
	}

	elog(DEBUG2, "TEE Adaptive: Extracted Features - Rels: %d, EstRows: %.0f, MaxRel: %.0f, Small: %d, Large: %d",
		 feats->num_relations, feats->estimated_total_rows, feats->max_rel_rows,
		 feats->small_rel_count, feats->large_rel_count);

	if (feats->num_relations > 0 && feats->index_total_count > 0)
		feats->avg_index_per_rel = (double) feats->index_total_count /
			(double) feats->num_relations;

	/* Count simple qual structure from jointree */
	if (parse->jointree && parse->jointree->quals)
		count_quals(parse->jointree->quals,
					&feats->num_quals,
					&feats->num_and_quals,
					&feats->num_or_quals);

	elog(DEBUG2, "TEE Adaptive: Quals total=%d ANDs=%d ORs=%d",
		 feats->num_quals, feats->num_and_quals, feats->num_or_quals);
}

static void
apply_adaptive_strategy(QueryFeatures *feats, const char *query_string)
{
	bool jn_on = false;
	bool ce_on = false;
	bool cm_on = false;
	const char *label = "Auto (weighted)";
	Scenario cached = SC_NONE;
	const char *raw_query = query_string ? query_string : debug_query_string;
	WorkloadType workload;
	double ce_score = 0.0;
	double cm_score = 0.0;
	double jn_score = 0.0;
	int rule_count = 0;
	const MetricRule *rules = NULL;
	bool cache_should_record = false;
	uint32 cache_hash = 0;
	uint32 cache_sh = 0;
	uint8 cache_cb = 0;

	cache_feedback_pending = false;
	cache_feedback_active = false;

	if (raw_query && lookup_cache_for_query(raw_query, &cached, &cache_should_record,
									   &cache_hash, &cache_sh, &cache_cb))
	{
		jn_on = ce_on = cm_on = false;
		switch (cached)
		{
			case SC_NONE: break;
			case SC_CM: cm_on = true; break;
			case SC_CE: ce_on = true; break;
			case SC_JN: jn_on = true; break;
			case SC_CE_CM: ce_on = true; cm_on = true; break;
			case SC_CE_JN: ce_on = true; jn_on = true; break;
			case SC_CM_JN: cm_on = true; jn_on = true; break;
			case SC_ALL: ce_on = cm_on = jn_on = true; break;
		}
		label = cache_should_record ? "Cache (update)" : "Cache";
		SetConfigOption(GUC_JN_ENABLE, jn_on ? "on" : "off", PGC_USERSET, PGC_S_SESSION);
		SetConfigOption(GUC_CE_ENABLE, ce_on ? "on" : "off", PGC_USERSET, PGC_S_SESSION);
		SetConfigOption(GUC_CM_ENABLE, cm_on ? "on" : "off", PGC_USERSET, PGC_S_SESSION);
		if (cache_should_record)
		{
			cache_feedback_pending = true;
			cache_feedback_hash = cache_hash;
			cache_feedback_sh = cache_sh;
			cache_feedback_cb = cache_cb;
		}
		log_strategy_decision(label,
							  jn_on ? "on" : "off",
							  ce_on ? "on" : "off",
							  cm_on ? "on" : "off");
		return;
	}

	load_data_profile_if_needed();
	extract_text_metrics(raw_query, feats);
	workload = data_profile.workload;

	if (raw_query == NULL || raw_query[0] == '\0')
	{
		jn_on = ce_on = cm_on = false;
		goto apply;
	}

	if (feats->table_mentioned_count == 0 &&
		feats->table_count_est == 0 &&
		feats->join_count == 0)
	{
		jn_on = ce_on = cm_on = false;
		goto apply;
	}

	rules = get_component_rules(COMP_CE, workload, &rule_count);
	ce_score = score_component(rules, rule_count, feats);
	rules = get_component_rules(COMP_CM, workload, &rule_count);
	cm_score = score_component(rules, rule_count, feats);
	rules = get_component_rules(COMP_JN, workload, &rule_count);
	jn_score = score_component(rules, rule_count, feats);

	ce_on = (ce_score >= component_threshold(COMP_CE, workload));
	cm_on = (cm_score >= component_threshold(COMP_CM, workload));
	jn_on = (jn_score >= component_threshold(COMP_JN, workload));

apply:
	/* Label for logging */
	if (!jn_on && !ce_on && !cm_on)
		label = "Auto: None";
	else if (!jn_on && !ce_on && cm_on)
		label = "Auto: CM";
	else if (!jn_on && ce_on && !cm_on)
		label = "Auto: CE";
	else if (jn_on && !ce_on && !cm_on)
		label = "Auto: JN";
	else if (!jn_on && ce_on && cm_on)
		label = "Auto: CE+CM";
	else if (jn_on && ce_on && !cm_on)
		label = "Auto: CE+JN";
	else if (jn_on && !ce_on && cm_on)
		label = "Auto: CM+JN";
	else
		label = "Auto: ALL";

	/* Apply */
	SetConfigOption(GUC_JN_ENABLE, jn_on ? "on" : "off", PGC_USERSET, PGC_S_SESSION);
	SetConfigOption(GUC_CE_ENABLE, ce_on ? "on" : "off", PGC_USERSET, PGC_S_SESSION);
	SetConfigOption(GUC_CM_ENABLE, cm_on ? "on" : "off", PGC_USERSET, PGC_S_SESSION);

	log_strategy_decision(label,
						  jn_on ? "on" : "off",
						  ce_on ? "on" : "off",
						  cm_on ? "on" : "off");
}


static PlannedStmt *
tee_adaptive_planner_hook(Query *parse, const char *query_string, int cursorOptions, ParamListInfo boundParams)
{
	/* Only intervene if enabled and it's a plannable statement (e.g., SELECT, INSERT...) */
	if (tee_adaptive_enable && parse->commandType != CMD_UTILITY)
	{
		QueryFeatures feats;

		/* 1. Analyze the query */
		extract_query_features(parse, &feats);

		/* 2. Apply GUC settings dynamically */
		/*
		 * Note: We use TRY-CATCH here because SetConfigOption might error out
		 * if the underlying extensions are not loaded. We want to fall back
		 * gracefully to the standard planner in that case.
		 */
		PG_TRY();
		{
			apply_adaptive_strategy(&feats, query_string);
		}
		PG_CATCH();
		{
			/* If setting GUCs failed, log a warning and proceed without adaptive logic */
			elog(WARNING, "TEE Adaptive: Failed to set extension options. Are underlying TEE extensions loaded? Proceeding with standard planner.");
			FlushErrorState();
		}
		PG_END_TRY();
	}

	/* 3. Call the next planner in the chain (which will use the new GUC settings) */
	if (prev_planner_hook)
		return prev_planner_hook(parse, query_string, cursorOptions, boundParams);
	else
		return standard_planner(parse, query_string, cursorOptions, boundParams);
}

Datum
tee_adaptive_selector_activate(PG_FUNCTION_ARGS)
{
	tee_adaptive_enable = true;
	elog(LOG, "TEE Adaptive Selector activated");
	PG_RETURN_BOOL(true);
}


void
_PG_init(void)
{
	DefineCustomBoolVariable("tee_adaptive_selector.enable",
							 "Enables the adaptive TEE meta-optimizer.",
							 NULL,
							 &tee_adaptive_enable,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("tee_adaptive_selector.use_cache",
							 "Use hash cache to pick best TEE combo when available.",
							 NULL,
							 &tee_adaptive_use_cache,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("tee_adaptive_selector.cache_populating",
							 "Enable cache update mode and reload cache CSV from disk on each lookup.",
							 NULL,
							 &tee_adaptive_cache_populating,
							 false,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomStringVariable("tee_adaptive_selector.source_csv",
							   "Path to csv of per-query best scenarios.",
							   NULL,
							   &tee_source_csv,
							   tee_source_csv,
							   PGC_USERSET,
							   0,
							   NULL,
							   NULL,
							   NULL);

	DefineCustomStringVariable("tee_adaptive_selector.cache_csv",
							   "Path to generated hash cache csv.",
							   NULL,
							   &tee_cache_csv,
							   tee_cache_csv,
							   PGC_USERSET,
							   0,
							   NULL,
							   NULL,
							   NULL);

	DefineCustomStringVariable("tee_adaptive_selector.query_dir",
							   "Directory for SQL files used to build the cache.",
							   NULL,
							   &tee_query_dir,
							   tee_query_dir,
							   PGC_USERSET,
							   0,
							   NULL,
							   NULL,
							   NULL);

	DefineCustomStringVariable("tee_adaptive_selector.workload",
							   "Workload selector (job/ceb/stack/tpcds).",
							   NULL,
							   &tee_adaptive_workload,
							   tee_adaptive_workload,
							   PGC_USERSET,
							   0,
							   NULL,
							   NULL,
							   NULL);

	DefineCustomBoolVariable("tee_adaptive_selector.log_decisions",
							 "Log per-query TEE component decisions and timestamps.",
							 NULL,
							 &tee_adaptive_log_decisions,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	prev_planner_hook = planner_hook;
	planner_hook = tee_adaptive_planner_hook;
	prev_executor_start_hook = ExecutorStart_hook;
	ExecutorStart_hook = tee_adaptive_executor_start;
	prev_executor_end_hook = ExecutorEnd_hook;
	ExecutorEnd_hook = tee_adaptive_executor_end;
	on_proc_exit(tee_adaptive_on_proc_exit, 0);
	if (!cache_prng_seeded)
	{
		pg_prng_seed(&cache_prng_state, (uint64) GetCurrentTimestamp());
		cache_prng_seeded = true;
	}

	elog(LOG, "TEE Adaptive Selector extension loaded.");
}

void
_PG_fini(void)
{
	planner_hook = prev_planner_hook;
	ExecutorStart_hook = prev_executor_start_hook;
	ExecutorEnd_hook = prev_executor_end_hook;
}
