#!/usr/bin/env python3
"""
Enhanced PostgreSQL query executor with EXPLAIN ANALYZE support.

Features:
- Compare performance across environments (bare metal / Docker / QEMU / etc.)
- Optional detailed Python-side timing (cursor creation / execute / fetch)
- Optional EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) capturing:
    * planning_time_ms, execution_time_ms
    * root node Actual Rows / Time / Loops
    * shared/local/temp read & written blocks
    * top-level node type, relation(s), index names
    * full JSON plan (toggleable)

Notes:
- EXPLAIN ANALYZE executes the query; we avoid re-executing to fetch rows when plan mode is on,
  to keep apples-to-apples timing for cross-environment comparisons.
"""

import os
import time
import glob
import psycopg2
from psycopg2 import sql
import json
from datetime import datetime
import sys
import statistics
from typing import Any, Dict, Optional, Tuple, List
# from sshtunnel import SSHTunnelForwarder  # still unused but kept in case you want SSH tunnel later


class EnhancedPostgreSQLExecutor:
    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        user: str,
        password: Optional[str] = None,
        environment_name: str = "Unknown",
        store_full_plan_json: bool = False,
        explain_options: Optional[Dict[str, Any]] = None,
        statement_timeout_ms: Optional[int] = None,
    ):
        """
        Initialize PostgreSQL connection parameters.

        Args:
            host (str): DB host
            port (int): DB port
            database (str): DB name
            user (str): DB user
            password (str|None): DB password
            environment_name (str): Label for environment, e.g., 'bare_metal', 'docker', 'qemu'
            store_full_plan_json (bool): If True, store full EXPLAIN JSON; otherwise store summary only
            explain_options (dict|None): Options for EXPLAIN. Keys supported:
                 {
                   "ANALYZE": True,
                   "BUFFERS": True,
                   "VERBOSE": False,
                   "COSTS": True,
                   "TIMING": True,
                   "FORMAT": "JSON"
                 }
            statement_timeout_ms (int|None): Postgres statement_timeout in ms.
                - None or <=0: no statement_timeout (default, i.e., no server-side query timeout).
                - >0: SET statement_timeout to this value.
        """
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.environment_name = environment_name
        self.connection = None
        self.results: List[Dict[str, Any]] = []
        self.round_times: List[float] = []
        self.connect_time = 0.0
        self.store_full_plan_json = store_full_plan_json
        self.explain_options = explain_options or {
            "ANALYZE": True,
            "BUFFERS": True,
            "VERBOSE": False,
            "COSTS": True,
            "TIMING": True,
            "FORMAT": "JSON",
        }
        self.statement_timeout_ms = statement_timeout_ms

    def connect(self) -> Tuple[bool, float]:
        """Establish connection and measure connect time."""
        try:
            print(f"Connecting to {self.host}:{self.port}/{self.database} ({self.environment_name}) ...")
            t0 = time.time()
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                connect_timeout=10,  # 10 second connection timeout for establishing connection
            )
            self.connection.autocommit = True

            # Optional server-side statement timeout
            cursor = self.connection.cursor()
            if self.statement_timeout_ms and self.statement_timeout_ms > 0:
                cursor.execute(f"SET statement_timeout = '{self.statement_timeout_ms}ms'")
                print(f"PostgreSQL statement_timeout set to {self.statement_timeout_ms} ms")
            else:
                print("No PostgreSQL statement_timeout set (no server-side query timeout).")
            cursor.close()

            t1 = time.time()
            dt = t1 - t0
            print(f"Connected. Connect time: {dt:.4f}s")
            return True, dt
        except psycopg2.Error as e:
            print(f"Connection failed: {e}")
            return False, 0.0

    def disconnect(self):
        """Close connection."""
        if self.connection:
            self.connection.close()
            print("Connection closed")

    # ---------------------------
    # EXPLAIN ANALYZE helpers
    # ---------------------------

    def _build_explain_sql(self, query: str) -> str:
        """Build EXPLAIN SQL with options."""
        opts = []
        for k, v in self.explain_options.items():
            if k.upper() == "FORMAT":
                # must be FORMAT JSON
                opts.append(f"FORMAT {v}")
            else:
                if isinstance(v, bool):
                    if v:
                        opts.append(k.upper())
                    else:
                        # TIMING false / COSTS false are valid toggles
                        opts.append(f"{k.upper()} false")
                else:
                    # For completeness if a non-bool is passed
                    opts.append(f"{k.upper()} {v}")
        opt_str = ", ".join(opts)
        return f"EXPLAIN ({opt_str}) {query}"

    def get_explain_analyze_json(self, query: str) -> Dict[str, Any]:
        """
        Run EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON, ...) and return parsed JSON.
        Returns a dict with keys:
            - plan_json: the raw EXPLAIN JSON (list with one dict)  [if store_full_plan_json=True]
            - plan_summary: compact summary extracted from JSON
            - explain_text: None (JSON mode), provided for completeness
        """
        if not self.connection:
            return {"error": "Not connected"}

        explain_sql = self._build_explain_sql(query)
        cur = None
        try:
            cur = self.connection.cursor()
            cur.execute(explain_sql)
            rows = cur.fetchall()
            # In FORMAT JSON, the first column of the first row is a JSON doc (as Python object via psycopg2)
            if not rows:
                return {"error": "No EXPLAIN output"}
            # rows[0][0] is a Python object (list/dict) depending on driver; serialize/deserialize for safety
            plan_obj = rows[0][0]
            # Ensure dict via json dumps/loads for consistent types
            plan_json = json.loads(json.dumps(plan_obj))
            plan_summary = self.summarize_plan(plan_json)
            out = {
                "plan_summary": plan_summary,
                "explain_text": None,
            }
            if self.store_full_plan_json:
                out["plan_json"] = plan_json
            return out
        except psycopg2.Error as e:
            return {"error": f"EXPLAIN failed: {e}"}
        finally:
            if cur:
                cur.close()

    @staticmethod
    def _safe_get(d: Dict[str, Any], *keys, default=None):
        cur = d
        for k in keys:
            if isinstance(cur, dict) and k in cur:
                cur = cur[k]
            else:
                return default
        return cur

    def summarize_plan(self, plan_json: Any) -> Dict[str, Any]:
        """
        Extract compact, comparable fields from EXPLAIN JSON.
        Structure of FORMAT JSON is:
          [ { "Plan": {...}, "Planning Time": x, "Execution Time": y, "JIT": {...}, ... } ]
        """
        try:
            top = plan_json[0] if isinstance(plan_json, list) and plan_json else plan_json
            plan = top.get("Plan", {})
            planning_time_ms = top.get("Planning Time")
            execution_time_ms = top.get("Execution Time")

            # Root node basic stats
            node_type = plan.get("Node Type")
            relation_name = plan.get("Relation Name")
            schema = plan.get("Schema")
            alias = plan.get("Alias")
            index_name = plan.get("Index Name")

            actual_rows = plan.get("Actual Rows")
            actual_total_time = plan.get("Actual Total Time")
            actual_loops = plan.get("Actual Loops")
            plan_rows = plan.get("Plan Rows")
            plan_width = plan.get("Plan Width")
            total_cost = plan.get("Total Cost")

            # Buffers summary (might exist at top or in "Shared Hit Blocks", etc.)
            def collect_buffers(p: Dict[str, Any], agg: Dict[str, int]):
                keys = [
                    "Shared Hit Blocks",
                    "Shared Read Blocks",
                    "Shared Dirtied Blocks",
                    "Shared Written Blocks",
                    "Local Hit Blocks",
                    "Local Read Blocks",
                    "Local Dirtied Blocks",
                    "Local Written Blocks",
                    "Temp Read Blocks",
                    "Temp Written Blocks",
                ]
                for k in keys:
                    if k in p and isinstance(p[k], (int, float)):
                        agg[k] = agg.get(k, 0) + int(p[k])
                # Recurse into children
                for child in p.get("Plans", []) or []:
                    if isinstance(child, dict):
                        collect_buffers(child, agg)

            buffers = {}
            collect_buffers(plan, buffers)

            # JIT (optional)
            jit = top.get("JIT")
            jit_summary = None
            if isinstance(jit, dict):
                jit_summary = {
                    "Functions": jit.get("Functions"),
                    "Options": jit.get("Options"),
                    # Optionally include timings if present (e.g., "Timing")
                    **({} if "Timing" not in jit else {"Timing": jit.get("Timing")}),
                }

            # Workers (parallelism) basic
            workers = plan.get("Workers Planned")
            workers_launched = plan.get("Workers Launched")

            # Extract complete plan tree
            complete_plan_tree = self.extract_plan_tree(plan)

            # Return a compact summary with complete plan tree
            return {
                "planning_time_ms": planning_time_ms,
                "execution_time_ms": execution_time_ms,
                "root": {
                    "node_type": node_type,
                    "relation": relation_name,
                    "schema": schema,
                    "alias": alias,
                    "index_name": index_name,
                    "plan_rows": plan_rows,
                    "plan_width": plan_width,
                    "total_cost": total_cost,
                    "actual_rows": actual_rows,
                    "actual_total_time_ms": actual_total_time,
                    "actual_loops": actual_loops,
                    "workers_planned": workers,
                    "workers_launched": workers_launched,
                },
                "buffers": buffers or None,
                "jit": jit_summary,
                "complete_plan_tree": complete_plan_tree,
            }
        except Exception as e:
            return {"error": f"Failed to summarize plan: {e}"}

    def extract_plan_tree(self, node: Dict[str, Any]) -> Dict[str, Any]:
        """
        Recursively extract a COMPLETE tree from a PostgreSQL FORMAT JSON plan node.
        Captures node-level costs/actuals, buffers, predicates, and children.
        """
        # Basic identifiers
        out = {
            "node_type": node.get("Node Type"),
            "parent_relationship": node.get("Parent Relationship"),  # e.g., "Outer", "Inner"
            "strategy": node.get("Strategy"),  # e.g., "Plain", "Sorted", "Hashed"
            "join_type": node.get("Join Type"),
            "parallel_aware": node.get("Parallel Aware"),
            "async_capable": node.get("Async Capable"),
            # Relations / indexes
            "relation": node.get("Relation Name"),
            "schema": node.get("Schema"),
            "alias": node.get("Alias"),
            "index_name": node.get("Index Name"),
            # Estimates
            "startup_cost": node.get("Startup Cost"),
            "total_cost": node.get("Total Cost"),
            "plan_rows": node.get("Plan Rows"),
            "plan_width": node.get("Plan Width"),
            # Actuals
            "actual_startup_time_ms": node.get("Actual Startup Time"),
            "actual_total_time_ms": node.get("Actual Total Time"),
            "actual_rows": node.get("Actual Rows"),
            "actual_loops": node.get("Actual Loops"),
            # Output list (if present)
            "output": node.get("Output"),
            # Predicates & keys (include if present)
            "filter": node.get("Filter"),
            "index_cond": node.get("Index Cond"),
            "recheck_cond": node.get("Recheck Cond"),
            "hash_cond": node.get("Hash Cond"),
            "join_filter": node.get("Join Filter"),
            "merge_cond": node.get("Merge Cond"),
            "sort_key": node.get("Sort Key"),
            "group_key": node.get("Group Key"),
            "hash_key": node.get("Hash Key"),
            "exact_heap_tuples": node.get("Exact Heap Tuples"),
            # Buffers (per node)
            "buffers": {
                k: node.get(k)
                for k in [
                    "Shared Hit Blocks",
                    "Shared Read Blocks",
                    "Shared Dirtied Blocks",
                    "Shared Written Blocks",
                    "Local Hit Blocks",
                    "Local Read Blocks",
                    "Local Dirtied Blocks",
                    "Local Written Blocks",
                    "Temp Read Blocks",
                    "Temp Written Blocks",
                ]
                if node.get(k) is not None
            }
            or None,
            # Workers info (if present on this node)
            "workers_planned": node.get("Workers Planned"),
            "workers_launched": node.get("Workers Launched"),
            # Children
            "children": [],
        }

        # Recurse into children
        for child in node.get("Plans", []) or []:
            if isinstance(child, dict):
                out["children"].append(self.extract_plan_tree(child))

        # Some nodes embed worker subplans; if present, preserve
        if "Workers" in node and isinstance(node["Workers"], list):
            out["workers"] = []
            for w in node["Workers"]:
                w_copy = dict(w)
                # If worker has its own "Plan", also extract recursively
                if isinstance(w_copy.get("Plan"), dict):
                    w_copy["PlanTree"] = self.extract_plan_tree(w_copy["Plan"])
                out["workers"].append(w_copy)

        return out

    # ---------------------------
    # Execution paths
    # ---------------------------

    def execute_query_with_detailed_timing(self, query, query_name):
        """Original detailed Python-side timing (no EXPLAIN)."""
        if not self.connection:
            return {"error": "Not connected"}

        cursor = None
        detailed_timing = {}
        t0 = time.time()
        try:
            # 1) cursor creation
            t_c0 = time.time()
            cursor = self.connection.cursor()
            t_c1 = time.time()
            detailed_timing["cursor_creation_time"] = t_c1 - t_c0

            # 2) execute (no fetching time inside PG, just client timing)
            t_e0 = time.time()
            cursor.execute(query)
            t_e1 = time.time()
            detailed_timing["query_execution_time"] = t_e1 - t_e0

            # 3) fetch results
            t_f0 = time.time()
            results = cursor.fetchall()
            t_f1 = time.time()
            detailed_timing["result_fetch_time"] = t_f1 - t_f0

            # 4) columns
            t_col0 = time.time()
            column_names = [desc[0] for desc in cursor.description] if cursor.description else []
            t_col1 = time.time()
            detailed_timing["column_info_time"] = t_col1 - t_col0

            total_time = (
                detailed_timing["cursor_creation_time"]
                + detailed_timing["query_execution_time"]
                + detailed_timing["result_fetch_time"]
                + detailed_timing["column_info_time"]
            )

            result = {
                "query_name": query_name,
                "environment": self.environment_name,
                "execution_time": total_time,
                "detailed_timing": detailed_timing,
                "row_count": len(results),
                "column_names": column_names,
                "results": results,
                "status": "success",
                "timestamp": datetime.now().isoformat(),
                "plan_summary": None,
            }

            print(
                f"OK {query_name}: total={total_time:.4f}s "
                f"exec={detailed_timing['query_execution_time']:.4f}s "
                f"fetch={detailed_timing['result_fetch_time']:.4f}s rows={len(results)}"
            )
            return result

        except psycopg2.Error as e:
            t1 = time.time()
            result = {
                "query_name": query_name,
                "environment": self.environment_name,
                "execution_time": t1 - t0,
                "error": str(e),
                "status": "error",
                "timestamp": datetime.now().isoformat(),
                "plan_summary": None,
            }
            print(f"FAIL {query_name}: {e}")
            return result

        finally:
            if cursor:
                cursor.close()

    def execute_query_with_explain(self, query, query_name):
        """
        Execute EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON, ...) to obtain plan & actual times.
        Does NOT fetch query result rows again (to avoid double execution).
        """
        if not self.connection:
            return {"error": "Not connected"}

        # Measure client round-trip around EXPLAIN
        t0 = time.time()
        explain_out = self.get_explain_analyze_json(query)
        t1 = time.time()
        client_elapsed = t1 - t0

        if "error" in explain_out:
            return {
                "query_name": query_name,
                "environment": self.environment_name,
                "execution_time": client_elapsed,
                "status": "error",
                "error": explain_out["error"],
                "timestamp": datetime.now().isoformat(),
            }

        # Prefer server-side times from EXPLAIN JSON:
        plan_summary = explain_out.get("plan_summary", {})
        planning_ms = plan_summary.get("planning_time_ms")
        execution_ms = plan_summary.get("execution_time_ms")
        # For comparability with previous fields, set 'execution_time' to server Execution Time if available
        # else fallback to client_elapsed
        primary_exec_seconds = (
            (execution_ms / 1000.0) if isinstance(execution_ms, (int, float)) else client_elapsed
        )

        result = {
            "query_name": query_name,
            "environment": self.environment_name,
            "execution_time": primary_exec_seconds,
            "status": "success",
            "timestamp": datetime.now().isoformat(),
            "row_count": plan_summary.get("root", {}).get("actual_rows"),
            "column_names": None,
            "results": None,
            "plan_summary": plan_summary,
        }
        if self.store_full_plan_json:
            result["plan_json"] = explain_out.get("plan_json")

        print(
            f"OK {query_name}: planning={planning_ms}ms "
            f"execution={execution_ms}ms (client {client_elapsed:.4f}s)"
        )
        return result

    def execute_query_simple(self, query, query_name):
        """Legacy simple timing (no EXPLAIN)."""
        if not self.connection:
            return {"error": "Not connected"}

        cursor = None
        t0 = time.time()
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            colnames = [d[0] for d in cursor.description] if cursor.description else []
            t1 = time.time()
            dt = t1 - t0
            result = {
                "query_name": query_name,
                "environment": self.environment_name,
                "execution_time": dt,
                "row_count": len(results),
                "column_names": colnames,
                "results": results,
                "status": "success",
                "timestamp": datetime.now().isoformat(),
                "plan_summary": None,
            }
            print(f"OK {query_name}: {dt:.4f}s rows={len(results)}")
            return result
        except psycopg2.Error as e:
            t1 = time.time()
            result = {
                "query_name": query_name,
                "environment": self.environment_name,
                "execution_time": t1 - t0,
                "error": str(e),
                "status": "error",
                "timestamp": datetime.now().isoformat(),
                "plan_summary": None,
            }
            print(f"FAIL {query_name}: {e}")
            return result
        finally:
            if cursor:
                cursor.close()

    def load_sql_files(self, directory):
        """Load all *.sql files from directory."""
        sql_files = []
        pattern = os.path.join(directory, "*.sql")
        for file_path in sorted(glob.glob(pattern)):
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    content = f.read().strip()
                    if content:
                        sql_files.append({"filename": os.path.basename(file_path), "content": content})
            except Exception as e:
                print(f"Failed to read {file_path}: {e}")
        print(f"Found {len(sql_files)} SQL files")
        return sql_files

    def execute_all_queries(self, sql_directory, iterations=5, detailed_timing=False, run_explain=False):
        """
        Execute all SQL queries across multiple rounds.

        Args:
            sql_directory (str): Dir of *.sql files
            iterations (int): Number of rounds
            detailed_timing (bool): Use Python-side detailed timings (no EXPLAIN)
            run_explain (bool): Use EXPLAIN ANALYZE JSON (preferred for plan/timing)
        """
        ok, connect_time = self.connect()
        if not ok:
            return
        self.connect_time = connect_time

        sql_files = self.load_sql_files(sql_directory)

        print(f"\nExecuting {len(sql_files)} files x {iterations} rounds ...")
        print(f"Environment: {self.environment_name}")
        print(f"Connect time: {connect_time:.4f}s")
        print("=" * 80)

        round_times: List[float] = []

        for r in range(1, iterations + 1):
            print(f"\n{'=' * 20} Round {r} {'=' * 20}")
            r0 = time.time()

            for i, s in enumerate(sql_files, 1):
                print(f"\n[Round {r}/{iterations}] [{i}/{len(sql_files)}] {s['filename']}")

                qname = f"{s['filename']}_round{r}"
                if run_explain:
                    res = self.execute_query_with_explain(s["content"], qname)
                elif detailed_timing:
                    res = self.execute_query_with_detailed_timing(s["content"], qname)
                else:
                    res = self.execute_query_simple(s["content"], qname)

                self.results.append(res)

            r1 = time.time()
            rt = r1 - r0
            round_times.append(rt)

            succ = sum(
                1
                for x in self.results
                if x["status"] == "success" and f"_round{r}" in x["query_name"]
            )
            fail = sum(
                1
                for x in self.results
                if x["status"] == "error" and f"_round{r}" in x["query_name"]
            )
            print(f"\nRound {r} done: {rt:.4f}s  success={succ}  error={fail}")

        total = sum(round_times)
        avg = total / iterations
        mn = min(round_times)
        mx = max(round_times)
        sd = statistics.stdev(round_times) if len(round_times) > 1 else 0.0

        print("\n" + "=" * 80)
        print(f"All {iterations} rounds completed!")
        print("=" * 80)
        print("\nPer-round times:")
        for i, t in enumerate(round_times, 1):
            print(f"Round {i}: {t:.4f}s")
        print("\nSummary:")
        print(f"Total time: {total:.4f}s")
        print(f"Avg/round: {avg:.4f}s")
        print(f"Min round: {mn:.4f}s")
        print(f"Max round: {mx:.4f}s")
        print(f"Std dev:   {sd:.4f}s")

        self.round_times = round_times
        self.disconnect()

    def save_results(self, output_file=None):
        """Persist results as JSON."""
        if output_file is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"query_results_{self.environment_name}_{timestamp}.json"

        try:
            # Derive totals from results
            total_exec = sum(
                r.get("execution_time", 0.0) for r in self.results if r.get("status") == "success"
            )
            n = len(self.results)
            avg_query_time = (total_exec / n) if n else 0.0

            save_data = {
                "environment": self.environment_name,
                "connection_time": getattr(self, "connect_time", 0.0),
                "summary": {
                    "total_queries": len(self.results),
                    "successful_queries": sum(
                        1 for r in self.results if r["status"] == "success"
                    ),
                    "failed_queries": sum(
                        1 for r in self.results if r["status"] == "error"
                    ),
                    "total_execution_time": total_exec,
                    "average_query_time": avg_query_time,
                },
                "round_times": getattr(self, "round_times", []),
                "query_results": self.results,
            }

            if hasattr(self, "round_times") and self.round_times:
                save_data["summary"].update(
                    {
                        "rounds": len(self.round_times),
                        "average_round_time": sum(self.round_times) / len(self.round_times),
                        "min_round_time": min(self.round_times),
                        "max_round_time": max(self.round_times),
                        "total_round_time": sum(self.round_times),
                        "std_deviation": statistics.stdev(self.round_times)
                        if len(self.round_times) > 1
                        else 0.0,
                    }
                )

            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(save_data, f, indent=2, ensure_ascii=False, default=str)
            print(f"Results saved to: {output_file}")
        except Exception as e:
            print(f"Save failed: {e}")


def compare_environments(results_files):
    """Compare environments from saved JSONs."""
    print("\n" + "=" * 100)
    print("Environment Performance Comparison")
    print("=" * 100)

    envs = []
    for path in results_files:
        try:
            with open(path, "r", encoding="utf-8") as f:
                envs.append(json.load(f))
        except Exception as e:
            print(f"Read failed {path}: {e}")

    if not envs:
        print("No valid environment data")
        return

    envs.sort(key=lambda x: x.get("environment", "Unknown"))

    print(f"\n{'Environment':<15} {'Connect':<10} {'AvgRound':<12} {'TotalQuery':<12} {'StdDev':<10}")
    print("-" * 70)

    for env in envs:
        name = env.get("environment", "Unknown")
        conn = env.get("connection_time", 0.0)
        avg_round = env.get("summary", {}).get("average_round_time", 0.0)
        total_q = env.get("summary", {}).get("total_execution_time", 0.0)
        sd = env.get("summary", {}).get("std_deviation", 0.0)
        print(
            f"{name:<15} {conn:<10.4f} {avg_round:<12.4f} {total_q:<12.4f} {sd:<10.4f}"
        )

    fastest = min(
        envs, key=lambda x: x.get("summary", {}).get("average_round_time", float("inf"))
    )
    print(f"\nFastest: {fastest.get('environment', 'Unknown')}")
    print(
        f"Avg round time: {fastest.get('summary', {}).get('average_round_time', 0.0):.4f}s"
    )


def get_database_config(environment_name, config_file="database_configs.json"):
    """Load DB config by environment name, with sensible defaults."""
    default_configs = {
        "kvm": {
            "host": "localhost",
            "port": 5432,
            "database": "imdbload",
            "user": "qihan",
        },
        "cvm": {
            "host": "localhost",
            "port": 5432,
            "database": "imdbload",
            "user": "qihan",
        },
        "cvm_es": {
            "host": "localhost",
            "port": 5432,
            "database": "imdbload",
            "user": "qihan",
        },
        "cvm_snp": {
            "host": "localhost",
            "port": 5432,
            "database": "imdbload",
            "user": "qihan",
        },
    }

    try:
        if os.path.exists(config_file):
            with open(config_file, "r", encoding="utf-8") as f:
                cfg = json.load(f)
                envs = cfg.get("environments", {})
                if environment_name.lower() in envs:
                    conf = envs[environment_name.lower()].copy()
                    conf.pop("description", None)
                    print(f"Loaded {environment_name} config from file")
                    return conf
    except Exception as e:
        print(f"Config load failed: {e}; using defaults")

    return default_configs.get(environment_name.lower(), default_configs["kvm"])


def main():
    """
    CLI:
      argv[1] = iterations (int)                        default: 5
      argv[2] = environment_name (str)                  default: unknown
      argv[3] = mode                                    one of: '', 'detailed', 'explain'
      argv[4] = store_full_plan_json (optional)         'fullplan' to store full JSON plan
      argv[5] = config_file (optional)                  custom database_configs.json path
      argv[6] = statement_timeout_seconds (optional)    >0 to enable PG statement_timeout

    Example:
      python3 script.py 5 bare_metal explain fullplan
      python3 script.py 5 cvm_snp explain "" database_configs_direct.json 60
    """
    SQL_DIRECTORY = "/home/qihan/load_imdb/job_queries"
    ITERATIONS = 5
    ENVIRONMENT_NAME = "unknown"
    MODE = ""  # '', 'detailed', 'explain'
    STORE_FULL_PLAN = False
    CONFIG_FILE = "database_configs.json"
    STATEMENT_TIMEOUT_MS: Optional[int] = None

    if len(sys.argv) > 1:
        try:
            ITERATIONS = int(sys.argv[1])
            print(f"Iterations: {ITERATIONS}")
        except ValueError:
            print(f"Invalid iterations: {sys.argv[1]} (using default {ITERATIONS})")

    if len(sys.argv) > 2:
        ENVIRONMENT_NAME = sys.argv[2]
        print(f"Environment: {ENVIRONMENT_NAME}")

    if len(sys.argv) > 3:
        MODE = sys.argv[3].lower()
        if MODE not in ("", "detailed", "explain"):
            print(f"Unknown mode '{MODE}', must be '', 'detailed', or 'explain'. Using ''.")
            MODE = ""

    if len(sys.argv) > 4:
        if sys.argv[4].lower() == "fullplan":
            STORE_FULL_PLAN = True
            print("Will store full EXPLAIN JSON plans.")

    if len(sys.argv) > 5:
        CONFIG_FILE = sys.argv[5]
        print(f"Using config file: {CONFIG_FILE}")

    # Optional statement timeout (seconds) - must be manually provided
    if len(sys.argv) > 6:
        raw = sys.argv[6]
        try:
            timeout_sec = float(raw)
            if timeout_sec > 0:
                STATEMENT_TIMEOUT_MS = int(timeout_sec * 1000)
                print(
                    f"Statement timeout requested: {timeout_sec}s "
                    f"({STATEMENT_TIMEOUT_MS} ms)"
                )
            else:
                print(
                    f"Statement timeout <= 0 ({timeout_sec}), "
                    "no statement_timeout will be set."
                )
        except ValueError:
            print(
                f"Invalid statement timeout seconds '{raw}', "
                "ignoring and leaving it disabled."
            )

    DB_CONFIG = get_database_config(ENVIRONMENT_NAME, CONFIG_FILE)
    print(f"DB config: {DB_CONFIG}")

    if not os.path.exists(SQL_DIRECTORY):
        print(f"Error: SQL directory not found: {SQL_DIRECTORY}")
        sys.exit(1)

    executor = EnhancedPostgreSQLExecutor(
        environment_name=ENVIRONMENT_NAME,
        store_full_plan_json=STORE_FULL_PLAN,
        statement_timeout_ms=STATEMENT_TIMEOUT_MS,
        **DB_CONFIG,
    )

    try:
        if MODE == "explain":
            executor.execute_all_queries(
                SQL_DIRECTORY, ITERATIONS, detailed_timing=False, run_explain=True
            )
        elif MODE == "detailed":
            executor.execute_all_queries(
                SQL_DIRECTORY, ITERATIONS, detailed_timing=True, run_explain=False
            )
        else:
            executor.execute_all_queries(
                SQL_DIRECTORY, ITERATIONS, detailed_timing=False, run_explain=False
            )

        executor.save_results()
    except KeyboardInterrupt:
        print("\nInterrupted by user.")
    except Exception as e:
        print(f"\nError during run: {e}")
    finally:
        executor.disconnect()


if __name__ == "__main__":
    main()
