import argparse
import json
import os
import random
import sqlite3
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

from dump_reducer.planner import run_agent_and_generate
from generate_sample_dump import SampleDumpConfig, generate_sample_dump

MAX_SCENARIOS = 4


@dataclass(frozen=True)
class BenchmarkScenario:
    name: str
    description: str
    generator_config: Optional[SampleDumpConfig]
    table_count: Optional[int]
    synthetic_total_rows: Optional[int]
    synthetic_hidden_ratio: float
    seed: int
    target_rows: int


@dataclass(frozen=True)
class BenchmarkVariant:
    name: str
    prompt_note: str


SCENARIOS: List[BenchmarkScenario] = [
    BenchmarkScenario(
        name="synthetic_8_tables",
        description="Synthetic 8-table fixture with >100k source rows and mixed hidden links.",
        generator_config=None,
        table_count=8,
        synthetic_total_rows=120_000,
        synthetic_hidden_ratio=0.35,
        seed=101,
        target_rows=700,
    ),
    BenchmarkScenario(
        name="synthetic_20_tables",
        description="Synthetic 20-table fixture with >100k source rows and denser hidden links.",
        generator_config=None,
        table_count=20,
        synthetic_total_rows=160_000,
        synthetic_hidden_ratio=0.5,
        seed=202,
        target_rows=900,
    ),
    BenchmarkScenario(
        name="wide_90_tables_mixed",
        description="Generated wide fixture (~90 tables) with mixed hidden references and high row volume.",
        generator_config=SampleDumpConfig(
            scale="large",
            scenario="commerce",
            min_tables=90,
            hidden_reference_mode="mixed",
            seed=303,
            tenants=18,
            users_per_tenant=260,
            products_per_tenant=220,
            orders_per_tenant=900,
            tickets_per_tenant=280,
            projects_per_tenant=22,
            incidents_per_tenant=12,
            write_sql_dump=False,
        ),
        table_count=None,
        synthetic_total_rows=None,
        synthetic_hidden_ratio=0.0,
        seed=303,
        target_rows=1400,
    ),
    BenchmarkScenario(
        name="wide_96_tables_aggressive",
        description="Generated wide fixture (~96 tables) with aggressive hidden references and high row volume.",
        generator_config=SampleDumpConfig(
            scale="large",
            scenario="support",
            min_tables=96,
            hidden_reference_mode="aggressive",
            seed=404,
            tenants=22,
            users_per_tenant=320,
            products_per_tenant=260,
            orders_per_tenant=1100,
            tickets_per_tenant=360,
            projects_per_tenant=26,
            incidents_per_tenant=14,
            write_sql_dump=False,
        ),
        table_count=None,
        synthetic_total_rows=None,
        synthetic_hidden_ratio=0.0,
        seed=404,
        target_rows=1800,
    ),
]

VARIANTS: List[BenchmarkVariant] = [
    BenchmarkVariant("recent_activity", "Use recent operational activity as the anchor and keep the subset compact."),
    BenchmarkVariant("high_value", "Bias toward high-value revenue paths such as invoices, payments, and expensive products."),
    BenchmarkVariant("support_load", "Bias toward active support load, ticket discussions, and the users attached to them."),
    BenchmarkVariant("project_chain", "Bias toward projects, tasks, task dependencies, documents, and related incidents."),
    BenchmarkVariant("subscription_focus", "Bias toward subscription and renewal lifecycle entities while preserving dependencies."),
    BenchmarkVariant("compliance_focus", "Bias toward audit, compliance, and control-related entities while keeping referential consistency."),
    BenchmarkVariant("integration_focus", "Bias toward webhook, export, report, and notification entities, including hidden reference carriers."),
    BenchmarkVariant("inventory_focus", "Bias toward warehouses, inventory, product bundles, orders, and shipment chains."),
    BenchmarkVariant("hidden_ref_focus", "Intentionally search for non-obvious relationships stored in payloads, shadow columns, or hint tables."),
    BenchmarkVariant("balanced_cover", "Produce a balanced subset spanning transactional, support, and governance entities."),
]


def _default_llm_stats() -> Dict[str, float]:
    return {
        "calls_total": 0,
        "network_requests": 0,
        "cache_hits": 0,
        "cache_misses": 0,
        "billed_prompt_tokens": 0,
        "billed_completion_tokens": 0,
        "billed_total_tokens": 0,
        "cached_prompt_tokens": 0,
        "cached_completion_tokens": 0,
        "cached_total_tokens": 0,
        "billed_cost_usd": 0.0,
        "cached_cost_usd": 0.0,
        "cache_hit_rate": 0.0,
        "logical_prompt_tokens": 0,
        "logical_completion_tokens": 0,
        "logical_total_tokens": 0,
        "logical_cost_usd": 0.0,
    }


def _combine_llm_stats(lhs: Dict[str, float], rhs: Dict[str, float]) -> Dict[str, float]:
    merged = _default_llm_stats()
    merged.update(lhs)
    for key, value in rhs.items():
        if isinstance(value, (int, float)):
            merged[key] = merged.get(key, 0) + value
        else:
            merged[key] = value

    calls_total = int(merged.get("calls_total", 0))
    cache_hits = int(merged.get("cache_hits", 0))
    merged["cache_hit_rate"] = (cache_hits / calls_total) if calls_total else 0.0
    return merged


def _count_total_rows(db_path: Path) -> int:
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
        table_names = [row[0] for row in cur.fetchall()]
        total = 0
        for table_name in table_names:
            cur.execute(f'SELECT COUNT(*) FROM "{table_name}"')
            total += int(cur.fetchone()[0])
        return total
    finally:
        conn.close()


def _generate_synthetic_fixture(db_path: Path, table_count: int, total_rows: int, hidden_ratio: float, seed: int) -> None:
    if db_path.exists():
        db_path.unlink()

    rng = random.Random(seed)
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("PRAGMA foreign_keys = ON;")
        rows_per_table = max(1, total_rows // table_count)

        cur.execute(
            """
            CREATE TABLE root_entities (
                id INTEGER PRIMARY KEY,
                created_at TEXT NOT NULL,
                category TEXT NOT NULL,
                payload TEXT NOT NULL
            );
            """
        )

        root_rows = [
            (
                index + 1,
                f"2026-01-{(index % 28) + 1:02d}T12:00:00",
                f"cat_{(index % 12) + 1}",
                json.dumps({"root": index + 1, "kind": "root"}),
            )
            for index in range(rows_per_table)
        ]
        cur.executemany(
            "INSERT INTO root_entities (id, created_at, category, payload) VALUES (?, ?, ?, ?)",
            root_rows,
        )

        for table_index in range(1, table_count):
            table_name = f"entity_{table_index:02d}"
            prev_table = f"entity_{table_index - 1:02d}" if table_index > 1 else None
            ddl_parts = [
                "id INTEGER PRIMARY KEY",
                "root_id INTEGER NOT NULL",
                "created_at TEXT NOT NULL",
                "status TEXT NOT NULL",
                "shadow_root_id INTEGER",
                "hidden_payload TEXT",
            ]
            if prev_table:
                ddl_parts.append("prev_entity_id INTEGER")

            ddl_parts.append("FOREIGN KEY (root_id) REFERENCES root_entities (id)")
            if prev_table:
                ddl_parts.append(f'FOREIGN KEY (prev_entity_id) REFERENCES "{prev_table}" (id)')

            cur.execute(f'CREATE TABLE "{table_name}" ({", ".join(ddl_parts)});')

        for table_index in range(1, table_count):
            table_name = f"entity_{table_index:02d}"
            has_prev = table_index > 1
            rows = []
            for row_id in range(1, rows_per_table + 1):
                root_id = rng.randint(1, rows_per_table)
                shadow_root_id = rng.randint(1, rows_per_table) if rng.random() < hidden_ratio else None
                hidden_payload = (
                    json.dumps(
                        {
                            "shadow_root_id": shadow_root_id,
                            "table": table_name,
                            "hint_entity": f"entity_{rng.randint(1, table_index):02d}",
                            "hint_id": rng.randint(1, rows_per_table),
                        }
                    )
                    if shadow_root_id is not None
                    else None
                )
                prev_entity_id = rng.randint(1, rows_per_table) if has_prev else None
                rows.append(
                    (
                        row_id,
                        root_id,
                        f"2026-02-{(row_id % 28) + 1:02d}T08:30:00",
                        rng.choice(["new", "active", "closed", "blocked"]),
                        shadow_root_id,
                        hidden_payload,
                        prev_entity_id,
                    )
                )

            if has_prev:
                cur.executemany(
                    f"""
                    INSERT INTO "{table_name}" (
                        id, root_id, created_at, status, shadow_root_id, hidden_payload, prev_entity_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    rows,
                )
            else:
                cur.executemany(
                    f"""
                    INSERT INTO "{table_name}" (
                        id, root_id, created_at, status, shadow_root_id, hidden_payload
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    [row[:-1] for row in rows],
                )

        conn.commit()
    finally:
        conn.close()


def _ensure_fixture(base_dir: Path, scenario: BenchmarkScenario, rebuild: bool) -> Path:
    fixture_dir = base_dir / "fixtures"
    fixture_dir.mkdir(parents=True, exist_ok=True)
    db_path = fixture_dir / f"{scenario.name}.db"
    if rebuild or not db_path.exists():
        if scenario.generator_config is not None:
            generate_sample_dump(db_filename=str(db_path), filename=str(fixture_dir / f"{scenario.name}.sql"), config=scenario.generator_config)
        else:
            _generate_synthetic_fixture(
                db_path=db_path,
                table_count=scenario.table_count or 8,
                total_rows=scenario.synthetic_total_rows or 120_000,
                hidden_ratio=scenario.synthetic_hidden_ratio,
                seed=scenario.seed,
            )
    return db_path


def _render_markdown(results: Dict[str, object]) -> str:
    llm_stats = results.get("llm_stats") or _default_llm_stats()
    lines = [
        "# Benchmark Report",
        "",
        f"- Timestamp: `{results['timestamp']}`",
        f"- Model: `{results['model']}`",
        f"- Runs per scenario: `{results['runs_per_scenario']}`",
        f"- Scenario count: `{results['scenario_count']}`",
        f"- Cache directory: `{results['cache_dir']}`",
        f"- Overall status: `{results['status']}`",
        "",
        "## OpenRouter Totals",
        "",
        f"- Calls: `{llm_stats['calls_total']}`",
        f"- Network requests: `{llm_stats['network_requests']}`",
        f"- Cache hits: `{llm_stats['cache_hits']}`",
        f"- Cache misses: `{llm_stats['cache_misses']}`",
        f"- Billed tokens: `{llm_stats['billed_total_tokens']}`",
        f"- Cached tokens replayed: `{llm_stats['cached_total_tokens']}`",
        f"- Logical tokens: `{llm_stats['logical_total_tokens']}`",
        f"- Billed cost (USD): `{llm_stats['billed_cost_usd']:.6f}`",
        f"- Cache hit rate: `{llm_stats['cache_hit_rate']:.2%}`",
        "",
        "## Scenario Summary",
        "",
        "| Scenario | Success/Error | Unique plans | Cache hits | Cache misses | Avg seconds | Status |",
        "| --- | ---: | ---: | ---: | ---: | ---: | --- |",
    ]

    for scenario in results["scenarios"]:
        lines.append(
            f"| {scenario['name']} | {scenario['success_count']}/{scenario['error_count']} | {scenario['unique_plan_count']}/{scenario['success_count']} | "
            f"{scenario['cache_hits']} | {scenario['cache_misses']} | {scenario['avg_duration_seconds']:.2f} | "
            f"{scenario['status']} |"
        )

    lines.extend(["", "## Runs", ""])
    for scenario in results["scenarios"]:
        lines.extend(
            [
                f"### {scenario['name']}",
                "",
                f"- Description: {scenario['description']}",
                f"- Fixture: `{scenario['db_path']}`",
                f"- Source rows: `{scenario['source_total_rows']}`",
                f"- Target rows: `{scenario['target_rows']}`",
                f"- Status: `{scenario['status']}`",
                f"- Success/Error: `{scenario['success_count']}/{scenario['error_count']}`",
                f"- Unique plans: `{scenario['unique_plan_count']}/{scenario['success_count']}`",
                "",
                "| Run | Variant | Result | Duration (s) | Cache hits | Cache misses | Plan hash | Output | Error |",
                "| --- | --- | --- | ---: | ---: | ---: | --- | --- | --- |",
            ]
        )
        for run in scenario["runs"]:
            lines.append(
                f"| {run['run_index']} | {run['variant']} | {run['result']} | {run['duration_seconds']:.2f} | "
                f"{run['cache_hits']} | {run['cache_misses']} | `{run['plan_hash'][:12]}` | "
                f"`{run['out_path']}` | {run['error']} |"
            )
        lines.append("")

    return "\n".join(lines)


def run_benchmark(
    api_key: str,
    model: str,
    output_dir: str = "benchmark_artifacts",
    cache_dir: str = ".cache/openrouter",
    runs_per_scenario: int = 10,
    verify_ssl: bool = True,
    rebuild_fixtures: bool = False,
    max_scenarios: int = MAX_SCENARIOS,
    fail_on_issues: bool = False,
) -> Dict[str, object]:
    if max_scenarios < 1 or max_scenarios > MAX_SCENARIOS:
        raise ValueError(f"max_scenarios must be between 1 and {MAX_SCENARIOS}.")
    if runs_per_scenario > len(VARIANTS):
        raise ValueError(f"runs_per_scenario={runs_per_scenario} exceeds the {len(VARIANTS)} predefined unique variants.")

    base_dir = Path(output_dir)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_dir = base_dir / timestamp
    run_dir.mkdir(parents=True, exist_ok=True)

    summary = {
        "timestamp": timestamp,
        "model": model,
        "runs_per_scenario": runs_per_scenario,
        "scenario_count": max_scenarios,
        "cache_dir": cache_dir,
        "llm_stats": _default_llm_stats(),
        "scenarios": [],
    }

    selected_scenarios = SCENARIOS[:max_scenarios]
    for scenario in selected_scenarios:
        db_path = _ensure_fixture(base_dir, scenario, rebuild_fixtures)
        source_total_rows = _count_total_rows(db_path)
        scenario_dir = run_dir / scenario.name
        scenario_dir.mkdir(parents=True, exist_ok=True)

        run_results = []
        plan_hashes = set()
        cache_hits = 0
        cache_misses = 0
        durations: List[float] = []
        scenario_llm_stats: Dict[str, float] = _default_llm_stats()
        success_count = 0
        error_count = 0

        for run_index, variant in enumerate(VARIANTS[:runs_per_scenario], start=1):
            out_path = scenario_dir / f"{run_index:02d}_{variant.name}.sql"
            prompt_note = (
                f"Benchmark scenario '{scenario.name}', variant '{variant.name}', run {run_index}. "
                f"{variant.prompt_note} Produce a valid but distinct strategy for this variant."
            )

            started = time.perf_counter()
            try:
                result = run_agent_and_generate(
                    db_url=f"sqlite://{db_path}",
                    api_key=api_key,
                    model=model,
                    target_rows=scenario.target_rows,
                    out_path=str(out_path),
                    verify_ssl=verify_ssl,
                    prompt_note=prompt_note,
                    cache_dir=cache_dir,
                    print_openrouter_stats=False,
                )
                duration = time.perf_counter() - started
                durations.append(duration)
                success_count += 1
                plan_hashes.add(result["plan_hash"])
                cache_hits += result["cache_hits"]
                cache_misses += result["cache_misses"]
                scenario_llm_stats = _combine_llm_stats(scenario_llm_stats, result["llm_stats"])

                run_results.append(
                    {
                        "run_index": run_index,
                        "variant": variant.name,
                        "prompt_note": prompt_note,
                        "result": "OK",
                        "duration_seconds": duration,
                        "cache_hits": result["cache_hits"],
                        "cache_misses": result["cache_misses"],
                        "plan_hash": result["plan_hash"],
                        "steps": result["steps"],
                        "out_path": str(out_path),
                        "error": "",
                        "llm_stats": result["llm_stats"],
                    }
                )
            except Exception as exc:
                duration = time.perf_counter() - started
                durations.append(duration)
                error_count += 1
                run_results.append(
                    {
                        "run_index": run_index,
                        "variant": variant.name,
                        "prompt_note": prompt_note,
                        "result": "ERROR",
                        "duration_seconds": duration,
                        "cache_hits": 0,
                        "cache_misses": 0,
                        "plan_hash": "",
                        "steps": 0,
                        "out_path": str(out_path),
                        "error": str(exc).replace("\n", " ").replace("|", "/")[:300],
                        "llm_stats": _default_llm_stats(),
                    }
                )

        unique_plan_count = len(plan_hashes)
        if success_count == 0:
            scenario_status = "FAIL"
        elif error_count > 0:
            scenario_status = "PARTIAL"
        elif unique_plan_count != success_count:
            scenario_status = "DUPLICATE_PLANS"
        else:
            scenario_status = "PASS"

        scenario_result = {
            "name": scenario.name,
            "description": scenario.description,
            "db_path": str(db_path),
            "source_total_rows": source_total_rows,
            "target_rows": scenario.target_rows,
            "generator_config": asdict(scenario.generator_config) if scenario.generator_config else None,
            "table_count": scenario.table_count,
            "runs": run_results,
            "run_count": len(run_results),
            "success_count": success_count,
            "error_count": error_count,
            "unique_plan_count": unique_plan_count,
            "cache_hits": cache_hits,
            "cache_misses": cache_misses,
            "avg_duration_seconds": (sum(durations) / len(durations)) if durations else 0.0,
            "llm_stats": scenario_llm_stats,
            "status": scenario_status,
        }
        summary["scenarios"].append(scenario_result)
        summary["llm_stats"] = _combine_llm_stats(summary["llm_stats"], scenario_llm_stats)

    issue_scenarios = [scenario["name"] for scenario in summary["scenarios"] if scenario["status"] != "PASS"]
    summary["status"] = "PASS" if not issue_scenarios else "ISSUES"

    raw_path = run_dir / "benchmark_results.json"
    raw_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")

    report = _render_markdown(summary)
    report_path = run_dir / "benchmark_report.md"
    report_path.write_text(report, encoding="utf-8")

    latest_path = base_dir / "latest_benchmark_report.md"
    latest_path.write_text(report, encoding="utf-8")

    if fail_on_issues and issue_scenarios:
        raise RuntimeError(
            "Benchmark finished with issues for scenarios: "
            + ", ".join(issue_scenarios)
            + f". Inspect {report_path} for details."
        )

    return summary


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run cached LLM benchmark scenarios against generated SQLite fixtures.")
    parser.add_argument("--model", default="moonshotai/kimi-k2-thinking", help="OpenRouter model id.")
    parser.add_argument("--api-key", default=os.getenv("OPENROUTER_API_KEY", ""), help="OpenRouter API key.")
    parser.add_argument("--output-dir", default="benchmark_artifacts", help="Directory for fixtures, reports, and outputs.")
    parser.add_argument("--cache-dir", default=".cache/openrouter", help="Persistent cache directory for OpenRouter responses.")
    parser.add_argument("--runs-per-scenario", type=int, default=10, help="How many predefined unique prompt variants to execute per scenario.")
    parser.add_argument("--max-scenarios", type=int, default=MAX_SCENARIOS, help=f"How many scenarios to run (1-{MAX_SCENARIOS}).")
    parser.add_argument("--fail-on-issues", action="store_true", help="Exit non-zero if any scenario has errors or duplicate plans.")
    parser.add_argument("--rebuild-fixtures", action="store_true", help="Regenerate benchmark fixture databases even if they already exist.")
    parser.add_argument("--no-verify-ssl", action="store_false", dest="verify_ssl", help="Disable SSL verification for OpenRouter requests.")
    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    if not args.api_key:
        raise SystemExit("Missing OpenRouter API key. Set OPENROUTER_API_KEY or pass --api-key.")

    summary = run_benchmark(
        api_key=args.api_key,
        model=args.model,
        output_dir=args.output_dir,
        cache_dir=args.cache_dir,
        runs_per_scenario=args.runs_per_scenario,
        verify_ssl=args.verify_ssl,
        rebuild_fixtures=args.rebuild_fixtures,
        max_scenarios=args.max_scenarios,
        fail_on_issues=args.fail_on_issues,
    )
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
