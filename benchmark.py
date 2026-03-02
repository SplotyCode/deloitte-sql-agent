import argparse
import json
import os
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

from dump_reducer.planner import run_agent_and_generate
from generate_sample_dump import SampleDumpConfig, generate_sample_dump


@dataclass(frozen=True)
class BenchmarkScenario:
    name: str
    description: str
    generator_config: SampleDumpConfig
    target_rows: int


@dataclass(frozen=True)
class BenchmarkVariant:
    name: str
    prompt_note: str


SCENARIOS: List[BenchmarkScenario] = [
    BenchmarkScenario(
        name="enterprise_baseline",
        description="Balanced enterprise topology with hidden references disabled.",
        generator_config=SampleDumpConfig(scale="tiny", scenario="enterprise", min_tables=89, hidden_reference_mode="off", seed=101, write_sql_dump=False),
        target_rows=700,
    ),
    BenchmarkScenario(
        name="commerce_hidden",
        description="Commerce-heavy topology with mixed hidden transactional links.",
        generator_config=SampleDumpConfig(scale="tiny", scenario="commerce", min_tables=90, hidden_reference_mode="mixed", seed=202, write_sql_dump=False),
        target_rows=900,
    ),
    BenchmarkScenario(
        name="support_hidden",
        description="Support-heavy topology with mixed hidden operational links.",
        generator_config=SampleDumpConfig(scale="tiny", scenario="support", min_tables=92, hidden_reference_mode="mixed", seed=303, write_sql_dump=False),
        target_rows=900,
    ),
    BenchmarkScenario(
        name="stress_aggressive",
        description="Wide schema with aggressive hidden references and extra challenge tables.",
        generator_config=SampleDumpConfig(scale="tiny", scenario="commerce", min_tables=96, hidden_reference_mode="aggressive", seed=404, write_sql_dump=False),
        target_rows=1100,
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


def _ensure_fixture(base_dir: Path, scenario: BenchmarkScenario, rebuild: bool) -> Path:
    fixture_dir = base_dir / "fixtures"
    fixture_dir.mkdir(parents=True, exist_ok=True)
    db_path = fixture_dir / f"{scenario.name}.db"
    if rebuild or not db_path.exists():
        generate_sample_dump(db_filename=str(db_path), filename=str(fixture_dir / f"{scenario.name}.sql"), config=scenario.generator_config)
    return db_path


def _render_markdown(results: Dict[str, object]) -> str:
    lines = [
        "# Benchmark Report",
        "",
        f"- Timestamp: `{results['timestamp']}`",
        f"- Model: `{results['model']}`",
        f"- Runs per scenario: `{results['runs_per_scenario']}`",
        f"- Cache directory: `{results['cache_dir']}`",
        "",
        "## Scenario Summary",
        "",
        "| Scenario | Unique plans | Cache hits | Cache misses | Avg seconds | Status |",
        "| --- | ---: | ---: | ---: | ---: | --- |",
    ]

    for scenario in results["scenarios"]:
        lines.append(
            f"| {scenario['name']} | {scenario['unique_plan_count']}/{scenario['run_count']} | "
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
                f"- Target rows: `{scenario['target_rows']}`",
                f"- Unique plans: `{scenario['unique_plan_count']}/{scenario['run_count']}`",
                "",
                "| Run | Variant | Duration (s) | Cache hits | Cache misses | Plan hash | Output |",
                "| --- | --- | ---: | ---: | ---: | --- | --- |",
            ]
        )
        for run in scenario["runs"]:
            lines.append(
                f"| {run['run_index']} | {run['variant']} | {run['duration_seconds']:.2f} | "
                f"{run['cache_hits']} | {run['cache_misses']} | `{run['plan_hash'][:12]}` | "
                f"`{run['out_path']}` |"
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
) -> Dict[str, object]:
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
        "cache_dir": cache_dir,
        "scenarios": [],
    }

    for scenario in SCENARIOS:
        db_path = _ensure_fixture(base_dir, scenario, rebuild_fixtures)
        scenario_dir = run_dir / scenario.name
        scenario_dir.mkdir(parents=True, exist_ok=True)

        run_results = []
        plan_hashes = set()
        cache_hits = 0
        cache_misses = 0
        durations: List[float] = []

        for run_index, variant in enumerate(VARIANTS[:runs_per_scenario], start=1):
            out_path = scenario_dir / f"{run_index:02d}_{variant.name}.sql"
            prompt_note = (
                f"Benchmark scenario '{scenario.name}', variant '{variant.name}', run {run_index}. "
                f"{variant.prompt_note} Produce a valid but distinct strategy for this variant."
            )

            started = time.perf_counter()
            result = run_agent_and_generate(
                db_url=f"sqlite://{db_path}",
                api_key=api_key,
                model=model,
                target_rows=scenario.target_rows,
                out_path=str(out_path),
                verify_ssl=verify_ssl,
                prompt_note=prompt_note,
                cache_dir=cache_dir,
            )
            duration = time.perf_counter() - started
            durations.append(duration)
            plan_hashes.add(result["plan_hash"])
            cache_hits += result["cache_hits"]
            cache_misses += result["cache_misses"]

            run_results.append(
                {
                    "run_index": run_index,
                    "variant": variant.name,
                    "prompt_note": prompt_note,
                    "duration_seconds": duration,
                    "cache_hits": result["cache_hits"],
                    "cache_misses": result["cache_misses"],
                    "plan_hash": result["plan_hash"],
                    "steps": result["steps"],
                    "out_path": str(out_path),
                }
            )

        unique_plan_count = len(plan_hashes)
        scenario_result = {
            "name": scenario.name,
            "description": scenario.description,
            "db_path": str(db_path),
            "target_rows": scenario.target_rows,
            "generator_config": asdict(scenario.generator_config),
            "runs": run_results,
            "run_count": len(run_results),
            "unique_plan_count": unique_plan_count,
            "cache_hits": cache_hits,
            "cache_misses": cache_misses,
            "avg_duration_seconds": sum(durations) / len(durations),
            "status": "PASS" if unique_plan_count == len(run_results) else "FAIL",
        }
        summary["scenarios"].append(scenario_result)

    raw_path = run_dir / "benchmark_results.json"
    raw_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")

    report = _render_markdown(summary)
    report_path = run_dir / "benchmark_report.md"
    report_path.write_text(report, encoding="utf-8")

    latest_path = base_dir / "latest_benchmark_report.md"
    latest_path.write_text(report, encoding="utf-8")

    failed = [scenario["name"] for scenario in summary["scenarios"] if scenario["status"] != "PASS"]
    if failed:
        raise RuntimeError(
            "Benchmark finished but uniqueness checks failed for: "
            + ", ".join(failed)
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
    )
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
