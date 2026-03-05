import sqlite3
from pathlib import Path
from unittest.mock import patch

import pytest

from benchmark import run_benchmark


def _fake_generate_sample_dump(*, db_filename, **kwargs):
    conn = sqlite3.connect(db_filename)
    try:
        cur = conn.cursor()
        cur.execute("CREATE TABLE sample (id INTEGER PRIMARY KEY)")
        cur.execute("INSERT INTO sample (id) VALUES (1)")
        conn.commit()
    finally:
        conn.close()


def test_benchmark_runner_writes_report_and_passes_unique_hashes(tmp_path):
    counter = {"value": 0}

    def fake_run_agent_and_generate(**kwargs):
        counter["value"] += 1
        return {
            "plan": {"steps": []},
            "plan_hash": f"hash_{counter['value']}",
            "steps": 0,
            "tables": [],
            "cache_hits": 1,
            "cache_misses": 0,
            "messages": 2,
            "out_path": kwargs["out_path"],
            "prompt_note": kwargs["prompt_note"],
            "llm_stats": {
                "calls_total": 1,
                "network_requests": 1,
                "cache_hits": 0,
                "cache_misses": 1,
                "billed_prompt_tokens": 10,
                "billed_completion_tokens": 5,
                "billed_total_tokens": 15,
                "cached_prompt_tokens": 0,
                "cached_completion_tokens": 0,
                "cached_total_tokens": 0,
                "billed_cost_usd": 0.01,
                "cached_cost_usd": 0.0,
                "cache_hit_rate": 0.0,
                "logical_prompt_tokens": 10,
                "logical_completion_tokens": 5,
                "logical_total_tokens": 15,
                "logical_cost_usd": 0.01,
            },
        }

    with patch("benchmark.generate_sample_dump", side_effect=_fake_generate_sample_dump), patch(
        "benchmark.run_agent_and_generate", side_effect=fake_run_agent_and_generate
    ):
        summary = run_benchmark(
            api_key="key",
            model="model",
            output_dir=str(tmp_path),
            cache_dir=str(tmp_path / ".cache"),
        )

    assert len(summary["scenarios"]) == 4
    assert summary["status"] == "PASS"
    assert all(scenario["status"] == "PASS" for scenario in summary["scenarios"])
    assert (tmp_path / "latest_benchmark_report.md").exists()


def test_benchmark_runner_collects_issues_without_stopping(tmp_path):
    def fake_run_agent_and_generate(**kwargs):
        return {
            "plan": {"steps": []},
            "plan_hash": "duplicate_hash",
            "steps": 0,
            "tables": [],
            "cache_hits": 0,
            "cache_misses": 1,
            "messages": 2,
            "out_path": kwargs["out_path"],
            "prompt_note": kwargs["prompt_note"],
            "llm_stats": {
                "calls_total": 1,
                "network_requests": 1,
                "cache_hits": 0,
                "cache_misses": 1,
                "billed_prompt_tokens": 10,
                "billed_completion_tokens": 5,
                "billed_total_tokens": 15,
                "cached_prompt_tokens": 0,
                "cached_completion_tokens": 0,
                "cached_total_tokens": 0,
                "billed_cost_usd": 0.01,
                "cached_cost_usd": 0.0,
                "cache_hit_rate": 0.0,
                "logical_prompt_tokens": 10,
                "logical_completion_tokens": 5,
                "logical_total_tokens": 15,
                "logical_cost_usd": 0.01,
            },
        }

    with patch("benchmark.generate_sample_dump", side_effect=_fake_generate_sample_dump), patch(
        "benchmark.run_agent_and_generate", side_effect=fake_run_agent_and_generate
    ):
        summary = run_benchmark(
            api_key="key",
            model="model",
            output_dir=str(tmp_path),
            cache_dir=str(tmp_path / ".cache"),
        )

    assert summary["status"] == "ISSUES"
    assert all(scenario["status"] == "DUPLICATE_PLANS" for scenario in summary["scenarios"])
    reports = list(tmp_path.glob("*/benchmark_report.md"))
    assert reports


def test_benchmark_runner_continue_when_agent_throws(tmp_path):
    counter = {"value": 0}

    def fake_run_agent_and_generate(**kwargs):
        counter["value"] += 1
        if counter["value"] % 2 == 0:
            raise RuntimeError("simulated planner failure")
        return {
            "plan": {"steps": []},
            "plan_hash": f"hash_{counter['value']}",
            "steps": 0,
            "tables": [],
            "cache_hits": 0,
            "cache_misses": 1,
            "messages": 2,
            "out_path": kwargs["out_path"],
            "prompt_note": kwargs["prompt_note"],
            "llm_stats": {
                "calls_total": 1,
                "network_requests": 1,
                "cache_hits": 0,
                "cache_misses": 1,
                "billed_prompt_tokens": 10,
                "billed_completion_tokens": 5,
                "billed_total_tokens": 15,
                "cached_prompt_tokens": 0,
                "cached_completion_tokens": 0,
                "cached_total_tokens": 0,
                "billed_cost_usd": 0.01,
                "cached_cost_usd": 0.0,
                "cache_hit_rate": 0.0,
                "logical_prompt_tokens": 10,
                "logical_completion_tokens": 5,
                "logical_total_tokens": 15,
                "logical_cost_usd": 0.01,
            },
        }

    with patch("benchmark.generate_sample_dump", side_effect=_fake_generate_sample_dump), patch(
        "benchmark.run_agent_and_generate", side_effect=fake_run_agent_and_generate
    ):
        summary = run_benchmark(
            api_key="key",
            model="model",
            output_dir=str(tmp_path),
            cache_dir=str(tmp_path / ".cache"),
            max_scenarios=2,
        )

    assert summary["status"] == "ISSUES"
    assert len(summary["scenarios"]) == 2
    assert all(scenario["error_count"] > 0 for scenario in summary["scenarios"])
    assert all(any(run["result"] == "ERROR" for run in scenario["runs"]) for scenario in summary["scenarios"])


def test_benchmark_runner_can_fail_on_issues(tmp_path):
    def fake_run_agent_and_generate(**kwargs):
        return {
            "plan": {"steps": []},
            "plan_hash": "duplicate_hash",
            "steps": 0,
            "tables": [],
            "cache_hits": 0,
            "cache_misses": 1,
            "messages": 2,
            "out_path": kwargs["out_path"],
            "prompt_note": kwargs["prompt_note"],
            "llm_stats": {
                "calls_total": 1,
                "network_requests": 1,
                "cache_hits": 0,
                "cache_misses": 1,
                "billed_prompt_tokens": 10,
                "billed_completion_tokens": 5,
                "billed_total_tokens": 15,
                "cached_prompt_tokens": 0,
                "cached_completion_tokens": 0,
                "cached_total_tokens": 0,
                "billed_cost_usd": 0.01,
                "cached_cost_usd": 0.0,
                "cache_hit_rate": 0.0,
                "logical_prompt_tokens": 10,
                "logical_completion_tokens": 5,
                "logical_total_tokens": 15,
                "logical_cost_usd": 0.01,
            },
        }

    with patch("benchmark.generate_sample_dump", side_effect=_fake_generate_sample_dump), patch(
        "benchmark.run_agent_and_generate", side_effect=fake_run_agent_and_generate
    ):
        with pytest.raises(RuntimeError, match="Benchmark finished with issues"):
            run_benchmark(
                api_key="key",
                model="model",
                output_dir=str(tmp_path),
                cache_dir=str(tmp_path / ".cache"),
                fail_on_issues=True,
            )
