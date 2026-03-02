from pathlib import Path
from unittest.mock import patch

import pytest

from benchmark import run_benchmark


def _fake_generate_sample_dump(*, db_filename, **kwargs):
    Path(db_filename).write_text("fixture", encoding="utf-8")


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
    assert all(scenario["status"] == "PASS" for scenario in summary["scenarios"])
    assert (tmp_path / "latest_benchmark_report.md").exists()


def test_benchmark_runner_fails_when_unique_plan_count_drops(tmp_path):
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
        }

    with patch("benchmark.generate_sample_dump", side_effect=_fake_generate_sample_dump), patch(
        "benchmark.run_agent_and_generate", side_effect=fake_run_agent_and_generate
    ):
        with pytest.raises(RuntimeError, match="uniqueness checks failed"):
            run_benchmark(
                api_key="key",
                model="model",
                output_dir=str(tmp_path),
                cache_dir=str(tmp_path / ".cache"),
            )

    reports = list(tmp_path.glob("*/benchmark_report.md"))
    assert reports
