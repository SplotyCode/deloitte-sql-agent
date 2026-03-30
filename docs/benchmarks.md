# Benchmarks

## Benchmark Runner

`benchmark.py` runs repeated benchmarks across predefined scenarios.

Default benchmark shape:

- 4 scenarios (defaults to all 4)
- 10 runs per scenario
- 10 predefined prompt variants per scenario
- persistent cache reuse across reruns
- uniqueness check on final normalized plan hashes
- run-level failures are recorded and reported without stopping the full benchmark

Current scenarios include:

1. `synthetic_8_tables` (>100k rows)
2. `synthetic_20_tables` (>100k rows)
3. `wide_90_tables_mixed` (>100k rows, mixed hidden references)
4. `wide_96_tables_aggressive` (>100k rows, aggressive hidden references)

Each scenario uses a dedicated generated SQLite fixture and a fixed target row budget.

Run the benchmark:

```bash
uv run python benchmark.py \
  --model moonshotai/kimi-k2-thinking \
  --output-dir benchmark_artifacts \
  --cache-dir .cache/openrouter
```

Important arguments:

- `--runs-per-scenario`: defaults to `10`; must not exceed the predefined variant count
- `--max-scenarios`: run the first N scenarios (1-4)
- `--fail-on-issues`: optional strict mode; exits non-zero when any scenario has errors or duplicate plans
- `--output-dir`: where fixtures, outputs, JSON summaries, and Markdown reports are written
- `--cache-dir`: persistent cache location
- `--rebuild-fixtures`: regenerate fixture databases
- `--no-verify-ssl`: disable TLS verification

## How Uniqueness Is Enforced

The benchmark does not send the exact same prompt ten times. That would help cache reuse, but it would not guarantee ten distinct plans.

Instead:

1. Each scenario has ten fixed prompt variants.
2. Each variant adds a different planning bias, such as revenue focus, support focus, inventory focus, or hidden-reference focus.
3. The final plan JSON is normalized and hashed.
4. Any run failure is recorded in the report and benchmark execution continues.
5. A scenario is marked with issues when runs error out or duplicate plans appear.
6. If strict CI behavior is needed, use `--fail-on-issues`.

This gives you both:

- cache reuse across benchmark reruns
- ten expected distinct model outputs per scenario

## Benchmark Outputs

Each benchmark run writes:

- `benchmark_artifacts/<timestamp>/benchmark_results.json`
- `benchmark_artifacts/<timestamp>/benchmark_report.md`
- `benchmark_artifacts/latest_benchmark_report.md`
- per-scenario SQL outputs under `benchmark_artifacts/<timestamp>/<scenario>/`
- reusable fixture databases under `benchmark_artifacts/fixtures/`

The report includes:

- cache hits and misses
- average runtime per scenario
- plan hash per run
- pass/fail uniqueness status
