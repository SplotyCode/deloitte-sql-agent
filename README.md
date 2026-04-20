# Deloitte SQL Agent

`deloitte-sql-agent` is an LLM-based tool for database subsetting in PostgreSQL and SQLite. It reads a source database, asks an LLM for a subset plan, runs that plan in a `subset` schema, and exports the result as SQL.

The repository includes three main entry points:

1. `main.py` for normal subsetting runs
2. `generate_sample_dump.py` for large, difficult SQLite fixtures
3. `benchmark.py` for repeatable LLM benchmarks across several scenarios, with cache reuse

Additional project context:

- [LEARNINGS.md](./LEARNINGS.md) contains design and evaluation notes from recent work

## Documentation

- [Usage](./docs/usage.md): running the planner, generating fixtures, and development commands
- [How It Works](./docs/how-it-works.md): planner loop, tool calls, execution flow, and cache behavior
- [Benchmarks](./docs/benchmarks.md): benchmark scenarios, uniqueness checks, outputs, and reporting
- [Current State And Next Features](./docs/current-state-and-next-features.md): short roadmap note covering extension points and likely next steps

## Quick Start

Run a normal subsetting job:

```bash
uv run python main.py \
  --db-url sqlite:///absolute/path/to/source.db \
  --target-rows 1000 \
  --out subset.sql \
  --model moonshotai/kimi-k2-thinking
```

See [Usage](./docs/usage.md) for the full CLI reference, fixture generator options, and development commands.

## Benchmarking

Use the benchmark runner for repeatable model evaluation across several scenarios. It also reuses cache and checks if plans are unique.

```bash
uv run python benchmark.py \
  --model moonshotai/kimi-k2-thinking \
  --output-dir benchmark_artifacts \
  --cache-dir .cache/openrouter
```

See [Benchmarks](./docs/benchmarks.md) for scenario details, output files, and strict-mode behavior.

## Internals

Start with [How It Works](./docs/how-it-works.md).
