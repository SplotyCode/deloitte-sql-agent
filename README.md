# Deloitte SQL Agent

`deloitte-sql-agent` is an LLM-assisted database subsetting tool for PostgreSQL and SQLite. It inspects a source database, asks an LLM to build a subset plan, executes that plan into a `subset` schema, and exports the resulting SQL.

The repository now includes three working surfaces:

1. `main.py` for normal subsetting runs
2. `generate_sample_dump.py` for large, difficult SQLite fixtures
3. `benchmark.py` for repeatable multi-scenario LLM benchmarks with persistent cache reuse

## Core Flow

The planner loop in `dump_reducer/planner.py` works like this:

1. Build a system prompt that defines the subsetting task.
2. Send a user prompt with the row budget and optional operator note.
3. Let the model call tools:
   - `get_schema`
   - `get_stats`
   - `query_sql`
4. Parse the final JSON plan.
5. Create an empty `subset` schema.
6. Execute the generated SQL steps.
7. Clean dangling references.
8. Dump the subset schema to SQL.

The tool expects the model to return strict JSON with a `steps` array and optional `tables`.

## Planner Usage

Run a normal subsetting job:

```bash
uv run python main.py \
  --db-url sqlite:///absolute/path/to/source.db \
  --target-rows 1000 \
  --out subset.sql \
  --model moonshotai/kimi-k2-thinking
```

Important arguments:

- `--db-url`: PostgreSQL URL or SQLite path/URL
- `--target-rows`: rough total size budget
- `--out`: output SQL file
- `--model`: OpenRouter model id
- `--api-key`: OpenRouter API key, or use `OPENROUTER_API_KEY`
- `--prompt-note`: extra operator guidance appended to the user prompt
- `--cache-dir`: persistent cache directory for OpenRouter responses; pass an empty string to disable cache
- `--no-verify-ssl`: disables TLS verification for OpenRouter requests

Example with extra guidance:

```bash
uv run python main.py \
  --db-url sqlite:///absolute/path/to/source.db \
  --target-rows 1200 \
  --out subset.sql \
  --prompt-note "Prefer a strategy that preserves hidden operational links." \
  --cache-dir .cache/openrouter
```

## Fixture Generator

`generate_sample_dump.py` creates large SQLite fixtures for development and benchmarking. The new generator is scenario-driven, not hard-coded to a tiny e-commerce schema.

Base properties:

- 89-table enterprise-style schema by default
- cross-domain entities: users, projects, tasks, products, orders, invoices, incidents, tickets, compliance, analytics, notifications, imports
- declared foreign keys
- hidden references in payloads, shadow columns, hint tables, and alias tables
- deterministic generation through a seed

Generator usage:

```bash
uv run python generate_sample_dump.py \
  --db-filename sample_dump.db \
  --filename sample_dump.sql \
  --scenario support \
  --scale medium \
  --hidden-reference-mode aggressive \
  --min-tables 92
```

Important arguments:

- `--scenario`: `enterprise`, `commerce`, `support`
- `--scale`: `tiny`, `small`, `medium`, `large`
- `--hidden-reference-mode`: `off`, `light`, `mixed`, `aggressive`
- `--min-tables`: minimum total table count
- `--extra-noise-tables`: add more challenge tables beyond the base schema
- `--seed`: deterministic generation seed
- per-entity overrides:
  - `--tenants`
  - `--users-per-tenant`
  - `--products-per-tenant`
  - `--orders-per-tenant`
  - `--tickets-per-tenant`
  - `--projects-per-tenant`
  - `--incidents-per-tenant`
- `--skip-sql-dump`: generate only the SQLite database

### Hidden Reference Carriers

The generator deliberately creates relationships that are not declared as foreign keys. These are intended to challenge the subset agent.

Examples:

- `tasks.shadow_order_id`
- `tickets.shadow_order_id`
- `orders.shadow_project_id`
- `notifications.context_entity_type` + `notifications.context_entity_id`
- `dashboard_widgets.source_table` + `dashboard_widgets.source_record_id`
- `reports.filter_payload`
- `webhook_deliveries.request_body`
- `ml_training_examples.hidden_refs_json`
- `import_rows.resolved_entity_id`
- `entity_aliases`
- `relationship_hints`

## OpenRouter Cache

OpenRouter requests are cached on disk in `dump_reducer/client.py`.

Cache properties:

- cache key is derived from the full model payload
- same model + same messages + same tools => same cache key
- repeated runs can reuse cached model responses without re-calling OpenRouter
- cache is persistent across process runs

Default cache location:

```text
.cache/openrouter
```

Each cache file stores:

- the request payload
- the raw OpenRouter response

This matters for benchmarking because the benchmark reuses fixed prompt variants. A rerun with the same fixture, scenario, model, and variant note will hit cache.

## Benchmark Runner

`benchmark.py` automates repeated benchmark execution over four predefined scenarios.

Default benchmark shape:

- 4 scenarios
- 10 runs per scenario
- 10 predefined prompt variants per scenario
- persistent cache reuse across reruns
- uniqueness check on final normalized plan hashes

The four default scenarios are:

1. `enterprise_baseline`
2. `commerce_hidden`
3. `support_hidden`
4. `stress_aggressive`

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
- `--output-dir`: where fixtures, outputs, JSON summaries, and Markdown reports are written
- `--cache-dir`: persistent cache location
- `--rebuild-fixtures`: regenerate fixture databases
- `--no-verify-ssl`: disable TLS verification

### How Uniqueness Is Enforced

The benchmark does not send the exact same prompt ten times. That would make cache reuse useful but would not guarantee ten distinct plans.

Instead:

1. Each scenario has ten fixed prompt variants.
2. Each variant injects a specific planning bias such as revenue focus, support focus, inventory focus, or hidden-reference focus.
3. The final plan JSON is normalized and hashed.
4. The benchmark fails if a scenario does not produce ten unique final plan hashes.

This gives both:

- cache reuse across benchmark reruns
- ten expected distinct model outputs per scenario

### Benchmark Outputs

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

## Development Notes

Run tests:

```bash
uv run pytest
```

Focused test runs:

```bash
uv run pytest tests/test_sample_dump_generator.py
uv run pytest tests/test_sqlite_tools.py
```

## Practical Guidance

For normal iteration:

1. Generate or reuse a difficult SQLite fixture.
2. Run `main.py` with a cache directory enabled.
3. When evaluating model quality, run `benchmark.py`.
4. Inspect the Markdown benchmark report before changing prompts.

For hard cases:

- use `support` or `commerce` scenarios
- increase `--min-tables`
- use `--hidden-reference-mode aggressive`
- add `--extra-noise-tables`
- keep cache enabled so repeated benchmarks are cheap
