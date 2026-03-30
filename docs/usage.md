# Usage

## Planner

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
- `--target-rows`: rough row budget
- `--out`: output SQL file
- `--model`: OpenRouter model id
- `--api-key`: OpenRouter API key, or use env variable `OPENROUTER_API_KEY`
- `--prompt-note`: extra guidance added to the user prompt
- `--cache-dir`: cache directory for OpenRouter responses; pass an empty string to disable cache
- `--no-verify-ssl`: disable TLS verification for OpenRouter requests

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

`generate_sample_dump.py` creates large SQLite fixtures for development and benchmarking. It uses scenarios instead of one small fixed schema.

Base properties:

- 89-table enterprise-style schema by default
- cross-domain entities: users, projects, tasks, products, orders, invoices, incidents, tickets, compliance, analytics, notifications, imports
- declared foreign keys
- hidden references in payloads, shadow columns, hint tables, and alias tables
- deterministic generation with a seed

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
- per-entity overrides: `--tenants`, `--users-per-tenant`, `--products-per-tenant`, `--orders-per-tenant`, `--tickets-per-tenant`, `--projects-per-tenant`, `--incidents-per-tenant`
- `--skip-sql-dump`: generate only the SQLite database

Scale notes:

- `tiny` is for fast local checks. It is not meant for realistic volume benchmarks.
- Use explicit per-entity overrides (`--tenants`, `--users-per-tenant`, `--orders-per-tenant`, and others) when you need high source-row counts.

## Hidden Reference Carriers

The generator also creates relationships that are not declared as foreign keys. These cases are meant to challenge the subset agent.

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

## Development

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

- Use `support` or `commerce` scenarios.
- Increase `--min-tables`.
- Use `--hidden-reference-mode aggressive`.
