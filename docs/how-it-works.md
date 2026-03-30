# How It Works

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

The tool expects strict JSON from the model. The JSON must include a `steps` array and can also include `tables`.

## Main Scripts

The repository has three main scripts:

1. `main.py` for normal subsetting runs
2. `generate_sample_dump.py` for generating difficult SQLite fixtures
3. `benchmark.py` for repeatable evaluation across several scenarios

## OpenRouter Cache

OpenRouter requests are cached on disk in `dump_reducer/client.py`.

Cache properties:

- the cache key comes from the full model payload
- the same model, messages, and tools produce the same cache key
- repeated runs can reuse cached responses without calling OpenRouter again
- the cache stays on disk across process runs

Default cache location:

```text
.cache/openrouter
```

Each cache file stores:

- the request payload
- the raw OpenRouter response

This matters for benchmarks because the benchmark uses fixed prompt variants. If you rerun the same fixture, scenario, model, and variant note, it should hit cache.
