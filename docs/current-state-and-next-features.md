# Current State And Next Features

## Current State

The tool already supports SQLite and PostgreSQL, has a clean backend abstraction in `dump_reducer/db_tools/`, and includes a repeatable benchmark runner plus focused tests.

## Why Iteration Is Relatively Safe

The benchmark suite is already a useful regression net: multiple scenarios, fixed prompt variants, plan-hash checks, cache reuse, and reports. It is not perfect coverage, but it is big enough to try larger changes with reasonable confidence.

## Potential Next Features

### Add A New SQL Dialect / Driver

Oracle is a good example because the extension points are already clear in the codebase:

1. Add a new backend class such as `dump_reducer/db_tools/oracle.py` implementing `BaseDbTools`.
2. Register it in `dump_reducer/db_tools/__init__.py` so URL detection and dialect lookup can find it.
3. Add the Oracle driver dependency in `pyproject.toml` such as `oracledb`.
4. Define an Oracle `DialectSpec` with prompt guidance for Oracle-specific SQL behavior.
5. Add backend tests similar to the SQLite and PostgreSQL tool tests.

Main Oracle-specific decisions:

- decide where the subset lives: separate schema, same schema with prefixed tables, or temporary tables
- replace PostgreSQL / SQLite dedupe hints with Oracle-safe behavior such as `MERGE` or guarded `INSERT ... SELECT`
- account for syntax differences like `FETCH FIRST n ROWS ONLY` instead of `LIMIT`
- make sure dump/export logic works without relying on `pg_dump` or SQLite `iterdump`

### Benchmark Models Used Elsewhere In Deloitte

The benchmark flow is already model-agnostic. The next step is to turn it into a standard comparison process:

- identify the models that are common in other Deloitte services
- define a standard benchmark matrix across those models
- compare plan uniqueness, runtime, token cost, cache efficiency, and execution success rate
- keep one recommended default model and one cheaper fallback model

### Build An Application Around The Tool

A lightweight application would make the tool easier to use outside the CLI:

- expose the manual prompt note from the CLI in a web UI
- let users configure database connection, target row count, model, and prompt note
- save named benchmark or dump configurations
- run scheduled or manually triggered subset dumps
- show generated SQL, benchmark reports, and execution logs in the UI

### Explore Context Management

This is a strong research direction for planner quality:

- compress schema context after exploration so later steps keep only the important tables and relationships
- split exploration and plan generation into separate chats instead of one long conversation
- store a structured intermediate summary from `get_schema` / `get_stats` instead of replaying raw tool output
- compare one-shot planning versus staged planning with a handoff summary
