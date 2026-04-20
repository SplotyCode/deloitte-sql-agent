# Benchmark Report

- Timestamp: `20260420T101147Z`
- Model: `moonshotai/kimi-k2-thinking`
- Runs per scenario: `1`
- Scenario count: `2`
- Cache directory: `.cache/openrouter`
- Overall status: `ISSUES`

## OpenRouter Totals

- Calls: `18`
- Network requests: `18`
- Cache hits: `0`
- Cache misses: `18`
- Billed tokens: `298239`
- Cached tokens replayed: `0`
- Logical tokens: `298239`
- Billed cost (USD): `0.150906`
- Cache hit rate: `0.00%`

## Scenario Summary

| Scenario | Success/Error | Unique plans | Cache hits | Cache misses | Avg seconds | Status |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| wide_90_tables_mixed | 0/1 | 0/0 | 0 | 0 | 88.50 | FAIL |
| wide_96_tables_aggressive | 1/0 | 1/1 | 0 | 18 | 308.94 | PASS |

## Runs

### wide_90_tables_mixed

- Description: Generated wide fixture (~90 tables) with mixed hidden references and high row volume.
- Fixture: `benchmark_artifacts/wide_kimi_repair_smoke/fixtures/wide_90_tables_mixed.db`
- Source rows: `256216`
- Target rows: `1400`
- Status: `FAIL`
- Success/Error: `0/1`
- Unique plans: `0/0`

| Run | Variant | Result | Duration (s) | Cache hits | Cache misses | Plan hash | Output | Error |
| --- | --- | --- | ---: | ---: | ---: | --- | --- | --- |
| 1 | recent_activity | ERROR | 88.50 | 0 | 0 | `` | `benchmark_artifacts/wide_kimi_repair_smoke/20260420T101147Z/wide_90_tables_mixed/01_recent_activity.sql` | 429 Client Error: Too Many Requests for url: https://openrouter.ai/api/v1/chat/completions |

### wide_96_tables_aggressive

- Description: Generated wide fixture (~96 tables) with aggressive hidden references and high row volume.
- Fixture: `benchmark_artifacts/wide_kimi_repair_smoke/fixtures/wide_96_tables_aggressive.db`
- Source rows: `382232`
- Target rows: `1800`
- Status: `PASS`
- Success/Error: `1/0`
- Unique plans: `1/1`

| Run | Variant | Result | Duration (s) | Cache hits | Cache misses | Plan hash | Output | Error |
| --- | --- | --- | ---: | ---: | ---: | --- | --- | --- |
| 1 | recent_activity | OK | 308.94 | 0 | 18 | `06139def9d45` | `benchmark_artifacts/wide_kimi_repair_smoke/20260420T101147Z/wide_96_tables_aggressive/01_recent_activity.sql` |  |
