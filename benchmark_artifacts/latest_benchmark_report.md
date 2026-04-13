# Benchmark Report

- Timestamp: `20260305T092134Z`
- Model: `moonshotai/kimi-k2-thinking`
- Runs per scenario: `10`
- Scenario count: `4`
- Cache directory: `.cache/openrouter`
- Overall status: `ISSUES`

## OpenRouter Totals

- Calls: `100`
- Network requests: `8`
- Cache hits: `92`
- Cache misses: `8`
- Billed tokens: `31957`
- Cached tokens replayed: `300803`
- Logical tokens: `332760`
- Billed cost (USD): `0.019120`
- Cache hit rate: `92.00%`

## Scenario Summary

| Scenario | Success/Error | Unique plans | Cache hits | Cache misses | Avg seconds | Status |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| synthetic_8_tables | 10/0 | 1/10 | 64 | 6 | 5.96 | DUPLICATE_PLANS |
| synthetic_20_tables | 10/0 | 1/10 | 28 | 2 | 4.31 | DUPLICATE_PLANS |
| wide_90_tables_mixed | 0/10 | 0/0 | 0 | 0 | 10.57 | FAIL |
| wide_96_tables_aggressive | 0/10 | 0/0 | 0 | 0 | 5.95 | FAIL |

## Runs

### synthetic_8_tables

- Description: Synthetic 8-table fixture with >100k source rows and mixed hidden links.
- Fixture: `benchmark_artifacts\fixtures\synthetic_8_tables.db`
- Source rows: `120000`
- Target rows: `700`
- Status: `DUPLICATE_PLANS`
- Success/Error: `10/0`
- Unique plans: `1/10`

| Run | Variant | Result | Duration (s) | Cache hits | Cache misses | Plan hash | Output | Error |
| --- | --- | --- | ---: | ---: | ---: | --- | --- | --- |
| 1 | recent_activity | OK | 55.77 | 1 | 6 | `1851223eae2b` | `benchmark_artifacts\20260305T092134Z\synthetic_8_tables\01_recent_activity.sql` |  |
| 2 | high_value | OK | 0.47 | 7 | 0 | `1851223eae2b` | `benchmark_artifacts\20260305T092134Z\synthetic_8_tables\02_high_value.sql` |  |
| 3 | support_load | OK | 0.47 | 7 | 0 | `1851223eae2b` | `benchmark_artifacts\20260305T092134Z\synthetic_8_tables\03_support_load.sql` |  |
| 4 | project_chain | OK | 0.44 | 7 | 0 | `1851223eae2b` | `benchmark_artifacts\20260305T092134Z\synthetic_8_tables\04_project_chain.sql` |  |
| 5 | subscription_focus | OK | 0.44 | 7 | 0 | `1851223eae2b` | `benchmark_artifacts\20260305T092134Z\synthetic_8_tables\05_subscription_focus.sql` |  |
| 6 | compliance_focus | OK | 0.42 | 7 | 0 | `1851223eae2b` | `benchmark_artifacts\20260305T092134Z\synthetic_8_tables\06_compliance_focus.sql` |  |
| 7 | integration_focus | OK | 0.39 | 7 | 0 | `1851223eae2b` | `benchmark_artifacts\20260305T092134Z\synthetic_8_tables\07_integration_focus.sql` |  |
| 8 | inventory_focus | OK | 0.41 | 7 | 0 | `1851223eae2b` | `benchmark_artifacts\20260305T092134Z\synthetic_8_tables\08_inventory_focus.sql` |  |
| 9 | hidden_ref_focus | OK | 0.39 | 7 | 0 | `1851223eae2b` | `benchmark_artifacts\20260305T092134Z\synthetic_8_tables\09_hidden_ref_focus.sql` |  |
| 10 | balanced_cover | OK | 0.39 | 7 | 0 | `1851223eae2b` | `benchmark_artifacts\20260305T092134Z\synthetic_8_tables\10_balanced_cover.sql` |  |

### synthetic_20_tables

- Description: Synthetic 20-table fixture with >100k source rows and denser hidden links.
- Fixture: `benchmark_artifacts\fixtures\synthetic_20_tables.db`
- Source rows: `160000`
- Target rows: `900`
- Status: `DUPLICATE_PLANS`
- Success/Error: `10/0`
- Unique plans: `1/10`

| Run | Variant | Result | Duration (s) | Cache hits | Cache misses | Plan hash | Output | Error |
| --- | --- | --- | ---: | ---: | ---: | --- | --- | --- |
| 1 | recent_activity | OK | 39.64 | 1 | 2 | `fa5d5ec09f27` | `benchmark_artifacts\20260305T092134Z\synthetic_20_tables\01_recent_activity.sql` |  |
| 2 | high_value | OK | 0.43 | 3 | 0 | `fa5d5ec09f27` | `benchmark_artifacts\20260305T092134Z\synthetic_20_tables\02_high_value.sql` |  |
| 3 | support_load | OK | 0.37 | 3 | 0 | `fa5d5ec09f27` | `benchmark_artifacts\20260305T092134Z\synthetic_20_tables\03_support_load.sql` |  |
| 4 | project_chain | OK | 0.36 | 3 | 0 | `fa5d5ec09f27` | `benchmark_artifacts\20260305T092134Z\synthetic_20_tables\04_project_chain.sql` |  |
| 5 | subscription_focus | OK | 0.38 | 3 | 0 | `fa5d5ec09f27` | `benchmark_artifacts\20260305T092134Z\synthetic_20_tables\05_subscription_focus.sql` |  |
| 6 | compliance_focus | OK | 0.37 | 3 | 0 | `fa5d5ec09f27` | `benchmark_artifacts\20260305T092134Z\synthetic_20_tables\06_compliance_focus.sql` |  |
| 7 | integration_focus | OK | 0.37 | 3 | 0 | `fa5d5ec09f27` | `benchmark_artifacts\20260305T092134Z\synthetic_20_tables\07_integration_focus.sql` |  |
| 8 | inventory_focus | OK | 0.37 | 3 | 0 | `fa5d5ec09f27` | `benchmark_artifacts\20260305T092134Z\synthetic_20_tables\08_inventory_focus.sql` |  |
| 9 | hidden_ref_focus | OK | 0.38 | 3 | 0 | `fa5d5ec09f27` | `benchmark_artifacts\20260305T092134Z\synthetic_20_tables\09_hidden_ref_focus.sql` |  |
| 10 | balanced_cover | OK | 0.42 | 3 | 0 | `fa5d5ec09f27` | `benchmark_artifacts\20260305T092134Z\synthetic_20_tables\10_balanced_cover.sql` |  |

### wide_90_tables_mixed

- Description: Generated wide fixture (~90 tables) with mixed hidden references and high row volume.
- Fixture: `benchmark_artifacts\fixtures\wide_90_tables_mixed.db`
- Source rows: `256216`
- Target rows: `1400`
- Status: `FAIL`
- Success/Error: `0/10`
- Unique plans: `0/0`

| Run | Variant | Result | Duration (s) | Cache hits | Cache misses | Plan hash | Output | Error |
| --- | --- | --- | ---: | ---: | ---: | --- | --- | --- |
| 1 | recent_activity | ERROR | 96.83 | 0 | 0 | `` | `benchmark_artifacts\20260305T092134Z\wide_90_tables_mixed\01_recent_activity.sql` | You can only execute one statement at a time. |
| 2 | high_value | ERROR | 1.11 | 0 | 0 | `` | `benchmark_artifacts\20260305T092134Z\wide_90_tables_mixed\02_high_value.sql` | You can only execute one statement at a time. |
| 3 | support_load | ERROR | 0.97 | 0 | 0 | `` | `benchmark_artifacts\20260305T092134Z\wide_90_tables_mixed\03_support_load.sql` | You can only execute one statement at a time. |
| 4 | project_chain | ERROR | 0.97 | 0 | 0 | `` | `benchmark_artifacts\20260305T092134Z\wide_90_tables_mixed\04_project_chain.sql` | You can only execute one statement at a time. |
| 5 | subscription_focus | ERROR | 0.92 | 0 | 0 | `` | `benchmark_artifacts\20260305T092134Z\wide_90_tables_mixed\05_subscription_focus.sql` | You can only execute one statement at a time. |
| 6 | compliance_focus | ERROR | 1.05 | 0 | 0 | `` | `benchmark_artifacts\20260305T092134Z\wide_90_tables_mixed\06_compliance_focus.sql` | You can only execute one statement at a time. |
| 7 | integration_focus | ERROR | 0.93 | 0 | 0 | `` | `benchmark_artifacts\20260305T092134Z\wide_90_tables_mixed\07_integration_focus.sql` | You can only execute one statement at a time. |
| 8 | inventory_focus | ERROR | 0.96 | 0 | 0 | `` | `benchmark_artifacts\20260305T092134Z\wide_90_tables_mixed\08_inventory_focus.sql` | You can only execute one statement at a time. |
| 9 | hidden_ref_focus | ERROR | 0.95 | 0 | 0 | `` | `benchmark_artifacts\20260305T092134Z\wide_90_tables_mixed\09_hidden_ref_focus.sql` | You can only execute one statement at a time. |
| 10 | balanced_cover | ERROR | 1.07 | 0 | 0 | `` | `benchmark_artifacts\20260305T092134Z\wide_90_tables_mixed\10_balanced_cover.sql` | You can only execute one statement at a time. |

### wide_96_tables_aggressive

- Description: Generated wide fixture (~96 tables) with aggressive hidden references and high row volume.
- Fixture: `benchmark_artifacts\fixtures\wide_96_tables_aggressive.db`
- Source rows: `382232`
- Target rows: `1800`
- Status: `FAIL`
- Success/Error: `0/10`
- Unique plans: `0/0`

| Run | Variant | Result | Duration (s) | Cache hits | Cache misses | Plan hash | Output | Error |
| --- | --- | --- | ---: | ---: | ---: | --- | --- | --- |
| 1 | recent_activity | ERROR | 50.48 | 0 | 0 | `` | `benchmark_artifacts\20260305T092134Z\wide_96_tables_aggressive\01_recent_activity.sql` | UNIQUE constraint failed: warehouses.id |
| 2 | high_value | ERROR | 1.12 | 0 | 0 | `` | `benchmark_artifacts\20260305T092134Z\wide_96_tables_aggressive\02_high_value.sql` | UNIQUE constraint failed: warehouses.id |
| 3 | support_load | ERROR | 1.01 | 0 | 0 | `` | `benchmark_artifacts\20260305T092134Z\wide_96_tables_aggressive\03_support_load.sql` | UNIQUE constraint failed: warehouses.id |
| 4 | project_chain | ERROR | 0.98 | 0 | 0 | `` | `benchmark_artifacts\20260305T092134Z\wide_96_tables_aggressive\04_project_chain.sql` | UNIQUE constraint failed: warehouses.id |
| 5 | subscription_focus | ERROR | 0.98 | 0 | 0 | `` | `benchmark_artifacts\20260305T092134Z\wide_96_tables_aggressive\05_subscription_focus.sql` | UNIQUE constraint failed: warehouses.id |
| 6 | compliance_focus | ERROR | 1.00 | 0 | 0 | `` | `benchmark_artifacts\20260305T092134Z\wide_96_tables_aggressive\06_compliance_focus.sql` | UNIQUE constraint failed: warehouses.id |
| 7 | integration_focus | ERROR | 0.97 | 0 | 0 | `` | `benchmark_artifacts\20260305T092134Z\wide_96_tables_aggressive\07_integration_focus.sql` | UNIQUE constraint failed: warehouses.id |
| 8 | inventory_focus | ERROR | 0.98 | 0 | 0 | `` | `benchmark_artifacts\20260305T092134Z\wide_96_tables_aggressive\08_inventory_focus.sql` | UNIQUE constraint failed: warehouses.id |
| 9 | hidden_ref_focus | ERROR | 1.05 | 0 | 0 | `` | `benchmark_artifacts\20260305T092134Z\wide_96_tables_aggressive\09_hidden_ref_focus.sql` | UNIQUE constraint failed: warehouses.id |
| 10 | balanced_cover | ERROR | 0.96 | 0 | 0 | `` | `benchmark_artifacts\20260305T092134Z\wide_96_tables_aggressive\10_balanced_cover.sql` | UNIQUE constraint failed: warehouses.id |
