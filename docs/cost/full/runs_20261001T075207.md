# Data warehouse pipeline -- resource usage by full run

Generated 2026-10-01T07:52:07.705726+00:00 UTC. Dataset: fulldataset. Last 8 run(s) of 8 found.
- **Since cutoff:** `2026-09-19 17:41:02.235776` -- 0 run(s) excluded as started before it (8 of 8 eligible)

## Detailed data

### full_run_id 1

- Started: 2026-09-19T17:41:02.235776  Completed: 2026-09-19T18:25:41.810768
- Wall clock: 44m39s  Tables loaded: 65 (65 succeeded, 0 failed)
- Glue: 5364.00 DPU-sec ($0.6556)
- CloudWatch bytes ingested (shared log group total): 9.88 MiB
- CloudWatch (attributed to this run): n/a
- Rows: raw 20,568,186  gold 40,626,166

| process_type | tables | execution_seconds |
|---|---|---|
| agg | 3 | 42.9 |
| dim | 8 | 44.4 |
| fact | 23 | 1702.0 |
| staging | 31 | 742.7 |

### full_run_id 2

- Started: 2026-09-19T19:57:06.771674  Completed: 2026-09-19T20:35:56.290840
- Wall clock: 38m49s  Tables loaded: 65 (65 succeeded, 0 failed)
- Glue: 4657.00 DPU-sec ($0.5692)
- CloudWatch bytes ingested (shared log group total): 14.29 MiB
- CloudWatch (attributed to this run): n/a
- Rows: raw 20,568,186  gold 40,627,781

| process_type | tables | execution_seconds |
|---|---|---|
| agg | 3 | 45.0 |
| dim | 8 | 68.6 |
| fact | 23 | 1314.0 |
| staging | 31 | 750.7 |

### full_run_id 3

- Started: 2026-09-19T20:42:18.283884  Completed: 2026-09-19T21:20:16.168430
- Wall clock: 37m57s  Tables loaded: 65 (65 succeeded, 0 failed)
- Glue: 4550.00 DPU-sec ($0.5561)
- CloudWatch bytes ingested (shared log group total): 13.36 MiB
- CloudWatch (attributed to this run): n/a
- Rows: raw 20,568,186  gold 40,629,396

| process_type | tables | execution_seconds |
|---|---|---|
| agg | 3 | 47.0 |
| dim | 8 | 74.5 |
| fact | 23 | 1245.4 |
| staging | 31 | 758.5 |

### full_run_id 4

- Started: 2026-09-19T22:12:27.422217  Completed: 2026-09-19T22:49:27.367850
- Wall clock: 36m59s  Tables loaded: 65 (65 succeeded, 0 failed)
- Glue: 4446.00 DPU-sec ($0.5434)
- CloudWatch bytes ingested (shared log group total): 2.09 MiB
- CloudWatch (attributed to this run): n/a
- Rows: raw 20,568,186  gold 40,631,011

| process_type | tables | execution_seconds |
|---|---|---|
| agg | 3 | 45.5 |
| dim | 8 | 82.3 |
| fact | 23 | 1213.6 |
| staging | 31 | 742.1 |

### full_run_id 5

- Started: 2026-09-19T23:27:24.564409  Completed: 2026-09-20T00:04:50.357988
- Wall clock: 37m25s  Tables loaded: 65 (65 succeeded, 0 failed)
- Glue: 4459.00 DPU-sec ($0.5450)
- CloudWatch bytes ingested (shared log group total): 1.13 GiB
- CloudWatch (attributed to this run): n/a
- Rows: raw 20,568,186  gold 40,632,626

| process_type | tables | execution_seconds |
|---|---|---|
| agg | 3 | 47.8 |
| dim | 8 | 76.2 |
| fact | 23 | 1221.5 |
| staging | 31 | 743.1 |

### full_run_id 6

- Started: 2026-09-21T10:42:37.240036  Completed: 2026-09-21T11:21:02.815163
- Wall clock: 38m25s  Tables loaded: 65 (65 succeeded, 0 failed)
- Glue: 4589.00 DPU-sec ($0.5609)
- CloudWatch bytes ingested (shared log group total): 37.65 MiB
- CloudWatch (attributed to this run): n/a
- Rows: raw 20,568,186  gold 40,634,241

| process_type | tables | execution_seconds |
|---|---|---|
| agg | 3 | 48.3 |
| dim | 8 | 80.7 |
| fact | 23 | 1265.6 |
| staging | 31 | 754.9 |

### full_run_id 7

- Started: 2026-09-27T15:49:06.983541  Completed: 2026-09-27T16:29:10.883643
- Wall clock: 40m03s  Tables loaded: 65 (65 succeeded, 0 failed)
- Glue: 4833.00 DPU-sec ($0.5907)
- CloudWatch bytes ingested (shared log group total): 26.92 MiB
- CloudWatch bytes attributed (this run's own jobs): 1.12 MiB
- Rows: raw 20,568,186  gold 40,635,856

| process_type | tables | execution_seconds |
|---|---|---|
| agg | 3 | 47.2 |
| dim | 8 | 81.6 |
| fact | 23 | 1319.5 |
| staging | 31 | 813.3 |

### full_run_id 8

- Started: 2026-09-27T16:47:45.926481  Completed: 2026-09-27T17:25:50.567183
- Wall clock: 38m04s  Tables loaded: 65 (65 succeeded, 0 failed)
- Glue: 4575.00 DPU-sec ($0.5592)
- CloudWatch bytes ingested (shared log group total): 793.44 MiB
- CloudWatch bytes attributed (this run's own jobs): 1.12 MiB
- Rows: raw 20,568,186  gold 40,637,471

| process_type | tables | execution_seconds |
|---|---|---|
| agg | 3 | 49.4 |
| dim | 8 | 81.6 |
| fact | 23 | 1246.1 |
| staging | 31 | 764.8 |

## Statistical analysis across runs

n = 8 run(s).

| metric | mean | median | stdev | min | max |
|---|---|---|---|---|---|
| wall_clock_seconds | 2,343.354 | 2,295.108 | 146.780 | 2,219.946 | 2,679.575 |
| total_dpu_seconds | 4,684.125 | 4,582.000 | 300.348 | 4,446.000 | 5,364.000 |
| total_dpu_hours | 1.301 | 1.273 | 0.083 | 1.235 | 1.490 |
| total_execution_seconds | 2,195.099 | 2,145.652 | 147.267 | 2,083.604 | 2,532.077 |
| glue_cost_usd | 0.573 | 0.560 | 0.037 | 0.543 | 0.656 |
| cloudwatch_bytes_ingested | 269,092,580.625 | 21,607,778.500 | 475,631,097.126 | 2,189,168.000 | 1,211,518,853.000 |
| cloudwatch_bytes_attributed | 1,173,334.500 | 1,173,334.500 | 5,735.343 | 1,169,279.000 | 1,177,390.000 |
| job_count | 65 | 65.000 | 0.000 | 65 | 65 |
| failed_count | 0 | 0.000 | 0.000 | 0 | 0 |
| total_raw_rows | 20,568,186 | 20,568,186.000 | 0.000 | 20,568,186 | 20,568,186 |
| total_gold_rows | 40,631,818.500 | 40,631,818.500 | 3,955.926 | 40,626,166 | 40,637,471 |

## Current resource footprint (point-in-time, not per-run)

Aurora storage, S3, and CloudWatch log retention don't have a clean per-run reading (these metrics are daily-to-hourly and lag) -- this is the current state, for context alongside the per-run compute/logging trend above.

- **Measured:** 2026-10-01T07:52:06.946641+00:00
- **Aurora** `mimic4rds` (aurora-postgresql 17.9): 8.83 GiB volume, min/max ACU 0.0/4.0, 74.738 ACU-hr since 2026-09-19
- **Aurora cost**: $19.7415 compute+I/O ($8.9685 ACU + $10.7730 I/O, 53,865,025 requests) since 2026-09-19, plus $0.9480/month storage at current volume
- **Glue**: 14.6314 total DPU-hours across 5 job(s)
- **S3** `mimic4-dw-sourcefiles-full`: 10.23 GiB across 219 objects
- **CloudWatch log groups**: 1.79 GiB across 3 group(s)
- **IAM roles**: 2
- **Load + 1 year steady-state**: $12.60 (load $0.5333 + annual steady-state $12.07)
