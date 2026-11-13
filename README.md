# Sales Data Pipeline

A small, local-first cloud data engineering pipeline. It takes raw retail sales data, cleans it with PySpark, stores it on AWS S3 as partitioned Parquet, queries it with Athena, and models it into an analytics-ready **star schema** in Snowflake using dbt.

> **For the AI agent:** read this file fully before writing any code. It is the source of truth for what we are building and how we work.

## 1. Architecture

```
Raw CSV -> [local PySpark dev/test] -> S3 (raw) -> Glue (same PySpark script) -> S3 (curated Parquet, partitioned)
                                                                                      |
                                                         +----------------------------+----------------+
                                                         v                                             v
                                                Athena (SQL queries)                  Snowflake (load) -> dbt (staging -> marts)
```

## 2. Tech stack

| Area | Choice |
|---|---|
| Language | Python 3.10+, PySpark, SQL |
| Local dev | PySpark installed locally (primary dev/test environment) |
| Storage | Amazon S3 (`raw/`, `curated/`, `athena-results/`) |
| Processing | AWS Glue (Spark job, run sparingly) |
| Query | Amazon Athena + Glue Data Catalog |
| Warehouse | Snowflake (free trial) |
| Transformation | dbt-core + dbt-snowflake |
| VCS | Git + GitHub |
| AWS region | `ap-south-1` (Mumbai) unless told otherwise |

## 3. Working agreement (for the agent)

1. **Small phases.** Finish and verify one phase before starting the next.
2. **Ask before big decisions** (dataset, region, naming, anything that costs money).
3. **Keep code simple.** Clear, commented code over clever code. No single "do everything" script.
4. **Tell me what to do manually** (AWS console steps, Snowflake signup, credentials). Do not assume access.
5. **Commit often** with Conventional Commit messages (e.g. `feat: add pyspark cleaning job`).
6. Briefly explain non-obvious design choices (partitioning scheme, schema grain, dbt materializations) in the README or code comments.

## 4. Priority tiers

Not every item needs to be built. A smaller finished project beats a large half-done one.

**Tier 1: must build**
- Python + PySpark cleaning job, developed and tested **locally first**
- S3 for raw and curated storage
- SQL queries (Athena and/or Snowflake)
- Git repo with clean history
- Star schema (fact + dimension tables)

**Tier 2: build if time allows**
- Snowflake load + dbt models and tests (if only one, prioritize **dbt**)
- Run the same PySpark script on **AWS Glue** a few times at the end
- Athena queries on the curated Parquet data

**Tier 3: optional, safe to skip**
- CloudFormation (resources can be created manually in the console; if skipped, document the manual steps)
- GitHub Actions CI, incremental dbt models, extra polish

Keep a short **"Built vs skipped"** section in this README, updated as work proceeds, so the project docs only claim what was actually done.

## 5. Cost and safety rules (non-negotiable)

- **Budget target: under about $10.** Almost all cost is Glue (about $0.44 per DPU-hour, minimum 2 DPUs). Use the **minimum worker config** and consider **Flex execution**. State the estimated cost before every Glue run.
- Develop and debug **locally**. Use Glue only for final runs.
- Do not use Glue interactive sessions/notebooks (they bill while open). Avoid crawlers; write Athena table DDL by hand instead.
- Set an **AWS Budget alert** (about $5) in Phase 0.
- **Snowflake trial:** sign up only when starting the Snowflake phase (the trial is time-limited, about 30 days). Use an X-Small warehouse with **auto-suspend at 60 seconds**, and suspend when idle.
- **Never commit secrets.** Use env vars, AWS profiles, a git-ignored `~/.dbt/profiles.yml`, and a `.env.example`.
- **Least-privilege IAM** for the Glue role. No admin access on job roles.
- Document a **teardown** path: delete S3 data and the Glue job, drop or suspend the Snowflake warehouse.

## 6. Dataset

Use a public retail/FMCG-style sales dataset (Kaggle or similar) with orders, products, customers/stores, dates, quantities and revenue, under about 1 GB.

- **Propose 2-3 options and let me choose.**
- It needs enough columns for a fact table and at least 3 dimensions (date, product, customer or store).
- It should include messy data (nulls, duplicates, inconsistent formats), or add some, so the cleaning step is meaningful.

## 7. Phases

**Phase 0: Setup.** Repo structure, `.gitignore`, README skeleton, Python venv, local PySpark, AWS CLI profile, AWS Budget alert.

**Phase 1: Local PySpark cleaning (Tier 1).** Read raw CSV, enforce schema, drop duplicates, handle nulls, standardize dates and types, derive columns, basic data quality checks (row counts, null counts). Write **Parquet partitioned by year/month** locally. Add a few pytest unit tests.

**Phase 2: S3 (Tier 1).** Create the bucket and folders. Upload raw data and curated Parquet.

**Phase 3: Athena (Tier 1/2).** Create the table over curated Parquet. Save 5+ analytical queries in `sql/athena/` (aggregations, joins, CTEs, window functions). Short note comparing CSV vs Parquet data scanned.

**Phase 4: Glue run (Tier 2).** Adapt the same script to run as a Glue job (minimum workers, Flex if suitable). Run it a few times and confirm the output matches the local run.

**Phase 5: Snowflake + dbt (Tier 2).** Load curated data into Snowflake. Build dbt `staging` models and `marts` (`fact_sales`, `dim_date`, `dim_product`, `dim_customer` or `dim_store`). Add dbt tests (`unique`, `not_null`, `relationships`) and generate docs/lineage.

**Phase 6: Polish.** Final README with architecture diagram, setup steps, design decisions, screenshots, "Built vs skipped" section, and teardown steps. Tier 3 items only if time remains.

## 8. Repository layout

```
sales-data-pipeline/
|-- README.md
|-- .gitignore
|-- .env.example
|-- data/sample/            # tiny sample only; full data not committed
|-- glue_jobs/clean_sales.py
|-- tests/
|-- sql/athena/
|-- snowflake/setup.sql
|-- dbt_project/
|   |-- dbt_project.yml
|   `-- models/{staging,marts}/
|-- scripts/
|-- infra/                  # optional (CloudFormation), Tier 3
`-- docs/architecture.png
```

## 9. Code standards

- Python: PEP 8, type hints where helpful, small functions, docstrings, `logging` instead of `print`.
- SQL: consistent style, CTEs over nested subqueries, meaningful aliases.
- dbt: one model per file, `ref()`/`source()` only, descriptions and tests in YAML.
- No hardcoded account IDs, bucket names or credentials.

## 10. Definition of done

- [ ] Tier 1 complete: local PySpark job, Parquet on S3, SQL queries, star schema designed, clean Git repo.
- [ ] Tier 2 items completed or explicitly marked as skipped.
- [ ] README reproducible step by step, with architecture diagram.
- [ ] "Built vs skipped" section is accurate.
- [ ] No secrets in Git history; teardown documented.

## 11. First instruction to the agent

Start with **Phase 0**. Before writing anything, confirm back: (1) your understanding of the project in 3-4 lines, (2) 2-3 dataset options, and (3) which accounts I need to prepare now (AWS, GitHub) versus later (Snowflake trial).

## 12. Built vs skipped

_To be filled in as the project progresses._

| Item | Status |
|---|---|
| Local PySpark cleaning | Not started |
| S3 storage | Not started |
| Athena queries | Not started |
| Glue job | Not started |
| Snowflake + dbt | Not started |
| CloudFormation | Not started |
