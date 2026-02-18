# MyDocument

## Executive summary

This solution implements the NYC jobs assessment as a modular PySpark pipeline with:
- reproducible data processing and feature engineering
- KPI computation as reusable functions
- automated output generation (CSV, Parquet, charts)
- unit tests for core logic
- documentation for assumptions, tradeoffs, and deployment approach

## Solution architecture

The implementation is organized under `code/nyc_jobs/` with clear boundaries:

- `cleaning.py`: schema normalization and salary annualization
- `features.py`: feature engineering
- `exploration.py`: profiling and removable column detection
- `kpis.py`: KPI transformations
- `visualization.py`: chart generation
- `pipeline.py`: orchestration and output persistence

## Data exploration summary

The source dataset has 28 columns with mixed types:

- Numeric-like columns (stored as text in CSV): `Salary Range From`, `Salary Range To`, `# Of Positions`, `Level`
- Date-time columns (ISO-like timestamp strings): `Posting Date`, `Posting Updated`, `Process Date`
- Categorical columns: `Agency`, `Posting Type`, `Job Category`, `Salary Frequency`, `Civil Service Title`, etc.
- Long-form text columns: `Job Description`, `Minimum Qual Requirements`, `Preferred Skills`

Profiling output is written to `code/output/column_profile.csv` with:
- data type
- row count
- null count
- non-null count
- distinct count

## KPI implementation

1. Number of job postings per category (Top 10)
- `jobs_posting_top_categories`

2. Salary distribution per category
- `salary_distribution_per_category`
- min, p25, median, p75, max, avg on annualized salary midpoint

3. Correlation between degree requirement and salary
- `degree_salary_correlation`
- maps degree requirement to ordinal rank and computes Pearson correlation

4. Highest salary job posting per agency
- `highest_salary_job_per_agency`
- window function with `row_number` by agency

5. Average salary per agency for last 2 years
- `average_salary_per_agency_last_2_years`
- cutoff relative to max posting date available in the dataset

6. Highest paid skills
- `highest_paid_skills`
- keyword-based skill extraction from job text, aggregated by average salary

## Data processing and feature engineering

Implemented at least 3 techniques:

1. Salary annualization
- Converts annual/daily/hourly salaries to a common annual scale

2. Degree extraction
- Infers `highest_degree_required` and numeric `degree_rank` from text

3. Experience extraction
- Extracts `years_experience_required` from minimum qualifications text

4. Temporal features
- `posting_year`, `posting_month`, `posting_quarter`

5. Text combination for skill analysis
- `combined_text` for skill matching

## Feature removal logic

Low-signal columns are identified by near-unique ratio (`>= 0.98`) and removed from final processed output in pipeline scope:
- `To Apply`
- `Recruitment Contact`
- `Work Location 1`
- `Post Until`

Removed columns are logged to `code/output/removed_columns.txt`.

## Output artifacts

Generated outputs:

- `code/output/processed_jobs.parquet`
- KPI CSV files: `code/output/kpi_*.csv`
- Charts:
  - `code/output/charts/top_categories.png`
  - `code/output/charts/salary_by_category.png`
  - `code/output/charts/salary_distribution_box.png`
  - `code/output/charts/avg_salary_per_agency_last_2_years.png`
  - `code/output/charts/highest_paid_skills.png`
  - `code/output/charts/highest_salary_roles.png`
  - `code/output/charts/degree_salary_correlation.png`
  - `code/output/visual_report.md`

## Tests

Unit tests are provided under `code/tests/`:

- annualization correctness (`test_cleaning.py`)
- top category KPI correctness (`test_kpis.py`)
- highest salary per agency KPI correctness (`test_kpis.py`)

## What I learned during development

1. Data normalization is mandatory before analytics
Annual, daily, and hourly salary values are not directly comparable. Converting to one annualized metric is the first step to make KPIs meaningful.

2. Text-heavy public datasets need robust rule-based parsing
Degree and skill inference from free text is possible with practical regex/keyword methods, but results depend on phrase coverage and normalization quality.

3. Spark compatibility details matter in real projects
Spark 2.4 has constraints (for example, parquet column naming and API differences) that require implementation adjustments for stable execution.

4. Small modular functions improve reliability and maintainability
Splitting logic into cleaning/features/KPIs/visualization made testing, debugging, and extension much easier than notebook-only code.

5. Visualization improves communication of data engineering outputs
CSV outputs are technically complete, but chart artifacts make insights easier to review quickly during technical discussion.

## Proposal: Trigger Strategy

Recommended trigger options:

1. Local manual run
- `python code/run_pipeline.py`

2. Docker container run (assessment environment)
- `docker exec -it jupyter bash -lc "cd /app && /opt/spark/bin/spark-submit run_pipeline.py"`

3. Scheduled orchestration
- Airflow/Prefect scheduled job (daily/weekly) that runs pipeline and publishes outputs.

## Proposal: Deployment Steps

Production-style deployment proposal:

1. Containerize pipeline as a dedicated runtime image.
2. Externalize configuration (input/output path, schedule, retention).
3. Orchestrate with Airflow/Prefect/Kubernetes CronJob.
4. Persist processed data and KPI outputs to cloud storage/warehouse.
5. Add data quality checks, logging, monitoring, and alerting.
6. Add CI checks for tests and style before deployment.

## Challenges and learnings

- The source has mixed data quality and sparse categories in text fields.
- Converting all salary formats to one comparable metric is necessary before any salary KPI.
- Window functions and controlled text normalization were key to stable KPI logic.
