# MyDocument

## Solution approach

I implemented the assessment as a modular PySpark pipeline under `code/nyc_jobs/` with clear responsibility boundaries:

- `cleaning.py`: schema normalization and salary annualization
- `features.py`: feature engineering
- `exploration.py`: profiling and removable column detection
- `kpis.py`: KPI transformations
- `visualization.py`: chart generation
- `pipeline.py`: orchestration and output persistence

## Data exploration performed

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

1. Number of job postings per category (Top 10):
- `jobs_posting_top_categories`

2. Salary distribution per category:
- `salary_distribution_per_category`
- min, p25, median, p75, max, avg on annualized salary midpoint

3. Correlation between degree requirement and salary:
- `degree_salary_correlation`
- maps degree requirement to ordinal rank and computes Pearson correlation

4. Highest salary job posting per agency:
- `highest_salary_job_per_agency`
- window function with `row_number` by agency

5. Average salary per agency for last 2 years:
- `average_salary_per_agency_last_2_years`
- cutoff relative to max posting date available in the dataset

6. Highest paid skills:
- `highest_paid_skills`
- keyword-based skill extraction from job text, aggregated by average salary

## Feature engineering

Implemented at least 3 techniques:

1. Salary annualization:
- Converts annual/daily/hourly salaries to a common annual scale

2. Degree extraction:
- Infers `highest_degree_required` and numeric `degree_rank` from text

3. Experience extraction:
- Extracts `years_experience_required` from minimum qualifications text

4. Temporal features:
- `posting_year`, `posting_month`, `posting_quarter`

5. Text combination for skill analysis:
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

## Tests

Unit tests are provided under `code/tests/`:

- annualization correctness (`test_cleaning.py`)
- top category KPI correctness (`test_kpis.py`)
- highest salary per agency KPI correctness (`test_kpis.py`)

## Trigger/automation proposal

Recommended trigger options:

1. Local manual run:
- `python code/run_pipeline.py`

2. Docker container run (inside `jupyter` or `master`):
- `python /app/run_pipeline.py --cluster`

3. Scheduled orchestration:
- Airflow DAG or cron that runs daily/weekly and publishes KPI outputs to an object store or warehouse.

## Deployment proposal

For production-style deployment:

1. Package this code into a Docker image.
2. Parameterize input/output paths and date windows.
3. Run via orchestration (Airflow/Prefect).
4. Persist final datasets to S3/ADLS and KPIs to analytics DB.
5. Add data quality checks and alerting.

## Assumptions and considerations

- Salary annualization factors:
  - annual: 1
  - daily: 260 workdays/year
  - hourly: 2080 hours/year
- Skill extraction is keyword-based and can be improved with NLP/entity extraction.
- Degree and experience extraction are rule-based and may miss edge phrasing.
- Correlation result should be interpreted directionally (not causal).

## Challenges and learnings

- The source has mixed data quality and sparse categories in text fields.
- Converting all salary formats to one comparable metric is necessary before any salary KPI.
- Window functions and controlled text normalization were key to stable KPI logic.
