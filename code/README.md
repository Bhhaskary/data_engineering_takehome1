# NYC Jobs PySpark Assessment Solution

This folder contains a modular PySpark solution for the assessment:

- Data exploration and profiling
- KPI computation
- Data processing and feature engineering
- Visualization outputs
- Unit tests

## Structure

`nyc_jobs/`
- `config.py`: project constants and paths
- `spark.py`: Spark session factory
- `io.py`: read/write helpers
- `cleaning.py`: base data cleaning and type normalization
- `features.py`: feature engineering transformations
- `kpis.py`: KPI builders
- `visualization.py`: chart rendering
- `pipeline.py`: end-to-end orchestration

`tests/`
- `test_cleaning.py`
- `test_kpis.py`

## Run

From the `code/` folder:

```powershell
python run_pipeline.py
pytest tests -q
```

With docker compose stack running, execute from container:

```powershell
docker exec -it jupyter bash -lc "cd /app && /opt/spark/bin/spark-submit run_pipeline.py"
docker exec -it jupyter bash -lc "cd /app && pytest tests -q"
```

Inside docker containers, use equivalent python invocation if required.

## Outputs

Generated under `code/output/`:
- `processed_jobs.parquet`
- `kpi_*.csv`
- charts in `code/output/charts/`
  - `top_categories.png`
  - `salary_by_category.png`
  - `salary_distribution_box.png`
  - `avg_salary_per_agency_last_2_years.png`
  - `highest_paid_skills.png`
  - `highest_salary_roles.png`
  - `degree_salary_correlation.png`
