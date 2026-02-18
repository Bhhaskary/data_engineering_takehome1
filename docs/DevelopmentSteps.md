# Development Steps

This document explains the implementation process step by step.

## 1. Requirement analysis
- Reviewed challenge expectations in `README.md` and `INSTALL.md`.
- Confirmed mandatory deliverables: KPIs, processing, feature engineering, tests, visuals, and documentation.

## 2. Repository assessment
- Identified starter assets: dataset, docker-compose setup, starter notebook.
- Found missing production-style structure for reusable code and tests.

## 3. Architecture design
- Built modular PySpark package under `code/nyc_jobs/`.
- Split concerns into ingestion, cleaning, features, exploration, KPIs, visualization, orchestration.

## 4. Data processing implementation
- Added robust cleaning and type handling.
- Parsed date fields and normalized text.
- Implemented salary annualization for cross-frequency comparison.

## 5. Feature engineering
- Added degree extraction and degree ranking.
- Added experience extraction.
- Added temporal features (`year/month/quarter`).
- Added combined text feature for skill-based analysis.

## 6. KPI implementation
- Top categories by posting count.
- Salary distribution by category.
- Degree-salary correlation.
- Highest salary posting per agency.
- Average agency salary for last 2 years.
- Highest paid skills.

## 7. Profiling and feature removal
- Generated column profile output.
- Applied low-signal feature removal and logged removed columns.

## 8. Visualization
- Generated KPI charts with `matplotlib` and a visual summary report.

## 9. Testing
- Added unit tests for cleaning and KPI logic.
- Added Spark test fixture setup.

## 10. Runtime hardening
- Resolved Spark 2.4 compatibility issues.
- Fixed output path handling for container runtime.
- Sanitized parquet column names for compatibility.

## 11. Final outputs
- Processed parquet dataset.
- KPI CSV outputs.
- Chart artifacts.
- Documentation (`MyDocument.md`, this file, code README).

