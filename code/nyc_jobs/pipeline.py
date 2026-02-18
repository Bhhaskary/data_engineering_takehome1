"""End-to-end assessment pipeline."""

from pathlib import Path
from typing import Dict

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from nyc_jobs.cleaning import add_annualized_salary_columns, clean_base_columns
from nyc_jobs.config import CHARTS_DIR, DATASET_PATH, OUTPUT_DIR
from nyc_jobs.exploration import column_profile, low_signal_columns
from nyc_jobs.features import add_degree_features, add_experience_feature, add_skill_tokens, add_temporal_features
from nyc_jobs.io import read_jobs_csv, write_single_csv
from nyc_jobs.kpis import (
    average_salary_per_agency_last_2_years,
    degree_salary_correlation,
    highest_paid_skills,
    highest_salary_job_per_agency,
    jobs_posting_top_categories,
    salary_distribution_per_category,
)
from nyc_jobs.spark import create_spark_session
from nyc_jobs.visualization import (
    plot_avg_salary_per_agency,
    plot_degree_salary_correlation,
    plot_highest_paid_skills,
    plot_highest_salary_roles,
    plot_salary_by_category,
    plot_salary_distribution_box,
    plot_top_categories,
)


def prepare_dataset(raw_df: DataFrame) -> DataFrame:
    df = clean_base_columns(raw_df)
    df = add_annualized_salary_columns(df)
    df = add_temporal_features(df)
    df = add_degree_features(df)
    df = add_experience_feature(df)
    df = add_skill_tokens(df)
    df = df.filter(df["salary_mid_annualized"].isNotNull())
    return df


def build_kpis(df: DataFrame) -> Dict[str, DataFrame]:
    return {
        "kpi_top_categories": jobs_posting_top_categories(df),
        "kpi_salary_distribution_category": salary_distribution_per_category(df),
        "kpi_degree_salary_correlation": degree_salary_correlation(df),
        "kpi_highest_salary_per_agency": highest_salary_job_per_agency(df),
        "kpi_avg_salary_per_agency_last_2_years": average_salary_per_agency_last_2_years(df),
        "kpi_highest_paid_skills": highest_paid_skills(df),
    }


def write_kpi_outputs(kpis: Dict[str, DataFrame], output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    for name, kpi_df in kpis.items():
        write_single_csv(kpi_df, output_dir / f"{name}.csv")


def run(use_cluster_master: bool = False) -> None:
    spark = create_spark_session(use_cluster_master=use_cluster_master)
    raw_df = read_jobs_csv(spark, str(DATASET_PATH))

    profile_df = column_profile(raw_df)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    write_single_csv(profile_df, OUTPUT_DIR / "column_profile.csv")

    processed_df = prepare_dataset(raw_df)

    removable_candidates = [
        c for c in ["To Apply", "Recruitment Contact", "Work Location 1", "Post Until"] if c in processed_df.columns
    ]
    removable_cols = low_signal_columns(processed_df.select(*removable_candidates)) if removable_candidates else []
    with (OUTPUT_DIR / "removed_columns.txt").open("w", encoding="utf-8") as f:
        for col_name in removable_cols:
            f.write(col_name + "\n")

    final_df = processed_df.drop(*removable_cols)

    # Spark 2.4 parquet writer rejects spaces/special chars in column names.
    parquet_df = final_df.select(
        [F.col(c).alias(c.strip().lower().replace(" ", "_").replace("/", "_")) for c in final_df.columns]
    )
    parquet_df.write.mode("overwrite").parquet(str(OUTPUT_DIR / "processed_jobs.parquet"))

    kpis = build_kpis(final_df)
    write_kpi_outputs(kpis, OUTPUT_DIR)

    plot_top_categories(kpis["kpi_top_categories"], CHARTS_DIR / "top_categories.png")
    plot_salary_by_category(kpis["kpi_salary_distribution_category"], CHARTS_DIR / "salary_by_category.png")
    plot_salary_distribution_box(kpis["kpi_salary_distribution_category"], CHARTS_DIR / "salary_distribution_box.png")
    plot_avg_salary_per_agency(
        kpis["kpi_avg_salary_per_agency_last_2_years"], CHARTS_DIR / "avg_salary_per_agency_last_2_years.png"
    )
    plot_highest_paid_skills(kpis["kpi_highest_paid_skills"], CHARTS_DIR / "highest_paid_skills.png")
    plot_highest_salary_roles(kpis["kpi_highest_salary_per_agency"], CHARTS_DIR / "highest_salary_roles.png")
    plot_degree_salary_correlation(kpis["kpi_degree_salary_correlation"], CHARTS_DIR / "degree_salary_correlation.png")

    spark.stop()


if __name__ == "__main__":
    run(use_cluster_master=False)
