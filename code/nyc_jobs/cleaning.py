"""Data cleaning and normalization."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T


def _normalized_string(column_name: str) -> F.Column:
    return F.trim(F.regexp_replace(F.col(column_name), r"\s+", " "))


def clean_base_columns(df: DataFrame) -> DataFrame:
    numeric_columns = ["Salary Range From", "Salary Range To", "# Of Positions", "Level"]
    text_columns = [
        "Agency",
        "Posting Type",
        "Business Title",
        "Civil Service Title",
        "Job Category",
        "Full-Time/Part-Time indicator",
        "Salary Frequency",
        "Work Location",
        "Division/Work Unit",
        "Job Description",
        "Minimum Qual Requirements",
        "Preferred Skills",
        "Additional Information",
    ]

    cleaned = df
    existing_columns = set(df.columns)

    for column_name in text_columns:
        if column_name in existing_columns:
            cleaned = cleaned.withColumn(column_name, _normalized_string(column_name))

    for column_name in numeric_columns:
        if column_name in existing_columns:
            cleaned = cleaned.withColumn(
                column_name,
                F.regexp_replace(F.col(column_name), r"[^0-9.\-]", "").cast(T.DoubleType()),
            )

    cleaned = cleaned.withColumn(
        "Posting Date Parsed",
        F.to_timestamp("Posting Date", "yyyy-MM-dd'T'HH:mm:ss.SSS")
        if "Posting Date" in existing_columns
        else F.lit(None).cast("timestamp"),
    ).withColumn(
        "Posting Updated Parsed",
        F.to_timestamp("Posting Updated", "yyyy-MM-dd'T'HH:mm:ss.SSS")
        if "Posting Updated" in existing_columns
        else F.lit(None).cast("timestamp"),
    ).withColumn(
        "Process Date Parsed",
        F.to_timestamp("Process Date", "yyyy-MM-dd'T'HH:mm:ss.SSS")
        if "Process Date" in existing_columns
        else F.lit(None).cast("timestamp"),
    )

    fill_map = {}
    defaults = {
        "Job Category": "Unspecified",
        "Preferred Skills": "",
        "Minimum Qual Requirements": "",
        "Agency": "Unknown",
        "Salary Frequency": "Unknown",
    }
    for col_name, value in defaults.items():
        if col_name in existing_columns:
            fill_map[col_name] = value

    return cleaned.fillna(fill_map) if fill_map else cleaned


def add_annualized_salary_columns(df: DataFrame) -> DataFrame:
    annual_factor = (
        F.when(F.lower(F.col("Salary Frequency")) == "annual", F.lit(1.0))
        .when(F.lower(F.col("Salary Frequency")) == "daily", F.lit(260.0))
        .when(F.lower(F.col("Salary Frequency")) == "hourly", F.lit(2080.0))
        .otherwise(F.lit(None))
    )

    return (
        df.withColumn("annual_factor", annual_factor)
        .withColumn("salary_from_annualized", F.col("Salary Range From") * F.col("annual_factor"))
        .withColumn("salary_to_annualized", F.col("Salary Range To") * F.col("annual_factor"))
        .withColumn(
            "salary_mid_annualized",
            (F.col("salary_from_annualized") + F.col("salary_to_annualized")) / F.lit(2.0),
        )
    )
