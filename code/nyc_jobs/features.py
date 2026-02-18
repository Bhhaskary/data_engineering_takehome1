"""Feature engineering transformations."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def add_temporal_features(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("posting_year", F.year("Posting Date Parsed"))
        .withColumn("posting_month", F.month("Posting Date Parsed"))
        .withColumn("posting_quarter", F.quarter("Posting Date Parsed"))
    )


def add_degree_features(df: DataFrame) -> DataFrame:
    text = F.lower(
        F.concat_ws(
            " ",
            F.col("Minimum Qual Requirements"),
            F.col("Preferred Skills"),
            F.col("Job Description"),
        )
    )

    degree_level = (
        F.when(text.rlike(r"\b(phd|ph\.d|doctorate)\b"), F.lit("doctorate"))
        .when(text.rlike(r"\b(master'?s|mba|m\.s\.|ms )\b"), F.lit("masters"))
        .when(text.rlike(r"\b(bachelor|baccalaureate|ba |bs )\b"), F.lit("bachelors"))
        .when(text.rlike(r"\b(associate|high school|ged)\b"), F.lit("associate_or_hs"))
        .otherwise(F.lit("not_specified"))
    )

    degree_rank = (
        F.when(degree_level == "doctorate", F.lit(4))
        .when(degree_level == "masters", F.lit(3))
        .when(degree_level == "bachelors", F.lit(2))
        .when(degree_level == "associate_or_hs", F.lit(1))
        .otherwise(F.lit(0))
    )

    return df.withColumn("highest_degree_required", degree_level).withColumn("degree_rank", degree_rank)


def add_experience_feature(df: DataFrame) -> DataFrame:
    exp_text = F.lower(F.col("Minimum Qual Requirements"))
    years = F.regexp_extract(exp_text, r"(\d+)\s+year", 1).cast("int")
    return df.withColumn("years_experience_required", years)


def add_skill_tokens(df: DataFrame) -> DataFrame:
    combined_text = F.lower(
        F.concat_ws(" ", F.col("Preferred Skills"), F.col("Job Description"), F.col("Minimum Qual Requirements"))
    )
    return df.withColumn("combined_text", combined_text)

