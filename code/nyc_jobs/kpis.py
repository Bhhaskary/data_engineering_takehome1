"""KPI builders for the assessment."""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def jobs_posting_top_categories(df: DataFrame, top_n: int = 10) -> DataFrame:
    return (
        df.groupBy("Job Category")
        .agg(F.countDistinct("Job ID").alias("job_postings"))
        .orderBy(F.desc("job_postings"))
        .limit(top_n)
    )


def salary_distribution_per_category(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("Job Category")
        .agg(
            F.countDistinct("Job ID").alias("jobs"),
            F.min("salary_mid_annualized").alias("min_salary"),
            F.expr("percentile_approx(salary_mid_annualized, 0.25)").alias("p25_salary"),
            F.expr("percentile_approx(salary_mid_annualized, 0.5)").alias("median_salary"),
            F.expr("percentile_approx(salary_mid_annualized, 0.75)").alias("p75_salary"),
            F.max("salary_mid_annualized").alias("max_salary"),
            F.avg("salary_mid_annualized").alias("avg_salary"),
        )
        .orderBy(F.desc("avg_salary"))
    )


def degree_salary_correlation(df: DataFrame) -> DataFrame:
    correlation_value = df.stat.corr("degree_rank", "salary_mid_annualized")
    return df.sql_ctx.sparkSession.createDataFrame(
        [(correlation_value,)],
        ["degree_salary_corr"],
    )


def highest_salary_job_per_agency(df: DataFrame) -> DataFrame:
    w = Window.partitionBy("Agency").orderBy(F.desc("salary_to_annualized"), F.desc("salary_mid_annualized"))
    return (
        df.withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .select(
            "Agency",
            "Job ID",
            "Business Title",
            "Job Category",
            "salary_from_annualized",
            "salary_to_annualized",
            "salary_mid_annualized",
            "Posting Date Parsed",
        )
    )


def average_salary_per_agency_last_2_years(df: DataFrame) -> DataFrame:
    max_date = df.agg(F.max("Posting Date Parsed").alias("max_date")).collect()[0]["max_date"]
    if max_date is None:
        return df.sql_ctx.sparkSession.createDataFrame([], "Agency string, jobs bigint, avg_salary double")

    cutoff = F.add_months(F.lit(max_date), -24)
    filtered = df.filter(F.col("Posting Date Parsed") >= cutoff)
    return (
        filtered.groupBy("Agency")
        .agg(
            F.countDistinct("Job ID").alias("jobs"),
            F.avg("salary_mid_annualized").alias("avg_salary"),
        )
        .orderBy(F.desc("avg_salary"))
    )


def highest_paid_skills(df: DataFrame, top_n: int = 25) -> DataFrame:
    tracked_skills = [
        "python",
        "sql",
        "spark",
        "aws",
        "azure",
        "gcp",
        "java",
        "scala",
        "tableau",
        "power bi",
        "etl",
        "machine learning",
        "statistics",
        "data modeling",
        "snowflake",
    ]

    exploded = None
    for skill in tracked_skills:
        subset = (
            df.filter(F.col("combined_text").contains(skill))
            .select("Job ID", "salary_mid_annualized")
            .withColumn("skill", F.lit(skill))
        )
        exploded = subset if exploded is None else exploded.unionByName(subset)

    if exploded is None:
        return df.sql_ctx.sparkSession.createDataFrame([], "skill string, jobs_requiring_skill bigint, avg_salary double, max_salary double")

    return (
        exploded.groupBy("skill")
        .agg(
            F.countDistinct("Job ID").alias("jobs_requiring_skill"),
            F.avg("salary_mid_annualized").alias("avg_salary"),
            F.max("salary_mid_annualized").alias("max_salary"),
        )
        .orderBy(F.desc("avg_salary"))
        .limit(top_n)
    )
