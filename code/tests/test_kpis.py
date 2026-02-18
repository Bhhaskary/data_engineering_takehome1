from pyspark.sql import functions as F

from nyc_jobs.kpis import highest_salary_job_per_agency, jobs_posting_top_categories


def test_jobs_posting_top_categories(spark):
    rows = [
        ("1", "A", "IT", 100000.0, 120000.0, 110000.0),
        ("2", "A", "IT", 90000.0, 100000.0, 95000.0),
        ("3", "B", "Finance", 80000.0, 90000.0, 85000.0),
    ]
    cols = [
        "Job ID",
        "Agency",
        "Job Category",
        "salary_from_annualized",
        "salary_to_annualized",
        "salary_mid_annualized",
    ]
    df = spark.createDataFrame(rows, cols)
    result = jobs_posting_top_categories(df, top_n=2).collect()
    assert result[0]["Job Category"] == "IT"
    assert result[0]["job_postings"] == 2


def test_highest_salary_per_agency(spark):
    rows = [
        ("1", "AgencyA", "Role1", "IT", 100000.0, 120000.0, 110000.0),
        ("2", "AgencyA", "Role2", "IT", 130000.0, 150000.0, 140000.0),
        ("3", "AgencyB", "Role3", "Finance", 90000.0, 100000.0, 95000.0),
    ]
    cols = [
        "Job ID",
        "Agency",
        "Business Title",
        "Job Category",
        "salary_from_annualized",
        "salary_to_annualized",
        "salary_mid_annualized",
    ]
    df = spark.createDataFrame(rows, cols)
    df = df.withColumn("Posting Date Parsed", F.to_timestamp(F.lit("2024-01-01 00:00:00")))
    result = highest_salary_job_per_agency(df).orderBy("Agency").collect()
    assert len(result) == 2
    assert result[0]["Agency"] == "AgencyA"
    assert result[0]["Job ID"] == "2"
