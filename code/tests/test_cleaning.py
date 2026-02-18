from nyc_jobs.cleaning import add_annualized_salary_columns, clean_base_columns


def test_annualized_salary_conversion(spark):
    rows = [
        ("1", "Annual", "100", "200"),
        ("2", "Hourly", "10", "20"),
        ("3", "Daily", "100", "200"),
    ]
    cols = ["Job ID", "Salary Frequency", "Salary Range From", "Salary Range To"]
    df = spark.createDataFrame(rows, cols)
    cleaned = clean_base_columns(df)
    result = add_annualized_salary_columns(cleaned).orderBy("Job ID").collect()

    assert round(result[0]["salary_mid_annualized"], 2) == 150.0
    assert round(result[1]["salary_mid_annualized"], 2) == 31200.0
    assert round(result[2]["salary_mid_annualized"], 2) == 39000.0

