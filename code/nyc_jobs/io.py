"""I/O helpers."""

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession


def read_jobs_csv(spark: SparkSession, path: str) -> DataFrame:
    return (
        spark.read.option("header", True)
        .option("inferSchema", False)
        .csv(path)
    )


def write_single_csv(df: DataFrame, output_path: Path) -> None:
    temp_path = output_path.parent / f".{output_path.name}_tmp"
    (
        df.coalesce(1)
        .write.mode("overwrite")
        .option("header", True)
        .csv(str(temp_path))
    )
    part_files = list(temp_path.glob("part-*.csv"))
    if not part_files:
        raise RuntimeError(f"No CSV part file found in {temp_path}")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    part_files[0].replace(output_path)
    for file in temp_path.glob("*"):
        if file.exists():
            file.unlink()
    temp_path.rmdir()

