"""Data exploration and profiling utilities."""

from typing import List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def column_profile(df: DataFrame) -> DataFrame:
    row_count = df.count()
    profiles = []
    for field in df.schema.fields:
        col_name = field.name
        dtype = field.dataType.simpleString()
        non_null = df.filter(F.col(col_name).isNotNull()).count()
        nulls = row_count - non_null
        distinct_count = df.select(col_name).distinct().count()
        profiles.append((col_name, dtype, row_count, non_null, nulls, distinct_count))

    return df.sql_ctx.sparkSession.createDataFrame(
        profiles,
        ["column_name", "data_type", "row_count", "non_null_count", "null_count", "distinct_count"],
    )


def low_signal_columns(df: DataFrame, uniqueness_threshold: float = 0.98) -> List[str]:
    total = df.count()
    drops: List[str] = []
    for col_name in df.columns:
        distinct_ratio = df.select(col_name).distinct().count() / float(total)
        if distinct_ratio >= uniqueness_threshold:
            drops.append(col_name)
    return drops
