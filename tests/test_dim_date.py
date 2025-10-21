import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, to_date, date_format,
    year, month, dayofmonth, dayofweek, quarter, date_add
)
from pyspark.sql.types import IntegerType

@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder \
        .appName("unit-test-dim-date") \
        .master("local[2]") \
        .getOrCreate()
    yield spark
    spark.stop()

def test_dim_date_generation(spark):
    # Generar datos para 3 días
    df_dates = spark.range(3).withColumn(
        "id_int", col("id").cast(IntegerType())
    ).select(
        date_add(to_date(lit("2025-10-18")), col("id_int")).alias("date")
    ).withColumn(
        "date_id", date_format(col("date"), "yyyyMMdd").cast(IntegerType())
    ).withColumn(
        "day", dayofmonth(col("date"))
    ).withColumn(
        "month", month(col("date"))
    ).withColumn(
        "year", year(col("date"))
    ).withColumn(
        "day_of_week", dayofweek(col("date"))
    ).withColumn(
        "day_name", date_format(col("date"), "EEEE")
    ).withColumn(
        "quarter", quarter(col("date"))
    ).withColumn(
        "is_weekend", (col("day_of_week") == 1) | (col("day_of_week") == 7)
    ).withColumn(
        "partition_date", lit("static")
    )

    # Validaciones
    assert df_dates.count() == 3
    assert df_dates.columns == ["date", "date_id", "day", "month", "year", "day_of_week", "day_name", "quarter", "is_weekend", "partition_date"]
    assert df_dates.first().date_id == 20251018
    assert df_dates.first().day_name in ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]
    assert df_dates.first().is_weekend in [True, False]
