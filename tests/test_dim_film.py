import pytest
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import lit

@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder \
        .appName("unit-test-dim-film") \
        .master("local[2]") \
        .getOrCreate()
    yield spark
    spark.stop()

def test_dim_film_transformation(spark):
    # Datos de entrada simulados
    input_data = [
        Row(film_id=1, title="Inception", release_year=2010, last_update="2025-10-18"),
        Row(film_id=2, title="The Matrix", release_year=1999, last_update="2025-10-18")
    ]
    df_film = spark.createDataFrame(input_data)
    PROCESSING_DATE = "2025-10-18"

    # Simular transformación
    df_dim_film = df_film.withColumn("partition_date", lit(PROCESSING_DATE))

    # Validaciones
    assert df_dim_film.columns == ["film_id", "title", "release_year", "last_update", "partition_date"]
    assert df_dim_film.count() == 2
    assert df_dim_film.first().partition_date == PROCESSING_DATE
