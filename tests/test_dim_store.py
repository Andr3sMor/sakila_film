import pytest
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import lit

@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder \
        .appName("unit-test-dim-store") \
        .master("local[2]") \
        .getOrCreate()
    yield spark
    spark.stop()

def test_dim_store_transformation(spark):
    # Datos de entrada simulados
    input_data = [
        Row(store_id=1, address="123 Main St", last_update="2025-10-18"),
        Row(store_id=2, address="456 Oak Ave", last_update="2025-10-18")
    ]
    df_store = spark.createDataFrame(input_data)
    PROCESSING_DATE = "2025-10-18"

    # Simular transformación
    df_dim_store = df_store.withColumn("partition_date", lit(PROCESSING_DATE))

    # Validaciones
    assert df_dim_store.columns == ["store_id", "address", "last_update", "partition_date"]
    assert df_dim_store.count() == 2
    assert df_dim_store.first().partition_date == PROCESSING_DATE
