import pytest
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import lit

@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder \
        .appName("unit-test-dim-customer") \
        .master("local[2]") \
        .getOrCreate()
    yield spark
    spark.stop()

def test_dim_customer_transformation(spark):
    # Datos de entrada simulados
    input_data = [
        Row(customer_id=1, name="John Doe", last_update="2025-10-18", email="john@example.com"),
        Row(customer_id=2, name="Jane Smith", last_update="2025-10-18", email="jane@example.com")
    ]
    df_customer = spark.createDataFrame(input_data)
    PROCESSING_DATE = "2025-10-18"

    # Simular transformación
    df_dim_customer = df_customer.withColumn("partition_date", lit(PROCESSING_DATE))

    # Validaciones
    assert df_dim_customer.columns == ["customer_id", "name", "last_update", "email", "partition_date"]
    assert df_dim_customer.count() == 2
    assert df_dim_customer.first().partition_date == PROCESSING_DATE
