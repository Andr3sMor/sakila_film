import pytest
from pyspark.sql import SparkSession
from unittest.mock import MagicMock, patch
from pyspark.sql import Row
from pyspark.sql.functions import lit

# Configurar Spark para pruebas locales
@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder \
        .appName("unit-test-fact-rental") \
        .master("local[2]") \
        .getOrCreate()
    yield spark
    spark.stop()


def test_fact_rental_transformation(spark):
    # --- 1. Datos simulados de entrada ---
    input_data = [
        Row(rental_id=1, rental_date="2025-10-18", inventory_id=101, customer_id=5,
            staff_id=2, last_update="2025-10-18", amount=10.5, payment_date="2025-10-18"),
        Row(rental_id=2, rental_date="2025-10-18", inventory_id=102, customer_id=7,
            staff_id=3, last_update="2025-10-18", amount=7.5, payment_date="2025-10-18")
    ]
    df_rental = spark.createDataFrame(input_data)

    PROCESSING_DATE = "2025-10-18"

    # --- 2. Simular la transformación ---
    from pyspark.sql.functions import col, date_format

    df_fact_rental = (
        df_rental.withColumn("date_id", date_format(col("rental_date"), "yyyyMMdd").cast("int"))
        .withColumn("film_id", lit(1))
        .select(
            "amount",
            "rental_date",
            "date_id",
            "customer_id",
            col("film_id").alias("film_id"),
            col("staff_id").alias("store_id")
        )
        .withColumn("partition_date", lit(PROCESSING_DATE))
    )

    # --- 3. Validaciones ---
    expected_columns = ["amount", "rental_date", "date_id", "customer_id", "film_id", "store_id", "partition_date"]
    assert df_fact_rental.columns == expected_columns, "Las columnas no coinciden"

    first_row = df_fact_rental.first()
    assert first_row.date_id == 20251018, "El campo date_id no se calculó correctamente"
    assert first_row.film_id == 1, "El campo film_id debería ser constante = 1"
    assert first_row.partition_date == PROCESSING_DATE, "El campo partition_date no coincide con el valor esperado"

    # --- 4. Verificar cantidad de registros de simulación ---
    assert df_fact_rental.count() == 2, "No se conservaron todos los registros"

