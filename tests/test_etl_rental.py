import pytest
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, date_format, lit

@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder \
        .appName("unit-test-fact-rental") \
        .master("local[2]") \
        .getOrCreate()
    yield spark
    spark.stop()

def test_fact_rental_transformation(spark):
    # 1. Datos de entrada simulados (ahora incluyen film_id y store_id)
    input_data = [
        Row(
            rental_id=1,
            rental_date="2025-10-18",
            customer_id=5,
            staff_id=2,
            amount=10.5,
            payment_date="2025-10-18",
            film_id=10,  # Valor real desde inventory
            store_id=1   # Valor real desde store
        ),
        Row(
            rental_id=2,
            rental_date="2025-10-18",
            customer_id=7,
            staff_id=3,
            amount=7.5,
            payment_date="2025-10-18",
            film_id=20,  # Valor real desde inventory
            store_id=2   # Valor real desde store
        )
    ]
    df_rental = spark.createDataFrame(input_data)
    PROCESSING_DATE = "2025-10-18"

    # 2. Simular la transformación (igual que en el script principal)
    df_fact_rental = (
        df_rental.withColumn("date_id", date_format(col("rental_date"), "yyyyMMdd").cast("int"))
        .select(
            "amount",
            "rental_date",
            "date_id",
            "customer_id",
            "film_id",  # Ahora se usa el valor real
            col("store_id").alias("store_id")  # Ahora se usa el valor real
        )
        .withColumn("partition_date", lit(PROCESSING_DATE))
    )

    # 3. Validaciones
    expected_columns = ["amount", "rental_date", "date_id", "customer_id", "film_id", "store_id", "partition_date"]
    assert df_fact_rental.columns == expected_columns, f"Columnas esperadas: {expected_columns}. Obtenidas: {df_fact_rental.columns}"

    # Validar valores específicos
    first_row = df_fact_rental.first()
    assert first_row.date_id == 20251018, f"date_id incorrecto: {first_row.date_id}"
    assert first_row.film_id == 10, f"film_id incorrecto: {first_row.film_id}"
    assert first_row.store_id == 1, f"store_id incorrecto: {first_row.store_id}"
    assert first_row.partition_date == PROCESSING_DATE, f"partition_date incorrecto: {first_row.partition_date}"

    # 4. Validar cantidad de registros
    assert df_fact_rental.count() == 2, f"Cantidad de registros incorrecta: {df_fact_rental.count()}"
