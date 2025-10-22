import pytest
from pyspark.sql import Row
from pyspark.sql.functions import col
from glue_scripts import etl_rental

def test_etl_rental_transformations(spark):
    # Simula tabla de entrada
    data = [
        Row(rental_id=1, customer_id=101, film_id=11, rental_date="2025-01-10", amount=4.99),
        Row(rental_id=2, customer_id=102, film_id=12, rental_date="2025-01-12", amount=2.99),
    ]
    df = spark.createDataFrame(data)

    # Ejecuta la función principal de transformación
    result_df = etl_rental.transform(df)

    # Validaciones de negocio
    assert "rental_id" in result_df.columns
    assert "amount" in result_df.columns
    assert result_df.count() == 2

    # Ejemplo: validar tipo de dato
    assert dict(result_df.dtypes)["amount"] == "double"

    # Ejemplo: validar monto total
    total = result_df.selectExpr("sum(amount) as total").collect()[0]["total"]
    assert round(total, 2) == 7.98
