from glue_scripts import dim_date
from pyspark.sql import Row

def test_dim_date_build(spark):
    df = dim_date.build_date_dim(spark, "2025-01-01", "2025-01-05")

    # Verifica estructura
    expected_cols = {"date_key", "date", "year", "month", "day", "weekday"}
    assert expected_cols.issubset(set(df.columns))

    # Verifica cantidad de filas
    assert df.count() == 5

    # Valida primer registro
    first = df.orderBy("date_key").first()
    assert first.year == 2025
    assert first.month == 1
