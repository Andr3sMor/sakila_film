from glue_scripts import dim_store
from pyspark.sql import Row

def test_dim_store_transform(spark):
    stores = [
        Row(store_id=1, manager_staff_id=2, address="123 Main St", city="Miami"),
        Row(store_id=2, manager_staff_id=3, address="456 Oak St", city="Orlando"),
    ]
    df = spark.createDataFrame(stores)

    result = dim_store.transform(df)

    assert "store_key" in result.columns
    assert result.count() == 2
    assert result.filter(result.city == "Miami").count() == 1
