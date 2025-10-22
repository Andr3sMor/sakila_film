from glue_scripts import dim_customer
from pyspark.sql import Row

def test_dim_customer_transform(spark):
    customers = [
        Row(customer_id=1, first_name="John", last_name="Doe", email="john@example.com", active=True),
        Row(customer_id=2, first_name="Jane", last_name="Smith", email="jane@example.com", active=False),
    ]
    df = spark.createDataFrame(customers)

    result = dim_customer.transform(df)

    assert "customer_key" in result.columns
    assert result.count() == 2
    assert result.filter(result.active == True).count() == 1
