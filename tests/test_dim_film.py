from glue_scripts import dim_film
from pyspark.sql import Row

def test_dim_film_transform(spark):
    films = [
        Row(film_id=10, title="Matrix", category="Sci-Fi", length=136, rating="R"),
        Row(film_id=11, title="Shrek", category="Animation", length=90, rating="PG"),
    ]
    df = spark.createDataFrame(films)

    result = dim_film.transform(df)
    assert "film_key" in result.columns
    assert result.count() == 2
    assert result.filter(result.category == "Sci-Fi").count() == 1
