import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import col, lit, date_format
from awsglue.job import Job

def main():
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    sc = SparkContext.getOrCreate()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    # Parámetros
    S3_TARGET_PATH = "s3://cmjm-datalake/facts/fact_rental/"
    CONNECTION_NAME = "Jdbc connection"
    GLUE_DATABASE = "sakila_dwh"
    GLUE_TABLE = "fact_rental"

    # Lectura desde RDS usando Job Bookmark
    datasource = glueContext.create_dynamic_frame.from_options(
        connection_type="jdbc",
        connection_options={
            "useConnectionProperties": "true",
            "connectionName": CONNECTION_NAME,
            "dbtable": """
                (SELECT
                    r.rental_id, r.rental_date, r.customer_id, r.staff_id, p.amount, p.payment_date,
                    i.film_id, s.store_id
                FROM rental r
                JOIN payment p ON r.rental_id = p.rental_id
                JOIN inventory i ON r.inventory_id = i.inventory_id
                JOIN staff st ON r.staff_id = st.staff_id
                JOIN store s ON st.store_id = s.store_id) AS rental_data
            """,
            "pushDownPredicate": "$(pushdown_predicate)"
        }
    )
    df_rental = datasource.toDF()

    # Transformaciones
    df_fact_rental = df_rental.withColumn(
        "date_id", date_format(col("rental_date"), "yyyyMMdd").cast("int")
    ).select(
        "amount", "rental_date", "date_id", "customer_id", "film_id", col("store_id").alias("store_id")
    ).withColumn(
        "partition_date", lit("static")
    )

    # Escritura en S3 y actualización del Catálogo para Athena
    print(f"-> Escribiendo datos en S3: {S3_TARGET_PATH}")
    dyf = glueContext.create_dynamic_frame.fromDF(df_fact_rental, glueContext, "dyf_fact_rental")
    sink = glueContext.getSink(
        path=S3_TARGET_PATH,
        connection_type="s3",
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=["partition_date"],
        enableUpdateCatalog=True,
        transformation_ctx="sink_fact_rental"
    )
    sink.setFormat("glueparquet")
    sink.setCatalogInfo(catalogDatabase=GLUE_DATABASE, catalogTableName=GLUE_TABLE)
    sink.writeFrame(dyf)

    job.commit()
    spark.stop()

if __name__ == '__main__':
    main()