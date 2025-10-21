import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import col, lit, date_format
from awsglue.job import Job

def main():
    # 1. Inicialización
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'PROCESSING_DATE'])
    sc = SparkContext.getOrCreate()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    # 2. Parámetros
    PROCESSING_DATE = args['PROCESSING_DATE']
    S3_TARGET_PATH = "s3://cmjm-datalake/facts/fact_rental/"
    CONNECTION_NAME = "sakila-rds-connection"

    # 3. Predicado para lectura incremental
    predicate = f"DATE(r.rental_date) = '{PROCESSING_DATE}'"
    print(f"-> Leyendo datos de RDS con filtro: {predicate}")

    # 4. Consulta SQL mejorada para obtener film_id y store_id
    datasource = glueContext.create_dynamic_frame.from_options(
        connection_type="jdbc",
        connection_options={
            "useConnectionProperties": "true",
            "connectionName": CONNECTION_NAME,
            "dbtable": "rental",
            "query": f"""
                SELECT
                    r.rental_id,
                    r.rental_date,
                    r.customer_id,
                    r.staff_id,
                    p.amount,
                    p.payment_date,
                    i.film_id,
                    s.store_id
                FROM rental r
                JOIN payment p ON r.rental_id = p.rental_id
                JOIN inventory i ON r.inventory_id = i.inventory_id
                JOIN staff st ON r.staff_id = st.staff_id
                JOIN store s ON st.store_id = s.store_id
                WHERE {predicate}
            """
        },
        transformation_ctx="JDBC_Source"
    )
    df_rental = datasource.toDF()

    # 5. Transformaciones para el modelo dimensional
    df_fact_rental = df_rental.withColumn(
        "date_id",
        date_format(col("rental_date"), "yyyyMMdd").cast("int")
    ).select(
        "amount",
        "rental_date",
        "date_id",
        "customer_id",
        "film_id",  # Ahora obtenido correctamente desde inventory
        col("store_id").alias("store_id")  # Ahora obtenido correctamente desde store
    ).withColumn(
        "partition_date",
        lit(PROCESSING_DATE)
    )

    # 6. Escritura en S3 (formato Parquet, particionado por fecha)
    print(f"-> Escribiendo datos en S3: {S3_TARGET_PATH}")
    df_fact_rental.write.mode("append").format("parquet").partitionBy("partition_date").save(S3_TARGET_PATH)

    job.commit()
    spark.stop()

if __name__ == '__main__':
    main()
