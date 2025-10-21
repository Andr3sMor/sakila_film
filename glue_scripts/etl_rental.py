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

    try:
        # 1. Lectura desde RDS
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
                "job-bookmark-keys": ["rental_id"],
                "job-bookmark-keys-sort-order": "asc"
            },
            transformation_ctx="datasource_rental"
        )

        # 2. ----> CORRECCIÓN: Verificar si el DynamicFrame está vacío ANTES de convertirlo <----
        if datasource.count() == 0:
            print("-> No se encontraron registros nuevos para procesar. Job finalizado.")
            job.commit()
            return
            
        # 3. Si no está vacío, continuar
        df_rental = datasource.toDF()
            
        # Transformaciones
        df_fact_rental = df_rental.withColumn(
            "date_id", date_format(col("rental_date"), "yyyyMMdd").cast("int")
        ).select(
            "amount", "rental_date", "date_id", "customer_id", "film_id", col("store_id")
        ).withColumn(
            "partition_date", lit("static")
        )

        # Escritura en S3
        print(f"-> Escribiendo {df_fact_rental.count()} registros en S3: {S3_TARGET_PATH}")
        df_fact_rental.write.mode("append").format("parquet").partitionBy("partition_date").save(S3_TARGET_PATH)

    except Exception as e:
        print(f"-> Error durante la ejecución del job: {e}")
        raise e

    job.commit()

if __name__ == '__main__':
    main()