# src/etl_fact_rental_sql.py
import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import ApplySQL
from pyspark.sql.functions import lit

def main():
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    sc = SparkContext.getOrCreate()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    # Configuración (ajusta si tu repositorio/entorno usa otros nombres)
    CONNECTION_NAME = "Jdbc connection"   # nombre de tu Glue JDBC connection
    S3_TARGET_PATH = "s3://cmjm-datalake/facts/fact_rental_sql/"
    TRANSFORMATION_CTX = "apply_sql"

    try:
        print("==> Leyendo tablas desde RDS (creando DynamicFrames)...")

        # Cargar tablas desde RDS a DynamicFrames
        customer = glueContext.create_dynamic_frame.from_options(
            connection_type="jdbc",
            connection_options={
                "useConnectionProperties": "true",
                "connectionName": CONNECTION_NAME,
                "dbtable": "customer"
            },
            transformation_ctx="customer"
        )

        film = glueContext.create_dynamic_frame.from_options(
            connection_type="jdbc",
            connection_options={
                "useConnectionProperties": "true",
                "connectionName": CONNECTION_NAME,
                "dbtable": "film"
            },
            transformation_ctx="film"
        )

        rental = glueContext.create_dynamic_frame.from_options(
            connection_type="jdbc",
            connection_options={
                "useConnectionProperties": "true",
                "connectionName": CONNECTION_NAME,
                "dbtable": "rental"
            },
            transformation_ctx="rental"
        )

        store = glueContext.create_dynamic_frame.from_options(
            connection_type="jdbc",
            connection_options={
                "useConnectionProperties": "true",
                "connectionName": CONNECTION_NAME,
                "dbtable": "store"
            },
            transformation_ctx="store"
        )

        print("==> Aplicando transformación SQL (ApplySQL)...")

        # La consulta SQL que realiza los joins y calcula date_id
        sql_query = """
            SELECT
                r.rental_id,
                r.rental_date,
                CAST(DATE_FORMAT(r.rental_date, 'yyyyMMdd') AS INT) AS date_id,
                r.customer_id,
                i.film_id,
                st.store_id,
                p.amount,
                p.payment_date
            FROM rental r
            JOIN inventory i ON r.inventory_id = i.inventory_id
            JOIN film f ON i.film_id = f.film_id
            JOIN customer c ON r.customer_id = c.customer_id
            LEFT JOIN payment p ON r.rental_id = p.rental_id
            JOIN staff st ON r.staff_id = st.staff_id
            JOIN store s ON st.store_id = s.store_id
        """

        # Mapa de DynamicFrames disponibles para la consulta
        frames_map = {
            "customer": customer,
            "film": film,
            "rental": rental,
            "store": store
            # si necesitas payment o inventory como DynamicFrames separadas podrías cargarlas también,
            # pero aquí usamos las tablas base que definimos arriba y los joins en SQL
        }

        transformed = ApplySQL.apply(
            frame=frames_map,
            query=sql_query,
            transformation_ctx=TRANSFORMATION_CTX
        )

        print("==> Convertir a DataFrame Spark y añadir partición...")

        df_fact = transformed.toDF().withColumn("partition_date", lit("static"))

        write_count = df_fact.count()
        print(f"-> Registros a escribir: {write_count}")

        print(f"==> Escribiendo Parquet en S3: {S3_TARGET_PATH} (mode=append, partitionBy=partition_date)")
        df_fact.write.mode("append").format("parquet").partitionBy("partition_date").save(S3_TARGET_PATH)

        print(f"✅ Escritura finalizada. {write_count} registros escritos en {S3_TARGET_PATH}")

    except Exception as e:
        print(f"❌ Error durante ejecución del job: {e}")
        raise
    finally:
        job.commit()

if __name__ == "__main__":
    main()
