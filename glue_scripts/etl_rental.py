import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import col, lit, date_format
from awsglue.job import Job
from awsglue import DynamicFrame # Se mantiene para compatibilidad con el entorno Glue

# Inicialización de objetos globales (serán mockeados en el test)
sc = None
glueContext = None
spark = None

def main():
    global sc, glueContext, spark
    
    # 1. Recepción de Argumentos
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'PROCESSING_DATE']) 
    sc = SparkContext.getOrCreate()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    # --- PARÁMETROS ---
    PROCESSING_DATE = args['--PROCESSING_DATE']
    S3_TARGET_PATH = "s3://cmjm-datalake/facts/fact_rental/"
    CONNECTION_NAME = "sakila-rds-connection" 
    DB_TABLE = "rental"

    # 2. Definir el Predicado de Filtrado (Lectura Incremental)
    # Filtra la base de datos RDS para cargar solo los datos de la fecha específica
    predicate = f"DATE(r.rental_date) = '{PROCESSING_DATE}'"

    print(f"-> Leyendo la tabla {DB_TABLE} con filtro: {predicate}")

    # 3. Lectura desde RDS con 'Pushdown Predicate'
    datasource = glueContext.create_dynamic_frame.from_options(
        connection_type="jdbc",
        connection_options={
            "useConnectionProperties": "true", 
            "connectionName": CONNECTION_NAME, 
            "dbtable": DB_TABLE,
            "query": f"""
                SELECT 
                    r.rental_id, r.rental_date, r.inventory_id, r.customer_id, r.staff_id, r.last_update, 
                    p.amount, p.payment_date
                FROM rental r
                JOIN payment p ON r.rental_id = p.rental_id
                WHERE {predicate}
            """
        },
        transformation_ctx="JDBC_Source"
    )

    df_rental = datasource.toDF()

    # 4. Transformaciones para el Modelo Dimensional
    df_fact_rental = df_rental.withColumn(
        "date_id", 
        date_format(col("rental_date"), "yyyyMMdd").cast("int")
    ).withColumn(
        "film_id", 
        lit(1).cast("int") # Placeholder para FK Film
    ).select(
        "amount",
        "rental_date",
        "date_id",
        "customer_id",
        col("film_id").alias("film_id"),
        col("staff_id").alias("store_id")
    ).withColumn(
        "partition_date", 
        lit(PROCESSING_DATE)
    )

    # 5. Escritura en S3 de archivos en formato Parquet
    print(f"-> Escribiendo datos en S3 y particionando por {PROCESSING_DATE}")
    
    df_fact_rental.write.mode("append").format("parquet").partitionBy("partition_date").save(S3_TARGET_PATH)
    
    job.commit()
    spark.stop()

# Punto de entrada para la ejecución de AWS Glue
if __name__ == '__main__':
    main()