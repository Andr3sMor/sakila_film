import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import col, lit, date_format
from awsglue.job import Job

def main():
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'PROCESSING_DATE'])
    sc = SparkContext.getOrCreate()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    # Parámetros
    PROCESSING_DATE = args['PROCESSING_DATE']
    S3_TARGET_PATH = "s3://cmjm-datalake/dimensions/dim_film/"
    CONNECTION_NAME = "sakila-rds-connection"
    DB_TABLE = "film"

    # Predicado para lectura incremental (asumiendo que hay un campo `last_update`)
    predicate = f"DATE(last_update) = '{PROCESSING_DATE}'"
    print(f"-> Leyendo tabla {DB_TABLE} con filtro: {predicate}")

    # Lectura desde RDS
    datasource = glueContext.create_dynamic_frame.from_options(
        connection_type="jdbc",
        connection_options={
            "useConnectionProperties": "true",
            "connectionName": CONNECTION_NAME,
            "dbtable": DB_TABLE,
            "query": f"SELECT * FROM {DB_TABLE} WHERE {predicate}"
        }
    )
    df_film = datasource.toDF()

    # Transformaciones (ej: añadir partición por fecha)
    df_dim_film = df_film.withColumn("partition_date", lit(PROCESSING_DATE))

    # Escritura en S3
    print(f"-> Escribiendo datos en S3: {S3_TARGET_PATH}")
    df_dim_film.write.mode("append").format("parquet").partitionBy("partition_date").save(S3_TARGET_PATH)

    job.commit()
    spark.stop()

if __name__ == '__main__':
    main()
