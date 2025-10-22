import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import col, lit
from awsglue.job import Job

def main():
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    sc = SparkContext.getOrCreate()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    # Parámetros
    S3_TARGET_PATH = "s3://cmjm-datalake/dimensions/dim_film/"
    CONNECTION_NAME = "Jdbc connection"
    DB_TABLE = "film"

    # Lectura desde RDS usando Job Bookmark
    datasource = glueContext.create_dynamic_frame.from_options(
        connection_type="jdbc",
        connection_options={
            "useConnectionProperties": "true",
            "connectionName": CONNECTION_NAME,
            "dbtable": DB_TABLE,
            "pushDownPredicate": "$(pushdown_predicate)"
        }
    )
    df_film = datasource.toDF()

    # Transformaciones
    df_dim_film = df_film.withColumn("partition_date", lit("static"))

    # Escritura en S3
    print(f"-> Escribiendo datos en S3: {S3_TARGET_PATH}")
    df_dim_film.write.mode("append").format("parquet").partitionBy("partition_date").save(S3_TARGET_PATH)

    job.commit()
    spark.stop()

if __name__ == '__main__':
    main()