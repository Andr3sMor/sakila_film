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
    GLUE_DATABASE = "sakila_dwh"
    GLUE_TABLE = "dim_film"

    # Lectura desde RDS usando Job Bookmark
    datasource = glueContext.create_dynamic_frame.from_options(
        connection_type="jdbc",
        connection_options={
            "useConnectionProperties": "true",
            "connectionName": CONNECTION_NAME,
            "dbtable": DB_TABLE
        },
        transformation_ctx="datasource_dim_film"
    )
    df_film = datasource.toDF()

    # Transformaciones
    df_dim_film = df_film.withColumn("partition_date", lit("static"))

    # Escribir como parquet y registrar/actualizar tabla en Glue Data Catalog
    dyf = glueContext.create_dynamic_frame.fromDF(df_dim_film, glueContext, "dyf_dim_film")
    print(f"-> Escribiendo datos en S3: {S3_TARGET_PATH}")
    sink = glueContext.getSink(
        path=S3_TARGET_PATH,
        connection_type="s3",
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=["partition_date"],
        enableUpdateCatalog=True,
        transformation_ctx="sink_dim_film"
    )
    sink.setFormat("glueparquet")
    sink.setCatalogInfo(catalogDatabase=GLUE_DATABASE, catalogTableName=GLUE_TABLE)
    sink.writeFrame(dyf)

    job.commit()
    spark.stop()

if __name__ == '__main__':
    main()