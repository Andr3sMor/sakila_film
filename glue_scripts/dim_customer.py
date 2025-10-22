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
    S3_TARGET_PATH = "s3://cmjm-datalake/dimensions/dim_customer/"
    CONNECTION_NAME = "Jdbc connection"
    DB_TABLE = "customer"
    GLUE_DATABASE = "sakila_dwh"
    GLUE_TABLE = "dim_customer"

    # Lectura desde RDS usando Job Bookmark
    datasource = glueContext.create_dynamic_frame.from_options(
        connection_type="jdbc",
        connection_options={
            "useConnectionProperties": "true",
            "connectionName": CONNECTION_NAME,
            "dbtable": DB_TABLE
        },
        transformation_ctx="datasource_dim_customer"
    )
    df_customer = datasource.toDF()

    # Transformaciones
    df_dim_customer = df_customer.withColumn("partition_date", lit("static"))

    # Conversión a DynamicFrame para escribir con catálogo de Glue
    dyf_dim_customer = glueContext.create_dynamic_frame.fromDF(df_dim_customer, glueContext, "dyf_dim_customer")

    # Escritura en S3 y actualización del Catálogo para Athena
    print(f"-> Escribiendo datos en S3: {S3_TARGET_PATH}")
    sink = glueContext.getSink(
        path=S3_TARGET_PATH,
        connection_type="s3",
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=["partition_date"],
        enableUpdateCatalog=True,
        transformation_ctx="sink_dim_customer"
    )
    sink.setFormat("glueparquet")
    sink.setCatalogInfo(catalogDatabase=GLUE_DATABASE, catalogTableName=GLUE_TABLE)
    sink.writeFrame(dyf_dim_customer)

    job.commit()
    spark.stop()

if __name__ == '__main__':
    main()