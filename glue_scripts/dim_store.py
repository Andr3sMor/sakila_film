import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import lit

def main():
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    sc = SparkContext.getOrCreate()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    CONNECTION_NAME = "Jdbc connection"
    S3_TARGET_PATH = "s3://cmjm-datalake/dimensions/dim_store/"
    DB_TABLE = "store"
    GLUE_DATABASE = "sakila_dwh"
    GLUE_TABLE = "dim_store"

    try:
        print(f"==> Leyendo tabla {DB_TABLE} desde RDS...")

        datasource = glueContext.create_dynamic_frame.from_options(
            connection_type="jdbc",
            connection_options={
                "useConnectionProperties": "true",
                "connectionName": CONNECTION_NAME,
                "dbtable": DB_TABLE
            },
            transformation_ctx="datasource_dim_store"
        )

        count = datasource.count()
        if count == 0:
            print("-> No se encontraron registros nuevos. Job finalizado.")
            job.commit()
            return

        df = datasource.toDF().withColumn("partition_date", lit("static"))

        print(f"-> Escribiendo {df.count()} registros en {S3_TARGET_PATH}")
        dyf = glueContext.create_dynamic_frame.fromDF(df, glueContext, "dyf_dim_store")
        sink = glueContext.getSink(
            path=S3_TARGET_PATH,
            connection_type="s3",
            updateBehavior="UPDATE_IN_DATABASE",
            partitionKeys=["partition_date"],
            enableUpdateCatalog=True,
            transformation_ctx="sink_dim_store"
        )
        sink.setFormat("glueparquet")
        sink.setCatalogInfo(catalogDatabase=GLUE_DATABASE, catalogTableName=GLUE_TABLE)
        sink.writeFrame(dyf)

    except Exception as e:
        print(f"-> Error durante la ejecución del job: {e}")
        raise e

    job.commit()

if __name__ == '__main__':
    main()
