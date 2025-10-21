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
    S3_TARGET_PATH = "s3://cmjm-datalake/dimensions/dim_film/"
    DB_TABLE = "film"

    try:
        print(f"==> Leyendo tabla {DB_TABLE} desde RDS...")

        datasource = glueContext.create_dynamic_frame.from_options(
            connection_type="jdbc",
            connection_options={
                "useConnectionProperties": "true",
                "connectionName": CONNECTION_NAME,
                "dbtable": DB_TABLE
            },
            transformation_ctx="datasource_film"
        )

        count = datasource.count()
        if count == 0:
            print("-> No se encontraron registros nuevos. Job finalizado.")
            job.commit()
            return

        df = datasource.toDF().withColumn("partition_date", lit("static"))

        print(f"-> Escribiendo {df.count()} registros en {S3_TARGET_PATH}")
        df.write.mode("append").format("parquet").partitionBy("partition_date").save(S3_TARGET_PATH)

    except Exception as e:
        print(f"-> Error durante la ejecución del job: {e}")
        raise e

    job.commit()

if __name__ == '__main__':
    main()
