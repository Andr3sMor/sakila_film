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
    S3_TARGET_PATH = "s3://cmjm-datalake/dimensions/dim_store/"
    CONNECTION_NAME = "Jdbc connection"
    DB_TABLE = "store"

    try:
        # 1. Lectura desde RDS
        datasource = glueContext.create_dynamic_frame.from_options(
            connection_type="jdbc",
            connection_options={
                "useConnectionProperties": "true",
                "connectionName": CONNECTION_NAME,
                "dbtable": DB_TABLE
            },
            transformation_ctx="datasource_store"
        )
        
        # 2. ----> CORRECCIÓN: Verificar si el DynamicFrame está vacío ANTES de convertirlo <----
        if datasource.count() == 0:
            print("-> No se encontraron registros nuevos para procesar. Job finalizado.")
            job.commit()
            return
            
        # 3. Si no está vacío, continuar
        df_store = datasource.toDF()
            
        # Transformaciones
        df_dim_store = df_store.withColumn("partition_date", lit("static"))

        # Escritura en S3
        print(f"-> Escribiendo {df_dim_store.count()} registros en S3: {S3_TARGET_PATH}")
        df_dim_store.write.mode("append").format("parquet").partitionBy("partition_date").save(S3_TARGET_PATH)

    except Exception as e:
        print(f"-> Error durante la ejecución del job: {e}")
        raise e

    job.commit()

if __name__ == '__main__':
    main()