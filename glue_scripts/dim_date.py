import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import (
    col, lit, to_date, date_format,
    year, month, dayofmonth, dayofweek, quarter, date_add
)
from pyspark.sql.types import IntegerType
from awsglue.job import Job

def main():
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    sc = SparkContext.getOrCreate()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    # Parámetros
    S3_TARGET_PATH = "s3://cmjm-datalake/dimensions/dim_date/"
    START_DATE = "2015-01-01"
    END_DATE = "2030-12-31"
    GLUE_DATABASE = "sakila_dwh"
    GLUE_TABLE = "dim_date"

    # Generar rango de fechas
    df_dates = spark.range(
        (date_add(to_date(lit(END_DATE)), 1) - to_date(lit(START_DATE))).cast("int")
    ).withColumn(
        "id_int", col("id").cast(IntegerType())
    ).select(
        date_add(to_date(lit(START_DATE)), col("id_int")).alias("date")
    ).withColumn(
        "date_id", date_format(col("date"), "yyyyMMdd").cast(IntegerType())
    ).withColumn(
        "day", dayofmonth(col("date"))
    ).withColumn(
        "month", month(col("date"))
    ).withColumn(
        "year", year(col("date"))
    ).withColumn(
        "day_of_week", dayofweek(col("date"))
    ).withColumn(
        "day_name", date_format(col("date"), "EEEE")
    ).withColumn(
        "quarter", quarter(col("date"))
    ).withColumn(
        "is_weekend", (col("day_of_week") == 1) | (col("day_of_week") == 7)
    ).withColumn(
        "partition_date", lit("static")
    )

    # Escribir en S3 y registrar la tabla para Athena
    print(f"-> Escribiendo Dim_Date en S3: {S3_TARGET_PATH}")
    dyf = glueContext.create_dynamic_frame.fromDF(df_dates, glueContext, "dyf_dim_date")
    sink = glueContext.getSink(
        path=S3_TARGET_PATH,
        connection_type="s3",
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=[],
        enableUpdateCatalog=True,
        transformation_ctx="sink_dim_date"
    )
    sink.setFormat("glueparquet")
    sink.setCatalogInfo(catalogDatabase=GLUE_DATABASE, catalogTableName=GLUE_TABLE)
    sink.writeFrame(dyf)

    job.commit()
    spark.stop()

if __name__ == '__main__':
    main()