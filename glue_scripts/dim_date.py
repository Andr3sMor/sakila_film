import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import (
    col, lit, to_date, date_format,
    year, month, dayofmonth, dayofweek,
    quarter, date_add, sequence, explode
)
from pyspark.sql.types import IntegerType, StringType, DateType
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
    START_DATE = "2015-01-01"  # Ajusta según tus necesidades
    END_DATE = "2030-12-31"    # Ajusta según tus necesidades

    # Generar rango de fechas
    df_dates = spark.range(
        (date_add(to_date(lit(END_DATE)), 1) - to_date(lit(START_DATE))).cast("int")
    ).select(
        date_add(to_date(lit(START_DATE)), col("id")).alias("date_id_raw")
    ).withColumn(
        "date_id", date_format(col("date_id_raw"), "yyyyMMdd").cast(IntegerType())
    ).withColumn(
        "date", col("date_id_raw").cast(DateType())
    ).withColumn(
        "day", dayofmonth(col("date"))
    ).withColumn(
        "month", month(col("date"))
    ).withColumn(
        "month_name", date_format(col("date"), "MMMM")
    ).withColumn(
        "year", year(col("date"))
    ).withColumn(
        "day_of_week", dayofweek(col("date"))
    ).withColumn(
        "day_name", date_format(col("date"), "EEEE")
    ).withColumn(
        "quarter", quarter(col("date"))
    ).withColumn(
        "is_weekend", (col("day_of_week") == 1).cast(IntegerType()) | (col("day_of_week") == 7).cast(IntegerType())
    ).withColumn(
        "is_holiday", lit(0)  # Puedes personalizar esto para festivos específicos
    ).drop("date_id_raw")

    # Añadir partición (opcional, ya que es una tabla estática)
    df_dim_date = df_dates.withColumn("partition_date", lit("static"))

    # Escribir en S3
    print(f"-> Escribiendo Dim_Date en S3: {S3_TARGET_PATH}")
    df_dim_date.write.mode("overwrite").format("parquet").save(S3_TARGET_PATH)

    job.commit()
    spark.stop()

if __name__ == '__main__':
    main()
