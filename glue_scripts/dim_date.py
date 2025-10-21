import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import (
    col, lit, to_date, date_format,
    year, month, dayofmonth, dayofweek, quarter, date_add
)
from pyspark.sql.types import IntegerType

def main():
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    sc = SparkContext.getOrCreate()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    S3_TARGET_PATH = "s3://cmjm-datalake/dimensions/dim_date/"
    START_DATE = "2015-01-01"
    END_DATE = "2030-12-31"

    print("==> Generando dimensión de fechas...")

    days_diff = spark.sql(f"SELECT DATEDIFF('{END_DATE}', '{START_DATE}') AS days").collect()[0]["days"]

    df = spark.range(days_diff + 1).select(
        date_add(to_date(lit(START_DATE)), col("id")).alias("date")
    ).withColumn("date_id", date_format(col("date"), "yyyyMMdd").cast(IntegerType())) \
     .withColumn("day", dayofmonth(col("date"))) \
     .withColumn("month", month(col("date"))) \
     .withColumn("year", year(col("date"))) \
     .withColumn("day_of_week", dayofweek(col("date"))) \
     .withColumn("day_name", date_format(col("date"), "EEEE")) \
     .withColumn("quarter", quarter(col("date"))) \
     .withColumn("is_weekend", (col("day_of_week") == 1) | (col("day_of_week") == 7)) \
     .withColumn("partition_date", lit("static"))

    print(f"-> Escribiendo {df.count()} filas en {S3_TARGET_PATH}")
    df.write.mode("overwrite").format("parquet").save(S3_TARGET_PATH)

    job.commit()

if __name__ == '__main__':
    main()
