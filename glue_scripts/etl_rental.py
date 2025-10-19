import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from awsglue import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql.functions import col, lit, date_format
from awsgluedq.transforms import EvaluateDataQuality

# -----------------------------
# 1. Parámetros necesarios
# -----------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME", "PROCESSING_DATE"])
PROCESSING_DATE = args["PROCESSING_DATE"]

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

print(f"Iniciando ETL para fecha: {PROCESSING_DATE}")

# -----------------------------
# 2. Tablas desde Glue Catalog
# -----------------------------
rental_dyf = glueContext.create_dynamic_frame.from_catalog(
    database="sakilabd",
    table_name="sakila_rental",
    transformation_ctx="rental_dyf"
)

payment_dyf = glueContext.create_dynamic_frame.from_catalog(
    database="sakilabd",
    table_name="sakila_payment",
    transformation_ctx="payment_dyf"
)

# Convertimos a DataFrame para transformar fácilmente
rental_df = rental_dyf.toDF()
payment_df = payment_dyf.toDF()

# -----------------------------
# 3. Join entre rental y payment
# -----------------------------
df_joined = rental_df.join(payment_df, on="rental_id", how="inner")

# -----------------------------
# 4. Filtro incremental por fecha de procesamiento
# -----------------------------
df_filtered = df_joined.filter(date_format(col("rental_date"), "yyyy-MM-dd") == PROCESSING_DATE)

# -----------------------------
# 5. Transformaciones a modelo dimensional
# -----------------------------
df_fact_rental = (
    df_filtered
    .withColumn("date_id", date_format(col("rental_date"), "yyyyMMdd").cast("int"))
    .withColumn("film_id", lit(1))
    .withColumn("partition_date", lit(PROCESSING_DATE))
    .select(
        "amount",
        "rental_date",
        "date_id",
        "customer_id",
        col("film_id").alias("film_id"),
        col("staff_id").alias("store_id"),
        "partition_date"
    )
)

# -----------------------------
# 6. Data Quality Básico
# -----------------------------
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

dq_result = EvaluateDataQuality().process_rows(
    frame=DynamicFrame.fromDF(df_fact_rental, glueContext, "dq_fact_rental"),
    ruleset=DEFAULT_DATA_QUALITY_RULESET,
    publishing_options={"dataQualityEvaluationContext": "dq_fact_rental"},
    additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"}
)

# -----------------------------
# 7. Escritura incremental a S3 (particionado por fecha)
# -----------------------------
S3_TARGET_PATH = "s3://cmjm-datalake/facts/fact_rental/"

sink = glueContext.getSink(
    path=S3_TARGET_PATH,
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["partition_date"],
    enableUpdateCatalog=True,
    transformation_ctx="sink_fact_rental"
)

sink.setCatalogInfo(
    catalogDatabase="finanzas2",
    catalogTableName="fact_rental_inc"
)

sink.setFormat("glueparquet", compression="snappy")

print(f"Escribiendo fact_rental incrementalmente en {S3_TARGET_PATH}")
sink.writeFrame(DynamicFrame.fromDF(df_fact_rental, glueContext, "fact_rental_dyf"))

job.commit()
print(f"ETL completado para la fecha: {PROCESSING_DATE}")
