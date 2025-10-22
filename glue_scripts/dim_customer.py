import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

# --- Configuración inicial ---
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# --- Leer desde RDS (tabla customer) ---
customer_df = glueContext.create_dynamic_frame.from_catalog(
    database="sakila_rds",
    table_name="customer",
    transformation_ctx="customer_df"
)

# --- Transformaciones ---
customer_df = customer_df.rename_field("customer_id", "customer_key")
customer_df = customer_df.rename_field("first_name", "first_name")
customer_df = customer_df.rename_field("last_name", "last_name")

# Crear columna completa del nombre
customer_spark_df = customer_df.toDF()
from pyspark.sql.functions import concat_ws
customer_spark_df = customer_spark_df.withColumn(
    "full_name", concat_ws(" ", customer_spark_df.first_name, customer_spark_df.last_name)
)

final_customer_df = DynamicFrame.fromDF(customer_spark_df, glueContext, "final_customer_df")

# --- Escribir en
