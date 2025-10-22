import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

store_df = glueContext.create_dynamic_frame.from_catalog(
    database="sakila_rds",
    table_name="store",
    transformation_ctx="store_df"
)

store_df = store_df.rename_field("store_id", "store_key")

glueContext.write_dynamic_frame.from_options(
    frame=store_df,
    connection_type="s3",
    connection_options={"path": "s3://cmjm-datalake/dim_store/", "partitionKeys": []},
    format="parquet",
    format_options={"compression": "snappy"}
)

job.commit()
