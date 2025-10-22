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

film_df = glueContext.create_dynamic_frame.from_catalog(
    database="sakila_rds",
    table_name="film",
    transformation_ctx="film_df"
)

film_df = film_df.rename_field("film_id", "film_key")

glueContext.write_dynamic_frame.from_options(
    frame=film_df,
    connection_type="s3",
    connection_options={"path": "s3://cmjm-datalake/dim_film/", "partitionKeys": []},
    format="parquet",
    format_options={"compression": "snappy"}
)

job.commit()
