import sys
import holidays
from io import BytesIO
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
import boto3
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job

# --- Configuración inicial ---
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
s3 = boto3.client("s3")

bucket_input = "cmjm-datalake"
prefix_input = "fact_rental/"
bucket_output = "cmjm-datalake"
prefix_output = "dim_date/"

# --- Leer archivos parquet del hecho ---
response = s3.list_objects_v2(Bucket=bucket_input, Prefix=prefix_input)
files = [obj["Key"] for obj in response.get("Contents", []) if obj["Key"].endswith(".parquet")]

dfs = []
for file_key in files:
    obj = s3.get_object(Bucket=bucket_input, Key=file_key)
    buffer = BytesIO(obj["Body"].read())
    table = pq.read_table(buffer)
    df = table.to_pandas()
    dfs.append(df)

fact_df = pd.concat(dfs, ignore_index=True)
fact_df["rental_date"] = pd.to_datetime(fact_df["rental_date"]).dt.date

# --- Generar dimensión de fechas ---
unique_dates = sorted(fact_df["rental_date"].unique())
dates_df = pd.DataFrame({"rental_date": unique_dates})
dates_df["date_id"] = pd.to_datetime(dates_df["rental_date"]).dt.strftime("%Y%m%d").astype(int)
dates_df["day"] = pd.to_datetime(dates_df["rental_date"]).dt.day
dates_df["month"] = pd.to_datetime(dates_df["rental_date"]).dt.month
dates_df["year"] = pd.to_datetime(dates_df["rental_date"]).dt.year
dates_df["day_of_week"] = pd.to_datetime(dates_df["rental_date"]).dt.day_name()
dates_df["week_of_year"] = pd.to_datetime(dates_df["rental_date"]).dt.isocalendar().week.astype(int)
dates_df["quarter"] = pd.to_datetime(dates_df["rental_date"]).dt.quarter
dates_df["is_weekend"] = pd.to_datetime(dates_df["rental_date"]).dt.dayofweek.isin([5, 6])
dates_df["is_holiday"] = dates_df["rental_date"].isin(holidays.US())

# --- Guardar en S3 como parquet snappy ---
table = pa.Table.from_pandas(dates_df, preserve_index=False)
buffer_out = BytesIO()
pq.write_table(table, buffer_out, compression="snappy")

output_key = f"{prefix_output}dim_date.snappy.parquet"
s3.put_object(Bucket=bucket_output, Key=output_key, Body=buffer_out.getvalue())

print(f"✅ ETL completado: s3://{bucket_output}/{output_key}")
job.commit()
