from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import col, lit, date_format
from awsglue.utils import getResolvedOptions
import sys

# 1. Inicialización de Contexto de Glue
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'PROCESSING_DATE']) 
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# --- PARÁMETROS CLAVE ---
PROCESSING_DATE = args['PROCESSING_DATE'] 
S3_TARGET_PATH = "s3://cmjm-datalake/facts/fact_rental/"
# Mantenemos el nombre de la conexión.
CONNECTION_NAME = "Jdbc connection" 
DB_TABLE = "rental"

# 2. Definir el Predicado de Filtrado (Lectura Incremental)
predicate = f"DATE(rental_date) = '{PROCESSING_DATE}'"

print(f"-> Leyendo la tabla {DB_TABLE} con filtro: {predicate}")

# 3. Lectura desde RDS con 'Pushdown Predicate'
# **¡Aquí es donde AWS Glue hace la magia con Secrets Manager!**
datasource = glueContext.create_dynamic_frame.from_options(
    connection_type="jdbc",
    connection_options={
        # 'useConnectionProperties': 'true' le indica a Glue que use la configuración de la conexión nombrada.
        "useConnectionProperties": "true", 
        # 'connectionName': Glue lo utiliza para buscar la configuración, 
        # incluyendo el secreto asociado en Secrets Manager.
        "connectionName": CONNECTION_NAME, 
        "dbtable": DB_TABLE,
        "query": f"""
            SELECT 
                r.rental_id, r.rental_date, r.inventory_id, r.customer_id, r.staff_id, r.last_update, 
                p.amount, p.payment_date
            FROM rental r
            JOIN payment p ON r.rental_id = p.rental_id
            WHERE DATE(r.rental_date) = '{PROCESSING_DATE}'
        """
    },
    transformation_ctx="datasource"
)

df_rental = datasource.toDF()

# 4. Transformaciones para el Modelo Dimensional
df_fact_rental = df_rental.withColumn(
    "date_id", 
    date_format(col("rental_date"), "yyyyMMdd").cast("int")
).withColumn(
    "film_id", 
    lit(1)
).select(
    "amount",
    "rental_date",
    "date_id",
    "customer_id",
    col("film_id").alias("film_id"),
    col("staff_id").alias("store_id")
).withColumn(
    "partition_date", 
    lit(PROCESSING_DATE)
)

# 5. Escritura en S3 en formato Parquet y Particionamiento
df_fact_rental.write.mode("append").format("parquet").partitionBy("partition_date").save(S3_TARGET_PATH)

print(f"-> Fact_rental cargado y particionado para el día: {PROCESSING_DATE}")

spark.stop()