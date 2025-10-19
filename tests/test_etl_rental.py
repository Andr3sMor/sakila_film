import pytest
from unittest.mock import MagicMock, patch, call
import sys

# ==========================================================
# PASO 1: MOCKEAR los módulos de AWS Glue para Test Unitario
# Esto permite que el script ETL se importe sin errores.
# ==========================================================
sys.modules['awsglue'] = MagicMock()
sys.modules['awsglue.context'] = MagicMock()
sys.modules['awsglue.utils'] = MagicMock()
sys.modules['pyspark.context'] = MagicMock()
# Mockear las clases exactas que el ETL importa
sys.modules['awsglue.context'].GlueContext = MagicMock()
sys.modules['awsglue.utils'].getResolvedOptions = MagicMock()
sys.modules['pyspark.context'].SparkContext = MagicMock()
# Mockear las funciones de PySpark (col, lit, date_format) para que no fallen al ser llamadas
from pyspark.sql import functions
for attr in ['col', 'lit', 'to_date', 'date_format']:
    setattr(functions, attr, MagicMock(return_value=MagicMock()))


# Asegura el path de importación
sys.path.append('glue_scripts')
# Importa el script ETL
from etl_rental import glueContext, getResolvedOptions 


# --- Configuraciones del Test Unitario ---

@patch('etl_rental.getResolvedOptions', return_value={
    'JOB_NAME': 'test-job',
    'PROCESSING_DATE': '2025-10-17' # Fecha de prueba
})
@patch('etl_rental.SparkContext.getOrCreate')
@patch('etl_rental.glueContext.create_dynamic_frame.from_options')
def test_etl_rental_pipeline_calls(
    mock_from_options, mock_spark_context, mock_get_resolved_options):
    
    # Simular la salida de la lectura JDBC (DynamicFrame)
    mock_dynamic_frame = MagicMock()

    # Simular que el DynamicFrame se convierte a Spark DataFrame (df_rental)
    mock_df_input = MagicMock()
    mock_dynamic_frame.toDF.return_value = mock_df_input
    mock_from_options.return_value = mock_dynamic_frame

    # Simular la cadena de transformaciones y la escritura final
    # Hacemos que cada paso de la cadena de Spark devuelva un MagicMock
    mock_df_input.withColumn.return_value.withColumn.return_value.select.return_value.withColumn.return_value = mock_df_input
    mock_write = MagicMock()
    mock_df_input.write = mock_write
    
    # Ejecución de la Unidad de Código ETL
    with patch('etl_rental.spark.stop'):
        import etl_rental

    # --- VERIFICACIONES (ASSERTS) ---

    # 1. VERIFICACIÓN DE LA LECTURA INCREMENTAL (E de ETL)
    mock_from_options.assert_called_once()
    
    # 1a. Verificar la conexión JDBC y el filtro incremental
    expected_predicate = "DATE(r.rental_date) = '2025-10-17'"
    actual_query = mock_from_options.call_args[1]['connection_options']['query']
    
    assert "sakila-rds-connection" in mock_from_options.call_args[1]['connection_options']['connectionName'], "Error: Nombre de conexión JDBC incorrecto."
    assert expected_predicate in actual_query, "Error: El predicado incremental de fecha no se aplicó en la query."

    # 2. VERIFICACIÓN DE LA ESCRITURA EN S3 (L de ETL)
    
    # 2a. Verificar la secuencia final de escritura (mode, format, partitionBy, save)
    mock_write.mode.assert_called_once_with("append")
    mock_write.mode().format.assert_called_once_with("parquet")
    mock_write.mode().format().partitionBy.assert_called_once_with("partition_date")
    
    # 2b. Verificar la ruta de S3
    expected_s3_path = "s3://cmjm-datalake/facts/fact_rental/"
    mock_write.mode().format().partitionBy().save.assert_called_once_with(expected_s3_path)
    
    print("\nTest Unitario de Pipeline Aprobado.")