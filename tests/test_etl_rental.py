import pytest
from unittest.mock import MagicMock, patch, call
import sys
# ... (MOCKEAR módulos awsglue como en el paso anterior, necesario)

# ==========================================================
# PASO 1: MOCKEAR los módulos de AWS Glue (SOLUCIÓN AL ModuleNotFoundError)
# ==========================================================
sys.modules['awsglue'] = MagicMock()
sys.modules['awsglue.context'] = MagicMock()
sys.modules['awsglue.utils'] = MagicMock()
sys.modules['pyspark.context'] = MagicMock()
sys.modules['awsglue.context'].GlueContext = MagicMock()
sys.modules['awsglue.utils'].getResolvedOptions = MagicMock()
sys.modules['pyspark.context'].SparkContext = MagicMock()

# Asegura el path de importación
sys.path.append('glue_scripts')

# Importa el script ETL (esto ejecuta la inicialización sc = SparkContext.getOrCreate(), etc.)
from etl_rental import glueContext, getResolvedOptions 
from etl_rental import spark as etl_spark_session # Importamos la sesión de Spark inicializada

# --- Configuraciones del Test Unitario ---

@patch('etl_rental.getResolvedOptions', return_value={
    'JOB_NAME': 'test-job',
    'PROCESSING_DATE': '2025-10-17' # Fecha de prueba
})
@patch('etl_rental.spark.stop') # Parcheamos la función de cierre de Spark
def test_etl_rental_pipeline_calls(mock_spark_stop, mock_get_resolved_options):
    
    # 1. Crear el mock que simula la función de lectura de RDS
    mock_from_options = MagicMock()

    # 2. Reemplazar la función real del objeto glueContext por nuestro mock
    # Esto es crucial para un módulo que inicializa el contexto a nivel global.
    glueContext.create_dynamic_frame.from_options = mock_from_options
    
    # Simular la salida del DynamicFrame
    mock_dynamic_frame = MagicMock()
    mock_df_input = MagicMock()
    
    # Configuramos la cadena de mocks
    mock_dynamic_frame.toDF.return_value = mock_df_input
    mock_from_options.return_value = mock_dynamic_frame
    
    # Simular la escritura final
    mock_write = MagicMock()
    mock_df_input.write = mock_write
    
    # Configuramos los mocks para la cadena de transformaciones
    mock_df_input.withColumn.return_value.withColumn.return_value.select.return_value.withColumn.return_value = mock_df_input

    # --- Ejecución del Código ETL ---
    # Ya que importamos el script arriba, solo necesitamos correr el código que llama a las funciones
    # Nota: En este caso, el ETL se ejecuta completamente en el import, por lo que la ejecución ya ocurrió.
    
    # --- VERIFICACIONES (ASSERTS) ---

    # 1. VERIFICACIÓN DE LA LECTURA INCREMENTAL (E de ETL)
    mock_from_options.assert_called_once()
    
    # 1a. Verificar la conexión JDBC y el filtro incremental
    expected_predicate = "DATE(r.rental_date) = '2025-10-17'"
    actual_query = mock_from_options.call_args[1]['connection_options']['query']
    
    assert "sakila-rds-connection" in mock_from_options.call_args[1]['connection_options']['connectionName'], "Error: Nombre de conexión JDBC incorrecto."
    assert expected_predicate in actual_query, "Error: El predicado incremental de fecha no se aplicó en la query."

    # 2. VERIFICACIÓN DE LA ESCRITURA EN S3 (L de ETL)
    mock_write.mode.assert_called_once_with("append")
    mock_write.mode().format.assert_called_once_with("parquet")
    mock_write.mode().format().partitionBy.assert_called_once_with("partition_date")
    
    expected_s3_path = "s3://cmjm-datalake/facts/fact_rental/"
    mock_write.mode().format().partitionBy().save.assert_called_once_with(expected_s3_path)
    
    mock_spark_stop.assert_called_once()
    
    print("\nTest Unitario de Pipeline Aprobado.")