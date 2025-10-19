import pytest
from unittest.mock import MagicMock, patch, call
import sys
from pyspark.sql import functions 

# 1a. Crear Módulos Falsos para AWS Glue
sys.modules['awsglue'] = MagicMock()
sys.modules['awsglue.context'] = MagicMock()
sys.modules['awsglue.utils'] = MagicMock()
sys.modules['pyspark.context'] = MagicMock()

# 1b. Crear Mocks de las Clases y Funciones
# Esto simula las clases que se importan en el script ETL, satisfaciendo a Python
sys.modules['awsglue.context'].GlueContext = MagicMock()
sys.modules['awsglue.utils'].getResolvedOptions = MagicMock()
sys.modules['pyspark.context'].SparkContext = MagicMock()

# 1c. Mockear todas las funciones de PySpark que se usan a nivel de código
# Esto evita que PySpark compruebe el SparkContext al importar funciones como col().
for attr in ['col', 'lit', 'to_date', 'date_format']:
    setattr(functions, attr, MagicMock(return_value=MagicMock()))


# ==========================================================
# 2. IMPORTACIÓN DEL CÓDIGO ETL 
# ==========================================================
# Asegura el path de importación
sys.path.append('glue_scripts')
# Importa la función principal del ETL
from etl_rental import main as etl_main
# Importa los objetos globales que se inicializarán dentro de main()
from etl_rental import glueContext, getResolvedOptions, spark 


# --- Configuraciones del Test Unitario ---

@patch('etl_rental.getResolvedOptions', return_value={
    'JOB_NAME': 'test-job',
    'PROCESSING_DATE': '2025-10-17' # Fecha de prueba
})
@patch('etl_rental.SparkContext.getOrCreate')
@patch('etl_rental.GlueContext', autospec=True)
def test_etl_rental_pipeline_calls(
    mock_glue_context_class, mock_spark_context_get_or_create, mock_get_resolved_options):
    
    # 1. Configurar Mocks

    # Mock del objeto glueContext INSTANCIADO
    mock_glue_context_instance = mock_glue_context_class.return_value
    mock_from_options = mock_glue_context_instance.create_dynamic_frame.from_options
    
    # Simular la cadena de Spark
    mock_df_input = MagicMock()
    
    # Configuramos la cadena de mocks
    mock_from_options.return_value.toDF.return_value = mock_df_input
    mock_df_input.write = MagicMock()
    
    # Configuramos los mocks para la cadena de transformaciones (simular el .withColumn().select()...)
    mock_df_input.withColumn.return_value.withColumn.return_value.select.return_value.withColumn.return_value = mock_df_input

    # --- EJECUCIÓN DEL CÓDIGO ETL ---
    # Llamamos a la función main()
    etl_main()

    # --- VERIFICACIONES (ASSERTS) ---

    # 1. VERIFICACIÓN DE LA LECTURA INCREMENTAL
    mock_from_options.assert_called_once()
    
    # 1a. Verificar la conexión JDBC y el filtro incremental
    expected_predicate = "DATE(rental_date) = '2025-10-17'"
    actual_query = mock_from_options.call_args[1]['connection_options']['query']
    
    assert "sakila-rds-connection" in mock_from_options.call_args[1]['connection_options']['connectionName']
    assert expected_predicate in actual_query, f"Error: El predicado incremental de fecha no se aplicó correctamente. Query: {actual_query}"

    # 2. VERIFICACIÓN DE LA ESCRITURA EN S3
    mock_df_input.write.mode.assert_called_once_with("append")
    mock_df_input.write.mode().format.assert_called_once_with("parquet")
    mock_df_input.write.mode().format().partitionBy.assert_called_once_with("partition_date")
    
    expected_s3_path = "s3://cmjm-datalake/facts/fact_rental/"
    mock_df_input.write.mode().format().partitionBy().save.assert_called_once_with(expected_s3_path)
    
    # Verificar que Spark se apagó
    mock_spark_context_get_or_create.return_value.stop.assert_called_once()
    
    print("\nTest Unitario de Pipeline Aprobado.")