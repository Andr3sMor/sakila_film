# tests/test_etl_rental.py
import pytest
from unittest.mock import MagicMock, patch, call
from datetime import datetime

# Importamos el script ETL directamente
# Asegúrate de que el path de importación sea correcto desde tu carpeta /tests
import sys
sys.path.append('glue_scripts')
from etl_rental import glueContext, getResolvedOptions # Importamos las funciones necesarias (simuladas)

# --- Mocking del Entorno AWS Glue/Spark ---

# 1. Mock de los argumentos de entrada de AWS Glue
@patch('etl_rental.getResolvedOptions', return_value={
    'JOB_NAME': 'test-job',
    'PROCESSING_DATE': '2025-10-17' # Fecha de prueba
})
# 2. Mock del contexto de Spark y Glue
@patch('etl_rental.SparkContext.getOrCreate')
@patch('etl_rental.GlueContext', autospec=True)
# 3. Mock de la salida de datos de la base de datos (RDS)
@patch('etl_rental.glueContext.create_dynamic_frame.from_options')
def test_etl_rental_processing_date_and_output(
    mock_from_options, mock_glue_context, mock_spark_context, mock_get_resolved_options):
    
    # Configuración de Mocks:

    # 3.1 Simular los datos leídos de RDS (Datos de prueba para Fact_rental)
    # Se simula un DataFrame de Spark con las columnas esperadas después de la lectura
    mock_df_input = MagicMock()
    mock_df_input.columns = [
        "rental_id", "rental_date", "inventory_id", "customer_id", 
        "staff_id", "last_update", "amount", "payment_date"
    ]
    # Usamos una estructura de datos simple para evitar la complejidad de PySpark
    mock_df_input.toDF.return_value = mock_df_input 

    # 3.2 La lectura de RDS debe retornar este mock de datos
    mock_from_options.return_value = mock_df_input

    # 3.3 Mockear el método .write para verificar las llamadas finales
    mock_write = MagicMock()
    mock_df_input.write = mock_write
    
    # Ejecución del Script ETL
    with patch('etl_rental.spark.stop'): # Evitar que el script termine la sesión de Spark
        import etl_rental

    # --- Verificaciones ---

    # 1. Verificar la llamada a la lectura incremental (RDS)
    # Se verifica que el filtro (WHERE clause) para la fecha se pasó correctamente a RDS
    expected_predicate = "DATE(rental_date) = '2025-10-17'"
    
    # Se verifica que se llamó a la lectura de datos
    mock_from_options.assert_called_once()
    
    # Se verifica que el 'query' dentro de connection_options contenga la fecha correcta
    actual_query = mock_from_options.call_args[1]['connection_options']['query']
    assert expected_predicate in actual_query, "El predicado de fecha incremental no se aplicó correctamente en la query."
    assert "sakila-rds-connection" in mock_from_options.call_args[1]['connection_options']['connectionName'], "El nombre de la conexión JDBC no es el esperado."

    # 2. Verificar la llamada a la escritura Parquet (S3)
    # Se verifica que la escritura final se hizo con los parámetros correctos
    
    # Verificación de que se llama a .write con el modo 'append' y formato 'parquet'
    mock_write.mode.assert_called_once_with("append")
    mock_write.mode().format.assert_called_once_with("parquet")
    
    # Verificación del particionamiento
    mock_write.mode().format().partitionBy.assert_called_once_with("partition_date")
    
    # Verificación de la ruta de S3
    expected_s3_path = "s3://cmjm-datalake/facts/fact_rental/"
    mock_write.mode().format().partitionBy().save.assert_called_once_with(expected_s3_path)
    
    print("\nTest de Integración Funcional (Mocked) Aprobado.") 