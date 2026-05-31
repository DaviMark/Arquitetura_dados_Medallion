import requests
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def main():
    # Inicializa a sessão Spark no ambiente Databricks
    spark = SparkSession.builder.appName("IngestApiToBronze").getOrCreate()
    
    # Simulação da chamada de API
    api_url = "https://api.exemplo.com/dados"
    print(f"Iniciando extração de dados da API: {api_url}")
    
    # Mock de dados simulando o retorno JSON da API para o portfólio
    mock_data = [
        {"id": 1, "nome": "Cliente Alpha", "status": "Ativo"},
        {"id": 2, "nome": "Cliente Beta", "status": "Inativo"},
        {"id": 3, "nome": "Cliente Gama", "status": "Ativo"}
    ]
    
    # Definição estrita do schema para garantir governança na entrada
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("nome", StringType(), True),
        StructField("status", StringType(), True)
    ])
    
    # Criação do DataFrame Spark
    df = spark.createDataFrame(mock_data, schema)
    
    # Definição do caminho de destino na camada Bronze (simulação no DBFS)
    bronze_path = "/mnt/datalake/bronze/clientes"
    
    print(f"Salvando dados brutos na camada Bronze em formato Delta: {bronze_path}")
    df.write.format("delta").mode("overwrite").save(bronze_path)
    print("Processo de ingestão concluído com sucesso.")

if __name__ == "__main__":
    main()