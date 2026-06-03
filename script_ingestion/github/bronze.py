import requests
import json
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import Row
import pyspark.sql.functions as F

# 1. PARÂMETROS E SEGURANÇA
USUARIO = "DaviMark"
SCHEMA = "github_bronze"

# Cria um campo de texto no topo do Notebook para você colar o Token de forma segura
dbutils.widgets.text("GITHUB_TOKEN", "", "Insira seu Token do GitHub:")
TOKEN = dbutils.widgets.get("GITHUB_TOKEN")

if not TOKEN:
    raise ValueError("Pare! Insira o token no campo 'GITHUB_TOKEN' no topo da tela e rode novamente.")

HEADERS = {
    "Authorization": f"token {TOKEN}",
    "Accept": "application/vnd.github.v3+json"
}

# 2. FUNÇÕES DE EXTRAÇÃO (CLEAN CODE)
def extrair_repositorios(usuario):
    """Retorna todos os repositórios do usuário."""
    repos = []
    pagina = 1
    while True:
        url = f"https://api.github.com/users/{usuario}/repos?per_page=100&page={pagina}"
        resp = requests.get(url, headers=HEADERS)
        if resp.status_code != 200 or not resp.json():
            break
        repos.extend(resp.json())
        pagina += 1
    return repos

def extrair_commits(usuario, repo_nome, repo_id):
    """Retorna o histórico de commits de um repositório, anexando o ID do repo."""
    commits = []
    pagina = 1
    while True:
        url = f"https://api.github.com/repos/{usuario}/{repo_nome}/commits?per_page=100&page={pagina}"
        resp = requests.get(url, headers=HEADERS)
        if resp.status_code != 200 or not resp.json():
            break
        
        for c in resp.json():
            c["custom_repo_id"] = repo_id
            commits.append(Row(raw_payload=json.dumps(c)))
        pagina += 1
    return commits

# 3. ORQUESTRAÇÃO DA INGESTÃO
spark.sql(f"CREATE DATABASE IF NOT EXISTS {SCHEMA}")

# Extrai repositórios
lista_repos = extrair_repositorios(USUARIO)
linhas_repos = [Row(raw_payload=json.dumps(r)) for r in lista_repos]

# Extrai commits de cada repositório
linhas_commits = []
for repo in lista_repos:
    linhas_commits.extend(extrair_commits(USUARIO, repo["name"], repo["id"]))

if not linhas_commits:
    linhas_commits.append(Row(raw_payload=json.dumps({})))

# 4. GRAVAÇÃO NA CAMADA BRONZE (DELTA)
schema_bronze = StructType([StructField("raw_payload", StringType(), True)])

# Cria DataFrames e adiciona data de ingestão
df_repos = spark.createDataFrame(linhas_repos, schema=schema_bronze).withColumn("ingestion_date", F.current_timestamp())
df_commits = spark.createDataFrame(linhas_commits, schema=schema_bronze).withColumn("ingestion_date", F.current_timestamp())

# Salva tabelas
df_repos.write.format("delta").mode("overwrite").saveAsTable(f"{SCHEMA}.repositorios")
df_commits.write.format("delta").mode("overwrite").saveAsTable(f"{SCHEMA}.commits")

print("Camada Bronze finalizada com sucesso!")