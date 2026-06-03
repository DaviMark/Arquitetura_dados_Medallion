# CAMADA SILVER: LIMPEZA E PADRONIZAÇÃO
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType
import pyspark.sql.functions as F

SCHEMA_BRONZE = "github_bronze"
SCHEMA_SILVER = "github_silver"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {SCHEMA_SILVER}")
print(f"Iniciando processamento da camada Silver no schema: {SCHEMA_SILVER}")

# Schemas de leitura do JSON bruto
schema_json_repo = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("full_name", StringType(), True),
    StructField("private", BooleanType(), True),
    StructField("html_url", StringType(), True),
    StructField("description", StringType(), True),
    StructField("language", StringType(), True),
    StructField("stargazers_count", IntegerType(), True),
    StructField("forks_count", IntegerType(), True),
    StructField("watchers_count", IntegerType(), True),
    StructField("open_issues_count", IntegerType(), True),
    StructField("size", IntegerType(), True),
    StructField("has_wiki", BooleanType(), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("pushed_at", StringType(), True),
    StructField("license", StructType([StructField("name", StringType(), True)]), True)
])

schema_json_commit = StructType([
    StructField("sha", StringType(), True),
    StructField("custom_repo_id", IntegerType(), True),
    StructField("html_url", StringType(), True),
    StructField("commit", StructType([
        StructField("message", StringType(), True),
        StructField("comment_count", IntegerType(), True),
        StructField("author", StructType([
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("date", StringType(), True)
        ]), True),
        StructField("committer", StructType([
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("date", StringType(), True)
        ]), True),
        StructField("verification", StructType([StructField("verified", BooleanType(), True)]), True)
    ]), True)
])

# Leitura, parsing e tipagem dos repositórios
df_silver_repos = spark.read.table(f"{SCHEMA_BRONZE}.repositorios") \
    .withColumn("parsed", F.from_json(F.col("raw_payload"), schema_json_repo)) \
    .select("parsed.*", "ingestion_date") \
    .withColumn("repo_created_at", F.to_timestamp("created_at", "yyyy-MM-dd'T'HH:mm:ss'Z'")) \
    .withColumn("repo_updated_at", F.to_timestamp("updated_at", "yyyy-MM-dd'T'HH:mm:ss'Z'")) \
    .withColumn("repo_pushed_at", F.to_timestamp("pushed_at", "yyyy-MM-dd'T'HH:mm:ss'Z'")) \
    .withColumn("repo_license", F.col("license.name")) \
    .withColumnRenamed("id", "repo_id") \
    .withColumnRenamed("name", "repo_name") \
    .withColumnRenamed("full_name", "repo_full_name") \
    .withColumnRenamed("private", "repo_private") \
    .withColumnRenamed("html_url", "repo_html_url") \
    .withColumnRenamed("description", "repo_description") \
    .withColumnRenamed("language", "repo_language") \
    .withColumnRenamed("stargazers_count", "repo_stars") \
    .withColumnRenamed("forks_count", "repo_forks") \
    .withColumnRenamed("watchers_count", "repo_watchers") \
    .withColumnRenamed("open_issues_count", "repo_open_issues") \
    .withColumnRenamed("size", "repo_size_kb") \
    .withColumnRenamed("has_wiki", "repo_has_wiki") \
    .drop("created_at", "updated_at", "pushed_at", "license") \
    .withColumn("source_system", F.lit("API_GITHUB"))

# Leitura, parsing e tipagem dos commits
df_silver_commits = spark.read.table(f"{SCHEMA_BRONZE}.commits") \
    .withColumn("parsed", F.from_json(F.col("raw_payload"), schema_json_commit)) \
    .select("parsed.*", "ingestion_date") \
    .filter(F.col("sha").isNotNull()) \
    .withColumn("commit_autor_data", F.to_timestamp("commit.author.date", "yyyy-MM-dd'T'HH:mm:ss'Z'")) \
    .withColumn("commit_committer_data", F.to_timestamp("commit.committer.date", "yyyy-MM-dd'T'HH:mm:ss'Z'")) \
    .withColumn("autor_nome", F.coalesce(F.col("commit.author.name"), F.lit("Unknown"))) \
    .withColumn("autor_email", F.coalesce(F.col("commit.author.email"), F.lit("unknown@github.com"))) \
    .withColumn("committer_nome", F.coalesce(F.col("commit.committer.name"), F.lit("Unknown"))) \
    .withColumn("committer_email", F.coalesce(F.col("commit.committer.email"), F.lit("unknown@github.com"))) \
    .withColumn("commit_mensagem", F.col("commit.message")) \
    .withColumn("commit_comment_count", F.col("commit.comment_count")) \
    .withColumn("commit_verified", F.coalesce(F.col("commit.verification.verified"), F.lit(False))) \
    .withColumnRenamed("sha", "commit_sha") \
    .withColumnRenamed("custom_repo_id", "repo_id") \
    .withColumnRenamed("html_url", "commit_url") \
    .select("commit_sha", "repo_id", "commit_url", "autor_nome", "autor_email", "commit_autor_data", 
            "committer_nome", "committer_email", "commit_committer_data", "commit_mensagem", 
            "commit_comment_count", "commit_verified", "ingestion_date") \
    .withColumn("source_system", F.lit("API_GITHUB"))

# Persistência no formato Delta
df_silver_repos.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SCHEMA_SILVER}.repositorios")
df_silver_commits.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SCHEMA_SILVER}.commits")
print("Camada Silver finalizada com sucesso!")