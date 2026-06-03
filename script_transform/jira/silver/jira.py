from pyspark.sql.functions import from_json, col, to_timestamp, coalesce, lit
from pyspark.sql.types import StructType, StructField, StringType, BooleanType

schema_jira = StructType([
    StructField("id", StringType(), True),
    StructField("key", StringType(), True),
    StructField("fields", StructType([
        StructField("summary", StringType(), True),
        StructField("created", StringType(), True),
        StructField("updated", StringType(), True),
        StructField("issuetype", StructType([
            StructField("name", StringType(), True),
            StructField("subtask", BooleanType(), True)
        ]), True),
        StructField("status", StructType([
            StructField("name", StringType(), True),
            StructField("id", StringType(), True)
        ]), True),
        StructField("assignee", StructType([
            StructField("displayName", StringType(), True),
            StructField("accountId", StringType(), True)
        ]), True),
        StructField("priority", StructType([
            StructField("name", StringType(), True)
        ]), True),
        StructField("parent", StructType([
            StructField("id", StringType(), True),
            StructField("key", StringType(), True)
        ]), True)
    ]), True)
])

df_bronze = spark.read.table("jira_bronze.jira_issues_bronze")

df_parsed = df_bronze.withColumn("parsed", from_json(col("raw_payload"), schema_jira))

df_silver = df_parsed.select(
    col("parsed.id").alias("issue_id"),
    col("parsed.key").alias("issue_key"),
    col("parsed.fields.summary").alias("summary"),
    col("parsed.fields.issuetype.name").alias("issue_type"),
    coalesce(col("parsed.fields.issuetype.subtask"), lit(False)).alias("is_subtask"),
    to_timestamp(col("parsed.fields.created"), "yyyy-MM-dd'T'HH:mm:ss.SSSZ").alias("created_at"),
    to_timestamp(col("parsed.fields.updated"), "yyyy-MM-dd'T'HH:mm:ss.SSSZ").alias("updated_at"),
    col("parsed.fields.status.name").alias("status_name"),
    col("parsed.fields.status.id").alias("status_id"),
    col("parsed.fields.assignee.displayName").alias("assignee_name"),
    col("parsed.fields.assignee.accountId").alias("assignee_id"),
    col("parsed.fields.priority.name").alias("priority_name"),
    col("parsed.fields.parent.key").alias("parent_issue_key"),
    col("ingested_at")
)

spark.sql("CREATE SCHEMA IF NOT EXISTS jira_silver")

df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("jira_silver.jira_issues_silver")