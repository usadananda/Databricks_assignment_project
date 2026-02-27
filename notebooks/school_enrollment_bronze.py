# Databricks notebook source
# DBTITLE 1,Cell 1
# Set catalog and schema
spark.sql("USE CATALOG school_enrollment_db")
spark.sql("USE SCHEMA school_schema_db")

# Read CSV from Workspace using file:// protocol
df = spark.read \
    .format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .load("file:///Workspace/school_ enrollment_education/school_enrollment_data.csv")

display(df)

# COMMAND ----------


df.write.format("delta") \
  .mode("overwrite")\
  .option("overwriteSchema", "true")\
  .partitionBy("year")\
  .saveAsTable("bronze_shool_enrollment")

print("✅ Bronze table created with 10,000 records.")