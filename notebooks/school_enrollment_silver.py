# Databricks notebook source
# Use catalog & schema
spark.sql("USE CATALOG school_enrollment_db")
spark.sql("USE SCHEMA school_schema_db")

from pyspark.sql.functions import col, upper, current_timestamp

# Read Bronze table
bronze_df = spark.table("bronze_shool_enrollment")

# Capture bronze row count
bronze_count = bronze_df.count()
print(f"Bronze Row Count: {bronze_count}")

# Apply Cleaning & Filters
silver_df = bronze_df \
    .dropDuplicates(["school_id", "grade", "gender", "year"]) \
    .filter(col("school_id").isNotNull()) \
    .filter(col("school_name").isNotNull()) \
    .filter(col("region").isNotNull()) \
    .filter(col("grade").between(1,12)) \
    .filter(col("gender").isin("M","F")) \
    .filter(col("year").isNotNull()) \
    .filter(col("enrollment") > 0) \
    .filter(col("performance_score").between(0,100)) \
    .withColumn("region", upper(col("region"))) \
    .withColumn("silver_processed_time", current_timestamp())

# Capture silver row count
silver_count = silver_df.count()
print(f"Silver Row Count: {silver_count}")

#validation

null_school = silver_df.filter(col("school_id").isNull()).count()
invalid_enrollment = silver_df.filter(col("enrollment") <= 0).count()
invalid_performance = silver_df.filter(
    (col("performance_score") < 0) | 
    (col("performance_score") > 100)
).count()

print(f"Null school_id count: {null_school}")
print(f"Invalid enrollment count: {invalid_enrollment}")
print(f"Invalid performance count: {invalid_performance}")

# Fail pipeline if validation fails
if null_school > 0 or invalid_enrollment > 0 or invalid_performance > 0:
    raise Exception("❌ Data Validation Failed!")
else:
    print("✅ Data validation passed successfully.")

display(silver_df.limit(100))


# COMMAND ----------

# Write Silver table
silver_df.write \
  .format("delta") \
  .mode("overwrite") \
  .option("overwriteSchema", "true") \
  .partitionBy("year") \
  .saveAsTable("silver_shool_enrollment")

print("✅ Silver table created successfully")