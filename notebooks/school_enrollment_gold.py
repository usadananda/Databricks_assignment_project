# Databricks notebook source
spark.sql("USE CATALOG school_enrollment_db")
spark.sql("USE SCHEMA school_schema_db")

silver_df = spark.table("silver_shool_enrollment")

display(silver_df.limit(100))


# COMMAND ----------

from pyspark.sql.functions import sum, avg

gold_enrollment_trend = silver_df.groupBy("year") \
    .agg(
        sum("enrollment").alias("total_enrollment"),
        avg("performance_score").alias("avg_performance_score")
    ) \
    .orderBy("year")

display(gold_enrollment_trend)

gold_enrollment_trend.write \
  .format("delta") \
  .mode("overwrite") \
  .saveAsTable("school_enrollment_db.school_schema_db.gold_yearly_enrollment_trend")

# COMMAND ----------

gold_gender_distribution = silver_df.groupBy("year", "gender") \
    .agg(
        sum("enrollment").alias("total_enrollment")
    ) \
    .orderBy("year")

display(gold_gender_distribution)

gold_gender_distribution.write \
  .format("delta") \
  .mode("overwrite") \
  .saveAsTable("school_enrollment_db.school_schema_db.gold_gender_distribution")

# COMMAND ----------

gold_region_performance = silver_df.groupBy("region") \
    .agg(
        sum("enrollment").alias("total_students"),
        avg("performance_score").alias("avg_performance")
    ) \
    .orderBy("avg_performance", ascending=False)

display(gold_region_performance)

gold_region_performance.write \
  .format("delta") \
  .mode("overwrite") \
  .saveAsTable("school_enrollment_db.school_schema_db.gold_region_performance")

# COMMAND ----------

gold_school_ranking = silver_df.groupBy("school_id", "school_name") \
    .agg(
        avg("performance_score").alias("avg_score"),
        sum("enrollment").alias("total_students")
    ) \
    .orderBy("avg_score", ascending=False)

display(gold_school_ranking)

gold_school_ranking.write \
  .format("delta") \
  .mode("overwrite") \
  .saveAsTable("school_enrollment_db.school_schema_db.gold_school_ranking")