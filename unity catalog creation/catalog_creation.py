# Databricks notebook source
# Create catalog if it does not exist
spark.sql("""
CREATE CATALOG IF NOT EXISTS school_enrollment_db 
MANAGED LOCATION 'abfss://unity-catalog-storage@dbstorageqrqv7rfwczezo.dfs.core.windows.net/7405613349767315'
""")

# Create schema in the new catalog if it does not exist
spark.sql("CREATE SCHEMA IF NOT EXISTS school_enrollment_db.school_schema_db")