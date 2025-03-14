# Databricks notebook source
dbutils.widgets.text("Table", "")

# COMMAND ----------

table = dbutils.widgets.get("Table")

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp, hour, avg, sum

# 1. Läs in rådata till Bronze-lagret
bronze_df = spark.read.table(table)

bronze_df.write.format("delta").mode("overwrite").saveAsTable("development.bronze.taxi_zone_lookup")

# 2. Transformera och lagra i Silver-lagret
silver_df = spark.read.table("development.bronze.taxi_zone_lookup")

silver_df.write.format("delta").mode("overwrite").saveAsTable("development.silver.taxi_zone_lookup")

# 3. Skapa Gold-lagret för analys
gold_df = silver_df

gold_df.write.format("delta").mode("overwrite").saveAsTable("development.gold.dim_taxi_zone_lookup")

print("ETL-processen är klar!")