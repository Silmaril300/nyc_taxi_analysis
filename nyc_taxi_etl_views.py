# Databricks notebook source
dbutils.widgets.text("Table", "")

# COMMAND ----------

table = dbutils.widgets.get("Table")

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp, hour, avg, sum

# 1. Läs in rådata till Bronze-lagret
bronze_df = spark.read.table(table)

bronze_df = bronze_df.dropna(subset=["lpep_pickup_datetime", "lpep_dropoff_datetime", "fare_amount", "payment_type", "trip_type", "passenger_count"])
bronze_df.write.format("delta").mode("overwrite").saveAsTable("development.bronze.stage_nyc_taxi")

spark.sql(f"""
        MERGE INTO development.bronze.nyc_taxi AS t
        USING development.bronze.stage_nyc_taxi AS s
        ON s.lpep_pickup_datetime = t.lpep_pickup_datetime AND s.lpep_dropoff_datetime = t.lpep_dropoff_datetime
        WHEN NOT MATCHED
        THEN INSERT *
        """)


# 2. Transformera och lagra i Silver-lagret
spark.sql(f"""
        MERGE INTO development.silver.nyc_taxi AS t
        USING development.bronze_views.nyc_taxi AS s
        ON s.pickup_datetime = t.pickup_datetime AND s.dropoff_datetime = t.dropoff_datetime
        WHEN NOT MATCHED
        THEN INSERT *
        """)
        
silver_df = spark.sql(f'SELECT * FROM development.silver.nyc_taxi')

# First time load only
#silver_df = spark.sql(f'SELECT * FROM development.bronze_views.nyc_taxi')
#silver_df.write.format("delta").mode("overwrite").saveAsTable("development.silver.nyc_taxi")

# 3. Skapa Gold-lagret för analys
gold_df_rev = silver_df.withColumn("hour_of_day", hour(col("pickup_datetime"))) \
                   .groupBy("hour_of_day") \
                   .agg(avg("fare_amount").alias("avg_fare"), sum("fare_amount").alias("total_revenue"))


gold_df_rev.write.format("delta").mode("overwrite").saveAsTable("development.gold.total_revenue_nyc_taxi")

gold_df = spark.sql(f'SELECT * FROM development.silver_views.nyc_taxi')

gold_df.write.format("delta").mode("overwrite").saveAsTable("development.gold.nyc_taxi")

print("ETL-processen är klar!")