# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW development.bronze_views.nyc_taxi
# MAGIC AS
# MAGIC SELECT 
# MAGIC     VendorID
# MAGIC     ,lpep_pickup_datetime AS pickup_datetime
# MAGIC     ,lpep_dropoff_datetime AS dropoff_datetime
# MAGIC     ,date_format(lpep_pickup_datetime, 'HH:mm:ss') AS pickup_time
# MAGIC     ,date_format(lpep_dropoff_datetime, 'HH:mm:ss') AS dropoff_time
# MAGIC     ,ROUND((unix_timestamp(lpep_dropoff_datetime)-unix_timestamp(lpep_pickup_datetime))/(60)) AS duration_min
# MAGIC     ,PULocationID AS pickup_location_id
# MAGIC     ,DOLocationID AS dropoff_location_id
# MAGIC     ,passenger_count
# MAGIC     ,trip_distance
# MAGIC     ,ROUND((trip_distance * 1609.34)/1000,1) AS distance_km
# MAGIC     ,ROUND(TRY_DIVIDE(distance_km,duration_min)*60,1) AS average_speed_kph
# MAGIC     ,fare_amount
# MAGIC     ,ROUND(TRY_DIVIDE(total_amount - tip_amount,distance_km),2) AS price_per_km
# MAGIC     ,extra
# MAGIC     ,mta_tax
# MAGIC     ,tip_amount
# MAGIC     ,tolls_amount
# MAGIC     --,ehail_fee
# MAGIC     ,improvement_surcharge
# MAGIC     ,total_amount
# MAGIC     --,fare_amount + tolls_amount + extra + improvement_surcharge + tip_amount + mta_tax + congestion_surcharge + coalesce(ehail_fee,0) AS grand_total 
# MAGIC     --,grand_total - total_amount AS difference
# MAGIC     ,payment_type
# MAGIC     ,trip_type
# MAGIC     ,congestion_surcharge
# MAGIC     ,CASE 
# MAGIC         WHEN HOUR(pickup_datetime) BETWEEN 0 AND 2 THEN 0
# MAGIC         WHEN HOUR(pickup_datetime) BETWEEN 3 AND 5 THEN 3
# MAGIC         WHEN HOUR(pickup_datetime) BETWEEN 6 AND 8 THEN 6
# MAGIC         WHEN HOUR(pickup_datetime) BETWEEN 9 AND 11 THEN 9
# MAGIC         WHEN HOUR(pickup_datetime) BETWEEN 12 AND 14 THEN 12
# MAGIC         WHEN HOUR(pickup_datetime) BETWEEN 15 AND 17 THEN 15
# MAGIC         WHEN HOUR(pickup_datetime) BETWEEN 18 AND 20 THEN 18
# MAGIC         WHEN HOUR(pickup_datetime) BETWEEN 21 AND 23 THEN 21
# MAGIC     END AS Pickup_Time_Bin
# MAGIC FROM development.bronze.nyc_taxi

# COMMAND ----------

