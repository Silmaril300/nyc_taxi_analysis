# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW development.silver_views.nyc_taxi
# MAGIC AS
# MAGIC SELECT 
# MAGIC     VendorID
# MAGIC     ,DATE(pickup_datetime) AS pickup_date
# MAGIC     --,DATE(dropoff_datetime) AS dropoff_date
# MAGIC     ,pickup_time
# MAGIC     ,dropoff_time
# MAGIC     ,duration_min
# MAGIC     ,zp.Zone AS pickup_location
# MAGIC     ,pickup_location_id
# MAGIC     ,dropoff_location_id
# MAGIC     ,zd.Zone AS dropoff_location
# MAGIC     ,passenger_count
# MAGIC     ,trip_distance
# MAGIC     ,ROUND((trip_distance * 1609.34)/1000,1) AS distance_km
# MAGIC     ,ROUND(TRY_DIVIDE(distance_km,duration_min)*60,1) AS average_speed_kph
# MAGIC     ,fare_amount
# MAGIC     ,ROUND(TRY_DIVIDE(total_amount - tip_amount,distance_km),2) AS price_per_km
# MAGIC     ,tip_amount
# MAGIC     ,total_amount
# MAGIC     ,Pickup_Time_Bin
# MAGIC     ,hour(pickup_datetime) AS hour_of_day
# MAGIC FROM development.bronze_views.nyc_taxi t
# MAGIC LEFT JOIN development.bronze.taxi_zone_lookup zp 
# MAGIC ON t.pickup_location_id = zp.LocationID
# MAGIC LEFT JOIN development.bronze.taxi_zone_lookup zd 
# MAGIC ON t.dropoff_location_id = zd.LocationID