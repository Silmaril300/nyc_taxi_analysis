# Databricks notebook source
# MAGIC %pip install geopandas

# COMMAND ----------

# MAGIC %pip install contextily

# COMMAND ----------

# MAGIC %pip install mapclassify

# COMMAND ----------

# DBTITLE 1,Import libraries
# import modules
import pandas as pd
import numpy as np

import matplotlib.pyplot as plt
%matplotlib inline
import seaborn as sns

import contextily as ctx
import mapclassify

import warnings

# set column and row display
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

# disable warnings
pd.options.mode.chained_assignment = None
warnings.filterwarnings('ignore')

# COMMAND ----------

# DBTITLE 1,Load Shape file
import geopandas as gpd
sdf = gpd.read_file('/Volumes/development/default/shape/taxi_zones.shx')

# set shapefile format to metric (epsg = 3857)
df_cab = sdf.to_crs(epsg=3857)

# set taxi zone ID column format to integer
df_cab['LocationID'] = df_cab['LocationID'].astype(int)

# rename columns
df_cab.rename(columns = {'LocationID':'Pickup_ID'}, inplace = True)

df_cab.head()

# COMMAND ----------

# DBTITLE 1,Manipulate geometry
# make a copy of dataframe
points = df_cab.copy()

# change geometry to centroid (center point of a specific area) 
points['geometry'] = points['geometry'].centroid

points.head()

# COMMAND ----------

# DBTITLE 1,find out airport zone ID codes
# find out airport zone ID codes
df_airport_codes = points.loc[points['zone'].isin(['Newark Airport', 'JFK Airport', 'LaGuardia Airport'])]

df_airport_codes.head()

# COMMAND ----------

# DBTITLE 1,pickup points with most revenue
# MAGIC %sql
# MAGIC -- pickup points with most revenue
# MAGIC SELECT g.LocationID, g.Zone, SUM(s.fare_amount) AS total_revenue
# MAGIC FROM development.gold.nyc_taxi s
# MAGIC LEFT JOIN development.gold.dim_taxi_zone_lookup g ON s.pickup_location_id = g.LocationID
# MAGIC GROUP BY g.LocationID, g.Zone
# MAGIC ORDER BY total_revenue DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# DBTITLE 1,Select Airport locations
# select the three airport locations
airport_points = points.loc[points['Pickup_ID'].isin([1, 132, 138])]

# remove airport locations from other taxi zones
pickup_points = points[~points['Pickup_ID'].isin([1, 132, 138])]

# select pickup points with most revenue (from previous analysis)
pickup_points_revenue = points.loc[points['Pickup_ID'].isin([74,75,166,82,95,43,41,65,244,97])]

# COMMAND ----------

# DBTITLE 1,plot taxi zones and airport locations
# plot taxi zones and airport locations
# taxi zone color is defined by the ID number (1-263)
ax = df_cab.plot(column='Pickup_ID', figsize=(20, 20), alpha=0.5, edgecolor='k', legend=False)

# set axis, title etc.
ax.set_axis_off();
ax.set(title='NYC airports and taxi zones')
ax.title.set_size(24)

# all zones are marked with red dots
# airports are marked with white dots
# pickup_points.plot(ax=ax, marker='o', color='red', markersize= 50)
airport_points.plot(ax=ax, marker='o', color='white', markersize= 600)
pickup_points_revenue.plot(ax=ax, marker='o', color='blue', markersize= 100)

# add New York City basemap
ctx.add_basemap(
    ax,
    # CRS definition (without the line below, the map is incorrect)
    crs=df_cab.crs.to_string(),
)

# COMMAND ----------

# DBTITLE 1,select columns including borough and taxi zone names
# select columns including borough and taxi zone names
df_locations = df_cab.iloc[:,[3,4,5]]

# change format to integer
df_locations['Pickup_ID'] = df_locations['Pickup_ID'].astype(int)

df_locations.head()

# COMMAND ----------

# DBTITLE 1,Read Taxi data
# create a copy of data
# df = spark.read.table("development.default.green_tripdata_2024_04")
df = spark.read.table("development.gold.nyc_taxi")
data = df.filter(df['dropoff_location_id'].isin([1, 132, 138]))

data_copy = data.select("*").toPandas()

# rename columns
data_copy.rename(columns = {'pickup_location_id':'Pickup_ID', 'dropoff_location_id':'Dropoff_ID'}, inplace = True)

data_copy.head()

# COMMAND ----------

# DBTITLE 1,Merge taxi and pickup locations
# rename columns
df_locations.rename(columns = {'borough':'Pickup_Borough', 'zone':'Pickup_Zone'}, inplace = True)

# merge dataframes
df_pickups = pd.merge(data_copy, df_locations, on= 'Pickup_ID',how='left')

df_pickups.head()

# COMMAND ----------

# MAGIC %md
# MAGIC Next we do the exact same thing on dropoff locations. All we have to do is to rename the columns in df_locations dataframe and merge dataframes again. This time we will merge df_pickups with df_locations, and we will call the result df_dropoffs.

# COMMAND ----------

# DBTITLE 1,Merge taxi and dropoff locations
# rename columns
df_locations.rename(columns = {'Pickup_ID':'Dropoff_ID', 'Pickup_Borough':'Dropoff_Borough', 'Pickup_Zone':'Dropoff_Zone'}, inplace = True)

# merge dataframes
df_dropoffs = pd.merge(df_pickups, df_locations, on= 'Dropoff_ID',how='left')

df_dropoffs.head()

# COMMAND ----------

# rename dataframe
data = df_dropoffs

# drop unnecessary columns
data.drop(["pickup_location", "dropoff_location"], axis=1, inplace = True)

# COMMAND ----------

# DBTITLE 1,Set minimum duration and cost and max distance for trip
# set minimum duration of trip to five minutes
data = data[(data['duration_min'] >= 5)]

# set minimum cost of a fate to five dollars
data = data[(data['fare_amount'] >= 5.0)]

# set maximum distance of trip to 150 km
data = data[data.distance_km < 150.0]

# check distance percentiles
data['distance_km'].describe(percentiles=[.25, .5, .75, .99])

# COMMAND ----------

# check price_per_km percentiles
data['price_per_km'].describe(percentiles=[.25, .5, .75, .99])

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM development.silver.nyc_taxi
# MAGIC WHERE distance_km > 150
# MAGIC --WHERE price_per_km > 50 AND distance_km > 1 AND duration_min > 5

# COMMAND ----------

# define time slot bins
bins = [0, 3, 6, 9, 12, 15, 18, 21, 24]

# add the bins to the dataframe as column
data['Pickup_Time_Bin2'] = pd.cut(pd.to_datetime(data['pickup_time']).dt.hour, bins, right=False, duplicates = 'drop')

# show unique values
data['Pickup_Time_Bin2'].unique()

# pickup slot percentages
pickup_slot = data['Pickup_Time_Bin2'].value_counts(normalize=True) * 100

pickup_slot

# COMMAND ----------

# set barplot parameters
sns.set_style("darkgrid")
#sns.set(font="Verdana")
ax = sns.barplot(data=data, x='Pickup_Time_Bin2', y='price_per_km', errorbar=None)


# set axes, title etc.
ax.figure.set_size_inches(14,8)
ax.set_title('NYC taxi: Average cost of ride/km', size = 24, weight='bold')
ax.set_xlabel('Time of Day (three-hour slots)', size = 14, weight='bold')
ax.set_ylabel('Cost per km (USD)', size = 14, weight='bold')
ax.title.set_size(18)
ax.xaxis.set_tick_params(labelsize = 14)
ax.yaxis.set_tick_params(labelsize = 14)
ax.grid(False)

plt.show()

# COMMAND ----------

# set barplot parameters
sns.set_style("darkgrid")
# sns.set(font="Verdana")
ax = sns.barplot(data=data, x='Pickup_Time_Bin2', y='duration_min', errorbar=None)


# set axes, title etc.
ax.figure.set_size_inches(14,8)
ax.set_title('NYC Taxi: Average ride duration in minutes', size = 24, weight='bold')
ax.set_xlabel('Time of Day (three-hour slots)', size = 14, weight='bold')
ax.set_ylabel('Duration in minutes', size = 14, weight='bold')
ax.title.set_size(18)
ax.xaxis.set_tick_params(labelsize = 14)
ax.yaxis.set_tick_params(labelsize = 14)
ax.grid(False)


plt.show()

# COMMAND ----------

# set barplot parameters
sns.set_style("darkgrid")
# sns.set(font="Verdana")
ax = sns.barplot(data=data, x='Pickup_Time_Bin2', y='average_speed_kph', errorbar=None)


# set axes, title etc.
ax.figure.set_size_inches(14,8)
ax.set_title('NYC Taxi: average speed (km/h)', size = 24, weight='bold')
ax.set_xlabel('Time of Day (three-hour slots)', size = 14, weight='bold')
ax.set_ylabel('Average speed (km/h)', size = 14, weight='bold')
ax.title.set_size(18)
ax.xaxis.set_tick_params(labelsize = 14)
ax.yaxis.set_tick_params(labelsize = 14)
ax.grid(False)

plt.show()

# COMMAND ----------

# set barplot parameters
sns.set_style("darkgrid")
# sns.set(font="Verdana")
ax = sns.barplot(data=data, x='Pickup_Time_Bin2', y='distance_km', errorbar=None)


# set axes, title etc.
ax.figure.set_size_inches(14,8)
ax.set_title('NYC Taxi: Average ride distance (km)', size = 24, weight='bold')
ax.set_xlabel('Time of Day (three-hour slots)', size = 14, weight='bold')
ax.set_ylabel('Distance (km)', size = 14, weight='bold')
ax.title.set_size(18)
ax.xaxis.set_tick_params(labelsize = 14)
ax.yaxis.set_tick_params(labelsize = 14)
ax.grid(False)

plt.show()

# COMMAND ----------

# set barplot parameters
sns.set_style("darkgrid")
# sns.set(font="Verdana")
ax = sns.barplot(data=data, x='Pickup_Time_Bin2', y='total_amount', errorbar=None)


# set axes, title etc.
ax.figure.set_size_inches(14,8)
ax.set_title('NYC Taxi: Average total cost of ride ($)', size = 24, weight='bold')
ax.set_xlabel('Time of Day (three-hour slots)', size = 14, weight='bold')
ax.set_ylabel('Total price in USD (inc. tips)', size = 14, weight='bold')
ax.title.set_size(18)
ax.xaxis.set_tick_params(labelsize = 14)
ax.yaxis.set_tick_params(labelsize = 14)
ax.grid(False)

plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Popular Pickup locations

# COMMAND ----------

# MAGIC %md
# MAGIC ## Newark Airport

# COMMAND ----------

# DBTITLE 1,Select Newark data
# select Newark data
df_newark = data.loc[data['Dropoff_ID'] == 1]

# group values
df_newark_pickup = df_newark[df_newark['Dropoff_ID'] == 1].groupby(['Pickup_ID']).size().reset_index(name='Pickup_Amount')

# sort values
df_newark_pickup = df_newark_pickup.sort_values(by='Pickup_Amount', ascending = False)

df_newark_pickup.head()

# COMMAND ----------

# DBTITLE 1,Create Pickup_ID_Zone Column
# create new column based on existing ones
data['Pickup_ID_Zone'] = data[['Pickup_ID','Pickup_Zone']].apply(lambda x : '{}_{}'.format(x[0],x[1]), axis=1)

data.head()

# COMMAND ----------

# create list of all pickup locations
pickup_list = data['Pickup_ID_Zone'].unique()

# create new dataframe with empty column
df_all_pickup = pd.DataFrame(columns=["Pickup_ID_Zone"])

# add column values from list
df_all_pickup['Pickup_ID_Zone'] = np.array(pickup_list)

# split column into two columns
df_all_pickup[['Pickup_ID', 'Pickup_Zone']] = df_all_pickup['Pickup_ID_Zone'].str.split('_', expand=True)

# drop column
df_all_pickup = df_all_pickup.drop(columns=['Pickup_ID_Zone'], axis = 1)

# values to integer
df_all_pickup['Pickup_ID'] = df_all_pickup['Pickup_ID'].astype(int)

df_all_pickup.head()

# COMMAND ----------

# DBTITLE 1,Merge dataframes
# merge dataframes
df_newark_map = pd.merge(df_newark_pickup, df_all_pickup, on= 'Pickup_ID',how='left')

# select 20 top rows
df_newark_20 = df_newark_map.iloc[:20,:]

# create list
newark_list = df_newark_20['Pickup_ID'].unique()

df_newark_20.head(10)

# COMMAND ----------

# merge dataframe on location_i column
# outer join creates NaN values (no pickups in that zone)
df_newark_color = pd.merge(df_cab, df_newark_map, on = "Pickup_ID", how = "outer")

# replace NaN values with 0
df_newark_color['Pickup_Amount'] = df_newark_color['Pickup_Amount'].fillna(0)

# convert values to integer
df_newark_color['Pickup_Amount'] = df_newark_color['Pickup_Amount'].astype(int)

# COMMAND ----------

# select Newark data
newark = points.loc[points['Pickup_ID'] == 1]

# create list of Newark pickup location ID numbers
newark_pickup = points[points['Pickup_ID'].isin(newark_list)]

# the lighter the zone color on the NYC city area is, the more pickups it has
# see map legend for more information
ax = df_newark_color.plot(column='Pickup_Amount', figsize=(20, 20), alpha=0.5, edgecolor='k', 
                  cmap='hot', scheme='natural_breaks', categorical = False, k = 4, legend = True)

# most popular pickup spots are marked with black dots
# airport is marked with white dot
newark_pickup.plot(ax=ax, marker='o', color='black', markersize= 100)
newark.plot(ax=ax, marker='o', color='white', markersize= 600)

# set axis, title etc.
ax.set_axis_off();
ax.set(title='Newark airport: taxi pickup locations')
ax.title.set_size(24)

# add basemap in the background
ctx.add_basemap(
    ax,
    crs=df_cab.crs.to_string(),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## JFK Airport

# COMMAND ----------

df_jfk_20.head(10)

# COMMAND ----------

df_jfk = data.loc[data['Dropoff_ID'] == 132]

df_jfk_pickup = df_jfk[df_jfk['Dropoff_ID'] == 132].groupby(['Pickup_ID']).size().reset_index(name='Pickup_Amount')

df_jfk_pickup = df_jfk_pickup.sort_values(by='Pickup_Amount', ascending = False)

df_jfk_map = pd.merge(df_jfk_pickup, df_all_pickup, on= 'Pickup_ID',how='left')

df_jfk_20 = df_jfk_map.iloc[:20,:]

df_jfk_color = pd.merge(df_cab, df_jfk_map, on = "Pickup_ID", how = "outer")

df_jfk_color['Pickup_Amount'] = df_jfk_color['Pickup_Amount'].fillna(0)

df_jfk_color['Pickup_Amount'] = df_jfk_color['Pickup_Amount'].astype(int)

jfk_list = df_jfk_20['Pickup_ID'].unique()

jfk = points.loc[points['Pickup_ID'] == 132]

jfk_pickup = points[points['Pickup_ID'].isin(jfk_list)]

ax = df_jfk_color.plot(column='Pickup_Amount', figsize=(20, 20), alpha=0.5, edgecolor='k', 
                  cmap='hot', scheme='natural_breaks', categorical = False, k = 4, legend = True)
ax.set_axis_off();
ax.set(title='JFK airport: taxi pickup locations')
ax.title.set_size(24)


jfk_pickup.plot(ax=ax, marker='o', color='black', markersize= 100)
jfk.plot(ax=ax, marker='o', color='white', markersize= 600)


ctx.add_basemap(
    ax,
    crs=df_cab.crs.to_string(),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##  LaGuardia Airport

# COMMAND ----------

df_laguardia_20.head(10)

# COMMAND ----------

df_laguardia = data.loc[data['Dropoff_ID'] == 138]

df_laguardia_pickup = df_laguardia[df_laguardia['Dropoff_ID'] == 138].groupby(['Pickup_ID']).size().reset_index(name='Pickup_Amount')

df_laguardia_pickup = df_laguardia_pickup.sort_values(by='Pickup_Amount', ascending = False)

df_laguardia_map = pd.merge(df_laguardia_pickup, df_all_pickup, on= 'Pickup_ID',how='left')

df_laguardia_20 = df_laguardia_map.iloc[:20,:]

df_laguardia_color = pd.merge(df_cab, df_laguardia_map, on = "Pickup_ID", how = "outer")

df_laguardia_color['Pickup_Amount'] = df_laguardia_color['Pickup_Amount'].fillna(0)

df_laguardia_color['Pickup_Amount'] = df_laguardia_color['Pickup_Amount'].astype(int)

laguardia_list = df_laguardia_20['Pickup_ID'].unique()

laguardia = points.loc[points['Pickup_ID'] == 138]

laguardia_pickup = points[points['Pickup_ID'].isin(laguardia_list)]

ax = df_laguardia_color.plot(column='Pickup_Amount', figsize=(20, 20), alpha=0.5, edgecolor='k', 
                  cmap='hot', scheme='natural_breaks', categorical = False, k = 4, legend = True)
ax.set_axis_off();
ax.set(title='LaGuardia airport: taxi pickup locations')
ax.title.set_size(24)

laguardia_pickup.plot(ax=ax, marker='o', color='black', markersize= 100)
laguardia.plot(ax=ax, marker='o', color='white', markersize= 600)

ctx.add_basemap(
    ax,
    crs=df_cab.crs.to_string(),
)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC -- Mest trafikerade timmar
# MAGIC SELECT hour_of_day, COUNT(*) AS trip_count
# MAGIC FROM development.gold.nyc_taxi
# MAGIC GROUP BY hour_of_day
# MAGIC ORDER BY trip_count DESC
# MAGIC --LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- mest inkomstbringande timme
# MAGIC SELECT * FROM development.gold.total_revenue_nyc_taxi
# MAGIC ORDER BY total_revenue DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Upphämtningsplatser med mest intäkter
# MAGIC SELECT g.LocationID, g.Zone, SUM(s.fare_amount) AS total_revenue
# MAGIC FROM development.silver.nyc_taxi s
# MAGIC LEFT JOIN development.gold.dim_taxi_zone_lookup g ON s.pickup_location_id = g.LocationID
# MAGIC GROUP BY g.LocationID, g.Zone
# MAGIC ORDER BY total_revenue DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Upphämtningsplatser med mest intäkter
# MAGIC SELECT pickup_location_id, SUM(fare_amount) AS total_revenue
# MAGIC FROM development.gold.nyc_taxi
# MAGIC GROUP BY pickup_location_id
# MAGIC ORDER BY total_revenue DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Genomsnittligt pris per mil beroende på tid på dygnet
# MAGIC SELECT hour_of_day, AVG(fare_amount / trip_distance) AS avg_price_per_mile
# MAGIC FROM development.gold.nyc_taxi
# MAGIC WHERE trip_distance > 0
# MAGIC GROUP BY hour_of_day
# MAGIC ORDER BY hour_of_day;
# MAGIC