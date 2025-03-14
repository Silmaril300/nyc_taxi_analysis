# nyc_taxi_analysis
This is a repo holding databricks notebooks for the analysis of NYC Taxi data. Data used for the analysis comes from the NYC Taxi and Limousine Commission (https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

Below is a figure describing the way data can be loaded into Databricks
![image](https://github.com/user-attachments/assets/7c6df1fc-5245-46fb-bb40-23d4b0930421)


# Data flow:
1. Download the trip data, Taxi Zone lookup data, and Taxi Zone shapefile data from the NYC Taxi and Limousine Commission (https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).
2. Load the data into Unity Catalog in Databricks under the ``default`` schema: trip data and lookup data as ``tables``, shapefile data as ``files``.
3. Run the notebook ``nyc_taxi_etl_first_load`` to create the bronze, silver, and gold Taxi trip data tables. The notebook uses the ``default`` table name you want to load as a parameter.
4. Run the notebook ``nyc_taxi_lookup_etl`` to create the bronze, silver, and gold Taxi Zone lookup data tables. The notebook uses the ``default`` table name you want to load as a parameter.
5. Once the trip and lookup data is in the gold layer, the analysis notebook ``nyc_taxi_analysis`` can be run.
6. You can add new data to the analysis by running the  ETL notebook ``nyc_taxi_etl_views.py``. The notebook takes data uploaded to Unity Catalog in Databricks and transforms it following the medallion architecture. Data is transformed and loaded from Bronze to Silver to Gold using SQL views ``(BronzeViews_NYTaxi.py and SilverViews_NYTaxis.py)``.
7. Setup a workflow to orchestrate a data pipeline to load new data from raw to gold.

