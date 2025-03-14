# nyc_taxi_analysis
This is a repo holding databricks notebooks for the analysis of NYC Taxi data. Data used for the analysis comes from the NYC Taxi and Limousine Commission (https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

Below is a figure describing the way data can be loaded into Databricks
![image](https://github.com/user-attachments/assets/29bb1fab-245e-4f1b-b787-1780b14ecc73)

The data pipeline consists of a databricks workflow running the ETL notebook ``nyc_taxi_etl_views.py``. The notebook takes data uploaded to Unity Catalog in Databricks and transforms it following the medallion architecture.
