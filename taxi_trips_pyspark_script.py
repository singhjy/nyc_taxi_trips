#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os


# In[2]:


os.environ["JAVA_HOME"] = "/Library/Java/JavaVirtualMachines/jdk-17.jdk/Contents/Home"
os.environ['SPARK_HOME'] = "/Users/jyotisingh/Downloads/spark-3.5.9-bin-hadoop3"
os.environ['PYSPARK_PYTHON'] = 'python'
os.environ['PYSPARK_DRIVER_PYTHON'] = 'jupyter'


# In[3]:


from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col


# In[4]:


from pyspark.sql.functions import col, row_number, rank, dense_rank, lag, lead, sum, avg, ntile, percent_rank
from pyspark.sql.window import Window


# In[5]:


spark = SparkSession.builder.master("local").appName("ReadParquet").getOrCreate()


# In[6]:


data = [("Alice",25), ("Bob",30), ("Charlie",35)]


# In[7]:


df = spark.createDataFrame(data, ["Name","Age"])


# In[8]:


df.show()


# yellow_tripdata_2026-01.parquet
# 
# To read a Parquet file in PySpark, use the **spark.read.parquet() method**, which loads the data directly into a DataFrame.

# In[65]:


# Read a Parquet file
trips = spark.read.parquet("/Users/jyotisingh/Downloads/yellow_tripdata_2026-01.parquet") 
#\
           #  .option('header', True)


# In[66]:


trips.createOrReplaceTempView("trips_table")


# In[67]:


spark.sql("""
    SELECT *
    FROM trips_table
    LIMIT 5
""").show(truncate=False)


# In[68]:


spark.sql("""
    SELECT tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, payment_type, fare_amount, tip_amount
    FROM trips_table
    LIMIT 5
""").show()


# In[15]:


type(trips)


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# PULocationID: The specific TLC taxi zone ID where the taximeter was turned on for pickup.
# 
# DOLocationID: The specific TLC taxi zone ID where the taximeter was turned off for drop-off.

# In[69]:


trips.show(1, truncate=False)


# In[70]:


trips.printSchema()


# In[83]:


from pyspark.sql.functions import current_date, current_timestamp, col, unix_timestamp


# In[72]:


# Selecting a particular column from the dataframe. | 2026-01-01 00:54:04|
trips.select(current_date().alias("date")) #Returns the System date as DateType (yyyy-MM-dd)

#df.select(current_date().alias("date"), current_timestamp().alias("ts"))
#current_timestamp(): returns system timestamp as TimestampType


# In[73]:


trips_subset = trips.select(["tpep_pickup_datetime","tpep_dropoff_datetime","passenger_count","trip_distance", "payment_type", "fare_amount","tip_amount","total_amount"])


# In[74]:


trips_subset.show(3)


# In[75]:


trips_subset.dtypes


# In[76]:


#trips_new = trips_subset.withColumn("extracted_date", F.to_date("tpep_pickup_datetime")) \
#                        .withColumn("extracted_hour", F.hour("tpep_pickup_datetime")) \
#                        .withColumn("extracted_time", F.date_format("tpep_pickup_datetime", "HH:mm:ss")) \
#                        .withColumn("extracted_day_name", F.date_format("tpep_pickup_datetime", "EEEE"))

trips_new = trips_subset.withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))          .withColumn("dropoff_date", F.to_date("tpep_dropoff_datetime"))          .withColumn("pickup_timestamp", F.date_format("tpep_pickup_datetime", "HH:mm:ss"))          .withColumn("dropoff_timestamp", F.date_format("tpep_dropoff_datetime", "HH:mm:ss"))          .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))          .withColumn("dropoff_hour", F.hour("tpep_dropoff_datetime"))          .withColumn("extracted_day_name", F.date_format("tpep_pickup_datetime", "EEEE"))

#.withColumn("extracted_weekday_name", F.date_format("tpep_pickup_datetime","EEEE")) \ 

#F.date_format("timestamp_column_name", "what_you_want_to_extract") to get names like Monday, Tuesday.

#.withColumn("extracted_day", F.date_format("tpep_pickup_datetime", "E"))


# In[77]:


type(trips_new)


# Because PySpark DataFrames are immutable, transformation operations like select() **never alter your original data frame.**
# 
# Instead, they instantly yield a completely new DataFrame containing only your specified columns.

# In[41]:


trips_new.show(2)


# In[78]:


# Using agg is highly recommended because it allows you to run multiple aggregations at once
# easily rename (alias) your output columns.

# Aggregation by date

trips_grouped_by_date = trips_new.groupBy("pickup_date")                                  .agg(F.round(F.sum("total_amount"),2).alias("cal_trip_amount"))                                  .orderBy("pickup_date")                                  .show()


# In[79]:


# Aggregation by Date and Hour
trips_grouped_by_date_hour = trips_new.groupBy(["pickup_date","pickup_hour"])                                  .agg(F.round(F.sum("total_amount"),2)                                  .alias("cal_trip_amount_hourly"))                                  .orderBy(["pickup_date","pickup_hour"])                                  .show(100)


# In[84]:


# Create a new Feature Trip Duration.
trips_subset.show(1)


# In[86]:


# If timestamps need calculation (or compare with existing trip_duration)
df_duration = trips_subset.withColumn("trip_duration_secs",
    unix_timestamp(col("tpep_dropoff_datetime")) - unix_timestamp(col("tpep_pickup_datetime"))
)

df_duration.select("tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_duration_secs").show(5)

#trips_new.withColumn("trip_duration_mins", ((F.col("dropoff_timestamp").cast("long") - F.col("pickup_timestamp").cast("long"))/60))  \
#         .select(["trip_duration_mins","tpep_pickup_datetime","tpep_dropoff_datetime", "pickup_timestamp", "dropoff_timestamp"]) \
#         .show(12)

#trips_subset.withColumn("trip_duration_mins", (((F.col("dropoff_timestamp").cast("long"))- (F.col("pickup_timestamp").cast("long"))) / 60)) \
#         .select(["trip_duration_mins","tpep_pickup_datetime","tpep_dropoff_datetime", "pickup_timestamp", "dropoff_timestamp"]) \
#         .show(4)

#df.withColumn(
#    "diff_minutes", 
#    (F.col("end_timestamp").cast("long") - F.col("start_timestamp").cast("long")) / 60
#

# Syntax: timestamp_diff(unit, start_column, end_column)
#df_diff = df.withColumn("minutes_diff", F.timestamp_diff("MINUTE", F.col("start_time"), F.col("end_time")))

#df_days = df.withColumn("days_diff", F.datediff(F.col("end_time"), F.col("start_time")))

#this approach subtracts the start time from the end time in seconds and converts it to minutes.


# In[ ]:


# If timestamps need calculation (or compare with existing trip_duration)
df_duration = df.with-column(
    "calculated_duration_secs",
    unix_timestamp(col("dropoff_datetime")) - unix_timestamp(col("pickup_datetime"))
)

df_duration.select("pickup_datetime", "dropoff_datetime", "calculated_duration_secs").show(5)


# In[87]:


spark.stop()


# In[ ]:





# If you need specific individual components instead of a formatted time string, you can use individual PySpark functions:
# 
# 
#  - Extracting Date Only: **F.to_date(col)** returns a DateType column.
#  
#  
#  - Extracting Year: F.year(col).
#  - Extracting Month: F.month(col).
#  - Extracting Day: F.dayofmonth(col).
#  - Extracting Hour: F.hour(col).
#  - Extracting Minute: F.minute(col).
#  - Extracting Second: F.second(col).

# In[112]:


# CODING EXAMPLE

# Sample data with a timestamp
sample_date = [("2026-07-29 16:19:45.123",)]
df = spark.createDataFrame(sample_date, ["timestamp_str"])

# Convert string to an actual TimestampType column
df = df.withColumn("timestamp_col", F.to_timestamp("timestamp_str"))

# Extract Date and Time components
df_extracted = df.withColumn("extracted_date", F.to_date("timestamp_col"))                  .withColumn("extracted_weekday", F.weekday("timestamp_col"))                  .withColumn("extracted_weekday_name", F.date_format("timestamp_col","EEEE"))                  .withColumn("extracted_time", F.date_format("timestamp_col", "HH:mm:ss"))

df_extracted.select("timestamp_col", "extracted_date", "extracted_time", "extracted_weekday", "extracted_weekday_name").show(truncate=False)


# In[36]:


trips.dtypes # returns a list of tuples containing (column_name, data_type)


# In[ ]:





# In[ ]:


# Create a new Feature - date column.

# Create a new Feature - timestamp column.

# Calculate Total fare amount for the month.

# Also, calculate the fare_amount for each date.

# Also, calculate the fare_amount for each day of the week.

# Create a new Feature Trip Duration.


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# **select**: transformation method used to project and extract specific columns from a DataFrame.

# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[18]:


trips.count()


# In[19]:


column_names = trips.columns


# In[20]:


column_names


# In[121]:


spark.stop()


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




