# nyc_taxi_trips

NYC Taxi Trip Analysis Report

<img width="694" height="461" alt="Screen Shot 2026-08-05 at 3 29 59 PM" src="https://github.com/user-attachments/assets/b6f09f04-4add-4577-8fec-5104937c723a" />

# Overview:

This project utilizes Apache Spark to process and analyze massive New York City Yellow taxi trip datasets. 

It extracts driving trends, revenue optimizations, and temporal demand patterns using distributed computing.

This project analyzes trip records to uncover travel patterns, peak demand hours, fare distributions, and spatial hotspots.

# DatasetSource: NYC Taxi and Limousine Commission (TLC) Trip Record Data.

Content: Pickup/drop-off timestamps, trip distances, itemized fares, rate types, and passenger counts.

Format: Parquet files partitioned by year and month.

Volume: Millions of rows per month.

# Key Features Analyzed

- Temporal trends (hourly, daily, and seasonal ride counts).
- Geographic hot spots (top pickup and drop-off zones).
- Fare and tip amount correlations with distance and time of day.
- Key Performance Indicators (KPIs): Average speed per hour across boroughs to identify traffic bottlenecks.High-value tipping zones based on drop-off pickup combinations.Utilization rates of drivers during shift handovers.

# Data Pipeline Steps

1. Ingestion: Reading raw Parquet files from public S3 buckets into Spark DataFrames.
2. Cleansing: Filtering out anomalies (e.g., negative trip distances, zero fares, invalid coordinates).
3. Aggregation: Using Spark SQL window functions to calculate peak hourly demand and spatial hot spots.
4. Storage: Writing optimized analytical views back to storage, partitioned by pickup_date.
