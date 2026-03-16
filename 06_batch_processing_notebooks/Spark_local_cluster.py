#!/usr/bin/env python
# coding: utf-8
import argparse

import pandas as pd
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types
from pyspark.sql.functions import col

parser = argparse.ArgumentParser()

parser.add_argument('--input_green', required=True)
parser.add_argument('--input_yellow', required=True)
parser.add_argument('--output', required=True)

args = parser.parse_args()

input_green = args.input_green
input_yellow = args.input_yellow
output = args.output


pyspark.__file__

spark = SparkSession.builder \
        .appName('test') \
        .getOrCreate()

spark

df_green = \
    spark.read \
    .option('header', 'true') \
    .option('recursiveFileLookup', 'true') \
    .parquet(input_green)

df_yellow = \
    spark.read \
    .option('header', 'true') \
    .option('recursiveFileLookup', 'true') \
    .parquet(input_yellow)


# normalizing green columns: 
df_green = df_green \
    .withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime') \
    .withColumn('taxi_type', F.lit('green')) \
    .select(
        'VendorID', 
        'pickup_datetime', 
        'dropoff_datetime', 
        'store_and_fwd_flag',
        'RatecodeID',
        'PULocationID',
        'DOLocationID',
        'passenger_count',
        'trip_distance',
        'fare_amount',
        'extra',
        'mta_tax',
        'tip_amount',
        'tolls_amount',
        'ehail_fee',
        'improvement_surcharge',
        'total_amount',
        'payment_type',
        'trip_type',
        'congestion_surcharge',
        'taxi_type'
    )


# normalizing yellow columns: 
df_yellow = df_yellow \
    .withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime') \
    .withColumn('taxi_type', F.lit('yellow')) \
    .select(
        'VendorID', 
        'pickup_datetime', 
        'dropoff_datetime', 
        'passenger_count',
        'trip_distance',
        'RatecodeID',
        'store_and_fwd_flag',
        'PULocationID',
        'DOLocationID',
        'payment_type',
        'fare_amount',
        'extra',
        'mta_tax',
        'tip_amount',
        'tolls_amount',
        'improvement_surcharge',
        'total_amount',
        'congestion_surcharge', 
        'taxi_type'
    )


commons_cols = []

for col_green in df_green.columns:
    for col_yellow in df_yellow.columns:

        if col_green == col_yellow: 
            commons_cols.append(col_green)

        else: 
            None

commons_cols


df_green = df_green.select(commons_cols)
df_yellow = df_yellow.select(commons_cols)

yellow_green_dataset_unioned = df_yellow.union(df_green)

df_trips_data_consolidated = \
    yellow_green_dataset_unioned \
    .withColumn('revenue_month', F.date_trunc('month', yellow_green_dataset_unioned.pickup_datetime)) \
    .select(
        #Grouping columns: 
        yellow_green_dataset_unioned.PULocationID.alias('pickup_zone_ID'), 
        F.col('revenue_month'), 

        #Aggregations columns: 
        yellow_green_dataset_unioned.total_amount, 
        yellow_green_dataset_unioned.PULocationID
    ) \
    .filter(F.col('revenue_month') >= '2019-01-01 00:00:00') \
    .groupBy(
        F.col('pickup_zone_ID'), 
        F.col('revenue_month')
    ) \
    .agg(
        F.sum(yellow_green_dataset_unioned.total_amount).alias('Total_revenue_monthly_yellow'), 
        F.count(yellow_green_dataset_unioned.PULocationID).alias('Total_trips_monthly_yellow')
    ) \
    .orderBy(
        F.desc(F.col('Total_revenue_monthly_yellow'))
    )

df_trips_data_consolidated \
    .coalesce(1) \
    .write.parquet(output, 
                   mode= 'overwrite'
                  )



