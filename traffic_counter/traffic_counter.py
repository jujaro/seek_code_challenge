from pathlib import Path

import pyspark.sql.types as T
import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

def get_schema()->T.StructType:
    return T.StructType([
            T.StructField("timestamp",T.TimestampType()),
            T.StructField("counts",T.IntegerType())
        ])

def load_data(spark: SparkSession, data_path:Path)->DataFrame:
    return (
        spark
        .read
        .schema(get_schema())
        .csv(data_path.as_posix(), sep=" ")
    )

def total_number_of_cars(df: DataFrame)->int:
    return (
        df
        .agg(F.sum(F.col("counts")))
        .collect()[0][0]
    )


def get_cars_per_day(df:DataFrame)->DataFrame:
    return (
    df
    .withColumn("date",F.to_date(F.col("timestamp")))
    .groupby("date")
    .agg(F.sum(F.col("counts")).alias('daily_counts'))
    .orderBy("date")
    )

def get_top_half_hours(df:DataFrame, limit:int)->DataFrame:
    return (
    df
    .orderBy(F.col("counts").desc())
    .limit(limit)
    )

def get_period_with_least_cars(df:DataFrame)->DataFrame:

    # To establish the 1.5 hour ranges we assign a number to it which is consecutive
    # obtained from the unix_timestamp (half_hour_counter). Then we aggregate the counts in a window
    # over that consecutive.
    # We also keep the number of records on each range since not all the records are consecutive.
    window = Window.orderBy(F.col("half_hour_counter")).rangeBetween(-1, 1)
    sdf1 = (
        df
        .withColumn("half_hour_counter", F.unix_timestamp(F.col("Timestamp"))/(60 * 30))
        .withColumn("range_veh_count", F.sum(F.col("counts")).over(window))
        .withColumn("range_record_count", F.count(F.col("counts")).over(window))
    )

    # We find the first and last records (by the half_hour_counter) to remove them
    # from the result since they will never be in the middle of complete 1.5 hour ranges.
    min_half_hour, max_half_hour = sdf1.agg(
        F.min(F.col('half_hour_counter')),
        F.max(F.col('half_hour_counter'))
        ).collect()[0]

    # Now find the half_hour_counter of the 3 record period with the least cars.
    half_hour_counter_min_range_veh_count = (
        sdf1
        .where(F.col('half_hour_counter')!=min_half_hour)
        .where(F.col('half_hour_counter')!=max_half_hour)
        .where(F.col('range_record_count')==3)
        .orderBy("range_record_count")
        .limit(1)
        .select("half_hour_counter")
    ).collect()[0][0]

    # Use the number above to filter the records to find only the period that we are
    # interested in
    return (
        sdf1
        .where(F.col("half_hour_counter") >= half_hour_counter_min_range_veh_count - 1)
        .where(F.col("half_hour_counter") <= half_hour_counter_min_range_veh_count + 1)
        .select("timestamp", "counts")
    )