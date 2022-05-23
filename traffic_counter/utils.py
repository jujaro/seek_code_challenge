from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

import datetime


def _format(val)->str:
    if isinstance(val, str):
        return str(val)
    elif isinstance(val, datetime.date):
        return str(val)
    elif isinstance(val, datetime.datetime):
        return val.isoformat()
    else:
        return str(val)

def get_spark_session(appName:str)->SparkSession:
    return (
        SparkSession.builder
        .master("local[1]")
        .appName(appName)
        .getOrCreate()
    )

def print_df_result(df:DataFrame, heading:str):
    print()
    print(heading)
    for row in df.collect():
        print(" ".join([_format(v) for v in row]))
    print()