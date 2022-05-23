from pathlib import Path
import argparse
parser = argparse.ArgumentParser()

from .traffic_counter import get_top_half_hours, load_data, total_number_of_cars, \
                                get_cars_per_day, get_period_with_least_cars
from .utils import get_spark_session, print_df_result

def main():
    parser.add_argument('--data', help='Location of the data')
    args = parser.parse_args()

    spark = get_spark_session(__name__)
    df = load_data(spark, Path(args.data))

    print("TOTAL NUMBER OF CARS", total_number_of_cars(df))

    print_df_result(get_cars_per_day(df), "CARS SEEN EACH DAY")

    print_df_result(get_top_half_hours(df,3), "TOP 3 HALF HOURS")

    print_df_result(get_period_with_least_cars(df), "1.5 HOUR PERIOD WITH LEAST CARS")

main()
