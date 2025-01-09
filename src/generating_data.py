from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format
from datetime import timedelta
from utils import get_start_end_dates, print_num_parts


def run(spark: SparkSession):
    """
    Generates a DataFrame with dates for the current year and adds calculated columns for month and day of the month.

    :param spark: The Spark session used for creating DataFrames.
    :return: A DataFrame containing dates from the start to the end of the current year, with calculated month and
             day columns.
    """
    # Get the start and end dates for the current year
    start_date, end_date = get_start_end_dates()

    # Create a list of all dates within the given range (start_date to end_date)
    dates = [(start_date + timedelta(days=i)) for i in range((end_date - start_date).days)]

    # Convert the list of dates to a DataFrame
    df = spark.createDataFrame([(d,) for d in dates], ["date"])

    # Add calculated columns: 'month' and 'day_of_month'
    df = print_num_parts(
        "Generated DataFrame with date details",
        df.withColumn("month", date_format(col("date"), "MMMM"))
          .withColumn("day_of_month", date_format(col("date"), "dd"))
    )

    return df
