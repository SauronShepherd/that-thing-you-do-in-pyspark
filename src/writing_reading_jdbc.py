from pyspark.sql.functions import col
from utils import *


def run(df: DataFrame):
    """
    Runs a sequence of JDBC read and write operations on the provided DataFrame.

    This function demonstrates various ways to interact with JDBC sources, including:
    - Writing to JDBC with different batch sizes and repartitioning strategies.
    - Reading from JDBC using partitioning by date and day_of_month for scalability.
    - Measuring and logging execution times for each operation.

    :param df: The input DataFrame to be used in JDBC operations.
    """
    # Get the current Spark session
    spark = df.sparkSession

    # JDBC write with default batch size of 1000
    print_time(
        "JDBC write using batchSize=1000 (default)",
        lambda: write_df_to_jdbc(df, "dates")
    )

    # JDBC write with a larger batch size of 10000
    print_time(
        "JDBC write using batchSize=10000",
        lambda: write_df_to_jdbc(df, "dates", options={"batchSize": "10000"})
    )

    # JDBC write after repartitioning the DataFrame by 'day_of_month'
    print_time(
        "JDBC write repartitioning by day_of_month",
        lambda: write_df_to_jdbc(df.repartition(col("day_of_month")), "dates")
    )

    # Get the start and end dates for the current year
    start_date, end_date = get_start_end_dates()

    # Options for JDBC read partitioning by 'date'
    jdbc_read_by_date_options = {
        "partitionColumn": "date",
        "lowerBound": str(start_date),
        "upperBound": str(end_date),
        "numPartitions": spark.sparkContext.defaultParallelism
    }

    # Read from JDBC using date-based partitioning
    print_time(
        "",
        lambda: print_num_parts(
            "JDBC read partitioning by date",
            read_df_from_jdbc(spark, "dates", options=jdbc_read_by_date_options)
        )
    )

    # Options for JDBC read partitioning by 'day_of_month'
    jdbc_read_by_day_of_month_options = {
        "partitionColumn": "day_of_month",
        "lowerBound": "1",
        "upperBound": "31",
        "numPartitions": spark.sparkContext.defaultParallelism
    }

    # Custom query for reading specific columns and casting types
    query = "(SELECT date, CAST(day_of_month AS INT) FROM dates) AS subq"

    # Read from JDBC using day_of_month-based partitioning
    print_time(
        "",
        lambda: print_num_parts(
            "JDBC read partitioning by day_of_month",
            read_df_from_jdbc(spark, query, options=jdbc_read_by_day_of_month_options)
        )
    )
