from pyspark.sql.functions import col
from utils import *


def run(df: DataFrame):
    """
    Runs a sequence of JDBC read and write operations on the provided DataFrame.

    This function demonstrates various ways to interact with JDBC sources, including:
    - Writing to JDBC with different batch sizes by date and day_of_month.
    - Reading from JDBC using partitioning by date and day_of_month for scalability.
    - Measuring and logging execution times for each operation.

    :param df: The input DataFrame to be used in JDBC operations.
    """
    # Get the current Spark session
    spark = df.sparkSession

    # Repartition the DataFrame by day of month
    df_rep_by_dom = print_num_parts("Repartition the DataFrame by day of month",
                                    df.repartition(col("day_of_month")))

    # Different batch sizes
    batchsizes = [100, 1_000, 10_000, 25_000]

    # Loop to test different batch sizes
    for batchsize in batchsizes:

        # JDBC write
        print_time(
            f"JDBC write (batchsize={batchsize})",
            lambda: write_df_to_jdbc(df, "dates",
                                     options={"batchsize": batchsize})
        )

        # JDBC write after repartitioning the DataFrame by 'day_of_month'
        print_time(
            f"JDBC write repartitioned by day of month (batchsize={batchsize})",
            lambda: write_df_to_jdbc(df_rep_by_dom, "dates",
                                     options={"batchsize": batchsize})
        )

    # Get the start and end dates for the current year
    start_date, end_date = get_start_end_dates()

    # Different fetch sizes
    fetchsizes = [100, 1_000, 10_000, 25_000]

    # Loop to test different fetch sizes
    for fetchsize in fetchsizes:

        # Options for JDBC read partitioning by 'date'
        jdbc_read_by_date_options = {
            "partitionColumn": "date",
            "lowerBound": str(start_date),
            "upperBound": str(end_date),
            "numPartitions": spark.sparkContext.defaultParallelism,
            "fetchsize": fetchsize
        }

        # Read from JDBC using date-based partitioning
        text = f"JDBC read partitioning by date (fetchsize={fetchsize}"
        print_time(
            text,
            lambda: print_num_parts(
                text,
                read_df_from_jdbc(spark, "dates", options=jdbc_read_by_date_options)
            )
        )

        # Options for JDBC read partitioning by 'day_of_month'
        jdbc_read_by_day_of_month_options = {
            "partitionColumn": "day_of_month",
            "lowerBound": "1",
            "upperBound": "31",
            "numPartitions": spark.sparkContext.defaultParallelism,
            "fetchsize": fetchsize
        }

        # Custom query for reading specific columns and casting types
        query = "(SELECT date, CAST(day_of_month AS INT) FROM dates) AS subq"

        # Read from JDBC using day_of_month-based partitioning
        text = f"JDBC read partitioning by day of month (fetchsize={fetchsize})"
        print_time(
            text,
            lambda: print_num_parts(
                text,
                read_df_from_jdbc(spark, query, options=jdbc_read_by_day_of_month_options)
            )
        )
