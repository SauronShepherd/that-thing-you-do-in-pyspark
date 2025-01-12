from pyspark.sql import DataFrame
from pyspark.sql.functions import expr
from utils import print_num_parts, print_time
from itertools import product


def run(df: DataFrame, filtered_df: DataFrame,
        filtered_rep_df: DataFrame, filtered_coa_df: DataFrame) -> DataFrame:
    """
    Performs various join operations on the provided DataFrames and returns a final DataFrame.

    - Joins two copies of the data on a date condition.
    - Performs multiple broadcast joins with coalesced DataFrame.

    :param df: The original DataFrame to perform the join operations on.
    :param filtered_df: The DataFrame filtered for specific criteria.
    :param filtered_rep_df: The repartitioned and filtered DataFrame.
    :param filtered_coa_df: The coalesced and filtered DataFrame.
    :return: A DataFrame resulting from the concatenation of the join operations.
    """

    def perform_joins(threshold: str, dfs: dict[str, DataFrame]):
        """
        Perform joins between all combinations of input DataFrames based on a specific join condition.

        Args:
            threshold (str): The current value of spark.sql.autoBroadcastJoinThreshold.
            dfs (dict): A dictionary of DataFrames to join, with descriptive keys.

        Returns:
            list: A list of DataFrames resulting from the join operations.
        """

        # Define the join condition: start date must be less than end date
        on_condition = expr("start.date < end.date")

        # Generate all combinations of the input DataFrames for pairwise joins
        combinations = list(product(dfs.items(), repeat=2))

        result_dfs = []
        for (start_key, start_df), (end_key, end_df) in combinations:
            # Log the join operation being performed with the current autoBroadcastJoinThreshold
            text = f"Joining start_df({start_key}) with end_df({end_key}) with autoBroadcastJoinThreshold={threshold}"

            # Perform the join operation with aliases for clarity
            result_df = start_df.alias("start").join(end_df.alias("end"), on_condition)

            # Measure and log the time taken for the join, and append the result to the list
            result_dfs.append(print_time("", lambda: print_num_parts(text, result_df)))

        return result_dfs

    # Define input DataFrames with descriptive keys
    input_dfs = {
        "all data": df,
        "filtered": filtered_df,
        "filtered + repartitioned": filtered_rep_df,
        "filtered + coalesced": filtered_coa_df
    }

    # Retrieve the Spark session from one of the DataFrames
    spark = df.sparkSession

    # Get the current value of the autoBroadcastJoinThreshold
    auto_broadcast_join_threshold = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")

    # Perform joins using the current autoBroadcastJoinThreshold
    join_dfs = perform_joins(auto_broadcast_join_threshold, input_dfs)

    # Unpersist all intermediate join results to free up memory
    for join_df in join_dfs:
        join_df.unpersist()

    # Temporarily set the autoBroadcastJoinThreshold to "-1" (disable autobroadcast)
    df.sparkSession.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

    # Perform joins with the broadcast threshold disabled
    perform_joins("-1", input_dfs)

    # Restore the original autoBroadcastJoinThreshold value
    df.sparkSession.conf.set("spark.sql.autoBroadcastJoinThreshold", auto_broadcast_join_threshold)

    # Prepare the first DataFrame to join by coalescing it into a single partition and renaming the 'month' column
    first_df = filtered_coa_df.withColumnRenamed("month", "month1").alias("df1")

    # Coalesce the original DataFrame into one partition to prepare it for joining
    one_partition_df = df.coalesce(1)

    # Perform a series of chained joins using a loop to concatenate results based on the 'month' column
    multi_join_df = first_df
    for index in range(2, 4):
        prev_index = index - 1

        # Log and perform the join operation between the current result and one_partition_df
        multi_join_df = print_num_parts(
            "Multiple broadcast joins",
            multi_join_df.alias("start").join(
                one_partition_df.alias("end"),
                expr(f"start.month{prev_index} <= end.month")  # Join condition on sequential month columns
            ).select(multi_join_df["*"], one_partition_df["month"].alias(f"month{index}"))
            # Add the new month column to the result
        )

    # Return the final DataFrame after performing all joins
    return multi_join_df
