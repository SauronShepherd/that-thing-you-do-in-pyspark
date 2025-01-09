from pyspark.sql import DataFrame
from pyspark.sql.functions import expr
from utils import print_num_parts


def run(df: DataFrame, filtered_coa_df: DataFrame) -> DataFrame:
    """
    Performs various join operations on the provided DataFrames and returns a final DataFrame.

    - Joins two copies of the data on a date condition.
    - Performs multiple broadcast joins with coalesced DataFrame.

    :param df: The original DataFrame to perform the join operations on.
    :param filtered_coa_df: The coalesced DataFrame used for concatenating multiple joins.
    :return: A DataFrame resulting from the concatenation of the join operations.
    """
    # Define the join condition: start date must be less than end date
    on_condition = expr("start.date < end.date")

    # Alias the data_df for the start and end DataFrames
    start_df = df.alias("start")
    end_df = df.alias("end")

    # Print the number of partitions after performing an inner join without broadcasting
    print_num_parts("Inner join", start_df.join(end_df, on_condition))

    # Define the range of indexes for the multiple broadcast joins
    indexes = range(2, 4)

    # Prepare the first DataFrame to join by coalescing it into a single partition and renaming the 'month' column
    first_df = filtered_coa_df.coalesce(1).withColumnRenamed("month", "month1").alias("df1")

    # Coalesce the original DataFrame into one partition to prepare it for joining
    one_partition_df = df.coalesce(1)

    # Perform a series of joins using a loop to concatenate joins on the 'month' column
    multi_join_df = first_df
    for index in indexes:
        prev_index = index - 1

        # Perform the join between the current DataFrame and one_partition_df on the 'month' column
        multi_join_df = print_num_parts(
            "Multiple broadcast joins",
            multi_join_df.alias("start").join(
                one_partition_df.alias("end"),
                expr(f"start.month{prev_index} <= end.month")
            ).select(multi_join_df["*"], one_partition_df["month"].alias(f"month{index}"))
        )

    # Return the final DataFrame after performing all joins
    return multi_join_df
