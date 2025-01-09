from pyspark.sql import DataFrame
from pyspark.sql.functions import lit
from utils import print_num_parts


def run(df: DataFrame) -> tuple[DataFrame, DataFrame, DataFrame]:
    """
    Filters the DataFrame based on the "January" month and prints the number of partitions
    after applying different partitioning strategies (no change, repartition, coalesce).

    :param df: The original DataFrame to filter.
    :return: A tuple containing:
             - A DataFrame filtered without changing partitioning.
             - A DataFrame filtered after repartitioning by "month".
             - A DataFrame filtered after coalescing to a single partition.
    """
    # Define a filter condition to select rows where the month is "January"
    month_filter = df["month"] == lit("January")

    # Apply filtering without changing partitioning and print the number of partitions
    jan_df = print_num_parts(
        "Filter without changing partitioning",
        df.filter(month_filter)
    )

    # Apply repartitioning by the "month" column and then filter, printing the number of partitions
    jan_rep_df = print_num_parts(
        "Repartition and then filter",
        df.repartition("month").filter(month_filter)
    )

    # Apply coalescing to a single partition and then filter, printing the number of partitions
    jan_coa_df = print_num_parts(
        "Coalesce and then filter",
        df.coalesce(1).filter(month_filter)
    )

    # Return the filtered DataFrames as a tuple
    return jan_df, jan_rep_df, jan_coa_df
