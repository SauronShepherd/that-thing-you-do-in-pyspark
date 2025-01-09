from pyspark.sql import DataFrame
from utils import print_num_parts


def run(df: DataFrame, filtered_df: DataFrame,
        filtered_rep_df: DataFrame, filtered_coa_df: DataFrame) -> None:
    """
    Processes and prints the number of partitions for grouped DataFrames.

    :param df: The original DataFrame containing all data.
    :param filtered_df: The DataFrame filtered for specific criteria.
    :param filtered_rep_df: The repartitioned and filtered DataFrame.
    :param filtered_coa_df: The coalesced and filtered DataFrame.
    """
    print_num_parts("Count grouped by month (all data)",
                    df.groupBy("month").count())
    print_num_parts("Count grouped by month (filtered data)",
                    filtered_df.groupBy("month").count())
    print_num_parts("Count grouped by month (filtered data repartitioned)",
                    filtered_rep_df.groupBy("month").count())
    print_num_parts("Count grouped by month (filtered data coalesced)",
                    filtered_coa_df.groupBy("month").count())
