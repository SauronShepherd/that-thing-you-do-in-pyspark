import os
from pyspark.sql import DataFrame
from utils import print_num_parts


def run(df: DataFrame) -> DataFrame:
    """
    Performs a series of operations on the given DataFrame:
    - Repeatedly creates a union of the DataFrame with itself and writes it to a specified folder path.
    - Loads the files back into a DataFrame and prints the number of partitions.
    - Counts and prints the number of Parquet files in the folder.

    :param df: The DataFrame to be processed.
    :return: The DataFrame loaded from the files after all operations.
    """
    # Get the current Spark session
    spark = df.sparkSession

    read_files_df = df

    # Loop to perform the operations several times
    num_joins = 5
    for i in range(1, num_joins + 1):
        # Define the folder path where the DataFrame will be written
        folder_path = f"data/df{i}"

        # Perform union with itself and write to the specified folder, overwriting if exists
        read_files_df.union(df).write.mode("overwrite").save(folder_path)

        # Load the written files back into a DataFrame and print the number of partitions
        read_files_df = print_num_parts("Read data from files", spark.read.load(folder_path))

        # Unpersist the cached DataFrame
        read_files_df.unpersist()

        # Count and print the number of Parquet files in the folder
        parquet_files_count = len([f for f in os.listdir(folder_path) if f.endswith(".parquet")])

        # Print the number of Parquet files found in the folder
        print(f"Parquet files count: {parquet_files_count}\n")

    # Set a configuration parameter for maximum partition size
    spark.conf.set("spark.sql.files.maxPartitionBytes", str(18 * 1024))

    # Load the data with the updated configuration and print the number of partitions
    print_num_parts(
        "Read data from files with 18 KB maxPartitionBytes",
        spark.read.load(f"data/df{num_joins}")
    )

    # Return the final DataFrame
    return read_files_df
