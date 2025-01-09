import os
from pyspark.sql import DataFrame, SparkSession
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

    # Loop to perform the operations 5 times
    for i in range(1, 6):
        # Define the folder path where the DataFrame will be written
        folder_path = f"data/df{i}"

        # Perform union with itself and write to the specified folder, overwriting if exists
        read_files_df.union(df).write.mode("overwrite").save(folder_path)

        # Load the written files back into a DataFrame and print the number of partitions
        read_files_df = print_num_parts("Read data from files", spark.read.load(folder_path))

        # Count and print the number of Parquet files in the folder
        parquet_files_count = len([f for f in os.listdir(folder_path) if f.endswith(".parquet")])

        # Print the number of Parquet files found in the folder
        print(f"Parquet files count: {parquet_files_count}\n")

    # Set a configuration parameter for maximum partition size
    spark.conf.set("spark.sql.files.maxPartitionBytes", "18432")  # 18 KB max partition size

    # Load the data with the updated configuration and print the number of partitions
    print_num_parts(
        "Read data from files with 18 KB maxPartitionBytes",
        spark.read.load("data/df5")
    )

    # Stop the current Spark session
    spark.stop()

    # Create a new Spark session with custom configuration for maxPartitionBytes
    spark = SparkSession.builder.master("local[*]").config("spark.sql.files.maxPartitionBytes", "18432").getOrCreate()

    # Read the data again with the new session and print the number of partitions
    read_files_df = print_num_parts(
        "Read data from files with 18 KB maxPartitionBytes (new Spark session)",
        spark.read.load("data/df5")
    )

    # Return the final DataFrame
    return read_files_df
