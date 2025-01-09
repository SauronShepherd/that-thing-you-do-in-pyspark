from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from datetime import date


def create_spark_session(config: dict = None) -> SparkSession:
    """
    Creates a SparkSession with the provided configuration.

    :param config: A dictionary of configuration options.
    :return: A SparkSession instance.
    """
    if config is None:
        config = {}

    config |= {
        "spark.driver.memory": "2G",
        "spark.jars.packages": "org.postgresql:postgresql:42.7.4"
    }

    builder = SparkSession.builder.master("local[*]")
    for key, value in config.items():
        builder = builder.config(key, value)

    return builder.getOrCreate()


def get_start_end_dates() -> tuple[date, date]:
    """
    Calculates the start and end dates for the current year.

    :return: A tuple containing:
             - The start date (January 1st of the current year).
             - The end date (January 1st of the next year).
    """
    current_year = date.today().year
    start_date = date(current_year, 1, 1)
    end_date = date(current_year + 1, 1, 1)
    return start_date, end_date


def print_section(text: str):
    """
    Prints a section title within a decorated box for better visibility.

    :param text: The title text.
    """
    box_char = '*'
    border = box_char * (len(text) + 4)
    print(f"\n{border}\n{box_char} {text} {box_char}\n{border}")


def print_num_parts(text: str, df: DataFrame) -> DataFrame:
    """
    Prints the number of partitions and row count of a DataFrame.

    :param text: Description of the DataFrame.
    :param df: The DataFrame to analyze.
    :return: The input DataFrame (unchanged).
    """
    num_partitions = df.rdd.getNumPartitions()
    row_count = df.cache().count()
    print(f"{text} => numPartitions: {num_partitions} - rowCount: {row_count}")
    return df


def print_time(text: str, func, *args, **kwargs):
    """
    Measures and prints the execution time of a block of code.

    :param text: Description of the code block.
    :param func: The callable to execute.
    :param args: Positional arguments for the callable.
    :param kwargs: Keyword arguments for the callable.
    """
    import time
    start_time = time.time()
    func(*args, **kwargs)
    end_time = time.time()
    duration_seconds = end_time - start_time
    print(f"{text} => Executed in {duration_seconds:.2f} s")


def jdbc_options(dbtable: str, options: dict = None) -> dict:
    """
    Generates JDBC connection options, merging default values with provided ones.

    :param dbtable: The table name to connect to.
    :param options: Additional connection options.
    :return: A dictionary of JDBC connection options.
    """
    if options is None:
        options = {}

    default_options = {
        "url": "jdbc:postgresql://localhost:5432/db",
        "user": "ps",
        "password": "ps",
        "driver": "org.postgresql.Driver",
        "dbtable": dbtable
    }
    return {**default_options, **options}


def write_df_to_jdbc(df: DataFrame, dbtable: str, options: dict = None):
    """
    Writes a DataFrame to a JDBC destination.

    :param df: The DataFrame to write.
    :param dbtable: The destination table name.
    :param options: Additional write options.
    """
    df.write.format("jdbc") \
        .mode("overwrite") \
        .options(**jdbc_options(dbtable, options)) \
        .save()


def read_df_from_jdbc(spark: SparkSession, dbtable: str, options: dict = None) -> DataFrame:
    """
    Reads a DataFrame from a JDBC source.

    :param spark: The SparkSession instance.
    :param dbtable: The source table name.
    :param options: Additional read options.
    :return: The DataFrame read from JDBC.
    """
    return spark.read.format("jdbc") \
        .options(**jdbc_options(dbtable, options)) \
        .load()
