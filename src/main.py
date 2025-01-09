import generating_data
import filtering_data
import grouping_data
import joining_data
import writing_reading_files
import writing_reading_jdbc
from utils import create_spark_session, print_section


def main():
    """
    Main entry point for the application.
    """
    # Initialize Spark session
    spark = create_spark_session()

    # Generating Data
    print_section("Generating Data")
    df = generating_data.run(spark)

    # Filtering Data
    print_section("Filtering Data")
    filtered_df, filtered_rep_df, filtered_coa_df = filtering_data.run(df)

    # Grouping Data
    print_section("Grouping Data")
    grouping_data.run(df, filtered_df, filtered_rep_df, filtered_coa_df)

    # Joining Data
    print_section("Joining Data")
    multi_join_df = joining_data.run(df, filtered_coa_df)

    # Writing & Reading using files
    print_section("Writing & Reading using files")
    read_files_df = writing_reading_files.run(multi_join_df)

    # Writing & Reading using JDBC
    print_section("Writing & Reading using JDBC")
    writing_reading_jdbc.run(read_files_df)

    # Stop Spark session
    spark.stop()


if __name__ == "__main__":
    main()
