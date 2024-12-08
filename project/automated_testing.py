import os
import sqlite3
import unittest
import pandas as pd
from automated_datapipeline import main, download_and_extract_in_memory, load_and_clean_data, \
    transform_gun_violence_data, transform_state_gdp_data, transform_state_gdp_per_capita


def check_num_cols(df, num):
    """
    Checks if the number of columns in a DataFrame matches the expected number of columns.
    Args:
        df (pandas.DataFrame): Used to check the DataFrame.
        num (int): Specifies expected number of columns.
    Raises:
        AssertionError: If the number of columns doesn't match the expected number.
    """
    assert df.shape[1] == num, f"Number of columns should be {num}"


def check_column_names(df, expected_column_names):
    """
    Checks if the column names in a DataFrame matches the expected column names.
    Args:
        df (pandas.DataFrame): Used to check the DataFrame.
        expected_column_names (list of str): The expected column names list in the DataFrame.
    Raises:
        AssertionError: If the column names don't match the expected names.
    """
    for x, y in zip(expected_column_names, df.columns):
        assert x == y, f"Column name incorrect: {y} instead of {x}"


def check_null_values(df, cols):
    """
    Checks if any of the specified columns in a DataFrame contains a null values.

    Args:
        df (pandas.DataFrame): Used to check the DataFrame.
        cols (list of str): The names of the columns to check for null values.
    Raises:
        AssertionError: If any of the specified columns contain null values.
    """
    for col in cols:
        # Allow missing values in the 'GeoName' column
        if col == 'GeoName':  # An acceptation is made for the column GeoName
            continue
        assert not df[col].isna().any(), f"Column {col} contains null values"


def read_sql_table(db_path, table_name):
    """
    Reads a table from an SQLite database into a pandas DataFrame.
    Args:
        db_path (str): The path to the SQLite database file.
        table_name (str): The name of the table to read.
    Returns:
        pandas.DataFrame: The DataFrame containing the table data.
    """
    query = f"SELECT * FROM {table_name}"
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df


def check_gun_violence_data(gun_violence_df):
    """
    Checks the structure and integrity of a DataFrame containing wildfire data.
    Args:
        gun_violence_df (pandas.DataFrame): The DataFrame containing gun violence data.
    Raises:
        AssertionError: If the DataFrame fails any of the integrity checks.
    """
    check_num_cols(gun_violence_df, 13)
    gun_violence_expected_columns = ['Date', 'State', 'City/County', 'Address', 'No_of_Killed',
                                     'No_of_Injured', 'Congressional_District', 'Incident_Characteristics',
                                     'No_Guns_Involved', 'Notes', 'Participant_Age', 'Participant_Age_Group',
                                     'Participant_Gender']
    check_column_names(gun_violence_df, gun_violence_expected_columns)
    check_null_values(gun_violence_df, gun_violence_df.columns)


def check_gdp_by_state_data_table(gdp_by_state_df):
    """
    Checks the structure and integrity of a DataFrame containing emissions data.
    Args:
        gdp_by_state_df (pandas.DataFrame): The DataFrame containing GDP by state data.
    Raises:
        AssertionError: If the DataFrame fails any of the integrity checks.
    """
    check_num_cols(gdp_by_state_df, 25)
    gdp_by_state_expected_columns = ['1997', '1998', '1999', '2000', '2001', '2002', '2003', '2004',
                                     '2005',
                                     '2006', '2007', '2008', '2009', '2010', '2011', '2012', '2013', '2014',
                                     '2015', '2016', '2017', '2018', '2019', 'Unit', 'GeoName', ]
    check_column_names(gdp_by_state_df, gdp_by_state_expected_columns)
    check_null_values(gdp_by_state_df, gdp_by_state_df.columns)


def check_gdp_per_capita_data_table(gdp_per_capita_df):
    """
    Checks the structure and integrity of a DataFrame containing emissions data.
    Args:
        gdp_per_capita_df (pandas.DataFrame): The DataFrame containing GDP per capita data.
    Raises:
        AssertionError: If the DataFrame fails any of the integrity checks.
    """
    check_num_cols(gdp_per_capita_df, 6)
    gdp_per_capita_expected_columns = ['State', '2013', '2014', '2015', '2016', '2017']
    check_column_names(gdp_per_capita_df, gdp_per_capita_expected_columns)
    check_null_values(gdp_per_capita_df, gdp_per_capita_df.columns)


class TestDataPipeline(unittest.TestCase):
    """
    A test case class for testing the data pipeline.
    This class contains test methods to validate the execution of the data pipeline
    and the integrity of the output data files and tables.
    """

    @classmethod
    def setUpClass(cls):
        """
        Set up the test environment by executing the data pipeline.
        This method is called once before any tests in the class are run.
        """
        main()

    def test_output_files_exist(self):
        """
        Test whether the expected output files and tables exist.
        This test method checks if the database file and the required tables exist
        after executing the data pipeline.
        """
        # Check if the database file exists
        db_file = 'data/us_gun_violence_with_economy.db'
        self.assertTrue(os.path.isfile(db_file), f"Database file '{db_file}' does not exist")

        # Check if the tables exist within the database
        with sqlite3.connect(db_file) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()
            tables = [table[0] for table in tables]

        expected_tables = ['Gun_violence_data', 'US_State_GDP_data', 'GDP_Per_Capita_data']
        for table in expected_tables:
            self.assertIn(table, tables, f"Table '{table}' does not exist in the database")
        print("Test output_files_exist passed successfully.")

    def test_pipeline_execution(self):
        """
        Test the execution of the data pipeline.
        This test method executes the data pipeline, including data loading,
        transformation, and validation, and ensures that the output data tables
        meet the expected structure and integrity criteria.
        """
        # Define the Kaggle dataset URLs
        gun_violence_url = "jameslko/gun-violence-data"
        gdp_by_state_url = "davidbroberts/us-gdp-by-state-19972020"
        gdp_per_capita_url = "solorzano/gdp-per-capita-in-us-states"

        gun_violence_file = download_and_extract_in_memory(gun_violence_url)
        gdp_by_state_file = download_and_extract_in_memory(gdp_by_state_url)
        gdp_per_capita_file = download_and_extract_in_memory(gdp_per_capita_url)

        gun_violence_df = load_and_clean_data(gun_violence_file)
        gdp_by_state_df = load_and_clean_data(gdp_by_state_file)
        gdp_per_capita_df = load_and_clean_data(gdp_per_capita_file)

        # Transform the datasets
        gun_violence_df = transform_gun_violence_data(gun_violence_df).copy()
        gdp_by_state_df = transform_state_gdp_data(gdp_by_state_df).copy()
        gdp_per_capita_df = transform_state_gdp_per_capita(gdp_per_capita_df).copy()

        # Check gun violence data table
        check_gun_violence_data(gun_violence_df)

        # Check GDP by state data table
        check_gdp_by_state_data_table(gdp_by_state_df)

        # Check GDP per capita data table
        check_gdp_per_capita_data_table(gdp_per_capita_df)

        print("Test pipeline_execution passed successfully.")


if __name__ == '__main__':
    unittest.main()
