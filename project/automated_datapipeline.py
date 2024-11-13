import pandas as pd
import numpy as np
import os
import kagglehub
from sqlalchemy import create_engine

def download_and_extract_in_memory(url):
    """
    Downloads a dataset from Kaggle using kagglehub and loads the first CSV file into a pandas DataFrame.
    
    Returns:
    pd.DataFrame: The first CSV file loaded as a DataFrame.
    """
    
        # Download the dataset using kagglehub
    print(f"Downloading dataset from: {url}")
    path = kagglehub.dataset_download(url)
        
        # List files in the downloaded folder
    downloaded_files = os.listdir(path)
    print(f"Files in downloaded folder: {downloaded_files}")
        
        # Filter out any files that are not CSV files
    csv_files = [f for f in downloaded_files if f.endswith('.csv')]
        
        # Select the first CSV file found
    csv_file_path = os.path.join(path, csv_files[0])
    print(f"Using CSV file: {csv_file_path}")

    return csv_file_path
        
    

def load_and_clean_data(file_like, delimiter=','):
    """
    Load data from a CSV file-like object into a pandas DataFrame and perform data cleaning.
    
    Returns:
    pd.DataFrame: A cleaned DataFrame with no missing values.
    """
    df = pd.read_csv(file_like, delimiter=delimiter)
    df.dropna(inplace=True)
    return df

def drop_irrelevant_columns(df, columns_to_drop):
    """
    Removes specified columns from a pandas DataFrame.
    
    Returns:
    pd.DataFrame: Modified DataFrame with specified columns dropped.
    """
    df.drop(columns=columns_to_drop, inplace=True)
    return df

def rename_columns(df, columns_mapping):
    """
    Rename columns in a DataFrame based on the the required changes.
        
    Returns:
    pd.DataFrame: DataFrame with columns renamed.
    """
    df.rename(columns=columns_mapping, inplace=True)
    return df

def drop_rows_with_zeros(df, columns):
    """
    Drop rows from a DataFrame that contain zero and Null in specified columns.
        
    Returns:
        pd.DataFrame: DataFrame with rows containing zero and Null in specified columns dropped.
    """
    df = df.loc[~(df[columns] == 0 | (df[columns].isna()) ).all(axis=1)]
    return df

def impute_data(df):
    # Numerical Columns
    for col in df.select_dtypes(include=['int', 'float']):
        if df[col].isnull().sum() > 0:  
           df.loc[df[col].isnull(), col] = df[col].mean()  

    # Categorical Columns    
    for col in df.select_dtypes(include=['object']):
        if df[col].isnull().sum() > 0:
            df.loc[df[col].isnull(), col] = df[col].mode()[0] 

    return df


def create_database(db_dir, db_name):
    """
    Create a SQLite database in the specified directory.
    
    Returns:
    str: Path of the created SQLite database.
    """
    db_path = os.path.join(db_dir, f"{db_name}.db")
    if not os.path.exists(db_dir):
        os.makedirs(db_dir)
    return db_path


def transform_gun_violence_data(df):
    """
    Transforms Gun Violence data in a pandas DataFrame 

    This function performs several operations on the DataFrame to prepare this data for analysis:
    1. Drop the unnecessary columns.
    2. Replace missing values with Mean Imputation
    3. Drops the duplicates in the columns.
    4. Rename columns for consistency and clarity
    5. Reorder the columns. 
    
    Args:
        df (str): Path to the CSV file containing gun violence data.

    Returns:
        pd.DataFrame: Transformed gun violence DataFrame.
    """
    gun_violence_columns_to_drop = ['incident_id', 'incident_url', 'source_url', 'incident_url_fields_missing', 
                                    'latitude', 'longitude', 'gun_type', 'gun_stolen', 'sources', 
                                    'state_senate_district', 'state_house_district', 'participant_type', 
                                    'participant_status', 'participant_relationship', 'participant_name']
    df = drop_irrelevant_columns(df, gun_violence_columns_to_drop).copy()
    handling_missing_columns_data = ["congressional_district", "incident_characteristics", "n_guns_involved", 
                                     "participant_age", "participant_age_group", 'participant_gender']
    df = impute_data(df).copy()
    df = df.drop_duplicates()
    gun_violence_columns_rename = {
        'date': 'Date',
        'state': 'State',
        'city_or_county': 'City/County',
        'address': 'Address',
        'n_killed': 'No_of_Killed',
        'n_injured': 'No_of_Injured',
        'congressional_district': 'Congressional_District',
        'incident_characteristics': 'Incident_Characteristics',
        'n_guns_involved': 'No_Guns_Involved',
        'notes': 'Notes',
        'participant_age': 'Participant_Age',
        'participant_age_group': 'Participant_Age_Group',
        'participant_gender': 'Participant_Gender'}
    df = rename_columns(df, gun_violence_columns_rename).copy()
    return df.reindex(columns=['Date', 'State', 'City/County', 'Address', 'No_of_Killed',
                               'No_of_Injured', 'Congressional_District', 'Incident_Characteristics', 
                               'No_Guns_Involved', 'Notes', 'Participant_Age', 'Participant_Age_Group',
                               'Participant_Gender'])


def transform_state_gdp_data(df):
    """
    Transforms US every state GDP data in a pandas DataFrame 

    This function performs several operations on the DataFrame to prepare this data for analysis:
    1. Drop the unnecessary columns.
    2. Replace missing values with Mean Imputation
    3. Drops the duplicates in the columns.
    4. Rename columns for consistency and clarity
    5. Reorder the columns. 
    
    Args:
        df (str): Path to the CSV file containing state GDP data.

    Returns:
        pd.DataFrame: Transformed state GDP DataFrame.
    """
    state_gdp_columns_to_drop = ['GeoFIPS', 'TableName', 'LineCode', 'IndustryClassification']
    df = drop_irrelevant_columns(df, state_gdp_columns_to_drop).copy()
    years = [str(year) for year in range(1997, 2020)]
    non_year_columns = ['Unit', 'GeoName']
    columns_to_impute = years + non_year_columns
    df = impute_data(df).copy()
    df = df.drop_duplicates()
    us_gdp_state_column_rename = {'GeoName': 'State', 'Region': 'Region_no', 'Description': 'Industry_names'}
    df = rename_columns(df, us_gdp_state_column_rename).copy()
    df = df.reindex(columns=columns_to_impute)
    return df


def transform_state_gdp_per_capita(df):
    """
    Transforms US every state GDP per capita data in a pandas DataFrame 

    This function performs several operations on the DataFrame to prepare this data for analysis:
    1. Drop the unnecessary columns.
    2. Drops the duplicates in the columns.
    3. Rename columns for consistency and clarity
    4. Reorder the columns. 
    
    Args:
        df (str): Path to the CSV file containing state GDP per capita data.

    Returns:
        pd.DataFrame: Transformed state GDP per capita DataFrame.
    """
    state_gdp_columns_to_drop = ['Fips']
    df = drop_irrelevant_columns(df, state_gdp_columns_to_drop).copy()
    state_gdp_per_capita_column_rename = {'Area': 'State'}
    df = rename_columns(df, state_gdp_per_capita_column_rename).copy()
    return df.reindex(columns=['State', '2013', '2014', '2015', '2016', '2017'])


def create_database(db_dir, db_name):
    """
    Create a SQLite database in the specified directory.
    
    Returns:
    str: Path of the created SQLite database.
    """
    db_path = os.path.join(db_dir, f"{db_name}.db")
    if not os.path.exists(db_dir):
        os.makedirs(db_dir)
    return db_path


def main():
    """
    Main function to execute the data engineering automated pipeline. This function downloads,
    extracts, transforms, and saves all three different data.
    
    Steps:
        - Download and extract data.
        - Transform gun violence and GDP data.
        - Save the transformed data to a SQLite database.
    
    Returns:
        None
    """

    # Define the Kaggle dataset URLs
    gun_violence_url = "jameslko/gun-violence-data"
    gdp_by_state_url = "davidbroberts/us-gdp-by-state-19972020"
    gdp_per_capita_url = "solorzano/gdp-per-capita-in-us-states"
    
    # Download and load the datasets 
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
    
    # SQLite database path
    db_dir = 'data'
    db_name = 'us_gun_violence_with_economy'
    db_path = create_database(db_dir, db_name)
    engine = create_engine(f'sqlite:///{db_path}')
    
    # Save the transformed data to the database
    gun_violence_df.to_sql('Gun_violence_data', engine, index=False, if_exists='replace')
    gdp_by_state_df.to_sql('US_State_GDP_data', engine, index=False, if_exists='replace')
    gdp_per_capita_df.to_sql('GDP_Per_Capita_data', engine, index=False, if_exists='replace')
    
    

if __name__ == "__main__":
    main()
