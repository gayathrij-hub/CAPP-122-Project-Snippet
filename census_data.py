import requests
import json
import pandas as pd
from pathlib import Path

# Dictionary consisting of Census State codes for targetted states
STATE_CODE_DICTIONARY = {
    "Illinois": "17",
    "Louisiana": "22",
    "Washington": "53",
    "Florida": "12",
    "California": "06",
}

DESIRED_COLUMNS = [
    "geo_id",
    "state",
    "State_Name",
    "county",
    "County_Name",
    "tract",
]


def load_dataframe(response):
    """
    Convert JSON response to a pandas DataFrame.

    Parameters:
    - response (Response): The JSON response object obtained from an API request.

    Returns:
    - dataframe (DataFrame): A pandas DataFrame containing the data from the JSON response.
    """
    data_json = response.json()
    columns = data_json[0]
    rows = data_json[1:]
    dataframe = pd.DataFrame(rows, columns=columns)
    return dataframe


## Data Cleaning and Appending to Dataframe
def preprocess_name_column(dataframe):
    """
    Preprocesses a DataFrame by splitting the 'NAME' column and cleaning
    up column values.

    Parameters:
    - dataframe (pandas DataFrame): DataFrame containing census data.

    Returns:
    - preprocessed_dataframe (pandas DataFrame): Preprocessed census DataFrame.
    """
    # Split 'NAME' column into 'Census_Tract', 'County_Name', and 'State_Name'
    dataframe[["Census_Tract", "County_Name", "State_Name"]] = dataframe[
        "NAME"
    ].str.split("; ", expand=True)

    # Drop unnecessary columns
    dataframe.drop(columns=["Census_Tract", "NAME"], inplace=True)

    # Remove leading and trailing whitespaces
    dataframe["State_Name"] = dataframe["State_Name"].str.strip()
    dataframe["County_Name"] = dataframe["County_Name"].str.strip()

    return dataframe


# Adding the Census_Tract_ID
def add_geo_id(dataframe, state_column, county_column, tract_column):
    """
    Adds a new column 'geo_id' to the DataFrame by concatenating state,
    county, and tract codes.

    Parameters:
    - dataframe (DataFrame): The pandas DataFrame to which the new column will be added.
    - state_column (str): The name of the column containing state codes.
    - county_column (str): The name of the column containing county codes.
    - tract_column (str): The name of the column containing tract codes.

    Returns:
    - dataframe (DataFrame): The modified DataFrame with the new 'Census_Tract_ID' column.
    """
    # Convert columns to strings
    dataframe[state_column] = dataframe[state_column].astype(str)
    dataframe[county_column] = dataframe[county_column].astype(str)
    dataframe[tract_column] = dataframe[tract_column].astype(str)

    # Create new column 'geo_id' containing concatenated values
    dataframe["geo_id"] = (
        dataframe[state_column] + dataframe[county_column] + dataframe[tract_column]
    )
    return dataframe


def population_distribution_api():
    """
    Fetches population distribution data from the U.S. Census Bureau's API for the year 2020.

    Returns:
    dict: A dictionary containing dataframes for population distribution for each state.
          The keys are state names, and the values are pandas DataFrames
    """
    url_dp = "https://api.census.gov/data/2020/dec/dp?"
    params_dp = {
        "get": "NAME,DP1_0078C,DP1_0079C,DP1_0080C,DP1_0081C,DP1_0082C,DP1_0083C",
        "for": "TRACT:*",
    }
    dp_dataframes = {}
    for state, state_code in STATE_CODE_DICTIONARY.items():
        params_dp["in"] = f"state:{state_code}"
        response_dp = requests.get(url_dp, params=params_dp)
        dp_dataframes[state] = load_dataframe(response_dp)
    return dp_dataframes


def clean_population_distribution_data():
    """
    Cleans the population distribution data obtained from the population_distribution_api function.
    - Concatenates dataframes of individual states
    - Adds geo_id
    - Reordering

    Returns:
    pandas.DataFrame: Dataframe comsisting of race-wise population distribution
    at tract level
    """
    dp_dataframes = population_distribution_api()
    dp_dictionary = {
        "DP1_0078C": "white_population",
        "DP1_0079C": "black_african_american_population",
        "DP1_0080C": "american_indian_alaskan_native_population",
        "DP1_0081C": "asian_population",
        "DP1_0082C": "native_hawaiian_and_other_pacific_islander_population",
        "DP1_0083C": "Other_race_population",
    }
    # Renaming column names as per variable list
    for state, dataframe in dp_dataframes.items():
        # Mapping old column names to new ones
        column_mapping_dhc = {
            old_name: dp_dictionary.get(old_name, old_name)
            for old_name in dataframe.columns
        }
        dp_dataframes[state] = dataframe.rename(columns=column_mapping_dhc)

    compiled_dp = []
    for state, df in dp_dataframes.items():
        compiled_dp.append(df)
    compiled_dataframe_dp = pd.concat(compiled_dp, ignore_index=True)
    compiled_dataframe_dp = add_geo_id(
        compiled_dataframe_dp,
        "state",
        "county",
        "tract",
    )
    compiled_dataframe_dp = preprocess_name_column(compiled_dataframe_dp)
    remaining_columns = [
        col for col in compiled_dataframe_dp.columns if col not in DESIRED_COLUMNS
    ]
    new_order = DESIRED_COLUMNS + remaining_columns
    compiled_dataframe_dp = compiled_dataframe_dp[new_order]
    return compiled_dataframe_dp


def housing_characteristics_api():
    """
    Fetches housing characteristics data from the U.S. Census Bureau's API for the year 2020.

    Returns:
    dict: A dictionary containing DataFrames for housing characteristics for each state.
    """
    url_dhc = "https://api.census.gov/data/2020/dec/dhc?"
    params_dhc = {
        "get": "NAME,H12A_002N,H12A_010N,H12B_002N,H12B_010N,H12C_002N,H12C_010N,H12D_002N,H12D_010N,H12E_002N,H12E_010N,H12F_002N,H12F_010N",
        "for": "TRACT:*",
    }
    ddhc_dataframes = {}
    for state, state_code in STATE_CODE_DICTIONARY.items():
        params_dhc["in"] = f"state:{state_code}"
        response_ddhc = requests.get(url_dhc, params=params_dhc)

        ddhc_dataframes[state] = load_dataframe(response_ddhc)
    return ddhc_dataframes


def clean_housing_characteristics_data():
    """
     Cleans housing characteristics data obtained from the U.S. Census Bureau's
     API for the year 2020.

    Returns:
    pandas.DataFrame: A DataFrame containing cleaned housing characteristics
    data, including owner-occupied and renter-occupied housing counts for
    different racial demographics at the tract level.
    """
    ddhc_dataframes = housing_characteristics_api()
    dhc_variable_dictionary = {
        "H12A_002N": "owner_occuppied_white",
        "H12A_010N": "renter_occupied_white",
        "H12B_002N": "owner_occupied_black_or_african_american",
        "H12B_010N": "renter_occupied_black_or_african_american",
        "H12C_002N": "owner_occuppied_american_indian_alaska_native",
        "H12C_010N": "renter_occupied_american_indian_alaska_native",
        "H12D_002N": "owner_occupied_asian",
        "H12D_010N": "renter_occupied_asian",
        "H12E_002N": "owner_occupied_native_hawaiian",
        "H12E_010N": "renter_occupied_native_hawaiian",
        "H12F_002N": "owner_occuppied_other_race",
        "H12F_010N": "renter_occuppied_other_race",
    }
    # Renaming column names as per variable list
    for state, dataframe in ddhc_dataframes.items():
        # Mapping old column names to new ones
        column_mapping_dhc = {
            old_name: dhc_variable_dictionary.get(old_name, old_name)
            for old_name in dataframe.columns
        }
        ddhc_dataframes[state] = dataframe.rename(columns=column_mapping_dhc)

    compiled_dhc = []
    for state, df in ddhc_dataframes.items():
        compiled_dhc.append(df)

    compiled_dataframe_dhc = pd.concat(compiled_dhc, ignore_index=True)
    compiled_dataframe_dhc = add_geo_id(
        compiled_dataframe_dhc,
        "state",
        "county",
        "tract",
    )
    compiled_dataframe_dhc = preprocess_name_column(compiled_dataframe_dhc)
    remaining_columns = [
        col for col in compiled_dataframe_dhc.columns if col not in DESIRED_COLUMNS
    ]
    new_order = DESIRED_COLUMNS + remaining_columns
    compiled_dataframe_dhc = compiled_dataframe_dhc[new_order]
    return compiled_dataframe_dhc


def community_resilience_api():
    """
    Fetches community resilience estimates data from the U.S. Census Bureau's
    API for the year 2022.

    Returns:
    dict: A dictionary containing DataFrames for community resilience indicators
     for each state in the STATE_DICTIONARY.
    """
    url = "https://api.census.gov/data/2022/cre?"
    params_cre = {
        "get": "NAME,PRED0_E,PRED0_PE,PRED12_E,PRED12_PE,PRED3_E,PRED3_PE",
        "for": "TRACT:*",
    }
    cre_dataframes = {}
    for state, state_code in STATE_CODE_DICTIONARY.items():
        params_cre["in"] = f"state:{state_code}"
        response_cre = requests.get(url, params=params_cre)

        cre_dataframes[state] = load_dataframe(response_cre)
    return cre_dataframes


def clean_community_resilience_data():
    """
    Cleans community resilience data obtained from the U.S. Census Bureau's API
    for the year 2022.

    Returns:
    pandas.DataFrame: A DataFrame containing cleaned community resilience
    indicators data, including estimated numbers and rates of individuals with
    different levels of social vulnerability.
    """
    cre_dataframes = community_resilience_api()
    cre_dictionary = {
        "PRED0_E": "estimated_number_of_individuals_with_zero_components_of_social_vulnerability",
        "PRED0_PE": "rate_of_individuals_with_zero_components_of_social_vulnerability",
        "PRED12_E": "estimated_number_of_individuals_with_one_two_components_of_social_vulnerability",
        "PRED12_PE": "rate_of_individuals_with_one_two_components_of_social_vulnerability",
        "PRED3_E": "estimated_number_of_individuals_with_three_or_more_components_of_social_vulnerability",
        "PRED3_PE": "rate_of_individuals_with_three_or_more_components_of_social_vulnerability",
    }
    # CRE: Renaming column names as per variable list
    for state, dataframe in cre_dataframes.items():
        # Mapping old column names to new ones
        column_mapping_cre = {
            old_name: cre_dictionary.get(old_name, old_name)
            for old_name in dataframe.columns
        }
        cre_dataframes[state] = dataframe.rename(columns=column_mapping_cre)

    cre_compiled_dfs = []
    for state, df in cre_dataframes.items():
        cre_compiled_dfs.append(df)

    # Concatenate all DataFrames in the list along the rows (axis=0)
    compiled_dataframe_cre = pd.concat(cre_compiled_dfs, ignore_index=True)
    compiled_dataframe_cre = add_geo_id(
        compiled_dataframe_cre,
        "state",
        "county",
        "tract",
    )
    compiled_dataframe_cre = preprocess_name_column(compiled_dataframe_cre)
    remaining_columns = [
        col for col in compiled_dataframe_cre.columns if col not in DESIRED_COLUMNS
    ]
    new_order = DESIRED_COLUMNS + remaining_columns
    compiled_dataframe_cre = compiled_dataframe_cre[new_order]
    return compiled_dataframe_cre


def process_census_data_to_csv():
    """
    Processes cleaned dataframes and exports them to a CSV file named 'census_data.csv'.

    The function performs the following steps:
    1. Cleans population distribution, housing characteristics, and community
        resilience data.
    2. Merges cleaned dataframes based on geographic identifiers.
    3. Exports the merged dataframe to a CSV file named 'census_data.csv' in
        the 'output_data' directory.

    Note:
    - The cleaned dataframes are merged based on common geographic identifiers.
    - The output CSV file is stored in the 'output_data' directory.
    """
    dp_dataframe = clean_population_distribution_data()
    dhc_dataframe = clean_housing_characteristics_data()
    cre_dataframe = clean_community_resilience_data()

    # Merge dataframes
    merge_dp_and_dhc = pd.merge(
        dp_dataframe,
        dhc_dataframe,
        on=["geo_id", "state", "State_Name", "county", "County_Name", "tract"],
        how="outer",
    )

    merged_census_data = pd.merge(
        merge_dp_and_dhc,
        cre_dataframe,
        on=["geo_id", "state", "State_Name", "county", "County_Name", "tract"],
        how="outer",
    )
    # Exporting dataframe to csv file
    output_path = Path(__file__).resolve().parent / "output_data"
    output_path.mkdir(parents=True, exist_ok=True)
    merged_census_data.to_csv(output_path / "census_data.csv", index=False)


# Creating the Census_data.csv for SQL Database
census_data = process_census_data_to_csv()
