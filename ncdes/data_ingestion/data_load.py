import os
import pandas as pd
import tomli
from datetime import datetime
from pathlib import PurePath
import logging
#import json
from ncdes.data_ingestion.amender import *
import csv



# def load_json_config_file(path):
#     with open(path) as f:
#         file = json.load(f)
#     return file


def get_config(loc:str) -> dict:
    """
    Function Actions:
    - Reads the config toml file containing user inputs.
    - Returns a dictionary of parameters.
    """   

    filename = PurePath(loc)
    assert os.path.isfile(filename), "config.toml file could not be found. Please make sure the toml file is saved in the main repo location and the name is 'config.toml' as expected."

    with open(filename, "rb") as f:
        config_file = tomli.load(f)

    logging.info('Config file read in.')

    return config_file


def load_csvs_in_directory_as_concat_dataframe(directory):
    """
    Requirements:
        All files in the directory must be csvs
        All files must have the same headings
    """
    phase_file_names = os.listdir(directory)

    logging.info(f"Loading in files {phase_file_names}")

    holder = []
    for phase in phase_file_names:
        phase_filepath = PurePath(directory, phase)
        df = pd.read_csv(phase_filepath, quotechar='"', delimiter=',', quoting = csv.QUOTE_ALL)
        holder.append(df)

    output_df = pd.concat(holder, ignore_index=True)

    logging.info("Data loaded")

    return output_df


def get_sql_query_strings(reporting_period):
    """
    This function contains data for a query to the the SQL database.

    If you need to edit the queries do so here.

    Input:
        reporting period in the format %Y%m%d (YYYY-MM-DD)

    Output:
        1 query
    """

    reporting_period = "'{0}'".format(reporting_period)

    ## Query for the ODS_PRACTICE_V02 table
    # Get all open practices as of reporting period end date
    prac = """SELECT DISTINCT CODE AS 'PRACTICE_CODE',
                NAME AS 'PRACTICE_NAME'
                FROM [dbo].[ODS_PRACTICE_V02] WHERE OPEN_DATE <= {0} AND (CLOSE_DATE >= {0} 
                OR CLOSE_DATE IS NULL) AND DSS_RECORD_START_DATE <= {0} 
                AND (DSS_RECORD_END_DATE >= {0} OR DSS_RECORD_END_DATE IS NULL)""".format(
        reporting_period
    )

    return prac


def load_indicator_and_measure_data_dictionaries(root):
    """
    Output:
        Measure and Indicator dictionary in pandas dataframe
    """

    #Indicator mappings made from metadata
    indicator_dictionary, measure_dictionary = measure_and_indicator_mappings(root)
    
    return indicator_dictionary, measure_dictionary

