# -*- coding: utf-8 -*-
from datetime import datetime
from datetime import date
import pandas as pd
import os
import logging
from pathlib import PurePath


def run_all_column_has_expected_values_validations(NCDes_with_geogs, root_directory, indicator_dict, measure_dict):

    Automated_Checks_folderpath = PurePath(root_directory, "Output", "Automated Checks")

    get_unexpected(
        NCDes_with_geogs,
        list(indicator_dict["Indicator ID"]),
        PurePath(Automated_Checks_folderpath, "Unexpected Indicators History.csv"),
        "IND_CODE",
        root_directory
    )
    get_unexpected(
        NCDes_with_geogs,
        list(measure_dict["MEASURE ID"]),
        PurePath(Automated_Checks_folderpath, "Unexpected Measures History.csv"),
        "MEASURE",
        root_directory
    )
    # Check missing indicator values
    get_missing(
        NCDes_with_geogs,
        list(indicator_dict["Indicator ID"]),
        "IND_CODE",
        PurePath(Automated_Checks_folderpath, "Missing Indicators History.csv"),
        root_directory
    )
    get_missing(
        NCDes_with_geogs,
        list(measure_dict["MEASURE ID"]),
        "MEASURE",
        PurePath(Automated_Checks_folderpath, "Missing Measures History.csv"),
        root_directory
    )


def get_unexpected(ncd_table, list_expected, check_hist_path, ncd_table_col_name_str, root_directory):
    """
    Inputs:
        ncd_table: The table we're checking
        list_expected: A list of the indicators or measures we expect from the data. This can be found from the
                        relevant data dictionary
        check_hist_path: This is the path for where the relevant check history is located
        ncd_table_col_name_str: This gives the relevant column of the ncdTable that we are checking, this should be in str form

    Outputs:
        This function has no outputs but writes the results to a csv file

    """
    logging.info(f"Checking for unexpected {ncd_table_col_name_str}")

    # Set up
    unexpected = set()

    # Check if we have any codes we do not expect and add problem codes to set
    for code in ncd_table[ncd_table_col_name_str]:
        if code not in list_expected:
            unexpected.add(code)

    # Case where we have unexpected codes
    if len(unexpected) != 0:
        logging.warning(f"WARNING:, {len(unexpected)} UNEXPECTED CODES IN NCD TABLE: {unexpected}")
        df_check_results = put_problem_codes_in_df(problem_codes=unexpected)

    # Case where we have no unexpected codes
    elif len(unexpected) == 0:
        # create dataframe showing no issues for the run date
        df_check_results = pd.DataFrame.from_dict({"Date": [datetime.now().strftime("%Y-%m-%d")], "Code": ["No Issues"]})

    # Save checks
    save_check_results(path=check_hist_path, type_of_check='unexpected', df_check_results=df_check_results, ncd_table_col_name_str=ncd_table_col_name_str, root_directory=root_directory)

    return


def get_missing(ncd_table, list_expected, ncd_table_col_name_str, check_hist_path, root_directory):
    """
    Inputs
    ncdTable: The table we're checking
    list_expected: A list of the expected indicators or measures we expect from the data. This can be found from the
                    relevant data dictionary
    check_hist_path: This is the path for where the relevant check history is located
    ncdTable_col_name_str: This gives the relevant column of the ncdTable that we are checking, this should be in str form

    Outputs
    This function has no outputs but writes the results to a csv file
    """
    logging.info(f"Checking for missing {ncd_table_col_name_str}")

    # Set Up
    missing = set()

    # Check if we have anything missing and add problem codes to set
    for code in list_expected:
        if code not in list(ncd_table[ncd_table_col_name_str]):
            missing.add(code)

    # Case where we have missing codes
    if len(missing) != 0:
        logging.warning(f"WARNING: {len(missing)}, CODES ARE MISSING FROM THE INPUT TABLE: {missing}")
        df_check_results = put_problem_codes_in_df(problem_codes=missing)

    # Case where there are no missing codes
    elif len(missing) == 0:
        # create dataframe showing no issues for the run date
        df_check_results = pd.DataFrame.from_dict({"Date": [datetime.now().strftime("%Y-%m-%d")], "Code": ["No Issues"]})
    
    # Save checks
    save_check_results(path=check_hist_path, type_of_check='missing', df_check_results=df_check_results, ncd_table_col_name_str=ncd_table_col_name_str, root_directory=root_directory)
    
    return


def put_problem_codes_in_df(problem_codes):
    """
    Returns: The problem codes in a dataframe with 2 columns: Problem codes, Run date
    """
    run_date = datetime.now().strftime("%Y-%m-%d")

    # Get dictionary of missing codes with the run date
    holder_dict = {"Date": [], "Code": []}

    # Add values to dictionary
    for bad_code in list(problem_codes):
        holder_dict["Date"].append(run_date)
        holder_dict["Code"].append(bad_code)
    
    # Convert to dataframe
    return pd.DataFrame.from_dict(holder_dict)


def save_check_results(path, type_of_check, df_check_results, ncd_table_col_name_str, root_directory):
    """
    Description:
        Overwrites the previous check results 
    Inputs:
        type_of_check = {'unexpected' or 'missing'}
        df_check_results: a dataframe containing the check results
    """

    if do_check_history_files_exist(root_directory) == True:
        # Add the current checks to the history
        check_hist = pd.read_csv(path)
        new_file = pd.concat([df_check_results, check_hist], ignore_index=True)
    else:
        new_file = df_check_results
    # Overwrite existing file
    logging.info(f"Overwriting previous {type_of_check} {ncd_table_col_name_str} archive file")
    new_file.to_csv(path, index=False)

    return


def do_check_history_files_exist(root_directory):
    """
    Checks whether there are any automated check history files in our project directory

    This is mainly for public use as they will not have access to automated checks history 
    """
    path_to_check_hist_folder = PurePath(root_directory , "Output", "Automated Checks")

    if len(os.listdir(path_to_check_hist_folder)) < 4:
        return False
    else:
        return True