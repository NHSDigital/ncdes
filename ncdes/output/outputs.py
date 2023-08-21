import pandas as pd
from datetime import datetime
import os

def save_NCDes_main_to_csv(NCDes_problem_ind_rem, root_directory):
    dates_table = get_date_for_name(NCDes_problem_ind_rem)
    file_name = get_file_name(dates_table)
    file_folder = get_file_folder(dates_table)
    NCDes_problem_ind_rem.to_csv(
        f"{root_directory}Output\\" + file_folder + r"\\" + file_name + ".csv",
        index=False,
    )


def save_NCDes_by_ruleset_to_csvs(NCDes_with_rulesets, root_directory):
    dates_table = get_date_for_name(NCDes_with_rulesets)
    file_name = get_file_name(dates_table)
    file_folder = get_file_folder(dates_table)
    for RULESET_ID in NCDes_with_rulesets['Ruleset ID'].unique():
        ncdes_data_ruleset = NCDes_with_rulesets.loc[NCDes_with_rulesets['Ruleset ID'] == RULESET_ID].drop(columns = "Ruleset ID")
        ncdes_data_ruleset.to_csv(
            f"{root_directory}Output\\{file_folder}\\{file_name}_{RULESET_ID}.csv",
        index=False,
        )

def get_date_for_name(NCDes_with_geogs):
    """
    Input:
        Takes the table with geogs that we made a copy of earlier

    Outputs:
        An object that has all the correct date data we need to create the filename
    """

    date = pd.to_datetime(NCDes_with_geogs["ACH_DATE"].iloc[0])

    dates_table = NCDes_with_geogs[["ACH_DATE"]].drop_duplicates()

    # Convert ACH_DATE to datetime
    dates_table["ACH_DATE"] = pd.to_datetime(dates_table["ACH_DATE"], format="%d/%m/%Y")

    # Extract the month name (3-characters) from ACH_DATE
    dates_table["Month_Name"] = dates_table["ACH_DATE"].dt.strftime("%B")

    # Extract the month number from ACH_DATE
    dates_table["Month_Number"] = dates_table["ACH_DATE"].dt.strftime("%m")

    # Extract the year for the year range used in the financial year column
    dates_table["Year"] = dates_table["ACH_DATE"].dt.strftime("%y")

    return dates_table


def get_file_name(dates_table):
    month = dates_table["Month_Name"].iloc[0]
    year = dates_table["Year"].iloc[0]

    file_name = "NCDes" + month + year

    return file_name


def get_file_folder(dates_table):

    month_num = dates_table["Month_Number"].iloc[0]
    year = dates_table["Year"].iloc[0]

    if int(month_num) >= 4:
        file_folder = r"NCD_" + year + "_" + str(int(year) + 1)

    elif int(month_num) < 4:
        file_folder = r"NCD_" + str(int(year) - 1) + "_" + year

    return file_folder

def archive_input_as_csv(ncdes_raw, root_directory):
    today = (datetime.today()).strftime("%Y_%m_%d")
    ncdes_raw.to_csv(f"{root_directory}Input\\Archive\\NCDes_" + today + ".csv", index=False)


def remove_files_from_input_folder(path):
    for file in os.listdir(path):
        os.remove(path + file)
