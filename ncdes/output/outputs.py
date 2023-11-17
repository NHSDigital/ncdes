import pandas as pd
from datetime import datetime as datetime2
import os
import zipfile
from ..output.outputexcel import *
from ..output.trendmonitor import *

def get_data_month(NCDes_problem_ind_rem):
        ach_date = NCDes_problem_ind_rem["ACH_DATE"].unique()[0]
        ach_date = str(ach_date)
        date_object = datetime2.strptime(ach_date, "%d/%m/%Y")
        data_month = date_object.strftime('%B')
        return data_month

def get_data_month_and_year(NCDes_problem_ind_rem):
        ach_date = NCDes_problem_ind_rem["ACH_DATE"].unique()[0]
        ach_date = str(ach_date)
        date_object = datetime2.strptime(ach_date, "%d/%m/%Y")
        data_month_year = date_object.strftime('%B%y')
        return data_month_year


def check_and_create_folder(folder_path):
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)


def save_NCDes_main_to_csv(NCDes_problem_ind_rem, root_directory):
    dates_table = get_date_for_name(NCDes_problem_ind_rem)
    file_name = get_file_name(dates_table)
    file_folder = get_file_folder(dates_table)

    #check if year folder exists, if not create one 
    check_and_create_folder(f"{root_directory}Output\\{file_folder}")

    #check if month folder exists, if not create one
    data_month = get_data_month(NCDes_problem_ind_rem)
    print(f"data month is {data_month}")
    check_and_create_folder(f"{root_directory}Output\\{file_folder}\\CSV_archive\\{data_month}")

    #to csv
    print("converting main df to csv")
    NCDes_problem_ind_rem.to_csv(f"{root_directory}Output\\{file_folder}\\CSV_archive\\{data_month}" + r"\\" + file_name + ".csv",
                                     index=False)
    
def save_NCDes_main_to_zip(NCDes_problem_ind_rem, root_directory):
    dates_table = get_date_for_name(NCDes_problem_ind_rem)
    file_name = get_file_name(dates_table)
    file_folder = get_file_folder(dates_table)
    data_month = get_data_month(NCDes_problem_ind_rem)   
    check_and_create_folder(f"{root_directory}Output\\{file_folder}\\Zip_archive\\{data_month}")
    #to zip
    with zipfile.ZipFile(f'{root_directory}Output\\{file_folder}\\Zip_archive\\{data_month}\\{file_name}.zip','w') as zipMe:
            filenamecsv = f"{file_name}.csv"
            print(f"filename is {filenamecsv}")
            file = f"{root_directory}Output\\{file_folder}\\CSV_archive\\{data_month}\\" + filenamecsv
            zipMe.write(file, arcname=filenamecsv, compress_type=zipfile.ZIP_DEFLATED)
    

def save_NCDes_main_to_excel(NCDes_problem_ind_rem, root_directory, server, database):
    main_to_excel(NCDes_problem_ind_rem, root_directory, server, database)

def save_trendmonitor(NCDes_problem_ind_rem, root_directory):
    data_month = get_data_month(NCDes_problem_ind_rem)   
    write_trend_monitor(NCDes_problem_ind_rem, root_directory,data_month)

def save_NCDes_by_ruleset_to_csvs(NCDes_with_rulesets, root_directory):
    dates_table = get_date_for_name(NCDes_with_rulesets)
    file_name = get_file_name(dates_table)
    file_folder = get_file_folder(dates_table)
    data_month = get_data_month(NCDes_with_rulesets) 
    #to csv
    for RULESET_ID in NCDes_with_rulesets['Ruleset ID'].unique():
        ncdes_data_ruleset = NCDes_with_rulesets.loc[NCDes_with_rulesets['Ruleset ID'] == RULESET_ID].drop(columns = "Ruleset ID")
        ncdes_data_ruleset.to_csv(
            f"{root_directory}Output\\{file_folder}\\CSV_archive\\{data_month}\\{file_name}_{RULESET_ID}.csv",
        index=False,
        )

def save_NCDes_by_ruleset_to_zip(NCDes_with_rulesets, root_directory):
    dates_table = get_date_for_name(NCDes_with_rulesets)
    file_name = get_file_name(dates_table)
    file_folder = get_file_folder(dates_table) 
    data_month = get_data_month(NCDes_with_rulesets) 
    data_month_year = get_data_month_and_year(NCDes_with_rulesets)
    #to zip
    with zipfile.ZipFile(f'{root_directory}Output\\{file_folder}\\Zip_archive\\{data_month}\\NCDes{data_month_year}_By_Ruleset.zip','w') as zipMe:
        for RULESET_ID in NCDes_with_rulesets['Ruleset ID'].unique():
            filenamecsv = f"{file_name}_{RULESET_ID}.csv"
            file = f"{root_directory}Output\\{file_folder}\\CSV_archive\\{data_month}\\{filenamecsv}"
            zipMe.write(file, arcname=filenamecsv, compress_type=zipfile.ZIP_DEFLATED)


def get_date_for_name(NCDes_with_geogs):
    """
    Input:
        Takes the table with geogs that we made a copy of earlier

    Outputs:
        An object that has all the correct date data we need to create the filename
    """

    date = pd.to_datetime(NCDes_with_geogs["ACH_DATE"].iloc[0], infer_datetime_format=True)

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
    today = (datetime2.today()).strftime("%Y_%m_%d")
    ncdes_raw.to_csv(f"{root_directory}Input\\Archive\\NCDes_" + today + ".csv", index=False)


def remove_files_from_input_folder(path):
    for file in os.listdir(path):
        os.remove(path + file)
