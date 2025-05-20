import pandas as pd
from datetime import datetime as datetime2
import os
import zipfile
from ncdes.data_ingestion.data_load import *
import subprocess
import shutil
from pathlib import PurePath
import logging

def output_file_name_components(NCDes_df):
    dates_table = get_date_for_name(NCDes_df)
    file_name = get_file_name(dates_table)
    file_folder = get_file_folder(dates_table)
    data_month = get_data_month(NCDes_df)
    data_month_year = get_data_month_and_year(NCDes_df)
    return file_name, file_folder, data_month, data_month_year

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

def test_run_change_outputs_fldr(test_run, root_directory):
    if test_run.lower() == "true":
          output_directory = check_and_create_folder(PurePath(os.getcwd(), 'Outputs')) 
          logging.info(f"Outputting to Test folder: {output_directory}")
    else:
          output_directory = PurePath(root_directory, 'Output')
          logging.info(f"WARNING: Outputting to root directory: {output_directory}.")
    
    return output_directory

def check_and_create_folder(folder_path):
    try:
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
    except FileExistsError:
        logging.info(f'{folder_path}: Folder already exists')
    return folder_path

def save_NCDes_main_to_csv(NCDes_problem_ind_rem, output_directory, file_name, file_folder, data_month):
    #check if year folder exists, if not create one 
    save_filepath1 = PurePath(output_directory, file_folder, "CSV_archive")

    check_and_create_folder(save_filepath1)

    #check if month folder exists, if not create one
    logging.info(f"data month is {data_month}")

    save_filepath2 = PurePath(save_filepath1, data_month)
    check_and_create_folder(save_filepath2)

    #to csv
    logging.info("Convert main df to csv (takes a while)")
    save_filepath3 = PurePath(save_filepath2, str(file_name + ".csv"))
    NCDes_problem_ind_rem.to_csv(save_filepath3, index=False)
    

def open_outputs(output_directory, file_folder, data_month):
    """opens the output folder"""
    outputs_path = PurePath(output_directory, file_folder, 'Zip_archive', data_month)
    subprocess.Popen(["explorer", os.path.realpath(f'{outputs_path}')])

    
def save_NCDes_main_to_zip(output_directory, file_name, file_folder, data_month):
    #check if year folder exists, if not create one 
    outputs_path = PurePath(output_directory, file_folder, 'Zip_archive', data_month)
    check_and_create_folder(outputs_path)
    
    #to zip
    with zipfile.ZipFile(PurePath(outputs_path, str(file_name + '.zip')),'w') as zipMe:
            filenamecsv = f"{file_name}.csv"
            logging.info(f"File name is {filenamecsv}")
            file = PurePath(output_directory, file_folder, 'CSV_archive', data_month, filenamecsv)
            zipMe.write(file, arcname=filenamecsv, compress_type=zipfile.ZIP_DEFLATED)

def save_NCDes_by_ruleset_to_csvs(NCDes_with_rulesets, output_directory, file_name, file_folder, data_month):
    #check if year folder exists, if not create one 
    CSV_archive_folderpath = PurePath(output_directory, file_folder,'CSV_archive', data_month) 
    check_and_create_folder(CSV_archive_folderpath)

    #to csv
    for RULESET_ID in NCDes_with_rulesets['Ruleset ID'].unique():
        ncdes_data_ruleset = NCDes_with_rulesets.loc[NCDes_with_rulesets['Ruleset ID'] == RULESET_ID].drop(columns = "Ruleset ID")
        ncdes_data_ruleset_filepath = PurePath(CSV_archive_folderpath, f"{file_name}_{RULESET_ID}.csv")
        ncdes_data_ruleset.to_csv(ncdes_data_ruleset_filepath, index=False)

def save_NCDes_by_ruleset_to_zip(NCDes_with_rulesets, output_directory, file_name, file_folder, data_month, data_month_year):
    #to zip
    csv_filepath1 = PurePath(output_directory, file_folder, 'Zip_archive', data_month, f"NCDes{data_month_year}_By_Ruleset.zip")
    with zipfile.ZipFile(csv_filepath1, 'w') as zipMe:
        for RULESET_ID in NCDes_with_rulesets['Ruleset ID'].unique():
            filenamecsv = f"{file_name}_{RULESET_ID}.csv"
            csv_filepath2 = PurePath(output_directory, file_folder, 'CSV_archive', data_month, filenamecsv)
            zipMe.write(csv_filepath2, arcname=filenamecsv, compress_type=zipfile.ZIP_DEFLATED)


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
        file_folder = year + "_" + str(int(year) + 1)

    elif int(month_num) < 4:
        file_folder = str(int(year) - 1) + "_" + year

    return file_folder



def archive_input(root_directory):
    from os import walk
    
    #set folderpaths
    ncdes_raw_filepath = PurePath(root_directory, 'Input', 'Current') 
    ncdes_archive_filepath = PurePath(root_directory, 'Input', 'Archive')
    today_suffix = (datetime2.today()).strftime("%Y%m%d")

    #return a list of all files within the 'Input' folder
    files_list = []
    for (dirpath, dirnames, filenames) in walk(ncdes_raw_filepath):
        files_list.extend(filenames)
        break

    #move and rename each file with todays date as a suffix
    for file in files_list:
        shutil.move( os.path.join(ncdes_raw_filepath, file),  os.path.join(ncdes_archive_filepath, file))
        os.rename(os.path.join(ncdes_archive_filepath, file) , str(os.path.join(ncdes_archive_filepath,  str(file[:-4] + f"_{today_suffix}" + file[-4:]))))



def remove_files_from_input_folder(folderpath):
    for file in os.listdir(folderpath):
        os.remove(PurePath(folderpath, file))