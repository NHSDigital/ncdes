import pandas as pd
from datetime import datetime as datetime2
import pyodbc as dbc
import os
from ncdes.data.practice_maps import *
from ncdes.output.outputs import *


def measure_and_indicator_mappings(root: str) -> pd.DataFrame:
    """Gets the measure and indicator mappings from proposed metadata"""

    df = pd.read_csv(f"{root}\\Input\\CQRS_files\\NCD_metadata.csv", encoding='unicode_escape')
    df = df.sort_values("INDICATOR_ID", ascending=True)
    
    #Indicator mappings
    ind_dict = df[["INDICATOR_ID","INDICATOR_DESCRIPTION","Ruleset"]]
    ind_dict = ind_dict.drop_duplicates()
    ind_dict["Payment or Management Information (MI)"] = "MI"

    ind_dict.loc[(ind_dict["INDICATOR_ID"] == "HI03") |
                (ind_dict["INDICATOR_ID"] == "CAN02"), "Payment or Management Information (MI)"] = "Payment"

    ind_dict = ind_dict.rename(columns={"INDICATOR_ID": "Indicator ID",
                                        "INDICATOR_DESCRIPTION":"Indicator Description",
                                        "Ruleset":"Ruleset ID"})

    #Measure mappings
    meas_dict = df[["CQRS short code", "MEASURE_DESCRIPTION","MEASURE_TYPE"]]
    meas_dict = meas_dict.drop_duplicates()
    meas_dict = meas_dict.rename(columns={"CQRS short code":"MEASURE ID"})
    meas_dict = meas_dict[meas_dict["MEASURE ID"] != "n/a"]
    meas_dict["MEASURE_DESCRIPTION"] = meas_dict["MEASURE_DESCRIPTION"].replace({"Numerator":"Numerator count for indicator", "Denominator":"Denominator count for indicator"})  

    #Adding numerator fix from ncdes review
    meas_dict.loc[meas_dict["MEASURE_TYPE"] == "Numerator", "MEASURE ID"] = "Numerator"
    meas_dict.loc[meas_dict["MEASURE_TYPE"] == "Denominator", "MEASURE ID"] = "Denominator"
    return ind_dict, meas_dict


def remove_codes(df: pd.DataFrame, config:dict) -> pd.DataFrame:
    """removes any data for PCN codes and only leaves practice codes"""

    codes = code_map(config['Connections']["server"], config['Connections']["database"])
    codes = codes.rename(columns={"CODE":"ORG_CODE"})
    df = codes.merge(df, on="ORG_CODE", how="left")

    return df


def update_dataframe(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """Updates the new CQRS dataframe so it can be processed by the job"""

    #Check if it is new data or old data format
    df_cols = df.columns
    req_cols = ["PRMRY_MDCL_CARE_SRVC_SHRT_NAME","ORG_CODE","ACH_DATE","IND_CODE",
                "FIELD_NAME","VALUE","APPROVED_STATUS"]
    
    #If data is imported as the original version, pass through function
    if list(df_cols) == req_cols:
        print("Old format data recieved, skipping amender")
        return df

    #print(os.getcwd())
    root = config['Filepaths']["root_directory"]
    field_map = pd.read_csv(f"{root}\\Input\\CQRS_files\\NCD_CQRS_Metadata_Mapping.csv")

    #Drop all U codes
    df = remove_codes(df, config)

    prac_sup_df = practice_supplier_map(config['Connections']["map_server"], config['Connections']["map_database"])
    prac_sup_df = prac_sup_df.rename(columns={"GP_Code":"ORG_CODE"})


    #If data imported is the new version, convert it to the original version
    df = df.drop(["NAME","DEFAULT_VALUES_USED"], axis=1)
    df = df.rename(columns={"QUALITY_SERVICE":"PRMRY_MDCL_CARE_SRVC_SHRT_NAME"})

    df = pd.merge(df, field_map, on=["IND_CODE","FIELD_NAME"], how="left")
    df = df.drop(["FIELD_NAME"], axis=1)
    df = df.rename(columns={"SHORT_NAME":"FIELD_NAME"})

    #check if year folder exists, if not create one 
    check_and_create_folder(f"{root}\\Output\\CQRS_omitted_tracker\\")

    #Duplicated rows, logged
    not_last_sub = df[df["LAST_SUBMISSION"] == "N"]
    not_last_sub = not_last_sub[["ORG_CODE","SUBMIT_DATE","SUBMIT_TIME"]].merge(prac_sup_df, on="ORG_CODE", how="left")
    not_last_sub = not_last_sub.fillna("Unknown")
    root_directory = config['Filepaths']["root_directory"]
    not_last_sub = not_last_sub.drop_duplicates()
    not_last_sub.to_csv(f"{root_directory}\\Output\\CQRS_omitted_tracker\\CQRS_omits.csv", index=False)

    #Finishing off cleaning the data
    df = df[df["LAST_SUBMISSION"] == "Y"].reset_index()
    df = df.drop(['SUBMIT_USER_ID','SUBMIT_DATE','SUBMIT_TIME','LAST_SUBMISSION',"index"], axis=1)

    #now lets order it
    df = df.sort_values(by=["ORG_CODE","IND_CODE","FIELD_NAME"], ascending=[True, True, True])
    df = df.drop_duplicates()
    df = df.reset_index(drop=True)


    return df




