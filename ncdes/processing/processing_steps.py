# -*- coding: utf-8 -*-
# def get_achievment_date(ncdTable):
import pandas as pd
from datetime import datetime
from ..data import data_load
from collections import Counter

def clean_ncdes(ncdes_raw):
    """
    Input:
        Raw ncdes data
    Output:
        Ncdes table with columns re-named, the approval status column dropped
        and date formatted
    """
    ncdes_raw = ncdes_raw.rename(
        columns={
            "PRMRY_MDCL_CARE_SRVC_SHRT_NAME": "QUALITY_SERVICE",
            "ORG_CODE": "PRACTICE_CODE",
            "FIELD_NAME": "MEASURE",
        }
    )

    ncdes_raw = ncdes_raw.drop(columns={"APPROVED_STATUS"})

    ncdes_raw.ACH_DATE = pd.to_datetime(
        ncdes_raw.ACH_DATE, format="%Y%m%d"
    ).dt.strftime("%d/%m/%Y")

    return ncdes_raw


def get_formatted_reporting_end_date_from_ncdes_data(ncdes_clean):
    Date = str(ncdes_clean.ACH_DATE[0])

    parsed_date = datetime.strptime(Date, "%d/%m/%Y")

    return parsed_date.strftime("%Y%m%d")


def sql_df_cols_to_upper_case(geo_ccg_df, geo_reg_df, stp_df):
    """
    Formats the relevant SQL dataframes. Making pre-defined columns
    values uppercased.
    """
    geo_ccg_df["CCG_NAME"] = geo_ccg_df["CCG_NAME"].str.upper()
    geo_reg_df["REGION_NAME"] = geo_reg_df["REGION_NAME"].str.upper()
    stp_df["STP_NAME"] = stp_df["STP_NAME"].str.upper()

    return geo_ccg_df, geo_reg_df, stp_df


def epcn_transform(epcn_table):
    """
    Description:
        Applies transforms to the raw epcn file

    Input:
        Raw epcn file

    Output:
        transformed epcn table
    """
    new_cols = ["PRACTICE_CODE", "PCN_ODS_CODE", "PCN_NAME", "START_DATE", "END_DATE"]
    epcn_table.columns = new_cols

    epcn_table = epcn_table.loc[epcn_table.END_DATE.isnull()]
    epcn_table = epcn_table.drop(columns=["END_DATE"])

    epcn_prac_table = epcn_table.groupby(["PRACTICE_CODE"], sort=True)[
        "START_DATE"
    ].max()
    epcn_table_filtered = epcn_table.merge(
        epcn_prac_table, on=["PRACTICE_CODE", "START_DATE"], how="inner"
    )
    epcn_df = epcn_table_filtered.drop(columns=["START_DATE"])

    return epcn_df


def create_mapping_table(
    geo_ccg_df,
    geo_reg_df,
    stp_df,
    prac_df,
    epcn_df,
    includeCols = ["PRACTICE_CODE", "PRACTICE_NAME"],
):
    """
    Creates a super mapping table by merging all input mapping tables
    """
    superTable = (
        prac_df.merge(geo_ccg_df, on="CCG_ODS_CODE", how="inner")
        .merge(epcn_df, on="PRACTICE_CODE", how="left")
        .merge(stp_df, on="CCG_ODS_CODE", how="inner")
        .merge(geo_reg_df, on="REGION_ODS_CODE", how="inner")[
            [
                "PRACTICE_CODE",
                "PRACTICE_NAME",
                "PCN_ODS_CODE",
                "PCN_NAME",
                "CCG_ONS_CODE",
                "CCG_ODS_CODE",
                "CCG_NAME",
                "STP_ONS_CODE",
                "STP_ODS_CODE",
                "STP_NAME",
                "REGION_ONS_CODE",
                "REGION_ODS_CODE",
                "REGION_NAME",
            ]
        ]
        .sort_values(by="PRACTICE_CODE")[includeCols]
        .drop_duplicates()
    )

    return superTable


def merge_tables_fill_Na_reorder_cols(mapping_df, ncdes_df_cleaned, CORRECT_COLUMN_ORDER_NCDes_with_geogs):
    ncdes_with_geogs = (
        pd.merge(mapping_df, ncdes_df_cleaned, how="right", on="PRACTICE_CODE")
        .fillna("UNALLOCATED")
        .reindex(columns=CORRECT_COLUMN_ORDER_NCDes_with_geogs)
    )
    return ncdes_with_geogs


def replace_placeholders(ncdes_table):
    """
    The NCD013 indicator cannot be fully configured in CQRS until the numerator has been sent to them by NHSD. As a result all of GPES inputs
    (denominator/exclusions/PCAs) are remapped to indicators NCDMI996, NCDMI997, NCDMI998, NCDMI999. As of 04/07/22 there is no plan on CQRS's
    side to internally re-map the indicators to NCD013 so we instead do it here manually.
    """
    # First replace the field name values
    ncdes_table.loc[ncdes_table.IND_CODE == "NCDMI996", "MEASURE"] = "Denominator"
    ncdes_table.loc[ncdes_table.IND_CODE == "NCDMI997", "MEASURE"] = "PAT_AGEU18"
    ncdes_table.loc[ncdes_table.IND_CODE == "NCDMI998", "MEASURE"] = "NURSHOME"
    ncdes_table.loc[ncdes_table.IND_CODE == "NCDMI999", "MEASURE"] = "CARHOMDEC1"

    # All problem indicators need to then have their IND_CODE remapped to NCD013
    remap_dict = {
        "NCDMI996": "NCD013",
        "NCDMI997": "NCD013",
        "NCDMI998": "NCD013",
        "NCDMI999": "NCD013",
    }
    ncdes_table = ncdes_table.replace({"IND_CODE": remap_dict})

    return ncdes_table


# Below functions deal with suppression
def suppress_PCA_values(
    main_table,
    root_directory,
    measure_dict_meas_col_name='MEASURE ID',
    measure_dict_meas_type_col_name = 'MEASURE_TYPE',
    main_table_meas_col_name='MEASURE',
    main_table_value_col_name='VALUE',
    main_table_prac_code_col_name='PRACTICE_CODE',
    main_table_ind_code_col_name='IND_CODE',  
):
    """
    This function will suppress sensitive PCA data from the input table 
    by applying the below rule:
    
    Denominator rule: If an indicator's denominator value for any given practice is 0
        
        AND
        
    PCA rule: If only one PCA is non-zero
    
        THEN
    
    Suppress all PCA values for indicator 
    """
    # Load in measure dictionary
    measure_dict = data_load.load_indicator_and_measure_data_dictionaries(root_directory)[1]
          
    # Format measures dictionary
    measure_dict_PCA = filter_to_only_contain_PCAs(
        input_df=measure_dict, 
        measure_type_col_name=measure_dict_meas_type_col_name
    )

    # Merge in the PCA measure type data with NCDes data so we can identify the PCA rows
    main_with_PCA = merge_main_df_and_measures_dict(
        main_table = main_table, 
        measure_dict_PCA = measure_dict_PCA, 
        main_table_meas_col_name=main_table_meas_col_name, 
        measure_dict_meas_col_name=measure_dict_meas_col_name
    )
        
    # Filter the new table to only contain practices/indicator combinations that staisfy the denominator condition
    main_denom_condition = filter_main_table_by_denom_condition(
        main_table_with_PCA=main_with_PCA,
        main_table_meas_col_name=main_table_meas_col_name,
        main_table_value_col_name=main_table_value_col_name,
        main_table_prac_code_col_name=main_table_prac_code_col_name,
        main_table_ind_code_col_name=main_table_ind_code_col_name
    )
    # print(main_denom_condition)
    # Get rid of all rows from the filtered table that aren't PCAs
    main_prepivot = filter_to_only_contain_PCAs(
        input_df=main_denom_condition, 
        measure_type_col_name=measure_dict_meas_type_col_name
    )
        
    # Pivot data 
    pivoted = pivot_measures_col_fill_na(
        main_table_prepivot=main_prepivot,
        main_table_value_col_name=main_table_value_col_name,
        main_table_prac_code_col_name=main_table_prac_code_col_name,
        main_table_ind_code_col_name=main_table_ind_code_col_name,
        main_table_meas_col_name=main_table_meas_col_name
    )
    
    # Get problematic practice/indidicators 
    prac_ind_to_suppress = get_problem_practice_indicator_pairs(
        pivoted = pivoted,
        main_table_prac_code_col_name=main_table_prac_code_col_name,
        main_table_ind_code_col_name=main_table_ind_code_col_name
    )
    #print(main_with_PCA)
    
    # Complete suppression of prblematic PCAs
    main_PCA_suppressed = suppress(
        prac_ind_to_suppress=prac_ind_to_suppress,
        main_with_PCA=main_with_PCA,
        main_table_prac_code_col_name=main_table_prac_code_col_name,
        main_table_ind_code_col_name=main_table_ind_code_col_name,
        measure_dict_meas_type_col_name=measure_dict_meas_type_col_name
    )
    
    return main_PCA_suppressed
   
def filter_to_only_contain_PCAs(
    input_df, 
    measure_type_col_name='MEASURE_TYPE'
):
    """
    Input:
        input_df: Dataframe we wish to filter
        measure_type_col_name: The name of the column that holds the measure type info in the input df
    Output:
        The input dataframe with only rows that have measure type == PCA
    """
    return input_df[input_df[measure_type_col_name] == 'PCA']

def merge_main_df_and_measures_dict(
    main_table, 
    measure_dict_PCA, 
    main_table_meas_col_name='MEASURE', 
    measure_dict_meas_col_name='MEASURE ID'
):
    """
    Input:
        main_table: The main input table you are trying to suppress
        measures dictionary with only PCA values
        main_table_meas_col_name: The column name for the measure column in the main table 
        measure_dict_meas_col_name: The column name for the measure column in the measure dictionary table 
    
    Returns:
        The main table with a new column that identifies which rows are PCA's
    """
    # Merge in measure type data with NCDes data
    main_table_measure_typed = pd.merge(
        main_table, 
        measure_dict_PCA, 
        left_on=main_table_meas_col_name, 
        right_on=measure_dict_meas_col_name, 
        how='left'
    )
    
    return main_table_measure_typed

def filter_main_table_by_denom_condition(
    main_table_with_PCA,
    main_table_meas_col_name='MEASURE',
    main_table_value_col_name='VALUE',
    main_table_prac_code_col_name='PRACTICE_CODE',
    main_table_ind_code_col_name='IND_CODE'
):
    """
    Filters the main table to only contain the practices/indicator rows that 
    satisfy the denominator condition
    
    Input:
        main_table_with_PCA: main table we are trying to suppress with associated PCA values merged in
    
    Return:
        The main table now filtered to only contain practices/indicator combinations that staisfy the 
        denominator condition
        
    """
    main_table_filtered_only_denoms = main_table_with_PCA[
        (main_table_with_PCA[main_table_meas_col_name] == 'Denominator') & (main_table_with_PCA[main_table_value_col_name] == 0)
    ]

    main_table_full_filtered = pd.merge(
        main_table_with_PCA, 
        main_table_filtered_only_denoms[[main_table_prac_code_col_name, main_table_ind_code_col_name]], 
        on=[main_table_prac_code_col_name, main_table_ind_code_col_name], 
        how='inner'
    )

    return main_table_full_filtered

def pivot_measures_col_fill_na(
    main_table_prepivot,
    main_table_value_col_name='VALUE',
    main_table_prac_code_col_name='PRACTICE_CODE',
    main_table_ind_code_col_name='IND_CODE',
    main_table_meas_col_name='MEASURE'
):
    """
    The test which rows meet the 'PCA rule'  
    """
    pivoted_and_filled = pd.pivot_table(
        main_table_prepivot, 
        values = main_table_value_col_name, 
        index=[main_table_prac_code_col_name, main_table_ind_code_col_name], 
        columns = main_table_meas_col_name).reset_index().fillna(0)
    

    return pivoted_and_filled

def get_problem_practice_indicator_pairs(
    pivoted,
    main_table_prac_code_col_name='PRACTICE_CODE',
    main_table_ind_code_col_name='IND_CODE'
):
    """
    Itterates through the pivoted dataframe and checks which rows are complient with the 'PCA rule'
    it adds the practice code/indicator code combinations for these probelmatic rows to a list which it 
    then returns
    """
    list_of_prac_ind_to_supress = []
    
    for i, row in pivoted.iterrows():
        row_reduced = row[2:]
        # If below is true it means theres only one non-zero PCA for that prac/ind combo
        if Counter(row_reduced)[0.0] == len(row_reduced)-1:
            list_of_prac_ind_to_supress.append(
                (row[main_table_prac_code_col_name], row[main_table_ind_code_col_name])
            )

    return list_of_prac_ind_to_supress

def suppress(
    prac_ind_to_suppress,
    main_with_PCA,
    main_table_prac_code_col_name,
    main_table_ind_code_col_name,
    measure_dict_meas_type_col_name
):
    indexes_to_suppress = get_indexes_to_suppress(
        prac_ind_to_suppress=prac_ind_to_suppress,
        main_with_PCA=main_with_PCA,
        main_table_prac_code_col_name=main_table_prac_code_col_name,
        main_table_ind_code_col_name=main_table_ind_code_col_name,
        measure_dict_meas_type_col_name=measure_dict_meas_type_col_name
    )

    
    main_with_PCA.loc[indexes_to_suppress, 'VALUE'] = '*'
    
    
    return main_with_PCA.drop(columns=["MEASURE ID", "MEASURE_DESCRIPTION", "MEASURE_TYPE"])

def get_indexes_to_suppress(
    prac_ind_to_suppress,
    main_with_PCA,
    main_table_prac_code_col_name,
    main_table_ind_code_col_name,
    measure_dict_meas_type_col_name
):
    index_to_suppress = []
    
    for practice, indicator in prac_ind_to_suppress:
        
        subset_to_suppress = main_with_PCA[(main_with_PCA[main_table_prac_code_col_name] == practice) 
                                     & (main_with_PCA[main_table_ind_code_col_name] == indicator) 
                                     & (main_with_PCA[measure_dict_meas_type_col_name] == 'PCA')]  

        list_subset_to_suppress = [x for x in subset_to_suppress.index]
        
        index_to_suppress.extend(list_subset_to_suppress)
    
    return index_to_suppress

def merge_mapped_data_with_ruleset_id(ncdes_with_geogs, root_directory):

    # Load in measure dictionary
    indicator_dictionary = data_load.load_indicator_and_measure_data_dictionaries(root_directory)[0]
    # Join ruleset ID onto data
    ncdes_with_geogs_and_rulesets = pd.merge(ncdes_with_geogs, indicator_dictionary, left_on = "IND_CODE", right_on = "Indicator ID", how = "left").drop(columns = ["Indicator Description","Payment or Management Information (MI)","Indicator ID"])
    
    return ncdes_with_geogs_and_rulesets