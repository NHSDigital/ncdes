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

#--------------------------------------------------- Suppression logic start -----------------------------------------------------------------#

def split_dataframe(
    merged_table,
    measure_dict_meas_col_name='MEASURE ID',
    measure_dict_meas_type_col_name = 'MEASURE_TYPE',
    main_table_ind_code_col_name='IND_CODE'
):
    """
    Split dataframe according to how many PCAs each indicator has

    Input:
        Raw df merged with measure dictionary

    Output:
        merged_df_1_PCA: input df containing only indicators/practices with one 1 PCA
        merged_df_2_or_more_PCA: input df containing only indicators/practices with 2 or more PCAs
        merged_df_0_PCA: input df containing only indicators/practices with 0 PCAs
    """

    # Create count dict that maps IND to number of PCAs
    unique_meas_ind_table = merged_table[[main_table_ind_code_col_name, measure_dict_meas_col_name,measure_dict_meas_type_col_name]].drop_duplicates()
    PCA_count_dict = {}
    for ind in set(unique_meas_ind_table[main_table_ind_code_col_name]):
        ind_sub_df = unique_meas_ind_table[unique_meas_ind_table[main_table_ind_code_col_name] == ind]
        PCA_sum = list(ind_sub_df[measure_dict_meas_type_col_name]).count('PCA')
        PCA_count_dict[ind] = PCA_sum
    
    # Split main table into two sub tables depending on which PCA condition the table meets 
    one_PCA_inds = [key for key, val in PCA_count_dict.items() if val == 1]
    two_plus_PCA_inds = [key for key, val in PCA_count_dict.items() if val > 1]
    zero_PCA_inds = [key for key, val in PCA_count_dict.items() if val == 0]
    
    merged_df_1_PCA = merged_table[merged_table[main_table_ind_code_col_name].isin(one_PCA_inds)]
    merged_df_2_plus_PCA = merged_table[merged_table[main_table_ind_code_col_name].isin(two_plus_PCA_inds)]
    merged_df_0_PCA = merged_table[merged_table[main_table_ind_code_col_name].isin(zero_PCA_inds)]
    
    return merged_df_1_PCA, merged_df_2_plus_PCA, merged_df_0_PCA

def denom_condition_1_PCA(
    merged_df_1_PCA,
    main_table_meas_col_name='MEASURE',
    main_table_value_col_name='VALUE',
    main_table_prac_code_col_name='PRACTICE_CODE',
    main_table_ind_code_col_name='IND_CODE'
):
    """
    Denominator condition for indicators/practices with one PCA: denominator must be less that <2 
    
    Input:
        merged_df_1_PCA
    
    Output:
        Filtered out all indicators from input that don't meet the denominator condition for indicators with one PCA
        """
    # Filter out which of these indicators don't meet the denom condition for each practice 
    denom_condition_met_subset = merged_df_1_PCA[
        (merged_df_1_PCA[main_table_meas_col_name] == 'Denominator') & (merged_df_1_PCA[main_table_value_col_name] < 2)
        ]
    # Merge in above df with input to ensure that we retain other measures and not just denominators 
    denom_condition_met_whole = pd.merge(
        merged_df_1_PCA.reset_index(), 
        denom_condition_met_subset[[main_table_prac_code_col_name, main_table_ind_code_col_name]], 
        on=[main_table_prac_code_col_name, main_table_ind_code_col_name], 
        how='inner'
    )
    
    return denom_condition_met_whole

def suppress_1_PCA(
    denom_condition_met_1_PCA,
    merged_df_1_PCA,
    measure_dict_meas_type_col_name='MEASURE_TYPE',
    main_table_value_col_name='VALUE',
    main_table_prac_code_col_name='PRACTICE_CODE',
    main_table_ind_code_col_name='IND_CODE',
    main_table_meas_col_name='MEASURE'    
):
    """
    Test which indicators have a PCA > 0 in 'denom_condition_met_1_PCA' df and if this condition is met, 
    suppress the PCAs and their associated denominators 
    """
    # Drop all non-PCA rows
    PCA_filtered = denom_condition_met_1_PCA[denom_condition_met_1_PCA[measure_dict_meas_type_col_name] == 'PCA']
    
    # Get a table with only non-zero PCAs
    table_for_suppression_a = PCA_filtered[PCA_filtered[main_table_value_col_name] > 0]
    
    # Get unique identifier to isolate PCAs that need to be suppressed; list these indicator/PCA combinations and the respective indicator/denominator combinations
    ZIP = zip(table_for_suppression_a[main_table_prac_code_col_name], table_for_suppression_a[main_table_ind_code_col_name], table_for_suppression_a[main_table_meas_col_name])
    unique_identifier_a = []
    for val in ZIP:
        unique_identifier_a.append(val)
        unique_identifier_a.append(list(val[0:2]) + ['Denominator'])
    
    ## Loop through using unqie identifiers and suppress the PCAs and denominators
    PCA_1_out = merged_df_1_PCA.copy()
    for ind in unique_identifier_a:
        PCA_1_out.loc[(PCA_1_out[main_table_prac_code_col_name] == ind[0]) & (PCA_1_out[main_table_ind_code_col_name] == ind[1]) & (PCA_1_out[main_table_meas_col_name] == ind[2]) , main_table_value_col_name] = '*'
    
    return PCA_1_out

def denom_condition_2_plus_PCA(
    merged_df_2_plus_PCA,
    main_table_meas_col_name='MEASURE',
    main_table_value_col_name='VALUE',
    main_table_prac_code_col_name='PRACTICE_CODE',
    main_table_ind_code_col_name='IND_CODE'
):
    """
    Denominator condition for indicators/practices with two or more PCAs: Denominator must be equal to 0 
    
    Input:
        merged_df_2_plus_PCA
    
    Output:
        Filtered out all indicators from input that don't meet the denominator condition for indicators with 2 or more PCAs

        
    """
    # Filter to only get denominator/practice rows that meet the denominator condition 
    merged_filtered = merged_df_2_plus_PCA[
        (merged_df_2_plus_PCA[main_table_meas_col_name] == 'Denominator') & (merged_df_2_plus_PCA[main_table_value_col_name] == 0)
    ]
    
    # Merge in above df with input to ensure that all items for practice/indicator combinations that meet the denominator criterion are identified
    full_table_filtered = pd.merge(
        merged_df_2_plus_PCA, 
        merged_filtered[[main_table_prac_code_col_name, main_table_ind_code_col_name]], 
        on=[main_table_prac_code_col_name, main_table_ind_code_col_name], 
        how='inner'
    )

    return full_table_filtered

def pivot_measures_col(
    only_PCAs,
    main_table_value_col_name='VALUE',
    main_table_prac_code_col_name='PRACTICE_CODE',
    main_table_ind_code_col_name='IND_CODE',
    main_table_meas_col_name='MEASURE'
):
    """
    Pivot dataframe in preparation for testing the PCA condition for indicators with 2+ PCAs
    """
    pivoted = pd.pivot_table(
                    only_PCAs, 
                    values = main_table_value_col_name, 
                    index=[main_table_prac_code_col_name, main_table_ind_code_col_name], 
                    columns = main_table_meas_col_name
                    ).reset_index().fillna(0)
    

    return pivoted

def get_problem_practice_indicator_pairs(
    pivoted,
    main_table_prac_code_col_name='PRACTICE_CODE',
    main_table_ind_code_col_name='IND_CODE'
):
    """
    PCA Condition for indicators with 2+ PCAs: The sum of all PCAs is equal to the value of any one PCA
    
    Function iterates through the pivoted dataframe and checks which rows are compliant with the 
    'PCA Condition for indicators with 2+ PCAs'. It adds the practice code/indicator code combinations 
    for these rows to a list which it then returns.
    """
    list_of_prac_ind_to_supress = []
    # Iterate through pivoted dataframe rows where each row represents all the PCAs for a practice/indicator
    # pair that meets the denominator condition
    for i, row in pivoted.iterrows():
        # Get temp row with only PCA cols
        row_reduced = row[2:]
        # If below is true it means there's only one non-zero PCA for that practice/indicator combination (number of zero count PCAs = number of all PCAs-1)
        # and therefore one PCA value = sum of PCA values for that practice/indicator combination
        if Counter(row_reduced)[0.0] == len(row_reduced)-1:
            # Append problem indicators to the list
            list_of_prac_ind_to_supress.append(
                (row[main_table_prac_code_col_name], row[main_table_ind_code_col_name])
            )

    return list_of_prac_ind_to_supress

def suppress_2_plus_PCA(
    list_of_prac_ind_to_supress,
    merged_df_2_plus_PCA,
    main_table_prac_code_col_name,
    main_table_ind_code_col_name,
    measure_dict_meas_type_col_name,
    measure_dict_meas_col_name,
    measure_dict_meas_description_col_name,
    main_table_value_col_name
):
    """
    Retrieves indexes to suppress from the 'get_indexes_to_suppress' fn; uses these to locate the 
    problem values and inserts a *
    """
    indexes_to_suppress = get_indexes_to_suppress(
        prac_ind_to_suppress=list_of_prac_ind_to_supress,
        merged_df_2_plus_PCA=merged_df_2_plus_PCA,
        main_table_prac_code_col_name=main_table_prac_code_col_name,
        main_table_ind_code_col_name=main_table_ind_code_col_name,
        measure_dict_meas_type_col_name=measure_dict_meas_type_col_name
    )

    
    merged_df_2_plus_PCA.loc[indexes_to_suppress, main_table_value_col_name] = '*'    
    return merged_df_2_plus_PCA.drop(columns=[measure_dict_meas_col_name, measure_dict_meas_description_col_name, measure_dict_meas_type_col_name])

def get_indexes_to_suppress(
    prac_ind_to_suppress,
    merged_df_2_plus_PCA,
    main_table_prac_code_col_name,
    main_table_ind_code_col_name,
    measure_dict_meas_type_col_name
):
    """
    Gets a list of indexes to suppress from the rows that meet the suppression
    condition 
    """
    index_to_suppress = []
    
    for practice, indicator in prac_ind_to_suppress:
        # Retrieves a sub dataframe whose rows meet the conditions for suppression
        subset_to_suppress = merged_df_2_plus_PCA[
            (merged_df_2_plus_PCA[main_table_prac_code_col_name] == practice) &
            (merged_df_2_plus_PCA[main_table_ind_code_col_name] == indicator) &
            (merged_df_2_plus_PCA[measure_dict_meas_type_col_name] == 'PCA')
            ]  
        # Adds the indexes for these rows to a list
        list_subset_to_suppress = [x for x in subset_to_suppress.index]
        index_to_suppress.extend(list_subset_to_suppress)
    
    return index_to_suppress

def suppress_output(
    main_table,
    root_directory,
    measure_dict_meas_col_name='MEASURE ID',
    measure_dict_meas_type_col_name='MEASURE_TYPE',
    measure_dict_meas_description_col_name='MEASURE_DESCRIPTION',
    main_table_meas_col_name='MEASURE',
    main_table_value_col_name='VALUE',
    main_table_prac_code_col_name='PRACTICE_CODE',
    main_table_ind_code_col_name='IND_CODE'
):
    """
    Applies the following rules to the fully processed dataframe:

    1. For all fractional indicators: omit exclusion counts from publications (this is already implemented for LDHC, but would be a change for all other publications e.g. QOF, NCDES, INLIQ...)

    2. For fractional indicators with 1 PCA specified: where denominator <2 and PCA > 0, suppress the PCA and the denominator for that indicator

    3. For fractional indicators with >1 PCA specified: where denominator = 0 and the sum of all PCAs is equal to the value of any one PCA, suppress all the PCAs for that indicator
    
    
    Function can be broken down into 4 main parts:
    
    a. Ingestion/pre-processing 
    b. Dealing with suppression relating to point '2.' above
    c. Dealing with suppression realting to point '3.' above
    d. Recombining dataframes
    """
    # -------------------------------------------------- a -------------------------------------------------------- #
    # Load in measure dictionary
    measure_dict = data_load.load_indicator_and_measure_data_dictionaries(root_directory)[1]

    
    # Merge in measure type data to main table
    merged_table = pd.merge(
        main_table, 
        measure_dict, 
        left_on=main_table_meas_col_name, 
        right_on=measure_dict_meas_col_name, 
        how='left'
    )
    
    # Split dataframe so different suppression rules can be applied to each split
    merged_df_1_PCA, merged_df_2_plus_PCA, merged_df_0_PCA = split_dataframe(merged_table)
    
    # -------------------------------------------------- b -------------------------------------------------------- #
    # filter relevant merged df according to denominator condition for indicators with one PCA 
    denom_condition_met_1_PCA = denom_condition_1_PCA(merged_df_1_PCA)
    # Apply suppression to indicators with one PCA that meet the denominator condition and PCA condition
    PCA_1_out = suppress_1_PCA(
        denom_condition_met_1_PCA,
        merged_df_1_PCA,
        measure_dict_meas_type_col_name,
        main_table_value_col_name,
        main_table_prac_code_col_name,
        main_table_ind_code_col_name,
        main_table_meas_col_name
    )

    
    
    # -------------------------------------------------- c -------------------------------------------------------- #
    # filter relevant merged df according to denominator condition for indicators with 2 or more PCAs
    denom_condition_met_2_plus_PCA = denom_condition_2_plus_PCA(
        merged_df_2_plus_PCA,
        main_table_meas_col_name=main_table_meas_col_name,
        main_table_value_col_name=main_table_value_col_name,
        main_table_prac_code_col_name=main_table_prac_code_col_name,
        main_table_ind_code_col_name=main_table_ind_code_col_name
    )
    # Filter the above df to only contain PCA values; i.e. isolate PCAs for practice/indicator combinations meeting denominator suppression criterion
    only_PCAs = denom_condition_met_2_plus_PCA[denom_condition_met_2_plus_PCA[measure_dict_meas_type_col_name] == 'PCA']
    
    
    # Pivot the filtered table so that we can apply summation logic
    pivoted = pivot_measures_col(
        only_PCAs,
        main_table_value_col_name=main_table_value_col_name,
        main_table_prac_code_col_name=main_table_prac_code_col_name,
        main_table_ind_code_col_name=main_table_ind_code_col_name,
        main_table_meas_col_name=main_table_meas_col_name
    )
    
    # Get list of indicators that meet the PCA condition for indicators with more than one PCA (and meet the above
    # denominator condition)
    list_of_prac_ind_to_supress = get_problem_practice_indicator_pairs(
        pivoted,
        main_table_prac_code_col_name=main_table_prac_code_col_name,
        main_table_ind_code_col_name=main_table_ind_code_col_name
    )
    
    # Apply suppression to indicators with 2 or more PCAs that meet the denominator condition and PCA condition
    PCA_2_plus_out = suppress_2_plus_PCA(
        list_of_prac_ind_to_supress,
        merged_df_2_plus_PCA,
        main_table_prac_code_col_name,
        main_table_ind_code_col_name,
        measure_dict_meas_type_col_name,
        measure_dict_meas_col_name,
        measure_dict_meas_description_col_name,
        main_table_value_col_name
    )
    
    # -------------------------------------------------- d -------------------------------------------------------- #
    re_merged_df = pd.concat(
        [PCA_1_out, PCA_2_plus_out, merged_df_0_PCA]
    ).drop(
        columns=[measure_dict_meas_col_name, measure_dict_meas_description_col_name, measure_dict_meas_type_col_name]
    )
    # Remerge in measure dictionary data so that we can drop exclusions
    re_merged_df_typed = pd.merge(
        re_merged_df, 
        measure_dict, 
        left_on=main_table_meas_col_name, 
        right_on=measure_dict_meas_col_name, 
        how='left'
    )
    exclusions_dropped = re_merged_df_typed[re_merged_df_typed[measure_dict_meas_type_col_name] != 'Exclusion']
    # Drop columns from final output
    fully_suppressed_df = exclusions_dropped.drop(
        columns=[measure_dict_meas_col_name, measure_dict_meas_description_col_name,measure_dict_meas_type_col_name]
    )
    # Print how many rows are being suppressed
    total_suppressed = len(fully_suppressed_df[fully_suppressed_df.VALUE == '*'])
    print(f"Suppressed {total_suppressed} rows")
    
    return fully_suppressed_df    

#--------------------------------------------------- Suppression logic end -----------------------------------------------------------------#

def merge_data_with_ruleset_id(NCDes_problem_ind_rem, root_directory):

    # Load in measure dictionary
    indicator_dictionary = data_load.load_indicator_and_measure_data_dictionaries(root_directory)[0]
    # Join ruleset ID onto data
    ncdes_with_rulesets = pd.merge(
        NCDes_problem_ind_rem, 
        indicator_dictionary, 
        left_on = "IND_CODE", 
        right_on = "Indicator ID", 
        how = "left"
        ).drop(
            columns = ["Indicator Description","Payment or Management Information (MI)","Indicator ID"]
            )
    
    return ncdes_with_rulesets
