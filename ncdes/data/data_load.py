import os
import pandas as pd
import json


def load_json_config_file(path):
    with open(path) as f:
        file = json.load(f)
    return file


def load_csvs_in_directory_as_concat_dataframe(directory):
    """
    Requirements:
        All files in the directory must be csvs
        All files must have the same headings
    """
    phase_file_names = os.listdir(directory)

    print(f"loading in files {phase_file_names}")

    holder = []
    for phase in phase_file_names:
        df = pd.read_csv(directory + "\\" + phase)
        holder.append(df)

    output_df = pd.concat(holder, ignore_index=True)

    print("data loaded")

    return output_df


def get_sql_query_strings(reporting_period):
    """
    This function contains data for the 4 queries that are made to the the SQL database.

    If you need to edit the queries do so here.

    Input:
        reporting period in the format %Y%m%d (YYYY-MM-DD)

    Output:
        4 queries
    """

    reporting_period = "'{0}'".format(reporting_period)

    ## Query 1 for the ONS_CHD_GEO_EQUIVALENTS table
    geo_ccg = """SELECT DISTINCT GEOGRAPHY_CODE AS 'CCG_ONS_CODE',
                CASE WHEN DH_GEOGRAPHY_CODE ='18C'THEN 
                'NHS HEREFORDSHIRE AND WORCESTERSHIRE CCG' 
                WHEN DH_GEOGRAPHY_CODE ='06N'THEN 
                'NHS HERTS VALLEYS CCG' ELSE GEOGRAPHY_NAME END AS 'CCG_NAME',
                DH_GEOGRAPHY_CODE AS 'CCG_ODS_CODE' 
                FROM [dbo].[ONS_CHD_GEO_EQUIVALENTS] WHERE IS_CURRENT = 1 
                AND DH_GEOGRAPHY_CODE IS NOT NULL AND ENTITY_CODE = 'E38'"""

    ## Query 2 for the ONS_CHD_GEO_EQUIVALENTS table
    geo_reg = """SELECT DISTINCT GEOGRAPHY_CODE AS 'REGION_ONS_CODE',GEOGRAPHY_NAME AS 'REGION_NAME',
                    DH_GEOGRAPHY_CODE AS 'REGION_ODS_CODE' 
                    FROM [dbo].[ONS_CHD_GEO_EQUIVALENTS] WHERE IS_CURRENT = 1 
                    AND DH_GEOGRAPHY_CODE IS NOT NULL AND ENTITY_CODE = 'E40'"""

    ## Query for the ONS_LSOA_CCG_STP_LAD_V01 table
    stp = """SELECT DISTINCT CCGCDH AS 'CCG_ODS_CODE', STPCD AS 'STP_ONS_CODE', 
            STPNM AS 'STP_NAME' 
            FROM [dbo].[ONS_LSOA_CCG_STP_LAD_V01] WHERE DSS_RECORD_START_DATE <= {0}
            AND (DSS_RECORD_END_DATE >= {0} OR DSS_RECORD_END_DATE IS NULL)\
            AND DSS_ONS_PUBLISHED_DATE = (SELECT MAX(DSS_ONS_PUBLISHED_DATE) AS DSS_ONS_PUBLISHED_DATE
            FROM  [dbo].[ONS_LSOA_CCG_STP_LAD_V01] WHERE DSS_RECORD_START_DATE <= {0}
            AND (DSS_RECORD_END_DATE >= {0} OR DSS_RECORD_END_DATE IS NULL))""".format(
        reporting_period
    )

    ## Query for the ODS_PRACTICE_V02 table
    # Get all open practices as of reporting period end date
    prac = """SELECT DISTINCT CODE AS 'PRACTICE_CODE',
                NAME AS 'PRACTICE_NAME',
                COMMISSIONER_ORGANISATION_CODE AS 'CCG_ODS_CODE',
                HIGH_LEVEL_HEALTH_GEOGRAPHY AS 'STP_ODS_CODE',
                NATIONAL_GROUPING AS 'REGION_ODS_CODE'
                FROM [dbo].[ODS_PRACTICE_V02] WHERE OPEN_DATE <= {0} AND (CLOSE_DATE >= {0} 
                OR CLOSE_DATE IS NULL) AND DSS_RECORD_START_DATE <= {0} 
                AND (DSS_RECORD_END_DATE >= {0} OR DSS_RECORD_END_DATE IS NULL)""".format(
        reporting_period
    )

    return geo_ccg, geo_reg, stp, prac


def load_epcn_excel_table(epcn_path):
    return pd.read_excel(epcn_path, sheet_name="PCN Core Partner Details", usecols="A,E,F,I,J")


def load_indicator_and_measure_data_dictionaries(root):
    """
    Output:
        Measure and Indicator dictionary in pandas dataframe
    """

    indicator_dictionary = pd.read_csv(f"{root}Input\\Data dictionary 23_24\\indicator dictionary.csv")
    measure_dictionary = pd.read_csv(f"{root}Input\\Data dictionary 23_24\\measure dictionary.csv")

    return indicator_dictionary, measure_dictionary

