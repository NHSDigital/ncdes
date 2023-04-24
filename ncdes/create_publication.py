from datetime import datetime
import pandas as pd
import os

from .data import sql_connection
from .data import data_load

from .processing import processing_steps
from .processing import validation_check as check

from .output import outputs

from .utils.adhoc_fix import remove_problem_indicators


CORRECT_COLUMN_ORDER_NCDes_with_geogs = [
    "PRACTICE_CODE",
    "PRACTICE_NAME",
    "QUALITY_SERVICE",
    "ACH_DATE",
    "IND_CODE",
    "MEASURE",
    "VALUE",
]


def main() -> None:
    print("\n"*3,"Loading config file")
    config = data_load.load_json_config_file(".\\config.json")

    print("Establish SQL connection")
    connection = sql_connection.connect(server=config["server"], database=config["database"])

    root_directory = config["root_directory"]

    print("Loading in NCDes data")
    ncdes_raw = data_load.load_csvs_in_directory_as_concat_dataframe(f"{root_directory}\\Input\\Current")

    print("Cleaning NCDes data")
    ncdes_clean = processing_steps.clean_ncdes(ncdes_raw)
    reporting_period = processing_steps.get_formatted_reporting_end_date_from_ncdes_data(ncdes_clean)

    geo_ccg_sql_str, geo_reg_sql_str, stp_sql_str, prac_sql_str = data_load.get_sql_query_strings(reporting_period)

    print("Loading in SQL mapping data")
    geo_ccg_df = pd.read_sql(sql=geo_ccg_sql_str, con=connection)
    geo_reg_df = pd.read_sql(sql=geo_reg_sql_str, con=connection)
    stp_df = pd.read_sql(sql=stp_sql_str, con=connection)
    prac_df = pd.read_sql(sql=prac_sql_str, con=connection)

    print("Formatting SQL mapping data")
    geo_ccg_df, geo_reg_df, stp_df = processing_steps.sql_df_cols_to_upper_case(geo_ccg_df, geo_reg_df, stp_df)

    print("Loading in epcn data")
    raw_epcn = data_load.load_epcn_excel_table(epcn_path=config["epcn_path"])
    epcn_df = processing_steps.epcn_transform(raw_epcn)

    print("Creating super mapping table")
    mapping_table = processing_steps.create_mapping_table(geo_ccg_df, geo_reg_df, stp_df, prac_df, epcn_df)

    print("Merging NCDes data with supermapping data")
    NCDes_with_geogs = processing_steps.merge_tables_fill_Na_reorder_cols(mapping_df=mapping_table, ncdes_df_cleaned=ncdes_clean, CORRECT_COLUMN_ORDER_NCDes_with_geogs=CORRECT_COLUMN_ORDER_NCDes_with_geogs)
    
    print("Starting validation checks")
    check.run_all_column_has_expected_values_validations(NCDes_with_geogs, root_directory)

    print("Replacing placeholder indicator and measures")
    NCDes_with_geogs = processing_steps.replace_placeholders(NCDes_with_geogs)

    print("Applying suppression")
    NCDes_suppressed = processing_steps.suppress_output(
        main_table=NCDes_with_geogs,
        root_directory=root_directory,
        measure_dict_meas_col_name='MEASURE ID',
        measure_dict_meas_type_col_name='MEASURE_TYPE',
        measure_dict_meas_description_col_name='MEASURE_DESCRIPTION',
        main_table_meas_col_name='MEASURE',
        main_table_value_col_name='VALUE',
        main_table_prac_code_col_name='PRACTICE_CODE',
        main_table_ind_code_col_name='IND_CODE'
    )

    print("Removing bad indicators")
    NCDes_problem_ind_rem = remove_problem_indicators.remove(NCDes_suppressed, ["NCD015"])

    print("Joining ruleset ID to copy of output data for ruleset-specific outputs")
    NCDes_with_rulesets = processing_steps.merge_data_with_ruleset_id(NCDes_problem_ind_rem, root_directory)

    print("Saving main output")
    outputs.save_NCDes_main_to_csv(NCDes_problem_ind_rem, root_directory)

    print("Saving outputs split by ruleset")
    outputs.save_NCDes_by_ruleset_to_csvs(NCDes_with_rulesets, root_directory)

    print("Archiving input")
    outputs.archive_input_as_csv(ncdes_raw, root_directory)

    print("Deleting input files from input folder")
    outputs.remove_files_from_input_folder(path=f"{root_directory}Input\\Current\\")

    print("Job complete")


if __name__ == "__main__":
    main()
