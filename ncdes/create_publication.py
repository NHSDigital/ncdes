from datetime import datetime
import pandas as pd
import os
from pathlib import PurePath
import logging

from ncdes.utils import sql_connection
from ncdes.data_ingestion.data_load import *
from ncdes.data_ingestion import amender
from ncdes.processing import processing_steps
from ncdes.processing import validation_check as check

from ncdes.data_export import outputs

from ncdes.utils.adhoc_fix import remove_problem_indicators, remove_problem_measures, remove_problem_indicator_measure_pairs
from ncdes.utils.setup import check_saved_changes, log_setup
import warnings

warnings.simplefilter(action="ignore", category=UserWarning)
pd.options.mode.chained_assignment = None

warnings.simplefilter(action="ignore", category=UserWarning)
pd.options.mode.chained_assignment = None

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

    #check that all changes made are saved and have indeed been updated ready to run - important for test runs
    check_saved_changes()

    #load config parameters
    print("\n"*3,"Loading config file")
    config = get_config('config.toml')
    config_conn = config['Connections']
    config_fp = config['Filepaths']
    root_directory = config_fp["root_directory"]
    test_run = config['Setup']['test_mode']
    pub_date = config['Dates']['Publication_date']


    output_directory = outputs.test_run_change_outputs_fldr(test_run, root_directory)
    log_output_directory = PurePath(output_directory, "Logs")
    # set test or live outputs folder and create directory where this does not already exist
    if not os.path.exists(str(log_output_directory)):
            os.makedirs(str(log_output_directory))

    # logger setup
    log_setup(log_output_directory, pub_date)

    #input logger messages around this run
    logging.info(f"Test mode established as: {test_run} \n\n\n")
    logging.info(f'Outputs will be written to the following folder: {output_directory}.')

    logging.info("Establishing SQL connection")
    connection = sql_connection.connect(server=config_conn["server"], database=config_conn["database"])

    logging.info("Loading NCDes data")
    ncdes_raw_filepath = PurePath(root_directory, 'Input', 'Current')
    ncdes_raw = load_csvs_in_directory_as_concat_dataframe(ncdes_raw_filepath)
    ncdes_raw_archive = ncdes_raw.copy(deep=True)

    logging.info("Amending CQRS data")
    ncdes_raw = amender.update_dataframe(ncdes_raw, config)

    logging.info("Cleaning NCDes data")
    ncdes_clean = processing_steps.clean_ncdes(ncdes_raw)
    reporting_period = processing_steps.get_formatted_reporting_end_date_from_ncdes_data(ncdes_clean)

    geo_ccg_sql_str, geo_reg_sql_str, stp_sql_str, prac_sql_str = get_sql_query_strings(reporting_period)

    logging.info("Loading SQL mapping data")
    geo_ccg_df = pd.read_sql(sql=geo_ccg_sql_str, con=connection)
    geo_reg_df = pd.read_sql(sql=geo_reg_sql_str, con=connection)
    stp_df = pd.read_sql(sql=stp_sql_str, con=connection)
    prac_df = pd.read_sql(sql=prac_sql_str, con=connection)

    logging.info("Formatting SQL mapping data")
    geo_ccg_df, geo_reg_df, stp_df = processing_steps.sql_df_cols_to_upper_case(geo_ccg_df, geo_reg_df, stp_df)

    logging.info("Loading ePCN data")
    raw_epcn = load_epcn_excel_table(epcn_path=config_fp["epcn_path"])
    epcn_df = processing_steps.epcn_transform(raw_epcn)

    logging.info("Creating mapping table")
    mapping_table = processing_steps.create_mapping_table(geo_ccg_df, geo_reg_df, stp_df, prac_df, epcn_df)

    logging.info("Merging NCDes data with mapping data")
    NCDes_with_geogs = processing_steps.merge_tables_fill_Na_reorder_cols(mapping_df=mapping_table, ncdes_df_cleaned=ncdes_clean, CORRECT_COLUMN_ORDER_NCDes_with_geogs=CORRECT_COLUMN_ORDER_NCDes_with_geogs)
    
    logging.info("Starting validation checks")
    check.run_all_column_has_expected_values_validations(NCDes_with_geogs, root_directory)

    logging.info("Applying suppression")
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

    #logging.info(f"Removing problem indicators: {str(config["Indicators"]["removal_indicator_list"])}")
    logging.info("Removing problem indicators")
    NCDes_problem_ind_rem = remove_problem_indicators.remove_indicators(NCDes_suppressed,  removal_indicator_list=config["Indicators"]["removal_indicator_list"])
    logging.info("Removing problem measures")
    NCDes_problem_meas_rem = remove_problem_measures.remove_measures(NCDes_problem_ind_rem, bad_measure_list=config["Measures"]["bad_measure_list"])

    logging.info("Removing problem indicator-measure combinations if required")
    NCDes_problem_meas_rem = remove_problem_indicator_measure_pairs.remove_pairs(NCDes_problem_ind_rem, bad_indicator_measure_list=config["Pairs"]['remove_ind_pair'])
    NCDes_problem_meas_rem = remove_problem_indicator_measure_pairs.remove_pairs(NCDes_problem_meas_rem, bad_indicator_measure_list=config["Pairs"]['remove_meas_pair'])
    
    logging.info("Joining ruleset ID to copy of output data for ruleset-specific outputs")
    NCDes_with_rulesets = processing_steps.merge_data_with_ruleset_id(NCDes_problem_meas_rem, root_directory)
    
    logging.info("Saving main output")
    outputs.save_NCDes_main_to_csv(NCDes_problem_meas_rem, output_directory)

    logging.info("Zipping main output")
    outputs.save_NCDes_main_to_zip(NCDes_problem_meas_rem, output_directory)

    logging.info("Saving outputs split by ruleset")
    outputs.save_NCDes_by_ruleset_to_csvs(NCDes_with_rulesets, output_directory)

    logging.info("Zipping outputs split by ruleset")
    outputs.save_NCDes_by_ruleset_to_zip(NCDes_with_rulesets, output_directory)
    
    logging.info("Trend monitor updates")
    if test_run.lower() == "true":
        logging.info("Test mode: skipping trend monitor")
    else:
        outputs.save_trendmonitor(NCDes_problem_meas_rem, root_directory)

    logging.info("Saving LDHC excel output")
    outputs.save_NCDes_main_to_excel(NCDes_problem_meas_rem, root_directory, server=config_conn["server"], database=config_conn["database"], test_run=test_run)

    logging.info("Archiving input")
    outputs.archive_input(output_directory)

    logging.info("Deleting input files from input folder")
    outputs.remove_files_from_input_folder(ncdes_raw_filepath)

    logging.info("Job complete")
    outputs.open_outputs(NCDes_problem_meas_rem, output_directory)


if __name__ == "__main__":
    main()
