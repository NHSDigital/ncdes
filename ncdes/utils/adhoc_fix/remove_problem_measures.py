import pandas as pd
import logging

def remove_measures(df: pd.DataFrame, bad_measure_list: list) -> pd.DataFrame:
    """Removes the rows associated with the measures listed in the measure list 
    from the input dataframe.

    Measures currently being removed:

    Num Patients in Set
    Number of patients for the specified indicators is identical to the denominator. 
    Rather than republishing all previous releases, an appropriate entry has been added to the data dictionary

    Args:
        df (pd.DataFrame): Suppressed output
        removal_indicator_list (list): A list of indicators to remove

    Returns:
        pd.DataFrame: input df with bad indicators removed
    """
    #this returns a df with all rows that DO NOT contain the MEASURE found in the bad_measure_list      
    NCDes_bad_meas_removed = df[~df["MEASURE"].isin(bad_measure_list)]

    if len(bad_measure_list) == 0:
        logging.info("No measures removed.")
    else:    
        logging.info(f"Removed measure(s): {bad_measure_list}")
    
    return NCDes_bad_meas_removed