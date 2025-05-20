import pandas as pd
import logging
from typing import List, Tuple


def remove_pairs(NCDes_suppressed: pd.DataFrame, config: dict) -> pd.DataFrame:
    """Removes the rows associated with the indicator, measure combos listed in the indicator_measure list 
    from the input dataframe.

    combinations currently being removed:

    None

    Args:
        NCDes_suppressed (pd.DataFrame): Suppressed output
        config dictionary

    Returns:
        pd.DataFrame: Original dataframe with the bad indicator measure combinations removed
    """ 
    #Sets bad_indicator_measure_list to a dictionary of the indicator-measure pairs listed in the config file, if an error occurs sets bad_indicator_measure_list to an empty dictionary
    try:
        bad_indicator_measure_list = config["Pairs"]['remove_ind_pair']
    except Exception:
        bad_indicator_measure_list = {}

    for indicator, measure in bad_indicator_measure_list.items(): #updated to a dictionary, so multiple runs can be made to the same df if required
        NCDes_suppressed = NCDes_suppressed[(NCDes_suppressed.IND_CODE != indicator) | (NCDes_suppressed.MEASURE != measure)]

    if len(bad_indicator_measure_list) == 0: 
        logging.info("No measure indicators combos removed.")
    else:    
        logging.info(f"Removed measure indicator combos(s): {bad_indicator_measure_list}")
    
    return NCDes_suppressed