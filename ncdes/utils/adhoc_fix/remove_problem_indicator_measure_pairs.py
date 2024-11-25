import pandas as pd
import logging
from typing import List, Tuple


def remove_pairs(NCDes_suppressed: pd.DataFrame, bad_indicator_measure_list: dict) -> pd.DataFrame:
    """Removes the rows associated with the indicator, measure combos listed in the indicator_measure list 
    from the input dataframe.

    combinations currently being removed:

    NCDMI198 Numerator
    The NCDMI198 numerator is sourced from GPAD. This means that for some practices we only get this information. The 
    decision was therefore taken to remove this indicator measure combination from the publication

    Args:
        NCDes_suppressed (pd.DataFrame): Suppressed output
        bad_indicator_measure_list (List[Tuple[str, str]]): (indicator_name, measure_name) List containing tuples of the bad indicator measure combinations to be removed

    Returns:
        pd.DataFrame: Original dataframe with the bad indicator measure combinations removed
    """    
    for indicator, measure in bad_indicator_measure_list.items(): #updated to a dictionary, so multiple runs can be made to the same df if required
        NCDes_suppressed = NCDes_suppressed[(NCDes_suppressed.IND_CODE != indicator) | (NCDes_suppressed.MEASURE != measure)]

    if len(bad_indicator_measure_list) == 0: 
        logging.info("No measure indicators combos removed.")
    else:    
        logging.info(f"Removed measure indicator combos(s): {bad_indicator_measure_list}")
    
    return NCDes_suppressed