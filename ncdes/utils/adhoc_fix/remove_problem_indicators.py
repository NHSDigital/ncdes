import pandas as pd

def remove_indicators(NCDes_suppressed: pd.DataFrame, removal_indicator_list: list) -> pd.DataFrame:
    """
    Removes the rows associated with the indicators listed in the indicator list 
    from the input dataframe.

    Args:
    NCDes_suppressed (pd.DataFrame): Suppressed output
    removal_indicator_list (list): A list of indicators to remove

    Returns:
        pd.DataFrame: input df with listed indicators removed
    """
    #this returns a df with all rows that DO NOT contain the IND_CODE found in the removal_indicators_list    
    NCDes_bad_inds_removed = NCDes_suppressed[~NCDes_suppressed["IND_CODE"].isin(removal_indicator_list)]

    if len(NCDes_bad_inds_removed) == len(NCDes_suppressed): #if saying  =0 then saying that all the original df had items of IND_CODE within the removals list...
        print("No indicators removed.")
    else:
        print(f"Removed indicator(s): {removal_indicator_list}")
        
    return NCDes_bad_inds_removed