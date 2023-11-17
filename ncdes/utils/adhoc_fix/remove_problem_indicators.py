import pandas as pd

def remove(NCDes_suppressed: pd.DataFrame, bad_indicator_list: list) -> pd.DataFrame:
    """Removes the rows associated with the indicatorslisted in the indicator list 
    from the input dataframe.

    Indicators currently being removed:

    NCD015
    NCD015 is one of the IIF indicators, CVD-02/NCD015 – “Percentage of registered patients on the QOF Hypertension Register”. 
    This data comes from QOF and it doesn’t directly feed any indicators but instead is used by CQRS for prevalence adjustments for NCD011 in some way
    This data needs removing from the publication as it isn't collected as part of NCD and is added in by CQRS which we only get sent when there's extraction delays.

    NCD026
    its a non GPES indicator referring to the GPAD 2 week wait and needs to be removed

    Args:
        NCDes_suppressed (pd.DataFrame): Suppressed output
        bad_indicator_list (list): A list of indicators to remove

    Returns:
        pd.DataFrame: input df with bad indicators removed
    """    
    NCDes_bad_inds_removed = NCDes_suppressed[~NCDes_suppressed["IND_CODE"].isin(bad_indicator_list)]
    print(f"Removed indicator(s): {bad_indicator_list}")
    return NCDes_bad_inds_removed