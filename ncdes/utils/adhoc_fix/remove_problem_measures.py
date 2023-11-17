import pandas as pd

def remove(df: pd.DataFrame, bad_measure_list: list) -> pd.DataFrame:
    """Removes the rows associated with the measures listed in the measure list 
    from the input dataframe.

    Measures currently being removed:

    Num Patients in Set
    Number of patients in set probably shouldn't be sent by CQRS, as it's used internally for adjustments. 
    In reality, for the specified indicators it is identical to the denominator. 
    Rather than republishing all previous releases, have added an appropriate entry to the data dictionary

    Args:
        df (pd.DataFrame): Suppressed output
        bad_indicator_list (list): A list of indicators to remove

    Returns:
        pd.DataFrame: input df with bad indicators removed
    """    
    NCDes_bad_meas_removed = df[~df["MEASURE"].isin(bad_measure_list)]
    print(NCDes_bad_meas_removed.query("MEASURE == 'Num Patients in Set'"))
    print(f"Removed measure(s): {bad_measure_list}")
    return NCDes_bad_meas_removed