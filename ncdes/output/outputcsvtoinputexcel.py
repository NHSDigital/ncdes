import pandas as pd
from ..output.practice_map_from_sql import *

def pivot_indicator(df : pd.DataFrame, dfbase: pd.DataFrame) -> pd.DataFrame:
    """This recursive function pivots all the indicators given and measures as columns rather
    than a grouped indicator and measure column.
    
    Parameters:
    df : The output csv dataframe which has been pre processed
    dfbase: The stripped dataframe to attach the new pivoted column on

    Returns:
    dfpivoted : Pivoted dataframe"""
    
    #If the indicator contains only 1 measure
    if len(df["MEASURE"].unique()) == 1:
        
        #Drop measure column and pivot with practice_code, then reattach to dfbase.
        df = df.drop(["MEASURE"], axis = 1)
        df_pivot = df.pivot(index=["PRACTICE_CODE"], columns="IND_CODE", values="VALUE")
        df_pivot = df_pivot.reset_index()
        dfpivoted = dfbase.merge(df_pivot, on="PRACTICE_CODE", how="left")
      
    #If the indicator doesnt contain 1 measure, strip measures, rerun function and join
    else:
        
        #start with dfbase to add pivoted columns on
        dfpivoted = dfbase
        
        #loop through each measure and pivot
        for measure in df["MEASURE"].unique():
            df2 = df[df["MEASURE"] == measure]
            dfpivoted = pivot_indicator(df2, dfpivoted)
            
            #Rename the column as the measure 
            column_name_str = measure
            dfpivoted = dfpivoted.rename(columns={dfpivoted.columns[-1] : column_name_str })
     
    #Drop duplicate rows
    dfpivoted = dfpivoted.drop_duplicates()
    return dfpivoted                

#Add HI01 data
def add_HI01_data(input_df, df_base):
    """
    Add the HI01 numerator column on data
    Parameters
    input_df : the orginal csv dataframe
    df_base : dataframe you want to attach HI01 data to
    Returns
    df : A cleaned and processed dataframe
    """
    df = input_df[input_df.IND_CODE.isin(["HI01"])]
    df = df[df.MEASURE == "Numerator"]
    df = df.groupby("PRACTICE_CODE")["VALUE"].sum().reset_index()
    df = df.rename(columns = {"VALUE":"HI01Numerator"})
    
    df = df_base.merge(df, how="left", on="PRACTICE_CODE")
    
    return df
    
#Stage 1, pivot data and clean
def get_data(input_df : pd.DataFrame) -> pd.DataFrame:
    """
    Pivot the indicators and clean data
    Parameters
    input_df : main NCDes dataframe
    Returns
    df : A cleaned and processed dataframe
    """
    #Reduce dataframe to required indicators
    df = input_df[input_df.IND_CODE.isin(["HI03"])]
    df = df[df.MEASURE.isin(["Numerator", "Denominator"])]
    
    #Strip out the date for further use. If multiple dates, then print Error
    if len(df["ACH_DATE"].unique()) == 1:
        ach_date = df["ACH_DATE"].unique()[0]
        ach_date = str(ach_date)
    else:
        print("ERROR: Multiple different values in ACH_DATE")
      
    #Drop unneccesary columns
    df = df.drop(["ACH_DATE", "QUALITY_SERVICE"], axis=1)
    
    #Pivot the table and convert new columns into ints
    df = pivot_indicator(df, df[df.columns[:2]])
    
    #get HI01 data
    df = add_HI01_data(input_df, df)

    df[["Numerator", "Denominator","HI01Numerator"]] = df[["Numerator", "Denominator","HI01Numerator"]].astype('int')
    df["No_ethnicity"] = df["HI01Numerator"] - df["Numerator"]
    df["No_ethnicity"] = df["No_ethnicity"].astype("int")
    return df, ach_date

def write_perc_col(df: pd.DataFrame, col1 : str, col2: str, new_col :str) -> pd.DataFrame:
    """
    Write a percentage column by dividing two columns
    Parameters
    df : The dataframe to write over
    col1 : Name of column to divide (numerator)
    col2 : Name of column to divide (denominator)
    new_col : Name of new column 
    Returns
    df : The processed dataframe
    """
    df[new_col] = df[col1] / df[col2]
    df[new_col] = df[new_col].astype("float") 
    df[new_col] = df[new_col].round(4)
    return df

def geog_agg(df:pd.DataFrame,dropcol:list, groupcol : list) -> pd.DataFrame:
    """
    Write a percentage column by dividing two columns
    Parameters
    df : The dataframe to geographically aggregate
    dropcol: Cols to drop when aggregating
    groupcol : Cols to keep when aggregating
    Returns
    df_agg : The aggregrated dataframe
    """
    df = df.drop(dropcol, axis=1)
    df2 = df.groupby(groupcol).sum().reset_index()
    return df2


def add_summary(df:pd.DataFrame, num_prac_cols : int) -> pd.DataFrame:
    """This section is to add a total/average column at the top of the dataframe
    Parameters:
       df: dataframe that needs summary added
       num_prac_cols : The number of practice columns 
    Returns:
        df: the orginal dataframe with summary"""

    #lets try it a different way
    dfbase = df[df.columns[:num_prac_cols]]
    dfvals = df[df.columns[num_prac_cols:]]
    
    #Fill in blank values for practice row
    result_series = ['ENGLAND']
    for i in range(num_prac_cols-1):
        result_series.append('')
    
    #Get the sums here
    for col in dfvals.columns:
        num = dfvals[col].sum()
        result_series.append(num)
          
    #append the summary, add England as first value, and move to top row  
    df2 = df.copy()
    df2.loc[len(df2)] = result_series
    df2 = pd.concat([df2.iloc[-1:], df2.iloc[:-1]], ignore_index=True)

    return df2

def produce_processed_csv(NCDes_main_df : pd.DataFrame, root_directory, server, database) -> pd.DataFrame:
    """
    Produce CSVs ready for excel
    Parameters
    NCDes_main_df : The main NCDes DataFrame (which goes to csv)
    Returns
    sub_icb_df : The sub_icb aggregated dataframe
    icb_df : The icb aggregated dataframe
    region_df : The region aggregated dataframe
    """
    #get cleaned and pivoted data
    df, ach_date = get_data(NCDes_main_df)

    #Get practice details and merge
    prac_df = get_practice_map(server, database)
    
    #check if any practices are missing
    prac_in_data = df["PRACTICE_CODE"].to_list()
    all_pracs = prac_df["PRACTICE_CODE"].to_list()
    
    unknown_pracs = [prac for prac in prac_in_data if prac not in all_pracs]
    if len(unknown_pracs) != 0:
        print(f"All missing practice's codes are {unknown_pracs}")
    else:
        print(f"They are no missing practice codes.")

    #make an unknown column
    if len(unknown_pracs) != 0:
        for i in range(len(unknown_pracs)):
            prac_df.loc[len(prac_df)] = [unknown_pracs[i], "Unknown","Unknown","Unknown","Unknown","Unknown","Unknown","Unknown"]


    prac_df = prac_df.drop(["PRACTICE_NAME"], axis = 1)
    output_df = df.merge(prac_df, how="left", on="PRACTICE_CODE")
    
    #Reorder columns.
    col_order = ['PRACTICE_CODE', 'PRACTICE_NAME','Sub ICB Location ODS Code', 'Sub ICB Location Name', 
                'ICB ODS Code', 'ICB Name', 'Region ODS Code', 'Region Name', 
                 'Denominator','Numerator',"HI01Numerator","No_ethnicity"]
    output_df = output_df[col_order]

    
    sub_icb_df = geog_agg(output_df, ['PRACTICE_CODE', 'PRACTICE_NAME'], ['Sub ICB Location ODS Code', 'Sub ICB Location Name','ICB ODS Code', 'ICB Name', 'Region ODS Code', 'Region Name'])
    icb_df = geog_agg(output_df, ['Sub ICB Location ODS Code', 'Sub ICB Location Name'],['ICB ODS Code', 'ICB Name', 'Region ODS Code', 'Region Name'])
    region_df = geog_agg(output_df, ['ICB ODS Code', 'ICB Name'], ['Region ODS Code', 'Region Name'])
    
    sub_icb_df = add_summary(sub_icb_df, 6)
    icb_df = add_summary(icb_df, 4)
    region_df = add_summary(region_df, 2)
    
    sub_icb_df = write_perc_col(sub_icb_df,'Numerator', 'Denominator', "%")
    icb_df = write_perc_col(icb_df,'Numerator', 'Denominator', "%")
    region_df = write_perc_col(region_df,'Numerator', 'Denominator', "%")
    
    
    sub_icb_df = write_perc_col(sub_icb_df, "No_ethnicity", "HI01Numerator", "2%")
    icb_df = write_perc_col(icb_df, "No_ethnicity", "HI01Numerator", "2%")
    region_df = write_perc_col(region_df, "No_ethnicity", "HI01Numerator", "2%")
    

    
    def rename_cols(df):
        
        #reorder columns
        df_first_part = df.iloc[:,:-4]
        df_last_four = df.iloc[:,-4:]
        df_last_four = df_last_four.iloc[:,[2,0,1,3]]
        df = pd.concat([df_first_part, df_last_four], axis =1)

        #rename columns
        df = df.rename(columns={'Denominator':'Denominator - total number of patients on LD Register (minus declines)',
               'Numerator':'Number of patients had health check and health action plan AND have ethnicity recorded',
               '%' : '% Health check, action plan and ethnicity recorded', 
               "HI01Numerator" : 'Number of patients had health check and health action plan', 
               "No_ethnicity" : 'Number of patients had health check and action plan NO ethnicity recorded',
               '2%' : '% Patients had health Check and action plan NO ethnicity recorded'})
        
    
        return df
    
    sub_icb_df = rename_cols(sub_icb_df)
    icb_df = rename_cols(icb_df)
    region_df = rename_cols(region_df)
    
    return sub_icb_df, icb_df, region_df, ach_date