import pandas as pd
from datetime import datetime as datetime2
import openpyxl 

def clean_func(df: pd.DataFrame):
    """
    Returns the values needed for the trend monitor.
    Parameters:
    df - The NCDes_main_df dataframe
    Returns:

    """
    df = df[df.IND_CODE.isin(["NCDMI060","NCDMI136"])]
    df["VALUE"] = df["VALUE"].astype("int")
    df = df.drop(["PRACTICE_NAME","QUALITY_SERVICE"], axis=1)
    
    if df.ACH_DATE.nunique() == 1:
        date = df.ACH_DATE.unique()[0]
        #test -> date = "30/08/2023"
        date_object = datetime2.strptime(date, "%d/%m/%Y")
        month = date_object.strftime('%b %Y')
    else:
        print("Error: More than one date in dataframe")
    
    list_size_df = df[df.IND_CODE == "NCDMI060"]
    list_size_df = list_size_df[list_size_df.MEASURE == "Denominator"]
    list_size = list_size_df["VALUE"].sum()
    
    opt_out_df = df[df.IND_CODE == "NCDMI136"]
    #All measures here are Management infomation
    opt_out = opt_out_df["VALUE"].sum()
    
    perc_opt_out = round(opt_out / list_size,4)
    
    return list_size, opt_out, perc_opt_out, month


def grab_data(T_DATA) -> pd.DataFrame:
    """
    Writes over worksheet
    Parameter:
    T_DATA = Data worksheet
    Returns:
    excel_df = The data from trend monitor as a pandas dataframe
    """
    #Get data from excel
    columns = ['Month', 'Opt outs', 'List size', '% opt out', 'Count of practices']
    excel_df = pd.DataFrame(columns=columns)
    i = 0
    for row in T_DATA.iter_rows(min_row = 2, max_row=12, min_col =1, max_col = 5,values_only= True):
        if row[0] is not None:
            row = list(row)
            row[0] = pd.to_datetime(row[0])
            row[0] = row[0].strftime('%b %Y')
            excel_df.loc[i] = row
        i += 1   
    return excel_df


def check_if_copy(excel_df, month) -> int: 
    """
    Checks if the run is already in trend monitor and if it wants overrunning
    Parameters:
    excel_df : The excel dataframe
    month : Month-year for dataframe
    Returns:
    override : A number which decides which override process to run
    """
    override = 2
    if (excel_df["Month"] == str(month)).any():
        while True:
            user_input = input(f"Duplicate Month found for trend monitor ({month}), Overide? (Y/N):")
            if user_input.upper() == "Y":
                print("Continuing...")
                override = 1
                break
            elif user_input.upper() == "N":
                print("Exiting...")
                override = 0
                break
            else:
                print("Invalid input, Please enter Y or N.")
    return override    

def write_trend_monitor(NCDes_main_df: pd.DataFrame, root_directory :str, data_month:str) -> None:
    """
    Uses NCDes_main_df and updates trend monitor.
    Parameters:
    root_directory = directory of root
    NCDes_main_df - The NCDes Main datagrame
    Returns:
    None
    """

    root = root_directory
    
    list_size, opt_out, perc_opt_out, month = clean_func(NCDes_main_df)
    
    #List Size
    Count_of_prac = NCDes_main_df.PRACTICE_CODE.nunique()

    #with pd.ExcelWriter("Testexcel.xlsx", engine="openpyxl") as writer:
    wb = openpyxl.load_workbook(f"{root}\\Output\\Opt Out Tracking\\NCD_Opt_Out_Tracking.xlsx")
    wb.save(f"{root}\\Output\\Opt Out Tracking\\Archive\\NCD_Opt_Out_Tracking_{data_month}.xlsx")
    T_DATA = wb["Data"]
    excel_df = grab_data(T_DATA)
    override = check_if_copy(excel_df, month)
    
    if override == 0:
        print("Done no changes to trend monitor.")
    #Else override, THIS WILL ONLY OVERRIDE LAST ROW, ASSUMING DATA IS NEWEST
    elif override == 1:
        row_num, col_num = excel_df.shape
        excel_df.loc[row_num-1] = [month, opt_out, list_size, perc_opt_out, Count_of_prac]
    #Else continue normally
    elif override == 2:
        row_num, col_num = excel_df.shape
        excel_df.loc[row_num] = [month, opt_out, list_size, perc_opt_out, Count_of_prac]
   
    with pd.ExcelWriter(f"{root}\\Output\\Opt Out Tracking\\NCD_Opt_Out_Tracking.xlsx", engine="openpyxl") as writer:   
        writer.book = wb
        writer.sheets = dict((ws.title, ws) for ws in wb.worksheets)
        excel_df.to_excel(writer, sheet_name= "Data" ,index=False, header = False,startrow=1)

        
