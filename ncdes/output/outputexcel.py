import pandas as pd
import openpyxl
from datetime import datetime as datetime2
from ..output.outputcsvtoinputexcel import *
from ..output.fonts import *
from openpyxl.styles import Border, Side

def set_border(ws, df, end_col):
    df_length = len(df)
    start_cell = "A7"
    end_cell = end_col + str(7+df_length)
    cell_range = start_cell + ":" + end_cell
    
    thin = Side(border_style="thin", color="000000")
    for row in ws[cell_range]:
        for cell in row:
            cell.border = Border(top=thin, left=thin, right=thin, bottom=thin)


def main_to_excel(NCDes_main_df: pd.DataFrame, output_directory, root_directory :str, server, database, service_year) -> None:
    """
    Converts NCDes_main_df into an excel file.
    root_directory = directory of root
    Parameters:
    NCDes_main_df - The NCDes Main datagrame
    service_year (str, optional): The relevant service year of the publication being run in the form
    yy_yy. Defaults to "22_23".
    """
    sub_icb_df, icb_df, region_df, ach_date = produce_processed_csv(NCDes_main_df, root_directory, server, database)

    Control_File_NCDes = pd.read_csv(f"{root_directory}\\templates\\Control_File_NCDes.csv")
    URL = Control_File_NCDes.at[0, "Friendly URL"]

    Copyright_date = Control_File_NCDes.at[0, "PubDate"]
    Copyright_date = Copyright_date[-4:]
    Copyright = "Copyright © {}, NHS England.".format(Copyright_date)

    Source = "Source: Data source, NHS England"

    Notes = "The data presented in this report are collected as part of the Network Contract Directed Enhanced Service (NCDES) regarding whether patients with a learning disability who have had an annual health check and a health action plan have a recorded ethnicity." 
    Fyear = Control_File_NCDes.at[0, "FYEAR"]

    Pub_date = Control_File_NCDes.at[0, "PubDate"]
    Pub_date = datetime2.strptime(Pub_date, "%d/%m/%Y").date()
    Publication_date = Pub_date.strftime("%d %B %Y")
                                        
    Short_year_str = str(Fyear)
    Short_year_A = Short_year_str[2:4]
    Short_year_B = Short_year_str[5:7]
    short_year = Short_year_A+Short_year_B

    LDHC_title = "NCDES LDHC Ethnicity recording (NCDES LDHC) {}".format(Fyear)
    date_object = datetime2.strptime(ach_date, "%d/%m/%Y")
    MMMYYYY = date_object.strftime('%b %Y')
    print(f"MMMYYYY is {MMMYYYY}")
    Datasheetsname = ["Sub ICB", "ICB", "Region"]

    output_folder = f"{output_directory}\\Output\\{service_year}"
    file_name = f"\\NCDES LDHC Ethnicity Recording - {MMMYYYY}.xlsx"
    output_path = f"{output_folder}{file_name}"

    def write_title(wb):
        """
        Fills in the title page of the workbook.
        Parameters
        wb : The workbook
        Returns
        wb : The workbook
        None.
        """
        T1_TITLE = wb["Title Page"]
        
        T1_TITLE["A7"] = LDHC_title
        T1_TITLE["A7"].font = NAME_FONT
        T1_TITLE["A9"] = MMMYYYY
        T1_TITLE["B10"] = Publication_date
        T1_TITLE["B10"].font = DATE_FONT
        T1_TITLE["B11"].hyperlink = URL
        T1_TITLE["B11"].style = "Hyperlink"
        T1_TITLE["B11"].font = LINK_FONT
        T1_TITLE["A14"] = Notes
        T1_TITLE["A14"].font = DATE_FONT
        T1_TITLE["A36"] = Copyright
        T1_TITLE["A36"].font = DATE_FONT    
        
        
    def write_datasheets(wb, T1_XX_list : list, prac_counts :list):
        """
        Fills in the data pages of the workbook.
        Parameters
        wb : The workbook
        T1_XX_list : list of Datasheet names
        prac_counts = length of dataframes
        Returns
        wb : The workbook
        None.
        """
        for i in range(len(Datasheetsname)):
            
            prac_count = prac_counts[i]
            prac_count_pub = "A"+str(prac_count + 10)
            source_count_ref = "A"+str(prac_count + 7)
            source_count_pub = prac_count + 7
            
            T1_XX_list[i][prac_count_pub] = Copyright
            T1_XX_list[i][prac_count_pub].font = C_FONT
            
            #T1_SUB.title = Datasheetname
            T1_XX_list[i]["A1"] = 'Learning Disability health check, health action plan and ethnicity recording - ' + Fyear + ' - ' + MMMYYYY
            T1_XX_list[i]["A2"] = 'Learning Disability health check, health action plan and ethnicity recording - ' + Fyear
            T1_XX_list[i]["A3"] = MMMYYYY + ' Publication'
            T1_XX_list[i]["I3"].font = BOLD_FONT
            
    


    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        wb = openpyxl.load_workbook(f"{root_directory}\\templates\\template NCDES LDHC Ethnicity Recording MMM YYYY.xlsx")
        write_title(wb)
        T1_SUB = wb["Sub ICB"]
        T1_ICB = wb["ICB"]
        T1_REG = wb["Region"]
        write_datasheets(wb, [T1_SUB, T1_ICB, T1_REG], [len(sub_icb_df), len(icb_df), len(region_df)])

        
        writer.book = wb
        writer.sheets = dict((ws.title, ws) for ws in wb.worksheets)
        sub_icb_df.to_excel(writer, sheet_name= "Sub ICB" ,index=False, header = False,startrow=7)
        icb_df.to_excel(writer, sheet_name= "ICB" ,index=False, header = False,startrow=7)
        region_df.to_excel(writer, sheet_name= "Region" ,index=False, header = False,startrow=9)

        set_border(T1_SUB, sub_icb_df, "L")
        set_border(T1_ICB, icb_df, "J")


        
    