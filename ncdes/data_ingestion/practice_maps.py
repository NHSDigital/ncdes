
import pandas as pd
from datetime import datetime as datetime2
import pyodbc as dbc
import os


def code_map(server : str, database : str) -> pd.DataFrame:
    """
    Uses SQL tables to get most up to date practice to supplier mapping, and returns it as a dataframe.
    """
    conn_string = ("Driver=SQL Server;"
                   f"Server={server};" 
                   f"Database={database};" 
                   "Trusted_Connection=yes;")
    
    def connect(conn_str):
        try:
            conn = dbc.connect (conn_str)
        except:
            raise Exception("Database Connection unsuccessful")
            conn = None
        return conn
    
    conn = connect(conn_string)
    ach_date_query = "SELECT DISTINCT CODE FROM [dbo].[ODS_PRACTICE_V02] WHERE PRESCRIBING_SETTING = 4"

    df = pd.read_sql_query(ach_date_query,conn)
    return df
