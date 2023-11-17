import pyodbc as dbc
import pandas as pd

def get_practice_map(server, database):
    """
    Uses SQL tables to get most up to date practice mapping, and returns it as a dataframe.
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
    
    ach_date_query = "SELECT MAX(EXTRACT_DATE) AS ACHIEVEMENT_DATE FROM dbo.GP_PATIENT_LIST"
    
    
    df = pd.read_sql_query(ach_date_query,conn)
    ## Need to extract ach_date to use for calls below and add " " to use the date in SQL query
    ach_date = "%s" % df.ACHIEVEMENT_DATE[0]
        
            
    practice_mapping_query = """SELECT a.[CODE] AS PRACTICE_CODE
                                     , a.[NAME] AS PRACTICE_NAME
                                     , a.[COMMISSIONER_ORGANISATION_CODE] AS SUB_ICB_LOCATION_CODE
                                     , a.[HIGH_LEVEL_HEALTH_GEOGRAPHY] AS ICB_CODE
                                     , a.[NATIONAL_GROUPING] AS COMM_REGION_CODE
                                FROM [ODS_PRACTICE_V02] as a
                                WHERE a.OPEN_DATE <= ? 
                                AND (a.CLOSE_DATE IS NULL OR a.CLOSE_DATE >= ?)
                                AND a.DSS_RECORD_START_DATE <= ?
                                AND (a.DSS_RECORD_END_DATE IS NULL OR a.DSS_RECORD_END_DATE >= ?)
                                AND a.CODE IN (SELECT DISTINCT PRACTICE_CODE FROM [GP_PATIENT_LIST] WHERE EXTRACT_DATE = ?)
                                ORDER BY a.CODE"""
    
    practice_mapping = pd.read_sql_query(practice_mapping_query, conn, params=(ach_date,)*5)
    
    practice_mapping_query = """SELECT DH_GEOGRAPHY_CODE, DH_GEOGRAPHY_NAME
                           FROM ONS_CHD_GEO_EQUIVALENTS
                           WHERE ENTITY_CODE = 'E38' AND IS_CURRENT = 1
                             """
    sub_icb_map = pd.read_sql_query(practice_mapping_query, conn)
    sub_icb_map = sub_icb_map.rename(columns={"DH_GEOGRAPHY_CODE":"SUB_ICB_LOCATION_CODE","DH_GEOGRAPHY_NAME":"SUB_ICB_NAME"})
    practice_mapping = practice_mapping.merge(sub_icb_map, on="SUB_ICB_LOCATION_CODE", how="left")
    
    
    practice_mapping_query = """SELECT DH_GEOGRAPHY_CODE, DH_GEOGRAPHY_NAME
                           FROM ONS_CHD_GEO_EQUIVALENTS
                           WHERE ENTITY_CODE = 'E54' AND IS_CURRENT = 1
                             """
    icb_map = pd.read_sql_query(practice_mapping_query, conn)
    icb_map = icb_map.rename(columns={"DH_GEOGRAPHY_CODE":"ICB_CODE","DH_GEOGRAPHY_NAME":"ICB_NAME"})
    practice_mapping = practice_mapping.merge(icb_map, on="ICB_CODE", how="left")
    
    practice_mapping_query = """SELECT DH_GEOGRAPHY_CODE, DH_GEOGRAPHY_NAME
                           FROM ONS_CHD_GEO_EQUIVALENTS
                           WHERE ENTITY_CODE = 'E40' AND IS_CURRENT = 1
                             """
    region_map = pd.read_sql_query(practice_mapping_query, conn)
    region_map = region_map.rename(columns={"DH_GEOGRAPHY_CODE":"COMM_REGION_CODE","DH_GEOGRAPHY_NAME":"REGION_NAME"})
    practice_mapping = practice_mapping.merge(region_map, on="COMM_REGION_CODE", how="left")
    
    
    practice_mapping = practice_mapping[["PRACTICE_CODE","PRACTICE_NAME","SUB_ICB_LOCATION_CODE","SUB_ICB_NAME",
                                         "ICB_CODE","ICB_NAME", "COMM_REGION_CODE","REGION_NAME"]]
    
    practice_mapping = practice_mapping.rename(columns={"SUB_ICB_LOCATION_CODE":"Sub ICB Location ODS Code",
                                                       "SUB_ICB_NAME":"Sub ICB Location Name",
                                                       "ICB_CODE":"ICB ODS Code",
                                                       "ICB_NAME": "ICB Name",
                                                       "COMM_REGION_CODE":"Region ODS Code",
                                                       "REGION_NAME":"Region Name"})
    return practice_mapping
