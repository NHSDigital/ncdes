import pyodbc as dbc
import pandas as pd
import logging

def connect(server, database, driver='SQL SERVER', trusted_connection=True):
    """
    Input:
        driver: driver name 
        server: server name 
        database: database name 
    
    Output:
        returns connection variable
    
    """
    logging.info('Connecting via trusted connection')
    connection = dbc.connect('DRIVER='+driver+'; SERVER='+server+'; DATABASE='+database+';TRUSTED_CONNECTION='+str(trusted_connection))
    
    return connection


