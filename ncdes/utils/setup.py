from tkinter import *
import tkinter.messagebox
import logging
from datetime import datetime as dt
from pathlib import PurePath
import sys
import os
 

def check_saved_changes(): 
    root = Tk()
    
    answer = tkinter.messagebox.askquestion('Pre-run check',
                                            'Have you saved all changes? ' +
                                            'Particulary in the config.toml?')
    
    if answer == 'Yes':
        logging.info('User has accepted they have saved.',
                                    'Script will now run.')
        #tkinter.messagebox.showinfo('message')
        #activate BackdoorLogin.py
    
    elif answer == 'No':
        logging.info('User has accepted they have NOT saved.',
               'Script will not run. Please save and try again.')
        #tkinter.messagebox.showinfo('message')

    #automatically closes window after clicking a button    
    root.destroy() 


    #---------------------------------------------------------------------------------------------

def log_setup(log_loc, pub_date):
    """
    Function Actions:
    - Set up logging file with script name and timestamp.
    - Formats the log message as 'info'.
    - Saves into the cwd.
    """
    global logger

    #remove all current existing handlers
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
        
    #define time, log name and filepath with name for log
    timestamp = str(dt.today()).replace(":",".")[:-10]
    log_name = f"NCDes_Publication_log_{pub_date}_{timestamp}.log"
    filename = PurePath(log_loc, log_name)
    
    #setup logging configuration
    logging.basicConfig(#filename=filename
                        encoding='utf-8'
                        #, filemode='a'
                        , level=logging.DEBUG
                        , format='%(asctime)s %(levelname)s:%(name)s | %(message)s' #format of text in logger
                        , datefmt='%H:%M:%S'
                        , handlers=[
                            logging.FileHandler(filename)       #first handler to print log message log file
                            ,logging.StreamHandler(sys.stdout)  #second handler to print log message to screen 
                        ])
    
    #set up logger and write a message to it
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    
    #print brief message in terminal
    logging.info(f"{log_name} created")


