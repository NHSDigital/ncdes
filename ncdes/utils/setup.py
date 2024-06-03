from tkinter import *
import tkinter.messagebox
import sys
 

def check_saved_changes(): 
    root = Tk()
    
    answer = tkinter.messagebox.askquestion('Pre-run check',
                                            'Have you saved all changes? ' +
                                            'Particulary in the config.toml?')
    
    if answer == 'Yes':
        print('User has accepted they have saved.',
                                    'Script will now run.')
        #tkinter.messagebox.showinfo('message')
        #activate BackdoorLogin.py
    
    elif answer == 'No':
        print('User has accepted they have NOT saved.',
               'Script will not run. Please save and try again.')
        #tkinter.messagebox.showinfo('message')

    #automatically closes window after clicking a button    
    root.destroy() 
