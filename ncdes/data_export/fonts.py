#This file just contain fonts for excel. These are in their seperate files
#as its more cleaner and easier to read through


from openpyxl.styles import PatternFill, Border, Side, Alignment, Protection, Font, Fill
import datetime
import pandas as pd 

#Copyright font
C_FONT = Font(name='Arial',
                size=10,
                bold=False,
                italic=False,
                vertAlign=None,
                underline='none',
                strike=False,
                color='000000')

#Publication name on Title sheet
NAME_FONT = Font(name='Arial',
                size=27,
                bold=True,
                italic=False,
                vertAlign=None,
                underline='none',
                strike=False,
                color='005EB8')

#Publication date font on Title sheet
DATE_FONT = Font(name='Arial',
                size=11,
                bold=False,
                italic=False,
                vertAlign=None,
                underline='none',
                strike=False,
                color='000000')

#Message font on group sheets
MESSAGE_FONT = Font(name='Arial',
                size=10,
                bold=False,
                italic=True,
                vertAlign=None,
                underline='none',
                strike=False,
                color='000000')

LINK_FONT = Font(name='Arial',
                size=11,
                bold=False,
                italic=False,
                vertAlign=None,
                underline='single',
                strike=False,
                color='003087')

SOURCE_FONT = Font(name='Arial',
                size=9,
                bold=True,
                italic=False,
                vertAlign=None,
                underline='none',
                strike=False,
                color='000000')

BOLD_FONT = Font(name='Arial',
                size=14,
                bold=True,
                italic=False,
                vertAlign=None,
                underline='none',
                strike=False,
                color='000000')

T_BORDER = Border(left=Side(border_style='thin',color='FFFFFF'),
                    right=Side(border_style='thin',color='FFFFFF'),
                    top=Side(border_style='thin',color='FFFFFF'),
                    bottom=Side(border_style='thin',color='BFBFBF'))