#! /usr/bin/env python

############################################################################################################################################################################################################################################################
####################################################################### Written by Peng Yang, Silver Lake Resources Limited Sugar Zone Mine Engineering Student, Summer 2023 (May - August) ################################################################
####################################################################### Purpose: Improved automation of data entry from Weekly Tracker to SLR Tracker ######################################################################################################
############################################################################################################################################################################################################################################################

# Import libraries
import sys
import subprocess
import os
from datetime import datetime as dt

try:
  import pandas as pd
except ImportError or ModuleNotFoundError:
  subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'pandas'])
  import pandas as pd

try:
  import numpy as np
except ImportError or ModuleNotFoundError:
  subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'numpy'])
  import numpy as np

try:
  import xlsxwriter 
except ImportError or ModuleNotFoundError:
  subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'xlsxwriter'])
  import xlsxwriter
  
try:    
  import calweek 
except ImportError or ModuleNotFoundError:
  subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'calweek'])
  import calweek 
 
def csv_import(csv_file: 'UTF-8 csv file') -> 'Pandas dataframe':
  '''
  Description:
    Imports Weekly Tracker CSV file

  Arguments:
    csv_file: The uploaded csv file

  Return:
    df: The imported csv as a Pandas dataframe

  '''
  fname = csv_file

  # Read CSV file as data table
  with open(fname, 'r') as f:
    try:
      df = pd.read_csv(f, header = None, low_memory = False, encoding = 'utf-8') # Import csv as data table
    except:
      print('Please check to see if csv file name is correct and is in UTF-8 encoding')
    else:
      print('CSV import successful\n')

  # Remove columns and rows with no entries
  df.dropna(axis = 'index', how = 'all', inplace = True)
  df.dropna(axis = 'columns', how = 'all', inplace = True)
  if 'ï»¿' in df[0].unique():
    df.drop(labels = 0, inplace = True, axis = 'columns') # Removes the column containing UTF-8 BOM, which doesn't appear to be an issue in Google Colab

  # Dataframe information
  # df.head()

  return df

def col_adjust(df_raw: 'Pandas Dataframe', discip_names: list = ['DEVELOPMENT', 'ORE HAULAGE', 'WASTE HAULAGE', 'BACKFILLING', 'REMOTE MUCKING', 'LONGHOLE'], activ_names: list = ['WASTE DEVELOPMENT', 'SILL DEVELOPMENT', 'TRUCKING', 'STOPE', 'BLASTING', 'DRILLING']):
  '''
    Description:
      Adjust the columns by inserting or renaming

    Arguments:
      df_raw: The raw dataframe
      discip_names: The options for discipline
      activ_names: The options for activity

    Return:
      df: The column adjusted dataframe
  '''
  # Create new columns
  df = df_raw.copy() 
  del df_raw
  df.insert(loc = 0, column = 'Discipline', value = np.NaN) # Create a column for discipline and fill with NaN
  df.insert(loc = 1, column = 'Activity', value = np.NaN) # Create a column for activity and fill with NaN

  # Change names of existing columns
  heading_col = df.columns[ df.applymap(lambda word: str(word).lower() in 'heading').any() ][0] # Find the column containing the word Headings
  df.rename(columns = {heading_col : 'Location'}, inplace = True) # Rename column containing headings
  act_col = df.columns[ df.applymap(lambda word: str(word).lower() in 'actual').any() ][0] # Find the column containing the word Actual
  df.rename(columns = {act_col: 'Actual/Planned'}, inplace = True) # Rename column containing planned or actual

  # Set the other columns to the corresponding date
  row_dates = df[ df.applymap(lambda word: str(word).lower() in 'thursday').any(axis = 'columns') ].index[0] - 1 # The row above the days of the week, which correspond to the dates
  df.loc[row_dates] = df.loc[row_dates].fillna(method = 'ffill') # Fill adjacent NaN cells as the dates in the row containing the dates
  row_dates = df.loc[row_dates, act_col+1:act_col+15] # The dates in the row containing the dates
  orig_date_cols = [col for col in range(act_col+1, act_col+15)] # The original column names that contain the dates
  new_date_cols = [ dt.strptime(date, '%d-%b-%y').strftime('%Y-%m-%d') + ' D/S' if orig_date_col % 2 == 0 else dt.strptime(date, '%d-%b-%y').strftime('%Y-%m-%d') + ' N/S' for date, orig_date_col in zip(row_dates, orig_date_cols) ] # The dates in YYYY-MM-DD and indicates day/night shift
  # Change the original column names to the dates
  df.rename(columns = dict( zip(orig_date_cols, new_date_cols) ), inplace = True)

  # Labels for Activity & Discipline
  disc = list( set(discip_names) )
  act = list( set(activ_names + ['WASTE TO TLO', 'WASTE TO SURFACE']) ) # Waste to TLO & Surface aren't actually in SLR tracker activity category but needed at this point

  # Copy the Activity/Discipline to the new columns and remove the original column
  orig_col = df.columns[ df.applymap(lambda word: str(word).lower() in 'development').any() ][0] # Column containing the activities/disciplines
  df['Activity'] = df['Activity'].where( ~df[orig_col].isin(act), df[orig_col], axis = 'index' ) # Add entries that match the activity list to the activity column
  df['Discipline'] = df['Discipline'].where( ~df[orig_col].isin(disc), df[orig_col], axis = 'index' ) # Add entries that match the discipline list to the discipline column
  df.drop(labels = orig_col, inplace = True, axis = 'columns') # Remove the original column

  # Dataframe information
  # df

  return df

def data_cleaning(df_in: 'Pandas Dataframe'):
  '''
    Description:
      Clean the data table such that it only contains information required for SLR tracker entries

    Arguments:
      df_in: The columns adjusted dataframe

    Outputs:
      df: The cleaned dataframe
      dates: The dates for the weekly plan

  '''
  
  # Remove everything after the last row and column containing useful data
  df = df_in.copy()
  del df_in
  last_row = df[['Actual/Planned']][df[['Actual/Planned']].applymap(lambda word: str(word) in ('Plan' or 'Actual')).any(axis = 'columns')].index[-1] # Get last useful row index
  last_col = df.columns.get_loc('Actual/Planned') + 14 # Last useful column
  df = df.loc[:last_row, df.columns[:last_col+1]] # Replace existing dataframe removing rows and columns beyond last useful ones

  # Append any additional text to Location column from adjacent right column
  col_text = df.columns.get_loc('Location') + 1 # The column name to the adjacent right of Location column
  rows_text = df[ df[[col_text]].applymap(lambda word: type(word) == str).any(axis = 'columns') ].index # Indices in adjacent column containing string text to be appended
  text_to_append = df.loc[rows_text, 'Location'].astype(str) + ' ' + df.loc[rows_text, col_text].astype(str) # New text for Location column
  df.loc[rows_text, 'Location'] = text_to_append # Add new text to Location column

  # Remove the rest of the useless columns
  df.drop(labels = [c for c in range(col_text, df.columns.get_loc('Actual/Planned'))], axis = 'columns', inplace = True, errors = 'ignore')

  # Fill every NaN in Activity and Discipline columns based on adjacent entries
  df[['Discipline', 'Activity']] = df[['Discipline', 'Activity']].fillna(method = 'ffill')

  # Remove NaN entries in Location column
  df.dropna(subset = 'Location', inplace = True)

  # Remove rows in Location column containing 0's in both numerical and string forms
  df = df[ (df['Location'] != '0') & (df['Location'] != 0.0) ]

  # Remove rows in Location column containing the words heading, truck, stope, and department
  df = df[ ~(df['Location'].str.contains('truck|stope|department|heading', case = False)) ]

  # Drop Headings without any activities
  dates = list(df.columns)[-14:] # The dates (7 days of week, 2 shifts)
  df.dropna(subset = dates, thresh = 1, inplace = True)
  df.reset_index(drop = True, inplace = True) # Reset indices

  # Correct the entries in Activity column
  where_repl = ['ORE HAULAGE', 'BACKFILLING', 'REMOTE MUCKING'] # For which columns to look
  what_repl = ['SILL DEVELOPMENT', 'WASTE TO SURFACE', 'WASTE TO SURFACE'] # For what entries to look
  repl_with = ['TRUCKING', 'STOPE', 'STOPE'] # 'With what the entries should be replaced
  for where, what, word in zip(where_repl, what_repl, repl_with):
    mask = df['Discipline'] == where # Filtering to narrow down entries of interest
    df[mask] = df[mask].replace(to_replace = what, value = word)

  # Fill NaN in Actual/Planned Column with Actual (Corresponding to waste haulage and remote mucking)
  df['Actual/Planned'] = df['Actual/Planned'].fillna(value = 'Actual')

  # Dataframe information
  # df[dates].count().sum() # Check total number of non zero entries to check with excel file
  # df

  return df, dates

def SLR_Actuals(df_in: 'Pandas Dataframe', dates_shift: list, dpath: str, cols: list = ['Dates', 'Discipline', 'Activity', 'Location', 'Destination', 'Shift', 'Material', 'Quantity', 'Unit']):
  '''
  Description:
    Export data table to an excel file containing the data entries corresponding to the SLR Tracker format in the Actuals spreadsheet tab

  Arguments:
    df_in: The cleaned dataframe
    dates_shift: The dates for the weekly plan with the shift
    dpath: The directory path if using normal method
    cols: List of columns in final table

  Outputs:
    None

  '''
  df = df_in.copy()
  del df_in
  
  # Filter data table to only containing entries under Actual row
  df_act = df[ df['Actual/Planned'] == 'Actual' ]
  df_act = df_act.replace(0, np.nan, regex = True) # Replace 0's with NaN

  # Create the data table to be exported as excel file
  df_act_slr = pd.DataFrame(columns = cols)

  for date_shift in dates_shift:
    # Get all none NaN entries for a certain date and store temporarily
    df_act.loc[:, date_shift] = df_act.loc[:, date_shift].apply(lambda w: np.nan if w == ' ' else w) # Replace manually entered space with NaN
    temp = df_act[ (~df_act[date_shift].isna()) ]
    df_temp = pd.DataFrame(columns = cols) # Temporary data table to be appended to master one
    date, shift = date_shift.split()[0], date_shift.split()[1] # Separate dates and shift

    # Fill Quantity column with amount
    df_temp['Quantity'] = temp[date_shift].astype('float', errors = 'ignore')

    # Add entries in the three columns to data table
    df_temp[['Discipline', 'Activity', 'Location']] = temp[['Discipline', 'Activity', 'Location']]

    # Enter TLO or Surface in Destination column depending on what's labelled in Activity column
    df_temp['Destination'] = df_temp['Destination'].where(~df_temp['Activity'].str.contains('TLO', case = False), 'TLO')
    df_temp['Destination'] = df_temp['Destination'].where(~df_temp['Activity'].str.contains('SURFACE', case = False), 'Surface')

    # Update the Activity column to Trucking for waste haulages
    df_temp['Activity'] = df_temp['Activity'].where(~df_temp['Activity'].isin(['WASTE TO TLO', 'WASTE TO SURFACE']), 'TRUCKING')

    # Fill the Date column with the correct date
    df_temp['Dates'].fillna(value = dt.strptime(date, '%Y-%m-%d'), inplace = True)

    # Fill the Shift column with the correct shift
    df_temp['Shift'] = shift

    # Fill Material column with the correct material type
    df_temp['Material'].fillna(value = 'ORE', inplace = True)
    df_temp['Material'] = df_temp['Material'].where(~df_temp['Activity'].str.contains('WASTE'), 'WASTE')
    df_temp['Material'] = df_temp['Material'].where(~df_temp['Discipline'].str.contains('BACKFILL'), 'BACKFILL')

    # Fill Unit column with correct units
    df_temp['Unit'].fillna(value = 't', inplace = True)
    df_temp['Unit'] = df_temp['Unit'].where(~df_temp['Activity'].str.contains('DRILLING|DEVELOPMENT', regex = True), 'm')

    # Append temporary table to master table
    df_act_slr = pd.concat([df_act_slr, df_temp])
    print('Added {} data to Actuals table'.format(date_shift))


  # Excel file name
  year, week = dt.strptime(dates_shift[0].split()[0], '%Y-%m-%d').isocalendar()[0], calweek.weeknum(dt.strptime(dates_shift[0].split()[0], '%Y-%m-%d')) # Based on starting date of the week
  fname = 'Weekly{}_{}_actuals.xlsx'.format(year, week)

  # Export as excel file (May require downloading)
  if dpath is not None:
    fname = os.path.join(dpath, fname)
  with pd.ExcelWriter(fname, date_format = 'yyyy-mm-dd', datetime_format = 'yyyy-mm-dd', engine_kwargs = {'options': {'strings_to_numbers': True}}) as writer:
    df_act_slr.to_excel(excel_writer = writer, index = False)
  print('Actuals Export Completed for {}\n'.format(fname))

  # Dataframe information
  # df_act_slr

  return

def SLR_Planned(df_in: 'Pandas Dataframe', dates_shift: list, dpath: str, cols: list = ['Dates', 'Plan', 'Activity', 'Discipline', 'Location', 'Shift', 'Material', 'Quantity', 'Unit']):
  '''
  Description:
    Export data table to an excel file containing the data entries corresponding to the SLR Tracker format in the Planning spreadsheet tab

  Arguments:
    df_in: The cleaned dataframe
    dates_shift: The dates for the weekly plan with the shift
    dpath: The directory path if using normal method
    cols: List of columns in final table

  Outputs:
    None

  '''
  df = df_in.copy()
  del df_in
  
  # Filter data table to only containing entries under Plan row
  df_plan = df[ df['Actual/Planned'] == 'Plan' ]
  df_plan = df_plan.replace(0, np.nan, regex = True) # Replace 0's with NaN

  # Create the data table to be exported as excel file
  df_plan_slr = pd.DataFrame(columns = cols)

  for date_shift in dates_shift:
    # Get all none NaN entries for a certain date and store temporarily
    df_plan.loc[:, date_shift] = df_plan.loc[:, date_shift].apply(lambda w: np.nan if w == ' ' else w) # Replace manually entered space with NaN
    temp = df_plan[ (~df_plan[date_shift].isna()) ]
    df_temp = pd.DataFrame(columns = cols) # Temporary data table to be appended to master one  
    date, shift = date_shift.split()[0], date_shift.split()[1] # Separate dates and shift

    # Fill Quantity column with amount
    df_temp['Quantity'] = temp[date_shift].astype('float', errors = 'ignore')

    # Add entries in the three columns to data table
    df_temp[['Discipline', 'Activity', 'Location']] = temp[['Discipline', 'Activity', 'Location']]

    # Update the Activity column to Trucking for waste haulages
    df_temp['Activity'] = df_temp['Activity'].where(~df_temp['Activity'].isin(['WASTE TO TLO', 'WASTE TO SURFACE']), 'TRUCKING')

    # Fill the Date column with the correct date
    df_temp['Dates'].fillna(value = dt.strptime(date, '%Y-%m-%d'), inplace = True)

    # Fill the Shift column with the correct shift
    df_temp['Shift'] = shift

    # Fill Material column with the correct material type
    df_temp['Material'].fillna(value = 'ORE', inplace = True)
    df_temp['Material'] = df_temp['Material'].where(~df_temp['Activity'].str.contains('WASTE'), 'WASTE')
    df_temp['Material'] = df_temp['Material'].where(~df_temp['Discipline'].str.contains('BACKFILL'), 'BACKFILL')

    # Fill Unit column with correct units
    df_temp['Unit'].fillna(value = 't', inplace = True)
    df_temp['Unit'] = df_temp['Unit'].where(~df_temp['Activity'].str.contains('DRILLING|DEVELOPMENT', regex = True), 'm')

    # Append temporary table to master table
    df_plan_slr = pd.concat([df_plan_slr, df_temp])
    print('Added {} data to Planned table'.format(date_shift))


  # Excel file name
  year, week = dt.strptime(dates_shift[0].split()[0], '%Y-%m-%d').isocalendar()[0], calweek.weeknum(dt.strptime(dates_shift[0].split()[0], '%Y-%m-%d')) # Based on starting date of the week
  fname = 'Weekly{}_{}_planned.xlsx'.format(year, week)

  # Add the Plan name to Plan Column
  df_plan_slr['Plan'] = 'Weekly{}_{}'.format(year, week)

  # Export as excel file 
  if dpath is not None:
    fname = os.path.join(dpath, fname)
  with pd.ExcelWriter(fname, date_format = 'yyyy-mm-dd', datetime_format = 'yyyy-mm-dd', engine_kwargs = {'options': {'strings_to_numbers': True}}) as writer:
    df_plan_slr.to_excel(excel_writer = writer, index = False)
  print('Planned Export Complete for {}\n'.format(fname))

  # Dataframe information
  # df_act_plan

  return

while True:
  print('Please enter full folder directory path containing the csv file: ')
  dir_path = input()

  print('Please enter csv file name: ')
  file_name = input()
  
  if '.csv' not in file_name:
    file_name = file_name + '.csv'
  try:  
    wkly_tracker_csv = os.path.join(dir_path, file_name)
  except:
    ('Issue with directory path or file name. Please try again.')
    continue
  break

df_cleaned, dates_and_shifts = data_cleaning(col_adjust(csv_import(csv_file = wkly_tracker_csv)))
SLR_Actuals(df_cleaned, dates_and_shifts, dpath = dir_path)
SLR_Planned(df_cleaned, dates_and_shifts, dpath = dir_path)

print('\nAll Completed. Created files are located in the same folder as the csv files.')
print('The resulting tables in the created excel files can be copied and pasted directly to the SLR Tracker.\n')

print('Code written by Peng Yang (August, 2023)\n')
print('Press enter to exit')
input()
exit()

