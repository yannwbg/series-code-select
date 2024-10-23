import pandas as pd
import numpy as np
import re
pd.set_option('display.max_columns', None)

###Read the file
filepath_current_3 = "/Users/Danjing 1/Lingsu/Jobs/2024 WB STC/Sector/Data_original/cleaned/un3_current.csv"

df_current_3 = pd.read_csv(filepath_current_3)
print(df_current_3.head())

# Get the ios and country columns and turn into a mapping file.
iso_mapping_current_3 = df_current_3.iloc[:, :2].drop_duplicates()
#print(iso_mapping.head())
iso_mapping_current_3.to_csv("/Users/Danjing 1/Lingsu/Jobs/2024 WB STC/Sector/Process/iso_mapping_current_3.csv", index=False)

##########Step 2. Sector check
##### Transforme the original dataset

#Create a pivot table for the values, all sectors became column numbers
pivot_df_current_3 = df_current_3.pivot_table(index=['country','year','series_code'], columns='code', values = 'value')
print(pivot_df_current_3.head())

#pivot_df_current_3 = df_current_3.pivot_table(index=['iso3', 'country','year','series_code'], columns='code', values = 'value')
# When using this line of code, the results become different. Why?
# We can add the iso3 column using a mapping table later.

#Reset the index to make 'country','year','series_code' into columns
pivot_df_current_3.reset_index(inplace=True)
print(pivot_df_current_3.head())

###Data cleaning for ISICC 3 file
pivot_df_current_3 = pivot_df_current_3.drop(['01','02','1','2','60-63','64','D.21','D.21-D.31','D.31','P.119'], axis=1)
print(pivot_df_current_3.tail())
#pivot_df_current_3.to_csv("/Users/Danjing 1/Lingsu/Jobs/2024 WB STC/Sector/Process/current_3_before.csv")

# If A and B both have value, then keep them, let A+B become NA. If not, then keep A+B, let A and B both become NA. Same for G+H, J+K.

# Function to apply for that.
def update_dataframe(df, col1, col2, col_sum):
    """
    Conditionally updates the 'col_sum' column based on the non-NA status of 'col1' and 'col2'.
    If both 'col1' and 'col2' are not NA, 'col_sum' is set to NA but 'col1' and 'col2' are retained.
    """

    def apply_logic(row):
        if pd.notna(row[col1]) and pd.notna(row[col2]):
            row[col_sum] = np.nan  # Only set 'col_sum' to NaN if both are not NaN
        else:
            row[col1] = np.nan  # Set 'col1' and 'col2' to NaN if either is NaN
            row[col2] = np.nan
        return row

    return df.apply(apply_logic, axis=1)


# Apply function
pivot_df_current_3 = update_dataframe(pivot_df_current_3, 'A', 'B', 'A+B')
pivot_df_current_3 = update_dataframe(pivot_df_current_3, 'G', 'H', 'G+H')
pivot_df_current_3 = update_dataframe(pivot_df_current_3, 'J', 'K', 'J+K')

def update_dataframe3(df, col1, col2, col3, col_sum):
    """
    Conditionally updates the 'col_sum' column based on the non-NA status of 'col1' and 'col2' and 'col3'.
    If both 'col1' and 'col2' and 'col3' are not NA, 'col_sum' is set to NA but 'col1' and 'col2' and 'col3' are retained.
    """

    def apply_logic3(row):
        if pd.notna(row[col1]) and pd.notna(row[col2]) and pd.notna(row[col3]):
            row[col_sum] = np.nan  # Only set 'col_sum' to NaN if both are not NaN
        else:
            row[col1] = np.nan  # Set 'col1' and 'col2' to NaN if either is NaN
            row[col2] = np.nan
            row[col3] = np.nan
        return row

    return df.apply(apply_logic3, axis=1)

# Apply function
pivot_df_current_3 = update_dataframe3(pivot_df_current_3, 'M', 'N', 'O','M+N+O')
print(pivot_df_current_3.tail())

### Add columns to check if there's any sectors missing.
## First figure out if AB, GH, JK are missing
def calculate_missing_combination(df, col1, col2, col_sum, new_col_name):
    """
    Function to calculate a new column based on conditions involving two columns and their sum.

    Parameters:
    - df (DataFrame): The DataFrame to operate on.
    - col1 (str): The name of the first column.
    - col2 (str): The name of the second column.
    - col_sum (str): The name of the column representing the sum or combination of the first two columns.
    - new_col_name (str): The name of the new column to be added to indicate missing status.

    Returns:
    - DataFrame: The DataFrame with the new column added.
    """
    # Calculate conditions
    condition1 = df[col1].notna() & df[col2].notna() & df[col_sum].isna()
    condition2 = df[col_sum].notna()

    # Determine the new column should be False
    df[new_col_name] = ~(condition1 | condition2)

    return df

# Apply the function with specific column names and the new column name 'missing_AB'
pivot_df_current_3 = calculate_missing_combination(pivot_df_current_3, 'A', 'B', 'A+B', 'missing_AB')
pivot_df_current_3 = calculate_missing_combination(pivot_df_current_3, 'G', 'H', 'G+H', 'missing_GH')
pivot_df_current_3 = calculate_missing_combination(pivot_df_current_3, 'J', 'K', 'J+K', 'missing_JK')

## Then figure out if MNO are missing
def calculate_missing_combination3(df, col1, col2, col3, col_sum, new_col_name):
    """Function to calculate a new column based on conditions involving 3 columns and their sum."""
    # Calculate conditions
    condition1 = df[col1].notna() & df[col2].notna() & df[col3].notna() & df[col_sum].isna()
    condition2 = df[col_sum].notna()

    # Determine the new column should be False
    df[new_col_name] = ~(condition1 | condition2)

    return df
# Apply the function with specific column names and the new column name 'missing_AB'
pivot_df_current_3 = calculate_missing_combination3(pivot_df_current_3, 'M', 'N', 'O', 'M+N+O', 'missing_MNO')

## Then check all the other sectors: C, D, E, F, I, L. Please not sector P can be missing, so not included.
columns_to_check = ['C', 'D', 'E', 'F', 'I', 'L']
pivot_df_current_3['missing_other_exclP'] = pivot_df_current_3[columns_to_check].isna().any(axis=1)

## Combine them and get the final results  : if any sector is missing
pivot_df_current_3['missing_any_sector'] = pivot_df_current_3.iloc[:, -5:].any(axis=1)
#print(pivot_df_current_3.head(10))
#print(pivot_df_current_3.tail(10))

### Add more columns for data analysis purpose
#Aggregate the footnote colum to check for any footnotes
footnotes = df_current_3.groupby(['country','year','series_code'])['note'].agg(lambda x: ', '.join(set(y for y in x if pd.notna(y) and y != '')))

# The footnotes series_code needs to have its index reset for joining. Then add it to the pivot table.
footnotes = footnotes.reset_index()
footnotes.rename(columns={'note':'Footnotes'}, inplace=True)
pivot_df_current_3 = pivot_df_current_3.merge(footnotes, on=['country','year','series_code'], how='left')
#print(pivot_df_current_3.head())

# Add part column to make sure we get the correct footnotes information
mapping_part = df_current_3[['country','part']].drop_duplicates(subset='country')
#print(mapping_part)
pivot_df_current_3 = pd.merge(pivot_df_current_3,mapping_part,on='country',how='left')
#print(pivot_df_current_3.head())

# put all missing sectors into a new column
'''Define a function to find missing sectors (columns)'''
def missing_column(row):
    return ', '.join(row.index[row.isna()])
# Apply the function and get a new column 'sector_missing'
pivot_df_current_3['sector_missing']= pivot_df_current_3.loc[:, 'A':'P'].apply(missing_column, axis=1)
#print(pivot_df_current_3.head())

### calculate the discard score
columns_to_sum = list(range(3,6))+list(range(8,25))
pivot_df_current_3['sum_of_sector'] = pivot_df_current_3.iloc[:,columns_to_sum].sum(axis=1)
pivot_df_current_3['score'] = ((pivot_df_current_3['sum_of_sector'] - pivot_df_current_3['B.1g'])/pivot_df_current_3['B.1g']).abs()
pivot_df_current_3['discard'] = (pivot_df_current_3['score']-0.005)>=0
#pivot_df_current_3.to_csv("/Users/Danjing 1/Lingsu/Jobs/2024 WB STC/Sector/Process/current_3_try.csv")

# if b.1g is missing and b.1*g is missing, do not discard. (discard = false)
for index in range(len(pivot_df_current_3)):
    if pd.isna(pivot_df_current_3.loc[index, 'B.1g']) and pd.isna(pivot_df_current_3.loc[index, 'B.1*g']):  # Check if the value in A is NaN (or None)
        pivot_df_current_3.loc[index, 'discard'] = False     # Set B to False if A is empty

# if b.1g is missing, but b.1*g is not missing. check for footnotes of other sectors.
# If footnotes are related to tax ans subsidy add or minus, then we can still use 0.005.
# if no footnotes or footnotes not related to correct ones, we can use 10%
# To check the footnote, not only needs to check the footnote columns, also need to check the part column.
#filtered_df_1 = pivot_df_current_3[pd.isna(pivot_df_current_3['B.1g']) & pd.notna(pivot_df_current_3['B.1*g']) & pd.notna(pivot_df_current_3['Footnotes'])]
filtered_df = pivot_df_current_3[pd.isna(pivot_df_current_3['B.1g']) & pd.notna(pivot_df_current_3['B.1*g'])]

#print(filtered_df_1) #115 rows *31 columns
print(filtered_df) #115 rows *31 columns
# pd.notna(pivot_df_current_3['Footnotes'] seems not work

filtered_df.to_csv("/Users/Danjing 1/Lingsu/Jobs/2024 WB STC/Sector/Process/un3_current_b1g.csv")
# Need to check manually for the footnote.

# Went through the file and did not see the related footnote. So will use b1*g and 10%.
# Also, when sum_of_sector equals to zero, discard.
pivot_df_current_3['score*'] = None
for index in range(len(pivot_df_current_3)):
    pivot_df_current_3['score*'] = (
            (pivot_df_current_3['sum_of_sector'] - pivot_df_current_3['B.1*g']) / pivot_df_current_3[
        'B.1*g']).abs()
    if pd.isna(pivot_df_current_3.loc[index, 'B.1g']) and pd.notna(pivot_df_current_3.loc[index, 'B.1*g']):
        pivot_df_current_3.loc[index, 'discard'] = (pivot_df_current_3.loc[index, 'score*'] - 0.1) >= 0
    if pivot_df_current_3.loc[index, 'sum_of_sector'] ==0:  # meaning no sector value at all. This only happens in ISICC3 files.
        pivot_df_current_3.loc[index, 'discard'] = True

#print(pivot_df_current_3.tail(10))

###FISIM
## Add a column FISIM, to show whether FISIM is included for B1g or excluded from B1g.
df_current_3_fisim = pd.read_csv('/Users/Danjing 1/Lingsu/Jobs/2024 WB STC/Sector/Process/current_3_fisim.csv')
# Convert 'note_code' to string in df2 for easier inclusion checks
df_current_3_fisim['note_code_str'] = df_current_3_fisim['note_code'].astype(str)

# Function to check for exact matches of note_code in notes
def match_notes(row, df2):
    relevant_rows = df2[df2['part'] == row['part']]
    notes = row['Footnotes']
    for _, df2_row in relevant_rows.iterrows():
        # Use regular expressions to find exact matches
        pattern = r'\b' + re.escape(df2_row['note_code_str']) + r'\b'  # \b denotes a word boundary
        if re.search(pattern, notes):
            return df2_row['FISIM']
    return 'No Match'

# Apply this function across df1
pivot_df_current_3['FISIM'] = pivot_df_current_3.apply(match_notes, axis=1, args=(df_current_3_fisim,))
print(pivot_df_current_3.head(10))
#pivot_df_current_3.to_csv("/Users/Danjing 1/Lingsu/Jobs/2024 WB STC/Sector/Process/current_3_try.csv")

print(pivot_df_current_3['FISIM'].describe())

for index in range(len(pivot_df_current_3)):
    if pivot_df_current_3.loc[index, 'FISIM'] == 'exclude':
        if pivot_df_current_3.loc[index, 'sum_of_sector'] < pivot_df_current_3.loc[index, 'B.1g']:
            pivot_df_current_3.loc[index, 'discard'] = True
        else:
            pivot_df_current_3.loc[index, 'discard'] = (pivot_df_current_3.loc[index, 'score'] - 0.075) >= 0


    if pivot_df_current_3.loc[index, 'FISIM'] == 'include':
        if pivot_df_current_3.loc[index, 'sum_of_sector'] > pivot_df_current_3.loc[index,'B.1g']:
            pivot_df_current_3.loc[index, 'discard'] = True
        else:
            pivot_df_current_3.loc[index, 'discard'] = (pivot_df_current_3.loc[index, 'score'] - 0.075) >= 0

#print(pivot_df_current_3.head(10))

### Save a copy of current discard column, without considering the number of sectors.
pivot_df_current_3['discard_nc_Nsector'] = pivot_df_current_3['discard']

# If there is any missing sector, then discard. Otherwise,  do not discard. (P can be missing)
conditions = pivot_df_current_3['missing_any_sector'] == True
pivot_df_current_3.loc[conditions, 'discard'] = True
print(pivot_df_current_3.head(10))

###### Final Data Clean and Export
#Our period of interest is 1990 – 2023.
pivot_df_current_3 = pivot_df_current_3[(pivot_df_current_3['year']>=1990) & (pivot_df_current_3['year']<=2023)]

# reset year and series into integer
pivot_df_current_3['year'] = pivot_df_current_3['year'].astype(int)
pivot_df_current_3['series_code'] = pivot_df_current_3['series_code'].astype(int)

# Merge iso3
pivot_df_current_3 = pd.merge(pivot_df_current_3, iso_mapping_current_3, on='country', how='left')

#export the pivot table dataframe to a csv file
print(pivot_df_current_3.head(10))
pivot_df_current_3.to_csv("/Users/Danjing 1/Lingsu/Jobs/2024 WB STC/Sector/Process/un3_current_Sectorvalue.csv")

# Keep only those not discarded.
df_current_3_int = pivot_df_current_3[pivot_df_current_3['discard']== False]
#df_current_3_int.drop(columns='discard', inplace=True)
print(df_current_3_int.head(10))
df_current_3_int.to_csv("/Users/Danjing 1/Lingsu/Jobs/2024 WB STC/Sector/Process/un3_current_intermediate.csv")

# Keep those discarded in a seperate file
df_current_3_int_discard = pivot_df_current_3[pivot_df_current_3['discard']== True]
df_current_3_int_discard.to_csv("/Users/Danjing 1/Lingsu/Jobs/2024 WB STC/Sector/Process/un3_current_intermediate_discard.csv")


# Export a file that only contains country, year, series_code, and iso3.
df_current_3_int = pd.concat([df_current_3_int.iloc[:,0:3],df_current_3_int.iloc[:,-1: ]], axis=1)
print(df_current_3_int.head())
df_current_3_int.to_csv("/Users/Danjing 1/Lingsu/Jobs/2024 WB STC/Sector/Process/un3_current_5cols.csv")
#
# # ### issues:
# # # ###1) GDP footntoes

## GAP issue
## footnote part, pd.notna.
