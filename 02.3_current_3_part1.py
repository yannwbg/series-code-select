import pandas as pd
import numpy as np
pd.set_option('display.max_columns', None)

###Read the file
filepath_current_3 = "/Users/Danjing 1/Lingsu/Jobs/2024 WB STC/Sector/Data_original/cleaned/un3_current.csv"

df_current_3 = pd.read_csv(filepath_current_3)
print(df_current_3.head())


##########Step 2. Sector check
##### Transforme the original dataset

#Create a pivot table for the values, all sectors became column numbers
pivot_df_current_3 = df_current_3.pivot_table(index=['country','year','series_code'], columns='code', values = 'value')
print(pivot_df_current_3.head())

#pivot_df_current_4 = df_current_4.pivot_table(index=['iso3', 'country','year','series_code'], columns='code', values = 'value')
# When using this line of code, the results become different. Why?
# We can add the iso3 column using a mapping table later.

#Reset the index to make 'country','year','series_code' into columns
pivot_df_current_3.reset_index(inplace=True)
print(pivot_df_current_3.head())

###Data cleaning
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

pivot_df_current_3['sector_missing']= pivot_df_current_3.loc[:, 'A':'P'].apply(missing_column, axis=1)
print(pivot_df_current_3.head())

# # reset year and series into integer
# pivot_df_current_3['year'] = pivot_df_current_3['year'].astype(int)
# pivot_df_current_3['series_code'] = pivot_df_current_3['series_code'].astype(int)

# calculate the discard score
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

filtered_df.to_csv("/Users/Danjing 1/Lingsu/Jobs/2024 WB STC/Sector/Process/current_3_filtered_for_b1g.csv")
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


#pivot_df_current_3.to_csv("/Users/Danjing 1/Lingsu/Jobs/2024 WB STC/Sector/Process/current_3_try.csv")

# ###FISIM not considered yet.
# # check if footnote 6 and footnote 36 is included.
# pivot_df_current_3['footnote_6'] = pivot_df_current_3['Footnotes'].str.contains(r'\b6\b', na=False)
# pivot_df_current_3['footnote_36'] = pivot_df_current_3['Footnotes'].str.contains('36', na=False)
#
# #Footnote 6. The value of financial intermediation services indirectly measured (FISIM) is deducted from gross value added. (Only show in category B1.g)
# #If the sum of the sectors are smaller than b1.g, then there’s an issue. Discard.
# #If the sum of the sectors are bigger than b1.g, then use the 0.075 threshold.
#
# #Footnote 36. includes financial intermediation services indirectly measured (FISIM) of the Total Economy. (Only show in category B1.g)
# #If the sum of the sectors are bigger than b1.g, then there’s an issue. Discard.
# #If the sum of the sectors are smaller than b1.g, then use the 0.075 threshold.
#
# for index in range(len(pivot_df_current_3)):
#     if pivot_df_current_3['footnote_6'] is True:
#         if pivot_df_current_3['sum_of_sector'] < pivot_df_current_3['B.1g']:
#             pivot_df_current_3.loc[index, 'discard'] = True
#         else:
#             pivot_df_current_3['discard'] = (pivot_df_current_3['score'] - 0.075) >= 0
#
#
#     if pivot_df_current_3['footnote_36'] is True:
#         if pivot_df_current_3['sum_of_sector'] > pivot_df_current_3['B.1g']:
#             pivot_df_current_3.loc[index, 'discard'] = True
#         else:
#             pivot_df_current_3['discard'] = (pivot_df_current_3['score'] - 0.075) >= 0
#
#Our period of interest is 1990 – 2023.
pivot_df_current_3 = pivot_df_current_3[(pivot_df_current_3['year']>=1990) & (pivot_df_current_3['year']<=2023)]

#export the pivot table dataframe to a csv file
print(pivot_df_current_3.head(10))
pivot_df_current_3.to_csv("/Users/Danjing 1/Lingsu/Jobs/2024 WB STC/Sector/Process/current_3_Sectorvalue.csv")

df_current_3_int = pivot_df_current_3[pivot_df_current_3['discard']== False]
df_current_3_int.drop(columns='discard', inplace=True)
print(df_current_3_int.head(10))

df_current_3_int.to_csv("/Users/Danjing 1/Lingsu/Jobs/2024 WB STC/Sector/Process/current_3_intermediate.csv")

df_current_3_int = df_current_3_int.iloc[:,0:3]
print(df_current_3_int.head())
df_current_3_int.to_csv("/Users/Danjing 1/Lingsu/Jobs/2024 WB STC/Sector/Process/current_3_3cols.csv")

# ### issues:
# ###1) GDP footntoes
# ###2) FISIM not appliaed yet.
