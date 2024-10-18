import pandas as pd
import numpy as np
pd.set_option('display.max_columns', None)
#
# df_constant_3 = pd.read_csv("/Users/Danjing 1/Lingsu/Jobs/2024 WB STC/Sector/Data_original/cleaned/un4_constant.csv")
###Read the file
filepath_constant_3 = "/Users/Danjing 1/Lingsu/Jobs/2024 WB STC/Sector/Data_original/cleaned/un3_constant.csv"

df_constant_3 = pd.read_csv(filepath_constant_3)
print(df_constant_3.head())

##########Step 2. Sector check
##### Transforme the original dataset

#Create a pivot table for the values, all sectors became column numbers
pivot_df_constant_3 = df_constant_3.pivot_table(index=['country','year','series_code','base'], columns='code', values = 'value')
print(pivot_df_constant_3.head(10))

#Reset the index to make 'country','year','series_code' into columns
pivot_df_constant_3.reset_index(inplace=True)
print(pivot_df_constant_3.head(10))

###Data cleaning
pivot_df_constant_3 = pivot_df_constant_3.drop(['1','2','60-63','64','D.21','D.21-D.31','D.31','P.119'], axis=1)
print(pivot_df_constant_3.tail())

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
pivot_df_constant_3 = update_dataframe(pivot_df_constant_3, 'A', 'B', 'A+B')
pivot_df_constant_3 = update_dataframe(pivot_df_constant_3, 'G', 'H', 'G+H')
pivot_df_constant_3 = update_dataframe(pivot_df_constant_3, 'J', 'K', 'J+K')

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
pivot_df_constant_3 = update_dataframe3(pivot_df_constant_3, 'M', 'N', 'O','M+N+O')
print(pivot_df_constant_3.tail())


#Aggregate the footnote colum to check for any footnotes
#print(df_constant_3['note'].dtype) #object
footnotes = df_constant_3.groupby(['country','year','series_code','base'])['note'].agg(lambda x: ', '.join(set(y for y in x if pd.notna(y) and y != '')))

# The footnotes series_code needs to have its index reset for joining. Then add it to the pivot table.
footnotes = footnotes.reset_index()
footnotes.rename(columns={'note':'Footnotes'}, inplace=True)
pivot_df_constant_3 = pivot_df_constant_3.merge(footnotes, on=['country','year','series_code','base'], how='left')
#print(pivot_df_constant_3.head(10))

# Add part column to make sure we get the correct footnotes information
mapping_part = df_constant_3[['country','part']].drop_duplicates(subset='country')
#print(mapping_part)
pivot_df_constant_3 = pd.merge(pivot_df_constant_3,mapping_part,on='country',how='left')

# put all missing sectors into a new column
'''Define a function to find missing sectors (columns)'''
def missing_column(row):
    return ', '.join(row.index[row.isna()])

pivot_df_constant_3['sector_missing']= pivot_df_constant_3.loc[:, 'A':'P'].apply(missing_column, axis=1)
print(pivot_df_constant_3.head(10))

# # reset year and series into integer
# pivot_df_constant_3['year'] = pivot_df_constant_3['year'].astype(int)
# pivot_df_constant_3['series_code'] = pivot_df_constant_3['series_code'].astype(int)
# #print(pivot_df_constant_3.head(10))

# calculate the discard score
columns_to_sum = list(range(4,7))+list(range(9,26))
pivot_df_constant_3['sum_of_sector'] = pivot_df_constant_3.iloc[:,columns_to_sum].sum(axis=1)
pivot_df_constant_3['score'] = ((pivot_df_constant_3['sum_of_sector'] - pivot_df_constant_3['B.1g'])/pivot_df_constant_3['B.1g']).abs()
pivot_df_constant_3['discard'] = (pivot_df_constant_3['score']-0.005)>=0
print(pivot_df_constant_3.head(10))

# # if b.1g is empty, do not discard. (discard = false)
# for index in range(len(pivot_df_constant_3)):
#     if pd.isna(pivot_df_constant_3.loc[index, 'B.1g']):  # Check if the value in A is NaN (or None)
#         pivot_df_constant_3.loc[index, 'discard'] = False     # Set B to False if A is empty

# if b.1g is missing and b.1*g is missing, do not discard. (discard = false)
for index in range(len(pivot_df_constant_3)):
    if pd.isna(pivot_df_constant_3.loc[index, 'B.1g']) and pd.isna(pivot_df_constant_3.loc[index, 'B.1*g']):  # Check if the value in A is NaN (or None)
        pivot_df_constant_3.loc[index, 'discard'] = False     # Set B to False if A is empty

# if b.1g is missing, but b.1*g is not missing. check for footnotes of other sectors.
# If footnotes are related to tax and subsidy add or minus, then we can still use 0.005.
# if no footnotes or footnotes not related to correct ones, we can use 10%
# To check the footnote, not only needs to check the footnote columns, also need to check the part column.

#filtered_df_1 = pivot_df_constant_3[pd.isna(pivot_df_constant_3['B.1g']) & pd.notna(pivot_df_constant_3['B.1*g']) & pd.notna(pivot_df_constant_3['Footnotes'])]
filtered_df = pivot_df_constant_3[pd.isna(pivot_df_constant_3['B.1g']) & pd.notna(pivot_df_constant_3['B.1*g'])]
#print(filtered_df_1) #258 rows *32 columns
print(filtered_df) #258 rows *32 columns
# pd.notna(pivot_df_constant_3['Footnotes'] seems not work

filtered_df.to_csv("/Users/Danjing 1/Lingsu/Jobs/2024 WB STC/Sector/Process/constant_3_filtered_for_b1g.csv")
# Need to check manually for the footnote.

# Went through the file and did not see the related footnote. So will use b1*g and 10%.
# Also, when sum_of_sector equals to zero, discard.
pivot_df_constant_3['score*'] = None
for index in range(len(pivot_df_constant_3)):
    pivot_df_constant_3['score*'] = (
            (pivot_df_constant_3['sum_of_sector'] - pivot_df_constant_3['B.1*g']) / pivot_df_constant_3[
        'B.1*g']).abs()
    if pd.isna(pivot_df_constant_3.loc[index, 'B.1g']) and pd.notna(pivot_df_constant_3.loc[index, 'B.1*g']):
        pivot_df_constant_3.loc[index, 'discard'] = (pivot_df_constant_3.loc[index, 'score*'] - 0.1) >= 0
    if pivot_df_constant_3.loc[index, 'sum_of_sector'] ==0:  # meaning no sector value at all. This only happens in ISICC3 files.
        pivot_df_constant_3.loc[index, 'discard'] = True

pivot_df_constant_3.to_csv("/Users/Danjing 1/Lingsu/Jobs/2024 WB STC/Sector/Process/constant_3_try.csv")


# # FISIM
# # check if footnote 7 and footnote 37 is included.
# pivot_df_constant_3['footnote_7'] = pivot_df_constant_3['Footnotes'].str.contains(r'\b7\b', na=False)
# pivot_df_constant_3['footnote_37'] = pivot_df_constant_3['Footnotes'].str.contains('37', na=False)
#

# #Footnote 7. The value of financial intermediation services indirectly measured (FISIM) is deducted from gross value added. (Only show in category B1.g)
# #If the sum of the sectors are smaller than b1.g, then there’s an issue. Discard.
# #If the sum of the sectors are bigger than b1.g, then use the 0.075 threshold.
#
# #Footnote 37. includes financial intermediation services indirectly measured (FISIM) of the Total Economy. (Only show in category B1.g)
# #If the sum of the sectors are bigger than b1.g, then there’s an issue. Discard.
# #If the sum of the sectors are smaller than b1.g, then use the 0.075 threshold.
#
# for index in range(len(pivot_df_constant_3)):
#     if pivot_df_constant_3['footnote_7'] is True:
#         if pivot_df_constant_3['sum_of_sector'] < pivot_df_constant_3['B.1g']:
#             pivot_df_constant_3.loc[index, 'discard'] = True
#         else:
#             pivot_df_constant_3['discard'] = (pivot_df_constant_3['score'] - 0.075) >= 0
#
#
#     if pivot_df_constant_3['footnote_37'] is True:
#         if pivot_df_constant_3['sum_of_sector'] > pivot_df_constant_3['B.1g']:
#             pivot_df_constant_3.loc[index, 'discard'] = True
#         else:
#             pivot_df_constant_3['discard'] = (pivot_df_constant_3['score'] - 0.075) >= 0
#
# #print(pivot_df_constant_3.head(10))
#
#Our period of interest is 1990 – 2023.
pivot_df_constant_3 = pivot_df_constant_3[(pivot_df_constant_3['year']>=1990) & (pivot_df_constant_3['year']<=2023)]

#export the pivot table dataframe to a csv file
print(pivot_df_constant_3.head(10))
pivot_df_constant_3.to_csv("/Users/Danjing 1/Lingsu/Jobs/2024 WB STC/Sector/Process/constant_3_Sectorvalue.csv")

df_constant_3_int = pivot_df_constant_3[pivot_df_constant_3['discard']== False]
df_constant_3_int.drop(columns='discard', inplace=True)
print(df_constant_3_int.head(10))

df_constant_3_int.to_csv("/Users/Danjing 1/Lingsu/Jobs/2024 WB STC/Sector/Process/constant_3_intermediate.csv")

df_constant_3_int = df_constant_3_int.iloc[:,0:4]
print(df_constant_3_int.head())
df_constant_3_int.to_csv("/Users/Danjing 1/Lingsu/Jobs/2024 WB STC/Sector/Process/constant_3_4cols.csv")

# ### issues:
# ###1) GDP footntoes
# ###2) FISIM not appliaed yet.