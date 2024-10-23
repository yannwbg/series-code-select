#import os
#current_path = os.getcwd()
#print(current_path)

import pandas as pd
import numpy as np
pd.set_option('display.max_columns', None)

df_current_4 = pd.read_csv("/Users/Danjing 1/Lingsu/Jobs/2024 WB STC/Sector/Data_original/cleaned/un4_current.csv")
print(df_current_4.head())

# Get the ios and country columns and turn into a mapping file.
iso_mapping = df_current_4.iloc[:, :2].drop_duplicates()
#print(iso_mapping.head())
iso_mapping.to_csv("/Users/Danjing 1/Lingsu/Jobs/2024 WB STC/Sector/Process/iso_mapping.csv", index=False)

##########Step 2. Sector check
##### Transforme the original dataset
######## For Current file

#Create a pivot table for the values, all sectors became column numbers
pivot_df_current_4 = df_current_4.pivot_table(index=['country','year','series_code'], columns='code', values = 'value')

#Aggregate the footnote colum to check for any footnotes
#print(df_current_4['note'].dtype) #object
footnotes = df_current_4.groupby(['country','year','series_code'])['note'].agg(lambda x: ', '.join(set(y for y in x if pd.notna(y) and y != '')))

#Reset the index to make 'country','year','series_code' into columns
pivot_df_current_4.reset_index(inplace=True)

# The footnotes series_code needs to have its index reset for joining. Then add it
# to the pivot table.
footnotes = footnotes.reset_index()
footnotes.rename(columns={'note':'Footnotes'}, inplace=True)
pivot_df_current_4 = pivot_df_current_4.merge(footnotes, on=['country','year','series_code'], how='left')

# put all missing sectors into a new column
'''Define a function to find missing sectors (columns)'''
def missing_column(row):
    return ', '.join(row.index[row.isna()])

pivot_df_current_4['sector_missing']= pivot_df_current_4.loc[:, 'A':'R+S+T'].apply(missing_column, axis=1)

# Add a column showing the number of sectors. If no missing sector, it should be 15.
columns_to_cal = [3,4]+list(range(6,19))
pivot_df_current_4['n_sectors'] = pivot_df_current_4.iloc[:, columns_to_cal].count(axis=1)
#pivot_df_current_4['n_sectors'] = columns.apply(lambda x: x.count(), axis=1)

# Add a column to check if D is missing. Another column to show if E is missing. Another column to show if B is missing.
pivot_df_current_4['B_missing'] = pivot_df_current_4['B'].isna()
pivot_df_current_4['D_missing'] = pivot_df_current_4['D'].isna()
pivot_df_current_4['E_missing'] = pivot_df_current_4['E'].isna()

# pivot_df_current_4.info()
pivot_df_current_4['only_B_missing'] = (pivot_df_current_4['B_missing'] == True) & (pivot_df_current_4['n_sectors'] == 14)
pivot_df_current_4['only_DorE_missing'] = ((pivot_df_current_4['D_missing'] == True) & (pivot_df_current_4['n_sectors'] == 14)) | ((pivot_df_current_4['E_missing'] == True) & (pivot_df_current_4['n_sectors'] == 14))

# print(pivot_df_current_4.head(10))

# calculate the discard score
#columns_to_sum = [3,4]+list(range(6,19))
pivot_df_current_4['sum_of_sector'] = pivot_df_current_4.iloc[:,columns_to_cal].sum(axis=1)
pivot_df_current_4['score'] = ((pivot_df_current_4['sum_of_sector'] - pivot_df_current_4['B.1g'])/pivot_df_current_4['B.1g']).abs()
pivot_df_current_4['discard'] = (pivot_df_current_4['score']-0.005)>=0

# if b.1g is empty, do not discard. (discard = false)
for index in range(len(pivot_df_current_4)):
    if pd.isna(pivot_df_current_4.loc[index, 'B.1g']):  # Check if the value in A is NaN (or None)
        pivot_df_current_4.loc[index, 'discard'] = False     # Set B to False if A is empty

print(pivot_df_current_4.head(10))

###FISIM not considered yet.
# check if footnote 6 and footnote 36 is included.
pivot_df_current_4['footnote_6'] = pivot_df_current_4['Footnotes'].str.contains(r'\b6\b', na=False)
pivot_df_current_4['footnote_36'] = pivot_df_current_4['Footnotes'].str.contains('36', na=False)

#Footnote 6. The value of financial intermediation services indirectly measured (FISIM) is deducted from gross value added. (Only show in category B1.g)
#If the sum of the sectors are smaller than b1.g, then there’s an issue. Discard.
#If the sum of the sectors are bigger than b1.g, then use the 0.075 threshold.

#Footnote 36. includes financial intermediation services indirectly measured (FISIM) of the Total Economy. (Only show in category B1.g)
#If the sum of the sectors are bigger than b1.g, then there’s an issue. Discard.
#If the sum of the sectors are smaller than b1.g, then use the 0.075 threshold.

for index in range(len(pivot_df_current_4)):
    if pivot_df_current_4['footnote_6'] is True:
        if pivot_df_current_4['sum_of_sector'] < pivot_df_current_4['B.1g']:
            pivot_df_current_4.loc[index, 'discard'] = True
        else:
            pivot_df_current_4['discard'] = (pivot_df_current_4['score'] - 0.075) >= 0


    if pivot_df_current_4['footnote_36'] is True:
        if pivot_df_current_4['sum_of_sector'] > pivot_df_current_4['B.1g']:
            pivot_df_current_4.loc[index, 'discard'] = True
        else:
            pivot_df_current_4['discard'] = (pivot_df_current_4['score'] - 0.075) >= 0


# Save a copy of current discard column, without considering the number of sectors.
pivot_df_current_4['discard_nc_Nsector'] = pivot_df_current_4['discard']

# If the n_sector is smaller than 15 (total sector columns), unless only missing D or E, then discard. Otherwise,  do not discard.
conditions = (pivot_df_current_4['n_sectors'] < 15) & (pivot_df_current_4['only_DorE_missing'] == False)
pivot_df_current_4.loc[conditions, 'discard'] = True

### Final Data Clean and Export
#Our period of interest is 1990 – 2023.
pivot_df_current_4 = pivot_df_current_4[(pivot_df_current_4['year']>=1990) & (pivot_df_current_4['year']<=2023)]

# reset year and series into integer
pivot_df_current_4['year'] = pivot_df_current_4['year'].astype(int)
pivot_df_current_4['series_code'] = pivot_df_current_4['series_code'].astype(int)

# Merge the iso3
pivot_df_current_4 = pd.merge(pivot_df_current_4, iso_mapping, on='country', how='left')

#export the pivot table dataframe to a csv file
print(pivot_df_current_4.head(10))
pivot_df_current_4.to_csv("/Users/Danjing 1/Lingsu/Jobs/2024 WB STC/Sector/Process/un4_current_Sectorvalue.csv")

# Keep only those not discarded.
df_current_4_int = pivot_df_current_4[pivot_df_current_4['discard']== False]
# df_current_4_int.drop(columns='discard', inplace=True)
print(df_current_4_int.head(10))
df_current_4_int.to_csv("/Users/Danjing 1/Lingsu/Jobs/2024 WB STC/Sector/Process/un4_current_intermediate.csv")

# Keep those discarded in a seperate file
df_current_4_int_discard = pivot_df_current_4[pivot_df_current_4['discard']== True]
df_current_4_int_discard.to_csv("/Users/Danjing 1/Lingsu/Jobs/2024 WB STC/Sector/Process/un4_current_intermediate_discard.csv")

# Export a file that only contains country, year, series_code, and iso3.
df_current_4_int = pd.concat([df_current_4_int.iloc[:,0:3],df_current_4_int.iloc[:,-1: ]], axis=1)
print(df_current_4_int.head())
df_current_4_int.to_csv("/Users/Danjing 1/Lingsu/Jobs/2024 WB STC/Sector/Process/un4_current_4cols.csv")

# ###small issues:
###1)Footnote 42 & 43 Refers to Gross Domestic Product. Philippines: For B.1g marked 42 & 43, the other sectors marked 19. However, for series 100, 2000-2012, sector D 2001-2008 marked 1, all other sectors has no footnote.


