#import os
#current_path = os.getcwd()
#print(current_path)

import pandas as pd
pd.set_option('display.max_columns', None)

df_current_4 = pd.read_csv("/Users/Danjing 1/Lingsu/Jobs/2024 WB STC/Sector/Data_original/cleaned/un4_current.csv")
print(df_current_4.head())

#df_constant_4 = pd.read_csv("/Users/Danjing 1/Lingsu/Jobs/2024 WB STC/Sector/Data_original/cleaned/un4_constant.csv")
#print(df_constant_4.head())

# ##########Step 1. Before getting started, confirm few things
# ######## For Current file
# ### per country, year, and series, is there more than one currency type('Currency')?
# unique_counts_currency_current = df_current_4.groupby(['country','year','series_code'])['currency'].nunique()
# more_than_two_currency_current = unique_counts_currency_current >1
# count_true_currency_current = more_than_two_currency_current.sum()
# print(f"In the Current file, for each combination of country, year, and series, there are {count_true_currency_current} with more than 1 unique Currency type ")
# #Zero out of 4598 combinations
#
# ### per country, year, and series, is there more than one fiscal year type('Fiscal year Type')?
# unique_counts_fis_current = df_current_4.groupby(['country','year','series_code'])['year_type'].nunique()
# more_than_two_fis_current = unique_counts_fis_current >1
# count_true_fis_current = more_than_two_fis_current.sum()
# print(f"In the Current file, for each combination of country, year, and series, there are {count_true_fis_current} with more than 1 unique Fiscal year type ")
# #Zero out of 4598 combinations
#
# ######## For Constant file
# ### per country, year, Base year and series, is there more than one currency type('Currency')?
# unique_counts_currency_constant = df_constant_4.groupby(['country','year','base','series_code'])['currency'].nunique()
# more_than_two_currency_constant = unique_counts_currency_constant >1
# count_true_currency_constant = more_than_two_currency_constant.sum()
# print(f"In the Constant file, for each combination of country, year, base year, and series, there are {count_true_currency_constant} with more than 1 unique Currency type ")
# #Zero out of 6600 combinations
#
# ### per country, year, Base year and series, is there more than one fiscal year type('Fiscal year Type')?
# unique_counts_fis_constant = df_constant_4.groupby(['country','year','base','series_code'])['year_type'].nunique()
# more_than_two_fis_constant = unique_counts_fis_constant >0
# count_true_fis_constant = more_than_two_fis_constant.sum()
# print(f"In the Constant file, for each combination of country, year, base year, and series, there are {count_true_fis_constant} with more than 1 unique Fiscal year type ")
# #Zero out of 546 combinations??? The column year_type seems to have some problem.
#


##########Step 2. Sector check
##### Transforme the original dataset
######## For Current file

#Create a pivot table for the values, all sectors became column numbers
pivot_df_current_4 = df_current_4.pivot_table(index=['country','year','series_code'], columns='code', values = 'value')

#pivot_df_current_4 = df_current_4.pivot_table(index=['iso3', 'country','year','series_code'], columns='code', values = 'value')
# When using this line of code, the results become different. Why?
# We can add the iso3 column using a mapping table later.

#Aggregate the footnote colum to check for any footnotes
#print(df_current_4['note'].dtype) #object
footnotes = df_current_4.groupby(['country','year','series_code'])['note'].agg(lambda x: ', '.join(set(y for y in x if pd.notna(y) and y != '')))

#Reset the index to make 'country','year','series_code' into columns
pivot_df_current_4.reset_index(inplace=True)

# The footnotes series_code needs to have its index reset for joining. Then add it to the pivot table.
footnotes = footnotes.reset_index()
footnotes.rename(columns={'note':'Footnotes'}, inplace=True)
pivot_df_current_4 = pivot_df_current_4.merge(footnotes, on=['country','year','series_code'], how='left')

# put all missing sectors into a new column
'''Define a function to find missing sectors (columns)'''
def missing_column(row):
    return ', '.join(row.index[row.isna()])

pivot_df_current_4['sector_missing']= pivot_df_current_4.loc[:, 'A':'R+S+T'].apply(missing_column, axis=1)


# reset year and series into integer
pivot_df_current_4['year'] = pivot_df_current_4['year'].astype(int)
pivot_df_current_4['series_code'] = pivot_df_current_4['series_code'].astype(int)

# print(pivot_df_current_4.head(10))
# calculate the discard score
columns_to_sum = [3,4]+list(range(6,19))
pivot_df_current_4['sum_of_sector'] = pivot_df_current_4.iloc[:,columns_to_sum].sum(axis=1)
pivot_df_current_4['score'] = ((pivot_df_current_4['sum_of_sector'] - pivot_df_current_4['B.1g'])/pivot_df_current_4['B.1g']).abs()
pivot_df_current_4['discard'] = (pivot_df_current_4['score']-0.005)>=0

# if b.1g is empty, do not discard. (discard = false)
for index in range(len(pivot_df_current_4)):
    if pd.isna(pivot_df_current_4.loc[index, 'B.1g']):  # Check if the value in A is NaN (or None)
        pivot_df_current_4.loc[index, 'discard'] = False     # Set B to False if A is empty


###FISIM not considered yet.
# check if footnote 6 and footnote 36 is included.
pivot_df_current_4['footnote_6'] = pivot_df_current_4['Footnotes'].str.contains(r'\b6\b', na=False)
pivot_df_current_4['footnote_36'] = pivot_df_current_4['Footnotes'].str.contains('36', na=False)

#Footnote 6. The value of financial intermediation services indirectly measured (FISIM) is deducted from gross value added. (Only show in category B1.g)
#If the sum of the sectors are smaller than b1.g, then there’s an issue. Discard.
#If the sum of the sectors are bigger than b1.g, then use the 0.075 threshold.

#Footnote 36. ncludes financial intermediation services indirectly measured (FISIM) of the Total Economy. (Only show in category B1.g)
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

#Our period of interest is 1990 – 2023.
pivot_df_current_4 = pivot_df_current_4[(pivot_df_current_4['year']>=1990) & (pivot_df_current_4['year']<=2023)]

#export the pivot table dataframe to a csv file
print(pivot_df_current_4.head(10))
pivot_df_current_4.to_csv("/Users/Danjing 1/Lingsu/Jobs/2024 WB STC/Sector/Process/Current_4_Sectorvalue.csv")

df_current_4_int = pivot_df_current_4[pivot_df_current_4['discard']== False]
df_current_4_int.drop(columns='discard', inplace=True)
print(df_current_4_int.head(10))

df_current_4_int.to_csv("/Users/Danjing 1/Lingsu/Jobs/2024 WB STC/Sector/Process/Current_4_intermediate.csv")

######## For Constant file

#Create a pivot table for the values, all sectors became column numbers
pivot_df_constant_4 = df_constant_4.pivot_table(index=['country','year','series_code','base'], columns='code', values = 'value')
#pivot_df_constant_4 = df_constant_4.pivot_table(index=['Country or Area','Fiscal Year','Series','Base Year'], columns='SNA93 Item Code', values = 'Value')
#print(pivot_df_constant_4.head(10))

# #pivot_df_constant_4 = df_constant_4.pivot_table(index=['iso3', 'country','year','series_code'], columns='code', values = 'value')
# # When using this line of code, the results become different. Why?
# # We can add the iso3 column using a mapping table later.
#
#Aggregate the footnote colum to check for any footnotes
#print(df_constant_4['note'].dtype) #object
footnotes = df_constant_4.groupby(['country','year','series_code','base'])['note'].agg(lambda x: ', '.join(set(y for y in x if pd.notna(y) and y != '')))

#Reset the index to make 'country','year','series_code' into columns
pivot_df_constant_4.reset_index(inplace=True)

# The footnotes series_code needs to have its index reset for joining. Then add it to the pivot table.
footnotes = footnotes.reset_index()
footnotes.rename(columns={'note':'Footnotes'}, inplace=True)
pivot_df_constant_4 = pivot_df_constant_4.merge(footnotes, on=['country','year','series_code','base'], how='left')
#print(pivot_df_constant_4.head(10))

# put all missing sectors into a new column
'''Define a function to find missing sectors (columns)'''
def missing_column(row):
    return ', '.join(row.index[row.isna()])

pivot_df_constant_4['sector_missing']= pivot_df_constant_4.loc[:, 'A':'R+S+T'].apply(missing_column, axis=1)


# reset year and series into integer
pivot_df_constant_4['year'] = pivot_df_constant_4['year'].astype(int)
pivot_df_constant_4['series_code'] = pivot_df_constant_4['series_code'].astype(int)

#print(pivot_df_constant_4.head(10))
# calculate the discard score
columns_to_sum = [4,5]+list(range(7,20))
pivot_df_constant_4['sum_of_sector'] = pivot_df_constant_4.iloc[:,columns_to_sum].sum(axis=1)
pivot_df_constant_4['score'] = ((pivot_df_constant_4['sum_of_sector'] - pivot_df_constant_4['B.1g'])/pivot_df_constant_4['B.1g']).abs()
pivot_df_constant_4['discard'] = (pivot_df_constant_4['score']-0.005)>=0
print(pivot_df_constant_4.head(10))

# if b.1g is empty, do not discard. (discard = false)
for index in range(len(pivot_df_constant_4)):
    if pd.isna(pivot_df_constant_4.loc[index, 'B.1g']):  # Check if the value in A is NaN (or None)
        pivot_df_constant_4.loc[index, 'discard'] = False     # Set B to False if A is empty




# check if footnote 7 and footnote 37 is included.
pivot_df_constant_4['footnote_7'] = pivot_df_constant_4['Footnotes'].str.contains(r'\b7\b', na=False)
pivot_df_constant_4['footnote_37'] = pivot_df_constant_4['Footnotes'].str.contains('37', na=False)

#Footnote 7. The value of financial intermediation services indirectly measured (FISIM) is deducted from gross value added. (Only show in category B1.g)
#If the sum of the sectors are smaller than b1.g, then there’s an issue. Discard.
#If the sum of the sectors are bigger than b1.g, then use the 0.075 threshold.

#Footnote 37. includes financial intermediation services indirectly measured (FISIM) of the Total Economy. (Only show in category B1.g)
#If the sum of the sectors are bigger than b1.g, then there’s an issue. Discard.
#If the sum of the sectors are smaller than b1.g, then use the 0.075 threshold.

for index in range(len(pivot_df_constant_4)):
    if pivot_df_constant_4['footnote_7'] is True:
        if pivot_df_constant_4['sum_of_sector'] < pivot_df_constant_4['B.1g']:
            pivot_df_constant_4.loc[index, 'discard'] = True
        else:
            pivot_df_constant_4['discard'] = (pivot_df_constant_4['score'] - 0.075) >= 0


    if pivot_df_constant_4['footnote_37'] is True:
        if pivot_df_constant_4['sum_of_sector'] > pivot_df_constant_4['B.1g']:
            pivot_df_constant_4.loc[index, 'discard'] = True
        else:
            pivot_df_constant_4['discard'] = (pivot_df_constant_4['score'] - 0.075) >= 0

#print(pivot_df_constant_4.head(10))

#Our period of interest is 1990 – 2023.
pivot_df_constant_4 = pivot_df_constant_4[(pivot_df_constant_4['year']>=1990) & (pivot_df_constant_4['year']<=2023)]

#export the pivot table dataframe to a csv file
print(pivot_df_constant_4.head(10))
pivot_df_constant_4.to_csv("/Users/Danjing 1/Lingsu/Jobs/2024 WB STC/Sector/Process/Constant_4_Sectorvalue.csv")

df_constant_4_int = pivot_df_constant_4[pivot_df_constant_4['discard']== False]
df_constant_4_int.drop(columns='discard', inplace=True)
print(df_constant_4_int.head(10))

df_constant_4_int.to_csv("/Users/Danjing 1/Lingsu/Jobs/2024 WB STC/Sector/Process/Constant_4_intermediate.csv")

###Need to check the following:
## Footnote 42, 48, and 29 (Refers to GDP)
## check randonmy if the calculations are correct.