#import os
#current_path = os.getcwd()
#print(current_path)

import pandas as pd
pd.set_option('display.max_columns', None)

df_current_4 = pd.read_csv("/Users/Danjing 1/Lingsu/Jobs/2024 WB STC/Sector/Data_original/UNdata_current.csv")
#print(df_current_4.head())

#df_constant_4 = pd.read_csv("/Users/Danjing 1/Lingsu/Jobs/2024 WB STC/Sector/Data_original/UNdata_constant.csv")
#print(df_constant_4.head())

# ##########Step 1. Before getting started, confirm few things
# ######## For Current file
# ### per country, year, and series, is there more than one currency type('Currency')?
# unique_counts_currency_current = df_current_4.groupby(['Country or Area','Year','Series'])['Currency'].nunique()
# more_than_two_currency_current = unique_counts_currency_current >0
# count_true_currency_current = more_than_two_currency_current.sum()
# print(f"In the Current file, for each combination of country, year, and series, there are {count_true_currency_current} with more than 1 unique Currency type ")
# #Zero out of 5497 combinations
#
#
# ### per country, year, and series, is there more than one fiscal year type('Fiscal Year Type')?
# unique_counts_fis_current = df_current_4.groupby(['Country or Area','Year','Series'])['Fiscal Year Type'].nunique()
# more_than_two_fis_current = unique_counts_fis_current >0
# count_true_fis_current = more_than_two_fis_current.sum()
# print(f"In the Current file, for each combination of country, year, and series, there are {count_true_fis_current} with more than 1 unique Fiscal Year type ")
# #Zero out of 5497 combinations
#
# ######## For Constant file
# ### per country, year, Base Year and series, is there more than one currency type('Currency')?
# unique_counts_currency_constant = df_constant_4.groupby(['Country or Area','Fiscal Year','Base Year','Series'])['Currency'].nunique()
# more_than_two_currency_constant = unique_counts_currency_constant >0
# count_true_currency_constant = more_than_two_currency_constant.sum()
# print(f"In the Constant file, for each combination of country, year, base year, and series, there are {count_true_currency_constant} with more than 1 unique Currency type ")
# #Zero out of 7896 combinations
#
# ### per country, year, Base Year and series, is there more than one fiscal year type('Fiscal Year Type')?
# unique_counts_fis_constant = df_constant_4.groupby(['Country or Area','Fiscal Year','Base Year','Series'])['Fiscal Year Type'].nunique()
# more_than_two_fis_constant = unique_counts_fis_constant >0
# count_true_fis_constant = more_than_two_fis_constant.sum()
# print(f"In the Constant file, for each combination of country, year, base year, and series, there are {count_true_fis_constant} with more than 1 unique Fiscal Year type ")
# #Zero out of 7896 combinations


##########Step 2. Sector check
##### Transforme the original dataset
######## For Current file

#Create a pivot table for the values, all sectors became column numbers
pivot_df_current_4 = df_current_4.pivot_table(index=['Country or Area','Year','Series'], columns='SNA93 Item Code', values = 'Value')

#Aggregate the footnote colum to check for any footnotes
#print(df_current_4['Value Footnotes'].dtype) #object
footnotes = df_current_4.groupby(['Country or Area','Year','Series'])['Value Footnotes'].agg(lambda x: ', '.join(set(y for y in x if pd.notna(y) and y != '')))

#Reset the index to make 'Country or Area','Year','Series' into columns
pivot_df_current_4.reset_index(inplace=True)

# THe footnotes Series needs to have its index reset for joining. Then add it to the pivot table.
footnotes = footnotes.reset_index()
footnotes.rename(columns={'Value Footnotes':'Footnotes'}, inplace=True)
pivot_df_current_4 = pivot_df_current_4.merge(footnotes, on=['Country or Area','Year','Series'], how='left')

# reset year and series into integer
pivot_df_current_4['Year'] = pivot_df_current_4['Year'].astype(int)
pivot_df_current_4['Series'] = pivot_df_current_4['Series'].astype(int)

# calculate the discard score
columns_to_sum = [3,4]+list(range(6,19))
pivot_df_current_4['sum_of_sector'] = pivot_df_current_4.iloc[:,columns_to_sum].sum(axis=1)
pivot_df_current_4['score'] = ((pivot_df_current_4['sum_of_sector'] - pivot_df_current_4['B.1g'])/pivot_df_current_4['B.1g']).abs()
pivot_df_current_4['discard'] = (pivot_df_current_4['score']-0.005)>=0

# put all missing sectors
'''Define a function to find missing sectors (columns)'''
def missing_column(row):
    return ', '.join(row.index[row.isna()])

pivot_df_current_4['sector_missing']= pivot_df_current_4.loc[:, 'A':'R+S+T'].apply(missing_column, axis=1)

#export the pivot table dataframe to a csv file
print(pivot_df_current_4.head(10))
pivot_df_current_4.to_csv("/Users/Danjing 1/Lingsu/Jobs/2024 WB STC/Sector/Process/Current_4_SectorValue.csv")


