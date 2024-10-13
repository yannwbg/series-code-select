#import os
#current_path = os.getcwd()
#print(current_path)

import pandas as pd
pd.set_option('display.max_columns', None)

df_current_4 = pd.read_csv("/Users/Danjing 1/Lingsu/Jobs/2024 WB STC/Sector/Data_original/cleaned/un4_current.csv")
print(df_current_4.head())

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

#Our period of interest is 1990 – 2023.
pivot_df_current_4 = pivot_df_current_4[(pivot_df_current_4['year']>=1990) & (pivot_df_current_4['year']<=2023)]

#export the pivot table dataframe to a csv file
print(pivot_df_current_4.head(10))
pivot_df_current_4.to_csv("/Users/Danjing 1/Lingsu/Jobs/2024 WB STC/Sector/Process/Current_4_Sectorvalue.csv")

df_current_4_int = pivot_df_current_4[pivot_df_current_4['discard']== False]
df_current_4_int.drop(columns='discard', inplace=True)
print(df_current_4_int.head(10))

df_current_4_int.to_csv("/Users/Danjing 1/Lingsu/Jobs/2024 WB STC/Sector/Process/Current_4_intermediate.csv")


###small issues:
###1)Footnote 42 & 43 Refers to Gross Domestic Product. Philippines: For B.1g marked 42 & 43, the other sectors marked 19. However, for series 100, 2000-2012, sector D 2001-2008 marked 1, all other sectors has no footnote.
###2) ISO3 has not been appended yet.

