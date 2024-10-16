### Pre-setting
import pandas as pd
import numpy as np
pd.set_option('display.max_columns', None)

#### Read the file
filepath = "/Users/Danjing 1/Lingsu/Jobs/2024 WB STC/Sector/Process/Current_4_3cols.csv"

df_current_4 = pd.read_csv(filepath, index_col=None)
print(df_current_4.head())

### Data cleaning
df_current_4 = df_current_4.iloc[:,1:4]
df_current_4['valid_data'] = True
print(df_current_4.head())

### Data transforming to show for each country & year combination, what series has value.
pivot_df_current_4 = df_current_4.pivot_table(index=['country','year'], columns='series_code', values = 'valid_data', aggfunc=('count'))
#print(pivot_df_current_4.head())

####Reset the index to make 'country' into columns
pivot_df_current_4.reset_index(inplace=True)
# Now you have a new dataframe with columns country, year, 100, 200 300, 400 500, 1000, 1100

### Add a column count, to show how many valid series options are there.
pivot_df_current_4['count'] = pivot_df_current_4.iloc[:,2:9].sum(axis=1)
#print(pivot_df_current_4['count'].describe()) #until 75% the numer is still 1. max is 3.

print(pivot_df_current_4.head(10))
# Now you have a new dataframe with columns country, year, 100, 200 300, 400 500, 1000, 1100, count.

###Add a column, to put the highest sereis value in this column.
#List of columns to check
columns_to_check = [100, 200, 300, 400, 500, 1000, 1100]

# Function to get highest non_NA column name
def get_highest_non_na_column(row, columns):
    # Filter the row to include only the desired columns and drop NAs
    valid_columns = row[columns].dropna()
    # Return the highest numbered column name, converting to int for comparison
    if not valid_columns.empty:
        return max(valid_columns.index, key=int)
    return np.nan

# Apply the function to each row in the DataFrame
pivot_df_current_4['highest_series'] = pivot_df_current_4.apply(get_highest_non_na_column, columns=columns_to_check, axis=1)

### Add the final_series column.
## When count =1, then the final series would be the one that has value.
#for col in [100, 200, 300, 400, 500, 1000, 1100]:
for col in columns_to_check:
    pivot_df_current_4.loc[(pivot_df_current_4['count'] == 1) & (pivot_df_current_4[col] == 1), 'final_series'] = col

print(pivot_df_current_4.head(10))

## When count is over 1, we want minimum switch
# First from bottom up. Forward checking with next row, if same country and the next row has a series choosen, then use the same sereis if also have the same sereis..
for i in range(len(pivot_df_current_4) - 2, -1, -1):  # start from second last to the first row
    if pd.isna(pivot_df_current_4.loc[i, 'final_series']) and pivot_df_current_4.loc[i, 'count'] > 1: # if in row i, final_sereis is still NA and count >1
        if pivot_df_current_4.loc[i, 'country'] == pivot_df_current_4.loc[i + 1, 'country'] and pd.notna(pivot_df_current_4.loc[i + 1, 'final_series']): # if same country for row i and i+1 & row I+1 is not NA
            next_col = pivot_df_current_4.loc[i + 1, 'final_series']
            if pivot_df_current_4.loc[i, next_col] == 1 and pivot_df_current_4.loc[i + 1, next_col] == 1:
                pivot_df_current_4.loc[i, 'final_series'] = next_col


# Backward checking with previous row, if final_sereis is still NA, if same country and the previous row has a series choosen, then use the same sereis if also have the same sereis..
for i in range(1, len(pivot_df_current_4)):
    if pd.isna(pivot_df_current_4.loc[i, 'final_series']) and pivot_df_current_4.loc[i, 'count'] > 1:
        if pivot_df_current_4.loc[i, 'country'] == pivot_df_current_4.loc[i - 1, 'country'] and pd.notna(pivot_df_current_4.loc[i - 1, 'final_series']):
            prev_col = pivot_df_current_4.loc[i - 1, 'final_series']
            # Check if both this and previous row have 1 in the same column
            if pivot_df_current_4.loc[i, prev_col] == 1:
                pivot_df_current_4.loc[i, 'final_series'] = prev_col

# Check for NAs
#print(pivot_df_current_4.head(10))
na_count = pivot_df_current_4['final_series'].isna().sum()
print(f' The number of NA  in column final_series is {na_count}')
# 10 NA when checking both next and previous rows.

# Take a look at the NA rows.
country_check = pivot_df_current_4.loc[pivot_df_current_4['final_series'].isna(),'country']
print(country_check)
#  country name is Bosnia and Herzegovina

series_check = pivot_df_current_4[pivot_df_current_4['country']=='Bosnia and Herzegovina']
print(series_check)
# Based on the information, can use the hgihest sereis directly.
pivot_df_current_4.loc[pivot_df_current_4['final_series'].isna(), 'final_series'] = pivot_df_current_4['highest_series']
##Manully put the highest series number for NA values,

na_count = pivot_df_current_4['final_series'].isna().sum()
print(f' The number of NA  in column final_series is {na_count}')
# na_count is zero now.


## Check for switch and add overlap.
pivot_df_current_4['n_series'] = pivot_df_current_4.groupby('country')['final_series'].transform('nunique')
print(pivot_df_current_4.head(10))
print(pivot_df_current_4['n_series'].describe())
# There are several countries that has switches. n_series has value over 1 (2 or 3 here).
distribution = pivot_df_current_4.drop_duplicates('country')['n_series'].value_counts()
print(distribution)
# 90 countries has no switch. 18 countries have 1 switch. 1 country has 2 switch.
# check for final result against it (should have 20 new lines added).
print(pivot_df_current_4.tail(10))

pivot_df_current_4['overlap'] = None
# From bottom up. for the same country, if final_series are different for row i and i-1
for i in range(len(pivot_df_current_4) - 2, -1, -1):  # start from second last one to the first row
    if pivot_df_current_4.loc[i, 'country'] == pivot_df_current_4.loc[i+1, 'country'] and pivot_df_current_4.loc[i, 'final_series'] != pivot_df_current_4.loc[i + 1, 'final_series']: #same country but different final_series (a switch)
        this_col = pivot_df_current_4.loc[i, 'final_series']
        if pivot_df_current_4.loc[i+1, this_col] == 1:
            new_row = pivot_df_current_4.iloc[i+1].copy()
            new_row['final_series'] = pivot_df_current_4.loc[i, 'final_series']
            new_row['overlap'] = True
            pivot_df_current_4.loc[i+1, 'overlap'] = True
            pivot_df_current_4 = pd.concat([pivot_df_current_4.iloc[:i+1], pd.DataFrame([new_row], columns=pivot_df_current_4.columns), pivot_df_current_4.iloc[i+1:]]).reset_index(drop=True)

print(pivot_df_current_4.tail(10))
#1908 rows - 1889 rows = 19 rows added
#still missing 1 row. Needs to check the missing part.

filtered_df = pivot_df_current_4[pivot_df_current_4['n_series']>1]
result = filtered_df.groupby('country').apply(lambda g: g['overlap'].isna().all())
filtered_countries = result[result].index.tolist()
print(f'countries with switch but do not have overlap lines: {filtered_countries}.')
# countries with switch but do not have overlap lines: ['Malta'].

overlap_check = pivot_df_current_4[pivot_df_current_4['country']=='Malta']
print(overlap_check)
# No overlap. From 2001 to 2013, final series is 300. IN 2014, final series is 1000.
# Drop year 2014.
index_to_drop = pivot_df_current_4[(pivot_df_current_4['country']=='Malta') & (pivot_df_current_4['year']==2014)].index
pivot_df_current_4.drop(index_to_drop, inplace=True)

overlap_check = pivot_df_current_4[pivot_df_current_4['country']=='Malta']
print(overlap_check)

# column n_series can also be changed to 1. Update the calulation of n_series.
pivot_df_current_4['n_series'] = pivot_df_current_4.groupby('country')['final_series'].transform('nunique')

overlap_check = pivot_df_current_4[pivot_df_current_4['country']=='Malta']
print(overlap_check)

pivot_df_current_4['final_series'] = pivot_df_current_4['final_series'].astype(int)
pivot_df_current_4.reset_index(drop=True, inplace=True)
print(pivot_df_current_4.tail(10))

pivot_df_current_4.to_csv("/Users/Danjing 1/Lingsu/Jobs/2024 WB STC/Sector/Process/current_4_4final.csv")

df_current_4_final = pivot_df_current_4[['country','year','final_series','overlap']]
print(df_current_4_final.head())
df_current_4_final.to_csv("/Users/Danjing 1/Lingsu/Jobs/2024 WB STC/Sector/Final/current_4_final.csv")







