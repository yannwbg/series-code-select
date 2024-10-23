### Pre-setting
import pandas as pd
import numpy as np
pd.set_option('display.max_columns', None)

#### Read the file
filepath = "/Users/Danjing 1/Lingsu/Jobs/2024 WB STC/Sector/Process/un3_current_5cols.csv"

df_current_3 = pd.read_csv(filepath, index_col=None)
#print(df_current_3.head())

### Data cleaning
df_current_3 = df_current_3.iloc[:,1:4]
df_current_3['valid_data'] = True
print(df_current_3.head())

### Data transforming to show for each country & year combination, what series has value.
pivot_df_current_3 = df_current_3.pivot_table(index=['country','year'], columns='series_code', values = 'valid_data', aggfunc=('count'))
#print(pivot_df_current_3.head())

### Reset the index to make 'country' into columns
pivot_df_current_3.reset_index(inplace=True)
print(pivot_df_current_3.head())
# Now you have a new dataframe with columns country, year, 10, 20, 30, 40, 50, 60, 100, 150, 200, 300, 400, 500, 1000, 1100

### Add a column count, to show how many valid series options are there.
pivot_df_current_3['count'] = pivot_df_current_3.iloc[:,2:16].sum(axis=1)
#print(pivot_df_current_3['count'].describe()) #until 75% the numer is still 1. max is 3.
# Now you have a new dataframe with columns country, year, 10, 20, 30, 40, 50, 60, 100, 150, 200, 300, 400, 500, 1000, 1100，count.

###Add a column, to put the highest sereis value in this column.
#List of columns to check
columns_to_check = [10, 20, 30, 40, 50, 100, 200, 300, 400, 500, 1000, 1100]

# Function to get highest non_NA column name
def get_highest_non_na_column(row, columns):
    # Filter the row to include only the desired columns and drop NAs
    valid_columns = row[columns].dropna()
    # Return the highest numbered column name, converting to int for comparison
    if not valid_columns.empty:
        return max(valid_columns.index, key=int)
    return np.nan

# Apply the function to each row in the DataFrame
pivot_df_current_3['highest_series'] = pivot_df_current_3.apply(get_highest_non_na_column, columns=columns_to_check, axis=1)
#print(pivot_df_current_3.head(10))

### Add the final_series column.
## When count =1, then the final series would be the one that has value.
for col in columns_to_check:
    pivot_df_current_3.loc[(pivot_df_current_3['count'] == 1) & (pivot_df_current_3[col] == 1), 'final_series'] = col

print(pivot_df_current_3.head(10))

## When count is over 1, we want to minimum switch
# First from bottom up. Forward checking with next row, if same country and the next row has a series choosen, then use the same sereis if also have the same sereis..
for i in range(len(pivot_df_current_3) - 2, -1, -1):  # start from second last to the first row
    if pd.isna(pivot_df_current_3.loc[i, 'final_series']) and pivot_df_current_3.loc[i, 'count'] > 1: # if in row i, final_sereis is still NA and count >1
        if pivot_df_current_3.loc[i, 'country'] == pivot_df_current_3.loc[i + 1, 'country'] and pd.notna(pivot_df_current_3.loc[i + 1, 'final_series']): # if same country for row i and i+1 & row I+1 is not NA
            next_col = pivot_df_current_3.loc[i + 1, 'final_series']
            if pivot_df_current_3.loc[i, next_col] == 1 and pivot_df_current_3.loc[i + 1, next_col] == 1:
                pivot_df_current_3.loc[i, 'final_series'] = next_col


# Backward checking with previous row, if final_sereis is still NA, if same country and the previous row has a series choosen, then use the same sereis if also have the same sereis..
for i in range(1, len(pivot_df_current_3)):
    if pd.isna(pivot_df_current_3.loc[i, 'final_series']) and pivot_df_current_3.loc[i, 'count'] > 1:
        if pivot_df_current_3.loc[i, 'country'] == pivot_df_current_3.loc[i - 1, 'country'] and pd.notna(pivot_df_current_3.loc[i - 1, 'final_series']):
            prev_col = pivot_df_current_3.loc[i - 1, 'final_series']
            # Check if both this and previous row have 1 in the same column
            if pivot_df_current_3.loc[i, prev_col] == 1:
                pivot_df_current_3.loc[i, 'final_series'] = prev_col

# Check for NAs
#print(pivot_df_current_3.head(10))
na_count = pivot_df_current_3['final_series'].isna().sum()
print(f' The number of NA  in column final_series is {na_count}')
# 54 NA when checking both next and previous rows.
#  too many NAs, export to a csv file to check.

filtered_df = pivot_df_current_3[pivot_df_current_3['final_series'].isna()]
countries_nan_series = filtered_df['country'].unique().tolist()
num_countries_with_gaps = len(countries_nan_series)
print(f'There are {num_countries_with_gaps} countries with NaN in final_series: {countries_nan_series}')
#There are 7 countries with NaN in final_series: ['Finland', 'Greece', 'Hong Kong SAR, China', 'Korea, Rep.', 'Russian Federation', 'South Africa', 'Yemen, Rep.']
df_series_check = pivot_df_current_3[pivot_df_current_3['country'].isin(countries_nan_series)]
df_series_check.to_csv("/Users/Danjing 1/Lingsu/Jobs/2024 WB STC/Sector/Process/un3_current_series_NAcheck.csv")

# Now let's use the hgihest sereis to replace NA.
pivot_df_current_3.loc[pivot_df_current_3['final_series'].isna(), 'final_series'] = pivot_df_current_3['highest_series']

na_count = pivot_df_current_3['final_series'].isna().sum()
print(f' The number of NA  in column final_series is {na_count}')
# na_count should be zero now.

### Check for switch and add overlap.
## check for switch
pivot_df_current_3['n_series'] = pivot_df_current_3.groupby('country')['final_series'].transform('nunique')
# print(pivot_df_current_3.head(10))
print(pivot_df_current_3['n_series'].describe())
# There are several countries that has switches. n_series has value over 1 (2 or 3 here).
distribution = pivot_df_current_3.drop_duplicates('country')['n_series'].value_counts()
print(distribution)
# 65 countries has no switch. 41 countries have 1 switch. 7 country has 2 switch. 1 countries have 3 switch
# check for final result against it (should have 41+14+3=58 new lines added).
print(pivot_df_current_3.tail(10))
# currently there are 1838 lines.

# Add a column 'switch' to show if there's a switch of sereis code within one country.
pivot_df_current_3['switch'] = None
for i in range(1, len(pivot_df_current_3)):
    if (pivot_df_current_3.loc[i, 'country'] == pivot_df_current_3.loc[i-1, 'country']) & (pivot_df_current_3.loc[i, 'final_series'] != pivot_df_current_3.loc[i - 1, 'final_series']): # same country, different series
        pivot_df_current_3.loc[i-1, 'switch'] = True
        pivot_df_current_3.loc[i, 'switch'] = True


## Add a column 'overlap'. If there's a row during the switch that can serve as the overlap line, mark it and insert the new line.
pivot_df_current_3['overlap'] = None
# From bottom up. for the same country, if final_series are different for row i and i+1
for i in range(len(pivot_df_current_3) - 2, -1, -1):  # start from second last one to the first row
    if pivot_df_current_3.loc[i, 'country'] == pivot_df_current_3.loc[i+1, 'country'] and pivot_df_current_3.loc[i, 'final_series'] != pivot_df_current_3.loc[i + 1, 'final_series']: #same country but different final_series (a switch)
        this_col = pivot_df_current_3.loc[i, 'final_series']
        if pivot_df_current_3.loc[i+1, this_col] == 1:
            new_row = pivot_df_current_3.iloc[i+1].copy()
            new_row['final_series'] = pivot_df_current_3.loc[i, 'final_series']
            new_row['overlap'] = True
            pivot_df_current_3.loc[i+1, 'overlap'] = True
            pivot_df_current_3 = pd.concat([pivot_df_current_3.iloc[:i+1], pd.DataFrame([new_row], columns=pivot_df_current_3.columns), pivot_df_current_3.iloc[i+1:]]).reset_index(drop=True)

# No need to consider if row i could serve as the overlap line, because it would already be changed to the same final_series as row+1 in previous steps.

print(pivot_df_current_3.tail(10))
#1875 rows - 1838 rows = 37 rows added
#still missing 21 row. Needs to check the missing part.


### Deal with the situation there is no overlap rows for the switch
filtered_df = pivot_df_current_3[pivot_df_current_3['n_series']>1]
result = filtered_df.groupby('country').apply(lambda g: g['overlap'].isna().all())
filtered_countries = result[result].index.tolist()
print(f'countries with switch but do not have overlap lines: {filtered_countries}.')
# countries with switch but do not have overlap lines: ['Andorra', 'Austria', 'Bahrain', 'Cabo Verde', 'Germany', 'Malaysia', 'Niger', 'Poland', 'Qatar', 'Serbia', 'Spain', 'Sri Lanka', 'Tunisia', 'Türkiye', 'United Kingdom'].
# Too many countries. Need to export to csv file to check.
filtered_df = filtered_df[filtered_df['country'].isin(filtered_countries)]
filtered_df.to_csv("/Users/Danjing 1/Lingsu/Jobs/2024 WB STC/Sector/Process/un3_current_overlapcheck.csv")

# # For switch lines that have count >1 for both lines, there might be a chance to have a third switch to keep all rows.
# ############################## Need to talk with Yann, figuring out how to do the switch. Do it manually or with code.
# # Check for countries: Malaysia, Malta, etc
#
# # (For switch lines that have counts = 1 for both lines, had to drop some rows.)
# # Had to drop the series that has less counts for this country.
# #################################################################################### Need to talk with Yann to confirm.
# filtered_df['series_count'] = filtered_df.groupby(['country', 'final_series'])['final_series'].transform('size')
# filtered_df['max_count'] = filtered_df.groupby('country')['series_count'].transform('max')
# df_max_count = filtered_df[filtered_df['series_count'] == filtered_df['max_count']] # keep only the sereis with highest count. But if 2 series code have the same count
#
# # If the counts are the same for different series, than pick up the one with latest (higher series)
# df_max_count['max_final_series'] = df_max_count.groupby('country')['final_series'].transform('max')
# df_overlap = df_max_count[df_max_count['final_series'] == df_max_count['max_final_series']]
# df_overlap['overlap_checked'] = True
# df_overlap_mapping = df_overlap[['country', 'year', 'overlap_checked']]
#
# #print(df_overlap.head(20))
# #filtered_df.to_csv("/Users/Danjing 1/Lingsu/Jobs/2024 WB STC/Sector/Process/current_3_overlapcheck.csv")
# #df_overlap.to_csv("/Users/Danjing 1/Lingsu/Jobs/2024 WB STC/Sector/Process/current_3_overlapcheck1.csv")
# df_overlap_mapping.to_csv("/Users/Danjing 1/Lingsu/Jobs/2024 WB STC/Sector/Process/current_3_overlap_mapping.csv")
#
# # merge the mapping back to main dataframe and drop the rows not needed.
# pivot_df_current_3 = pd.merge(pivot_df_current_3, df_overlap_mapping, on = ['country', 'year'], how='left')
# #print(pivot_df_current_3.head())
#
# to_drop = pivot_df_current_3['country'].isin(filtered_countries) & pivot_df_current_3['overlap_checked'].isna()
# pivot_df_current_3 = pivot_df_current_3[~ to_drop]
# print(pivot_df_current_3.head())
# #pivot_df_current_3.to_csv("/Users/Danjing 1/Lingsu/Jobs/2024 WB STC/Sector/Process/current_3_try.csv")


### Gap check
pivot_df_current_3 = pivot_df_current_3.sort_values(by=['country','year'])
pivot_df_current_3['year_diff'] = pivot_df_current_3.groupby('country')['year'].diff()
pivot_df_current_3['has_gap'] = pivot_df_current_3['year_diff']>1
countries_with_gaps = pivot_df_current_3[pivot_df_current_3['has_gap']]['country'].unique()
num_countries_with_gaps = len(countries_with_gaps)
print(f' There are {num_countries_with_gaps} countries have year gaps, including: {countries_with_gaps}')
#  There are 31 countries have year gaps, including: ['Andorra' 'Argentina' 'Austria' 'Belize' 'Burkina Faso' 'Cabo Verde'
#  'Cameroon' 'Colombia' 'Czechia' 'Germany' 'Iceland' 'Iraq' 'Ireland'
#  'Italy' 'Jamaica' 'Jordan' 'Kenya' 'Luxembourg' 'Mauritania' 'Mexico'
#  'Montenegro' 'Namibia' 'Netherlands' 'Niger' 'Norway' 'Sri Lanka'
#  'Turks and Caicos Islands' 'Türkiye' 'Ukraine' 'United Kingdom'
#  'West Bank and Gaza']

df_gap_current3 = pivot_df_current_3[pivot_df_current_3['country'].isin(countries_with_gaps)]
df_gap_current3.to_csv("/Users/Danjing 1/Lingsu/Jobs/2024 WB STC/Sector/Process/un3_current_gap.csv")
# Need mannul check


### Final Data Clean and Export
pivot_df_current_3['final_series'] = pivot_df_current_3['final_series'].astype(int)
pivot_df_current_3.reset_index(drop=True, inplace=True)
print(pivot_df_current_3.tail(10))

# Merge the iso3
iso_mapping_current_3 = pd.read_csv("/Users/Danjing 1/Lingsu/Jobs/2024 WB STC/Sector/Process/iso_mapping_current_3.csv")
pivot_df_current_3 = pd.merge(pivot_df_current_3, iso_mapping_current_3, on='country', how='left')
print(pivot_df_current_3.tail(10))

pivot_df_current_3.to_csv("/Users/Danjing 1/Lingsu/Jobs/2024 WB STC/Sector/Process/un3_current_Forfinal.csv")

df_current_3_final = pivot_df_current_3[['country','year','final_series','overlap','iso3']]
print(df_current_3_final.head())
df_current_3_final.to_csv("/Users/Danjing 1/Lingsu/Jobs/2024 WB STC/Sector/Final/un3_current_final.csv")


# ## Need to add year gap -- in the begining or in the end? Need to apply for ISICC 3 files too. If there's gap, keep them and mark or delete them?
# ## For switch lines that have count >1 for both lines, there might be a chance to have a third switch to keep all rows.
# #
# #
