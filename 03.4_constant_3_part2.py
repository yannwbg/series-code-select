## Pre-setting
import pandas as pd
import numpy as np
pd.set_option('display.max_columns', None)

#### Read the file
filepath = "/Users/Danjing 1/Lingsu/Jobs/2024 WB STC/Sector/Process/constant_3_4cols.csv"

df_constant_3 = pd.read_csv(filepath, index_col=None)
#print(df_constant_3.head())

### Data cleaning
df_constant_3 = df_constant_3.iloc[:,1:5]
df_constant_3['valid_data'] = True
#print(df_constant_3.head())

### Data transforming to show for each country & year combination, what series has value.
pivot_df_constant_3 = df_constant_3.pivot_table(index=['country','year','base'], columns='series_code', values = 'valid_data', aggfunc=('count'))
#print(pivot_df_constant_3.head())

####Reset the index to make 'country' into columns
pivot_df_constant_3.reset_index(inplace=True)
#print(pivot_df_constant_3.head())
# Now you have a new dataframe with columns country, year, base, 10, 20, 30, 40, 50, 60, 100, 150, 200, 300, 350, 400, 500, 1000, 1100

### Add a column count, to show how many valid series options are there.
pivot_df_constant_3['count'] = pivot_df_constant_3.iloc[:,3:18].sum(axis=1)
print(pivot_df_constant_3['count'].describe()) #until 75% the numer is still 1. max is 4.

print(pivot_df_constant_3.head(10))
# Now you have a new dataframe with columns country, year, base, 10, 20, 30, 40, 50, 60, 100, 150, 200, 300, 350, 400, 500, 1000, 1100, count.

###Add a column, to put the highest sereis value in this column.
#List of columns to check
columns_to_check = [10, 20, 30, 40, 50, 60, 100, 150, 200, 300, 350, 400, 500, 1000, 1100]

# Function to get highest non_NA column name
def get_highest_non_na_column(row, columns):
    # Filter the row to include only the desired columns and drop NAs
    valid_columns = row[columns].dropna()
    # Return the highest numbered column name, converting to int for comparison
    if not valid_columns.empty:
        return max(valid_columns.index, key=int)
    return np.nan

# Apply the function to each row in the DataFrame
pivot_df_constant_3['highest_series'] = pivot_df_constant_3.apply(get_highest_non_na_column, columns=columns_to_check, axis=1)
#print(pivot_df_constant_3.head(10))

### Add the final_series column.
## When count =1, then the final series would be the one that has value.
for col in columns_to_check:
    pivot_df_constant_3.loc[(pivot_df_constant_3['count'] == 1) & (pivot_df_constant_3[col] == 1), 'final_series'] = col

print(pivot_df_constant_3.head(10))

# sort the dataframe with 1. country, 2. base year, 3. year
pivot_df_constant_3 = pivot_df_constant_3.sort_values(by=['country','base','year'])

na_count = pivot_df_constant_3['final_series'].isna().sum()
print(f' The number of NA  in column final_series is {na_count}')
# 715 NA
# print(pivot_df_constant_3['base'].describe())

## When count is over 1, we want minimum switch
# First from bottom up. Forward checking with next row, if same country and the next row has a series choosen, then use the same sereis if also have the same sereis..
for i in range(len(pivot_df_constant_3) - 2, -1, -1):  # start from second last to the first row
    if pd.isna(pivot_df_constant_3.loc[i, 'final_series']) and pivot_df_constant_3.loc[i, 'count'] > 1: # if in row i, final_sereis is still NA and count >1
        if pivot_df_constant_3.loc[i, 'country'] == pivot_df_constant_3.loc[i + 1, 'country'] and pivot_df_constant_3.loc[i, 'base'] == pivot_df_constant_3.loc[i + 1, 'base'] and pd.notna(pivot_df_constant_3.loc[i + 1, 'final_series']): # if same country & base year for row i and i+1 & row I+1 is not NA
            next_col = pivot_df_constant_3.loc[i + 1, 'final_series']
            if pivot_df_constant_3.loc[i, next_col] == 1 and pivot_df_constant_3.loc[i + 1, next_col] == 1:
                pivot_df_constant_3.loc[i, 'final_series'] = next_col
#Why there are more NA after adding: and pivot_df_constant_3.loc[i, 'base'] == pivot_df_constant_3.loc[i + 1, 'base']???

# Backward checking with previous row, if final_sereis is still NA, if same country and the previous row has a series choosen, then use the same sereis if also have the same sereis..
for i in range(1, len(pivot_df_constant_3)):
    if pd.isna(pivot_df_constant_3.loc[i, 'final_series']) and pivot_df_constant_3.loc[i, 'count'] > 1:
        if pivot_df_constant_3.loc[i, 'country'] == pivot_df_constant_3.loc[i - 1, 'country'] and pivot_df_constant_3.loc[i, 'base'] == pivot_df_constant_3.loc[i - 1, 'base'] and pd.notna(pivot_df_constant_3.loc[i - 1, 'final_series']):
            prev_col = pivot_df_constant_3.loc[i - 1, 'final_series']
            # Check if both this and previous row have 1 in the same column
            if pivot_df_constant_3.loc[i, prev_col] == 1:
                pivot_df_constant_3.loc[i, 'final_series'] = prev_col

#Why there are more NA after adding: ?and pivot_df_constant_3.loc[i, 'base'] == pivot_df_constant_3.loc[i - 1, 'base']???

# Check for NAs
#print(pivot_df_constant_3.head(10))
na_count = pivot_df_constant_3['final_series'].isna().sum()
print(f' The number of NA  in column final_series is {na_count}')
# 390 NA when checking both next and previous rows.
#pivot_df_constant_3.to_csv("/Users/Danjing 1/Lingsu/Jobs/2024 WB STC/Sector/Process/constant_3_try.csv")

# #Take a look at the NA rows.
# country_check = pivot_df_constant_3.loc[pivot_df_constant_3['final_series'].isna(),'country']
# print(country_check)
# # #  country names are Bosnia and Herzegovina, Cyprus, Montenegro, Philippines, Romania, Serbia.
# ## Only Bosnia and Herzegovina should be here. Other countries should be captured based on the meaning of the for loop. Why???

# Take a look at the NA rows.
country_check = pivot_df_constant_3.loc[pivot_df_constant_3['final_series'].isna(),'country']
print(country_check)
pivot_df_constant_3.to_csv("/Users/Danjing 1/Lingsu/Jobs/2024 WB STC/Sector/Process/constant_3_nacheck.csv")
#  too many NAs, export to a csv file to check.


# Based on the information, can use the highest series directly.

# series_check = pivot_df_constant_3[pivot_df_constant_3['country']=='Philippines']
# print(series_check)
# # Based on the information, can use the highest sereis directly.

pivot_df_constant_3.loc[pivot_df_constant_3['final_series'].isna(), 'final_series'] = pivot_df_constant_3['highest_series']
##Manully put the highest series number for NA values,

na_count = pivot_df_constant_3['final_series'].isna().sum()
print(f' The number of NA  in column final_series is {na_count}')
# na_count is zero now.

## Check for switch and add overlap.
pivot_df_constant_3['n_series'] = pivot_df_constant_3.groupby(['country','base'])['final_series'].transform('nunique')
print(pivot_df_constant_3.head(10))
print(pivot_df_constant_3['n_series'].describe())
# There are some countries & base year combination that has switches. n_series has value over 1 (2 here).
distribution = pivot_df_constant_3.drop_duplicates(['country','base'])['n_series'].value_counts()
print(distribution)
# 433 countries & base combination has no switch. 52 combination have 1 switch. 8 combination have 2 switch.
# check for final result against it (should have 52+16=68 new lines added).
print(pivot_df_constant_3.tail(10))
# 5450 lines

pivot_df_constant_3['overlap'] = None
# From bottom up. for the same country, if final_series are different for row i and i+1
for i in range(len(pivot_df_constant_3) - 2, -1, -1):  # start from second last one to the first row
    if pivot_df_constant_3.loc[i, 'country'] == pivot_df_constant_3.loc[i+1, 'country'] and pivot_df_constant_3.loc[i, 'base'] == pivot_df_constant_3.loc[i + 1, 'base'] and pivot_df_constant_3.loc[i, 'final_series'] != pivot_df_constant_3.loc[i + 1, 'final_series']: #same country but different final_series (a switch)
        this_col = pivot_df_constant_3.loc[i, 'final_series']
        if pivot_df_constant_3.loc[i+1, this_col] == 1:
            new_row = pivot_df_constant_3.iloc[i+1].copy()
            new_row['final_series'] = pivot_df_constant_3.loc[i, 'final_series']
            new_row['overlap'] = True
            pivot_df_constant_3.loc[i+1, 'overlap'] = True
            pivot_df_constant_3 = pd.concat([pivot_df_constant_3.iloc[:i+1], pd.DataFrame([new_row], columns=pivot_df_constant_3.columns), pivot_df_constant_3.iloc[i+1:]]).reset_index(drop=True)

print(pivot_df_constant_3.tail(10))
#5490 rows - 5450 rows = 40 rows missing
# Need to check the missing part.

filtered_df = pivot_df_constant_3[pivot_df_constant_3['n_series']>1]
result = filtered_df.groupby(['country','base']).apply(lambda g: g['overlap'].isna().all())
filtered_countries = result[result].index.tolist()
print(f'countries with switch but do not have overlap lines: {filtered_countries}.')
# countries with switch but do not have overlap lines: [('Austria', 2000), ('Bahrain', 1989), ('Belarus', -1),
# ('Belize', 1984), ('Bhutan', 2000), ('Botswana', 1994), ('Burkina Faso', 1999), ('Eswatini', 1985), ('Fiji', 1995),
# ('Georgia', -1), ('Iraq', 1975), ('Israel', 1995), ('Jamaica', 1986), ('Kazakhstan', -1), ('Mexico', 1993),
# ('Mongolia', 2000), ('Niger', 1987), ('North Macedonia', -1), ('Papua New Guinea', 1983), ('Qatar', 1988),
# ('Russian Federation', -1), ('Uzbekistan', -1), ('Yemen, Rep.', 2000), ('Zimbabwe', 1990), ('Zimbabwe', 2009)].

# Too many countries. Need to export to csv file to check.
filter_df = pd.DataFrame(filtered_countries, columns=['country', 'base'])
filtered_df = pd.merge(pivot_df_constant_3,filter_df, on=['country','base'])
print(filtered_df.head())
filtered_df.to_csv("/Users/Danjing 1/Lingsu/Jobs/2024 WB STC/Sector/Process/constant_3_overlapcheck.csv")


# For switch lines that have count >1 for both lines, there might be a chance to have a third switch to keep all rows.
############################## Need to talk with Yann, figuring out how to do the switch. Do it manually or with code.
# Check for countries: Malaysia, Malta, and Uganda

# (For switch lines that have counts = 1 for both lines, had to drop some rows.)
# Had to drop the series that has less counts for this country.
#################################################################################### Need to talk with Yann to confirm.
filtered_df['series_count'] = filtered_df.groupby(['country', 'base', 'final_series'])['final_series'].transform('size')
filtered_df['max_count'] = filtered_df.groupby(['country','base'])['series_count'].transform('max')
df_max_count = filtered_df[filtered_df['series_count'] == filtered_df['max_count']] # keep only the sereis with highest count. But if 2 series code have the same count

# If the counts are the same for different series, than pick up the one with latest (higher series)
df_max_count['max_final_series'] = df_max_count.groupby(['country','base'])['final_series'].transform('max')
df_overlap = df_max_count[df_max_count['final_series'] == df_max_count['max_final_series']]
df_overlap['overlap_checked'] = True
df_overlap_mapping = df_overlap[['country', 'year', 'base','overlap_checked']]

#print(df_overlap.head(20))
#filtered_df.to_csv("/Users/Danjing 1/Lingsu/Jobs/2024 WB STC/Sector/Process/constant_3_overlapcheck.csv")
#df_overlap.to_csv("/Users/Danjing 1/Lingsu/Jobs/2024 WB STC/Sector/Process/constant_3_overlapcheck1.csv")
df_overlap_mapping.to_csv("/Users/Danjing 1/Lingsu/Jobs/2024 WB STC/Sector/Process/constant_3_overlap_mapping.csv")

# merge the mapping back to main dataframe and drop the rows not needed.
pivot_df_constant_3 = pd.merge(pivot_df_constant_3, df_overlap_mapping, on = ['country', 'year', 'base'], how='left')
#print(pivot_df_constant_3.head())
mask = pivot_df_constant_3.apply(lambda x: (x['country'], x['base']) in filtered_countries, axis=1)
to_drop = mask & pivot_df_constant_3['overlap_checked'].isna()
pivot_df_constant_3 = pivot_df_constant_3[~ to_drop]
print(pivot_df_constant_3.head())
pivot_df_constant_3.to_csv("/Users/Danjing 1/Lingsu/Jobs/2024 WB STC/Sector/Process/constant_3_try.csv")


### Data Clean (final touch)
pivot_df_constant_3['final_series'] = pivot_df_constant_3['final_series'].astype(int)
pivot_df_constant_3.reset_index(drop=True, inplace=True)
print(pivot_df_constant_3.tail(10))

### Gap check
pivot_df_constant_3 = pivot_df_constant_3.sort_values(by=['country','base','year'])
pivot_df_constant_3['year_diff'] = pivot_df_constant_3.groupby(['country','base'])['year'].diff()
pivot_df_constant_3['has_gap'] = pivot_df_constant_3['year_diff']>1
#countries_with_gaps = pivot_df_constant_3[pivot_df_constant_3['has_gap']][['country','base']].unique()
gap_counts = pivot_df_constant_3.groupby(['country','base']).apply(lambda x: x['has_gap'].sum()).reset_index(name = 'gap_count')
gaps_info = gap_counts[gap_counts['gap_count']>0]
print(gap_counts)
print(gaps_info)
#There are several country & base year combination with gaps. 87 rows.

pivot_df_constant_3.to_csv("/Users/Danjing 1/Lingsu/Jobs/2024 WB STC/Sector/Process/constant_3_4final.csv")

df_constant_3_final = pivot_df_constant_3[['country','year','base','final_series','overlap']]
print(df_constant_3_final.head())
df_constant_3_final.to_csv("/Users/Danjing 1/Lingsu/Jobs/2024 WB STC/Sector/Final/constant_3_final.csv")

### Need to know how to fix the gap issue.