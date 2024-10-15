#Pre-setting
import pandas as pd
pd.set_option('display.max_columns', None)

#Read the file
filepath = "/Users/Danjing 1/Lingsu/Jobs/2024 WB STC/Sector/Process/Current_4_3cols.csv"
df_current_4 = pd.read_csv(filepath, index_col=None)
print(df_current_4.head())

# Data cleaning
df_current_4 = df_current_4.iloc[:,1:4]
df_current_4['valid_data']=True
print(df_current_4.head())

# Data transforming to show for each country & year combination, what series has value.
pivot_df_current_4 = df_current_4.pivot_table(index=['country','year'], columns='series_code', values = 'valid_data', aggfunc=('count'))
#print(pivot_df_current_4.head())

#Reset the index to make 'country' into columns
pivot_df_current_4.reset_index(inplace=True)

pivot_df_current_4['count'] = pivot_df_current_4.iloc[:,2:9].sum(axis=1)
#print(pivot_df_current_4['count'].describe()) #until 75% the numer is still 1. max is 3.

print(pivot_df_current_4.head(10))

#df_over1 = pivot_df_current_4[pivot_df_current_4['count']>1]
#df_over1.to_csv("/Users/Danjing 1/Lingsu/Jobs/2024 WB STC/Sector/Process/current_4_over1.csv")
#print(df_over1.head(10))

# When count =1, then the final series would be the one that has value.
for col in [100, 200, 300, 400, 500, 1000, 1100]:
    pivot_df_current_4.loc[(pivot_df_current_4['count'] == 1) & (pivot_df_current_4[col] == 1), 'final_series'] = col

#pivot_df_current_4['final_series'] = pivot_df_current_4['final_series'].astype(int)

print(pivot_df_current_4.head(10))

# Minimum switch
# when count >1 (2 or 3 here), if same country and the previous row has a sereis choosen, then use the same sereis if also have the same sereis..
for i in range(1, len(pivot_df_current_4)):
    # Backward checking with previous row
    if (pivot_df_current_4.loc[i, 'count'] > 1) and (pivot_df_current_4.loc[i, 'country'] == pivot_df_current_4.loc[i - 1, 'country']) and (pd.notna(pivot_df_current_4.loc[i - 1, 'final_series'])):
        prev_col = pivot_df_current_4.loc[i - 1, 'final_series']
        # Check if both this and previous row have 1 in the same column
        if pivot_df_current_4.loc[i, prev_col] == 1:
            pivot_df_current_4.loc[i, 'final_series'] = prev_col

#Forward checking with next row if final_sereis is still NA
for i in range(len(pivot_df_current_4) - 2, -1, -1):  # start from second last to the first row
    if pd.isna(pivot_df_current_4.loc[i, 'final_series']) and pivot_df_current_4.loc[i, 'count'] > 1: # if still NA and count >1
        if pivot_df_current_4.loc[i, 'country'] == pivot_df_current_4.loc[i + 1, 'country'] and pd.notna(pivot_df_current_4.loc[i + 1, 'final_series']): # if same country for row i and i+1 & row I+1 is not NA
            next_col = pivot_df_current_4.loc[i + 1, 'final_series']
            if pivot_df_current_4.loc[i, next_col] == 1 and pivot_df_current_4.loc[i + 1, next_col] == 1:
                pivot_df_current_4.loc[i, 'final_series'] = next_col

# for i in range(len(pivot_df_current_4) - 2, -1, -1):
#     if pd.isna(pivot_df_current_4.loc[i, 'final_series']) and pivot_df_current_4.loc[i, 'count'] > 1:
#         if pivot_df_current_4.loc[i, 'country'] == pivot_df_current_4.loc[i + 1, 'country'] and pd.notna(pivot_df_current_4.loc[i + 1, 'final_series']):
#             next_col = pivot_df_current_4.loc[i + 1, 'final_series']
#             col_position = pivot_df_current_4.columns.get_loc(next_col)
#             n = pivot_df_current_4.loc[i + 1, 'count']  # in row i+1
#
#             if n>1: # i+1 might be the overlap row.
#                 for col_index in range(col_position-1, 1, -1):
#                     if pivot_df_current_4.loc[i, col_index] == 1 and pivot_df_current_4.loc[i + 1, col_index] == 1:
#                         pivot_df_current_4.loc[i+1, 'overlap'] = True
#                         pivot_df_current_4.loc[i, 'final_series'] = next_col
#                         break


#print(pivot_df_current_4.head(10))
na_count = pivot_df_current_4['final_series'].isna().sum()
print(f' The number of NA  in column final_series is {na_count}')
# 173 NA when only checking previous row.
# 10 NA when checking both previous row and next row.



#pivot_df_current_4.to_csv("/Users/Danjing 1/Lingsu/Jobs/2024 WB STC/Sector/Process/current_4_try.csv")









