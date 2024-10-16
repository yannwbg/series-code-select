import pandas as pd
pd.set_option('display.max_columns', None)

filepath_current_3 = "/Users/Danjing 1/Lingsu/Jobs/2024 WB STC/Sector/Data_original/cleaned/un3_current.csv"
filepath_constant_3 = "/Users/Danjing 1/Lingsu/Jobs/2024 WB STC/Sector/Data_original/cleaned/un3_constant.csv"

df_current_3 = pd.read_csv(filepath_current_3)
print(df_current_3.head())

df_constant_3 = pd.read_csv(filepath_constant_3)
print(df_constant_3.head())

##########Step 1. Before getting started, confirm few things
######## For Current file
### per country, year, and series, is there more than one currency type('Currency')?
unique_counts_currency_current = df_current_3.groupby(['country','year','series_code'])['currency'].nunique()
more_than_two_currency_current = unique_counts_currency_current > 1
count_true_currency_current = more_than_two_currency_current.sum()
print(f"In the Current file, for each combination of country, year, and series, there are {count_true_currency_current} with more than 1 unique Currency type ")
#Zero out of 7774 combinations

### per country, year, and series, is there more than one fiscal year type('Fiscal Year Type')?
unique_counts_fis_current = df_current_3.groupby(['country','year','series_code'])['year_type'].nunique()
more_than_two_fis_current = unique_counts_fis_current > 1
count_true_fis_current = more_than_two_fis_current.sum()
print(f"In the Current file, for each combination of country, year, and series, there are {count_true_fis_current} with more than 1 unique Fiscal Year type ")
#Zero out of 7774 combinations

######## For Constant file
### per country, year, Base Year and series, is there more than one currency type('Currency')?
unique_counts_currency_constant = df_constant_3.groupby(['country','year','base','series_code'])['currency'].nunique()
more_than_two_currency_constant = unique_counts_currency_constant >1
count_true_currency_constant = more_than_two_currency_constant.sum()
print(f"In the Constant file, for each combination of country, year, base year, and series, there are {count_true_currency_constant} with more than 1 unique Currency type ")
#Zero out of 8771 combinations

### per country, year, Base Year and series, is there more than one fiscal year type('Fiscal Year Type')?
unique_counts_fis_constant = df_constant_3.groupby(['country','year','base','series_code'])['year_type'].nunique()
more_than_two_fis_constant = unique_counts_fis_constant >1
count_true_fis_constant = more_than_two_fis_constant.sum()
print(f"In the Constant file, for each combination of country, year, base year, and series, there are {count_true_fis_constant} with more than 1 unique Fiscal Year type ")
#Zero out of 900 combinations The column year_type seems to have some problem.




