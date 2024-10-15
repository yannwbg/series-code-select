filter_sectors_ISIC3 <- function(df) {
  # Get unique combinations of country, year, and series
  unique_combinations <- unique(df[, c("country", "year", "series_code")])
  
  # Initialize an empty data frame to store the results
  filtered_df <- data.frame()
  
  for (i in 1:nrow(unique_combinations)) {
    
    print(paste(round((i/nrow(unique_combinations))*100,2), "% completed."))
    
    # Extract the current combination
    combination <- unique_combinations[i, ]
    
    # Filter the data frame for the current combination
    combination_df <- subset(df, country == combination$country & year == combination$year & series_code == combination$series_code)
    
    #Select sectors that always need to be present
    filtered_df <- rbind(filtered_df, subset(combination_df, code %in% c("C", "D", "E", "F", "I", "L", "P", "B.1g")))
    
    # Check if both 'A' and 'B' are present
    if ('A' %in% combination_df$code & 'B' %in% combination_df$code) {
      # Keep both 'A' and 'B'
      filtered_df <- rbind(filtered_df, subset(combination_df, code %in% c('A', 'B')))
    } else {
      # Keep 'A+B'
      filtered_df <- rbind(filtered_df, subset(combination_df, code == 'A+B'))
    }
    
    # Check if both 'G' and 'H' are present
    if ('G' %in% combination_df$code & 'H' %in% combination_df$code) {
      # Keep both 'G' and 'H'
      filtered_df <- rbind(filtered_df, subset(combination_df, code %in% c('G', 'H')))
    } else {
      # Keep 'G+H'
      filtered_df <- rbind(filtered_df, subset(combination_df, code == 'G+H'))
    }
    
    # Check if both 'J' and 'K' are present
    if ('J' %in% combination_df$code & 'K' %in% combination_df$code) {
      # Keep both 'J' and 'K'
      filtered_df <- rbind(filtered_df, subset(combination_df, code %in% c('J', 'K')))
    } else {
      # Keep 'J+K'
      filtered_df <- rbind(filtered_df, subset(combination_df, code == 'J+K'))
    }
    
    # Check if all of 'M', 'N' and 'O' are present
    if ('M' %in% combination_df$code & 'N' %in% combination_df$code & 'O' %in% combination_df$code) {
      # Keep both 'M', 'N' and 'O'
      filtered_df <- rbind(filtered_df, subset(combination_df, code %in% c('M', 'N', 'O')))
    } else {
      # Keep 'M+N+O'
      filtered_df <- rbind(filtered_df, subset(combination_df, code == 'M+N+O'))
    }
    
  }
  
  return(filtered_df)
}