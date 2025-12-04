library(httr)
library(jsonlite)
library(rjstat)

# Define your URL (same as before)
url <- "https://api.statbank.dk/v1/data/FOLK1A/JSONSTAT?OMR%C3%85DE=791&K%C3%98N=TOT%2C1%2C2&ALDER=IALT&CIVILSTAND=TOT%2CU%2CG%2CE%2CF&Tid=>2008K1<2025K4"

# Send GET request
response <- GET(url)

# Extract the content as a JSON string
json_data <- content(response, "text")

# Use rjstat to parse the JSON-stat data into a usable format
parsed_data <- fromJSONstat(json_data)

# The parsed data is a list. Extract the first table from the parsed data
df <- parsed_data[[1]]

# Check the structure of the data
str(df)

# View the data in a table format
View(df)

