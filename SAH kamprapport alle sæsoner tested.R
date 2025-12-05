library(rvest)
library(httr)
library(stringr)
library(pdftools)

#Define the URL of the page to be scraped

url <- "https://tophaandbold.dk/kampprogram/herreligaen?year=9&team=68&home_game=0&home_game=1&away_game=0"

#Send a GET request to retrieve the page's HTML content

response <- GET(url)

#Check if the request was successful (status code 200)

if (status_code(response) == 200) { 
  
  #Extract the HTML content as text
  page_content <- content(response, "text")
  
  #Parse the raw HTML content into a structured R object
  page <- read_html(page_content) 
  
  #Extract all <a> tags and their corresponding href attributes (links) 
  links <- page %>%
    html_nodes("a")  %>%    #Extract all <a> tags
    html_attr("href")       #Extract href attributes (URLs)
  
  #Extract all the text from the <a> tags
  link_texts <- page %>%
    html_nodes("a")  %>%
    html_text() 
  
  #Now filter for links that contain "Kamprapport"
  kamprapport_links <- links[grepl("Kamprapport", link_texts)]
  
  #Clean up the links (prepend base URL if they are relative links)
  base_url <- "https://tophaandbold.dk"
  full_links <-paste0(base_url, kamprapport_links)
  
  #Clean up any leading/trailing whitespace or newline characters
  cleaned_links <- str_trim(full_links) 
  
  #Print the cleaned links
  print(cleaned_links)
  
  #Optionally, save the links to a CSV file
  write.csv(cleaned_links, "kamprapport_links.csv", row.names = FALSE)
} else {
  #Print error message if the request failed
  stop(paste("Failed to retrieve the page. Status code:", status_code(response)))
}

# Function to extract and view text from a PDF
extract_text_from_pdf <- function(pdf_link) {
  # Download the PDF first
  download.file(pdf_link, destfile = "kamprapport_report.pdf", mode = "wb")
  
  # Extract text from the PDF
  text <- pdf_text("kamprapport_report.pdf")
  
  # Print the first page of text (you can adjust to print more pages if needed)
  cat(text[1])  # Text from the first page
}

# Example: Extract text from the first Kamprapport PDF link
extract_text_from_pdf(cleaned_links[1])  # Extract text from the first report

#Define the URL of the page to be scraped

url <- "https://tophaandbold.dk/kampprogram/herreligaen?year=8&team=68&home_game=0&home_game=1&away_game=0"

#Send a GET request to retrieve the page's HTML content

response <- GET(url)

#Check if the request was successful (status code 200)

if (status_code(response) == 200) { 
  
  #Extract the HTML content as text
  page_content <- content(response, "text")
  
  #Parse the raw HTML content into a structured R object
  page <- read_html(page_content) 
  
  #Extract all <a> tags and their corresponding href attributes (links) 
  links <- page %>%
    html_nodes("a")  %>%    #Extract all <a> tags
    html_attr("href")       #Extract href attributes (URLs)
  
  #Extract all the text from the <a> tags
  link_texts <- page %>%
    html_nodes("a")  %>%
    html_text() 
  
  #Now filter for links that contain "Kamprapport"
  kamprapport_links <- links[grepl("Kamprapport", link_texts)]
  
  #Clean up the links (prepend base URL if they are relative links)
  base_url <- "https://tophaandbold.dk"
  full_links <-paste0(base_url, kamprapport_links)
  
  #Clean up any leading/trailing whitespace or newline characters
  cleaned_links <- str_trim(full_links) 
  
  #Print the cleaned links
  print(cleaned_links)
  
  #Optionally, save the links to a CSV file
  write.csv(cleaned_links, "kamprapport_links.csv", row.names = FALSE)
} else {
  #Print error message if the request failed
  stop(paste("Failed to retrieve the page. Status code:", status_code(response)))
}

# Function to extract and view text from a PDF
extract_text_from_pdf <- function(pdf_link) {
  # Download the PDF first
  download.file(pdf_link, destfile = "kamprapport_report.pdf", mode = "wb")
  
  # Extract text from the PDF
  text <- pdf_text("kamprapport_report.pdf")
  
  # Print the first page of text (you can adjust to print more pages if needed)
  cat(text[1])  # Text from the first page
}

# Example: Extract text from the first Kamprapport PDF link
extract_text_from_pdf(cleaned_links[1])  # Extract text from the first report

#Define the URL of the page to be scraped

url <- "https://tophaandbold.dk/kampprogram/herreligaen?year=7&team=68&home_game=0&home_game=1&away_game=0"

#Send a GET request to retrieve the page's HTML content

response <- GET(url)

#Check if the request was successful (status code 200)

if (status_code(response) == 200) { 
  
  #Extract the HTML content as text
  page_content <- content(response, "text")
  
  #Parse the raw HTML content into a structured R object
  page <- read_html(page_content) 
  
  #Extract all <a> tags and their corresponding href attributes (links) 
  links <- page %>%
    html_nodes("a")  %>%    #Extract all <a> tags
    html_attr("href")       #Extract href attributes (URLs)
  
  #Extract all the text from the <a> tags
  link_texts <- page %>%
    html_nodes("a")  %>%
    html_text() 
  
  #Now filter for links that contain "Kamprapport"
  kamprapport_links <- links[grepl("Kamprapport", link_texts)]
  
  #Clean up the links (prepend base URL if they are relative links)
  base_url <- "https://tophaandbold.dk"
  full_links <-paste0(base_url, kamprapport_links)
  
  #Clean up any leading/trailing whitespace or newline characters
  cleaned_links <- str_trim(full_links) 
  
  #Print the cleaned links
  print(cleaned_links)
  
  #Optionally, save the links to a CSV file
  write.csv(cleaned_links, "kamprapport_links.csv", row.names = FALSE)
} else {
  #Print error message if the request failed
  stop(paste("Failed to retrieve the page. Status code:", status_code(response)))
}

# Function to extract and view text from a PDF
extract_text_from_pdf <- function(pdf_link) {
  # Download the PDF first
  download.file(pdf_link, destfile = "kamprapport_report.pdf", mode = "wb")
  
  # Extract text from the PDF
  text <- pdf_text("kamprapport_report.pdf")
  
  # Print the first page of text (you can adjust to print more pages if needed)
  cat(text[1])  # Text from the first page
}

# Example: Extract text from the first Kamprapport PDF link
extract_text_from_pdf(cleaned_links[1])  # Extract text from the first report

#Define the URL of the page to be scraped

url <- "https://tophaandbold.dk/kampprogram/herreligaen?year=6&team=68&home_game=0&home_game=1&away_game=0"

#Send a GET request to retrieve the page's HTML content

response <- GET(url)

#Check if the request was successful (status code 200)

if (status_code(response) == 200) { 
  
  #Extract the HTML content as text
  page_content <- content(response, "text")
  
  #Parse the raw HTML content into a structured R object
  page <- read_html(page_content) 
  
  #Extract all <a> tags and their corresponding href attributes (links) 
  links <- page %>%
    html_nodes("a")  %>%    #Extract all <a> tags
    html_attr("href")       #Extract href attributes (URLs)
  
  #Extract all the text from the <a> tags
  link_texts <- page %>%
    html_nodes("a")  %>%
    html_text() 
  
  #Now filter for links that contain "Kamprapport"
  kamprapport_links <- links[grepl("Kamprapport", link_texts)]
  
  #Clean up the links (prepend base URL if they are relative links)
  base_url <- "https://tophaandbold.dk"
  full_links <-paste0(base_url, kamprapport_links)
  
  #Clean up any leading/trailing whitespace or newline characters
  cleaned_links <- str_trim(full_links) 
  
  #Print the cleaned links
  print(cleaned_links)
  
  #Optionally, save the links to a CSV file
  write.csv(cleaned_links, "kamprapport_links.csv", row.names = FALSE)
} else {
  #Print error message if the request failed
  stop(paste("Failed to retrieve the page. Status code:", status_code(response)))
}

# Function to extract and view text from a PDF
extract_text_from_pdf <- function(pdf_link) {
  # Download the PDF first
  download.file(pdf_link, destfile = "kamprapport_report.pdf", mode = "wb")
  
  # Extract text from the PDF
  text <- pdf_text("kamprapport_report.pdf")
  
  # Print the first page of text (you can adjust to print more pages if needed)
  cat(text[1])  # Text from the first page
}

# Example: Extract text from the first Kamprapport PDF link
extract_text_from_pdf(cleaned_links[1])  # Extract text from the first report

#Define the URL of the page to be scraped

url <- "https://tophaandbold.dk/kampprogram/herreligaen?year=5&team=68&home_game=0&home_game=1&away_game=0"

#Send a GET request to retrieve the page's HTML content

response <- GET(url)

#Check if the request was successful (status code 200)

if (status_code(response) == 200) { 
  
  #Extract the HTML content as text
  page_content <- content(response, "text")
  
  #Parse the raw HTML content into a structured R object
  page <- read_html(page_content) 
  
  #Extract all <a> tags and their corresponding href attributes (links) 
  links <- page %>%
    html_nodes("a")  %>%    #Extract all <a> tags
    html_attr("href")       #Extract href attributes (URLs)
  
  #Extract all the text from the <a> tags
  link_texts <- page %>%
    html_nodes("a")  %>%
    html_text() 
  
  #Now filter for links that contain "Kamprapport"
  kamprapport_links <- links[grepl("Kamprapport", link_texts)]
  
  #Clean up the links (prepend base URL if they are relative links)
  base_url <- "https://tophaandbold.dk"
  full_links <-paste0(base_url, kamprapport_links)
  
  #Clean up any leading/trailing whitespace or newline characters
  cleaned_links <- str_trim(full_links) 
  
  #Print the cleaned links
  print(cleaned_links)
  
  #Optionally, save the links to a CSV file
  write.csv(cleaned_links, "kamprapport_links.csv", row.names = FALSE)
} else {
  #Print error message if the request failed
  stop(paste("Failed to retrieve the page. Status code:", status_code(response)))
}

# Function to extract and view text from a PDF
extract_text_from_pdf <- function(pdf_link) {
  # Download the PDF first
  download.file(pdf_link, destfile = "kamprapport_report.pdf", mode = "wb")
  
  # Extract text from the PDF
  text <- pdf_text("kamprapport_report.pdf")
  
  # Print the first page of text (you can adjust to print more pages if needed)
  cat(text[1])  # Text from the first page
}

# Example: Extract text from the first Kamprapport PDF link
extract_text_from_pdf(cleaned_links[1])  # Extract text from the first report

#Define the URL of the page to be scraped

url <- "https://tophaandbold.dk/kampprogram/herreligaen?year=4&team=11&home_game=0&home_game=1&away_game=0"

#Send a GET request to retrieve the page's HTML content

response <- GET(url)

#Check if the request was successful (status code 200)

if (status_code(response) == 200) { 
  
  #Extract the HTML content as text
  page_content <- content(response, "text")
  
  #Parse the raw HTML content into a structured R object
  page <- read_html(page_content) 
  
  #Extract all <a> tags and their corresponding href attributes (links) 
  links <- page %>%
    html_nodes("a")  %>%    #Extract all <a> tags
    html_attr("href")       #Extract href attributes (URLs)
  
  #Extract all the text from the <a> tags
  link_texts <- page %>%
    html_nodes("a")  %>%
    html_text() 
  
  #Now filter for links that contain "Kamprapport"
  kamprapport_links <- links[grepl("Kamprapport", link_texts)]
  
  #Clean up the links (prepend base URL if they are relative links)
  base_url <- "https://tophaandbold.dk"
  full_links <-paste0(base_url, kamprapport_links)
  
  #Clean up any leading/trailing whitespace or newline characters
  cleaned_links <- str_trim(full_links) 
  
  #Print the cleaned links
  print(cleaned_links)
  
  #Optionally, save the links to a CSV file
  write.csv(cleaned_links, "kamprapport_links.csv", row.names = FALSE)
} else {
  #Print error message if the request failed
  stop(paste("Failed to retrieve the page. Status code:", status_code(response)))
}

# Function to extract and view text from a PDF
extract_text_from_pdf <- function(pdf_link) {
  # Download the PDF first
  download.file(pdf_link, destfile = "kamprapport_report.pdf", mode = "wb")
  
  # Extract text from the PDF
  text <- pdf_text("kamprapport_report.pdf")
  
  # Print the first page of text (you can adjust to print more pages if needed)
  cat(text[1])  # Text from the first page
}

# Example: Extract text from the first Kamprapport PDF link
extract_text_from_pdf(cleaned_links[1])  # Extract text from the first report

#Define the URL of the page to be scraped

url <- "https://tophaandbold.dk/kampprogram/herreligaen?year=3&team=11&home_game=0&home_game=1&away_game=0"

#Send a GET request to retrieve the page's HTML content

response <- GET(url)

#Check if the request was successful (status code 200)

if (status_code(response) == 200) { 
  
  #Extract the HTML content as text
  page_content <- content(response, "text")
  
  #Parse the raw HTML content into a structured R object
  page <- read_html(page_content) 
  
  #Extract all <a> tags and their corresponding href attributes (links) 
  links <- page %>%
    html_nodes("a")  %>%    #Extract all <a> tags
    html_attr("href")       #Extract href attributes (URLs)
  
  #Extract all the text from the <a> tags
  link_texts <- page %>%
    html_nodes("a")  %>%
    html_text() 
  
  #Now filter for links that contain "Kamprapport"
  kamprapport_links <- links[grepl("Kamprapport", link_texts)]
  
  #Clean up the links (prepend base URL if they are relative links)
  base_url <- "https://tophaandbold.dk"
  full_links <-paste0(base_url, kamprapport_links)
  
  #Clean up any leading/trailing whitespace or newline characters
  cleaned_links <- str_trim(full_links) 
  
  #Print the cleaned links
  print(cleaned_links)
  
  #Optionally, save the links to a CSV file
  write.csv(cleaned_links, "kamprapport_links.csv", row.names = FALSE)
} else {
  #Print error message if the request failed
  stop(paste("Failed to retrieve the page. Status code:", status_code(response)))
}

# Function to extract and view text from a PDF
extract_text_from_pdf <- function(pdf_link) {
  # Download the PDF first
  download.file(pdf_link, destfile = "kamprapport_report.pdf", mode = "wb")
  
  # Extract text from the PDF
  text <- pdf_text("kamprapport_report.pdf")
  
  # Print the first page of text (you can adjust to print more pages if needed)
  cat(text[1])  # Text from the first page
}

# Example: Extract text from the first Kamprapport PDF link
extract_text_from_pdf(cleaned_links[1])  # Extract text from the first report

#Define the URL of the page to be scraped

url <- "https://tophaandbold.dk/kampprogram/herreligaen?year=2&team=11&home_game=0&home_game=1&away_game=0"

#Send a GET request to retrieve the page's HTML content

response <- GET(url)

#Check if the request was successful (status code 200)

if (status_code(response) == 200) { 
  
  #Extract the HTML content as text
  page_content <- content(response, "text")
  
  #Parse the raw HTML content into a structured R object
  page <- read_html(page_content) 
  
  #Extract all <a> tags and their corresponding href attributes (links) 
  links <- page %>%
    html_nodes("a")  %>%    #Extract all <a> tags
    html_attr("href")       #Extract href attributes (URLs)
  
  #Extract all the text from the <a> tags
  link_texts <- page %>%
    html_nodes("a")  %>%
    html_text() 
  
  #Now filter for links that contain "Kamprapport"
  kamprapport_links <- links[grepl("Kamprapport", link_texts)]
  
  #Clean up the links (prepend base URL if they are relative links)
  base_url <- "https://tophaandbold.dk"
  full_links <-paste0(base_url, kamprapport_links)
  
  #Clean up any leading/trailing whitespace or newline characters
  cleaned_links <- str_trim(full_links) 
  
  #Print the cleaned links
  print(cleaned_links)
  
  #Optionally, save the links to a CSV file
  write.csv(cleaned_links, "kamprapport_links.csv", row.names = FALSE)
} else {
  #Print error message if the request failed
  stop(paste("Failed to retrieve the page. Status code:", status_code(response)))
}

# Function to extract and view text from a PDF
extract_text_from_pdf <- function(pdf_link) {
  # Download the PDF first
  download.file(pdf_link, destfile = "kamprapport_report.pdf", mode = "wb")
  
  # Extract text from the PDF
  text <- pdf_text("kamprapport_report.pdf")
  
  # Print the first page of text (you can adjust to print more pages if needed)
  cat(text[1])  # Text from the first page
}

# Example: Extract text from the first Kamprapport PDF link
extract_text_from_pdf(cleaned_links[1])  # Extract text from the first report

#Define the URL of the page to be scraped

url <- "https://tophaandbold.dk/kampprogram/herreligaen?year=1&team=11&home_game=0&home_game=1&away_game=0"

#Send a GET request to retrieve the page's HTML content

response <- GET(url)

#Check if the request was successful (status code 200)

if (status_code(response) == 200) { 
  
  #Extract the HTML content as text
  page_content <- content(response, "text")
  
  #Parse the raw HTML content into a structured R object
  page <- read_html(page_content) 
  
  #Extract all <a> tags and their corresponding href attributes (links) 
  links <- page %>%
    html_nodes("a")  %>%    #Extract all <a> tags
    html_attr("href")       #Extract href attributes (URLs)
  
  #Extract all the text from the <a> tags
  link_texts <- page %>%
    html_nodes("a")  %>%
    html_text() 
  
  #Now filter for links that contain "Kamprapport"
  kamprapport_links <- links[grepl("Kamprapport", link_texts)]
  
  #Clean up the links (prepend base URL if they are relative links)
  base_url <- "https://tophaandbold.dk"
  full_links <-paste0(base_url, kamprapport_links)
  
  #Clean up any leading/trailing whitespace or newline characters
  cleaned_links <- str_trim(full_links) 
  
  #Print the cleaned links
  print(cleaned_links)
  
  #Optionally, save the links to a CSV file
  write.csv(cleaned_links, "kamprapport_links.csv", row.names = FALSE)
} else {
  #Print error message if the request failed
  stop(paste("Failed to retrieve the page. Status code:", status_code(response)))
}

# Function to extract and view text from a PDF
extract_text_from_pdf <- function(pdf_link) {
  # Download the PDF first
  download.file(pdf_link, destfile = "kamprapport_report.pdf", mode = "wb")
  
  # Extract text from the PDF
  text <- pdf_text("kamprapport_report.pdf")
  
  # Print the first page of text (you can adjust to print more pages if needed)
  cat(text[1])  # Text from the first page
}

# Example: Extract text from the first Kamprapport PDF link
extract_text_from_pdf(cleaned_links[1])  # Extract text from the first report

  