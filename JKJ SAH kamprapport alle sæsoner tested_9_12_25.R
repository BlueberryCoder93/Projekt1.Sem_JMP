library(rvest)
library(httr)
library(stringr)
library(pdftools)
library(dplyr)
library(purrr)
library(tibble)


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
  cat(text[1])  
}

# Example: Extract text from the first Kamprapport PDF link
extract_text_from_pdf(cleaned_links)

dir.create("pdfs", showWarnings = FALSE)

walk2(cleaned_links, paste0("pdfs/report_", seq_along(cleaned_links), ".pdf"),
      ~download.file(.x, .y, mode = "wb"))

pdf_files <- list.files("pdfs", full.names = TRUE)
pdf_text_list <- map(pdf_files, pdf_text)

extract_info <- function(lines) {
  
  #KampNr + Hjemmehold + Udehold line
  header_idx <- which(str_detect(lines, "^[0-9]{6,}"))
  header_line <- lines[header_idx][1]
  
  header_parts <- str_split(header_line, "\\s{2,}", simplify = TRUE)
  
  kampnr <- header_parts[1]
  hjemmehold <- header_parts[2]
  udehold <- header_parts[3]
  
  # Spillested + Dato + Tid + Tilskuertal
  # Find line that starts with "Spillested"
  spillested_idx <- which(str_detect(lines, "^Spillested"))
  
  # The next line contains all needed values
  info_line <- lines[spillested_idx + 1]
  
  # Extract date, time, spectators
  dato <- str_extract(info_line, "\\d{2}-\\d{2}-\\d{4}")
  tid <- str_extract(info_line, "\\d{2}:\\d{2}")
  
  
  #Tilskuertal = number immediately *after* the time
  #robust extraction using the first numbeer following the time
  tilskuertal <- str_match(info_line, paste0(tid, "\\s+([0-9]{2,5})"))[,2]   # captures 980, 2500, etc.
  
  # Spillested = everything before the date
  spillested <- str_trim(str_sub(info_line, 1, str_locate(info_line, dato)[1] - 1))
  
  # --- 3. Extract Slutresultat (e.g., "33-23") ---
  # It always appears after the Kapacitet line:
  # "Kapacitet: 2000     33-23"
  slut_idx <- which(str_detect(lines, "^Kapacitet:"))
  slut_line <- lines[slut_idx + 1]
  
  slutresultat <- str_extract(slut_line, "\\b\\d{1,2}-\\d{1,2}\\b")
  
  tibble(
    KampNr = kampnr,
    Hjemmehold = hjemmehold,
    Udehold = udehold,
    Spillested = spillested,
    Dato = dato,
    Tid = tid,
    Tilskuertal = tilskuertal,
    Slutresultat = slutresultat
  )
}

results <- map(pdf_text_list, function(pages) {
  
  lines <- pages %>%
    str_split("\n") %>%
    unlist() %>%
    str_trim() %>%
    discard(~ .x == "")
  
  extract_info(lines) 
}) 

matches <- bind_rows(results)

View(matches)

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
  cat(text[1])  
}

# Example: Extract text from the first Kamprapport PDF link
extract_text_from_pdf(cleaned_links)

dir.create("pdfs", showWarnings = FALSE)

walk2(cleaned_links, paste0("pdfs/report_", seq_along(cleaned_links), ".pdf"),
      ~download.file(.x, .y, mode = "wb"))

pdf_files <- list.files("pdfs", full.names = TRUE)
pdf_text_list <- map(pdf_files, pdf_text)

extract_info <- function(lines) {
  
  #KampNr + Hjemmehold + Udehold line
  header_idx <- which(str_detect(lines, "^[0-9]{6,}"))
  header_line <- lines[header_idx][1]
  
  header_parts <- str_split(header_line, "\\s{2,}", simplify = TRUE)
  
  kampnr <- header_parts[1]
  hjemmehold <- header_parts[2]
  udehold <- header_parts[3]
  
  # Spillested + Dato + Tid + Tilskuertal
  # Find line that starts with "Spillested"
  spillested_idx <- which(str_detect(lines, "^Spillested"))
  
  # The next line contains all needed values
  info_line <- lines[spillested_idx + 1]
  
  # Extract date, time, spectators
  dato <- str_extract(info_line, "\\d{2}-\\d{2}-\\d{4}")
  tid <- str_extract(info_line, "\\d{2}:\\d{2}")
  
  
  #Tilskuertal = number immediately *after* the time
  #robust extraction using the first numbeer following the time
  tilskuertal <- str_match(info_line, paste0(tid, "\\s+([0-9]{2,5})"))[,2]   # captures 980, 2500, etc.
  
  # Spillested = everything before the date
  spillested <- str_trim(str_sub(info_line, 1, str_locate(info_line, dato)[1] - 1))
  
  # --- 3. Extract Slutresultat (e.g., "33-23") ---
  # It always appears after the Kapacitet line:
  # "Kapacitet: 2000     33-23"
  slut_idx <- which(str_detect(lines, "^Kapacitet:"))
  slut_line <- lines[slut_idx + 1]
  
  slutresultat <- str_extract(slut_line, "\\b\\d{1,2}-\\d{1,2}\\b")
  
  tibble(
    KampNr = kampnr,
    Hjemmehold = hjemmehold,
    Udehold = udehold,
    Spillested = spillested,
    Dato = dato,
    Tid = tid,
    Tilskuertal = tilskuertal,
    Slutresultat = slutresultat
  )
}

results <- map(pdf_text_list, function(pages) {
  
  lines <- pages %>%
    str_split("\n") %>%
    unlist() %>%
    str_trim() %>%
    discard(~ .x == "")
  
  extract_info(lines) 
}) 

matches <- bind_rows(results)

View(matches)
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
  cat(text[1])  
}

# Example: Extract text from the first Kamprapport PDF link
extract_text_from_pdf(cleaned_links)

dir.create("pdfs", showWarnings = FALSE)

walk2(cleaned_links, paste0("pdfs/report_", seq_along(cleaned_links), ".pdf"),
      ~download.file(.x, .y, mode = "wb"))

pdf_files <- list.files("pdfs", full.names = TRUE)
pdf_text_list <- map(pdf_files, pdf_text)

extract_info <- function(lines) {
  
  #KampNr + Hjemmehold + Udehold line
  header_idx <- which(str_detect(lines, "^[0-9]{6,}"))
  header_line <- lines[header_idx][1]
  
  header_parts <- str_split(header_line, "\\s{2,}", simplify = TRUE)
  
  kampnr <- header_parts[1]
  hjemmehold <- header_parts[2]
  udehold <- header_parts[3]
  
  # Spillested + Dato + Tid + Tilskuertal
  # Find line that starts with "Spillested"
  spillested_idx <- which(str_detect(lines, "^Spillested"))
  
  # The next line contains all needed values
  info_line <- lines[spillested_idx + 1]
  
  # Extract date, time, spectators
  dato <- str_extract(info_line, "\\d{2}-\\d{2}-\\d{4}")
  tid <- str_extract(info_line, "\\d{2}:\\d{2}")
  
  
  #Tilskuertal = number immediately *after* the time
  #robust extraction using the first numbeer following the time
  tilskuertal <- str_match(info_line, paste0(tid, "\\s+([0-9]{2,5})"))[,2]   # captures 980, 2500, etc.
  
  # Spillested = everything before the date
  spillested <- str_trim(str_sub(info_line, 1, str_locate(info_line, dato)[1] - 1))
  
  # --- 3. Extract Slutresultat (e.g., "33-23") ---
  # It always appears after the Kapacitet line:
  # "Kapacitet: 2000     33-23"
  slut_idx <- which(str_detect(lines, "^Kapacitet:"))
  slut_line <- lines[slut_idx + 1]
  
  slutresultat <- str_extract(slut_line, "\\b\\d{1,2}-\\d{1,2}\\b")
  
  tibble(
    KampNr = kampnr,
    Hjemmehold = hjemmehold,
    Udehold = udehold,
    Spillested = spillested,
    Dato = dato,
    Tid = tid,
    Tilskuertal = tilskuertal,
    Slutresultat = slutresultat
  )
}

results <- map(pdf_text_list, function(pages) {
  
  lines <- pages %>%
    str_split("\n") %>%
    unlist() %>%
    str_trim() %>%
    discard(~ .x == "")
  
  extract_info(lines) 
}) 

matches <- bind_rows(results)

View(matches)

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
  cat(text[1])  
}

# Example: Extract text from the first Kamprapport PDF link
extract_text_from_pdf(cleaned_links)

dir.create("pdfs", showWarnings = FALSE)

walk2(cleaned_links, paste0("pdfs/report_", seq_along(cleaned_links), ".pdf"),
      ~download.file(.x, .y, mode = "wb"))

pdf_files <- list.files("pdfs", full.names = TRUE)
pdf_text_list <- map(pdf_files, pdf_text)

extract_info <- function(lines) {
  
  #KampNr + Hjemmehold + Udehold line
  header_idx <- which(str_detect(lines, "^[0-9]{6,}"))
  header_line <- lines[header_idx][1]
  
  header_parts <- str_split(header_line, "\\s{2,}", simplify = TRUE)
  
  kampnr <- header_parts[1]
  hjemmehold <- header_parts[2]
  udehold <- header_parts[3]
  
  # Spillested + Dato + Tid + Tilskuertal
  # Find line that starts with "Spillested"
  spillested_idx <- which(str_detect(lines, "^Spillested"))
  
  # The next line contains all needed values
  info_line <- lines[spillested_idx + 1]
  
  # Extract date, time, spectators
  dato <- str_extract(info_line, "\\d{2}-\\d{2}-\\d{4}")
  tid <- str_extract(info_line, "\\d{2}:\\d{2}")
  
  
  #Tilskuertal = number immediately *after* the time
  #robust extraction using the first numbeer following the time
  tilskuertal <- str_match(info_line, paste0(tid, "\\s+([0-9]{2,5})"))[,2]   # captures 980, 2500, etc.
  
  # Spillested = everything before the date
  spillested <- str_trim(str_sub(info_line, 1, str_locate(info_line, dato)[1] - 1))
  
  # --- 3. Extract Slutresultat (e.g., "33-23") ---
  # It always appears after the Kapacitet line:
  # "Kapacitet: 2000     33-23"
  slut_idx <- which(str_detect(lines, "^Kapacitet:"))
  slut_line <- lines[slut_idx + 1]
  
  slutresultat <- str_extract(slut_line, "\\b\\d{1,2}-\\d{1,2}\\b")
  
  tibble(
    KampNr = kampnr,
    Hjemmehold = hjemmehold,
    Udehold = udehold,
    Spillested = spillested,
    Dato = dato,
    Tid = tid,
    Tilskuertal = tilskuertal,
    Slutresultat = slutresultat
  )
}

results <- map(pdf_text_list, function(pages) {
  
  lines <- pages %>%
    str_split("\n") %>%
    unlist() %>%
    str_trim() %>%
    discard(~ .x == "")
  
  extract_info(lines) 
}) 

matches <- bind_rows(results)

View(matches)

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
  cat(text[1])  
}

# Example: Extract text from the first Kamprapport PDF link
extract_text_from_pdf(cleaned_links)

dir.create("pdfs", showWarnings = FALSE)

walk2(cleaned_links, paste0("pdfs/report_", seq_along(cleaned_links), ".pdf"),
      ~download.file(.x, .y, mode = "wb"))

pdf_files <- list.files("pdfs", full.names = TRUE)
pdf_text_list <- map(pdf_files, pdf_text)

extract_info <- function(lines) {
  
  #KampNr + Hjemmehold + Udehold line
  header_idx <- which(str_detect(lines, "^[0-9]{6,}"))
  header_line <- lines[header_idx][1]
  
  header_parts <- str_split(header_line, "\\s{2,}", simplify = TRUE)
  
  kampnr <- header_parts[1]
  hjemmehold <- header_parts[2]
  udehold <- header_parts[3]
  
  # Spillested + Dato + Tid + Tilskuertal
  # Find line that starts with "Spillested"
  spillested_idx <- which(str_detect(lines, "^Spillested"))
  
  # The next line contains all needed values
  info_line <- lines[spillested_idx + 1]
  
  # Extract date, time, spectators
  dato <- str_extract(info_line, "\\d{2}-\\d{2}-\\d{4}")
  tid <- str_extract(info_line, "\\d{2}:\\d{2}")
  
  
  #Tilskuertal = number immediately *after* the time
  #robust extraction using the first numbeer following the time
  tilskuertal <- str_match(info_line, paste0(tid, "\\s+([0-9]{2,5})"))[,2]   # captures 980, 2500, etc.
  
  # Spillested = everything before the date
  spillested <- str_trim(str_sub(info_line, 1, str_locate(info_line, dato)[1] - 1))
  
  # --- 3. Extract Slutresultat (e.g., "33-23") ---
  # It always appears after the Kapacitet line:
  # "Kapacitet: 2000     33-23"
  slut_idx <- which(str_detect(lines, "^Kapacitet:"))
  slut_line <- lines[slut_idx + 1]
  
  slutresultat <- str_extract(slut_line, "\\b\\d{1,2}-\\d{1,2}\\b")
  
  tibble(
    KampNr = kampnr,
    Hjemmehold = hjemmehold,
    Udehold = udehold,
    Spillested = spillested,
    Dato = dato,
    Tid = tid,
    Tilskuertal = tilskuertal,
    Slutresultat = slutresultat
  )
}

results <- map(pdf_text_list, function(pages) {
  
  lines <- pages %>%
    str_split("\n") %>%
    unlist() %>%
    str_trim() %>%
    discard(~ .x == "")
  
  extract_info(lines) 
}) 

matches <- bind_rows(results)

View(matches)

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
  cat(text[1])  
}

# Example: Extract text from the first Kamprapport PDF link
extract_text_from_pdf(cleaned_links)

dir.create("pdfs", showWarnings = FALSE)

walk2(cleaned_links, paste0("pdfs/report_", seq_along(cleaned_links), ".pdf"),
      ~download.file(.x, .y, mode = "wb"))

pdf_files <- list.files("pdfs", full.names = TRUE)
pdf_text_list <- map(pdf_files, pdf_text)

extract_info <- function(lines) {
  
  #KampNr + Hjemmehold + Udehold line
  header_idx <- which(str_detect(lines, "^[0-9]{6,}"))
  header_line <- lines[header_idx][1]
  
  header_parts <- str_split(header_line, "\\s{2,}", simplify = TRUE)
  
  kampnr <- header_parts[1]
  hjemmehold <- header_parts[2]
  udehold <- header_parts[3]
  
  # Spillested + Dato + Tid + Tilskuertal
  # Find line that starts with "Spillested"
  spillested_idx <- which(str_detect(lines, "^Spillested"))
  
  # The next line contains all needed values
  info_line <- lines[spillested_idx + 1]
  
  # Extract date, time, spectators
  dato <- str_extract(info_line, "\\d{2}-\\d{2}-\\d{4}")
  tid <- str_extract(info_line, "\\d{2}:\\d{2}")
  
  
  #Tilskuertal = number immediately *after* the time
  #robust extraction using the first numbeer following the time
  tilskuertal <- str_match(info_line, paste0(tid, "\\s+([0-9]{2,5})"))[,2]   # captures 980, 2500, etc.
  
  # Spillested = everything before the date
  spillested <- str_trim(str_sub(info_line, 1, str_locate(info_line, dato)[1] - 1))
  
  # --- 3. Extract Slutresultat (e.g., "33-23") ---
  # It always appears after the Kapacitet line:
  # "Kapacitet: 2000     33-23"
  slut_idx <- which(str_detect(lines, "^Kapacitet:"))
  slut_line <- lines[slut_idx + 1]
  
  slutresultat <- str_extract(slut_line, "\\b\\d{1,2}-\\d{1,2}\\b")
  
  tibble(
    KampNr = kampnr,
    Hjemmehold = hjemmehold,
    Udehold = udehold,
    Spillested = spillested,
    Dato = dato,
    Tid = tid,
    Tilskuertal = tilskuertal,
    Slutresultat = slutresultat
  )
}

results <- map(pdf_text_list, function(pages) {
  
  lines <- pages %>%
    str_split("\n") %>%
    unlist() %>%
    str_trim() %>%
    discard(~ .x == "")
  
  extract_info(lines) 
}) 

matches <- bind_rows(results)

View(matches)

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
  cat(text[1])  
}

# Example: Extract text from the first Kamprapport PDF link
extract_text_from_pdf(cleaned_links)

dir.create("pdfs", showWarnings = FALSE)

walk2(cleaned_links, paste0("pdfs/report_", seq_along(cleaned_links), ".pdf"),
      ~download.file(.x, .y, mode = "wb"))

pdf_files <- list.files("pdfs", full.names = TRUE)
pdf_text_list <- map(pdf_files, pdf_text)

extract_info <- function(lines) {
  
  #KampNr + Hjemmehold + Udehold line
  header_idx <- which(str_detect(lines, "^[0-9]{6,}"))
  header_line <- lines[header_idx][1]
  
  header_parts <- str_split(header_line, "\\s{2,}", simplify = TRUE)
  
  kampnr <- header_parts[1]
  hjemmehold <- header_parts[2]
  udehold <- header_parts[3]
  
  # Spillested + Dato + Tid + Tilskuertal
  # Find line that starts with "Spillested"
  spillested_idx <- which(str_detect(lines, "^Spillested"))
  
  # The next line contains all needed values
  info_line <- lines[spillested_idx + 1]
  
  # Extract date, time, spectators
  dato <- str_extract(info_line, "\\d{2}-\\d{2}-\\d{4}")
  tid <- str_extract(info_line, "\\d{2}:\\d{2}")
  
  
  #Tilskuertal = number immediately *after* the time
  #robust extraction using the first numbeer following the time
  tilskuertal <- str_match(info_line, paste0(tid, "\\s+([0-9]{2,5})"))[,2]   # captures 980, 2500, etc.
  
  # Spillested = everything before the date
  spillested <- str_trim(str_sub(info_line, 1, str_locate(info_line, dato)[1] - 1))
  
  # --- 3. Extract Slutresultat (e.g., "33-23") ---
  # It always appears after the Kapacitet line:
  # "Kapacitet: 2000     33-23"
  slut_idx <- which(str_detect(lines, "^Kapacitet:"))
  slut_line <- lines[slut_idx + 1]
  
  slutresultat <- str_extract(slut_line, "\\b\\d{1,2}-\\d{1,2}\\b")
  
  tibble(
    KampNr = kampnr,
    Hjemmehold = hjemmehold,
    Udehold = udehold,
    Spillested = spillested,
    Dato = dato,
    Tid = tid,
    Tilskuertal = tilskuertal,
    Slutresultat = slutresultat
  )
}

results <- map(pdf_text_list, function(pages) {
  
  lines <- pages %>%
    str_split("\n") %>%
    unlist() %>%
    str_trim() %>%
    discard(~ .x == "")
  
  extract_info(lines) 
}) 

matches <- bind_rows(results)

View(matches)

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
  cat(text[1])  
}

# Example: Extract text from the first Kamprapport PDF link
extract_text_from_pdf(cleaned_links)

dir.create("pdfs", showWarnings = FALSE)

walk2(cleaned_links, paste0("pdfs/report_", seq_along(cleaned_links), ".pdf"),
      ~download.file(.x, .y, mode = "wb"))

pdf_files <- list.files("pdfs", full.names = TRUE)
pdf_text_list <- map(pdf_files, pdf_text)

extract_info <- function(lines) {
  
  #KampNr + Hjemmehold + Udehold line
  header_idx <- which(str_detect(lines, "^[0-9]{6,}"))
  header_line <- lines[header_idx][1]
  
  header_parts <- str_split(header_line, "\\s{2,}", simplify = TRUE)
  
  kampnr <- header_parts[1]
  hjemmehold <- header_parts[2]
  udehold <- header_parts[3]
  
  # Spillested + Dato + Tid + Tilskuertal
  # Find line that starts with "Spillested"
  spillested_idx <- which(str_detect(lines, "^Spillested"))
  
  # The next line contains all needed values
  info_line <- lines[spillested_idx + 1]
  
  # Extract date, time, spectators
  dato <- str_extract(info_line, "\\d{2}-\\d{2}-\\d{4}")
  tid <- str_extract(info_line, "\\d{2}:\\d{2}")
  
  
  #Tilskuertal = number immediately *after* the time
  #robust extraction using the first numbeer following the time
  tilskuertal <- str_match(info_line, paste0(tid, "\\s+([0-9]{2,5})"))[,2]   # captures 980, 2500, etc.
  
  # Spillested = everything before the date
  spillested <- str_trim(str_sub(info_line, 1, str_locate(info_line, dato)[1] - 1))
  
  # --- 3. Extract Slutresultat (e.g., "33-23") ---
  # It always appears after the Kapacitet line:
  # "Kapacitet: 2000     33-23"
  slut_idx <- which(str_detect(lines, "^Kapacitet:"))
  slut_line <- lines[slut_idx + 1]
  
  slutresultat <- str_extract(slut_line, "\\b\\d{1,2}-\\d{1,2}\\b")
  
  tibble(
    KampNr = kampnr,
    Hjemmehold = hjemmehold,
    Udehold = udehold,
    Spillested = spillested,
    Dato = dato,
    Tid = tid,
    Tilskuertal = tilskuertal,
    Slutresultat = slutresultat
  )
}

results <- map(pdf_text_list, function(pages) {
  
  lines <- pages %>%
    str_split("\n") %>%
    unlist() %>%
    str_trim() %>%
    discard(~ .x == "")
  
  extract_info(lines) 
}) 

matches <- bind_rows(results)

View(matches)

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
  cat(text[1])  
}

# Example: Extract text from the first Kamprapport PDF link
extract_text_from_pdf(cleaned_links)

dir.create("pdfs", showWarnings = FALSE)

walk2(cleaned_links, paste0("pdfs/report_", seq_along(cleaned_links), ".pdf"),
      ~download.file(.x, .y, mode = "wb"))

pdf_files <- list.files("pdfs", full.names = TRUE)
pdf_text_list <- map(pdf_files, pdf_text)

extract_info <- function(lines) {
  
  #KampNr + Hjemmehold + Udehold line
  header_idx <- which(str_detect(lines, "^[0-9]{6,}"))
  header_line <- lines[header_idx][1]
  
  header_parts <- str_split(header_line, "\\s{2,}", simplify = TRUE)
  
  kampnr <- header_parts[1]
  hjemmehold <- header_parts[2]
  udehold <- header_parts[3]
  
  # Spillested + Dato + Tid + Tilskuertal
  # Find line that starts with "Spillested"
  spillested_idx <- which(str_detect(lines, "^Spillested"))
  
  # The next line contains all needed values
  info_line <- lines[spillested_idx + 1]
  
  # Extract date, time, spectators
  dato <- str_extract(info_line, "\\d{2}-\\d{2}-\\d{4}")
  tid <- str_extract(info_line, "\\d{2}:\\d{2}")
  
  
  #Tilskuertal = number immediately *after* the time
  #robust extraction using the first numbeer following the time
  tilskuertal <- str_match(info_line, paste0(tid, "\\s+([0-9]{2,5})"))[,2]   # captures 980, 2500, etc.
  
  # Spillested = everything before the date
  spillested <- str_trim(str_sub(info_line, 1, str_locate(info_line, dato)[1] - 1))
  
  # --- 3. Extract Slutresultat (e.g., "33-23") ---
  # It always appears after the Kapacitet line:
  # "Kapacitet: 2000     33-23"
  slut_idx <- which(str_detect(lines, "^Kapacitet:"))
  slut_line <- lines[slut_idx + 1]
  
  slutresultat <- str_extract(slut_line, "\\b\\d{1,2}-\\d{1,2}\\b")
  
  tibble(
    KampNr = kampnr,
    Hjemmehold = hjemmehold,
    Udehold = udehold,
    Spillested = spillested,
    Dato = dato,
    Tid = tid,
    Tilskuertal = tilskuertal,
    Slutresultat = slutresultat
  )
}

results <- map(pdf_text_list, function(pages) {
  
  lines <- pages %>%
    str_split("\n") %>%
    unlist() %>%
    str_trim() %>%
    discard(~ .x == "")
  
  extract_info(lines) 
}) 

matches <- bind_rows(results)

View(matches)
