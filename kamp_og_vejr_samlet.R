library(dplyr)
library(lubridate)
library(stringr)
library(purrr)
library(tidyr)
library(readr)
library(tibble)
library(progress)

matches <- readRDS(
  "C:/Users/lady_/Documents/vff_hjemmekamp_2021_til_2025_datoer.rds"
)

matches <- matches %>%
  mutate(
    match_datetime = ymd_hm(
      paste(match_date, tidspunkt),
      tz = "Europe/Copenhagen"
    )
  ) %>%
  filter(!is.na(match_datetime)) %>%
  arrange(match_datetime)

summary(matches$match_datetime)

match_windows <- matches %>%
  mutate(match_id = row_number()) %>%
  transmute(
    match_id,
    match_datetime,
    window_start = match_datetime - hours(1),
    window_end   = match_datetime + hours(1)
  )

match_dates <- matches %>%
  filter(match_date >= as.Date("2021-07-01")) %>%
  filter(!is.na(match_datetime)) %>%
  mutate(date = as.Date(match_datetime)) %>%
  pull(match_date) %>%
  unique()

head(match_dates)
length(match_dates)

bulk_files_needed <- bulk_files[
  basename(bulk_files) %in% paste0(match_dates, ".txt")
]

length(bulk_files_needed)
head(bulk_files_needed)


bulk_root <- "C:/Users/lady_/Documents/dmi_bulk_data"

bulk_files <- list.files(
  bulk_root,
  pattern = "\\.txt$",
  recursive = TRUE,
  full.names = TRUE
)

stopifnot(length(bulk_files) > 0)

parse_bulk_file_fast <- function(file, match_windows) {
  
  raw <- read_lines(file, progress = FALSE)
  
  # Only keep JSON feature lines
  raw <- raw[str_detect(raw, '"parameterId"')]
  
  if (length(raw) == 0) return(NULL)
  
  df <- tibble(json = raw) %>%
    mutate(parsed = map(json, jsonlite::fromJSON)) %>%
    transmute(
      stationId   = map_chr(parsed, ~ .x$properties$stationId),
      parameterId = map_chr(parsed, ~ .x$properties$parameterId),
      value       = map_dbl(parsed, ~ as.numeric(.x$properties$value)),
      datetime    = ymd_hms(
        map_chr(parsed, ~ .x$properties$observed),
        tz = "UTC"
      )
    ) %>%
    mutate(datetime = with_tz(datetime, "Europe/Copenhagen"))
  
  # Filter to match windows
  df <- df %>%
    inner_join(
      match_windows,
      join_by(between(datetime, window_start, window_end))
    )
  
  if (nrow(df) == 0) return(NULL)
  
  # Keep only needed weather variables
  df %>%
    filter(parameterId %in% c(
      "temp_dry",
      "precip_past1h",
      "cloud_cover",
      "wind_dir"
    )) %>%
    group_by(match_id, parameterId) %>%
    summarise(value = mean(value, na.rm = TRUE), .groups = "drop") %>%
    pivot_wider(
      names_from = parameterId,
      values_from = value
    )
}

matches_weather <- matches_weather |>
  mutate(
    precip_past1h = if_else(
      precip_past1h < 0,
      NA_real_,
      precip_past1h
    )
  )


pb <- progress_bar$new(
  total = length(bulk_files_needed),
  format = "Reading bulk files [:bar] :current/:total (:percent) ETA: :eta",
  clear = FALSE,
  width = 60
)

weather_list <- purrr::map(
  bulk_files_needed,
  function(file) {
    pb$tick()
    parse_bulk_file_fast(file, match_windows)
  }
)

weather_all <- bind_rows(weather_list)

matches_weather <- matches %>%
  mutate(match_id = row_number()) %>%
  left_join(weather_all, by = "match_id") %>%
  arrange(match_datetime)

print(matches_weather)

saveRDS(
  matches_weather,
  "vff_matches_weather_final.rds"
)

vff_matches_weather_final <- matches_weather
View(vff_matches_weather_final)
