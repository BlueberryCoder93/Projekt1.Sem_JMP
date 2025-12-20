# ============================================================
# 0) Packages
# ============================================================
suppressPackageStartupMessages({
  library(dplyr)
  library(lubridate)
  library(stringr)
  library(purrr)
  library(tidyr)
  library(readr)
  library(tibble)
})

# ============================================================
# 1) Load VFF match data (RDS)
# ============================================================
vff_matches <- readRDS(
  "C:/Users/lady_/Documents/vff_hjemmekamp_2021_til_2025_datoer.rds"
)

# ============================================================
# 2) Clean & prepare VFF match datetimes
# ============================================================
vff_matches <- vff_matches %>%
  mutate(
    match_datetime = ymd_hm(
      paste(match_date, tidspunkt),
      tz = "Europe/Copenhagen"
    )
  ) %>%
  filter(!is.na(match_datetime)) %>%
  arrange(match_datetime)

# Optional sanity check
summary(vff_matches$match_datetime)

# ============================================================
# 3) Extract unique VFF match dates (lookup table)
# ============================================================
vff_match_dates <- vff_matches %>%
  mutate(match_date = as.Date(match_datetime)) %>%
  distinct(match_date) %>%
  mutate(vff_played = TRUE)

# ============================================================
# 4) Load / use SAH scraped match data
#     (this assumes sah_kampprogram_raw already exists
#      from your scraping script)
# ============================================================
sah_matches <- sah_kampprogram_raw %>%
  mutate(
    kamp_datetime = dmy_hm(kamp_dato_raw, tz = "Europe/Copenhagen"),
    kamp_dato     = as.Date(kamp_datetime)
  ) %>%
  filter(!is.na(kamp_dato)) %>%
  arrange(kamp_dato)


# ============================================================
# 5) Compare: did VFF play on the same day as SAH?
# ============================================================
sah_vs_vff <- sah_matches %>%
  left_join(
    vff_match_dates,
    by = c("kamp_dato" = "match_date")
  ) %>%
  mutate(
    vff_played_same_day = if_else(
      is.na(vff_played),
      "No",
      "Yes"
    )
  ) %>%
  select(-vff_played) %>%   # cleanup
  arrange(kamp_dato)

# ============================================================
# 6) Result
# ============================================================
View(sah_vs_vff)


