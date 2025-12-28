# ============================================================
# 0) Packages
# ============================================================
library(dplyr)
library(lubridate)

# ============================================================
# 1) File paths
# ============================================================
paths <- list(
  matches   = "C:/Users/lady_/Documents/",
  tilskuere = "C:/Users/lady_/Documents/",
  placering = "C:/Users/lady_/Documents/",
  calendar  = "C:/Users/lady_/Documents/",
  weather   = "C:/Users/lady_/Documents/",
  sah       = "C:/Users/lady_/Documents/",
  output    = "C:/Users/lady_/Documents/"
)


# ============================================================
# 2) Load data
# ============================================================
vff_matches <- readRDS(
  paste0(paths$matches, "vff_hjemmekamp_2021_til_2025_datoer_with_season.rds")
)

vff_tilskuere <- readRDS(
  paste0(paths$tilskuere, "vffkort01.rds")
)

vff_placering <- readRDS(
  paste0(paths$placering, "vff_hjemmekamp_2021_til_2025_placering.rds")
)

vff_calendar <- readRDS(
  paste0(paths$calendar, "vff_hjemmekamp_2021_til_2025_helligdag.rds")
)

vff_schoolvac <- readRDS(
  paste0(paths$calendar, "vff_hjemmekamp_2021_til_2025_skoleferie.rds")
)

vff_weather <- readRDS(
  paste0(paths$weather, "vff_matches_weather_final.rds")
)

sah_vs_vff <- readRDS(
  paste0(paths$sah, "vff_hjemmekamp_2021_til_2025_sah.rds")
)

# ============================================================
# 3) Standardise base match table
# ============================================================
vff_matches <- vff_matches %>%
  mutate(
    runde = as.integer(runde),
    match_datetime = ymd_hm(
      paste(match_date, tidspunkt),
      tz = "Europe/Copenhagen"
    ),
    match_date = as.Date(match_datetime)
  )

vff_matches <- vff_matches %>%
  mutate(
    weekday_da = case_when(
      wday(match_date) == 1 ~ "Søndag",
      wday(match_date) == 2 ~ "Mandag",
      wday(match_date) == 3 ~ "Tirsdag",
      wday(match_date) == 4 ~ "Onsdag",
      wday(match_date) == 5 ~ "Torsdag",
      wday(match_date) == 6 ~ "Fredag",
      wday(match_date) == 7 ~ "Lørdag"
    )
  )

# ============================================================
# 4) Standardise other tables
# ============================================================
vff_tilskuere <- vff_tilskuere %>%
  mutate(runde = as.integer(runde))

vff_placering <- vff_placering %>%
  mutate(
    runde = as.integer(runde),
    placering = as.integer(placering)
  )

vff_calendar <- vff_calendar %>%
  transmute(
    match_date,
    helligdag = holiday
  )

vff_schoolvac <- vff_schoolvac %>%
  transmute(
    match_date,
    skoleferie = school_holiday_yes_no
  )

vff_weather <- vff_weather %>%
  select(
    match_date,
    temp_dry,
    precip_past1h,
    wind_dir,
    cloud_cover
  )

sah_vs_vff <- sah_vs_vff %>%
  transmute(
    match_date = kamp_dato,
    sah_played = vff_played_same_day
  )

# ============================================================
# 5) Build MASTER TABLE
# ============================================================
vff_master <- vff_matches %>%
  left_join(
    vff_tilskuere %>% select(sæson, runde, tilskuere),
    by = c("sæson", "runde")
  ) %>%
  left_join(
    vff_placering,
    by = c("match_date", "runde")
  ) %>%
  left_join(
    vff_calendar,
    by = "match_date"
  ) %>%
  left_join(
    vff_schoolvac,
    by = "match_date"
  ) %>%
  left_join(
    vff_weather,
    by = "match_date"
  ) %>%
  left_join(
    sah_vs_vff,
    by = "match_date"
  ) %>%
  arrange(match_datetime)

# ============================================================
# 6) Fill missing values
# ============================================================
vff_master <- vff_master %>%
  mutate(
    tilskuere       = coalesce(tilskuere, 0L),
    placering       = coalesce(placering, 0L),
    temp_dry        = coalesce(temp_dry, 0),
    precip_past1h   = coalesce(precip_past1h, 0),
    cloud_cover     = coalesce(cloud_cover, 0),
    wind_dir        = coalesce(wind_dir, 0),
    helligdag       = coalesce(helligdag, "No"),
    skoleferie      = coalesce(skoleferie, "No"),
    sah_played      = coalesce(sah_played, "No")
  )

vff_master <- vff_master %>%
  filter(match_datetime > ymd_hm("2021-03-03 14:00", tz = "Europe/Copenhagen"))

# ============================================================
# 7) Final column order
# ============================================================
vff_master <- vff_master %>%
  select(
    match_datetime,
    sæson,
    runde,
    tilskuere,
    placering,
    weekday_da,
    helligdag,
    skoleferie,
    temp_dry,
    precip_past1h,
    wind_dir,
    cloud_cover,
    sah_played
  )

# ============================================================
# 8) Save
# ============================================================
saveRDS(
  vff_master,
  paste0(paths$output, "vff_master_2021_2025.rds")
)

View(vff_master)
