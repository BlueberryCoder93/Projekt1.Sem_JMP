# =============================================================================
# RAW – WEATHER FOR KARUP (06060)
# =============================================================================
# Formål med dette script:
#   - Hente alle DMI-observationer for parameteren “weather” (present weather)
#     for station 06060 (Flyvestation Karup) via metObs-API’et.
#   - Gemme dem som et råt faktalayer i Azure SQL:
#         PBA01_Raw.fact_weather_raw
#   - Sikre, at fuld-load kun køres én gang (dummy-sikring mod dubletter).
#   - Efterfølgende kunne opdatere tabellen inkrementelt (kun nye rækker).
#
# “RAW” betyder her:
#   - Tæt på API-strukturen (ingen tolkning, ingen mapping til koder endnu).
#   - Kun let teknisk formatering (observed_raw → POSIXct, sortering osv.).
#   - Bruges som “sandhedskilde” til clean/transform/join-ready lag senere.
# =============================================================================


# =============================================================================
# SCRIPT 1 – Hent alle weather-data for Karup (06060)
# =============================================================================
# Dette script:
#   - sætter DMI-API’et op (endpoint, station, parameter),
#   - definerer en hjælpefunktion til at hente ét datointerval,
#   - laver én række år-intervaller fra 2000 til i dag,
#   - looper år for år og binder alle resultater sammen til weather_karup.
suppressPackageStartupMessages({
  if (!requireNamespace("pacman", quietly = TRUE)) install.packages("pacman")
  pacman::p_load(httr2, jsonlite, dplyr, tibble, purrr, lubridate)
})

# 1) API-nøgle ---------------------------------------------------------------
# DMI_API_KEY skal ligge i .Renviron.
# Hvis den ikke findes, stopper vi tidligt med en klar fejl.
dmi_api_key <- Sys.getenv("DMI_API_KEY")
if (dmi_api_key == "") {
  stop("DMI_API_KEY er ikke sat – tjek .Renviron og genstart R.")
}

# 2) Opsætning ---------------------------------------------------------------
# Faste parametre for alle kald:
#   - base_url: metObs observation-endpoint.
#   - station_id: 06060 (Karup).
#   - param_id: weather (present weather).
base_url   <- "https://dmigw.govcloud.dk/v2/metObs/collections/observation/items"
station_id <- "06060"      # Flyvestation Karup
param_id   <- "weather"    # present weather

# 3) Funktion til at hente ét datointerval -----------------------------------
# fetch_weather_interval:
#   - bygger et datetime-interval i ISO-format (UTC),
#   - kalder DMI API for det interval,
#   - mapper hver feature til en række i en tibble,
#   - returnerer tom tibble, hvis der ingen features er.
fetch_weather_interval <- function(start_date, end_date) {
  
  start_txt <- paste0(format(start_date, "%Y-%m-%d"), "T00:00:00Z")
  end_txt   <- paste0(format(end_date,   "%Y-%m-%d"), "T23:59:59Z")
  dt_range  <- paste0(start_txt, "/", end_txt)
  
  cat("  Henter interval:", start_txt, "→", end_txt, "\n")
  
  req <- request(base_url) |>
    req_url_query(
      stationId   = station_id,
      parameterId = param_id,
      datetime    = dt_range,
      limit       = 300000   # max pr. kald
    ) |>
    req_headers(
      "X-Gravitee-Api-Key" = dmi_api_key
    )
  
  resp <- req_perform(req)
  x    <- resp_body_json(resp, simplifyVector = FALSE)
  
  if (is.null(x$features) || length(x$features) == 0) {
    cat("    Ingen features i dette interval.\n")
    return(tibble())
  }
  
  out <- map_dfr(
    x$features,
    function(feat) {
      tibble(
        stationId    = feat$properties$stationId,
        parameterId  = feat$properties$parameterId,
        observed_raw = feat$properties$observed,
        value        = feat$properties$value,
        lon          = feat$geometry$coordinates[[1]],
        lat          = feat$geometry$coordinates[[2]]
      )
    }
  )
  
  cat("    Rækker hentet i dette interval:", nrow(out), "\n")
  out
}

# 4) Lav års-intervaller fra 2000-01-01 til i dag ----------------------------
# For at gøre kaldene robuste laver vi ét kald per år:
#   - start_total / end_total definerer hele perioden,
#   - years er listen over år,
#   - intervals indeholder start- og slutdato for hvert år.
start_total <- as.Date("2000-01-01")
end_total   <- Sys.Date()

years <- seq(year(start_total), year(end_total))

intervals <- map(years, function(y) {
  start_y <- as.Date(paste0(y, "-01-01"))
  end_y   <- as.Date(paste0(y, "-12-31"))
  
  tibble(
    start = max(start_y, start_total),
    end   = min(end_y,   end_total)
  )
}) |>
  bind_rows()

cat("Intervaller der hentes (år for år):\n")
print(intervals)

# 5) Hent alle år og bind sammen ---------------------------------------------
# Vi looper over alle års-intervaller med fetch_weather_interval
# og binder dem efterfølgende til ét samlet RAW-datasæt.
cat("🚀 Starter hentning af weather for Karup...\n")

weather_list <- map2(intervals$start, intervals$end, fetch_weather_interval)

weather_karup_raw <- bind_rows(weather_list)

cat("Samlet antal rækker hentet:", nrow(weather_karup_raw), "\n")

# 6) Rens og lav endelig (RAW) tabel -----------------------------------------
# Vi laver kun “teknisk” rensning:
#   - observed_raw → POSIXct (UTC),
#   - vælger kun relevante kolonner,
#   - sorterer kronologisk.
# Det er stadig et RAW-lag – ingen oversættelser til kode-tabeller endnu.
weather_karup <- weather_karup_raw |>
  mutate(
    observed = ymd_hms(observed_raw, tz = "UTC")
  ) |>
  select(
    stationId,
    parameterId,
    observed,
    value,
    lon,
    lat
  ) |>
  arrange(observed)

# Kig på data (udvikling / QA)
cat("Første rækker af weather_karup:\n")
print(head(weather_karup))
View(weather_karup, title = "weather_karup – weather for Karup")

# Valgfrit: gem til senere brug lokalt
# saveRDS(weather_karup, "weather_karup_2000_nu.rds")



# =============================================================================
# SCRIPT A – FØRSTE FULDE LOAD AF WEATHER_KARUP TIL Azure SQL (RAW-LAG)
#            → PBA01_Raw.fact_weather_raw
#            (med dummy-sikring mod dublet-upload)
# =============================================================================
# Dette script:
#   - opretter forbindelse til Azure SQL,
#   - sikrer at PBA01_Raw.fact_weather_raw findes,
#   - tjekker om der allerede er data for 06060/weather,
#   - uploader hele weather_karup første gang (fuld historisk load).
suppressPackageStartupMessages({
  if (!requireNamespace("pacman", quietly = TRUE)) install.packages("pacman")
  pacman::p_load(DBI, odbc, dplyr, lubridate)
})

cat("🔧 Starter fuld load af weather_karup til Azure SQL...\n")

# 1) Læs login til Azure SQL fra .Renviron -----------------------------------
# Loginoplysninger læses fra miljøvariabler, så de ikke hardcodes i scriptet.
server <- Sys.getenv("AZURE_SQL_SERVER")
db     <- Sys.getenv("AZURE_SQL_DB")
uid    <- Sys.getenv("AZURE_SQL_UID")
pwd    <- Sys.getenv("AZURE_SQL_PWD")
schema_raw <- "PBA01_Raw"

if (server == "" || db == "" || uid == "" || pwd == "") {
  stop("AZURE_SQL_* miljøvariable er ikke sat korrekt.")
}

# 2) Opret forbindelse til Azure SQL -----------------------------------------
# Vi åbner en ODBC-forbindelse til Azure. 
# Denne bruges kun til fuld-load-delen.
cat("🌐 Opretter forbindelse til Azure SQL...\n")

con <- dbConnect(
  drv   = odbc::odbc(),
  Driver   = "ODBC Driver 18 for SQL Server",
  Server   = server,
  Database = db,
  UID      = uid,
  PWD      = pwd,
  Encrypt  = "yes",
  TrustServerCertificate = "no",
  Authentication = "SqlPassword",
  Port     = 1433
)

cat("✅ Forbindelse oprettet.\n")

# 3) Forbered data til upload -------------------------------------------------
# Vi sikrer, at weather_karup findes (API-scriptet skal være kørt først).
if (!exists("weather_karup")) {
  dbDisconnect(con)
  stop("Objektet 'weather_karup' findes ikke i R – kør API-scriptet først.")
}

# Vi tilføjer load_timestamp, så vi kan se, hvornår rækkerne blev indlæst.
weather_upload <- weather_karup |>
  mutate(
    load_timestamp = Sys.time()
  )

cat("📦 Klar til upload. Antal rækker i weather_upload:", nrow(weather_upload), "\n")

# 4) Opret tabel i SQL hvis den ikke findes ----------------------------------
# Her arbejder vi nu i RAW-schemaet PBA01_Raw.
# Tabellen oprettes kun, hvis den ikke allerede findes.
cat("🧱 Sikrer at PBA01_Raw.fact_weather_raw findes...\n")

dbExecute(con, sprintf("
IF NOT EXISTS (
  SELECT 1
  FROM INFORMATION_SCHEMA.TABLES
  WHERE TABLE_SCHEMA = '%s'
    AND TABLE_NAME   = 'fact_weather_raw'
)
BEGIN
  CREATE TABLE %s.fact_weather_raw (
    weather_id      bigint IDENTITY(1,1) PRIMARY KEY,
    stationId       varchar(10),
    parameterId     varchar(50),
    observed        datetime2,
    value           float,
    lon             float,
    lat             float,
    load_timestamp  datetime2
  );
END;
", schema_raw, schema_raw))

cat("✅ Tabel", paste0(schema_raw, ".fact_weather_raw"), "klar.\n")

# 4b) DUMMY-SIKRING – tjek om der allerede er data for denne station/parameter
# Dummy-sikringen sikrer, at vi kun fuld-loader én gang for 06060/weather.
antal_eksisterende <- dbGetQuery(con, sprintf("
  SELECT COUNT(1) AS n
  FROM %s.fact_weather_raw
  WHERE stationId   = '06060'
    AND parameterId = 'weather';
", schema_raw))$n[1]

if (antal_eksisterende > 0) {
  cat("\n*** DUMMY-SIKRING AKTIVERET ***\n")
  cat("Tabellen ", schema_raw, ".fact_weather_raw indeholder allerede ",
      antal_eksisterende,
      " rækker for station 06060 / weather.\n", sep = "")
  cat("Fuld-load afbrydes for at undgå dubletter.\n")
  dbDisconnect(con)
  stop("Fuld-load må kun køres på en tom tabel for denne station/parameter. Brug inkrementel script i stedet.")
}

cat("Dummy-sikring: ingen eksisterende data for 06060 / weather – fuld-load fortsætter.\n\n")

# 5) Upload data (første fulde load) -----------------------------------------
# Vi uploader alle rækker fra weather_upload til PBA01_Raw.fact_weather_raw.
# append = TRUE, overwrite = FALSE er sikkert, fordi vi lige har verificeret,
# at der ikke ligger data for denne station/parameter.
cat("⬆️ Uploader weather-data til PBA01_Raw.fact_weather_raw...\n")

dbWriteTable(
  con,
  name      = DBI::Id(schema = schema_raw, table = "fact_weather_raw"),
  value     = weather_upload,
  append    = TRUE,
  overwrite = FALSE
)

cat("✅ Første fulde load færdig. Antal rækker uploadet:", nrow(weather_upload), "\n")

dbDisconnect(con)
cat("🔚 Forbindelse til Azure SQL lukket.\n")



# =============================================================================
# SCRIPT B – INKREMENTEL OPDATERING AF WEATHER-DATA I Azure SQL
#            → PBA01_Raw.fact_weather_raw
# =============================================================================
# Dette script:
#   - finder seneste observed i PBA01_Raw.fact_weather_raw,
#   - bygger et datetime-interval fra (seneste + 1 sekund) til nu,
#   - henter kun de nye observationer fra DMI API,
#   - appender de nye rækker til RAW-tabellen.
suppressPackageStartupMessages({
  if (!requireNamespace("pacman", quietly = TRUE)) install.packages("pacman")
  pacman::p_load(DBI, odbc, dplyr, lubridate, httr2, tibble, purrr)
})

cat("🔄 Starter inkrementel opdatering af weather for Karup (06060)...\n")

# 1) Læs login til Azure SQL --------------------------------------------------
server <- Sys.getenv("AZURE_SQL_SERVER")
db     <- Sys.getenv("AZURE_SQL_DB")
uid    <- Sys.getenv("AZURE_SQL_UID")
pwd    <- Sys.getenv("AZURE_SQL_PWD")
schema_raw <- "PBA01_Raw"

if (server == "" || db == "" || uid == "" || pwd == "") {
  stop("AZURE_SQL_* miljøvariable er ikke sat korrekt.")
}

# 2) Opret forbindelse til Azure SQL -----------------------------------------
cat("🌐 Opretter forbindelse til Azure SQL...\n")

con <- dbConnect(
  drv   = odbc::odbc(),
  Driver   = "ODBC Driver 18 for SQL Server",
  Server   = server,
  Database = db,
  UID      = uid,
  PWD      = pwd,
  Encrypt  = "yes",
  TrustServerCertificate = "no",
  Authentication = "SqlPassword",
  Port     = 1433
)

cat("✅ Forbindelse oprettet.\n")

# 3) Find seneste observed i SQL for Karup weather ---------------------------
# Vi finder den maksimale observed for 06060/weather – det er vores “cut”.
last_obs_df <- dbGetQuery(con, sprintf("
  SELECT MAX(observed) AS last_obs
  FROM %s.fact_weather_raw
  WHERE stationId   = '06060'
    AND parameterId = 'weather';
", schema_raw))

last_obs <- last_obs_df$last_obs[1]

if (is.na(last_obs)) {
  dbDisconnect(con)
  stop("Der er ingen data i PBA01_Raw.fact_weather_raw endnu – kør fuld-load-scriptet først.")
}

cat("📌 Seneste observed i SQL:", as.character(last_obs), "\n")

# 4) Opsæt DMI API -----------------------------------------------------------
dmi_api_key <- Sys.getenv("DMI_API_KEY")
if (dmi_api_key == "") {
  dbDisconnect(con)
  stop("DMI_API_KEY er ikke sat – tjek .Renviron.")
}

base_url   <- "https://dmigw.govcloud.dk/v2/metObs/collections/observation/items"
station_id <- "06060"
param_id   <- "weather"

# 5) Definér tidsinterval for nye data ---------------------------------------
# Start = seneste observed + 1 sekund, slut = nu (UTC),
# så vi undgår dubletter men ikke misser målinger.
start_time <- with_tz(last_obs, "UTC") + seconds(1)
end_time   <- with_tz(Sys.time(), "UTC")

start_txt <- format(start_time, "%Y-%m-%dT%H:%M:%SZ")
end_txt   <- format(end_time,   "%Y-%m-%dT%H:%M:%SZ")

dt_range  <- paste0(start_txt, "/", end_txt)

cat("🕒 Henter nye data i interval:", dt_range, "\n")

# 6) Kald DMI API for nye observationer --------------------------------------
req <- request(base_url) |>
  req_url_query(
    stationId   = station_id,
    parameterId = param_id,
    datetime    = dt_range,
    limit       = 300000
  ) |>
  req_headers(
    "X-Gravitee-Api-Key" = dmi_api_key
  )

resp <- req_perform(req)
x    <- resp_body_json(resp, simplifyVector = FALSE)

if (is.null(x$features) || length(x$features) == 0) {
  cat("ℹ️ Ingen nye observationer fundet i DMI API.\n")
  dbDisconnect(con)
  cat("🔚 Forbindelse lukket.\n")
} else {
  
  # Map nye observationer til tibble i samme struktur som RAW-tabellen
  new_weather <- map_dfr(
    x$features,
    function(feat) {
      tibble(
        stationId    = feat$properties$stationId,
        parameterId  = feat$properties$parameterId,
        observed     = ymd_hms(feat$properties$observed, tz = "UTC"),
        value        = feat$properties$value,
        lon          = feat$geometry$coordinates[[1]],
        lat          = feat$geometry$coordinates[[2]]
      )
    }
  ) |>
    arrange(observed) |>
    mutate(load_timestamp = Sys.time())
  
  cat("📦 Antal nye rækker hentet:", nrow(new_weather), "\n")
  
  if (nrow(new_weather) > 0) {
    # Append nye rækker til RAW-tabellen i PBA01_Raw
    dbWriteTable(
      con,
      name      = DBI::Id(schema = schema_raw, table = "fact_weather_raw"),
      value     = new_weather,
      append    = TRUE,
      overwrite = FALSE
    )
    cat("✅ Nye rækker er nu indsat i", paste0(schema_raw, ".fact_weather_raw"), "\n")
  } else {
    cat("ℹ️ Der var ingen nye rækker at uploade.\n")
  }
  
  dbDisconnect(con)
  cat("🔚 Forbindelse lukket.\n")
}

cat("✅ Inkrementel opdatering af weather færdig.\n")

