# =============================================================================
# RAW – WIND_SPEED FOR KARUP (06060)
# =============================================================================
# Formål med dette script:
#   - Hente alle DMI-observationer for parameteren “wind_speed” (middelvind, m/s)
#     for station 06060 (Flyvestation Karup) via metObs-API’et.
#   - Gemme dem som et råt faktalayer i Azure SQL:
#         PBA01_Raw.fact_wind_speed_raw
#   - Sikre, at fuld-load kun køres én gang (dummy-sikring mod dubletter).
#   - Efterfølgende kunne opdatere tabellen inkrementelt (kun nye rækker).
#
# “RAW” betyder her:
#   - Tæt på API-strukturen (ingen tolkning, ingen binning/klasser endnu).
#   - Kun let teknisk formatering (observed_raw → POSIXct, sortering osv.).
#   - Bruges som “sandhedskilde” til clean/transform/join-ready lag senere.
# =============================================================================



# =============================================================================
# SCRIPT 1 – HENT ALLE WIND_SPEED-DATA FOR KARUP (06060) – ROBUST VERSION
# =============================================================================
# Dette afsnit:
#   - sætter DMI-API’et op (endpoint, station, parameter),
#   - definerer en rå kald-funktion for ét datointerval,
#   - wrapper den med retry-logik (fetch_wind_interval),
#   - laver ét års-intervallager fra 2000 til i dag,
#   - og binder alle resultater sammen til wind_speed_karup (RAW i R).
suppressPackageStartupMessages({
  if (!requireNamespace("pacman", quietly = TRUE)) install.packages("pacman")
  pacman::p_load(httr2, jsonlite, dplyr, tibble, purrr, lubridate)
})

# 1) API-nøgle ---------------------------------------------------------------
# DMI_API_KEY skal ligge i .Renviron.
# Hvis den ikke findes, stopper vi med en klar fejl, så man opdager det tidligt.
dmi_api_key <- Sys.getenv("DMI_API_KEY")
if (dmi_api_key == "") {
  stop("DMI_API_KEY er ikke sat – tjek .Renviron og genstart R.")
}

# 2) Opsætning ---------------------------------------------------------------
# Faste parametre for alle kald:
#   - base_url: metObs observation-endpoint.
#   - station_id: 06060 (Karup).
#   - param_id: wind_speed (middelvind).
base_url   <- "https://dmigw.govcloud.dk/v2/metObs/collections/observation/items"
station_id <- "06060"        # Flyvestation Karup
param_id   <- "wind_speed"   # middelvind (m/s)

# 3) Rå kald-funktion for ét interval ----------------------------------------
# do_call_wind:
#   - bygger et datetime-interval i ISO-format (UTC),
#   - kalder DMI API for det interval,
#   - mapper hver feature til en række i en tibble,
#   - returnerer tom tibble, hvis der ingen features er.
do_call_wind <- function(start_date, end_date) {
  
  start_txt <- paste0(format(start_date, "%Y-%m-%d"), "T00:00:00Z")
  end_txt   <- paste0(format(end_date,   "%Y-%m-%d"), "T23:59:59Z")
  dt_range  <- paste0(start_txt, "/", end_txt)
  
  cat("  Kald DMI API for interval:", start_txt, "→", end_txt, "\n")
  
  req <- request(base_url) |>
    req_url_query(
      stationId   = station_id,
      parameterId = param_id,
      datetime    = dt_range,
      limit       = 100000   # rigeligt pr. år for én parameter
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

# 4) Robust wrapper med retry (håndterer fejl, 504 osv.) ---------------------
# fetch_wind_interval:
#   - kalder do_call_wind med op til max_retries forsøg,
#   - fanger fejl med try(),
#   - venter kort tid mellem forsøg,
#   - returnerer tom tibble, hvis alle forsøg fejler.
fetch_wind_interval <- function(start_date, end_date, max_retries = 3) {
  attempt <- 1
  while (attempt <= max_retries) {
    cat("  Forsøg", attempt, "af", max_retries, "\n")
    
    res <- try(
      do_call_wind(start_date, end_date),
      silent = TRUE
    )
    
    # Hvis ikke der er fejl, returnér resultatet
    if (!inherits(res, "try-error")) {
      return(res)
    }
    
    # Hvis der ER fejl, print og prøv igen
    cat("    Fejl ved kald – prøver igen...\n")
    
    if (attempt < max_retries) {
      cat("    Venter 5 sekunder...\n")
      Sys.sleep(5)
    }
    
    attempt <- attempt + 1
  }
  
  cat("    GIVER OP for dette interval efter", max_retries, "forsøg – returnerer tomt tibble.\n")
  tibble()
}

# 5) Lav års-intervaller fra 2000-01-01 til i dag ----------------------------
# For at gøre kaldene robuste laver vi ét kald per år:
#   - start_total / end_total definerer hele periode,
#   - years er listen af år,
#   - intervals indeholder start- og slutdato pr. år.
start_total <- as.Date("2000-01-01")
end_total   <- Sys.Date()

years <- seq(year(start_total), year(end_total))

intervals <- map(years, function(y) {
  start_y <- as.Date(paste0(y, "-01-01"))
  end_y   <- as.Date(paste0(y, "-12-31"))
  
  tibble(
    year  = y,
    start = max(start_y, start_total),
    end   = min(end_y,   end_total)
  )
}) |>
  bind_rows()

cat("Intervaller der hentes (år for år):\n")
print(intervals)

# 6) Hent alle år og bind sammen ---------------------------------------------
# Vi looper over alle års-intervaller med fetch_wind_interval
# og binder dem efterfølgende til ét samlet RAW-datasæt.
wind_list <- map(
  seq_len(nrow(intervals)),
  function(i) {
    this_year  <- intervals$year[i]
    start_i    <- intervals$start[i]
    end_i      <- intervals$end[i]
    
    cat("\n============================================\n")
    cat("År:", this_year, "– henter wind_speed\n")
    cat("============================================\n")
    
    fetch_wind_interval(start_i, end_i, max_retries = 3)
  }
)

wind_speed_karup_raw <- bind_rows(wind_list)

cat("\nSamlet antal rækker hentet (wind_speed):", nrow(wind_speed_karup_raw), "\n")

# 7) Rens og lav endelig (RAW) tabel -----------------------------------------
# Her laver vi kun “teknisk” rensning:
#   - observed_raw → POSIXct (UTC),
#   - vælger de centrale kolonner,
#   - sorterer efter observed.
# Ingen business-logik endnu – stadig et RAW-lag i R.
wind_speed_karup <- wind_speed_karup_raw |>
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
cat("Første rækker af wind_speed_karup:\n")
print(head(wind_speed_karup))
View(wind_speed_karup, title = "wind_speed_karup – wind_speed for Karup")

# Valgfrit: gem lokalt til senere brug
# saveRDS(wind_speed_karup, "wind_speed_karup_2000_nu.rds")



# =============================================================================
# SCRIPT A – FØRSTE FULDE LOAD AF WIND_SPEED_KARUP TIL Azure SQL (RAW-LAG)
#            → PBA01_Raw.fact_wind_speed_raw
#            (med dummy-sikring mod dublet-upload)
# =============================================================================
# Dette afsnit:
#   - læser logininfo fra .Renviron,
#   - opretter ODBC-forbindelse,
#   - sikrer at PBA01_Raw.fact_wind_speed_raw eksisterer,
#   - tjekker om der allerede er data for 06060/wind_speed,
#   - uploader hele wind_speed_karup én gang (første fuld-load).
suppressPackageStartupMessages({
  if (!requireNamespace("pacman", quietly = TRUE)) install.packages("pacman")
  pacman::p_load(DBI, odbc, dplyr, lubridate)
})

cat("=== WIND_SPEED – fuld load til Azure SQL (RAW) ===\n")

# 1) Læs login til Azure SQL fra .Renviron -----------------------------------
# Loginoplysninger holdes uden for koden via miljøvariabler, så scriptet
# kan deles uden at afsløre credentials.
server     <- Sys.getenv("AZURE_SQL_SERVER")
db         <- Sys.getenv("AZURE_SQL_DB")
uid        <- Sys.getenv("AZURE_SQL_UID")
pwd        <- Sys.getenv("AZURE_SQL_PWD")
schema_raw <- "PBA01_Raw"

if (server == "" || db == "" || uid == "" || pwd == "") {
  stop("AZURE_SQL_* miljøvariable er ikke sat korrekt.")
}

# 2) Opret forbindelse til Azure SQL -----------------------------------------
cat("Opretter forbindelse til Azure SQL...\n")

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

cat("Forbindelse oprettet.\n")

# 3) Tjek at wind_speed_karup findes -----------------------------------------
# Vi stopper, hvis API-datasættet ikke er tilgængeligt i environment.
if (!exists("wind_speed_karup")) {
  dbDisconnect(con)
  stop("Objektet 'wind_speed_karup' findes ikke i R – kør API-scriptet først.")
}

cat("Objekt 'wind_speed_karup' fundet i environment.\n")

# 4) Forbered data til upload -------------------------------------------------
# Vi tilføjer load_timestamp (hvornår rækken blev indlæst i RAW-laget).
wind_speed_upload <- wind_speed_karup |>
  mutate(
    load_timestamp = Sys.time()
  )

cat("Antal rækker klar til upload:", nrow(wind_speed_upload), "\n")

# 5) Opret tabel i SQL hvis den ikke findes ----------------------------------
# Vi opretter tabellen i RAW-schemaet PBA01_Raw, hvis den ikke allerede findes.
cat("Sikrer at PBA01_Raw.fact_wind_speed_raw findes...\n")

dbExecute(con, sprintf("
IF NOT EXISTS (
  SELECT 1
  FROM INFORMATION_SCHEMA.TABLES
  WHERE TABLE_SCHEMA = '%s'
    AND TABLE_NAME   = 'fact_wind_speed_raw'
)
BEGIN
  CREATE TABLE %s.fact_wind_speed_raw (
    wind_id        bigint IDENTITY(1,1) PRIMARY KEY,
    stationId      varchar(10),
    parameterId    varchar(50),
    observed       datetime2,
    value          float,
    lon            float,
    lat            float,
    load_timestamp datetime2
  );
END;
", schema_raw, schema_raw))

cat("Tabel-tjek/creation færdig for", paste0(schema_raw, ".fact_wind_speed_raw"), ".\n")

# 5b) DUMMY-SIKRING – tjek om der allerede er data for denne station/parameter
# Dummy-sikringen sikrer, at vi kun laver fuld-load én gang for 06060/wind_speed.
antal_eksisterende <- dbGetQuery(con, sprintf("
  SELECT COUNT(1) AS n
  FROM %s.fact_wind_speed_raw
  WHERE stationId   = '06060'
    AND parameterId = 'wind_speed';
", schema_raw))$n[1]

if (antal_eksisterende > 0) {
  cat("\n*** DUMMY-SIKRING AKTIVERET ***\n")
  cat("Tabellen ", schema_raw, ".fact_wind_speed_raw indeholder allerede ",
      antal_eksisterende,
      " rækker for station 06060 / wind_speed.\n", sep = "")
  cat("Fuld-load afbrydes for at undgå dubletter.\n")
  dbDisconnect(con)
  stop("Fuld-load må kun køres på en tom tabel for denne station/parameter. Brug inkrementel script i stedet.")
}

cat("Dummy-sikring: ingen eksisterende data for 06060 / wind_speed – fuld-load fortsætter.\n\n")

# 6) Upload data (første fulde load) -----------------------------------------
# Vi appender alle rækker til PBA01_Raw.fact_wind_speed_raw.
# append = TRUE, overwrite = FALSE er sikkert, fordi vi lige har tjekket,
# at tabellen er tom for denne station/parameter.
cat("Uploader data til PBA01_Raw.fact_wind_speed_raw...\n")

dbWriteTable(
  con,
  name      = DBI::Id(schema = schema_raw, table = "fact_wind_speed_raw"),
  value     = wind_speed_upload,
  append    = TRUE,
  overwrite = FALSE
)

cat("Første fulde load færdig. Antal rækker uploadet:", nrow(wind_speed_upload), "\n")

dbDisconnect(con)
cat("Forbindelse til Azure SQL lukket.\n")



# =============================================================================
# SCRIPT B – INKREMENTEL OPDATERING AF WIND_SPEED-DATA I Azure SQL
#            → PBA01_Raw.fact_wind_speed_raw
# =============================================================================
# Dette afsnit:
#   - finder seneste observed i PBA01_Raw.fact_wind_speed_raw,
#   - definerer et interval fra (seneste + 1 sekund) til nu (UTC),
#   - henter kun nye observationer fra DMI,
#   - appender dem til RAW-tabellen.
suppressPackageStartupMessages({
  if (!requireNamespace("pacman", quietly = TRUE)) install.packages("pacman")
  pacman::p_load(DBI, odbc, dplyr, lubridate, httr2, tibble, purrr)
})

cat("=== WIND_SPEED – inkrementel opdatering (RAW) ===\n")

# 1) Læs login til Azure SQL --------------------------------------------------
server     <- Sys.getenv("AZURE_SQL_SERVER")
db         <- Sys.getenv("AZURE_SQL_DB")
uid        <- Sys.getenv("AZURE_SQL_UID")
pwd        <- Sys.getenv("AZURE_SQL_PWD")
schema_raw <- "PBA01_Raw"

if (server == "" || db == "" || uid == "" || pwd == "") {
  stop("AZURE_SQL_* miljøvariable er ikke sat korrekt.")
}

# 2) Opret forbindelse til Azure SQL -----------------------------------------
cat("Opretter forbindelse til Azure SQL...\n")

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

cat("Forbindelse oprettet.\n")

# 3) Find seneste observed i SQL for Karup wind_speed ------------------------
cat("Finder seneste observed for station 06060, parameter 'wind_speed'...\n")

last_obs_df <- dbGetQuery(con, sprintf("
  SELECT MAX(observed) AS last_obs
  FROM %s.fact_wind_speed_raw
  WHERE stationId   = '06060'
    AND parameterId = 'wind_speed';
", schema_raw))

last_obs <- last_obs_df$last_obs[1]

if (is.na(last_obs)) {
  dbDisconnect(con)
  stop("Der er ingen data i PBA01_Raw.fact_wind_speed_raw endnu – kør fuld-load (Script A) først.")
}

cat("Seneste observed i SQL:", as.character(last_obs), "\n")

# 4) Opsæt DMI API -----------------------------------------------------------
dmi_api_key <- Sys.getenv("DMI_API_KEY")
if (dmi_api_key == "") {
  dbDisconnect(con)
  stop("DMI_API_KEY er ikke sat – tjek .Renviron.")
}

base_url   <- "https://dmigw.govcloud.dk/v2/metObs/collections/observation/items"
station_id <- "06060"
param_id   <- "wind_speed"

# 5) Definér tidsinterval for nye data ---------------------------------------
# Start = 1 sekund efter seneste observed, slut = nu (UTC).
# På den måde undgår vi dubletter, men mister ikke målinger.
start_time <- with_tz(last_obs, "UTC") + seconds(1)
end_time   <- with_tz(Sys.time(), "UTC")

start_txt <- format(start_time, "%Y-%m-%dT%H:%M:%SZ")
end_txt   <- format(end_time,   "%Y-%m-%dT%H:%M:%SZ")

dt_range  <- paste0(start_txt, "/", end_txt)

cat("Henter nye data i interval:", dt_range, "\n")

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
  cat("Ingen nye observationer fundet i DMI API.\n")
  dbDisconnect(con)
  cat("Forbindelse til Azure SQL lukket.\n")
} else {
  
  # Map nye observationer til samme struktur som RAW-tabellen
  new_wind <- map_dfr(
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
  
  cat("Antal nye rækker hentet:", nrow(new_wind), "\n")
  
  if (nrow(new_wind) > 0) {
    cat("Uploader nye rækker til PBA01_Raw.fact_wind_speed_raw...\n")
    
    dbWriteTable(
      con,
      name      = DBI::Id(schema = schema_raw, table = "fact_wind_speed_raw"),
      value     = new_wind,
      append    = TRUE,
      overwrite = FALSE
    )
    
    cat("Nye rækker er nu indsat i", paste0(schema_raw, ".fact_wind_speed_raw"), ".\n")
  } else {
    cat("Der var ingen nye rækker at uploade.\n")
  }
  
  dbDisconnect(con)
  cat("Forbindelse til Azure SQL lukket.\n")
}
