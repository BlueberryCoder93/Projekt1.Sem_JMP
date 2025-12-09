# =============================================================================
# RAW – PRECIP_PAST10MIN (NEDBØR) FOR KARUP (06060)
# =============================================================================
# Formål med dette script:
#   - Hente alle DMI-observationer for parameteren “precip_past10min”
#     (nedbør de sidste 10 minutter) for station 06060 (Flyvestation Karup).
#   - Gemme dem som et råt faktalayer i Azure SQL:
#         PBA01_Raw.fact_precip_past10min_raw
#   - Sikre, at fuld-load kun køres én gang (dummy-sikring mod dubletter).
#   - Efterfølgende kunne opdatere tabellen inkrementelt (kun nye rækker).
#
# “RAW” betyder her:
#   - Tæt på API-strukturen (ingen tolkning, ingen aggregater).
#   - Kun teknisk formatering (observed_raw → POSIXct, sortering osv.).
#   - Bruges som “sandhedslager” til clean/transform/join-ready lag senere.
# =============================================================================


# =============================================================================
# Script 1 – Hent alle precip_past10min (nedbør) for Karup (06060)
# =============================================================================
# Dette afsnit:
#   - sætter DMI-API’et op (endpoint, station, parameter),
#   - definerer en funktion til at hente ét datointerval (fx ét år),
#   - laver års-intervaller fra 2000 til i dag,
#   - binder alle interval-resultater sammen til precip_karup (RAW i R).

suppressPackageStartupMessages({
  if (!requireNamespace("pacman", quietly = TRUE)) install.packages("pacman")
  pacman::p_load(httr2, jsonlite, dplyr, tibble, purrr, lubridate)
})

# 1) API-nøgle ---------------------------------------------------------------
# DMI_API_KEY skal ligge i .Renviron.
# Hvis den ikke findes, stopper scriptet, så man opdager problemet tidligt.
dmi_api_key <- Sys.getenv("DMI_API_KEY")
if (dmi_api_key == "") {
  stop("DMI_API_KEY er ikke sat – tjek .Renviron og genstart R.")
}

# 2) Opsætning ---------------------------------------------------------------
# Faste parametre for alle kald:
#   - base_url: metObs observation-endpoint,
#   - station_id: 06060 (Karup),
#   - param_id: precip_past10min (nedbør de sidste 10 minutter).
base_url   <- "https://dmigw.govcloud.dk/v2/metObs/collections/observation/items"
station_id <- "06060"             # Flyvestation Karup
param_id   <- "precip_past10min"  # nedbør sidste 10 min

# 3) Funktion til at hente ét datointerval -----------------------------------
# fetch_precip_interval:
#   - bygger et datetime-interval i ISO-format (UTC),
#   - kalder DMI API for det interval,
#   - mapper hver feature til en række i en tibble,
#   - returnerer tom tibble, hvis der ingen features er.
fetch_precip_interval <- function(start_date, end_date) {
  
  start_txt <- paste0(format(start_date, "%Y-%m-%d"), "T00:00:00Z")
  end_txt   <- paste0(format(end_date,   "%Y-%m-%d"), "T23:59:59Z")
  dt_range  <- paste0(start_txt, "/", end_txt)
  
  cat("    Henter interval:", start_txt, "→", end_txt, "\n")
  
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
    cat("      Ingen features i dette interval.\n")
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
  
  cat("      Rækker hentet i interval:", nrow(out), "\n")
  out
}

# 4) Lav års-intervaller fra 2000-01-01 til i dag ----------------------------
# Vi deler hele perioden op i ét interval pr. år:
#   - start_total / end_total definerer den samlede periode,
#   - years er listen af år,
#   - intervals indeholder start/slut pr. år (trimmet til start_total/end_total).
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
# Vi mapper fetch_precip_interval hen over alle års-intervaller
# og binder resultatet til ét samlet RAW-datasæt.
cat("\nStarter hentning af precip_past10min for Karup...\n")

precip_list <- map2(intervals$start, intervals$end, fetch_precip_interval)

precip_karup_raw <- bind_rows(precip_list)

cat("\nSamlet antal rækker hentet:", nrow(precip_karup_raw), "\n")

# 6) Rens og lav endelig RAW-tabel -------------------------------------------
# Her laver vi kun teknisk rensning:
#   - observed_raw → POSIXct (UTC),
#   - vælger de relevante kolonner,
#   - sorterer efter observed.
# Ingen businesslogik eller aggregeringer – stadig et RAW-lag i R.
precip_karup <- precip_karup_raw |>
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
cat("Første rækker:\n")
print(head(precip_karup))

View(precip_karup, title = "precip_karup – precip_past10min for Karup")

# Valgfrit: gem til senere brug
# saveRDS(precip_karup, "precip_karup_2000_nu.rds")



# =============================================================================
# Script 2 – FØRSTE FULDE LOAD AF PRECIP_KARUP TIL Azure SQL (RAW-LAG)
#            → PBA01_Raw.fact_precip_past10min_raw
#            (med dummy-sikring mod dublet-upload)
# =============================================================================
# Dette afsnit:
#   - læser logininfo fra .Renviron,
#   - opretter ODBC-forbindelse,
#   - sikrer at PBA01_Raw.fact_precip_past10min_raw eksisterer,
#   - tjekker om der allerede er data for 06060/precip_past10min,
#   - uploader hele precip_karup én gang (første fuld-load).

suppressPackageStartupMessages({
  if (!requireNamespace("pacman", quietly = TRUE)) install.packages("pacman")
  pacman::p_load(DBI, odbc, dplyr, lubridate)
})

# 1) Læs login til Azure SQL fra .Renviron -----------------------------------
# Login holdes uden for koden via miljøvariabler, så scriptet kan deles trygt.
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

# 3) Forbered data til upload -------------------------------------------------
# Vi stopper, hvis precip_karup ikke ligger i environment (API-script ikke kørt).
if (!exists("precip_karup")) {
  dbDisconnect(con)
  stop("Objektet 'precip_karup' findes ikke i R – kør API-scriptet først.")
}

precip_upload <- precip_karup %>%
  mutate(
    load_timestamp = Sys.time()    # hvornår rækken blev læsset i RAW-laget
  )

cat("Klar til upload. Antal rækker i precip_upload:", nrow(precip_upload), "\n")

# 4) Opret tabel i SQL hvis den ikke findes ----------------------------------
# Vi opretter tabellen i RAW-schemaet PBA01_Raw, hvis den ikke allerede findes.
cat("Sikrer at PBA01_Raw.fact_precip_past10min_raw findes...\n")

dbExecute(con, sprintf("
IF NOT EXISTS (
  SELECT 1
  FROM INFORMATION_SCHEMA.TABLES
  WHERE TABLE_SCHEMA = '%s'
    AND TABLE_NAME   = 'fact_precip_past10min_raw'
)
BEGIN
  CREATE TABLE %s.fact_precip_past10min_raw (
    precip_id      bigint IDENTITY(1,1) PRIMARY KEY,
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

cat("Tabel tjek/creation færdig for", paste0(schema_raw, ".fact_precip_past10min_raw"), ".\n")

# 4b) DUMMY-SIKRING: tjek om der allerede er data i tabellen -----------------
# Idé:
#   - Fuld-load er kun meningen at køre én gang på en tom tabel.
#   - Hvis der allerede findes data for station 06060 / precip_past10min,
#     afbrydes scriptet for at undgå dubletter.
antal_eksisterende <- dbGetQuery(con, sprintf("
  SELECT COUNT(1) AS n
  FROM %s.fact_precip_past10min_raw
  WHERE stationId   = '06060'
    AND parameterId = 'precip_past10min';
", schema_raw))$n[1]

if (antal_eksisterende > 0) {
  cat("\n*** DUMMY-SIKRING AKTIVERET ***\n")
  cat("Tabellen ", schema_raw, ".fact_precip_past10min_raw indeholder allerede ",
      antal_eksisterende,
      " rækker for station 06060 / precip_past10min.\n", sep = "")
  cat("Fuld-load afbrydes for at undgå dubletter.\n")
  dbDisconnect(con)
  stop("Fuld-load må kun køres på en tom tabel. Brug inkrementel script i stedet.")
}

cat("Dummy-sikring: tabellen er tom for denne station/parameter – fuld-load fortsætter.\n\n")

# 5) Upload data (første fulde load) -----------------------------------------
# Vi appender alle rækker til PBA01_Raw.fact_precip_past10min_raw.
# append = TRUE, overwrite = FALSE er sikkert, fordi dummy-sikringen lige er kørt.
cat("Uploader data til PBA01_Raw.fact_precip_past10min_raw...\n")

dbWriteTable(
  con,
  name      = DBI::Id(schema = schema_raw, table = "fact_precip_past10min_raw"),
  value     = precip_upload,
  append    = TRUE,   # her er det ok, fordi tabellen lige er tjekket tom
  overwrite = FALSE
)

cat("Første fulde load færdig. Antal rækker uploadet:", nrow(precip_upload), "\n")

dbDisconnect(con)
cat("Forbindelse lukket.\n")



# =============================================================================
# Script 3 – INKREMENTEL OPDATERING AF PRECIP_PAST10MIN FOR KARUP (06060)
#            → PBA01_Raw.fact_precip_past10min_raw
# =============================================================================
# Dette afsnit:
#   - finder seneste observed i PBA01_Raw.fact_precip_past10min_raw,
#   - definerer et interval fra (seneste + 1 sekund) til nu (UTC),
#   - henter kun nye observationer fra DMI,
#   - appender dem til RAW-tabellen.

suppressPackageStartupMessages({
  if (!requireNamespace("pacman", quietly = TRUE)) install.packages("pacman")
  pacman::p_load(DBI, odbc, dplyr, lubridate, httr2, tibble, purrr)
})

cat("=== INKREMENTEL OPDATERING AF NEDBØR (precip_past10min) STARTER ===\n")

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

# 3) Find seneste observed i SQL for Karup precip_past10min ------------------
cat("Finder seneste observed i PBA01_Raw.fact_precip_past10min_raw...\n")

last_obs_df <- dbGetQuery(con, sprintf("
  SELECT MAX(observed) AS last_obs
  FROM %s.fact_precip_past10min_raw
  WHERE stationId   = '06060'
    AND parameterId = 'precip_past10min';
", schema_raw))

last_obs <- last_obs_df$last_obs[1]

if (is.na(last_obs)) {
  dbDisconnect(con)
  stop("Der er ingen data i PBA01_Raw.fact_precip_past10min_raw endnu – kør fuldt load-scriptet først.")
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
param_id   <- "precip_past10min"

# 5) Definér tidsinterval for nye data ---------------------------------------
# Start:
#   - hvis last_obs allerede er en POSIXt, konverteres den til UTC og +1 sekund,
#   - ellers parses den som tekst og +1 sekund.
# Slut:
#   - nuværende tidspunkt i UTC.
if (inherits(last_obs, "POSIXt")) {
  start_time <- with_tz(last_obs, "UTC") + seconds(1)
} else {
  start_time <- ymd_hms(last_obs, tz = "UTC") + seconds(1)
}

end_time <- with_tz(Sys.time(), "UTC")

start_txt <- format(start_time, "%Y-%m-%dT%H:%M:%SZ")
end_txt   <- format(end_time,   "%Y-%m-%dT%H:%M:%SZ")

dt_range <- paste0(start_txt, "/", end_txt)

cat("Henter nye data i interval:\n  ", dt_range, "\n")

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

cat("Kalder DMI API...\n")
resp <- req_perform(req)
cat("Svar modtaget fra DMI.\n")

x <- resp_body_json(resp, simplifyVector = FALSE)

if (is.null(x$features) || length(x$features) == 0) {
  cat("Ingen nye observationer fundet i DMI API for dette interval.\n")
  dbDisconnect(con)
  cat("Forbindelse lukket. Ingen ændringer foretaget.\n")
} else {
  
  cat("Parser nye observationer...\n")
  
  # Map nye observationer til samme struktur som RAW-tabellen
  new_precip <- map_dfr(
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
  ) %>%
    arrange(observed) %>%
    mutate(load_timestamp = Sys.time())
  
  cat("Antal nye rækker hentet:", nrow(new_precip), "\n")
  
  if (nrow(new_precip) > 0) {
    cat("Uploader nye rækker til PBA01_Raw.fact_precip_past10min_raw...\n")
    
    dbWriteTable(
      con,
      name      = DBI::Id(schema = schema_raw, table = "fact_precip_past10min_raw"),
      value     = new_precip,
      append    = TRUE,
      overwrite = FALSE
    )
    
    cat("Upload færdig. Nye rækker indsat i PBA01_Raw.fact_precip_past10min_raw.\n")
  } else {
    cat("Der var ingen nye rækker at uploade.\n")
  }
  
  dbDisconnect(con)
  cat("Forbindelse lukket.\n")
}

cat("=== INKREMENTEL OPDATERING AF NEDBØR FÆRDIG ===\n")


