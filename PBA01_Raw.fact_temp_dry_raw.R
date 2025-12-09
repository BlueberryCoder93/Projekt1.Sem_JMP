# =============================================================================
# RAW – TEMP_DRY FOR KARUP (06060)
# =============================================================================
# Formål med dette script:
#   - Hente alle DMI-observationer for temp_dry (2 m lufttemperatur) for station 06060
#   - Gemme dem som et råt faktalayer i Azure SQL (PBA01_Raw.fact_temp_dry_raw)
#   - Sikre, at fuld-load kun køres én gang (dummy-sikring mod dubletter)
#   - Efterfølgende kunne opdatere tabellen inkrementelt (kun nye rækker)
#
# RAW betyder her:
#   - direkte afspejling af DMI API (ingen join, ingen tolkning)
#   - kun let teknisk formatering (fx observed_raw → POSIXct)
#   - bruges som “sandhedskilde” for senere clean/transform/join-ready lag
# =============================================================================


# =============================================================================
# SCRIPT 1 – HENT ALLE TEMP_DRY-DATA FOR KARUP (06060) – ROBUST VERSION
# =============================================================================
# Dette script står for selve API-pullet:
#   - opsætter DMI-API (endpoint, station, parameter)
#   - henter data år for år fra år 2000 til dags dato med retry-logik
#   - returnerer én samlet tibble: temp_dry_karup
#   - ingen SQL i dette script – kun R-objekt i memory
suppressPackageStartupMessages({
  if (!requireNamespace("pacman", quietly = TRUE)) install.packages("pacman")
  pacman::p_load(httr2, jsonlite, dplyr, tibble, purrr, lubridate)
})

cat("=== TEMP_DRY – fuldt pull fra DMI for station 06060 ===\n")

# 1) API-nøgle ---------------------------------------------------------------
# DMI_API_KEY læses fra .Renviron. 
# Hvis den ikke findes, stopper vi, så vi ikke kalder API’et uden nøgle.
dmi_api_key <- Sys.getenv("DMI_API_KEY")
if (dmi_api_key == "") {
  stop("DMI_API_KEY er ikke sat – tjek .Renviron og genstart R.")
}

# 2) Opsætning ---------------------------------------------------------------
# Faste parametre til alle kald:
#   - base_url: metObs observation-endpoint
#   - station_id: DMI-station 06060 (Karup)
#   - param_id: temp_dry (2 m lufttemperatur)
base_url   <- "https://dmigw.govcloud.dk/v2/metObs/collections/observation/items"
station_id <- "06060"        # Flyvestation Karup
param_id   <- "temp_dry"     # 2 m lufttemperatur

# 3) Rå kald-funktion for ét interval ----------------------------------------
# do_call_temp:
#   - bygger datetime-interval i ISO-format (UTC)
#   - kalder DMI API for det interval
#   - mapper JSON-features til en tibble
#   - returnerer tom tibble, hvis der ingen features er
do_call_temp <- function(start_date, end_date) {
  
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
  
  out <- purrr::map_dfr(
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
  
  cat("    Rækker hentet i interval:", nrow(out), "\n")
  out
}

# 4) Robust wrapper med retry (håndterer fx 504) -----------------------------
# fetch_temp_interval:
#   - kalder do_call_temp indenfor en while-løkke
#   - hvis kaldet fejler (try-error), forsøger vi igen op til max_retries
#   - indlægger 5 sekunders pause mellem forsøg
#   - returnerer tom tibble, hvis alle forsøg fejler
fetch_temp_interval <- function(start_date, end_date, max_retries = 3) {
  attempt <- 1
  while (attempt <= max_retries) {
    cat("  Forsøg", attempt, "af", max_retries, "\n")
    
    res <- try(
      do_call_temp(start_date, end_date),
      silent = TRUE
    )
    
    if (!inherits(res, "try-error")) {
      return(res)
    }
    
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
# For at gøre loadet robust laver vi ét kald pr. år:
#   - start_total / end_total definerer hele perioden
#   - years = 2000…nu
#   - intervals indeholder start- og slutdato for hvert år
start_total <- as.Date("2000-01-01")
end_total   <- Sys.Date()

years <- seq(lubridate::year(start_total), lubridate::year(end_total))

intervals <- purrr::map(years, function(y) {
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
# Vi looper over alle årsintervaller med fetch_temp_interval
# og binder resultatet sammen til én stor tibble: temp_dry_karup_raw.
temp_list <- purrr::map(
  seq_len(nrow(intervals)),
  function(i) {
    this_year  <- intervals$year[i]
    start_i    <- intervals$start[i]
    end_i      <- intervals$end[i]
    
    cat("\n============================================\n")
    cat("År:", this_year, "– henter temp_dry\n")
    cat("============================================\n")
    
    fetch_temp_interval(start_i, end_i, max_retries = 3)
  }
)

temp_dry_karup_raw <- bind_rows(temp_list)

cat("\nSamlet antal rækker hentet (temp_dry):", nrow(temp_dry_karup_raw), "\n")

# 7) Rens og lav endelig tabel ----------------------------------------------
# Vi laver kun “teknisk” rensning:
#   - observed_raw → POSIXct med UTC
#   - vælger kun relevante kolonner
#   - sorterer kronologisk
#
# Dette er stadig RAW-laget (ingen tolkning, ingen mapping).
temp_dry_karup <- temp_dry_karup_raw |>
  mutate(
    observed = lubridate::ymd_hms(observed_raw, tz = "UTC")
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

cat("Dataset 'temp_dry_karup' er nu klar i R.\n")
print(head(temp_dry_karup))
View(temp_dry_karup, title = "temp_dry_karup – temp_dry for Karup")

# Valgfrit: gem lokalt til offline-brug
# saveRDS(temp_dry_karup, "temp_dry_karup_2000_nu.rds")



# =============================================================================
# SCRIPT A – FØRSTE FULDE LOAD AF TEMP_DRY_KARUP TIL Azure SQL (RAW-LAG)
#            → PBA01_Raw.fact_temp_dry_raw
#            (med dummy-sikring mod dublet-upload)
# =============================================================================
# Script A flytter data fra R til Azure SQL (RAW-schema):
#   - opretter forbindelse til Azure
#   - sikrer at tabellen findes i PBA01_Raw
#   - tjekker om der allerede findes data for 06060/temp_dry
#   - uploader hele temp_dry_karup første gang (fuld historisk load)
suppressPackageStartupMessages({
  if (!requireNamespace("pacman", quietly = TRUE)) install.packages("pacman")
  pacman::p_load(DBI, odbc, dplyr, lubridate)
})

cat("=== TEMP_DRY – fuld load til Azure SQL ===\n")

# 1) Læs login til Azure SQL fra .Renviron -----------------------------------
# Login læses fra miljøvariabler:
#   - AZURE_SQL_SERVER
#   - AZURE_SQL_DB
#   - AZURE_SQL_UID
#   - AZURE_SQL_PWD
server <- Sys.getenv("AZURE_SQL_SERVER")
db     <- Sys.getenv("AZURE_SQL_DB")
uid    <- Sys.getenv("AZURE_SQL_UID")
pwd    <- Sys.getenv("AZURE_SQL_PWD")
schema_raw <- "PBA01_Raw"

if (server == "" || db == "" || uid == "" || pwd == "") {
  stop("AZURE_SQL_* miljøvariable er ikke sat korrekt.")
}

# 2) Opret forbindelse til Azure SQL -----------------------------------------
# Vi åbner ODBC-forbindelsen til Azure SQL. 
# Denne bruges kun til selve uploaden i Script A.
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

# 3) Tjek at temp_dry_karup findes -------------------------------------------
# Vi sikrer os, at Script 1 er kørt først, ellers giver det ingen mening
# at uploade.
if (!exists("temp_dry_karup")) {
  dbDisconnect(con)
  stop("Objektet 'temp_dry_karup' findes ikke i R – kør Script 1 (API) først.")
}

cat("Objekt 'temp_dry_karup' fundet i environment.\n")

# 4) Forbered data til upload -------------------------------------------------
# Vi tilføjer load_timestamp, så vi kan se, hvornår rækken blev indlæst
# i databasen (klassisk datalager-praksis).
temp_dry_upload <- temp_dry_karup |>
  mutate(
    load_timestamp = Sys.time()
  )

cat("Antal rækker klar til upload:", nrow(temp_dry_upload), "\n")

# 5) Opret tabel i SQL hvis den ikke findes ----------------------------------
# Her arbejder vi nu i RAW-schemaet PBA01_Raw (ikke længere dbo).
# Strukturen matcher 1:1 de kolonner, vi uploader fra R.
cat("Sikrer at PBA01_Raw.fact_temp_dry_raw findes...\n")

dbExecute(con, sprintf("
IF NOT EXISTS (
  SELECT 1
  FROM INFORMATION_SCHEMA.TABLES
  WHERE TABLE_SCHEMA = '%s'
    AND TABLE_NAME   = 'fact_temp_dry_raw'
)
BEGIN
  CREATE TABLE %s.fact_temp_dry_raw (
    temp_id        bigint IDENTITY(1,1) PRIMARY KEY,
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

cat("Tabel tjek/creation færdig.\n")

# 5b) DUMMY-SIKRING – tjek om der allerede er data for denne station/parameter
# Dummy-sikringen sørger for, at vi ikke kommer til at fuld-loade oven i
# eksisterende data for 06060/temp_dry. 
# Tanken:
#   - fuld-load → kun på en tom tabel
#   - derefter bruges Script B (inkrementel) fremover.
antal_eksisterende <- dbGetQuery(con, sprintf("
  SELECT COUNT(1) AS n
  FROM %s.fact_temp_dry_raw
  WHERE stationId   = '06060'
    AND parameterId = 'temp_dry';
", schema_raw))$n[1]

if (antal_eksisterende > 0) {
  cat("\n*** DUMMY-SIKRING AKTIVERET ***\n")
  cat("Tabellen ", schema_raw, ".fact_temp_dry_raw indeholder allerede ",
      antal_eksisterende,
      " rækker for station 06060 / temp_dry.\n", sep = "")
  cat("Fuld-load afbrydes for at undgå dubletter.\n")
  dbDisconnect(con)
  stop("Fuld-load må kun køres på en tom tabel for denne station/parameter. Brug inkrementel script i stedet.")
}

cat("Dummy-sikring: ingen eksisterende data for 06060 / temp_dry – fuld-load fortsætter.\n\n")

# 6) Upload data (første fulde load) -----------------------------------------
# Vi uploader alle rækker fra temp_dry_upload til PBA01_Raw.fact_temp_dry_raw.
# append = TRUE, overwrite = FALSE er sikkert, fordi vi lige har konstateret,
# at der ikke ligger data for 06060/temp_dry i forvejen.
cat("Uploader data til PBA01_Raw.fact_temp_dry_raw...\n")

dbWriteTable(
  con,
  name      = DBI::Id(schema = schema_raw, table = "fact_temp_dry_raw"),
  value     = temp_dry_upload,
  append    = TRUE,
  overwrite = FALSE
)

cat("Første fulde load færdig. Antal rækker uploadet:", nrow(temp_dry_upload), "\n")

dbDisconnect(con)
cat("Forbindelse til Azure SQL lukket.\n")



# =============================================================================
# SCRIPT B – TEMP_DRY – INKREMENTEL OPDATERING (KUN NYE RÆKKER)
# =============================================================================
# Script B bruges kun efter første fuld-load:
#   - finder seneste observed i PBA01_Raw.fact_temp_dry_raw
#   - henter data fra DMI API fra (seneste + 1 sekund) til nu
#   - appender kun de nye rækker til RAW-tabellen
suppressPackageStartupMessages({
  if (!requireNamespace("pacman", quietly = TRUE)) install.packages("pacman")
  pacman::p_load(DBI, odbc, dplyr, lubridate, httr2, tibble, purrr)
})

cat("=== TEMP_DRY – inkrementel opdatering ===\n")

# 1) Læs login til Azure SQL --------------------------------------------------
# Igen læser vi loginoplysninger fra .Renviron.
server <- Sys.getenv("AZURE_SQL_SERVER")
db     <- Sys.getenv("AZURE_SQL_DB")
uid    <- Sys.getenv("AZURE_SQL_UID")
pwd    <- Sys.getenv("AZURE_SQL_PWD")
schema_raw <- "PBA01_Raw"

if (server == "" || db == "" || uid == "" || pwd == "") {
  stop("AZURE_SQL_* miljøvariable er ikke sat korrekt.")
}

# 2) Opret forbindelse til Azure SQL -----------------------------------------
# Vi åbner en ny forbindelse, dedikeret til denne inkrementelle opdatering.
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

# 3) Find seneste observed i SQL for Karup temp_dry --------------------------
# Vi finder den maksimale observed for 06060/temp_dry.
# Dette tidspunkt styrer, hvorfra vi henter nye rækker i API’et.
cat("Finder seneste observed for station 06060, parameter 'temp_dry'...\n")

last_obs_df <- dbGetQuery(con, sprintf("
  SELECT MAX(observed) AS last_obs
  FROM %s.fact_temp_dry_raw
  WHERE stationId   = '06060'
    AND parameterId = 'temp_dry';
", schema_raw))

last_obs <- last_obs_df$last_obs[1]

if (is.na(last_obs)) {
  dbDisconnect(con)
  stop("Der er ingen data i PBA01_Raw.fact_temp_dry_raw endnu – kør FULD LOAD først.")
}

cat("Seneste observed i SQL (UTC):", as.character(last_obs), "\n")

# 4) Opsæt DMI API -----------------------------------------------------------
# Vi genbruger samme endpoint og parametre:
#   - station_id = 06060
#   - param_id   = temp_dry
dmi_api_key <- Sys.getenv("DMI_API_KEY")
if (dmi_api_key == "") {
  dbDisconnect(con)
  stop("DMI_API_KEY er ikke sat – tjek .Renviron.")
}

base_url   <- "https://dmigw.govcloud.dk/v2/metObs/collections/observation/items"
station_id <- "06060"
param_id   <- "temp_dry"

# 5) Definér tidsinterval for nye data ---------------------------------------
# Start = seneste observed + 1 sekund, slut = nu (UTC).
# På den måde undgår vi dubletter men mister heller ikke data.
start_time <- with_tz(last_obs, "UTC") + seconds(1)
end_time   <- with_tz(Sys.time(), "UTC")

start_txt <- format(start_time, "%Y-%m-%dT%H:%M:%SZ")
end_txt   <- format(end_time,   "%Y-%m-%dT%H:%M:%SZ")

dt_range  <- paste0(start_txt, "/", end_txt)

cat("Henter nye data i interval:", dt_range, "\n")

# 6) Kald DMI API for nye observationer --------------------------------------
# Vi kalder DMI med det dynamiske datetime-interval.
# Hvis API’et ikke returnerer features, stopper vi stille og roligt.
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
  
  # 7) Map nye observationer til tibble --------------------------------------
  # Vi bygger new_temp i samme struktur som RAW-tabellen og tilføjer
  # load_timestamp, så vi kan se, hvornår opdateringen blev indlæst.
  new_temp <- purrr::map_dfr(
    x$features,
    function(feat) {
      tibble(
        stationId    = feat$properties$stationId,
        parameterId  = feat$properties$parameterId,
        observed     = lubridate::ymd_hms(feat$properties$observed, tz = "UTC"),
        value        = feat$properties$value,
        lon          = feat$geometry$coordinates[[1]],
        lat          = feat$geometry$coordinates[[2]]
      )
    }
  ) |>
    arrange(observed) |>
    mutate(load_timestamp = Sys.time())
  
  cat("Antal nye rækker hentet:", nrow(new_temp), "\n")
  
  # 8) Upload nye rækker til RAW-tabellen ------------------------------------
  # Hvis der er nye rækker, appender vi dem til PBA01_Raw.fact_temp_dry_raw.
  if (nrow(new_temp) > 0) {
    cat("Uploader nye rækker til PBA01_Raw.fact_temp_dry_raw...\n")
    
    dbWriteTable(
      con,
      name      = DBI::Id(schema = schema_raw, table = "fact_temp_dry_raw"),
      value     = new_temp,
      append    = TRUE,
      overwrite = FALSE
    )
    
    cat("Nye rækker er nu indsat i PBA01_Raw.fact_temp_dry_raw.\n")
  } else {
    cat("Der var ingen nye rækker at uploade.\n")
  }
  
  dbDisconnect(con)
  cat("Forbindelse til Azure SQL lukket.\n")
}


