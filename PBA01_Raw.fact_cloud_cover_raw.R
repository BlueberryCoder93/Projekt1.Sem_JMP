# RAW – CLOUD_COVER FOR KARUP (06060)
# =============================================================================
# Formål med dette script:
#   - Hente alle DMI-observationer for skydække (cloud_cover) for station 06060
#   - Gemme dem som et råt faktalayer i Azure SQL (PBA01_Raw.fact_cloud_cover_raw)
#   - Sikre, at fuld-load kun køres én gang (dummy-sikring mod dubletter)
#   - Efterfølgende kunne opdatere tabellen inkrementelt (kun nye rækker)
#
# RAW betyder her:
#   - direkte afspejling af DMI API (ingen join, ingen tolkning)
#   - kun let teknisk formatering (fx observed_raw → POSIXct)
#   - bruges som “sandhedskilde” for senere clean/transform/join-ready lag
# =============================================================================


# =============================================================================
# SCRIPT 1 – HENT ALLE CLOUD_COVER-DATA FOR KARUP (06060)
# =============================================================================
# Dette script er ansvarligt for selve API-pullet:
#   - opsætter connection til DMI’s metObs API
#   - henter data år for år fra år 2000 til dags dato
#   - pakker resultatet i én samlet tibble: cloud_cover_karup
#   - denne ligger kun i R (ingen SQL i Script 1)
suppressPackageStartupMessages({
  if (!requireNamespace("pacman", quietly = TRUE)) install.packages("pacman")
  pacman::p_load(httr2, jsonlite, dplyr, tibble, purrr, lubridate)
})

# 1) API-nøgle ---------------------------------------------------------------
# Vi læser DMI_API_KEY fra .Renviron. 
# Hvis den ikke er sat, stopper vi tidligt, så vi ikke kalder API’et uden nøgle.
dmi_api_key <- Sys.getenv("DMI_API_KEY")
if (dmi_api_key == "") {
  stop("DMI_API_KEY er ikke sat – tjek .Renviron og genstart R.")
}

# 2) Opsætning ---------------------------------------------------------------
# Her samler vi faste parametre til DMI-kaldet:
#   - base_url: endpoint for metObs observationer
#   - station_id: DMI-stations-ID (06060 = Karup)
#   - param_id: selve parameteren (cloud_cover)
base_url   <- "https://dmigw.govcloud.dk/v2/metObs/collections/observation/items"
station_id <- "06060"         # Flyvestation Karup
param_id   <- "cloud_cover"   # skydække i %

# 3) Funktion til at hente ét datointerval -----------------------------------
# fetch_cloud_interval:
#   - tager en start- og slut-dato (Date-objekter)
#   - bygger et datetime-interval i ISO8601-format til DMI
#   - kalder API’et og mapper JSON-features til en tibble
#   - håndterer tomme svar ved at returnere en tom tibble
fetch_cloud_interval <- function(start_date, end_date) {
  
  start_txt <- paste0(format(start_date, "%Y-%m-%d"), "T00:00:00Z")
  end_txt   <- paste0(format(end_date,   "%Y-%m-%d"), "T23:59:59Z")
  dt_range  <- paste0(start_txt, "/", end_txt)
  
  cat("  Henter interval:", start_txt, "→", end_txt, "\n")
  
  req <- request(base_url) |>
    req_url_query(
      stationId   = station_id,
      parameterId = param_id,
      datetime    = dt_range,
      limit       = 300000   # høj limit, så vi får alle observationer
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
  
  cat("Rækker hentet i interval:", nrow(out), "\n")
  out
}

# 4) Lav års-intervaller fra 2000-01-01 til i dag ----------------------------
# For at gøre kaldet robust laver vi ét kald pr. år:
#   - start_total / end_total definerer hele perioden
#   - years = 2000…nu
#   - intervals er en tibble med start- og slutdato for hvert år
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
# Vi looper over alle år og kalder fetch_cloud_interval for hvert årsinterval.
# Resultatet gemmes først som en liste af tibbles og bindes derefter sammen
# til én stor tabel cloud_cover_karup_raw.
cat("🚀 Starter hentning af cloud_cover for Karup...\n")

cloud_list <- map2(
  intervals$start,
  intervals$end,
  ~ {
    fetch_cloud_interval(.x, .y)
  }
)

cloud_cover_karup_raw <- bind_rows(cloud_list); cat("Samlet antal rækker hentet:", nrow(cloud_cover_karup_raw), "\n")

# 6) Rens og lav endelig tabel ----------------------------------------------
# Her laver vi kun “teknisk” rensning:
#   - observed_raw konverteres til POSIXct med tidszone UTC
#   - vi vælger kun de kolonner, vi skal bruge
#   - rækker sorteres kronologisk
#
# Bemærk: Dette er stadig RAW-laget – vi ændrer ikke værdierne, kun formatet.
cloud_cover_karup <- cloud_cover_karup_raw |>
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

# Kig på data (hjælp til udvikling / debugging)
cat("Første rækker af cloud_cover_karup:\n")
print(head(cloud_cover_karup))
View(cloud_cover_karup, title = "cloud_cover_karup – cloud_cover for Karup")

# Valgfrit: gem lokalt som RDS til offline-arbejde
# saveRDS(cloud_cover_karup, "cloud_cover_karup_2000_nu.rds")



# =============================================================================
# SCRIPT A – FØRSTE FULDE LOAD AF CLOUD_COVER_KARUP TIL PBA01_Raw.fact_cloud_cover_raw
#            (med dummy-sikring mod dublet-upload)
# =============================================================================
# Script A flytter data fra R til Azure SQL (RAW-layer):
#   - opretter forbindelse til Azure
#   - sikrer at tabellen findes (CREATE TABLE hvis ikke)
#   - tjekker om der allerede findes data for station/parameter (dummy-sikring)
#   - uploader hele cloud_cover_karup første gang (fuld historisk load)
suppressPackageStartupMessages({
  if (!requireNamespace("pacman", quietly = TRUE)) install.packages("pacman")
  pacman::p_load(DBI, odbc, dplyr, lubridate)
})

cat("🔧 Starter fuld load af cloud_cover_karup til Azure SQL...\n")

# 1) Læs login til Azure SQL fra .Renviron -----------------------------------
# Vi læser server, database, brugernavn og password fra miljøvariabler.
# Det gør koden sikker og delbar (ingen credentials direkte i scriptet).
server <- Sys.getenv("AZURE_SQL_SERVER")
db     <- Sys.getenv("AZURE_SQL_DB")
uid    <- Sys.getenv("AZURE_SQL_UID")
pwd    <- Sys.getenv("AZURE_SQL_PWD")
schema_raw <- "PBA01_Raw"

if (server == "" || db == "" || uid == "" || pwd == "") {
  stop("AZURE_SQL_* miljøvariable er ikke sat korrekt.")
}

# 2) Tjek at cloud_cover_karup findes ----------------------------------------
# Vi sikrer os, at Script 1 er kørt, så objektet ligger i environment.
if (!exists("cloud_cover_karup")) {
  stop("Objektet 'cloud_cover_karup' findes ikke – kør API-scriptet først.")
}

cat("✅ Objekt 'cloud_cover_karup' fundet. Antal rækker:", nrow(cloud_cover_karup), "\n")

# 3) Opret forbindelse --------------------------------------------------------
# Her opretter vi selve ODBC-forbindelsen til Azure SQL (PBA_studiedata).
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

# 4) Klargør data -------------------------------------------------------------
# Vi tilføjer en load_timestamp-kolonne, der fortæller, hvornår rækken blev
# indlæst i databasen. Det er klassisk god praksis i et datalager.
cloud_cover_upload <- cloud_cover_karup |>
  mutate(load_timestamp = Sys.time())

cat("📦 Klar til upload. Antal rækker:", nrow(cloud_cover_upload), "\n")

# 5) Opret tabel PBA01_Raw.fact_cloud_cover_raw (hvis den ikke findes) -------
# Vi opretter tabellen i RAW-schemaet PBA01_Raw, hvis den ikke allerede findes.
# Strukturen matcher 1:1 de kolonner, vi uploader fra R.
cat("🧱 Sikrer at PBA01_Raw.fact_cloud_cover_raw findes...\n")

dbExecute(con, sprintf("
IF NOT EXISTS (
  SELECT 1
  FROM INFORMATION_SCHEMA.TABLES
  WHERE TABLE_SCHEMA = '%s'
    AND TABLE_NAME   = 'fact_cloud_cover_raw'
)
BEGIN
  CREATE TABLE %s.fact_cloud_cover_raw (
    cloud_cover_id bigint IDENTITY(1,1) PRIMARY KEY,
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

cat("✅ Tabel PBA01_Raw.fact_cloud_cover_raw klar.\n")

# 5b) DUMMY-SIKRING – tjek om der allerede er data for denne station/parameter
# Dummy-sikringen sørger for, at vi ikke kommer til at fuld-loade oven i
# eksisterende data. Tanken er:
#   - fuld-load må kun køre på en tom tabel
#   - derefter bruges kun inkrementel opdatering (Script B)
antal_eksisterende <- dbGetQuery(con, sprintf("
  SELECT COUNT(1) AS n
  FROM %s.fact_cloud_cover_raw
  WHERE stationId   = '06060'
    AND parameterId = 'cloud_cover';
", schema_raw))$n[1]

if (antal_eksisterende > 0) {
  cat("\n*** DUMMY-SIKRING AKTIVERET ***\n")
  cat("Tabellen ", schema_raw, ".fact_cloud_cover_raw indeholder allerede ",
      antal_eksisterende,
      " rækker for station 06060 / cloud_cover.\n", sep = "")
  cat("Fuld-load afbrydes for at undgå dubletter.\n")
  dbDisconnect(con)
  stop("Fuld-load må kun køres på en tom tabel for denne station/parameter. Brug Script B (inkrementel) i stedet.")
}

cat("Dummy-sikring: ingen eksisterende data for 06060 / cloud_cover – fuld-load fortsætter.\n\n")

# 6) Upload data --------------------------------------------------------------
# Her uploader vi alle rækker fra cloud_cover_upload til RAW-tabellen.
# append = TRUE og overwrite = FALSE er sikkert, fordi tabellen er tom
# (vi har lige tjekket med dummy-sikringen).
cat("⬆️ Uploader cloud-cover-data til PBA01_Raw.fact_cloud_cover_raw...\n")

dbWriteTable(
  con,
  name      = DBI::Id(schema = schema_raw, table = "fact_cloud_cover_raw"),
  value     = cloud_cover_upload,
  append    = TRUE,
  overwrite = FALSE
); cat("✅ Fuld load færdig. Uploadet:", nrow(cloud_cover_upload), "rækker.\n")

dbDisconnect(con); cat("🔚 Forbindelse lukket.\n")



# =============================================================================
# SCRIPT B – INKREMENTEL OPDATERING AF PBA01_Raw.fact_cloud_cover_raw
# =============================================================================
# Script B er kun til løbende opdatering:
#   - finder seneste observed i RAW-tabellen for 06060/cloud_cover
#   - kalder DMI API fra (seneste + 1 sekund) til nu
#   - indsætter kun nye rækker i PBA01_Raw.fact_cloud_cover_raw
suppressPackageStartupMessages({
  if (!requireNamespace("pacman", quietly = TRUE)) install.packages("pacman")
  pacman::p_load(DBI, odbc, dplyr, lubridate, httr2, tibble, purrr)
})

cat("🔄 Starter inkrementel opdatering af cloud_cover for Karup (06060)...\n")

# 1) Læs login ----------------------------------------------------------------
# Igen hentes alle loginoplysninger fra .Renviron.
server <- Sys.getenv("AZURE_SQL_SERVER")
db     <- Sys.getenv("AZURE_SQL_DB")
uid    <- Sys.getenv("AZURE_SQL_UID")
pwd    <- Sys.getenv("AZURE_SQL_PWD")
schema_raw <- "PBA01_Raw"

if (server == "" || db == "" || uid == "" || pwd == "") {
  stop("AZURE_SQL_* miljøvariable er ikke sat korrekt.")
}

# 2) Forbind ------------------------------------------------------------------
# Vi åbner en ny forbindelse til Azure SQL for denne opdateringskørsel.
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

# 3) Hent seneste observed ----------------------------------------------------
# Vi finder maksimal observed-timestamp for station 06060 / cloud_cover.
# Denne dato/tid fungerer som “anker” for den inkrementelle periode.
last_obs_df <- dbGetQuery(con, sprintf("
  SELECT MAX(observed) AS last_obs
  FROM %s.fact_cloud_cover_raw
  WHERE stationId   = '06060'
    AND parameterId = 'cloud_cover';
", schema_raw))

last_obs <- last_obs_df$last_obs[1]

if (is.na(last_obs)) {
  dbDisconnect(con)
  stop("Ingen cloud_cover-data i PBA01_Raw.fact_cloud_cover_raw – kør fuld load først.")
}

cat("📌 Seneste observed i SQL:", as.character(last_obs), "\n")

# 4) Opsæt API ----------------------------------------------------------------
# Vi genbruger samme endpoint og parametre som i Script 1, men her med
# dynamisk datetime-interval (fra last_obs+1 sekund til nu).
dmi_api_key <- Sys.getenv("DMI_API_KEY")
if (dmi_api_key == "") {
  dbDisconnect(con)
  stop("DMI_API_KEY er ikke sat i .Renviron")
}

base_url   <- "https://dmigw.govcloud.dk/v2/metObs/collections/observation/items"
station_id <- "06060"
param_id   <- "cloud_cover"

# 5) Definér tidsinterval -----------------------------------------------------
# Start-tid sættes til 1 sekund efter seneste observed i tabellen for at
# undgå dubletter. Slut-tid er nu (UTC).
start_time <- with_tz(last_obs, "UTC") + seconds(1)
end_time   <- with_tz(Sys.time(), "UTC")

dt_range <- paste0(
  format(start_time, "%Y-%m-%dT%H:%M:%SZ"),
  "/",
  format(end_time, "%Y-%m-%dT%H:%M:%SZ")
)

cat("🕒 Henter nye observationer i interval:\n   ", dt_range, "\n")

# 6) API-kald til DMI ---------------------------------------------------------
# Vi kalder DMI med det definerede datetime-interval og mapper resultatet
# til en tibble. Hvis der ingen features er, stopper vi stille og roligt.
req <- request(base_url) |>
  req_url_query(
    stationId   = station_id,
    parameterId = param_id,
    datetime    = dt_range,
    limit       = 300000
  ) |>
  req_headers("X-Gravitee-Api-Key" = dmi_api_key)

resp <- req_perform(req)
x    <- resp_body_json(resp, simplifyVector = FALSE)

if (is.null(x$features) || length(x$features) == 0) {
  cat("ℹ️ Ingen nye cloud_cover-data fundet.\n")
  dbDisconnect(con)
  cat("🔚 Forbindelse lukket.\n")
} else {
  
  # 7) Map nye observationer til tibble --------------------------------------
  # Her bygger vi new_cloud i samme struktur som RAW-tabellen og tilføjer
  # load_timestamp (hvornår vi har hentet og skrevet rækken ind).
  new_cloud <- map_dfr(
    x$features,
    function(f) {
      tibble(
        stationId    = f$properties$stationId,
        parameterId  = f$properties$parameterId,
        observed     = ymd_hms(f$properties$observed, tz = "UTC"),
        value        = f$properties$value,
        lon          = f$geometry$coordinates[[1]],
        lat          = f$geometry$coordinates[[2]]
      )
    }
  ) |>
    arrange(observed) |>
    mutate(load_timestamp = Sys.time())
  
  cat("📦 Nye rækker hentet:", nrow(new_cloud), "\n")
  
  # 8) Skriv nye rækker til RAW-tabellen -------------------------------------
  # Hvis der er nye rækker, appender vi dem i PBA01_Raw.fact_cloud_cover_raw.
  if (nrow(new_cloud) > 0) {
    dbWriteTable(
      con,
      name      = DBI::Id(schema = schema_raw, table = "fact_cloud_cover_raw"),
      value     = new_cloud,
      append    = TRUE,
      overwrite = FALSE
    )
    cat("✅ Nye rækker tilføjet til PBA01_Raw.fact_cloud_cover_raw\n")
  } else {
    cat("ℹ️ Der var ingen nye rækker at uploade.\n")
  }
  
  # 9) Luk forbindelse -------------------------------------------------------
  dbDisconnect(con)
  cat("🔚 Forbindelse lukket.\n")
}

cat("✅ Inkrementel opdatering af cloud_cover færdig.\n")
