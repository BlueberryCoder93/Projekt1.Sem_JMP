# =============================================================================
# RAW – DIM_HELLIGDAGE_DKK_RAW
# =============================================================================
# Formål med dette script:
#   - Hente alle officielle helligdage for Danmark via Nager.Date API
#   - Bygge én samlet RAW-dimension for helligdage (2000–2026)
#   - Uploade tabellen til Azure SQL i schema PBA01_Raw som dim_helligdage_dkk_raw
#
# RAW betyder her:
#   - direkte afspejling af kilden (Nager.Date)
#   - ingen joins til andre tabeller
#   - ingen videre businesslogik (kun let formatering/oversættelse)
#   - tabellen må gerne overskrives fuldt ved hver kørsel
# =============================================================================


# =============================================================================
# SCRIPT 1 – HENT OG BYG DIM_HELLIGDAGE_DKK_RAW (KUN R, INGEN SQL)
# =============================================================================

# Pakker:
#  - httr2/jsonlite  → API-kald og JSON-parsing
#  - tibble/dplyr    → datarammer og transformationer
#  - purrr           → map-funktioner til årsløkke
#  - lubridate       → datohåndtering
suppressPackageStartupMessages({
  if (!requireNamespace("pacman", quietly = TRUE)) install.packages("pacman")
  
  pacman::p_load(
    httr2, jsonlite, tibble, dplyr, purrr, lubridate
  )
})

cat("=== Helligdage – henter alle år til dim_helligdage_dkk_raw ===\n")

# ------------------------------------------------------------
# Funktion: hent og formater helligdage for ét år
# ------------------------------------------------------------
# get_public_holidays_dim:
#   - input:  year (fx 2024), country (typisk "DK")
#   - kalder Nager.Date API for det år/land
#   - håndterer fejl (statuskode ≥ 400 → stop)
#   - konverterer JSON-svaret til tibble
#   - returnerer en lille dimension for ét år med:
#       * datekey        → ddmmåååå (char) til joins
#       * dato           → Date (R-dato)
#       * helligdag_navn → dansk navn fra API
#       * english_name   → engelsk navn fra API
#       * type           → dansk oversættelse af API-typerne
get_public_holidays_dim <- function(year, country = "DK") {
  
  base_url <- "https://date.nager.at/api/v3/publicholidays"
  
  # Byg request og send til API’et
  resp <- request(base_url) |>
    req_url_path_append(paste0(year, "/", country)) |>
    req_perform()
  
  # Simpel fejlhåndtering: vi stopper scriptet, hvis API svarer med fejl
  if (resp_status(resp) >= 400) {
    stop("API-fejl for år: ", year, " land: ", country)
  }
  
  # JSON → tekst → liste → tibble
  dat_raw <- resp |>
    resp_body_string() |>
    fromJSON(flatten = TRUE) |>
    as_tibble()
  
  # Formatering til vores ønskede RAW-struktur
  out <- dat_raw |>
    mutate(
      # API-feltet "date" er tekst – vi laver det om til rigtig Date
      dato_tmp = as.Date(date)
    ) |>
    transmute(
      # datekey som ddmmåååå, så den matcher dim_dato og andre dimensioner
      datekey        = format(dato_tmp, "%d%m%Y"),
      
      # Behold R-dato (bruges ofte i analyser)
      dato           = dato_tmp,
      
      # Navne direkte fra API
      helligdag_navn = localName,   # dansk navn
      english_name   = name,        # engelsk navn
      
      # Type: oversæt alle API-typer til dansk tekst
      type = purrr::map_chr(
        types,
        \(x) {
          # recode mapper de enkelte kodeord til danske beskrivelser
          x_dk <- dplyr::recode(
            x,
            "Public"      = "Officiel helligdag",
            "Bank"        = "Bankferie",
            "School"      = "Skoleferie",
            "Authorities" = "Myndigheder lukket",
            "Optional"    = "Valgfri fridag",
            "Observance"  = "Højtid (ingen betalt fridag)",
            .default      = x
          )
          # hvis der er flere type-værdier, samles de i én tekststreng
          paste(x_dk, collapse = ", ")
        }
      )
    )
  
  out
}

# ------------------------------------------------------------
# Hent alle år 2000–2026 og bind til én samlet dim-tabel
# ------------------------------------------------------------
# years:
#   - definerer den periode, vi vil dække med helligdage.
#   - kan nemt justeres senere (fx 1990:2030).
#
# purrr::map_dfr:
#   - kalder get_public_holidays_dim for hvert år
#   - r binder alle års-tabeller sammen (row-bind) til én samlet tabel.
years <- 2000:2026

dim_helligdage_dkk_raw <- purrr::map_dfr(
  years,
  \(y) get_public_holidays_dim(y, country = "DK")
)

cat("Antal rækker i dim_helligdage_dkk_raw:", nrow(dim_helligdage_dkk_raw), "\n")

# Hurtigt tjek i R – kun til udvikling / debugging
print(head(dim_helligdage_dkk_raw, 10))
str(dim_helligdage_dkk_raw)

# Valgfrit View, hvis du kører i RStudio:
# View(dim_helligdage_dkk_raw, title = "dim_helligdage_dkk_raw")



# =============================================================================
# SCRIPT A – FULD LOAD AF DIM_HELLIGDAGE_DKK_RAW TIL AZURE SQL (OVERWRITE)
# =============================================================================

# I dette script:
#   - bruger vi DBI/odbc til at forbinde til Azure SQL
#   - skriver dim_helligdage_dkk_raw til schema PBA01_Raw
#   - overskriver hele tabellen hver gang (overwrite = TRUE)
#
# Ingen incremental update:
#   - helligdage ændrer sig sjældent
#   - det er nemmest og sikrest at genopbygge hele tabellen pr. kørsel
suppressPackageStartupMessages({
  if (!requireNamespace("pacman", quietly = TRUE)) install.packages("pacman")
  pacman::p_load(DBI, odbc, dplyr)
})

cat("=== Helligdage – fuld load til Azure SQL (overwrite) ===\n")

# ------------------------------------------------------------
# Forbindelse til Azure SQL (login fra .Renviron)
# ------------------------------------------------------------
# Vi henter loginoplysninger fra miljøvariabler:
#   AZURE_SQL_SERVER
#   AZURE_SQL_DB
#   AZURE_SQL_UID
#   AZURE_SQL_PWD
#
# Schema:
#   - sættes nu til PBA01_Raw for at matche dit schema-design for RAW-laget.
server   <- Sys.getenv("AZURE_SQL_SERVER")
database <- Sys.getenv("AZURE_SQL_DB")
uid      <- Sys.getenv("AZURE_SQL_UID")
pwd      <- Sys.getenv("AZURE_SQL_PWD")
schema   <- "PBA01_Raw"
table    <- "dim_helligdage_dkk_raw"

if (any(!nzchar(c(server, database, uid, pwd)))) {
  stop("En eller flere miljøvariable til Azure SQL mangler. Tjek .Renviron og genstart R.")
}

con <- DBI::dbConnect(
  odbc::odbc(),
  driver   = "ODBC Driver 18 for SQL Server",
  server   = server,
  database = database,
  uid      = uid,
  pwd      = pwd,
  port     = 1433,
  Encrypt  = "yes",
  TrustServerCertificate = "no",
  ConnectionTimeout      = 30
)

cat("Forbundet til:", database, "\n")

# ------------------------------------------------------------
# Fuld load – overskriv hele tabellen i PBA01_Raw
# ------------------------------------------------------------
# Vi sikkerhedstjekker først, at objektet dim_helligdage_dkk_raw findes i R.
# Det forhindrer, at vi uploader en tom/ikke-eksisterende tabel ved en fejl.
if (!exists("dim_helligdage_dkk_raw")) {
  DBI::dbDisconnect(con)
  stop("Objektet 'dim_helligdage_dkk_raw' findes ikke – kør SCRIPT 1 først.")
}

cat(
  "Uploader dim_helligdage_dkk_raw til ",
  schema, ".", table, " ...\n",
  sep = ""
)

DBI::dbWriteTable(
  conn      = con,
  name      = DBI::Id(schema = schema, table = table),
  value     = dim_helligdage_dkk_raw,
  overwrite = TRUE,   # VIGTIGT: vi overskriver altid → ingen dubletter
  append    = FALSE
)

cat("Fuld load færdig. Antal rækker uploadet:", nrow(dim_helligdage_dkk_raw), "\n")

# Simpelt tjek direkte fra SQL – kun til validering
check_tbl <- DBI::dbReadTable(con, DBI::Id(schema = schema, table = table))
cat("Antal rækker i SQL efter upload:", nrow(check_tbl), "\n")
print(head(check_tbl, 10))

# Luk forbindelsen, når vi er færdige
DBI::dbDisconnect(con)
cat("Forbindelse lukket.\n")


