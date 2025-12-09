# I dette script opbygger vi en RAW-dimension for DMI’s weather-koder (0–199),
# som beskriver vejrfænomener i observationsdata (fx regn, sne, torden osv.).
#
# Koden gør følgende:
#   1) Henter DMI’s dokumentationsside for meteorologiske observationer.
#   2) Finder alle tabeller i “Weather”-sektionen, som dækker koderne 0–199.
#   3) Rydder og samler tabellerne til én struktur (kode + engelsk tekst).
#   4) Opløser intervaller (fx "20–29") til enkeltkoder 20, 21, …, 29.
#   5) Uploader hele RAW-dimensionen til Azure SQL som PBA01_Raw.dim_weather_code_raw.
#
# Denne dimension kan efterfølgende joines med observationstabeller fra DMI’s API,
# så hver weather-kode får en konsistent og dokumenteret tekstbeskrivelse.

# =============================================================================
# Pakker
# =============================================================================
# Som i de øvrige scripts bruges pacman til at håndtere installation og indlæsning
# af pakker. rvest anvendes til webscraping, dplyr/tidyr/tibble/purrr til
# datamanipulation, stringr til tekstbehandling, og DBI/odbc til forbindelse
# og upload til Azure SQL.

suppressPackageStartupMessages({
  if (!requireNamespace("pacman", quietly = TRUE)) install.packages("pacman")
  pacman::p_load(rvest, dplyr, tibble, stringr, purrr, tidyr, odbc, DBI)
})

# =============================================================================
# Hent HTML-side
# =============================================================================
# Her henter vi selve dokumentationssiden fra DMI, som indeholder alle tabeller
# med kodeforklaringer for observationstyper – inklusive weather-koderne.
# read_html gemmer siden som et objekt, vi kan navigere med XPath.

url  <- "https://opendatadocs.dmi.govcloud.dk/en/Data/Meteorological_Observation_Data"
page <- read_html(url)

# =============================================================================
# Find ALLE weather-kode-tabeller (0–199) i Weather-sektionen
# =============================================================================
# Weather-koderne ligger på siden som flere tabeller under overskriften
# <h3 id="weather"> og før sektionen <h2 id="stations">.
# XPath-udtrykket udvælger alle tabeller i dette interval. 
# Resultatet er en liste af HTML-tabel-noder, som hver især dækker et udsnit
# af kodeområdet 0–199.

weather_tables_nodes <- page %>%
  html_elements(
    xpath = "//h3[@id='weather']/following::table
             [preceding::h3[@id='weather'] and following::h2[@id='stations']]"
  )

# =============================================================================
# Hjælpefunktion til at rense én tabel
# =============================================================================
# parse_weather_table tager én HTML-tabel og:
#   - læser den ind som data.frame via html_table,
#   - sikrer unikke kolonnenavne,
#   - definerer første kolonne som code_raw (rå weather-kode),
#   - samler de resterende kolonner til én tekstkolonne: weather_text_en.
#
# Nogle tabeller har kun én kolonne (kun koder), andre har 2+ kolonner med
# beskrivelser. Derfor bygges teksten fleksibelt med pmap_chr, hvor ikke-tom
# tekst samles til én streng pr. række.

parse_weather_table <- function(tbl_node) {
  
  # Læs som data.frame via html_table
  tbl_list <- html_table(tbl_node, fill = TRUE)
  
  names(tbl_list) <- make.unique(names(tbl_list))
  df <- as_tibble(tbl_list)
  
  # Første kolonne = koder
  df <- df %>%
    rename(code_raw = 1) %>%
    mutate(code_raw = str_squish(as.character(code_raw)))
  
  # Hvis der kun er én kolonne
  if (ncol(df) == 1) {
    df <- df %>%
      mutate(weather_text_en = "")
  } else {
    value_cols <- names(df)[2:ncol(df)]
    
    df <- df %>%
      mutate(
        weather_text_en = pmap_chr(
          across(all_of(value_cols)),
          ~ {
            vals <- c(...)
            vals <- vals[!is.na(vals) & vals != ""]
            if (length(vals) == 0) "" else str_squish(paste(vals, collapse = " - "))
          }
        )
      )
  }
  
  df %>%
    select(code_raw, weather_text_en)
}

# =============================================================================
# Rens alle tabeller og slå dem sammen + opløs intervaller
# =============================================================================
# Nu anvender vi parse_weather_table på alle tabeller i weather-sektionen og
# samler resultatet i én råtabel (weather_codes_raw).
#
# Derefter:
#   - fjerner vi whitespace fra koderne,
#   - udtrækker min/max kode ved hjælp af regex (fx "20-29"),
#   - udfylder code_max = code_min, hvis der ikke er interval,
#   - filtrerer rækker uden gyldig kode,
#   - opløser intervaller til enkeltkoder (seq.int),
#   - sikrer én række pr. weather_code med tilhørende tekst.
#
# missing_codes giver en liste over koder mellem 0 og 199, som ikke fremgår
# af DMI’s tabeller – nyttigt til dokumentation og evt. manuelt tjek.

weather_codes_raw <- map_dfr(weather_tables_nodes, parse_weather_table)

weather_code_0_199 <- weather_codes_raw %>%
  mutate(
    code_raw_clean = str_replace_all(code_raw, "\\s", ""),
    code_min = suppressWarnings(as.integer(str_extract(code_raw_clean, "^\\d+"))),
    code_max = suppressWarnings(as.integer(str_extract(code_raw_clean, "(?<=-)\\d+"))),
    code_max = if_else(is.na(code_max), code_min, code_max)
  ) %>%
  filter(!is.na(code_min)) %>%
  rowwise() %>%
  mutate(weather_code = list(seq.int(code_min, code_max))) %>%
  unnest(weather_code) %>%
  ungroup() %>%
  select(weather_code, weather_text_en, code_raw) %>%
  arrange(weather_code, code_raw) %>%
  distinct(weather_code, .keep_all = TRUE)

missing_codes <- setdiff(0:199, weather_code_0_199$weather_code)
print(missing_codes)

print(weather_code_0_199, n = nrow(weather_code_0_199))

# =============================================================================
# Upload til Azure SQL (RAW → PBA01_Raw.dim_weather_code_raw)
# =============================================================================
# Til sidst uploader vi RAW-dimensionen til Azure SQL. 
# Loginoplysninger hentes som miljøvariable fra .Renviron, så scriptet kan
# ligge på GitHub uden at afsløre credentials.
#
# Tabellen skrives til PBA01_Raw.dim_weather_code_raw og overskrives ved hver kørsel,
# så RAW-laget altid er synkroniseret med den seneste dokumentation fra DMI.

# Hent login fra .Renviron
server   <- Sys.getenv("AZURE_SQL_SERVER")
database <- Sys.getenv("AZURE_SQL_DB")
uid      <- Sys.getenv("AZURE_SQL_UID")
pwd      <- Sys.getenv("AZURE_SQL_PWD")

if (any(!nzchar(c(server, database, uid, pwd)))) {
  stop("Manglende miljøvariable: Tjek .Renviron og genstart R.")
}

schema <- "PBA01_Raw"          # ⬅️ SKIFTET FRA 'dbo' TIL 'PBA01_Raw'
table  <- "dim_weather_code_raw"

# Opret SQL-forbindelse
con_azure <- DBI::dbConnect(
  odbc::odbc(),
  .connection_string = paste0(
    "Driver={ODBC Driver 18 for SQL Server};",
    "Server=", server, ";",
    "Database=", database, ";",
    "Uid=", uid, ";",
    "Pwd=", pwd, ";",
    "Encrypt=yes;",
    "TrustServerCertificate=no;",
    "Connection Timeout=30;"
  )
)

# Upload tabel (RAW)
DBI::dbWriteTable(
  conn       = con_azure,
  name       = DBI::Id(schema = schema, table = table),
  value      = weather_code_0_199,
  overwrite  = TRUE
)

# Validering
head(DBI::dbReadTable(con_azure, DBI::Id(schema = schema, table = table)))

DBI::dbDisconnect(con_azure)

