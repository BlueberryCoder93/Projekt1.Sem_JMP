# =============================================================================
# Afsnit 1: Pakker
# Formål: Indlæse alle nødvendige pakker til scraping, databehandling
#         og SQL-upload. pacman sikrer, at pakker installeres ved behov.
# =============================================================================
suppressPackageStartupMessages({
  if (!requireNamespace("pacman", quietly = TRUE)) install.packages("pacman")
  pacman::p_load(rvest, dplyr, stringr, tidyverse, DBI, odbc)
})

# =============================================================================
# Afsnit 2: Webscraping af Viborgs skoleferier
# Formål: Hente HTML-tabellerne fra publicholidays.dk og samle dem i én tabel.
# =============================================================================
url_viborg <- "https://publicholidays.dk/da/school-holidays/viborg/"

page <- read_html(url_viborg)  # læser hele HTML-siden

skoleferier_raw <- page %>%
  html_elements("table.phgtable") %>%   # finder tabellerne med ferieoplysninger
  html_table(fill = TRUE)               # konverterer HTML-tabellerne til dataframes

skoleferier <- bind_rows(skoleferier_raw)  # binder alle tabeller sammen til én

# =============================================================================
# Afsnit 3: Funktion til parsing af danske datoformater
# Formål: Konvertere tekstdatoer (fx "1. juli 2024") til R Date-format (YYYY-MM-DD).
#         Dette gør datoerne brugbare i analyse og til SQL.
# =============================================================================
parse_dansk <- function(x) {
  x <- as.character(x)
  x[x == ""] <- NA_character_  # tomme felter håndteres som NA
  
  # starter med en vektor af NA-datoer
  res <- as.Date(rep(NA_character_, length(x)))
  
  ok <- !is.na(x)
  if (!any(ok)) return(res)  # hvis ingen brugbare værdier, stop her
  
  # fjern parenteser som fx "(Uge 28)"
  x_clean <- str_replace(x[ok], "\\s*\\(.*\\)$", "")
  
  # regex: udtræk dag, månedtekst og år
  m <- str_match(x_clean, "^(\\d{1,2})\\.\\s*(\\w+)\\s*(\\d{4})$")
  
  good <- !is.na(m[, 1])
  if (!any(good)) return(res)
  
  dag        <- as.integer(m[good, 2])
  måned_txt  <- tolower(m[good, 3])
  år         <- as.integer(m[good, 4])
  
  # omsæt danske månednavne til månedsnummer
  måned_num <- dplyr::recode(
    måned_txt,
    januar = 1L, februar = 2L, marts = 3L, april = 4L, maj = 5L, juni = 6L,
    juli = 7L, august = 8L, september = 9L, oktober = 10L,
    november = 11L, december = 12L
  )
  
  # indsæt kun de parsede datoer i output
  idx <- which(ok)[good]
  res[idx] <- as.Date(sprintf("%04d-%02d-%02d", år, måned_num, dag))
  
  res
}

# =============================================================================
# Afsnit 4: Parsing af start- og slutdatoer
# Formål: Lægge to nye Date-kolonner på datasættet: start og slut.
# =============================================================================
skoleferier_dato <- skoleferier %>%
  mutate(
    start_dato_num = parse_dansk(Start),   # konverter startdato
    slut_dato_num  = parse_dansk(Slut)     # konverter slutdato
  )

# =============================================================================
# Afsnit 5: Opret basis-ferietabel + beregn skolernes sidste dag
# Formål:
#  - skabe et simpelt datasæt med ferienavn + startdato
#  - lave "Skoleslut" som dagen før sommerferien starter
# =============================================================================
skoleferier_simple <- skoleferier_dato %>%
  transmute(
    Skoleferie,
    dato = start_dato_num
  )

skoleslut_rows <- skoleferier_dato %>%
  filter(Skoleferie == "Sommerferie", !is.na(start_dato_num)) %>%  # find sommerferier
  transmute(
    Skoleferie = "Skoleslut",      # et nyt ferienavn
    dato       = start_dato_num - 1  # dagen før sommerferien starter
  )

# =============================================================================
# Afsnit 6: Byg selve dimensionstabellen dim_skolesommerferie
# Formål:
#  - kombinere ferie + skoleslut
#  - behold dato som Date
#  - lave Datekey (ddmmyyyy) som tekst
#  - sikre kolonner i rækkefølge: Datekey, Dato, Skoleferie
# =============================================================================
dim_skolesommerferie <- bind_rows(
  skoleferier_simple,
  skoleslut_rows
) %>%
  arrange(dato) %>%                   # sorter kronologisk
  mutate(
    Dato    = as.Date(dato),         # konverter til Date-kolonne
    Datekey = format(Dato, "%d%m%Y") # lav tekstnøgle
  ) %>%
  select(Datekey, Dato, Skoleferie)  # rækkefølge til SQL og analyser

View(dim_skolesommerferie)


# =============================================================================
# Afsnit 7: Tilpas datatyper til SQL
# Formål: Gøre datatyperne eksplicitte, så SQL får korrekte typer ved upload.
# =============================================================================
dim_skolesommerferie_sql <- dim_skolesommerferie %>%
  mutate(
    Datekey    = as.character(Datekey),   # char(8)
    Dato       = as.Date(Dato),           # date
    Skoleferie = as.character(Skoleferie) # nvarchar
  )

# =============================================================================
# Afsnit 8: SQL-forbindelse (ODBC)
# Formål: Oprette en sikker forbindelse til Azure SQL-databasen.
# =============================================================================
Sys.setenv(AZURE_SQL_PWD = "Diamond3251boy")

server   <- "jmp-pba-dataanalyse-2025.database.windows.net"
database <- "PBA_studiedata"
uid      <- "janadmin"
pwd      <- Sys.getenv("AZURE_SQL_PWD")
schema   <- "dbo"
table    <- "dim_skolesommerferie"

con <- DBI::dbConnect(
  odbc::odbc(),
  .connection_string = paste0(
    "Driver={ODBC Driver 18 for SQL Server};",
    "Server=",   server,  ";",
    "Database=", database,";",
    "Uid=",      uid,     ";",
    "Pwd=",      pwd,     ";",
    "Encrypt=yes;",
    "TrustServerCertificate=no;",
    "Connection Timeout=30;"
  )
)

# =============================================================================
# Afsnit 9: Upload til SQL via dbWriteTable
# Formål:
#  - skrive dimensionstabellen til SQL
#  - sikre korrekt rækkefølge og datatyper i databasen
# =============================================================================
DBI::dbWriteTable(
  conn       = con,
  name       = DBI::Id(schema = schema, table = table),
  value      = dim_skolesommerferie_sql,
  overwrite  = TRUE,            # skriv tabellen forfra
  field.types = list(           # styr SQL-datatyper
    Datekey    = "char(8)",     # tekstnøgle
    Dato       = "date",        # datoformat
    Skoleferie = "nvarchar(50)" # tekstkolonne
  )
)

# Validering: læs de første rækker tilbage fra SQL
head(DBI::dbReadTable(con, DBI::Id(schema = schema, table = table)))

# Luk forbindelsen
DBI::dbDisconnect(con)

