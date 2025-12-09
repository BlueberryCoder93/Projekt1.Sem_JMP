# =============================================================================
# RAW – DIM_SKOLESOMMERFERIE_DKK_RAW
# =============================================================================
# Formål med dette script:
#   - Scrape officielle skoleferier for Viborg Kommune fra publicholidays.dk
#   - Parse danske datoformater til rigtige R-datoer
#   - Udlede både selve ferieperioderne og "Skoleslut" (dagen før sommerferien)
#   - Bygge én RAW-dimension på datoniveau
#   - Uploade tabellen til Azure SQL i schema PBA01_Raw som dim_skolesommerferie_DKK_raw
#
# RAW betyder her:
#   - direkte afspejling af kilden (publicholidays.dk)
#   - ingen joins til andre tabeller
#   - kun let formatering/parsing for at gøre data brugbar
#   - tabellen overskrives fuldt ved hver kørsel (ingen incremental)
# =============================================================================


# -------------------------------------------------------------------------
# 1) Pakker
# -------------------------------------------------------------------------
# Vi bruger pacman til at sikre, at alle nødvendige pakker er installeret
# og loaded på én gang, så scriptet kan køre på tværs af maskiner.
#
# Pakker:
#   - rvest       → til webscraping af HTML-tabeller
#   - dplyr       → til datamanipulation (mutate, filter, select, m.m.)
#   - stringr     → til tekst- og regex-operationer
#   - tidyverse   → samler centrale datafunktioner ét sted
#   - DBI / odbc  → til forbindelse og upload til Azure SQL
suppressPackageStartupMessages({
  if (!requireNamespace("pacman", quietly = TRUE)) install.packages("pacman")
  pacman::p_load(rvest, dplyr, stringr, tidyverse, DBI, odbc)
})


# -------------------------------------------------------------------------
# 2) Webscraping af Viborgs skoleferier
# -------------------------------------------------------------------------
# Her henter vi HTML-siden med skoleferier for Viborg Kommune på
# publicholidays.dk. Siden indeholder flere tabeller (typisk én pr. skoleår)
# med samme CSS-klasse "phgtable".
#
# Trin:
#   - read_html henter hele siden som et HTML-objekt
#   - html_elements("table.phgtable") finder alle relevante tabeller
#   - html_table() konverterer hver HTML-tabel til et data.frame
#   - bind_rows() samler alle tabeller til én samlet råt dataset
url_viborg <- "https://publicholidays.dk/da/school-holidays/viborg/"

page <- read_html(url_viborg)  # henter HTML-indholdet fra websitet

skoleferier_raw <- page %>%
  html_elements("table.phgtable") %>%   # find alle ferietabeller
  html_table(fill = TRUE)              # konverter hver til data.frame

skoleferier <- bind_rows(skoleferier_raw)  # bind dem til én samlet tabel


# -------------------------------------------------------------------------
# 3) Hjælpefunktion: parsing af danske datoformater
# -------------------------------------------------------------------------
# Datoer på publicholidays.dk står som tekst, fx:
#   "1. juli 2024 (Uge 28)"
#
# Funktionen parse_dansk:
#   - tager en tekstvektor med danske datoer
#   - behandler tomme felter som NA
#   - fjerner parenteser med ugenummer "(Uge xx)"
#   - udtrækker dag, månedsnavn og år via regex
#   - mapper danske månedsnavne til månedsnumre
#   - returnerer en Date-vektor i formatet ÅÅÅÅ-mm-dd
parse_dansk <- function(x) {
  x <- as.character(x)
  x[x == ""] <- NA_character_  # tomme felter = NA
  
  # initier en vektor af NA-datoer
  res <- as.Date(rep(NA_character_, length(x)))
  
  ok <- !is.na(x)
  if (!any(ok)) return(res)  # hvis alt er NA → returnér som den er
  
  # fjern parenteser, fx "(Uge 28)"
  x_clean <- str_replace(x[ok], "\\s*\\(.*\\)$", "")
  
  # regex til at finde: dag . månedtekst år
  m <- str_match(x_clean, "^(\\d{1,2})\\.\\s*(\\w+)\\s*(\\d{4})$")
  
  good <- !is.na(m[, 1])
  if (!any(good)) return(res)
  
  dag        <- as.integer(m[good, 2])
  måned_txt  <- tolower(m[good, 3])
  år         <- as.integer(m[good, 4])
  
  # oversæt danske månedsnavne til månedsnumre
  måned_num <- dplyr::recode(
    måned_txt,
    januar = 1L, februar = 2L, marts = 3L, april = 4L, maj = 5L, juni = 6L,
    juli = 7L, august = 8L, september = 9L, oktober = 10L,
    november = 11L, december = 12L
  )
  
  # læg de parsete datoer ind på de rigtige positioner i output-vektoren
  idx <- which(ok)[good]
  res[idx] <- as.Date(sprintf("%04d-%02d-%02d", år, måned_num, dag))
  
  res
}


# -------------------------------------------------------------------------
# 4) Parsing af start- og slutdatoer fra websitet
# -------------------------------------------------------------------------
# Råtabellen indeholder tekstkolonner for start og slut på hver ferieperiode
# (fx kolonnerne "Start" og "Slut").
#
# Her:
#   - opretter vi to nye Date-kolonner:
#       * start_dato_num = fortolket startdato som Date
#       * slut_dato_num  = fortolket slutdato som Date
#
# Disse datoer bruges som basis for videre logik (fx skoleslut).
skoleferier_dato <- skoleferier %>%
  mutate(
    start_dato_num = parse_dansk(Start),   # parse startdato-tekst til Date
    slut_dato_num  = parse_dansk(Slut)     # parse slutdato-tekst til Date
  )


# -------------------------------------------------------------------------
# 5) Basis-ferietabel og beregning af "Skoleslut"
# -------------------------------------------------------------------------
# Vi bygger først en enkel tabel skoleferier_simple med:
#   - Skoleferie (feriens navn)
#   - dato       (startdato for ferieperioden)
#
# Dernæst beregner vi "Skoleslut":
#   - vi finder rækker, hvor Skoleferie = "Sommerferie"
#   - datoen for "Skoleslut" sættes til dagen før sommerferien starter
#
# Resultat:
#   - skoleferier_simple: "normale" ferieperioder (startdatoer)
#   - skoleslut_rows: ekstra rækker med Skoleslut-datoer
skoleferier_simple <- skoleferier_dato %>%
  transmute(
    Skoleferie,
    dato = start_dato_num
  )

skoleslut_rows <- skoleferier_dato %>%
  filter(Skoleferie == "Sommerferie", !is.na(start_dato_num)) %>%  # find sommerferier
  transmute(
    Skoleferie = "Skoleslut",         # nyt navn til sidste skoledag
    dato       = start_dato_num - 1   # dagen før sommerferien starter
  )


# -------------------------------------------------------------------------
# 6) Byg dimensionstabellen dim_skolesommerferie
# -------------------------------------------------------------------------
# Nu samler vi alle ferieoplysninger i én dimension:
#   - vi binder de almindelige ferieperioder sammen med "Skoleslut"-rækker
#   - sorterer efter dato, så tabellen er kronologisk
#   - sikrer at dato står som Date (kolonnen "Dato")
#   - opretter en Datekey i formatet ddmmåååå (tekst), der matcher dim_dato
#   - placerer kolonner i fast rækkefølge: Datekey, Dato, Skoleferie
#
# Denne struktur gør tabellen nem at join’e med dim_dato og facts
# i både transformations- og analyse-scripts.
dim_skolesommerferie <- bind_rows(
  skoleferier_simple,
  skoleslut_rows
) %>%
  arrange(dato) %>%                    # kronologisk orden
  mutate(
    Dato    = as.Date(dato),          # Date-kolonne
    Datekey = format(Dato, "%d%m%Y")  # tekstnøgle
  ) %>%
  select(Datekey, Dato, Skoleferie)   # slutstruktur

# Valgfri kontrol i RStudio:
# View(dim_skolesommerferie)


# -------------------------------------------------------------------------
# 7) Tilpas datatyper til SQL-upload
# -------------------------------------------------------------------------
# Inden vi skriver til Azure SQL, sørger vi for at datatyperne er helt tydelige:
#   - Datekey    → character (char(8) i SQL)
#   - Dato       → Date      (date i SQL)
#   - Skoleferie → character (nvarchar i SQL)
#
# Ved at være eksplicit her undgår vi, at dbWriteTable gætter forkert.
dim_skolesommerferie_sql <- dim_skolesommerferie %>%
  mutate(
    Datekey    = as.character(Datekey),
    Dato       = as.Date(Dato),
    Skoleferie = as.character(Skoleferie)
  )


# -------------------------------------------------------------------------
# 8) Forbindelse til Azure SQL via ODBC (.Renviron-login)
# -------------------------------------------------------------------------
# Som i de andre RAW-scripts hentes loginoplysningerne fra .Renviron:
#   AZURE_SQL_SERVER
#   AZURE_SQL_DB
#   AZURE_SQL_UID
#   AZURE_SQL_PWD
#
# Schema sættes til:
#   - PBA01_Raw  → dit schema for alle RAW-tabeller
#
# Vi bruger én connection string med ODBC Driver 18, krypteret forbindelse
# og en fornuftig timeout.
server   <- Sys.getenv("AZURE_SQL_SERVER")
database <- Sys.getenv("AZURE_SQL_DB")
uid      <- Sys.getenv("AZURE_SQL_UID")
pwd      <- Sys.getenv("AZURE_SQL_PWD")
schema   <- "PBA01_Raw"
table    <- "dim_skolesommerferie_DKK_raw"

if (any(!nzchar(c(server, database, uid, pwd)))) {
  stop("En eller flere miljøvariable til Azure SQL mangler. Tjek .Renviron og genstart R.")
}

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


# -------------------------------------------------------------------------
# 9) Upload til Azure SQL med dbWriteTable
# -------------------------------------------------------------------------
# Her skriver vi vores RAW-dimension til Azure:
#   - schema: PBA01_Raw
#   - tabel:  dim_skolesommerferie_DKK_raw
#
# Vi bruger:
#   overwrite   = TRUE  → tabellen bygges forfra ved hver kørsel
#   field.types → styrer SQL-datatyperne eksplicit
#
# Det gør det:
#   - robust at køre scriptet flere gange
#   - nemt at genbruge i gruppen (samme struktur hver gang)
DBI::dbWriteTable(
  conn        = con,
  name        = DBI::Id(schema = schema, table = table),
  value       = dim_skolesommerferie_sql,
  overwrite   = TRUE,            # skriv tabellen forfra hver gang
  field.types = list(
    Datekey    = "char(8)",      # tekstnøgle ddmmåååå
    Dato       = "date",         # datoformat
    Skoleferie = "nvarchar(50)"  # ferienavn / "Skoleslut"
  )
)


# -------------------------------------------------------------------------
# 10) Hurtig validering direkte fra SQL
# -------------------------------------------------------------------------
# Som en sidste sanity check henter vi et lille udsnit af tabellen op igen
# direkte fra databasen. Det er især nyttigt under udvikling:
#   - vi ser, at tabellen findes
#   - vi kan visuelt tjekke indhold og typer
head(DBI::dbReadTable(con, DBI::Id(schema = schema, table = table)))

# Luk forbindelsen pænt ned, når vi er færdige
DBI::dbDisconnect(con)


