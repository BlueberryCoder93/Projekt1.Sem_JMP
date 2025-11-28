# I dette script opbygger vi projektets skoledimension for Viborg Kommune, 
# som supplerer vores datodimension med information om skoleferier. 
# Formålet er at hente officielle ferieperioder fra publicholidays.dk, 
# omsætte dem til dato-niveau og udlede både selve feriedagene samt 
# "Skoleslut" (dagen før sommerferien starter). 
# Den færdige dimension skrives til Azure SQL og kan efterfølgende joines 
# med dim_dato og billetsalgsdata for at undersøge, hvordan skoleferier 
# påvirker tilskuertallet til Viborg F.F.’s hjemmekampe.

# -------------------------------------------------------------------------
# 1) Pakker
# -------------------------------------------------------------------------
# Ligesom i de øvrige scripts bruger vi pacman til at håndtere pakker, 
# så scriptet er nemt at køre på tværs af gruppens maskiner. 
# rvest anvendes til webscraping af HTML-tabeller, 
# dplyr og stringr til databehandling og tekstmanipulation, 
# DBI og odbc til forbindelsen til Azure SQL.
# (tidyverse er med for at sikre, at centrale funktioner er tilgængelige samlet.)

suppressPackageStartupMessages({
  if (!requireNamespace("pacman", quietly = TRUE)) install.packages("pacman")
  pacman::p_load(rvest, dplyr, stringr, tidyverse, DBI, odbc)
})

# -------------------------------------------------------------------------
# 2) Webscraping af Viborgs skoleferier
# -------------------------------------------------------------------------
# I dette afsnit henter vi de officielle skoleferier for Viborg Kommune 
# fra publicholidays.dk. Siden indeholder flere HTML-tabeller – typisk én 
# pr. skoleår – som alle bruger samme CSS-klasse. 
# Vi:
#  - læser hele HTML-siden,
#  - finder alle relevante tabeller,
#  - konverterer dem til dataframes
#  - og samler dem i én samlet rå tabel med ferieinformationer.

url_viborg <- "https://publicholidays.dk/da/school-holidays/viborg/"

page <- read_html(url_viborg)  # henter HTML-indholdet fra siden

skoleferier_raw <- page %>%
  html_elements("table.phgtable") %>%   # finder tabellerne med ferieoplysninger
  html_table(fill = TRUE)               # konverterer hver tabel til et dataframe

skoleferier <- bind_rows(skoleferier_raw)  # binder alle tabeller sammen til én tabel

# -------------------------------------------------------------------------
# 3) Hjælpefunktion: parsing af danske datoformater
# -------------------------------------------------------------------------
# Publicholidays angiver datoer som tekst, fx "1. juli 2024 (Uge 28)".
# For at kunne arbejde analytisk med datoerne – og senere skrive dem til SQL –
# skal de konverteres til R’s Date-format.
#
# Funktionen parse_dansk:
#  - håndterer tomme felter som manglende værdier (NA),
#  - fjerner parenteser som indeholder ugenumre,
#  - udtrækker dag, månedsnavn og år ved hjælp af regex,
#  - omsætter danske månedsnavne til månedsnumre,
#  - returnerer en vektor af datoer i formatet ÅÅÅÅ-mm-dd.

parse_dansk <- function(x) {
  x <- as.character(x)
  x[x == ""] <- NA_character_  # tomme felter håndteres som NA
  
  # start med en vektor af NA-datoer
  res <- as.Date(rep(NA_character_, length(x)))
  
  ok <- !is.na(x)
  if (!any(ok)) return(res)  # hvis ingen brugbare værdier, returner NA-vektor
  
  # fjern parenteser som fx "(Uge 28)"
  x_clean <- str_replace(x[ok], "\\s*\\(.*\\)$", "")
  
  # regex: udtræk dag, månedtekst og år
  m <- str_match(x_clean, "^(\\d{1,2})\\.\\s*(\\w+)\\s*(\\d{4})$")
  
  good <- !is.na(m[, 1])
  if (!any(good)) return(res)
  
  dag        <- as.integer(m[good, 2])
  måned_txt  <- tolower(m[good, 3])
  år         <- as.integer(m[good, 4])
  
  # omsæt danske månedsnavne til månedsnummer
  måned_num <- dplyr::recode(
    måned_txt,
    januar = 1L, februar = 2L, marts = 3L, april = 4L, maj = 5L, juni = 6L,
    juli = 7L, august = 8L, september = 9L, oktober = 10L,
    november = 11L, december = 12L
  )
  
  # indsæt kun de parsede datoer i output på de rigtige positioner
  idx <- which(ok)[good]
  res[idx] <- as.Date(sprintf("%04d-%02d-%02d", år, måned_num, dag))
  
  res
}

# -------------------------------------------------------------------------
# 4) Parsing af start- og slutdatoer fra websitet
# -------------------------------------------------------------------------
# Råtabellen indeholder tekstkolonner for Start og Slut på hver ferieperiode.
# Her lægger vi to nye Date-kolonner på – én for startdato og én for slutdato – 
# ved hjælp af parse_dansk. 
# Disse felter udgør fundamentet for den videre konstruktion af 
# vores skoledimension.

skoleferier_dato <- skoleferier %>%
  mutate(
    start_dato_num = parse_dansk(Start),   # konverter startdato-tekst til Date
    slut_dato_num  = parse_dansk(Slut)     # konverter slutdato-tekst til Date
  )

# -------------------------------------------------------------------------
# 5) Basis-ferietabel og beregning af "Skoleslut"
# -------------------------------------------------------------------------
# Vi ønsker én simpel tabel med:
#  - ferienavn (Skoleferie)
#  - dato (startdato for hver ferieperiode)
#
# Derudover tilføjer vi en særlig markør "Skoleslut":
#  - defineret som dagen før sommerferien starter,
#  - da denne dag ofte har særlig betydning (sidste skoledag).
#
# Resultatet er to deltabeller:
#  - skoleferier_simple: alle ferieperioder med deres startdato
#  - skoleslut_rows: rækker for skoleslut, afledt af sommerferiens startdato

skoleferier_simple <- skoleferier_dato %>%
  transmute(
    Skoleferie,
    dato = start_dato_num
  )

skoleslut_rows <- skoleferier_dato %>%
  filter(Skoleferie == "Sommerferie", !is.na(start_dato_num)) %>%  # find sommerferier
  transmute(
    Skoleferie = "Skoleslut",         # nyt ferienavn
    dato       = start_dato_num - 1   # dagen før sommerferien starter
  )

# -------------------------------------------------------------------------
# 6) Byg dimensionstabellen dim_skolesommerferie
# -------------------------------------------------------------------------
# Her samler vi alle ferieoplysninger i én dimensionstabel:
#  - vi kombinerer de almindelige ferier med "Skoleslut",
#  - sorterer kronologisk,
#  - sikrer at dato ligger som Date,
#  - opretter en Datekey i formatet ddmmåååå (tekst),
#  - og sætter kolonnerne i en fast rækkefølge: Datekey, Dato, Skoleferie.
#
# Denne struktur gør tabellen let at join’e med vores øvrige dimensioner 
# og facts i datalageret.

dim_skolesommerferie <- bind_rows(
  skoleferier_simple,
  skoleslut_rows
) %>%
  arrange(dato) %>%                    # sorter kronologisk
  mutate(
    Dato    = as.Date(dato),          # sikrer Date-kolonne
    Datekey = format(Dato, "%d%m%Y")  # tekstnøgle til join med dim_dato
  ) %>%
  select(Datekey, Dato, Skoleferie)   # endelig kolonnerækkefølge

# Valgfri visuel kontrol i RStudio (kan kommenteres ud i produktionskørsel)
# View(dim_skolesommerferie)

# -------------------------------------------------------------------------
# 7) Tilpas datatyper til SQL-upload
# -------------------------------------------------------------------------
# Inden upload til Azure SQL gør vi datatyperne helt eksplicit:
#  - Datekey gemmes som tekst (char),
#  - Dato beholdes som Date,
#  - Skoleferie som tekst (nvarchar).
#
# Det mindsker risikoen for, at dbWriteTable gætter forkert på typerne 
# og sikrer en konsistent struktur i databasen.

dim_skolesommerferie_sql <- dim_skolesommerferie %>%
  mutate(
    Datekey    = as.character(Datekey),   # char(8)
    Dato       = as.Date(Dato),           # date
    Skoleferie = as.character(Skoleferie) # nvarchar
  )

# -------------------------------------------------------------------------
# 8) Forbindelse til Azure SQL via ODBC (.Renviron-login)
# -------------------------------------------------------------------------
# Som i de øvrige scripts hentes servernavn, databasenavn, brugernavn og password 
# fra miljøvariabler defineret i .Renviron. 
# Det betyder:
#  - at loginoplysninger ikke er hårdkodet i scriptet,
#  - at filen trygt kan deles via GitHub,
#  - og at hver bruger kan køre den med egne credentials.
#
# Forbindelsen oprettes med ODBC Driver 18 til Azure SQL.

server   <- Sys.getenv("AZURE_SQL_SERVER")
database <- Sys.getenv("AZURE_SQL_DB")
uid      <- Sys.getenv("AZURE_SQL_UID")
pwd      <- Sys.getenv("AZURE_SQL_PWD")
schema   <- "dbo"
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
# Til sidst skrives dimensionstabellen til Azure SQL i schema dbo 
# under navnet dim_skolesommerferie_DKK_raw.
# Vi:
#  - overskriver tabellen ved hver kørsel for at sikre en opdateret version,
#  - styrer de konkrete SQL-datatyper via field.types.
#
# Tabellen kan herefter anvendes direkte i joins mod dim_dato og facts 
# for at analysere sammenhængen mellem skoleferier og fx tilskuertal.

DBI::dbWriteTable(
  conn        = con,
  name        = DBI::Id(schema = schema, table = table),
  value       = dim_skolesommerferie_sql,
  overwrite   = TRUE,            # skriv tabellen forfra ved hver kørsel
  field.types = list(            # eksplicitte SQL-datatyper
    Datekey    = "char(8)",      # tekstnøgle
    Dato       = "date",         # datoformat
    Skoleferie = "nvarchar(50)"  # tekstkolonne til ferienavn
  )
)

# -------------------------------------------------------------------------
# 10) Hurtig validering direkte fra SQL
# -------------------------------------------------------------------------
# Som afsluttende kontrol henter vi et udsnit af tabellen tilbage fra databasen. 
# Det gør det nemt at tjekke, at:
#  - tabellen er oprettet,
#  - datatyperne ser korrekte ud,
#  - og indholdet matcher forventningerne fra R-objektet.

head(DBI::dbReadTable(con, DBI::Id(schema = schema, table = table)))

DBI::dbDisconnect(con)

