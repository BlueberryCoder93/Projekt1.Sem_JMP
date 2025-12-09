# =============================================================================
# RAW – DIM_DATO_RAW
# =============================================================================
# Dette script opbygger projektets datodimension (RAW-lag).
# Idéen er:
#   - at generere én række pr. dato i perioden 2000–2026
#   - at berige med klassiske tidsattributter (år, måned, uge, ugedag, weekend)
#   - at gemme det som en stabil RAW-dimension i Azure SQL
#
# RAW her betyder:
#   - ingen joins til andre tabeller
#   - ingen businesslogik udover “ren tidsstruktur”
#   - tabellen må frit overskrives, når scriptet køres igen
# =============================================================================


# =============================================================================
# Pakker
# =============================================================================
# pacman:
#   - installerer manglende pakker
#   - loader dem alle i én kommando
#
# dplyr/tibble  → datamanipulation og tabelstruktur
# lubridate     → dato- og tidsfunktioner (år, måned, uge osv.)
# DBI/odbc      → forbindelse til Azure SQL og upload af tabellen
suppressPackageStartupMessages({
  if (!requireNamespace("pacman", quietly = TRUE)) install.packages("pacman")
  pacman::p_load(dplyr, lubridate, DBI, odbc, tibble)
})


# =============================================================================
# Opsætning af datointerval og danske navne
# =============================================================================
# Vi definerer først det datointerval, som dimensionen skal dække.
#  - start:  2000-01-01
#  - slut:   2026-12-31
#
# Derefter definerer vi:
#  - danske månedsnavne
#  - danske ugedagsnavne
#
# De to vektorer gør det nemt at mappe måned/ugedag-numre til pæne navne,
# som kan bruges direkte i rapporter og QMD’er, uden yderligere oversættelse.
start <- as.Date("2000-01-01")
slut  <- as.Date("2026-12-31")

månednavne <- c(
  "januar","februar","marts","april","maj","juni",
  "juli","august","september","oktober","november","december"
)

ugedagsnavne <- c(
  "mandag","tirsdag","onsdag","torsdag",
  "fredag","lørdag","søndag"
)


# =============================================================================
# 1) Byg selve datodimensionen i R
# =============================================================================
# Her:
#  1) genererer vi en sekvens af datoer fra start til slut (én pr. dag)
#  2) beregner klassiske tidsfelter:
#       - Datekey (ddmmåååå) til joins
#       - år, kvartal, måned, uge
#       - måned_navn, ugedag_navn
#       - dagstype (hverdag/weekend)
#
# Bemærk:
#  - Datekey holdes som tekst (char), så vi kan bruge den som stabil nøgle.
#  - Dato (Dato) gemmes som dd-mm-åååå til visning / menneskelæsbarhed.
dim_dato <- tibble(
  Dato_raw = seq.Date(from = start, to = slut, by = "day")
) %>%
  mutate(
    # Tekstversion af dato (til visning)
    Dato        = format(Dato_raw, "%d-%m-%Y"),
    
    # Datekey til joins (ddmmåååå) – samme format som i andre dimensioner
    Datekey     = format(Dato_raw, "%d%m%Y"),
    
    # Klassiske tidsfelter
    år          = year(Dato_raw),
    kvartal     = quarter(Dato_raw),
    måned       = month(Dato_raw),
    måned_navn  = månednavne[måned],
    uge         = isoweek(Dato_raw),
    
    # Ugedag (1 = mandag, 7 = søndag)
    ugedag_nr   = wday(Dato_raw, week_start = 1),
    ugedag_navn = ugedagsnavne[ugedag_nr],
    
    # Hverdag/weekend – bruges meget ofte ifm. tilskueranalyser
    dagstype    = if_else(ugedag_nr >= 6, "weekend", "hverdag")
  ) %>%
  # Vi behøver ikke de midlertidige felter i den endelige tabel
  select(-Dato_raw, -ugedag_nr) %>%
  # Sæt en logisk kolonnerækkefølge
  select(
    Datekey, Dato, år, kvartal, måned,
    måned_navn, uge, ugedag_navn, dagstype
  )


# =============================================================================
# 2) Klargør datatyper til SQL-upload
# =============================================================================
# Azure SQL kræver, at vi tænker over datatyper:
#   - Datekey         → char(8)
#   - Dato            → date
#   - år/kvartal/...  → heltalstyper
#   - tekstfelter     → nvarchar
#
# transmute() bruges her til:
#   - eksplicit at caste alle felter til de ønskede R-typer
#   - kun beholde de kolonner, der skal med til SQL
#
# Når vi senere angiver field.types i dbWriteTable, mappes de R-typer
# til de ønskede SQL-typer.
dim_dato_sql <- dim_dato %>%
  transmute(
    Datekey     = as.character(Datekey),
    Dato        = as.Date(Dato, format = "%d-%m-%Y"),
    år          = as.integer(år),
    kvartal     = as.integer(kvartal),
    måned       = as.integer(måned),
    måned_navn  = as.character(måned_navn),
    uge         = as.integer(uge),
    ugedag_navn = as.character(ugedag_navn),
    weekend     = as.character(dagstype)   # vi kalder den "weekend" i SQL
  )


# =============================================================================
# 3) Forbindelse til Azure SQL – credentials fra .Renviron
# =============================================================================
# Vi bruger miljøvariabler til at hente login-info:
#   AZURE_SQL_SERVER
#   AZURE_SQL_DB
#   AZURE_SQL_UID
#   AZURE_SQL_PWD
#
# Fordele:
#   - ingen passwords i scriptet
#   - nem deling i gruppen
#   - samme kode på alle maskiner
#
# Bemærk:
#   - schema sættes nu til PBA01_Raw for at afspejle dit nye schema-design.
server   <- Sys.getenv("AZURE_SQL_SERVER")
database <- Sys.getenv("AZURE_SQL_DB")
uid      <- Sys.getenv("AZURE_SQL_UID")
pwd      <- Sys.getenv("AZURE_SQL_PWD")

schema   <- "PBA01_Raw"
table    <- "dim_dato_raw"

if (any(!nzchar(c(server, database, uid, pwd)))) {
  stop("En eller flere miljøvariable til Azure SQL mangler. Tjek .Renviron og genstart R.")
}

con <- DBI::dbConnect(
  odbc::odbc(),
  Driver                 = "ODBC Driver 18 for SQL Server",
  Server                 = paste0(server, ",1433"),
  Database               = database,
  UID                    = uid,
  PWD                    = pwd,
  Encrypt                = "yes",
  TrustServerCertificate = "no",
  Timeout                = 30
)


# =============================================================================
# 4) Skriv til PBA01_Raw.dim_dato_raw (overskriv altid)
# =============================================================================
# Her uploader vi datodimensionen til schemaet PBA01_Raw.
#
# overwrite = TRUE:
#   - vi accepterer, at hele dimensionen genskabes ved hver kørsel
#   - ingen incremental update er nødvendig for en ren datokalender
#
# field.types:
#   - sikrer, at SQL-tabellen får præcis de typer vi ønsker
#   - gør schemaet mere kontrolleret og let at dokumentere
DBI::dbWriteTable(
  conn        = con,
  name        = DBI::Id(schema = schema, table = table),
  value       = dim_dato_sql,
  overwrite   = TRUE,
  field.types = list(
    Datekey     = "char(8)",
    Dato        = "date",
    år          = "smallint",
    kvartal     = "tinyint",
    måned       = "tinyint",
    måned_navn  = "nvarchar(20)",
    uge         = "tinyint",
    ugedag_navn = "nvarchar(20)",
    weekend     = "nvarchar(10)"
  )
)


# =============================================================================
# 5) Hurtigt tjek direkte fra SQL (validering)
# =============================================================================
# Som afsluttende sanity check:
#   - tæller vi antal rækker (skal svare til antal datoer i perioden)
#   - henter de første 10 rækker for at se struktur og indhold
#
# Disse SELECT-kald er kun til udvikling og dokumentation og kan fjernes
# senere, hvis I vil have meget “ren” produktionskode.
DBI::dbGetQuery(
  con,
  paste0(
    "SELECT COUNT(*) AS antal_rækker FROM ",
    schema, ".", table, ";"
  )
)

DBI::dbGetQuery(
  con,
  paste0(
    "SELECT TOP 10 * FROM ",
    schema, ".", table, " ORDER BY Dato;"
  )
)


# =============================================================================
# Luk forbindelsen
# =============================================================================
# Når vi er færdige, lukker vi altid forbindelsen til databasen for at:
#   - frigive ressourcer
#   - undgå hængende forbindelser i Azure
DBI::dbDisconnect(con)



