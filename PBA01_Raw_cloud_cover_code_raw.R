# =============================================================================
# RAW – DIM_CLOUD_COVER_CODE_RAW
# =============================================================================
# Dette script scraper DMI's officielle cloud-cover kodeoversigt direkte fra
# dokumentationssiden. Vi gemmer resultatet som en RAW-dimension i Azure SQL.
#
# RAW-laget må gerne overskrives, for det skal til enhver tid afspejle den
# nyeste version af eksterne datakilder uden nogen form for business logic.
# =============================================================================


# =============================================================================
# Pakker
# =============================================================================
# pacman bruges for at sikre, at pakker automatisk installeres hvis de mangler,
# og loades samlet herefter. Det giver et stabilt og repeterbart setup.
#
# rvest  → webscraping af DMI's HTML-tabel
# stringr → rengøring og behandling af tekstdata
# dplyr/tibble → strukturerer og renser data til en RAW-tabel
# DBI/odbc → uploader tabellen til Azure SQL (RAW-schema)
suppressPackageStartupMessages({
  if (!requireNamespace("pacman", quietly = TRUE)) install.packages("pacman")
  pacman::p_load(rvest, dplyr, tibble, stringr, DBI, odbc)
})


# =============================================================================
# Hent HTML-siden fra DMI
# =============================================================================
# read_html() downloader hele dokumentationssiden som HTML, så vi kan udtrække
# præcis den tabel vi skal bruge. Vi arbejder IKKE med API'et her – dette er
# en statisk kodeoversigt.
url  <- "https://opendatadocs.dmi.govcloud.dk/en/Data/Meteorological_Observation_Data"
page <- read_html(url)


# =============================================================================
# Find den rigtige cloud-cover tabel
# =============================================================================
# Tabellen ligger struktureret lige efter en <h3 id="cloud_cover">-overskrift.
# Derfor vælger vi et XPath-selector, som trækker præcis denne tabel og ingen
# af de andre kodeoversigter på siden.
#
# Hvis tabellen ikke findes (f.eks. hvis DMI ændrer layout), stopper scriptet.
node_cloud <- page %>%
  html_element(xpath = "//h3[@id='cloud_cover']/following-sibling::figure[1]/table")

if (is.null(node_cloud)) {
  stop("Kunne ikke finde Cloud_cover tabellen på DMI’s dokumentationsside.")
}

# Konverter HTML-tabel → tibble
cloud_tbl_raw <- node_cloud %>%
  html_table(fill = TRUE) %>%
  as_tibble()


# =============================================================================
# Rensning og RAW-struktur
# =============================================================================
# Dette er stadig RAW-laget! Det betyder:
#   - Vi retter kun tekniske problemer (whitespace, datatype)
#   - Vi ændrer IKKE forretningslogik eller indhold
#
# Årsager til hvert trin:
#   rename() → sikrer ens standardnavne i hele din datamodel
#   str_replace_all() → DMI har mellemrum i nogle koder; de skal fjernes
#   as.integer() → koderne skal være numeriske for korrekt relationel brug
dim_cloud_cover_code_raw <- cloud_tbl_raw %>%
  rename(
    cloud_cover_code    = 1,
    cloud_cover_text_en = 2
  ) %>%
  mutate(
    cloud_cover_code = str_replace_all(cloud_cover_code, "\\s", ""),
    cloud_cover_code = as.integer(cloud_cover_code)
  )

print(dim_cloud_cover_code_raw)


# =============================================================================
# Forbind til Azure SQL (RAW-schema)
# =============================================================================
# Her henter vi credentials fra .Renviron.
# Fordele:
#   ✓ Ingen passwords i kodefilerne
#   ✓ Alle på holdet kan køre scriptet
#   ✓ Stabil drift uden manuelle ændringer
#
# Bemærk: Tabellen skrives til schema **PBA01_Raw**, som er RAW-lagets schema.
server   <- Sys.getenv("AZURE_SQL_SERVER")
database <- Sys.getenv("AZURE_SQL_DB")
uid      <- Sys.getenv("AZURE_SQL_UID")
pwd      <- Sys.getenv("AZURE_SQL_PWD")

if (any(!nzchar(c(server, database, uid, pwd)))) {
  stop("Azure SQL miljøvariabler mangler i .Renviron.")
}

schema <- "PBA01_Raw"
table  <- "dim_cloud_cover_code_raw"

con_azure <- DBI::dbConnect(
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
# Upload til Azure SQL (RAW)
# =============================================================================
# overwrite = TRUE → RAW-laget skal altid kunne regenereres direkte fra kilden.
# Derfor må RAW-tabel altid overskrives fuldstændigt uden incremental updates.
#
# Dette er netop forskellen på RAW og dine API-facts:
#   RAW        → statiske tabeller, altid overskrives
#   API FACTS  → dynamiske tabeller, incremental updates
DBI::dbWriteTable(
  conn      = con_azure,
  name      = DBI::Id(schema = schema, table = table),
  value     = dim_cloud_cover_code_raw,
  overwrite = TRUE
)


# =============================================================================
# Validering af upload
# =============================================================================
# Som god ETL-praksis henter vi de første rækker tilbage fra databasen.
# Dette sikrer, at tabellen:
#   - eksisterer
#   - har korrekt struktur
#   - er uploadet uden fejl
head(DBI::dbReadTable(con_azure, DBI::Id(schema = schema, table = table)))


# Luk forbindelsen
DBI::dbDisconnect(con_azure)


