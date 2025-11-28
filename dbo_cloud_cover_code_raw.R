# I dette script bygger vi en RAW-dimension for DMI’s sky­dække-koder 
# (“cloud cover codes”). 
#
# Tabellen findes som en HTML-tabel i DMI’s dokumentation, og formålet er:
#   - at scrape den officielle cloud_cover-kodeoversigt,
#   - at standardisere kolonnenavne og datatyper,
#   - at fjerne whitespace og sikre korrekt numerisk kodning,
#   - og at uploade råtabellen til Azure SQL som dbo.dim_cloud_cover_code_raw.
#
# Denne dimension anvendes senere til at berige vejrobservationer fra API’et
# (fx 06060 Karup), så vi kan mappe cloud-cover målinger til deres 
# tekstbeskrivelse på engelsk.


# =============================================================================
# Pakker
# =============================================================================
# pacman bruges som i alle dine andre scripts, så pakker installeres ved behov
# og indlæses samlet. rvest bruges til webscraping, stringr til tekst, 
# tibble/dplyr til databehandling, og DBI/odbc til upload til Azure SQL.

suppressPackageStartupMessages({
  if (!requireNamespace("pacman", quietly = TRUE)) install.packages("pacman")
  pacman::p_load(rvest, dplyr, tibble, stringr, DBI, odbc)
})

# =============================================================================
# 1) Hent HTML fra DMI
# =============================================================================
# Vi henter dokumentationssiden “Meteorological Observation Data”, som indeholder
# alle kodeoversigter for DMI’s observationstyper. 
# read_html indlæser hele siden som et XML/HTML-objekt, som vi kan navigere i.

url  <- "https://opendatadocs.dmi.govcloud.dk/en/Data/Meteorological_Observation_Data"
page <- read_html(url)

# =============================================================================
# 2) Find Cloud_cover-tabellen baseret på overskriften
# =============================================================================
# DMI strukturerer deres dokumentation med <h3 id="cloud_cover">, efterfulgt
# af en <figure> der indeholder tabellen. 
# XPath-udtrykket finder præcis den tabel.
#
# Hvis tabellen ikke findes, stopper scriptet med en klar fejlbesked.

node_cloud <- page %>%
  html_element(xpath = "//h3[@id='cloud_cover']/following-sibling::figure[1]/table")

if (is.null(node_cloud)) stop("Kunne ikke finde Cloud_cover tabellen på siden.")

# Konverter HTML-tabellen til R-tibble
cloud_tbl_raw <- node_cloud %>%
  html_table(fill = TRUE) %>%
  as_tibble()

# =============================================================================
# 3) Rens og navngiv korrekt (RAW-dimension)
# =============================================================================
# Her standardiserer vi cloud_cover-koderne:
#   - omdøber kolonnerne til cloud_cover_code og cloud_cover_text_en,
#   - fjerner whitespace (nogle koder har mellemrum i DMI-tabellen),
#   - konverterer koden til integer for at sikre korrekt datatype.
#
# Resultatet er en ren RAW-dimension, som passer til stil og struktur 
# i resten af dine RAW-tabeller.

dim_cloud_cover_code_raw <- cloud_tbl_raw %>%
  rename(
    cloud_cover_code     = 1,
    cloud_cover_text_en  = 2
  ) %>%
  mutate(
    cloud_cover_code = str_replace_all(cloud_cover_code, "\\s", ""),
    cloud_cover_code = as.integer(cloud_cover_code)
  )

print(dim_cloud_cover_code_raw)

# =============================================================================
# 4) Upload til Azure SQL som dim_cloud_cover_code_raw
# =============================================================================
# Nu opretter vi forbindelse til Azure SQL via miljøvariabler (.Renviron), 
# så koden kan deles sikkert uden hardcoded credentials. 
# Tabellen overskrives ved hver kørsel for at sikre et ensartet RAW-lag.

server   <- Sys.getenv("AZURE_SQL_SERVER")
database <- Sys.getenv("AZURE_SQL_DB")
uid      <- Sys.getenv("AZURE_SQL_UID")
pwd      <- Sys.getenv("AZURE_SQL_PWD")

if (any(!nzchar(c(server, database, uid, pwd)))) {
  stop("En eller flere miljøvariable til Azure SQL mangler. Tjek .Renviron og genstart R.")
}

schema <- "dbo"
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

DBI::dbWriteTable(
  conn      = con_azure,
  name      = DBI::Id(schema = schema, table = table),
  value     = dim_cloud_cover_code_raw,
  overwrite = TRUE
)

# Validering: læs et udsnit fra SQL, så vi kan se at uploaden lykkedes
head(DBI::dbReadTable(con_azure, DBI::Id(schema = schema, table = table)))

DBI::dbDisconnect(con_azure)

