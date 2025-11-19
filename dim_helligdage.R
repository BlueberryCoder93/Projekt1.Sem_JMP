# ------------------------------------------------------------
# Afsnit 1: Pakker
# Formål: Indlæse alle nødvendige pakker til API-kald, databehandling
#         og forbindelse til SQL-databasen.
# ------------------------------------------------------------
suppressPackageStartupMessages({
  if (!requireNamespace("pacman", quietly = TRUE)) install.packages("pacman")
  
  pacman::p_load(
    httr2, jsonlite, tibble, dplyr, purrr, lubridate,
    DBI, odbc
  )
})

# ------------------------------------------------------------
# Afsnit 2: Funktion til at hente og formatere helligdage
# Formål: Hente data fra API'et for et bestemt år og omsætte
#         svarformatet til en struktureret dimensionstabel.
# ------------------------------------------------------------
get_public_holidays_dim <- function(year, country = "DK") {
  
  base_url <- "https://date.nager.at/api/v3/publicholidays"
  
  # API-kald: henter helligdage for det givne år og land
  resp <- request(base_url) |>
    req_url_path_append(paste0(year, "/", country)) |>
    req_perform()
  
  # Fejlhåndtering hvis API svarer med fejl
  if (resp_status(resp) >= 400) {
    stop("API-fejl for år: ", year, " land: ", country)
  }
  
  # Omdan JSON til tibble
  dat_raw <- resp |>
    resp_body_string() |>
    fromJSON(flatten = TRUE) |>
    as_tibble()
  
  # Formater og omdøb kolonner
  out <- dat_raw |>
    mutate(
      # Konverter API-dato til R Date
      dato_tmp = as.Date(date)
    ) |>
    transmute(
      # Primær nøgle og første kolonne
      datekey        = format(dato_tmp, "%d%m%Y"),
      
      # Behold original dato som R Date
      dato           = dato_tmp,
      
      # Danske og engelske navne
      helligdag_navn = localName,
      english_name   = name,
      
      # Type: oversæt alle kategori-typer til dansk
      type = purrr::map_chr(
        types,
        \(x) {
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
          paste(x_dk, collapse = ", ")
        }
      )
    )
  
  return(out)
}

# ------------------------------------------------------------
# Afsnit 3: Hent alle år 2000–2026 og bind sammen
# Formål: Automatisere hentning af helligdage for hvert år.
# ------------------------------------------------------------
years <- 2000:2026

dim_helligdage_dk <- purrr::map_dfr(
  years,
  \(y) get_public_holidays_dim(y, country = "DK")
)

# Hurtigt tjek af datastruktur
print(head(dim_helligdage_dk, 10))
str(dim_helligdage_dk)

# ------------------------------------------------------------
# Afsnit 4: Forbindelse til Azure SQL
# Formål: Oprette forbindelse til din database.
# ------------------------------------------------------------
Sys.setenv(AZURE_SQL_PWD = "Diamond3251boy")

server   <- "jmp-pba-dataanalyse-2025.database.windows.net"
database <- "PBA_studiedata"
uid      <- "janadmin"
pwd      <- Sys.getenv("AZURE_SQL_PWD")
schema   <- "dbo"
table    <- "dim_helligdage"

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

# ------------------------------------------------------------
# Afsnit 5: Skriv tabel til SQL
# Formål: Lægge dimensionstabellen i SQL med korrekt kolonnerækkefølge.
# ------------------------------------------------------------
DBI::dbWriteTable(
  conn      = con,
  name      = DBI::Id(schema = schema, table = table),
  value     = dim_helligdage_dk,
  overwrite = TRUE
)

# Tjek at SQL indeholder det forventede
check_tbl <- DBI::dbReadTable(con, DBI::Id(schema = schema, table = table))
print(head(check_tbl, 10))

# Luk forbindelsen
DBI::dbDisconnect(con)
