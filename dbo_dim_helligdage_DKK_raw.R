# ------------------------------------------------------------
# Pakker
# ------------------------------------------------------------
# I dette afsnit sikrer vi, at alle nødvendige pakker er tilgængelige.
# Vi bruger pacman til både at installere og indlæse pakkerne, så scriptet
# kan køres på tværs af maskiner uden manuel installation.
# httr2 og jsonlite anvendes til at foretage API-kald og håndtere JSON-svar.
# tibble, dplyr, purrr og lubridate bruges til at strukturere, transformere
# og berige data, mens DBI og odbc står for selve databaseforbindelsen
# til Azure SQL, hvor vi gemmer helligdagsdimensionen.
suppressPackageStartupMessages({
  if (!requireNamespace("pacman", quietly = TRUE)) install.packages("pacman")
  
  pacman::p_load(
    httr2, jsonlite, tibble, dplyr, purrr, lubridate,
    DBI, odbc
  )
})

# ------------------------------------------------------------
#  Funktion til at hente og formatere helligdage
# ------------------------------------------------------------
# Her definerer vi en genanvendelig funktion, der henter danske helligdage
# for et givent år via Nager.Date API'et. Funktionen håndterer hele forløbet:
# API-kald, fejlhåndtering, konvertering fra JSON til tibble samt
# formatering af kolonner. Vi opretter en datekey, bevarer datoen som
# R Date og medtager både dansk og engelsk navn på helligdagen.
# Samtidig oversætter vi API'ets typer til danske beskrivelser, så
# dimensionstabellen kan bruges direkte i analyser af tilskuertal,
# kampdage og aktivitetsmønstre omkring helligdage.
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
# Hent alle år 2000–2026 og bind sammen
# ------------------------------------------------------------
# I dette afsnit automatiserer vi indhentningen af helligdage ved at
# iterere over alle år fra 2000 til 2026. Med purrr::map_dfr kombinerer vi
# resultatet fra hvert år til én samlet dimensionstabel.
# Det betyder, at vi med ét kald får en komplet helligdagsdimension
# på tværs af hele vores analyseperiode, som kan kobles på kampdata
# og tilskuertal i senere analyser.

years <- 2000:2026

dim_helligdage_dkk_raw <- purrr::map_dfr(
  years,
  \(y) get_public_holidays_dim(y, country = "DK")
)

# Hurtigt tjek af datastruktur
print(head(dim_helligdage_dkk_raw, 10))
str(dim_helligdage_dkk_raw)

# ------------------------------------------------------------
# Forbindelse til Azure SQL
# ------------------------------------------------------------
# Her etablerer vi forbindelse til vores Azure SQL-database, hvor
# helligdagsdimensionen skal lagres som RAW-lag. Loginoplysningerne
# hentes fra miljøvariabler i .Renviron, så brugernavn, servernavn
# og password ikke står i selve scriptet. Det gør koden mere sikker
# og nemmere at dele i gruppen og i et fælles repository.
# Selve forbindelsen oprettes via DBI og odbc ved hjælp af en
# connection string, der angiver driver, server, database og
# krypteringsindstillinger.

server   <- Sys.getenv("AZURE_SQL_SERVER")
database <- Sys.getenv("AZURE_SQL_DB")
uid      <- Sys.getenv("AZURE_SQL_UID")
pwd      <- Sys.getenv("AZURE_SQL_PWD")
schema   <- "dbo"
table    <- "dim_helligdage_DKK_raw"

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

# ------------------------------------------------------------
# Skriv tabel til SQL
# ------------------------------------------------------------
# I det afsluttende afsnit skriver vi helligdagsdimensionen til
# Azure SQL som en rå dimensionstabel i schemaet dbo.
# dbWriteTable genererer selv den nødvendige SQL i baggrunden,
# så vi undgår at skrive manuel SQL-kode. Tabellen overskrives
# ved hver kørsel, hvilket sikrer at databasen altid indeholder
# den nyeste version af helligdagsdata.
# Til sidst læser vi de første rækker tilbage fra SQL som en
# simpel validering af, at uploaden er lykkedes, før vi lukker
# forbindelsen til databasen igen.

DBI::dbWriteTable(
  conn      = con,
  name      = DBI::Id(schema = schema, table = table),
  value     = dim_helligdage_dkk_raw,
  overwrite = TRUE
)

# Tjek at SQL indeholder det forventede
check_tbl <- DBI::dbReadTable(con, DBI::Id(schema = schema, table = table))
print(head(check_tbl, 10))

DBI::dbDisconnect(con)


