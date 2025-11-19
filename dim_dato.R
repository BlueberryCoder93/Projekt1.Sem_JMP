# -------------------------------------------------------------------------
# Pakker
# -------------------------------------------------------------------------
suppressPackageStartupMessages({
  if (!requireNamespace("pacman", quietly = TRUE)) install.packages("pacman")
  pacman::p_load(dplyr, lubridate, DBI, odbc, tibble)
})

# -------------------------------------------------------------------------
# Opsætning: start/slut-dato + danske navne
# -------------------------------------------------------------------------
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

# -------------------------------------------------------------------------
# Dato-dimension (dim_dato) til arbejde i R
# -------------------------------------------------------------------------
dim_dato <- tibble(
  Dato_raw = seq.Date(from = start, to = slut, by = "day")
) %>%
  mutate(
    Dato        = format(Dato_raw, "%d-%m-%Y"),   # tekst til visning
    Datekey     = format(Dato_raw, "%d%m%Y"),
    år          = year(Dato_raw),
    kvartal     = quarter(Dato_raw),
    måned       = month(Dato_raw),
    måned_navn  = månednavne[måned],
    uge         = isoweek(Dato_raw),
    ugedag_nr   = wday(Dato_raw, week_start = 1),
    ugedag_navn = ugedagsnavne[ugedag_nr],
    dagstype    = if_else(ugedag_nr >= 6, "weekend", "hverdag")
  ) %>%
  select(-Dato_raw, -ugedag_nr) %>%      # rydder op
  select(Datekey, everything())          # Datekey som første kolonne

# Kig i data i RStudio
View(dim_dato)

# -------------------------------------------------------------------------
# Behandling før upload til SQL (dim_dato_sql)
# -------------------------------------------------------------------------
dim_dato_sql <- dim_dato %>%
  transmute(
    Datekey     = as.character(Datekey),                    # char(8)
    Dato        = as.Date(Dato, format = "%d-%m-%Y"),       # date
    år          = as.integer(år),                           # smallint/int
    kvartal     = as.integer(kvartal),                      # tinyint
    måned       = as.integer(måned),                        # tinyint
    måned_navn  = as.character(måned_navn),                 # nvarchar
    uge         = as.integer(uge),                          # tinyint
    ugedag_navn = as.character(ugedag_navn),                # nvarchar
    weekend     = if_else(dagstype == "weekend", 1L, 0L)    # 0/1 → bit
  )

# -------------------------------------------------------------------------
# Upload til Azure SQL
# -------------------------------------------------------------------------

Sys.setenv(AZURE_SQL_PWD = "Diamond3251boy")

server   <- "jmp-pba-dataanalyse-2025.database.windows.net"
database <- "PBA_studiedata"
uid      <- "janadmin"
pwd      <- Sys.getenv("AZURE_SQL_PWD")
schema   <- "dbo"
table    <- "dim_dato"

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

DBI::dbWriteTable(
  conn       = con,
  name       = DBI::Id(schema = schema, table = table),
  value      = dim_dato_sql,
  overwrite  = TRUE,
  field.types = list(
    Datekey     = "char(8)",
    Dato        = "date",
    år          = "smallint",
    kvartal     = "tinyint",
    måned       = "tinyint",
    måned_navn  = "nvarchar(20)",
    uge         = "tinyint",
    ugedag_navn = "nvarchar(20)",
    weekend     = "bit"
  )
)

# Tjek de første rækker i SQL-tabellen
head(DBI::dbReadTable(con, DBI::Id(schema = schema, table = table)))

DBI::dbDisconnect(con)
