# I dette script arbejder vi med de interne VFF-data, som ligger lokalt hos os,
# og flytter dem op i Azure SQL som RAW-tabeller.
# Kilden er dels en lokal SQLite-database (fodbolddata.sqlite) og dels to RDS-filer:
#   - dim_vff_klubber / fact_vff_billetsalg fra SQLite
#   - klub-RDS (fcidk.rds) til validering af klubber
#   - billetsalgs-RDS (vffkort01.rds) som alternativ rå faktatabel
#
# Formålet er:
#   1) at læse data ind fra SQLite og RDS,
#   2) at tjekke om strukturer og klubnavne stemmer overens,
#   3) at uploade tre RAW-tabeller til Azure SQL i skema PBA01_Raw:
#        * PBA01_Raw.dim_vff_klubber_raw
#        * PBA01_Raw.fact_vff_billetsalg_raw_SQLite
#        * PBA01_Raw.fact_vff_billetsalg_raw_RDS
# RAW-laget bruges efterfølgende som udgangspunkt for rensning og modellering.

# -------------------------------------------------------------------------
# Afsnit 1: Pakker
# -------------------------------------------------------------------------
# Vi bruger pacman-tilgangen til pakker, så scriptet er nemt at køre
# på tværs af gruppens maskiner. DBI/RSQLite håndterer SQLite-forbindelsen,
# odbc bruges til Azure SQL, og dplyr/tidyverse giver os de centrale
# datahåndteringsfunktioner.

suppressPackageStartupMessages({
  if (!requireNamespace("pacman", quietly = TRUE)) install.packages("pacman")
  pacman::p_load(DBI, RSQLite, dplyr, tidyverse, odbc)
})

# -------------------------------------------------------------------------
# Afsnit 2: Hent data fra SQLite 
# -------------------------------------------------------------------------
# Her kobler vi os på den lokale SQLite-database med VFF-data.
# Flow:
#   - byg sti til databasen,
#   - åbn forbindelse,
#   - læs tabellerne ind i R (dim_vff_klubber og fact_vff_billetsalg),
#   - luk forbindelsen igen,
#   - og få et hurtigt overblik over struktur og indhold.

# Sti til mappen med databasen 
mappe  <- "C:\\Users\\janpe\\OneDrive\\Skrivebord\\PBA Dataanlyse\\01_Første semester\\1 Semester projekt\\Database SQL\\VFF data"
db_fil <- file.path(mappe, "fodbolddata.sqlite")

# Forbind til SQLite 
con_sqlite <- dbConnect(SQLite(), db_fil)

# Se hvilke tabeller der er i databasen og læg dem som tabeller
tabeller <- dbListTables(con_sqlite)
cat("Tabeller i fodbolddata.sqlite:\n")
print(tabeller)

# Her antager vi, at tabeller[1] = klubber, tabeller[2] = billetsalg
dim_vff_klubber     <- dbReadTable(con_sqlite, tabeller[1])
fact_vff_billetsalg <- dbReadTable(con_sqlite, tabeller[2])

# Afbryd forbindelsen til SQLite
dbDisconnect(con_sqlite)

# Tjek strukturer
glimpse(fact_vff_billetsalg); str(fact_vff_billetsalg)
glimpse(dim_vff_klubber);     str(dim_vff_klubber)

# Se data (kun hvis interaktiv)
if (interactive()) {
  View(fact_vff_billetsalg)
  View(dim_vff_klubber)
}

# -------------------------------------------------------------------------
# Afsnit 3: Hent data fra RDS-filer
# -------------------------------------------------------------------------
# Nu læser vi de to RDS-filer:
#   - fcidk.rds: klubdata til validering (uploades ikke)
#   - vffkort01.rds: billetsalgsdata som skal op i RAW-laget.
# Vi bruger klub-RDS’en til at tjekke, om alle klubnavne også findes
# i dim_vff_klubber fra SQLite, og vi gemmer evt. afvigelser til videre analyse.

# Klub-RDS (bruges kun til tjek, uploades ikke)
RDSfil1  <- readRDS("C:/Users/janpe/OneDrive/Skrivebord/PBA Dataanlyse/01_Første semester/1 Semester projekt/Database SQL/VFF data/fcidk.rds")
df_1     <- data.frame(RDSfil1)

tjek_ens <- df_1 |> 
  mutate(
    findes_i_dim = ifelse(
      navn %in% dim_vff_klubber$navn,
      "ja",
      "nej"
    )
  )

if (interactive()) {
  View(tjek_ens)
}

afvigelser <- tjek_ens |> 
  filter(findes_i_dim == "nej")

if (interactive()) {
  View(afvigelser)
}

# Billetsalgs-RDS (den skal vi også uploade som RAW)
RDSfil2 <- readRDS("C:/Users/janpe/OneDrive/Skrivebord/PBA Dataanlyse/01_Første semester/1 Semester projekt/Database SQL/VFF data/vffkort01.rds")
df_2    <- as_tibble(RDSfil2)              # billetsalg fra RDS
df_fact <- as_tibble(fact_vff_billetsalg)  # billetsalg fra SQLite

# -------------------------------------------------------------------------
# Afsnit 4: Sammenligning af kolonner (struktur-tjek)
# -------------------------------------------------------------------------
# Her laver vi et simpelt struktur-tjek mellem billetsalgsdata fra
# SQLite (df_fact) og RDS (df_2). Vi finder:
#   - kolonner, som kun findes i RDS,
#   - kolonner, som kun findes i SQLite.
# Det giver et hurtigt overblik over, hvordan de to kilder adskiller sig.

kolonner_kun_i_df2  <- setdiff(names(df_2),    names(df_fact))
kolonner_kun_i_fact <- setdiff(names(df_fact), names(df_2))

message("Kolonner i df_2 (RDS) som IKKE er i fact_vff_billetsalg (SQLite):")
print(kolonner_kun_i_df2)

message("Kolonner i fact_vff_billetsalg (SQLite) som IKKE er i df_2 (RDS):")
print(kolonner_kun_i_fact)

# -------------------------------------------------------------------------
# Afsnit 5: Forbindelse til Azure SQL (via .Renviron)
# -------------------------------------------------------------------------
# Nu opretter vi forbindelsen til Azure SQL. 
# Alle login-oplysninger hentes fra miljøvariabler (.Renviron), så:
#   - credentials ikke ligger hårdkodet i scriptet,
#   - koden kan versionstyres og deles frit,
#   - hver bruger kan have egne login-oplysninger lokalt.
# Vi stopper scriptet tidligt, hvis en nødvendig variabel mangler.

server   <- Sys.getenv("AZURE_SQL_SERVER")
database <- Sys.getenv("AZURE_SQL_DB")
uid      <- Sys.getenv("AZURE_SQL_UID")
pwd      <- Sys.getenv("AZURE_SQL_PWD")

if (any(!nzchar(c(server, database, uid, pwd)))) {
  stop("En eller flere miljøvariable til Azure SQL mangler. Tjek .Renviron og genstart R.")
}

# VIGTIG ÆNDRING:
# RAW-laget skal ligge i skemaet PBA01_Raw – ikke dbo.
schema <- "PBA01_Raw"

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

# -------------------------------------------------------------------------
# Afsnit 6: Upload til Azure SQL (skema PBA01_Raw)
# Formål:
#   1) PBA01_Raw.dim_vff_klubber_raw
#   2) PBA01_Raw.fact_vff_billetsalg_raw_SQLite  (fra SQLite)
#   3) PBA01_Raw.fact_vff_billetsalg_raw_RDS     (fra RDS)
# -------------------------------------------------------------------------
# I sidste afsnit skriver vi vores tre RAW-tabeller til Azure SQL.
# Hver dbWriteTable overskriver den eksisterende version, så RAW-laget
# altid svarer til de nyeste data, når scriptet køres.

# 1) Dimension: klubber (én version – fra SQLite)
DBI::dbWriteTable(
  conn      = con_azure,
  name      = DBI::Id(schema = schema, table = "dim_vff_klubber_raw"),
  value     = dim_vff_klubber,
  overwrite = TRUE
)

# 2) Faktatabel RAW: billetsalg fra SQLite
DBI::dbWriteTable(
  conn      = con_azure,
  name      = DBI::Id(schema = schema, table = "fact_vff_billetsalg_raw_SQLite"),
  value     = df_fact,
  overwrite = TRUE
)

# 3) Faktatabel RAW: billetsalg fra RDS
DBI::dbWriteTable(
  conn      = con_azure,
  name      = DBI::Id(schema = schema, table = "fact_vff_billetsalg_raw_RDS"),
  value     = df_2,
  overwrite = TRUE
)

# (valgfrit) Tjek et hurtigt udsnit fra hver tabel
head(DBI::dbReadTable(con_azure, DBI::Id(schema = schema, table = "dim_vff_klubber_raw")))
head(DBI::dbReadTable(con_azure, DBI::Id(schema = schema, table = "fact_vff_billetsalg_raw_SQLite")))
head(DBI::dbReadTable(con_azure, DBI::Id(schema = schema, table = "fact_vff_billetsalg_raw_RDS")))

DBI::dbDisconnect(con_azure)

