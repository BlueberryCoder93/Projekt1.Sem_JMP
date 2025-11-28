# I dette afsnit opbygger vi projektets datodimension, 
# som danner grundlag for tidsbaseret analyse i data warehouse-strukturen. 
# Formålet er at generere én samlet tabel, hvor hver dato mellem år 2000 
# og 2026 er beriget med centrale tidsattributter som ugedag, måned, kvartal 
# og weekendtype. Datodimensionen indlæses direkte i Azure SQL, hvorfra den kan 
# bruges af hele gruppens modeller og analyser.

# -------------------------------------------------------------------------
# Pakker
# -------------------------------------------------------------------------
# I starten af scriptet håndterer vi pakkeindlæsning. Vi bruger pacman, 
# som både kan installere manglende pakker og indlæse dem i én samlet funktion,
# hvilket sikrer, at scriptet kan køres på tværs af maskiner uden manuelt 
# installationsarbejde. 
# Pakkerne dplyr, lubridate, DBI, odbc og tibble anvendes,
# da de tilsammen muliggør datahåndtering, datooperationer og sikker 
# kommunikation med Azure SQL via R.

suppressPackageStartupMessages({
  if (!requireNamespace("pacman", quietly = TRUE)) install.packages("pacman")
  pacman::p_load(dplyr, lubridate, DBI, odbc, tibble)
})

# -------------------------------------------------------------------------
# Opsætning: start/slut-dato + danske navne
# -------------------------------------------------------------------------
# Her definerer vi hele det tidsinterval, der skal bruges i datodimensionen. 
# Start- og slutdato er valgt, så de dækker både historiske og fremtidige kampe, 
# hvilket sikrer fleksibilitet i modeller og rapportering. 
# Derudover oprettes lister med danske måneds- og ugedagsnavne, 
# så tabellen bliver let at læse og kan bruges direkte i rapporter og dashboard.

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
# 1) Byg datodimension i R
# -------------------------------------------------------------------------
# I dette afsnit konstruerer vi selve datodimensionen. 
# Først genereres en komplet liste af datoer i perioden, hvorefter hver dato
# beriges med centrale tidsattributter såsom ugenummer, kvartal, måned og år. 
# Derudover tilføjes en kategorisering af dagen som enten weekend eller hverdag,
# hvilket kan bruges til analyser af publikumsadfærd og aktivitetsniveau.

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
  select(-Dato_raw, -ugedag_nr) %>%      
  select(Datekey, Dato, år, kvartal, måned,
         måned_navn, uge, ugedag_navn, dagstype)

# -------------------------------------------------------------------------
# 2) Klargør til SQL: weekend som tekst ("weekend"/"hverdag")
# -------------------------------------------------------------------------
# Inden tabellen skrives til SQL, sikrer vi at datatyperne er konsistente.
# Dette trin omdanner alle kolonner til SQL-kompatible typer, eksempelvis 
# datoformatet og heltalstyper. 
# Weekend-kolonnen konverteres til tekst ("weekend"/"hverdag"), da det skaber 
# en mere naturlig og letforståelig tabel i vores datalager.

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
    weekend     = as.character(dagstype)
  )

# -------------------------------------------------------------------------
# 3) Forbindelse til Azure SQL – ALT login fra .Renviron
# -------------------------------------------------------------------------
# For at holde koden sikker og delevenlig anvender vi miljøvariabler
# til at hente servernavn, brugernavn og password. 
# På den måde indgår loginoplysninger aldrig direkte i koden, 
# hvilket både er god praksis og gør filen nem at dele i gruppen.
# DBI og odbc bruges til at etablere forbindelsen til Azure SQL.

server   <- Sys.getenv("AZURE_SQL_SERVER")
database <- Sys.getenv("AZURE_SQL_DB")
uid      <- Sys.getenv("AZURE_SQL_UID")
pwd      <- Sys.getenv("AZURE_SQL_PWD")

schema   <- "dbo"
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

# -------------------------------------------------------------------------
# 4) Skriv til dbo.dim_dato_raw (overskriver hvis den findes)
# -------------------------------------------------------------------------
# I dette afsnit uploades den færdige datodimension til Azure SQL.
# dbWriteTable håndterer al SQL-genereringen automatisk, 
# så vi undgår manuel SQL-kode. Tabellen overskrives ved kørsel, 
# hvilket sikrer, at vores database altid indeholder den nyeste version
# af datodimensionen, uanset hvem i gruppen der kører scriptet.

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

# -------------------------------------------------------------------------
# Valgfrit hurtigt tjek direkte fra SQL
# -------------------------------------------------------------------------
# Til slut foretager vi en simpel kontrol, hvor vi henter både antallet af rækker 
# og et udsnit af data direkte fra SQL-tabellen. 
# Disse SELECT-kald bruges kun til validering, og sikrer at tabellen 
# er korrekt oprettet og indeholder de forventede værdier. 
# Dette trin er særligt nyttigt under udvikling og dokumentation.

DBI::dbGetQuery(con, "SELECT COUNT(*) AS antal_rækker FROM dbo.dim_dato_raw;")
DBI::dbGetQuery(con, "SELECT TOP 10 * FROM dbo.dim_dato_raw ORDER BY Dato;")

DBI::dbDisconnect(con)


