library(DBI)
library(odbc)

# ------------------------------------------------------------
# Indtast login-oplysninger her
# ------------------------------------------------------------

# Brugernavn (det tilsendte)
Username <- ""   # eller lade den være tom: ""

# Password (det tilsendte)
Sys.setenv(AZURE_SQL_PWD = "")   # gemt som miljøvariabel


# ------------------------------------------------------------
# Opret forbindelse til Azure SQL
# ------------------------------------------------------------
con <- dbConnect(
  odbc::odbc(),
  Driver                 = "ODBC Driver 18 for SQL Server",
  Server                 = "jmp-pba-dataanalyse-2025.database.windows.net",
  Database               = "PBA_studiedata",
  UID                    = Username,
  PWD                    = Sys.getenv("AZURE_SQL_PWD"),
  Encrypt                = "yes",
  TrustServerCertificate = "no",
  Timeout                = 10
)


# ------------------------------------------------------------
# Test af tabellerne dim_helligdage, dim_dato og dim_skoleferie
# ------------------------------------------------------------

cat("Tester adgang til dim_helligdage:\n")
print(
  dbReadTable(
    con,
    DBI::Id(schema = "dbo", table = "dim_helligdage")
  ) |> head()
)

cat("\nTester adgang til dim_dato:\n")
print(
  dbReadTable(
    con,
    DBI::Id(schema = "dbo", table = "dim_dato")
  ) |> head()
)

cat("\nTester adgang til dim_skolesommerferie:\n")
print(
  dbReadTable(
    con,
    DBI::Id(schema = "dbo", table = "dim_skolesommerferie")
  ) |> head()
)


# ------------------------------------------------------------
# Luk forbindelse
# ------------------------------------------------------------
dbDisconnect(con)




