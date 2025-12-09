# =============================================================================
# Pakker
# =============================================================================
# pacman bruges som “pakke-manager”:
#  - sikrer at nødvendige pakker er installeret
#  - loader dem i én samlet p_load(...)
#
# Her nøjes vi med de pakker, vi faktisk bruger i dette RAW-scrape:
#  - rvest    : HTML-scraping
#  - dplyr    : data-manipulation (tibbles, select, mutate, bind_rows)
#  - stringr  : str_* helpers til trim, regex mv.
#  - purrr    : map_dfr til at loope over tabeller/sæsoner
#  - tibble   : tibble() til at bygge data.frames på en pæn måde
suppressPackageStartupMessages({
  if (!requireNamespace("pacman", quietly = TRUE)) install.packages("pacman")
  pacman::p_load(
    rvest, dplyr, stringr, purrr, tibble
  )
})

# =============================================================================
# 1) Hjælpefunktion til at parse ÉN runde-tabel (RAW-niveau)
# =============================================================================
# VIGTIGT: Dette er et RAW-parse:
#  - Vi trækker kun det ud, som fremgår direkte af HTML-tabellen.
#  - Vi laver KUN den manipulation, der er nødvendig for overhovedet
#    at få data ud i kolonner (ikke beregninger som ugedag, dato-objekter osv.).
#
# Output-kolonner (tæt på kildens struktur):
#  - season_value   : sæson-parameteren fra URL’en (fx "2000")
#  - runde_header   : rå tekst i tabel-header, fx "Runde 1"
#  - runde_nr       : selve runde-nummeret (int) udtrukket fra header
#  - ugedag_raw     : tekst som står i første kolonne (mandag/tirsdag/... el.lign.)
#  - dato_tid_raw   : ren tekst fra 2. kolonne (fx "22/07 15:30")
#  - hold_raw       : tekst fra 3. kolonne (fx "BIF-AGF") – vi splitter IKKE her
#  - resultat_raw   : tekst/score fra 4. kolonne (fx "2-1") – ingen split til mål
#  - tilskuertal_raw: rå tekst fra 5. kolonne (fx "12.345") – ingen konvertering
#  - dommer_raw     : dommertekst fra 6. kolonne
#  - tv_kanal_raw   : alt-tekst på TV-logoet, hvis det findes – ellers NA
#
# Alt hvad der er “fortolkning” (dato-objekter, hjemmehold/udehold,
# antal mål, numeric tilskuertal osv.) flyttes til CLEAN/JOINS senere.
parse_runde_tabel_raw <- function(tbl_node, season_value) {
  
  # ---------------------------------------------------------------------------
  # 1. Læs runde-overskriften fra thead
  # ---------------------------------------------------------------------------
  runde_header <- tbl_node %>%
    html_element("thead tr th") %>%
    html_text2() %>%
    str_squish()
  
  # Udtræk runde-nummer som heltal, fx "Runde 1" → 1
  runde_nr <- str_extract(runde_header, "\\d+") %>% as.integer()
  
  # ---------------------------------------------------------------------------
  # 2. Find alle rækker i tbody
  # ---------------------------------------------------------------------------
  rows <- tbl_node %>% html_elements("tbody tr")
  if (length(rows) == 0) return(tibble())
  
  # ---------------------------------------------------------------------------
  # 3. Map over hver række og træk rå værdier ud
  # ---------------------------------------------------------------------------
  map_dfr(rows, function(row) {
    
    # Alle celler i rækken
    tds <- row %>% html_elements("td")
    
    # Vi forventer mindst 6 kolonner (ugedag, dato/tid, hold, resultat,
    # tilskuertal, dommer). TV-kolonne er typisk nr. 7 og kan mangle.
    if (length(tds) < 6) return(tibble())
    
    # Rå tekster, direkte fra HTML-tabellen, kun trimmet for whitespace
    ugedag_raw   <- html_text2(tds[1]) %>% str_squish()
    dato_tid_raw <- html_text2(tds[2]) %>% str_squish()
    hold_raw     <- html_text2(tds[3]) %>% str_squish()
    
    # Resultat ligger ofte i en <a> inde i 4. kolonne
    res_node <- tds[4] %>% html_element("a")
    resultat_raw <- if (!is.null(res_node)) {
      res_node %>% html_text2() %>% str_squish()
    } else {
      html_text2(tds[4]) %>% str_squish()
    }
    
    tilskuertal_raw <- html_text2(tds[5]) %>% str_squish()
    dommer_raw      <- html_text2(tds[6]) %>% str_squish()
    
    # TV-kanal (kan mangle) – vi gemmer alt-teksten på billedet som RAW
    tv_node <- tryCatch(
      tds[7] %>% html_element("img"),
      error = function(e) NULL
    )
    
    tv_kanal_raw <- if (!is.null(tv_node)) {
      html_attr(tv_node, "alt") %>% str_squish()
    } else {
      NA_character_
    }
    
    # Returner én række som tibble – 100 % RAW + let strukturering
    tibble(
      season_value    = season_value,   # parameteren vi scraper sæsonen med
      runde_header    = runde_header,   # fuld header-tekst
      runde_nr        = runde_nr,       # ekstra hjælp senere, men stadig direkte afledt
      ugedag_raw      = ugedag_raw,
      dato_tid_raw    = dato_tid_raw,
      hold_raw        = hold_raw,
      resultat_raw    = resultat_raw,
      tilskuertal_raw = tilskuertal_raw,
      dommer_raw      = dommer_raw,
      tv_kanal_raw    = tv_kanal_raw
    )
  })
}

# =============================================================================
# 2) Scrape én sæson (RAW)
# =============================================================================
# Denne funktion:
#  - bygger URL til en bestemt sæson,
#  - finder alle tabeller,
#  - vælger dem, hvis headeren matcher "Runde X",
#  - parser hver runde-tabel via parse_runde_tabel_raw.
scrape_superstats_saeson_raw <- function(season_value) {
  
  url <- paste0("https://superstats.dk/program?season=", season_value)
  cat("Henter sæson", season_value, "fra", url, "...\n")
  
  pg <- read_html(url)
  
  # Find alle <table>-elementer på siden
  alle_tabeller <- pg %>% html_elements("table")
  
  # Udvælg kun dem, hvor første <th> i thead starter med "Runde <tal>"
  er_runde <- vapply(
    alle_tabeller,
    function(tb) {
      head_node <- tb %>% html_element("thead tr th")
      
      # Hvis der ikke er noget header-node, er det ikke en runde-tabel
      if (is.null(head_node) || length(head_node) == 0) return(FALSE)
      
      txt <- tryCatch(
        head_node %>% html_text2() %>% str_squish(),
        error = function(e) ""
      )
      
      str_detect(txt, "^Runde\\s+\\d+")
    },
    logical(1)
  )
  
  runde_tabeller <- alle_tabeller[er_runde]
  
  if (length(runde_tabeller) == 0) {
    cat("Ingen runde-tabeller fundet for sæson", season_value, "\n")
    return(tibble())
  }
  
  # Parse alle runde-tabeller for denne sæson og bind dem sammen
  map_dfr(runde_tabeller, ~parse_runde_tabel_raw(.x, season_value))
}

# =============================================================================
# 3) Loop over flere sæsoner → samlet RAW-datasæt
# =============================================================================
# Her vælger du selv hvilke sæsoner, du vil have med.
# Eksempel: 2000–2026 som i din oprindelige kode.
saesoner_vec <- 2000:2026

superliga_program_raw <- map_dfr(saesoner_vec, scrape_superstats_saeson_raw)

cat("\nSamlet antal rækker i superliga_program_raw:", nrow(superliga_program_raw), "\n\n")

# Hurtig struktur- og indholdstjek i R:
glimpse(superliga_program_raw); View(superliga_program_raw)  

# =============================================================================
# 4) Upload til Azure som RAW-fact (PBA01_Raw)
# =============================================================================
# Nu hvor vi har et rent RAW-scrape i superliga_program_raw, skal det op som
# en RAW-tabel i Azure SQL. Det passer ind i din lagdeling:
#  - PBA01_Raw     : landings-/RAW-lag (ingen business-logik, minimal parsing)
#  - PBA02_Clean   : rensning, parsing af datoer, split af hold, mål osv.
#  - PBA03_JoinReady / marts-lag: join med billetdata, vejr, dim_dato, osv.
#
# Vi bruger samme connection-setup som i de andre scripts, men skriver eksplicit
# til schema = "PBA01_Raw".

library(DBI)
library(odbc)

con_azure <- dbConnect(
  odbc::odbc(),
  driver   = "ODBC Driver 18 for SQL Server",
  server   = Sys.getenv("AZURE_SQL_SERVER"),
  database = Sys.getenv("AZURE_SQL_DB"),
  uid      = Sys.getenv("AZURE_SQL_UID"),
  pwd      = Sys.getenv("AZURE_SQL_PWD"),
  port     = 1433,
  Encrypt  = "yes",
  TrustServerCertificate = "no",
  ConnectionTimeout      = 30
)

cat("Forbundet til Azure (", Sys.getenv("AZURE_SQL_DB"), ") – skriver til PBA01_Raw...\n\n", sep = "")

# Skriv/overskriv RAW-tabellen
DBI::dbWriteTable(
  conn      = con_azure,
  name      = DBI::Id(schema = "PBA01_Raw", table = "fact_superliga_program_raw"),
  value     = superliga_program_raw,
  overwrite = TRUE
)

cat("Tabel PBA01_Raw.fact_superliga_program_raw er nu opdateret.\n")

# (valgfrit) lille sanity check fra databasen
head_db <- DBI::dbReadTable(
  con_azure,
  DBI::Id(schema = "PBA01_Raw", table = "fact_superliga_program_raw")
)
print(head_db)

dbDisconnect(con_azure)
cat("🔚 Forbindelse til Azure lukket.\n")
