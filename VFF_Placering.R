install.packages("rvest")
install.packages("tidyverse")
install.packages("lubridate")


library(rvest)
library(tidyverse)
library(lubridate)


rm(list = ls())
cat("\014")

# Final placering

# --- Funktion Definition ---
hent_vff_runde_placering_endelig <- function(aarstal) {
  
  url <- paste0("https://superstats.dk/stilling/pladser-runde?season=", aarstal, "&klub=vff")
  saeson_str <- paste0(aarstal, "%2F", aarstal + 1)
  
  cat(paste("Prøver sæson:", saeson_str, "...\n"))
  
  tryCatch({
    webpage <- read_html(url)
    tabeller <- html_table(webpage, header = FALSE, fill = TRUE) 
    
    # KORREKTION 1: Brug det korrekte INDEX 1
    data <- tabeller[[1]] 
    
    # 
    vff_row <- data %>%
      # Rens X1, før vi sammenligner
      mutate(X1 = as.character(X1)) %>%
      mutate(X1 = str_trim(X1)) %>%
      filter(X1 == "VFF") # BRUGER X1 I STEDET FOR V1
    
    # 
    if (nrow(vff_row) == 0) {
      cat("  ❌ VFF række ikke fundet (muligvis 1. Division, ignorerer).\n")
      return(NULL)
    }
    
    # Vælg VFF's placering fra den SIDSTE KOLONNE
    final_placement <- vff_row[1, ncol(vff_row)]
    
    # Strukturér outputtet
    vff_data <- tibble(
      Sæson = saeson_str,
      Placering = as.numeric(final_placement)
    )
    
    # 4. Filter for KUN Superligaen (Ignorerer placeringer > 14)
    if (vff_data$Placering > 14) {
      cat(paste("  ⚠️ VFF placering er > 14 (", vff_data$Placering, "). Ignorerer 1. Div. sæson.\n"))
      return(NULL) 
    }
    
    cat("  ✅ Succes.\n")
    return(vff_data)
    
  }, error = function(e) {
    cat(paste("  ❌ Global Fejl i sæson", saeson_str, ":", e$message, "\n"))
    return(NULL) 
  })
}


# Opret input listen for startåret (2000 til 2024)
aarstal_inputs <- 2000:2024 

cat("\n--- Starter FINAL FINAL Scraping (X1 Korrektion) ---\n")

vff_grundspil_placering_final <- aarstal_inputs %>%
  map(hent_vff_runde_placering_endelig) %>%
  list_rbind()

# --- Verificér ---
print(vff_grundspil_placering_final)

# Gemmer dataen som en enkelt, komprimeret fil (.rds)
saveRDS(vff_grundspil_placering_final, "vff_superliga_placeringer_2000_2025.rds")

vff_placering_final <- readRDS("vff_superliga_placeringer_2000_2025.rds")


# Rundte placering 

hent_vff_alle_runder <- function(aarstal) {
  
  url <- paste0("https://superstats.dk/stilling/pladser-runde?season=", aarstal, "&klub=vff")
  saeson_str <- paste0(aarstal, "%2F", aarstal + 1)
  
  cat(paste("Prøver sæson:", saeson_str, "...\n"))
  
  tryCatch({
    webpage <- read_html(url)
    tabeller <- html_table(webpage, header = FALSE, fill = TRUE) 
    
    data <- tabeller[[1]] 
    
    # Filtrer KUN VFF rækken
    vff_row <- data %>%
      mutate(X1 = as.character(X1)) %>%
      mutate(X1 = str_trim(X1)) %>%
      filter(X1 == "VFF")
    
    # Tjek om rækken eksisterer
    if (nrow(vff_row) == 0) {
      cat("  ❌ VFF række ikke fundet (1. Division, ignorerer).\n")
      return(NULL)
    }
    
    # 
    vff_round_data <- vff_row %>%
      select(2:ncol(vff_row)) %>%
      # Gør alle placeringer numeriske
      mutate(across(everything(), as.numeric))
    
    
    # Vi skal renavngive kolonnerne fra X2, X3, X4 til Runde_1, Runde_2, etc.
    # Vi ved, at dataen starter fra kolonne 2 i den rå tabel (dvs. Runde 1)
    new_names <- paste0("Runde_", 1:(ncol(vff_row) - 1))
    colnames(vff_round_data) <- new_names
    
    # Tilføj Sæson kolonnen igen for at identificere rækken
    vff_round_data <- vff_round_data %>%
      mutate(Sæson = saeson_str, .before = 1)
    
    # Vi beholder ikke 1. Div. filteret her, da vi bare vil have VFFs runder.
    # Hvis dataen er skrabt, er den godkendt.
    
    cat("  ✅ Succes. Hentede alle runder.\n")
    return(vff_round_data)
    
  }, error = function(e) {
    cat(paste("  ❌ Global Fejl i sæson", saeson_str, ":", e$message, "\n"))
    return(NULL) 
  })
}

# Opret input listen for startåret (2000 til 2024)
aarstal_inputs <- 2000:2024 

cat("\n--- Starter FINAL FINAL Scraping (Alle Runder) ---\n")

vff_runde_placering_komplet <- aarstal_inputs %>%
  map(hent_vff_alle_runder) %>%
  list_rbind()

# --- Verificér ---
print(vff_runde_placering_komplet)

# Gemmer dataen som en enkelt, komprimeret fil (.rds)
saveRDS(vff_runde_placering_komplet, "vff_komplet_runde_placeringer_2000_2025.rds")




