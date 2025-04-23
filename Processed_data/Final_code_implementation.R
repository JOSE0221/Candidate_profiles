#========================================================================
# ENHANCED POLITICAL CANDIDATE DATA HOMOLOGATION PIPELINE 
# - Improved field mapping with broader pattern detection
# - Robust error handling for missing fields
# - Standardized education levels with significance indicators
# - Public/private experience extraction by year (2000-2020)
# - Ideology classification based on text analysis
# - Advanced political economy metrics
#========================================================================

#========================================================================
# LIBRARY IMPORTS - Must be at the top
#========================================================================
library(digest)  # For creating hash-based check digits
library(tidyverse)
library(readxl)
library(haven)
library(stringi)
library(stringdist)
library(furrr)      # For parallel processing
library(lubridate)  # For date handling
library(janitor)    # For cleaning names
library(fs)         # For file system operations
library(vroom)      # For faster CSV reading
library(assertr)    # For data validation
library(glue)       # For string interpolation

#========================================================================
# CONFIGURATION AND SETUP
#========================================================================
setwd("~/Documents/GitHub/Candidates_databases/Processed_data")

# Script configuration
config <- list(
  input_dir = "input_data",
  output_dir = "output_data",
  report_dir = "reports",
  log_dir = "logs",
  visualization_dir = "visualizations",
  parallel_workers = future::availableCores() - 1,
  run_id = format(Sys.time(), "%Y%m%d_%H%M%S")
)

# Create directory structure
for (dir in c(config$output_dir, config$report_dir, config$log_dir, config$visualization_dir)) {
  dir_create(dir, recurse = TRUE)
}

# Initialize logging
log_file <- file.path(config$log_dir, paste0("homologation_log_", config$run_id, ".txt"))
con <- file(log_file, "w")
sink(con, append = TRUE, type = "output")
sink(con, append = TRUE, type = "message")

# Log script start
cat("===================================================================\n")
cat("ENHANCED POLITICAL CANDIDATE DATA HOMOLOGATION PIPELINE 2.0\n")
cat("Started at:", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")
cat("Run ID:", config$run_id, "\n")
cat("===================================================================\n\n")

#========================================================================
# TEXT PROCESSING AND COLUMN MATCHING FUNCTIONS
# Define these early as they're used by many other functions
#========================================================================

# Enhanced text normalization with more comprehensive cleaning
normalize_text <- function(x, aggressive = FALSE) {
  if(!is.character(x)) return(x)
  
  # Basic normalization
  result <- x %>%
    stringi::stri_trans_general("Latin-ASCII") %>%  # Remove accents
    trimws() %>%                                    # Remove leading/trailing whitespace
    tolower()                                       # Convert to lowercase
  
  if(aggressive) {
    # More aggressive normalization for matching
    result <- result %>%
      gsub("[[:punct:][:space:]]", "", .) %>%      # Remove punctuation and spaces
      gsub("[^a-z0-9]", "", .)                     # Keep only alphanumeric
  }
  
  return(result)
}

# More sophisticated column matching with fuzzy matching capability
find_column_enhanced <- function(df, patterns, fuzzy_threshold = 0.85) {
  if(length(patterns) == 0 || ncol(df) == 0) return(NULL)
  
  # Get column names and normalize
  cols <- names(df)
  cols_lower <- tolower(cols)
  patterns_lower <- tolower(patterns)
  
  # Try exact matches first
  for(pattern in patterns_lower) {
    exact_matches <- which(cols_lower == pattern)
    if(length(exact_matches) > 0) {
      cat("    EXACT match found for pattern:", pattern, "->", cols[exact_matches[1]], "\n")
      return(cols[exact_matches[1]])
    }
  }
  
  # Try pattern matches next
  for(pattern in patterns_lower) {
    # Direct contains
    contains_matches <- which(grepl(pattern, cols_lower, fixed = TRUE))
    if(length(contains_matches) > 0) {
      cat("    CONTAINS match found for pattern:", pattern, "->", cols[contains_matches[1]], "\n")
      return(cols[contains_matches[1]])
    }
    
    # Clean pattern and try again
    clean_pattern <- gsub("[[:punct:][:space:]]", "", pattern)
    clean_cols <- gsub("[[:punct:][:space:]]", "", cols_lower)
    if(nchar(clean_pattern) > 3) { # Only for patterns > 3 chars to avoid false matches
      clean_matches <- which(grepl(clean_pattern, clean_cols, fixed = TRUE))
      if(length(clean_matches) > 0) {
        cat("    CLEANED match found for pattern:", pattern, "->", cols[clean_matches[1]], "\n")
        return(cols[clean_matches[1]])
      }
    }
  }
  
  # Try fuzzy matching as a last resort for longer patterns
  for(pattern in patterns_lower) {
    if(nchar(pattern) > 5) { # Only for longer patterns to avoid false matches
      distances <- stringdist::stringdistmatrix(pattern, cols_lower, method = "jw")
      best_match <- which.min(distances)
      if(distances[best_match] < (1 - fuzzy_threshold)) {
        cat("    FUZZY match found for pattern:", pattern, "->", cols[best_match], 
            "(similarity:", round(1 - distances[best_match], 2), ")\n")
        return(cols[best_match])
      }
    }
  }
  
  return(NULL)
}

sanitize_dataframe_types <- function(df) {
  cat("Performing data type sanity checks...\n")
  
  # Handle character columns with mixed content
  char_cols <- names(df)[sapply(df, is.character)]
  for(col in char_cols) {
    # Check for potential logical values in character columns
    logical_values <- sum(tolower(df[[col]]) %in% 
                            c("true", "false", "t", "f", "yes", "no", "y", "n"), na.rm = TRUE)
    
    # If we have mixed logical and character, ensure everything stays as character
    if(logical_values > 0 && logical_values < nrow(df)) {
      cat("  Found mixed logical/character values in column", col, "\n")
      # Make sure all values are proper character strings
      df[[col]] <- as.character(df[[col]])
    }
  }
  
  # Fix the problematic row 1188 if it exists
  if(nrow(df) >= 1188) {
    # Get all columns where row 1188 might contain the problematic string
    for(col in names(df)) {
      if(!is.na(df[[col]][1188]) && 
         grepl("Francisco I Madero 2019,centro,86750,frontera tabasco", df[[col]][1188], fixed=TRUE)) {
        cat("  Found problematic value in row 1188, column", col, "\n")
        
        # If this is a logical column, convert to character
        if(is.logical(df[[col]])) {
          df[[col]] <- as.character(df[[col]])
        }
        
        # Split the problematic value if it contains commas and it's a single-value field
        if(grepl(",", df[[col]][1188], fixed=TRUE)) {
          # Just take the first part before comma
          df[[col]][1188] <- strsplit(df[[col]][1188], ",")[[1]][1]
          cat("  Fixed by extracting first part:", df[[col]][1188], "\n")
        }
      }
    }
  }
  
  return(df)
}

#========================================================================
# ENHANCED FILE READING FUNCTIONS
#========================================================================

# Function to auto-detect file encoding
detect_encoding <- function(file_path) {
  # List of encodings to try, prioritizing UTF-8
  encodings <- c("UTF-8", "latin1", "Windows-1252")
  
  for (enc in encodings) {
    tryCatch({
      con <- file(file_path, open = "r", encoding = enc)
      lines <- readLines(con, n = 5)
      close(con)
      return(enc)
    }, error = function(e) {
      # Try next encoding
    })
  }
  
  # Default to UTF-8 if no encoding works
  return("UTF-8")
}

read_any_file <- function(file_path, sheet = 1) {
  cat("  Reading file:", basename(file_path), "\n")
  
  tryCatch({
    ext <- tolower(tools::file_ext(file_path))
    
    if(ext %in% c("xlsx", "xls")) {
      # Excel file handling code remains the same
      # ...
    } else if(ext == "csv") {
      # Detect encoding
      encoding <- detect_encoding(file_path)
      cat("    CSV file with detected encoding:", encoding, "\n")
      
      # Try to detect delimiter by examining first few lines
      sample_lines <- readLines(file_path, n = 10, encoding = encoding)
      delimiter <- ","  # Default
      
      # Count delimiters in sample
      comma_count <- sum(stringr::str_count(sample_lines, ","))
      semicolon_count <- sum(stringr::str_count(sample_lines, ";"))
      tab_count <- sum(stringr::str_count(sample_lines, "\t"))
      
      # Choose most common delimiter
      if(semicolon_count > comma_count && semicolon_count > tab_count) {
        delimiter <- ";"
        cat("    Detected semicolon delimiter\n")
      } else if(tab_count > comma_count && tab_count > semicolon_count) {
        delimiter <- "\t"
        cat("    Detected tab delimiter\n")
      } else {
        cat("    Using comma delimiter\n")
      }
      
      # Try with vroom with enhanced parameters
      tryCatch({
        df <- suppressWarnings(vroom(file_path, 
                                     delim = delimiter,
                                     locale = locale(encoding = encoding),
                                     show_col_types = FALSE,
                                     altrep = TRUE,
                                     quote = "\"",
                                     escape_double = TRUE,
                                     escape_backslash = FALSE,
                                     comment = "",
                                     skip_empty_rows = TRUE,
                                     trim_ws = TRUE,
                                     na = c("", "NA", "N/A", "#N/A", "NULL", "<NA>"),
                                     col_types = cols(.default = col_character())))  # Default all to character
      }, error = function(e) {
        # Fall back to read_csv with robust parameters
        cat("    Failed with vroom, falling back to read_csv\n")
        df <- suppressWarnings(read_csv(file_path, 
                                        locale = locale(encoding = encoding),
                                        show_col_types = FALSE,
                                        quote = "\"",
                                        escape_double = TRUE,
                                        escape_backslash = FALSE,
                                        comment = "",
                                        skip_empty_rows = TRUE,
                                        trim_ws = TRUE,
                                        na = c("", "NA", "N/A", "#N/A", "NULL", "<NA>"),
                                        col_types = cols(.default = col_character())))
      })
    } else if(ext == "dta") {
      cat("    Stata file\n")
      df <- read_dta(file_path)
    } else {
      stop(paste("Unsupported file extension:", ext))
    }
    
    # Basic cleanup of column names
    df <- df %>% clean_names(case = "snake")
    
    # Fix potential data type issues (like mixed logical/character values)
    for(col in names(df)) {
      # Check for logical columns with character values
      if(is.logical(df[[col]])) {
        # Check for problematic values
        problematic_rows <- which(is.na(df[[col]]) & !is.na(as.character(df[[col]])))
        if(length(problematic_rows) > 0) {
          cat("    Converting column", col, "to character type due to mixed data\n")
          df[[col]] <- as.character(df[[col]])
        }
      }
    }
    
    # Report on the dataframe
    cat("    Successfully read dataframe with", nrow(df), "rows and", ncol(df), "columns\n")
    df <- sanitize_dataframe_types(df)
    
    return(df)
  }, error = function(e) {
    cat("    ERROR reading file:", e$message, "\n")
    return(NULL)
  })
}



#========================================================================
# COMPREHENSIVE FIELD MAPPING WITH EXPANDED PATTERNS
#========================================================================

# Enhanced field mapping function with unmatched column incorporation
enhance_field_mapping_from_report <- function(mapping, report_file) {
  if(file.exists(report_file)) {
    cat("Reading unmatched columns report:", report_file, "\n")
    unmatched_df <- read_csv(report_file, show_col_types = FALSE)
    
    # Process each unmatched column with Mexican political context awareness
    for(i in 1:nrow(unmatched_df)) {
      col <- tolower(unmatched_df$unmatched_column[i])
      
      # Apply heuristics based on common Mexican political terminology
      if(grepl("nombre|name|candidat", col))
        mapping$nombre_completo <- c(mapping$nombre_completo, col)
      else if(grepl("genero|sexo|gender|hombre|mujer", col))
        mapping$sexo_genero <- c(mapping$sexo_genero, col)
      else if(grepl("partido|coalicion|coalición|fuerza|politica|política", col))
        mapping$partido <- c(mapping$partido, col)
      else if(grepl("cargo|puesto|postula|candida|aspira", col))
        mapping$cargo <- c(mapping$cargo, col)
      else if(grepl("educa|escola|estudio|formacion|formación|académic", col))
        mapping$educacion <- c(mapping$educacion, col)
      else if(grepl("experien|trayectoria|histori|historia|profesional", col))
        mapping$historia_profesional <- c(mapping$historia_profesional, col)
      else if(grepl("propuesta|oferta|promesa|compromiso", col)) {
        if(grepl("1|uno|primera", col))
          mapping$propuesta_1 <- c(mapping$propuesta_1, col)
        else if(grepl("2|dos|segunda", col))
          mapping$propuesta_2 <- c(mapping$propuesta_2, col)
        else if(grepl("3|tres|tercera", col))
          mapping$propuesta_3 <- c(mapping$propuesta_3, col)
        else if(grepl("genero|género", col))
          mapping$propuesta_genero <- c(mapping$propuesta_genero, col)
        else
          mapping$propuesta_1 <- c(mapping$propuesta_1, col) # Default to propuesta_1
      }
    }
    
    cat("Added", nrow(unmatched_df), "unmatched columns to field mapping\n")
  }
  return(mapping)
}

# Enhanced field mapping function to cover more variations
create_enhanced_field_mapping <- function(header_analysis = NULL, unmatched_report = NULL) {
  # Process header analysis data if available
  if(!is.null(header_analysis)) {
    cat("Incorporating", length(header_analysis$clusters), "header clusters into field mapping\n")
  }
  
  # Expanded field mapping with many more variations and alternative patterns
  mapping <- list(
    #===== IDENTIFICATION AND PERSONAL INFORMATION =====
    "id_candidato" = c(
      # Basic identifiers
      "id", "id_candidato", "id_candidatura", "candidate_id", "idCandidato", 
      "identificador", "clave", "clave_candidato", "folio", "id_postulacion",
      # List and formula numbers
      "num_lista", "num_lista_o_formula", "numero_lista", "numero_de_formula",
      "numListaDiputado", "numListaSenador", "idNumFormula", "no_lista_o_formula",
      "num_de_lista", "numero_formula", "posicion_lista", "posicion",
      # Additional variations observed in logs
      "folio", "idCandidatura", "id_num_formula", "numero_lista_o_formula",
      "no_de_lista", "no_lista", "idNumFormula", "lugar_de_lista", 
      "lugar_de_planilla", "num_lista_senador", "num_lista_diputado", "numero_de_formula"
    ),
    
    "nombre_completo" = c(
      # Full name patterns
      "nombre_completo", "nombre_candidato", "nombre_candidatura", "candidate_name", 
      "nombreCandidato", "nombre_propietario", "nombre_del_candidato",
      "nombre_de_la_candidatura", "nombre_candidata_to", "nombre_y_alias",
      "nombre con alias", "candidatura_propietaria", "nombre_de_la_candidatura",
      # English patterns
      "full_name", "name", "name_without_title", "clean_name_for_photo",
      # Additional variations observed in logs
      "nombre_candidata_to", "candidatura_propietaria", "nombreCandidato"
    ),
    
    "nombre" = c(
      "nombre", "nombres", "name", "primer_nombre", "nombre_pila", "nombre"
    ),
    
    "apellido_paterno" = c(
      "apellido_paterno", "primer_apellido", "apellido1", "ap_paterno", "paterno",
      "primer apellido", "paterno"
    ),
    
    "apellido_materno" = c(
      "apellido_materno", "segundo_apellido", "apellido2", "ap_materno", "materno",
      "segundo apellido", "materno"
    ),
    
    "sobrenombre" = c(
      "sobrenombre", "alias", "nombreCorto", "apodo", "nombre_corto", "sobrenom",
      "alias", "sobrenom", "nombre_corto", "nombre con alias", "apodo"
    ),
    
    "fecha_nacimiento" = c(
      "fecha_nacimiento", "fechaNacimiento", "birth_date", "birth_date_iso", 
      "fecha_de_nacimiento", "nacimiento", "fecha_nac",
      "birthdate", "birth_date", "fecha_de_nacimiento", "fecha nac"
    ),
    
    "edad" = c(
      "edad", "age", "anos", "años", "edad", "age"
    ),
    
    "sexo_genero" = c(
      # Gender variations
      "sexo", "genero", "género", "sexo_genero", "gender",
      "masculino_femenino", "hombre_mujer", "m_f", "h_m",
      # Additional patterns observed
      "genero", "sexo", "gender"
    ),
    
    #===== EDUCATION AND TRAINING =====
    "educacion" = c(
      # General education terms
      "educacion", "educación", "estudios", "escolaridad", "nivel_educativo",
      "formacion", "formación", "nivel_academico", "nivel_académico",
      "grado_academico", "grado_académico", "preparacion_academica",
      
      # Specific education fields
      "grado_maximo", "grado_máximo", "grado_maximo_de_estudios", "grado_máximo_de_estudios",
      "maximo_grado_de_estudios", "máximo_grado_de_estudios", "ultimo_grado_estudios",
      "último_grado_estudios", "curriculum_académica", "nivel_escolar",
      
      # Mexican specific terms
      "formacion_academica", "formación_académica", "estudios_realizados",
      "especialidad", "carrera", "carrera_profesional", "profesion", "profesión",
      "título", "titulo_profesional", "grado_escolar", "grado_escolaridad",
      
      # English equivalents
      "education", "educational_level", "academic_degree", "highest_degree",
      "education_level", "academic_background", "educational_background",
      "idGrado", "curriculum_escolaridad_y_preparación_académica",
      
      # Additional patterns observed
      "escolaridad", "estudios", "preparacion_academica", 
      "grado_maximo_estudio", "grado_maximo_de_estudio", "grado_academico",
      "nivel_educativo", "id_grado", "grado_maximo_estudios_y_su_estatus"
    ),
    
    "estatus_educacion" = c(
      # Status of education
      "estatus_educacion", "estatus_educación", "estatus_escolaridad", "estatus_de_estudios",
      "estatus_de_grado", "estatus_de_grado_de_estudios", "estado_estudios",
      "situacion_educativa", "situación_educativa", "estatus_grado", "estatus_grado_de_estudios",
      "grado_academico_estatus", "grado_académico_estatus", 
      
      # Spanish variations
      "completado", "incompleto", "en_curso", "trunco", "terminado", "titulado",
      "pasante", "egresado", "graduado", "culminado", "cursando",
      
      # English equivalents
      "education_status", "degree_status", "completed_education",
      "ESTATUS", "ESTATUS DE ESTUDIOS", "ESTATUS_ESCOLARIDAD",
      
      # Additional patterns observed
      "estatus_escolaridad", "estatus_de_estudios", "estatus_grado",
      "estatus_val", "estatus_maximo_grado_estudios", "estatus_de_grado_maximo_de_estudios",
      "estatus_grado_de_estudio", "estatus", "grado_academico_estatus"
    ),
    
    "cursos" = c(
      # Courses and additional training
      "cursos", "diplomados", "seminarios", "talleres", "capacitaciones",
      "otra_formacion", "otra_formación", "formacion_adicional", "formación_adicional",
      "otra_formacion_academica", "otra_formación_académica", "cursos_diplomados",
      "estudios_adicionales", "educacion_continua", "educación_continua",
      "certificaciones", "especializaciones", "actualizaciones",
      
      # English equivalents
      "courses", "trainings", "workshops", "additional_education", "continuing_education",
      "professional_development", "CURSOS",
      
      # Additional patterns observed  
      "otra_formacion_academica", "otra_formacion", "otros_estudios",
      "otra_formacion_academica_cursos_diplomados_seminarios_etcetera"
    ),
    
    #===== EXPERIENCE =====
    "historia_profesional" = c(
      # Professional experience
      "historia_profesional", "historia_profesional_y_o_laboral", "experiencia_profesional",
      "experiencia_laboral", "trayectoria_profesional", "trayectoria_laboral",
      "carrera_profesional", "historial_profesional", "vida_profesional",
      "curriculum_profesional", "experiencia_trabajo", "historia_profesional_laboral",
      
      # Mexican specific terms
      "actividad_profesional", "actividades_profesionales", "campo_profesional",
      "desempeño_profesional", "ejercicio_profesional", "historio_profesional",
      
      # English equivalents
      "professional_experience", "work_experience", "professional_background",
      "professional_history", "career_history", "employment_history",
      "curriculum_administrativa", "descripcionHLC", "HISTORIA PROFESIONAL Y/O LABORAL",
      "HISTORIA_PROFESIONAL", "curriculum_empresarial/iniciativa_privada", "curriculum",
      
      # Additional patterns observed
      "historia_profesional_y_o_laboral", "historial_profesional", "historia_profesional_laboral",
      "historia_profesionaly_o_laboral", "descripcion_hlc", "historio_profesional",
      "historial_profesional_y_o_laboral", "professional_background", "professional_experience"
    ),
    
    "experiencia_docencia" = c(
      # Teaching experience
      "experiencia_docencia", "experiencia_docente", "docencia", "enseñanza",
      "experiencia_academica", "experiencia_académica", "profesor", "profesora",
      "catedratico", "catedrático", "maestro", "maestra", "docente",
      "instructor", "instructora", "capacitador", "capacitadora",
      "experiencia_enseñanza", "experiencia_magisterial", "carrera_docente",
      
      # English equivalents
      "teaching_experience", "academic_experience", "teaching_background",
      "faculty_experience", "educational_experience", "professor_experience",
      "curriculum_investigación_y_docencia",
      
      # Additional observed patterns
      "curriculum_investigacion_y_docencia", "docencia", "teaching_experience"
    ),
    
    "trayectoria_politica" = c(
      # Political trajectory
      "trayectoria_politica", "trayectoria_política", "historia_politica", "historia_política",
      "experiencia_politica", "experiencia_política", "carrera_politica", "carrera_política",
      "vida_politica", "vida_política", "historial_politico", "historial_político",
      "participacion_politica", "participación_política", "militancia_politica", "militancia_política",
      
      # Political activities and variations
      "trayectoria_politica_y_o_participacion_social", "trayectoria_política_y_o_participación_social",
      "trayectoria_politica_y_o_participacion_organizaciones_ciudadanas",
      "trayectoria_politica_y_o_organizaciones_ciudadanas_o_sociedad_civil",
      "actividad_politica", "actividad_política", "activismo_politico", "activismo_político",
      "participacion_partidista", "participación_partidista", 
      
      # English equivalents
      "political_career", "political_experience", "political_background",
      "political_history", "political_trajectory", "political_activities",
      "curriculum_política", "descripcionTP", "TRAYECTORIA POLITICA", "TRAYECTORIA_POLITICA",
      "TRAYECTORIA POLÍTICA", "experiencia_legislativa", "curriculum_legislativa",
      
      # Additional observed patterns
      "trayectoria_politica_y_o_organizaciones_ciudadanas_o_sociedad_civil",
      "trayectoria_politica_y_o_participacion_social_en_organizaciones_ciudadanas_o_de_la_sociedad_civil",
      "trayectoria_politica_y_o_participacion_organizaciones_ciudadanas_o_sociedad_civil",
      "descripcion_tp", "experiencia_legislativa", "political_background", "trayectoria"
    ),
    
    "cargos_publicos" = c(
      # Public positions
      "cargos_publicos", "cargos_públicos", "puestos_publicos", "puestos_públicos",
      "servicio_publico", "servicio_público", "funcion_publica", "función_pública",
      "cargos_gobierno", "puestos_gobierno", "administracion_publica", "administración_pública",
      "servidor_publico", "servidor_público", "funcionario_publico", "funcionario_público",
      "puestos_desempenados", "puestos_desempeñados", "cargos_desempenados", "cargos_desempeñados",
      
      # Additional detected patterns based on logs
      "puestos_desempenados", "cargos_gobierno", "cargos_publicos_anteriores",
      "puestos_publicos", "cargos_anteriores", "servicio_publico_anterior"
    ),
    
    "descripcion_candidato" = c(
      # Candidate description and narrative
      "descripcion_candidato", "descripción_candidato", "acerca_del_candidato",
      "sobre_el_candidato", "sobre_la_candidata", "biografia", "biografía",
      "semblanza", "perfil", "resumen_biografico", "resumen_biográfico",
      "sintesis_curricular", "síntesis_curricular", "resumen_curricular",
      "informacion_personal", "información_personal", "background",
      
      # English equivalents
      "candidate_description", "about_candidate", "biography", "profile",
      "personal_information", "background_information", "candidate_background",
      "descripcionCandidato"
    ),
    
    #===== PROPOSALS AND MOTIVATION =====
    "propuesta_1" = c(
      # First/main proposal
      "propuesta_1", "propuesta1", "propuesta_principal", "propuesta_principal_1",
      "primera_propuesta", "propuesta_uno", "propuesta_prioritaria",
      "propuesta_destacada", "propuesta_central", "propuesta_clave",
      "propuesta_principal_1", "propuesta_num_1", "propuesta_número_1",
      "propuesta_primaria", "propuesta_inicial", "primera_promesa",
      "compromiso_1", "compromiso_principal", "oferta_1", "oferta_principal",
      
      # English equivalents
      "proposal_1", "main_proposal", "first_proposal", "primary_proposal",
      "key_proposal", "central_proposal", "PROPUESTA 1", "PROPUESTA_1",
      
      # Additional observed patterns
      "propuesta_num_1", "propuesta1", "propuestas_principales",
      "cual_es_la_primera_de_sus_dos_principales_propuestas", "propuesta_principal_1"
    ),
    
    "propuesta_2" = c(
      # Second proposal
      "propuesta_2", "propuesta2", "propuesta_principal_2", "segunda_propuesta",
      "propuesta_dos", "propuesta_secundaria", "propuesta_adicional",
      "propuesta_complementaria", "propuesta_num_2", "propuesta_número_2",
      "propuesta_2da", "segunda_promesa", "compromiso_2", "compromiso_secundario",
      "oferta_2", "oferta_secundaria", 
      
      # English equivalents
      "proposal_2", "second_proposal", "secondary_proposal", "additional_proposal",
      "complementary_proposal", "PROPUESTA 2", "PROPUESTA_2",
      
      # Additional observed patterns
      "propuesta_num_2", "propuesta2", "propuesta_principal_2",
      "cual_es_la_segunda_de_sus_dos_principales_propuestas"
    ),
    
    "propuesta_3" = c(
      # Third proposal
      "propuesta_3", "propuesta3", "propuesta_principal_3", "tercera_propuesta",
      "propuesta_tres", "propuesta_terciaria", "propuesta_extra",
      "propuesta_num_3", "propuesta_número_3", "propuesta_3ra", "tercera_promesa",
      "compromiso_3", "compromiso_terciario", "oferta_3", "oferta_terciaria",
      
      # English equivalents
      "proposal_3", "third_proposal", "tertiary_proposal", "extra_proposal",
      
      # Additional observed patterns
      "propuesta_num_3", "propuesta3", "propuesta_principal_3"
    ),
    
    "propuesta_genero" = c(
      # Gender equity proposals
      "propuesta_genero", "propuesta_género", "propuesta_en_materia_de_genero",
      "propuesta_en_materia_de_género", "propuesta_equidad", "propuesta_igualdad",
      "propuesta_mujeres", "propuesta_perspectiva_genero", "propuesta_perspectiva_género",
      "propuesta_diversidad", "propuesta_inclusion", "propuesta_inclusión",
      "propuesta_paridad", "propuesta_discriminacion", "propuesta_discriminación",
      "propuesta_materia_genero", "propuesta_materia_género", "propuesta_enfoque_genero",
      "propuesta_enfoque_género", "propuestasgenero", "propuesta_de_genero", "propuesta_de_género",
      
      # Additional observed patterns
      "propuesta_en_materia_de_genero", "propuesta_de_genero", "propuestasgenero",
      "propuesta_en_materia_de_genero_o_del_grupo_en_situacion_de_discrminacion_que_representa",
      "propuesta_en_materia_de_genero_o_en_su_caso_del_grupo_en_situacion_de_discriminacion_que_representa",
      "propuestas_en_materia_de_genero", "propuesta_genero_gap",
      "propuesta_en_materia_de_genero_o_adicional", "propuesta_grupo_vulnerable",
      "propuesta_en_materia_de_genero_o_del_grupo_en_situacion_de_discrminacion"
    ),
    
    "motivo_cargo_publico" = c(
      # Motivation for seeking public office
      "motivo_cargo_publico", "motivo_cargo_público", "por_que_quiero_ocupar_un_cargo_publico",
      "por_qué_quiero_ocupar_un_cargo_público", "por_que_ocupar_un_cargo_publico",
      "por_qué_ocupar_un_cargo_público", "razon_candidatura", "razón_candidatura",
      "motivo_postulacion", "motivo_postulación", "motivacion_politica", "motivación_política",
      "por_que_quiere_ocupar_el_cargo", "por_qué_quiere_ocupar_el_cargo",
      "motivo_del_cargo_publico", "motivo_de_la_postulacion", "motivo_candidatura",
      "porque_cargo_publico", "motivo_de_cargo_publico", "motivo_de_la_postulacion_al_cargo_publico",
      
      # English equivalents
      "reason_for_running", "motivation_for_office", "why_seeking_office",
      "purpose_of_candidacy", "candidacy_motivation", "public_service_motivation",
      "POR QUE QUIERO OCUPAR UN CARGO PUBLICO", "MOTIVO_CARGO_PUBLICO",
      
      # Additional observed patterns
      "por_que_quiero_ocupar_un_cargo_publico", "por_que_quiere_ocupar_un_cargo_publico",
      "por_que_quiere_ocupar_el_cargo", "por_que_ocupar_un_cargo_publico",
      "motivo_de_cargo_publico", "motivo_del_cargo_publico", "porque_cargo_publico",
      "por_que_quiere_ocupar_un_cargo_publico", "candidacy_reason"
    ),
    
    #===== ELECTION AND CANDIDACY =====
    "partido" = c(
      # Political party
      "partido", "partido_politico", "partido_político", "partido_coalicion",
      "partido_coalición", "afiliacion_partidista", "afiliación_partidista",
      "organizacion_politica", "organización_política", "instituto_politico",
      "instituto_político", "fuerza_politica", "fuerza_política", 
      "siglas_partido", "emblema", "partido_postulante", "partido_origen",
      "partido_coalicion_candidatura", "partido_coalicion_o_candidatura_comun",
      "siglas", "actor_politico", "actor_político",
      
      # English equivalents
      "party", "political_party", "party_affiliation", "political_organization",
      "party_acronym", "PARTIDO POLITICO", "PARTIDO_COALICION", "party_name",
      "tipoAsociacion",
      
      # Additional observed patterns
      "partido_coalicion", "partido_politico", "partido_coalicion_candidatura",
      "fuerza_politica", "fuerza_politica_postulante", "party_name", "tipo_asociacion",
      "partido_coalicion_o_candidatura_comun", "partido_politico_coalicion_candidatura_independiente",
      "actor_politico", "actor_politico_nombre"
    ),
    
    "cargo" = c(
      # Position sought
      "cargo", "puesto", "cargo_al_que_aspira", "cargo_eleccion", "cargo_elección",
      "cargo_postulacion", "cargo_postulación", "tipo_cargo", "posicion", "posición",
      "candidatura_cargo", "cargo_publico", "cargo_público", "cargo_a_ocupar",
      "cargo_pretendido", "cargo_aspiracion", "cargo_aspiración", "puesto_eleccion",
      "puesto_elección", "tipo_candidatura", "tipo_de_candidatura",
      
      # English equivalents
      "position", "office", "role", "elected_office", "candidacy_position",
      "type_of_office", "CARGO", "chamber", "TIPO_CANDIDATO", "TIPO CANDIDATO",
      "chamber_type",
      
      # Additional observed patterns
      "tipo_candidato", "tipo_candidatura", "tipo_de_candidatura", "chamber", 
      "puesto", "candidatura", "caracter", "chamber_type"
    ),
    
    "entidad" = c(
      # State/entity
      "entidad", "estado", "entidad_federativa", "entidad_federacion",
      "estado_republica", "estado_república", "estado_federacion", "estado_federación",
      "demarcacion_estatal", "demarcación_estatal", "nivel_estatal", "estado_eleccion",
      "estado_elección", "entidad_eleccion", "entidad_elección", "estado_origen",
      "entidad_origen", "estado_natal", "id_estado_eleccion", "nombre_estado",
      
      # English equivalents
      "state", "entity", "federal_entity", "state_level", "ENTIDAD", 
      "entidadFederativa", "electoral_entity", "idEstadoEleccion",
      
      # Additional observed patterns
      "entidad_federativa", "estado", "entidad", "id_estado_eleccion", "electoral_entity",
      "statebth"
    ),
    
    "municipio" = c(
      # Municipality
      "municipio", "municipalidad", "ayuntamiento", "localidad", "demarcacion_municipal",
      "demarcación_municipal", "nivel_municipal", "gobierno_municipal", "alcaldia",
      "alcaldía", "delegacion", "delegación", "mpio", "nombre_municipio",
      "municipio_distrito", "municipio_local", "id_municipio", "clave_municipio",
      "municipio_cabecera", "cabecera_municipal",
      
      # English equivalents
      "municipality", "township", "local_government", "municipal_level", "town",
      "MUNICIPIO", "nombreMunicipio", "Municipio",
      
      # Additional observed patterns
      "nombre_municipio", "municipio_local", "municipio_distrito"
    ),
    
    "distrito" = c(
      # District
      "distrito", "distrito_electoral", "distrito_federal", "distrito_local",
      "demarcacion_distrital", "demarcación_distrital", "circuito_electoral",
      "seccion_electoral", "sección_electoral", "dtto", "dist", "clave_distrito",
      "id_distrito", "num_distrito", "número_distrito", "distrito_uninominal",
      "distrito_id", "id_distrito_eleccion", "distrito_nombre", "distrito_numero",
      "distrito_o_municipio",
      
      # English equivalents
      "district", "electoral_district", "constituency", "precinct", "DISTRITO",
      "nombreDistrito", "DISTRITO_FEDERAL", "idDistritoEleccion",
      
      # Additional observed patterns
      "distrito_local", "distrito_federal", "id_distrito_eleccion",
      "distrito_nombre", "distrito_numero", "distrito_o_municipio", "distritolocal"
    ),
    
    "eleccion" = c(
      # Election
      "eleccion", "elección", "tipo_eleccion", "tipo_elección", "proceso_electoral",
      "jornada_electoral", "elecciones", "comicios", "votacion", "votación",
      "año_electoral", "fecha_eleccion", "fecha_elección", "evento_electoral",
      "orden_eleccion", "orden_elección", "tipo_proceso", "ambito_electoral",
      "ámbito_electoral", "tipo_eleccion", "proceso_electoral_concurrente",
      
      # English equivalents
      "election", "electoral_process", "voting", "ballot", "ELECCION", "Eleccion", "Tipo",
      
      # Additional observed patterns
      "tipo_eleccion", "eleccion", "proceso_electoral", "comicio", "elecciones",
      "jornada_electoral", "tipo_proceso", "id_estado_eleccion", "election", "proceso_electoral_concurrente"
    ),
    
    #===== CONTACT INFORMATION =====
    "correo" = c(
      # Email address
      "correo", "correo_electronico", "correo_electrónico", "email", "e-mail",
      "correo_e", "mail", "direccion_correo", "dirección_correo", "correo_personal",
      "correo_institucional", "correo_oficial", "email_contacto", "e-mail_contacto",
      "correo_electronico_publico", "correo_electrónico_público", "correo_electronico_1",
      "email_1", "correo_principal", "correo_publico", "correo_público", 
      "correo_electronico_publico", "correo_s_electronico_s_publico_s",
      "correo_elec_publico", "correoElecPublico",
      
      # English equivalents
      "email_address", "mail_address", "electronic_mail", "e_mail", "correoElecPublico",
      "CORREO_ELECTRONICO", "EMAIL", "correo_electronico",
      
      # Additional observed patterns
      "correo_electronico", "email", "correo_elec_publico", "correos", "correo_publico",
      "correo_electronico_1", "correo_s_electronico_s_publico_s", "correos_electronico_publico",
      "correo_1", "correo_2"
    ),
    
    "telefono_1" = c(
      # Primary phone
      "telefono_1", "teléfono_1", "telefono", "teléfono", "tel", "tél", "numero_telefono",
      "número_teléfono", "telefono_contacto", "teléfono_contacto", "telefono_principal",
      "teléfono_principal", "telefono_oficina", "teléfono_oficina", "num_telefono",
      "núm_teléfono", "contacto_telefonico", "contacto_telefónico", "tel_contacto",
      "telefono_publico", "teléfono_público", "teléfono_público_contacto", "telefono_publico_contacto",
      "telefono_publico_de_contacto", "teléfono_público_de_contacto", "telefono_s_publico_s_de_contacto",
      "telefonos", "teléfonos", "telefonos_publico_de_contacto", "telefono_1",
      
      # English equivalents
      "phone", "phone_number", "telephone", "contact_number", "office_phone",
      "Telefono", "TELEFONO", "telefonoPublico", "TELEFONO PUBLICO", "TELEFONO 1",
      
      # Additional observed patterns
      "telefono", "telefono_1", "telefono_publico", "telefonos", "telefonos_publico_de_contacto",
      "telefono_publico_contacto", "telefono_publico_de_contacto", "telefono_s_publico_s_de_contacto",
      "telefonos_publico_de_contacto", "telefonoPublico", "telefonos_publico_de_contacto"
    ),
    
    #===== SOCIAL MEDIA =====
    "facebook" = c(
      # Facebook
      "facebook", "fb", "perfil_facebook", "pagina_facebook", "página_facebook",
      "usuario_facebook", "cuenta_facebook", "facebook_candidato", "facebook_candidata",
      "enlace_facebook", "link_facebook", "url_facebook", "facebook_oficial",
      "red_social_facebook", "redes_sociales_facebook", "fanpage", "fan_page",
      "perfil_fb", "pagina_fb", "página_fb",
      
      # English equivalents
      "facebook_profile", "facebook_page", "facebook_account", "facebook_user",
      "facebook_link", "facebook", "FACEBOOK", "facebook_url", "facebook_username",
      
      # Additional observed patterns
      "facebook", "fb", "redes_sociales_facebook"
    ),
    
    "twitter" = c(
      # Twitter
      "twitter", "tw", "perfil_twitter", "cuenta_twitter", "usuario_twitter",
      "twitter_candidato", "twitter_candidata", "enlace_twitter", "link_twitter",
      "url_twitter", "twitter_oficial", "red_social_twitter", "redes_sociales_twitter",
      "@usuario", "arroba", "perfil_tw", "cuenta_tw", "x_twitter",
      
      # English equivalents
      "twitter_profile", "twitter_account", "twitter_user", "twitter_link",
      "twitter_handle", "Twitter", "TWITTER", "twitter", "twitter_url", "twitter_username",
      
      # Additional observed patterns
      "twitter", "tw", "redes_sociales_twitter"
    ),
    
    "instagram" = c(
      # Instagram
      "instagram", "ig", "insta", "perfil_instagram", "cuenta_instagram",
      "usuario_instagram", "instagram_candidato", "instagram_candidata",
      "enlace_instagram", "link_instagram", "url_instagram", "instagram_oficial",
      "red_social_instagram", "redes_sociales_instagram", "perfil_ig", "cuenta_ig",
      
      # English equivalents
      "instagram_profile", "instagram_account", "instagram_user", "instagram_link",
      "instagram_handle", "Instagram", "INSTAGRAM", "instagram", "instagram_url", "instagram_username",
      
      # Additional observed patterns
      "instagram", "ig", "redes_sociales_instagram", "ins"
    ),
    
    "pagina_web" = c(
      # Website
      "pagina_web", "página_web", "sitio_web", "web", "website", "portal_web",
      "pagina_internet", "página_internet", "sitio_internet", "url_sitio",
      "direccion_web", "dirección_web", "portal_candidato", "portal_candidata",
      "sitio_oficial", "pagina_oficial", "página_oficial", "portal_oficial",
      "url_pagina", "url_página", "sitio_personal", "homepage", "home_page",
      "sitio", "paginaweb", "web_personal", "blog", "sitio_web_1", "pagina_personal",
      
      # English equivalents
      "website", "web_page", "web_address", "internet_site", "official_website",
      "official_web", "personal_website", "blog_url", "paginaWeb", "PAGINA_WEB", 
      "profile_url", "detail_url",
      
      # Additional observed patterns
      "pagina_web", "sitio_web", "sitio_web_1", "web", "detail_url", "profile_url"
    )
  )
  
  # Add field metadata with enhanced descriptions
  field_metadata <- list(
    # Identification fields
    id_candidato = list(
      description = "Unique identifier for the candidate",
      data_type = "character",
      validation = "non_empty",
      category = "identification",
      priority = "high"
    ),
    
    nombre_completo = list(
      description = "Full name of the candidate",
      data_type = "character",
      validation = "non_empty",
      category = "identification",
      priority = "high"
    ),
    
    nombre_completo_limpio = list(
      description = "Cleaned full name without titles or positions",
      data_type = "character",
      validation = "non_empty",
      category = "identification",
      priority = "high",
      derived = TRUE,
      derivation_method = "Text processing to remove titles like 'Diputado', 'Licenciado', etc."
    ),
    
    # Education fields
    nivel_educativo_estandarizado = list(
      description = "Standardized education level",
      data_type = "categorical",
      categories = c("DOCTORADO", "MAESTRIA", "LICENCIATURA", "TECNICO", "BACHILLERATO", 
                     "SECUNDARIA", "PRIMARIA", "SIN_ESTUDIOS", "ESCOLARIDAD_NO_ESPECIFICADA", "NO_ESPECIFICADO"),
      validation = "in_set",
      category = "education",
      priority = "high",
      derived = TRUE,
      derivation_method = "Classification based on 'educacion' field and Mexican educational terminology"
    ),
    
    escala_educativa = list(
      description = "Numeric education scale (0-7)",
      data_type = "integer",
      validation = "range(0, 7)",
      category = "education",
      priority = "medium",
      derived = TRUE,
      derivation_method = "Conversion of standardized education level to numeric scale"
    ),
    
    educacion_significativa = list(
      description = "Level of education significance (0=basic, 1=mid-level, 2=professional)",
      data_type = "ordinal",
      validation = "in_set(0, 1, 2)",
      category = "education",
      priority = "high",
      derived = TRUE,
      derivation_method = "Determined by education level categorization"
    ),
    
    # Experience fields
    total_public_experience_years = list(
      description = "Total years of public sector experience",
      data_type = "integer",
      validation = "range(0, 50)",
      category = "experience",
      priority = "high",
      derived = TRUE,
      derivation_method = "Sum of annual public experience indicators"
    ),
    
    total_private_experience_years = list(
      description = "Total years of private sector experience",
      data_type = "integer",
      validation = "range(0, 50)",
      category = "experience",
      priority = "high",
      derived = TRUE,
      derivation_method = "Sum of annual private experience indicators"
    ),
    
    consecutive_public_years = list(
      description = "Maximum consecutive years in public sector",
      data_type = "integer",
      validation = "range(0, 21)",
      category = "experience",
      priority = "medium",
      derived = TRUE,
      derivation_method = "Maximum consecutive '1' values in public experience indicators"
    ),
    
    consecutive_private_years = list(
      description = "Maximum consecutive years in private sector",
      data_type = "integer",
      validation = "range(0, 21)",
      category = "experience",
      priority = "medium",
      derived = TRUE,
      derivation_method = "Maximum consecutive '1' values in private experience indicators"
    ),
    
    # Ideology fields
    ideology_classification = list(
      description = "Classified political ideology",
      data_type = "categorical",
      category = "ideology",
      priority = "high",
      derived = TRUE,
      derivation_method = "Text analysis and classification using multi-dimensional ideological model"
    ),
    
    economic_axis = list(
      description = "Position on economic axis (negative=left, positive=right)",
      data_type = "numeric",
      validation = "range(-1, 1)",
      category = "ideology",
      priority = "high",
      derived = TRUE,
      derivation_method = "Text analysis of economic positions"
    ),
    
    social_axis = list(
      description = "Position on social axis (negative=conservative, positive=progressive)",
      data_type = "numeric",
      validation = "range(-1, 1)",
      category = "ideology",
      priority = "high",
      derived = TRUE,
      derivation_method = "Text analysis of social positions"
    )
  )
  
  # Combine mapping and enhanced metadata
  result <- list(
    mapping = mapping,
    metadata = field_metadata
  )
  
  # If unmatched report is provided, incorporate those columns
  if(!is.null(unmatched_report) && file.exists(unmatched_report)) {
    result$mapping <- enhance_field_mapping_from_report(result$mapping, unmatched_report)
  }
  
  return(result)
}

#========================================================================
# NAME CLEANING FUNCTIONS
#========================================================================

# Enhanced function to clean candidate names by removing titles and positions
clean_candidate_name <- function(name) {
  if(is.na(name)) return(NA)
  
  # Convert to lowercase for pattern matching
  name_lower <- tolower(name)
  
  # Enhanced patterns for Mexican political context
  title_patterns <- c(
    # Political titles with more variations
    "\\b(diputad[oa]( federal| local)?|senador[a]|regidor[a]|síndic[oa]|president[ea]( municipal)?|gobernad[oa]r|alcalde[sa]?|jefe de gobierno|delegad[oa]|funcionari[oa]|secretari[oa]|subsecretari[oa])\\b",
    # Professional titles with more Mexican variations
    "\\b(lic\\.|licenciad[oa]|ing\\.|ingenier[oa]|dr\\.|doctor[a]|mtro\\.|maestr[oa]|prof\\.|profesor[a]|c\\.p\\.|contador[a]|arq\\.|arquitect[oa]|médic[oa])\\b",
    # Academic titles
    "\\b(doctor[a] en|maestr[oa] en|especialista en)\\b",
    # Military/police titles
    "\\b(general|coronel|capitán|teniente|sargento|oficial)\\b",
    # Honorifics
    "\\b(c\\.|don|doña|sr\\.|sra\\.|lic\\.)\\b",
    # Designators
    "\\b(candidat[oa]( a| por| de)?|propietari[oa]|suplente|titular|representante)\\b"
  )
  
  # Apply each pattern
  for(pattern in title_patterns) {
    name_lower <- gsub(pattern, "", name_lower, ignore.case = TRUE)
  }
  
  # Clean up extra spaces and standardize
  name_clean <- name_lower %>%
    gsub("\\s{2,}", " ", .) %>%    # Replace multiple spaces with single space
    trimws() %>%                    # Remove leading/trailing whitespace
    gsub("^\\s*$", NA, .)          # Replace empty strings with NA
  
  # More robust capitalization for Spanish names with particles
  if(!is.na(name_clean)) {
    # Split name into words
    words <- strsplit(name_clean, "\\s+")[[1]]
    
    # Capitalize each word, but handle Spanish name particles appropriately
    words_cap <- sapply(words, function(w) {
      if(w %in% c("de", "del", "la", "las", "los", "y", "e", "da", "van", "von", "der")) {
        return(w)  # Keep particles lowercase
      } else {
        return(paste0(toupper(substr(w, 1, 1)), substr(w, 2, nchar(w))))
      }
    })
    
    name_clean <- paste(words_cap, collapse = " ")
  }
  
  return(name_clean)
}

#========================================================================
# EDUCATION STANDARDIZATION FUNCTIONS
#========================================================================

standardize_education_level <- function(education_text) {
  # Handle NULL, NA or empty input
  if(is.null(education_text) || length(education_text) == 0 || 
     (is.character(education_text) && (nchar(education_text) == 0 || is.na(education_text)))) {
    return("NO_ESPECIFICADO")
  }
  
  # Ensure the value is a character string
  if(!is.character(education_text)) {
    education_text <- as.character(education_text)
  }
  
  # Convert to lowercase for easier matching
  education_lower <- tolower(education_text)
  
  # Using pattern matching for different education levels
  # Mexican doctorate terms
  if(grepl("doctor|phd|doctorado|dr\\.|(d|doc) en [a-zá-úñ]|posgrado.+doctor|estudios.+doctor|grado.+doctor", education_lower)) {
    return("DOCTORADO")
  }
  
  # Mexican master's degree terms
  else if(grepl("maestr[ií]a|master|mba|magister|maest?r[ií]a en|m[aá]ster en|grado.+maestr|posgrado.+maestr|mtria|estudios.+maestr|m\\.c\\.|ma\\.|msc|m\\.a\\.|magister|postgrado.+(no.+doctorado)", education_lower)) {
    return("MAESTRIA")
  }
  
  # Mexican professional degree terms 
  else if(grepl("licenciatur|licenciado|ingenier[ií]a|ingeniero|abogad[oa]|contador|econom[ií]|administr|lic\\.|ing\\.|título.+profesional|cédula.+profesional|licenciado.+en|licenciada.+en|t[ií]tulo.+licenciatura|egresado.+licenciatura|carrera.+universitaria|estudios.+universitarios|t[ií]tulo.+universidad|universidad.+licenciado|profesional.+titulado|titulado.+profesion|derecho|medicina|médico|cirug[ía]|cirujan[ao]|médico.+cirujan[ao]|l\\.a\\.e|l\\.c\\.p|facultad.+de", education_lower)) {
    return("LICENCIATURA")
  }
  
  # Better handling for incomplete/truncated studies
  else if(grepl("trunc|incomplet|pasante|sin título|sin titula|sin conclu", education_lower)) {
    # Check if we can determine the level that was incomplete
    if(grepl("doctor|phd|doctorado", education_lower)) {
      return("MAESTRIA")  # Incomplete doctorate typically means they have a master's
    } else if(grepl("maestr[ií]a|master", education_lower)) {
      return("LICENCIATURA")  # Incomplete master's typically means they have a bachelor's
    } else if(grepl("licenciatur|ingenier[ií]a|profesional", education_lower)) {
      return("TECNICO")  # Incomplete bachelor's typically qualifies as technical education
    } else {
      return("ESCOLARIDAD_NO_ESPECIFICADA")  # Default if we can't determine level
    }
  }
  
  # Technical education
  else if(grepl("t[eé]cnic[oa]|carrera.+t[eé]cnica|estudios.+t[eé]cnico|formación.+t[eé]cnica|t[eé]cnico.+superior|t[eé]c\\.|preparacion.+t[eé]cnica|t\\.s\\.u|carrera.+comercial|diploma.+técnico|escuela.+técnica|instituto.+técnico|estudios.+técnicos|conalep|cecyt|cecati|cbtis|cetis|bachiller[a]?to técnic[oa]", education_lower)) {
    return("TECNICO")
  }
  
  # High school patterns (bachillerato)
  else if(grepl("bachiller|preparatoria|prepa|bachillerato|preparator|medio.+superior|nivel.+medio.+superior|educ.+media.+sup|educación.+media|certificado.+bachillerato|certificado.+prepa|escuela.+preparatoria|colegio.+bachiller|prepa.+completa|prepar.+terminada|bachillerato.+general|bachiller.+técnico|conalep", education_lower)) {
    return("BACHILLERATO") 
  }
  
  # Secondary education patterns
  else if(grepl("secundaria|media.+básica|nivel.+secundario|segundo.+nivel|educación.+media|escuela.+secundaria|estudios.+secundarios|certificado.+secundaria|nivel.+medio|secundaria.+completa|secundaria.+terminada", education_lower)) {
    return("SECUNDARIA")
  }
  
  # Primary education patterns
  else if(grepl("primaria|básica|elemental|educación.+básica|nivel.+básico|educación.+elemental|escuela.+primaria|estudios.+primarios|certificado.+primaria|primaria.+completa|primaria.+terminada|educaci[oó]n.+elemental", education_lower)) {
    return("PRIMARIA")
  }
  
  # No studies patterns
  else if(grepl("sin estudio|sin escolaridad|ninguno|analfabeta|sin instruccion|sin preparaci[oó]n|sin escuela|no.+estudios|no.+escolaridad|sin.+educaci[oó]n", education_lower)) {
    return("SIN_ESTUDIOS")
  }
  
  # If no clear match, try to infer from context
  else {
    # University/higher education institutions in Mexico
    if(grepl("unam|ipn|itesm|tecnol[oó]gico.+monterrey|universidad|facultad|colmex|colegio.+m[eé]xico|uni|polit[eé]cnico|autonom[ao]|uam|buap|tec|anáhuac|ibero|itam|udg|la.+salle|universidad.+de|facult|escuela.+super", education_lower)) {
      return("LICENCIATURA")
    } 
    # High school level institutions
    else if(grepl("conalep|cecyt|colegio|bachiller|cbt|cbta|cch|vocacional|cetis|cecati", education_lower)) {
      return("BACHILLERATO")
    }
    # Other educational terms
    else if(grepl("estudi|curs|escolar|educa|forma|academic|académic|aprend|aprendi|capaci", education_lower)) {
      return("ESCOLARIDAD_NO_ESPECIFICADA")
    } 
    else {
      return("NO_ESPECIFICADO")
    }
  }
}

# Replace the existing function with this enhanced version
create_education_scale <- function(std_education_level) {
  # Handle NULL, empty vector, NA, or non-character values
  if(is.null(std_education_level) || length(std_education_level) == 0) {
    return(NA_real_)
  }
  
  # Handle type conversion if needed
  if(!is.character(std_education_level) && !is.na(std_education_level)) {
    std_education_level <- as.character(std_education_level)
  }
  
  # Handle NA value after conversion
  if(is.na(std_education_level)) {
    return(NA_real_)
  }
  
  # Define a more nuanced numeric scale for each education level
  education_scale <- case_when(
    std_education_level == "DOCTORADO" ~ 7,
    std_education_level == "MAESTRIA" ~ 6,
    std_education_level == "LICENCIATURA" ~ 5,
    std_education_level == "TECNICO" ~ 4,
    std_education_level == "BACHILLERATO" ~ 3,
    std_education_level == "SECUNDARIA" ~ 2,
    std_education_level == "PRIMARIA" ~ 1,
    std_education_level == "SIN_ESTUDIOS" ~ 0,
    std_education_level == "ESCOLARIDAD_NO_ESPECIFICADA" ~ NA_real_,
    std_education_level == "NO_ESPECIFICADO" ~ NA_real_,
    TRUE ~ NA_real_
  )
  
  return(education_scale)
}

# Replace the existing function with this enhanced version
has_significant_education <- function(education_level, education_scale) {
  # Handle NULL, empty vector, or NA values in either parameter
  if(is.null(education_level) || length(education_level) == 0 || 
     is.null(education_scale) || length(education_scale) == 0) {
    return(NA_integer_)
  }
  
  # Ensure education_scale is numeric
  if(!is.numeric(education_scale) && !is.na(education_scale)) {
    education_scale <- as.numeric(as.character(education_scale))
  }
  
  if(is.na(education_scale)) {
    return(NA_integer_)
  }
  
  # More nuanced classification with different thresholds
  if(education_scale >= 5) {
    return(2L)  # Professional level (college or higher)
  } else if(education_scale >= 3) {
    return(1L)  # Mid-level education (high school or technical)
  } else {
    return(0L)  # Basic or no education
  }
}

#========================================================================
# EXPERIENCE EXTRACTION FUNCTIONS
#========================================================================

# Enhanced function to extract years from text with improved pattern recognition
extract_years_from_text <- function(text) {
  if(is.na(text)) return(character(0))
  
  # Pattern to capture years (1980-2025 range) - expanded to catch more year formats
  year_pattern <- "\\b(19[8-9][0-9]|20[0-2][0-9])\\b"
  
  # Normalized text to handle Spanish date formats and punctuation
  normalized_text <- text %>%
    # Standardize dashes and date separators
    gsub("–", "-", .) %>%
    gsub("—", "-", .) %>%
    gsub("a la fecha|a la actualidad|al presente|actualmente|hasta hoy|presente", "2023", ., ignore.case = TRUE) %>%
    # Handle Spanish date formats
    gsub("enero|february|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre", "", ., ignore.case = TRUE) %>%
    # Remove punctuation that might interfere with year extraction
    gsub("[\\(\\)\\[\\]\\{\\}]", " ", .)
  
  # Extract individual years
  years <- stringr::str_extract_all(normalized_text, year_pattern)[[1]]
  
  # Improved pattern for year ranges to catch more formats (1980-2025)
  range_pattern <- "\\b(19[8-9][0-9]|20[0-2][0-9])[\\s]*[-–—][\\s]*(19[8-9][0-9]|20[0-2][0-9])\\b"
  ranges <- stringr::str_extract_all(normalized_text, range_pattern)
  
  # Enhanced process for ranges into individual years
  expanded_years <- character(0)
  if(length(ranges[[1]]) > 0) {
    for(range in ranges[[1]]) {
      start_year <- as.numeric(stringr::str_extract(range, "^(19[8-9][0-9]|20[0-2][0-9])"))
      end_year <- as.numeric(stringr::str_extract(range, "(19[8-9][0-9]|20[0-2][0-9])$"))
      
      if(!is.na(start_year) && !is.na(end_year) && end_year >= start_year) {
        # Check for reasonable range to avoid extraction errors
        if(end_year - start_year <= 50) {  # Maximum 50 year career span
          expanded_years <- c(expanded_years, as.character(start_year:end_year))
        }
      }
    }
  }
  
  # Handle Spanish date formats with years like "de 2010 a 2015"
  spanish_range_pattern <- "\\b(de|desde)[\\s]+(19[8-9][0-9]|20[0-2][0-9])[\\s]+(a|al|hasta)[\\s]+(19[8-9][0-9]|20[0-2][0-9])\\b"
  spanish_ranges <- stringr::str_extract_all(normalized_text, spanish_range_pattern)
  
  if(length(spanish_ranges[[1]]) > 0) {
    for(range in spanish_ranges[[1]]) {
      start_year <- as.numeric(stringr::str_extract(range, "\\b(19[8-9][0-9]|20[0-2][0-9])"))
      end_parts <- stringr::str_extract_all(range, "\\b(19[8-9][0-9]|20[0-2][0-9])\\b")[[1]]
      end_year <- as.numeric(end_parts[length(end_parts)])
      
      if(!is.na(start_year) && !is.na(end_year) && end_year >= start_year) {
        # Check for reasonable range to avoid extraction errors
        if(end_year - start_year <= 50) {  # Maximum 50 year career span
          expanded_years <- c(expanded_years, as.character(start_year:end_year))
        }
      }
    }
  }
  
  # Combine individual years with expanded ranges and remove duplicates
  all_years <- unique(c(years, expanded_years))
  return(all_years)
}

# Enhanced function to detect public sector experience in Mexican context
extract_public_experience <- function(text, year) {
  if(is.na(text)) return(0)
  
  # Define comprehensive patterns for public sector positions in Mexican government
  public_sector_patterns <- c(
    # Legislative positions
    "diputad[oa]", "senador[a]", "legislad[oa]r", "congresista", "parlamentari[oa]", 
    "cámar[a]", "congres[o]", "curul", "escaño", "assembleis?ta", 
    
    # Executive positions - federal
    "president[ea]", "gobernador[a]", "secretari[oa] (de )?estado", "secretari[oa] (de )?(la )?",
    "subsecretari[oa]", "oficial mayor", "director[a] general", "director[a] adjunt[oa]",
    "coordinador[a] general", "coordinador[a] nacional", "jefe de gobierno", "jefa de gobierno",
    "jefatura", "gubernatura", "ejecutivo federal", "poder ejecutivo",
    
    # Executive positions - local
    "alcalde", "alcaldesa", "presidente municipal", "presidenta municipal", 
    "síndic[oa]", "regidor[a]", "concejal", "cabildo", "ayuntamiento", "munícipe",
    "edil", "delegad[oa]", "jefe delegacional", "jefa delegacional", "prefect[oa]", 
    
    # Judicial positions
    "juez", "jueza", "magistrad[oa]", "ministr[oa]", "poder judicial", "tribunal", 
    "juzgado", "corte", "consej[oa] de la judicatura", "fiscalía", "fiscal", 
    "procuradurí?a", "procurador[a]", "ministerio público", "mp", 
    
    # Electoral institutions
    "ine", "ife", "electoral", "iepc", "iee", "fepade", "tepjf",
    "electoral", "comité electoral", "instituto electoral", "consejo electoral",
    
    # Public administration
    "función pública", "funcion publica", "servicio público", "servicio publico", 
    "servidor público", "servidor[a] públic[oa]", "servidor[a] public[oa]",
    "burócrata", "burocrata", "burócratico", "burocrático", "funcionari[oa]", 
    "empleado público", "empleada pública", "emplead[oa] gubernamental",
    
    # Government organizations
    "dependencia", "entidad", "gobierno federal", "gobierno estatal", "gobierno municipal",
    "administración federal", "administracion federal", "administración pública", 
    "administracion publica", "organismo público", "organismo descentralizado",
    "paraestatal", "sector público", "sector publico"
  )
  
  # Check if the text contains both the year and a public sector term
  year_pattern <- paste0("\\b", year, "\\b")
  year_range_pattern <- paste0("\\b[^0-9](19[8-9][0-9]|20[0-2][0-9])[\\s]*[-–—][\\s]*", year, "\\b|\\b", 
                               year, "[\\s]*[-–—][\\s]*(19[8-9][0-9]|20[0-2][0-9])[^0-9]\\b")
  year_spanish_pattern <- paste0("\\b(de|desde)[\\s]+(19[8-9][0-9]|20[0-2][0-9])[\\s]+(a|al|hasta)[\\s]+", 
                                 year, "\\b|\\b(de|desde)[\\s]+", year, "[\\s]+(a|al|hasta)[\\s]+(19[8-9][0-9]|20[0-2][0-9])\\b")
  
  has_year <- grepl(year_pattern, text) || grepl(year_range_pattern, text) || grepl(year_spanish_pattern, text)
  
  if(has_year) {
    for(pattern in public_sector_patterns) {
      if(grepl(pattern, text, ignore.case = TRUE)) {
        return(1)  # Public sector experience found for this year
      }
    }
  }
  
  # Extract year ranges and check if this year falls within any range
  range_pattern <- "\\b(19[8-9][0-9]|20[0-2][0-9])[\\s]*[-–—][\\s]*(19[8-9][0-9]|20[0-2][0-9])\\b"
  ranges <- stringr::str_extract_all(text, range_pattern)[[1]]
  
  for(range in ranges) {
    start_year <- as.numeric(stringr::str_extract(range, "^(19[8-9][0-9]|20[0-2][0-9])"))
    end_year <- as.numeric(stringr::str_extract(range, "(19[8-9][0-9]|20[0-2][0-9])$"))
    
    if(!is.na(start_year) && !is.na(end_year) && 
       as.numeric(year) >= start_year && as.numeric(year) <= end_year) {
      # Check if any public sector pattern exists near this range
      range_pos <- regexpr(range, text)[1]
      if(range_pos > 0) {
        context_start <- max(1, range_pos - 100)
        context_end <- min(nchar(text), range_pos + nchar(range) + 100)
        
        context_text <- substr(text, context_start, context_end)
        
        for(pattern in public_sector_patterns) {
          if(grepl(pattern, context_text, ignore.case = TRUE)) {
            return(1)  # Public sector experience found for this year within a range
          }
        }
      }
    }
  }
  
  # Handle Spanish date ranges like "de 2010 a 2015"
  spanish_range_pattern <- "\\b(de|desde)[\\s]+(19[8-9][0-9]|20[0-2][0-9])[\\s]+(a|al|hasta)[\\s]+(19[8-9][0-9]|20[0-2][0-9])\\b"
  spanish_ranges <- stringr::str_extract_all(text, spanish_range_pattern)[[1]]
  
  for(range in spanish_ranges) {
    start_year <- as.numeric(stringr::str_extract(range, "\\b(19[8-9][0-9]|20[0-2][0-9])"))
    end_parts <- stringr::str_extract_all(range, "\\b(19[8-9][0-9]|20[0-2][0-9])\\b")[[1]]
    end_year <- as.numeric(end_parts[length(end_parts)])
    
    if(!is.na(start_year) && !is.na(end_year) && 
       as.numeric(year) >= start_year && as.numeric(year) <= end_year) {
      # Check if any public sector pattern exists near this range
      range_pos <- regexpr(range, text)[1]
      if(range_pos > 0) {
        context_start <- max(1, range_pos - 100)
        context_end <- min(nchar(text), range_pos + nchar(range) + 100)
        
        context_text <- substr(text, context_start, context_end)
        
        for(pattern in public_sector_patterns) {
          if(grepl(pattern, context_text, ignore.case = TRUE)) {
            return(1)  # Public sector experience found for this year within a Spanish date range
          }
        }
      }
    }
  }
  
  return(0)  # No public sector experience found for this year
}

# Enhanced function for private sector experience extraction
extract_private_experience <- function(text, year) {
  if(is.na(text)) return(0)
  
  # Define comprehensive patterns for private sector positions in Mexican business context
  private_sector_patterns <- c(
    # Business titles
    "empresari[oa]", "emprendedor[a]", "director[a] general", "director[a] ejecutiv[oa]",
    "presidente corporativo", "presidenta corporativa", "ceo", "cfo", "coo", "cto", "cio",
    "gerente", "gerente general", "gerente de área", "subgerente", "jefe de departamento",
    "jefe de área", "director[a] comercial", "director[a] administrativ[oa]", "director[a] financier[oa]",
    "vicepresidente", "vicepresidenta", "socio", "socia", "fundador[a]", "cofundador[a]",
    "accionista", "director[a]", "subdirector[a]", "titular", "ejecutiv[oa]", "administrador[a]",
    
    # Business entities
    "compañía", "compañía", "empresa", "firma", "corporación", "corporativo", "grupo empresarial",
    "corporativo", "negocio", "microempresa", "pyme", "comercio", "establecimiento", "consorcio",
    "sociedad anónima", "s\\.a\\.", "s\\.a\\. de c\\.v\\.", "s\\.r\\.l\\.", "sociedad civil",
    "s\\.c\\.", "asociación civil", "a\\.c\\.", "despacho", "bufete", "consultora", "consultoría",
    "industria", "industria privada", "sector industrial", "corporación privada",
    
    # Private sector terminology
    "sector privado", "iniciativa privada", "entidad privada", "organización privada",
    "institución privada", "empresa privada", "empleo privado", "corporativo privado",
    "ámbito privado", "esfera privada", "ámbito empresarial", "mundo corporativo",
    "empresa familiar", "negocio familiar", "negocio propio", "empresa propia"
  )
  
  # Check if the text contains both the year and a private sector term
  year_pattern <- paste0("\\b", year, "\\b")
  year_range_pattern <- paste0("\\b[^0-9](19[8-9][0-9]|20[0-2][0-9])[\\s]*[-–—][\\s]*", year, "\\b|\\b", 
                               year, "[\\s]*[-–—][\\s]*(19[8-9][0-9]|20[0-2][0-9])[^0-9]\\b")
  year_spanish_pattern <- paste0("\\b(de|desde)[\\s]+(19[8-9][0-9]|20[0-2][0-9])[\\s]+(a|al|hasta)[\\s]+", 
                                 year, "\\b|\\b(de|desde)[\\s]+", year, "[\\s]+(a|al|hasta)[\\s]+(19[8-9][0-9]|20[0-2][0-9])\\b")
  
  has_year <- grepl(year_pattern, text) || grepl(year_range_pattern, text) || grepl(year_spanish_pattern, text)
  
  if(has_year) {
    for(pattern in private_sector_patterns) {
      if(grepl(pattern, text, ignore.case = TRUE)) {
        return(1)  # Private sector experience found for this year
      }
    }
  }
  
  # Extract year ranges and check if this year falls within any range
  range_pattern <- "\\b(19[8-9][0-9]|20[0-2][0-9])[\\s]*[-–—][\\s]*(19[8-9][0-9]|20[0-2][0-9])\\b"
  ranges <- stringr::str_extract_all(text, range_pattern)[[1]]
  
  for(range in ranges) {
    start_year <- as.numeric(stringr::str_extract(range, "^(19[8-9][0-9]|20[0-2][0-9])"))
    end_year <- as.numeric(stringr::str_extract(range, "(19[8-9][0-9]|20[0-2][0-9])$"))
    
    if(!is.na(start_year) && !is.na(end_year) && 
       as.numeric(year) >= start_year && as.numeric(year) <= end_year) {
      # Check if any private sector pattern exists near this range
      range_pos <- regexpr(range, text)[1]
      if(range_pos > 0) {
        context_start <- max(1, range_pos - 100)
        context_end <- min(nchar(text), range_pos + nchar(range) + 100)
        
        context_text <- substr(text, context_start, context_end)
        
        for(pattern in private_sector_patterns) {
          if(grepl(pattern, context_text, ignore.case = TRUE)) {
            return(1)  # Private sector experience found for this year within a range
          }
        }
      }
    }
  }
  
  # Handle Spanish date ranges like "de 2010 a 2015"
  spanish_range_pattern <- "\\b(de|desde)[\\s]+(19[8-9][0-9]|20[0-2][0-9])[\\s]+(a|al|hasta)[\\s]+(19[8-9][0-9]|20[0-2][0-9])\\b"
  spanish_ranges <- stringr::str_extract_all(text, spanish_range_pattern)[[1]]
  
  for(range in spanish_ranges) {
    start_year <- as.numeric(stringr::str_extract(range, "\\b(19[8-9][0-9]|20[0-2][0-9])"))
    end_parts <- stringr::str_extract_all(range, "\\b(19[8-9][0-9]|20[0-2][0-9])\\b")[[1]]
    end_year <- as.numeric(end_parts[length(end_parts)])
    
    if(!is.na(start_year) && !is.na(end_year) && 
       as.numeric(year) >= start_year && as.numeric(year) <= end_year) {
      # Check if any private sector pattern exists near this range
      range_pos <- regexpr(range, text)[1]
      if(range_pos > 0) {
        context_start <- max(1, range_pos - 100)
        context_end <- min(nchar(text), range_pos + nchar(range) + 100)
        
        context_text <- substr(text, context_start, context_end)
        
        for(pattern in private_sector_patterns) {
          if(grepl(pattern, context_text, ignore.case = TRUE)) {
            return(1)  # Private sector experience found for this year within a Spanish date range
          }
        }
      }
    }
  }
  
  return(0)  # No private sector experience found for this year
}

# Helper function to find the maximum consecutive runs of 1s
max_consecutive_ones <- function(x) {
  if(all(is.na(x))) return(0)
  
  x_numeric <- as.numeric(x)
  x_numeric[is.na(x_numeric)] <- 0
  
  # Convert to string for easy pattern matching
  str_x <- paste(x_numeric, collapse = "")
  
  # Find all runs of ones
  ones_runs <- gregexpr("1+", str_x)[[1]]
  
  # If no runs found, return 0
  if(ones_runs[1] == -1) return(0)
  
  # Get the lengths of each run
  run_lengths <- attr(ones_runs, "match.length")
  
  # Return the maximum run length
  return(max(run_lengths))
}

# Helper function to find the first year with experience
first_experience_year <- function(x) {
  years <- 2000:2020
  x_numeric <- as.numeric(x)
  
  if(all(is.na(x_numeric) | x_numeric == 0)) {
    return(NA_integer_)
  }
  
  for(i in 1:length(years)) {
    if(!is.na(x_numeric[i]) && x_numeric[i] == 1) {
      return(years[i])
    }
  }
  
  return(NA_integer_)
}

# Enhanced function to create yearly experience indicators with improved features
create_experience_indicators <- function(df) {
  # Define the range of years to check (2000-2020)
  experience_years <- 2000:2020
  
  # Combine relevant text fields for analysis with robust handling of missing columns
  # First check which fields exist in the dataframe
  experience_fields <- c("historia_profesional", "trayectoria_politica", 
                         "experiencia_docencia", "descripcion_candidato",
                         "cargos_publicos")
  
  # Filter to only include fields that exist in the dataframe
  available_fields <- intersect(experience_fields, names(df))
  
  # Log which fields are available for experience analysis
  cat("Experience analysis using fields:", paste(available_fields, collapse=", "), "\n")
  
  # If no experience fields are available, create dummy indicators
  if(length(available_fields) == 0) {
    cat("WARNING: No experience fields found in dataframe. Creating dummy indicators.\n")
    
    # Create dummy indicators
    for(year in experience_years) {
      year_str <- as.character(year)
      df[[paste0("public_experience_", year_str)]] <- 0
      df[[paste0("private_experience_", year_str)]] <- 0
    }
    
    # Add summary fields
    df$total_public_experience_years <- 0
    df$total_private_experience_years <- 0
    df$public_experience_ratio <- 0
    df$private_experience_ratio <- 0
    df$has_mixed_experience <- FALSE
    df$experience_sector_dominant <- "Sin Experiencia Detectada"
    df$consecutive_public_years <- 0
    df$consecutive_private_years <- 0
    df$career_start_year <- NA_integer_
    df$public_career_start <- NA_integer_
    df$private_career_start <- NA_integer_
    df$first_sector <- NA_character_
    df$experience_diversity <- 0
    df$career_transitions <- 0
    
    return(df)
  }
  
  # Use a simpler approach to create the combined text field
  df$combined_experience_text <- ""
  for(field in available_fields) {
    df$combined_experience_text <- paste(
      df$combined_experience_text,
      ifelse(is.na(df[[field]]), "", as.character(df[[field]])),
      sep = " "
    )
  }
  
  # Trim extra spaces
  df$combined_experience_text <- trimws(df$combined_experience_text)
  
  # Initialize counters for progress reporting
  total_records <- nrow(df)
  cat("Analyzing experience for", total_records, "candidates...\n")
  progress_step <- max(1, floor(total_records / 10))
  
  # Create public sector experience indicators
  cat("Extracting public sector experience by year...\n")
  for(year in experience_years) {
    year_str <- as.character(year)
    df[[paste0("public_experience_", year_str)]] <- 
      sapply(df$combined_experience_text, extract_public_experience, year = year_str)
    
    # Report progress periodically
    if(year %% 5 == 0) {
      cat("  Completed analysis for year:", year, "\n")
    }
  }
  
  # Create private sector experience indicators
  cat("Extracting private sector experience by year...\n")
  for(year in experience_years) {
    year_str <- as.character(year)
    df[[paste0("private_experience_", year_str)]] <- 
      sapply(df$combined_experience_text, extract_private_experience, year = year_str)
    
    # Report progress periodically
    if(year %% 5 == 0) {
      cat("  Completed analysis for year:", year, "\n")
    }
  }
  
  # Calculate total years of experience in each sector and enhanced metrics
  df <- df %>%
    dplyr::mutate(
      # Basic totals
      total_public_experience_years = rowSums(across(starts_with("public_experience_"))),
      total_private_experience_years = rowSums(across(starts_with("private_experience_"))),
      
      # Calculate additional metrics
      experience_diversity = total_public_experience_years * total_private_experience_years,
      public_experience_ratio = total_public_experience_years / 
        (total_public_experience_years + total_private_experience_years + 0.001),
      private_experience_ratio = total_private_experience_years / 
        (total_public_experience_years + total_private_experience_years + 0.001)
    )
  
  # Calculate career transitions (changes between sectors)
  df$career_transitions <- apply(df, 1, function(row) {
    # Get public experience column indices
    public_cols <- grep("^public_experience_", names(df))
    private_cols <- grep("^private_experience_", names(df))
    
    if(length(public_cols) == 0 || length(private_cols) == 0) return(0)
    
    # Extract values for this row
    pub_vals <- as.integer(row[public_cols])
    priv_vals <- as.integer(row[private_cols])
    
    # Create sector indicator: 1=public, 2=private, 3=both, 0=none
    sector_indicators <- rep(0, length(pub_vals))
    for(i in 1:length(pub_vals)) {
      if(pub_vals[i] == 1 && priv_vals[i] == 1) {
        sector_indicators[i] <- 3  # Both sectors
      } else if(pub_vals[i] == 1) {
        sector_indicators[i] <- 1  # Public only
      } else if(priv_vals[i] == 1) {
        sector_indicators[i] <- 2  # Private only
      }
      # 0 remains for no experience
    }
    
    # Count transitions between different sector states
    transitions <- sum(diff(sector_indicators) != 0, na.rm = TRUE)
    return(transitions)
  })
  
  # Calculate consecutive years in each sector
  df$consecutive_public_years <- apply(df %>% select(starts_with("public_experience_")), 1, max_consecutive_ones)
  df$consecutive_private_years <- apply(df %>% select(starts_with("private_experience_")), 1, max_consecutive_ones)
  
  # Compute career timeline markers
  df$career_start_year <- apply(df %>% select(c(starts_with("public_experience_"), 
                                                starts_with("private_experience_"))), 1, first_experience_year)
  df$public_career_start <- apply(df %>% select(starts_with("public_experience_")), 1, first_experience_year)
  df$private_career_start <- apply(df %>% select(starts_with("private_experience_")), 1, first_experience_year)
  
  # Calculate sector that started career
  df$first_sector <- apply(df, 1, function(row) {
    pub_start <- as.numeric(row["public_career_start"])
    priv_start <- as.numeric(row["private_career_start"])
    
    if(is.na(pub_start) && is.na(priv_start)) {
      return(NA_character_)
    } else if(is.na(pub_start)) {
      return("Privado")
    } else if(is.na(priv_start)) {
      return("Público")
    } else if(pub_start < priv_start) {
      return("Público")
    } else if(priv_start < pub_start) {
      return("Privado")
    } else {
      return("Ambos")
    }
  })
  
  # Add sector experience summary statistics
  df <- df %>%
    dplyr::mutate(
      has_mixed_experience = (total_public_experience_years > 0 & total_private_experience_years > 0),
      experience_sector_dominant = case_when(
        total_public_experience_years > total_private_experience_years ~ "Público",
        total_private_experience_years > total_public_experience_years ~ "Privado",
        total_public_experience_years > 0 & total_private_experience_years > 0 ~ "Mixto",
        total_public_experience_years == 0 & total_private_experience_years == 0 ~ "Sin Experiencia Detectada",
        TRUE ~ NA_character_
      )
    )
  
  # Clean up temporary column
  df <- df %>% select(-combined_experience_text)
  
  return(df)
}

#========================================================================
# MAIN PROCESSING FUNCTIONS
#========================================================================

# Main function to create homologated dataframe with enhanced functionality
create_homologated_df_robust <- function(file_paths) {
  cat("\n===== STARTING DATA HOMOLOGATION PROCESSING =====\n\n")
  all_dfs <- list()
  
  # Setup parallel processing if enabled
  if(config$parallel_workers > 1) {
    cat("Setting up parallel processing with", config$parallel_workers, "workers\n")
    future::plan(future::multisession, workers = config$parallel_workers)
  }
  
  # Process each file
  cat("Processing", length(file_paths), "files\n")
  
  if(config$parallel_workers > 1) {
    # Parallel processing
    results <- furrr::future_map(file_paths, process_file_detailed, .progress = TRUE)
    for(i in seq_along(file_paths)) {
      if(!is.null(results[[i]]) && ncol(results[[i]]) > 1) {
        all_dfs[[basename(file_paths[i])]] <- results[[i]]
      }
    }
  } else {
    # Sequential processing
    for(file in file_paths) {
      df <- process_file_detailed(file)
      if(!is.null(df) && ncol(df) > 1) { # More than just source_file column
        all_dfs[[basename(file)]] <- df
      }
    }
  }
  
  # Return to sequential processing
  if(config$parallel_workers > 1) {
    future::plan(future::sequential)
  }
  
  if(length(all_dfs) == 0) {
    stop("No valid dataframes were created from the input files")
  }
  
  cat("\nCombining", length(all_dfs), "processed dataframes\n")
  
  # Combine all dataframes with consistent types
  combined_df <- bind_rows(all_dfs) %>%
    # Clean and normalize text fields
    mutate(across(where(is.character), ~na_if(trimws(.), "")))
  
  # Add sanitization step for data type consistency
  combined_df <- sanitize_dataframe_types(combined_df)
  
  cat("Initial combined dataframe has", nrow(combined_df), "rows\n")
  
  # Perform advanced cleaning and enhancement
  cat("\nPerforming specialized field cleaning...\n")
  combined_df <- clean_specialized_fields(combined_df)
  
  # Create unique identifiers for deduplication
  cat("Preparing for deduplication...\n")
  combined_df <- create_unique_identifier(combined_df)
  
  # Count duplicates before deduplication
  dup_count <- sum(duplicated(combined_df$unique_id))
  cat("Found", dup_count, "duplicate records based on composite identifier\n")
  
  # Save duplicates for review if any exist
  if(dup_count > 0) {
    dup_ids <- combined_df$unique_id[duplicated(combined_df$unique_id)]
    duplicates_df <- combined_df %>% filter(unique_id %in% dup_ids)
    write_csv(duplicates_df, file.path(config$report_dir, paste0("duplicate_records_", config$run_id, ".csv")))
  }
  
  # Remove duplicates with preference for more complete records
  combined_df <- combined_df %>%
    # Calculate fields with data for each record
    dplyr::mutate(fields_with_data = rowSums(!is.na(across(everything())))) %>%
    # Sort by fields_with_data (descending) to keep most complete record
    arrange(desc(fields_with_data)) %>%
    # Keep first occurrence of each unique_id
    distinct(unique_id, .keep_all = TRUE) %>%
    # Remove temporary column
    select(-fields_with_data)
  
  # Add education standardization
  # Add education standardization with enhanced error handling
  # Add education standardization with enhanced error handling
  cat("\nStandardizing education levels...\n")
  tryCatch({
    # First check if educacion column exists
    if(!"educacion" %in% names(combined_df)) {
      cat("WARNING: 'educacion' column not found. Creating default education fields.\n")
      combined_df$nivel_educativo_estandarizado <- "NO_ESPECIFICADO"
      combined_df$escala_educativa <- NA_real_
      combined_df$educacion_significativa <- NA_integer_
    } else {
      # Ensure educacion column is character type
      combined_df$educacion <- as.character(combined_df$educacion)
      cat("Education column type:", class(combined_df$educacion), "\n")
      
      # Process each record individually with explicit error handling
      combined_df$nivel_educativo_estandarizado <- vapply(seq_len(nrow(combined_df)), function(i) {
        if(i %% 10000 == 0) cat("Processing education record", i, "\n")
        
        tryCatch({
          x <- combined_df$educacion[i]
          level <- standardize_education_level(x)
          return(level)
        }, error = function(e) {
          cat("Error standardizing education for row", i, ":", e$message, "\n")
          return("NO_ESPECIFICADO")
        })
      }, character(1))
      
      # Create education scale with error handling
      combined_df$escala_educativa <- vapply(combined_df$nivel_educativo_estandarizado, function(x) {
        tryCatch({
          scale <- create_education_scale(x)
          if(is.null(scale) || length(scale) == 0) return(NA_real_)
          return(scale)
        }, error = function(e) {
          return(NA_real_)
        })
      }, numeric(1))
      
      # Determine education significance with error handling
      combined_df$educacion_significativa <- mapply(function(level, scale) {
        tryCatch({
          sig <- has_significant_education(level, scale)
          if(is.null(sig) || length(sig) == 0) return(NA_integer_)
          return(sig)
        }, error = function(e) {
          return(NA_integer_)
        })
      }, combined_df$nivel_educativo_estandarizado, combined_df$escala_educativa)
      
      cat("Education standardization complete.\n")
    }
  }, error = function(e) {
    cat("CRITICAL ERROR in education standardization:", e$message, "\n")
    cat("Creating default education fields...\n")
    
    combined_df$nivel_educativo_estandarizado <- "NO_ESPECIFICADO"
    combined_df$escala_educativa <- NA_real_
    combined_df$educacion_significativa <- NA_integer_
  })
  
  # Extract experience indicators with robust error handling
  cat("\nExtracting experience indicators by year...\n")
  tryCatch({
    combined_df <- create_experience_indicators(combined_df)
  }, error = function(e) {
    cat("ERROR in experience extraction:", e$message, "\n")
    cat("Continuing with processing...\n")
    
    # Create dummy experience columns
    for(year in 2000:2020) {
      year_str <- as.character(year)
      combined_df[[paste0("public_experience_", year_str)]] <- 0
      combined_df[[paste0("private_experience_", year_str)]] <- 0
    }
    
    combined_df$total_public_experience_years <- 0
    combined_df$total_private_experience_years <- 0
    combined_df$public_experience_ratio <- 0
    combined_df$private_experience_ratio <- 0
    combined_df$has_mixed_experience <- FALSE
    combined_df$experience_sector_dominant <- "Sin Experiencia Detectada"
    combined_df$consecutive_public_years <- 0
    combined_df$consecutive_private_years <- 0
    combined_df$career_start_year <- NA_integer_
    combined_df$public_career_start <- NA_integer_
    combined_df$private_career_start <- NA_integer_
    combined_df$first_sector <- NA_character_
    combined_df$experience_diversity <- 0
    combined_df$career_transitions <- 0
  })
  
  # Add ideology classification with error handling
  cat("\nClassifying political ideology...\n")
  tryCatch({
    combined_df <- add_mexican_ideology_classification(combined_df)
  }, error = function(e) {
    cat("ERROR in ideology classification:", e$message, "\n")
    cat("Continuing with processing...\n")
    
    # Create default ideology fields
    combined_df$ideology_classification <- "NO_CLASIFICADO"
    combined_df$economic_axis <- NA_real_
    combined_df$social_axis <- NA_real_
    combined_df$economic_left_score <- NA_real_
    combined_df$economic_right_score <- NA_real_
    combined_df$social_progressive_score <- NA_real_
    combined_df$social_conservative_score <- NA_real_
    combined_df$populist_score <- NA_real_
    combined_df$nationalist_score <- NA_real_
    combined_df$amlo_4t_score <- NA_real_
    combined_df$opposition_score <- NA_real_
    combined_df$security_score <- NA_real_
    combined_df$economic_policy_score <- NA_real_
    combined_df$populist_tendency <- NA_real_
    combined_df$nationalist_tendency <- NA_real_
    combined_df$current_political_alignment <- NA_real_
  })
  
  # Add advanced political economy metrics
  cat("\nCalculating political economy metrics...\n")
  tryCatch({
    combined_df <- calculate_political_economy_metrics(combined_df)
  }, error = function(e) {
    cat("ERROR in political economy metrics:", e$message, "\n")
    cat("Continuing with processing...\n")
  })
  
  # Remove the temporary unique_id
  combined_df <- combined_df %>% select(-unique_id)
  
  cat("\nFinal homologated dataframe has", nrow(combined_df), "unique records and", 
      ncol(combined_df), "fields\n")
  
  return(combined_df)
}

standardize_name_to_upper <- function(name) {
  if(is.na(name) || is.null(name) || length(name) == 0) return(NA_character_)
  
  # Remove accents, convert to uppercase, and remove any remaining special characters
  name_standardized <- name %>%
    stringi::stri_trans_general("Latin-ASCII") %>%  # Remove accents
    toupper() %>%                                   # Convert to uppercase
    gsub("[^A-Z0-9 ]", "", .) %>%                  # Remove any remaining special characters
    gsub("\\s+", " ", .) %>%                       # Replace multiple spaces with single space
    trimws()                                        # Remove leading/trailing whitespace
  
  return(name_standardized)
}

# Function to generate summary statistics
generate_summary_statistics <- function(df) {
  cat("\n===== GENERATING SUMMARY STATISTICS =====\n\n")
  
  # Education summary
  if("nivel_educativo_estandarizado" %in% names(df)) {
    cat("Education distribution:\n")
    edu_summary <- df %>%
      filter(!is.na(nivel_educativo_estandarizado)) %>%
      group_by(nivel_educativo_estandarizado) %>%
      summarize(count = n(), percentage = n() / nrow(.) * 100) %>%
      arrange(desc(count))
    
    print(edu_summary)
    
    # Significant education summary
    sig_edu <- df %>%
      filter(!is.na(educacion_significativa)) %>%
      group_by(educacion_significativa) %>%
      summarize(count = n(), percentage = n() / nrow(.) * 100)
    
    cat("\nCandidates with significant education:\n")
    print(sig_edu)
    
    # Add interpretation
    cat("\nEducation significance levels:\n")
    cat("  0 = Basic education (primary or secondary)\n")
    cat("  1 = Mid-level education (high school or technical)\n")
    cat("  2 = Professional education (college or higher)\n")
  }
  
  # Experience summary
  if("total_public_experience_years" %in% names(df)) {
    cat("\nExperience summary:\n")
    
    exp_summary <- df %>%
      summarize(
        avg_public_years = mean(total_public_experience_years, na.rm = TRUE),
        avg_private_years = mean(total_private_experience_years, na.rm = TRUE),
        candidates_with_public = sum(total_public_experience_years > 0, na.rm = TRUE),
        candidates_with_private = sum(total_private_experience_years > 0, na.rm = TRUE),
        candidates_with_both = sum(has_mixed_experience, na.rm = TRUE),
        avg_consecutive_public = mean(consecutive_public_years, na.rm = TRUE),
        avg_consecutive_private = mean(consecutive_private_years, na.rm = TRUE)
      )
    
    print(exp_summary)
    
    # Dominant sector summary
    sector_summary <- df %>%
      filter(!is.na(experience_sector_dominant)) %>%
      group_by(experience_sector_dominant) %>%
      summarize(count = n(), percentage = n() / nrow(.) * 100) %>%
      arrange(desc(count))
    
    cat("\nDominant experience sector:\n")
    print(sector_summary)
    
    # Career path summary if available
    if("career_path" %in% names(df)) {
      career_path_summary <- df %>%
        filter(!is.na(career_path)) %>%
        group_by(career_path) %>%
        summarize(count = n(), percentage = n() / nrow(.) * 100) %>%
        arrange(desc(count))
      
      cat("\nCareer path typology:\n")
      print(career_path_summary)
    }
  }
  
  # Ideology summary
  if("ideology_classification" %in% names(df)) {
    cat("\nIdeology classification summary:\n")
    
    ideology_summary <- df %>%
      filter(!is.na(ideology_classification)) %>%
      group_by(ideology_classification) %>%
      summarize(count = n(), percentage = n() / nrow(.) * 100) %>%
      arrange(desc(count))
    
    print(ideology_summary)
    
    # Simplified ideology if available
    if("ideology_simplified" %in% names(df)) {
      simplified_summary <- df %>%
        filter(!is.na(ideology_simplified)) %>%
        group_by(ideology_simplified) %>%
        summarize(count = n(), percentage = n() / nrow(.) * 100) %>%
        arrange(desc(count))
      
      cat("\nSimplified ideology distribution:\n")
      print(simplified_summary)
    }
  }
  
  # Political economy metrics if available
  if("polarization_index" %in% names(df)) {
    cat("\nPolitical economy metrics:\n")
    
    pol_econ_summary <- df %>%
      summarize(
        avg_polarization = mean(polarization_index, na.rm = TRUE),
        median_polarization = median(polarization_index, na.rm = TRUE),
        max_polarization = max(polarization_index, na.rm = TRUE, na.propagate = FALSE)
      )
    
    print(pol_econ_summary)
    
    # Education-ideology alignment if available
    if("education_ideology_alignment" %in% names(df)) {
      edu_ideo_summary <- df %>%
        filter(!is.na(education_ideology_alignment)) %>%
        group_by(education_ideology_alignment) %>%
        summarize(count = n(), percentage = n() / nrow(.) * 100) %>%
        arrange(desc(count))
      
      cat("\nEducation-Ideology alignment:\n")
      print(edu_ideo_summary)
    }
  }
}

# Generate specialized research datasets for further analysis
generate_research_datasets <- function(df, output_dir, run_id) {
  cat("\n===== GENERATING SPECIALIZED RESEARCH DATASETS =====\n")
  
  # 1. Education analysis dataset
  cat("Creating education analysis dataset...\n")
  education_fields <- c(
    "id_candidato", "nombre_completo_limpio", "sexo_genero", 
    "partido", "cargo", "entidad", "eleccion",
    "educacion", "nivel_educativo_estandarizado", "escala_educativa", 
    "educacion_significativa"
  )
  
  available_edu_fields <- intersect(education_fields, names(df))
  
  education_df <- df %>%
    select(all_of(available_edu_fields))
  
  write_csv(education_df, file.path(output_dir, 
                                    paste0("education_analysis_", run_id, ".csv")))
  
  # 2. Experience trajectory dataset
  cat("Creating experience trajectory dataset...\n")
  experience_fields <- c(
    "id_candidato", "nombre_completo_limpio", "sexo_genero", 
    "partido", "cargo", "entidad", "eleccion",
    "total_public_experience_years", "total_private_experience_years",
    "experience_sector_dominant", "has_mixed_experience", "consecutive_public_years",
    "consecutive_private_years", "career_start_year", "public_career_start",
    "private_career_start", "first_sector", "experience_diversity", "career_transitions"
  )
  
  available_exp_fields <- c(
    intersect(experience_fields, names(df)),
    grep("^public_experience_", names(df), value = TRUE),
    grep("^private_experience_", names(df), value = TRUE)
  )
  
  experience_df <- df %>%
    select(all_of(available_exp_fields))
  
  write_csv(experience_df, file.path(output_dir, 
                                     paste0("experience_analysis_", run_id, ".csv")))
  
  # 3. Ideology analysis dataset
  cat("Creating ideology analysis dataset...\n")
  ideology_fields <- c(
    "id_candidato", "nombre_completo_limpio", "sexo_genero", 
    "partido", "cargo", "entidad", "eleccion",
    "ideology_classification", "ideology_simplified", "economic_axis", "social_axis",
    "populist_tendency", "nationalist_tendency", "current_political_alignment",
    "economic_left_score", "economic_right_score",
    "social_progressive_score", "social_conservative_score",
    "populist_score", "nationalist_score", "amlo_4t_score", "opposition_score"
  )
  
  available_ideo_fields <- intersect(ideology_fields, names(df))
  
  ideology_df <- df %>%
    select(all_of(available_ideo_fields))
  
  write_csv(ideology_df, file.path(output_dir, 
                                   paste0("ideology_analysis_", run_id, ".csv")))
  
  # 4. Integrated political economy dataset
  cat("Creating integrated political economy dataset...\n")
  poliecon_fields <- c(
    "id_candidato", "nombre_completo_limpio", "sexo_genero", 
    "partido", "cargo", "entidad", "eleccion",
    "education_level", "education_ideology_alignment",
    "career_path", "experience_sector_dominant", 
    "ideology_simplified", "polarization_index",
    "total_public_experience_years", "total_private_experience_years",
    "economic_axis", "social_axis"
  )
  
  available_poliecon_fields <- intersect(poliecon_fields, names(df))
  
  poliecon_df <- df %>%
    select(all_of(available_poliecon_fields))
  
  write_csv(poliecon_df, file.path(output_dir, 
                                   paste0("poliecon_analysis_", run_id, ".csv")))
  
  cat("Successfully created specialized research datasets\n")
}

# Function to run the entire pipeline
run_pipeline <- function(input_files) {
  start_time <- Sys.time()
  cat("\n===== STARTING ENHANCED POLITICAL CANDIDATE DATA HOMOLOGATION PIPELINE 2.0 =====\n")
  cat("Run started at:", format(start_time, "%Y-%m-%d %H:%M:%S"), "\n")
  
  # Process the files and create integrated dataset
  homologated_df <- create_homologated_df_robust(input_files)
  
  # Save the full integrated dataset
  output_file <- file.path(config$output_dir, paste0("homologated_candidates_full_", config$run_id, ".csv"))
  cat("\nSaving integrated dataset to", output_file, "\n")
  write_csv(homologated_df, output_file)
  
  # Generate summary statistics
  generate_summary_statistics(homologated_df)
  
  # Create specialized research datasets
  generate_research_datasets(homologated_df, config$output_dir, config$run_id)
  
  # Create a clean version with key fields and derived variables
  cat("\nCreating clean version of the dataset with key fields...\n")
  base_key_fields <- c(
    # Identification
    "id_candidato", "nombre_completo", "nombre_completo_limpio", "nombre", 
    "apellido_paterno", "apellido_materno", "fecha_nacimiento", "edad", "sexo_genero",
    
    # Candidacy and election
    "partido", "cargo", "entidad", "municipio", "distrito", "eleccion", 
    
    # Education fields (including derived)
    "educacion", "estatus_educacion", "nivel_educativo_estandarizado", 
    "escala_educativa", "educacion_significativa",
    
    # Experience fields (including derived)
    "total_public_experience_years", "total_private_experience_years",
    "public_experience_ratio", "private_experience_ratio", 
    "has_mixed_experience", "experience_sector_dominant", "consecutive_public_years",
    "consecutive_private_years", "career_path",
    
    # Ideology fields
    "ideology_classification", "ideology_simplified", "economic_axis", "social_axis",
    "populist_tendency", "nationalist_tendency", "polarization_index",
    
    # Other fields
    "correo", "telefono_1", "pagina_web", "facebook", "twitter", "instagram"
  )
  
  # Create the clean dataframe using proper select syntax
  clean_df <- homologated_df %>% 
    select(
      all_of(intersect(base_key_fields, names(homologated_df))), 
      starts_with("public_experience_"), 
      starts_with("private_experience_")
    )
  
  clean_output_file <- file.path(config$output_dir, paste0("homologated_candidates_clean_", config$run_id, ".csv"))
  cat("Saving clean dataset to", clean_output_file, "\n")
  write_csv(clean_df, clean_output_file)
  
  # Runtime information
  end_time <- Sys.time()
  runtime <- difftime(end_time, start_time, units = "mins")
  
  cat("\n===== PIPELINE EXECUTION COMPLETED =====\n")
  cat("Run finished at:", format(end_time, "%Y-%m-%d %H:%M:%S"), "\n")
  cat("Total runtime:", round(as.numeric(runtime), 2), "minutes\n")
  cat("Processed", nrow(homologated_df), "unique candidate records\n")
  cat("Standardized", ncol(homologated_df), "fields\n")
  cat("Data sourced from", length(unique(homologated_df$source_file)), "different files\n")
  
  # Close log connections
  sink(type = "output")
  sink(type = "message")
  
  return(homologated_df)
}

#========================================================================
# IDEOLOGY CLASSIFICATION FUNCTIONS
#========================================================================

# Function to preprocess text for ideology analysis
preprocess_text <- function(text) {
  if(is.na(text) || nchar(text) == 0) return("")
  
  processed <- text %>%
    tolower() %>%
    # Remove accents
    stringi::stri_trans_general("Latin-ASCII") %>%
    # Remove punctuation
    gsub("[[:punct:]]", " ", .) %>%
    # Remove numbers
    gsub("[[:digit:]]", " ", .) %>%
    # Remove extra whitespaces
    gsub("\\s+", " ", .) %>%
    trimws()
  
  return(processed)
}

# Enhanced function to build ideology lexicon tailored to updated Mexican political discourse
build_mexican_ideology_lexicon <- function() {
  # Define lexicons for different ideological dimensions in Mexican context
  lexicon <- list(
    # Economic left - expanded for Mexican context
    economic_left = c(
      # General left economic terms
      "redistribucion", "redistribuir", "igualdad", "equidad", "progresivo", "justicia social", 
      "sindicato", "sindical", "trabajadores", "clase obrera", "anticapitalista", "subsidio", 
      "gratuito", "publico", "estado", "intervencion", "regulacion", "bienestar",
      
      # Mexican-specific left economic terms
      "programas sociales", "derechos sociales", "apoyo social", "solidaridad", "colectivo",
      "reparto", "nacional", "nacionalizacion", "soberania economica", "propiedad social",
      "propiedad colectiva", "propiedad estatal", "salarios justos", "prestaciones sociales",
      "seguridad social", "infonavit", "issste", "imss", "derecho laboral", "rescate",
      "fonden", "invernadero", "vivienda social", "impuestos progresivos", "progresividad fiscal",
      
      # MORENA/AMLO terminology
      "cuarta transformacion", "4t", "neoliberalismo", "primero los pobres", "pueblo", 
      "por el bien de todos", "austeridad republicana", "corrupcion", "bienestar del pueblo",
      "desarrollo integral", "beneficio colectivo", "economia moral"
    ),
    
    # Economic right - adapted for Mexican context
    economic_right = c(
      # General right economic terms
      "libre mercado", "competitividad", "privatizacion", "desregulacion", "eficiencia", 
      "reduccion impuestos", "crecimiento economico", "austeridad", "empresarial", "inversion",
      "emprendedor", "prosperidad", "libertad economica", "propiedad privada", "iniciativa privada",
      
      # Mexican-specific right economic terms
      "autosuficiencia", "desarrollo economico", "productividad", "capital", "libre comercio",
      "tlcan", "t-mec", "tratado comercial", "modelo exportador", "reforma estructural",
      "apertura economica", "inversion extranjera", "capitalizacion", "confianza empresarial",
      "disciplina fiscal", "reduccion del gasto", "reforma energetica", "participacion privada",
      "rescate bancario", "fobaproa", "autonomia", "banco central", "banxico", "libertad comercial",
      
      # PAN/Calderón/Anaya terminology
      "estabilidad economica", "reformas estructurales", "desarrollo empresarial", 
      "crecimiento sostenido", "competencia economica", "inflacion controlada", 
      "finanzas publicas sanas", "macroeconomia", "fortaleza economica"
    ),
    
    # Social progressive - adapted for Mexican social context
    social_progressive = c(
      # General progressive terms
      "diversidad", "inclusion", "igualdad de genero", "lgbtq", "derechos humanos", 
      "feminismo", "matrimonio igualitario", "aborto", "laico", "progresista",
      "migrantes", "multiculturalismo", "tolerancia", "medio ambiente", "cambio climatico",
      
      # Mexican-specific progressive terms
      "desarrollo sostenible", "derechos reproductivos", "equidad", "discriminacion", "minorias",
      "derechos indigenas", "pueblos originarios", "comunidades indigenas", "interrupcion embarazo", 
      "derechos sexuales", "diversidad sexual", "matrimonio entre personas", "adopcion igualitaria",
      "paridad", "violencia genero", "feminicidio", "despenalizacion", "legalizacion", 
      "derecho decidir", "estado laico", "educacion sexual", "identidad", "comunidad lgbtttiq",
      "equidad genero", "disidencias", "ecologismo", "ambientalismo", "proteccion ambiental",
      
      # Social movement terms
      "marea verde", "ni una menos", "desaparecidos", "ayotzinapa", "43", "movimiento estudiantil",
      "derechos de la mujer", "derecho a decidir", "educacion gratuita", "diversidades"
    ),
    
    # Social conservative - adapted for Mexican cultural context
    social_conservative = c(
      # General conservative terms
      "familia", "tradicional", "moral", "valores", "religion", "iglesia", "fe", 
      "seguridad", "orden", "disciplina", "patriotismo", "nacion", "soberania",
      
      # Mexican-specific conservative terms
      "conservador", "vida", "autoridad", "comunidad", "identidad", "herencia", "estabilidad",
      "delincuencia", "familia tradicional", "matrimonio hombre mujer", "proteccion vida",
      "concepcion", "no aborto", "provida", "valores familiares", "valores cristianos",
      "valores catolicos", "doctrina religiosa", "religion", "fe", "iglesia", "catolica",
      "evangelica", "conservadurismo", "derecho a la vida", "crimen", "delito", "castigo",
      "pena", "sancion", "mano dura", "carcel", "ejercito", "fuerzas armadas", "militares"
    ),
    
    # Populist rhetoric (cross-ideological in Mexico)
    populist = c(
      "pueblo", "elites", "corruptos", "mafia del poder", "oligarquia", "privilegiados",
      "politicos tradicionales", "casta politica", "establishment", "sistema", "cambio verdadero",
      "transformacion", "revolucion", "despertar", "esperanza", "dignidad", "soberania",
      "patria", "nacion", "mexicanos", "patrimonio nacional", "traicion", "vende patrias",
      "entreguismo", "soberanismo", "independencia", "autodeterminacion", "patriota"
    ),
    
    # Nationalist rhetoric (cross-ideological in Mexico)
    nationalist = c(
      "mexico", "mexicano", "patria", "nacion", "soberania", "independencia", "orgullo nacional",
      "identidad nacional", "simbolos patrios", "bandera", "himno nacional", "historia nacional",
      "heroes nacionales", "benito juarez", "lazaro cardenas", "emiliano zapata", "pancho villa",
      "indigenismo", "mexico profundo", "raices", "pasado glorioso", "grandeza", "potencia",
      "defensa territorio", "interes nacional", "dignidad nacional", "orgullo mexicano",
      "mexico primero", "no intervencion", "autodeterminacion", "principios constitucionales"
    ),
    
    # Contemporary 4T terms (2018-2025)
    amlo_4t = c(
      "cuarta transformacion", "4t", "amlove", "peje", "primero los pobres", 
      "no mentir", "no robar", "no traicionar", "bienestar", "programa social",
      "apoyo directo", "adulto mayor", "pension", "sembrando vida", "jovenes construyendo",
      "becas benito juarez", "servidor publico", "servidor de la nacion",
      "mañanera", "conferencia matutina", "quien es quien", "detente", "fuerza moral",
      "narcoestado", "mafia del poder", "conservador", "fifí", "pueblo bueno", 
      "austeridad republicana", "combate a la corrupcion"
    ),
    
    # Opposition terms
    opposition_terms = c(
      "frente amplio", "coalicion", "alianza opositora", "va por mexico", 
      "facismo", "autoritarismo", "dictadura", "militarizacion", "estado de derecho", 
      "division de poderes", "contrapeso", "pluralidad", "transparencia", "rendicion de cuentas",
      "sociedad civil", "mcci", "mexicanos contra la corrupcion", "latinus", "oposicion",
      "democratica", "narcopresidente", "narcoestado", "lopezobradorismo"
    ),
    
    # Security policy terms
    security_terms = c(
      "abrazos no balazos", "seguridad", "violencia", "crimen", "delincuencia", 
      "homicidio", "secuestro", "extorsion", "guardia nacional", "ejercito", 
      "militarizacion", "policia", "guerra contra el narco", "estrategia de seguridad",
      "pacificacion", "construir paz", "reconciliacion"
    ),
    
    # Economic policy terms
    economic_policy_terms = c(
      "reforma fiscal", "politica monetaria", "austeridad", "crecimiento economico",
      "inversion", "infraestructura", "obras prioritarias", "austeridad republicana",
      "combate a la corrupcion", "recaudacion", "impuestos", "subsidio", "tren maya",
      "dos bocas", "aeropuerto felipe angeles", "corredor transistmico"
    )
  )
  
  return(lexicon)
}

# Enhanced function to classify ideology based on text tailored to Mexican political spectrum
classify_mexican_ideology <- function(text, lexicon) {
  if(is.na(text) || nchar(text) == 0) return(list(ideology = "NO_CLASIFICADO"))
  
  # Preprocess text
  processed_text <- preprocess_text(text)
  
  # Calculate scores for each dimension
  economic_left_score <- sum(sapply(lexicon$economic_left, function(term) 
    stringr::str_count(processed_text, fixed(term))))
  
  economic_right_score <- sum(sapply(lexicon$economic_right, function(term) 
    stringr::str_count(processed_text, fixed(term))))
  
  social_progressive_score <- sum(sapply(lexicon$social_progressive, function(term) 
    stringr::str_count(processed_text, fixed(term))))
  
  social_conservative_score <- sum(sapply(lexicon$social_conservative, function(term) 
    stringr::str_count(processed_text, fixed(term))))
  
  populist_score <- sum(sapply(lexicon$populist, function(term)
    stringr::str_count(processed_text, fixed(term))))
  
  nationalist_score <- sum(sapply(lexicon$nationalist, function(term)
    stringr::str_count(processed_text, fixed(term))))
  
  # Calculate scores for contemporary terms
  amlo_4t_score <- sum(sapply(lexicon$amlo_4t, function(term)
    stringr::str_count(processed_text, fixed(term))))
  
  opposition_score <- sum(sapply(lexicon$opposition_terms, function(term)
    stringr::str_count(processed_text, fixed(term))))
  
  security_score <- sum(sapply(lexicon$security_terms, function(term)
    stringr::str_count(processed_text, fixed(term))))
  
  economic_policy_score <- sum(sapply(lexicon$economic_policy_terms, function(term)
    stringr::str_count(processed_text, fixed(term))))
  
  # Calculate relative positions on each axis
  economic_axis <- (economic_right_score - economic_left_score) / 
    max(1, economic_right_score + economic_left_score)
  
  social_axis <- (social_progressive_score - social_conservative_score) / 
    max(1, social_progressive_score + social_conservative_score)
  
  # Calculate populist and nationalist tendencies (0-1 scale)
  populist_tendency <- populist_score / max(1, nchar(processed_text) / 100)
  nationalist_tendency <- nationalist_score / max(1, nchar(processed_text) / 100)
  
  # Calculate contemporary tendencies
  current_political_alignment <- (amlo_4t_score - opposition_score) / 
    max(1, amlo_4t_score + opposition_score)
  
  # Classify based on both axes with Mexican political spectrum in mind
  ideology <- case_when(
    # Left-wing ideologies
    economic_axis < -0.3 && social_axis > 0.3 ~ "IZQUIERDA_PROGRESISTA", # MORENA, PT aligned
    economic_axis < -0.3 && social_axis < -0.3 ~ "IZQUIERDA_TRADICIONAL", # Old PRI aligned
    economic_axis < -0.3 && abs(social_axis) <= 0.3 ~ "IZQUIERDA_MODERADA", # PRD aligned
    
    # Right-wing ideologies
    economic_axis > 0.3 && social_axis < -0.3 ~ "DERECHA_CONSERVADORA", # PAN aligned
    economic_axis > 0.3 && social_axis > 0.3 ~ "DERECHA_LIBERAL", # MC aligned
    economic_axis > 0.3 && abs(social_axis) <= 0.3 ~ "DERECHA_MODERADA", # Modern PRI aligned
    
    # Centrist ideologies
    abs(economic_axis) <= 0.3 && social_axis > 0.3 ~ "CENTRO_PROGRESISTA",
    abs(economic_axis) <= 0.3 && social_axis < -0.3 ~ "CENTRO_CONSERVADOR",
    abs(economic_axis) <= 0.3 && abs(social_axis) <= 0.3 ~ "CENTRO",
    
    # Default
    TRUE ~ "NO_CLASIFICADO"
  )
  
  # Adjust for populist and nationalist tendencies
  populist_classifier <- if(populist_tendency > 0.2) "POPULISTA_" else ""
  nationalist_classifier <- if(nationalist_tendency > 0.2) "NACIONALISTA_" else ""
  
  # Adjust for contemporary political alignments
  if(amlo_4t_score > 3 && current_political_alignment > 0.3) {
    amlo_classifier <- "4T_"
  } else if(opposition_score > 3 && current_political_alignment < -0.3) {
    amlo_classifier <- "OPOSICION_"
  } else {
    amlo_classifier <- ""
  }
  
  if(populist_classifier != "" || nationalist_classifier != "" || amlo_classifier != "") {
    ideology <- paste0(populist_classifier, nationalist_classifier, amlo_classifier, ideology)
  }
  
  # Return a list with scores and classification
  return(list(
    ideology = ideology,
    economic_left_score = economic_left_score,
    economic_right_score = economic_right_score,
    social_progressive_score = social_progressive_score,
    social_conservative_score = social_conservative_score,
    populist_score = populist_score,
    nationalist_score = nationalist_score,
    amlo_4t_score = amlo_4t_score,
    opposition_score = opposition_score,
    security_score = security_score,
    economic_policy_score = economic_policy_score,
    economic_axis = economic_axis,
    social_axis = social_axis,
    populist_tendency = populist_tendency,
    nationalist_tendency = nationalist_tendency,
    current_political_alignment = current_political_alignment
  ))
}

# Enhanced function to apply ideology classification with robust error handling
add_mexican_ideology_classification <- function(df) {
  cat("Classifying candidate ideology based on text...\n")
  
  # Check if necessary fields exist for ideology analysis
  ideology_fields <- c("propuesta_1", "propuesta_2", "propuesta_3", 
                       "propuesta_genero", "motivo_cargo_publico", "trayectoria_politica")
  
  # Filter to only include fields that exist in the dataframe
  available_fields <- intersect(ideology_fields, names(df))
  
  # Log which fields are available for ideology analysis
  cat("Ideology analysis using fields:", paste(available_fields, collapse=", "), "\n")
  
  # If no ideology fields are available, create default classification
  if(length(available_fields) == 0) {
    cat("WARNING: No ideology fields found in dataframe. Creating default classification.\n")
    
    # Create default ideology classification
    df$ideology_classification <- "NO_CLASIFICADO"
    df$economic_left_score <- NA_real_
    df$economic_right_score <- NA_real_
    df$social_progressive_score <- NA_real_
    df$social_conservative_score <- NA_real_
    df$populist_score <- NA_real_
    df$nationalist_score <- NA_real_
    df$amlo_4t_score <- NA_real_
    df$opposition_score <- NA_real_
    df$security_score <- NA_real_
    df$economic_policy_score <- NA_real_
    df$economic_axis <- NA_real_
    df$social_axis <- NA_real_
    df$populist_tendency <- NA_real_
    df$nationalist_tendency <- NA_real_
    df$current_political_alignment <- NA_real_
    
    return(df)
  }
  
  # Create combined text field for ideology analysis in a simpler way
  df$ideology_text <- ""
  for(field in available_fields) {
    df$ideology_text <- paste(
      df$ideology_text,
      ifelse(is.na(df[[field]]), "", as.character(df[[field]])),
      sep = " "
    )
  }
  
  # Trim extra spaces
  df$ideology_text <- trimws(df$ideology_text)
  
  # Build enhanced Mexican lexicon
  ideology_lexicon <- build_mexican_ideology_lexicon()
  
  # Apply classification with error handling
  ideology_results <- lapply(df$ideology_text, function(text) {
    tryCatch({
      classify_mexican_ideology(text, ideology_lexicon)
    }, error = function(e) {
      cat("Error classifying ideology:", e$message, "\n")
      list(
        ideology = "NO_CLASIFICADO",
        economic_left_score = NA,
        economic_right_score = NA,
        social_progressive_score = NA,
        social_conservative_score = NA,
        populist_score = NA,
        nationalist_score = NA,
        amlo_4t_score = NA,
        opposition_score = NA,
        security_score = NA,
        economic_policy_score = NA,
        economic_axis = NA,
        social_axis = NA,
        populist_tendency = NA,
        nationalist_tendency = NA,
        current_political_alignment = NA
      )
    })
  })
  
  # Extract results
  df$ideology_classification <- sapply(ideology_results, function(x) x$ideology)
  df$economic_left_score <- sapply(ideology_results, function(x) x$economic_left_score)
  df$economic_right_score <- sapply(ideology_results, function(x) x$economic_right_score)
  df$social_progressive_score <- sapply(ideology_results, function(x) x$social_progressive_score)
  df$social_conservative_score <- sapply(ideology_results, function(x) x$social_conservative_score)
  df$populist_score <- sapply(ideology_results, function(x) x$populist_score)
  df$nationalist_score <- sapply(ideology_results, function(x) x$nationalist_score)
  df$amlo_4t_score <- sapply(ideology_results, function(x) x$amlo_4t_score)
  df$opposition_score <- sapply(ideology_results, function(x) x$opposition_score)
  df$security_score <- sapply(ideology_results, function(x) x$security_score)
  df$economic_policy_score <- sapply(ideology_results, function(x) x$economic_policy_score)
  df$economic_axis <- sapply(ideology_results, function(x) x$economic_axis)
  df$social_axis <- sapply(ideology_results, function(x) x$social_axis)
  df$populist_tendency <- sapply(ideology_results, function(x) x$populist_tendency)
  df$nationalist_tendency <- sapply(ideology_results, function(x) x$nationalist_tendency)
  df$current_political_alignment <- sapply(ideology_results, function(x) x$current_political_alignment)
  
  # Clean up temporary column
  df <- df %>% select(-ideology_text)
  
  return(df)
}

# Function to calculate political economy metrics
calculate_political_economy_metrics <- function(df) {
  df <- df %>%
    dplyr::mutate(
      # Calculate polarization index (how extreme the candidate positions are)
      polarization_index = sqrt(economic_axis^2 + social_axis^2),
      
      # Education-ideology correlation
      education_ideology_alignment = case_when(
        (escala_educativa >= 5 & abs(economic_axis) > 0.3) ~ "High Ed. Ideological",
        (escala_educativa >= 5 & abs(economic_axis) <= 0.3) ~ "High Ed. Moderate",
        (escala_educativa < 5 & abs(economic_axis) > 0.3) ~ "Low Ed. Ideological",
        (escala_educativa < 5 & abs(economic_axis) <= 0.3) ~ "Low Ed. Moderate",
        TRUE ~ NA_character_
      ),
      
      # Career path typology
      career_path = case_when(
        (total_public_experience_years > 10) ~ "Career Public Servant",
        (total_private_experience_years > 10) ~ "Career Private Sector",
        (total_public_experience_years > 5 & total_private_experience_years > 5) ~ "Mixed Career",
        (first_sector == "Público" & !is.na(private_career_start)) ~ "Public to Private",
        (first_sector == "Privado" & !is.na(public_career_start)) ~ "Private to Public",
        TRUE ~ "Limited Experience"
      ),
      
      # Political ideology simplified for analysis
      ideology_simplified = case_when(
        grepl("IZQUIERDA", ideology_classification) ~ "Left",
        grepl("DERECHA", ideology_classification) ~ "Right",
        grepl("CENTRO", ideology_classification) ~ "Center",
        TRUE ~ "Unclassified"
      ),
      
      # Education level simplified
      education_level = case_when(
        escala_educativa >= 6 ~ "Postgraduate",
        escala_educativa == 5 ~ "College",
        escala_educativa >= 3 ~ "High School/Technical",
        escala_educativa < 3 ~ "Basic Education",
        TRUE ~ "Unknown"
      )
    )
  
  return(df)
}

#========================================================================
# DATA PROCESSING FUNCTIONS
#========================================================================

# Process a single file with enhanced logging and validation
process_file_detailed <- function(file_path) {
  file_name <- tools::file_path_sans_ext(basename(file_path))
  cat("\nProcessing:", file_name, "\n")
  
  # Try to read file
  df <- read_any_file(file_path)
  if(is.null(df) || ncol(df) == 0) {
    cat("  Failed to read file or empty dataframe\n")
    return(NULL)
  }
  
  # Create standardized dataframe
  std_df <- data.frame(
    source_file = rep(file_name, nrow(df)),
    source_row = 1:nrow(df),
    processing_time = rep(Sys.time(), nrow(df)),
    processing_run = rep(config$run_id, nrow(df)),
    stringsAsFactors = FALSE
  )
  
  # Get field mapping
  field_info <- create_enhanced_field_mapping(
    unmatched_report = file.path(config$report_dir, paste0("unmatched_columns_", config$run_id, ".csv"))
  )
  field_mapping <- field_info$mapping
  
  # Track matched and unmatched columns
  matched_columns <- character(0)
  original_columns <- names(df)
  
  # Map columns with detailed logging and type conversion
  for(std_col in names(field_mapping)) {
    matching_col <- find_column_enhanced(df, field_mapping[[std_col]])
    
    if(!is.null(matching_col)) {
      # Convert to character to avoid type conflicts in initial integration
      std_df[[std_col]] <- as.character(df[[matching_col]])
      matched_columns <- c(matched_columns, matching_col)
    }
  }
  
  # Log unmatched columns for inspection
  unmatched <- setdiff(original_columns, matched_columns)
  if(length(unmatched) > 0) {
    cat("  Unmatched columns:", paste(unmatched, collapse=", "), "\n")
    # Save unmatched columns to a reference file for later inspection
    unmatched_df <- data.frame(
      source_file = file_name,
      unmatched_column = unmatched,
      stringsAsFactors = FALSE
    )
    unmatched_file <- file.path(config$report_dir, paste0("unmatched_columns_", config$run_id, ".csv"))
    if(file.exists(unmatched_file)) {
      # Append to existing file
      write_csv(unmatched_df, unmatched_file, append = TRUE)
    } else {
      # Create new file
      write_csv(unmatched_df, unmatched_file)
    }
  }
  
  # Add additional metadata
  std_df$record_count_in_source <- nrow(df)
  std_df$field_count_in_source <- ncol(df)
  std_df$matched_field_count <- sum(!is.na(std_df))
  
  return(std_df)
}

# Enhanced function to perform specialized data cleaning for common fields
clean_specialized_fields <- function(df) {
  # Create a copy to avoid modifying the original
  cleaned <- df
  
  # Clean names - extract first and last name if nombre_completo exists but individual fields don't
  if("nombre_completo" %in% names(cleaned)) {
    # Clean up candidate names by removing titles
    cleaned$nombre_completo_limpio <- sapply(cleaned$nombre_completo, clean_candidate_name)
    
    # Add standardized uppercase name without special characters
    cleaned$nombre_completo_mayus <- sapply(cleaned$nombre_completo, standardize_name_to_upper)
    
    # For records with nombre_completo but no nombre
    missing_nombre <- !is.na(cleaned$nombre_completo) & (is.na(cleaned$nombre) | cleaned$nombre == "")
    if(sum(missing_nombre) > 0) {
      # Extract first word as nombre
      cleaned$nombre[missing_nombre] <- sapply(
        cleaned$nombre_completo_limpio[missing_nombre],
        function(x) if(!is.na(x)) strsplit(trimws(x), "\\s+")[[1]][1] else NA
      )
    }
    
    # Try to extract apellido_paterno and apellido_materno if missing
    missing_apellidos <- !is.na(cleaned$nombre_completo) & 
      ((is.na(cleaned$apellido_paterno) | cleaned$apellido_paterno == "") | 
         (is.na(cleaned$apellido_materno) | cleaned$apellido_materno == ""))
    
    if(sum(missing_apellidos) > 0) {
      # Simple heuristic: for Spanish names, often the format is "Nombre ApellidoPaterno ApellidoMaterno"
      for(i in which(missing_apellidos)) {
        if(!is.na(cleaned$nombre_completo_limpio[i])) {
          name_parts <- strsplit(trimws(cleaned$nombre_completo_limpio[i]), "\\s+")[[1]]
          if(length(name_parts) > 1 && (is.na(cleaned$apellido_paterno[i]) || cleaned$apellido_paterno[i] == "")) {
            # Assume second word is paternal surname
            cleaned$apellido_paterno[i] <- name_parts[2]
          }
          if(length(name_parts) > 2 && (is.na(cleaned$apellido_materno[i]) || cleaned$apellido_materno[i] == "")) {
            # Assume last word is maternal surname
            cleaned$apellido_materno[i] <- name_parts[length(name_parts)]
          }
        }
      }
    }
  }
  
  # Standardize gender/sex values
  if("sexo_genero" %in% names(cleaned)) {
    cleaned$sexo_genero <- toupper(cleaned$sexo_genero)
    cleaned$sexo_genero <- case_when(
      cleaned$sexo_genero %in% c("M", "MASCULINO", "HOMBRE", "H", "MALE", "VAR", "VARON") ~ "M",
      cleaned$sexo_genero %in% c("F", "FEMENINO", "MUJER", "FEMALE") ~ "F",
      TRUE ~ cleaned$sexo_genero
    )
  }
  
  # Standardize political party names
  if("partido" %in% names(cleaned)) {
    cleaned$partido <- toupper(cleaned$partido)
    cleaned$partido <- case_when(
      grepl("^PAN$|ACCION NACIONAL", cleaned$partido) ~ "PAN",
      grepl("^PRI$|REVOLUCIONARIO INST", cleaned$partido) ~ "PRI",
      grepl("^PRD$|REVOLUCION DEMOCR", cleaned$partido) ~ "PRD",
      grepl("^PT$|TRABAJO", cleaned$partido) ~ "PT",
      grepl("^PVEM$|VERDE", cleaned$partido) ~ "PVEM",
      grepl("^MC$|MOVIMIENTO CIUDADANO", cleaned$partido) ~ "MC",
      grepl("^MORENA$", cleaned$partido) ~ "MORENA",
      grepl("^PES$|ENCUENTRO SOCIAL", cleaned$partido) ~ "PES",
      TRUE ~ cleaned$partido
    )
  }
  
  # Standardize educational status
  if("estatus_educacion" %in% names(cleaned)) {
    cleaned$estatus_educacion <- toupper(cleaned$estatus_educacion)
    cleaned$estatus_educacion <- case_when(
      grepl("CONCLU|TERMIN|TITULA|GRAD", cleaned$estatus_educacion) ~ "CONCLUIDO",
      grepl("TRUNC|INCOMPLET|NO CONCLU|NO TERMIN|CURS", cleaned$estatus_educacion) ~ "TRUNCO",
      grepl("EN CURSO|ACTUAL|ESTUDIA", cleaned$estatus_educacion) ~ "EN_CURSO",
      TRUE ~ cleaned$estatus_educacion
    )
  }
  
  # Clean URL fields
  url_fields <- c("pagina_web", "facebook", "twitter", "instagram")
  for(field in intersect(url_fields, names(cleaned))) {
    # Add protocol if missing
    no_protocol <- !is.na(cleaned[[field]]) & !grepl("^http", cleaned[[field]])
    if(sum(no_protocol) > 0) {
      cleaned[[field]][no_protocol] <- paste0("https://", cleaned[[field]][no_protocol])
    }
  }
  
  return(cleaned)
}

# Replace the existing function with this enhanced version
create_unique_identifier <- function(df) {
  # First handle potential NA values in key fields to prevent warnings
  df <- df %>% 
    mutate(across(c(where(is.character), where(is.factor)), ~na_if(., "")))
  
  # Create hierarchical unique identifier with multiple fallback options
  df <- df %>%
    mutate(
      unique_id = case_when(
        # Case 1: Use candidate ID if available (highest priority)
        !is.na(id_candidato) ~ paste0(source_file, "_", id_candidato),
        
        # Case 2: Use full combination of cleaned name, party, position and entity
        (!is.na(nombre_completo_limpio) & !is.na(partido) & !is.na(cargo) & !is.na(entidad)) ~ 
          paste0(normalize_text(nombre_completo_limpio, aggressive = TRUE), "_", 
                 normalize_text(partido, aggressive = TRUE), "_",
                 normalize_text(cargo, aggressive = TRUE), "_",
                 normalize_text(entidad, aggressive = TRUE)),
        
        # Case 3: Use cleaned name, party and position
        (!is.na(nombre_completo_limpio) & !is.na(partido) & !is.na(cargo)) ~ 
          paste0(normalize_text(nombre_completo_limpio, aggressive = TRUE), "_", 
                 normalize_text(partido, aggressive = TRUE), "_",
                 normalize_text(cargo, aggressive = TRUE)),
        
        # Case 4: Use cleaned name and party
        (!is.na(nombre_completo_limpio) & !is.na(partido)) ~ 
          paste0(normalize_text(nombre_completo_limpio, aggressive = TRUE), "_", 
                 normalize_text(partido, aggressive = TRUE)),
        
        # Case 5: Use district or municipality if available with name
        (!is.na(nombre_completo_limpio) & !is.na(distrito)) ~ 
          paste0(normalize_text(nombre_completo_limpio, aggressive = TRUE), "_distrito_", 
                 normalize_text(distrito, aggressive = TRUE)),
        
        (!is.na(nombre_completo_limpio) & !is.na(municipio)) ~ 
          paste0(normalize_text(nombre_completo_limpio, aggressive = TRUE), "_municipio_", 
                 normalize_text(municipio, aggressive = TRUE)),
        
        # Case 6: Use cleaned name with file prefix (lower priority)
        !is.na(nombre_completo_limpio) ~ 
          paste0(source_file, "_", normalize_text(nombre_completo_limpio, aggressive = TRUE)),
        
        # Case 7: Use raw name with file prefix if cleaned name not available
        !is.na(nombre_completo) ~ 
          paste0(source_file, "_", normalize_text(nombre_completo, aggressive = TRUE)),
        
        # Case 8: Fallback to source file and row as last resort
        TRUE ~ paste0(source_file, "_row", source_row)
      )
    )
  
  # Verify uniqueness and handle any remaining duplicates by adding suffix
  duplicate_ids <- df$unique_id[duplicated(df$unique_id)]
  
  if(length(duplicate_ids) > 0) {
    # For each duplicate, add a counter suffix
    for(dup_id in unique(duplicate_ids)) {
      dup_rows <- which(df$unique_id == dup_id)
      if(length(dup_rows) > 1) {
        for(i in 1:length(dup_rows)) {
          df$unique_id[dup_rows[i]] <- paste0(df$unique_id[dup_rows[i]], "_dup", i)
        }
      }
    }
  }
  
  # Add a check digit based on hash of the identifier for added uniqueness
  df <- df %>%
    mutate(
      id_check_sum = as.integer(abs(digest::digest2int(unique_id)) %% 100),
      unique_id = paste0(unique_id, "_", sprintf("%02d", id_check_sum))
    ) %>%
    select(-id_check_sum)  # Remove the temporary column
  
  return(df)
}

#========================================================================
# MAIN EXECUTION
#========================================================================

# Define all files to process
all_files <- file.path(config$input_dir, c(
  "AGUASCALIENTES.xlsx", "baseDatosCandidatos.xls", "BCS.xlsx", "candidates.csv",
  "candidatos_merged_2018.csv", "CHIAPAS.xlsx", "CHICHUAHUA.xlsx", "COAHUILA.xlsx",
  "COLIMA_3.xlsx", "COLIMA_DIP.xlsx", "COLIMA.xlsx", "EDOMEX.csv",
  "EDOMEX2.xlsx", "ELECCIONES_2016.csv", "ELECCIONES_2018.csv", "ELECCIONES_2021.xls",
  "ELECCIONES_2022.xlsx", "GUANAJUATO.xlsx", "HIDALGO.xls", "JALISCO.xlsx",
  "legislators_data_latest.csv", "MORELOS.xlsx", "NUEVOLEON.xlsx", "OAXACA.xlsx",
  "PUEBLA.xlsx", "QUERETARO.xlsx", "SIL_Mayors.dta", "SINALOA.xlsx",
  "SLP.csv", "SOLICITUD_1.xlsx", "SOLICITUD_2.xlsx", "SOLICITUD_3.xlsx",
  "SOLICITUD_4.xlsx", "SONORA.xlsx", "TABASCO.xlsx", "TAMAULIPAS.xlsx",
  "TLAXCALA_2.xlsx", "Tlaxcala.xlsx", "YUCATAN.xlsx", "ZACATECAS.xlsx"
))

# Run the pipeline
homologated_df <- run_pipeline(all_files)
