#----------------------
# Autor:  Carlos Ortega
# Date:   2023-10-25
# Input:  File from Ministery with contracts CNAE (in xls!!)
# Output: data.table ready to process with all the columns right formatted.
#-----------------------


rm(list = ls())
tidytable::inv_gc()
# cat("\014")  # ctrl+L

suppressPackageStartupMessages({
  library(data.table)
  library(ggplot2)
  library(tictoc)
  library(stringr)
  library(janitor)
  library(ggeasy)
  library(stringi)
  library(forcats)
  library(lubridate)
  library(tidytable)
  library(SheetReader)
  library(broom)
  library(xml2)
  library(rvest)
  library(httr)
})

tini <- Sys.time()


miurl <- 'https://expinterweb.mites.gob.es/ibi_apps/WFServlet?IBIF_ex=SEFCNT01&OPC_PAD=12&OPC_PRN=1&VTIP_LLA=R&VINC_INF=&RND=2023111500.50.26&INFORME=NO&LGRUPOS=+++++++1&sel_10=12&TIENEGEO=N&ESTADISTICA=08&URL_INTERNET=INTERNET&SELANA=0000000000058&SELCLA=0000000000219&SEL1RES=844&SEL1RES=845&SEL1RES=846&SEL1RES=847&SEL1RES=848&SEL1RES=849&SEL1RES=850&SEL1RES=851&SEL1RES=852&SEL1RES=853&SEL1RES=854&SEL1RES=855&SEL1RES=856&SEL1RES=857&SEL1RES=858&SEL1RES=859&SEL1RES=860&SEL1RES=861&SEL1RES=862&SEL1RES=863&SEL1RES=864&SEL1RES=865&SEL1RES=866&SEL1RES=867&SEL1RES=868&SEL1RES=869&SEL1RES=870&SEL1RES=871&SEL1RES=872&SEL1RES=873&SEL1RES=874&SEL1RES=875&SEL1RES=876&SEL1RES=877&SEL1RES=878&SEL1RES=879&SEL1RES=880&SEL1RES=881&SEL1RES=882&SEL1RES=883&SEL1RES=884&SEL1RES=885&SEL1RES=886&SEL1RES=887&SEL1RES=888&SEL1RES=889&SEL1RES=890&SEL1RES=891&SEL1RES=892&SEL1RES=893&SEL1RES=894&SEL1RES=895&SEL1RES=896&SEL1RES=897&SEL1RES=898&SEL1RES=899&SEL1RES=900&SEL1RES=901&SEL1RES=902&SEL1RES=903&SEL1RES=904&SEL1RES=905&SEL1RES=906&SEL1RES=907&SEL1RES=908&SEL1RES=909&SEL1RES=910&SEL1RES=911&SEL1RES=912&SEL1RES=913&SEL1RES=914&SEL1RES=915&SEL1RES=916&SEL1RES=917&SEL1RES=918&SEL1RES=919&SEL1RES=920&SEL1RES=921&SEL1RES=922&SEL1RES=923&SEL1RES=924&SEL1RES=925&SEL1RES=926&SEL1RES=927&SEL1RES=928&SEL1RES=929&SEL1RES=930&SEL1RES=931&SEL1RES=932&SEL1RES=967&ANYO=2023&ANYO=2022&ANYO=2021&ANYO=2020&ANYO=2019&ANYO=2018&ANYO=2017&ANYO=2016&ANYO=2015&ANYO=2014&ANYO=2013&ANYO=2012&ANYO=2011&ANYO=2010&ANYO=2009&VAL_PER_INF=1+&VAL_PER_INF=2+&VAL_PER_INF=3+&VAL_PER_INF=4+&VAL_PER_INF=5+&VAL_PER_INF=6+&VAL_PER_INF=7+&VAL_PER_INF=8+&VAL_PER_INF=9+&VAL_PER_INF=10&VAL_PER_INF=11&VAL_PER_INF=12&TIP_FOR=EXC2K&INFORME1=SI'

# Enviar solicitud GET
respuesta <- GET(miurl, config(ssl_verifypeer = 0L))

# Extraer la cabecera 'Content-Disposition'
content_disposition <- headers(respuesta)$`content-disposition`

# Analizar para obtener el nombre del archivo
nombre_archivo <- str_match(content_disposition, 'filename="([^"]+)"')[,2]
nombre_archivo <- "./output/Salida_MT_.xls"

# Verifica si se extrajo correctamente el nombre del archivo
if (!is.na(nombre_archivo) && nombre_archivo != "") {
  # Guardar el contenido de la respuesta en un archivo con el nombre extraído
  writeBin(respuesta$content, nombre_archivo)
} else {
  warning("No se pudo extraer el nombre del archivo de la respuesta HTTP.")
}

#---  In fact xls file is an html file. Process in that way.
mifile <- "./output/Salida_MT_.xls"
dattmp <- read_html(mifile)

#-- File includes a table with the data. Extract it with rvest.
datbad <- dattmp %>%
  html_element("table") %>%
  html_table() %>%
  as.data.table() %>%
  drop_na() %>%
  mutate(
    across(where(is.character), ~ stri_replace_all_fixed(.x, ",", ""))
  ) %>%
  as.data.table()

#--- Put the right headers
micabe <- datbad[1, ]  %>%
  pivot_longer( cols = everything()) %>%
  pull(value)

datbad %<>% 
  filter(X3 != "TOTAL") %>%
  as.data.table()

names(datbad) <- micabe

#--- Put dataframe in the right format to process it
datin <- datbad %>%
  clean_names() %>%
  rename( anio = 1) %>%
  rename( mes = 2) %>%
  mutate( 
    mes = stri_replace_all_fixed(
      mes,
      c('Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio', 
        'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre', '  '),
      c('1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', ' '),
      vectorize_all = FALSE
    )
  ) %>%
  mutate( anio = as.numeric(anio)) %>%
  mutate( mes = as.numeric(mes)) %>%
  fill(anio) %>%
  pivot_longer( cols = c(contains("total"):contains("nc_no")) )  %>%
  mutate( name = stri_replace_all_fixed(name, "x", "") ) %>%
  mutate( value = as.numeric(value)) %>%
  as.data.table()

#--------- END OF FILE ----
tend <- Sys.time(); tend - tini
# Time difference of 9.485486 secs
