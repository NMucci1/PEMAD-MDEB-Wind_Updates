#########################################################
## Scrape USCG MSI webpage for info. on OSW activities ##  
#########################################################

## USCG MSI url
uscg_url <- "https://www.navcen.uscg.gov"

## scrape USCG MSI page
uscg_msi <- paste0(uscg_url, '/msi') |>
  httr2::request() |>
  httr2::req_perform() |>
  httr2::resp_body_html()

## grab Light List (LL) links to download
ll_links <- uscg_msi |>
  rvest::html_element('#llFileListing') |>
  rvest::html_elements('a') |>
  rvest::html_attr('href')

## extract district IDs from links
# D: Matches the literal character "D"
# ( ... ): This is a capturing group, which saves text that matches this part
# [^\\s_]*: Don't match white space, \\s, or underscores, _
# *: Match capture group zero or more times between 'D' and next '_'
ll_districts <- ll_links |>
  basename() |>
  tools::file_path_sans_ext() |> 
  stringr::str_extract(pattern = "D([^\\s_]*)") 

## filter LL links for districts 1, 5
ll_filt_links <- ll_links[which(ll_districts %in% c('D01', 'D05'))]

## download LL data using filtered links
for (file in ll_filt_links) {
  
  ## downloaded file name
  filename = file |>
    basename()
  
  ## download file
  download.file(url = paste0(uscg_url, file), destfile = here::here("data-raw", filename), mode = "wb")

}

## paths to data files
data_files <- here::here("data-raw") |> 
  list.files(full.names = TRUE)

## aggregate data
ll_data <- lapply(X = data_files, FUN = function(X) {
  
  ## load data
  chk_data <- X |> 
    sf::st_read()
  
  ## check for WTGs, OSSs
  chk_wtgs <- grepl(pattern = 'WTG|PROD', x = chk_data$NAME)
  if (any(chk_wtgs)) {
    
    ## format output
    out_df = chk_data |> 
      dplyr::filter(chk_wtgs) |>
      dplyr::select(NAME, DESCRIPTION_TYPE, REMARK, STRUCTURE_REMARK, CREATE_DATE, MODIFIED_DATE, DECIMAL_LONGITUDE, DECIMAL_LATITUDE, geometry)
    
    ## return
    return(out_df)
    
  }
  
})
 
## remove NULL elements
ll_data <- Filter(Negate(is.null), ll_data)

## format data
ll_df <- do.call(dplyr::bind_rows, ll_data) |>
  dplyr::filter(DESCRIPTION_TYPE %in% c('LT', 'LB')) |>
  dplyr::mutate(OWF = gsub(pattern = 'WTG.*|PROD.*', replacement = '', x = NAME) |> trimws(),
                WTG_NAME = stringr::str_extract_all(string = NAME, pattern = '[a-zA-Z0-9]+')) |>
  dplyr::rowwise() |>
  dplyr::mutate(STRUCTURE_ID = dplyr::last(WTG_NAME)) |>
  dplyr::select(-WTG_NAME) |>
  dplyr::select(OWF, WTG_ID, NAME, DESCRIPTION_TYPE, REMARK, STRUCTURE_REMARK, CREATE_DATE, MODIFIED_DATE, DECIMAL_LONGITUDE, DECIMAL_LATITUDE, geometry)

## fix name error
ll_df$OWF[which(ll_df$OWF == 'Vineyard Wind')] = 'Vineyard Wind 1'
ll_df |>
  dplyr::group_by(OWF) |>
  dplyr::summarize(n = dplyr::n())

## output
sf::st_write(obj = ll_df, dsn = here::here('data', 'uscg_msi_struct.gdb'), delete_dsn = TRUE, append = FALSE)
