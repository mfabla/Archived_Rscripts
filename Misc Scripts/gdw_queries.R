#! GDW query functions


# sba placed order query --------------------------------------------------

#' Query SBA Placed Orders (thru As400 only)
#'
#' A wrapper for querying SBA placed orders data
#'
#' @param start_date beginning date query in 'YYYY-MM-DD' format
#' @param end_date ending date in query in 'YYYY-MM-DD' format
#' @param gdw_username enter GDW username login. Typically, this is network login.
#' @param gdw_password enter GDW password login. Typically, this is network password.
#' @param teradata_police stopper to ensure time range max of query is no more than one period. Default value is TRUE.
#' To query a time range greater than one period, set \code{teradata_police} to FALSE.
#' @param dropship_flg option to add fields to idenitfy if order has at least one dropship or stock item. Default value is FALSE.
#' @param print_sql Print SQL executed by function in the console window.
#'
#' @family query functions
#' @note The function opens and closes an ODBC connection using \code{RODBC:odbcCOnnect()}
#' @return a SBA Placed Orders data.frame object
#' @export
#'
#' @examples
#'  \dontrun{
#'  sba_orders <- query_sba_placedorders('2018-01-01', '2018-01-02', 'xxxxx', 'xxxxx', dropship_flg = T)
#'  }

  query_sba_placedorders <- function(start_date, end_date, gdw_username, gdw_password,
                                     teradata_police = TRUE, dropship_flg = FALSE, print_sql = FALSE){

    # validate date arguments
    if (missing(start_date)) {
      return(print('Need to enter an end date'))
    }
    if (stringr::str_detect(start_date, '^201[0-9]-[0-9]{1,2}-[0-9]{1,2}$') == F) {
      return("incorrect start_date format, use 'YYYY-MM-DD'")
    }
    if (missing(end_date)) {
      return(print('Need to enter an end date'))
    }
    if (stringr::str_detect(end_date, '^201[0-9]-[0-9]{1,2}-[0-9]{1,2}$') == F) {
      return("incorrect end_date format, use 'YYYY-MM-DD'")
    }
    if (start_date > end_date) return('check your dates')

    # teradata police check

    if (as.numeric(difftime(end_date, start_date, units = 'days')) > 35 & teradata_police == TRUE) {
      return(cat("timeframe is greater than one period and may increase query processing time. \nto override, set teradata_police = FALSE.\n"))
    }

    # server and RODBC arguments
    if ("RODBC" %in% rownames(installed.packages()) == FALSE) {
      return("stop! need to install RODBC package to run this function")
    }
    if (any(c("gdw", "GDW", "GDWPROD") %in% names(RODBC::odbcDataSources(type = "user"))) == F) {
      return("stop! verify you have access to gdw and in your ODBC connection it is named gdw, GDW or GDWPROD")
    }

    # build query string
    start_date_gdw <- as.numeric(gsub("-", "", start_date))
    end_date_gdw <- as.numeric(gsub("-", "", end_date))

    if (dropship_flg == F) {
      query_string <- paste0("
                             SELECT distinct
                             t1.Ohord#,
                             t1.Ohcust,
                             t1.Ohcrdt,
                             t1.Ohoto$,
                             t1.Ohoqty
                             FROM 	PRD_USD_OPV.SUNRISE_SFBASLIB_SFORDH_V t1
                             join PRD_USD_OPV.SUNRISE_SFBASLIB_SFORDR_V  t2  on ( t1.OHORD#=t2.ORORD# AND t1.OHLINK=t2.ORLINK)
                             WHERE
                             t1.Ohlink=0 AND
                             t1.Ohtype in ('OR', 'QU', 'MO') AND
                             t1.Ohcrdt between ",start_date_gdw," and ",end_date_gdw,"")
    } else {
      query_string <- paste0("
                             SELECT distinct
                             t1.Ohord#,
                             t1.Ohcust,
                             t1.Ohcrdt,
                             t1.Ohoto$,
                             t1.Ohoqty,
                             max(case when t2.ORTYPE = 'SK' then 1 else 0 end) as stock,
                             max(case when t2.ORDROP = 'Y' then 1 else 0 end) as dropship
                             FROM 	PRD_USD_OPV.SUNRISE_SFBASLIB_SFORDH_V t1
                             join PRD_USD_OPV.SUNRISE_SFBASLIB_SFORDR_V  t2  on ( t1.OHORD#=t2.ORORD# AND t1.OHLINK=t2.ORLINK)
                             WHERE
                             t1.Ohlink=0 AND
                             t1.Ohtype in ('OR', 'QU', 'MO') AND
                             t1.Ohcrdt between ",start_date_gdw," and ",end_date_gdw,"
                             GROUP BY
                             1,2,3,4,5")
    }

  # run query
  odbc_name <- names(RODBC::odbcDataSources(type = "user"))[which(names(RODBC::odbcDataSources(type = "user")) %in% c("GDWPROD","gdw", "GDW"))]
  conn <- RODBC::odbcConnect(odbc_name, uid = gdw_username, pwd = gdw_password)
  df <- RODBC::sqlQuery(conn, query_string, stringsAsFactors = F)
  RODBC::odbcClose(conn)

  if (print_sql == T) {
    cat('\n\nGDW SQL Query: \n', query_string, '\n\n')
  }

  df

  }
