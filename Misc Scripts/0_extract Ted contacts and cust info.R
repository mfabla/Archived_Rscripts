# Contact Extract and merge with Cust Segment Data
#
# 30 JAN 2018
# by Mike Abla
#
# NOTES:
# Ask is to pull 2 yrs of contact data and where SBA order is identified merge CDM data; 
# specifically, customer segment information.



# set env -----------------------------------------------------------------

library(rIA)

# extract ted data --------------------------------------------------------

ted.query <- function(start_date,end_date){
  connUAT <- odbcConnect('cs-db-uat') 
  ted.x <- NULL
  ted.x <- sqlQuery(connUAT, paste0("
                                    SELECT 
                                    'SBA' AS business_unit,
                                    CAST(ahdcallcommentdate AS date) AS date, 
                                    ahdcallorderacctnumber AS ted_reference, 
                                    dispcatname AS issue_reason, 
                                    ahdcalldispositionexplanation AS customer_story
                                    FROM ticketing.dbo.conahd_comment a
                                    JOIN ticketing.dbo.conahd_ahdcall b ON a.ahdcallid=b.ahdcallid
                                    JOIN ticketing.dbo.conahd_dispositioncategory c ON b.dispositioncatid=c.dispositioncatid
                                    WHERE CAST(ahdcallcommentdate AS date) BETWEEN '",start_date,"' and '",end_date,"'
                                    ORDER BY ahdcallcommentdate"), stringsAsFactors = F)
  
  ted.x$sba_order_number <- str_replace_all(ted.x$ted_reference, c('#'='', '^chat\\s+2[0-9]+/'='', '^[0-9]+/'=''))
  ted.x$sba_order_number <- as.numeric(ted.x$sba_order_number)
  odbcClose(connUAT)
  return(ted.x)
}

ted_df <- ted.query('2016-01-31', '2018-01-27' )
ted_sba_ords <- ted_df$sba_order_number[!is.na(ted_df$sba_order_number)]


# extract sba oms data ----------------------------------------------------

sba_ord_batcher <- function(x, len_batches){
  
  order_numbers <- unique(x[!is.na(x)])
  split_orders <- split(order_numbers, ceiling(seq_along(order_numbers)/len_batches))
  order_list <- lapply(split_orders, function(x)paste("'",x,"'", collapse = ",", sep = ""))
  
  query_ords <- c()
  for(i in seq_along(order_list)){
    
    string <- paste0("Select Distinct Ohord#, Ohcust
                     FROM PRD_USD_OPV.SUNRISE_SFBASLIB_SFORDH_V
                     WHERE Ohord# IN (",order_list[[i]],") and Ohlink= 0")
    
    query_ords <- c(query_ords, string)
  }
  
  connGDW <- odbcConnect('gdw')
  sba_orders <- lapply(query_ords, function(x) sqlQuery(connGDW, x, stringsAsFactors = F))
  sba_orders
  
  }

ord_cust_nos <- sba_ord_batcher(ted_sba_ords, 1000)
ord_cust_nos1 <- do.call("rbind", ord_cust_nos)
ord_cust_nos1[,1:2] <- sapply(ord_cust_nos1[,1:2], as.numeric)

# cdm extract -------------------------------------------------------------

sba.cdm.cust.query <- function(){
  connCDM <- odbcConnect('cdm')
  cdm_cust_ext <- sqlQuery(connCDM, "SELECT Distinct	
                           prtflo_seg_cd as cust_type,
                           mstr_cust_num,
                           mstr_cust_nm,
                           most_rcnt_mbrshp_typ_nm,
                           initl_mbrshp_prchs_dt
                           FROM	PRD_CONTR_DMV.D_CUST_MSTR_V
                           Where 1=1
                           AND mstr_cust_num <> 0", stringsAsFactors = F)
  cdm_cust_ext$last_update <- Sys.Date()
  odbcClose(connCDM)
  return(cdm_cust_ext)
}
sba_custs_df <- sba.cdm.cust.query()


# join it all together ----------------------------------------------------

final_df <- left_join(ted_df, ord_cust_nos1, by = c("sba_order_number" = "Ohord#")) %>%
  left_join(sba_custs_df, by = c("Ohcust" = "mstr_cust_num")) %>%
  rename(cust_id = Ohcust) %>%
  select(-most_rcnt_mbrshp_typ_nm, -last_update, -initl_mbrshp_prchs_dt)


# export ------------------------------------------------------------------

write.csv(final_df, 'FY16-FY17YTD TED Contacts.csv', row.names = F)
