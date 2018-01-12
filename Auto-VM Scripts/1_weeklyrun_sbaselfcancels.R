###### Weekly Script to Update Tables for the SBA Self-Cancel Dashboard #############

# This is the weekly script to run and update the following dev tables needed for SBA Self-Cancels Dashboard:
#     [CSAQuery].[dbo].[sba_cancels]
#     [CSAQuery].[dbo].[sba_selfcancels]
#     [CSAQuery].[dbo].[csi_cancel_rtrns]
#     [CSAQuery].[dbo].[sba_ordercounts]


# set env -----------------------------------------------------------------

library(rIA)
set_env(clear_env = T,  dir = "~/VM-jobs/R", extra_pkgs = c('mailR'))


# weekly run --------------------------------------------------------------

sba_selfcancel_weekly <- function(){
  
  sun_pswd <- c('xxxxxx')
  password <- 'xxxxxxx'
  
  #previous week
  prev_wk <- calendar %>%
    filter(calendar_date == Sys.Date() - 7) %>%
    select(fiscal_week_of_period_long, fiscal_week_start_date, fiscal_week_end_date)
  
  #date range
  start_date <- as.character(prev_wk$fiscal_week_start_date)
  end_date <- as.character(prev_wk$fiscal_week_end_date)
  gdw_start_date <- as.numeric(gsub("-", "", as.character(prev_wk$fiscal_week_start_date)))
  gdw_end_date <- as.numeric(gsub("-", "", as.character(prev_wk$fiscal_week_end_date)))
  
  #fiscal wk for report
  fw <- prev_wk$fiscal_week_of_period_long
  
  #calendar
  df_calendar <- calendar %>%
    select(calendar_date, fiscal_week_start_date, fiscal_week_end_date, fiscal_week_of_period_long, fiscal_quarter, holiday_flag) %>%
    mutate(holiday_flag = ifelse(holiday_flag == 'Y', 1, 0))
  
  #check if update is needed
  cat("\n running report checker...")
  connDEV <- NULL
  connDEV <- odbcConnect('cs-db-dev')
  last_update_cancels <- sqlQuery(connDEV, paste0("Select distinct [fiscal_week_of_period_long] FROM [CSAQuery].[dbo].[sba_cancels]"), stringsAsFactors = F)
  last_update_selfcancels <- sqlQuery(connDEV, paste0("Select distinct [fiscal_week_of_period_long] FROM [CSAQuery].[dbo].[sba_selfcancels]"), stringsAsFactors = F)
  last_update_csicancels <- sqlQuery(connDEV, paste0("Select distinct [fiscal_week_of_period_long] FROM [CSAQuery].[dbo].[csi_cancel_rtrns]"), stringsAsFactors = F)
  last_update_orders <- sqlQuery(connDEV, paste0("Select distinct [fiscal_week_of_period_long] FROM [CSAQuery].[dbo].[sba_ordercounts]"), stringsAsFactors = F)
  odbcClose(connDEV)
  last_update <- rbind(last_update_cancels, last_update_selfcancels, last_update_csicancels, last_update_orders) %>% distinct()
  
  if(any(fw %in% last_update$fiscal_week_of_period_long)) {
   return(cat("\n already up-to-date"))} else{
   
     #orders & ordermethods
     cat("\n querying order data...")
     connDEV <- NULL
     connDEV <- odbcConnect('cs-db-dev')
     orderm_ref <- sqlQuery(connDEV, "Select OrderID, OrderMethod1 FROM CSAQuery.dbo.sba_ordermethod_ref", stringsAsFactors = F)
     odbcClose(connDEV)
     
     connGDW <- NULL
     connGDW <- odbcConnect("gdw")
     ordm <- sqlQuery(connGDW, paste0("SELECT distinct
                                               t1.Ohord#,
                                               t1.Ohcrdt,
                                               t1.Ohcust,
                                               t1.Ohordm
                                               FROM 	PRD_USD_OPV.SUNRISE_SFBASLIB_SFORDH_V t1
                                               WHERE 
                                               t1.Ohlink=0 AND
                                               t1.Ohtype in ('OR', 'QU', 'MO') AND 
                                               t1.Ohcrdt between ",gdw_start_date," and ",gdw_end_date,""), stringsAsFactors = F)
     odbcClose(connGDW)
     
    df_orders <- left_join(ordm, orderm_ref, by = c("Ohordm" = "OrderID")) %>%
       mutate(total_orders = 1, 
              elg_orders = ifelse(OrderMethod1 == 'Internet', 1, 0),
              calendar_date = as.Date.character(Ohcrdt, format = "%Y%m%d")) %>%
       inner_join(df_calendar, by = c("calendar_date")) %>%
       group_by(fiscal_week_of_period_long, fiscal_week_start_date, fiscal_week_end_date) %>%
       summarise(total_orders = sum(total_orders, na.rm = T),
                 elg_orders = sum(elg_orders, na.rm = T))
     
     #total cancelled orders
     cat("\n querying cancelled orders...")
     
     connGDW <- NULL
     connGDW <- odbcConnect('gdw')
     sba_cancels <- sqlQuery(connGDW, paste0("SELECT distinct
                             t1.Ohord#,
                             t1.Ohordm,
                             t1.Ohcust,
                             t1.Ohcrdt, 
                             t1.Ohcrtm,
                             t1.Ohcndt, 
                             t1.Ohcntm
                             FROM	PRD_USD_OPV.SUNRISE_SFBASLIB_SFORDH_V t1 
                             Join PRD_USD_OPV.SUNRISE_SFBASLIB_SFORDD_V t2 on t1.Ohord# =  t2.Odord# and t1.Ohlink=t2.Odlink
                             where t1.Ohlink= 0 and (t1.Ohcrdt between ", gdw_start_date ," and ", gdw_end_date ,") and t2.Odstat = 'CAN' and t1.Ohcntm <> 0"), stringsAsFactors = F)
     odbcClose(connGDW)
     
     df_sba_cancels <- sba_cancels %>%
       mutate(calendar_date = as.Date.character(Ohcrdt, format = "%Y%m%d"),
              cancel_date = as.Date.character(Ohcndt, format = "%Y%m%d")) %>%
       inner_join(df_calendar, by = c("calendar_date"))
     
     df_sba_cancels1 <- df_sba_cancels %>%
       left_join(orderm_ref, by = c("Ohordm" = "OrderID")) %>%
       mutate(elg_cancel = ifelse(OrderMethod1 == 'Internet', 1, 0),total_cancels = 1) %>%
       group_by(fiscal_week_of_period_long, fiscal_week_start_date, fiscal_week_end_date, fiscal_quarter) %>%
       summarise(total_cancels = sum(total_cancels, na.rm = T),
                 elg_cancels = sum(elg_cancel, na.rm = T))
     
     #csi data and merge w/ eligible orders
     cat("\n querying csi data...")
     connUAT <- NULL
     connUAT <- odbcConnect('cs-db-uat')
     csi_cncl_rtrns <- sqlQuery(connUAT, paste0("SELECT [FCR_ID],[CONTACT_METHOD],[FCR_DATE],[CSI_BU_NAME],[Topic],[Type],[ORDERNUMBER],[CUSTOMERNUMBER_ACCOUNTNUMBER]
                                FROM [NADCS_DW].[dbo].[smryCSIReporting]
                                Where 1=1
                                AND Topic in ('Return', 'Cancel', 'Change Order')
                                AND CSI_BU_NAME = 'SA US'
                                AND FCR_DATE between '", start_date,"' and '", end_date,"'"), stringsAsFactors = F)
     odbcClose(connUAT)
     csi_cncl_rtrns1 <- left_join(csi_cncl_rtrns, ordm, by = c("ORDERNUMBER" = "Ohord#")) %>%
       mutate(cust_id = ifelse(!is.na(CUSTOMERNUMBER_ACCOUNTNUMBER), CUSTOMERNUMBER_ACCOUNTNUMBER, Ohcust),
              FCR_DATE = as.Date(FCR_DATE, format = '%Y-%m-%d')) %>%
       select(-CUSTOMERNUMBER_ACCOUNTNUMBER, -Ohcust) %>%
       left_join(orderm_ref, by = c("Ohordm" = "OrderID"))
     df_csi_cncl_rtrns <- csi_cncl_rtrns1  %>% rename(calendar_date = FCR_DATE)
     
     df_csi_cncl_rtrns1 <- df_csi_cncl_rtrns %>%
       left_join(select(.data = df_calendar, calendar_date, fiscal_week_start_date, fiscal_week_end_date, fiscal_week_of_period_long), by = c("calendar_date")) %>%
       mutate(total_return_contacts = ifelse(Topic == 'Return', 1, 0),
              total_cancel_contacts = ifelse(Topic == 'Cancel', 1, 0),
              total_changeorder_contacts = ifelse(Topic == 'Change Order', 1, 0),
              elg_return_contacts = ifelse(Topic == 'Return' & OrderMethod1 == 'Internet', 1, 0),
              elg_cancel_contacts = ifelse(Topic == 'Cancel' & OrderMethod1 == 'Internet', 1, 0),
              elg_changeorder_contacts = ifelse(Topic == 'Change Order' & OrderMethod1 == 'Internet', 1, 0)) %>%
       group_by(fiscal_week_of_period_long, fiscal_week_start_date, fiscal_week_end_date) %>%
       summarise(total_return_contacts = sum(total_return_contacts, na.rm = T), 
                 elg_return_contacts = sum(elg_return_contacts, na.rm = T),
                 total_cancel_contacts = sum(total_cancel_contacts, na.rm = T),
                 elg_cancel_contacts = sum(elg_cancel_contacts, na.rm = T),
                 total_changeorder_contacts = sum(total_changeorder_contacts, na.rm = T),
                 elg_changeorder_contacts = sum(elg_changeorder_contacts, na.rm = T)) %>%
       arrange(fiscal_week_start_date)
     
     #self cancel reasons
     cat("\n finalizing self-cancel reasons and counts...")
     
     connSUN <- NULL
     connSUN <- odbcConnect(dsn = 'Sunsysh', uid = 'CCFLMZA', pwd = sun_pswd)
     sba_selfcancels <- sqlQuery(connSUN, paste0("SELECT DISTINCT CLORD#, CLLINK, CLSUCSS, CLCRESN, CLCRDT, CLCRTM
                                                 FROM SFBASLIB.SFSBACAN
                                                 Where 1=1
                                                 AND CLSUCSS = 'Y' 
                                                 AND CLCRDT between ", gdw_start_date ," and ", gdw_end_date ,""), stringsAsFactors = F)
     odbcClose(connSUN)
     sba_selfcancels$selfcan_reason <- str_replace(sba_selfcancels$CLCRESN, "^.*\\Specific\\b" , "") 
     sba_selfcancels$selfcan_reason <- ifelse(str_detect(sba_selfcancels$selfcan_reason, "^ "), trim(str_replace(sba_selfcancels$selfcan_reason, " - " , "")), "No Response")
     sba_selfcancels$`CLORD#` <- as.numeric(sba_selfcancels$`CLORD#`)
     
     #sba_cancels1 <- left_join(sba_cancels, select(.data = sba_selfcancels, `CLORD#`, CLSUCSS, selfcan_reason), by = c("Ohord#"= "CLORD#")) %>%
      # mutate(self_can = ifelse(is.na(CLSUCSS), 0, 1)) %>%
      # select(-CLSUCSS)
     
     sba_selfcancels1 <- sba_selfcancels %>%
       mutate(self_can = ifelse(is.na(CLSUCSS), 0, 1),
              calendar_date = as.Date.character(CLCRDT, format = '%Y%m%d')) %>%
       left_join(df_calendar, by = c("calendar_date")) %>%
       dcast(fiscal_week_of_period_long + fiscal_week_start_date + fiscal_week_end_date~ selfcan_reason, fun.aggregate = sum, value.var = 'self_can', fill = 0) %>%
       mutate(total_selfcancels = rowSums(.[-(1:3)], na.rm = T))
     names(sba_selfcancels1) <- gsub(" ","", names(sba_selfcancels1))
     names(sba_selfcancels1) <- gsub("-","", names(sba_selfcancels1))
     
     connDEV <- NULL
     connDEV <- odbcConnect('cs-db-dev')
     selfcancel_columns <- sqlQuery(connDEV, "SELECT * FROM dbo.sba_selfcancels")
     odbcClose(connDEV)
     nms <- names(selfcancel_columns)
     
     Missing <- setdiff(nms, names(sba_selfcancels1))
     sba_selfcancels1[Missing] <- 0
     sba_selfcancels1 <- sba_selfcancels1[nms]
     
     #update dev tables
     cat("\n updating tables...")
     connDEV <- odbcConnect('cs-db-dev')
     sqlSave(connDEV, as.data.frame(df_csi_cncl_rtrns1), tablename = 'dbo.csi_cancel_rtrns', varTypes = c(fiscal_week_start_date = "date", fiscal_week_end_date = "date"), rownames = F, append = T)
     sqlSave(connDEV, as.data.frame(df_sba_cancels1), tablename = 'dbo.sba_cancels', varTypes = c(fiscal_week_start_date = "date", fiscal_week_end_date = "date"), rownames = F, append = T)
     sqlSave(connDEV, as.data.frame(sba_selfcancels1), tablename = 'dbo.sba_selfcancels', varTypes = c(fiscal_week_start_date = "date", fiscal_week_end_date = "date"), rownames = F, append = T)
     sqlSave(connDEV, as.data.frame(df_orders), tablename = 'dbo.sba_ordercounts', varTypes = c(fiscal_week_start_date = "date", fiscal_week_end_date = "date"), rownames = F, append = T)
     odbcClose(connDEV)
     odbcCloseAll()
     
     #send email
     cat("\n preparing email...")
     
     from1 <- "michael.abla@Staples.com"
     to1 <- c("Shelby.Hubach@Staples.com", "Cheryl.Mervich@Staples.com")
     carbon1 <- "michael.abla@Staples.com"
     subject1 <- paste0("SBA Self-Cancel Report - ", fw, "")
     body1 <- paste0("SBA Self-Cancel Report for <b><u>", fw,"</u></b> is now available. <br><br>
                     <a href='https://davinci-dev.staples.com/#/site/CSI/workbooks/3758/views'>Visit Dashboard for Further Details</a> ")
     
     send.mail(from = from1,
               to = to1,
               cc = carbon1,
               subject = subject1,
               html = TRUE,
               inline = TRUE,
               body = body1,
               authenticate = TRUE,
               smtp = list(host.name = "smtp.office365.com", port = 587, user.name = "michael.abla@Staples.com", passwd = password, tls = TRUE))
     cat("\n all done!")
   }
}

update_sbaselfcancels <- sba_selfcancel_weekly()
