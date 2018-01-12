############# COM Cancels Time Series Model ##############

#by Mike Abla
#10NOV2016

#NOTES:

#Purpose:
#1) Build a forecast to estimate .COM cancellation contacts
#2) Build a glide path for the BU to reach a 50% reduction in cancellation contacts
#as a result of the self-serve cancellation initiative

#Methodoloty:
#Analysis consist of three components: 
#1) Build a forecast of Orders Placed using a YoY Moving Average (2)
#2) Build a cancellation contacts regression model with Placed Orders being predictor
#3) Used the cancellation contact forecast to create a glide path to reach a 50% contact by mid-year (TBD exact date)


# Load rIA package & setwd -----------------------------------------

  set_env(clear_env = T, dir = "~/Projects/Cancellation-Model/Time Models" )

  library(rIA)
  library(xlsx)
  
  #below packages only need if doing ARIMA
  #library(tseries)
  #library(forecast)
  #library(fUnitRoots)

  
  
  connGDW <- ia_odbc('gdw')
  connSQL <- odbcConnect('cs-db-uat')

# Time Range -------------------------------------------------------

start_date <- 20161121
end_date <- 20170205

# COM Data Extraction --------------------------------------------

  #Calendar
  calendar_filter <- calendar_simple %>%
    filter(calendar_date >= '2014-02-02' & calendar_date <='2017-09-30') %>%
    group_by(fiscal_period, fiscal_week) %>%
    summarise(calendar_date=min(calendar_date)) %>%
    distinct(fiscal_week,.keep_all = TRUE) %>%
    arrange(fiscal_period, fiscal_week)

  calendar_filter$recID <- seq(1:nrow(calendar_filter))


  #COM: Orders
  
  #df_orders_com <- read.csv("Com Orders Placed Data.csv", stringsAsFactors = F)
  df_orders_com <- sqlQuery(connGDW,paste0("Select (t1.OrdersPlaced+t2.OrdersPlaced) as OrdersPlaced,
                                                    t1.OrdersDate, 'Staples.com' as BusinessUnit
                                            from 
                                                  (SELECT count(ORDER_NO) as OrdersPlaced, 
                                                  cast (ORDER_DATE as date format 'YYYY-MM-DD') as OrdersDate
                                                  FROM PRD_USD_OPV.SOMS_YFS_ORDER_HEADER_V
                                                  WHERE (ORDER_DATE> '2014-02-01 00:00:00.000' 
                                                  AND ENTERPRISE_KEY='SBD_US' AND DOCUMENT_TYPE='0001')
                                                  group by 2)as t2
                                            left join
                                                  (SELECT count(OHORD#) as OrdersPlaced,
                                                  cast(trim(OHCRDT) as date format 'YYYYMMDD')  as OrdersDate
                                                  FROM PRD_USD_OPV.SUNRISE_OEBASLIB_OEORDH_V  WHERE ((OHLINK=0 AND OHCRDT>=20140202 and OHCONO=1))
                                                  group by 2) as T1
                                            on t1.OrdersDate = t2.OrdersDate
                                            group by 1,2,3"))
  colnames(df_orders_com) <- c('Com.OrdersPlaced','OrdersDate','BusinessUnit')
  df_orders_com <- left_join(df_orders_com,calendar_simple,by=c("OrdersDate"="calendar_date"))
  df_orders_com1 <- df_orders_com %>%
                    group_by(fiscal_period,fiscal_week) %>%
                    summarise(Com.OrdersPlaced = sum(Com.OrdersPlaced)) %>%
                    arrange(fiscal_period,fiscal_week)
  
    #plot.ts(df_orders_com1$Com.OrdersPlaced) #data concerns with order levels shown in early 2014
    #plot.ts(diff(df_orders_com1$Com.OrdersPlaced,52)) #2nd half appears stationary

  #COM: Cancel Contacts - Data pulled from CSI Volume Dashboard
  
  df_csi_com <- adj_csi_contacts()
  
  df_csi_com <- read.xlsx('COM Contact Volume Data_Cancels.xlsx',sheetName = "Summary")
    #plot.ts(df_csi_com$Adjusted.Contact.Volume)
    #plot.ts(diff(df_csi_com$Adjusted.Contact.Volume)) #stationary with lag1
  
  

# SBA Data Extraction --------------------------------------------
     
  #SBA: Eligible Customers and their orders (NOTE!!! Update the date fields below by weekly intervals)
  

  
  sba_custs <- read.csv("SBA Eligible Customers.csv", header = TRUE)
  cust_numbers <- sba_custs$CUSTOMERNUMBER
 
  
  cust_num_lists <- split(cust_numbers, ceiling(seq_along(cust_numbers)/1000))
  cust_num_lists <- lapply(cust_num_lists, function(x) paste("'", x, "'", collapse=',', sep=''))
  
  
  queries <- lapply(cust_num_lists, function(x)  
    paste0("SELECT DISTINCT OHCUST,
           OHORD#,
           OHCRDT
           FROM PRD_USD_OPV.SUNRISE_SFBASLIB_SFORDH_V
           WHERE OHLINK = 0
           AND OHTYPE = 'OR'
           AND OHCRDT between '",start_date,"' and '",end_date,"'
           AND OHCUST IN (", x,")
           "))
  
  queries_batch1 <- unlist(queries, use.names = F)[1:48]
  
  run_batches <- function(batch){
    
    out <- NULL; j <- 0
    for(i in batch){
      j <- j + 1
      out[[j]] <- data.frame(sqlQuery(connGDW, i, stringsAsFactors = F))
    }
    
    out <- dplyr::bind_rows(out)
    out  
  }
  
  batch1 <- run_batches(batch = queries_batch1)
  df_orders_sba <- batch1
  df_orders_sba$Ohcrdt <- as.Date(as.character(df_orders_sba$Ohcrdt), "%Y%m%d")
  
  df_orders_sba1 <- df_orders_sba %>%
                    group_by(Ohcrdt) %>%
                    summarise(SBA_Orders = n())
  
  df_orders_sba1 <- left_join(df_orders_sba1,calendar,by=c("Ohcrdt"="calendar_date")) %>%
                    select(fiscal_week_of_period_long,fiscal_week_start_date,fiscal_period_name,SBA_Orders) %>%
                    group_by(fiscal_week_of_period_long,fiscal_week_start_date,fiscal_period_name) %>%
                    summarise(SBA.OrdersPlaced = sum(SBA_Orders)) %>%
                    arrange(fiscal_week_start_date)
  df_orders_sba1 <- as.data.frame(df_orders_sba1)

          #Bring in previous data run on SBA orders and merge
          df_orders_sba_old <- read.csv("SBA_Eligible_DailyOrders.csv", header=TRUE, stringsAsFactors = F)
          df_orders_sba_old <- df_orders_sba_old[,-c(1)]
          df_orders_sba_old$fiscal_week_start_date <- as.Date(df_orders_sba_old$fiscal_week_start_date,"%m/%d/%Y")
          df_orders_sba1 <- anti_join(df_orders_sba1, df_orders_sba_old, by= c("fiscal_week_start_date"))
          df_orders_sba1 <- rbind(df_orders_sba_old,df_orders_sba1) %>%
                            distinct(.keep_all=TRUE) %>%
                            arrange(fiscal_week_start_date)
          #df_orders_sba1 <- df_orders_sba_old
          
          write.csv(df_orders_sba_old,"SBA_Eligible_DailyOrders.csv")
  
  #SBA: Eligible Customer Contacts
  
  df_csi_sba <- sqlQuery(connSQL,paste0("select 
                                              count(b.BusinessUnitName) as ContactVolume /*, 
                                                  a.Option_ContactMethod as ContactMethod, 
                                                  a.Option_ContactedBy as ContactedBy*/, 
                                                  CONVERT(VARCHAR(10),a.RowLoadDate,111) as ContactDate /*, 
                                                  a.CustomerNumber_AccountNumber, 
                                                  a.RewardsNumber*/, 
                                                  a.OrderNumber /*,
                                                  c.Topic, 
                                                  c.Domain, 
                                                  c.Type, 
                                                  c.Area, 
                                                  d.Tier1Text, 
                                                  c.Tier2Text, 
                                                  g.Note, 
                                                  a.ItemNumber, 
                                                  e.ItemProductName, 
                                                  a.Option_SelfService_AdditionalComments, 
                                                  h.TransferredContactName, 
                                                  lower(f.LANID) as lanid, 
                                                  i.EnforceOrderNumber*/
                                          from CSIReporting.dbo.ContactRecords a
                                              left join CSIReporting.dbo.BusinessUnits b on a.BusinessUnit_ID=b.BusinessUnit_ID
                                              left join CSIReporting.dbo.LU_Tier2 c on a.Tier2_ID=c.Tier2_ID
                                              left join CSIReporting.dbo.LU_Tier1 d on c.Tier1_ID=d.Tier1_ID
                                              left join CSIReporting.dbo.LU_Item_ProductCategory e on a.ItemProductCategory_ID=e.ItemProductCategory_ID
                                              left join CSIReporting.dbo.Users f on a.UserLANID=f.SecurityHashedID
                                              left join CSIReporting.dbo.UserNotes g on a.RecordID=g.RecordID
                                              left join CSIReporting.dbo.LU_TransferredContact h on a.TransferredContact_ID=h.TransferredContact_ID
                                              and a.BusinessUnit_ID=h.BusinessUnit_ID
                                              left join CSIReporting.dbo.ActiveDropdownItems i on c.Tier2_ID=i.Tier2_ID
                                              and b.BusinessUnit_ID=i.BusinessUnit_ID
                                          where a.RowLoadDate >= '2016-01-31'
                                              and b.BusinessUnitName = 'SA US'
                                              and d.Tier1Text = 'Cancelled / Stop Delivery Request'
                                          group by a.RowLoadDate, a.OrderNumber"))
  
  df_csi_sba$ContactDate <- as.Date(df_csi_sba$ContactDate, "%Y/%m/%d")
  
  df_csi_sba1 <- inner_join(df_csi_sba,df_orders_sba, by=c("OrderNumber"="Ohord.")) 
  
          #save new outputs to historical data
          df_csi_sba_old <- read.csv("SBA_Eligible_Contacts.csv", header=TRUE)
          df_csi_sba_old <- df_csi_sba_old[,-c(1)]
          df_csi_sba_old$ContactDate <- as.Date(df_csi_sba_old$ContactDate,"%Y-%m-%d")
          df_csi_sba_old$Ohcrdt <- as.Date(df_csi_sba_old$Ohcrdt,"%Y-%m-%d")
          df_csi_sba_add <- anti_join(df_csi_sba1, df_csi_sba_old, by=c("OrderNumber","ContactDate"))
          df_csi_sba_new <- rbind(df_csi_sba_old,df_csi_sba_add)
          
          write.csv(df_csi_sba_new,"SBA_Eligible_Contacts.csv")
  
  df_csi_sba1 <- df_csi_sba1%>%
                 left_join(calendar,by=c("ContactDate"="calendar_date"))         
          
  df_csi_sba1 <- df_csi_sba_new%>%
                 left_join(calendar,by=c("ContactDate"="calendar_date")) 
  
  df_csi_sba1 <- df_csi_sba1%>%
                 select(fiscal_week_of_period_long,fiscal_week_start_date,fiscal_period_name,ContactVolume) %>%
                 group_by(fiscal_week_of_period_long,fiscal_week_start_date,fiscal_period_name) %>%
                 summarise(ContactVolume=sum(ContactVolume))
  
# COM Model Build ----------------------------------------------------
    
    #Com Order Forecast
    fc_orders_com <- df_orders_com1
      fc_orders_com$recID <- seq(1:nrow(fc_orders_com))
      
    fc_orders_com1 <- left_join(calendar_filter,fc_orders_com, by=c('recID')) 
    fc_orders_com1 <- as.data.frame(fc_orders_com1) %>%
                      select(recID,fiscal_period=fiscal_period.x, fiscal_week=fiscal_week.x, calendar_date, Com.OrdersPlaced) %>%
                      arrange(recID) 
    fc_orders_com1$Com.OrdersPlaced_lag <- lag(fc_orders_com1$Com.OrdersPlaced,52) 
    fc_orders_com1$Com.OrdersPlaced_lag2 <- lag(fc_orders_com1$Com.OrdersPlaced,104) 
    fc_orders_com1$Com.OrdersPlaced_proj <- rowMeans(fc_orders_com1[,c(5,6)],na.rm=TRUE) 
    
   
    
    #Incorporate Cancel Contacts
    fc_csi_com <- df_csi_com %>%
                  select(Time.DropDown,Period,Adjusted.Contact.Volume,Week) %>%
                  mutate(calendar_date=as.Date(Week, format= "%m/%d/%Y"))
      
    fc_csi_com1 <-left_join(fc_orders_com1,fc_csi_com, by=c('calendar_date')) %>%
                  select(-Time.DropDown,-Period, -Week)
    
    #model data
    model.data <- fc_csi_com1 %>%
                  select(recID, fiscal_week,Com.OrdersPlaced,Com.OrdersPlaced_proj,Adjusted.Contact.Volume) %>%
                  filter(!is.na(Adjusted.Contact.Volume))
    
    model.cancel_prj <- lm(Adjusted.Contact.Volume~Com.OrdersPlaced_proj, model.data)
    
    summary(model.cancel_prj)
    plot(model.cancel_prj)
    
    model.data$cancel_fc <- predict(model.cancel_prj,model.data)
    sqrt(mean((model.data$cancel_fc - model.data$Adjusted.Contact.Volume)^2,na.rm=TRUE)) #MSE 365
    plot(model.data$cancel_fc,model.data$Adjusted.Contact.Volume, main="Actual vs Forecasted Staples.com Cancel Contacts", ylab="Actuals", xlab="Projected")
    abline(0,1)
    
# COM Forecast & Glide Path --------------------------------------
    
    #Forecast
    model.output <-predict.lm(model.cancel_prj,fc_csi_com1, interval = "prediction")
    com_cancels_forecast <- cbind(fc_csi_com1,model.output)
    
    #Glide Path
    glide_time <- subset(com_cancels_forecast, calendar_date >= '2017-04-01')
    glide_time$glide_rate <- seq(from=.98,to=.30,length.out = nrow(glide_time))
    glide_time1 <- glide_time %>%
                  mutate(fit_glide = glide_rate*fit, lwr_glide = glide_rate*lwr, upr_glide=glide_rate*upr) %>%
                  select(recID, fit_glide, lwr_glide, upr_glide)
    
    #merge
    com_cancels_forecast1 <- left_join(com_cancels_forecast, glide_time1, by = c('recID'))

# SBA Model Build ----------------------------------------------------
    
    #SBA Order Forecast
    fc_orders_sba <- df_orders_sba1
    fc_orders_sba$recID <- seq(1:nrow(fc_orders_sba))
    
    fc_orders_sba1 <- left_join(calendar_filter,fc_orders_sba, by=c('recID')) 
    fc_orders_sba1 <- as.data.frame(fc_orders_sba1) %>%
                      select(recID,fiscal_period, fiscal_week, calendar_date, SBA.OrdersPlaced) %>%
                      arrange(recID) 
    fc_orders_sba1$SBA.OrdersPlaced_lag <- lag(fc_orders_sba1$SBA.OrdersPlaced,52) 
    fc_orders_sba1$SBA.OrdersPlaced_lag2 <- lag(fc_orders_sba1$SBA.OrdersPlaced,104) 
    fc_orders_sba1$SBA.OrdersPlaced_proj <- rowMeans(fc_orders_sba1[,c(6,7)],na.rm=TRUE) 
    
   
    #ts.plot(cbind(fc_orders_sba1$SBA.OrdersPlaced,fc_orders_sba1$SBA.OrdersPlaced_proj), gpars = list(col = c("red", "blue")))
    
    #Incorporate Cancel Contacts
    fc_csi_sba <- as.data.frame(df_csi_sba1) 
    
    fc_csi_sba1 <-left_join(fc_orders_sba1,fc_csi_sba, by=c("calendar_date"= "fiscal_week_start_date")) %>%
      select(-fiscal_week_of_period_long,- fiscal_period_name)
    
    #model data
    model.data2 <- fc_csi_sba1 %>%
      select(recID, fiscal_week,SBA.OrdersPlaced,SBA.OrdersPlaced_proj,ContactVolume) %>%
      filter(!is.na(ContactVolume) & recID > 105)
    
    #plot(model.data2$SBA.OrdersPlaced,model.data2$ContactVolume)
    #cor(model.data2$SBA.OrdersPlaced,model.data2$ContactVolume) #0.50
    #cor(model.data2$SBA.OrdersPlaced_proj,model.data2$ContactVolume) #0.27
    #ts.plot(cbind(model.data2$SBA.OrdersPlaced_proj,model.data2$SBA.OrdersPlaced), gpars = list(col = c("red", "blue")))
    
    
    model.cancel_prj2 <- lm(ContactVolume~SBA.OrdersPlaced_proj, model.data2)
    
    summary(model.cancel_prj2)
    plot(model.cancel_prj2)
    
    
    model.data2$cancel_fc <- predict(model.cancel_prj2,model.data2)
    sqrt(mean((model.data2$cancel_fc - model.data2$ContactVolume)^2,na.rm=TRUE)) #MSE 150
    plot(model.data2$cancel_fc,model.data2$ContactVolume, main="Actual vs Forecasted Eligible SBA Cancel Contacts", ylab="Actuals", xlab="Projected")
    abline(0,1)
    
# SBA Forecast & Glide Path --------------------------------------
    
    #Forecast
    model.output2 <-predict.lm(model.cancel_prj2,fc_csi_sba1, interval = "prediction")
    sba_cancels_forecast <- cbind(fc_csi_sba1,model.output2)
    
    #Glide Path
    glide_time2 <- subset(sba_cancels_forecast, calendar_date >= '2017-04-01')
    glide_time2$glide_rate <- seq(from=.98,to=.30,length.out = nrow(glide_time2))
    glide_time2 <- glide_time2 %>%
      mutate(fit_glide = glide_rate*fit, lwr_glide = glide_rate*lwr, upr_glide=glide_rate*upr) %>%
      select(recID, fit_glide, lwr_glide, upr_glide)
    
    #merge
    sba_cancels_forecast1 <- left_join(sba_cancels_forecast, glide_time2, by = c('recID'))
    
    odbcClose(connGDW) 
    odbcClose(connSQL) 
       
# Output Data -----------------------------------------------
    

    write.csv(sba_cancels_forecast1,"SBA_Cancel_Forecast.csv")
    write.csv(com_cancels_forecast1,"Cancellation Contact Volume.twb Files/Data/Time Models/COM_Cancel_Forecast.csv")
   
    