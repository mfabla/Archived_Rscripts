############# Weekly Script to Update Place Order Forecasts ################

#by Mike Abla

#This is the weekly script to run and update the placeorder_forecast table in dev


# set_env -----------------------------------------------------------------

library(rIA)
set_env(clear_env = T, dir = "~/VM-jobs/R", extra_pkgs = c('mailR'))


# run function ------------------------------------------------------------

placeorders_actuals <- function(){
  
  #previous week
  prev_wk <- calendar %>%
    filter(calendar_date == Sys.Date() - 7) %>%
    select(fiscal_week_of_period_long, fiscal_week_start_date, fiscal_week_end_date)
  
  #gdw - convert to gdw date format
  start_com <- paste(prev_wk$fiscal_week_start_date, "00:00:00", sep = " ")
  end_com <- paste(prev_wk$fiscal_week_end_date, "23:59:59", sep = " ")
  
  start_sba <- as.numeric(gsub("-", "", as.character(prev_wk$fiscal_week_start_date)))
  end_sba <- as.numeric(gsub("-", "", as.character(prev_wk$fiscal_week_end_date)))
  
  #fiscal wk for report
  fw <- prev_wk$fiscal_week_of_period_long
  
 
  #### check if update is needed ####
  cat("\n running report checker...")
  connDev <- odbcConnect('cs-db-dev')
  last_update <- sqlQuery(connDev, "Select distinct [fiscal_week] FROM [CSAQuery].[dbo].[placeorders_forecast]Where OrdersPlaced is NULL", stringsAsFactors = F)
  odbcClose(connDev)
  if(any(!fw %in% last_update$fiscal_week)){
    return(cat("\n report is already up-to-date"))} else {
      
    #### extract order data ####
      
      #com orders
      cat("\n querying com order data...")
      
      connGDW <- ia_odbc("gdw")
      com_orders <- sqlQuery(connGDW,paste0("Select (t1.OrdersPlaced+t2.OrdersPlaced) as OrdersPlaced,
                                                    t1.OrdersDate, 'Staples.com' as BusinessUnit
                                               from 
                                               (SELECT count(ORDER_NO) as OrdersPlaced, 
                                               cast (ORDER_DATE as date format 'YYYY-MM-DD') as OrdersDate
                                               FROM PRD_USD_OPV.SOMS_YFS_ORDER_HEADER_V
                                               WHERE (ORDER_DATE BETWEEN '",start_com,"' and '", end_com,"' 
                                               AND ENTERPRISE_KEY='SBD_US' AND DOCUMENT_TYPE='0001')
                                               group by 2)as t2
                                               left join
                                               (SELECT count(OHORD#) as OrdersPlaced,
                                               cast(trim(OHCRDT) as date format 'YYYYMMDD')  as OrdersDate
                                               FROM PRD_USD_OPV.SUNRISE_OEBASLIB_OEORDH_V  WHERE ((OHLINK=0 AND OHCRDT BETWEEN '",start_sba,"' and '", end_sba,"' and OHCONO=1))
                                               group by 2) as T1
                                               on t1.OrdersDate = t2.OrdersDate
                                               group by 1,2,3"))
      com_orders <- com_orders %>% group_by(BusinessUnit) %>% summarise(OrdersPlaced = sum(OrdersPlaced, na.rm=T))
      
      #sba orders
      cat("\n querying sba order data...")
      
      sba_orders <- sqlQuery(connGDW, paste0("SELECT 
                                                'SBA' as BusinessUnit,
                                                 count(distinct(OHORD#)) as OrdersPlaced
                                              FROM PRD_USD_OPV.SUNRISE_SFBASLIB_SFORDH_V
                                              WHERE OHLINK = 0
                                                 AND OHTYPE = 'OR'
                                                 AND OHCRDT between '",start_sba,"' and '",end_sba,"'
                                             Group By BusinessUnit"))
      odbcClose(connGDW)
      
      #### update dev table ####
      cat("\n updating table...")
      connDev <- odbcConnect('cs-db-dev')
      
      sqlQuery(connDev, paste0("UPDATE [CSAQuery].[dbo].[placeorders_forecast]
                                SET OrdersPlaced = ", com_orders$OrdersPlaced,"
                               WHERE businessunit = 'Staples.com' AND fiscal_week = '",fw ,"'" ))
      
      sqlQuery(connDev, paste0("UPDATE [CSAQuery].[dbo].[placeorders_forecast]
                                SET OrdersPlaced = ", sba_orders$OrdersPlaced,"
                               WHERE businessunit = 'SBA' AND fiscal_week = '",fw ,"'" ))
      
      odbcClose(connDev)
      
      ##### create charts #####
      cat("\n creating charts...")
      connDev <- odbcConnect('cs-db-dev')
      df_final <- sqlQuery(connDev, "SELECT * FROM [CSAQuery].[dbo].[placeorders_forecast] WHERE OrdersPlaced_proj is not null", stringsAsFactors = F)
      odbcClose(connDev)
      
      df_final1 <- df_final %>% mutate(week_start = as.Date(week_start, format = '%Y-%m-%d'))
      
      lineplot_com <- ggplot(df_final1[df_final$businessunit == 'Staples.com',], aes(x = week_start)) +
        theme_bw() +
        geom_line(aes(y = OrdersPlaced, colour = "Actuals"), size = 1.05) +
        geom_line(aes(y = OrdersPlaced_proj, colour = "Projected"), size = 1.05, linetype = "longdash") +
        scale_y_continuous(name = "Placed Orders", labels = comma) + 
        scale_colour_manual(values = c("steelblue3","darkorange")) +
        scale_x_date(labels = date_format("%m/%d/%y"), breaks = date_breaks("6 weeks"), name = "Week") +
        ggtitle("Staples.com Placed Orders: Weekly Actuals vs Projections") +
        theme(legend.position="bottom", legend.direction ="horizontal",legend.title = element_blank()) +
        ggsave(width = 8, height = 4.5, units = "in",filename = "C:/Users/AblMi001/Documents/VM-jobs/R/PlaceOrder_Charts/COM_Weekly_POs.png")
      
      lineplot_sba <- ggplot(df_final1[df_final$businessunit == 'SBA',], aes(x = week_start)) +
        theme_bw() +
        geom_line(aes(y = OrdersPlaced, colour = "Actuals"), size = 1.05) +
        geom_line(aes(y = OrdersPlaced_proj, colour = "Projected"), size = 1.05, linetype = "longdash") +
        scale_y_continuous(name = "Placed Orders", labels = comma) + 
        scale_colour_manual(values = c("steelblue3","darkorange")) +
        scale_x_date(labels = date_format("%m/%d/%y"), breaks = date_breaks("16 weeks"), name = "Week") +
        ggtitle("SBA Placed Orders: Weekly Actuals vs Projections") +
        theme(legend.position="bottom", legend.direction ="horizontal",legend.title = element_blank()) +
        ggsave( width = 8, height = 4.5, units = "in",filename = "C:/Users/AblMi001/Documents/VM-jobs/R/PlaceOrder_Charts/SBA_Weekly_POs.png")
      
      ###### send email with attachment #####
      cat("\n preparing email...")
      
      from1 <- "michael.abla@Staples.com"
      to1 <- c("michael.abla@Staples.com")
      carbon1 <- c("michael.abla@Staples.com", "Richard.Ostberg@Staples.com", "Domingo.Aguado@Staples.com", "Cheryl.Mervich@Staples.com")
      subject1 <- paste0("Place Orders Report - ", fw,"")
      body1 <- paste0("Below is the latest update for Place Orders actuals vs projections thru end of FY17.<br><br>
                   <b>Place Orders Weekly Update for <u>", fw,"</u> : </b> <br><br>
                   <u>SBA</u><br>
                   SBA Placed Orders Actuals: ", sba_orders$OrdersPlaced," <br>
                   SBA Placed Orders Projected: ", df_final1[df_final1$businessunit == 'SBA' & df_final1$fiscal_week == fw, c("OrdersPlaced_proj")], "<br>
                   Forecast Difference: ", df_final1[df_final1$businessunit == 'SBA' & df_final1$fiscal_week == fw, c(8)] - sba_orders$OrdersPlaced, "<br>
                   Forecast Difference Percent: ", ((df_final1[df_final1$businessunit == 'SBA' & df_final1$fiscal_week == fw, c(8)] - sba_orders$OrdersPlaced)/sba_orders$OrdersPlaced)*100,"<br><br>
                   <img src= \"PlaceOrder_Charts/SBA_Weekly_POs.png\", width = '800' height = '450'> <br>
                   <u>Staples.com</u><br>
                   COM Placed Orders Actuals: ", com_orders$OrdersPlaced," <br>
                   COM Placed Orders Projected: ", df_final1[df_final1$businessunit == 'Staples.com' & df_final1$fiscal_week == fw, c("OrdersPlaced_proj")], "<br>
                   Forecast Difference: ", df_final1[df_final1$businessunit == 'Staples.com' & df_final1$fiscal_week == fw, c(8)] - com_orders$OrdersPlaced,"<br><br>
                   Forecast Difference Percent: ", ((df_final1[df_final1$businessunit == 'Staples.com' & df_final1$fiscal_week == fw, c(8)] - com_orders$OrdersPlaced)/com_orders$OrdersPlaced)*100,"<br><br>
                   <img src= \"PlaceOrder_Charts/COM_Weekly_POs.png\", width = '800' height = '450'><br>") 
      
      #fileattachment <- c("PlaceOrder_Charts/SBA_Weekly_POs.png","PlaceOrder_Charts/COM_Weekly_POs.png")
      
      send.mail(from = from1,
               to = to1,
               cc = carbon1,
               subject = subject1,
               html = TRUE,
               inline = TRUE,
               body = body1,
               #attach.files = fileattachment,
               authenticate = TRUE,
               smtp = list(host.name = "smtp.office365.com", port = 587, user.name = "michael.abla@Staples.com", passwd = "xxxxxxx", tls = TRUE))
      cat("\n all done!")

    }
  }
 
update_actuals <-  placeorders_actuals()

odbcCloseAll()




