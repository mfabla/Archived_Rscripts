###### Initial Step to Upload Placed Order History for SBA & .COM ############

#Notes: For .COM Orders, GDW only maintains a 2 yr history, which is why the need to upload older
#order counts to perform the contact forecasting



# set_env -----------------------------------------------------------------

library(rIA)
set_env(clear_env = T, dir = "H:/Projects/MikeAbla/Contact Forecast", extra_pkgs = c('xlsx'))


# sba data --------------------------------------------------------------

#import data history
df_sba <- read.xlsx("SBA Placed Orders_FY14P01W1-FY17P07W1.xlsx", sheetIndex = 1, stringsAsFactors = F)
df_sba <- left_join(df_sba, select(.data = calendar, calendar_date, fiscal_week_of_period_long), by = c("week_start"="calendar_date")) %>%
  rename(fiscal_week = fiscal_week_of_period_long, OrdersPlaced = Sum.of.OrdersPlaced) %>%
  arrange(week_start) %>%
  mutate(businessunit = 'SBA') %>%
  select(businessunit, week_start, fiscal_week, -fiscal_period, OrdersPlaced) %>%
  filter(week_start != '2017-07-30')

#import contact history
#connUAT <- odbcConnect('cs-db-uat')
#sba_totalcontacts <- sqlQuery(connUAT, "SELECT min(cast(t2.[CalendarDate] as date)) as week_start
                                         #   ,t1.[BusinessUnit]
                                         #   ,t1.[TotalContacts]
                                       # FROM [ECOMMERCE].[dbo].[CIIPOSummary] t1
                                       # JOIN [NADCS_DW].[dbo].[Dim_Day] t2 on (t1.Week_ID = t2.[Week_ID])
                                       # Where t1.BusinessUnit = 'Staples Advantage' 
                                        #    AND (t1.FiscalPeriodName LIKE 'FY16%' OR t1.FiscalPeriodName LIKE 'FY17%') 
                                       # Group By 
                                        #    t1.[BusinessUnit]
                                        #   ,t1.[TotalContacts]
                                       # ORDER BY week_start", stringsAsFactors = F)

 #sqlQuery(connUAT, "SELECT cast(t2.[FiscalWeekStartDate] as date) as week_start
                                          #  ,t1.[CSI_BU_NAME]
                                          #  ,count(distinct(t1.[FCR_ID])) as total_csi
                                    #  FROM [NADCS_DW].[dbo].[smryCSIReporting] t1
                                    #  JOIN [NADCS_DW].[dbo].[Dim_Date] t2 on (t1.Day_ID = t2.[Date_ID])
                                    #  Where t1.[CSI_BU_NAME] = 'SA US' 
                                    #        AND t2.[FiscalWeekStartDate] between '2016-01-31' and '2017-07-23'
                                    #  Group by
                                    #        t2.[FiscalWeekStartDate]
                                    #       ,t1.[CSI_BU_NAME]
                                    #  Order by t2.[FiscalWeekStartDate]", stringsAsFactors = F)

#odbcCloseAll()
sba_contacts <- read.xlsx("SBAAdjContacts_CSIDashboard.xlsx", sheetIndex = 1, stringsAsFactors = F) %>%
  rename(fiscal_week = Select.Time.and.Trend, adj_contacts = Adjusted.Contact.Volume) %>%
  select(-Fiscal.Year, -Business.Unit) %>%
  mutate(fiscal_week = gsub("FY", "FY20", fiscal_week))

#join
df_sba1 <- left_join(df_sba, sba_contacts, by = c("fiscal_week"))

# com data --------------------------------------------------------------

#import data history
df_com <- read.xlsx("Com Placed Orders_FY14P10W2-FY17P07W1.xlsx", sheetIndex = 1, stringsAsFactors = F)
df_com <- left_join(df_com, select(.data = calendar, calendar_date, fiscal_week_of_period_long), by = c("week_start"="calendar_date")) %>%
  rename(fiscal_week = fiscal_week_of_period_long, OrdersPlaced = Sum.of.OrdersPlaced) %>%
  filter(week_start != '2014-11-09') %>%
  arrange(week_start)%>%
  mutate(businessunit = 'Staples.com') %>%
  select(businessunit, week_start, fiscal_week, -fiscal_period, OrdersPlaced) %>%
  filter(week_start != '2017-07-30')

#import contact history
com_contacts <- read.xlsx("COMAdjContacts_CSIDashboard.xlsx", sheetIndex = 1, stringsAsFactors = F) %>%
  rename(fiscal_week = Select.Time.and.Trend, adj_contacts = Adjusted.Contact.Volume) %>%
  select(-Fiscal.Year, -Business.Unit) %>%
  mutate(fiscal_week = gsub("FY", "FY20", fiscal_week))

#join
df_com1 <- left_join(df_com, com_contacts, by = c("fiscal_week"))


# join and export ---------------------------------------------------------

df_final <- rbind(df_sba1, df_com1) %>%
  arrange(fiscal_week) %>%
  mutate(recID = seq(1:nrow(.))) %>%
  select(recID, businessunit, week_start, fiscal_week, adj_contacts, OrdersPlaced)

write.xlsx(df_final, "SBA_COM_Historicals.xlsx", row.names = F)
