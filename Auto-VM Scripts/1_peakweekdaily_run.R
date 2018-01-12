########## Peak Week 2018: Script to get Actuals for Peak Week Reporting ############

# 12 DEC 2017
# by Mike Abla

# NOTES:

# This script is to update the daily and hourly csi data to feed into Peak Week 2018 reporting/dashboard


# set env -----------------------------------------------------------------

library(rIA)
set_env(dir = "~/VM-jobs/R")

connDEV <- odbcConnect('cs-db-dev')
date_start <- as.character(sqlQuery(connDEV, "SELECT max(FCR_DATE) FROM [CSAQuery].[dbo].[peak_week_daily]", stringsAsFactors = F))
odbcClose(connDEV)

date_end <- as.character(Sys.Date())



# extract daily data ------------------------------------------------------

connPROD <- odbcConnect('cs-db-prod')
daily <- sqlQuery(connPROD, paste0("SELECT
                                         t1.FCR_ID
                                        ,t2.DayOfWeekNameAbbr as DOW
                                        ,t1.FCR_DATE
                                        ,t1.FCR_TIME
                                        ,DATEPART( hour, t1.FCR_TIME) as HR
                                        ,t1.Topic
                                   FROM [NADCS_DW].[dbo].[smryCSIReporting] t1
                                   inner join [NADCS_DW].[dbo].[Dim_Date] t2 on t1.Day_ID = t2.Date_ID
                                   WHERE t1.CSI_BU_ID = 2 AND FCR_DATE between '",date_start,"' and '", date_end ,"'"), stringsAsFactors = F)
odbcClose(connPROD)


# clean data --------------------------------------------------------------

#rollup daily data
daily_df <- daily %>%
  mutate(Topic = ifelse(is.na(Topic), 'Transfer', Topic),
         Topic = str_replace_all(Topic, c("/"=".", " " = ".", "\\." = "")),
         Peak_Week = ifelse(FCR_DATE >= '2017-12-31' & FCR_DATE <= '2018-01-06', 'PW Wk1', 
                            ifelse(FCR_DATE >= '2018-01-07' & FCR_DATE <= '2018-01-13', 'PW Wk2', 'No')),
         total_contacts = 1) %>%
  dcast(FCR_DATE + Peak_Week + DOW + HR ~ Topic, value.var = "total_contacts", fill = 0, sum) %>%
  mutate(Total_Contacts = rowSums(select(., Account:WiMS))) %>%
  arrange(FCR_DATE, HR) %>%
  head(., -1) 


#pull column names to ensure nothing is dropped
connDEV <- odbcConnect('cs-db-dev')
columns <- sqlQuery(connDEV, "SELECT * FROM [CSAQuery].[dbo].[peak_week_daily]", stringsAsFactors = F)
odbcClose(connDEV)
nms <- names(columns)

Missing <- setdiff(nms, names(daily_df))
daily_df[Missing] <- 0
daily_df <- daily_df[nms]


# identify new records to update ------------------------------------------

row_updates <- anti_join(daily_df, columns, by = c("FCR_DATE", "Peak_Week", "DOW", "HR")) %>%
  arrange(FCR_DATE, HR)


# update dev table --------------------------------------------------------

connDEV <- odbcConnect('cs-db-dev')
sqlSave(connDEV, as.data.frame(row_updates), tablename = 'dbo.peak_week_daily', rownames = F, varTypes = c(FCR_DATE = 'date'), append = T)
odbcClose(connDEV)


# log record --------------------------------------------------------------

dir1 <- '~/VM-jobs/logs/peakweek-daily/'
log_name <- paste0(dir1, 'peakweek-', format(Sys.time(), '%Y-%m-%d_%H%M%S'), '.txt')

sink(log_name)

paste0("Total number of rows added: ", as.character(nrow(row_updates)), "")

sink()
