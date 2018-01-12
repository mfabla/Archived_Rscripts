############# Set up projected placed orders  #################################

#Note: this script is built on the extrated data pulled from '0_extract placed order contact history.R'


# set_env -----------------------------------------------------------------

library(rIA)
set_env(clear_env = T, dir = "H:/Projects/MikeAbla/Contact Forecast", extra_pkgs = c('xlsx'))

# projected timeframes ----------------------------------------------------

proj_start <- as.Date('2017-07-30')
proj_end <- as.Date('2018-02-03')

proj_weeks <- calendar %>% select(fiscal_week_start_date, fiscal_week_of_period_long) %>% 
  filter_(paste0("fiscal_week_start_date >= '",proj_start,"'")) %>% 
  filter_(paste0("fiscal_week_start_date <= '",proj_end,"'")) %>%
  distinct() %>% 
  rename(week_start = fiscal_week_start_date, fiscal_week = fiscal_week_of_period_long)

# import data and split by bu ---------------------------------------------

df <- read.xlsx("SBA_COM_Historicals.xlsx", sheetIndex = 1, stringsAsFactors = F) %>% select(-recID)
df_weeks <- select(.data = df, week_start, fiscal_week) %>%
  rbind(proj_weeks) %>%
  left_join(df, by = c("week_start", "fiscal_week")) %>% distinct()

df_sba <- df_weeks %>% filter(businessunit == 'SBA' | is.na(businessunit)) %>% mutate(businessunit = ifelse(is.na(businessunit), 'SBA', 'SBA'))
df_com <- df_weeks %>% filter(businessunit == 'Staples.com' | is.na(businessunit)) %>% mutate(businessunit = ifelse(is.na(businessunit), 'Staples.com', 'Staples.com'))


# sba data prep -----------------------------------------------------------

df_sba$OrdersPlaced_lag1 <- lag(df_sba$OrdersPlaced, 52)
df_sba$OrdersPlaced_lag2 <- lag(df_sba$OrdersPlaced, 104)
df_sba$OrdersPlaced_proj <- round(rowMeans(df_sba[,c("OrdersPlaced_lag1", "OrdersPlaced_lag2")], na.rm = F) * 1.05,0) #increase projections by 5%

sba.test <- df_sba %>% filter(!is.na(OrdersPlaced_proj)) %>% filter(!is.na(OrdersPlaced)) %>% summarise(OrdersPlaced = mean(OrdersPlaced), OrdersPlaced_proj = mean(OrdersPlaced_proj),rmse = sqrt(mean((OrdersPlaced-OrdersPlaced_proj)^2, na.rm=T))) %>% print(.) 
plot(df_sba$OrdersPlaced_proj, df_sba$OrdersPlaced, xlim = c(250000,700000), ylim = c(250000,700000))
abline(0,1)

# com data prep -----------------------------------------------------------

df_com$OrdersPlaced_lag1 <- lag(df_com$OrdersPlaced, 52)
df_com$OrdersPlaced_lag2 <- lag(df_com$OrdersPlaced, 104)
df_com$OrdersPlaced_proj <- round(rowMeans(df_com[,c("OrdersPlaced_lag1", "OrdersPlaced_lag2")], na.rm = F) * .985,0) #drecrease projections by -1.5%

com.test <- df_com %>% filter(!is.na(OrdersPlaced_proj)) %>% filter(!is.na(OrdersPlaced)) %>% summarise(OrdersPlaced = mean(OrdersPlaced), OrdersPlaced_proj = mean(OrdersPlaced_proj),rmse = sqrt(mean((OrdersPlaced-OrdersPlaced_proj)^2, na.rm=T))) %>% print(.) 
plot(df_com$OrdersPlaced_proj, df_com$OrdersPlaced, xlim = c(150000,600000), ylim = c(15000,600000))
abline(0,1)


# join and upload ---------------------------------------------------------

df_final <- rbind(df_sba, df_com) %>% arrange(week_start) %>% mutate(recID = seq(1:nrow(.)), week_start=as.character(week_start)) %>% select(recID, everything()) %>%
  #mutate(forecast_diff = OrdersPlaced_proj-OrdersPlaced) %>%
  select(-adj_contacts)

connDev <- odbcConnect('cs-db-dev')
sqlSave(connDev, df_final, tablename = 'dbo.placeorders_forecast', rownames = F)
odbcCloseAll()


# chart -------------------------------------------------------------------

df_final1 <- df_final %>% mutate(week_start = as.Date(week_start, format = '%Y-%m-%d')) %>% filter(!is.na(forecast_diff))

lineplot_com <- ggplot(df_final1[df_final$businessunit == 'Staples.com',], aes(x = week_start)) +
  geom_line(aes(y = OrdersPlaced, colour = "Actuals")) +
  geom_line(aes(y = OrdersPlaced_proj, colour = "Projected")) 

lineplot_sba <- ggplot(df_final1[df_final$businessunit == 'SBA',], aes(x = week_start)) +
  geom_line(aes(y = OrdersPlaced, colour = "Actuals")) +
  geom_line(aes(y = OrdersPlaced_proj, colour = "Projected")) 
