### Returns/Cancels Relationship Analysis ####

# 26 SEP 2017
# by Mike Abla

# NOTES: The ask is to determine whether we since an inverse relationship b/w cancel and return contacts
# The hypothesis is that more cancels results in fewer returns

# set env -----------------------------------------------------------------

library(rIA)
set_env(clear_env = T, dir = "H:/Projects/MikeAbla/_Ad-hoc Requests/Cancel-Returns Relationship", extra_pkgs = "caret")

# extract data ------------------------------------------------------------

#sba_csi <- query_csi(bu = "sba", start_date = '2016-01-31', end_date = '2017-08-26', simple = T)
#com_csi <- query_csi(bu = "dotcom", start_date = '2016-01-31', end_date = '2017-08-26', simple = T)
#save(sba_csi,com_csi, file = "SBA-COM CSI Contacts_FY16P1-FY17P8")

#com_cancels <- read.csv("COM_CancelTracking_thruFY17P7.csv", stringsAsFactors = F) %>% select(-Self.Adoption.Rate)
#save(sba_csi,com_csi, com_cancels, file = "SBA-COM CSI Contacts_FY16P1-FY17P8.Rdata")
load("SBA-COM CSI Contacts_FY16P1-FY17P8.Rdata")

connDEV <- odbcConnect('cs-db-dev')
sba_op <- sqlQuery(connDEV, "SELECT [week_start], [fiscal_week], [OrdersPlaced] FROM [CSAQuery].[dbo].[placeorders_forecast]
                   WHERE businessunit = 'SBA' and OrdersPlaced is not null") %>% mutate(week_start = as.Date(week_start))
com_op <- sqlQuery(connDEV, "SELECT [week_start], [fiscal_week], [OrdersPlaced] FROM [CSAQuery].[dbo].[placeorders_forecast]
                   WHERE businessunit = 'Staples.com' and OrdersPlaced is not null") %>% mutate(week_start = as.Date(week_start))
odbcCloseAll()


# clean data --------------------------------------------------------------

sba_op1 <- inner_join(sba_op, select(.data = calendar, calendar_date, fiscal_week_of_period_name), by = c("week_start" = "calendar_date"))
com_op1 <- inner_join(com_op, select(.data = calendar, calendar_date, fiscal_week_of_period_name), by = c("week_start" = "calendar_date"))

sba_csi1 <- sba_csi %>% filter(topic %in% c('Return', 'Cancel')) %>% mutate(total_contact = 1) %>% group_by(fiscal_week) %>% 
  summarize(cancels = sum(total_contact[topic == 'Cancel'], na.rm = T), returns = sum(total_contact[topic == 'Return'], na.rm = T)) %>%
  left_join(select(.data = sba_op1, fiscal_week, fiscal_week_of_period_name, OrdersPlaced), by = c("fiscal_week" = "fiscal_week_of_period_name")) %>%
  mutate(cancels_cpo = (cancels/OrdersPlaced) * 100, returns_cpo = (returns/OrdersPlaced) * 100) 
com_csi1 <- com_csi %>% filter(topic %in% c('Return', 'Cancel')) %>% mutate(total_contact = 1) %>% group_by(fiscal_week) %>% 
  summarize(cancels = sum(total_contact[topic == 'Cancel'], na.rm = T), returns = sum(total_contact[topic == 'Return'], na.rm = T)) %>%
  left_join(select(.data = com_op1, fiscal_week, fiscal_week_of_period_name, OrdersPlaced), by = c("fiscal_week" = "fiscal_week_of_period_name")) %>%
  mutate(cancels_cpo = (cancels/OrdersPlaced) * 100, returns_cpo = (returns/OrdersPlaced) * 100) 

df.com <- inner_join(com_csi1, select(.data = com_cancels, Fiscal.Week, Total.Self.Cancels.bw.0.30.min, Total.Associate.Cancels.bw.0.30.mins), by = c("fiscal_week" = "Fiscal.Week")) %>%
  select(-fiscal_week, -fiscal_week.y)


# assocation --------------------------------------------------------------

cor(df.com)
                                          #cancels    returns OrdersPlaced cancels_cpo   returns_cpo        Total.Self.Cancels.bw.0.30.min Total.Associate.Cancels.bw.0.30.mins
#cancels                               1.0000000000  0.6719024    0.8701031   0.9347474  0.0009631176                     -0.6815374                            0.6243039
#returns                               0.6719024254  1.0000000    0.7227883   0.5657026  0.5945705518                     -0.5961069                            0.5444044
#OrdersPlaced                          0.8701031319  0.7227883    1.0000000   0.6506158 -0.1165303959                     -0.4752034                            0.5584421
#cancels_cpo                           0.9347474434  0.5657026    0.6506158   1.0000000  0.1203839906                     -0.7565768                            0.5940764
#returns_cpo                           0.0009631176  0.5945706   -0.1165304   0.1203840  1.0000000000                     -0.3284053                            0.1269355
#Total.Self.Cancels.bw.0.30.min       -0.6815374250 -0.5961069   -0.4752034  -0.7565768 -0.3284053062                      1.0000000                           -0.5076548
#Total.Associate.Cancels.bw.0.30.mins  0.6243039296  0.5444044    0.5584421   0.5940764  0.1269354864                     -0.5076548                            1.0000000

# notable negative relationship seen b/w self-cancels and returns

plot(df.com$Total.Self.Cancels.bw.0.30.min, df.com$returns, xlab = "Self Cancels per Week", ylab = "CSI Return Contacts per Week",
     xlim = c(0,5000), ylim = c(0,8000), col = "red", main = "Staples.com: Self-Cancels vs CSI Returns")
abline(lm(df.com$returns~df.com$Total.Self.Cancels.bw.0.30.min))

# regression --------------------------------------------------------------

ctrl <- trainControl(method = "cv", number = 10)
model.1 <- train(returns~Total.Self.Cancels.bw.0.30.min, df.com, method = "lm", trControl = ctrl)
summary(model.1)

#Residuals:
#  Min      1Q  Median      3Q     Max 
#-1535.8  -664.5    -6.9   508.8  1606.3 

#Coefficients:
#  Estimate Std. Error t value             Pr(>|t|)    
#(Intercept)                    6417.0236   256.9462  24.974 < 0.0000000000000002 ***
#  Total.Self.Cancels.bw.0.30.min   -0.4723     0.1006  -4.696            0.0000311 ***
#  ---
#  Signif. codes:  0 '***' 0.001 '**' 0.01 '*' 0.05 '.' 0.1 ' ' 1

#Residual standard error: 792.4 on 40 degrees of freedom
#Multiple R-squared:  0.3553,	Adjusted R-squared:  0.3392 
#F-statistic: 22.05 on 1 and 40 DF,  p-value: 0.00003107

#train/test
set.seed(15)
train1 <- createDataPartition(df.com$returns, p = .6, list = F)
df.test <- df.com[-train1,]

model.1.pred <- predict(model.1, df.test)
RMSE(model.1.pred, df.test$returns) #864
plot(model.1.pred, df.test$returns, col = "red")
abline(0,1)
