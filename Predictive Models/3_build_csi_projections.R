##### Set up Projected CSI Contacts ######

#Note: this script is to pull historical contacts and build model


# set_env -----------------------------------------------------------------

library(rIA)
set_env(clear_env = T, dir = "H:/Projects/MikeAbla/Contact Forecast", extra_pkgs = c('xlsx', 'caret', 'randomForest', 'xgboost', 'Matrix', 'gbm'))


# extract csi and placed orders data --------------------------------------

connDEV <- odbcConnect('cs-db-dev')
csi_extract <- sqlQuery(connDEV, "SELECT [business_unit],[fiscal_week],[topic], sum([adjusted_contact_volume]) as adj_contacts
                                  FROM [CSAQuery].[dbo].[csi_daily]
                                  WHERE business_unit in ('sba', 'dotcom') AND fiscal_week NOT LIKE ('FY15%')
                                  Group by [business_unit],[fiscal_week],[topic]
                                  ORDER BY [business_unit],[fiscal_week],[topic]", stringsAsFactors = F) %>%
  group_by(business_unit, fiscal_week, topic) %>% 
  summarise(adj_contacts=sum(adj_contacts,na.rm = T)) %>% ungroup()
odbcClose(connDEV)

csi_extract1 <- csi_extract %>% #filter out P8 since it is not complete yet
  filter(fiscal_week != 'FY17 P08 W1')

calendar_edit <- calendar %>%
  select(fiscal_week_of_period_long, fiscal_week_start_date, holiday_flag) %>%
  mutate(holiday_flag = ifelse(holiday_flag == 'Y', 1, 0)) %>%
  group_by(fiscal_week_of_period_long, fiscal_week_start_date) %>%
  summarise(holiday_wk = max(holiday_flag)) %>%
  arrange(fiscal_week_start_date) %>%
  ungroup %>%
  mutate(post_holiday_wk = ifelse(lag(holiday_wk, 1) == 1, 1, 0),
         fiscal_week_of_period_long = gsub('20', '', fiscal_week_of_period_long ))

csi_df <- left_join(csi_extract1, calendar_edit, by = c("fiscal_week" = "fiscal_week_of_period_long")) %>% 
  dplyr::rename(businessunit = business_unit, week_start = fiscal_week_start_date) %>%
  mutate(businessunit = ifelse(businessunit == 'sba', 'SBA', 'Staples.com'))

csi_df_totalcsi <- csi_df %>% group_by(businessunit, fiscal_week) %>% summarise(total_adj_contacts = sum(adj_contacts, na.rm = T))
csi_df <- left_join(csi_df, csi_df_totalcsi, by = c("businessunit", "fiscal_week")) %>% mutate(perc_of_total = adj_contacts/total_adj_contacts)

connDEV <- odbcConnect('cs-db-dev')  
placedorders_df <- sqlQuery(connDEV, "SELECT * FROM [CSAQuery].[dbo].[placeorders_forecast]", stringsAsFactors = F) %>% 
  mutate(week_start = as.Date(week_start))
odbcClose(connDEV)

csi_po_df <- left_join(csi_df, placedorders_df, by = c("week_start", "businessunit")) %>% select(-fiscal_week.x) %>% dplyr::rename(fiscal_week = fiscal_week.y) %>% 
  dplyr::mutate(topic = as.factor(topic), week1 = ifelse(grepl("W1", fiscal_week), 1, 0))
         
sba_df <- csi_po_df %>% filter(businessunit == 'SBA') 
com_df <- csi_po_df %>% filter(businessunit == 'Staples.com')


# sba csi model -----------------------------------------------------------

#split data
set.seed(83)
train1 <- createDataPartition(sba_df$adj_contacts, p = .6, list = F, )

sba_train <- sba_df[train1, c("adj_contacts", "topic", "holiday_wk",  "OrdersPlaced", "week1")]
sba_test <- sba_df[-train1,c("adj_contacts", "topic", "holiday_wk",  "OrdersPlaced", "week1")]

#linear regression
sba.lm <- train(adj_contacts ~ ., sba_train, method = "lm")
summary(sba.lm)

sba.lm.pred <- predict(sba.lm, sba_test)
sqrt(mean((sba.lm.pred - sba_test$adj_contacts)^2, na.rm = T)) #859
plot(sba.lm.pred, sba_test$adj_contacts, col = "red", main = "sba linear reg")
abline(0,1)

#cross validation
sba.ctrl <- trainControl(method = "cv", number = 10)
sba.cv <- train(adj_contacts ~ topic + holiday_wk + OrdersPlaced + week1, sba_df, method = "lm", trControl = sba.ctrl, metric = "Rsquared")
summary(sba.cv)

sba.cv.pred <- predict(sba.cv, sba_test)
RMSE(sba.cv.pred, sba_test$adj_contacts) #842
plot(sba.cv.pred, sba_test$adj_contacts, col = "red", main = "sba cv linear reg")
abline(0,1)

#TRANSPOSE DATA!!!
sba_train.x <- sba_train %>% dplyr::mutate(topic_dum = 1) %>% dcast(adj_contacts + holiday_wk  + OrdersPlaced + week1 ~ topic, fill = 0, value.var = "topic_dum")
sba_test.x <- sba_test %>% dplyr::mutate(topic_dum = 1) %>% dcast(adj_contacts + holiday_wk + OrdersPlaced + week1 ~ topic, fill = 0, value.var = "topic_dum")

#RandomForest 
sba_train.rf <- sba_train.x
names(sba_train.rf) <- gsub(" ", "_", names(sba_train.rf))
names(sba_train.rf) <- gsub("/", ".", names(sba_train.rf))
sba_test.rf <- sba_test.x
names(sba_test.rf) <- gsub(" ", "_", names(sba_test.rf))
names(sba_test.rf) <- gsub("/", ".", names(sba_test.rf))

set.seed(83)
sba.rf <- randomForest(adj_contacts ~ ., sba_train.rf, ntree = 10000, mtry = 11 )

sba.rf.pred <- predict(sba.rf, sba_test.rf)
RMSE(sba.rf.pred, sba_test.rf$adj_contacts) #794
plot(sba.rf.pred, sba_test.rf$adj_contacts, col = "red", main = "sba rf")
abline(0,1)

#gbm
sba.gbmGrid <- expand.grid(
  n.trees = c(1750, 2000, 2250),
  shrinkage = c( 0.01, .0085, .007), #lambda
  interaction.depth = c(4,6),
  n.minobsinnode = c(10)) #default

#sba.gbmtrcl <- trainControl(
  #method = "cv",
  #number = 5,
  #verboseIter = F,
  #allowParallel = T)

sba.gbmtrcl <- trainControl(
  method = "cv",
  number = 10)

set.seed(83)
sba.gbm <- train(x = as.matrix(sba_train.x[, -c(1)]), sba_train.x$adj_contacts, trControl = sba.gbmtrcl, tuneGrid = sba.gbmGrid, method = "gbm")

head(sba.gbm$results[with(sba.gbm$results, order(RMSE)),]) #get the top 5 models
#   shrinkage interaction.depth n.minobsinnode n.trees     RMSE  Rsquared   RMSESD  RsquaredSD
#17    0.0100                 6             10    2000 849.2252 0.9726873 158.6111 0.007341833
#16    0.0100                 6             10    1750 849.6679 0.9726864 155.6592 0.007105897
#18    0.0100                 6             10    2250 849.9096 0.9726497 159.1987 0.007432441
#12    0.0085                 6             10    2250 850.6283 0.9725414 157.5254 0.007387848
#6     0.0070                 6             10    2250 851.5511 0.9725189 151.0837 0.006880743
#10    0.0085                 6             10    1750 851.8653 0.9724414 153.0967 0.007069346

sba.gbm$bestTune
#   shrinkage interaction.depth n.minobsinnode n.trees     RMSE  Rsquared   RMSESD  RsquaredSD
#17    0.0100                 6             10    2000 849.2252 0.9726873 158.6111 0.007341833

sba.gbm.pred <- predict(sba.gbm, sba_test.x)
RMSE(sba.gbm.pred, sba_test.x$adj_contacts) #773
plot(sba.gbm.pred, sba_test.x$adj_contacts, col = "red", main = "sba gbm")
abline(0,1)

#XGBoost (Non-Caret)
sba.xgb1 <- xgboost(data = as.matrix(sba_train.x[,-1]),
                    label = sba_train.x$adj_contacts,
                    objective = "reg:linear",
                    eval_metric = "rmse",
                    seed = 83,
                    nrounds = 2500,
                    eta = 0.025,
                    max_depth = 2,
                    nthread = 2)

sba.xgb1.pred <- predict(sba.xgb1, data.matrix(sba_test.x[,-1]))
RMSE(sba.xgb1.pred, sba_test.x$adj_contacts) #779
plot(sba.xgb1.pred, sba_test.x$adj_contacts, col = "red", main = "sba xgboost")
abline(0,1)

#### XGBoost Method (Caret) - Not recommended to use
sba.xbGrid <- expand.grid(
  nrounds = c(2000, 2250),
  max_depth = c(1, 2, 4),
  eta = c(0.007, 0.008),
  gamma = c(0, 1, 2),
  colsample_bytree = c(1, 0.5, 0.25),
  min_child_weight = c(1, 2), 
  subsample = 1)

sba.xbtrcl <- trainControl(
  method = "repeatedcv",
  number = 5,
  repeats = 2,
  verboseIter = FALSE,
  returnData = FALSE,
  allowParallel = TRUE)

sba.xb <- train(x = as.matrix(sba_train.x[, -c(1)]), sba_train.x$adj_contacts, trControl = sba.xbtrcl, tuneGrid = sba.xbGrid, objective = "reg:linear", method = "xgbTree")

head(sba.xb$results[with(sba.xb$results, order(RMSE)),])
sba.xb.pred <- predict(sba.xb, sba_test.x)
RMSE(sba.xb.pred, sba_test.x$adj_contacts) #799
plot(sba.xb.pred, sba_test.x$adj_contacts, col = "red", main = "xgboost 2")
abline(0,1)


# sba model predictions ---------------------------------------------------

#if using xgboost, convert dataset to matrix
sba_df_trans <- sba_df %>% dplyr::mutate(topic_dum = 1) %>%  
  dcast(businessunit + adj_contacts + week_start + fiscal_week + holiday_wk + OrdersPlaced + week1 ~ topic, fill = 0, value.var = "topic_dum")

sba_df_trans_xgboost <- sba_df_trans %>% select(-businessunit, -week_start, -fiscal_week)
 
#Enter Model below to use:
#sba_pred <- predict(sba.gbm, sba_df_trans)
sba_pred <- predict(sba.xgb1, data.matrix(sba_df_trans_xgboost[,-1]))
#names(sba_df_trans) <- gsub(" ", "_", names(sba_df_trans))
#names(sba_df_trans) <- gsub("/", ".", names(sba_df_trans))
#sba_pred <- predict(sba.rf, sba_df_trans)

#Clean data and calculate RMSE
sba_df_trans1 <- sba_df_trans %>% dplyr::mutate(adj_contact_proj = sba_pred)
sba_contacts <- sba_df_trans1 %>% 
  group_by(businessunit,week_start, fiscal_week, holiday_wk, OrdersPlaced, week1) %>%
  dplyr::summarise(adj_contacts = sum(adj_contacts, na.rm = T), adj_contact_proj = sum(adj_contact_proj, na.rm = T)) %>%
  dplyr::mutate(contact_miss = adj_contact_proj - adj_contacts)

RMSE(sba_contacts$adj_contacts, sba_contacts$adj_contact_proj) #3027
plot(sba_contacts$adj_contacts, sba_contacts$adj_contact_proj, col = "red", main = "SBA CSI Contact Actuals vs Projected")
abline(0,1)
 
#compare against lm
test.lm <- lm(adj_contacts ~ OrdersPlaced + holiday_wk + week1, sba_contacts)
summary(test.lm)
test.lm.pred <- predict(test.lm, sba_contacts)
RMSE(test.lm.pred, sba_contacts$adj_contacts) #4538


# com csi model -----------------------------------------------------------

#split data
set.seed(83)
train2 <- createDataPartition(com_df$adj_contacts, p = 0.65, list = F) #increase train split to factor the .com volatility

com_train <- com_df[train2, c("adj_contacts", "topic", "holiday_wk",  "OrdersPlaced", "week1")]
com_test <- com_df[-train2, c("adj_contacts", "topic", "holiday_wk",  "OrdersPlaced", "week1")]

#linear regression
com.lm <- train(adj_contacts~., data = com_train, method = "lm")
summary(com.lm)

com.lm.pred <- predict(com.lm, com_test)
RMSE(pred = com.lm.pred, obs = com_test$adj_contacts) #1057
plot(com.lm.pred, com_test$adj_contacts, col = "red", main = "com linear reg" )
abline(0,1)


#cross validation
com.ctrl <- trainControl(method = "cv", number = 10)
com.cv <- train(adj_contacts~topic + holiday_wk + OrdersPlaced + week1, com_df, method = "lm", trControl = com.ctrl, metric = "Rsquared")
summary(com.cv)

com.cv.pred <- predict(com.cv, com_test)
RMSE(com.cv.pred, com_test$adj_contacts) #1046
plot(com.cv.pred, com_test$adj_contacts, col = "red", main = "com cv")
abline(0,1)

#TRANPOSE TOPICS!!
com_train.x <- com_train %>% dplyr::mutate(topic_dum = 1) %>% dcast(adj_contacts + holiday_wk + OrdersPlaced + week1 ~ topic, fill = 0, value.var = "topic_dum")
com_test.x <- com_test %>% dplyr::mutate(topic_dum = 1) %>% dcast(adj_contacts + holiday_wk + OrdersPlaced + week1 ~ topic, fill = 0, value.var = "topic_dum")

#randomForest
com_train.rf <- com_train.x
names(com_train.rf) <- gsub(" ", "_", names(com_train.rf))
names(com_train.rf) <- gsub("/", ".", names(com_train.rf))
com_test.rf <- com_test.x
names(com_test.rf) <- gsub(" ", "_", names(com_test.rf))
names(com_test.rf) <- gsub("/", ".", names(com_test.rf))

com.rf <- randomForest(adj_contacts~., com_train.rf, ntree = 7500, mtry = 8)

com.rf.pred <- predict(com.rf, com_test.rf)
RMSE(com.rf.pred, com_test.rf$adj_contacts) #1070
plot(com.rf.pred, com_test.rf$adj_contacts, col = "red", main = "com rf")
abline(0,1)

#gbm
com.gbmGrid <- expand.grid(
  n.trees = c(1000, 1500, 2000),
  shrinkage = c(0.001, 0.005, 0.01),
  interaction.depth = c(4,6),
  n.minobsinnode = c(10))

com.gbmtrcl <- trainControl(
  method = "cv",
  number = 10)

set.seed(83)
com.gbm <- train(adj_contacts~., com_train.x, method = "gbm", trControl = com.gbmtrcl, tuneGrid = com.gbmGrid)

head(com.gbm$results[with(com.gbm$results, order(RMSE)),])
#shrinkage interaction.depth n.minobsinnode n.trees     RMSE  Rsquared   RMSESD RsquaredSD
#17     0.010                 6             10    1500 1104.833 0.9335382 110.3637 0.01394784
#18     0.010                 6             10    2000 1105.182 0.9332979 112.9676 0.01428526
#15     0.010                 4             10    2000 1106.330 0.9332508 107.4829 0.01373297
#12     0.005                 6             10    2000 1112.439 0.9329335 111.0286 0.01450639
#16     0.010                 6             10    1000 1113.563 0.9328780 113.6022 0.01449226
#14     0.010                 4             10    1500 1115.180 0.9325989 112.4441 0.01439145

com.gbm$bestTune
#shrinkage interaction.depth n.minobsinnode n.trees     RMSE  Rsquared   RMSESD RsquaredSD
#17     0.010                 6             10    1500 1104.833 0.9335382 110.3637 0.01394784

com.gbm.pred <- predict(com.gbm, com_test.x)
RMSE(com.gbm.pred, com_test.x$adj_contacts) #1092
plot(com.gbm.pred, com_test.x$adj_contacts, col = "red", main = "com gbm")
abline(0,1)

#XGBoost (Non-Caret)
com.xgb1 <- xgboost(data = as.matrix(com_train.x[,-1]),
                    label = com_train.x$adj_contacts,
                    objective = "reg:linear",
                    eval_metric = "rmse",
                    seed = 83,
                    nrounds = 12500,
                    eta = 0.005,
                    max_depth = 2,
                    nthread = 2)

com.xgb1.pred <- predict(com.xgb1, data.matrix(com_test.x[,-1]))
RMSE(com.xgb1.pred, com_test.x$adj_contacts) #1042
plot(com.xgb1.pred, com_test.x$adj_contacts, col = "red", main = "com xgboost")
abline(0,1)


# com predictions ---------------------------------------------------------

com_df_trans <- com_df %>% dplyr::mutate(topic_dum = 1) %>%  
  dcast(businessunit + adj_contacts + week_start + fiscal_week + holiday_wk + OrdersPlaced + week1 ~ topic, fill = 0, value.var = "topic_dum") 

com_df_trans_xgboost <- com_df_trans %>% select(-businessunit, -week_start, -fiscal_week)

#Enter Model below to use:
#com_pred <- predict(com.gbm, com_df_trans)
com_pred <- predict(com.xgb1, data.matrix(com_df_trans_xgboost[,-1]))

#clean data and calculate RMSE
com_df_trans1 <- com_df_trans %>% dplyr::mutate(adj_contact_proj = com_pred)

com_contacts <- com_df_trans1 %>%
  group_by(businessunit,week_start, fiscal_week, holiday_wk, OrdersPlaced, week1) %>%
  dplyr::summarise(adj_contacts = sum(adj_contacts, na.rm = T), adj_contact_proj = sum(adj_contact_proj, na.rm = T)) %>%
  dplyr::mutate(contact_miss = adj_contact_proj - adj_contacts) 

RMSE(com_contacts$adj_contact_proj, com_contacts$adj_contacts) #5169
plot(com_contacts$adj_contacts, com_contacts$adj_contact_proj, col = "red", main = "CSI Contact Actuals vs Projected")
abline(0,1)

#compare against lm
com.test.lm <- lm(adj_contacts ~ OrdersPlaced + holiday_wk + week1, com_contacts)
summary(com.test.lm)
com.test.lm.pred <- predict(com.test.lm, com_contacts)
RMSE(com.test.lm.pred, com_contacts$adj_contacts) #7096
plot(com.test.lm.pred, com_contacts$adj_contacts, col = "red", main = "LM Test")
abline(0,1)


# create projection table -------------------------------------------------

#sba - historical projections
sba_contacts1 <- sba_df_trans1 %>%
  melt(id.vars = c('businessunit','week_start', 'fiscal_week', 'holiday_wk', 'week1', 'OrdersPlaced', 'adj_contacts', 'adj_contact_proj')) %>%
  filter(value == 1) %>% arrange(fiscal_week) %>% select(-value) %>% mutate(adj_contact_proj = ifelse(adj_contact_proj < 0, 0, adj_contact_proj))
sba_contacts_total <- sba_contacts1 %>% dplyr::group_by(businessunit, week_start, fiscal_week, holiday_wk, week1,  OrdersPlaced) %>%
  dplyr::summarise(adj_contacts = sum(adj_contacts, na.rm = T), adj_contact_proj = sum(adj_contact_proj, na.rm = T)) %>% dplyr::mutate(variable = "Total") %>%
  dplyr::ungroup()
sba_contacts_hist <- rbind(sba_contacts1, sba_contacts_total) %>% dplyr::arrange(fiscal_week) %>% left_join(select(.data = placedorders_df, businessunit, week_start, OrdersPlaced_proj) , by = c("businessunit","week_start"))
#sba_contacts_hist1 <- sba_contacts_hist %>% filter(variable == 'Total') %>% dplyr::mutate(for.diff = adj_contacts - adj_contact_proj)

#com - historical projections
com_contacts1 <- com_df_trans1 %>%
  melt(id.vars = c('businessunit','week_start', 'fiscal_week', 'holiday_wk', 'week1', 'OrdersPlaced', 'adj_contacts', 'adj_contact_proj')) %>%
  filter(value == 1) %>% arrange(fiscal_week) %>% select(-value) %>% mutate(adj_contact_proj = ifelse(adj_contact_proj < 0, 0, adj_contact_proj))
com_contacts_total <- com_contacts1 %>% dplyr::group_by(businessunit, week_start, fiscal_week, holiday_wk, week1,  OrdersPlaced) %>%
  dplyr::summarise(adj_contacts = sum(adj_contacts, na.rm = T), adj_contact_proj = sum(adj_contact_proj, na.rm = T)) %>% dplyr::mutate(variable = "Total") %>%
  dplyr::ungroup()
com_contacts_hist <- rbind(com_contacts1, com_contacts_total) %>% dplyr::arrange(fiscal_week) %>% left_join(select(.data = placedorders_df, businessunit, week_start, OrdersPlaced_proj) , by = c("businessunit","week_start"))
#com_contacts_hist1 <- com_contacts_hist %>% filter(variable == 'Total') %>% dplyr::mutate(for.diff = adj_contacts - adj_contact_proj)

#create week projection dates to end of FY17
calendar_edit1 <- calendar_edit %>% 
  filter(fiscal_week_start_date <= '2018-01-28' & fiscal_week_start_date >= '2017-01-29') %>% 
  anti_join(csi_df, by = c('fiscal_week_start_date' = 'week_start')) %>%
  dplyr::mutate(week1 = ifelse(grepl("W1", fiscal_week_of_period_long), 1, 0)) %>%
  arrange(fiscal_week_start_date) %>% 
  dplyr::rename(week_start = fiscal_week_start_date, fiscal_week =  fiscal_week_of_period_long) %>%
  dplyr::select(-post_holiday_wk)
  
#sba - create projection table and add predicted contacts
sba_topics <- sba_contacts_hist %>% distinct(variable)
sba_topics$variable <- factor(sba_topics$variable, levels = c("Account", "Cancel", "Change Order", "Coupon" ,"Customer Education", "Customer Experience", "Documentation", "Financial", 
           "No contact", "Other","Other Dept./Group", "Place Order", "Price Match", "Product", "Return", "Store", "Tax Exempt", "Transfer", "WiMS", "Total"))
sba_topics1 <- cbind(calendar_edit1, variable = rep(sba_topics$variable, nrow(calendar_edit1))) %>% dplyr::arrange(week_start) %>% dplyr::mutate(businessunit = 'SBA') %>% select(businessunit, week_start, everything())
sba_contacts_proj <- left_join(select(.data= sba_topics1, -fiscal_week), select(.data = placedorders_df, -recID, -OrdersPlaced_lag1, -OrdersPlaced_lag2), by = c("businessunit", "week_start")) %>%
  dplyr::mutate(adj_contacts = NA )
    
    #create future predictions
    sba_df_trans2 <- sba_contacts_proj %>% dplyr::mutate(topic_dum = 1) %>%  
      dcast(businessunit + adj_contacts + week_start + fiscal_week + holiday_wk + OrdersPlaced + OrdersPlaced_proj + week1 + variable ~ variable, fill = 0, value.var = "topic_dum") %>%
      filter(variable != 'Total') %>%
      select(-Total)
    sba_df_trans_xgboost2 <- sba_df_trans2 %>% select(-(businessunit:fiscal_week), -OrdersPlaced, -variable)
    
    sba_pred2 <- predict(sba.xgb1, data.matrix(sba_df_trans_xgboost2))
    sba_df_trans3 <- sba_df_trans2 %>%  
      dplyr::mutate(adj_contact_proj = sba_pred2) %>%
      dplyr::mutate(adj_contact_proj = ifelse(adj_contact_proj < 0, 0, adj_contact_proj)) %>%
      select(-(Account:WiMS)) %>%
      select(businessunit, week_start, fiscal_week, holiday_wk, week1, OrdersPlaced, adj_contacts, adj_contact_proj, variable, OrdersPlaced_proj)
    
    sba_df_totals <- sba_df_trans3 %>% 
      group_by(businessunit, week_start, fiscal_week, holiday_wk, week1, OrdersPlaced, adj_contacts,OrdersPlaced_proj) %>%
      dplyr::summarise(adj_contact_proj = sum(adj_contact_proj, na.rm = T)) %>%
      dplyr::mutate(variable = 'Total') %>%
      dplyr::ungroup()

#sba - finalize table
sba_contacts_final <- rbind(sba_contacts_hist, sba_df_trans3, sba_df_totals)

#com - create projection table and add predicted contacts
com_topics <- com_contacts_hist %>% distinct(variable)
com_topics$variable <- factor(com_topics$variable, levels = c("Account", "Cancel", "Change Order", "Coupon" ,"Customer Education", "Customer Experience", "Documentation", "Financial", 
                                                              "No contact", "Other","Other Dept./Group", "Place Order", "Price Match", "Product", "Rebate", "Return", "Rewards","Store", "Tax Exempt", "Transfer", "WiMS", "Total"))
com_topics1 <- cbind(calendar_edit1, variable = rep(com_topics$variable, nrow(calendar_edit1))) %>% dplyr::arrange(week_start) %>% dplyr::mutate(businessunit = 'Staples.com') %>% select(businessunit, week_start, everything())
com_contacts_proj <- left_join(select(.data= com_topics1, -fiscal_week), select(.data = placedorders_df, -recID, -OrdersPlaced_lag1, -OrdersPlaced_lag2), by = c("businessunit", "week_start")) %>%
  dplyr::mutate(adj_contacts = NA )

  #create future predictions
  com_df_trans2 <- com_contacts_proj %>% dplyr::mutate(topic_dum = 1) %>%  
    dcast(businessunit + adj_contacts + week_start + fiscal_week + holiday_wk + OrdersPlaced + OrdersPlaced_proj + week1 + variable ~ variable, fill = 0, value.var = "topic_dum") %>%
    filter(variable != 'Total') %>%
    select(-Total)
  com_df_trans_xgboost2 <- com_df_trans2 %>% select(-(businessunit:fiscal_week), -OrdersPlaced, -variable)
  
  com_pred2 <- predict(com.xgb1, data.matrix(com_df_trans_xgboost2))
  com_df_trans3 <- com_df_trans2 %>%  
    dplyr::mutate(adj_contact_proj = com_pred2) %>%
    dplyr::mutate(adj_contact_proj = ifelse(adj_contact_proj < 0, 0, adj_contact_proj)) %>%
    select(-(Account:WiMS)) %>%
    select(businessunit, week_start, fiscal_week, holiday_wk, week1, OrdersPlaced, adj_contacts, adj_contact_proj, variable, OrdersPlaced_proj)
  
  com_df_totals <- com_df_trans3 %>% 
    group_by(businessunit, week_start, fiscal_week, holiday_wk, week1, OrdersPlaced, adj_contacts,OrdersPlaced_proj) %>%
    dplyr::summarise(adj_contact_proj = sum(adj_contact_proj, na.rm = T)) %>%
    dplyr::mutate(variable = 'Total') %>%
    dplyr::ungroup()

#com - finalize table
com_contacts_final <- rbind(com_contacts_hist, com_df_trans3, com_df_totals)


# export ------------------------------------------------------------------

final_df <- rbind(com_contacts_final, sba_contacts_final) %>%
  select(businessunit, week_start, fiscal_week, holiday_wk, week1, variable, adj_contacts, adj_contact_proj) %>%
  dplyr::filter(variable != 'Total') %>% #note initial run included Totals in table but decided to remove
  dplyr::arrange(week_start, businessunit)

connDEV <- odbcConnect('cs-db-dev')
sqlSave(connDEV, final_df, tablename = 'dbo.csi_contact_forecast', varTypes = c(week_start="date"), rownames = F)
odbcClose(connDEV)
