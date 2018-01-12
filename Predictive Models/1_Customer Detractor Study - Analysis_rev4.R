################ Customer Detractor Analysis Rev IV #####################

#by Mike Abla

#Notes:
#This is Part II, conducting the analysis
#read the "SBA FY16-FY17 Q1.Rdata" file to bring in the data
#Rev IV is looking at Q1 YoY Results, which needed to be cleaned and 
#converted to contact rates

##########################################################################


# Setup Environment -------------------------------------------------------

library(rIA)
set_env(clear_env = T, dir = "~/Projects/MikeAbla/Customer Detractor Study",
        extra_pkgs = c('caret', 'rattle', 'car', 'corrplot', 'rpart', 'rpart.plot', 'ROSE'))

load("SBA FY16-FY17 Q1_v2.Rdata")
load("customer detractor base data.rdata")
rm(MidMarkets, ordermethod, ordermethod2, return.orders, sba.csi.oms, sba.cust.type, sba.oms, sba_custs, ted, wlfk) #remove all, looking to only get medallia data

# Clean Data --------------------------------------------------------------

#convert to per order rate
  #FY16
  df.rates.FY16 <- as.data.frame(apply(select(.data = FY16_FY17.df, Ohoqty_2016:total_contacts_2016), 
                         2, function(x) (x/FY16_FY17.df$total_orders_2016)))%>% 
    mutate_each( funs(.*100), -Ohoqty_2016)  #convert %s to numeric except for Ohoto & Ohoqty
  df.FY16 <- cbind(FY16_FY17.df[, c("CCCUST", "Industry", "Cccrdt","Ohoto_2016")], df.rates.FY16)
  
  #FY17
  df.rates.FY17 <- as.data.frame(apply(select(.data = FY16_FY17.df, Ohoqty_2017:total_contacts_2017), 
                                       2, function(x) (x/FY16_FY17.df$total_orders_2017)))%>% 
    mutate_each( funs(.*100), -Ohoqty_2017)  #convert %s to numeric except for Ohoto & Ohoqty
  df.FY17 <- cbind(FY16_FY17.df[, c("CCCUST", "Industry", "Cccrdt","Ohoto_2017")], df.rates.FY17) 
  df.FY17[is.na(df.FY17)] <- 0
  
#take difference from FY2017 to FY2016
  df.diff <- as.data.frame(df.FY17[,-c(1:3)] - df.FY16[,-c(1:3)]) 
  names(df.diff) <- gsub("_2017","",names(df.diff))
  df.diff <- cbind(FY16_FY17.df[, c("CCCUST", "Industry", "Cccrdt")], df.diff) %>% rename(total_spend = Ohoto)
  
#bring in FY Q1 totals
  df.totals.FY16 <- FY16_FY17.df %>%
    group_by(CCCUST) %>%
    summarise(total_orders_FY16 = sum(total_orders_2016, na.rm = T),
              total_spend_FY16 = sum(Ohoto_2016, na.rm = T),
              total_contacts_FY16 = sum(total_contacts_2016, na.rm = T),
              total_ted_FY16 = sum(ted_contact_2016, na.rm = T))
  
  df.totals.FY17 <- FY16_FY17.df %>%
    group_by(CCCUST) %>%
    summarise(total_orders_FY17 = sum(total_orders_2017, na.rm = T),
              total_spend_FY17 = sum(Ohoto_2017, na.rm = T),
              total_contacts_FY17 = sum(total_contacts_2017, na.rm = T),
              total_ted_FY17 = sum(ted_contact_2017, na.rm = T))

  
#medallia data
  medallia.Q1 <- medallia %>%
    filter(staples_period_column_responses_export %in% c("P1'16", "P2'16", "P3'16")) %>%
    select(AccountNumber, Q2_OverallSatisfaction, Q4_EaseofTransaction, Q5_LikelihoodtoRecommend,AccountNumber, Q12_Valueforpricepaid) %>%
    mutate(Q2_OverallSatisfaction = as.numeric(ifelse(is.na(Q2_OverallSatisfaction)| Q2_OverallSatisfaction == 'N/A', NA,as.character(Q2_OverallSatisfaction))),
           Q4_EaseofTransaction = as.numeric(ifelse(is.na(Q4_EaseofTransaction)| Q4_EaseofTransaction == 'N/A', NA,as.character(Q4_EaseofTransaction))),
           Q5_LikelihoodtoRecommend = as.numeric(ifelse(is.na(Q5_LikelihoodtoRecommend)| Q5_LikelihoodtoRecommend == 'N/A', NA,as.character(Q5_LikelihoodtoRecommend))),
           Q12_Valueforpricepaid = as.numeric(ifelse(is.na(Q12_Valueforpricepaid)| Q12_Valueforpricepaid == 'N/A', NA,as.character(Q12_Valueforpricepaid)))) %>%
    group_by(AccountNumber) %>%
    summarise(Q2_OverallSatisfaction = mean(Q2_OverallSatisfaction, na.rm = T),
              Q4_EaseofTransaction = mean(Q4_EaseofTransaction, na.rm = T),
              Q5_LikelihoodtoRecommend = mean(Q5_LikelihoodtoRecommend, na.rm = T),
              Q12_Valueforpricepaid = mean(Q12_Valueforpricepaid, na.rm = T)) %>%
    mutate(Q2_OverallSatisfaction.prom = as.factor(ifelse(Q2_OverallSatisfaction >= 4, 1, 0)),
           Q4_EaseofTransaction.prom = as.factor(ifelse(Q4_EaseofTransaction >= 4, 1, 0)),
           Q5_LikelihoodtoRecommend.prom = as.factor(ifelse(Q5_LikelihoodtoRecommend >= 9, 1, 0)),
           Q12_Valueforpricepaid.prom = as.factor(ifelse(Q12_Valueforpricepaid >= 4, 1, 0)))
    
#join FY total to diff table
  #df.final <- df.diff %>%
    #left_join(df.totals.FY16, by = c("CCCUST")) %>%
    #left_join(df.totals.FY17, by = c("CCCUST")) %>%
    #mutate(manual_order = PHONE.ORDER + E.MAIL.ORDER + SALESMAN.RECD.ORDER,
           #manual_order.csi = `PHONE ORDER.csi` + `SALES RECD PHONE` + `E.MAIL.ORDER.csi`,
           #spend_change_quartile = as.factor(ntile(total_spend, 4)),
           #spend_change_percentile = percent_rank(total_spend)*100,
           #spend_change_percentile_bin = as.factor(ntile(spend_change_percentile, 10)),
           #spend_yoy_loss = as.factor(ifelse(total_spend < 0, 1, 0)),
           #FY16spend_quartile = as.factor(ntile(total_spend_FY16,4)),
           #FY16spend_percentile = percent_rank(total_spend_FY16)*100,
           #FY16spend_percentile_bin = as.factor(ntile(FY16spend_percentile, 10)),
           #churn = ifelse(total_spend_FY17 == 0, 1, 0)) %>%
    #rename(PHONE.ORDER.csi = `PHONE ORDER.csi`,
           #SALES.RECD.PHONE.csi = `SALES RECD PHONE`) %>%
    #filter(churn != 1, Industry != 'Unknown', total_spend_FY16 != 0 ) %>%
    #mutate(total_spend_perc = ((total_spend_FY17 - total_spend_FY16)/(total_spend_FY16)) *100) %>%
    #filter(total_spend_perc < 1000)

#join FY total to FY16 data   
  df.final.a <- select(.data = df.diff, CCCUST, Industry, Cccrdt, total_spend) %>%
    left_join(df.totals.FY16, by = c("CCCUST")) %>%
    left_join(df.totals.FY17, by = c("CCCUST")) %>%
    left_join(select(
      .data = df.FY16, CCCUST, stock_2016:total_contacts_2016), by = c("CCCUST")) %>%
    left_join(medallia.Q1,by = c("CCCUST" = "AccountNumber")) 
    
  names(df.final.a) <- gsub('\\_2016', '', names(df.final.a))

   df.final <- df.final.a %>%
     mutate(manual_order.csi = PHONE.ORDER.csi + SALES.RECD.PHONE.csi + `E.MAIL.ORDER.csi` + `FAX.ORDER.csi` + MOBILE.ORDER.csi,
            #spend_change_quartile = as.factor(ntile(total_spend, 4)),
            #spend_change_percentile = percent_rank(total_spend)*100,
            #spend_change_percentile_bin = as.factor(ntile(spend_change_percentile, 10)),
            spend_yoy_loss = as.factor(ifelse(total_spend < 0, 1, 0)),
            FY16spend_quartile = as.factor(ntile(total_spend_FY16,4)),
            #FY16spend_percentile = percent_rank(total_spend_FY16)*100,
            #FY16spend_percentile_bin = as.factor(ntile(FY16spend_percentile, 10)),
            churn = as.factor(ifelse(total_spend_FY17 == 0, 1, 0)),
            order_issue = latedelivery + fillkill + returns + backorder,
            wraplabel_bin = cut(wraplabel, c(-Inf,0,25,50,75,100, Inf)),
            dropship_bin = cut(dropship, c(-Inf,0,25,50,75,100, Inf)),
            order_issue_bin = cut(order_issue, c(-Inf,0,25,50,75,100, Inf)),
            wraplabel_75plus = ifelse(wraplabel >= 75, 1, 0),
            latedelivery_75plus = ifelse(latedelivery >= 75, 1, 0),
            Manual.Order.EBROKER = Manual.Order + E.BROKER,
            dropship_75plus = ifelse(dropship >= 75, 1, 0),
            NPS = ifelse(Q5_LikelihoodtoRecommend >= 9, 100, ifelse(Q5_LikelihoodtoRecommend <=8 & Q5_LikelihoodtoRecommend >= 7, 0, -100)),
            new_cust = ifelse(Cccrdt > 20151101, 1, 0)) %>% #new cust defined as 181 days or less
     filter(total_spend_FY16 != 0) %>%
     mutate(total_spend_perc = ((total_spend_FY17 - total_spend_FY16)/(total_spend_FY16)) *100) 

save(df.final, file = "SBA FY16-FY17 Q1_v3.Rdata")   
     
# EDA  --------------------------------------------------------------------

  #means
  means_table <- select(.data = df.final, stock:manual_order.csi, 
                        spend_yoy_loss, churn, 
                        total_spend, total_spend_perc,
                        FY16spend_quartile, Industry, Q2_OverallSatisfaction:Q12_Valueforpricepaid, order_issue)  %>%
    #mutate(churn = as.numeric(churn)) %>%
    #group_by(FY16spend_quartile) %>%
    #group_by(FY16spend_quartile, churn) %>%
    group_by(churn) %>%
    summarise_each(funs(mean(.,na.rm=T)))
  View(t(means_table))
  
  #means by spendloss
  means_spendloss <- select(.data = df.final, stock:total_contacts, 
                        spend_yoy_loss, manual_order, manual_order.csi, Q2_OverallSatisfaction:Q12_Valueforpricepaid, order_issue)  %>%
    group_by(spend_yoy_loss) %>%
    summarise_each(funs(mean(., na.rm = T)))
  View(t(means_spendloss))
  
  tapply(df.final$total_spend, df.final$Q5_LikelihoodtoRecommend.prom,mean)
  tapply(df.final$total_spend, df.final$FY16spend_quartile,mean)
  tapply(df.final$Q5_LikelihoodtoRecommend, df.final$spend_yoy_loss, mean, na.rm = T)
  
  #T-tests
  ttest.spendloss <- function(x_var){
    spend_loss <- NULL
    spend_loss <- df.final$spend_yoy_loss == 1
    
    loss <- df.final %>% filter(spend_loss == 1) %>% select_(x_var)
    gain <- df.final %>% filter(spend_loss != 1) %>% select_(x_var)
    
    t.test(loss, gain)
  }
  
  ttest.spendloss("total_contacts")
  ttest.spendloss("dropship")
  ttest.spendloss("UPS")
  ttest.spendloss("ted_contact")
  ttest.spendloss("manual_order.csi")
  ttest.spendloss("Q5_LikelihoodtoRecommend")
  ttest.spendloss("Q12_Valueforpricepaid")
  ttest.spendloss("Q2_OverallSatisfaction")
  ttest.spendloss("Q4_EaseofTransaction")
  
  #histograms
  hist.plot <- function(x_var, facet_var, xlim.min, xlim.max){
    ggplot(data = df.final, aes_string(x= x_var), fill = FY16spend_quartile) + geom_density() +
            labs(title = paste('Histogram for', x_var,' by ',facet_var)) +
      facet_wrap(reformulate(facet_var),ncol = 1) +
      xlim(xlim.min, xlim.max)
  }
  
  hist.plot("Q5_LikelihoodtoRecommend", "spend_change_quartile", 0, 10)
  hist.plot("Q5_LikelihoodtoRecommend", "FY16spend_quartile", 0, 10)
  hist.plot("Q5_LikelihoodtoRecommend","spend_yoy_loss", 0, 10)
  
  hist.plot("wraplabel", "spend_change_quartile", 0, 100)
  hist.plot("wraplabel", "FY16spend_quartile", 0, 100)
  hist.plot("wraplabel","spend_yoy_loss", 0, 100)
  
  hist.plot("SALES.RECD.PHONE.csi", "spend_change_quartile", 0, 100)
  hist.plot("SALES.RECD.PHONE.csi", "FY16spend_quartile", 0, 100)
  hist.plot("SALES.RECD.PHONE.csi","spend_yoy_loss", 0, 100)

  hist.plot("order_issue", "spend_change_quartile", 0, 100)
  hist.plot("order_issue", "FY16spend_quartile", 0, 100)
  hist.plot("order_issue","spend_yoy_loss", 0, 100)
  
  hist.plot("Q5_LikelihoodtoRecommend", "churn", 0, 10)
  hist.plot("wraplabel", "churn", 0, 100)
  hist.plot("dropship", "churn", 0, 100)
  hist.plot("Manual.Order", "churn", 0, 100)
  hist.plot("Internet","churn", 0, 100)
  hist.plot("wraplabel","churn", 0, 100)
  hist.plot("returns","churn", 0, 100)
  hist.plot("ted_contact","churn", 0, 100)
  hist.plot("latedelivery","churn", 0, 100)
  hist.plot("UPS","churn", 0, 100)
  hist.plot("E.BROKER","churn", -10, 110)
  
  ggplot(data = df.final[df.final$new_cust == 1,], aes(x= CustomerEducation, fill = churn)) + geom_density(alpha = .5) +
    labs(title = paste('Histogram for Dropship by Churn')) +
    facet_grid(df.final[df.final$new_cust == 1, ]$churn~.) +
    xlim(0, 100)
 
  #correlations
  contact_corr <- list()
  
  for(i in 1:4){
    contact_corr[[i]] <- sapply(df.final[df.final$FY16spend_quartile == i, c(12:98,103:104,113:114)], 
                                cor,use="complete.obs", y = df.final[df.final$FY16spend_quartile == i, c("total_spend_perc")])
  }
  contact_corr <- data.frame(contact_corr)
  names(contact_corr) <- c("Q1", "Q2", "Q3", "Q4")
  
  overall_corr <- data.frame(sapply(df.final[, c(12:98,103:104,113:114)], 
                         cor, use="complete.obs", y = df.final[, c("total_spend_perc")]))
  names(overall_corr) <- c("Correlations to total_spend_%")
  
  #correlation plot
  corr.df.plot <- function(quartile){
    selected.df <- NULL
    selected.df <- df.final %>% 
      select(total_spend_perc, stock:total_contacts, manual_order, manual_order.csi, FY16spend_quartile,
             Q2_OverallSatisfaction:Q12_Valueforpricepaid, order_issue) %>%
      mutate(FY16spend_quartile = as.numeric(FY16spend_quartile)) %>%
      filter(FY16spend_quartile == quartile) %>%
      select(-FY16spend_quartile)
    corr.df <- NULL
    corr.df <- cor(selected.df)
    corr.total_spend <- NULL
    corr.total_spend <- as.matrix(sort(corr.df [,'total_spend_perc'], decreasing = T))
    corr.idx <- NULL 
    corr.idx <- names(which(apply(corr.total_spend, 1, function(x) (x > .05 | x < -.03))))
    
    print(corrplot(as.matrix(corr.df[corr.idx, corr.idx]), type = 'upper', 
                   method = 'shade', addCoef.col = 'black', title = paste('Quartile ', quartile ,' Correlation Matrix', sep = ''), 
                   tl.cex = .8, tl.col = 'black', number.cex = .7, tl.offset = 1, 
                   cl.ratio = 0.2))
    View(as.matrix(corr.df[corr.idx, corr.idx]))
  }
  
  corr.df.plot(4)
  
  selected.df <- df.final %>% 
    select(total_spend, stock:total_contacts, manual_order, manual_order.csi,
           Q2_OverallSatisfaction:Q12_Valueforpricepaid, order_issue)
  corr.df <- cor(selected.df, use = "complete.obs")
  corr.total_spend <- as.matrix(sort(corr.df [,'total_spend'], decreasing = T))
  corr.idx <- names(which(apply(corr.total_spend, 1, function(x) (x > .02 | x < -.02))))
  View(as.matrix(corr.df[corr.idx, corr.idx]))
  
  #boxplot
  boxplot.query <- function(x_var, y_var,ylim.start, ylim.end ){
    df.selected <- NULL
    df.selected <- df.final %>% select_(x_var,y_var)
    
    ggplot(df.selected, aes_string(x_var, y_var)) + geom_boxplot() +
      theme(legend.position = "bottom") + 
      scale_y_continuous(limits = c(ylim.start,ylim.end)) +
      ggtitle(paste0('Boxplot: ', x_var,' vs ', y_var)) +
      stat_summary(fun.y = mean, geom = 'point', color = "darkred", size = 3)
  }
  
  boxplot.query("churn", "Manual.Order", 0, 5)
  boxplot.query("spend_yoy_loss", "total_spend", -2000, 2000)
  boxplot.query("spend_yoy_loss", "total_contacts", 0, 100)
  boxplot.query("spend_yoy_loss", "Phone", 0, 100)
  boxplot.query("spend_yoy_loss", "PlaceOrder", 0, 100)
  boxplot.query("spend_yoy_loss", "Account", 0, 100)
  boxplot.query("spend_yoy_loss", "returns", 0, 100)
  boxplot.query("Q5_LikelihoodtoRecommend", "spend_change_quartile", 0, 100)
  
  #anova testing
  anova.tester <- function(x_var, y_var){
    fit <- NULL
    fit <- lm(as.formula(paste("",y_var,"~", x_var,sep = "")), df.final)
    print(summary(fit))
  }
  
  anova.tester("Industry", "total_spend_perc")
  
  fit.test <- lm(total_spend_perc~Industry + LINKplus.ORDER.METHOD + 
                   returns + UPS + CustomerEducation + Account + Documentation +
                   manual_order.csi + FY16spend_quartile + dropship + SALES.RECD.PHONE.csi +
                   latedelivery, df.final)
  vif(fit.test)
  summary(fit.test)
  
# standardize Data -------------------------------------------------------

  df.standardized <- df.final %>%
    select(stock:total_contacts, manual_order:manual_order.csi, -total_orders) %>%
    preProcess(method = c("center", "scale")) %>%
    #preProcess(method = c("range")) %>%
    predict(select(.data = df.final, stock:total_contacts, manual_order:manual_order.csi, -total_orders)) %>%
    cbind(df.final[, c("total_spend", "total_spend_perc", "Industry", "FY16spend_quartile", "spend_yoy_loss")])
  

# Split Train and Test Data -----------------------------------------------

  df.model <- df.final %>%
    select(Industry,
           stock:total_contacts, 
           -total_orders,
           manual_order.csi, 
           FY16spend_quartile,
           churn,
           #Q2_OverallSatisfaction:Q12_Valueforpricepaid,
           #total_spend_FY16,
           order_issue:new_cust) %>%
    mutate(Industry = as.factor(Industry)) %>%
    filter(complete.cases(.)) %>% 
    #filter(FY16spend_quartile == 4)
    filter(new_cust == 1)

intrain <- createDataPartition(df.model$churn, p = 0.6, list = F)

d.train.pre <- df.model[intrain,]
d.test <- df.model[-intrain,]

d.train <- ovun.sample(churn~., data = d.train.pre, method = 'both', seed = 4001, N = 35000, p = .5)$data 
#d.train <- ovun.sample(churn~., data = d.train.pre, method = 'both', seed = 145, N = 1000, p =.5)$data #randomforest

# Random Forest Exploratory -----------------------------------------------

  #churn model
  set.seed(133)
  rf.model.2 <- train(churn~., d.train, method = "rf" )
  plot(varImp(rf.model.2),top = 20, main = "RF Quartile 4 Var Importance Measure - w/o Survey")
  
  #test
  rtree.pred.2 <- predict(rf.model.2, d.test)
  confusionMatrix(rtree.pred.2, d.test$churn, positive = "1") #Accuracy = 79% / Sensitivity = 72.39%
  roc.curve(d.test$churn, rtree.pred.2) 
  
  
# DT Exploratory ----------------------------------------------------------

  #model
  set.seed(5550)
  dt.model.2 <- train(churn~., d.train, method = "rpart") 
  rpart.plot(dt.model.2$finalModel)
  
  #test
  dt.pred.2 <- predict(dt.model.2, d.test)
  confusionMatrix(dt.pred.2, d.test$churn, positive = "1")
  roc.curve(d.test$churn, dt.pred.2) 
  
# Logistic Model ----------------------------------------------------------

  #model
  log.model.1 <- train(churn~
                         new_cust +
                         dropship +
                         Manual.Order.EBROKER +
                         Account +
                         backorder +
                         OnTrac +
                         Fleet +
                         ted_contact +
                         WiMS,
                       d.train, 
                       method = "glm", family = "binomial" )
  plot(varImp(log.model.1), main = "Q4 Churn Variable Importance Measures")
  varImp(log.model.1)
  summary(log.model.1)
  exp(coef(log.model.1$finalModel))
  
  #test
  log.pred.1 <- predict(log.model.1, d.test)
  confusionMatrix(log.pred.1, d.test$churn, positive = "1") 
  roc.curve(d.test$churn, log.pred.1) 
  
  
# Regression Model --------------------------------------------------------

  lm.model.1 <- lm(total_spend_perc~ FY16spend_quartile + CustomerEducation + 
                     LINKplus.ORDER.METHOD + backorder + 
                     Q5_LikelihoodtoRecommend + wraplabel, df.final)
  
  vif(lm.model.1)
  summary(lm.model.1)
  
  # Multicollinearity Look --------------------------------------------------

  corr_data <- df.model %>% select(returns , backorder , UPS , 
                                   STANDARD , latedelivery , total_contacts , LINKplus.ORDER.METHOD ,
                                   WiMS , Chat , fillkill , AccountManager ,
                                   Account , CustomerExperience , Documentation ,
                                   SALES.RECD.PHONE.csi)

  multi.df <- cor(corr_data)
