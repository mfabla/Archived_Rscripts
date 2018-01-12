############### NPS/LTR Key Drivers ##################

#by Mike Abla

#Notes: Data set will be based on FY16 SBA Data


# set up env --------------------------------------------------------------

library(rIA)
set_env(clear_env = T, dir = "H:/Projects/MikeAbla/NPS-LTR Drivers", extra_pkgs = c('xlsx', 'caret', 'car'))

load("~/Projects/MikeAbla/Customer Detractor Study/customer detractor base data.rdata") #load file that has medallia data
rm("MidMarkets", "ordermethod", "ordermethod2", "return.orders", "sba.csi.oms", "sba.cust.type", "sba.oms", "sba_custs", "ted")

load("~/Projects/MikeAbla/zz_tmp Order Method/Order Method FY16 Data.Rdata") #load perivious dataset run for order method analysis
ord.method.FY16 <- ord.method.FY16[,-c(31:33)]


# clean up data -----------------------------------------------------------

#select relevant medallia fields or low missing rate
medallia.1 <- medallia %>% 
  select(-ShipmentLocation, -Website, -TransactionTypeDetailCode, -TransactionCurrency, -AccountNumber, 
         -(Q24_Repofferedproofoforder:Q28_Ordercompletedwhenpromised), 
         -(Q33_ContactedCustomerServiceduringtransaction:Q38_Numberofattemptstoresolvereasonforcall),
         -(Q46_Successfulinusingallpromotionaloffers:Q50_Satisfactionwithrewardsprogram), -staples_web_all_exp_journey_alt,
         -staples_web_all_exp_alt, -SurveyType, -Journey, -TransactionDateandTime, -TransactionAmount,
         -UniqueItemCount, -UniqueDepartmentCount, -TransactionTypeCode, -PurchaseMethod, -TransactionOrigin,
         -Q23_Reasonitemsdidnotmeetexpectations, -Q312_Otherissuetypetext, -staples_period_column_responses_export,
         -productcategory, -Q1_Purchasemethod, -Q3_OverallSatisfactionComment, -Q172_Deliveryontimeothertext,
         -Q18_Awareofdeliveryinmultipleshipments, -Q311_Issuetype, -Q32_Issueresolved, -(Q40_Returnmethod:Q44_Purchasepurpose)) %>%
  mutate(ResponseDateandTime = as.Date(as.character(ResponseDateandTime), format = "%m/%d/%Y"))

#join oms & wraplabel/fillkill to medallia
medallia.2 <- left_join(medallia.1, ord.method.FY16, by = c("OrderNumber" = "Ohord#")) %>%
  left_join(wlfk, by = c("OrderNumber" = "Ohord#")) %>%
  select( -Ohcrdt.y, -Ohcust, -Cccrdt) %>%
  rename(Ohcrdt = Ohcrdt.x) 

medallia.2[,43:56][is.na(medallia.2[,43:56])] <- 0 #enter zeros for missing csi vars & fillkill/wraplabel

medallia.3 <- medallia.2 %>%
  mutate(Q2_OverallSatisfaction = as.numeric(as.character(Q2_OverallSatisfaction)),
         Q4_EaseofTransaction = as.numeric(as.character(Q4_EaseofTransaction)),
         Q5_LikelihoodtoRecommend = as.numeric(as.character(Q5_LikelihoodtoRecommend)),
         Q6_OverallSatisfactionwithWebsite = as.numeric(as.character(Q6_OverallSatisfactionwithWebsite)),
         Q7_Easeoffindingproducts = as.numeric(as.character(Q7_Easeoffindingproducts)),
         Q8_Easeofnavigatingwebsite = as.numeric(as.character(Q8_Easeofnavigatingwebsite)),
         Q9_Easeofonlinecheckout = as.numeric(as.character(Q9_Easeofonlinecheckout)),
         Q10_Productselection = as.numeric(as.character(Q10_Productselection)),
         Q11_Clarityofproductdescriptions = as.numeric(as.character(Q11_Clarityofproductdescriptions)),
         Q12_Valueforpricepaid = as.numeric(as.character(Q12_Valueforpricepaid)),
         Q14_DeliveryDate = as.numeric(as.character(Q14_DeliveryDate)),
         Q19_Communicationoforderstatus = as.numeric(as.character(Q19_Communicationoforderstatus)),
         Q20_Helpfulnessofdriver = as.numeric(as.character(Q20_Helpfulnessofdriver)),
         Q21_Wayorderwaspackaged = as.numeric(as.character(Q21_Wayorderwaspackaged)),
         Q13_Firstchoiceavailable = as.factor(ifelse(Q13_Firstchoiceavailable == '', NA, as.character(Q13_Firstchoiceavailable))),
         Q15_Orderdeliveredinmultipleshipments = as.factor(ifelse(Q15_Orderdeliveredinmultipleshipments == '', NA, as.character(Q15_Orderdeliveredinmultipleshipments))),
         Q16_Entireorderreceived = as.factor(ifelse(Q16_Entireorderreceived == '', NA, as.character(Q16_Entireorderreceived))),
         Q171_Ontimedelivery = as.factor(ifelse(Q171_Ontimedelivery == '', NA, as.character(Q171_Ontimedelivery))),
         Q22_Itemsmetexpectations = as.factor(ifelse(Q22_Itemsmetexpectations == '', NA, as.character(Q22_Itemsmetexpectations))),
         Q29_Firstorderwithbrand = as.factor(ifelse( Q29_Firstorderwithbrand == '', NA, as.character( Q29_Firstorderwithbrand))),
         Q30_Experiencedissue = as.factor(ifelse(Q30_Experiencedissue == '', NA, as.character(Q30_Experiencedissue))),
         Q39_Madeaproductreturninthelastmonth = as.factor(ifelse(Q39_Madeaproductreturninthelastmonth == '', NA, as.character(Q39_Madeaproductreturninthelastmonth))),
         Q45_Flexpoint = as.factor(ifelse(Q45_Flexpoint == '', NA, as.character(Q45_Flexpoint))))

save(medallia.3, file = "FY16 Medallia Data plus OMS-CIS.Rdata")
rm(ord.method.FY16, wlfk)

# prep data for modeling --------------------------------------------------

model.prep <- medallia.3 %>%
  filter(!is.na(`Ohoto$`), !is.na(Q2_OverallSatisfaction)) %>%
  mutate(driver_interaction = ifelse(is.na(Q20_Helpfulnessofdriver),1,0),
         tenure_less1yr = ifelse(as.numeric(tenure_days) < 366, 1, 0),
         Q5_LikelihoodtoRecommend = ifelse(is.na(Q5_LikelihoodtoRecommend) & Q2_OverallSatisfaction == 1, 2.9,
                                            ifelse(is.na(Q5_LikelihoodtoRecommend) & Q2_OverallSatisfaction == 2, 4.9,
                                            ifelse(is.na(Q5_LikelihoodtoRecommend) & Q2_OverallSatisfaction == 3, 6.5,
                                            ifelse(is.na(Q5_LikelihoodtoRecommend) & Q2_OverallSatisfaction == 4, 8.7,
                                            ifelse(is.na(Q5_LikelihoodtoRecommend) & Q2_OverallSatisfaction == 5, 9.7,
                                            Q5_LikelihoodtoRecommend))))),
         Q14_DeliveryDate = ifelse(is.na(Q14_DeliveryDate) & Q19_Communicationoforderstatus == 1, 2.1,
                                           ifelse(is.na(Q14_DeliveryDate) & Q19_Communicationoforderstatus == 2, 2.7,
                                           ifelse(is.na(Q14_DeliveryDate) & Q19_Communicationoforderstatus == 3, 3.4,
                                           ifelse(is.na(Q14_DeliveryDate) & Q19_Communicationoforderstatus == 4, 4.2,
                                           ifelse(is.na(Q14_DeliveryDate) & Q19_Communicationoforderstatus == 5, 4.9, 
                                           Q14_DeliveryDate))))),
         Q19_Communicationoforderstatus = ifelse(is.na(Q14_DeliveryDate) & Q14_DeliveryDate == 1, 2.1,
                                   ifelse(is.na(Q19_Communicationoforderstatus) & Q14_DeliveryDate == 2, 2.9,
                                   ifelse(is.na(Q19_Communicationoforderstatus) & Q14_DeliveryDate == 3, 3.5,
                                   ifelse(is.na(Q19_Communicationoforderstatus) & Q14_DeliveryDate == 4, 4.1,
                                   ifelse(is.na(Q19_Communicationoforderstatus) & Q14_DeliveryDate == 5, 4.8, 
                                          Q19_Communicationoforderstatus))))),
         Q22_Itemsmetexpectations = as.factor(ifelse(is.na(Q22_Itemsmetexpectations) & Q5_LikelihoodtoRecommend >= 9, 'Yes',
                                           ifelse(is.na(Q22_Itemsmetexpectations) & Q5_LikelihoodtoRecommend <= 6, 'No',
                                                  as.character(Q22_Itemsmetexpectations)))),
         Q15_Orderdeliveredinmultipleshipments = as.factor(ifelse(is.na(Q15_Orderdeliveredinmultipleshipments), "I am not sure",
                                                                   as.character(Q15_Orderdeliveredinmultipleshipments))),
         Q171_Ontimedelivery = as.factor(ifelse(is.na(Q171_Ontimedelivery) & Q5_LikelihoodtoRecommend >= 9, 'Yes',
                                                ifelse(is.na(Q171_Ontimedelivery) & Q5_LikelihoodtoRecommend <= 6, 'No',
                                                as.character(Q171_Ontimedelivery)))),
         total_survey = 1) %>%
  select(-Q20_Helpfulnessofdriver, -tenure_days, -Q21_Wayorderwaspackaged)

model.prep.1 <- model.prep %>%
  filter(complete.cases(.)) %>%
  select(-Industry, -Manual_Order, -EDI_Order, -Internet_Order) %>%
  mutate_at(.vars = vars(Q2_OverallSatisfaction:Q14_DeliveryDate, -Q5_LikelihoodtoRecommend, -Q13_Firstchoiceavailable, Q19_Communicationoforderstatus),
            .funs = funs(ifelse(. >= 4.9, 1, 0))) %>%
  mutate_at(.vars = vars(Q13_Firstchoiceavailable, Q15_Orderdeliveredinmultipleshipments:Q171_Ontimedelivery, Q22_Itemsmetexpectations:Q45_Flexpoint),
            .funs = funs("_yes" = ifelse(grepl("^Yes",.),1,0))) %>%
  mutate(NPS = ifelse(Q5_LikelihoodtoRecommend >= 9, 100, ifelse(Q5_LikelihoodtoRecommend < 9 & Q5_LikelihoodtoRecommend > 6, 0, -100)),
         MidMarket = ifelse(segment == "Mid-Market", 1, 0),
         eDiversity_ord = ifelse(Label == "eDiversity",1,0),
         Linkplus_ord = ifelse(Label == "LINK+ ORDER METHOD",1,0),
         Mobile_ord = ifelse(Label == "MOBILE ORDER",1,0),
         Stapleslink.com = ifelse(Label == "STAPLESLINK.COM",1,0)) %>%
  select(-Q13_Firstchoiceavailable, -(Q15_Orderdeliveredinmultipleshipments:Q171_Ontimedelivery), -(Q22_Itemsmetexpectations:Q45_Flexpoint),
         -segment, -Label) 

  model.fc <- dcast(model.prep.1, SurveyID ~ fulfillmentcenter, value.var = "total_survey", fun.aggregate = max, fill = 0) %>%
    rename(Unknown_FC = Var.2)
  
  model.prep.2 <- inner_join(model.prep.1, model.fc, by = c("SurveyID")) %>%
    select(-(ResponseDateandTime:OrderNumber)) %>%
    distinct()

# roll up to daily --------------------------------------------------------

df.daily_pre1 <- model.prep.2 %>%
    select(Q2_OverallSatisfaction:Q19_Communicationoforderstatus, Ohcrdt, -Q5_LikelihoodtoRecommend,
           Q13_Firstchoiceavailable__yes:Q39_Madeaproductreturninthelastmonth__yes) %>%
    group_by(Ohcrdt) %>%
    summarise_all(funs("topbox" = mean(.)*100))
    
df.daily_pre2 <- model.prep.2 %>%
  group_by(Ohcrdt) %>%
  summarise(contacs_per_survey_rate = (sum(total_contact, na.rm = T)/sum(total_survey, na.rm = T)) * 100,
            total_survey = sum(total_survey, na.rm = T),
            `Ohoto$_avg` = mean(`Ohoto$`, na.rm = T),
            Ohoqty_avg = mean(Ohoqty, na.rm = T),
            Q5_LikelihoodtoRecommend = mean(Q5_LikelihoodtoRecommend, na.rm = T),
            NPS = mean(NPS, na.rm = T))

df.daily_pre3 <- model.prep.2 %>%
  select(Ohcrdt, stock:tenure_less1yr, MidMarket:Stockton) %>%
  group_by(Ohcrdt) %>%
  summarise_all(funs(mean(., na.rm = T)*100))

df.final <- inner_join(df.daily_pre2, df.daily_pre1, by = c("Ohcrdt")) %>%
  inner_join(df.daily_pre3, by = c("Ohcrdt")) 
names(df.final) <- gsub(" ",".", names(df.final))
names(df.final) <- gsub("\\$","", names(df.final))


# eda ---------------------------------------------------------------------

overall.cor <- cor(df.final[,-c(1)]) %>% as.data.frame() %>% select(Q6_OverallSatisfactionwithWebsite_topbox) %>% View

NPS.cor <- cor(df.final[,-c(1,6,8)]) %>% as.data.frame() %>% select(NPS) #remove LTR & CSAT
NPS.topvars.names <- names(which(apply(NPS.cor, 1, function(x) (x > .5 | x < -.20)))) 
NPS.top.vars <- df.final[,which(names(df.final) %in% NPS.topvars.names)] %>% pairs()

NPS.plot <- function(x_var){
  myplot <- ggplot(df.final,aes_string(x = x_var, y = "NPS")) + 
    geom_point() + 
    geom_smooth(method = "lm") +
    ggtitle(paste0("Scatterplot: ",x_var," vs NPS")) +
    theme_bw()
  print(myplot)
  }


NPS.rf <- randomForest::randomForest(NPS~.,df.final[,-c(1,5,7)], importance = T)
randomForest::varImpPlot(NPS.rf)

# split train/test --------------------------------------------------------

df.final.1 <- df.final %>% select(-Ohcrdt, -Q5_LikelihoodtoRecommend, -Q2_OverallSatisfaction_topbox) %>%
  mutate(Financial_binary = ifelse(Financial > 0, 1, 0)) %>%
  filter(total_survey > 4)
  
  
set.seed(105)
inTrain <- createDataPartition(df.final.1$NPS, p = .8, list = F)

d.train <- df.final.1[inTrain,]
d.test <- df.final.1[-inTrain,]

# linear regression model -------------------------------------------------

lm.model.1 <- lm(NPS~., df.final.1)
summary(lm.model.1)
plot(lm.model.1)
varImp(lm.model.1) %>% View

####
  
lm.model.2 <- lm(NPS~returns +
                   Financial +
                   Q6_OverallSatisfactionwithWebsite_topbox +
                   Q171_Ontimedelivery__yes_topbox +
                   Q22_Itemsmetexpectations__yes_topbox +
                   Q30_Experiencedissue__yes_topbox,
                 d.train) 
summary(lm.model.2)
vif(lm.model.2)
varImp(lm.model.2) %>% View

pred.lm2 <- predict(lm.model.2, d.test)
sqrt(mean((pred.lm2 - d.test$NPS)^2, na.rm = T)) #17.8
plot(d.test$NPS, pred.lm2)
abline(0,1)

#####

train_control <- trainControl(method="cv", number=10)
set.seed(125)
model.2 <- train(NPS~returns +
                   Financial +
                   Q6_OverallSatisfactionwithWebsite_topbox +
                   Q171_Ontimedelivery__yes_topbox +
                   Q22_Itemsmetexpectations__yes_topbox +
                   Q30_Experiencedissue__yes_topbox, df.final.1, trControl=train_control, method ="lm", metric = "Rsquared")
print(model.2)
summary(model.2)
plot(varImp(model.2))
QuantPsyc::lm.beta(model.2) %>% as.data.frame %>% t()

resid.vals <- resid(model.2)
plot(df.final.1$NPS, resid.vals)
abline(0,0)

pred.vals <- predict(model.2)
sqrt(mean((pred.vals - df.final.1$NPS)^2, na.rm = T)) #8.8
plot(df.final.1$NPS, pred.vals)
abline(0,1)

act_vs_pred <- as.data.frame(cbind(df.final.1$NPS, pred.vals))
ggplot(act_vs_pred, aes(df.final.1$NPS, pred.vals)) + 
  geom_point(colour = "darkorange") +
  geom_abline() +
  theme_bw() +
  ggtitle("Model Performance: Actuals vs Predicted") +
  xlab("Actual NPS") +
  ylab("Predicted NPS")

# IPA for Website CSAT ----------------------------------------------------

web.model <- lm(Q6_OverallSatisfactionwithWebsite_topbox~
                  Q19_Communicationoforderstatus_topbox +
                  Q9_Easeofonlinecheckout_topbox +
                  Q7_Easeoffindingproducts_topbox +
                  Q4_EaseofTransaction_topbox +
                  #Q8_Easeofnavigatingwebsite_topbox +
                  Q11_Clarityofproductdescriptions_topbox, data = d.train)
summary(web.model)
vif(web.model)
varImp(web.model)


web.pred <- predict(web.model, d.test)
sqrt(mean((web.pred - d.test$Q6_OverallSatisfactionwithWebsite_topbox)^2, na.rm = T)) #6.4
plot(d.test$Q6_OverallSatisfactionwithWebsite_topbox, web.pred)
abline(0,1)

std.coef <- QuantPsyc::lm.beta(web.model) %>% as.data.frame %>% t()
row.names(std.coef) <- "std_coef"


perf.means <- df.final.1 %>% 
  select(Q19_Communicationoforderstatus_topbox,
         Q9_Easeofonlinecheckout_topbox,
         Q7_Easeoffindingproducts_topbox,
         Q4_EaseofTransaction_topbox,
         #Q8_Easeofnavigatingwebsite_topbox,
         Q11_Clarityofproductdescriptions_topbox) %>%
  summarise_all(funs(mean(., na.rm=T)))
row.names(perf.means) <- "means"

ipa <- rbind(std.coef, perf.means) %>% t()

write.xlsx(ipa, "CSAT Web IPA.xlsx")
