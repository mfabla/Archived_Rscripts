############### NPS/LTR Key Drivers ##################

#by Mike Abla

#Notes: Data set will be based on FY16 SBA Data


# set up env --------------------------------------------------------------

library(rIA)
set_env(clear_env = T, dir = "H:/Projects/MikeAbla/NPS-LTR Drivers", extra_pkgs = c('xlsx', 'caret', 'car'))

load("~/Projects/MikeAbla/Customer Detractor Study/customer detractor base data.rdata") #load file that has medallia data
rm("MidMarkets", "ordermethod", "ordermethod2", "return.orders", "sba.csi.oms", "sba.oms", "sba.cust.type", "sba_custs", "ted", "wlfk")

load("~/Projects/MikeAbla/zz_tmp Order Method/Order Method FY16 Data.Rdata") #load perivious dataset run for order method analysis
ord.method.FY16 <- ord.method.FY16[,-c(31:33)]

load("~/Projects/MikeAbla/Customer Detractor Study/SBA FY16-FY17 Q1_v2.Rdata")

# clean up data -----------------------------------------------------------

#churned custs
churn <- FY16_FY17.df %>%
  filter(Ohoto_2016 > 0) %>%
  mutate(churn = as.factor(ifelse(Ohoto_2017 == 0, 1, 0))) %>%
  filter(churn == '1') %>%
  select(CCCUST, churn)


#select relevant medallia fields 
medallia.1 <- medallia %>% 
  select(SurveyID, ResponseDateandTime, OrderNumber, Q2_OverallSatisfaction, Q5_LikelihoodtoRecommend) %>%
  mutate(ResponseDateandTime = as.Date(as.character(ResponseDateandTime), format = "%m/%d/%Y"))

#join oms to medallia
medallia.2 <- left_join(select(.data = ord.method.FY16, Ohcust, segment,`Ohord#`, Ohcrdt,`Ohoto$`), medallia.1, by = c("Ohord#" = "OrderNumber")) 

medallia.mm <- medallia.2 %>%
  mutate(Q2_OverallSatisfaction = as.numeric(as.character(Q2_OverallSatisfaction)),
         Q5_LikelihoodtoRecommend = as.numeric(as.character(Q5_LikelihoodtoRecommend))) %>%
  filter(segment == 'Mid-Market')
 
rm(ord.method.FY16,medallia.2, medallia.1, medallia)

#identify which customers had responsed to survey
medallia.mm1 <- medallia.mm %>%
  filter(`Ohoto$` != 0) %>% #noticing many orders with $0, it appears some are due to survey incentives possibly
  mutate(SurveyID = ifelse(is.na(SurveyID), 0, SurveyID)) %>%
  group_by(Ohcust) %>%
  filter(max(SurveyID) > 0) %>%
  arrange(Ohcust, desc(Ohcrdt))

medallia.mm2 <- medallia.mm1 %>%
  mutate(next_ord_days = as.numeric(difftime(lag(Ohcrdt, 1), Ohcrdt, units = "days")),
         next_Ohoto = lag(`Ohoto$`, 1)) %>%
  filter(SurveyID != 0) %>%
  ungroup()

#calculate average LTR scores for CSAT scale for imputation purposes
tapply(medallia.mm2$Q5_LikelihoodtoRecommend, medallia.mm2$Q2_OverallSatisfaction, mean, na.rm = T)
#       1        2        3        4        5 
#2.787479 4.785586 6.401823 8.700592 9.694363 

#calculate general time in days until next order
quantile(medallia.mm2$next_ord_days, probs = c(0.25, 0.50, 0.75, 0.85, 0.90, 0.95), na.rm = T)
#25% 50% 75% 85% 90% 95% 
#  1   5  11  17  22  35 

summary(medallia.mm2[medallia.mm2$Ohcrdt <= '2016-07-30', c("next_ord_days")]) #only 37 records show no future purchases after 1H
summary(medallia.mm2[medallia.mm2$Ohcrdt <= '2016-12-31', c("next_ord_days")])

#enter imputations and identify churn custs
medallia.mm3 <- medallia.mm2 %>%
  mutate(Q5_LikelihoodtoRecommend = ifelse(is.na(Q5_LikelihoodtoRecommend) & Q2_OverallSatisfaction == 1, 2.8,
                                           ifelse(is.na(Q5_LikelihoodtoRecommend) & Q2_OverallSatisfaction == 2, 4.8,
                                                  ifelse(is.na(Q5_LikelihoodtoRecommend) & Q2_OverallSatisfaction == 3, 6.4,
                                                         ifelse(is.na(Q5_LikelihoodtoRecommend) & Q2_OverallSatisfaction == 4, 8.7,
                                                                ifelse(is.na(Q5_LikelihoodtoRecommend) & Q2_OverallSatisfaction == 5, 9.7,
                                                                       Q5_LikelihoodtoRecommend)))))) %>%
  left_join(churn, by = c("Ohcust" = "CCCUST")) %>%
  mutate(next_Ohoto = ifelse(is.na(next_Ohoto) & churn == '1', 0, next_Ohoto)) %>% #identify those who have churned and enter $0 for their next purchase
  filter(!is.na(next_Ohoto)) %>%
  mutate(spend_diff = next_Ohoto - `Ohoto$`,
         NPS = ifelse(Q5_LikelihoodtoRecommend >= 9, 100, ifelse(Q5_LikelihoodtoRecommend < 9 & Q5_LikelihoodtoRecommend > 6, 0, -100)))

# eda ---------------------------------------------------------------------

summary(medallia.mm3)

plot(medallia.mm3$Q5_LikelihoodtoRecommend, medallia.mm3$spend_diff)
cor.test(medallia.mm3$Q5_LikelihoodtoRecommend, medallia.mm3$spend_diff) # cor = 0.028 p-value = 0.0004493

boxplot(spend_diff~NPS, data = medallia.mm3, ylim = c(-500,500))
tapply(medallia.mm3$spend_diff, medallia.mm3$NPS, mean, na.rm = T)
#-100         0       100 
#-85.81388 -73.42310 -55.88379 
tapply(medallia.mm3$spend_diff, medallia.mm3$NPS, median, na.rm = T)
#   -100       0     100 
#-54.085 -58.350 -46.410 

#test.lm <- lm(spend_diff~Q5_LikelihoodtoRecommend, data = medallia.mm3)
test.lm <- lm(spend_diff~as.factor(NPS), data = medallia.mm3)
anova(test.lm)

#Analysis of Variance Table

#Response: spend_diff
#Df     Sum Sq Mean Sq F value  Pr(>F)  
#as.factor(NPS)     2    1779089  889545  4.3395 0.01306 *
#  Residuals      15859 3250906240  204988    

summary(test.lm)

#Call:
#  lm(formula = spend_diff ~ as.factor(NPS), data = medallia.mm3)

#Residuals:
#  Min       1Q   Median       3Q      Max 
#-14493.5   -110.6     13.2    115.0  11839.6 

#Coefficients:
#                     Estimate  Std. Error t value            Pr(>|t|)    
#  (Intercept)         -85.81      10.48  -8.192 0.000000000000000277 ***
#  as.factor(NPS)0      12.39      14.37   0.862              0.38853    
#  as.factor(NPS)100    29.93      11.27   2.656              0.00792 ** 
#  ---
#  Signif. codes:  0 '***' 0.001 '**' 0.01 '*' 0.05 '.' 0.1 ' ' 1

#Residual standard error: 452.8 on 15859 degrees of freedom
#(86 observations deleted due to missingness)
#Multiple R-squared:  0.000547,	Adjusted R-squared:  0.0004209 
#F-statistic: 4.339 on 2 and 15859 DF,  p-value: 0.01306

# split train/test --------------------------------------------------------

df.final <- medallia.mm3 %>% select(spend_diff, Q5_LikelihoodtoRecommend)  %>% filter(!is.na(Q5_LikelihoodtoRecommend))
  
set.seed(105)
inTrain <- createDataPartition(df.final$spend_diff, p = .7, list = F)

d.train <- df.final[inTrain,]
d.test <- df.final[-inTrain,]

# linear regression model -------------------------------------------------

trainControl <- trainControl(method = "cv", number = 10)
lm.model <- train(spend_diff~Q5_LikelihoodtoRecommend, data = df.final, trControl = trainControl, method = "lm")
print(lm.model)
summary(lm.model)

#Call:
#  lm(formula = .outcome ~ ., data = dat)

#Residuals:
#  Min       1Q   Median       3Q      Max 
#-14488.9   -110.4     12.8    114.9  11832.5 

#Coefficients:
#  Estimate Std. Error t value           Pr(>|t|)    
#(Intercept)              -114.244     14.883  -7.676 0.0000000000000173 ***
#  Q5_LikelihoodtoRecommend    5.983      1.646   3.635           0.000279 ***
#  ---
#  Signif. codes:  0 '***' 0.001 '**' 0.01 '*' 0.05 '.' 0.1 ' ' 1

#Residual standard error: 452.7 on 15860 degrees of freedom
#Multiple R-squared:  0.0008323,	Adjusted R-squared:  0.0007693 
#F-statistic: 13.21 on 1 and 15860 DF,  p-value: 0.0002792


