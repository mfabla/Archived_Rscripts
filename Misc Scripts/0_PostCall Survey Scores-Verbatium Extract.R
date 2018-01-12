#### Pull Survey Response for Business (rev 2)

#by Mike Abla

#Notes: Adjusted rIA query_postcall function to query smryPostCallSurvey table instead and
#capture needed variables in order to produce monthly Difficult to Understand report

# set environment ---------------------------------------------------------

library(rIA)
set_env(clear_env = T, dir = "H:/Projects/MikeAbla/PostCall Surveys", extra_pkgs = c('xlsx'))


# revise post call survey function to different table ---------------------

query_postcall2 <- function(bu, fiscal_period){
  bu <- unlist(lapply(bu, function(x) switch(x, com = 1, sba = 2)))
  bu <- paste0("'",bu, "'", collapse = ",")
  
  fiscal_period <- paste0("'", fiscal_period, "'", collapse = ",")
  
  #if (!any(server %in% c("sqlServerUAT", "sqlServerProd"))) {
    #stop("use `sqlServerUAT` or `sqlServerProd` as server arg fool!", call. = F)
  #}
  
  conn <- ia_odbc("sqlServerUAT")
  cat("pulling query")
  
  post_call <- RODBC::sqlQuery(conn, paste0("
                                      SELECT a.IVRSurvey_ID
                                            ,a.Date_ID
                                            ,a.Calendar_Date
                                            ,b.fiscalperiodnamelong as fiscal_period
                                            ,a.NetworkName
                                            ,a.LocationName as location
                                            ,a.CompanyName as vendor
                                            ,a.As_Entered_Emp_ID
                                            ,a.AssociateHierarchy_ID
                                            ,a.Associate_ID
                                            ,a.AssocName
                                            ,a.Manager1Name
                                            ,a.Manager2Name
                                            ,a.Manager3Name
                                            ,a.Manager4Name
                                            ,a.Manager5Name
                                            ,a.Manager6Name
                                            ,a.Manager7Name
                                            ,a.FCRContact_ID
                                            ,a.QUESTION1_VALUE
                                            ,a.QUESTION2_VALUE
                                            ,a.QUESTION3_ID
                                            ,a.QUESTION3_OPTION_ID
                                            ,a.TranscriptionComments
                                            ,a.CustomerNumber
                                            ,a.OrderNumber
                                            ,a.AudioFile
                                            FROM NADCS_DW.dbo.smryPostCallSurvey a
                                            join nadcs_dw.dbo.dim_date b on (a.Date_ID=b.date_id)
                                            WHERE a.Network_ID in (",bu,") AND
                                            b.fiscalperiodnamelong in (",fiscal_period,")"), stringsAsFactors = F)
  
  RODBC::odbcClose(conn)
  cat("query complete")
  
  names(post_call) <- tolower(names(post_call))
  post_call$vendor <- tolower(post_call$vendor)
  post_call$location <- tolower(post_call$location)
  
  post_call
}


# data extract ------------------------------------------------------------

fperiods <- c('FY17 P11')
  
  #c('FY16 P03','FY16 P04','FY16 P05','FY16 P06','FY16 P07','FY16 P09', 'FY16 P10','FY16 P11','FY16 P12','FY17 P01','FY17 P02','FY17 P03','FY17 P04', 'FY17 P05')

com.postcall.surveys <- query_postcall2(bu=c('com'),fiscal_period =  fperiods)
sba.postcall.surveys <- query_postcall2(bu=c('sba'),fiscal_period =  fperiods)
postcall.surveys <- rbind(com.postcall.surveys, sba.postcall.surveys)
#save(postcall.surveys, file = "sba_com_postcall_surveys.Rdata")

# associate data --------------------------------------------------------------

survey.associates <- postcall.surveys %>%
  rename(bu = networkname, Q1 = question1_value, Q2 = question2_value, comments = transcriptioncomments) %>%
  select(-date_id, -c(question3_id:question3_option_id)) %>%
  mutate(total_surveys = 1, fiscal_period = as.factor(fiscal_period), location = as.factor(location), vendor = as.factor(vendor), bu = as.factor(bu)) %>%
  mutate(Q1 = ifelse(is.na(Q1), 9, Q1),
         Q2 = ifelse(is.na(Q2), 9, Q2),
         total_comments = ifelse(!is.na(comments),1,0),
         comments = tolower(comments)) %>%
  filter(Q1 != 9 & Q2 != 9, !is.na(associate_id) | !is.na(assocname)) %>%
  mutate(Q1.TopBox = ifelse(Q1 == 5, 1, 0),
         Q2.TopBox = ifelse(Q2 == 5, 1, 0),
         Q1.Top2Box = ifelse(Q1 %in% (4:5), 1, 0),
         Q2.Top2Box = ifelse(Q2 %in% (4:5), 1, 0)) 

diff.terms <- c('accent','america','dialect','english','enunciate','french','indian','language','outsourc','philippine','spanish','understand english', 'offshore')
survey.associates$diffunderstand <- ifelse(str_detect(survey.associates$comments, paste(diff.terms, collapse = '|')),1,0)

load(file = "sba_com_postcall_surveys_assoc.Rdata") #new.data.assoc 
old.data.assoc <- new.data.assoc
new.data.assoc <- rbind(old.data.assoc, survey.associates)
save(new.data.assoc, file = "sba_com_postcall_surveys_assoc.Rdata")

com.survey.assoc <- survey.associates %>% filter(bu == '.com', diffunderstand== 1) %>% write.xlsx(file = "com_postcall_survey_assoc.xlsx", sheetName = "com PostCall", row.names = F)
sba.survey.assoc <- survey.associates %>% filter(bu == 'SAUS', diffunderstand== 1) %>% write.xlsx(file = "sba_postcall_survey_assoc.xlsx", sheetName = "sba PostCall", row.names = F)


# vendor and location summary ---------------------------------------------

#bu
survey.bu <- survey.associates %>%
  filter(!vendor %in% (c('infosys', 'no eid match'))) %>%
  mutate(business.type = ifelse(bu == 'SAUS', 'SBA Overall',
                         ifelse(bu == '.com', 'COM Overall', 'Other')))%>%
  group_by(fiscal_period, business.type) %>%
  summarise(total_surveys = sum(total_surveys, na.rm = T),
            Q1_TopBox = mean(Q1.TopBox, na.rm = T),
            Q1_Top2Box = mean(Q1.Top2Box, na.rm = T),
            Q2_TopBox = mean(Q2.TopBox, na.rm = T),
            Q2_Top2Box = mean(Q2.Top2Box, na.rm = T),
            Q1_Avg = mean(Q1, na.rm = T),
            Q2_Avg = mean(Q2, na.rm = T),
            total_comments = sum(total_comments, na.rm = T),
            Diff.Understand.rate = sum(diffunderstand, na.rm = T)/total_comments,
            Q1_TopBox.rate.du = mean(Q1.TopBox[diffunderstand == 1], na.rm = T),
            Q1_Top2Box.rate.du = mean(Q1.Top2Box[diffunderstand == 1], na.rm = T),
            Q2_TopBox.rate.du = mean(Q2.TopBox[diffunderstand == 1], na.rm = T),
            Q2_Top2Box.rate.du = mean(Q2.Top2Box[diffunderstand == 1], na.rm = T),
            Q1_Avg.du = mean(Q1[diffunderstand == 1], na.rm = T),
            Q2_Avg.du = mean(Q2[diffunderstand == 1], na.rm = T)) %>%
  select (fiscal_period:total_surveys, total_comments, Diff.Understand.rate, 
          Q1_TopBox, Q1_Top2Box, Q1_TopBox.rate.du, Q1_Top2Box.rate.du, Q1_Avg, Q1_Avg.du,
          Q2_TopBox, Q2_Top2Box, Q2_TopBox.rate.du, Q2_Top2Box.rate.du, Q2_Avg, Q2_Avg.du)

#vendor
survey.vendor <- survey.associates %>%
  filter(!vendor %in% (c('infosys', 'no eid match'))) %>%
  mutate(business.type = ifelse(bu == 'SAUS' & vendor == 'arise', 'SBA Arise',
                         ifelse(bu == 'SAUS' & vendor == 'sitel', 'SBA Sitel',
                         ifelse(bu == 'SAUS' & vendor == 'staples', 'SBA Staples',
                         ifelse(bu == '.com' & vendor == 'arise', 'COM Arise',
                         ifelse(bu == '.com' & vendor == 'sitel', 'COM Sitel',
                         ifelse(bu == '.com' & vendor == 'staples', 'COM Staples', 'Other Vendor'))))))) %>%
  filter(business.type != 'Other Vendor') %>%
  group_by(fiscal_period, business.type) %>%
  summarise(total_surveys = sum(total_surveys, na.rm = T),
            Q1_TopBox = mean(Q1.TopBox, na.rm = T),
            Q1_Top2Box = mean(Q1.Top2Box, na.rm = T),
            Q2_TopBox = mean(Q2.TopBox, na.rm = T),
            Q2_Top2Box = mean(Q2.Top2Box, na.rm = T),
            Q1_Avg = mean(Q1, na.rm = T),
            Q2_Avg = mean(Q2, na.rm = T),
            total_comments = sum(total_comments, na.rm = T),
            Diff.Understand.rate = sum(diffunderstand, na.rm = T)/total_comments,
            Q1_TopBox.rate.du = mean(Q1.TopBox[diffunderstand == 1], na.rm = T),
            Q1_Top2Box.rate.du = mean(Q1.Top2Box[diffunderstand == 1], na.rm = T),
            Q2_TopBox.rate.du = mean(Q2.TopBox[diffunderstand == 1], na.rm = T),
            Q2_Top2Box.rate.du = mean(Q2.Top2Box[diffunderstand == 1], na.rm = T),
            Q1_Avg.du = mean(Q1[diffunderstand == 1], na.rm = T),
            Q2_Avg.du = mean(Q2[diffunderstand == 1], na.rm = T)) %>%
  select (fiscal_period:total_surveys, total_comments, Diff.Understand.rate, 
          Q1_TopBox, Q1_Top2Box, Q1_TopBox.rate.du, Q1_Top2Box.rate.du, Q1_Avg, Q1_Avg.du,
          Q2_TopBox, Q2_Top2Box, Q2_TopBox.rate.du, Q2_Top2Box.rate.du, Q2_Avg, Q2_Avg.du)

#location
survey.location <- survey.associates %>%
  filter(!vendor %in% (c('infosys', 'no eid match'))) %>%
  mutate(business.type = ifelse(bu == 'SAUS' & location == 'sitel-tarlac', 'SBA Sitel Tarlac',
                         ifelse(bu == 'SAUS' & location == 'sitel-eton', 'SBA Sitel Eton',
                         ifelse(bu == '.com' & location == 'sitel-tarlac', 'COM Sitel Tarlac',
                         ifelse(bu == '.com' & location == 'sitel-eton', 'COM Sitel Eton', 'Other location'))))) %>%
  filter(business.type != 'Other location') %>%
  group_by(fiscal_period, business.type) %>%
  summarise(total_surveys = sum(total_surveys, na.rm = T),
            Q1_TopBox = mean(Q1.TopBox, na.rm = T),
            Q1_Top2Box = mean(Q1.Top2Box, na.rm = T),
            Q2_TopBox = mean(Q2.TopBox, na.rm = T),
            Q2_Top2Box = mean(Q2.Top2Box, na.rm = T),
            Q1_Avg = mean(Q1, na.rm = T),
            Q2_Avg = mean(Q2, na.rm = T),
            total_comments = sum(total_comments, na.rm = T),
            Diff.Understand.rate = sum(diffunderstand, na.rm = T)/total_comments,
            Q1_TopBox.rate.du = mean(Q1.TopBox[diffunderstand == 1], na.rm = T),
            Q1_Top2Box.rate.du = mean(Q1.Top2Box[diffunderstand == 1], na.rm = T),
            Q2_TopBox.rate.du = mean(Q2.TopBox[diffunderstand == 1], na.rm = T),
            Q2_Top2Box.rate.du = mean(Q2.Top2Box[diffunderstand == 1], na.rm = T),
            Q1_Avg.du = mean(Q1[diffunderstand == 1], na.rm = T),
            Q2_Avg.du = mean(Q2[diffunderstand == 1], na.rm = T)) %>%
  select (fiscal_period:total_surveys, total_comments, Diff.Understand.rate, 
          Q1_TopBox, Q1_Top2Box, Q1_TopBox.rate.du, Q1_Top2Box.rate.du, Q1_Avg, Q1_Avg.du,
          Q2_TopBox, Q2_Top2Box, Q2_TopBox.rate.du, Q2_Top2Box.rate.du, Q2_Avg, Q2_Avg.du)

#combine all
survey.final <- rbind(survey.bu, survey.vendor, survey.location) %>%
  arrange(fiscal_period, business.type) %>% as.data.frame()

#write.xlsx(survey.final, file = 'postcall_survey_summary.xlsx', row.names = F)
old.data.summary <- read.xlsx( 'postcall_survey_summary.xlsx', sheetName = 'Sheet1', stringsAsFactors = F)
new.data.summary <- rbind(old.data.summary, survey.final) %>% arrange(business.type, fiscal_period)
write.xlsx(new.data.summary, file = 'postcall_survey_summary.xlsx', row.names = F )


# line charts -------------------------------------------------------------

fp <- tail(unique(new.data.summary$fiscal_period),12)

survey.final.1 <- new.data.summary %>% 
  filter(fiscal_period %in% fp) %>%
  rename(`Q1 Top Box` = Q1_TopBox, `Q2 Top Box` = Q2_TopBox, 
         `Q1 Top Box - Diff Understand` = Q1_TopBox.rate.du, `Q2 Top Box - Diff Understand` = Q2_TopBox.rate.du )

topbox.query <-  function(df){
  x_bu <- unique(df$business.type)

  for (i in x_bu){
   plots <- ggplot(data = df[df$business.type == i,], aes(x =fiscal_period)) + theme_bw() +
     geom_line(aes(y = `Q1 Top Box`, group = 1, color = "Q1 Top Box") , size = 1.05) +
     geom_line(aes(y = `Q2 Top Box`, group = 1, color = "Q2 Top Box"), size = 1.05) +
     geom_line(aes(y = `Q1 Top Box - Diff Understand`, group = 1, color = "Q1 Top Box - Diff Understand"), linetype = "dashed", size = 1.05) +
     geom_line(aes(y = `Q2 Top Box - Diff Understand`, group = 1, color = "Q2 Top Box - Diff Understand"), linetype = "dashed", size = 1.05) +
     geom_text(aes(y = `Q1 Top Box`,label = paste0(round(`Q1 Top Box`*100,1),"%")), nudge_y = .03, colour = "darkblue") +
     geom_text(aes(y = `Q2 Top Box`,label = paste0(round(`Q2 Top Box`*100,1),"%")), nudge_y = .03, colour = "darkorange2") +
     geom_text(aes(y = `Q1 Top Box - Diff Understand`,label = paste0(round(`Q1 Top Box - Diff Understand`*100,1),"%")), nudge_y = .03, colour = "blue") +
     geom_text(aes(y = `Q2 Top Box - Diff Understand`,label = paste0(round(`Q2 Top Box - Diff Understand`*100,1),"%")), nudge_y = .03, colour = "orange") +
     scale_colour_manual(values = c("darkblue", "blue", "darkorange2", "orange")) +
     scale_y_continuous(name = "Top Box %", labels = percent, limits = c(0,1)) + xlab("Fiscal Period") + 
     ggtitle(paste0("",i,": Top Box Scores with a Subset of Difficulty Understanding")) +
     theme(legend.position="bottom", legend.direction ="horizontal", legend.title = element_blank()) 

   print(plots)
   ggsave(plots, width = 8, height = 4.5, units = "in",
          filename = paste("",i,"_chart.png", sep = ""))
  }
}

diffunderstand.query <- function(df){
  x_bu <- unique(df$business.type)
  
  for(i in x_bu){
  plots <- ggplot(data = df[df$business.type == i,], aes(x = fiscal_period, y = Diff.Understand.rate, group = 1,
                                                         label = paste0(round(Diff.Understand.rate*100,1),"%"))) +
    theme_bw() +
    geom_line(color = "darkblue", size = 1.05) +
    scale_y_continuous(name = "Difficulty Understanding (%)", limits = c(0,.25), labels = percent ) + xlab("Fiscal Period") + 
    ggtitle(paste0("",i,": Percentage of Customers Reporting Difficulty Understanding")) +
    geom_point() +
    geom_text(size = 4, nudge_y = 0.01)
  
  print(plots)
  ggsave(plots, width = 8, height = 4.5, units = "in",
         filename = paste("",i,"_DiffUnderstand_chart.png", sep = ""))
  }
}


topbox.query(survey.final.1)
diffunderstand.query(survey.final.1)

