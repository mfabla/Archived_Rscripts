############# Weekly Script to Update CSI Contact Forecasts ################

#by Mike Abla

#This is the weekly script to run and update the placeorder_forecast table in dev


# set env -----------------------------------------------------------------

library(rIA)
set_env(clear_env = T,  dir = "~/VM-jobs/R", extra_pkgs = c('mailR'))

# weekly run --------------------------------------------------------------

csicontact_actuals <- function(){

#previous week
prev_wk <- calendar %>%
  filter(calendar_date == Sys.Date() - 7) %>%
  select(fiscal_week_of_period_long, fiscal_week_start_date, fiscal_week_end_date)

#date range
start_date <- as.character(prev_wk$fiscal_week_start_date)
end_date <- as.character(prev_wk$fiscal_week_end_date)
fw <- prev_wk$fiscal_week_of_period_long

#check if update is needed
cat("\n running report checker...")
connDEV <- odbcConnect('cs-db-dev')
last_update <- sqlQuery(connDEV, paste0("Select distinct fiscal_week FROM [CSAQuery].[dbo].[csi_contact_forecast] Where adj_contacts is NULL"), stringsAsFactors = F)
odbcClose(connDEV)

if (any(!fw %in% last_update$fiscal_week)) {
  return(cat("\n already up-to-date"))} else{

#extract from csi-daily
cat("\n querying csi data...")
connDEV <- odbcConnect('cs-db-dev')
csi_extract <- sqlQuery(connDEV, paste0("SELECT [business_unit],[fiscal_week],[topic], sum([adjusted_contact_volume]) as adj_contacts
                                  FROM [CSAQuery].[dbo].[csi_daily]
                                  WHERE business_unit in ('sba', 'dotcom') AND 
                                      calendar_date between '",start_date,"' and '", end_date,"'
                                  Group by [business_unit],[fiscal_week],[topic]
                                  ORDER BY [business_unit],[fiscal_week],[topic]"), stringsAsFactors = F) %>%
  dplyr::group_by(business_unit, fiscal_week, topic) %>% 
  dplyr::summarise(adj_contacts=sum(adj_contacts,na.rm = T)) %>% 
  ungroup() %>%
  dplyr::mutate(businessunit = ifelse(business_unit == 'dotcom', 'Staples.com', 'SBA'),
                fiscal_week = gsub("FY", "FY20", fiscal_week)) %>%
  dplyr::select(-business_unit) 
odbcClose(connDEV)

#update dev table
cat("\n updating dev table...")
connDEV <- odbcConnect('cs-db-dev')
week_update <- sqlQuery(connDEV, paste0("select * From [CSAQuery].[dbo].[csi_contact_forecast] where fiscal_week = '",fw,"'"), stringsAsFactors = F)
week_update <- week_update %>% left_join(csi_extract, by = c("fiscal_week", "businessunit", "variable" = "topic"), copy = T) %>%
  dplyr::mutate(adj_contacts = adj_contacts.y) %>% select(-adj_contacts.x, -adj_contacts.y)
sqlUpdate(connDEV, week_update, tablename = "csi_contact_forecast", index = c("ID"), verbose = F, test = F)
odbcClose(connDEV)

#check if projections exceed actuals for the week by +/- 10K
cat("\n checking to see if projections exceeded actuals by +/- 10k")
week_update1 <- week_update %>% 
  dplyr::group_by(businessunit, fiscal_week) %>% 
  dplyr::summarise(adj_contacts = sum(adj_contacts, na.rm = T), adj_contact_proj = sum(adj_contact_proj, na.rm = T)) %>%
  dplyr::mutate(diff = adj_contact_proj - adj_contacts)

#email notifications

from1 <- "michael.abla@Staples.com"
to1 <- c("michael.abla@Staples.com")
carbon1 <- c("michael.abla@Staples.com", "Richard.Ostberg@Staples.com", "Domingo.Aguado@Staples.com", "Cheryl.Mervich@Staples.com")
password <- 'MGMA2525!'

if (any(abs(week_update1$diff) > 10000)) {
  cat("\n preparing email alert!...")
  subject1 <- paste0("ALERT! CSI Contact Forecast Miss  - ", fw,"")
  body1 <- paste0("CSI contact forecast missed by more than -/+ 10K for ", fw,". See dashboard for further details to verify anything unusual occured to business: <br><br>
                    <b> ",fw," CSI Contact Volume: </b><br><br>
                    <table style='border:1px solid black;border-collapse:collapse;'>
                        <tr>
                          <th>Business</th>
                          <th>Actuals</th>
                          <th>Forecast</th>
                          <th>Difference</th>
                        </tr>
                        <tr>
                          <td>Staples.com</td>
                          <td>",week_update1[week_update1$businessunit == 'Staples.com', 'adj_contacts'],"</td>
                          <td>",round(week_update1[week_update1$businessunit == 'Staples.com', 'adj_contact_proj'],0),"</td>
                          <td>",round(week_update1[week_update1$businessunit == 'Staples.com', 'diff'],0),"</td>
                        </tr>
                        <tr>
                          <td>SBA</td>
                          <td>",week_update1[week_update1$businessunit == 'SBA', 'adj_contacts'],"</td>
                          <td>",round(week_update1[week_update1$businessunit == 'SBA', 'adj_contact_proj'],0),"</td>
                          <td>",round(week_update1[week_update1$businessunit == 'SBA', 'diff'],),"</td>
                        </tr></table><br><br>

                    <a href='https://davinci-dev.staples.com/#/site/CSI/workbooks/3649/views'>Visit Dashboard for further details</a>") 
  
  send.mail(from = from1,
            to = to1,
            cc = carbon1,
            subject = subject1,
            html = TRUE,
            inline = TRUE,
            body = body1,
            authenticate = TRUE,
            smtp = list(host.name = "smtp.office365.com", port = 587, user.name = "michael.abla@Staples.com", passwd = password, tls = TRUE))
  cat("\n all done!")
  
  } else {
    cat("\n preparing email to myself...")
    subject2 <- paste0("CSI Contacts Update - ", fw,"")
    body2 <- paste0("CSI Contacts are now updated for ", fw,". See weekly results below: <br><br>
                  <b> ",fw," CSI Contact Volume: </b><br><br>
                    <table style='border:1px solid black;border-collapse:collapse;'>
                        <tr>
                          <th>Business</th>
                          <th>Actuals</th>
                          <th>Forecast</th>
                          <th>Difference</th>
                        </tr>
                        <tr>
                          <td>Staples.com</td>
                          <td>",week_update1[week_update1$businessunit == 'Staples.com', 'adj_contacts'],"</td>
                          <td>",round(week_update1[week_update1$businessunit == 'Staples.com', 'adj_contact_proj'],0),"</td>
                          <td>",round(week_update1[week_update1$businessunit == 'Staples.com', 'diff'],0),"</td>
                        </tr>
                        <tr>
                          <td>SBA</td>
                          <td>",week_update1[week_update1$businessunit == 'SBA', 'adj_contacts'],"</td>
                          <td>",round(week_update1[week_update1$businessunit == 'SBA', 'adj_contact_proj'],0),"</td>
                          <td>",round(week_update1[week_update1$businessunit == 'SBA', 'diff'],),"</td>
                        </tr></table><br><br>

                    <a href='https://davinci-dev.staples.com/#/site/CSI/workbooks/3649/views'>Visit Dashboard for further details</a>") 
    
    send.mail(from = from1,
              to = to1,
              cc = carbon1,
              subject = subject2,
              html = TRUE,
              inline = TRUE,
              body = body2,
              authenticate = TRUE,
              smtp = list(host.name = "smtp.office365.com", port = 587, user.name = "michael.abla@Staples.com", passwd = password, tls = TRUE))
    cat("\n all done!")
    }
  }
}

update_actuals <- csicontact_actuals()

odbcCloseAll()