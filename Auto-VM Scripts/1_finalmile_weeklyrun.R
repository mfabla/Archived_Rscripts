############# Weekly Script to Update Dev Final Mile Table  ################

#by Mike Abla

#This is the weekly script to run and update the final_mile table in dev
#Suggested to run mid-week (wednesday) to capture any lag contacts from prev week's orders


# set env -----------------------------------------------------------------

library(rIA)
set_env(clear_env = T, dir = "~/VM-jobs/R")


# run function ------------------------------------------------------------


final_mile_wk_report <- function(){
  
#previous week
prev_fw <- calendar %>%
  filter(calendar_date == Sys.Date()- 7) %>%
  select(fiscal_week_of_period_long, fiscal_week_of_period_name, fiscal_week_start_date, fiscal_week_end_date)

#gdw - convert to gdw date format
start_gdw <- as.numeric(gsub("-", "", as.character(prev_fw$fiscal_week_start_date)))
end_gdw <- as.numeric(gsub("-", "", as.character(prev_fw$fiscal_week_end_date)))

#mssql dates
start_sql <- prev_fw$fiscal_week_start_date
end_sql <- prev_fw$fiscal_week_end_date + 3 #add a few days to capture any lag contacts from prev wk orders

#fiscal wk for report
fw <- prev_fw$fiscal_week_of_period_long


# check if update is needed -----------------------------------------------

cat("\n running report checker...")
connDev <- odbcConnect('cs-db-dev')
last_update <- sqlQuery(connDev, paste0("SELECT Distinct [FiscalWeek] as fw_weeks FROM [CSAQuery].[dbo].[sba_final_mile]"), stringsAsFactors = F)
odbcClose(connDev)

if(any(fw %in% last_update$fw_weeks)){
  return(cat("\n report is already up-to-date"))} else{

# extract order data ------------------------------------------------------
cat("\n querying order data...")
connGDW <- ia_odbc("gdw")
sba_orders <- sqlQuery(connGDW, paste0("SELECT DISTINCT 
                                    AL1.ORSHDT, 
                                    AL1.ORORD#, 
                                    AL1.ORSHP#, 
                                    AL1.ORPSTA, 
                                    AL1.ORHLOC, 
                                    AL1.ORCARR, 
                                    AL1.OR#CTN 
                                FROM PRD_USD_OPV.SUNRISE_SFBASLIB_SFORDR_V AL1, 
                                PRD_USD_OPV.SUNRISE_SFBASLIB_SFORDH_V AL2
                                WHERE 
                                    AL1.ORORD#=AL2.OHORD# AND 
                                    AL2.OHLINK=AL1.ORLINK AND
                                    AL1.ORLINK=0 AND
                                    AL2.OHCRDT BETWEEN '",start_gdw,"' and '",end_gdw,"' AND
                                    AL2.OHTYPE='OR' AND
                                    AL1.ORSHDT <> 0 AND
                                    AL1.ORCARR <> 'DR*'"), stringsAsFactors = F)
odbcClose(connGDW)
sba_shipments <- sba_orders %>% group_by(`Orord#`) %>% summarise(shipment_number = n())
sba_orders1 <- inner_join(sba_orders, sba_shipments, by = c("Orord#"))

# extract csi data --------------------------------------------------------
cat("\n querying csi data...")
connProd <- ia_odbc("sqlServerProd") 
sba_wims <- sqlQuery(connProd, paste0("SELECT [FCR_ID]
                                         ,[CONTACT_METHOD]
                                         ,[FCR_DATE]
                                         ,[CSI_BU_NAME]
                                         ,[TIER1TEXT]
                                         ,[TIER2TEXT]
                                         ,[Topic]
                                         ,[Type]
                                         ,[ORDERNUMBER]
                                     FROM [NADCS_DW].[dbo].[smryCSIReporting]
                                     Where 1=1 
                                         AND [CSI_BU_NAME] = 'SA US'
                                         AND [Topic] = 'WiMS'
                                         AND ([Type] IN ('Damaged/Missing', 'POD Dispute', 'Cargo Loss', 'Missing Order', 'Misdelivery', 'ETA')
                                         OR ([TIER1TEXT] = 'Notified another group(Log or Alert)' AND [TIER2TEXT] = 'Order delayed')
                                         OR ([TIER1TEXT] = 'Reship missing order/container/box' AND [TIER2TEXT] = 'Customer is missing order/container/box'))
                                         AND [FCR_DATE] between '",start_sql,"' and '",end_sql,"' 
                                         AND [ORDERNUMBER] IS NOT NULL"), stringsAsFactors = F)
odbcClose(connProd)
sba_totalcontacts <- sba_wims %>% group_by(ORDERNUMBER) %>% summarise(total_ord_contacts = n())
sba_wims1 <- inner_join(sba_wims, sba_totalcontacts, by = c("ORDERNUMBER"))

# extract hub data --------------------------------------------------------
cat("\n querying hub data...")
connUAT <- ia_odbc("sqlServerUAT") 
hub_sql <- sqlQuery(connUAT, paste0("SELECT [TransportationHub_ID]
                                          ,[HubNumber]
                                          ,[HubName]
                                          ,[HubAbbreviation]
                                          ,[Terminal]
                                          ,[FCNumber]
                                          ,[FCName]
                                          ,[Carrier]
                                          ,[Address1]
                                          ,[City]
                                          ,[State]
                                          ,[Zip]
                                          ,[Division]
                                          ,[DivisionManager]
                                          ,[SupportManager]
                                          ,[CourierTerminalMgr]
                                      FROM [NADCS_DW].[dbo].[Dim_TransportationHub]"), stringsAsFactors = F)
odbcClose(connUAT)

connGDW <- ia_odbc("gdw")
hub_gdw <- sqlQuery(connGDW, paste0("SELECT hubnumber, deliveryhub FROM PRD_USD_UMV.HUB_LOC_MASTER_AV"), stringsAsFactors = F)
odbcClose(connGDW)

hub_df <- left_join(hub_sql, hub_gdw, by = c("HubNumber"="hubnumber")) %>%
  mutate(MainHub = ifelse(is.na(deliveryhub), HubNumber, deliveryhub),
         Carrier = ifelse(Carrier == 'LTL   ', 'LTL', ifelse(Carrier == 'Ontrac', 'OnTrac', as.character(Carrier))))

#upload latest hubs to sandbox
hub_df1 <- hub_df %>% mutate(updated = as.character(Sys.Date())) %>% select(-HubName, -deliveryhub, -Terminal)

connDev <- odbcConnect('cs-db-dev')
sqlSave(connDev, hub_df1, tablename = 'dbo.hub_details', append = F, rownames = F, safer = F)
odbcClose(connDev)

# extract ciipo data ------------------------------------------------------
cat("\n querying ciipo data...")
connProd <- ia_odbc("sqlServerProd")

#phone ciipo
phone_ciipo <- sqlQuery(connProd, paste0("SELECT 'Phone' AS InteractionType,SUM(AIS.ACDCalls) AS Total     
                                         FROM FALCON.dbo.tblAvayaIntradaySkills AS AIS
                                           JOIN FALCON.dbo.tblLookupAvayaGroupSkill AS LAS 
                                           ON LAS.SkillNumber = AIS.Skill
                                           and LAS.ACDNumber = AIS.ACD
                                           AND ais.Calendar_Date >= LAS.StartDate
                                           AND ais.Calendar_Date <= LAS.EndDate
                                         WHERE  AIS.Calendar_Date between '",start_sql,"' and '",end_sql,"'
                                         AND AIS.BusinessUnit IN ('Staples Advantage')
                                         AND las.ciipo= 1"))
#email ciipo
email_ciipo <- sqlQuery(connProd, paste0("SELECT 'Email' AS InteractionType, SUM(KIS.Reply + KIS.NoAnswer + KIS.Forward + KIS.Redirect) as Total
                                         FROM FALCON.dbo.tblKanaIntradaySkills AS KIS
                                           JOIN FALCON.dbo.tblLookupKana AS LK 
                                           ON KIS.FolderID = lk.FolderID
                                           AND kis.[partition] = lk.[partition]
                                           AND KIS.Calendar_Date >= lk.StartDate
                                           AND kis.Calendar_Date <=LK.EndDate
                                         WHERE KIS.Calendar_Date between '",start_sql,"' and '",end_sql,"'
                                         AND lk.CIIPO = 1
                                         AND (KIS.BusinessUnit='Staples Advantage' and KIS.FolderID NOT IN (99999,99998,99997,999996))"))
#chat ciipo
chat_ciipo <- sqlQuery(connProd, paste0("SELECT 'Chat' AS InteractionType, SUM(CIS.NumChatSessionsCompleted) AS Total
                                        FROM FALCON.dbo.tblChatIntradaySkills AS CIS  
                                          JOIN FALCON.dbo.tbllookupChat tc ON cis.BusinessUnit= tc.BusinessUnit
                                          AND tc.Queue_ID = cis.QueueID
                                          AND cis.Calendar_Date >= tc.StartDate
                                          AND cis.Calendar_Date <= tc.EndDate      
                                        WHERE CIS.Calendar_Date between '",start_sql,"' and '",end_sql,"'
                                        AND CIS.BusinessUnit IN ('Staples Advantage')
                                        AND tc.CIIPO = 1"))

#csi total
csi_total <- sqlQuery(connProd, paste0("SELECT 'CSI' as InteractionType, count([FCR_ID]) as Total
                                        FROM [NADCS_DW].[dbo].[smryCSIReporting]
                                        WHERE CSI_BU_NAME = 'SA US' 
                                          AND FCR_DATE between '",start_sql,"' and '",end_sql,"'"))
odbcClose(connProd)

csi_ciipo_factor <- rbind(phone_ciipo, email_ciipo, chat_ciipo, csi_total) %>%  dcast(.~InteractionType, value.var = "Total") %>% mutate(volume = Phone + Email + Chat, csi_factor = CSI/volume)

# join oms + csi + hub ----------------------------------------------------
cat("\n finalizing data...")
#join
sba_orders2 <- left_join(sba_orders1, sba_wims1, by = c("Orord#"="ORDERNUMBER")) %>%
  mutate(fractional_contacts = ifelse(is.na(total_ord_contacts), 0, (1/shipment_number)),
         fractional_shipments = ifelse(is.na(total_ord_contacts), 1, (1/total_ord_contacts)),
         fractional_orders = ifelse(is.na(total_ord_contacts),(1/shipment_number),
                                    1/(total_ord_contacts*shipment_number))) %>%
  inner_join(hub_df, by = c("Orhloc" = "HubNumber")) %>%
  mutate(RealCourier = ifelse(Orcarr == 'UPS', 'UPS', as.character(Carrier)), #adjust for UPS (UPS can delivery from any hub, but courier OMS data would reflect that)
         fractional_contacts = ifelse(is.na(fractional_contacts), 0, fractional_contacts)) 

# aggregate ---------------------------------------------------------------

#orders/shipments by courier/hub
courier_orders <- sba_orders2 %>%
  select(RealCourier, MainHub,  Division, DivisionManager, SupportManager, FCNumber, FCName, City, State, Zip, fractional_shipments, fractional_orders) %>%
  group_by(RealCourier, MainHub,  Division, DivisionManager, SupportManager, FCNumber, FCName, City, State, Zip) %>%
  summarise(Orders = sum(fractional_orders, na.rm = T), Shipments = sum(fractional_shipments, na.rm = T))
courier_orders$fiscal_week_of_period_name <- prev_fw$fiscal_week_of_period_name  

#contacts by courier/hub
courier_csi <- sba_orders2 %>%
  mutate(csi_factor = csi_ciipo_factor$csi_factor) %>%
  select(RealCourier, MainHub,Type, fractional_contacts, csi_factor) %>%
  group_by(RealCourier, MainHub, Type) %>%
  #group_by(RealCourier, MainHub, FCNumber, FCName, Type) %>%
  summarise(Contacts = sum(fractional_contacts, na.rm = T)/max(csi_factor)) 

#join orders + contacs
courier_df <- left_join(courier_orders, courier_csi, by = c("MainHub", "RealCourier")) %>% mutate(Type = ifelse(is.na(Type),"", as.character(Type)))
#courier_df <- left_join(courier_orders, courier_csi, by = c("RealCourier", "MainHub", "FCNumber", "FCName"))

# clean & update final mile table -----------------------------------------
cat("\n updating final mile table")
courier_df$FiscalWeek = prev_fw$fiscal_week_of_period_long

final_df <- courier_df %>%
  ungroup() %>%
  mutate(quart_id = 1) %>%
  select(FiscalWeek, fiscal_week_of_period_name, FCName, FCNumber, MainHub, RealCourier, City, State, Zip, Type, Contacts, Orders, Shipments, quart_id) %>%
  rename(Hub = MainHub, Courier = RealCourier, ContactType = Type)

connDev <- odbcConnect('cs-db-dev')
sqlSave(connDev, final_df, tablename = 'dbo.sba_final_mile', append = T, rownames = F)
odbcClose(connDev)

# calc quartiles ----------------------------------------------------------

cat("\n calculating quartiles...")
connDev <- odbcConnect('cs-db-dev')
df_finalmile <- sqlQuery(connDev, paste0("SELECT * FROM [CSAQuery].[dbo].[sba_final_mile]"), stringsAsFactors = F)
odbcClose(connDev)

hubs_orders <- df_finalmile %>% 
  group_by(FiscalWeek,Hub, Courier, FCNumber) %>%
  summarise(Orders = max(Orders, na.rm = T), Shipments = max(Shipments, na.rm = T)) %>%
  group_by(Hub) %>%
  summarise(Orders = sum(Orders, na.rm = T), Shipments = sum(Shipments, na.rm = T)) %>%
  filter(Orders > 100)

hubs_contacts <- df_finalmile  %>% 
  group_by(Hub)  %>%
  summarise(Contacts = sum(Contacts, na.rm = F)) 

quartiles <- inner_join(hubs_orders, hubs_contacts, by = c("Hub")) %>%
  ungroup() %>%
  mutate(Contacts = ifelse(is.na(Contacts), 0, Contacts),
         Shipments = ifelse(is.na(Shipments), 0, Shipments)) %>%
  mutate(quart_id = 1,
         CPS = (Contacts/Shipments),
         Quartile.1 = quantile(CPS, prob = c(.0)),
         Quartile.2 = quantile(CPS, prob = c(.25)),
         Quartile.3 = quantile(CPS, prob = c(.5)),
         Quartile.4 = quantile(CPS, prob = c(.75))) %>%
  select(quart_id, Quartile.1, Quartile.2, Quartile.3, Quartile.4) %>%
  distinct()

connDev <- odbcConnect('cs-db-dev')
sqlSave(connDev, quartiles, tablename = 'dbo.sba_final_mile_cpsquartiles', append = F, rownames = F, safer = F)
odbcClose(connDev)

# export final table to csv file for Tableau ------------------------------
cat("\n export to csv file...")
connDev <- odbcConnect('cs-db-dev')
export_df <- sqlQuery(connDev, paste0("SELECT DISTINCT
                                            t1.*,
                                            t2.Division,
                                            t2.DivisionManager,
                                            t2.SupportManager,
                                            t3.Quartile1,
                                            t3.Quartile2,
                                            t3.Quartile3,
                                            t3.Quartile4
                                       FROM [CSAQuery].[dbo].[sba_final_mile] t1
                                      inner join [CSAQuery].[dbo].[hub_details] t2 ON (t1.Hub = t2.MainHub AND t1.FCNumber = t2.FCNumber)
                                      inner join [CSAQuery].[dbo].[sba_final_mile_cpsquartiles] t3 ON (t1.quart_id = t3.quart_id)
                                      ORDER BY FiscalWeek"), stringsAsFactors = F)
odbcClose(connDev)
write.csv(export_df, file="~/VM-jobs/sba_final_mile/sba_final_mile_data.csv", append = F, row.names = F)
write.csv(export_df, file="//AURCOFILE2/AblMi001$/Projects/MikeAbla/Final Mile/sba_final_mile_data.csv", append = F, row.names = F)

#create log
#dir_path <- getwd()
#dir1 <- paste0(dir_path, '/logs')

#log_name <- paste0(dir1, 'final-mile-', format(Sys.time(), '%Y-%m-%d_%H%M%S'), '.txt')
#sink(log_name)
#sink()

cat("\n Done!")
  }

 report_list <- list("final_df" = final_df, "courier_df" = courier_df, "hub_df" = hub_df, 
                     "sba_orders2" = sba_orders2, "quartiles" = quartiles, "export_df"= export_df)
 return(report_list)
 
}

reports <- final_mile_wk_report()

odbcCloseAll()


