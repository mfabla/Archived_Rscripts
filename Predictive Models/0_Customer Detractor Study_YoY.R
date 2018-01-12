################### Customer Detractor Extract ###########################

#by Mike Abla


#NOTES:
#This is Part I, extracting and transforming the data
#This is a rev to look at Q1 YoY


########################################################################


# Set up environment ------------------------------------------------------

library(rIA)
set_env(clear_env = T, dir = "~/Projects/MikeAbla/Customer Detractor Study" )


# SBA Extract Functions ---------------------------------------------------

sba.query <- function(x){
  
  connGDW <- NULL
  connGDW <- ia_odbc("gdw")
  
  sqlQuery(connGDW, paste0(
    "SELECT
        CSI_ID, 
        CSI_DATE, 
        CONTACT_CHANNEL,
        CONTACT_PARTY,
        FSC_WK_OF_PRD_LN_NM,
        --ASSOCIATE_ID,
        --LAND_ID, 
        --CSI_BU_NAME, 
        --SPLS_LOCATION_NAME,
        DATAFILTER_TOPIC, 
        --DATAFILTER_DOMAIN, 
        --DATAFILTER_AREA, 
        --DATAFILTER_TYPE,
        --XFERRED_CONTACT_NM,        
        ORDER_NUMBER, 
        --ORDER_CREATE_DATE,
        ORDER_METHOD,
        LINK_NUMBER, 
        SHIPMENT_ID,
        SHIPMENT_NUMBER, 
        DELIVERY_COURIER,
        DELIVERY_TYPE,
        FULFILLMENT_METHOD, 
        FULFILLMENT_CENTER,
        --EXPECTED_DLV_DATE,
        SHIPMENT_STATUS,
        --RETURN_ORDER_NUMBER,
        CUSTOMER_NUMBER, 
        --CUST_NAME, 
        CUST_NBR1 
        --CUST_ACCT_CRT_DATE,
        --( CSI_DATE-CUST_ACCT_CRT_DATE) as date_diff
    FROM	BIPS_CSI_PROD.CSI_APPEND_T
    where CSI_BU_NAME = 'SA US' AND
    FSC_WK_OF_PRD_LN_NM = '",x,"'"), stringsAsFactors = F)
}

sba.oms.query <- function(x,y){
  connGDW <- NULL
  connGDW <- ia_odbc("gdw")
  sqlQuery(connGDW, paste0("
                           SELECT distinct
                           t1.Ohord#,
                           t1.Ohcust,
                           t1.Ohcrdt, 
                           t1.Ohoto$,
                           t1.Ohoqty,
                           max(case when t2.ORTYPE = 'SK' then 1 else 0 end) as stock,
                           max(case when t2.ORDROP = 'Y' then 1 else 0 end) as dropship,
                           max(case when t4.PTADDT > t4.PTSDDT then 1 else 0 end) as latedelivery
                           FROM 	PRD_USD_OPV.SUNRISE_SFBASLIB_SFORDH_V t1
                           join PRD_USD_OPV.SUNRISE_SFBASLIB_SFORDR_V  t2  on ( t1.OHORD#=t2.ORORD# AND t1.OHLINK=t2.ORLINK)
                           left join PRD_USD_OPV.SUNRISE_MMBASLIB_SIPTKH_V t4 on  ( t1.OHORD#=t4.PTORD# AND  t1.OHLINK=t4.PTLINK)
                           WHERE 
                           t1.Ohlink=0 AND
                           t1.Ohtype='OR'AND
                           t1.Ohcrdt between ",x," and ",y,"
                           GROUP BY 
                           1,2,3,4,5"))
}

sba.ordermethod.query <- function(x,y) {
  connGDW <- NULL
  connGDW <- ia_odbc("gdw")
  sqlQuery(connGDW, paste0("
                           SELECT DISTINCT
                           Ohord#,
                           OHCRDT,
                           OHORDM
                           FROM PRD_USD_OPV.SUNRISE_SFBASLIB_SFORDH_V
                           WHERE  1=1
                           AND OHCRDT between ",x," and ",y,"
                           AND Ohlink = 0
                           AND OHTYPE = 'OR'
                           "))
  }

sba.backorders.query <- function(x,y){
  connGDW <- NULL
  connGDW <- ia_odbc("gdw")
  sqlQuery(connGDW, paste0("SELECT DISTINCT
                                tb1.OHORD#
                                FROM PRD_USD_OPV.SUNRISE_SFBASLIB_SFORDH_V as tb1
                                Join PRD_USD_OPV.SUNRISE_SFBASLIB_SFORDR_V as tb2 on tb1.OHORD# = tb2.ORORD# AND tb1.OHLINK = tb2.ORLINK
                                Join PRD_USD_OPV.SUNRISE_SFBASLIB_SFORDD_V as tb3 on tb2.ORORD# = tb3.ODORD# and tb2.ORLINK = tb3.ODLINK and tb2.ORSHP# = tb3.ODSHP#
                            WHERE ((OHLINK=0 AND OHTYPE IN('OR') AND OHCRDT BETWEEN ", x," and ", y,"))
                                AND tb2.ORTYPE = 'BO'
                                AND (tb3.ODDROP = 'N' or tb3.ODDROP = ' ')"))
}

#TED (Escallations Data) #note that the order_number is either an order or customer #

ted.query <- function(x,y){
    connUAT <- NULL
    connUAT <- ia_odbc('sqlServerUAT') 
    ted.x <- NULL
    ted.x <- sqlQuery(connUAT, paste0("
                                    SELECT 
                                    'SBA' AS business_unit,
                                    CAST(ahdcallcommentdate AS date) AS date, 
                                    ahdcallorderacctnumber AS order_number, 
                                    dispcatname AS issue_reason, 
                                    ahdcalldispositionexplanation AS customer_story
                                    FROM ticketing.dbo.conahd_comment a
                                    JOIN ticketing.dbo.conahd_ahdcall b ON a.ahdcallid=b.ahdcallid
                                    JOIN ticketing.dbo.conahd_dispositioncategory c ON b.dispositioncatid=c.dispositioncatid
                                    WHERE CAST(ahdcallcommentdate AS date) BETWEEN '",x,"' and '",y,"'
                                    AND ahdcalldispositionexplanation != ''
                                    AND ahdcallorderacctnumber != '' 
                                    ORDER BY ahdcallcommentdate"), stringsAsFactors = F)
    
    ted.x$order_number <- str_replace_all(ted.x$order_number, c('#'='', '^chat\\s+2[0-9]+/'='', '^[0-9]+/'=''))
    ted.x$order_number <- as.numeric(ted.x$order_number)
    return(ted.x <- ted.x[!is.na(ted.x$order_number), ])
}

wlfk.query <- function(x,y){
  connGDW <- NULL
  connGDW <- ia_odbc("gdw")
  sqlQuery(connGDW, paste0("
                           SELECT DISTINCT
  	                         t1.OHORD#,
                             t1.OHCRDT,
                             max(CASE WHEN t2.ODSTAT = 'KIL' then 1 else 0 end) as fillkill,
                             max(CASE WHEN t2.ODDROP <> 'Y' AND t2.ODSKMA IN ('US', 'SR') then 1 else 0 end) as wraplabel
                           FROM PRD_USD_OPV.SUNRISE_SFBASLIB_SFORDH_V t1
                           INNER JOIN  PRD_USD_OPV.SUNRISE_SFBASLIB_SFORDD_V t2 ON (t1.OHORD#=ODORD#  AND t1.OHLINK=t2.ODLINK)
                           WHERE 1=1
                             AND t1.OHLINK = 0
                             AND t1.OHTYPE IN ('OR', 'MO', 'QU') 
                             AND t1.OHCRDT BETWEEN ",x," AND ", y,"
                           GROUP BY 1,2
                           HAVING fillkill = 1 OR wraplabel=1"))
}


returns.query <- function(x, y){
  connGDW <- NULL
  connGDW <- ia_odbc("gdw")
  return.orders <- sqlQuery(connGDW, paste0("
                                            SELECT  Distinct 
                                            OHORD#,
                                            1 as total_returns
                                            FROM   PRD_USD_OPV.SUNRISE_SFBASLIB_SFORDR_V
                                            INNER JOIN SUNRISE_SFBASLIB_SFORDH_V   ON OHORD# = ORORD# and OHLINK= ORLINK
                                            WHERE 1=1
                                            AND Ortype = 'RT' 
                                            AND OHTYPE = 'AJ' 
                                            AND OHOSTA<>'CAN'
                                            AND OHCRDT between ",x," and ",y,""))
}

carrier.query <- function(x, y){
  connGDW <- NULL
  connGDW <- ia_odbc("gdw")
  
  sba.carriers <- sqlQuery(connGDW,paste0(
    "SELECT
    t2.Ohord#,
    max(case when  t5.carrier = 'LaserShip' then 1 else 0 end) as LaserShip,
    max(case when  t5.carrier = 'Fleet' then 1 else 0 end) as Fleet,
    max(case when  t5.carrier = 'OnTrac' then 1 else 0 end) as OnTrac,
    max(case when  t5.carrier = 'UPS' then 1 else 0 end) as UPS,
    max(case when  t5.carrier = 'Courier Express' then 1 else 0 end) as CourierExpress,
    max(case when  t5.carrier = 'LSO' then 1 else 0 end) as LSO,
    max(case when  t5.carrier = 'Postal Express' then 1 else 0 end) as PostalExpress,
    max(case when  t5.carrier = 'BES Trucking' then 1 else 0 end) as BESTrucking
    FROM	PRD_USD_OPV.SUNRISE_SFBASLIB_SFORDH_V t2
    join PRD_USD_OPV.SUNRISE_SFBASLIB_SFORDR_V  t4  on ( t2.OHORD#=t4.ORORD# AND t2.OHLINK=t4.ORLINK)
    join  PRD_USD_UMV.HUB_LOC_MASTER_AV t5 on ( t4.ORHLOC = t5.hubnumber)
    where 	   
    Ohlink= 0  and
    Ohtype = 'OR' and
    OHCRDT between ", x," and ", y,"
    group by 
    1")) 
  
  sba.carriers <- mutate(sba.carriers, other_carrier = ifelse(rowSums(sba.carriers[2:9]) > 0, 0, 1))
  
}


# FY17 Q1 Extract ---------------------------------------------------------

#csi append data(CareBear)
FY17P1.csi <- sba.query('FY17 P01')
FY17P2.csi <- sba.query('FY17 P02')
FY17P3.csi <- sba.query('FY17 P03')

mm.csi.FY17 <- rbind(FY17P1.csi, FY17P2.csi, FY17P3.csi)
rm(FY17P1.csi, FY17P2.csi, FY17P3.csi)

#sba oms data
FY17P1.oms <- sba.oms.query(20170129,20170225)
FY17P2.oms <- sba.oms.query(20170226,20170401)
FY17P3.oms <- sba.oms.query(20170402,20170429)

oms.FY17 <- rbind(FY17P1.oms, FY17P2.oms, FY17P3.oms)
rm(FY17P1.oms, FY17P2.oms, FY17P3.oms)

#backorders
FY17P1.backorders <- sba.backorders.query(20170129,20170225)
FY17P2.backorders <- sba.backorders.query(20170226,20170401)
FY17P3.backorders <- sba.backorders.query(20170402,20170429)

backorders.FY17 <- rbind(FY17P1.backorders, FY17P2.backorders, FY17P3.backorders)
rm(FY17P1.backorders, FY17P2.backorders, FY17P3.backorders)

FY16P1.backorders <- sba.backorders.query(20160131,20160227)
FY16P2.backorders <- sba.backorders.query(20160228,20160402)
FY16P3.backorders <- sba.backorders.query(20160403,20160430)

backorders.FY16 <- rbind(FY16P1.backorders, FY16P2.backorders, FY16P3.backorders)
rm(FY16P1.backorders, FY16P2.backorders, FY16P3.backorders)

#wrap-and-labels & fill/kills
FY17P1.wlfk <- wlfk.query(20170129,20170225)
FY17P2.wlfk <- wlfk.query(20170226,20170401)
FY17P3.wlfk <- wlfk.query(20170402,20170429) 

wlfk.FY17 <- rbind(FY17P1.wlfk, FY17P2.wlfk, FY17P3.wlfk)
rm(FY17P1.wlfk, FY17P2.wlfk, FY17P3.wlfk)

#returns
returns.FY17 <- returns.query(20170129,20170429)

#carriers
FY17P1.carriers <- carrier.query(20170129,20170225)
FY17P2.carriers <- carrier.query(20170226,20170401)
FY17P3.carriers <- carrier.query(20170402,20170429)

FY17.carriers <- rbind(FY17P1.carriers, FY17P2.carriers, FY17P3.carriers)
rm(FY17P1.carriers, FY17P2.carriers, FY17P3.carriers)

FY16P1.carriers <- carrier.query(20160131,20160227)
FY16P2.carriers <- carrier.query(20160228,20160402)
FY16P3.carriers <- carrier.query(20160403,20160430)

FY16.carriers <- rbind(FY16P1.carriers, FY16P2.carriers, FY16P3.carriers)
rm(FY16P1.carriers, FY16P2.carriers, FY16P3.carriers)
  
#ted data
ted.FY17 <- ted.query('2017-1-29', '2017-4-29')

#order method
ordermethod<- sba.ordermethod.query(20170129,20170429)

connSQL <- odbcConnect('cs-db-uat')
ordermethod_ref <- sqlQuery(connSQL, paste0("SELECT [As400OrderMethods_ID],[OHORDM],[Label],[BusinessUnit]FROM [NADCS_DW].[dbo].[Dim_As400OrderMethods]WHERE [BusinessUnit] = 'Staples Advantage'"))
ordermethod <- left_join(ordermethod, ordermethod_ref, by = c("Ohordm" = "OHORDM")) %>% select(`Ohord#`, Ohcrdt, Label) 
ordermethod1 <- ordermethod %>% mutate(total = 1) %>% filter(!is.na(Label))
#ordermethod1 <- ordermethod %>% mutate(Label = ifelse(!Label %in% c('STAPLESLINK.COM', 'LINK+ ORDER METHOD', 'SNAP/3RD PARTY ORDER', 'PHONE ORDER', 'eDiversity', 'E-MAIL', 'X.12 STANDARD EDI', 'SALESMAN RECD ORDER'),'Other_OrderMethod', as.character(Label)),total = 1)
ordermethod1 <- ordermethod %>%
  mutate(EDI = ifelse(Label %in% c("Via Email (EDI)", "PC EDI ORDER", "ECOS REMOTE ORDER", "eDiversity", "SNAP/3RD PARTY ORDER", "X.12 STANDARD EDI", "REMOTE ORDER", "PROPRIETARY EDI"),1,0),
         Internet = ifelse(Label %in% c("STAPLESLINK.COM", "LINK+ ORDER METHOD", "MOBILE ORDER"), 1, 0),
         Manual.Order = ifelse(Label %in% c("FAX ORDER", "OCR FAX ORDER", "E-MAIL", "DRIVER RECD ORDER", "SOLUTIONS", "STORE ORDER", "MAIL ORDER", "PHONE ORDER", "SALESMAN RECD ORDER"),1,0))
ordermethod2 <- ordermethod1 %>%
  mutate(STAPLESLINK.COM = ifelse(Label == 'STAPLESLINK.COM', 1, 0),
         `LINK+ ORDER METHOD` = ifelse(Label == 'LINK+ ORDER METHOD', 1, 0),
         `SNAP/3RD PARTY ORDER` = ifelse(Label == 'SNAP/3RD PARTY ORDER', 1, 0),
         `PHONE ORDER` = ifelse(Label == 'PHONE ORDER', 1, 0),
         `eDiversity` = ifelse(Label == 'eDiversity', 1, 0),
         `E-MAIL` = ifelse(Label == 'E-MAIL', 1, 0),
         `X.12 STANDARD EDI` = ifelse(Label == 'X.12 STANDARD EDI', 1, 0),
         `SALESMAN RECD ORDER` = ifelse(Label == 'SALESMAN RECD ORDER', 1, 0),
         `MOBILE ORDER` = ifelse(Label == 'MOBILE ORDER', 1, 0),
         `E-BROKER` = ifelse(Label == 'E-BROKER', 1, 0), 
         `FAX ORDER` = ifelse(Label == 'FAX ORDER', 1, 0),
         `SOLUTIONS` = ifelse(Label == 'SOLUTIONS', 1, 0),
         `DESKTOP SUPERSTORE` = ifelse(Label == 'DESKTOP SUPERSTORE', 1, 0),
         `TELE-LINK` = ifelse(Label == 'TELE-LINK', 1, 0),
         `REMOTE ORDER` = ifelse(Label == 'REMOTE ORDER', 1, 0),
         `MAIL ORDER` = ifelse(Label == 'MAIL ORDER', 1, 0),
         `Via Email (EDI)`= ifelse(Label == 'Via Email (EDI)', 1, 0),
         `ECOS REMOTE ORDER` = ifelse(Label == 'ECOS REMOTE ORDER', 1, 0),
         `PROPRIETARY EDI` = ifelse(Label == 'PROPRIETARY EDI', 1, 0),
         `OCR FAX ORDER` = ifelse(Label == 'OCR FAX ORDER', 1, 0),
         `DRIVER RECD ORDER` = ifelse(Label == 'DRIVER RECD ORDER', 1, 0),
         `PC EDI ORDER` = ifelse(Label == 'PC EDI ORDER', 1, 0),
         `STORE ORDER` = ifelse(Label == 'STORE ORDER', 1, 0),
         `Unknown` = ifelse(Label == 'Unknown', 1, 0)) %>%
  filter(!is.na(Label))

ordermethod2.FY17 <- ordermethod2

rm(ordermethod_ref, ordermethod1, ordermethod2)

#customer account start dates
connGDW <- ia_odbc("gdw")
cust_dates <- sqlQuery(connGDW, paste0("
                                        SELECT 
                                       Cccust, 
                                       Cccrdt
                                       FROM	PRD_USD_OPV.SUNRISE_SFBASLIB_SFCUST_V"))
odbcCloseAll()

save(backorders.FY16, backorders.FY17, FY16.carriers, FY17.carriers, mm.csi.FY17, oms.FY17, ordermethod,
     ordermethod2.FY17, returns.FY17, ted.FY17, wlfk.FY17, cust_dates, file = "customer detractor base data_YoY.rdata")

# FY16 Q1 OMS Data Clean --------------------------------------------------

load("customer detractor base data.rdata")

#update ordermethod

ordermethod <- ordermethod %>% filter(!is.na(Label))
ordermethod1 <- ordermethod %>% mutate(total = 1)
ordermethod1 <- ordermethod %>%
  mutate(EDI = ifelse(Label %in% c("Via Email (EDI)", "PC EDI ORDER", "ECOS REMOTE ORDER", "eDiversity", "SNAP/3RD PARTY ORDER", "X.12 STANDARD EDI", "REMOTE ORDER", "PROPRIETARY EDI"),1,0),
         Internet = ifelse(Label %in% c("STAPLESLINK.COM", "LINK+ ORDER METHOD", "MOBILE ORDER"), 1, 0),
         Manual.Order = ifelse(Label %in% c("FAX ORDER", "OCR FAX ORDER", "E-MAIL", "DRIVER RECD ORDER", "SOLUTIONS", "STORE ORDER", "MAIL ORDER", "PHONE ORDER", "SALESMAN RECD ORDER"),1,0))
ordermethod2 <- ordermethod1 %>%
  filter(Ohcrdt <= 20160430) %>%
  mutate(STAPLESLINK.COM = ifelse(Label == 'STAPLESLINK.COM', 1, 0),
         `LINK+ ORDER METHOD` = ifelse(Label == 'LINK+ ORDER METHOD', 1, 0),
         `SNAP/3RD PARTY ORDER` = ifelse(Label == 'SNAP/3RD PARTY ORDER', 1, 0),
         `PHONE ORDER` = ifelse(Label == 'PHONE ORDER', 1, 0),
         `eDiversity` = ifelse(Label == 'eDiversity', 1, 0),
         `E-MAIL` = ifelse(Label == 'E-MAIL', 1, 0),
         `X.12 STANDARD EDI` = ifelse(Label == 'X.12 STANDARD EDI', 1, 0),
         `SALESMAN RECD ORDER` = ifelse(Label == 'SALESMAN RECD ORDER', 1, 0),
         `MOBILE ORDER` = ifelse(Label == 'MOBILE ORDER', 1, 0),
         `E-BROKER` = ifelse(Label == 'E-BROKER', 1, 0), 
         `FAX ORDER` = ifelse(Label == 'FAX ORDER', 1, 0),
         `SOLUTIONS` = ifelse(Label == 'SOLUTIONS', 1, 0),
         `DESKTOP SUPERSTORE` = ifelse(Label == 'DESKTOP SUPERSTORE', 1, 0),
         `TELE-LINK` = ifelse(Label == 'TELE-LINK', 1, 0),
         `REMOTE ORDER` = ifelse(Label == 'REMOTE ORDER', 1, 0),
         `MAIL ORDER` = ifelse(Label == 'MAIL ORDER', 1, 0),
         `Via Email (EDI)`= ifelse(Label == 'Via Email (EDI)', 1, 0),
         `ECOS REMOTE ORDER` = ifelse(Label == 'ECOS REMOTE ORDER', 1, 0),
         `PROPRIETARY EDI` = ifelse(Label == 'PROPRIETARY EDI', 1, 0),
         `OCR FAX ORDER` = ifelse(Label == 'OCR FAX ORDER', 1, 0),
         `DRIVER RECD ORDER` = ifelse(Label == 'DRIVER RECD ORDER', 1, 0),
         `PC EDI ORDER` = ifelse(Label == 'PC EDI ORDER', 1, 0),
         `STORE ORDER` = ifelse(Label == 'STORE ORDER', 1, 0),
         `Unknown` = ifelse(Label == 'Unknown', 1, 0)) 

#add orders, fill-kill, wrap-and-label, ordermethod, returns
mm.oms.FY16 <- inner_join(MidMarkets, sba.oms, by = c("CCCUST" = "Ohcust")) %>%
  filter(Ohcrdt <= 20160430) %>%
  left_join(wlfk, by = c("Ohord#", "Ohcrdt")) %>%
  left_join(ordermethod2, by = c("Ohord#", "Ohcrdt")) %>%
  left_join(return.orders, by = c("Ohord#")) %>%
  mutate(fillkill = ifelse(is.na(fillkill),0,fillkill),
         wraplabel = ifelse(is.na(wraplabel),0,wraplabel),
         returns = ifelse(is.na(returns),0,1)) 

#add backorders, carriers
backorders.FY16$total <- 1
mm.oms.FY16 <- left_join(mm.oms.FY16, backorders.FY16, by = c("Ohord#")) %>%
  mutate(backorder = ifelse(is.na(total), 0, 1)) %>% 
  select(-total) %>%
  left_join(FY16.carriers, by = c("Ohord#"))

#add ted data
ted$ted_contact <- 1
ted$date1 <- as.numeric(gsub('-','', ted$date))
ted_orderjoin <- inner_join(ted[,c(3,6,7)],mm.oms.FY16[,c(1,3,4)], by=c("order_number" = "Ohord#")) %>%
  distinct() %>%
  group_by(order_number) %>%
  summarise(ted_contact = sum(ted_contact, na.rm = T))

mm.oms.FY16 <- left_join(mm.oms.FY16, ted_orderjoin, by = c("Ohord#" = "order_number")) %>%
  mutate(ted_contact = ifelse(is.na(ted_contact), 0, ted_contact))

#replace NAs w/ zeros
mm.oms.FY16[is.na(mm.oms.FY16)] <- 0

#rollup to customer
mm.oms.FY16.1 <- mm.oms.FY16 %>%
  mutate(total_orders = 1) %>%
  select(-Ohcrdt, -`Ohord#`) %>%
  group_by(CCCUST, Industry) %>%
  summarise_each(funs(sum), -Label)

rm(MidMarkets, sba.oms, wlfk, ordermethod2, return.orders, backorders.FY16, FY16.carriers, ted, ted_orderjoin, mm.oms.FY16)

# FY16 Q1 CSI Data Clean --------------------------------------------------

#Clean CSI Data
csi.FY16 <- sba.csi.oms %>%
  mutate(CCCUST = ifelse(is.na(CUST_NBR1) | CUST_NBR1 == 0, CUSTOMER_NUMBER, CUST_NBR1),
         DELIVERY_COURIER = ifelse(!DELIVERY_COURIER %in% c('LaserShip', 'Fleet', 'OnTrac', 'UPS', 'Courier Express', 'LSO', 'Postal Express', 'BES Trucking') & !is.na(DELIVERY_COURIER), 'Other', as.character(DELIVERY_COURIER)),
         ORDER_METHOD = ifelse(!ORDER_METHOD %in% c('STAPLESLINK.COM', 'LINK+ ORDER METHOD', 'SNAP/3RD PARTY ORDER', 'PHONE ORDER', 'eDiversity', 'E-MAIL', 'X.12 STANDARD EDI', 'SALES RECD PHONE', 'MOBILE ORDER', 'FAX ORDER') & !is.na(ORDER_METHOD), 'Other', as.character(ORDER_METHOD)),
         CONTACT_PARTY = ifelse(!CONTACT_PARTY %in% c('Customer', 'Account Manager') & !is.na(CONTACT_PARTY), 'Other', as.character(CONTACT_PARTY)),
         total = 1) %>%
  filter(!is.na(CCCUST), CSI_DATE <= '2016-04-30') 

#Transpose/Dummy  
csi_channel.FY16 <- dcast(csi.FY16, CSI_ID~CONTACT_CHANNEL, value.var = "total", fun.aggregate = max, na.rm = T, fill = 0)
csi_party.FY16 <- dcast(csi.FY16, CSI_ID~CONTACT_PARTY, value.var = "total", fun.aggregate = max, na.rm = T, fill = 0)
csi_topic.FY16 <- dcast(csi.FY16, CSI_ID~DATAFILTER_TOPIC, value.var = "total", fun.aggregate = max, na.rm = T, fill = 0)
csi_order.FY16 <- dcast(csi.FY16, CSI_ID~ORDER_METHOD, value.var = "total", fun.aggregate = max, na.rm = T, fill = 0)
csi_courier.FY16 <- dcast(csi.FY16, CSI_ID~DELIVERY_COURIER, value.var = "total", fun.aggregate = max, na.rm = T, fill = 0)
csi_delivery.FY16 <- dcast(csi.FY16, CSI_ID~DELIVERY_TYPE, value.var = "total", fun.aggregate = max, na.rm = T, fill = 0)
csi_fullfuillment.FY16 <- dcast(csi.FY16, CSI_ID~FULFILLMENT_METHOD, value.var = "total", fun.aggregate = max, na.rm = T, fill = 0)

csi_channel.FY16 <- csi_channel.FY16 %>% rename(Other_channel = Other) 
csi_party.FY16 <- csi_party.FY16 %>% rename(Other_party = Other)
csi_topic.FY16 <- csi_topic.FY16 %>% rename(Other_csi = Other, Unknown = `NA`)
csi_order.FY16 <- csi_order.FY16 %>% rename(Other_order = Other, No_order = `NA`)
csi_courier.FY16 <- csi_courier.FY16 %>% rename(Other_courier = Other, No_courier = `NA`)
csi_delivery.FY16 <- csi_delivery.FY16 %>% mutate(No_delivery = ifelse(`NA` == 1, 1, ` `)) %>% select(-` `, -`NA`)
csi_fullfuillment.FY16 <- csi_fullfuillment.FY16 %>% rename(No_fullfillment = `NA`)

csi.FY16.1 <- csi.FY16 %>%
  select(CSI_ID, CCCUST, ORDER_NUMBER) %>%
  distinct() %>%
  left_join(csi_channel.FY16, by = c("CSI_ID")) %>%
  left_join(csi_party.FY16, by = c("CSI_ID")) %>%
  left_join(csi_topic.FY16, by = c("CSI_ID")) %>%
  left_join(csi_order.FY16, by = c("CSI_ID")) %>%
  left_join(csi_courier.FY16, by = c("CSI_ID")) %>%
  left_join(csi_delivery.FY16, by = c("CSI_ID")) %>%
  left_join(csi_fullfuillment.FY16, by = c("CSI_ID")) %>%
  mutate(total_contacts = 1) %>%
  ungroup() %>%
  select(-CSI_ID, -ORDER_NUMBER) %>%
  group_by(CCCUST) %>%
  summarise_each(funs(sum))

rm(csi.FY16, csi_channel.FY16, csi_courier.FY16, csi_delivery.FY16, csi_fullfuillment.FY16, csi_order.FY16, 
   csi_party.FY16, csi_topic.FY16)

# FY16 Q1 Join OMS and CSI Data -------------------------------------------

FY16.df <- left_join(mm.oms.FY16.1, csi.FY16.1, by = c("CCCUST")) 
FY16.df[is.na(FY16.df)] <- 0
names(FY16.df) <- gsub("\\.x", "", names(FY16.df))
names(FY16.df) <- gsub("\\.y", ".csi", names(FY16.df)) 
FY16.df <- FY16.df %>%
  rename(Ohoto = `Ohoto$`,
         LINKplus.ORDER.METHOD = `LINK+ ORDER METHOD`,
         SNAP.3RD.PARTY.ORDER = `SNAP/3RD PARTY ORDER`,
         PHONE.ORDER = `PHONE ORDER`,
         E.MAIL.ORDER = `E-MAIL`,
         X.12.STANDARD.EDI = `X.12 STANDARD EDI`,
         SALESMAN.RECD.ORDER = `SALESMAN RECD ORDER`,
         MOBILE.ORDER = `MOBILE ORDER`,
         E.BROKER = `E-BROKER`,
         FAX.ORDER = `FAX ORDER`,
         DESKTOP.SUPERSTORE = `DESKTOP SUPERSTORE`,
         TELE.LINK = `TELE-LINK`,
         REMOTE.ORDER = `REMOTE ORDER`,
         MAIL.ORDER = `MAIL ORDER`,
         Via.Email.EDI = `Via Email (EDI)`,
         ECOS.REMOTE.ORDER = `ECOS REMOTE ORDER`,
         PROPRIETARY.EDI = `PROPRIETARY EDI`,
         OCR.FAX.ORDER = `OCR FAX ORDER`,
         DRIVER.RECD.ORDER = `DRIVER RECD ORDER`,
         PC.EDI.ORDER = `PC EDI ORDER`,
         STORE.ORDER = `STORE ORDER`,
         AccountManager = `Account Manager`,
         ChangeOrder = `Change Order`,
         CustomerEducation = `Customer Education`,
         CustomerExperience = `Customer Experience`,
         OtherDept.Group = `Other Dept./Group`,
         PlaceOrder = `Place Order`,
         PriceMatch = `Price Match`,
         TaxExempt = `Tax Exempt`,
         E.MAIL.ORDER.csi = `E-MAIL.csi`,
         FAX.ORDER.csi = `FAX ORDER.csi`,
         MOBILE.ORDER.csi = `MOBILE ORDER.csi`,
         PHONE.ORDER.csi = `PHONE ORDER.csi`,
         LINKplus.ORDER.METHOD.csi = `LINK+ ORDER METHOD.csi`,
         SNAP.3RD.PARTY.ORDER.csi = `SNAP/3RD PARTY ORDER.csi`,
         X.12.STANDARD.EDI.csi = `X.12 STANDARD EDI.csi`,
         SALES.RECD.PHONE.csi = `SALES RECD PHONE`,
         Other_OrderMethod.csi = Other_order,
         BESTrucking.csi = `BES Trucking`,
         CourierExpress.csi = `Courier Express`,
         PostalExpress.csi = `Postal Express`,
         Other_carrier.csi = Other_courier,
         dropship.csi = DROPSHIP,
         stock.csi = `FULFILLMENT CENTER`,
         wholesalerdrop.csi = `WHOLESALER DROPSHIP`,
         wraplabel.csi = `WRAP AND LABEL`) %>%
  ungroup()
 
rm(mm.oms.FY16.1, csi.FY16.1)
 
# FY17 Q1 OMS Data Clean --------------------------------------------------

mm.oms.FY17 <- oms.FY17 %>%
  left_join(wlfk.FY17, by = c("Ohord#", "Ohcrdt")) %>%
  left_join(ordermethod2.FY17, by = c("Ohord#", "Ohcrdt")) %>%
  left_join(returns.FY17, by = c("Ohord#")) %>%
  mutate(fillkill = ifelse(is.na(fillkill),0,fillkill),
         wraplabel = ifelse(is.na(wraplabel),0,wraplabel),
         returns = ifelse(is.na(total_returns),0,1)) %>%
  select(-total_returns)

#add backorders, carriers
backorders.FY17$total <- 1
mm.oms.FY17 <- left_join(mm.oms.FY17, backorders.FY17, by = c("Ohord#")) %>%
  mutate(backorder = ifelse(is.na(total), 0, 1)) %>% 
  select(-total) %>%
  left_join(FY17.carriers, by = c("Ohord#"))

#add ted data
ted.FY17$ted_contact <- 1
ted.FY17$date1 <- as.numeric(gsub('-','', ted.FY17$date))
ted_orderjoin <- inner_join(ted.FY17[,c(3,6,7)],mm.oms.FY17[,c(1:3)], by=c("order_number" = "Ohord#")) %>%
  distinct() %>%
  group_by(order_number) %>%
  summarise(ted_contact = sum(ted_contact, na.rm = T))

mm.oms.FY17 <- left_join(mm.oms.FY17, ted_orderjoin, by = c("Ohord#" = "order_number")) %>%
  mutate(ted_contact = ifelse(is.na(ted_contact), 0, ted_contact))

#remove NAs w/ zeros
mm.oms.FY17[is.na(mm.oms.FY17)] <- 0

#rollup to customer
mm.oms.FY17.1 <- mm.oms.FY17 %>%
  mutate(total_orders = 1) %>%
  select(-Ohcrdt, -`Ohord#`) %>%
  group_by(Ohcust) %>%
  summarise_each(funs(sum), -Label) %>%
  rename(CCCUST = Ohcust)

rm(backorders.FY17, FY17.carriers, oms.FY17, ordermethod, ordermethod2.FY17, returns.FY17, ted.FY17, ted_orderjoin, wlfk.FY17, mm.oms.FY17)


# FY17 Q1 CSI Data Clean --------------------------------------------------

#Clean CSI Data
csi.FY17 <- mm.csi.FY17 %>%
  mutate(CCCUST = ifelse(is.na(CUST_NBR1) | CUST_NBR1 == 0, CUSTOMER_NUMBER, CUST_NBR1),
         DELIVERY_COURIER = ifelse(!DELIVERY_COURIER %in% c('LaserShip', 'Fleet', 'OnTrac', 'UPS', 'Courier Express', 'LSO', 'Postal Express', 'BES Trucking') & !is.na(DELIVERY_COURIER), 'Other', as.character(DELIVERY_COURIER)),
         ORDER_METHOD = ifelse(!ORDER_METHOD %in% c('STAPLESLINK.COM', 'LINK+ ORDER METHOD', 'SNAP/3RD PARTY ORDER', 'PHONE ORDER', 'eDiversity', 'E-MAIL', 'X.12 STANDARD EDI', 'SALES RECD PHONE', 'MOBILE ORDER', 'FAX ORDER') & !is.na(ORDER_METHOD), 'Other', as.character(ORDER_METHOD)),
         CONTACT_PARTY = ifelse(!CONTACT_PARTY %in% c('Customer', 'Account Manager') & !is.na(CONTACT_PARTY), 'Other', as.character(CONTACT_PARTY)),
         total = 1)

#Transpose/Dummy  
csi_channel.FY17 <- dcast(csi.FY17, CSI_ID~CONTACT_CHANNEL, value.var = "total", fun.aggregate = max, na.rm = T, fill = 0)
csi_party.FY17 <- dcast(csi.FY17, CSI_ID~CONTACT_PARTY, value.var = "total", fun.aggregate = max, na.rm = T, fill = 0)
csi_topic.FY17 <- dcast(csi.FY17, CSI_ID~DATAFILTER_TOPIC, value.var = "total", fun.aggregate = max, na.rm = T, fill = 0)
csi_order.FY17 <- dcast(csi.FY17, CSI_ID~ORDER_METHOD, value.var = "total", fun.aggregate = max, na.rm = T, fill = 0)
csi_courier.FY17 <- dcast(csi.FY17, CSI_ID~DELIVERY_COURIER, value.var = "total", fun.aggregate = max, na.rm = T, fill = 0)
csi_delivery.FY17 <- dcast(csi.FY17, CSI_ID~DELIVERY_TYPE, value.var = "total", fun.aggregate = max, na.rm = T, fill = 0)
csi_fullfuillment.FY17 <- dcast(csi.FY17, CSI_ID~FULFILLMENT_METHOD, value.var = "total", fun.aggregate = max, na.rm = T, fill = 0)

csi_channel.FY17 <- csi_channel.FY17 %>% rename(Other_channel = Other) 
csi_party.FY17 <- csi_party.FY17 %>% rename(Other_party = Other)
csi_topic.FY17 <- csi_topic.FY17 %>% rename(Other_csi = Other, Unknown = `NA`)
csi_order.FY17 <- csi_order.FY17 %>% rename(Other_order = Other, No_order = `NA`)
csi_courier.FY17 <- csi_courier.FY17 %>% rename(Other_courier = Other, No_courier = `NA`)
csi_delivery.FY17 <- csi_delivery.FY17 %>% rename(No_delivery = `NA`)
csi_fullfuillment.FY17 <- csi_fullfuillment.FY17 %>% rename(No_fullfillment = `NA`)

csi.FY17.1 <- csi.FY17 %>%
  select(CSI_ID, CCCUST, ORDER_NUMBER) %>%
  distinct() %>%
  left_join(csi_channel.FY17, by = c("CSI_ID")) %>%
  left_join(csi_party.FY17, by = c("CSI_ID")) %>%
  left_join(csi_topic.FY17, by = c("CSI_ID")) %>%
  left_join(csi_order.FY17, by = c("CSI_ID")) %>%
  left_join(csi_courier.FY17, by = c("CSI_ID")) %>%
  left_join(csi_delivery.FY17, by = c("CSI_ID")) %>%
  left_join(csi_fullfuillment.FY17, by = c("CSI_ID")) %>%
  mutate(total_contacts = 1) %>%
  ungroup() %>%
  select(-CSI_ID, -ORDER_NUMBER) %>%
  group_by(CCCUST) %>%
  summarise_each(funs(sum))

rm(csi.FY17, csi_channel.FY17, csi_courier.FY17, csi_delivery.FY17, csi_fullfuillment.FY17, csi_order.FY17, 
   csi_party.FY17, csi_topic.FY17)

# FY17 Q1 Join OMS and CSI Data -------------------------------------------

FY17.df <- left_join(mm.oms.FY17.1, csi.FY17.1, by = c("CCCUST")) 
FY17.df[is.na(FY17.df)] <- 0
names(FY17.df) <- gsub("\\.x", "", names(FY17.df))
names(FY17.df) <- gsub("\\.y", ".csi", names(FY17.df)) 
FY17.df <- FY17.df %>%
  rename(Ohoto = `Ohoto$`,
         LINKplus.ORDER.METHOD = `LINK+ ORDER METHOD`,
         SNAP.3RD.PARTY.ORDER = `SNAP/3RD PARTY ORDER`,
         PHONE.ORDER = `PHONE ORDER`,
         E.MAIL.ORDER = `E-MAIL`,
         X.12.STANDARD.EDI = `X.12 STANDARD EDI`,
         SALESMAN.RECD.ORDER = `SALESMAN RECD ORDER`,
         MOBILE.ORDER = `MOBILE ORDER`,
         E.BROKER = `E-BROKER`,
         FAX.ORDER = `FAX ORDER`,
         DESKTOP.SUPERSTORE = `DESKTOP SUPERSTORE`,
         TELE.LINK = `TELE-LINK`,
         REMOTE.ORDER = `REMOTE ORDER`,
         MAIL.ORDER = `MAIL ORDER`,
         Via.Email.EDI = `Via Email (EDI)`,
         ECOS.REMOTE.ORDER = `ECOS REMOTE ORDER`,
         PROPRIETARY.EDI = `PROPRIETARY EDI`,
         OCR.FAX.ORDER = `OCR FAX ORDER`,
         DRIVER.RECD.ORDER = `DRIVER RECD ORDER`,
         PC.EDI.ORDER = `PC EDI ORDER`,
         STORE.ORDER = `STORE ORDER`,
         AccountManager = `Account Manager`,
         ChangeOrder = `Change Order`,
         CustomerEducation = `Customer Education`,
         CustomerExperience = `Customer Experience`,
         OtherDept.Group = `Other Dept./Group`,
         PlaceOrder = `Place Order`,
         PriceMatch = `Price Match`,
         TaxExempt = `Tax Exempt`,
         E.MAIL.ORDER.csi = `E-MAIL.csi`,
         FAX.ORDER.csi = `FAX ORDER.csi`,
         MOBILE.ORDER.csi = `MOBILE ORDER.csi`,
         PHONE.ORDER.csi = `PHONE ORDER.csi`,
         LINKplus.ORDER.METHOD.csi = `LINK+ ORDER METHOD.csi`,
         SNAP.3RD.PARTY.ORDER.csi = `SNAP/3RD PARTY ORDER.csi`,
         X.12.STANDARD.EDI.csi = `X.12 STANDARD EDI.csi`,
         SALES.RECD.PHONE.csi = `SALES RECD PHONE`,
         Other_OrderMethod.csi = Other_order,
         BESTrucking.csi = `BES Trucking`,
         CourierExpress.csi = `Courier Express`,
         PostalExpress.csi = `Postal Express`,
         Other_carrier.csi = Other_courier,
         dropship.csi = DROPSHIP,
         stock.csi = `FULFILLMENT CENTER`,
         wholesalerdrop.csi = `WHOLESALER DROPSHIP`,
         wraplabel.csi = `WRAP AND LABEL`) %>%
  ungroup()

rm(mm.oms.FY17.1, csi.FY17.1)

# Join FY16 and FY17 ------------------------------------------------------

names(FY16.df)[-1:-2] <- paste0(names(FY16.df)[-1:-2], '_2016')
names(FY17.df)[-1] <- paste0(names(FY17.df)[-1], '_2017')

#merge
FY16_FY17.df <- left_join(FY16.df, FY17.df, by = c("CCCUST"))
FY16_FY17.df[is.na(FY16_FY17.df)] <- 0

#add cust creation dates
FY16_FY17.df <- FY16_FY17.df %>%
  left_join(cust_dates,  by = c("CCCUST" = "Cccust"))

# Export Final Table ------------------------------------------------------

save(FY16_FY17.df, file = "SBA FY16-FY17 Q1_v2.Rdata")
