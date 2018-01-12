#### Weekly CDM Query in order to maintain accesss (and now MrKDM)


# set env -----------------------------------------------------------------

library(rIA)
set_env()

connCDM <- odbcConnect("cdm")
cdmquery <- sqlQuery(connCDM, "Select top 1000 * FROM	PRD_CONTR_DMV.D_CUST_FLAT_V")
odbcClose(connCDM)

connMrKDM <- odbcConnect("MrKDM")
MrKDMquery <- sqlQuery(connMrKDM, "SELECT	Top 1000 * FROM	PRD_MKTG_BIV.CUSTOMER_ACCOUNT")
odbcClose(connMrKDM)

odbcCloseAll()
