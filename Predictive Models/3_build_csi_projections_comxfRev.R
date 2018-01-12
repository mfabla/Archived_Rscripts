#### update transfer projections for staples.com ###

#Note: need to manually update XF projections in forecast since recent IVR
#routing changes has greatly reduced contacts that the model isn't picking
#starting on FY17 P3 W2


# set env -----------------------------------------------------------------

library(rIA)
set_env()

# extract csi forecast table data -----------------------------------------

connDEV <- odbcConnect('cs-db-dev')
com_contacts <- sqlQuery(connDEV, "Select * FROM [CSAQuery].[dbo].[csi_contact_forecast] 
                         Where week_start >= '2017-04-09' AND
                         businessunit = 'Staples.com' AND
                         variable = 'Transfer'", stringsAsFactors = F)
odbcClose(connDEV)

com_contacts1 <- com_contacts %>%
  dplyr::mutate(adj_contact_proj = adj_contact_proj *.60)


# update table ------------------------------------------------------------

connDEV <- odbcConnect('cs-db-dev')
sqlUpdate(connDEV, com_contacts1, tablename = "csi_contact_forecast", index = c("ID"))
odbcClose(connDEV)
