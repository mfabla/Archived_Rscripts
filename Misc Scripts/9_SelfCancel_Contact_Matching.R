##### Extract SBA Self-Cancel Reasons


# set env -----------------------------------------------------------------

library(rIA)
set_env()

start_date <- 20171106
end_date <- 20180126

# self cancel extract -----------------------------------------------------

connSUN <- odbcConnect(dsn = 'Sunsysh', uid = 'CCFLMZA', pwd = 'SAB2012')
sba_selfcancels <- sqlQuery(connSUN, paste0("SELECT DISTINCT *
                            FROM SFBASLIB.SFSBACAN
                            Where 1=1
                            AND CLCRDT between ",start_date," and  ",end_date,""), stringsAsFactors = F)
odbcClose(connSUN)

cancel_orders <- sba_selfcancels %>%
  mutate(fail = ifelse(CLSUCSS == 'N', 1, 0),
         #fail_nomaint = ifelse(CLSUCSS == 'N' & str_detect(CLFRESN, c('status of Cancelled and can not be maintained')), 1, 0),
         success = ifelse(CLSUCSS == 'Y', 1, 0)) %>%
  select(`CLORD#`, success, fail) %>%
  distinct()

cancel_orders_flags <- cancel_orders %>%
  group_by(`CLORD#`) %>%
  summarise(success = max(success, na.rm = T),
            fail = max(fail, na.rm = T)) %>%
  mutate(success_flag = ifelse(success == 1 | (success == 1 & fail == 1), 1, 0))  %>%
  ungroup()
  #select(-fail_nomaint)
 # filter(success == 1)


# csi extract -------------------------------------------------------------

sba_csi <- query_csi(bu = 'sba',start_date = as.Date.character(start_date, format = "%Y%m%d"), end_date = as.Date.character(end_date, format = "%Y%m%d"), simple = T, include_calendar = F) %>%
  select(contact_date, contact_date_time, order_number, topic, type) %>%
  filter(!is.na(order_number))


# join and sum data  -----------------------------------------------------

df_can_csi <- left_join(cancel_orders_flags, sba_csi, by = c("CLORD#" = "order_number")) %>%
  mutate(csi_contact = ifelse(!is.na(contact_date), 1, 0))


cr_summary <- df_can_csi %>%
  group_by(`CLORD#`,success_flag) %>%
  summarise(total_orders = n(),
            total_contacts = sum(csi_contact, na.rm = T)) %>%
  group_by( success_flag) %>%
  summarise(total_orders = n(),
            total_contacts = sum(total_contacts, na.rm = T)) %>%
  mutate(contact_rate = total_contacts/total_orders)

topic_summary <- df_can_csi %>%
  group_by(topic,success_flag) %>%
  summarise(total_contacts = sum(csi_contact, na.rm = T)) %>%
  group_by(success_flag) %>%
  mutate(success_flag_contacts = sum(total_contacts, na.rm = T),
         mix_perc = total_contacts/success_flag_contacts,
         topic = ifelse(is.na(topic), 'Transfer', as.character(topic))) %>%
  arrange(success_flag, desc(mix_perc))


# extract order data ------------------------------------------------------

connGDW <- odbcConnect("gdw")
sba_orders <- sqlQuery(connGDW, paste0("
                                        SELECT distinct
                                        t1.Ohord#,
                                        t1.Ohcrdt,
                                        t1.Ohcrtm,
                                        t1.Ohordm, 
                                        max(case when t2.ORTYPE = 'SK' then 1 else 0 end) as stock,
                                        max(case when t2.ORDROP = 'Y' then 1 else 0 end) as dropship
                                        FROM	PRD_USD_OPV.SUNRISE_SFBASLIB_SFORDH_V t1
                                        join PRD_USD_OPV.SUNRISE_SFBASLIB_SFORDR_V  t2  on ( t1.OHORD#=t2.ORORD# AND t1.OHLINK=t2.ORLINK)
                                        WHERE 
                                        t1.Ohlink=0 AND
                                        t1.Ohtype in ('OR', 'QU', 'MO') AND 
                                        t1.Ohcrdt between ",start_date," and ",end_date,"
                                        GROUP BY 1,2,3,4"), stringsAsFactors = F)
odbcClose(connGDW)
sba_orders$mix <- ifelse(sba_orders$stock == 1 & sba_orders$dropship == 1, 1, 0)

# join oms to cancels and calc time diff  ---------------------------------

can_oms <- left_join(sba_selfcancels, sba_orders, by = c("CLORD#" = "Ohord#")) %>%
  filter(!is.na(Ohcrdt))

cat("\ Shelby wanted a quick look at the failed self cancels to see if the EDI orders are a driver - it's not")
View(prop.table(table(can_oms$Ohordm[can_oms$CLSUCSS == 'N'])))

can_oms_clean <- can_oms %>%
  mutate(CLCRDT = as.Date.character(CLCRDT, format = "%Y%m%d"),
         Ohcrdt = as.Date.character(Ohcrdt, format = "%Y%m%d"),
         CLCRTM =sprintf("%06.0f",CLCRTM),
         Ohcrtm = sprintf("%06.0f",Ohcrtm), 
         cancel_ts = as.POSIXct(paste(CLCRDT, CLCRTM), format = "%Y-%m-%d %H%M%S"),
         order_ts = as.POSIXct(paste(Ohcrdt, Ohcrtm), format = "%Y-%m-%d %H%M%S"),
         time_diff = as.numeric(difftime(cancel_ts, order_ts, units = "mins")),
         SelfCancel_Success = as.factor(CLSUCSS),
         dow_cancel = weekdays(cancel_ts),
         hour_cancel = as.numeric(format(cancel_ts, "%H")), 
         during_working_hrs = ifelse((hour_cancel >= 8 & hour_cancel <= 20) & !(dow_cancel %in% c('Saturday', 'Sunday')), 'Working Hours', 'Non Working Hours' ),
         order_type = ifelse(mix == 1, 'mix', ifelse(dropship == 1, 'dropship', 'stock'))) %>%
  arrange(`CLORD#`, cancel_ts) %>%
  group_by(`CLORD#`) %>%
  filter(row_number()==1) %>%
  select(-(CLLINK:CLSSEQ), -CLCRUSR, -(CLCHUSR:CLCHTM), -(stock:mix))


# summarize ---------------------------------------------------------------
cat('\breakdown of Successful vs Failed Self Cancels by Working & Non Working Hours')
table(can_oms_clean$during_working_hrs, can_oms_clean$CLSUCSS)
round(prop.table(table(can_oms_clean$during_working_hrs, can_oms_clean$CLSUCSS),1), 2)

cat('\breakdown of Successful vs Failed Self Cancels by Order Type')
table(can_oms_clean$order_type, can_oms_clean$CLSUCSS)
round(prop.table(table(can_oms_clean$order_type, can_oms_clean$CLSUCSS),1), 2)

time_summary <- can_oms_clean %>%
  group_by(SelfCancel_Success) %>%
  summarise(total_ords = n(),
            mean_mins = mean(time_diff, na.rm = T),
            percentile_15 = quantile(time_diff, probs = .15),
            percentile_25 = quantile(time_diff, probs = .25),
            percentile_40 = quantile(time_diff, probs = .40),
            median_mins = median(time_diff, na.rm = T),
            percentile_75 = quantile(time_diff, probs = .75)) %>%
  ungroup()


# plot histograms ---------------------------------------------------------

 ggplot(data = can_oms_clean, aes(x = time_diff, fill = SelfCancel_Success)) + 
  geom_histogram(binwidth = 10,  alpha=.5) +
  facet_grid(SelfCancel_Success ~ .) +
  theme_bw() +
  labs(title = "SBA Fail vs. Successful Self-Cancels: Time between Order and Cancel Attempt", y = "Total Orders", x = "Time difference b/w Order and Cancel (mins)") +
  xlim(-30, 300) +
  geom_vline(data = time_summary, aes(xintercept = median_mins, colour = SelfCancel_Success), linetype = "dashed", size = 1)
 

# export ------------------------------------------------------------------

write.csv(can_oms_clean[can_oms_clean$CLSUCSS == 'N',], "SBA Failed Self-Cancels.csv", row.names = F)
write.csv(time_summary, file = "Time Diff bw Order and Cancel.csv", row.names = F)
write.csv(cr_summary, file = "SBA SelfCan Contact Rates.csv", row.names = F)
write.csv(topic_summary, file = "SBA SelfCan Contact Mix by Topic.csv", row.names = F)
ggsave(width = 8, height = 4.5, units = "in", filename = "SBA Fail vs Success SelfCancels Time Difference.png")
