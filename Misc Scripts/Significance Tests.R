library(rIA)

set_env(dir = 'C:/Users/VezAn001/Documents/BIPS/Projects/Notifications')

df <- read.csv('Frequency Tables_Notified vs NotNotified.csv', stringsAsFactors = F)

df_cm <- df

tbl_cm <- table(df_cm$ContactMethod,df_cm$Group)
        prop.test(tbl_cm)
        pair_cm <- pairwise.prop.test(tbl_cm, p.adjust.method = "bonferroni")
                pair_cm <- as.data.frame(pair_cm[[3]])
                pair_cm$Group <- "Notified vs. Not Notified"
        
tbl_topic <- table(df_cm$Topic, df_cm$Group)
        prop.test(tbl_topic)
        pair_topic <- pairwise.prop.test(tbl_topic, p.adjust.method = "bonferroni")
                pair_topic <- as.data.frame(pair_topic[[3]])
                pair_topic$Group <- "Notified vs. Not Notified"
        
df2 <- read.csv('Frequency Tables_NotNotified_vs_NotInteracted.csv', stringsAsFactors = F)

tbl2_cm <- table(df2$ContactMethod,df2$Group)
        prop.test(tbl2_cm)
        pair2_cm <- pairwise.prop.test(tbl2_cm, p.adjust.method = "bonferroni")
                pair2_cm <- as.data.frame(pair2_cm[[3]])
                pair2_cm$Group <- "Not Notified vs. Notified, Not Interacted"

tbl2_topic <- table(df2$Topic,df2$Group)
        prop.test(tbl2_topic)
        pair2_topic <- pairwise.prop.test(tbl2_topic, p.adjust.method = "bonferroni")
                pair2_topic <- as.data.frame(pair2_topic[[3]])
                pair2_topic$Group <- "Not Notified vs. Notified, Not Interacted"
        
        
df_notified <- df[df$Group == 'Notified',]
        df_notified$Group <- 'Notified and Interacted'
        df_notified_sub <- df2[df2$Group == 'Notified',]
        df_notified_sub$Group <- 'Notified w/ No Interaction'
        df_notified <- rbind(df_notified,df_notified_sub)

tbl3_cm <- table(df_notified$ContactMethod, df_notified$Group)
        prop.test(tbl3_cm)
        pair3_cm <- pairwise.prop.test(tbl3_cm, p.adjust.method = "bonferroni")
                pair3_cm <- as.data.frame(pair3_cm[[3]])
                pair3_cm$Group <- "Notified, Interacted vs. Notified, Not Interacted"
        
tbl3_topic <- table(df_notified$Topic, df_notified$Group)
        prop.test(tbl3_topic)
        pair3_topic <- pairwise.prop.test(tbl3_topic, p.adjust.method = "bonferroni")
                pair3_topic <- as.data.frame(pair3_topic[[3]])
                pair3_topic$Group <- "Notified, Interacted vs. Notified, Not Interacted"

df_master <- df
        df_master <- rbind(df_master,df_notified_sub)
        
df_master_cm <- rbind(pair_cm,pair2_cm,pair3_cm)

df_master_topic <- rbind(pair_topic,pair2_topic,pair3_topic)
        
write.csv(df_master,"Notifications Contact Info.csv",row.names = F)
write.csv(df_master_cm,"P Values Contact Method.csv",row.names = T)
write.csv(df_master_topic,"P Values Topics.csv",row.names = T)


hsb2<-read.table("http://www.ats.ucla.edu/stat/data/hsb2.csv", sep=",", header=T)
        

        