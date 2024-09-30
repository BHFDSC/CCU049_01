### Health service use for each cohort and comparison
### Produce CCU049_01 Table 3

# onlycovid - covid only, non Long Covid
# nocovidc - non-covid (contemporary)
# nocovidh - pre-pandemic non-covid (historical)
# longcoh - Long Covid (historical)

install.packages("epitools")
install.packages("ggplot2")
install.packages("forestplot2")
library(epitools)
library(ggplot2)
library(stringr)
library(dplyr)
library(odbc)

## Mathced cohort healthcare use data in 
## Home Folder/CCU049/matched_use_data_v2
library(DBI)

setwd("D:/PhotonUser/My Files/Home Folder/ccu049")


## To load main tables
# Each of the following tables contains both the study group (long COVID) and the matched control group
load("D:/PhotonUser/My Files/Home Folder/ccu049/matched_use_data_v2/longcoh_both.rdata")
load("D:/PhotonUser/My Files/Home Folder/ccu049/matched_use_data_v2/nocovidc_both.rdata")
load("D:/PhotonUser/My Files/Home Folder/ccu049/matched_use_data_v2/nocovidh_both.rdata")
load("D:/PhotonUser/My Files/Home Folder/ccu049/matched_use_data_v2/onlycovid_both.rdata")


# Rename the table used for analysis as 'test' and with an age filter (age>=18)
test <- longcoh_all %>% filter(age>=18)
test <- nocovidh_all %>% filter(age>=18)
test <- onlycovid_all %>% filter(age>=18)
test <- nocovidc_all %>% filter(age>=18)


##### For Reviewer's comments regarding overlap between control group #####
#nocovidc_control <- subset(control, select = c(id))

#longcoh_nocovidh <- rbind(longcoh_control, nocovidh_control)
#longcoh_nocovidc <- rbind(longcoh_control, nocovidc_control)
#longcoh_onlycovid <- rbind(longcoh_control, onlycovid_control)
#onlycovid_nocovidh <- rbind(onlycovid_control, nocovidh_control)
#onlycovid_nocovidc <- rbind(onlycovid_control, nocovidc_control)
#nocovidh_nocovidc <- rbind(nocovidh_control, nocovidc_control)

#duplicate_count <- nocovidh_nocovidc %>%
#  group_by(id) %>%
#  filter(n()>1) %>%
#  ungroup()

#print(duplicate_count)


##### For Reviewer's comments regarding sample balance #####
#load("D:/PhotonUser/My Files/Home Folder/ccu049/matched_base_data/nocovidc_co_matched.rdata")
#load("D:/PhotonUser/My Files/Home Folder/ccu049/cohort_base_data/longc_co_base.rdata")

#nocovidh_co_
#nocovidh_co_matched <- left_join(nocovidh_co_matched1, nocovidh_co_matched2, by = c("case_id","control_id","n"))


#nocovidc_co_matched <- left_join(nocovidc_co_matched1, nocovidc_co_matched2, by = c("case_id","control_id","n"))
#nocovidc_co_matched <- left_join(nocovidc_co_matched, nocovidc_co_matched4, by = c("case_id","control_id","n"))
#longc_co <- dbGetQuery(con,'SELECT id,age,agecat,sex,ethnicity,white,imd,region,cvd,ht,dm,copd,asthma,
#                       depression,cancer,ckd,obese,dementia from dars_nic_391419_j3w9t_collab.ccu049_longc')
#nocovidc_co_matched <- nocovidc_co_matched %>% 
#  mutate(cvd = as.integer(ami==1|hf==1|af==1|stroke==1|cdy==1|pe==1|dvt==1|pad==1))
#cvd <- nocovidc_co_matched %>% filter(cvd==1)
#nocovidc_co <- subset(nocovidc_co_matched, select=-c(case_id,n,region42,smk,ami,hf,af,stroke,cdy,pe,dvt,pad))
#nocovidc_co <- nocovidc_co %>%
#  mutate(white = ifelse(ethnicity =="White",1,0))

#longc_co <- longc_co %>% select(id,age,agecat,sex,ethnicity,white,imd,region,cvd,ht,dm,copd,asthma,
#                                depression,cancer,ckd,obese,dementia)
#nocovidc_co <- nocovidc_co %>%
#  rename(
#    id=control_id
#  )
#nocovidc_co <- nocovidc_co %>% select (id,age,agecat,sex,ethnicity,white,imd,region,cvd,ht,dm,copd,asthma,
#                                       depression,cancer,ckd,obese,dementia)
#longc_co$treat <-1
#nocovidc_co$treat <- 0
#nocovidc_case_control_base <- rbind(longc_co,nocovidc_co)

#nocovidc_case_control_base[is.na(nocovidc_case_control_base)] = 0

#nocovidc_case_control_base$imd <- factor(nocovidc_case_control_base$imd)
#nocovidc_case_control_base$region <- factor(nocovidc_case_control_base$region)


#model <- glm(treat ~ age+sex+white+imd+region+cvd+ht+dm+copd+asthma+depression+cancer+ckd+obese+dementia, data=nocovidc_case_control_base, family=binomial())
#model <- glm(treat ~ age+sex+white+cvd+ht+dm+copd+asthma+depression+cancer+ckd+obese+dementia, data=nocovidc_case_control_base, family=binomial())

#model <- lm(treat ~ age+sex+white+imd+region+cvd+ht+dm+copd+asthma+depression+cancer+ckd+obese+dementia, data=nocovidc_case_control_base)
#model <- lm(treat ~ age+sex+white+imd+region, data=nocovidc_case_control_base)
#model <- lm(dementia ~ treat,data=nocovidc_case_control_base)

#summary(model)

#save(longc_co, file = "D:/PhotonUser/My Files/Home Folder/ccu049/matched_base_data/longc_co.rdata")
#save(nocovidc_co, file = "D:/PhotonUser/My Files/Home Folder/ccu049/matched_base_data/nocovidc_co_new.rdata")

# First, replace NA by 0
test[is.na(test)] = 0

# Second, calculate the follow-up period in months and years based on follow-up in days
test$followup_mon <- test$followup_day/30.44
test$followup_yr <- test$followup_day/365.25



# Make sure no one has negative follow up (this might happen due to error in the data)
# Check number of people who had a negative (<0) followup period
# After checking, found a few patients died before their first COVID record in COVID only group, revised their follow-up period to 0
sum(test$followup_mon<0)
test$followup_mon[test$followup_mon<0]=0
test$followup_yr[test$followup_yr<0]=0

# Descriptive statistics for the matched control groups
test %>% summarise(mean = sprintf("%0.3f",mean(age)), sd = sprintf("%0.3f",sd(age)), min = sprintf("%0.3f",min(age)), max = sprintf("%0.3f",max(age)))

sum(!is.na(test$dod)) # number of people who died

test %>%
  group_by(treat) %>%
  summarise(mean = sprintf("%0.3f",mean(followup_mon)), sd = sprintf("%0.3f",sd(followup_mon)), min = sprintf("%0.3f",min(followup_mon)), max = sprintf("%0.3f",max(followup_mon)))

test %>%
  group_by(treat) %>%
  summarise(mean = sprintf("%0.3f",mean(followup_yr)), sd = sprintf("%0.3f",sd(followup_yr)), min = sprintf("%0.3f",min(followup_yr)), max = sprintf("%0.3f",max(followup_yr)))



#### Calculate per month and per year health service use for each patient ####
##(when the followup period is 0, just replace the utilisation with 0)

# Number of GP appointments per month
test$gp_mon <- test$gp_all/test$followup_mon
test$gp_mon[test$followup_mon==0]<-0
# Number of OP appointments per month
test$op_mon <- test$op_all/test$followup_mon
test$op_mon[test$followup_mon==0]<-0
# Number of hospital admissions per month
test$apc_mon <- test$apc_all/test$followup_mon
test$apc_mon[test$followup_mon==0]<-0
# Days in hospital general wards (not in critical care) per month
test$apc_dur_mon <- test$apc_dur/test$followup_mon
test$apc_dur_mon[test$followup_mon==0]<-0
# Days in critical care per month
test$cc_dur_mon <- test$cc_dur/test$followup_mon
test$cc_dur_mon[test$followup_mon==0]<-0
# Number of ED attendance per month
test$ecds_mon <- test$ecds_all/test$followup_mon
test$ecds_mon[test$followup_mon==0]<-0
# Number of advanced cardiovascular support days per month (only used for cost)
test$acard_mon <- test$acard_all/test$followup_mon
test$acard_mon[test$followup_mon==0]<-0
# Number of basic cardiovascular support days per month (only used for cost)
test$bcard_mon <- test$bcard_all/test$followup_mon
test$bcard_mon[test$followup_mon==0]<-0
# Number of advanced respiratory support days per month (only used for cost)
test$ares_mon <- test$ares_all/test$followup_mon
test$ares_mon[test$followup_mon==0]<-0
# Number of basic respiratory support days per month (only used for cost)
test$bres_mon <- test$bres_all/test$followup_mon
test$bres_mon[test$followup_mon==0]<-0
# Number of critical care level 2 days per month (only used for cost)
test$lev2_mon <- test$lev2_all/test$followup_mon
test$lev2_mon[test$followup_mon==0]<-0
# Number of critical care level 3 days per month (only used for cost)
test$lev3_mon <- test$lev3_all/test$followup_mon
test$lev3_mon[test$followup_mon==0]<-0


# Number of GP appointments per year
test$gp_yr <- test$gp_all/test$followup_yr
test$gp_yr[test$followup_yr==0]<-0
# Number of OP appointments per year
test$op_yr <- test$op_all/test$followup_yr
test$op_yr[test$followup_yr==0]<-0
# Number of hospital admissions per year
test$apc_yr <- test$apc_all/test$followup_yr
test$apc_yr[test$followup_yr==0]<-0
# Days in hospital general wards (not in critical care) per year
test$apc_dur_yr <- test$apc_dur/test$followup_yr
test$apc_dur_yr[test$followup_yr==0]<-0
# Days in critical care per year
test$cc_dur_yr <- test$cc_dur/test$followup_yr
test$cc_dur_yr[test$followup_yr==0]<-0
# Number of ED attendance per year
test$ecds_yr <- test$ecds_all/test$followup_yr
test$ecds_yr[test$followup_yr==0]<-0
# Number of advanced cardiovascular support days per year (only used for cost)
test$acard_yr <- test$acard_all/test$followup_yr
test$acard_yr[test$followup_yr==0]<-0
# Number of basic cardiovascular support days per year (only used for cost)
test$bcard_yr <- test$bcard_all/test$followup_yr
test$bcard_yr[test$followup_yr==0]<-0
# Number of advanced respiratory support days per year (only used for cost)
test$ares_yr <- test$ares_all/test$followup_yr
test$ares_yr[test$followup_yr==0]<-0
# Number of basic respiratory support days per year (only used for cost)
test$bres_yr <- test$bres_all/test$followup_yr
test$bres_yr[test$followup_yr==0]<-0
# Number of critical care level 2 days per year (only used for cost)
test$lev2_yr <- test$lev2_all/test$followup_yr
test$lev2_yr[test$followup_yr==0]<-0
# Number of critical care level 3 days per year (only used for cost)
test$lev3_yr <- test$lev3_all/test$followup_yr
test$lev3_yr[test$followup_yr==0]<-0




#### Create columns for OP treatment specialty, tests/scans/procedures in OP and IP ####
## from the aggregated columns 'op_combined_tret' and 'apc_combined_tret'

test <- test %>%
  mutate(
    op.340 = str_count(test$op_combined_tret, "340"),
    op.650 = str_count(test$op_combined_tret, "650"),
    op.320 = str_count(test$op_combined_tret, "320"),
    op.812 = str_count(test$op_combined_tret, "812"),
    op.110 = str_count(test$op_combined_tret, "110"),
    op.130 = str_count(test$op_combined_tret, "130"),
    op.502 = str_count(test$op_combined_tret, "502"),
    op.120 = str_count(test$op_combined_tret, "120"),
    op.410 = str_count(test$op_combined_tret, "410"),
    op.330 = str_count(test$op_combined_tret, "330"),
    op.301 = str_count(test$op_combined_tret, "301"),
    op.101 = str_count(test$op_combined_tret, "101"),
    op.303 = str_count(test$op_combined_tret, "303"),
    op.400 = str_count(test$op_combined_tret, "400"),
    op.100 = str_count(test$op_combined_tret, "100"),
    op.560 = str_count(test$op_combined_tret, "560"),
    op.501 = str_count(test$op_combined_tret, "501"),
    op.300 = str_count(test$op_combined_tret, "300"),
    op.341 = str_count(test$op_combined_tret, "341"),
    op.191 = str_count(test$op_combined_tret, "191"),
    op.348 = str_count(test$op_combined_tret, "348"),
    
    op.u354 = str_count(test$op_combined_opcs, "U354"),
    op.u201 = str_count(test$op_combined_opcs, "U201"),
    op.u051 = str_count(test$op_combined_opcs, "U051"),
    op.u052 = str_count(test$op_combined_opcs, "U052"),
    
    apc.u354 = str_count(test$op_combined_opcs, "U354"),
    apc.u201 = str_count(test$op_combined_opcs, "U201"),
    apc.u051 = str_count(test$op_combined_opcs, "U051"),
    apc.u052 = str_count(test$op_combined_opcs, "U052"),
    apc.g451 = str_count(test$op_combined_opcs, "G451"),
    apc.g459 = str_count(test$op_combined_opcs, "G459"),
    apc.h221 = str_count(test$op_combined_opcs, "H221"),
    apc.h229 = str_count(test$op_combined_opcs, "H229")
    
  )







#### Calculate the MEAN and SD for health service use and cost per month and t-test ####

# First, need to remove those whose follow-up period are non-positive.
test <- test[test$followup_mon > 0, ] 

# Calculate the mean and sd for each category per month




#hist(treat$gp_yr, breaks=500, main = "Histogram of Values", xlab = "Values", col = "blue", border = "black")

#test %>%
#  filter(treat == 1) %>%
#  ggplot(aes(x = apc_dur_yr)) +
#  geom_histogram(bins = 500, fill = "blue", color = "black") +
#  ggtitle("Histogram of Column A where Column B == 1") +
#  xlab("Column A") +
#  ylab("Frequency")





test %>%
  group_by(treat) %>%
  summarise(mean = sprintf("%0.3f",mean(gp_mon)), sd = sprintf("%0.3f",sd(gp_mon)))


test %>%
  group_by(treat) %>%
  summarise(mean = sprintf("%0.3f",mean(op_mon)), sd = sprintf("%0.3f",sd(op_mon)))

test %>%
  group_by(treat) %>%
  summarise(mean = sprintf("%0.3f",mean(apc_mon)), sd = sprintf("%0.3f",sd(apc_mon)))

test %>%
  group_by(treat) %>%
  summarise(mean = sprintf("%0.3f",mean(apc_dur_mon)), sd = sprintf("%0.3f",sd(apc_dur_mon)))

test %>%
  group_by(treat) %>%
  summarise(mean = sprintf("%0.3f",mean(cc_dur_mon)), sd = sprintf("%0.3f",sd(cc_dur_mon)))


test %>%
  group_by(treat) %>%
  summarise(mean = sprintf("%0.3f",mean(ecds_mon)), sd = sprintf("%0.3f",sd(ecds_mon)))



# Critical Care (the following mean and sd was not used in Table 3, as otherwise the table would be too long)
test %>%
  group_by(treat) %>%
  summarise(mean = sprintf("%0.3f",mean(bres_mon)), sd = sprintf("%0.3f",sd(bres_mon)))

test %>%
  group_by(treat) %>%
  summarise(mean = sprintf("%0.3f",mean(ares_mon)), sd = sprintf("%0.3f",sd(ares_mon)))

test %>%
  group_by(treat) %>%
  summarise(mean = sprintf("%0.3f",mean(bcard_mon)), sd = sprintf("%0.3f",sd(bcard_mon)))

test %>%
  group_by(treat) %>%
  summarise(mean = sprintf("%0.3f",mean(acard_mon)), sd = sprintf("%0.3f",sd(acard_mon)))

test %>%
  group_by(treat) %>%
  summarise(mean = sprintf("%0.3f",mean(lev2_mon)), sd = sprintf("%0.3f",sd(lev2_mon)))

test %>%
  group_by(treat) %>%
  summarise(mean = sprintf("%0.3f",mean(lev3_mon)), sd = sprintf("%0.3f",sd(lev3_mon)))



## Find average monthly utilisation for each outpatient treatment specialty, tests and procedures in outpatient and admitted patient care
## These were only used for calculating cost.

test$op.340.n <- test$op.340/test$followup_mon
test$op.650.n <- test$op.650/test$followup_mon
test$op.320.n <- test$op.320/test$followup_mon
test$op.812.n <- test$op.812/test$followup_mon
test$op.110.n <- test$op.110/test$followup_mon
test$op.130.n <- test$op.130/test$followup_mon
test$op.502.n <- test$op.502/test$followup_mon
test$op.120.n <- test$op.120/test$followup_mon
test$op.410.n <- test$op.410/test$followup_mon
test$op.330.n <- test$op.330/test$followup_mon
test$op.301.n <- test$op.301/test$followup_mon
test$op.101.n <- test$op.101/test$followup_mon
test$op.303.n <- test$op.303/test$followup_mon
test$op.400.n <- test$op.400/test$followup_mon
test$op.100.n <- test$op.100/test$followup_mon
test$op.560.n <- test$op.560/test$followup_mon
test$op.501.n <- test$op.501/test$followup_mon
test$op.300.n <- test$op.300/test$followup_mon
test$op.341.n <- test$op.341/test$followup_mon
test$op.191.n <- test$op.191/test$followup_mon
test$op.348.n <- test$op.348/test$followup_mon

test$op.u354.n <- test$op.u354/test$followup_mon
test$op.u201.n <- test$op.u201/test$followup_mon
test$op.u051.n <- test$op.u051/test$followup_mon
test$op.u052.n <- test$op.u052/test$followup_mon


test$apc.u354.n <- test$apc.u354/test$followup_mon
test$apc.u201.n <- test$apc.u201/test$followup_mon
test$apc.g451.n <- test$apc.g451/test$followup_mon
test$apc.g459.n <- test$apc.g459/test$followup_mon
test$apc.h229.n <- test$apc.h229/test$followup_mon
test$apc.h221.n <- test$apc.h221/test$followup_mon
test$apc.u051.n <- test$apc.u051/test$followup_mon
test$apc.u052.n <- test$apc.u052/test$followup_mon

# If the follow-up period is 0, then replace the utilisation with 0 (otherwise it will be infinity!)

test$op.340.n[test$followup_mon==0]<-0
test$op.650.n[test$followup_mon==0]<-0
test$op.320.n[test$followup_mon==0]<-0
test$op.812.n[test$followup_mon==0]<-0
test$op.110.n[test$followup_mon==0]<-0
test$op.130.n[test$followup_mon==0]<-0
test$op.502.n[test$followup_mon==0]<-0
test$op.120.n[test$followup_mon==0]<-0
test$op.410.n[test$followup_mon==0]<-0
test$op.330.n[test$followup_mon==0]<-0
test$op.301.n[test$followup_mon==0]<-0
test$op.101.n[test$followup_mon==0]<-0
test$op.303.n[test$followup_mon==0]<-0
test$op.400.n[test$followup_mon==0]<-0
test$op.100.n[test$followup_mon==0]<-0
test$op.560.n[test$followup_mon==0]<-0
test$op.501.n[test$followup_mon==0]<-0
test$op.300.n[test$followup_mon==0]<-0
test$op.341.n[test$followup_mon==0]<-0
test$op.191.n[test$followup_mon==0]<-0
test$op.348.n[test$followup_mon==0]<-0


test$op.u354.n[test$followup_mon==0]<-0
test$op.u201.n[test$followup_mon==0]<-0
test$op.u051.n[test$followup_mon==0]<-0
test$op.u052.n[test$followup_mon==0]<-0

test$apc.u354.n[test$followup_mon==0]<-0
test$apc.u201.n[test$followup_mon==0]<-0
test$apc.g451.n[test$followup_mon==0]<-0
test$apc.g459.n[test$followup_mon==0]<-0
test$apc.h229.n[test$followup_mon==0]<-0
test$apc.h221.n[test$followup_mon==0]<-0
test$apc.u051.n[test$followup_mon==0]<-0
test$apc.u052.n[test$followup_mon==0]<-0

## Calculate monthly cost based on unit cost

test <- test %>% mutate(cost_mon = gp_mon*34+op.340.n*215+op.650.n*122
                        +op.320.n*196+op.812.n*51+op.110.n*192
                        +op.130.n*173+op.502.n*211+op.120.n*177
                        +op.410.n*180+op.330.n*173+op.301.n*169
                        +op.101.n*148+op.303.n*198 +op.400.n*212
                        +op.100.n*185+op.560.n*121+op.501.n*191
                        +op.300.n*217+op.341.n*163+op.191.n*244
                        +op.u354.n*136.1+op.u201.n*90+op.u051.n*102 
                        +op.u052.n*181+apc.u354.n*136+apc.u201.n*90
                        +apc.g451.n*215+apc.g459.n*303+apc.h221.n*303 
                        +apc.h229.n*225+apc.u051.n*102+apc.u052.n*181
                        +apc_dur_mon*650+cc_dur_mon*2621+ecds_mon*276)

# mean and sd of the monthly cost
test %>%
  group_by(treat) %>%
  summarise(mean = sprintf("%0.3f",mean(cost_mon)), sd = sprintf("%0.3f",sd(cost_mon)))


treat <- test %>% filter(treat==1)
control <- test %>% filter(treat==0)

# Median and IQR for utilisation and Cost
summary(test$gp_mon[test$treat == 1])
summary(test$op_mon[test$treat == 1])
summary(test$apc_mon[test$treat == 1])
summary(test$apc_dur_mon[test$treat == 1])
summary(test$cc_dur_mon[test$treat == 1])
summary(test$ecds_mon[test$treat == 1])
summary(test$cost_mon[test$treat == 1])

summary(test$gp_yr[test$treat == 1])
summary(test$op_yr[test$treat == 1])
summary(test$apc_yr[test$treat == 1])
summary(test$apc_dur_yr[test$treat == 1])
summary(test$cc_dur_yr[test$treat == 1])
summary(test$ecds_yr[test$treat == 1])
summary(test$cost_yr[test$treat == 1])

summary(test$gp_mon[test$treat == 0])
summary(test$op_mon[test$treat == 0])
summary(test$apc_mon[test$treat == 0])
summary(test$apc_dur_mon[test$treat == 0])
summary(test$cc_dur_mon[test$treat == 0])
summary(test$ecds_mon[test$treat == 0])
summary(test$cost_mon[test$treat == 0])

summary(test$gp_yr[test$treat == 0])
summary(test$op_yr[test$treat == 0])
summary(test$apc_yr[test$treat == 0])
summary(test$apc_dur_yr[test$treat == 0])
summary(test$cc_dur_yr[test$treat == 0])
summary(test$ecds_yr[test$treat == 0])
summary(test$cost_yr[test$treat == 0])


wilcox.test(treat$gp_mon, control$gp_mon,alternative="greater", paired=F)

wilcox.test(control$gp_mon, treat$gp_mon,alternative="greater", paired=F)



wilcox.test(treat$gp_mon, control$gp_mon,alternative="greater", paired=F)
wilcox.test(treat$op_mon, control$op_mon,alternative="greater", paired=F)
wilcox.test(treat$apc_mon, control$apc_mon,alternative="greater", paired=F)
wilcox.test(treat$apc_dur_mon, control$apc_dur_mon,alternative="greater", paired=F)
wilcox.test(treat$cc_dur_mon, control$cc_dur_mon,alternative="greater", paired=F)
wilcox.test(treat$ecds_mon, control$ecds_mon,alternative="greater", paired=F)
wilcox.test(treat$cost_mon, control$cost_mon,alternative="greater", paired=F)

# Last, Mann-Whitney U Test

u_gp <- wilcox.test(gp_mon ~ treat, data = test, alternative = "greater", paired = F)
u_gp
u_op <- wilcox.test(op_mon ~ treat, data = test, alternative = "greater", paired = F)
u_op
u_apc <- wilcox.test(apc_mon ~ treat, data = test, alternative = "greater", paired = F)
u_apc
u_apc_dur <- wilcox.test(apc_dur_mon ~ treat, data = test, alternative = "greater", paired = F)
u_apc_dur
u_cc_dur <- wilcox.test(cc_dur_mon ~ treat, data = test, alternative = "greater", paired = F)
u_cc_dur
u_ecds <- wilcox.test(ecds_mon ~ treat, data = test, alternative = "greater", paired = F)
u_ecds
u_cost_mon <- wilcox.test(cost_mon ~ treat, data = test, alternative = "greater", paired = F)
u_cost_mon






#### Calculate the MEAN and SD for health service use and cost per year and t-test ####

# Calculate the mean and sd for each category per year

test %>%
  group_by(treat) %>%
  summarise(mean = sprintf("%0.3f",mean(gp_yr)), sd = sprintf("%0.3f",sd(gp_yr)))

test %>%
  group_by(treat) %>%
  summarise(mean = sprintf("%0.3f",mean(op_yr)), sd = sprintf("%0.3f",sd(op_yr)))

test %>%
  group_by(treat) %>%
  summarise(mean = sprintf("%0.3f",mean(apc_yr)), sd = sprintf("%0.3f",sd(apc_yr)))

test %>%
  group_by(treat) %>%
  summarise(mean = sprintf("%0.3f",mean(apc_dur_yr)), sd = sprintf("%0.3f",sd(apc_dur_yr)))

test %>%
  group_by(treat) %>%
  summarise(mean = sprintf("%0.3f",mean(cc_dur_yr)), sd = sprintf("%0.3f",sd(cc_dur_yr)))

test %>%
  group_by(treat) %>%
  summarise(mean = sprintf("%0.3f",mean(ecds_yr)), sd = sprintf("%0.3f",sd(ecds_yr)))


# Critical Care (the following mean and sd was not used in Table 3, as otherwise the table would be too long)
test %>%
  group_by(treat) %>%
  summarise(mean = sprintf("%0.3f",mean(bres_yr)), sd = sprintf("%0.3f",sd(bres_yr)))

test %>%
  group_by(treat) %>%
  summarise(mean = sprintf("%0.3f",mean(ares_yr)), sd = sprintf("%0.3f",sd(ares_yr)))

test %>%
  group_by(treat) %>%
  summarise(mean = sprintf("%0.3f",mean(bcard_yr)), sd = sprintf("%0.3f",sd(bcard_yr)))

test %>%
  group_by(treat) %>%
  summarise(mean = sprintf("%0.3f",mean(acard_yr)), sd = sprintf("%0.3f",sd(acard_yr)))

test %>%
  group_by(treat) %>%
  summarise(mean = sprintf("%0.3f",mean(lev2_yr)), sd = sprintf("%0.3f",sd(lev2_yr)))

test %>%
  group_by(treat) %>%
  summarise(mean = sprintf("%0.3f",mean(lev3_yr)), sd = sprintf("%0.3f",sd(lev3_yr)))




## Find average yearly utilisation for each outpatient treatment specialty, tests and procedures in outpatient and admitted patient care
## These were only used for calculating cost.

test$op.340.n <- test$op.340/test$followup_yr
test$op.650.n <- test$op.650/test$followup_yr
test$op.320.n <- test$op.320/test$followup_yr
test$op.812.n <- test$op.812/test$followup_yr
test$op.110.n <- test$op.110/test$followup_yr
test$op.130.n <- test$op.130/test$followup_yr
test$op.502.n <- test$op.502/test$followup_yr
test$op.120.n <- test$op.120/test$followup_yr
test$op.410.n <- test$op.410/test$followup_yr
test$op.330.n <- test$op.330/test$followup_yr
test$op.301.n <- test$op.301/test$followup_yr
test$op.101.n <- test$op.101/test$followup_yr
test$op.303.n <- test$op.303/test$followup_yr
test$op.400.n <- test$op.400/test$followup_yr
test$op.100.n <- test$op.100/test$followup_yr
test$op.560.n <- test$op.560/test$followup_yr
test$op.501.n <- test$op.501/test$followup_yr
test$op.300.n <- test$op.300/test$followup_yr
test$op.341.n <- test$op.341/test$followup_yr
test$op.191.n <- test$op.191/test$followup_yr
test$op.348.n <- test$op.348/test$followup_yr

test$op.u354.n <- test$op.u354/test$followup_yr
test$op.u201.n <- test$op.u201/test$followup_yr
test$op.u051.n <- test$op.u051/test$followup_yr
test$op.u052.n <- test$op.u052/test$followup_yr


test$apc.u354.n <- test$apc.u354/test$followup_yr
test$apc.u201.n <- test$apc.u201/test$followup_yr
test$apc.g451.n <- test$apc.g451/test$followup_yr
test$apc.g459.n <- test$apc.g459/test$followup_yr
test$apc.h229.n <- test$apc.h229/test$followup_yr
test$apc.h221.n <- test$apc.h221/test$followup_yr
test$apc.u051.n <- test$apc.u051/test$followup_yr
test$apc.u052.n <- test$apc.u052/test$followup_yr

# If the follow-up period is 0, then replace the utilisation with 0 (otherwise it will be infinity!)

test$op.340.n[test$followup_yr==0]<-0
test$op.650.n[test$followup_yr==0]<-0
test$op.320.n[test$followup_yr==0]<-0
test$op.812.n[test$followup_yr==0]<-0
test$op.110.n[test$followup_yr==0]<-0
test$op.130.n[test$followup_yr==0]<-0
test$op.502.n[test$followup_yr==0]<-0
test$op.120.n[test$followup_yr==0]<-0
test$op.410.n[test$followup_yr==0]<-0
test$op.330.n[test$followup_yr==0]<-0
test$op.301.n[test$followup_yr==0]<-0
test$op.101.n[test$followup_yr==0]<-0
test$op.303.n[test$followup_yr==0]<-0
test$op.400.n[test$followup_yr==0]<-0
test$op.100.n[test$followup_yr==0]<-0
test$op.560.n[test$followup_yr==0]<-0
test$op.501.n[test$followup_yr==0]<-0
test$op.300.n[test$followup_yr==0]<-0
test$op.341.n[test$followup_yr==0]<-0
test$op.191.n[test$followup_yr==0]<-0
test$op.348.n[test$followup_yr==0]<-0


test$op.u354.n[test$followup_yr==0]<-0
test$op.u201.n[test$followup_yr==0]<-0
test$op.u051.n[test$followup_yr==0]<-0
test$op.u052.n[test$followup_yr==0]<-0

test$apc.u354.n[test$followup_yr==0]<-0
test$apc.u201.n[test$followup_yr==0]<-0
test$apc.g451.n[test$followup_yr==0]<-0
test$apc.g459.n[test$followup_yr==0]<-0
test$apc.h229.n[test$followup_yr==0]<-0
test$apc.h221.n[test$followup_yr==0]<-0
test$apc.u051.n[test$followup_yr==0]<-0
test$apc.u052.n[test$followup_yr==0]<-0

## Calculate yearly cost based on unit cost

test <- test %>% mutate(cost_yr = gp_yr*34+op.340.n*215+op.650.n*122
                        +op.320.n*196+op.812.n*51+op.110.n*192
                        +op.130.n*173+op.502.n*211+op.120.n*177
                        +op.410.n*180+op.330.n*173+op.301.n*169
                        +op.101.n*148+op.303.n*198 +op.400.n*212
                        +op.100.n*185+op.560.n*121+op.501.n*191
                        +op.300.n*217+op.341.n*163+op.191.n*244
                        +op.u354.n*136.1+op.u201.n*90+op.u051.n*102 
                        +op.u052.n*181+apc.u354.n*136+apc.u201.n*90
                        +apc.g451.n*215+apc.g459.n*303+apc.h221.n*303 
                        +apc.h229.n*225+apc.u051.n*102+apc.u052.n*181
                        +apc_dur_yr*650+cc_dur_yr*2621+ecds_yr*276)

# mean and sd of the yearly cost
test %>%
  group_by(treat) %>%
  summarise(mean = sprintf("%0.3f",mean(cost_yr)), sd = sprintf("%0.3f",sd(cost_yr)))



treat <- test %>% filter(treat==1)
control <- test %>% filter(treat==0)

# Last, Mann-Whitney U test

wilcox.test(treat$gp_yr, control$gp_yr,alternative="greater", paired=F)
wilcox.test(treat$op_yr, control$op_yr,alternative="greater", paired=F)
wilcox.test(treat$apc_yr, control$apc_yr,alternative="greater", paired=F)
wilcox.test(treat$apc_dur_yr, control$apc_dur_yr,alternative="greater", paired=F)
wilcox.test(treat$cc_dur_yr, control$cc_dur_yr,alternative="greater", paired=F)
wilcox.test(treat$ecds_yr, control$ecds_yr,alternative="greater", paired=F)
wilcox.test(treat$cost_yr, control$cost_yr,alternative="greater", paired=F)



u_gp <- wilcox.test(gp_yr ~ treat, data = test, paired = F)
u_gp
u_op <- wilcox.test(op_yr ~ treat, data = test, paired = F)
u_op
u_apc <- wilcox.test(apc_yr ~ treat, data = test, paired = F)
u_apc
u_apc_dur <- wilcox.test(apc_dur_yr ~ treat, data = test, paired = F)
u_apc_dur
u_cc_dur <- wilcox.test(cc_dur_yr ~ treat, data = test, paired = F)
u_cc_dur
u_ecds <- wilcox.test(ecds_yr ~ treat, data = test, paired = F)
u_ecds
u_cost_yr <- wilcox.test(cost_yr ~ treat, data = test, paired = F)
u_cost_yr

u_gp <- wilcox.test(gp_yr ~ treat, data = test, alternative = "greater", paired = F)
u_gp
u_op <- wilcox.test(op_yr ~ treat, data = test, alternative = "greater", paired = F)
u_op
u_apc <- wilcox.test(apc_yr ~ treat, data = test, alternative = "greater", paired = F)
u_apc
u_apc_dur <- wilcox.test(apc_dur_yr ~ treat, data = test, alternative = "greater", paired = F)
u_apc_dur
u_cc_dur <- wilcox.test(cc_dur_yr ~ treat, data = test, alternative = "greater", paired = F)
u_cc_dur
u_ecds <- wilcox.test(ecds_yr ~ treat, data = test, alternative = "greater", paired = F)
u_ecds
u_cost_yr <- wilcox.test(cost_yr ~ treat, data = test, alternative = "greater", paired = F)
u_cost_yr



