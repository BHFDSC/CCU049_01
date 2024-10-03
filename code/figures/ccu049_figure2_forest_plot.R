### Forest plot - Odds ratios of healthcare utilisation in individuals with long COVID, compared with each control group
### Produce CCU049_01 Figure 2

install.packages("epitools")
install.packages("ggplot2")
install.packages("forestplot2")
library(epitools)
library(ggplot2)
library(stringr)
library(dplyr)
library(odbc)

## To load main tables
# Each of the following tables contains both the study group (long COVID) and the matched control group
load("D:/PhotonUser/My Files/Home Folder/ccu049/matched_use_data_v2/longcoh_both.rdata")
load("D:/PhotonUser/My Files/Home Folder/ccu049/matched_use_data_v2/nocovidc_both.rdata")
load("D:/PhotonUser/My Files/Home Folder/ccu049/matched_use_data_v2/nocovidh_both.rdata")
load("D:/PhotonUser/My Files/Home Folder/ccu049/matched_use_data_v2/onlycovid_both.rdata")


## Analysis is conducted for each data frame separately. Each data frame below contains one control group and the treament group.
#### To obtain odds ratios and 95% CI ####

test <- longcoh_all
test <- onlycovid_all
test <- nocovidc_all
test <- nocovidh_all

# Adults only
test <- test %>% filter(age>=18)

# Remove people whose follow-up was negative
test <- test %>% filter(followup_day>=0)

# 1 if had GP appointment; 0 if never had GP appoitment
test$gp_use[!is.na(test$gp_all)] <- 1
test$gp_use[is.na(test$gp_all)] <- 0

# 1 if had OP appointment; 0 if never had OP appoitment
test$op_use[!is.na(test$op_all)] <- 1
test$op_use[is.na(test$op_all)] <- 0

# 1 if was admitted to hospital; 0 if never addmitted to hospital
test$apc_use[!is.na(test$apc_all)] <- 1
test$apc_use[is.na(test$apc_all)] <- 0

# 1 if was admitted to critical care unit; 0 if never admitted to critical care unit
test$cc_use[!is.na(test$cc_dur)] <- 1
test$cc_use[is.na(test$cc_dur)] <- 0

# 1 if attended A&E; 0 if never attended A&E
test$ed_use[!is.na(test$ecds_all)] <- 1
test$ed_use[is.na(test$ecds_all)] <- 0


gp_glm <- glm(gp_use ~ treat, data = test, family = 'binomial')
op_glm <- glm(op_use ~ treat, data = test, family = 'binomial')
apc_glm <- glm(apc_use ~ treat, data = test, family = 'binomial')
cc_glm <- glm(cc_use ~ treat, data = test, family = 'binomial')
ed_glm <- glm(ed_use ~ treat, data = test, family = 'binomial')


# Odds ratio and 95% confidence interval
exp(cbind("Odds ratio" = coef(gp_glm), confint.default(gp_glm, level = 0.95)))
exp(cbind("Odds ratio" = coef(op_glm), confint.default(op_glm, level = 0.95)))
exp(cbind("Odds ratio" = coef(apc_glm), confint.default(apc_glm, level = 0.95)))
exp(cbind("Odds ratio" = coef(cc_glm), confint.default(cc_glm, level = 0.95)))
exp(cbind("Odds ratio" = coef(ed_glm), confint.default(ed_glm, level = 0.95)))



#### To produce the forest plot using the numbers from above ####


# COVID only, no Long Covid
or_covid_only <- data.frame(category = factor(c("GP", "Outpatient", "Admission",
                                                "Critical Care", "Emergency"),
                                              levels = c("Emergency", "Critical Care",
                                                         "Admission", "Outpatient", "GP")),
                            boxOdds = c(8.021894,1.5343818,1.2183333,0.80176525,1.0605867), 
                            boxCILow = c(7.731272,1.5262806,1.2111098,0.78059885,1.0546679),
                            boxCIHigh = c(8.32344,1.5425259,1.2255999,0.82350559,1.0665387)
)

ggplot(or_covid_only, aes(y=category, x=boxOdds)) +
  geom_point(size=2, color = "red") +
  geom_errorbarh(aes(xmin = boxCILow, xmax = boxCIHigh), height = 0.2) +
  geom_vline(aes(xintercept=1), linetype="dashed", color="blue") +
  labs(x = "Odds Ratio", y = "", title = "COVID only, no Long Covid") +
  xlim(0, 50) +
  theme_minimal()
                      
# Contemporary non-COVID
or_non_covid <- data.frame(category = factor(c("GP", "Outpatient", "Admission",
                                               "Critical Care", "Emergency"),
                                             levels = c("Emergency", "Critical Care",
                                                        "Admission", "Outpatient", "GP")),
                           boxOdds = c(12.41125,1.359843,1.167009,1.085725913,1.0902031), 
                           boxCILow = c(11.97092,1.35268,1.160151,1.055217266,1.0841205),
                           boxCIHigh = c(12.86778,1.367043,1.1739071,1.117116631,1.0963198)
)

ggplot(or_non_covid, aes(y=category, x=boxOdds)) +
  geom_point(size=2, color = "red") +
  geom_errorbarh(aes(xmin = boxCILow, xmax = boxCIHigh), height = 0.2) +
  geom_vline(aes(xintercept=1), linetype="dashed", color="blue") +
  labs(x = "Odds Ratio", y = "", title = "Contemporary non-COVID") +
  xlim(0, 50) +
  theme_minimal()

# Pre-pandemic
or_pre_pandemic <- data.frame(category = factor(c("GP", "Outpatient", "Admission",
                                                  "Critical Care", "Emergency"),
                                                levels = c("Emergency", "Critical Care",
                                                           "Admission", "Outpatient", "GP")),
                              boxOdds = c(47.035454,0.9968747,0.8428969,0.78958655,1.8071531), 
                              boxCILow = c(45.361531,0.9913663,0.837922,0.76863551,1.7959793),
                              boxCIHigh = c(48.771149,1.002414,0.8479014,0.81110866, 1.8183964)
)

ggplot(or_pre_pandemic, aes(y=category, x=boxOdds)) +
  geom_point(size=2, color = "red") +
  geom_errorbarh(aes(xmin = boxCILow, xmax = boxCIHigh), height = 0.2) +
  geom_vline(aes(xintercept=1), linetype="dashed", color="blue") +
  labs(x = "Odds Ratio", y = "", title = "Pre-pandemic") +
  xlim(0, 50) +
  theme_minimal()


# Pre-Long Covid
or_pre_lc <- data.frame(category = factor(c("GP", "Outpatient", "Admission",
                                            "Emergency"),
                                          levels = c("Emergency",
                                                     "Admission", "Outpatient", "GP")),
                        boxOdds = c(38.248407,0.9483703,0.8756197,1.4436421), 
                        boxCILow = c(35.65595,0.938347,0.865717,1.427145),
                        boxCIHigh = c(41.029357,0.9585007,0.8856356,1.46033)
)


ggplot(or_pre_lc, aes(y=category, x=boxOdds)) +
  geom_point(size=2, color = "red") +
  geom_errorbarh(aes(xmin = boxCILow, xmax = boxCIHigh), height = 0.2) +
  geom_vline(aes(xintercept=1), linetype="dashed", color="blue") +
  labs(x = "Odds Ratio", y = "", title = "Pre-Long Covid") +
  xlim(0, 50) +
  theme_minimal()
