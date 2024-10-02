######################functions and librariesEpi###########################################
#rm(list=ls())
library("DBI")
library(odbc)
library(readr)
library(data.table)
library(tableone)
library(stargazer)
library(tidyverse)
library(R.utils)
library(lme4)
library(AER)
library(epitools)
library(epiR)
library(broom)
library(tidyverse)
library(table1)
library(survival)
library(survminer)
library(epiR)
library(MatchIt)
library(epitools)
library(Epi) #clogistic
library(dplyr)
library(plyr)




tab <- function(...){
  table(..., useNA="a")
}

na2bin <- function(x){
  
  tmm = is.na(x)
  if(sum(tmm)==0){
    tmm = x==min(x)
  }
  return (as.numeric(!tmm))
}

setwd("/mnt/efs/a.dashtban/covid")

### do not change float numbers
tobinIF <- function(x){
  if(class(x) == "integer"){
    x[is.na(x)] <- 0
    return (x)
  }
  else
    return(x)
}

#setwd("./longc/")
ln <- function(x){
  return(length(unique(x$id)))
}
"%ni%" <- Negate("%in%")

ln <- function(x){
  return(length(unique(x$id)))
}
"%ni%" <- Negate("%in%")

# connect to databricks instance

con <- dbConnect( odbc::odbc(), "Databricks", timeout = 60, 
                  PWD="",cache_size = ...)

con@info$dbms.name = "SQL"



#######################--------------loop withiout replacement##############

### integrity checks
sapply(dfm, function(x){if(class(x)=="numeric") sum(x)})

sapply(dfh, function(x){if(class(x)=="numeric") sum(x)})

sapply(dfl, function(x){if(class(x)=="numeric") sum(x)})


sapply(dfc, function(x){if(class(x)=="numeric") sum(x)})



#load("dflc.rdata")

# dfl <- data.table(dfl)
# df20 <- data.table(dfc)

dfl <- dfl[,.(id, rn,agecat,sex,imd ,region ,white,
              depression,cvd,cancer,dm,copd,ht, asthma, ckd, obese,type = "case")]
dfc <- dfc[,.(id, rn,agecat,sex,imd ,region ,white,
              depression,cvd,cancer,dm,copd,ht, asthma, ckd, obese,type = "control")]
dfh <- dfh[,.(id, rn,agecat,sex,imd ,region ,white,
              depression,cvd,cancer,dm,copd,ht, asthma, ckd, obese,type = "control")]
dfm <- dfm[,.(id, rn,agecat,sex,imd ,region ,white,
              depression,cvd,cancer,dm,copd,ht, asthma, ckd, obese,type = "control")]
# 
# save(dfl,dfm,dfc,dfh,file = "df_matching.rdata")


c12 = rbind(dfl,dfm)


# check integrity 
sapply(c12, function(x){if(class(x)=="numeric") sum(x)})


mm <- matchit(type ~ agecat+sex+imd +region +white+
                depression+cvd+cancer+dm+copd+ht+ asthma
              ,data = c12,method="exact", group = "all")


m <- match.data(mm, c12 , group = "all")#

save(mm, m, file="mm_dfc.rdata")
save(mm, m, file="mm_dfm.rdata")
save(mm, m, file="mm_dfh.rdata")




#m$id = seq.int(nrow(m))

m$subclass <- as.numeric(m$subclass)
setkeyv(m,c("subclass","sex","region"))


#separate cases and controls
d_cases <- m[type=="case",.(case_id=id,subclass)]
d_controls <- m[type=="control",.(control_id=id,subclass)]


#create consistent cases and control based on similar subclasses
class_in_both <- intersect(d_cases$subclass, d_controls$subclass)

d_cases <- d_cases[subclass %in% class_in_both ]
d_controls <- d_controls[subclass %in% class_in_both ]

###obtain number of similar control/cases
setkeyv(d_controls,"subclass") # we have to do this not just for efficiency but using cbind without computationally extensive merge
setkeyv(d_cases,"subclass")

g_controls <- d_controls[,.(num_control=.N),subclass]
g_cases <- d_cases[,.(num_case=.N),subclass]

g_cc <- merge(g_cases, g_controls, by="subclass")# as moving over time the number of cases outnumber the controls and causing errors

setkeyv(d_controls,"subclass") # we have to do this not just for efficiency but using cbind without computationally extensive merge
setkeyv(d_cases,"subclass")


############################################## matching part 2#####################################################

gcc = g_cc
mc = data.table()

for (i in 1:4){
  print("starting..........................")
  ### exclude cases if there is no control for some of them
  gcc <-  gcc[num_case>0]
  gcc[num_case> num_control, num_case := num_control]
  
  ##draw randomly a control with the same subclass and assign it to a case and build the matched ids
  set.seed(17)
  
  #-----sample from controls
  matched_control_idsW <- unlist(mapply(function(x,y) 
  {sample(as.list(d_controls[subclass==x]$control_id),y,replace=F)},gcc$subclass,gcc$num_case))
  
  #print(nrow(d_controls[control_id %in% matched_control_idsW]))
  
  #-----sample from cases
  matched_case_idsW <- unlist(mapply(function(x,y) 
  {sample(as.list(d_cases[subclass==x]$case_id),y,replace=F)},gcc$subclass,gcc$num_case))
  
  
  print('middle......................')
  macthed_case_controlW <- data.table(case_id=matched_case_idsW,control_id=matched_control_idsW, n=i)
  mc = rbind(mc, macthed_case_controlW)
  print(nrow(mc))
  
  #print(nrow(mc[control_id %in% matched_control_idsW]))
  #--- update control, remove as much as the number of cases that were samples in the iteration
  gcc[, num_control := num_control - num_case]
  
  
  #--- exclude controls who were sampled already
  print(paste0("iteration--",i,"----------------------------------"))
  
  d_controls <- d_controls[d_controls$control_id %ni% macthed_case_controlW$control_id] 
  print(paste0("after---",nrow(d_controls)))
  
}

#save(mc,file="maching_result_mc.rdata")


#### macthing success
table(df.c$n)/nrow(dfl)
table(df.m$n)/nrow(dfl)
table(df.h$n)/nrow(dfl)


nrow(dfl)

dim(mc[case_id %in% dfl$id])


### matching ratio:
length(unique(mc[case_id %in% d_cases$case_id]$case_id))/nrow(d_cases) *100
nrow(mc)/length(unique(mc[case_id %in% d_cases$case_id]$case_id)) # case:control rate, 3.985271
#length(unique(mc[control_id %in% d_controls$control_id]$control_id))
nrow(mc)


dim(macthed_case_controlW)
length(unique(macthed_case_controlW$case_id))
#length(unique(macthed_case_controlW$control_id))
#dim(macthed_case_controlW)


dim(d_cases)


dim(dfl)


length(unique(macthed_case_control$case_id))
length(unique(macthed_case_control$control_id)) 
dim(macthed_case_control)

dim(d_controls) 




save(m30,macthed_case_controlW,macthed_case_control,file="matched_j20.rdata")

#save(macthed_case_controlW,macthed_case_control,file="matched_j20_t.rdata")

#eligile for exacct matching 
dim(d_controls) 
dim(df[type==0]) 

# missed cases
missed_casesW <- setdiff(df[type==1]$id,macthed_case_controlW$case_id)
length(missed_casesW)
# with replacement

dim(macthed_case_controlW)

dim(macthed_case_control)

length(unique(macthed_case_control$control_id))
length(unique(macthed_case_controlW$control_id))
length(unique(macthed_case_controlW$case_id))

save(matched_control_idsW,macthed_case_control,file="matched_j20.rdata")
















