# Databricks notebook source
# Import some libraries and packages
import pyspark.sql.functions as F
from pyspark.sql.functions import *
import pyspark.sql.functions as Seq
import pyspark.sql.types as T
import pandas as pd
import sys
from pyspark.sql.window import Window
from pyspark.sql import Row
from pyspark.sql.functions import col,min,max,lit
%matplotlib inline

# COMMAND ----------

# Params
production_date = "2023-04-27" # Notebook CCU03_01_create_table_aliases   Cell 8

# Table names
# 1. Covid
cohort = "dars_nic_391419_j3w9t_collab.ccu049_covid"
date_today = "2023-01-31"


# COMMAND ----------

# MAGIC %run /Workspaces/dars_nic_391419_j3w9t_collab/SHDS/Mehrdad/helper_library/date_management

# COMMAND ----------

death = spark.sql(f"SELECT DEC_CONF_NHS_NUMBER_CLEAN_DEID, REG_DATE_OF_DEATH FROM dars_nic_391419_j3w9t.deaths_dars_nic_391419_j3w9t")
death = death.withColumnRenamed('DEC_CONF_NHS_NUMBER_CLEAN_DEID','id')
death = manage_deaths_recdate(df=death, col_name="REG_DATE_OF_DEATH")
death = death.select('id','REG_DATE_OF_DEATH')
display(death)

# COMMAND ----------

# Extract the study cohort IDs from the Long Covid cohort table
co = spark.sql(f"SELECT id,date,dod FROM {cohort}")
co = co.withColumnRenamed('date','cv_date')
co = co.join(death, on = "id", how = "left")
co = co.withColumn('dod', when(F.col('REG_DATE_OF_DEATH')>'2023-01-31', None).otherwise(F.col('REG_DATE_OF_DEATH')))
co = co.drop('REG_DATE_OF_DEATH')



# COMMAND ----------

import pyspark.sql.functions as F 
# First, create the date column that is one year ahead of Long Covid diagnosis
co_yr = co.withColumn('one_yr_date', F.date_add(co['cv_date'], 365)).withColumn('two_yr_date', F.date_add(co['cv_date'], 730))
display(co_yr)

# COMMAND ----------

# Second, change the one_yr_date to 2022-12-31 if the one-yr-date if higher than that
co_yr = co_yr.withColumn('one_yr', when(F.col('one_yr_date')<='2023-01-31', F.col('one_yr_date')).otherwise('2023-01-31'))
co_yr = co_yr.withColumn('two_yr', when(F.col('two_yr_date')<='2023-01-31', F.col('two_yr_date')).otherwise('2023-01-31'))
display(co_yr)

# COMMAND ----------

from pyspark.sql.functions import *
co_yr = co_yr.withColumn('one_yr_death', when(F.col('dod').isNull(), 0).when(F.col('dod')>F.col('one_yr'),0).otherwise(1))
co_yr = co_yr.withColumn('two_yr_death', when(F.col('dod').isNull(), 0).when(F.col('dod')>F.col('two_yr'),0).otherwise(1))
co_yr = co_yr.withColumn('date_today',lit('2023-01-31'))
co_yr = co_yr.withColumn('followup_day',when(F.col('dod').isNull(),floor(datediff(col('date_today'),col("cv_date"))))
                                 .otherwise(floor(datediff(col('dod'), col("cv_date")))))
co_yr = co_yr.withColumn('one_yr_followup_day', when(F.col('followup_day')>365,365).otherwise(F.col('followup_day')))
co_yr = co_yr.withColumn('two_yr_followup_day', when(F.col('followup_day')>730,730).otherwise(F.col('followup_day')))

co_yr = co_yr.withColumn("one_yr_followup_mon", round(F.col('one_yr_followup_day')/30.42+1,0))
co_yr = co_yr.withColumn("two_yr_followup_mon", round(F.col('two_yr_followup_day')/30.42+1,0))

                                 

# COMMAND ----------

co_yr = co_yr.select('id','one_yr_death','one_yr_followup_day','two_yr_death','two_yr_followup_day')


# COMMAND ----------

# MAGIC %run /Workspaces/dars_nic_391419_j3w9t_collab/SHDS/Mehrdad/helper_library/basic_functions

# COMMAND ----------

save_checkpoint(df_in=co_yr, database="dars_nic_391419_j3w9t_collab", temp_df="ccu049_onlycovid_death", global_view_name="ccu049")

# COMMAND ----------

# MAGIC %md
# MAGIC # Survival analysis - Hospitalisation in TWO years
# MAGIC
# MAGIC - Link each cohort to HES APC

# COMMAND ----------

# MAGIC %run /Workspaces/dars_nic_391419_j3w9t_collab/SHDS/Mehrdad/helper_library/date_management

# COMMAND ----------

production_date = "2023-03-31" # Notebook CCU03_01_create_table_aliases   Cell 8


hes_apc_archive = spark.sql(f'select * from dars_nic_391419_j3w9t_collab.hes_apc_all_years_archive')
hes_apc_otr_archive = spark.sql(f'select * from dars_nic_391419_j3w9t_collab.hes_apc_otr_all_years_archive')
hes_cc_archive = spark.sql(f'select * from dars_nic_391419_j3w9t_collab.hes_cc_all_years_archive')
icnarc_archive = spark.sql(f'select * from dars_nic_391419_j3w9t_collab.icnarc_dars_nic_391419_j3w9t_archive')
icd10 = "dss_corporate.icd10_group_chapter_v01"
opcs = "dss_corporate.opcs_codes_v02"

# COMMAND ----------

# Link the study cohort to HES-APC data
hes_apc = hes_apc_archive.filter(F.col("ProductionDate").startswith(production_date))
hes_apc_otr = hes_apc_otr_archive.filter(F.col("ProductionDate").startswith(production_date))
hes_cc = hes_cc_archive.filter(F.col("ProductionDate").startswith(production_date))
icnarc = icnarc_archive.filter(F.col("ProductionDate").startswith("2023-03-31"))

hes_apc = hes_apc.withColumnRenamed("PERSON_ID_DEID","id")
# hes_apc_otr = hes_apc_otr.withColumnRenamed("PERSON_ID_DEID","id")
hes_cc = hes_cc.withColumnRenamed("PERSON_ID_DEID","id")
icnarc = icnarc.withColumnRenamed("NHS_NUMBER_DEID","id")


# COMMAND ----------

# Extract the study cohort IDs from the Long Covid cohort table
co = spark.sql(f"SELECT id,date FROM {cohort}")
co = co.withColumnRenamed('date','cv_date')

# COMMAND ----------

# Join lc cohort with hes_apc table
# First, join hes_apc_otr and hes_apc by 'EPIKEY', create apc
# Second, join apc and lc_cohort, create lc_hes_apc
#apc = hes_apc.join(hes_apc_otr, on = "EPIKEY", how = "left")
hes_apc = co.join(hes_apc, on = "id", how = "left")
hes_apc = hes_apc.select('id','cv_date','ADMIDATE')

hes_apc = hes_apc.filter(F.col('ADMIDATE')>=F.col('cv_date'))
hes_apc = hes_apc.filter(F.col('ADMIDATE')<'2023-02-01')

# COMMAND ----------

from pyspark.sql.functions import col, min as min_

hes_apc_date = hes_apc.withColumn('ADMIDATE', col("ADMIDATE").cast("date")).groupBy('id').agg(min_("ADMIDATE"))

# COMMAND ----------

co_apc = co.join(hes_apc_date, on = "id", how = "left")
#display(co_apc)

# COMMAND ----------

import pyspark.sql.functions as F 
# First, create the date column that is one year ahead of Long Covid diagnosis
co_apc_two_yr = co_apc.withColumn('two_yr_date', F.date_add(co['cv_date'], 730))

# COMMAND ----------

# Second, change the two_yr_date to 2023-01-31 if the two-yr-date was later than that
co_apc_two_yr = co_apc_two_yr.withColumn('two_yr', when(F.col('two_yr_date')<='2023-01-31', F.col('two_yr_date')).otherwise('2023-01-31'))
co_apc_two_yr = co_apc_two_yr.withColumnRenamed('min(ADMIDATE)', 'first_admin')

# COMMAND ----------

from pyspark.sql.functions import *
co_apc_two_yr = co_apc_two_yr.withColumn('two_yr_hos', when(F.col('first_admin').isNull(), 0).when(F.col('first_admin')>F.col('two_yr'),0).otherwise(1))
co_apc_two_yr = co_apc_two_yr.withColumn('date_today',lit('2023-01-31'))
co_apc_two_yr = co_apc_two_yr.withColumn('followup_day',when(F.col('first_admin').isNull(),floor(datediff(col('date_today'),col("cv_date"))))
                                 .otherwise(floor(datediff(col('first_admin'), col("cv_date")))))
co_apc_two_yr = co_apc_two_yr.withColumn('two_yr_followup_day', when(F.col('followup_day')>730,730).otherwise(F.col('followup_day')))

co_apc_two_yr = co_apc_two_yr.withColumn("two_yr_followup_mon", round(F.col('two_yr_followup_day')/30.42+1,0))
                          

# COMMAND ----------

co_apc_two_yr = co_apc_two_yr.select('id','two_yr_hos','two_yr_followup_day')

# COMMAND ----------

co_apc_two_yr.write.format("delta").saveAsTable("dsa_391419_j3w9t_collab.ccu049_onlycovid_hosp")
