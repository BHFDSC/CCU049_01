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
# Contemporary non-COVID
cohort = "dars_nic_391419_j3w9t_collab.ccu049_cohortc"
date_today = "2023-01-31"


# COMMAND ----------

# MAGIC %run /Repos/yi.mu@ucl.ac.uk/collaboration/SHDS/Mehrdad/helper_library/date_management

# COMMAND ----------

death = spark.sql(f"SELECT DEC_CONF_NHS_NUMBER_CLEAN_DEID, REG_DATE_OF_DEATH FROM dars_nic_391419_j3w9t.deaths_dars_nic_391419_j3w9t")
death = death.withColumnRenamed('DEC_CONF_NHS_NUMBER_CLEAN_DEID','id')
death = manage_deaths_recdate(df=death, col_name="REG_DATE_OF_DEATH")
death = death.select('id','REG_DATE_OF_DEATH')

# COMMAND ----------

# Extract the study cohort IDs from the Long Covid cohort table
co = spark.sql(f"SELECT id FROM {cohort}")
co = co.withColumn('date', lit('2020-01-29'))
co = co.join(death, on = "id", how = "left")
co = co.withColumn('dod', when(F.col('REG_DATE_OF_DEATH')>'2023-01-31', None).otherwise(F.col('REG_DATE_OF_DEATH')))
co = co.drop('REG_DATE_OF_DEATH')

# COMMAND ----------

import pyspark.sql.functions as F 
# First, create the date column that is one year ahead of Long Covid diagnosis
co_two_yr = co.withColumn('two_yr', F.date_add(co['date'], 730))

# COMMAND ----------

from pyspark.sql.functions import *
co_two_yr = co_two_yr.withColumn('two_yr_death', when(F.col('dod').isNull(), 0).when(F.col('dod')>F.col('two_yr'),0).otherwise(1))
co_two_yr = co_two_yr.withColumn('date_today',lit('2023-01-31'))
co_two_yr = co_two_yr.withColumn('followup_day',when(F.col('dod').isNull(),floor(datediff(col('date_today'),col("date"))))
                                 .otherwise(floor(datediff(col('dod'), col("date")))))
co_two_yr = co_two_yr.withColumn('two_yr_followup_day', when(F.col('followup_day')>730,730).otherwise(F.col('followup_day')))

co_two_yr = co_two_yr.withColumn("two_yr_followup_mon", round(F.col('two_yr_followup_day')/30.42+1,0))
                                 

# COMMAND ----------

co_two_yr = co_two_yr.filter(F.col('two_yr_followup_day')>=0)

# COMMAND ----------

co_two_yr = co_two_yr.select('id','two_yr_death','two_yr_followup_day')

# COMMAND ----------

co_two_yr.write.format("delta").saveAsTable("dsa_391419_j3w9t_collab.ccu049_nocovidc_death")

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
co = spark.sql(f"SELECT id FROM {cohort}")
co = co.withColumn('date',lit('2020-01-29'))

# COMMAND ----------

# First, join LC cohort with hes_apc table
hes_apc = co.join(hes_apc, on = "id", how = "left")
hes_apc = hes_apc.select('id','date','ADMIDATE')

hes_apc = hes_apc.filter(F.col('ADMIDATE')>=F.col('date'))
hes_apc = hes_apc.filter(F.col('ADMIDATE')<'2023-02-01')

# COMMAND ----------

from pyspark.sql.functions import col, min as min_

hes_apc_date = hes_apc.withColumn('ADMIDATE', col("ADMIDATE").cast("date")).groupBy('id').agg(min_("ADMIDATE"))

# COMMAND ----------

co_apc = co.join(hes_apc_date, on = "id", how = "left")

# COMMAND ----------

import pyspark.sql.functions as F 
# First, create the date column that is one year ahead of Long Covid diagnosis
co_apc_two_yr = co_apc.withColumn('two_yr', F.date_add(co['date'], 730))

# COMMAND ----------

co_apc_two_yr = co_apc_two_yr.withColumnRenamed('min(ADMIDATE)', 'first_admin')

# COMMAND ----------

from pyspark.sql.functions import *
co_apc_two_yr = co_apc_two_yr.withColumn('two_yr_hos', when(F.col('first_admin').isNull(), 0).when(F.col('first_admin')>F.col('two_yr'),0).otherwise(1))
co_apc_two_yr = co_apc_two_yr.withColumn('date_today',lit('2023-01-31'))
co_apc_two_yr = co_apc_two_yr.withColumn('followup_day',when(F.col('first_admin').isNull(),floor(datediff(col('date_today'),col("date"))))
                                 .otherwise(floor(datediff(col('first_admin'), col("date")))))
co_apc_two_yr = co_apc_two_yr.withColumn('two_yr_followup_day', when(F.col('followup_day')>730,730).otherwise(F.col('followup_day')))

co_apc_two_yr = co_apc_two_yr.withColumn("two_yr_followup_mon", round(F.col('two_yr_followup_day')/30.42+1,0))
                          

# COMMAND ----------

co_apc_two_yr = co_apc_two_yr.select('id','two_yr_hos','two_yr_followup_day')

# COMMAND ----------

co_apc_two_yr.write.format("delta").saveAsTable("dsa_391419_j3w9t_collab.ccu049_nocovidc_hosp")
