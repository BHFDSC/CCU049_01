# Databricks notebook source
# MAGIC %md
# MAGIC # Link cohort to tables (GP,OP,APC,CC,ED)
# MAGIC
# MAGIC - Extract each cohort from base cohort table
# MAGIC - Link each cohort to relevant tables (GP, OP, APC, CC, ED)

# COMMAND ----------

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

production_date = "2023-03-31" # Notebook CCU03_01_create_table_aliases   Cell 8
# Table names
# 1. Long Covid
#cohort = "dars_nic_391419_j3w9t_collab.ccu049_longc"
#end_date = "2023-01-31"


# 2. Long Covid historical
#cohort = "dars_nic_391419_j3w9t_collab.ccu049_longc"
#start_date = "2018-01-01"
#end_date = "2019-12-31"


# 3. Non-COVID (contemporary)
cohort = "dars_nic_391419_j3w9t_collab.ccu049_cohortc"
start_date = "2020-01-29"
end_date = "2022-03-31"

# 4. Pre-pandemic non-COVID (historical)
#cohort = "dars_nic_391419_j3w9t_collab.ccu049_cohorth"
#start_date = "2018-01-01"
#end_date = "2019-12-31"


# 5. Covid only non-Long Covid (chase till 2023-01-31)
#cohort = "dars_nic_391419_j3w9t_collab.ccu049_covid"
#end_date = "2023-01-31"


hes_cc_archive = spark.sql(f'select * from dars_nic_391419_j3w9t_collab.hes_cc_all_years_archive')
icnarc_archive = spark.sql(f'select * from dars_nic_391419_j3w9t_collab.icnarc_dars_nic_391419_j3w9t_archive')
icd10 = "dss_corporate.icd10_group_chapter_v01"
opcs = "dss_corporate.opcs_codes_v02"

#output = "ccu049_longc_cc"

#snomed = snomed.select('conceptID','term')
#snomed = snomed.withColumnRenamed('conceptID','CODE')

# COMMAND ----------


# For 1. Long Covid group & 5. COVID only group

# Extract the study cohort IDs from the cohort table
#co = spark.sql(f"SELECT id,date FROM {cohort}")


# For 2. Historical Long Covid group, 3. Non-COVID contemporary & 4. Pre-pandemic non-COVID group

# Extract the study cohort IDs from the cohort table
co = spark.sql(f"SELECT id FROM {cohort}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join cohort with HES APC

# COMMAND ----------

# Link the study cohort to HES-APC data
hes_cc = hes_cc_archive.filter(F.col("ProductionDate").startswith(production_date))
hes_cc = hes_cc.withColumnRenamed("PERSON_ID_DEID","id")

# COMMAND ----------

# Join cohort with hes_apc table

### For 1. Long Covid AND 5. COVID-only non Long Covid
#co_hes_cc = co.join(hes_cc, on= "id", how= "left")
#co_hes_cc = co_hes_cc.select('id','date','SUSRECID','ADMIDATE','EPISTART','BESTMATCH','CCSTARTDATE','ACARDSUPDAYS','ARESSUPDAYS','BCARDSUPDAYS','BRESSUPDAYS','CCLEV2DAYS','CCLEV3DAYS','ORGSUPMAX')
#co_hes_cc = co_hes_cc.filter(F.col('ADMIDATE')>=F.col('date'))
#co_hes_cc = co_hes_cc.filter(F.col('ADMIDATE')<= end_date)

#co_hes_cc = co_hes_cc.filter(F.col('BESTMATCH')==1)



### For 2. Long Covid historical, for 3. non-COVID contemporary, for 4. pre-pandemic non-COVID

### Join lc cohort with GP table
co_hes_cc = co.join(hes_cc, on= "id", how= "left")
co_hes_cc = co_hes_cc.select('id','SUSRECID','ADMIDATE','EPISTART','BESTMATCH','CCSTARTDATE','ACARDSUPDAYS','ARESSUPDAYS','BCARDSUPDAYS','BRESSUPDAYS','CCLEV2DAYS','CCLEV3DAYS','ORGSUPMAX')
co_hes_cc = co_hes_cc.filter((F.col('ADMIDATE')>=start_date) & (F.col('ADMIDATE')<=end_date))
co_hes_cc = co_hes_cc.filter(F.col('BESTMATCH')==1)



# COMMAND ----------

co_hes_cc = co_hes_cc.dropDuplicates()

# COMMAND ----------

from pyspark.sql import functions as F, Window

# This is per patient: Count number of admissions per patient

w = Window.partitionBy(co_hes_cc["id"])
co_hes_cc = co_hes_cc.withColumn('acard_all', sum('ACARDSUPDAYS').over(w))
co_hes_cc = co_hes_cc.withColumn('bcard_all', sum('BCARDSUPDAYS').over(w))
co_hes_cc = co_hes_cc.withColumn('ares_all', sum('ARESSUPDAYS').over(w))
co_hes_cc = co_hes_cc.withColumn('bres_all', sum('BRESSUPDAYS').over(w))
co_hes_cc = co_hes_cc.withColumn('lev2_all', sum('CCLEV2DAYS').over(w))
co_hes_cc = co_hes_cc.withColumn('lev3_all', sum('CCLEV3DAYS').over(w))

# COMMAND ----------

# Drop duplicated records based on 'id', then we have number of visits/follow ups per patient
co_hes_cc_nodup = co_hes_cc.dropDuplicates(['id'])

# COMMAND ----------

### Adding the column of visits/admission/appointments in each calendar year during the pandemic

### For 1. Long Covid & 5. COVID only group
co_hes_cc_nodup = co_hes_cc_nodup.select('id','acard_all','bcard_all','ares_all','bres_all','lev2_all','lev3_all')
co_final = co.join(co_hes_cc_nodup, on='id', how='left')

# COMMAND ----------

### For 1. Long Covid and 5. COVID only
co_final = co_final.drop('date')

# COMMAND ----------

# MAGIC %run /Workspaces/dars_nic_391419_j3w9t_collab/SHDS/Mehrdad/helper_library/basic_functions

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE dars_nic_391419_j3w9t_collab.ccu049_longc_apc_v2

# COMMAND ----------

#save_checkpoint(df_in=co_final, database="dars_nic_391419_j3w9t_collab", temp_df="ccu049_longc_cc_v2", global_view_name="ccu049")
#save_checkpoint(df_in=co_final, database="dars_nic_391419_j3w9t_collab", temp_df="ccu049_longcoh_cc_v2", global_view_name="ccu049")
save_checkpoint(df_in=co_final, database="dars_nic_391419_j3w9t_collab", temp_df="ccu049_nocovidc_cc_v2", global_view_name="ccu049")
#save_checkpoint(df_in=co_final, database="dars_nic_391419_j3w9t_collab", temp_df="ccu049_nocovidh_cc_v2", global_view_name="ccu049")
#save_checkpoint(df_in=co_final, database="dars_nic_391419_j3w9t_collab", temp_df="ccu049_onlycovid_cc_v2", global_view_name="ccu049")
