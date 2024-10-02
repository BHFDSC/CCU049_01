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


hes_apc_archive = spark.sql(f'select * from dars_nic_391419_j3w9t_collab.hes_apc_all_years_archive')
hes_apc_otr_archive = spark.sql(f'select * from dars_nic_391419_j3w9t_collab.hes_apc_otr_all_years_archive')
hes_cc_archive = spark.sql(f'select * from dars_nic_391419_j3w9t_collab.hes_cc_all_years_archive')
icnarc_archive = spark.sql(f'select * from dars_nic_391419_j3w9t_collab.icnarc_dars_nic_391419_j3w9t_archive')
icd10 = "dss_corporate.icd10_group_chapter_v01"
opcs = "dss_corporate.opcs_codes_v02"
#output = "ccu049_longc_op"


#snomed = snomed.select('conceptID','term')
#snomed = snomed.withColumnRenamed('conceptID','CODE')
#display(snomed)

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
hes_apc = hes_apc_archive.filter(F.col("ProductionDate").startswith(production_date))
hes_apc_otr = hes_apc_otr_archive.filter(F.col("ProductionDate").startswith(production_date))
hes_cc = hes_cc_archive.filter(F.col("ProductionDate").startswith(production_date))
icnarc = icnarc_archive.filter(F.col("ProductionDate").startswith("2023-03-31"))

hes_apc = hes_apc.withColumnRenamed("PERSON_ID_DEID","id")
# hes_apc_otr = hes_apc_otr.withColumnRenamed("PERSON_ID_DEID","id")
#hes_cc = hes_cc.withColumnRenamed("PERSON_ID_DEID","id")
#icnarc = icnarc.withColumnRenamed("NHS_NUMBER_DEID","id")


# COMMAND ----------

# Join cohort with hes_apc table

### For 1. Long Covid AND 5. COVID-only non Long Covid
#co_hes_apc = co.join(hes_apc, on= "id", how= "left")
#co_hes_apc = co_hes_apc.select('id','date','SUSRECID','ADMIDATE','DISDATE','SPELDUR','SPELDUR_CALC','EPIDUR','EPISTART','EPIEND','EPISTAT','EPIORDER','FAE','CLASSPAT','DIAG_4_01','DIAG_4_CONCAT','OPERTN_4_01','OPERTN_4_02','OPERTN_4_03','OPERTN_4_04','OPERTN_4_05','OPERTN_4_06','OPERTN_4_07','OPERTN_4_08','OPERTN_4_09','OPERTN_4_10','OPERTN_4_11','OPERTN_4_12','OPERTN_4_13','OPERTN_4_14','OPERTN_4_15','OPERTN_4_16','OPERTN_4_17','OPERTN_4_18','OPERTN_4_19','OPERTN_4_20','OPERTN_4_21','OPERTN_4_22','OPERTN_4_23','OPERTN_4_24','OPERTN_4_CONCAT', 'MAINSPEF', 'TRETSPEF', 'SUSHRG','ADMISORC','DISDEST','ELECDATE','ELECDUR','ELECDUR_CALC','WAITLIST')
#co_hes_apc = co_hes_apc.filter(F.col('ADMIDATE')>=F.col('date'))
#co_hes_apc = co_hes_apc.filter(F.col('ADMIDATE')<= end_date)



### For 2. Long Covid historical, for 3. non-COVID contemporary, for 4. pre-pandemic non-COVID

### Join lc cohort with GP table
co_hes_apc = co.join(hes_apc, on= "id", how= "left")
co_hes_apc = co_hes_apc.select('id','SUSRECID','ADMIDATE','DISDATE','SPELDUR','SPELDUR_CALC','EPIDUR','EPISTART','EPIEND','EPISTAT','EPIORDER','FAE','CLASSPAT','DIAG_4_01','DIAG_4_CONCAT','OPERTN_4_01','OPERTN_4_02','OPERTN_4_03','OPERTN_4_04','OPERTN_4_05','OPERTN_4_06','OPERTN_4_07','OPERTN_4_08','OPERTN_4_09','OPERTN_4_10','OPERTN_4_11','OPERTN_4_12','OPERTN_4_13','OPERTN_4_14','OPERTN_4_15','OPERTN_4_16','OPERTN_4_17','OPERTN_4_18','OPERTN_4_19','OPERTN_4_20','OPERTN_4_21','OPERTN_4_22','OPERTN_4_23','OPERTN_4_24','OPERTN_4_CONCAT', 'MAINSPEF', 'TRETSPEF', 'SUSHRG','ADMISORC','DISDEST','ELECDATE','ELECDUR','ELECDUR_CALC','WAITLIST')
co_hes_apc = co_hes_apc.filter((F.col('ADMIDATE')>=start_date) & (F.col('ADMIDATE')<=end_date))


# COMMAND ----------

co_hes_apc = co_hes_apc.dropDuplicates()

# COMMAND ----------

# To add tests, procedures
import pyspark.sql.functions as f
opcs = co_hes_apc.select('id','OPERTN_4_CONCAT')
opcs = opcs.groupby("id").agg(f.concat_ws(", ", f.collect_list(opcs.OPERTN_4_CONCAT)))
opcs = opcs.withColumnRenamed('concat_ws(, , collect_list(OPERTN_4_CONCAT))', 'apc_combined_opcs')

# COMMAND ----------

# To add Treatment Specialty
import pyspark.sql.functions as f
tretsp = co_hes_apc.select('id','ADMIDATE','TRETSPEF')
tretsp_nodup_admin = tretsp.dropDuplicates(['id','ADMIDATE','TRETSPEF'])
tretsp_combined = tretsp_nodup_admin.groupby("id").agg(f.concat_ws(", ", f.collect_list(tretsp_nodup_admin.TRETSPEF)))
tretsp_combined = tretsp_combined.withColumnRenamed('concat_ws(, , collect_list(TRETSPEF))', 'apc_combined_tret')

# COMMAND ----------

from pyspark.sql import functions as F, Window

# This is per patient: Count number of admissions per patient


# For 1. Long Covid group & 3. Non COVID (contemporary) & 5. COVID only group
w = Window.partitionBy(co_hes_apc["id"])
co_hes_apc = co_hes_apc.withColumn('apc_all', F.size(F.collect_set('ADMIDATE').over(w)))

co_hes_apc= co_hes_apc.withColumn('year',F.year(F.to_timestamp('ADMIDATE', 'yyyy-MM-dd')))
w_1 = Window.partitionBy(co_hes_apc["id"],co_hes_apc["year"])
co_hes_apc = co_hes_apc.withColumn('apc_year', F.size(F.collect_set('ADMIDATE').over(w_1)))


# For 2. Long Covid historical group & 4. Non COVID (historical)

### This is per patient: Count number of admissions per patient

#w = Window.partitionBy(co_hes_apc["id"])
#co_hes_apc = co_hes_apc.withColumn('apc_all', F.size(F.collect_set('ADMIDATE').over(w)))

# COMMAND ----------

# For 1. Long Covid, 3. Non COVID (contemporary) & 5. COVID only non Long Covid

co_hes_apc_new = co_hes_apc.groupBy("id").pivot("year").agg(F.first("apc_year").alias("apc"))
co_hes_apc_new = co_hes_apc_new.withColumnRenamed('2020','apc_2020').withColumnRenamed('2021','apc_2021').withColumnRenamed('2022','apc_2022').withColumnRenamed('2023','apc_2023')

# COMMAND ----------

# For 1. Long Covid, 3. Non COVID (contemporary) & 5. COVID only non Long Covid

co_hes_apc = co_hes_apc.join(co_hes_apc_new, on='id', how='left')

# COMMAND ----------

# Drop duplicated records based on 'id', then we have number of visits/follow ups per patient
co_hes_apc_nodup = co_hes_apc.dropDuplicates(['id'])

# COMMAND ----------

### Adding the column of visits/admission/appointments in each calendar year during the pandemic

### For 1. Long Covid & 5. COVID only group
#co_hes_apc_nodup = co_hes_apc_nodup.select('id','apc_all','apc_2020','apc_2021','apc_2022','apc_2023')
#co_final = co.join(co_hes_apc_nodup, on='id', how='left')

### For 2. Long Covid historical, and pre-pandemic non-COVID
#co_hes_apc_nodup = co_hes_apc_nodup.select('id','apc_all')
#co_final = co.join(co_hes_apc_nodup, on='id', how='left')

### For 3. Non-COVID contemporary
### Adding the column of visits and follow-up to the main Long Covid cohort table
co_hes_apc_nodup = co_hes_apc_nodup.select('id','apc_all','apc_2020','apc_2021','apc_2022')
co_final = co.join(co_hes_apc_nodup, on='id', how='left')

co_final = co_final.join(tretsp_combined, on='id', how='left')
co_final = co_final.join(opcs, on='id', how='left')

# COMMAND ----------

### For 1. Long Covid and 5. COVID only
co_final = co_final.drop('date')

# COMMAND ----------

# MAGIC %run /Workspaces/dars_nic_391419_j3w9t_collab/SHDS/Mehrdad/helper_library/basic_functions

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE dars_nic_391419_j3w9t_collab.ccu049_longc_apc_v2

# COMMAND ----------

#save_checkpoint(df_in=co_final, database="dars_nic_391419_j3w9t_collab", temp_df="ccu049_longc_apc_v2", global_view_name="ccu049")
#save_checkpoint(df_in=co_final, database="dars_nic_391419_j3w9t_collab", temp_df="ccu049_longcoh_apc_v2", global_view_name="ccu049")
save_checkpoint(df_in=co_final, database="dars_nic_391419_j3w9t_collab", temp_df="ccu049_nocovidc_apc_v2", global_view_name="ccu049")
#save_checkpoint(df_in=co_final, database="dars_nic_391419_j3w9t_collab", temp_df="ccu049_nocovidh_apc_v2", global_view_name="ccu049")
#save_checkpoint(df_in=co_final, database="dars_nic_391419_j3w9t_collab", temp_df="ccu049_onlycovid_apc_v2", global_view_name="ccu049")
