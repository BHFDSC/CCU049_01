# Databricks notebook source
# MAGIC %md
# MAGIC # Link cohort to tables (GP,OP,APC,CC,ED)
# MAGIC
# MAGIC - Extract each cohort from base cohort table
# MAGIC - Link each cohort to relevant tables (GP, OP, APC, CC, ED)

# COMMAND ----------

spark.sql('CLEAR CACHE')
spark.conf.set('spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation', 'true')
spark.sql('CLEAR CACHE')

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

# MAGIC %run  "Shared/SHDS/Mehrdad/helper_library/date_management"

# COMMAND ----------

production_date = "2024-03-27" # Notebook CCU03_01_create_table_aliases   Cell 8
# Table names
# 1. Long Covid
#cohort = "dsa_391419_j3w9t_collab.ccu049_02_cohort_longc"
#end_date = "2023-12-31"

# 2. Depression
#cohort = "dsa_391419_j3w9t_collab.ccu049_02_cohort_depression"
#start_date = "2018-01-01"
#end_date = "2023-12-31"

# 3. COPD
#cohort = "dsa_391419_j3w9t_collab.ccu049_02_cohort_copd"
#start_date = "2018-01-01"
#end_date = "2023-12-31"

# 4. Heart Failure
cohort = "dsa_391419_j3w9t_collab.ccu060_hf_base"
#start_date = "2018-01-01"
end_date = "2023-12-31"
hf = spark.sql(f'select * from dsa_391419_j3w9t_collab.ccu060_hf_base')

# 5. Fatigue
#cohort = "dsa_391419_j3w9t_collab.ccu049_02_cohort_fatigue"
#start_date = "2018-01-01"
#end_date = "2023-12-31"


ecds_archive = spark.sql(f'select * from dars_nic_391419_j3w9t_collab.lowlat_ecds_all_years_archive')
snomed = "dss_corporate.snomed_sct2_description_full"

# COMMAND ----------


# For 1. Long Covid group & 5. COVID only group

# Extract the study cohort IDs from the cohort table
co = spark.sql(f"SELECT id,date FROM {cohort}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join cohort with ECDS

# COMMAND ----------

# Link the study cohort to ECDS data
ecds = ecds_archive.filter(F.col("ProductionDate").startswith(production_date))
ecds = ecds.withColumnRenamed("PERSON_ID_DEID","id")


# COMMAND ----------

# Join cohort with ECDS table

### For 1. Long Covid AND 5. COVID-only non Long Covid
co_ecds = co.join(ecds, on= "id", how= "left")
co_ecds = co_ecds.select('id','date','ARRIVAL_DATE', 'CONCLUSION_DATE','ATTENDANCE_SOURCE','ATTENDANCE_SOURCE_ORGANISATION','DISCHARGE_DESTINATION','DISCHARGE_FOLLOW_UP','DISCHARGE_STATUS','RECEIVING_SITE')
co_ecds = co_ecds.filter(F.col('ARRIVAL_DATE')>=F.col('date'))
co_ecds = co_ecds.filter(F.col('ARRIVAL_DATE')<= end_date)


### For 2. Long Covid historical, for 3. non-COVID contemporary, for 4. pre-pandemic non-COVID

### Join lc cohort with GP table
#co_ecds = co.join(ecds, on= "id", how= "left")
#co_ecds = co_ecds.select('id','ARRIVAL_DATE', 'CONCLUSION_DATE','ATTENDANCE_SOURCE','ATTENDANCE_SOURCE_ORGANISATION','DISCHARGE_DESTINATION','DISCHARGE_FOLLOW_UP','DISCHARGE_STATUS','RECEIVING_SITE')
#co_ecds = co_ecds.filter((F.col('ARRIVAL_DATE')>=start_date) & (F.col('ARRIVAL_DATE')<=end_date))


# COMMAND ----------

co_ecds = co_ecds.dropDuplicates()

# COMMAND ----------

from pyspark.sql import functions as F, Window

# This is per patient: Count number of GP visits per patient


# For 1. Long Covid group & 3. Non COVID (contemporary) & 5. COVID only group
w = Window.partitionBy(co_ecds["id"])
co_ecds = co_ecds.withColumn('ecds_all', F.size(F.collect_set('ARRIVAL_DATE').over(w)))

co_ecds= co_ecds.withColumn('year',F.year(F.to_timestamp('ARRIVAL_DATE', 'yyyy-MM-dd')))
w_1 = Window.partitionBy(co_ecds["id"],co_ecds["year"])
co_ecds = co_ecds.withColumn('ecds_year', F.size(F.collect_set('ARRIVAL_DATE').over(w_1)))


# For 2. Long Covid historical group & 4. Non COVID (historical)

### This is per patient: Count number of GP visits per patient

#w = Window.partitionBy(co_ecds["id"])
#co_ecds = co_ecds.withColumn('ecds_all', F.size(F.collect_set('ARRIVAL_DATE').over(w)))

# COMMAND ----------

co_ecds_new = co_ecds.groupBy("id").pivot("year").agg(F.first("ecds_year").alias("ecds"))
co_ecds_new = co_ecds_new.withColumnRenamed('2020','ecds_2020').withColumnRenamed('2021','ecds_2021').withColumnRenamed('2022','ecds_2022').withColumnRenamed('2023','ecds_2023')

# COMMAND ----------

co_ecds = co_ecds.join(co_ecds_new, on='id', how='left')

# COMMAND ----------

# Drop duplicated records based on 'id', then we have number of visits/follow ups per patient
co_ecds_nodup = co_ecds.dropDuplicates(['id'])

# COMMAND ----------

### Adding the column of visits and follow-up to the main Long Covid cohort table

### For 1. Long Covid & 5. COVID only group
co_ecds_nodup = co_ecds_nodup.select('id','ecds_all','ecds_2020','ecds_2021','ecds_2022','ecds_2023')
co_final = co.join(co_ecds_nodup, on='id', how='left')


# COMMAND ----------

### For 1. Long Covid and 5. COVID only
co_final = co_final.drop('date')

# COMMAND ----------

#save_checkpoint(df_in=co_final, database="dars_nic_391419_j3w9t_collab", temp_df="ccu049_longc_ecds_v2", global_view_name="ccu049")
#save_checkpoint(df_in=co_final, database="dars_nic_391419_j3w9t_collab", temp_df="ccu049_longcoh_ecds_v2", global_view_name="ccu049")
#save_checkpoint(df_in=co_final, database="dars_nic_391419_j3w9t_collab", temp_df="ccu049_nocovidc_ecds_v2", global_view_name="ccu049")
#save_checkpoint(df_in=co_final, database="dars_nic_391419_j3w9t_collab", temp_df="ccu049_nocovidh_ecds_v2", global_view_name="ccu049")
#save_checkpoint(df_in=co_final, database="dars_nic_391419_j3w9t_collab", temp_df="ccu049_onlycovid_ecds_v2", global_view_name="ccu049")
