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

hes_op_archive = spark.sql(f'select * from dars_nic_391419_j3w9t_collab.hes_op_all_years_archive')
icd10 = "dss_corporate.icd10_group_chapter_v01"

# COMMAND ----------


# For 1. Long Covid group & 5. COVID only group

# Extract the study cohort IDs from the cohort table
#co = spark.sql(f"SELECT id,date FROM {cohort}")


# For 2. Historical Long Covid group, 3. Non-COVID contemporary & 4. Pre-pandemic non-COVID group

# Extract the study cohort IDs from the cohort table
co = spark.sql(f"SELECT id FROM {cohort}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Join cohort with HES OP

# COMMAND ----------

# Link the study cohort to ECDS data
hes_op = hes_op_archive.filter(F.col("ProductionDate").startswith(production_date))
hes_op = hes_op.withColumnRenamed("PERSON_ID_DEID","id")


# COMMAND ----------

# Join cohort with HES OP table

### For 1. Long Covid AND 5. COVID-only non Long Covid
#co_hes_op = co.join(hes_op, on= "id", how= "left")
#co_hes_op = co_hes_op.select('id','date','APPTDATE','ATTENDANCE','ATTENDED','DIAG_4_01','DIAG_4_CONCAT','OPERTN_4_01','OPERTN_4_02','OPERTN_4_03','OPERTN_4_04','OPERTN_4_05','OPERTN_4_06','OPERTN_4_07','OPERTN_4_08','OPERTN_4_09','OPERTN_4_10','OPERTN_4_11','OPERTN_4_12','OPERTN_4_13','OPERTN_4_14','OPERTN_4_15','OPERTN_4_16','OPERTN_4_17','OPERTN_4_18','OPERTN_4_19','OPERTN_4_20','OPERTN_4_21','OPERTN_4_22','OPERTN_4_23','OPERTN_4_24','OPERTN_COUNT','MAINSPEF','TRETSPEF','OPERTN_4_CONCAT','SUSHRG','FIRSTATT','OUTCOME','REFSOURC','REQDATE','WAITDAYS','WAITING')
#co_hes_op = co_hes_op.filter(F.col('APPTDATE')>=F.col('date'))
#co_hes_op = co_hes_op.filter(F.col('APPTDATE')<= end_date)

#co_hes_op = co_hes_op.filter(F.col('ATTENDANCE')==1)




### For 2. Long Covid historical, for 3. non-COVID contemporary, for 4. pre-pandemic non-COVID

### Join lc cohort with GP table
co_hes_op = co.join(hes_op, on= "id", how= "left")
co_hes_op = co_hes_op.select('id','APPTDATE','ATTENDANCE','ATTENDED','DIAG_4_01','DIAG_4_CONCAT','OPERTN_4_01','OPERTN_4_02','OPERTN_4_03','OPERTN_4_04','OPERTN_4_05','OPERTN_4_06','OPERTN_4_07','OPERTN_4_08','OPERTN_4_09','OPERTN_4_10','OPERTN_4_11','OPERTN_4_12','OPERTN_4_13','OPERTN_4_14','OPERTN_4_15','OPERTN_4_16','OPERTN_4_17','OPERTN_4_18','OPERTN_4_19','OPERTN_4_20','OPERTN_4_21','OPERTN_4_22','OPERTN_4_23','OPERTN_4_24','OPERTN_COUNT','MAINSPEF','TRETSPEF','OPERTN_4_CONCAT','SUSHRG','FIRSTATT','OUTCOME','REFSOURC','REQDATE','WAITDAYS','WAITING')
co_hes_op = co_hes_op.filter((F.col('APPTDATE')>=start_date) & (F.col('APPTDATE')<=end_date))

co_hes_op = co_hes_op.filter(F.col('ATTENDANCE')==1)


# COMMAND ----------

co_hes_op = co_hes_op.dropDuplicates()

# COMMAND ----------

import pyspark.sql.functions as f
opcs = co_hes_op.select('id','OPERTN_4_CONCAT')
opcs = opcs.groupby("id").agg(f.concat_ws(", ", f.collect_list(opcs.OPERTN_4_CONCAT)))
opcs = opcs.withColumnRenamed('concat_ws(, , collect_list(OPERTN_4_CONCAT))', 'op_combined_opcs')
#display(opcs)

# COMMAND ----------

# To add Treatment Specialty
import pyspark.sql.functions as f
tretsp = co_hes_op.select('id','TRETSPEF')
tretsp = tretsp.groupby("id").agg(f.concat_ws(", ", f.collect_list(tretsp.TRETSPEF)))
tretsp = tretsp.withColumnRenamed('concat_ws(, , collect_list(TRETSPEF))', 'op_combined_tret')
#display(tretsp)

# COMMAND ----------

from pyspark.sql import functions as F, Window

# This is per patient: Count number of GP visits per patient

# For 1. Long Covid group & 3. Non COVID (contemporary) & 5. COVID only group
w = Window.partitionBy(co_hes_op["id"])
co_hes_op = co_hes_op.withColumn('op_all', F.size(F.collect_set('APPTDATE').over(w)))

co_hes_op= co_hes_op.withColumn('year',F.year(F.to_timestamp('APPTDATE', 'yyyy-MM-dd')))
w_1 = Window.partitionBy(co_hes_op["id"],co_hes_op["year"])
co_hes_op = co_hes_op.withColumn('op_year', F.size(F.collect_set('APPTDATE').over(w_1)))


# For 2. Long Covid historical group & 4. Non COVID (historical)

### This is per patient: Count number of GP visits per patient

#w = Window.partitionBy(co_hes_op["id"])
#co_hes_op = co_hes_op.withColumn('op_all', F.size(F.collect_set('APPTDATE').over(w)))


# COMMAND ----------

# For 1. Long Covid, 3. Non COVID (contemporary) & 5. COVID only non Long Covid

co_hes_op_new = co_hes_op.groupBy("id").pivot("year").agg(F.first("op_year").alias("op"))
co_hes_op_new = co_hes_op_new.withColumnRenamed('2020','op_2020').withColumnRenamed('2021','op_2021').withColumnRenamed('2022','op_2022').withColumnRenamed('2023','op_2023')

# COMMAND ----------

# For 1. Long Covid, 3. Non COVID (contemporary) & 5. COVID only non Long Covid
co_hes_op = co_hes_op.join(co_hes_op_new, on='id', how='left')

# COMMAND ----------

# Drop duplicated records based on 'id', then we have number of visits/follow ups per patient
co_hes_op_nodup = co_hes_op.dropDuplicates(['id'])

# COMMAND ----------

### Adding the column of visits and follow-up to the main Long Covid cohort table

### For 1. Long Covid & 5. COVID only group
#co_hes_op_nodup = co_hes_op_nodup.select('id','op_all','op_2020','op_2021','op_2022','op_2023')
#co_final = co.join(co_hes_op_nodup, on='id', how='left')



### For 2. Long Covid historical, and pre-pandemic non-COVID
#co_hes_op_nodup = co_hes_op_nodup.select('id','op_all')
#co_final = co.join(co_hes_op_nodup, on='id', how='left')


### For 3. Non-COVID contemporary
### Adding the column of visits and follow-up to the main Long Covid cohort table
co_hes_op_nodup = co_hes_op_nodup.select('id','op_all','op_2020','op_2021','op_2022')
co_final = co.join(co_hes_op_nodup, on='id', how='left')



co_final = co_final.join(tretsp, on='id', how='left')
co_final = co_final.join(opcs, on='id', how='left')

# COMMAND ----------

### For 1. Long Covid and 5. COVID only
co_final = co_final.drop('date')

# COMMAND ----------

# MAGIC %run /Workspaces/dars_nic_391419_j3w9t_collab/SHDS/Mehrdad/helper_library/basic_functions

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE dars_nic_391419_j3w9t_collab.ccu049_longc_op_v2

# COMMAND ----------

#save_checkpoint(df_in=co_final, database="dars_nic_391419_j3w9t_collab", temp_df="ccu049_longc_op_v2", global_view_name="ccu049")
#save_checkpoint(df_in=co_final, database="dars_nic_391419_j3w9t_collab", temp_df="ccu049_longcoh_op_v2", global_view_name="ccu049")
save_checkpoint(df_in=co_final, database="dars_nic_391419_j3w9t_collab", temp_df="ccu049_nocovidc_op_v2", global_view_name="ccu049")
#save_checkpoint(df_in=co_final, database="dars_nic_391419_j3w9t_collab", temp_df="ccu049_nocovidh_op_v2", global_view_name="ccu049")
#save_checkpoint(df_in=co_final, database="dars_nic_391419_j3w9t_collab", temp_df="ccu049_onlycovid_op_v2", global_view_name="ccu049")
