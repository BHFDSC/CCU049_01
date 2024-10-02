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

production_date = "2023-04-27" # Notebook CCU03_01_create_table_aliases   Cell 8
# Table names
# 1. Long Covid
#cohort = "dars_nic_391419_j3w9t_collab.ccu049_longc"
#end_date = "2023-01-31"


# 2. Long Covid historical
#cohort = "dars_nic_391419_j3w9t_collab.ccu049_longc"
#start_date = "2018-01-01"
#end_date = "2019-12-31"


# 3. Non-COVID (contemporary)
#cohort = "dars_nic_391419_j3w9t_collab.ccu049_cohortc"
#start_date = "2020-01-29"
#end_date = "2022-03-31"

# 4. Pre-pandemic non-COVID (historical)
#cohort = "dars_nic_391419_j3w9t_collab.ccu049_cohorth"
#start_date = "2018-01-01"
#end_date = "2019-12-31"


# 5. Covid only non-Long Covid (chase till 2023-01-31)
cohort = "dars_nic_391419_j3w9t_collab.ccu049_covid"
end_date = "2023-01-31"



gdppr_archive = spark.sql(f'select * from dars_nic_391419_j3w9t_collab.gdppr_dars_nic_391419_j3w9t_archive')
snomed = "dss_corporate.snomed_sct2_description_full"


# COMMAND ----------

# MAGIC %md
# MAGIC ## Update 'date of death' data

# COMMAND ----------

# MAGIC %run "../SHDS/Mehrdad/helper_library/date_management"

# COMMAND ----------

death = spark.sql(f"SELECT DEC_CONF_NHS_NUMBER_CLEAN_DEID, REG_DATE_OF_DEATH FROM dars_nic_391419_j3w9t.deaths_dars_nic_391419_j3w9t")
death = death.withColumnRenamed('DEC_CONF_NHS_NUMBER_CLEAN_DEID','id')
death = manage_deaths_recdate(df=death, col_name="REG_DATE_OF_DEATH")
death = death.select('id','REG_DATE_OF_DEATH')
display(death)

# COMMAND ----------

# Extract the study cohort IDs from the Long Covid cohort table, 'date' means the start date of following-up

# Extract the study cohort IDs from the Long Covid cohort table
co = spark.sql(f"SELECT id,date FROM {cohort}")
co = co.join(death, on = "id", how = "left")
co = co.withColumn('dod', when(F.col('REG_DATE_OF_DEATH')>end_date, None).otherwise(F.col('REG_DATE_OF_DEATH')))
co = co.drop('REG_DATE_OF_DEATH')
display(co)


# For 3. Non COVID (contemporary) & 4. Non COVID (historical)

#co = spark.sql(f"SELECT id FROM {cohort}")
#co = co.join(death, on = "id", how = "left")
#co = co.withColumn('dod', when(F.col('REG_DATE_OF_DEATH')>end_date, None).otherwise(F.col('REG_DATE_OF_DEATH')))
#co = co.drop('REG_DATE_OF_DEATH')
#co = co.dropDuplicates(['id'])
#display(co)




# COMMAND ----------

co = co.dropDuplicates(['id'])

# COMMAND ----------

# For 1. Long Covid group & 5. COVID only group

### This is per patient: Total follow up month or day
co = co.withColumn('end_date',lit(end_date))
co = co.withColumn("followup_day", when(F.col('dod').isNull(), datediff('end_date', col("date"))).otherwise(datediff(col("dod"), col("date"))))
co = co.drop('end_date','start_date')
###co = co.withColumn('start_2020', lit('2020-01-01'))
### In order to find out the utilisation per calander date, we need to convert GP record date to days since start of 2020, so later on, we can calculate utilisaton per calander month/year (2020: <=365; 2021: <=730; 2022: <=1095)
###co = co.withColumn("calendar_day", datediff(col('RECORD_DATE'), col('start_2020')))
###co = co.withColumn("followup_mon", round(col('followup_day')/30.42,2))
###co = co.withColumn("followup_yr", round(col('followup_day')/365,2))
###display(co)



# For 2. Long Covid historical group

### This is per patient: Total follow up month or day
#co = co.withColumn("followup_day", lit(729))


# For 3. non-COVID (contemporary)

### This is per patient: Total follow up month or day
#co = co.withColumn('end_date',lit(end_date))
#co = co.withColumn('start_date',lit(start_date))
#co = co.withColumn("followup_day", when(F.col('dod').isNull(), datediff(col('end_date'), col('start_date'))).otherwise(datediff(col("dod"), col('start_date'))))
#co = co.withColumn('start_2020', lit('2020-01-01'))
### In order to find out the utilisation per calander date, we need to convert GP record date to days since start of 2020, so later on, we can calculate utilisaton per calander month/year (2020: <=365; 2021: <=730; 2022: <=1095)
#co = co.withColumn("calendar_day", datediff(col('RECORD_DATE'), col('start_2020')))
#display(co)


# For 4. pre-pandemic non-COVID group

### This is per patient: Total follow up month or day
#co = co.withColumn('end_date',lit(end_date))
#co = co.withColumn('start_date',lit(start_date))
#co = co.withColumn("followup_day", when(F.col('dod').isNull(), datediff(col('end_date'), col('start_date'))).otherwise(datediff(col("dod"), col('start_date'))))
#co = co.drop('end_date','start_date')

display(co)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Join cohort with GDPPR

# COMMAND ----------

# Link the study cohort to GDPPR data
gdppr = gdppr_archive.filter(F.col("ProductionDate").startswith(production_date))
gdppr = gdppr.withColumnRenamed("NHS_NUMBER_DEID","id")
gdppr = gdppr.drop('DATE')
#display(gdppr)

# COMMAND ----------

# Join cohort with GP table

### For 1. Long Covid AND 5. COVID-only non Long Covid
co_gp = co.join(gdppr, on= "id", how= "left")
co_gp = co_gp.select('id','date','RECORD_DATE','CODE','EPISODE_PRESCRIPTION','VALUE1_CONDITION','VALUE2_CONDITION','VALUE1_PRESCRIPTION','VALUE2_PRESCRIPTION')
co_gp = co_gp.filter(F.col('RECORD_DATE')>=F.col('date'))
co_gp = co_gp.filter(F.col('RECORD_DATE')<= end_date)
### We also want to remove the prescription-only visits
co_gp_nopre = co_gp.filter(F.col('EPISODE_PRESCRIPTION').isNull())
###co_gp_nopre = lc_gp_nopre.join(snomed, on = 'CODE', how = 'left')
co_gp_med = co_gp.filter(F.col('EPISODE_PRESCRIPTION').isNotNull())
co_gp_test = co_gp.filter(F.col('VALUE1_CONDITION').isNotNull())
###display(co_gp_nopre)



### For 2. Long Covid historical, for 3. non-COVID contemporary, for 4. pre-pandemic non-COVID

### Join lc cohort with GP table
#co_gp = co.join(gdppr, on= "id", how= "left")
#co_gp = co_gp.select('id','RECORD_DATE','CODE','EPISODE_PRESCRIPTION','VALUE1_CONDITION','VALUE2_CONDITION','VALUE1_PRESCRIPTION','VALUE2_PRESCRIPTION')
#co_gp = co_gp.filter(F.col('RECORD_DATE')>=start_date)
#co_gp = co_gp.filter(F.col('RECORD_DATE')<= end_date)
### We also want to remove the prescription-only visits
#co_gp_nopre = co_gp.filter(F.col('EPISODE_PRESCRIPTION').isNull())
###co_gp_nopre = lc_gp_nopre.join(snomed, on = 'CODE', how = 'left')
#co_gp_med = co_gp.filter(F.col('EPISODE_PRESCRIPTION').isNotNull())
#co_gp_test = co_gp.filter(F.col('VALUE1_CONDITION').isNotNull())
#display(co_gp_nopre)


# COMMAND ----------

co_gp_nopre = co_gp_nopre.dropDuplicates()

# COMMAND ----------

# This is per patient: Count number of GP visits per patient
from pyspark.sql import functions as F, Window
w = Window.partitionBy(co_gp_nopre["id"])
co_gp_nopre = co_gp_nopre.withColumn('gp_all', F.size(F.collect_set('RECORD_DATE').over(w)))

co_gp_nopre = co_gp_nopre.withColumn('year',f.year(f.to_timestamp('RECORD_DATE', 'yyyy-MM-dd')))
w_1 = Window.partitionBy(co_gp_nopre["id"],co_gp_nopre["year"])
co_gp_nopre = co_gp_nopre.withColumn('gp_year', F.size(F.collect_set('RECORD_DATE').over(w_1)))
###lc_gp_nopre = lc_gp_nopre.withColumn('gp_all_per_mon', round(F.col('gp_all')/F.col('followup_mon'),2))



# For 2. Long Covid historical group & 4. Non COVID (historical)

### This is per patient: Count number of GP visits per patient
#from pyspark.sql import functions as F, Window
#w = Window.partitionBy(co_gp_nopre["id"])
#co_gp_nopre = co_gp_nopre.withColumn('gp_all', F.size(F.collect_set('RECORD_DATE').over(w)))

#display(co_gp_nopre)

# COMMAND ----------

# For 1. Long Covid, 3. Non COVID (contemporary) & 5. COVID only non Long Covid

co_gp_nopre_new = co_gp_nopre.groupBy("id").pivot("year").agg(F.first("gp_year").alias("gp"))
co_gp_nopre_new = co_gp_nopre_new.withColumnRenamed('2020','gp_2020').withColumnRenamed('2021','gp_2021').withColumnRenamed('2022','gp_2022').withColumnRenamed('2023','gp_2023')

#display(co_gp_nopre_new)

# COMMAND ----------

# For 1. Long Covid, 3. Non COVID (contemporary) & 5. COVID only non Long Covid

co_gp_nopre = co_gp_nopre.join(co_gp_nopre_new, on='id', how='left')
#display(co_gp_nopre)

# COMMAND ----------

# Drop duplicated records based on 'id', then we have number of visits/follow ups per patient
co_gp_nopre_nodup = co_gp_nopre.dropDuplicates(['id'])

# COMMAND ----------

### Adding the column of visits and follow-up to the main Long Covid cohort table

### For 1. Long Covid & 5. COVID only group
co_gp_nopre_nodup = co_gp_nopre_nodup.select('id','gp_all','gp_2020','gp_2021','gp_2022','gp_2023')
co_final = co.join(co_gp_nopre_nodup, on='id', how='left')

### For 2. Long Covid historical, and pre-pandemic non-COVID
#co_gp_nopre_nodup = co_gp_nopre_nodup.select('id','gp_all')
#co_final = co.join(co_gp_nopre_nodup, on='id', how='left')


### For 3. Non-COVID contemporary
### Adding the column of visits and follow-up to the main Long Covid cohort table
#co_gp_nopre_nodup = co_gp_nopre_nodup.select('id','gp_all','followup_day','gp_2020','gp_2021','gp_2022')
#co_final = co.join(co_gp_nopre_nodup, on='id', how='left')

# COMMAND ----------

### For 1. Long Covid and 5. COVID only
co_final = co_final.drop('date')

# COMMAND ----------

# MAGIC %run /Workspaces/dars_nic_391419_j3w9t_collab/SHDS/Mehrdad/helper_library/basic_functions

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE dars_nic_391419_j3w9t_collab.ccu049_onlycovid_gp_v2

# COMMAND ----------

#save_checkpoint(df_in=co_final, database="dars_nic_391419_j3w9t_collab", temp_df="ccu049_longc_gp_v2", global_view_name="ccu049")
#save_checkpoint(df_in=co_final, database="dars_nic_391419_j3w9t_collab", temp_df="ccu049_longcoh_gp_v2", global_view_name="ccu049")
#save_checkpoint(df_in=co_final, database="dars_nic_391419_j3w9t_collab", temp_df="ccu049_nocovidc_gp_v2", global_view_name="ccu049")
#save_checkpoint(df_in=co_final, database="dars_nic_391419_j3w9t_collab", temp_df="ccu049_nocovidh_gp_v2", global_view_name="ccu049")
save_checkpoint(df_in=co_final, database="dars_nic_391419_j3w9t_collab", temp_df="ccu049_onlycovid_gp_v2", global_view_name="ccu049")
