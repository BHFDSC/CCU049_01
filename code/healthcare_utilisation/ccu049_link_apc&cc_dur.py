# Databricks notebook source
# MAGIC %md
# MAGIC # Link cohort to tables (GP,OP,APC,CC,ED)
# MAGIC
# MAGIC - Extract each cohort from base cohort table
# MAGIC - Link each cohort to relevant tables (GP, OP, APC, CC, ED)

# COMMAND ----------

# Import some libraries and packages
import pyspark.sql.functions as f
import pyspark.sql.functions as Seq
import pyspark.sql.types as T
import pandas as pd
import sys
from pyspark.sql.window import Window
from pyspark.sql import Row
from pyspark.sql.functions import col,min,max,lit
%matplotlib inline

# COMMAND ----------

# MAGIC %run /Workspaces/dars_nic_391419_j3w9t_collab/SHDS/Mehrdad/helper_library/date_management

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


hes_apc_archive = spark.sql(f'select * from dars_nic_391419_j3w9t_collab.hes_apc_all_years_archive')
hes_apc_otr_archive = spark.sql(f'select * from dars_nic_391419_j3w9t_collab.hes_apc_otr_all_years_archive')
hes_cc_archive = spark.sql(f'select * from dars_nic_391419_j3w9t_collab.hes_cc_all_years_archive')
icnarc_archive = spark.sql(f'select * from dars_nic_391419_j3w9t_collab.icnarc_dars_nic_391419_j3w9t_archive')
icd10 = "dss_corporate.icd10_group_chapter_v01"
opcs = "dss_corporate.opcs_codes_v02"


# without dars_nic_391419_j3w9t_collab. prefix
# 1. Long Covid historical
#output = "ccu049_longcoh_dur"
# 2. Non-COVID (contemporary)
#output = "ccu049_nocovidc_dur"
# 3. Pre-pandemic non-COVID (historical)
#output = "ccu049_nocovidh_dur"
# 4. Covid only non-Long Covid
#output = "ccu049_onlycovid_dur"




# COMMAND ----------

# Link the study cohort to HES-APC data
hes_apc = hes_apc_archive.filter(F.col("ProductionDate").startswith(production_date))
hes_apc_otr = hes_apc_otr_archive.filter(F.col("ProductionDate").startswith(production_date))
hes_cc = hes_cc_archive.filter(F.col("ProductionDate").startswith(production_date))
icnarc = icnarc_archive.filter(F.col("ProductionDate").startswith("2023-01-31"))

hes_apc = hes_apc.withColumnRenamed("PERSON_ID_DEID","id")
# hes_apc_otr = hes_apc_otr.withColumnRenamed("PERSON_ID_DEID","id")
hes_cc = hes_cc.withColumnRenamed("PERSON_ID_DEID","id")
icnarc = icnarc.withColumnRenamed("NHS_NUMBER_DEID","id")

# COMMAND ----------

# For 1. Long Covid group & 5. COVID only group

# Extract the study cohort IDs from the cohort table
co = spark.sql(f"SELECT id,date FROM {cohort}")


# For 2. Historical Long Covid group, 3. Non-COVID contemporary & 4. Pre-pandemic non-COVID group

# Extract the study cohort IDs from the cohort table
#co = spark.sql(f"SELECT id FROM {cohort}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inpatient Care duration

# COMMAND ----------

# Join cohort with hes_apc table

### For 1. Long Covid AND 5. COVID-only non Long Covid
co_hes_apc = co.join(hes_apc, on = "id", how = "left")
co_hes_apc = co_hes_apc.select('id','date','SUSRECID','ADMIDATE','DISDATE','SPELDUR','SPELDUR_CALC','EPIDUR','EPISTART','EPIEND','EPISTAT','EPIORDER','FAE','CLASSPAT','DIAG_4_01','DIAG_4_CONCAT','OPERTN_4_01','OPERTN_4_CONCAT', 'MAINSPEF', 'TRETSPEF', 'SUSHRG','ADMISORC','DISDEST','ELECDATE','ELECDUR','ELECDUR_CALC')
co_hes_apc = co_hes_apc.filter(F.col('ADMIDATE')>=F.col('date'))
co_hes_apc = co_hes_apc.filter(F.col('ADMIDATE')<= end_date)



### For 2. Long Covid historical, for 3. non-COVID contemporary, for 4. pre-pandemic non-COVID
#co_hes_apc = co.join(hes_apc, on = "id", how = "left")
#co_hes_apc = co_hes_apc.select('id','SUSRECID','ADMIDATE','DISDATE','SPELDUR','SPELDUR_CALC','EPIDUR','EPISTART','EPIEND','EPISTAT','EPIORDER','FAE','CLASSPAT','DIAG_4_01','DIAG_4_CONCAT','OPERTN_4_01','OPERTN_4_CONCAT', 'MAINSPEF', 'TRETSPEF', 'SUSHRG','ADMISORC','DISDEST','ELECDATE','ELECDUR','ELECDUR_CALC')
#co_hes_apc = co_hes_apc.filter((F.col('ADMIDATE')>=start_date) & (F.col('ADMIDATE')<=end_date))


# COMMAND ----------

co_hes_apc = co_hes_apc.dropDuplicates()

# COMMAND ----------

from pyspark.sql import functions as F, Window
from pyspark.sql.functions import sum
window_sp = Window.partitionBy(co_hes_apc["id"],co_hes_apc["ADMIDATE"])
co_hes_apc = co_hes_apc.withColumn('spel_dur', sum(col('EPIDUR')).over(window_sp))

win = Window.partitionBy(co_hes_apc["id"])
co_hes_apc = co_hes_apc.withColumn('total_dur',sum(col('EPIDUR')).over(win))

# COMMAND ----------

# For 1. Long Covid, 3. Non COVID (contemporary) & 5. COVID only non Long Covid

from pyspark.sql.functions import *
co_hes_apc_new = co_hes_apc.withColumn('epi_st_yr',f.year(f.to_timestamp('EPISTART', 'yyyy-MM-dd')))
co_hes_apc_new = co_hes_apc_new.withColumn('epi_en_yr',f.year(f.to_timestamp('EPIEND', 'yyyy-MM-dd')))
co_hes_apc_new = co_hes_apc_new.withColumn('end_2020',lit('2020-12-31')).withColumn('end_2021',lit('2021-12-31')).withColumn('end_2022',lit('2022-12-31')).withColumn('end_2023',lit('2023-01-31'))


co_hes_apc_new = co_hes_apc_new.withColumn('epi_dur_2020',
                                       F.when((F.col('epi_st_yr')=='2020')&(F.col('epi_en_yr')=='2020'), col('EPIDUR'))
                                      .when ((F.col('epi_st_yr')=='2020')&(F.col('epi_en_yr')>'2020'), (datediff('end_2020','EPISTART'))).otherwise(0))



co_hes_apc_new = co_hes_apc_new.withColumn('epi_dur_2021',
                                           F.when((F.col('epi_st_yr')=='2021')&(F.col('epi_en_yr')=='2021'), col('EPIDUR'))
                                           .when((F.col('epi_st_yr')=='2021')&(F.col('epi_en_yr')>'2021'), datediff('end_2021','EPISTART'))
                                           .when((F.col('epi_st_yr')=='2020')&(F.col('epi_en_yr')=='2021'), datediff('EPIEND', 'end_2020'))
                                           .otherwise(0))

co_hes_apc_new = co_hes_apc_new.withColumn('epi_dur_2022',
                                       F.when((F.col('epi_st_yr')=='2022')&(F.col('epi_en_yr')=='2022'), col('EPIDUR'))
                                      .when ((F.col('epi_st_yr')=='2022')&(F.col('epi_en_yr')>'2022'), datediff('EPISTART', 'end_2021')).when((F.col('epi_st_yr')<'2022')&(F.col('epi_en_yr')=='2022'),datediff('EPIEND','end_2021')).otherwise(0))



co_hes_apc_new = co_hes_apc_new.withColumn('epi_dur_2023',
                                       F.when((F.col('EPISTART')>='2023-01-01')&(F.col('EPIEND')<='2023-01-31'), col('EPIDUR'))
                                      .when((F.col('EPISTART')<'2023-01-01')&(F.col('EPIEND')<='2023-01-31'),datediff('EPIEND','end_2022')).when((F.col('EPISTART')<'2023-01-01')&(F.col('EPIEND')>'2023-01-31'),31).when((F.col('EPISTART')>='2023-01-01')&(F.col('EPIEND')>'2023-01-31'),datediff('end_2023','EPISTART')).otherwise(0))



co_hes_apc_new = co_hes_apc_new.withColumn('total_dur_2020',sum(col('epi_dur_2020')).over(win)).withColumn('total_dur_2021',sum(col('epi_dur_2021')).over(win)).withColumn('total_dur_2022',sum(col('epi_dur_2022')).over(win)).withColumn('total_dur_2023',sum(col('epi_dur_2023')).over(win))

co_hes_apc_new = co_hes_apc_new.dropDuplicates(['id'])
co_hes_apc_new = co_hes_apc_new.select('id','total_dur_2020','total_dur_2021','total_dur_2022','total_dur_2023')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Critical Care duration

# COMMAND ----------

# MAGIC %run /Workspaces/dars_nic_391419_j3w9t_collab/SHDS/Mehrdad/helper_library/date_management

# COMMAND ----------

### For 1. Long Covid AND 5. COVID-only non Long Covid
co_hes_cc = co.join(hes_cc, on= "id", how= "left")
co_hes_cc = co_hes_cc.select('id','date','SUSRECID','ADMIDATE','DISDATE','EPISTART','EPIEND','CCSTARTDATE','CCDISDATE','CCACTSEQ','BESTMATCH')
co_hes_cc = co_hes_cc.filter((F.col('ADMIDATE')>=F.col('date')) & (F.col('ADMIDATE')<= end_date))


### For 2. Long Covid historical, for 3. non-COVID contemporary, for 4. pre-pandemic non-COVID
# Join cohort with hes_cc table
#co_hes_cc = co.join(hes_cc, on= "id", how= "left")
#co_hes_cc = co_hes_cc.select('id','SUSRECID','ADMIDATE','DISDATE','EPISTART','EPIEND','CCSTARTDATE','CCDISDATE','CCACTSEQ','BESTMATCH')
#co_hes_cc = co_hes_cc.filter((F.col('ADMIDATE')>=start_date) & (F.col('ADMIDATE')<=end_date))

co_hes_cc = co_hes_cc.filter(F.col('BESTMATCH')==1)

# COMMAND ----------

co_hes_cc = co_hes_cc.dropDuplicates()

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime
from pyspark.sql.functions import *
# Let's use a function from date_management (imported above) to replace the REG_DATE_OF_DEATH column with yyyy-mm-dd format
# Note: this column copies the old column to old_REG_DATE_OF_DEATH
co_hes_cc = manage_deaths_recdate(df=co_hes_cc, col_name="CCSTARTDATE")
co_hes_cc = manage_deaths_recdate(df=co_hes_cc, col_name="CCDISDATE")
co_hes_cc = co_hes_cc.withColumn('cc_dur', datediff(col('CCDISDATE'), col('CCSTARTDATE')))

# COMMAND ----------

co_hes_cc_nodup = co_hes_cc.dropDuplicates(['id','CCSTARTDATE','CCDISDATE'])
co_hes_cc_nodup = co_hes_cc_nodup.dropDuplicates(['id','CCSTARTDATE'])
co_hes_cc_nodup = co_hes_cc_nodup.dropDuplicates(['id','CCDISDATE'])

# COMMAND ----------

from pyspark.sql import functions as F, Window
from pyspark.sql.functions import sum
window_s = Window.partitionBy(co_hes_cc_nodup["id"],co_hes_cc_nodup["ADMIDATE"])
co_hes_cc_dur = co_hes_cc_nodup.withColumn('spel_cc_dur', sum(col('cc_dur')).over(window_s))

wind = Window.partitionBy(co_hes_cc_dur["id"])
co_hes_cc_dur = co_hes_cc_dur.withColumn('total_cc_dur',sum(col('cc_dur')).over(wind))


# COMMAND ----------

# For 1. Long Covid, 3. Non COVID (contemporary) & 5. COVID only non Long Covid

from pyspark.sql.functions import *
co_hes_cc_new = co_hes_cc_dur.withColumn('cc_st_yr',f.year(f.to_timestamp('CCSTARTDATE', 'yyyy-MM-dd')))
co_hes_cc_new = co_hes_cc_new.withColumn('cc_en_yr',f.year(f.to_timestamp('CCDISDATE', 'yyyy-MM-dd')))
co_hes_cc_new = co_hes_cc_new.withColumn('end_2020',lit('2020-12-31')).withColumn('end_2021',lit('2021-12-31')).withColumn('end_2022',lit('2022-12-31')).withColumn('end_2023',lit('2023-01-31'))


co_hes_cc_new = co_hes_cc_new.withColumn('cc_dur_2020',
                                       F.when((F.col('cc_st_yr')=='2020')&(F.col('cc_en_yr')=='2020'), col('cc_dur'))
                                      .when ((F.col('cc_st_yr')=='2020')&(F.col('cc_en_yr')>'2020'), (datediff('end_2020','CCSTARTDATE'))).otherwise(0))



co_hes_cc_new = co_hes_cc_new.withColumn('cc_dur_2021',
                                           F.when((F.col('cc_st_yr')=='2021')&(F.col('cc_en_yr')=='2021'), col('cc_dur'))
                                           .when((F.col('cc_st_yr')=='2021')&(F.col('cc_en_yr')>'2021'), datediff('end_2021','CCSTARTDATE'))
                                           .when((F.col('cc_st_yr')=='2020')&(F.col('cc_en_yr')=='2021'), datediff('CCDISDATE', 'end_2020'))
                                           .otherwise(0))

co_hes_cc_new = co_hes_cc_new.withColumn('cc_dur_2022',
                                       F.when((F.col('cc_st_yr')=='2022')&(F.col('cc_en_yr')=='2022'), col('cc_dur'))
                                      .when ((F.col('cc_st_yr')=='2022')&(F.col('cc_en_yr')>'2022'), datediff('CCSTARTDATE', 'end_2021')).when((F.col('cc_st_yr')<'2022')&(F.col('cc_en_yr')=='2022'),datediff('CCDISDATE','end_2021')).otherwise(0))

co_hes_cc_new = co_hes_cc_new.withColumn('cc_dur_2023',
                                       F.when((F.col('CCSTARTDATE')>='2023-01-01')&(F.col('CCDISDATE')<='2023-01-31'), col('cc_dur'))
                                      .when((F.col('CCSTARTDATE')<'2023-01-01')&(F.col('CCDISDATE')<='2023-01-31'),datediff('CCDISDATE','end_2022')).when((F.col('CCSTARTDATE')<'2023-01-01')&(F.col('CCDISDATE')>'2023-01-31'),31).when((F.col('CCSTARTDATE')>='2023-01-01')&(F.col('CCDISDATE')>'2023-01-31'),datediff('end_2023','CCSTARTDATE')).otherwise(0))


co_hes_cc_new = co_hes_cc_new.withColumn('cc_dur_2020',sum(col('cc_dur_2020')).over(wind)).withColumn('cc_dur_2021',sum(col('cc_dur_2021')).over(wind)).withColumn('cc_dur_2022',sum(col('cc_dur_2022')).over(wind)).withColumn('cc_dur_2023',sum(col('cc_dur_2023')).over(wind))

co_hes_cc_new = co_hes_cc_new.dropDuplicates(['id'])
co_hes_cc_new = co_hes_cc_new.select('id','cc_dur_2020','cc_dur_2021','cc_dur_2022','cc_dur_2023')
display(co_hes_cc_new)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Create tables showing N. of days in critical care and N. of days in APC (general ward) care

# COMMAND ----------

co_hes_apc_pp = co_hes_apc.dropDuplicates(['id'])
co_hes_cc_pp = co_hes_cc_dur.dropDuplicates(['id'])
co_cc_apc_pp = co_hes_apc_pp.join(co_hes_cc_pp, on = 'id', how = 'left')
co_cc_apc_pp = co_cc_apc_pp.withColumn('apc_dur',F.when(F.col('total_cc_dur').isNull(), F.col('total_dur')).otherwise(F.when((F.col('total_cc_dur').isNotNull()),F.col('total_dur')-F.col('total_cc_dur'))))
co_cc_apc_pp = co_cc_apc_pp.filter(F.col('apc_dur')>=0)
#display(co_cc_apc_pp)

# COMMAND ----------

# For 1. Long Covid, 3. Non COVID (contemporary) & 5. COVID only non Long Covid
co_cc_apc_new = co_hes_apc_new.join(co_hes_cc_new, on = 'id', how = 'left')

co_cc_apc_new = co_cc_apc_new.withColumn('apc_dur_2020',F.when(F.col('cc_dur_2020').isNotNull(), F.col('total_dur_2020')-F.col('cc_dur_2020')).otherwise(F.col('total_dur_2020'))).withColumn('apc_dur_2021',F.when(F.col('cc_dur_2021').isNotNull(), F.col('total_dur_2021')-F.col('cc_dur_2021')).otherwise(F.col('total_dur_2021'))).withColumn('apc_dur_2022',F.when(F.col('cc_dur_2022').isNotNull(), F.col('total_dur_2022')-F.col('cc_dur_2022')).otherwise(F.col('total_dur_2022'))).withColumn('apc_dur_2023',F.when(F.col('cc_dur_2023').isNotNull(), F.col('total_dur_2023')-F.col('cc_dur_2023')).otherwise(F.col('total_dur_2023')))
co_cc_apc_new = co_cc_apc_new.filter(F.col('apc_dur_2020')>=0)
co_cc_apc_new = co_cc_apc_new.filter(F.col('apc_dur_2021')>=0)
co_cc_apc_new = co_cc_apc_new.filter(F.col('apc_dur_2022')>=0)
co_cc_apc_new = co_cc_apc_new.filter(F.col('apc_dur_2023')>=0)

#display(co_cc_apc_new)

# COMMAND ----------

# For 1. Long Covid & 3. non-COVID contemporary & 5. COVID only non Long Covid
co_cc_apc_dur = co_cc_apc_pp.select('id','apc_dur','total_cc_dur','total_dur')
co_cc_apc_dur = co_cc_apc_dur.join(co_cc_apc_new, on='id', how='left')
co_cc_apc_dur = co_cc_apc_dur.select('id','apc_dur','total_cc_dur','total_dur','apc_dur_2020','cc_dur_2020','total_dur_2020','apc_dur_2021','cc_dur_2021','total_dur_2021','apc_dur_2022','cc_dur_2022','total_dur_2022','apc_dur_2023','cc_dur_2023','total_dur_2023')
co_cc_apc_dur = co_cc_apc_dur.withColumnRenamed('total_cc_dur','cc_dur')
co_final = co.join(co_cc_apc_dur, on='id', how='left')



### For 2. Long Covid historical, and 4. pre-pandemic non-COVID
#co_cc_apc_dur = co_cc_apc_pp.select('id','apc_dur','total_cc_dur','total_dur')
#co_cc_apc_dur = co_cc_apc_dur.withColumnRenamed('total_cc_dur','cc_dur')
#co_final = co.join(co_cc_apc_dur, on='id', how='left')


# COMMAND ----------

co_final = co_final.drop('date')

# COMMAND ----------

# MAGIC %run /Workspaces/dars_nic_391419_j3w9t_collab/SHDS/Mehrdad/helper_library/basic_functions

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE dars_nic_391419_j3w9t_collab.ccu049_onlycovid_dur_v2

# COMMAND ----------

#save_checkpoint(df_in=co_final, database="dars_nic_391419_j3w9t_collab", temp_df="ccu049_longc_dur_v2", global_view_name="ccu049")
#save_checkpoint(df_in=co_final, database="dars_nic_391419_j3w9t_collab", temp_df="ccu049_longcoh_dur_v2", global_view_name="ccu049")
#save_checkpoint(df_in=co_final, database="dars_nic_391419_j3w9t_collab", temp_df="ccu049_nocovidc_dur_v2", global_view_name="ccu049")
#save_checkpoint(df_in=co_final, database="dars_nic_391419_j3w9t_collab", temp_df="ccu049_nocovidh_dur_v2", global_view_name="ccu049")
save_checkpoint(df_in=co_final, database="dars_nic_391419_j3w9t_collab", temp_df="ccu049_onlycovid_dur_v2", global_view_name="ccu049")
