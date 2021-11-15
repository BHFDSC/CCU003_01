# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC **Project:** ***CCU003-CKD***
# MAGIC 
# MAGIC **Description** This notebook creats cohort tables
# MAGIC 
# MAGIC **Requierments** CCU003-CKD-CreateCodeList
# MAGIC 
# MAGIC **Author(s)** Muhammad Dashtban (aka. Ashkan)
# MAGIC 
# MAGIC **Date last updated** 10/22/2021
# MAGIC 
# MAGIC **Date last run** 04/02/2021, 5:02:35 PM 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Functions
# MAGIC 
# MAGIC **Author** Muhammad Dashtban (Ashkan) - a.dashtban@ucl.ac.uk

# COMMAND ----------

import pyspark.sql.functions as f
import pandas as pd
from pyspark.sql.functions import to_date, lit, col, when
from functools import reduce
import functools as fu
import pyspark.sql.types as fy
from pyspark.sql import HiveContext
assert isinstance(sqlContext, HiveContext)
from pyspark.sql.window import Window
from matplotlib import cm
from pyspark.sql import DataFrameStatFunctions as statFunc

%matplotlib inline

#::::::::::::::::::::::::::::::::::::::: helper functions :: a.dashtban@ucl.ac.uk 
def quant(df,var,q=[.25,.5,.75]):
  #import numpy as np
  #import pyspark.sql.functions as f
  print("quantiles: ",str(q),"::",
       [i for i in df.filter(col(var).isNotNull()).select(f.expr(f'percentile_approx({var}, array({",".join([str(i) for i in q])}))').alias("quants")).collect()[0]][0])

def scol(df,*vars): #search for columns in table
  #usage : col(df,"Nhs","sex")
  #usage : search_terms=["nhs","sex"]
  #        col(df,search_terms)
  if(type(vars[0])==tuple): vars=vars[0]
  print(df.select(df.colRegex("`.*("+'|'.join([i for i in vars])+").*`")).columns)

def tabg(data,*vars,id="id",agg="count"):
  if(type(vars[0])==tuple): vars=vars[0]
  fnlist={
  "count":f.count,
  "sum":f.sum,
  "max":f.max,
  "min":f.min,
  "mean":f.mean
}
  fg=lambda x:fnlist.get(agg)(x)
  #data.groupby([i for i in vars]).agg(fg(col(id))).sort(vars[0]).show()
  dt=data.groupby([i for i in vars]).agg(fg(col(id))).sort(vars[0])
  #print(pd.DataFrame(dt,columns=dt.columns))
  return(pd.DataFrame(dt.collect(),columns=dt.columns))


def tab(data,*vars,scale=1,order=1):
  if(type(vars[0])==tuple): vars=vars[0]
  #data.groupby([i for i in vars]).count().sort(vars[0]).show()
  dt=data.groupby([i for i in vars]).count().sort([i for i in vars],ascending=order)
  df=pd.DataFrame(dt.collect(),columns=dt.columns)
  df["count"] = df["count"].apply(pd.to_numeric, errors='coerce')/scale
  return(df)
       

def addCat(df,cat_name="age_cat",base_var="age",cat_ind="age_ind"):
  df=df.withColumn(cat_name,when(col(base_var)<=50,"<=50").when((col(base_var)>50) & (col(base_var)<=60),"51-60").when((col(base_var)>60) & (col(base_var)<=70),"61-70").when((col(base_var)>70) & (col(base_var)<=80),"71-80").otherwise(">80"))
  df=df.withColumn(cat_ind,when(col(base_var)<=50,1).when((col(base_var)>50) & (col(base_var)<=60),2).when((col(base_var)>60) & (col(base_var)<=70),3).when((col(base_var)>70) & (col(base_var)<=80),4).otherwise(5))
  return(df)
  

  
def addAge(df,age="age",dob="dob",dod="dod",index_date="2020-01-01",age_cat="age_cat",cat_index="age_index"):
  df=df.withColumn("age",f.when(col(dod).isNotNull(),f.round(f.datediff(col(dod),col(dob))/365.25,1)).otherwise(f.round(f.datediff(f.to_date(f.lit(index_date)),col(dob))/365.25,1)))
  
  df=df.withColumn(age_cat,when(col(age)<=50,"<=50").when((col(age)>50) & (col(age)<=60),"51-60").when((col(age)>60) & (col(age)<=70),"61-70").when((col(age)>70) & (col(age)<=80),"71-80").otherwise(">80"))
  df=df.withColumn(cat_index,when(col(age)<=50,1).when((col(age)>50) & (col(age)<=60),2).when((col(age)>60) & (col(age)<=70),3).when((col(age)>70) & (col(age)<=80),4).otherwise(5))
  return(df)
  
  
def load2(tableName:str,dbname="dars_nic_391419_j3w9t"):
  ckd_med_table=spark.table(f"""{dbname}.{tableName}""")
  return(ckd_med_table)


def load(tableName:str,dbname="dars_nic_391419_j3w9t_collab"):
  ckd_med_table=spark.table(f"""{dbname}.{tableName}""")
  return(ckd_med_table)

#override for creting intermediate one
def loadmed():
  ckd_med_table=spark.table(f"""dars_nic_391419_j3w9t_collab.ckd_med_table""")
  return(ckd_med_table)

def rep(df,tableName="ckd_med_table"): #--replicate
  save(df,tableName)
  return(load(tableName))

def save(df,tableName="ckd_med_table"):
  spark.sql(f"""DROP TABLE IF EXISTS dars_nic_391419_j3w9t_collab.{tableName}""")
  spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")
  df.createOrReplaceGlobalTempView(tableName)
  spark.sql(f"""CREATE TABLE dars_nic_391419_j3w9t_collab.{tableName} as SELECT * FROM global_temp.{tableName}""")
  spark.sql(f"""ALTER TABLE dars_nic_391419_j3w9t_collab.{tableName} OWNER TO dars_nic_391419_j3w9t_collab""")
  
#drop intermediate table or other tables
def rm(tableName="ckd_med_table",dbname="dars_nic_391419_j3w9t_collab"):
  spark.sql(f"""DROP TABLE IF EXISTS {dbname}.{tableName}""")

  
  
## load HES/GDDPR

def loadhes(exit_date="2020-12-01"):
  hes=spark.table(f"""dars_nic_391419_j3w9t_collab.hes_apc_all_years""")
  hes=hes.filter(f.col("EPISTART") < exit_date ).select(col("PERSON_ID_DEID").alias("id"),col("EPISTART").alias("date"),col("DIAG_4_CONCAT").alias("code"),col("SEX").alias("sex"))
  hes = hes.withColumn("code",f.regexp_replace(f.col("code"), "\\.", ""))
  return(hes)

def loadgdppr(exit_date="2020-12-01"):
  gdppr=spark.table(f"""dars_nic_391419_j3w9t.gdppr_dars_nic_391419_j3w9t""")
  gdppr=gdppr.filter(f.col("DATE") < exit_date ).select(col("NHS_NUMBER_DEID").alias("id"),col("DATE").alias("date"),col("CODE").alias("code"),col("SEX").alias("sex"),col("YEAR_OF_BIRTH").alias("yob"))
  gdppr=gdppr.withColumn("dob",f.concat(f.col("yob"),f.lit("-07-01")).cast("date"))
  gdppr=gdppr.drop("yob")
  return(gdppr)


####----------create covid covariates-------------------------------

def loadcovid(covid="covid",cov_date="c_date",cov_status="c_status",cov_month="c_month",covid_flag="covid"):
  #cov.drop("cov_flag","cov_date","cov_status","covid")
  # load new covid test data table from ccu002 - possible candidate for central COVID-positive patients table
  cov = spark.table('dars_nic_391419_j3w9t_collab.ccu002_covid19_test_all')
  cov=cov.withColumnRenamed("nhs_number_deid","id").groupBy("id").agg(f.min("record_date").alias(cov_date),f.max("Covid19_status").alias(cov_status)).filter(f.substring(col(cov_date),1,4)>=2020)
  #outputs ['id', 'cov_date', 'cov_status']
  #add covid flag
  cov=cov.withColumn(covid_flag,f.when(col(cov_status)!="Suspected_COVID19",1).otherwise(-1))
  cov=cov.withColumn(cov_month,f.substring(col(cov_date),6,2).cast("integer"))
  return(cov)

def joincovid(df,covid_flag="covid",cov_date="c_date",cov_status="c_status",cov_month="c_month",confirmed_covid="covidc"):
  df=df.drop("cov_flag","cov_date","cov_status","covid",covid_flag,cov_date,cov_status,cov_month,confirmed_covid)
  # load new covid test data table from ccu002 - possible candidate for central COVID-positive patients table
  cov = spark.table('dars_nic_391419_j3w9t_collab.ccu002_covid19_test_all')
  cov=cov.withColumnRenamed("nhs_number_deid","id").groupBy("id").agg(f.min("record_date").alias(cov_date),f.max("Covid19_status").alias(cov_status)).filter(f.substring(col(cov_date),1,4)>=2020)
  #outputs ['id', 'cov_date', 'cov_status']
  #add covid flag
  cov=cov.withColumn(confirmed_covid,f.when(col(cov_status)!="Suspected_COVID19",1).otherwise(0))
  
  #join with table and add covid flag
  df=df.join(cov,["id"],how="left")
  df=df.withColumn(covid_flag,f.when(col(cov_date).isNotNull(),1).otherwise(0))
  df=df.withColumn(cov_month,f.substring(col(cov_date),6,2).cast("integer"))
  return(df)


## add ONS covariates

def loadDeath(dod="dod",yod="yod",mod="mod",dor="dor"):
  dt0=load2("deaths_dars_nic_391419_j3w9t")
  dt=dt0.select(f.col("DEC_CONF_NHS_NUMBER_CLEAN_DEID").alias("DID"),f.col("REG_DATE").alias("dor"),col("REG_DATE_OF_DEATH").alias("dod"))
  
  dt=dt.withColumn("dod",f.concat(f.substring(dt.dod,1,4),f.lit("-"),f.substring(dt.dod,5,2),f.lit("-"),f.substring(dt.dod,7,2)).cast("date"))
  dt=dt.withColumn("dor",f.concat(f.substring(dt.dor,1,4),f.lit("-"),f.substring(dt.dor,5,2),f.lit("-"),f.substring(dt.dor,7,2)).cast("date"))
  
  dt=dt.filter(dt.dod.isNotNull())
  dt=dt.filter(dt.dor.isNotNull())
  dt=dt.filter(dt.DID.isNotNull())
  dt=dt.groupby("DID").agg(f.min(dt.dod).alias("dod"),f.min(dt.dor).alias("dor"))
  
  dt=dt.withColumn(mod,f.month(dt.dod))
  dt=dt.withColumn(yod,f.year(dt.dod))

  #dt=dt.withColumn("dod",f.to_date(f.unix_timestamp(dt.dod,'yyyymmdd').cast('timestamp')))
  #dt=dt.withColumn("REG_DATE",f.to_date(f.unix_timestamp(dt.REG_DATE,'yyyymmdd').cast('timestamp')))
  return(dt)


def joinDeath(df,dod="dod",yod="yod",mod="mod",dead="dead",deadc="deadc",base_date="c_date",dor="dor",yodr="yodr",modr="modr",dodr="dodr"):
  #this function requires a spark dataframe withan id field for join operaton
  dt=loadDeath(dod,yod,mod)
  df=df.drop("rny","dead_","deadc","deadci","deadi","deadr","dor_","dod_","nhs_number_deid","DID","dod","yod","deads","deadp","dor","m_d","m_d30","m_dr","m_dr30","mod","dead","yodr","modr","dodr","m_d30r")
  df=df.join(dt,dt.DID==df.id,how="left")
  

  df=df.withColumn("m_d",f.datediff(col(dod),col(base_date)))
  df=df.withColumn("m_d30",f.when(col("m_d")<=30,1).when(col("m_d")>30,0).otherwise(None))
  
  ##add year of death andmonth of death based on refinedd date
  df=df.withColumn(dodr,when(col("m_d")<0,col(dor)).otherwise(col(dod)))
  df=df.withColumn(modr,f.month(col(dodr)))
  df=df.withColumn(yodr,f.year(col(dodr)))
  
  ##add mortality based on new date
  df=df.withColumn("m_dr",f.datediff(col(dodr),col(base_date)))
  df=df.withColumn("m_dr30",f.when(col("m_d")<=30,1).when(col("m_dr")>30,0).otherwise(None))
  
  ##add deadd and deadr falgs based on dod and dodr
  df=df.withColumn(dead,when(col(dod).isNotNull(),1).otherwise(0)) #inituial incldduding negative numbers
  
  df=df.withColumn("deadci",when(col("m_d")>=0,1).otherwise(0)) #dead covid
  df=df.withColumn(deadc,when(col("m_dr")>=0,1).otherwise(0)) #dead covid
  
  #df=df.filter((col("m_dr")>=0) | (col("m_dr").isNull()) )
  df=df.drop("DID")
  return(df)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Load HES and GDDPR
# MAGIC ####select one candidate record from gdppr and hes

# COMMAND ----------

hes=loadhes()
gdppr=loadgdppr()
print(hes.columns)
print(gdppr.columns)


# COMMAND ----------

# MAGIC %md
# MAGIC #### load code lists including ICD10 and Snowmed concepts

# COMMAND ----------

ckd_codes = load('ckd_codelistx')
ckd_codes=ckd_codes.withColumn("id",f.regexp_replace(f.col("id"), "\\.", ""))
pcode=pd.DataFrame(ckd_codes.collect(),columns=ckd_codes.columns)
icdcode=codes[codes.term.str.contains("ICD")] #ICD-10 codes
snowmed_code=codes[-codes.term.str.contains("ICD")]#Snowmed codes (note '-')

# COMMAND ----------

# MAGIC %md
# MAGIC #### add CKD stages to HES

# COMMAND ----------

### extract ckd patients
#hes = hes.filter(f.col("date") < "2020-12-01")
hes = hes.where(reduce(lambda a, b: a|b, (hes_tmp['code'].like('%'+code+"%") for code in icdcode.id)))

### identify stage of CKD
hes = hes.withColumn("cat",lit(1))#fill with the lowest and then upadate with higher stages as you find
for cati in range(2,6):
  codelist=icdcode[icdcode.stage==cati].id
  codelist=icdcode[icdcode.stage==cati].id
  hes = hes.withColumn("cat", f.when(reduce(lambda a, b: a|b, (hes.code.like('%'+code+"%") for code in codelist)),cati).otherwise(hes.cat))

### identify dialysis 
codelist=icdcode[icdcode.dia==1].id #add dialysis patients
hes = hes.withColumn("dia", f.when(reduce(lambda a, b: a|b, (hes.code.like('%'+code+"%") for code in codelist)),1).otherwise(0))

### select candidate record
hes=hes.groupby("id").agg(f.max(col("date")).alias("date"),f.max(col("cat")).alias("cat"),f.max(col("dia")).alias("dia"))
hes=rep(hes,"hes_ckd")

# COMMAND ----------

# MAGIC %md
# MAGIC #### add CKD stages to gdppr

# COMMAND ----------

#gdppr = gdppr.filter(f.col("date") < "2020-12-01")
gdppr = gdppr.where(f.col("code").cast('string').isin(list(snowmed_code.id)))#select candidate patients using scode (snowmed codes)
gdppr=rep(gdppr)

gdppr = gdppr.withColumn("cat",lit(1))#fill with the lowest and then upadate with higher stages as you find
for cati in range(2,6):
  codelist=list(scode[scode.stage==cati].id)
  gdppr = gdppr.withColumn("cat", f.when(f.col("CODE").cast('string').isin(codelist), cati).otherwise(gdppr.cat))#select candidate patients using scode (snowmed codes)
###add dialysis patients
codelist=list(scode[scode.dia==1].id) #add dialysis patients
gdppr = gdppr.withColumn("dia", f.when(f.col("CODE").cast('string').isin(codelist), 1).otherwise(0))#select candidate patients


gdppr=gdppr.groupby("id").agg(f.max(col("date")).alias("date"),f.max(col("dia")).alias("dia"),f.max(col("cat")).alias("cat"))
gdppr=rep(gdppr,"gdppr_ckd")


# COMMAND ----------

# MAGIC %md
# MAGIC #### Create incident cohort and base cohort

# COMMAND ----------

#gx=load("gdppr_ckdx")
#g.count()#6982209
gx=rep(g,"gdppr_ckd")
hx=rep(h,"hes_ckd")

## base cohort for incident ckd :: combine two cohorts before classifying patients into ckd stages 
hx=hx.select(["id","date"])
gx=gx.select(["id","date"])
xx=hx.union(gx)  
xx=xx.groupby("id").agg(f.min(col("date")).alias("date"))

#add year of incident
xx=xx.withColumn("yoi",f.year(xx.date))
xx=rep(xx)
save(xx,"ckd_incident_base")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Add death data to incident ckd base cohort

# COMMAND ----------

xx=load("ckd_incident_base")
df=loadDeath()
df=xx.join(df,df.DID==xx.id,how="left")
###correct yod
df.withColumn("yod",f.year(col("dor")))
df.withColumn("mod",f.month(col("dor")))
df=df.drop("DID")
print(df.columns)
print(df.count())
xx=rep(df)
save(xx,"ckd_incident_base")

# COMMAND ----------

xx.select(f.countDistinct("id")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### create basee cohort for incident study

# COMMAND ----------

###incident_ckd :: create base cohort for incidence study
sqlcmd="""
select *  from(
select id, date, ROW_NUMBER() over(partition by id order by date ASC) rny from global_temp.xx
"""
#spark.sql(sqlcmd).count()
ckd_base=spark.sql(sqlcmd)
save(ckd_base,"ckd_incident_base")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### create incident cohort

# COMMAND ----------

###incident_ckd :: select the minimum date of the highest stage of CKD a patient ever developed for every patient 
sqlcmd="""
select id, date, cat  from(
select id, date, cat, ROW_NUMBER() over(partition by id order by date ASC) rny from(
select id, date,cat from (select id, date,cat,rank() over(partition by id order by cat DESC) rn from global_temp.xx where date>"1900-01-01" and date  is not null)x where x.rn=1)y)z where z.rny=1
"""
#spark.sql(sqlcmd).count()
ckd=spark.sql(sqlcmd)
save(ckd,"ckd_incident")

# COMMAND ----------

# MAGIC %md
# MAGIC ### add demographic :: sex, dob from source tables

# COMMAND ----------

# MAGIC %md
# MAGIC #### add SEX

# COMMAND ----------

ckd=load("ckd_incident")
ckd=ckd.withColumnRenamed("sex","sex_skt")
ckd.createOrReplaceGlobalTempView("ckd")
h.createOrReplaceGlobalTempView("h")
g.createOrReplaceGlobalTempView("g")

# COMMAND ----------

sqlcmd="""
select i.*, j.sex from global_temp.ckd i left join
(select id,sex from 
(select id,sex, ROW_NUMBER() over(partition by id order by date desc ) as rn from
(select id, sex, date from global_temp.h h 
  union  all 
  select id,sex,date from global_temp.g g
     where id is not null and sex is not null
  ) x
 )xx where xx.rn==1
 )j
 on i.id=j.id 

"""
ckd=spark.sql(sqlcmd)
ckd=rep(ckd)
ckd.columns
save(ckd,"ckd_incident")

# COMMAND ----------

# MAGIC %md
# MAGIC #### add date of birth

# COMMAND ----------

#save(ckd,"ckd_incidentx")
ckd=load("ckd_incident")
g=loadgdppr()
ckd=ckd.withColumnRenamed("dob","dob_skt")

# COMMAND ----------

g1=g.withColumnRenamed("id","id_")
g1=g1.filter(g1.dob.isNotNull()).groupby(col("id_")).agg(f.min(col("dob")).alias("dob"))
ckd=ckd.withColumnRenamed("dob","dob_skt")
ckd=ckd.join(g1.select(g1.id_,g1.dob),ckd.id==g1.id_,how="left")
ckd=ckd.drop("id_")
print(ckd.columns)
save(ckd,"ckd_incident")
save(ckd,"ckd_incident_backup")
#g1=g.groupby("id").agg(f.min(g.dob).alias("dob")).select(col("dob"),col("id"))
#pd.DataFrame(ckd.take(5),columns=ckd.columns)
ckd=rep(ckd)
#ckd.createOrReplaceGlobalTempView("ckd")

# COMMAND ----------

# MAGIC %md
# MAGIC #### add age, age_cat, year of incident, monthe of incident

# COMMAND ----------

print(ckd.columns)

# COMMAND ----------

ckd=load("ckd_incident")
ckd=ckd.drop("age","age_cat","ageInd","age_index")
ckd=addAge(ckd)
print(tab(ckd,"age_index","age_cat"))
ckd=rep(ckd)
save(ckd,"ckd_incident")

# COMMAND ----------

ckd=ckd.drop("yoi","moi")
ckd=ckd.withColumn("yoi",f.year(ckd.date)).withColumn("moi",f.month(ckd.date))
print(tab(ckd,"yoi",order=0).head(5))
ckd=rep(ckd)
save(ckd,"ckd_incident")

# COMMAND ----------

# MAGIC %md ### Add covid and death information

# COMMAND ----------

# MAGIC %md
# MAGIC #### add death info

# COMMAND ----------

p=rep(joinDeath(ckd))
save(p,"ckd_incidentx")
save(p,"ckd_incident")
#print(inspect.getsource(loadDeath))

# COMMAND ----------

# MAGIC %md
# MAGIC #### add covid fields

# COMMAND ----------

#ckd=load("ckd_incident")
p=rep(joincovid(ckd))
save(p,"ckd_incidentx")
#save(p,"ckd_incident")
#save(p,"ckd_incident_backup")

print(p.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ### create covid skinny table with death detailed

# COMMAND ----------

p=joinDeath(loadcovid())
cov=rep(p)
print(cov.columns)
save(cov,"cov_skinny")
save(cov,"covid_skinny")
