# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC **Project:** ***CCU003-CKD***
# MAGIC 
# MAGIC **Description** This notebook loads cohorts and extract numbers required for visualisations/Tables
# MAGIC 
# MAGIC **Requierments** CCU003-CKD-Main, CCU003-CKD-CreateCodeList
# MAGIC 
# MAGIC **Author(s)** Muhammad Dashtban (aka. Ashkan)
# MAGIC 
# MAGIC **Date last updated** 10/22/2021
# MAGIC 
# MAGIC **Date last run** 06/23/2021, 2:37:26 PM

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load CKD and Base Cohorts

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
def load(tableName:str,dbname="dars_nic_391419_j3w9t_collab"):
  ckd_med_table=spark.table(f"""{dbname}.{tableName}""")
  return(ckd_med_table)

# COMMAND ----------

#load("ckd_incident_cohort")
ckd=load("ckd_incident_may")
ckd.createOrReplaceGlobalTempView("ckd")
base=load("ckd_incident_base")
base.createOrReplaceGlobalTempView("base")

# COMMAND ----------

print(ckd.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scripts to calculate numbers for manuscript Tables/Plots

# COMMAND ----------

# MAGIC %md
# MAGIC #### Infection rate

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select  count(*) from global_temp.ckd where dod> "2020-03-01" and dod<"2021-03-01" limit 10;
# MAGIC select  month(dod), count(*) from global_temp.ckd where dod> "2021-01-01" group by month(dod) limit 10;
# MAGIC select  month(dod), count(*) from global_temp.ckd where dod> "2020-01-01" and dod<"2020-04-01" group by month(dod) limit 10;
# MAGIC select  month(dod), count(*) from global_temp.ckd where dod>= "2020-03-01" and dod<"2021-03-01" group by month(dod) order by month(dod);
# MAGIC select  year(covid_date), count(*) from global_temp.ckdm where dod>= "2020-03-01" and dod<"2021-03-01" group by year(covid_date);
# MAGIC select  count(*) from global_temp.ckdm where dod>= "2020-03-01" and dod<"2021-03-01" ;
# MAGIC select  year(covid_date), count(*) from global_temp.ckdm where dod>= "2020-03-01" and dod<"2021-03-01" group by year(covid_date);
# MAGIC select  count(*) from global_temp.ckdm where covid_date>= "2020-03-01" and covid_date<"2021-03-01";
# MAGIC select * from global_temp.ckdm limit 1;
# MAGIC -- result 1628344
# MAGIC 
# MAGIC 
# MAGIC -- qualified ckds stages 
# MAGIC select  deadc,count(*) from global_temp.ckd where  yoi>1996 and yoi<=2020 and (yod>2019 or yod is null) group by deadc;
# MAGIC 
# MAGIC --- infection rate :: age_cat above>65
# MAGIC select (age>65)  , covid, count(*) from global_temp.ckd where dob is not null and yoi>1996 and yoi<=2020 and (yod>2019 or yod is null) group by (age>65),covid order by covid asc;
# MAGIC 
# MAGIC --- infection rate :: sex
# MAGIC select sex , covid, count(*) from global_temp.ckd where sex in (1,2) and yoi>1996 and yoi<=2020 and (yod>2019 or yod is null) group by sex,covid order by covid asc;
# MAGIC 
# MAGIC --- infection rate :: stages of CKD
# MAGIC select covid, count(*) from global_temp.ckd where cat in (1,2,3) and  yoi>1996 and yoi<=2020 and (yod>2019 or yod is null) group by covid  order by covid asc;
# MAGIC select covid, count(*) from global_temp.ckd where cat in (4,5) and  yoi>1996 and yoi<=2020 and (yod>2019 or yod is null) group by covid  order by covid asc;
# MAGIC 
# MAGIC --- fatality rate
# MAGIC select covid, deadc, count(*) from global_temp.ckd where  yoi>1996 and yoi<=2020 and (yod>2019 or yod is null) group by covid, deadc  order by covid,deadc;
# MAGIC 
# MAGIC --- infection rate :: age_cat above>75
# MAGIC select (age>80)  , covid, count(*) from global_temp.ckd where dob is not null and yoi>1996 and yoi<=2020 and (yod>2019 or yod is null) group by (age>80),covid order by covid asc;
# MAGIC 
# MAGIC 
# MAGIC -- overall infection rate
# MAGIC select  covid_march, count(*) from global_temp.ckd where yoi>1996 and yoi<=2020 and (yod>2019 or yod is null) group by sex,covid order by covid asc;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Positive/Negative by Age

# COMMAND ----------

# MAGIC %sql
# MAGIC --- age group at risk pop
# MAGIC --select  count(distinct(id)) from global_temp.ckd where date<"2020-03-01" and date>"1996-03-01" and (dod>"2020-03-01" or dod is null) group by age_cat;
# MAGIC --select  count(distinct(id)) from global_temp.base where dod> "2019-03-01" and dod<="2020-03-01" and date>"1996-03-01";
# MAGIC --select  cat,age_cat,count(*) from global_temp.ckd where date<"2020-03-01" and date>"1996-03-01" and (dod>"2020-03-01" or dod is null) group by cat,age_cat order by cat;
# MAGIC 
# MAGIC --select  cat,count(*) from global_temp.ckd where date<"2020-03-01" and date>"1996-03-01" and (dod>"2020-03-01" and dod is not null) and covid_date is not null group by cat;
# MAGIC --select  cat, count(*), mean(age),std(age) from global_temp.ckd where date<"2021-03-01" and date>="2020-03-01" and (dod>"2020-03-01" or dod is  null) group by cat order by cat;
# MAGIC --select  age_cat, count(*), mean(age),std(age) from global_temp.ckd where date<"2020-03-01" and date>="1996-03-01" and (dod>"2020-03-01" and dod<"2021-0301") and covid_date is  null group by age_cat ;
# MAGIC --select mean(age),std(age) from global_temp.ckd where date<="2020-03-01" and date>="1996-03-01" and (dod>="2020-03-01" and dod<"2021-03-01") and covid_date is null
# MAGIC --select count(*) from global_temp.ckd where date<"2020-03-01" and date>="1996-03-01" and (dod>="2020-03-01" and dod is not null) and age is null and covid_date is not null;
# MAGIC 
# MAGIC --select sex , covid, count(*) from global_temp.ckd where sex in (1,2) and yoi>1996 and yoi<=2020 and (yod>2019 or yod is null) group by age_cat,covid order by covid asc;
# MAGIC select  age_cat,count(*) from global_temp.ckd where date<"2020-03-01" and date>"1996-03-01" and (dod>"2020-03-01" or dod is  null) and covid_date is not null group by age_cat;
# MAGIC --select  count(*) from global_temp.ckd where date<"2020-03-01" and date>"1996-03-01" and (dod>"2020-03-01" or dod is  null) 

# COMMAND ----------

# MAGIC 
# MAGIC %md
# MAGIC #### Fatality rate

# COMMAND ----------

# MAGIC %sql
# MAGIC --fatality per year for base cohort
# MAGIC select  count(distinct(id)) from global_temp.base where date<"2014-03-01" and date>"1996-03-01" and (dod>"2014-03-01" or dod is null);
# MAGIC select  count(distinct(id)) from global_temp.base where dod> "2020-03-01" and dod<="2021-03-01" and date>"1996-03-01";
# MAGIC --select count(*) from global_temp.base  limit 1;
# MAGIC --select  count(*) from global_temp.ckd where date<"2014-03-01" and date>"2015-03-01" and (dod>"2015-03-01" or dod is null);
# MAGIC 
# MAGIC --fatality ckd patients
# MAGIC select  count(*) from global_temp.ckd where dod<"2015-03-01" and dod>"2014-03-01" and (date>"1996-03-01");
# MAGIC select  count(*) from global_temp.ckd where date<"2015-03-01" and date>"1996-03-01" and (dod>"2015-03-01" or dod is null);
# MAGIC 
# MAGIC --incident
# MAGIC select  count(*) from global_temp.ckd where date<"2021-03-01" and date>="2020-03-01" and (dod>="2020-03-01" or dod is null);

# COMMAND ----------

# MAGIC %md
# MAGIC #### Fatality rate by sex

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select sum(case when (yoi<=2019 and (yod>2019 or yod is null)) then 1 else 0 end)as num_at_risk from global_temp.ckd;
# MAGIC 
# MAGIC select sum(case when j.x<=65 then 1 else 0 end) over65,sum(case when j.x<=65 then 0 else 1 end) under65 from 
# MAGIC (select yoi,yod,datediff("2015-01-01",dob)/366 x from global_temp.ckd where dob is not null)j
# MAGIC where j.yoi<=2014 and (j.yod>2014 or j.yod is null);
# MAGIC 
# MAGIC select y.age_cat,count(*) from (select (case when x.age<65 then 1 else 2 end) age_cat from (select datediff("2015-07-01",dob)/366 age from global_temp.ckd where yod=2015 and dob is not null)x)y group by y.age_cat;
# MAGIC 
# MAGIC select sex, yod, count(*) from global_temp.ckd where yod>2014 and sex in (1,2) group by sex,yod order by yod,sex;
# MAGIC 
# MAGIC select sum(case when (yoi<=2014 and (yod>2014 or yod is null)) and sex=1 then 1 else 0 end)as male_at_risk,
# MAGIC sum(case when (yoi<=2014 and (yod>2014 or yod is null)) and sex=2 then 1 else 0 end)as female_at_risk
# MAGIC from global_temp.ckd;
# MAGIC 
# MAGIC select  y.c_month,scovid,sdead, round(sdead/scovid*100,2) fatality from 
# MAGIC (select  c_month,count(*) scovid from global_temp.ckd where covid=1 group by c_month
# MAGIC )y
# MAGIC left join 
# MAGIC (select  c_month,sum(deadc) sdead  from global_temp.ckd where covid=1 group by c_month
# MAGIC )x
# MAGIC on x.c_month=y.c_month
# MAGIC order by c_month;

# COMMAND ----------

# MAGIC %md
# MAGIC ### number at risk

# COMMAND ----------

# MAGIC %sql
# MAGIC -- number at risk 
# MAGIC --- number of patients in base cohort in the date
# MAGIC select count(*) from global_temp.base where yoi>1996 and yoi<2021 and (yod>1996 or yod is null) ;
# MAGIC 
# MAGIC -- number at risk in each year
# MAGIC select 2020 year, sum(case when (yoi>1996 and yoi<=2019 and (yod>2019 or yod is null))  then 1 else 0 end)as num_at_risk from global_temp.base
# MAGIC union all
# MAGIC select 2019 year,sum(case when (yoi>1996 and yoi<=2018 and (yod>2018 or yod is null))  then 1 else 0 end)as num_at_risk from global_temp.base
# MAGIC union all
# MAGIC select 2018 year,sum(case when (yoi>1996 and yoi<=2017 and (yod>2017 or yod is null))  then 1 else 0 end)as num_at_risk from global_temp.base
# MAGIC union all
# MAGIC select 2017 year,sum(case when (yoi>1996 and yoi<=2016 and (yod>2016 or yod is null))  then 1 else 0 end)as num_at_risk from global_temp.base
# MAGIC union all
# MAGIC select 2016 year,sum(case when (yoi>1996 and yoi<=2015 and (yod>2015 or yod is null))then 1 else 0 end)as num_at_risk from global_temp.base
# MAGIC union all
# MAGIC select 2015 year ,sum(case when (yoi>1996 and yoi<=2014 and (yod>2014 or yod is null)) then 1 else 0 end)as num_at_risk from global_temp.base;
# MAGIC 
# MAGIC -- base year 
# MAGIC select 2014 year, sum(case when (yoi>1996 and yoi<=2013 and (yod>2013 or yod is null))  then 1 else 0 end)as num_at_risk from global_temp.ckd;
# MAGIC 
# MAGIC -- population at risk at the onset of the pandemic
# MAGIC select  sum(case when (yoi>1996 and yoi<=2020 and (yod>2019 or yod is null)) then 1 else 0 end)as num_at_risk from global_temp.base;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Incident cases added in 2020

# COMMAND ----------

# MAGIC %sql
# MAGIC --incident cases added in 2020,
# MAGIC select  covid,deadc,count(*) from global_temp.ckd where  yoi==2019 group by covid,deadc;
# MAGIC select  covid,yoi,count(*) from global_temp.ckd where yoi>1996 group by covid,yoi order by yoi desc,covid desc;
# MAGIC select  covid,count(*) from global_temp.ckd where yoi>1996 and yoi==2020 and (yod>2019 or yod is null)  group by covid;

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Covid death per cat

# COMMAND ----------

# MAGIC %sql
# MAGIC select cat,count(*)  from global_temp.ckd where covid=1 and deadc=1 group by cat order by cat
