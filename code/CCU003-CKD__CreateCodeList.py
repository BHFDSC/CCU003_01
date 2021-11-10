# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC **Project:** ***CCU003-CKD***
# MAGIC 
# MAGIC **Description** This notebook creates validated CKD code list prepared during CaReMe UCL-AZ project
# MAGIC 
# MAGIC **requierments** N/A
# MAGIC 
# MAGIC **Author(s)** Muhammad Dashtban (aka. Ashkan)
# MAGIC 
# MAGIC 
# MAGIC **Date last updated** 10/22/2021
# MAGIC 
# MAGIC 
# MAGIC **Date last run** 3/22/2021, 4:40:19 AM 
# MAGIC 
# MAGIC **Outputs:**
# MAGIC - ckd_codelistx

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Code list 

# COMMAND ----------

cols=["id","stage","dia","dueto","term"]
ckd_codelist=[("700379002",3,0,0,"chronic kidney disease stage 3b"),
              ("324121000000109",1,0,0,"chronic kidney disease stage 1 with proteinuria"),
              ("994401000006102",1,0,0,"chronic kidney disease stage 1"),
              ("431855005",1,0,0,"chronic kidney disease stage 1"),
              ("324181000000105",2,0,0,"chronic kidney disease stage 2 with proteinuria"),
              ("994411000006104",2,0,0,"chronic kidney disease stage 2"),
              ("431856006",2,0,0,"chronic kidney disease stage 2"),
              ("324251000000105",3,0,0,"chronic kidney disease stage 3 with proteinuria"),
              ("324311000000101",3,0,0,"chronic kidney disease stage 3a with proteinuria"),
              ("324541000000105",5,0,0,"chronic kidney disease stage 5 without proteinuria"),
              ("994441000006100",5,0,0,"chronic kidney disease stage 5"),
              ("950291000000103",5,0,0,"chronic kidney disease with glomerular filtration rate category g5 and albuminuria category a2"),
              ("950211000000107",4,0,0,"chronic kidney disease with glomerular filtration rate category g4 and albuminuria category a2"),
              ("324441000000106",4,0,0,"ckd (chronic kidney disease) stage 4 with proteinuria"),
              ("949881000000106",3,0,0,"chronic kidney disease with glomerular filtration rate category g3a and albuminuria category a1"),
              ("324341000000100",3,0,0,"ckd (chronic kidney disease) stage 3a without proteinuria"),
              ("714153000",5,0,0,"chronic kidney disease 5t"),
              ("714152005",5,0,0,"ckd (chronic kidney disease) stage 5d"),
              ("433146000",5,0,0,"chronic kidney disease stage 5"),
              ("950181000000106",4,0,0,"ckd g4a1 - chronic kidney disease with glomerular filtration rate category g4 and albuminuria category a1"),
              ("949521000000108",2,0,0,"chronic kidney disease with glomerular filtration rate category g2 and albuminuria category a1"),
              ("949621000000109",2,0,0,"ckd g2a3 - chronic kidney disease with glomerular filtration rate category g2 and albuminuria category a3"),
              ("949901000000109",3,0,0,"ckd g3aa2 - chronic kidney disease with glomerular filtration rate category g3a and albuminuria category a2"),
              ("949921000000100",3,0,0,"ckd g3aa3 - chronic kidney disease with glomerular filtration rate category g3a and albuminuria category a3"),
              ("950061000000103",3,0,0,"ckd g3ba1 - chronic kidney disease with glomerular filtration rate category g3b and albuminuria category a1"),
              ("950101000000101",3,0,0,"ckd g3ba3 - chronic kidney disease with glomerular filtration rate category g3b and albuminuria category a3"),
              ("950231000000104",4,0,0,"ckd g4a3 - chronic kidney disease with glomerular filtration rate category g4 and albuminuria category a3"),
              ("949421000000107",1,0,0,"ckd g1a2 - chronic kidney disease with glomerular filtration rate category g1 and albuminuria category a2"),
              ("431857002",4,0,0,"chronic kidney disease stage 4"),
              ("433144002",3,0,0,"chronic kidney disease stage 3"),
              ("324411000000105",3,0,0,"chronic kidney disease stage 3b without proteinuria"),
              ("324471000000100",4,0,0,"chronic kidney disease stage 4 without proteinuria"),
              ("324281000000104",3,0,0,"ckd (chronic kidney disease) stage 3 without proteinuria"),
              ("949481000000108",1,0,0,"chronic kidney disease with glomerular filtration rate category g1 and albuminuria category a3"),
              ("324371000000106",3,0,0,"chronic kidney disease stage 3b with proteinuria"),
              ("950251000000106",5,0,0,"ckd g5a1 - chronic kidney disease with glomerular filtration rate category g5 and albuminuria category a1"),
              ("950081000000107",3,0,0,"ckd g3ba2 - chronic kidney disease with glomerular filtration rate category g3b and albuminuria category a2"),
              ("949561000000100",2,0,0,"chronic kidney disease with glomerular filtration rate category g2 and albuminuria category a2"),
              ("994421000006107",3,0,0,"chronic kidney disease stage 3"),
              ("324501000000107",5,0,0,"ckd (chronic kidney disease) stage 5 with proteinuria"),
              ("950311000000102",5,0,0,"ckd g5a3 - chronic kidney disease with glomerular filtration rate category g5 and albuminuria category a3"),
              ("994431000006105",4,0,0,"chronic kidney disease stage 4"),
              ("700378005",3,0,0,"chronic kidney disease stage 3a"),
              ("216933008",5,1,0,"Failure of sterile precautions during kidney dialysis"),
              ("238318009",5,1,0,"Continuous ambulatory peritoneal dialysis"),
              ("423062001",5,1,0,"Stenosis of arteriovenous dialysis fistula"),
              ("438546008",5,1,0,"Ligation of arteriovenous dialysis fistula"),
              ("180272001",5,1,0,"Placement ambulatory dialysis apparatus - compens renal fail"),
              ("385971003",5,1,0,"[V]Preparatory care for dialysis"),
              ("426361000000104",5,1,0,"[V]Unspecified aftercare involving intermittent dialysis"),
              ("302497006",5,1,0,"Haemodialysis"),
              ("251859005",5,1,0,"Dialysis finding"),
              ("108241001",5,1,0,"Dialysis procedure"),
              ("398471000000102",5,1,0,"[V]Aftercare involving intermittent dialysis"),
              ("180273006",5,1,0,"Removal of ambulatory peritoneal dialysis catheter"),
              ("426340003",5,1,0,"Creation of graft fistula for dialysis"),
              ("410511000000103",5,1,0,"[V]Aftercare involving renal dialysis NOS"),
              ("426351000000102",5,1,0,"[V]Aftercare involving peritoneal dialysis"),
              ("71192002",5,1,0,"Peritoneal dialysis NEC"),
              ("116223007",5,1,0,"Kidney dialysis with complication, without blame"),
              ("428648006",5,1,0,"Automated peritoneal dialysis"),
              ("991521000000102",5,1,0,"Dialysis fluid glucose level"),
              ("161693006",5,1,0,"H/O: renal dialysis"),
              ("17778006",5,1,0,"Mechanical complication of dialysis catheter"),
              ("216878005",5,1,0,"Accidental cut, puncture, perforation or haemorrhage during kidney dialysis"),
              ("276883000",5,1,0,"Peritoneal dialysis-associated peritonitis"),
              ("265764009",5,1,0,"Renal dialysis"),
              ("180277007",5,1,0,"Insertion of temporary peritoneal dialysis catheter"),
              ("79827002",5,1,0,"cadaveric renal transplant"),
              ("269698004",5,1,0,"failure of sterile precautions during kidney dialysis"),
              ("269691005",5,1,0,"very mild acute rejection of renal transplant"),
              ("216904007",5,1,0,"acute rejection of renal transplant - grade i"),
              ("70536003",5,1,0,"Renal transplant stage 5"),
              ("427053002",5,1,0,"Extracorporeal albumin haemodialysis stage 5"),
              ("225283000",5,1,0,"Priming haemodialysis lines stage 5"),
              ("420106004",5,1,0,"Renal transplant venogram stage 5"),
              ("708932005",5,1,0,"Emergency haemodialysis stage 5"),
              ("5571000001109",5,1,0,"solution haemodialysis stage 5"),
              ("366961000000102",5,1,0,"Renal transplant recipient stage 5"),
              ("57274006",5,1,0,"Initial haemodialysis stage 5"),
              ("233575001",5,1,0,"Intermittent haemodialysis stage 5"),
              ("85223007",5,1,0,"Complication of haemodialysis stage 5"),
              ("N18.1",1,0,0,"ICD-10 chronic kidney disease stage 1"),
              ("N18.2",2,0,0,"ICD-10 chronic kidney disease stage 2"),
              ("N18.3",3,0,0,"ICD-10 chronic kidney disease stage 3"),
              ("N18.4",4,0,0,"ICD-10 chronic kidney disease stage 4"),
              ("N18.5",5,0,0,"ICD-10 chronic kidney disease stage 5"),
              ("T82.4",5,1,0,"ICD-10 mechanical complication of vascular dialysis catheter"),
              ("T86.1",5,1,0,"ICD-10 kidney transplant failure and rejection"),
              ("Y60.2",5,1,0,"ICD-10 during kidney dialysis or other perfusion"),
              ("Y61.2",5,1,0,"ICD-10 during kidney dialysis or other perfusion"),
              ("Y62.2",5,1,0,"ICD-10 during kidney dialysis or other perfusion"),
              ("Y84.1",5,1,0,"ICD-10 kidney dialysis"),
              ("Z49",5,1,0,"ICD-10 care involving dialysis"),
              ("Z49.0",5,1,0,"ICD-10 preparatory care for dialysis"),
              ("Z49.1",5,1,0,"ICD-10 extracorporeal dialysis"),
              ("Z49.2",5,1,0,"ICD-10 other dialysis"),
              ("Z94.0",5,1,0,"ICD-10 kidney transplant status"),
              ("Z99.2",5,1,0,"ICD-10 dependence on renal dialysis//on dialysis treatment"),
              ("N18.6",5,1,0,"ICD-10 patients with CKD requiring dialysis"),
              ("I77.0",5,1,0,"ICD-10 arteriovenous fistula"),
              ("N16.5",5,1,0,"ICD-10 renal tubulo-interstitial disorders in transplant rejection"),
              ("284991000119104",3,0,1,"Chronic kidney disease stage 3 due to benign hypertension (disorder)"),
              ("368441000119102",3,0,1,"Chronic kidney disease stage 3 due to drug induced diabetes mellitus (disorder)"),
              ("129171000119106",3,0,1,"Chronic kidney disease stage 3 due to hypertension (disorder)"),
              ("90741000119107",3,0,1,"Chronic kidney disease stage 3 due to type 1 diabetes mellitus (disorder)"),
              ("731000119105",3,0,1,"Chronic kidney disease stage 3 due to type 2 diabetes mellitus (disorder)"),
              ("285001000119105",4,0,1,"Chronic kidney disease stage 4 due to benign hypertension (disorder)"),
              ("368451000119100",4,0,1,"Chronic kidney disease stage 4 due to drug induced diabetes mellitus (disorder)"),
              ("90751000119109",4,0,1,"Chronic kidney disease stage 4 due to type 1 diabetes mellitus (disorder)"),
              ("721000119107",4,0,1,"Chronic kidney disease stage 4 due to type 2 diabetes mellitus (disorder)"),
              ("285011000119108",5,0,1,"Chronic kidney disease stage 5 due to benign hypertension (disorder)"),
              ("368461000119103",5,0,1,"Chronic kidney disease stage 5 due to drug induced diabetes mellitus (disorder)"),
              ("129161000119100",5,0,1,"Chronic kidney disease stage 5 due to hypertension (disorder)"),
              ("90761000119106",5,0,1,"Chronic kidney disease stage 5 due to type 1 diabetes mellitus (disorder)"),
              ("711000119100",5,0,1,"Chronic kidney disease stage 5 due to type 2 diabetes mellitus (disorder)"),
              ("96701000119107",5,1,1,"Hypertensive heart AND Chronic kidney disease on dialysis (disorder)"),
              ("96751000119106",1,0,1,"Hypertensive heart AND Chronic kidney disease stage 1 (disorder)"),
              ("96741000119109",2,0,1,"Hypertensive heart AND Chronic kidney disease stage 2 (disorder)"),
              ("96731000119100",3,0,1,"Hypertensive heart AND Chronic kidney disease stage 3 (disorder)"),
              ("96721000119103",4,0,1,"Hypertensive heart AND Chronic kidney disease stage 4 (disorder)"),
              ("96711000119105",5,0,1,"Hypertensive heart AND Chronic kidney disease stage 5 (disorder)"),
              ("285851000119102",1,0,1,"Malignant Hypertensive Chronic kidney disease stage 1 (disorder)"),
              ("285861000119100",2,0,1,"Malignant Hypertensive Chronic kidney disease stage 2 (disorder)"),
              ("285871000119106",3,0,1,"Malignant Hypertensive Chronic kidney disease stage 3 (disorder)"),
              ("285881000119109",4,0,1,"Malignant Hypertensive Chronic kidney disease stage 4 (disorder)"),
              ("153851000119106",5,0,1,"Malignant Hypertensive Chronic kidney disease stage 5 (disorder)"),
              ("285841000119104",5,1,1,"Malignant Hypertensive end stage renal disease (disorder)"),
              ("286371000119107",5,1,1,"Malignant Hypertensive end stage renal disease on dialysis (disorder)")]

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Show some rows - Save Code list

# COMMAND ----------

ckd_codelist=pd.DataFrame(ckd_codelist,columns=cols)
ckd_codelist.head()

# COMMAND ----------

ckd_codelistx=spark.createDataFrame(ckd_codelist,schema=cols)
save(ckd_codelistx)

# COMMAND ----------

ckd_codelistx.take(5)
