-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##### Data Pre-Processing

-- COMMAND ----------

--Please enter the clinical trial year
set Year = 2021;

-- COMMAND ----------

-- MAGIC %py
-- MAGIC file_clinic = '/FileStore/tables/clinicaltrial_2021.csv'
-- MAGIC file_pharma = '/FileStore/tables/pharma.csv'
-- MAGIC file_mesh = '/FileStore/tables/mesh.csv'

-- COMMAND ----------

create database if not exists clinical_trials;

-- COMMAND ----------

show databases

-- COMMAND ----------

use clinical_trials

-- COMMAND ----------

-- MAGIC %py
-- MAGIC ### This block create a hive directory and also copied the clinicaltrial csv file into the hive directory. This process was done because using the command "LOAD DATA INPATH" moves the clinicaltrial file into the hive folder created by the table schema command. As a result, other programmes that depend on the clinicaltrial file to execute their processing will not have access to the csv file, if moved into the hive directory.
-- MAGIC dbutils.fs.rm('FileStore/tables/clinicaltrial', True)
-- MAGIC dbutils.fs.mkdirs('FileStore/tables/clinicaltrial')
-- MAGIC dbutils.fs.cp(file_clinic, 'FileStore/tables/clinicaltrial/')

-- COMMAND ----------

drop table if exists clinical_trials.clinical_trial_table;
CREATE EXTERNAL TABLE if not exists clinical_trials.clinical_trial_table(
Id String,
Sponsor String,
Status String,
Start String,
Completion String,
Type String,
Submission String,
Conditions String,
Interventions String
)
COMMENT 'clinicaltrial Table'
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|'
LOCATION '/FileStore/tables/clinicaltrial/'
TBLPROPERTIES("skip.header.line.count" = "1");

-- COMMAND ----------

-- MAGIC %py
-- MAGIC dbutils.fs.ls('FileStore/tables/clinicaltrial')

-- COMMAND ----------

show tables;
describe clinical_trial_table;

-- COMMAND ----------

create view if not exists clinical_trial_view as(
  select * from clinical_trial_table where Id != "Id")

-- COMMAND ----------

select *
from clinical_trial_view

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Task 1: Counting the Number of studies in the dataset

-- COMMAND ----------

select count(*) as No_of_Studies
from clinical_trial_view

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Task 2: List all the types of studies with their frequencies ordered in Descending

-- COMMAND ----------

select Type as Types_of_Studies, count(Type) as Frequencies
from clinical_trial_view
group by Type
order by Frequencies desc;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Task 3: List the top 5 conditions with their frequencies

-- COMMAND ----------

select split(Conditions, ',') as exp_cond 
            from clinical_trial_view

-- COMMAND ----------

select exp_cond as Conditions, count(*) as Frequency
  from(
    select * 
      from(
         select explode(split(Conditions, ',')) as exp_cond 
            from clinical_trial_view
          )where exp_cond != ""
      )
group by exp_cond
order by Frequency desc
limit 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Task 4: Finding the 5 most frequest roots

-- COMMAND ----------

-- MAGIC %py
-- MAGIC dbutils.fs.rm('FileStore/tables/mesh', True)
-- MAGIC dbutils.fs.mkdirs('FileStore/tables/mesh')
-- MAGIC dbutils.fs.cp('FileStore/tables/mesh.csv', 'FileStore/tables/mesh/')

-- COMMAND ----------

--Creating meshtable
drop table if exists clinical_trials.meshtable;
CREATE EXTERNAL TABLE if not exists clinical_trials.meshtable(
term String,
tree String
)
COMMENT 'Mesh Table'
ROW FORMAT DELIMITED 
--FIELDS TERMINATED BY '.'
LOCATION '/FileStore/tables/mesh/'
TBLPROPERTIES("skip.header.line.count" = "1");

-- COMMAND ----------

select * from meshtable

-- COMMAND ----------

--Cleaning up the mesh dataset
select term, substring(tree_node, 1, 3) as tree_node
from(SELECT SUBSTRING(term, 1, CHARINDEX(',', term)-1) AS term,
       SUBSTRING(term, CHARINDEX(',', term) + 1, 1000) AS tree_node
       FROM(
            SELECT REPLACE(term, ', ', ' ')  AS term 
              FROM(
                   SELECT REPLACE(term, '"', '') AS term 
                      FROM(
                           SELECT * FROM meshtable
                              WHERE term !='term,tree'
                           )
                   )
           )
     )

-- COMMAND ----------

--Creating a view from the filtered conditions dataset
drop view if exists conditions_view;
create view if not exists conditions_view as(
    select exp_cond as Conditions, count(*) as Frequency
      from(
         select * from(
                   select explode(split(Conditions, ',')) as exp_cond 
                     from clinical_trial_view
                     where Conditions != ""))
                     group by exp_cond );
--Creating a view from the cleaned mesh dataset
drop view if exists mesh_view;
create view if not exists mesh_view as( 
select term, substring(tree_node, 1, 3) as tree_node
    FROM(SELECT SUBSTRING(term, 1, CHARINDEX(',', term)-1) AS term,
           SUBSTRING(term, CHARINDEX(',', term) + 1, 1000) AS tree_node
               FROM(
                    SELECT REPLACE(term, ', ', ' ')  AS term 
                      FROM(
                           SELECT REPLACE(term, '"', '') AS term 
                              FROM(
                                   SELECT * FROM meshtable
                                      WHERE term !='term,tree')))));

-- COMMAND ----------

--Creating a join from the mesh_view and clinicaltrial conditions to find the frequent roots
select m.tree_node, sum(c.Frequency) as Frequency
from mesh_view m
join conditions_view c
on c.Conditions = m.term
group by m.tree_node
order by Frequency desc
limit 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Task 5: Finding the 10 most common sponsors that are NOT pharmaceutical companies

-- COMMAND ----------

-- MAGIC %py
-- MAGIC dbutils.fs.mkdirs('FileStore/tables/pharma')
-- MAGIC dbutils.fs.cp('FileStore/tables/pharma.csv', 'FileStore/tables/pharma/')

-- COMMAND ----------

drop table if exists clinical_trials.pharma_table;
CREATE EXTERNAL TABLE if not exists clinical_trials.pharma_table(
Company string,
Parent_Company string,
Penalty_Amount string,
Subtraction_From_Penalty string,
Penalty_Amount_Adjusted_For_Eliminating_Multiple_Counting string,
Penalty_Year int,
Penalty_Date int,
Offense_Group string,
Primary_Offense string,
Secondary_Offense string,
Description string,
Level_of_Government string,
Action_Type string,
Agency string,
Civil_Criminal string,
Prosecution_Agreement string,
Court string,
Case_ID string,
Private_Litigation_Case_Title string,
Lawsuit_Resolution string,
Facility_State string,
City string,
Address string,
Zip string,
NAICS_Code int,
NAICS_Translation string,
HQ_Country_of_Parent string,
HQ_State_of_Parent string,
Ownership_Structure string,
Parent_Company_Stock_Ticker string,
Major_Industry_of_Parent string,
Specific_Industry_of_Parent string,
Info_Source string,
Notes string
)
COMMENT 'Pharma Table'
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
LOCATION '/FileStore/tables/pharma/'
TBLPROPERTIES("skip.header.line.count" = "1");

-- COMMAND ----------

select * from pharma_table

-- COMMAND ----------

select replace(Parent_Company, '"', '') as Parent_Company, count(*) as Frequency
from pharma_table
group by Parent_Company

-- COMMAND ----------

select Sponsor, count(*) as Frequency
from clinical_trial_view
group by Sponsor
order by Frequency desc

-- COMMAND ----------

-- Creating a view for the Parent Company table
drop view if exists Company;
create view if not exists Company as(
    select replace(Parent_Company, '"', '') as Parent_Company, count(*) as Frequency
      from pharma_table
      group by Parent_Company
      order by Frequency desc
);
-- Creating a view for the Sponsors table
drop view if exists Sponsors;
create view if not exists Sponsors as(
    select Sponsor, count(*) as Frequency
      from clinical_trial_view
      group by Sponsor
      order by Frequency desc
);
-- Subracting the sponsors that are not phamaceutical companies
select s.Sponsor, s.Frequency
from Sponsors s
full outer join Company c
on s.Sponsor = c.Parent_Company
where s.Sponsor is null or c.Parent_Company is null
order by Frequency desc
limit 10;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Task 6: Visualizing completed studies per month

-- COMMAND ----------

select Months, Num_of_Studies 
FROM(
      select case Months 
                  when 'Jan' then 1 
                  when 'Feb' then 2 
                  when 'Mar' then 3 
                  when 'Apr' then 4 
                  when 'May' then 5 
                  when 'Jun' then 6 
                  when 'Jul' then 7 
                  when 'Aug' then 8 
                  when 'Sep' then 9 
                  when 'Oct' then 10 
                  when 'Nov' then 11 
                  when 'Dec' then 12 
              end as Serial_Num, Months, count(Year) as Num_of_Studies
      FROM(
            select substring(Completion, 1, 3) as Months, substring(Completion, 4, 7) as Year
              FROM clinical_trials.clinical_trial_view
                WHERE Status = 'Completed' and Completion like '%${hiveconf:Year}'
           )
      group by Months
order by Serial_Num asc
);
