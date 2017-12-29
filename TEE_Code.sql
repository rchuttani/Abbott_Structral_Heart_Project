/////////////////////Transesophageal Echocardiogram///////////////////////////////////////


/////////////////////////////////////RAVEN CODE FOR TEE////////////////////////////////////////////////
CREATE TABLE project_analytics.presales.AC_PROC_RAVEN_TEE as 
SELECT   encrypted_key_1 as patient_suffix,
        claim_number,
        COALESCE(service_from,STATEMENT_FROM) as Service_Date,
        PROCEDURE
          
FROM    rwd_db.sandbox.claims_submits_procedure
where
UPPER(PROCEDURE) in ('93312',
'93313',
'93314',
'93315',
'93316',
'93317',
'93318',
'B244ZZ4', 
'B245ZZ4',
'B246ZZ4',
'B24DZZ4', 
'88.72',
'8872')

 AND   encrypted_key_1 IS NOT NULL 
AND    Upper(encrypted_key_1) <> 'NULL' 
AND    Upper(encrypted_key_1) NOT LIKE 'XXX -%' 
AND Year(Service_Date) in ('2014','2015','2016')

////TOTAL COUNTS FROM THE ABOVE TABLE////////////////////
Select * from project_analytics.presales.AC_PROC_RAVEN_TEE        ////////////1,318,141

----------------Joining with provider table to get NPI AND PROVIDER TYPE------------------
drop table if exists project_analytics.presales.AC_PROC_RAVEN_TEE_Prov;
create table project_analytics.presales.AC_PROC_RAVEN_TEE_Prov as
Select a.*, 
       b.PROVIDER_NPI,
       b.PROVIDER_TYPE

from project_analytics.presales.AC_PROC_RAVEN_TEE a
left join rwd_db.sandbox.claims_submits_provider b on a.claim_number=b.claim_number


//////////////////////////////////Adding case to add Provier NPI/////////////////
drop table if exists project_analytics.presales.AC_PROC_RAVEN_TEE_Prov_Mapping;
create table project_analytics.presales.AC_PROC_RAVEN_TEE_Prov_Mapping as

select a.*,

case when e.provider_NPI is null then 
case when f.provider_NPI is null then
g.provider_NPI else f.provider_NPI end 
else e.provider_NPI end as Facility_NPI


from  project_analytics.presales.AC_PROC_RAVEN_TEE_Prov as a 
left join (select distinct claim_number,Provider_NPI from project_analytics.presales.AC_PROC_RAVEN_TEE_Prov  where lower(provider_type) = 'facility') as e
on a.claim_number = e.claim_number
left join (select distinct claim_number,Provider_NPI from project_analytics.presales.AC_PROC_RAVEN_TEE_Prov  where lower(provider_type) = 'billing') as f
on a.claim_number = f.claim_number
left join (select distinct claim_number,Provider_NPI from project_analytics.presales.AC_PROC_RAVEN_TEE_Prov  where lower(provider_type) = 'servicelocation') as g
on a.claim_number = g.claim_number 

/////QC/////////////

Select * from project_analytics.presales.AC_PROC_RAVEN_TEE_Prov_Mapping   ////4,338,502
-------------------Joining with MADDYs TABLE to get NPI----------------

create table project_analytics.presales.AC_Raven_HealthBase_Facility_Transeal_Eco as

select a.*, 
b.organization_npi as NPI,

organization_label
from project_analytics.presales.AC_PROC_RAVEN_TEE_Prov_Mapping a
left join rwd_db.sandbox.claims_submits_facility_master_new b on a.claim_number=b.claim_number


////QC////
Select * from project_analytics.presales.AC_Raven_HealthBase_Facility_Transeal_Eco  ////4,338,502
limit 100


--------------NEW TABLE TO GET HEALTHBASE ID-----------------
drop table if exists project_analytics.presales.AC_Raven_HealthBase_Facility_Transeal_Eco_2;
create table project_analytics.presales.AC_Raven_HealthBase_Facility_Transeal_Eco_2 as
Select a.*,b.Healthbase_ID,
b.Label

from project_analytics.presales.AC_Raven_HealthBase_Facility_Transeal_Eco a
left join rwd_db.RWD.HEALTHBASE_ENTITYLOOKUP b on a.NPI=b.Identifier
where lower(a.Provider_Type) in ('servicelocation','billing','facility')  and
lower(b.Identifier_type)='n' and lower(b.Label) in ('hospital','ruralhealthclinic','homehealth',
'nursinghome','outpatientphysicaltherapy','portablexray','federallyqualifiedhealthcenter','asc','dialysisfacility','outpatientrehabfacility',
'hospice','mentalhealthcenter')


///////QC////////////////
Select * from project_analytics.presales.AC_Raven_HealthBase_Facility_Transeal_Eco_2   //////1,862,168

--------------joining with hospital--------------
dROP TABLE IF EXISTS project_analytics.presales.AC_Raven_Diag_Transeal_Echo_Hosp;
create table project_analytics.presales.AC_Raven_Diag_Transeal_Echo_Hosp as
select a.*,
b.Pos_Other_Fac_Name as Name,
B.POS_OTHER_ST_ADR AS ADDRESS,
b.POS_OTHER_CITY_NAME as City,
b.POS_OTHER_STATE_CD as State,
b.POS_OTHER_ZIP_CD as ZIP


from project_analytics.presales.AC_Raven_HealthBase_Facility_Transeal_Eco_2  a

inner join rwd_db.RWD.HEALTHBASE_HOSPITAL b on a.Healthbase_ID=b.Healthbase_ID 

-------------qc-----------

select * from project_analytics.presales.AC_Raven_Diag_Transeal_Echo_Hosp   /////
limit 10

--------------joining with Rural health clinic--------------
DROP TABLE IF EXISTS project_analytics.presales.AC_Raven_Diag_Transeal_Echo_RHC;
create table project_analytics.presales.AC_Raven_Diag_Transeal_Echo_RHC as
select a.*,
b.Pos_Other_Fac_Name as Name,
b.POS_OTHER_ST_ADR as Address,
b.POS_OTHER_CITY_NAME as City,
b.POS_OTHER_STATE_CD as State,
b.POS_OTHER_ZIP_CD as ZIP

from project_analytics.presales.AC_Raven_HealthBase_Facility_Transeal_Eco_2 a

inner join rwd_db.RWD.HEALTHBASE_RURALHEALTHCLINIC b on a.Healthbase_ID=b.Healthbase_ID

----------qc--------------

select * from project_analytics.presales.AC_Raven_Diag_Transeal_Echo_RHC
limit 10

--------------joining with Home Health--------------
DROP TABLE IF EXISTS project_analytics.presales.AC_Raven_Diag_Transeal_Echo_HH;
create table project_analytics.presales.AC_Raven_Diag_Transeal_Echo_HH as
select a.*,
b.Pos_Other_Fac_Name as Name,
b.POS_OTHER_ST_ADR as Address,
b.POS_OTHER_CITY_NAME as City,
b.POS_OTHER_STATE_CD as State
,b.POS_OTHER_ZIP_CD as ZIP

from project_analytics.presales.AC_Raven_HealthBase_Facility_Transeal_Eco_2 a

inner join rwd_db.RWD.HEALTHBASE_HOMEHEALTH b on a.Healthbase_ID=b.Healthbase_ID

----qc----

Select * from project_analytics.presales.AC_Raven_Diag_Transeal_Echo_HH
limit 10

---qc----------------------
select * from RWD.HEALTHBASE_NURSINGHOME
limit 10

--------------joining with Nursing Home--------------
DROP TABLE IF EXISTS project_analytics.presales.AC_Raven_Diag_Transeal_Echo_Nursing;
create table project_analytics.presales.AC_Raven_Diag_Transeal_Echo_Nursing as
select a.*,
b.Pos_Other_Fac_Name as Name,
b.POS_OTHER_ST_ADR as Address,
b.POS_OTHER_CITY_NAME as City,
b.POS_OTHER_STATE_CD as State
,b.POS_OTHER_ZIP_CD as ZIP
from project_analytics.presales.AC_Raven_HealthBase_Facility_Transeal_Eco_2 a

inner join rwd_db.RWD.HEALTHBASE_NURSINGHOME b on a.Healthbase_ID=b.Healthbase_ID

-------qc------------
select * from project_analytics.presales.AC_Raven_Diag_Transeal_Echo_Nursing
limit 10

--------------joining with Outpatient Phy therap--------------
DROP TABLE IF EXISTS project_analytics.presales.AC_Raven_Diag_Transeal_Echo_OPT;
create table project_analytics.presales.AC_Raven_Diag_Transeal_Echo_OPT as
select a.*,
b.Pos_Other_Fac_Name as Name,
b.POS_OTHER_ST_ADR as Address,
b.POS_OTHER_CITY_NAME as City,
b.POS_OTHER_STATE_CD as State
,b.POS_OTHER_ZIP_CD as ZIP
from project_analytics.presales.AC_Raven_HealthBase_Facility_Transeal_Eco_2 a

inner join rwd_db.RWD.HEALTHBASE_OUTPATIENTPHYSICALTHERAPY b on a.Healthbase_ID=b.Healthbase_ID

-------------qc-----------

select * from project_analytics.presales.AC_Raven_Diag_Transeal_Echo_OPT
limit 10

--------------joining with Portable X ray--------------
DROP TABLE IF EXISTS project_analytics.presales.AC_Raven_Diag_Transeal_Echo_PX;
create table project_analytics.presales.AC_Raven_Diag_Transeal_Echo_PX as
select a.*,
b.Pos_Other_Fac_Name as Name,
b.POS_OTHER_ST_ADR as Address,
b.POS_OTHER_CITY_NAME as City,
b.POS_OTHER_STATE_CD as State
,b.POS_OTHER_ZIP_CD as ZIP
from project_analytics.presales.AC_Raven_HealthBase_Facility_Transeal_Eco_2 a

inner join rwd_db.RWD.HEALTHBASE_PORTABLEXRAY b on a.Healthbase_ID=b.Healthbase_ID

---------qc---------------
select * from  project_analytics.presales.AC_Raven_Diag_Transeal_Echo_PX
limit 10

--------------joining with FQHC--------------
DROP TABLE IF EXISTS project_analytics.presales.AC_Raven_Diag_Transeal_Echo_FQHC;
create table project_analytics.presales.AC_Raven_Diag_Transeal_Echo_FQHC as
select a.*,
b.Pos_Other_Fac_Name as Name,
b.POS_OTHER_ST_ADR as Address,
b.POS_OTHER_CITY_NAME as City,
b.POS_OTHER_STATE_CD as State
,b.POS_OTHER_ZIP_CD as ZIP
from project_analytics.presales.AC_Raven_HealthBase_Facility_Transeal_Eco_2 a

inner join rwd_db.RWD.HEALTHBASE_FEDERALLYQUALIFIEDHEALTHCENTER b on a.Healthbase_ID=b.Healthbase_ID

------------qc-------------------
SELECT * FROM project_analytics.presales.AC_Raven_Diag_Transeal_Echo_FQHC
limit 5

--------------joining with ASC--------------
DROP TABLE IF EXISTS project_analytics.presales.AC_Raven_Diag_Transeal_Echo_ASC;
create table project_analytics.presales.AC_Raven_Diag_Transeal_Echo_ASC as
select a.*,
b.Pos_Other_Fac_Name as Name,
b.POS_OTHER_ST_ADR as Address,
b.POS_OTHER_CITY_NAME as City,
b.POS_OTHER_STATE_CD as State
,b.POS_OTHER_ZIP_CD as ZIP
from project_analytics.presales.AC_Raven_HealthBase_Facility_Transeal_Eco_2 a

inner join rwd_db.RWD.HEALTHBASE_ASC b on a.Healthbase_ID=b.Healthbase_ID

-------------qc------------------

select * from project_analytics.presales.AC_Raven_Diag_Transeal_Echo_ASC
limit 10

--------------joining with Dialysis--------------
DROP TABLE IF EXISTS project_analytics.presales.AC_Raven_Diag_Transeal_Echo_DF;
create table project_analytics.presales.AC_Raven_Diag_Transeal_Echo_DF as
select a.*,
b.Pos_Other_Fac_Name as Name,
b.POS_OTHER_ST_ADR as Address,
b.POS_OTHER_CITY_NAME as City,
b.POS_OTHER_STATE_CD as State
,b.POS_OTHER_ZIP_CD as ZIP
from project_analytics.presales.AC_Raven_HealthBase_Facility_Transeal_Eco_2 a

inner join rwd_db.RWD.HEALTHBASE_DIALYSISFACILITY b on a.Healthbase_ID=b.Healthbase_ID

-------------qc-------------

select * from project_analytics.presales.AC_Raven_Diag_Transeal_Echo_DF

limit 10

--------------joining with OPRF--------------
DROP TABLE IF EXISTS project_analytics.presales.AC_Raven_Diag_Transeal_Echo_OPRF;
create table project_analytics.presales.AC_Raven_Diag_Transeal_Echo_OPRF as
select a.*,
b.Pos_Other_Fac_Name as Name,
b.POS_OTHER_ST_ADR as Address,
b.POS_OTHER_CITY_NAME as City,
b.POS_OTHER_STATE_CD as State
,b.POS_OTHER_ZIP_CD as ZIP
from project_analytics.presales.AC_Raven_HealthBase_Facility_Transeal_Eco_2 a

inner join rwd_db.RWD.HEALTHBASE_OUTPATIENTREHABFACILITY b on a.Healthbase_ID=b.Healthbase_ID

----------qc---------------

select * from project_analytics.presales.AC_Raven_Diag_Transeal_Echo_OPRF
limit 10

--------------joining with hospice--------------
DROP TABLE IF EXISTS project_analytics.presales.AC_Raven_Diag_Transeal_Echo_HC;
create table project_analytics.presales.AC_Raven_Diag_Transeal_Echo_HC as
select a.*,
b.Pos_Other_Fac_Name as Name,
b.POS_OTHER_ST_ADR as Address,
b.POS_OTHER_CITY_NAME as City,
b.POS_OTHER_STATE_CD as State
,b.POS_OTHER_ZIP_CD as ZIP
from project_analytics.presales.AC_Raven_HealthBase_Facility_Transeal_Eco_2 a

inner join rwd_db.RWD.HEALTHBASE_HOSPICE b on a.Healthbase_ID=b.Healthbase_ID

------------------qc------------------

select * from project_analytics.presales.AC_Raven_Diag_Transeal_Echo_HC
limit 10

--------------joining with mentalhealthcenter--------------
DROP TABLE IF EXISTS project_analytics.presales.AC_Raven_Diag_Transeal_Echo_MHC;
create table project_analytics.presales.AC_Raven_Diag_Transeal_Echo_MHC as
select a.*,
b.Pos_Other_Fac_Name as Name,
b.POS_OTHER_ST_ADR as Address,
b.POS_OTHER_CITY_NAME as City,
b.POS_OTHER_STATE_CD as State
,b.POS_OTHER_ZIP_CD as ZIP
from project_analytics.presales.AC_Raven_HealthBase_Facility_Transeal_Eco_2 a

inner join rwd_db.RWD.HEALTHBASE_MENTALHEALTHCENTER b on a.Healthbase_ID=b.Healthbase_ID

////QC////
Select * from project_analytics.presales.AC_Raven_Diag_Transeal_Echo_MHC

Select distinct label from project_analytics.presales.AC_Raven_HealthBase_Facility_Transeal_Eco_2


----------------------------Union for all the tables-----------------
drop table if exists project_analytics.presales.AC_Healthbase_Transeal_Echo_Final;
create table project_analytics.presales.AC_Healthbase_Transeal_Echo_Final as
select * from project_analytics.presales.AC_Raven_Diag_Transeal_Echo_Hosp
UNION
select * from project_analytics.presales.AC_Raven_Diag_Transeal_Echo_RHC
UNION
select * from project_analytics.presales.AC_Raven_Diag_Transeal_Echo_HH

UNION
select * from project_analytics.presales.AC_Raven_Diag_Transeal_Echo_Nursing
UNION
select * from project_analytics.presales.AC_Raven_Diag_Transeal_Echo_OPT
UNION
select * from project_analytics.presales.AC_Raven_Diag_Transeal_Echo_PX
UNION
select * from project_analytics.presales.AC_Raven_Diag_Transeal_Echo_FQHC
 
UNION
select * from project_analytics.presales.AC_Raven_Diag_Transeal_Echo_ASC
UNION
select * from project_analytics.presales.AC_Raven_Diag_Transeal_Echo_DF
UNION
select * from project_analytics.presales.AC_Raven_Diag_Transeal_Echo_OPRF
UNION
select * from project_analytics.presales.AC_Raven_Diag_Transeal_Echo_HC
UNION
select * from project_analytics.presales.AC_Raven_Diag_Transeal_Echo_MHC

/////counts/////

Select * from project_analytics.presales.AC_Healthbase_Transeal_Echo_Final


////counts////
Select Year(service_date) as Year, NPI, Name, Address, City, State, ZIP, count(distinct(claim_number)) from project_analytics.presales.AC_Healthbase_Transeal_Echo_Final
 group by Year,2,3,4,5,6,7
  order by Year,2,3,4,5,6,7  
  
  //////////////////////REMOVING DUPLICATES OF THE SAME YEAR///////////////////


create table project_analytics.presales.AC_Healthbase_Transeal_Echo_Final_1 as

  with A_cte as
  (
    Select *, Year(service_date) as Year,
    row_number() over(partition by Year(service_date), Name, Address, City, State, ZIP  order by Name) as R
    from project_analytics.presales.AC_Healthbase_Transeal_Echo_Final    
  )
  , B_cte as
  (
    Select Year(service_date) as Year, Name, Address, City, State, ZIP
     , count(distinct claim_Number) as CLaimCount
    from project_analytics.presales.AC_Healthbase_Transeal_Echo_Final
    GROUP BY Year(service_date) , Name, Address, City, State, ZIP
  )
  SELECT distinct a.*, b.CLaimCount
  FROM A_cte a
  JOIN B_cte b ON A.Year = b.Year and  A.Name = b.Name and A.Address = b.Address
            and  A.City = b.City and  A.State = b.State and  A.ZIP = b.ZIP
  WHERE r = 1 
  
 /////counts////
 select Year, HealthBase_ID, Name, Address, City, State, ZIP, CLaimCount from project_analytics.presales.AC_Healthbase_Transeal_Echo_Final_1
 
 order by Year,HealthBase_ID, Name, Address, City, State, ZIP