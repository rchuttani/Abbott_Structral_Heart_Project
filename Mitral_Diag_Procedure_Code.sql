/////////////////////MITRAL VALVE DISEASE///////////////////////////////

///////////FETCHING PHARMACY DETAILS FROM MITRAL VALVE DISEASE////////////////
drop table if exists project_analytics.presales.AC_Claims_RXA_Mitral_Daig;
CREATE TABLE project_analytics.presales.AC_Claims_RXA_Mitral_Daig AS
SELECT
    'Pharmacy' as Data_Source,
    encrypted_key_1 AS patient_suffix,
    CONDOR_TDR_CLAIMID AS Claim_Number
    , to_date(date_of_service) as SERVICE_FROM
    ,diagnosis_code,
	 prescriber_id as Provider_NPI,
	 SERVICE_PROVIDER_ID as facility_npi 
    
    FROM rwd_db.rwd.condor_pharmacy_record

WHERE TRIM(UPPER(RESPONSE_CODE)) = 'P'
  AND trim(upper(diagnosis_code)) in ('424.0',
'394.0', 
'394.1', 
'394.2',
'394.9', 
'394.0', 
'394.1', 
'396.3', 
'396.0',
'396.1',
'396.2',
'396.3',
'396.8',
'396.9', 
'4240' ,
'3940' ,
'3941' ,
'3942' ,
'3949' ,
'3940' ,
'3941' ,
'3963' ,
'3960' ,
'3961' ,
'3962' ,
'3963' ,
'3968' ,
'3969',
'I34.8',
'I05.0',
'I05.1',
'I05.1',
'I05.2',
'I05.8',
'I05.0',
'I05.9', 
'I34.0',
'I34.1',
'I34.2',
'I34.8',
'I34.9',
'I08.0',
'I08.1',
'I08.8',
'I08.3',
'I08.9',
'I348' ,
'I050' ,
'I051' ,
'I051' ,
'I052' ,
'I058' ,
'I050' ,
'I059' ,
'I340' ,
'I341' ,
'I342' ,
'I348' ,
'I349' ,
'I080' ,
'I081' ,
'I088' ,
'I083' ,
'I089')
  and
  patient_suffix is not null
        and trim(patient_suffix) <> ''
        and upper(patient_suffix) not like 'XXX -%'
        and upper(patient_suffix) <> 'NULL'
AND Year(SERVICE_FROM) in ('2014','2015','2016')

/////////////////////TOTAL RECORDS FROM THE ABOVE TABLE/////////////////////
Select * from project_analytics.presales.AC_Claims_RXA_Mitral_Daig      //////////////7546

////////////////////FETCHIN PROCEDURE DETAILS FROM RAVEN TABLE///////////////////

drop table if exists project_analytics.presales.AC_Diag_Raven_Mitral ;
CREATE TABLE project_analytics.presales.AC_Diag_Raven_Mitral as 
SELECT   
'Raven' as Data_Source,
encrypted_key_1 as patient_suffix,
        claim_number,
        Coalesce(MIN_SERVICE_FROM,STATEMENT_FROM) as Service_Date,
        Diagnosis
          
FROM   rwd_db.sandbox.claims_submits_Diagnosis
where
UPPER(Diagnosis) in  ('424.0',
'394.0', 
'394.1', 
'394.2',
'394.9', 
'394.0', 
'394.1', 
'396.3', 
'396.0',
'396.1',
'396.2',
'396.3',
'396.8',
'396.9', 
'4240' ,
'3940' ,
'3941' ,
'3942' ,
'3949' ,
'3940' ,
'3941' ,
'3963' ,
'3960' ,
'3961' ,
'3962' ,
'3963' ,
'3968' ,
'3969',
'I34.8',
'I05.0',
'I05.1',
'I05.1',
'I05.2',
'I05.8',
'I05.0',
'I05.9', 
'I34.0',
'I34.1',
'I34.2',
'I34.8',
'I34.9',
'I08.0',
'I08.1',
'I08.8',
'I08.3',
'I08.9',
'I348' ,
'I050' ,
'I051' ,
'I051' ,
'I052' ,
'I058' ,
'I050' ,
'I059' ,
'I340' ,
'I341' ,
'I342' ,
'I348' ,
'I349' ,
'I080' ,
'I081' ,
'I088' ,
'I083' ,
'I089')

 AND   encrypted_key_1 IS NOT NULL 
AND    Upper(encrypted_key_1) <> 'NULL' 
AND    Upper(encrypted_key_1) NOT LIKE 'XXX -%' 
AND Year(Service_Date) in ('2014','2015','2016')

////QC?//////////////////////////
//////////////////12,454,030
Select * from project_analytics.presales.AC_Diag_Raven_Mitral  ////12,454,030

----------------Joining with provider table to get NPI AND PROVIDER TYPE------------------
drop table if exists project_analytics.presales.AC_Diag_Raven_Mitral_Prov;
create table project_analytics.presales.AC_Diag_Raven_Mitral_Prov as
Select a.*, 
       b.PROVIDER_NPI,
       b.PROVIDER_TYPE

from project_analytics.presales.AC_Diag_Raven_Mitral a
left join rwd_db.sandbox.claims_submits_provider b on a.claim_number=b.claim_number

/////////////////////////TOTAL RECORDS FROM THE ABOVE TABLE/////////////////
Select * from project_analytics.presales.AC_Diag_Raven_Mitral_Prov        /////40,856,015


//////////////////////////////////Adding case to add Provier NPI//////////////////
drop table if exists project_analytics.presales.AC_Diag_Raven_Mitral_Prov_Mapping;
create table project_analytics.presales.AC_Diag_Raven_Mitral_Prov_Mapping as

select a.*,

case when b.provider_NPI is null then
case when c.Provider_NPI is null then 
d.Provider_NPI else c.Provider_NPI end
else b.provider_NPI end as Physician_NPI


from  project_analytics.presales.AC_Diag_Raven_Mitral_Prov as a 
left join (select claim_number,Provider_NPI from project_analytics.presales.AC_Diag_Raven_Mitral_Prov where lower(provider_type) = 'attending') as b
on a.claim_number = b.claim_number
left join (select claim_number,Provider_NPI from project_analytics.presales.AC_Diag_Raven_Mitral_Prov where lower(provider_type) = 'rendering') as c
on a.claim_number = c.claim_number
left join (select claim_number,Provider_NPI from project_analytics.presales.AC_Diag_Raven_Mitral_Prov where lower(provider_type) = 'operating') as d
on a.claim_number = d.claim_number

////////////TOTAL RECORDS FROM THE ABOVE TABLE//////////////////////
Select * from project_analytics.presales.AC_Diag_Raven_Mitral_Prov_Mapping         /////////////42,685,109
limit 5


/////////////////UNION OF RAVEN AND PHARMACY/////////////
drop table if exists project_analytics.presales.AC_Diag_Raven_Pharm_Mitral_Union;
create table project_analytics.presales.AC_Diag_Raven_Pharm_Mitral_Union as
Select data_source, claim_number, patient_suffix, Service_Date, Physician_NPI from project_analytics.presales.AC_Diag_Raven_Mitral_Prov_Mapping 
UNION
Select data_source, Claim_number, patient_Suffix, Service_from, Provider_NPI from project_analytics.presales.AC_Claims_RXA_Mitral_Daig

///////TOTAL RECORDS FROM THE ABOVE TABLE///////////
Select * from project_analytics.presales.AC_Diag_Raven_Pharm_Mitral_Union         ///12,172,040

////////////////COUNTS//////////////////////////
Select count(distinct(patient_Suffix)), Year(Service_Date) as Year from project_analytics.presales.AC_Diag_Raven_Pharm_Mitral_Union
group by Year
order by year

*********************************************HEARYT FAILURE DISEASE*************************************************************

///////////PHARMACY CODE FOR HEART FAILURE////////////////
CREATE TABLE project_analytics.presales.AC_Claims_RXA_Heart_Daig AS
SELECT
    encrypted_key_1 AS patient_suffix,
    CONDOR_TDR_CLAIMID AS Claim_Number
    , to_date(date_of_service) as SERVICE_FROM
    ,diagnosis_code,
	 prescriber_id as Provider_NPI,
	 SERVICE_PROVIDER_ID as facility_npi 
    
    FROM rwd_db.rwd.condor_pharmacy_record

WHERE TRIM(UPPER(RESPONSE_CODE)) = 'P'
  AND trim(upper(diagnosis_code)) in ('428.0',
'428.1', 
'428.20',
'428.21',
'428.22',
'428.23', 
'428.30',
'428.31',
'428.32', 
'428.33',
'428.40',
'428.41',
'428.42',
'428.43',
'428.9',
'4280',
'4281',
'42820',
'42821',
'42822', 
'42823', 
'42830',
'42831',
'42832', 
'42833',
'42840',
'42841',
'42842',
'42843',
'4289',
'I50',
'I50.1',
'I50.20',
'I50.21',
'I50.22',
'I50.23',
'I50.30',
'I50.31',
'I50.32',
'I50.33',
'I50.40',
'I50.41',
'I50.42',
'I50.43',
'I50.810',
'I50.811',
'I50.812',
'I50.813',
'I50.814',
'I50.82',
'I50.83',
'I50.84',
'I50.89',
'I50.9',
'I501',
'I5020',
'I5021',
'I5022',
'I5023',
'I5030',
'I5031',
'I5032',
'I5033',
'I5040',
'I5041',
'I5042',
'I5043',
'I50810',
'I50811',
'I50812',
'I50813',
'I50814',
'I5082',
'I5083',
'I5084',
'I5089',
'I509'  )
  and
  patient_suffix is not null
        and trim(patient_suffix) <> ''
        and upper(patient_suffix) not like 'XXX -%'
        and upper(patient_suffix) <> 'NULL'
AND Year(SERVICE_FROM) in ('2014','2015','2016')

/////////////////////TOTAL RECORDS FROM THE ABOVE TABLE/////////////////////
Select * from project_analytics.presales.AC_Claims_RXA_Heart_Daig      //////////////178,558

////////////////////RAVEN CODE FOR HEART FAILURE DISEASE///////////////////

drop table if exists project_analytics.presales.AC_Diag_Raven_Heart ;
CREATE TABLE project_analytics.presales.AC_Diag_Raven_Heart as 
SELECT   encrypted_key_1 as patient_suffix,
        claim_number,
        Coalesce(MIN_SERVICE_FROM,STATEMENT_FROM) as Service_Date,
        Diagnosis
          
FROM   rwd_db.sandbox.claims_submits_Diagnosis
where
UPPER(Diagnosis) in  ('428.0',
'428.1', 
'428.20',
'428.21',
'428.22',
'428.23', 
'428.30',
'428.31',
'428.32', 
'428.33',
'428.40',
'428.41',
'428.42',
'428.43',
'428.9',
'4280',
'4281',
'42820',
'42821',
'42822', 
'42823', 
'42830',
'42831',
'42832', 
'42833',
'42840',
'42841',
'42842',
'42843',
'4289',
'I50',
'I50.1',
'I50.20',
'I50.21',
'I50.22',
'I50.23',
'I50.30',
'I50.31',
'I50.32',
'I50.33',
'I50.40',
'I50.41',
'I50.42',
'I50.43',
'I50.810',
'I50.811',
'I50.812',
'I50.813',
'I50.814',
'I50.82',
'I50.83',
'I50.84',
'I50.89',
'I50.9',
'I501',
'I5020',
'I5021',
'I5022',
'I5023',
'I5030',
'I5031',
'I5032',
'I5033',
'I5040',
'I5041',
'I5042',
'I5043',
'I50810',
'I50811',
'I50812',
'I50813',
'I50814',
'I5082',
'I5083',
'I5084',
'I5089',
'I509')

 AND   encrypted_key_1 IS NOT NULL 
AND    Upper(encrypted_key_1) <> 'NULL' 
AND    Upper(encrypted_key_1) NOT LIKE 'XXX -%' 
AND Year(Service_Date) in ('2014','2015','2016')

///////TOTAL RECORDS FROM THE ABOVE TABLE//////////////////////////
Select * from project_analytics.presales.AC_Diag_Raven_Heart   /////////////////65,613,023

S
----------------Joining with provider table to get NPI AND PROVIDER TYPE------------------
drop table if exists project_analytics.presales.AC_Diag_Raven_Heart_Prov;
create table project_analytics.presales.AC_Diag_Raven_Heart_Prov as
Select a.*, 
       b.PROVIDER_NPI,
       b.PROVIDER_TYPE

from project_analytics.presales.AC_Diag_Raven_Heart a
left join rwd_db.sandbox.claims_submits_provider b on a.claim_number=b.claim_number

//////TOTAL RECORDS FROM THE ABOVE TABLE//////////////////
Select * from project_analytics.presales.AC_Diag_Raven_Heart_Prov        ///// 185,473,699

//////////////////////////////////Adding case to add Provier NPI//////////////////
drop table if exists project_analytics.presales.AC_Diag_Raven_Heart_Prov_Mapping;
create table project_analytics.presales.AC_Diag_Raven_Heart_Prov_Mapping as

select a.*,

case when b.provider_NPI is null then
case when c.Provider_NPI is null then 
d.Provider_NPI else c.Provider_NPI end
else b.provider_NPI end as Physician_NPI


from  project_analytics.presales.AC_Diag_Raven_Heart_Prov as a 
left join (select claim_number,Provider_NPI from project_analytics.presales.AC_Diag_Raven_Heart_Prov where lower(provider_type) = 'attending') as b
on a.claim_number = b.claim_number
left join (select claim_number,Provider_NPI from project_analytics.presales.AC_Diag_Raven_Heart_Prov where lower(provider_type) = 'rendering') as c
on a.claim_number = c.claim_number
left join (select claim_number,Provider_NPI from project_analytics.presales.AC_Diag_Raven_Heart_Prov where lower(provider_type) = 'operating') as d
on a.claim_number = d.claim_number

////////////RECORDS FROM THE ABOVE TABLE//////////////////////
Select * from project_analytics.presales.AC_Diag_Raven_Heart_Prov_Mapping         ////////////216,004,082


/////////////////UNION OF RAVEN AND PHARMACY/////////////
drop table if exists project_analytics.presales.AC_Diag_Raven_Pharm_Heart_Union;
create table project_analytics.presales.AC_Diag_Raven_Pharm_Heart_Union as
Select 'Raven' as Data_Source, claim_number, patient_suffix, Service_Date, Physician_NPI from project_analytics.presales.AC_Diag_Raven_Heart_Prov_Mapping 
UNION
Select 'Pharmacy' as Data_source,Claim_number, patient_Suffix, Service_from, Provider_NPI from project_analytics.presales.AC_Claims_RXA_Heart_Daig

/////////RECORDS FROM THE ABOVE TABLE///////////
Select * from project_analytics.presales.AC_Diag_Raven_Pharm_Heart_Union         ///60,765,496


/////////QC//////////////////////////////////////
Select count(distinct(patient_suffix)), year(service_date) as Year from project_analytics.presales.AC_Diag_Raven_Pharm_Heart_Union 
group by year
order by year


/////////////////////////UNION of MITRAL AND HEART FAILURE///////////////////////
drop table if exists project_analytics.presales.AC_Mitral_Heart_Diagnosis;
create table project_analytics.presales.AC_Mitral_Heart_Diagnosis as
Select Data_Source, claim_number, patient_suffix, Service_Date, Physician_NPI from project_analytics.presales.AC_Diag_Raven_Pharm_Mitral_Union
UNION
Select Data_Source, claim_number, patient_suffix, Service_Date, Physician_NPI from project_analytics.presales.AC_Diag_Raven_Pharm_Heart_Union


//////Patients with Mitral and Heart failure/////

Select count(distinct patient_suffix) from project_analytics.presales.AC_Mitral_Heart_Diagnosis   ////10219711

/////Year wise Patients////
Select count(distinct patient_suffix), Year(service_date) as Year from project_analytics.presales.AC_Mitral_Heart_Diagnosis
group by Year
order by year

/////total records//////////////
Select * from project_analytics.presales.AC_Mitral_Heart_Diagnosis   /////////71,113,655


/////////////////////////////MITRAL VALVE REPAIR//////////////////////////////////////////////////////

//////////////////FETCHING PROCEDURE COUNTS FROM RAVEN TABLE////////////////////////////////////////////
CREATE TABLE project_analytics.presales.AC_PROC_RAVEN_Mitral_Repair as 
SELECT   encrypted_key_1 as patient_suffix,
        claim_number,
        COALESCE(service_from,STATEMENT_FROM) as Service_Date,
        PROCEDURE
          
FROM    rwd_db.sandbox.claims_submits_procedure
where
UPPER(PROCEDURE) in ('0345T',
'33418',
'33419',
'02QG0ZZ',
'33425',
'33427',
'33426',
'02QG0ZZ',
'02QG3ZZ',
'02QG4ZZ',
'02QG0ZE',
'02QG3ZE',
'02QG4ZE',
'V43.3',
'35.02',
'35.12',
'02QG0ZE',
'02QG0ZZ',
'02QG3ZE',
'02QG3ZZ',
'02QG4ZE',
'02QG4ZZ',
'V433',
'3502',
'3512',
'0343T',
'0344T')

 AND   encrypted_key_1 IS NOT NULL 
AND    Upper(encrypted_key_1) <> 'NULL' 
AND    Upper(encrypted_key_1) NOT LIKE 'XXX -%' 
AND Year(Service_Date) in ('2014','2015','2016')

//////////////////TOTAL RECORDS FROM THE ABOVE TABLE////////////////
Select * from project_analytics.presales.AC_PROC_RAVEN_Mitral_Repair    ///////////45,902

----------------JOINING THE RAVEN TABLE WITH RAVEN PROVIDER TABLE TO GET NPI AND PROVIDER TYPE------------------
drop table if exists project_analytics.presales.AC_PROC_RAVEN_Mitral_Repair_Prov;
create table project_analytics.presales.AC_PROC_RAVEN_Mitral_Repair_Prov as
Select a.*, 
       b.PROVIDER_NPI,
       b.PROVIDER_TYPE

from project_analytics.presales.AC_PROC_RAVEN_Mitral_Repair a
left join rwd_db.sandbox.claims_submits_provider b on a.claim_number=b.claim_number

////////////////////TOTAL RECORDS FROM THE ABOVE FILE/////////////////////////////
Select * from project_analytics.presales.AC_PROC_RAVEN_Mitral_Repair_Prov        /////160,218

//////////////////////////////////ADDING CASE TO ADD PROVIDER NPI//////////////////
drop table if exists project_analytics.presales.AC_PROC_RAVEN_Mitral_Repair_Prov_Mapping;
create table project_analytics.presales.AC_PROC_RAVEN_Mitral_Repair_Prov_Mapping as

select a.*,

case when b.provider_NPI is null then
case when c.Provider_NPI is null then 
d.Provider_NPI else c.Provider_NPI end
else b.provider_NPI end as Physician_NPI


from  project_analytics.presales.AC_PROC_RAVEN_Mitral_Repair_Prov as a 
left join (select claim_number,Provider_NPI from project_analytics.presales.AC_PROC_RAVEN_Mitral_Repair_Prov where lower(provider_type) = 'attending') as b
on a.claim_number = b.claim_number
left join (select claim_number,Provider_NPI from project_analytics.presales.AC_PROC_RAVEN_Mitral_Repair_Prov where lower(provider_type) = 'rendering') as c
on a.claim_number = c.claim_number
left join (select claim_number,Provider_NPI from project_analytics.presales.AC_PROC_RAVEN_Mitral_Repair_Prov where lower(provider_type) = 'operating') as d
on a.claim_number = d.claim_number


///////////////////////////////TOTAL RECORDS FROM THE ABOVE FILE///////////////////////
Select * from project_analytics.presales.AC_PROC_RAVEN_Mitral_Repair_Prov_Mapping   ///////////179,936




***************************************************MITRAL VALVE REPLACEMENT*********************************************************************************************

//////////////////FETCHING PROCEDURE DETAILS FROM RAVEN TABLE////////////////////////////////////////////
CREATE TABLE project_analytics.presales.AC_PROC_RAVEN_Mitral_Replacement as 
SELECT   encrypted_key_1 as patient_suffix,
        claim_number,
        COALESCE(service_from,STATEMENT_FROM) as Service_Date,
        PROCEDURE
          
FROM    rwd_db.sandbox.claims_submits_procedure
where
UPPER(PROCEDURE) in ('33430',
'33420',
'33422',
'35.23',
'35.24',
'02RG07Z',
'02RG08Z',
'02RG0JZ',
'02RG0KZ',
'02RG37H',
'02RG37Z',
'02RG38H',
'02RG38Z',
'02RG3JH',
'02RG3JZ',
'02RG3KH',
'02RG3KZ',
'02RG47Z',
'02RG48Z',
'02RG4JZ',
'02RG4KZ',
'3523',
'3524')

 AND   encrypted_key_1 IS NOT NULL 
AND    Upper(encrypted_key_1) <> 'NULL' 
AND    Upper(encrypted_key_1) NOT LIKE 'XXX -%' 
AND Year(Service_Date) in ('2014','2015','2016')

////////////TOTAL RECORDS FROM THE ABOVE TABLE///////////////////////////
Select * from project_analytics.presales.AC_PROC_RAVEN_Mitral_Replacement   ////////////43,138

----------------JOINING THE RAVEN TABLE WITH PROVIDER TABLE TO FETCH NPI AND PROVIDER TYPE------------------
drop table if exists project_analytics.presales.AC_PROC_RAVEN_Mitral_Replacement_Prov;
create table project_analytics.presales.AC_PROC_RAVEN_Mitral_Replacement_Prov as
Select a.*, 
       b.PROVIDER_NPI,
       b.PROVIDER_TYPE

from project_analytics.presales.AC_PROC_RAVEN_Mitral_Replacement a
left join rwd_db.sandbox.claims_submits_provider b on a.claim_number=b.claim_number

///////////////////////////TOTAL RECORDS FROM THE ABOVE TABLE//////////////////
Select * from project_analytics.presales.AC_PROC_RAVEN_Mitral_Replacement_Prov        /////141,122

//////////////////////////////////ADDING CASE TO ADD PROVDER NPI//////////////////
drop table if exists project_analytics.presales.AC_PROC_RAVEN_Mitral_Replacement_Prov_Mapping;
create table project_analytics.presales.AC_PROC_RAVEN_Mitral_Replacement_Prov_Mapping as

select a.*,

case when b.provider_NPI is null then
case when c.Provider_NPI is null then 
d.Provider_NPI else c.Provider_NPI end
else b.provider_NPI end as Physician_NPI


from  project_analytics.presales.AC_PROC_RAVEN_Mitral_Replacement_Prov as a 
left join (select claim_number,Provider_NPI from project_analytics.presales.AC_PROC_RAVEN_Mitral_Replacement_Prov where lower(provider_type) = 'attending') as b
on a.claim_number = b.claim_number
left join (select claim_number,Provider_NPI from project_analytics.presales.AC_PROC_RAVEN_Mitral_Replacement_Prov where lower(provider_type) = 'rendering') as c
on a.claim_number = c.claim_number
left join (select claim_number,Provider_NPI from project_analytics.presales.AC_PROC_RAVEN_Mitral_Replacement_Prov where lower(provider_type) = 'operating') as d
on a.claim_number = d.claim_number

//////////////TOTAL RECORDS FROM THE ABOVE TABLE//////////////////
Select * from project_analytics.presales.AC_PROC_RAVEN_Mitral_Replacement_Prov_Mapping  144,786


/////////////////UNION of Mitral valve repair and replacement////////////////////////
drop table if exists project_analytics.presales.AC_Proc_Mitral_Repair_Replacement;
create table project_analytics.presales.AC_Proc_Mitral_Repair_Replacement as
Select claim_number, patient_suffix, Service_Date, Physician_NPI from project_analytics.presales.AC_PROC_RAVEN_Mitral_Replacement_Prov_Mapping
UNION
Select claim_number, patient_suffix, Service_Date, Physician_NPI from project_analytics.presales.AC_PROC_RAVEN_Mitral_Repair_Prov_Mapping

//////QC///////////////////////
Select * from project_analytics.presales.AC_Proc_Mitral_Repair_Replacement  ///////////86,085



////////////////////JOINING MITRAL DISEASE WITH MITRAL PROCEDURES//////////////
drop table if exists project_analytics.presales.AC_Mitral_Diagnosis_Procedure_Final;
CREATE TABLE project_analytics.presales.AC_Mitral_Diagnosis_Procedure_Final as
select a.data_source, a.claim_number, a.patient_suffix, a.Service_Date, a.Physician_NPI from project_analytics.presales.AC_Proc_Mitral_Repair_Replacement a
Inner Join project_analytics.presales.AC_Mitral_Heart_Diagnosis b on a.patient_suffix=b.patient_suffix

////////////////qc//////////////
Select * from project_analytics.presales.AC_Mitral_Diagnosis_Procedure_Final  ////2,399,041

////Patients/////
Select count(distinct(patient_suffix)) from project_analytics.presales.AC_Mitral_Diagnosis_Procedure_Final

////Year wise////
Select count(distinct(patient_suffix)), Year(service_date) as Year from project_analytics.presales.AC_Mitral_Diagnosis_Procedure_Final
group by year
order by year


//////////////////////////////Joining to get speciality//////////////////////////////
drop table if exists project_analytics.presales.AC_Mitral_Diagnosis_Procedure_Final_Spec;
create  table project_analytics.presales.AC_Mitral_Diagnosis_Procedure_Final_Spec as
select a.*,
b.ENTITY_TYPE_CODE,
b.PROVIDER_NAME_PREFIX_TEXT,

cast(b.HEALTHCARE_PROVIDER_TAXONOMY_code_1 as varchar()) as HEALTHCARE_PROVIDER_TAXONOMY_code_1,
b.HEALTHCARE_PROVIDER_PRIMARY_TAXONOMY_SWITCH_1,

cast(b.HEALTHCARE_PROVIDER_TAXONOMY_code_2 as varchar()) as HEALTHCARE_PROVIDER_TAXONOMY_code_2,
b.HEALTHCARE_PROVIDER_PRIMARY_TAXONOMY_SWITCH_2,

cast(b.HEALTHCARE_PROVIDER_TAXONOMY_code_3 as varchar()) as HEALTHCARE_PROVIDER_TAXONOMY_code_3,
b.HEALTHCARE_PROVIDER_PRIMARY_TAXONOMY_SWITCH_3,

cast(b.HEALTHCARE_PROVIDER_TAXONOMY_code_4 as varchar()) as HEALTHCARE_PROVIDER_TAXONOMY_code_4,
b.HEALTHCARE_PROVIDER_PRIMARY_TAXONOMY_SWITCH_4,

cast(b.HEALTHCARE_PROVIDER_TAXONOMY_code_5 as varchar()) as HEALTHCARE_PROVIDER_TAXONOMY_code_5,
b.HEALTHCARE_PROVIDER_PRIMARY_TAXONOMY_SWITCH_5,

cast(b.HEALTHCARE_PROVIDER_TAXONOMY_code_6 as varchar()) as HEALTHCARE_PROVIDER_TAXONOMY_code_6,
b.HEALTHCARE_PROVIDER_PRIMARY_TAXONOMY_SWITCH_6,

cast(b.HEALTHCARE_PROVIDER_TAXONOMY_code_7 as varchar()) as HEALTHCARE_PROVIDER_TAXONOMY_code_7,
b.HEALTHCARE_PROVIDER_PRIMARY_TAXONOMY_SWITCH_7,

cast(b.HEALTHCARE_PROVIDER_TAXONOMY_code_8 as varchar()) as HEALTHCARE_PROVIDER_TAXONOMY_code_8,
b.HEALTHCARE_PROVIDER_PRIMARY_TAXONOMY_SWITCH_8,

cast(b.HEALTHCARE_PROVIDER_TAXONOMY_code_9 as varchar()) as HEALTHCARE_PROVIDER_TAXONOMY_code_9,
b.HEALTHCARE_PROVIDER_PRIMARY_TAXONOMY_SWITCH_9,

cast(b.HEALTHCARE_PROVIDER_TAXONOMY_code_10 as varchar()) as HEALTHCARE_PROVIDER_TAXONOMY_code_10,
b.HEALTHCARE_PROVIDER_PRIMARY_TAXONOMY_SWITCH_10,

cast(b.HEALTHCARE_PROVIDER_TAXONOMY_code_11 as varchar()) as HEALTHCARE_PROVIDER_TAXONOMY_code_11,
b.HEALTHCARE_PROVIDER_PRIMARY_TAXONOMY_SWITCH_11,

cast(b.HEALTHCARE_PROVIDER_TAXONOMY_code_12 as varchar()) as HEALTHCARE_PROVIDER_TAXONOMY_code_12,
b.HEALTHCARE_PROVIDER_PRIMARY_TAXONOMY_SWITCH_12,

cast(b.HEALTHCARE_PROVIDER_TAXONOMY_code_13 as varchar()) as HEALTHCARE_PROVIDER_TAXONOMY_code_13,
b.HEALTHCARE_PROVIDER_PRIMARY_TAXONOMY_SWITCH_13,

cast(b.HEALTHCARE_PROVIDER_TAXONOMY_code_14 as varchar()) as HEALTHCARE_PROVIDER_TAXONOMY_code_14,
b.HEALTHCARE_PROVIDER_PRIMARY_TAXONOMY_SWITCH_14,

cast(b.HEALTHCARE_PROVIDER_TAXONOMY_code_15 as varchar()) as HEALTHCARE_PROVIDER_TAXONOMY_code_15,
b.HEALTHCARE_PROVIDER_PRIMARY_TAXONOMY_SWITCH_15,
b.PROVIDER_CREDENTIAL_TEXT
from project_analytics.presales.AC_Mitral_Diagnosis_Procedure_Final a
left join RWD_DB.RWD.THIRD_PARTY_CMSNPI  b on a.Physician_NPI = cast(b.npi as varchar())

///////////////QC///////////////////
Select * from project_analytics.presales.AC_Mitral_Diagnosis_Procedure_Final_Spec   ////2,399,038

////////Adding TAXONOMY/////
drop table if exists project_analytics.presales.AC_Mitral_Diagnosis_Procedure_Final_Spec_2;
create table project_analytics.presales.AC_Mitral_Diagnosis_Procedure_Final_Spec_2 as
Select *,

Case when Upper(HEALTHCARE_PROVIDER_PRIMARY_TAXONOMY_SWITCH_1) = 'Y' then HEALTHCARE_PROVIDER_TAXONOMY_code_1
    when Upper(HEALTHCARE_PROVIDER_PRIMARY_TAXONOMY_SWITCH_2) = 'Y' then HEALTHCARE_PROVIDER_TAXONOMY_code_2
    when Upper(HEALTHCARE_PROVIDER_PRIMARY_TAXONOMY_SWITCH_3) = 'Y' then HEALTHCARE_PROVIDER_TAXONOMY_code_3
    when Upper(HEALTHCARE_PROVIDER_PRIMARY_TAXONOMY_SWITCH_4) = 'Y' then HEALTHCARE_PROVIDER_TAXONOMY_code_4
    when Upper(HEALTHCARE_PROVIDER_PRIMARY_TAXONOMY_SWITCH_5) = 'Y' then HEALTHCARE_PROVIDER_TAXONOMY_code_5
    when Upper(HEALTHCARE_PROVIDER_PRIMARY_TAXONOMY_SWITCH_6) = 'Y' then HEALTHCARE_PROVIDER_TAXONOMY_code_6
    when Upper(HEALTHCARE_PROVIDER_PRIMARY_TAXONOMY_SWITCH_7) = 'Y' then HEALTHCARE_PROVIDER_TAXONOMY_code_7
    when Upper(HEALTHCARE_PROVIDER_PRIMARY_TAXONOMY_SWITCH_8) = 'Y' then HEALTHCARE_PROVIDER_TAXONOMY_code_8
    when Upper(HEALTHCARE_PROVIDER_PRIMARY_TAXONOMY_SWITCH_9) = 'Y' then HEALTHCARE_PROVIDER_TAXONOMY_code_9
    when Upper(HEALTHCARE_PROVIDER_PRIMARY_TAXONOMY_SWITCH_10) = 'Y' then HEALTHCARE_PROVIDER_TAXONOMY_code_10
    when Upper(HEALTHCARE_PROVIDER_PRIMARY_TAXONOMY_SWITCH_11) = 'Y' then HEALTHCARE_PROVIDER_TAXONOMY_code_11
    when Upper(HEALTHCARE_PROVIDER_PRIMARY_TAXONOMY_SWITCH_12) = 'Y' then HEALTHCARE_PROVIDER_TAXONOMY_code_12
    when Upper(HEALTHCARE_PROVIDER_PRIMARY_TAXONOMY_SWITCH_13) = 'Y' then HEALTHCARE_PROVIDER_TAXONOMY_code_13
    when Upper(HEALTHCARE_PROVIDER_PRIMARY_TAXONOMY_SWITCH_14) = 'Y' then HEALTHCARE_PROVIDER_TAXONOMY_code_14
    when Upper(HEALTHCARE_PROVIDER_PRIMARY_TAXONOMY_SWITCH_15) = 'Y' then HEALTHCARE_PROVIDER_TAXONOMY_code_15 
    when HEALTHCARE_PROVIDER_PRIMARY_TAXONOMY_SWITCH_1 is not null then  HEALTHCARE_PROVIDER_TAXONOMY_code_1 end as Final_taxonomy 
    
 from project_analytics.presales.AC_Mitral_Diagnosis_Procedure_Final_Spec  


///?QC////////
Select * from project_analytics.presales.AC_Mitral_Diagnosis_Procedure_Final_Spec_2  /////2,399,041

/////////////Joining to get the speciality codes//////
drop table if exists project_analytics.presales.AC_Mitral_Diagnosis_Procedure_Final_Spec_Mapping_2;
create table project_analytics.presales.AC_Mitral_Diagnosis_Procedure_Final_Spec_Mapping_2 as
select a.*,
b.CLASSIFICATION,
b.GROUPING_X,
b.SPECIALIZATION,
b.SPECIALTY
from project_analytics.presales.AC_Mitral_Diagnosis_Procedure_Final_Spec_2 a 
left join RWD_DB.RWD.CMS_Provider_Specialty_Codes b on a.Final_taxonomy = b.code
where
a.ENTITY_TYPE_CODE='1'

/////////////QC///////////
Select * from project_analytics.presales.AC_Mitral_Diagnosis_Procedure_Final_Spec_Mapping_2   ////2,262,123

///////////Table for physician details///////
drop table if exists project_analytics.presales.AC_Mitral_Diagnosis_Procedure_Final_Spec_Mapping_2_Phy;
Create table project_analytics.presales.AC_Mitral_Diagnosis_Procedure_Final_Spec_Mapping_2_Phy as
Select  a.data_source, a.Claim_Number,a.Physician_NPI, a.patient_suffix, b.PROVIDER_FIRST_NAME as First_Name, b.PROVIDER_LAST_NAME_LEGAL_NAME as Last_Name,
b.PROVIDER_FIRST_LINE_BUSINESS_MAILING_ADDRESS as Mailing_Address, b.PROVIDER_BUSINESS_MAILING_ADDRESS_CITY_NAME as City_Name, b.PROVIDER_BUSINESS_MAILING_ADDRESS_STATE_NAME as State_Name,
b.PROVIDER_BUSINESS_MAILING_ADDRESS_POSTAL_CODE as ZIP, 
a.SPECIALTY, a.Service_Date, a.Final_taxonomy
from project_analytics.presales.AC_Mitral_Diagnosis_Procedure_Final_Spec_Mapping_2 a 
left join RWD_DB.RWD.THIRD_PARTY_CMSNPI b on a.Physician_NPI=cast(b.NPI as varchar())

//////QC///////////////////
Select distinct Final_taxonomy from project_analytics.presales.AC_Mitral_Diagnosis_Procedure_Final_Spec_Mapping_2_Phy  /////2,262,123
where
Upper(SPECIALTY) is null

/////////////COUNTS/////////////////////
Select Year(Service_Date) as Year, Physician_NPI, First_Name, Last_Name,Mailing_Address,City_Name,State_Name,ZIP, SPECIALTY, Data_Source, count(distinct(claim_number)) from project_analytics.presales.AC_Mitral_Diagnosis_Procedure_Final_Spec_Mapping_2_Phy

group by Year,2,3,4,5,6,7,8,9,10
order by Year,2,3,4,5,6,7,8,9,10

