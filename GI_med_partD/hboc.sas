/*HBOC merge and preparation code*/

/*add regroups of chemtoxicities as requested*/

/*add yes/no, dummy variables for GI meds*/


%let root=\\quintiles.net\enterprise\Apps\sasdata\SASb\SAS\SAS_DEV\WashU\UW\;
%let hboc=\\quintiles.net\enterprise\Apps\sasdata\SASb\SAS\SAS_DEV\WashU\UW\CH_HBOC_input.xlsx;



options  VALIDVARNAME=V7;

PROC IMPORT DATAFILE="&hboc"      
          OUT=registry DBMS=xlsx REPLACE ;
          GETNAMES = YES  ;
		  sheet="Cancer Registry";
		  
        RUN;

PROC IMPORT DATAFILE="&hboc"      
          OUT=chemtox_1 DBMS=xlsx REPLACE ;
          GETNAMES = YES  ;
		  sheet="Chemotoxicity Part I";
		  
        RUN;

data chemtox_1;
set chemtox_1;
chemtox1_ind=1;
label chemtox1_ind= "Has Chemotoxicity Part I symptom";
if deid_mrn ne "";
run;


PROC IMPORT DATAFILE="&hboc"      
          OUT=chemtox_2 DBMS=xlsx REPLACE ;
          GETNAMES = YES  ;
		  sheet="Chemotoxicity Part II";
		  
        RUN;

PROC IMPORT DATAFILE="&hboc"      
          OUT=chemtox2_lookup DBMS=xlsx REPLACE ;
          GETNAMES = YES  ;
		 sheet= "Chemtox2_lookuptable";
		  
        RUN;



data chemtox_2;
set chemtox_2;
icd_name=lowcase(icd_name);
if deid_mrn ne "";
run;

proc sort data= chemtox_2;
by ICD_Name;run;


proc sort data= chemtox2_lookup;
by icd_name;run;


data chemtox_2_part2;
merge chemtox_2(in=a) chemtox2_lookup(drop=label);
by icd_name;
if a;
run;

proc sort data=chemtox_2_part2;
by deid_mrn varname;
run;


proc means data=chemtox_2_part2 noprint n;
output out=chemtox2_groups ;
by deid_mrn varname;
run;

data chemtox2_groups;
set chemtox2_groups;
if _freq_>=1 then presence=1;
run;

proc sort data=chemtox2_groups;
by deid_mrn varname;
run;

proc sort data=CHEMTOX2_LOOKUP nodupkey out=chem2vars;
by varname;
run;

proc sort data=chemtox_2 out= chem2subjects(keep=deid_mrn) nodupkey;
by deid_mrn;
run;

proc sql noprint;
create table chem2vars_all as
select * from
chem2subjects, chem2vars(keep=varname);quit;


proc sort data=chem2vars_all;
by deid_mrn varname;
run;



data chemtox2_groups2;
merge chem2vars_all (in=a) chemtox2_groups;
by deid_mrn varname;
if a;
run;


data chemtox2_groups2;
set chemtox2_groups2;;
if presence=. then presence=0;
run;



proc sort data=chemtox2_groups2;
by deid_mrn ;
run;




proc transpose data= chemtox2_groups2 out=chemtox2_t(drop=_name_);
by deid_mrn ;
id varname;
var presence;
run;


/*add labels*/


data chemlabels;
set chem2vars;
length str $ 100;
str=cats(varname,"=",quote(strip(label)));
run;


proc sql noprint;
select str into :labelvar separated by ' '
from chemlabels;quit;

data chemtox2_final;
set chemtox2_t;

chemtox2_ind=1;


label &labelvar 
chemtox2_ind="Has Chemotoxicity Part II symptom"
;
run;

PROC IMPORT DATAFILE="&hboc"      
          OUT=gimed_lookup DBMS=xlsx REPLACE ;
          GETNAMES = YES  ;
		 sheet= "GImed_lookup";
		  
        RUN;


PROC IMPORT DATAFILE="&hboc"      
          OUT=gimed(drop=ageat:) DBMS=xlsx REPLACE ;
          GETNAMES = YES  ;
		 sheet= "GI Medications";
		  
        RUN;

proc sql noprint;
create table gimed2 as
select * from 
gimed_lookup a
left join gimed b
on upcase(a.medicationname)=upcase(b.medicationname);
quit;


proc sort data=gimed2;
by deid_mrn medvar;
run;



proc sort data=gimed out=gimed_subjects(keep=deid_mrn) nodupkey;by deid_mrn;run;

proc sort data=gimed_lookup out=gimed_vars(keep=medvar) nodupkey;by medvar;run;

proc sql noprint;
create table gimed_all as
select * from gimed_subjects,gimed_vars;quit;


proc sort data= gimed_all;
by deid_mrn medvar;
run;




proc means data=gimed2 noprint n;
output out=gimed_prevalence(drop=_type_) ;
by deid_mrn medvar ;
run;

data gimed_prevalence2;
merge gimed_all(in=a) gimed_prevalence;
by deid_mrn medvar;
if a;
run;


data gimed_prevalence2;
set gimed_prevalence2;
if _freq_=. then _freq_=0;
if _freq_>=1 then taken=1;else taken=0;
run;


proc sort data= gimed_prevalence2;by deid_mrn;run;


proc transpose data=gimed_prevalence2 out=gimed_prevalence_t(drop=_name_) suffix=_count;
by deid_mrn  ;
id medvar;
var _freq_;
run;



proc transpose data=gimed_prevalence2 out=gimed_taken_t(drop=_name_) suffix=_taken;
by deid_mrn  ;
id medvar;
var taken;
run;

data gimed_merged;
merge gimed_prevalence_t(in=a) gimed_taken_t(in=b);
by deid_mrn;
if a and b;
run;


data gimed_merged(drop=deid_mrn);
set gimed_merged;
deid_temp=strip(put(deid_mrn, best10.));
run;



data gimed_final(drop=deid_temp);
set gimed_merged;
deid_mrn=deid_temp;
gimed_ind=1;

label gimed_ind="Has taken GI medications"
;

run;

PROC IMPORT DATAFILE="&hboc"      
          OUT=otherdiagnosis DBMS=xlsx REPLACE ;
          GETNAMES = YES  ;
		 sheet= "Other Diagnosis";
		  
        RUN;


data otherdiagnosis(drop=diabetes_icd: hypertension_icd:);
set otherdiagnosis;
if deid_mrn ne "";
if sum(of hypertension:)>=1 then hypertension=1;else hypertension=1;
if sum(of diabetes_:)>=1 then diabetes=1;else diabetes=0;
run;

PROC IMPORT DATAFILE="&hboc"      
          OUT=wbc(rename=(var2=wbc_highest)) DBMS=xlsx REPLACE ;
          GETNAMES = YES  ;
		 sheet= "WBC Highest Level";
		  
        RUN;


data wbc;
set wbc;
if deid_mrn ne "";
run;
		
PROC IMPORT DATAFILE="&hboc"      
          OUT=crp DBMS=xlsx REPLACE ;
          GETNAMES = YES  ;
		 sheet= "CRP Highest Level";
		  
        RUN;

data crp;
set crp;
if deid_mrn ne "";
run;

		
PROC IMPORT DATAFILE="&hboc"      
          OUT=socioeconomic DBMS=xlsx REPLACE ;
          GETNAMES = YES  ;
		 sheet= "Socioeconomic";
		  
        RUN;




data Socioeconomic(drop=raw_:);
set Socioeconomic;
if deid_mrn ne "";
if upcase(raw_marital_status) = "MARRIED" then marital_status=1;else
marital_status=0;


if upcase(raw_employment_status) in ("FULL TIME","PART TIME","SELF EMPLOYED","On Active Military Duty")
then employment_status=1;
else if  upcase(raw_employment_status) =upcase("Student - Full Time")
or 
upcase(raw_employment_status)="NOT EMPLOYED" then employment_status=2;
else if upcase(raw_employment_status)="RETIRED" then employment_status=3;
else employment_status=4;
run;



data subjects (keep=deid_mrn);
set registry chemtox_1 chemtox_2 otherdiagnosis socioeconomic crp wbc;
run;

proc sort data=subjects nodupkey;
by deid_mrn;
run;


proc sort data=registry; by deid_mrn;run;

proc sort data=chemtox_1; by deid_mrn;run;


proc sort data=chemtox2_final; by deid_mrn;run;

proc sort data=gimed_final; by deid_mrn;run;



proc sort data=wbc; by deid_mrn;run;

proc sort data=crp;by deid_mrn;run;


proc sort data=socioeconomic;by deid_mrn;run;


data full_join1;
merge subjects(in=a) registry chemtox_1 chemtox2_final gimed_final wbc crp socioeconomic;
by deid_mrn;
run;

/*drop variables for final dataset*/

data full_join1/*(drop=Fatigue_: malaise weakness lethargy)*/;
set full_join1;


if sum(chemtox1_ind,chemtox2_ind)>=1 then chemtox_any=1;
else chemtox_any=0;
label chemtox_any ="Has Chemotoxicity";

if chemtox_any=1 then do;
if sum(fatigue_1,fatigue_2,malaise,weakness,lethargy)>=1 then 
fatigue=1;
else fatigue=0;
end;
run;

data full_join1;
retain deid_mrn race age_at_dx sex zip_code ICD_O_Site_Code Date_of_Dx  
Surgery_of_Primary_Site radiation immunotherapy chemotherapy clinical_m path_stage
fatigue;
set full_join1;
run;

data hboc_final(drop=chemtox1_ind chemtox2_ind);
set full_join1;


length chemtox_group_c $ 50;

if sum(anxiety,agitation,depression,cognitive_impairment,confusion,substance_abuse)>=1 then do;
chemtox_group_c="pysch";
chemtox_group=1;
end;

else if sum(fatigue,dizziness,insomnia)>=1 then do;
chemtox_group_c="fatigue";
chemtox_group=2;
end;

else if sum(anemia,hematological,neutropenia,leukocytosis)>=1 then do;
chemtox_group_c="Hematological";
chemtox_group=3;
end;


else if sum(appetite,chronic_pain,colitis,constipation,fever,nausea_vomiting,anorexia,Acid_Reflux_GERD,Mucositis,abdominal_pain,
heartburn)>=1 then do;
chemtox_group_c="GI";
chemtox_group=4;
end;

label chemtox_group_c ="Chemotoxicity group"

chemtox_group ="Chemotoxicity group number";



if gimed_ind=. then gimed_ind=0;
run;



proc export data=hboc_final
outfile=
"&root\hboc_final.xlsx"
dbms=xlsx replace label;
run;





