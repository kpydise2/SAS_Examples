

%let outpath='E:\Kiran\';

%let codepath='E:\Kiran\Code';

%let startdate='01JAN2014'd;

%let enddate='31DEC2018'd;

%let basefile=sample2018_basefile;


%let prod=asimv.ASIMV_BHIMV_08JAN2019_v3;

%let input_sheet=oxycontin2019_comparator_input_sheet;


%let as_inj=
ASID003R in (4,5)|
	ASID004R in (4,5)|
	ASID005R in (4,5)|
	ASID006R in (4,5)|
	ASID007R in (4,5)|
	ASID008R in (4,5)|
	ASID009R in (4,5)|
	ASID010R in (4,5)|
	ASID011R in (4,5)|
	ASID012R in (4,5) |
	ASID0020R in (4,5) |
	ASID_KR in (4,5) |
	ASID_MR in (4,5) |
	ASID_NR in (4,5) |
	ASID_OR in (4,5) |
	ASID_PR in (4,5) |
	ASID_QR in (4,5) |
	ASID_RR in (4,5) |
	ASID_SR in (4,5)
	;
	%put &as_inj;



%let outpath_unq = %qsysfunc(compress(&outpath,%str(%')));


libname outf &outpath.;

libname codef &codepath.;


%let codepath_unq = %qsysfunc(compress(&codepath,%str(%')));


filename code &codepath.;


filename ROA &outpath.;



data drugvars_import;
length comparator drugvar $ 50;
input comparator $ drugvar $;
infile datalines dlm=',' dsd;
datalines;
RefOxyContin,OP1F,
AnyOxyContin,OP1A,
,OP1F,
Morphine,OP8A,
,OP8B,
,OP8C,
,OP8D,
,OP8E,
,OP8F,
,OP8G,
,OP8H,
,OP8I,
,OP8J,
MSContin,OP8A,
MorphineIR,OP8B,
,OP8G,
MorphER,OP8A,
,OP8C,
,OP8D,
,OP8E,
,OP8F,
,OP8H,
,OP8I,
,OP8J,
HydrocoIR,OP3A,
,OP3B,
,OP3C,
,OP3D,
,OP3E,
,OP3F,
,OP3H,
OtherSchII,OP3A,
,OP3B,
,OP3C,
,OP3D,
,OP3E,
,OP3F,
,OP3H,
,OP18A,
,OP3I,
,OP9A,
,OP9B,
,OP9D,
,OP9F,
,OP9G,
,OP9H,
,OP9I,
,OP9J,
,OP7A,
,OP7C,
,OP7E,
,OP7F,
,OP7H,
,OP7I,
,OP7B,
,OP8A,
,OP8B,
,OP8C,
,OP8D,
,OP8E,
,OP8F,
,OP8G,
,OP8H,
,OP8I,
,OP8J,
,OP2A,
,OP2B,
,OP2C,
,OP2D,
,OP2E,
,OP2F,
,OP2H,
,OP16A,
,OP16B,
,OP16C,
,OP16D,
,OP16E,
,OP16F,
,OP16G,
,OP16H,
,OP16I,
OxymorphER,OP9A,
,OP9D,
,OP9H,
,OP9I,
,OP9J,
OxycIRSE,OP2E,
,OP16A,
,OP16C,
,OP16E,
,OP16F,
,OP16G,
,OP16H,
,OP16I,
OxycIRComb,OP2A,
,OP2B,
,OP2C,
,OP2D,
,OP2F,
,OP2H,
,OP16B,
,OP16D,
OxycIR,OP2A,
,OP2B,
,OP2C,
,OP2D,
,OP2E,
,OP2F,
,OP2H,
,OP16A,
,OP16B,
,OP16C,
,OP16D,
,OP16E,
,OP16F,
,OP16G,
,OP16H,
,OP16I,
Methadone,OP4A,
,OP4B,
,OP4D,
,OP4E,
,OP4F,
;
run;

data routes;
length route $ 50;
input route $ ;

infile datalines dlm=',';
datalines;
swallow
snort
smoke
inject_noniv
inject_iv
chew
other
dissolve
drank
patchonskin
suckpatch
chewpatch
buccal
;
run;


data source;
length route $ 50;
input route $ ;

infile datalines dlm=',';
datalines;
ownrx
multdrs
internet
boughtfamfriend
dealer
rxforgery
stole
othersource
stolefamfriend
trade
givenfamfriend
;
run;

data roa_group;
length group $ 50;
input group $  roa_index;
infile datalines dlm=',' dsd;
datalines;
anyoral,1
,6
,8
,9
,11
,12
,13
nonoral,2
,3
,4
,5
anyinject,4
,5
run;



data src_group;
length group $ 50;
input group $  roa_index;
infile datalines dlm=',' dsd;
datalines;
ownpresc,1
,2
famfriend,4
,9
,11
otherplustraded,8
,10
;
run;

data roasrc_group;
set roa_group src_group(in=b);
if b then src_ind=1;else src_ind=0;
run;

data roasrc_group(drop=group_temp);
set roasrc_group;
retain group_temp ;
if length(group) ne 1 then group_temp=group;
else group=group_temp;
run;

data drugvars_import;
set drugvars_import;
index=_n_;
run;

proc contents data=&prod. out=check_vars noprint;run;

proc sql;create table drugvars
as select a.* from drugvars_import a
inner join check_vars b
on lowcase(cats(a.drugvar,"_use"))=lowcase(b.name);
quit;

proc sort data=drugvars; by index;run;



/*op5b_use does not exist*/

data drugvars (drop=_comparatortemp);
set drugvars;
length case_drug $ 200;
retain _comparatortemp ;
if length(comparator) ne 1  then _comparatortemp=comparator; 

else  comparator=_comparatortemp; 



if   substr(drugvar,length(drugvar)-1,2)='30' or substr(drugvar,length(drugvar)-2,3)='30D' then days_ind=1;else days_ind=0;

if days_ind=0 then drugvar_use=cats(drugvar,"_use");
else drugvar_use=drugvar;

if days_ind= 1 then case_drug=cats(drugvar_use,">0");
else case_drug=cats(drugvar_use,"=1");

if lowcase(comparator)='oxycodoneircombo' and lowcase(drugvar)='op2h' then 
case_drug=catx(' ','(',drugvar_use,"=1 and testdate>=mdy(3,14,2015))");

if lowcase(comparator) in ('zohydro','hydrocodone_er_adf','opioids_adf_nolabel') and lowcase(drugvar)='op18a'
then case_drug=catx(' ','(',drugvar_use,"=1 and testdate>=mdy(3,14,2015))");

run;

proc sql;
select cats(drugvar,':') into :keeplist separated by " "
from drugvars;

%put &keeplist;




	data outf.basefile_drugvars(keep =  uniqueidentifier testdate version age rx_opioid zip3 OP9C op16r_gen: op16w_gen: &keeplist);
    set &prod. ;
   if &startdate <= testdate <= &enddate and
          age >= 18 and 
		  rx_opioid = 'Y' ;
run ;



proc sort data=outf.basefile_drugvars nodupkey; by uniqueidentifier testdate version age ; run;


data outf.basefile_drugvars;
set outf.basefile_drugvars;



if OP9C=1  then delete;


run;




data drugvars(drop=_comparatortemp);
set drugvars;
length case_cond $ 1000;
retain _comparatortemp case_cond;
if comparator=_comparatortemp then
case_cond=catx(' ',case_cond,'or',case_drug);
else do;
_comparatortemp=comparator;
case_cond=case_drug;
end;
run;

proc sort data=drugvars;by comparator index;run;

data drugvars2;
set drugvars;
by comparator;
if last.comparator;
run;

proc sort data=drugvars2;by index;run;

data drugvars2;
set drugvars2;
length query $ 2000;
comparator_index=_n_;
query=catx(' ',"proc sql; create table", cats("drug_count",comparator_index),"as select",quote(trim(comparator)),"as comparator,",
"sum(case when",case_cond,"then 1 else 0 end) as abuse_count from outf.basefile_drugvars;quit;");



run;

data _null_;
  set drugvars2;
  file "&outpath_unq\qc_queries.sas";
  put  query;
  file print;
 run;




proc sql;
select cats(comparator,'_abuse') into :abusevars separated by " "
from drugvars2;
 



proc means data=codef.&basefile;
var &abusevars;
output out=basefile_abuse_counts(drop=_type_ _Freq_) sum=;
run;



%include roa(qc_queries);

data qc_queries;
length comparator $ 50;
format comparator $50.;
set drug_count:;
run;

proc transpose data=basefile_abuse_counts out=basefile_abuse_counts_t;run;

proc sql;
create table checkdiffs as select
a.comparator,a.abuse_count as qc_count,b.col1 as basefile_count,
abs(a.abuse_count-b.col1) as qc_diff


from qc_queries a left join basefile_abuse_counts_t b
on cats(lowcase(a.comparator),'_abuse')=lowcase(b._name_)
order by calculated qc_diff desc;
quit;


data routes;
set routes;
roa_index = _n_;
run;

data source;
set source;
roa_index=_n_;
run;

data routesrc;
set routes source(in=b);
if b then src_ind=1; else src_ind=0;
run;


proc sql;
create table roa_combos as
select * 
from drugvars,routesrc;quit;

data roa_combos (keep=comparator drugvar index roa_index route src_ind);
set roa_combos;
run;


data roa_combos;
set roa_combos;
length drugvarroa $ 30;
if src_ind=0 then drugvarroa=cats(drugvar,'R',roa_index);

else drugvarroa=cats(drugvar,'W',roa_index);


if drugvar in ('OP16H', 'OP16F', 'OP16E', 'OP16I') then gen_ind=1;else gen_ind=0;

run;


proc sql;

create table roa_combos2 as select
a.*
from ( select x.*, y.gen_ind_comparator

from
roa_combos x left join
(select comparator,max(gen_ind) as gen_ind_comparator
from roa_combos group by comparator) y
on x.comparator=y.comparator)



a
inner join check_vars b on
a.drugvarroa=b.name;quit;


proc sort data=roa_combos2; by comparator route src_ind roa_index   index ;run;

data roa_combos2(drop=gen_ind);
set roa_combos2;
length roa_var case_roa $ 100;
roa_var=cats(comparator,"_",route);
case_roa=cats(drugvarroa,'=1');

run;



data roa_combos2(drop=_roa_vartemp);
set roa_combos2;
length case_cond_roa $ 3000;
retain _roa_vartemp case_cond_roa;
if roa_var=_roa_vartemp then
case_cond_roa=catx(' ',case_cond_roa,'or',case_roa);
else do;
_roa_vartemp =roa_var;
case_cond_roa=case_roa;
end;
run;

proc sort data=roa_combos2;by roa_var index;run;

data roa_combos2;
set roa_combos2;
by roa_var;
if last.roa_var;
run;

proc sort data=roa_combos2;by index;run;

data roa_combos2;
set roa_combos2;
if gen_ind_comparator=1 then

do;
if src_ind=1 then case_cond_roa=catx(' ',case_cond_roa,'or',cats('OP16W_gen',roa_index,'=1'));
else case_cond_roa=catx(' ',case_cond_roa,'or',cats('OP16R_gen',roa_index,'=1'));
end;
run;




proc sql;
create table roa_group_combo as 
select *,cats(comparator,"_",group) as roa_group from roa_combos2 a left join
roasrc_group b on 
a.roa_index=b.roa_index
and a.src_ind=b.src_ind
where b.group is not missing
;
quit;

proc sort data = roa_group_combo; by roa_group;run;

data roa_group_combo;
set roa_group_combo;
length case_cond_group $ 3000;
retain case_cond_group roa_grouptemp;
if roa_group=roa_grouptemp then
case_cond_group=catx(' ',case_cond_group,cats('or (',case_cond_roa,')'));
else do;
case_cond_group=cats('(',case_cond_roa,')');
roa_grouptemp=roa_group;
end;
run;


data roa_group_combo2;
set roa_group_combo;
by roa_group;
if last.roa_group;
run;


data roa_queries1(keep = roa_var case_condition);
set roa_combos2;
case_condition=case_cond_roa;
run;


data roa_queries2(keep = roa_var case_condition);
set roa_group_combo2(drop=roa_var);
case_condition=case_cond_group;
roa_var=roa_group;
run;

data queries_roa_all;
set roa_queries1 roa_queries2;
run;


data queries_roa_all;
set roa_queries1 roa_queries2;
length query $ 4000;
query=catx(' ',"proc sql; create table", cats("roa_count",_n_),"as select",quote(trim(roa_var)),"as roavar,",
"sum(case when",case_condition,"then 1 else 0 end) as roa_count from outf.basefile_drugvars;quit;");
run;

data _null_;
  set queries_roa_all;
  file "&outpath_unq\qc_queriesroa.sas";
  put  query;
  file print;
 run;

 %include roa(qc_queriesroa);

 data qc_queries_roa;
length roavar $ 50;
format roavar $50.;
set roa_count:;
run;

proc sql;
select roavar into :roavarlist separated by ' '
from qc_queries_roa;quit;


data method_names(keep=route);
set routes source roasrc_group(rename=(group=route));
run;

proc sort data=method_names nodupkey; by route;run;

proc sql;

create table methods_list 
as select * from (select distinct comparator from drugvars) a,
 method_names b;quit;

proc sql;
select cats(comparator,"_",route) into :roavarlist separated by " "
from methods_list;quit;

%put &roavarlist;

proc means data=codef.&basefile;
var &roavarlist;
output out=basefile_roa_counts(drop=_type_ _Freq_) sum=;
run;



proc transpose data=basefile_roa_counts out=basefile_roa_counts_t;run;

proc sql;
create table checkdiffsROA as select
b._name_,a.roa_count as qc_count,b.col1 as basefile_count,
abs(a.roa_count-b.col1) as qc_diff


from qc_queries_roa a right join basefile_roa_counts_t b
on lowcase(a.roavar)=lowcase(b._name_)
order by calculated qc_diff desc
;
quit;