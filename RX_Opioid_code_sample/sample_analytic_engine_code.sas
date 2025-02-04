

%let outpath='E:\Kiran\ 2020 Report';

%let codepath='E:\Kiran\ 2020 Report\Code';

%let startdate='01JAN2018'd;

%let enddate='31DEC2019'd;

%let basefile=fall2020_basefile;


%let prod=asimv.prod_Q22020;

%let input_sheet=comparator_input_sheet;




%let outpath_unq = %qsysfunc(compress(&outpath,%str(%')));


libname outf &outpath.;

libname codef &codepath.;


%let codepath_unq = %qsysfunc(compress(&codepath,%str(%')));


filename code &codepath.;


filename ROA &outpath.;

proc datasets lib=outf
 nolist kill;
quit;
run;


proc import datafile="&codepath_unq\&input_sheet"
dbms=xlsx
out = outf.comparator_inputs
replace;
sheet="comparators";
run;



proc import datafile="&codepath_unq\&input_sheet"
dbms=xlsx
out = outf.roa_vars
replace;
sheet="inputroa";
run;


data outf.comparator_inputs(drop= _comparatortemp);
set outf.comparator_inputs;
retain _comparatortemp ;
if length(comparator) ne 1  then do; _comparatortemp=comparator;  end;


else do; comparator=_comparatortemp;  end;

ind=_n_;
comparator=lowcase(comparator);
op_var=lowcase(op_var);




run;





proc sort data=outf.comparator_inputs out=outf.comparator_list_numbers(keep=comparator ind) nodupkey;
by comparator ;run;





proc sort data=outf.comparator_list_numbers;by ind;run;

data outf.comparator_list_numbers (drop=ind);
set outf.comparator_list_numbers ;
comparator_index=_n_;
run;

data codef.comparator_list_numbers;
set outf.comparator_list_numbers;
run;



proc sql;
create table comparator_inputs_temp1 as
select x.*, y.op_var as op_var3, case when y.op_var is not missing then 1 else 0 end as level2_match
from
(select a.*,b.op_var as op_var2, case when b.op_var is not missing then 1 else 0 end as level1_match
from outf.comparator_inputs a left join outf.comparator_inputs b
on a.op_var=b.comparator) x left join outf.comparator_inputs y
on x.op_var2=y.comparator

;
quit;

proc sql;
create table comparator_inputs_temp2 as
select x.*,y.op_var as op_var4, case when y.op_var is not missing then 1 else 0 end as level3_match
from comparator_inputs_temp1 x left join
outf.comparator_inputs y
on x.op_var3=y.comparator
order by x.comparator;
quit;



data outf.comparator_inputs;
set comparator_inputs_temp2;

if level3_match=1 then op_var=op_var4;

else if level2_match=1 then op_var=op_var3;
else if level1_match=1 then op_var=op_var2;
run;

/*data outf.roa_vars;*/
/*length var $ 50;*/
/*format var $50.;*/
/*set outf.roa_vars;*/
/*run;*/
/**/



proc sort data=outf.roa_vars;by var;run;


proc transpose data=outf.roa_vars out=outf.group_inputs(rename=(_name_=group var=method) where
=(col1=1) drop=_label_ )

;by var;run;

proc sort data=outf.group_inputs;by group;run;

data outf.group_inputs;
retain group method;
set outf.group_inputs (drop=col1);
run;



data outf.group_inputs (drop=_grouptemp);
set outf.group_inputs;
length drugcat $ 100;
retain _grouptemp;
if length(group) ne 1  then _grouptemp=group;  


else  group=_grouptemp;  

drugcat= cats('&drug._',method);
run;

proc sort data =outf.group_inputs;by group;run;

data outf.group_inputs;
set outf.group_inputs;

length group_string1 $ 1000  group_string2 $ 1000;

retain _group group_string1 group_string2 ;
if group=_group  then do;


group_string1= catx('=1 or ',group_string1,drugcat);

group_string2= catx('=0 or ',group_string2,drugcat);


end;

else do;
group_string1= drugcat/*cats(op_var,'&var=1 or ')*/;

group_string2= drugcat/*cats(op_var,'&var=0 or ')*/;


_group=group;

end;

drop _group;
run;


data outf.group_inputs;
set outf.group_inputs;
length  group_string_pt1a group_string_pt1b group_string_pt2a group_string_pt2b $ 500;

group_string_pt1a= 'if '||group_string1;

group_string_pt1b=cats(group_string_pt1a,'=1 then &drug._')||group|| '=1;';



group_string_pt2a= 'else if '||group_string2;

group_string_pt2b=cats(group_string_pt2a,'=0 then &drug._')||group|| '=0;';

group_string_pt3='else &drug._'||group|| '=.;';


run;



data outf.group_strings ;
set outf.group_INPUTS;
by group;
if last.group;
run;





proc sql;
create table outf.group_strings_print as

(select group, group_string_pt1b as string, 1 as order
from outf.group_strings) 

union 

(select group, group_string_pt2b as string, 2 as order
from outf.group_strings) 


union

(select group, group_string_pt3 as string, 3 as order
from outf.group_strings) ;
quit;

proc sort data= outf.group_strings_print;by group order;run;




data outf.comparator_inputs;
set outf.comparator_inputs;
op_var_use= cats(op_var,'_use');


length abuse_recode_string $1000 ;



abuse_recode_string=catx(' ','if ', op_var_use,'=1 then ',op_var_use,'=1; else if ',op_var_use,' in (0,2,7) then ',

op_var_use,'=0; else ',op_var_use,'=.;');




run;







data outf.comparator_inputs;
set outf.comparator_inputs;

length comparator_string1 $ 2000  comparator_string2 $ 2000 ;


retain _comparator comparator_string1 comparator_string2 ;
if comparator=_comparator  then do;


comparator_string1= catx('&var=1 or ',comparator_string1,op_var);

comparator_string2= catx('&var=0 or ',comparator_string2,op_var);




end;

else do;
comparator_string1= op_var/*cats(op_var,'&var=1 or ')*/;

comparator_string2= op_var/*cats(op_var,'&var=0 or ')*/;


_comparator=comparator;

end;

drop _comparator;
run;














data outf.comparator_inputs;
set outf.comparator_inputs;
length comparator_string comparator_string_pt1a comparator_string_pt1b comparator_string_pt2a comparator_string_pt2b $ 3000;

comparator_string_pt1a= 'if '||comparator_string1;

comparator_string_pt1b=cats(comparator_string_pt1a,'&var=1 then')||" "||cats(comparator,'_&route=1;');



comparator_string_pt2a= 'else if '||comparator_string2;

comparator_string_pt2b=cats(comparator_string_pt2a,'&var=0 then')||" "||cats(comparator,'_&route=0;');

comparator_string_pt3='else '||cats(comparator, '_&route=.;');


comparator_string= cats(comparator_string_pt1b,comparator_string_pt2b, comparator_string_pt3);
run;



data outf.comparator_strings ;
set outf.COMPARATOR_INPUTS;
by comparator;
if last.comparator;
run;


proc sql;
create table outf.comparator_strings_print as

(select comparator, comparator_string_pt1b as string, 1 as order
from outf.comparator_strings) 

union 

(select comparator, comparator_string_pt2b as string, 2 as order
from outf.comparator_strings) 


union

(select comparator, comparator_string_pt3 as string, 3 as order
from outf.comparator_strings) ;
quit;

proc sort data= outf.comparator_strings_print;by comparator order;run;


proc sort data= outf.comparator_inputs out=outf.recode_var_strings  nodupkey; by op_var; run;



data _null_;
  set outf.recode_var_strings;
  file "&outpath_unq\abuse_recodes.sas";
  put abuse_recode_string ;
  file print;
   run;



data _null_;
  set outf.comparator_strings_print;
  file "&outpath_unq\comparator_logic.sas";
  put string ;
  file print;
   run;




  data _null_;
  set outf.group_strings_print;
  file "&outpath_unq\group_logic.sas";
  put string ;
  file print;
   run;


proc sort data=outf.roa_vars; by roa_num;run;

data groups (rename=(group=var));
set outf.group_inputs(keep=group);
run;


proc sort data=groups nodupkey;by var; run;

data method;
set outf.roa_vars(keep=var) groups;
roa_num=_n_;
run;


proc sql;
create table comparators as 
select a.*,b.roa_type 
from outf.comparator_list_numbers a
left join outf.comparator_inputs b
on a.comparator=b.comparator;quit;

proc sort data=comparators nodupkey;by comparator_index;run;

proc sql;
create table comparator_roa as
select *
from comparators,method
;quit;

proc sort data=comparator_roa;by comparator_index roa_num;run;

data comparator_roa;
set comparator_roa;
length roa_var $ 100;
roa_var=cats(comparator,"_",var);
run;


proc sql;
	create table outf.use_vars  as
    select *
    from &prod. 
    where &startdate <= testdate <= &enddate and
          age >= 18 and 
		  rx_opioid = 'Y' ;
quit ;




proc sort data=outf.use_vars nodupkey; by uniqueidentifier testdate version age ; run;


data outf.use_vars;
set outf.use_vars;



if OP9C=1  then delete;
 if zip3 ='000' then delete ;


quarter_t  = put(testdate,yyq6.) ;

	year_t=year(testdate);

	year=year(testdate)+1-year(&startdate);

quarter=year*4+qtr(testdate) -4;

/*OP2H exception*/

/*if &startdate<=testdate<=mdy(12,31,2014) then period=1;*/
/*if mdy(7,1,2018)<=testdate<=mdy(12,31,2018) then period=2;*/

if ASID001D>0 then alcohol_any_abuse=1; else if ASID001D=0 then alcohol_any_abuse=0;  else alcohol_any_abuse=.;
if ASID002D>0 then alcohol_intox_abuse=1; else if ASID002D=0 then alcohol_intox_abuse=0;  else alcohol_intox_abuse=.;
if ASID003D>0 then heroin_abuse=1; else if ASID003D=0 then heroin_abuse=0;  else heroin_abuse=.;
if ASID004D>0 then methadonest_abuse=1; else if ASID004D=0 then methadonest_abuse=0;  else methadonest_abuse=.;
if ASID005D>0 then opioids_st_abuse=1; else if ASID005D=0 then opioids_st_abuse=0;  else opioids_st_abuse=.;
if ASID006D>0 then barbiturates_abuse=1; else if ASID006D=0 then barbiturates_abuse=0;  else barbiturates_abuse=.;
if ASID007D>0 then sedatives_abuse=1; else if ASID007D=0 then sedatives_abuse=0;  else sedatives_abuse=.;
if ASID008D>0 then cocaine_abuse=1; else if ASID008D=0 then cocaine_abuse=0;  else cocaine_abuse=.;
if ASID009D>0 then amphetamines_abuse=1; else if ASID009D=0 then amphetamines_abuse=0;  else amphetamines_abuse=.;
if ASID010D>0 then marijuana_abuse=1; else if ASID010D=0 then marijuana_abuse=0;  else marijuana_abuse=.;
if ASID011D>0 then hallucinogens_abuse=1; else if ASID011D=0 then hallucinogens_abuse=0;  else hallucinogens_abuse=.;
if ASID012D>0 then inhalents_abuse=1; else if ASID012D=0 then inhalents_abuse=0;  else inhalents_abuse=.;
if ASID_K30D>0 then ecstasy_abuse=1; else if ASID_K30D=0 then ecstasy_abuse=0;  else ecstasy_abuse=.;
if ASID_M30D>0 then ghb_abuse=1; else if ASID_M30D=0 then ghb_abuse=0;  else ghb_abuse=.;
if ASID_N30D>0 then ketamine_abuse=1; else if ASID_N30D=0 then ketamine_abuse=0;  else ketamine_abuse=.;
if ASID_O30D>0 then k2_abuse=1; else if ASID_O30D=0 then k2_abuse=0;  else k2_abuse=.;
if ASID_P30D>0 then rohypnol_abuse=1; else if ASID_P30D=0 then rohypnol_abuse=0;  else rohypnol_abuse=.;
if ASID_Q30D>0 then bathsalts_abuse=1; else if ASID_Q30D=0 then bathsalts_abuse=0;  else bathsalts_abuse=.;
if ASID_R30D>0 then otcmeds_abuse=1; else if ASID_R30D=0 then otcmeds_abuse=0;  else otcmeds_abuse=.;
if ASID_S30D>0 then otherdrugs_abuse=1; else if ASID_S30D=0 then otherdrugs_abuse=0;  else otherdrugs_abuse=.;

If ASID010R=1 then marijuana_swallow=1;
If ASID010R=2 then marijuana_snort=1;
If ASID010R=3 then marijuana_smoke=1;
If ASID010R=4 then marijuana_inject_nonIV=1;
If ASID010R=5 then marijuana_inject_IV=1;
If ASID010R=7 then marijuana_other=1;
If ASID010R=8 then marijuana_dissolveinmouth=1;
If ASID010R=9 then marijuana_drank=1;
If ASID010R=12 then marijuana_ate=1;
If ASID010R=13 then marijuana_sublingual=1;
If ASID010R=4 or ASID010R=5 then marijuana_anyinject=1;

if asid010R in (1,8,9,12,13) then marijuana_anyoral=1;

alcohol_any_swallow=.;
alcohol_any_snort=.;
alcohol_any_smoke=.;
alcohol_any_inject_nonIV=.;
alcohol_any_inject_IV=.;
alcohol_any_other=.;
alcohol_any_dissolveinmouth=.;
alcohol_any_drank=.;
alcohol_any_ate=.;
alcohol_any_sublingual=.;
alcohol_any_anyinject=.;


alcohol_intox_swallow=.;
alcohol_intox_snort=.;
alcohol_intox_smoke=.;
alcohol_intox_inject_nonIV=.;
alcohol_intox_inject_IV=.;
alcohol_intox_other=.;
alcohol_intox_dissolveinmouth=.;
alcohol_intox_drank=.;
alcohol_intox_ate=.;
alcohol_intox_sublingual=.;
alcohol_intox_anyinject=.;




inhalents_swallow=.;
inhalents_snort=.;
inhalents_smoke=.;
inhalents_inject_nonIV=.;
inhalents_inject_IV=.;
inhalents_other=.;
inhalents_dissolveinmouth=.;
inhalents_drank=.;
inhalents_ate=.;
inhalents_sublingual=.;
inhalents_anyinject=.;



if asid003r=1 then heroin_swallow=1;	if asid003r=2 then heroin_snort=1;	if asid003r=3 then heroin_smoke=1;	if asid003r=4 then heroin_inject_nonIV=1;	if asid003r=5 then heroin_inject_IV=1;
if asid004r=1 then methadonest_swallow=1;	if asid004r=2 then methadonest_snort=1;	if asid004r=3 then methadonest_smoke=1;	if asid004r=4 then methadonest_inject_nonIV=1;	if asid004r=5 then methadonest_inject_IV=1;
if asid005r=1 then opioids_st_swallow=1;	if asid005r=2 then opioids_st_snort=1;	if asid005r=3 then opioids_st_smoke=1;	if asid005r=4 then opioids_st_inject_nonIV=1;	if asid005r=5 then opioids_st_inject_IV=1;
if asid006r=1 then barbiturates_swallow=1;	if asid006r=2 then barbiturates_snort=1;	if asid006r=3 then barbiturates_smoke=1;	if asid006r=4 then barbiturates_inject_nonIV=1;	if asid006r=5 then barbiturates_inject_IV=1;
if asid007r=1 then sedatives_swallow=1;	if asid007r=2 then sedatives_snort=1;	if asid007r=3 then sedatives_smoke=1;	if asid007r=4 then sedatives_inject_nonIV=1;	if asid007r=5 then sedatives_inject_IV=1;
if asid008r=1 then cocaine_swallow=1;	if asid008r=2 then cocaine_snort=1;	if asid008r=3 then cocaine_smoke=1;	if asid008r=4 then cocaine_inject_nonIV=1;	if asid008r=5 then cocaine_inject_IV=1;
if asid009r=1 then amphetamines_swallow=1;	if asid009r=2 then amphetamines_snort=1;	if asid009r=3 then amphetamines_smoke=1;	if asid009r=4 then amphetamines_inject_nonIV=1;	if asid009r=5 then amphetamines_inject_IV=1;
if asid011r=1 then hallucinogens_swallow=1;	if asid011r=2 then hallucinogens_snort=1;	if asid011r=3 then hallucinogens_smoke=1;	if asid011r=4 then hallucinogens_inject_nonIV=1;	if asid011r=5 then hallucinogens_inject_IV=1;
if asid_kr=1 then ecstasy_swallow=1;	if asid_kr=2 then ecstasy_snort=1;	if asid_kr=3 then ecstasy_smoke=1;	if asid_kr=4 then ecstasy_inject_nonIV=1;	if asid_kr=5 then ecstasy_inject_IV=1;
if asid_mr=1 then ghb_swallow=1;	if asid_mr=2 then ghb_snort=1;	if asid_mr=3 then ghb_smoke=1;	if asid_mr=4 then ghb_inject_nonIV=1;	if asid_mr=5 then ghb_inject_IV=1;
if asid_nr=1 then ketamine_swallow=1;	if asid_nr=2 then ketamine_snort=1;	if asid_nr=3 then ketamine_smoke=1;	if asid_nr=4 then ketamine_inject_nonIV=1;	if asid_nr=5 then ketamine_inject_IV=1;
if asid_or=1 then k2_swallow=1;	if asid_or=2 then k2_snort=1;	if asid_or=3 then k2_smoke=1;	if asid_or=4 then k2_inject_nonIV=1;	if asid_or=5 then k2_inject_IV=1;
if asid_pr=1 then rohypnol_swallow=1;	if asid_pr=2 then rohypnol_snort=1;	if asid_pr=3 then rohypnol_smoke=1;	if asid_pr=4 then rohypnol_inject_nonIV=1;	if asid_pr=5 then rohypnol_inject_IV=1;
if asid_qr=1 then bathsalts_swallow=1;	if asid_qr=2 then bathsalts_snort=1;	if asid_qr=3 then bathsalts_smoke=1;	if asid_qr=4 then bathsalts_inject_nonIV=1;	if asid_qr=5 then bathsalts_inject_IV=1;
if asid_rr=1 then otcmeds_swallow=1;	if asid_rr=2 then otcmeds_snort=1;	if asid_rr=3 then otcmeds_smoke=1;	if asid_rr=4 then otcmeds_inject_nonIV=1;	if asid_rr=5 then otcmeds_inject_IV=1;
if asid_sr=1 then otherdrugs_swallow=1;	if asid_sr=2 then otherdrugs_snort=1;	if asid_sr=3 then otherdrugs_smoke=1;	if asid_sr=4 then otherdrugs_inject_nonIV=1;	if asid_sr=5 then otherdrugs_inject_IV=1;


if asid003r in (4,5) then heroin_anyinject=1;
if asid004r in (4,5) then methadonest_anyinject=1;
if asid005r in (4,5) then opioids_st_anyinject=1;
if asid006r in (4,5) then barbiturates_anyinject=1;
if asid007r in (4,5) then sedatives_anyinject=1;
if asid008r in (4,5) then cocaine_anyinject=1;
if asid009r in (4,5) then amphetamines_anyinject=1;
if asid011r in (4,5) then hallucinogens_anyinject=1;
if asid_kr in (4,5) then ecstasy_anyinject=1;
if asid_mr in (4,5) then ghb_anyinject=1;
if asid_nr in (4,5) then ketamine_anyinject=1;
if asid_or in (4,5) then k2_anyinject=1;
if asid_pr in (4,5) then rohypnol_anyinject=1;
if asid_qr in (4,5) then bathsalts_anyinject=1;
if asid_rr in (4,5) then otcmeds_anyinject=1;
if asid_sr in (4,5) then otherdrugs_anyinject=1;




heroin_dissolveinmouth=.;
methadonest_dissolveinmouth=.;
opioids_st_dissolveinmouth=.;
barbiturates_dissolveinmouth=.;
sedatives_dissolveinmouth=.;
cocaine_dissolveinmouth=.;
amphetamines_dissolveinmouth=.;

hallucinogens_dissolveinmouth=.;
inhalents_dissolveinmouth=.;
ecstasy_dissolveinmouth=.;
ghb_dissolveinmouth=.;
ketamine_dissolveinmouth=.;
k2_dissolveinmouth=.;
rohypnol_dissolveinmouth=.;
bathsalts_dissolveinmouth=.;
otcmeds_dissolveinmouth=.;
otherdrugs_dissolveinmouth=.;



heroin_drank=.;
methadonest_drank=.;
opioids_st_drank=.;
barbiturates_drank=.;
sedatives_drank=.;
cocaine_drank=.;
amphetamines_drank=.;

hallucinogens_drank=.;
inhalents_drank=.;
ecstasy_drank=.;
ghb_drank=.;
ketamine_drank=.;
k2_drank=.;
rohypnol_drank=.;
bathsalts_drank=.;
otcmeds_drank=.;
otherdrugs_drank=.;


heroin_ate=.;
methadonest_ate=.;
opioids_st_ate=.;
barbiturates_ate=.;
sedatives_ate=.;
cocaine_ate=.;
amphetamines_ate=.;

hallucinogens_ate=.;
inhalents_ate=.;
ecstasy_ate=.;
ghb_ate=.;
ketamine_ate=.;
k2_ate=.;
rohypnol_ate=.;
bathsalts_ate=.;
otcmeds_ate=.;
otherdrugs_ate=.;


heroin_sublingual=.;
methadonest_sublingual=.;
opioids_st_sublingual=.;
barbiturates_sublingual=.;
sedatives_sublingual=.;
cocaine_sublingual=.;
amphetamines_sublingual=.;

hallucinogens_sublingual=.;
inhalents_sublingual=.;
ecstasy_sublingual=.;
ghb_sublingual=.;
ketamine_sublingual=.;
k2_sublingual=.;
rohypnol_sublingual=.;
bathsalts_sublingual=.;
otcmeds_sublingual=.;
otherdrugs_sublingual=.;

heroin_other=.;
methadonest_other=.;
opioids_st_other=.;
barbiturates_other=.;
sedatives_other=.;
cocaine_other=.;
amphetamines_other=.;

hallucinogens_other=.;
inhalents_other=.;
ecstasy_other=.;
ghb_other=.;
ketamine_other=.;
k2_other=.;
rohypnol_other=.;
bathsalts_other=.;
otcmeds_other=.;
otherdrugs_other=.;


alcohol_any_anyoral=alcohol_any_swallow;
alcohol_intox_anyoral=alcohol_intox_swallow;

heroin_anyoral=heroin_swallow;
methadonest_anyoral=methadonest_swallow;
opioids_st_anyoral=opioids_st_swallow;
barbiturates_anyoral=barbiturates_swallow;
sedatives_anyoral=sedatives_swallow;
cocaine_anyoral=cocaine_swallow;
amphetamines_anyoral=amphetamines_swallow;
inhalents_anyoral=inhalents_swallow;
hallucinogens_anyoral=hallucinogens_swallow;
inhalents_anyoral=inhalents_swallow;
ecstasy_anyoral=ecstasy_swallow;
ghb_anyoral=ghb_swallow;
ketamine_anyoral=ketamine_swallow;
k2_anyoral=k2_swallow;
rohypnol_anyoral=rohypnol_swallow;
bathsalts_anyoral=bathsalts_swallow;
otcmeds_anyoral=otcmeds_swallow;
otherdrugs_anyoral=otherdrugs_swallow;




run;




/*check for generics*/

proc contents data=&prod. out=gen_check (keep=name where=(substr(name,length(name)-4,5)= 'R_gen')) noprint;

run;

data gen_check;
set gen_check;
name=lowcase(substr(name,1,length(name)-5));

run;


proc sql;
create table comparator_inputs_gen as
select a.*, case when b.name is not missing and substr(a.op_var,5,1) in ('e','f','h','i')

then 1 else 0 end as gen_ind


from outf.comparator_inputs a left join gen_check b
on substr(a.op_var,1,length(op_var)-1)=b.name;
quit;

data outf.comparator_inputs;
set comparator_inputs_gen;
run;


data outf.gen_op (keep= comparator gen_op);
set outf.comparator_inputs;
gen_op=substr(op_var,1,length(op_var)-1);

if gen_ind=1;
run;

proc sort data=outf.gen_op nodupkey;by comparator gen_op;run;



proc sort data=outf.comparator_inputs out=outf.comparator_list_numbers(keep=comparator ind) nodupkey;
by comparator ;run;


data outf.gen_op;
set outf.gen_op;
length roa_gen_string $ 400 src_gen_string $ 400;

roa_gen_string=cat('if &i>0 and ',trim(gen_op),'R_gen&i=1 then ',trim(comparator),'_&route=1;',
'if &i>0 and ',trim(gen_op),'R_gen&i=0 and ',trim(comparator),'_&route=. then ',trim(comparator),'_&route=0;');

src_gen_string=cat('if &i>0 and ',trim(gen_op),'W_gen&i=1 then ',trim(comparator),'_&route=1;',
'if &i>0 and ',trim(gen_op),'W_gen&i=0 and ',trim(comparator),'_&route=. then ',trim(comparator),'_&route=0;');




run;


data _null_;
  set outf.gen_op;
  file "&outpath_unq\roa_gen_string.sas";
  put roa_gen_string ;
  file print;
  /*put abuse_recode_string ;*/run;

data _null_;
  set outf.gen_op;
  file "&outpath_unq\src_gen_string.sas";
  put src_gen_string ;
  file print;
  /*put abuse_recode_string ;*/run;




%let rte0=abuse;





data _null_;
set outf.comparator_strings;
call execute('%let drg'||cat(_n_)||"="||comparator||";");
run;

proc sql;
select count(*) into :comp_count
from outf.comparator_strings;
quit;


proc sql;
select count(*) into :roa_count
from outf.roa_vars;
quit;


%let rte0=abuse;


data _null_;
set outf.roa_vars;
call execute('%let rte'||cat(_n_)||"="||var||";");
run;




%macro exception_op2h();


data outf.use_vars;
set outf.use_vars;
%do i=1 %to &&roa_count;

if OP2HR&i ne . then do;

if (OP2HR&i=0 and testdate >= '14mar2015'd)  then OP2HSER&i=0;

else  if  (OP2HR&i=1 and testdate >= '14mar2015'd) then OP2HSER&i=1;

else OP2HSER&i=.;

end;

if OP18AR&i ne . then do;

if (OP18AR&i=0 and testdate > ='14mar2015'd)  then OP18AADFR&i=0;

else  if  (OP18AR&i=1 and testdate >= '14mar2015'd) then OP18AADFR&i=1;

else OP18ASER&i=.;



end;
%end;



run;
%mend;
%exception_op2h;




%macro createvars();


data  outf.use_vars;
set outf.use_vars ; 
%include ROA(abuse_recodes);

if op2h_use ne . then do;
 if  (OP2H_Use=1 and testdate >= '14mar2015'd) then OP2HSE_use=1;
else if (OP2H_Use=0 and testdate >= '14mar2015'd)  then OP2HSE_use=0;

else op2hse_use=.;
end;


if op18a_use ne . then do;
 if  (op18a_Use=1 and testdate >= '01JUL2015'd) then op18aadf_use=1;
else if (op18a_Use=0 and testdate >= '01JUL2015'd)  then op18aadf_use=0;

else op18aadf_use=.;
end;


%do i=0 %to &&roa_count;
%if &i=0 %then %let var=_use;


%else %let var=%sysfunc(catt(R,&i.));
%let route=&&rte&i ;
/**make sure to recode abuse variable exceptions*/



%include ROA(comparator_logic);

/*generic abuses*/
%include ROA(roa_gen_string);



%end;



%do i=1 %to &&comp_count;

%let drug=&&drg&i ;

%include ROA(group_logic);

%end;




run;

%mend createvars;

%createvars;





%include code(basefile_custom_formats);


/* produce basefile*/






data outf.use_vars;
set outf.use_vars;

%include code(basefile_custom_vars);





year=year(testdate)-(year(&startdate)-1);

quarter=year*4-(4-qtr(testdate));

if op_illicit=1 then op_illicit_use=1;else op_illicit_use=0;


length key_new  $ 300;
key_new=catt(UniqueIDentifier,TestDate,Age,Version);


format agecat agecat. quarter qtr_fmt. year year_fmt.  
race race.   gender gender. modality_new modalityf.
region region.

;

run;
/* */
/*proc sql;*/
/*select roa_var into :drop_roa_all_list separated by ' '*/
/*from comparator_roa*/
/*where roa_type=3;quit;*/
/**/
/**/
/*proc sql;*/
/*select roa_var into :drop_roa_some_list separated by ' '*/
/*from comparator_roa*/
/*where roa_type=1 and 10<=roa_num<=14;quit;*/



/*create first basefile*/
proc sort data=outf.use_vars/*(drop= &drop_roa_all_list  &drop_roa_some_list)*/ out=codef.&basefile.;
by  testdate;
run;
