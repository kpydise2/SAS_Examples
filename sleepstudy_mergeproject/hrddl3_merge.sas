/*Kiran Pydisetty*/
/*7/13/23*/

/*Description: Will process all input files and perform HRDDL 3 merge*/





%let root=\\quintiles.net\enterprise\Apps\sasdata\SASb\SAS\SAS_DEV\WashU\test_path\HRDDL3;


libname tst "&root";

filename job "&root";


%macro importdat(dat=,import=);

%let filepath="&root\Mock HRDDL3 &import..xlsx";
/*%put &filepath;*/


PROC IMPORT DATAFILE=&filepath      
          OUT=&dat._import DBMS=xlsx REPLACE ;
          GETNAMES = yes  ;
        RUN;

%mend;
%importdat(import=Hourly,dat=EMA);
%importdat(import=Final,dat=Final);

%importdat(import=BOD,dat=BOD);

%importdat(import=Sleep,dat=Sleep);

%importdat(import=Baseline,dat=Baseline);




%macro rename(indat=,prefix=);

proc contents data=&indat out=varout noprint;
run;

data varout;
format name $50.;
set varout;
run;



%let prefixq="&prefix";

data varout;

set varout;
name_new=cats(&prefixq,"_",name);
length str $ 100;

str=cats(name,"=",name_new);

run;

proc sql noprint;
select str into :rename
separated by " "
from varout
where name ne "PID"
;quit;

data &indat(rename=(&rename));
set &indat;
run;


%mend;



%importdat(import=Hourly,dat=EMA);
%importdat(import=Final,dat=Final);

%importdat(import=BOD,dat=BOD);

%importdat(import=Sleep,dat=Sleep);

%importdat(import=Baseline,dat=Baseline);


%rename(indat=ema_import,prefix=ema);

%rename(indat=final_import,prefix=final);

%rename(indat=bod_import,prefix=bod);

%rename(indat=sleep_import,prefix=sleep);


%rename(indat=baseline_import,prefix=baseline);




%macro importbp;


%do i=1 %to 1000;

%let dir="&root\bp\Mock BP File PID &i..csv";



%if %sysfunc(fileexist(&dir)) %then %do;
PROC IMPORT DATAFILE=&dir        /* Import Edited BP data */
          OUT=bp_get_&i(Drop=var:) REPLACE ;
          GETNAMES = yes  ;
        RUN;
%end;

%end;
%mend;


%importbp;



data bp_import;
set bp_get_:;
run;

%rename(indat=bp_import,prefix=bp);




/*fix macro to rename vars*/




data bp_import;
set bp_import;

date_bp=mdy(bp_month,bp_day,bp_year);





run;


data bp_import(drop=pid_temp);

set bp_import;

retain pid_temp row_id_bp;

if pid=pid_temp then
row_id_bp=row_id_bp+1;

else do;
row_id_bp=1;
pid_temp=pid;
end;
run;





data ema_import(drop=pid_temp);

set ema_import;

retain pid_temp row_id_ema;

if pid=pid_temp then
row_id_ema=row_id_ema+1;

else do;
row_id_ema=1;
pid_temp=pid;
end;
run;




/*get list of bp patients*/

proc sort nodupkey data= bp_import out=bp_pids(keep= pid);by pid;run;


/*get list of ema pids*/

proc sort nodupkey data= ema_import out=ema_pids(keep= pid);by pid;run;


proc sql;
create table bp_only_pids as 
select a.* from
bp_pids a left join ema_pids b
on a.pid=b.pid
where b.pid is missing;quit;




proc sql;
create table ema_only_pids as 
select a.* from
ema_pids a left join bp_pids b
on a.pid=b.pid
where b.pid is missing;quit;



proc sql;
create table matching_pids as 
select a.* from
ema_pids a inner join bp_pids b
on a.pid=b.pid
;quit;




data ema_import;
set ema_import;
ema_hour=hour(ema_startdate);
date_ema=datepart(ema_startdate);
run;









/*clean sleep data*/

data sleep_dat(drop=str_: date_n_: sleep_inbeddate sleep_inbedtime);
set sleep_import(drop=sleep_sleepalgorithm);

length str_month str_day $ 20;

find1=find(Sleep_InBedDate,"/",1);

str_month=substr(sleep_inbeddate,1,find1-1);

date_n_month=input(str_month,best7.);


find2=find(Sleep_InBedDate,"/",find1+1);




str_day=substr(sleep_inbeddate,find1+1,find2-find1-1);

date_n_day=input(str_day,best7.);

str_year=substr(sleep_inbeddate,find2+1,length(sleep_inbeddate)-find2+1);


date_n_year=input(str_year,best7.);




newsleepdate=mdy(date_n_month,date_n_day,date_n_year);


time_new=input(trim(Sleep_InBedTime),time7.);




run ;



data sleep_dat(Drop=newsleepdate find1 find2 time_new);
set sleep_dat;
sleep_inbeddate=newsleepdate;
sleep_inbedtime=time_new;
format sleep_inbeddate date9.;

format sleep_inbedtime time6.;





run;

data sleep_dat;
retain pid sleep_inbeddate
Sleep_InBedTime sleep_totalsleeptimetst;
set sleep_dat;

sleep_datetime=dhms(sleep_inbeddate,hour(sleep_inbedtime),minute(sleep_inbedtime),second(sleep_inbedtime));

format sleep_datetime datetime.;
run;


proc sql;
create table sleep_cnt as
select pid,count(*) as sleep_cnt
from sleep_dat
group by pid;quit;



proc sql;
create table sleep_dat2 as

select *
from sleep_dat a left join sleep_cnt b
on a.pid=b.pid;quit;



proc sort data=sleep_dat2;
by pid descending Sleep_datetime;run;


data sleep_dat2;
set sleep_dat2;
by pid;
pid_lag=lag(pid);
run;






data sleep_dat3(drop=pid_lag sleep_cnt);
set sleep_dat2;
by pid;
if first.pid=0 and sleep_cnt>1 and pid=pid_lag then do;


lag_sleep_inbeddate=lag(sleep_inbeddate);

lag_sleep_inbedtime=lag(sleep_inbedtime);

lag_Sleep_TotalSleepTimeTSt=lag(Sleep_TotalSleepTimeTST);
end;

format lag_sleep_inbeddate date9.;
format lag_sleep_inbedtime time6.;


run;

data sleep_dat3;
set sleep_dat3;
sleep_match_day=sleep_inbeddate;



run;


data sleep_dat4;
set sleep_dat3;

if hour(sleep_inbedtime)<9 ;



sleep_match_day=sleep_inbeddate-1;


run;



data sleep_dat5;

set sleep_dat3(in=a) sleep_dat4;

sleep_outbedtime=intnx('minute',sleep_datetime,sleep_totalsleeptimetst);

format sleep_outbedtime datetime.;

run;




proc sort data=sleep_dat5;
by pid  Sleep_datetime;run;



data bp_rows(keep=pid row_id_bp bp_datetime date_bp);

set bp_import ;


bp_datetime=dhms(date_bp,bp_hour,bp_minute,0);

run;


proc sql;
create table sleep_bp_1 as
select * from 
sleep_dat5 a left join bp_rows  b
on a.pid=b.pid and a.sleep_match_day=b.date_bp
order by a.pid,a.sleep_datetime
;quit;

data sleep_bp_1;
set sleep_bp_1;
if sleep_datetime<bp_datetime<sleep_outbedtime then bp_taken_asleep=1;
else bp_taken_asleep=0;

run;


proc sort data=sleep_bp_1;
by  pid row_id_bp descending bp_taken_asleep;
run;


proc sort data=sleep_bp_1 out=sleep_bp_2 nodupkey;
by pid row_id_bp;run;


data sleep_final;
set sleep_bp_2(drop= sleep_match_day date_bp bp_datetime );
run;




%macro only(type=,pid=);

data dat;
set &type._import;
if pid=&pid;
hour_taken=dhms(date_&type,&type._hour,0,0);
format hour_taken datetime.;




run;

data dat;
set dat;
retain   hour_id 0 temp_hour;
if temp_hour ^= hour_taken
then do;

hour_id=hour_id+1;
temp_hour=hour_taken;
end;

else temp_hour=hour_taken;
run;

proc sort data=dat out=lags nodupkey;by hour_id;run;


data dat;
set dat;
next_hour_id=hour_id+1;
run;

data lags;
set lags;
run;





%rename(indat=lags,prefix=lag);




data lags;
set lags;
run;






proc sql;
create table &type._onlypid_&pid as
select * from
dat a left join lags b
on a.next_hour_id=b.lag_hour_id
order by a.row_id_&type;quit;

proc datasets lib=work;delete dat lags;run;


%mend;


data only_pids;
set bp_only_pids(in=a) ema_only_pids;
length type $ 20;
if a then type="bp";else type="ema";
length callstr $ 100;
callstr=cats("%","only(type=",type,",pid=",pid,");");

run;




data _null_;
  set only_pids;
  file "&root\macrocalls1.sas";
  put callstr;
  file print;
 ;run;



 %include job(macrocalls1.sas);





/*%macro matching(pid=);*/


%macro matching(pid=);
data bp_dat;
set bp_import;
if pid=&pid;

next_hr_dttime=dhms(date_bp,bp_hour,bp_minute,0)+60*60;

next_day=datepart(next_hr_dttime);
next_hour=hour(next_hr_dttime);

run;

data ema;
set ema_import;


if PID=&pid;



t_hour_ema=hour(ema_startdate);
t_minute_ema=minute(ema_startdate);
/*t_minutes_tot_ema=60*t_hour_ema + t_minute_ema;*/
t_date_ema=datepart(ema_startdate);

run;



proc sql;
create table merge_dat_samehr as
select *, 1 as same_hour_ema

from bp_dat a
inner join ema b
on a.bp_hour=b.t_hour_ema and date_bp=t_date_ema
order by a.row_id_bp
;quit;



data merge_dat_samehr;
set merge_dat_samehr;

time_diff_bp_ema=t_minute_ema-bp_minute;

if time_diff_bp_ema>=0 then ema_after=1;else ema_after=0;
run;


/*Part1 EMA on or after BP reading in same hour*/

data matchedsamehr_ema_part1_all;
set merge_dat_samehr;
if ema_after=1;
run;

/*only keep soonest taken EMA on or after BP*/

proc sort data=matchedsamehr_ema_part1_all ;
by row_id_bp  time_diff_bp_ema;
run;


proc sort data=matchedsamehr_ema_part1_all out= matchedsamehr_ema_part1 nodupkey;
by row_id_bp;
run;




/*match EMA taken soonest before BP*/

data matchedsamehr_ema_part2_all;
set merge_dat_samehr;
if ema_after=0;
run;


proc sort data=matchedsamehr_ema_part2_all ;
by row_id_bp descending time_diff_bp_ema;
run;

proc sort data=matchedsamehr_ema_part2_all out= matchedsamehr_ema_part2 nodupkey;
by row_id_bp;
run;


data matchedsamehr_ema;
set matchedsamehr_ema_part1 matchedsamehr_ema_part2;
run;



proc sort data=matchedsamehr_ema;by row_id_bp descending ema_after;run;



proc sort data=matchedsamehr_ema out=matchedsamehr_ema_final nodupkey
;by row_id_bp ;run;


/*match next hr*/


proc sql;
create table merge_dat_nexthr as
select *, 0 as same_hour_ema

from bp_dat a
inner join ema b
on a.next_hour=b.t_hour_ema and a.next_day=b.t_date_ema
order by a.row_id_bp
;quit;



data merge_dat_nexthr;
set merge_dat_nexthr;

time_diff_bp_ema=(60-bp_minute)+t_minute_ema;

if time_diff_bp_ema<50;
run;


proc sort data=merge_dat_nexthr;by row_id_bp time_diff_bp_ema;run;


proc sort data=merge_dat_nexthr out=matchednexthr_ema_final nodupkey;
by row_id_bp;run;


data matched_samehr_nexthr;
set matchedsamehr_ema_final matchednexthr_ema_final;

run;


proc sort data=matched_samehr_nexthr;
by row_id_bp descending same_hour_ema;
run;



proc sort data=matched_samehr_nexthr out=matched_bp_final nodupkey;
by row_id_bp;run;


proc sql;
create table unmatched_bp as

select a.* from 
bp_dat a left join matched_bp_final b on
a.row_id_bp=b.row_id_bp

where b.row_id_bp is missing
;quit;



data bp_matched_unmatched_part1;
set matched_bp_final(in=a)  unmatched_bp;
if a then matched_bp=1; else matched_bp=0;

hour_bp_taken=dhms(date_bp,bp_hour,0,0);
format hour_bp_taken datetime.;

run;


proc sort data=bp_matched_unmatched_part1;
by hour_bp_taken descending matched_bp descending row_id_bp;
run;

data bp_matched_unmatched_part2;
set bp_matched_unmatched_part1;
retain   hour_id 0 temp_hour;
if temp_hour ^= hour_bp_taken
then do;

hour_id=hour_id+1;
temp_hour=hour_bp_taken;
end;

else temp_hour=hour_bp_taken;
run;


data bp_matched_unmatched_part2;
set  bp_matched_unmatched_part2;

next_hour_id=hour_id+1;
run;



proc sort data=bp_matched_unmatched_part2 out=bp_lags nodupkey;
by hour_id;run;



%rename(indat=bp_lags,prefix=lag);


data bp_lags;
set bp_lags(keep=lag_bp_: lag_hour_id);
run;



proc sql;
create table bp_matched_unmatched_part3 as
select * 
from bp_matched_unmatched_part2 a
left join bp_lags b on
a.next_hour_id=b.lag_hour_id;quit;





proc sql;
create table unmatched_ema as

select a.* from 
ema a left join matched_bp_final b on
a.row_id_ema=b.row_id_ema

where b.row_id_ema is missing

order by row_id_ema
;quit;


data bp_ema_1;
set  bp_matched_unmatched_part3(in=a) unmatched_ema;
if a then bp_row_ind=1;
else bp_row_ind=0;

if matched_bp =. then matched_bp=0;
run;



data ema_dat;
set bp_ema_1(keep=matched_bp ema_StartDate
ema_Progress
ema_Duration__in_seconds_
ema_Posture
row_id_ema  );
if row_id_ema ^= .;

dt_ema=datepart(ema_startdate);
hour_ema_taken=dhms(dt_ema,hour(ema_startdate),0,0);
format hour_ema_taken datetime.;




run;

proc sort data=ema_dat;by row_id_ema;run;





proc sort data=ema_dat;
by hour_ema_taken descending matched_bp descending row_id_ema;
run;

data ema_dat2;
set ema_dat;
retain   hour_id_ema 0 temp_hour;
if temp_hour ^= hour_ema_taken
then do;

hour_id_ema=hour_id_ema+1;
temp_hour=hour_ema_taken;
end;

else temp_hour=hour_ema_taken;
run;


data ema_dat3
;
set  ema_dat2;

next_hour_id_ema=hour_id_ema+1;

run;




proc sort data=ema_dat3 out=ema_lags nodupkey;
by hour_id_ema ;run;




%rename(indat=ema_lags,prefix=lag);



proc sql;
create table ema_lags2 as 
select a.row_id_ema, b.*
from ema_dat3 a 
left join ema_lags b
on a.next_hour_id_ema=b.lag_hour_id_ema
order by row_id_ema
;quit;



proc sql;
create table bp_ema_2 as 
select * from 
bp_ema_1 a left join ema_lags2 b 
on a.row_id_ema=b.row_id_ema;quit; 





proc sort data= bp_ema_2;
by descending bp_row_ind row_id_bp;run;



data bp_ema_fin_&pid;
set bp_ema_2;
format date_bp date9.;


run;



%mend;


data matching_pids;
set matching_pids;
length callstr $ 100;
callstr=cats("%","matching(pid=",pid,");");

run;


data _null_;
  set matching_pids;
  file "&root\macrocalls2.sas";
  put callstr;
  file print;
 ;run;



 %include job(macrocalls2.sas);


data bp_ema_master(drop=hour_taken
hour_id
temp_hour
lag_date_bp
lag_row_id_bp
lag_hour_taken
lag_hour_id
lag_temp_hour
lag_row_id_ema
lag_ema_hour
lag_date_ema
next_hr_dttime
next_day
next_hour
t_hour_ema
t_minute_ema
t_date_ema
hour_bp_taken
lag_matched_bp
lag_dt_ema
lag_hour_ema_taken
lag_hour_id_ema
lag_next_hour_id_ema
);
set bp_onlypid_: ema_onlypid_: bp_ema_fin_:;
run;





data bp_ema_master(drop=datetime_bp_:);
set bp_ema_master;
datetime_bp_1 = dhms(mdy(bp_month,bp_day,bp_year),bp_hour,bp_minute,0);

datetime_bp_2 = dhms(mdy(lag_bp_month,lag_bp_day,lag_bp_year),lag_bp_hour,lag_bp_minute,0);

time_diff_bp_lag=intck('min',datetime_bp_1,datetime_bp_2);


time_diff_ema_lag=intck('min',ema_startdate,lag_ema_startdate);



run;

proc sql;
create table bp_ema_sleep as
select * from
bp_ema_master a
left join sleep_final b
on a.pid=b.pid and a.row_id_bp=b.row_id_bp;quit;


data bod_dat;
set bod_import;
pid_n=input(pid,best7.);
run;



data bp_ema_sleep2;
set bp_ema_sleep;

length bod_match_type_temp $ 20 ;

if date_bp <> . then 
do;
bod_match_date=date_bp;
bod_match_type_temp="bp";
end;

else if ema_startdate <> . then do;


bod_match_date=datepart(ema_startdate);
bod_match_type_temp="ema";
end;

run;






/*create bod match date*/

proc sql;
create table bp_ema_sleep_bod(drop=pid_n bod_match_date) as
select *


from
bp_ema_sleep2 a left join 
bod_dat b 
on a.pid=b.pid_n and a.bod_match_date=datepart(b.bod_startdate);quit;


data bp_ema_sleep_bod2(drop=bod_match_type_temp);
set bp_ema_sleep_bod;
length bod_match_type $ 20;

if bod_startdate <>.  then
do;
bod_match_type= bod_match_type_temp;
bod_match=1;
end;
else bod_match=0;
run;





proc sql;
create table tst.HRDDL3_merge_final as
select * from

bp_ema_sleep_bod2 a left join
baseline_import b
on a.pid=b.pid;quit;


proc sort data=tst.HRDDL3_merge_final ;
by pid descending matched_bp row_id_bp row_id_ema;run;







proc export data=tst.HRDDL3_merge_final
outfile=
"&root\HRDDL3_merge_final.xlsx"
dbms=xlsx replace;
run;





