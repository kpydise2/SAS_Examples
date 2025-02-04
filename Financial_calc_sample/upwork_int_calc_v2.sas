/*Kiran Pydisetty*/
/*6/30/24*/



/*set input file location and excel output*/

%let input=\\quintiles.net\enterprise\Apps\sasdata\SASb\SAS\SAS_DEV\WashU\test_path\Sample_Trans_in.xlsx;
%let output=\\quintiles.net\enterprise\Apps\sasdata\SASb\SAS\SAS_DEV\WashU\test_path\Sample_Trans_out.xlsx;


/*define US bank holiday list*/
data holidaylist;
length holiday $ 32;
input holiday $;
infile datalines dsd;
datalines;
CHRISTMAS
COLUMBUS
MEMORIAL
JUNETEENTH
LABOR
MEMORIAL
THANKSGIVING
USINDEPENDENCE
USPRESIDENTS
VETERANS
NEWYEAR
THANKSGIVINGCANADA
;
run;

/*put list into macro variable for holiday check later*/

proc sql;
select quote(trim(holiday)) into :holiday separated by ','
from holidaylist;quit;

%put &holiday;


/*import source data*/
PROC IMPORT DATAFILE="&input"      
          OUT=indat DBMS=xlsx REPLACE ;
          GETNAMES = yes  ;
        RUN;

		%let d=7;


		/*gett current balance based on previous balance*/
data indat;
set indat;
retain account_temp curr_bal;
if account_temp=account then 
curr_bal=curr_bal-transaction;
else do;

curr_bal=new_bal-transaction;

account_temp=account;
end;
run;

data indat2;
set indat;



/*increment one weekday and check for holidays and juneetenth. if match increment one more weekday*/

date_temp=intnx('weekday',start_dt,1);
format date_temp date9.;
if holidayname(date_temp) in (&holiday) or (month(date_temp)=6 and day(date_temp)=19) or (month(date_temp)=6 and day(date_temp)=21 and year(date_temp)=2027) or (month(date_temp)=6 and day(date_temp)=19) or (month(date_temp)=6 and day(date_temp)=21 and year(date_temp)=2032) or (month(date_temp)=6 and day(date_temp)=20 and year(date_temp)=2033) then 
date_temp=intnx('weekday',date_temp,1);


/*increment one weekday and check for holidays and juneetenth. if match increment one more weekday*/

date_temp2=intnx('weekday',date_temp,1);

if holidayname(date_temp2) in (&holiday) or (month(date_temp2)=6 and day(date_temp2)=19) or (month(date_temp)=6 and day(date_temp)=21 and year(date_temp)=2027) or (month(date_temp)=6 and day(date_temp)=21 and year(date_temp)=2032) or (month(date_temp)=6 and day(date_temp)=20 and year(date_temp)=2033) then 
date_temp2=intnx('weekday',date_temp2,1);
format date_temp2 date9.;

due_date=date_temp2;
format due_date mmddyy10.;

/*calculate daily rate for first row of account*/

dly_rate_temp=new_bal*int_rate/365;


		p = 10**&d;
   dly_rate = int(dly_rate_temp*p)/p;



run;

%let f=2;

data indat3;
set indat2;

by account;

lag_curr=lag(curr_bal);

/*get current daily rate and use previous balance for 2nd or following row*/

if first.account then
curr_dly_rate_temp=curr_bal*int_rate/365;
else curr_dly_rate_temp=(lag_curr*int_rate)/365;

	p = 10**&d;
   curr_dly_rate = int(curr_dly_rate_temp*p)/p;

   
/*use current daily rate for 2nd or follwoing row*/


if dly_rate=. then dly_rate=curr_dly_rate;

if dly_rate_temp=. then dly_rate_temp=curr_dly_rate_temp;
run;
data indat4;
set indat3;

/*calc accrued int and diff int*/
accrued_int_temp=dly_rate_temp*Days_Between_Trans;
	p = 10**&f;
   accrued_int = int(accrued_int_temp*p)/p;


accrued_int = int(accrued_int_temp*p)/p;


	p = 10**&d;

diff_int=act_int-accrued_int_temp;


new_bal=curr_bal;
diff_bal=act_bal-new_bal;

run;


/*final dat*/
data final_dat;
retain Account	Start_DT	Due_Date	New_Bal	Transaction	Days_Between_Trans	Int_Rate	Act_Int	Act_Bal	Dly_Rate	Accrued_Int	Diff_Int	Diff_Bal;


set indat4(keep=Account	Start_DT	Due_Date	New_Bal	Transaction	Days_Between_Trans	Int_Rate	Act_Int	Act_Bal	Dly_Rate	Accrued_Int	Diff_Int	Diff_Bal);
run;




proc export data=final_dat
outfile=
"&output"
dbms=xlsx replace;
run;





