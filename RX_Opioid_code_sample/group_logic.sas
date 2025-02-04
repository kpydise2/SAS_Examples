if &drug._inject_IV=1 or &drug._inject_nonIV=1 then &drug._anyinject=1;
else if &drug._inject_IV=0 or &drug._inject_nonIV=0 then &drug._anyinject=0;
else &drug._anyinject=.;
if &drug._buccal=1 or &drug._chew=1 or &drug._chewpatch=1 or &drug._dissolve=1 or &drug._drank=1 or &drug._suckpatch=1 or &drug._swallow=1 or &drug._tngdiss=1 then &drug._anyoral  =1;
else if &drug._buccal=0 or &drug._chew=0 or &drug._chewpatch=0 or &drug._dissolve=0 or &drug._drank=0 or &drug._suckpatch=0 or &drug._swallow=0 or &drug._tngdiss=0 then &drug._anyoral  =0;
else &drug._anyoral  =.;
