/******************************************************** 

	Program Name - MethadoneAny_roa.sas



*********************************************************/

MethadoneAny_swallow=.;
MethadoneAny_snort=.;
MethadoneAny_smoke=.;
MethadoneAny_injectNonIV=.;
MethadoneAny_injectIV=.;
MethadoneAny_chew=.;
MethadoneAny_other=.;
MethadoneAny_dissolve=.;
MethadoneAny_drank=.;
MethadoneAny_patchonskin=.;
MethadoneAny_suckpatch=.;
MethadoneAny_chewpatch=.;
MethadoneAny_dissolvetongue=.;
MethadoneAny_inject=.;
MethadoneAny_intended=.;
MethadoneAny_alternate=.;
MethadoneAny_anyoral=.;

/*Swallow whole: R1*/
if	(OP4AR1=0 | OP4BR1=0 | OP4DR1=0 | OP4ER1=0) then MethadoneAny_swallow=0;
if	(OP4AR1=1 | OP4BR1=1 | OP4DR1=1 | OP4ER1=1) then MethadoneAny_swallow=1;


/*Snort: R2 */
if	(OP4AR2=0 | OP4BR2=0 | OP4DR2=0 | OP4ER2=0) then MethadoneAny_snort=0;
if	(OP4AR2=1 | OP4BR2=1 | OP4DR2=1 | OP4ER2=1) then MethadoneAny_snort=1;


/*Smoke: R3 */
if	(OP4AR3=0 | OP4BR3=0 | OP4DR3=0 | OP4ER3=0) then MethadoneAny_smoke=0;
if	(OP4AR3=1 | OP4BR3=1 | OP4DR3=1 | OP4ER3=1) then MethadoneAny_smoke=1;


/*NonIV inject: R4 */
if	(OP4AR4=0 | OP4BR4=0 | OP4DR4=0 | OP4ER4=0) then MethadoneAny_injectNonIV=0;
if	(OP4AR4=1 | OP4BR4=1 | OP4DR4=1 | OP4ER4=1) then MethadoneAny_injectNonIV=1;


/*IV inject: R5 */
if	(OP4AR5=0 | OP4BR5=0 | OP4DR5=0 | OP4ER5=0) then MethadoneAny_injectIV=0;
if	(OP4AR5=1 | OP4BR5=1 | OP4DR5=1 | OP4ER5=1) then MethadoneAny_injectIV=1;


/*Chew: R6*/
if	(OP4AR6=0 | OP4BR6=0 | OP4DR6=0 | OP4ER6=0) then MethadoneAny_chew=0;
if	(OP4AR6=1 | OP4BR6=1 | OP4DR6=1 | OP4ER6=1) then MethadoneAny_chew=1;


/*Other: R7*/
if	(OP4AR7=0 | OP4BR7=0 | OP4DR7=0 | OP4ER7=0) then MethadoneAny_other=0;
if	(OP4AR7=1 | OP4BR7=1 | OP4DR7=1 | OP4ER7=1) then MethadoneAny_other=1;

 
/*Dissolve: R8*/
if	(OP4AR8=0 | OP4BR8=0 | OP4DR8=0 | OP4ER8=0) then MethadoneAny_dissolve=0;
if	(OP4AR8=1 | OP4BR8=1 | OP4DR8=1 | OP4ER8=1) then MethadoneAny_dissolve=1;


/*Drank: R9*/
if	(OP4AR9=0 | OP4BR9=0 | OP4DR9=0 | OP4ER9=0) then MethadoneAny_drank=0;
if	(OP4AR9=1 | OP4BR9=1 | OP4DR9=1 | OP4ER9=1) then MethadoneAny_drank=1;

 
/*Patch on skin: R10*/


/*Sucked on patch: R11*/


/*Chewed patch: R12*/


/*Dissolved on tongue: R13*/

 
/*Any injection: R4, R5 */
if	(MethadoneAny_injectNonIV=0 | MethadoneAny_injectIV=0) then MethadoneAny_inject=0;

if	(MethadoneAny_injectNonIV=1 | MethadoneAny_injectIV=1) then MethadoneAny_inject=1;

/*Intended route*/



/*Alternate route: any route other than the intended route*/



/*Any oral: R1, R6, R8, R9 */
if (MethadoneAny_swallow=0 | MethadoneAny_chew=0 | 
	MethadoneAny_dissolve=0 | MethadoneAny_drank=0) then MethadoneAny_anyoral=0;
	
if (MethadoneAny_swallow=1 | MethadoneAny_chew=1 | 
	MethadoneAny_dissolve=1 | MethadoneAny_drank=1) then MethadoneAny_anyoral=1;
