/******************************************************** 

	Program Name - MethadoneAny_abuse.sas

	Description - Create MethadoneAny_abuse
	

*********************************************************/

MethadoneAny_abuse=.;

if	(OP4A_Use=0 | OP4B_Use=0 | 
	 OP4D_Use=0 | OP4E_Use=0) then MethadoneAny_abuse=0; 

if	(OP4A_Use=1 | OP4B_Use=1 | 
	 OP4D_Use=1 | OP4E_Use=1) then MethadoneAny_abuse=1; 
