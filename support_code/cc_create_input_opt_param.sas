*------------------------------------------------------------------------------*
| Program: cc_create_input_opt_param
|
| Description: 
|
*------------------------------------------------------------------------------* ;
%macro cc_create_input_opt_param(inlib=cc
                   ,outlib=cc
                   ,input_opt_parameters=input_opt_parameters
				   ,input_opt_parameters_multi=input_opt_parameters_multi
				   ,_numsce = 2
				   ,_test_adj = 100
                   ,_worklib=casuser
                   ,_debug=1
                   );
				   
	/*************************/
   /******HOUSEKEEPING*******/
   /*************************/

   /* Do not proceed if previously there have been errors */
   %if &syscc > 4  %then %do;
      %put FATAL: There have been errors BEFORE this macro is executed, exiting from &sysmacroname.;
      %goto EXIT;
   %end;
   %put TRACE: Entering &sysmacroname. with SYSCC=&SYSCC.;

   /* Check missing inputs */
   %if %sysfunc(exist(&inlib..&input_opt_parameters.))=0 %then %do;
      %put FATAL: Missing &inlib..&input_opt_parameters., exiting from &sysmacroname.;
      %goto EXIT;
   %end;     
   
    /* List work tables */
   %let _work_tables=%str(  
				&_worklib.._tmp_input_opt_parameters    
				&_worklib..append_to_opt_parameters;
              );
      
   /* List output tables */
   %let output_tables=%str(         
             &outlib..&input_opt_parameters_multi
             );
			 
	/*Delete output data if already exists */
   %let i = 1;
   %let table = %scan(&output_tables, &i, ' ');
   %do %while (&table ne %str());
      %if %sysfunc(exist(&table)) %then %do;
         proc delete data= &table.;
         run;
      %end;
      %let i = %eval(&i + 1);
      %let table = %scan(&output_tables, &i, ' ');
   %end;

   /* Delete work data if already exists */
   %let i = 1;
   %let table = %scan(&_work_tables, &i, ' ');
   %do %while (&table ne %str());
      %if %sysfunc(exist(&table)) %then %do;
         proc delete data= &table.;
         run;
      %end;
      %let i = %eval(&i + 1);
      %let table = %scan(&_work_tables, &i, ' ');
   %end;	

   /************************************/
   /************ANALYTICS *************/
   /***********************************/
data &_worklib.._tmp_input_opt_parameters;
	set &inlib..input_opt_parameters;
	/*Scenario 1*/	
	Scenario_name = 'Scenario_1'; 
	output;	
	%do i = 2 %to &_numsce;
		Scenario_name = catx('_','Scenario',&i);
		if (parm_name =: 'RAPID_TESTS_PHASE' OR parm_name =: 'NOT_RAPID_TESTS_PHASE') then parm_value = (parm_value + (&_test_adj*&i));
		output;
	%end;	
drop i;
run;

/* Adding ALREADY_OPEN, ALLOW_OPENING_ONLY_ON_PHASE, SECONDARY_OBJECTIVE_TOLERANCE, MIN_DEMAND_RATIO, EMER_SURGICAL_PTS_RATIO*/
data &_worklib..append_to_opt_parameters;
   set &_worklib.._tmp_input_opt_parameters (obs=1 keep=facility service_line sub_service parm_name parm_value Scenario_name);

   /* Force some services to be already open */
   Scenario_name = 'Scenario_1';
   parm_name='ALREADY_OPEN';
   parm_value='YES';
   
   facility='Akron'; service_line='ENT'; sub_service='ALL'; output;
   facility='Avon'; service_line='ALL'; sub_service='ALL'; output;
   facility='Cleveland Clinic'; service_line='Cosmetic Procedures'; sub_service='Fat Removal'; output;
   facility='ALL'; service_line='Lab'; sub_service='ALL'; output;
   
   	/* Keeping it constant across scenarios - Change ALLOW_OPENING_ONLY_ON_PHASE and SECONDARY_OBJECTIVE_TOLERANCE */
   	%do i=1 %to &_numsce;
	   /*%let list_scen = %sysfunc(catx(%str(_),%str(Scenario),&i));
	   Scenario_name = &list_scen;*/
	   Scenario_name = catx('_','Scenario',&i);
	   facility='ALL'; service_line='ALL'; sub_service='ALL'; 
	   /*parm_name = 'ALLOW_OPENING_ONLY_ON_PHASE'; parm_value='YES'; output;*/
	   parm_name='SECONDARY_OBJECTIVE_TOLERANCE'; parm_value='90'; output;
   %end;
   
    /* Specify minimum demand for some hierarchy combinations */
   Scenario_name = 'Scenario_1'; facility='ALL'; service_line='ALL'; sub_service='ALL';  
   parm_name='MIN_DEMAND_RATIO';
   facility='ALL'; service_line='Cardiac Services'; sub_service='ALL'; parm_value='30'; output;
   facility='Cleveland Clinic'; service_line='Gastroenterology'; sub_service='Other GI Diagnostic Testing'; parm_value=10; output;
   facility='Cleveland Clinic'; service_line='Cardiac Services'; sub_service='ALL'; parm_value='40'; output;
   facility='Cleveland Clinic'; service_line='Cardiac_Services'; sub_service='Cardiac Cath'; parm_value='50'; output;
   
   /* Specify percentage of emergency surgical procedures {by service line} for some service lines */
   Scenario_name = 'Scenario_1'; parm_name='EMER_SURGICAL_PTS_RATIO';
   facility='ALL'; service_line='Cardiac Services'; sub_service='ALL'; parm_value='30'; output;
   facility='Cleveland Clinic'; service_line='Gynecology'; sub_service='ALL'; parm_value='10'; output;
   facility='ALL'; service_line='Gynecology'; sub_service='ALL'; parm_value='20'; output;
   facility='ALL'; service_line='Cardiology'; sub_service='ALL'; parm_value='40'; output;

run;

data &outlib..&input_opt_parameters_multi  (promote=yes);
   set &_worklib.._tmp_input_opt_parameters    
       &_worklib..append_to_opt_parameters;
run;
		
 /*************************/
   /******HOUSEKEEPING*******/
   /*************************/
   %if &_debug.=0  %then %do;
      %let i = 1;
      %let table = %scan(&_work_tables, &i, ' ');
      %do %while (&table ne %str());
         %if %sysfunc(exist(&table)) %then %do;
            proc delete data= &table.;
            run;
         %end;
         %let i = %eval(&i + 1);
         %let table = %scan(&_work_tables, &i, ' ');
      %end;
   %end;

   %EXIT:
   %put TRACE: Leaving &sysmacroname. with SYSCC=&SYSCC.;

%mend;   

/* temporary - to generate input_opt_param for different scenarios - Can remove this after we get data from cc*/
	%cc_create_input_opt_param(inlib=cc
                   ,outlib=cc
                   ,input_opt_parameters=input_opt_parameters
				   ,input_opt_parameters_multi=input_opt_parameters_multi
				   ,_numsce = 2
				   ,_test_adj = 100 
                   ,_worklib=casuser
                   ,_debug=0
                   );
