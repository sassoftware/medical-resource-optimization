*------------------------------------------------------------------------------*
| Program: cc_forecast_demand
|
| Description: 
|
*--------------------------------------------------------------------------------* ;
%macro cc_forecast_demand(
	inlib=cc
	,outlib=cc
	,input_demand =input_demand
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

   %if %sysfunc(exist(&inlib..&input_demand.))=0 %then %do;
      %put FATAL: Missing &inlib..&input_demand., from &sysmacroname.;
      %goto EXIT;
   %end; 


   /* List work tables */
   %let _work_tables=%str(  
        &_worklib.._tmp_input_demand
		&_worklib.._tmp_input_demand_week
		&_worklib.._tmp_output_fd_demand_fcst
         );

   /* List output tables */
   %let output_tables=%str(         
         &_worklib..output_fd_demand_fcst
         );

 /*Delete output data if already exists */
	proc delete data= &output_tables.;
	run;

 /* Delete work data if already exists */
	proc delete data= &_work_tables.;
	run;
 

	/************************************/
	/************ANALYTICS *************/
	/***********************************/
	
	/* For debugging purposes */
		%let _worklib=casuser;
	
	/* Prep Data  - Temporary, remove when data has been fixed*/
	data &_worklib.._tmp_input_demand;
		set &inlib..&input_demand. (rename = (date=datetime);
		date=datepart(datetime);
	run;
	
	/* Programatticaly obtain the first sunday and the last saturday in the input data: issue #7*/
	
	/* Hardcoded termporarily */
	%let tStart=21247; /* March 4th, 2018 */
	%let tEnd=21974; /* Feb 29, 2020 */
	
	proc cas;
	   timeData.timeSeries /
	      table={
			caslib="&_worklib.", 
			name="_tmp_input_demand", 
			groupby={"facility" "service_line" "sub_service" "med_surg_indicator" "ip_op_indicator"}} 
	      series={{
			name="demand" 
			acc="sum" 
			setmiss=0}}
	      timeId="date"
	      tStart=&tStart.
	      tEnd=&tEnd.
	      interval="week"
	      casOut={caslib="&_worklib." name="_tmp_input_demand_week" replace=true};
	   run;
	quit;
	
	
	proc tsmodel data=casuser._tmp_input_demand_week
        outobj=(outfor=&_worklib.._tmp_output_fd_demand_fcst);
        id date interval=week;
        by facility service_line sub_service med_surg_indicator ip_op_indicator;
        var demand;
        require atsm;
        
        submit;
        
        declare object tsdf(tsdf);
        rc = tsdf.Initialize();
        rc = tsdf.AddY(demand);
        
        declare object ev1(event);
        rc = ev1.Initialize();
        rc = tsdf.AddEvent(ev1,'USINDEPENDENCE', 'Required','MAYBE'); if rc < 0 then do; stop; end;
        rc = tsdf.AddEvent(ev1,'THANKSGIVING', 'Required','MAYBE'); if rc < 0 then do; stop; end;
        rc = tsdf.AddEvent(ev1,'NEWYEAR', 'Required','MAYBE'); if rc < 0 then do; stop; end;
        rc = tsdf.AddEvent(ev1,'MEMORIAL', 'Required','MAYBE'); if rc < 0 then do; stop; end;
        rc = tsdf.AddEvent(ev1,'LABOR', 'Required','MAYBE'); if rc < 0 then do; stop; end;
        rc = tsdf.AddEvent(ev1,'EASTER', 'Required','MAYBE'); if rc < 0 then do; stop; end;
        rc = tsdf.AddEvent(ev1,'CHRISTMAS', 'Required','MAYBE'); if rc < 0 then do; stop; end;
        
        declare object diagspec(diagspec);
        rc = diagspec.Open();
        rc = diagspec.SetESM();
        rc = diagspec.SetARIMAX();
        rc = diagspec.Close();
        
        declare object diagnose(diagnose);
        rc = diagnose.Initialize(tsdf);
        rc = diagnose.SetSpec(diagspec);
        rc = diagnose.Run();
        
        declare object forecast(foreng);
        rc = forecast.Initialize(diagnose);
        rc = forecast.SetOption('lead', 12);
        rc = forecast.Run();
        
        declare object outfor(outfor);
        rc = outfor.Collect(forecast);
        
        endsubmit;
	run;
	
	data &_worklib..output_fd_demand_fcst;
		set &_worklib.._tmp_output_fd_demand_fcst;
		if actual = .;
	run;

	/* Dissagregate weekly forecasts into daily through a dow profile: issue #8 */


   /*************************/
   /******HOUSEKEEPING*******/
   /*************************/

   %if &_debug.=0  %then %do;
	proc delete data= &_work_tables.;
	run;
   %end;

   %EXIT:
   %put TRACE: Leaving &sysmacroname. with SYSCC=&SYSCC.;

%mend cc_forecast_demand;