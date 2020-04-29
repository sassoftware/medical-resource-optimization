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
		&_worklib.._tmp_output_fd_demand_fcst_week
		&_worklib.._tmp_output_fd_demand_fcst_dly
		&_worklib.._tmp1_input_demand_dow 
		&_worklib.._tmp2_input_demand_dow 
		&_worklib.._tmp_input_demand_dow 
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
		set &inlib..&input_demand. (rename = (date=datetime));
		date=datepart(datetime);
		dow= weekday(date); 
	run;
	
	/* Programatticaly obtaining the first sunday and the last saturday in the input data*/
	/* First Sunday */
	proc means data=&_worklib.._tmp_input_demand Min noprint;
		where dow =1;		
	var Date;	
	output out=&_worklib.._tmpstats(where=(_STAT_='MIN'));
	run;
	
	/* Save relevant statistics in macro variables */
	data _null_;
	   set &_worklib.._tmpstats;
	   call symputx('tStart', Date);
	run;

	/* drop tmp table*/
	proc delete data = &_worklib.._tmpstats;
	run;

	/* Last Saturday */
	proc means data=&_worklib.._tmp_input_demand Max noprint;
		where dow =7;		
	var Date;	
	output out=&_worklib.._tmpstats(where=(_STAT_='MAX'));
	run;
	
	/* Save relevant statistics in macro variables */
	data _null_;
	   set &_worklib.._tmpstats;
	   call symputx('tEnd', Date);
	run;

	/* drop tmp table*/
	proc delete data = &_worklib.._tmpstats;
	run;

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
	
	/* Dissagregate weekly forecasts into daily through a dow profile: issue #8 */
	data &_worklib.._tmp_output_fd_demand_fcst_week;
		set &_worklib.._tmp_output_fd_demand_fcst;
		if actual = .;
		if date >= &tEnd;
	run;

	/* calculating the average proportion of demand per day of week */
	proc cas;
 	  aggregation.aggregate / table={caslib="casuser", name="_TMP_INPUT_DEMAND",  
 	     groupby={"facility","service_line","sub_service","IP_OP_Indicator","Med_Surg_Indicator", "dow"}} 
	     saveGroupByFormat=false 
 	     varSpecs={{name="demand", summarySubset="sum", columnNames="sumDemand"}} 
 	     casOut={caslib="casuser",name="_tmp1_input_demand_dow",replace=true}; run; 
	 
	  aggregation.aggregate / table={caslib="casuser", name="_TMP_INPUT_DEMAND",  
 	     groupby={"facility","service_line","sub_service","IP_OP_Indicator","Med_Surg_Indicator"}} 
	     saveGroupByFormat=false 
 	     varSpecs={{name="demand", summarySubset="sum", columnNames="TotalDemand"}} 
 	     casOut={caslib="casuser",name="_tmp2_input_demand_dow",replace=true}; run;  	
	quit;

	/* combine two tables to compute demand proportion */	
		data &_worklib.._tmp_input_demand_dow;
			merge 
				&_worklib.._tmp1_input_demand_dow (in=nodes)
				&_worklib.._tmp2_input_demand_dow;
			by facility service_line sub_service IP_OP_Indicator Med_Surg_Indicator;
			if nodes;
			if Totaldemand = 0 or Totaldemand = . then demand_proportion = 0;
			else demand_proportion= (Sumdemand / Totaldemand);
		run;

	 /* Dis-aggregate weekly forecasts into daily */
      data &_worklib.._tmp_output_fd_demand_fcst_dly;
         set &_worklib.._tmp_output_fd_demand_fcst_week;
         format predict_date date9.;

         if _n_ = 1 then do;
            declare hash h0(dataset:"&_worklib.._tmp_input_demand_dow", multidata:'y');
            h0.defineKey('facility','service_line','sub_service','IP_OP_Indicator','Med_Surg_Indicator');
            h0.defineData('dow','demand_proportion');
            h0.defineDone();
         end;

         dow = .;
         demand_proportion = .;
         do rc0 = h0.find() by 0 while (rc0 = 0);
            predict_date = intnx('day',date, (dow-1));
            daily_predict = (predict * demand_proportion);
            output;
            rc0 = h0.find_next();
         end;

         drop rc0;
      run;

		data &_worklib..output_fd_demand_fcst;
			set &_worklib.._tmp_output_fd_demand_fcst_dly;
		run;

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
