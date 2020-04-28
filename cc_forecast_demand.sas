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
		&_worklib.._tmp1_input_demand_dow 
		&_worklib.._tmp2_input_demand_dow 
		&_worklib..input_demand_dow 
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
	proc sql;
		select min(date) into
			:tStart from &_worklib.._tmp_input_demand
		where (dow = 1);

	/* Last Saturday */
	proc sql;
		select max(date) into
			:tEnd from &_worklib.._tmp_input_demand
		where (dow = 7);

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
	proc delete data=&_worklib..output_fd_demand_fcst; 
	run;
	data &_worklib..output_fd_demand_fcst (promote=yes);
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
	proc fedsql sessref=mysess _method ;
	   create table &_worklib..input_demand_dow {options replication=0 replace=true} as
		   
		select 
			A.facility, A.service_line, A.sub_service, A.IP_OP_Indicator, A.Med_Surg_Indicator, A.dow,
	 		A.Sumdemand, B.Totaldemand , 
			case
				when B.Totaldemand = 0 or B.Totaldemand IS NULL then 0
				else (A.Sumdemand / B.Totaldemand) end as demand_proportion
		from
			&_worklib.._tmp1_input_demand_dow A
		LEFT OUTER JOIN
			&_worklib.._tmp2_input_demand_dow B
		ON	
			A.facility = B.facility AND A.service_line = B.service_line and A.sub_service = B.sub_service AND
			A.IP_OP_Indicator = B.IP_OP_Indicator AND A.Med_Surg_Indicator = B.Med_Surg_Indicator
		;
	quit ;

	/* Dis-aggregate weekly forecasts into daily */
	proc fedsql sessref=mysess _method ;		
	   create table &_worklib..output_fd_demand_fcst_daily {options replication=0 replace=true} as
		   
		select 
			A.*, B.dow, put(intnx('day',A.date, (B.dow-1) ), date9.) as predict_date,
			(A.predict * B.demand_proportion) as daily_predict
		from
			&_worklib..output_fd_demand_fcst A
		LEFT OUTER JOIN
			&_worklib..input_demand_dow B
		ON	
			A.facility = B.facility AND A.service_line = B.service_line and A.sub_service = B.sub_service AND
			A.IP_OP_Indicator = B.IP_OP_Indicator AND A.Med_Surg_Indicator = B.Med_Surg_Indicator
		;
	quit ;	

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
