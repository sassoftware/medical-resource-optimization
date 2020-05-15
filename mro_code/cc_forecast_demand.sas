*------------------------------------------------------------------------------*
| Program: cc_forecast_demand
|
| Description: 
|
*--------------------------------------------------------------------------------* ;
%macro cc_forecast_demand(
	inlib=cc
	,outlib=cc
	,input_demand=input_demand_pp
	,output_fd_demand_fcst=output_fd_demand_fcst
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

   %if %sysfunc(exist(&_worklib..&input_demand.))=0 %then %do;
      %put FATAL: Missing &_worklib..&input_demand., from &sysmacroname.;
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
		&_worklib.._tmp_input_demand_dow_mas
		&_worklib.._tmp1_input_demand_dow_mas
		&_worklib.._tmpstats
		&_worklib.._tmp2_input_demand_woy 
		&_worklib.._tmp_output_fcst_woy_mas
		&_worklib.._tmp1_output_fcst_woy_mas
        );	

   /* List output tables */
   %let output_tables=%str(         
         &outlib..&output_fd_demand_fcst
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
	
   %let lead_weeks = %str();
   %let forecast_model = %str();
   proc sql noprint;
      select parm_value into :lead_weeks
         from &_worklib..input_opt_parameters_pp 
         where upcase(parm_name) = 'PLANNING_HORIZON';
      select lowcase(parm_value) into :forecast_model
         from &_worklib..input_opt_parameters_pp
         where upcase(parm_name) = 'FORECAST_MODEL';
   quit;
   %if &lead_weeks = %str() %then %let lead_weeks = 12;
   %if &forecast_model = %str() %then %let forecast_model = tsmdl;
   
	/* For debugging purposes */
 		/*%let _worklib=casuser; */
	
	/* Prep Data  - Temporary, remove when data has been fixed*/
	data &_worklib.._tmp_input_demand;
		set &_worklib..&input_demand. /*(rename = (date=datetime))*/;
/* 		date=datepart(datetime); */
		dow= weekday(date); 
	run;
	
	/* Programatticaly obtaining the first sunday and the last saturday in the input data*/
	/* First Sunday */
	proc cas;
 	  aggregation.aggregate / table={caslib="&_worklib.", name="_tmp_input_demand",
		where= "dow = 1"} 
 	     varSpecs={{name="date", summarySubset="Min", columnNames="Date"}}
 	     casOut={caslib="&_worklib.",name="_tmpstats",replace=true}; run; 
	 quit;
	
	/* Save relevant statistics in macro variables */
	data _null_;
	   set &_worklib.._tmpstats;
	   call symputx('tStart', Date);
	run;

	/* Last Saturday */
	proc cas;
 	  aggregation.aggregate / table={caslib="&_worklib.", name="_tmp_input_demand",
		where= "dow = 7"} 
 	     varSpecs={{name="date", summarySubset="Max", columnNames="Date"}} 
 	     casOut={caslib="&_worklib.",name="_tmpstats",replace=true}; run; 
	quit;

	/* Save relevant statistics in macro variables */
	data _null_;
	   set &_worklib.._tmpstats;
	   call symputx('tEnd', Date);
	   
	   if Date < today() then gap_weeks = intck('week',Date,today()) + 1;
	   else gap_weeks = 0;
	   call symputx('gap_weeks', gap_weeks);
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
	
	%let forecast_weeks = %eval(&lead_weeks + &gap_weeks);

%if &forecast_model. = tsmdl %then 
%do;
/* Start here for ts model */	
	proc tsmodel data=&_worklib.._tmp_input_demand_week
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
        rc = forecast.SetOption('lead', &forecast_weeks.);
        rc = forecast.Run();
        
        declare object outfor(outfor);
        rc = outfor.Collect(forecast);
        
        endsubmit;
	run;
	
	data &_worklib.._tmp_output_fd_demand_fcst_week;
		set &_worklib.._tmp_output_fd_demand_fcst;
		if actual = .;
		if date >= &tEnd;
	run;	
%end;

%if &forecast_model. = yoy %then 
%do;
/* Start here for the yoy model */ 
%let forecast_tEnd = &tEnd + (&forecast_weeks*7);	
	
	data &_worklib.._tmp2_input_demand_woy;
		set &_worklib.._tmp_input_demand_week;
		woy = week(date);
		dem_year = year(date);
		forecast_year = dem_year+1;
		rename demand = Predict;
		drop date;
	run;

	proc cas; 
	  aggregation.aggregate / table={caslib="&_worklib.", name="_TMP_INPUT_DEMAND",  
		 groupby={"facility","service_line","sub_service","IP_OP_Indicator","Med_Surg_Indicator"}} 
		 saveGroupByFormat=false 
		 varSpecs={{name="demand", summarySubset="sum", columnNames="TotalDemand"}} 
		 casOut={caslib="&_worklib.",name="_tmp2_input_demand_dow",replace=true}; run;  	
	quit;

	/* Master list for next two years {f,sl,ss, iof,msf} & 52 weeks */
	data &_worklib.._tmp_output_fcst_woy_mas;
	set &_worklib.._tmp2_input_demand_dow;
			do i=0 to 1;
				do j=0 to 53;
					woy = j;
					forecast_year=year(input("&sysdate9",date9.))+i;				
					dem_year=forecast_year-1;
					date=intnx('week',mdy(1,1,forecast_year),woy-1,'b');
					date_wk_end=intnx('week',mdy(1,1,forecast_year),woy-1,'e');
					yr_date_wk_start = year(date);
					yr_date_wk_end = year(date_wk_end);
					output;
			end;
		end;	
	drop i j;
	run;

	/* truncating the master list for next 52 weeks from &tEND */
	data &_worklib.._tmp1_output_fcst_woy_mas;
	set &_worklib.._tmp_output_fcst_woy_mas;
		if (yr_date_wk_start = forecast_year) or (yr_date_wk_end = forecast_year);		
		if date >= &tEnd.;
		if date <= &forecast_tEnd.;		
		keep facility service_line sub_service IP_OP_Indicator Med_Surg_Indicator woy dem_year forecast_year date; 	
	run;	

	data &_worklib.._tmp_output_fd_demand_fcst_week;
		merge 
			&_worklib.._tmp1_output_fcst_woy_mas (in=nodes)
			&_worklib.._tmp2_input_demand_woy;
		by facility service_line sub_service IP_OP_Indicator Med_Surg_Indicator woy dem_year forecast_year;		
		if nodes;
		if Predict=. then Predict=0;
	keep facility service_line sub_service IP_OP_Indicator Med_Surg_Indicator woy dem_year forecast_year date Predict;	
	run;
%end;
	
/******* Dissagregate weekly forecasts into daily through a dow profile: issue #8 *******/
	/* calculating the average proportion of demand per day of week */
	proc cas;
 	  aggregation.aggregate / table={caslib="&_worklib.", name="_TMP_INPUT_DEMAND",  
 	     groupby={"facility","service_line","sub_service","IP_OP_Indicator","Med_Surg_Indicator", "dow"}} 
	     saveGroupByFormat=false 
 	     varSpecs={{name="demand", summarySubset="sum", columnNames="sumDemand"}} 
 	     casOut={caslib="&_worklib.",name="_tmp1_input_demand_dow",replace=true}; run; 
	 
	  aggregation.aggregate / table={caslib="&_worklib.", name="_TMP_INPUT_DEMAND",  
 	     groupby={"facility","service_line","sub_service","IP_OP_Indicator","Med_Surg_Indicator"}} 
	     saveGroupByFormat=false 
 	     varSpecs={{name="demand", summarySubset="sum", columnNames="TotalDemand"}} 
 	     casOut={caslib="&_worklib.",name="_tmp2_input_demand_dow",replace=true}; run;  	
	quit;

	/* Aggregated table shows missing context in the historical data */ 
	/* This step creates a master table - {facility service_line sub_service IP_OP_Indicator Med_Surg_Indicator dow} */
	data &_worklib.._tmp_input_demand_dow_mas;
	set &_worklib.._tmp2_input_demand_dow;
			do j=1 to 7;
				dow = j;
				output;
			end;
	keep facility service_line sub_service IP_OP_Indicator Med_Surg_Indicator dow; 
	run;

	/* Join master table to the dow profile table - to get Sumdemand by context */	
	/* Setting Sumdemand = 0 if the context doesn't exist in the historical data */
	data &_worklib.._tmp1_input_demand_dow_mas;
		merge 
			&_worklib.._tmp_input_demand_dow_mas (in=nodes)
			&_worklib.._tmp1_input_demand_dow;
		by facility service_line sub_service IP_OP_Indicator Med_Surg_Indicator dow;
		if nodes;
		if Sumdemand=. then Sumdemand=0;
	run;
	
	/* combine two tables to compute demand proportion */	
	data &_worklib.._tmp_input_demand_dow;
		merge 
			&_worklib.._tmp1_input_demand_dow_mas (in=nodes)
			&_worklib.._tmp2_input_demand_dow;
		by facility service_line sub_service IP_OP_Indicator Med_Surg_Indicator;
		if nodes;
		if Totaldemand=. then Totaldemand=0;
		if Totaldemand = 0 then demand_proportion = 0;
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
            if /*today() <*/ predict_date <= today() + 7 * &lead_weeks then output;
            rc0 = h0.find_next();
         end;

         drop rc0;
      run;

		data &outlib..&output_fd_demand_fcst (promote=yes);
			set &_worklib.._tmp_output_fd_demand_fcst_dly;
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

%mend cc_forecast_demand;