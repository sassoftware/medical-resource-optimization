*------------------------------------------------------------------------------*
| Program: cc_forecast_accuracy
|
| Description: 
|
*--------------------------------------------------------------------------------* ;
%macro cc_forecast_accuracy(
	inlib=cc
	,outlib=cc
	,input_demand=input_demand_pp
	,output_fa_fit_fcst=output_fa_fit_fcst
	,output_fa_mape=output_fa_mape
	,output_fa_mape_comp=output_fa_mape_comp
	,forecast_testset_days = 30 
	,forecast_methods = tsmdl;yoy
	,_worklib=casuser
	,_debug=1
	);

/* Point to the forecast code: having a macro within a macro as an exception */
/* Have to call forecast macro twice with different forecast methods */
%let my_code_path=/r/ge.unx.sas.com/vol/vol410/u41/supazh/casuser/Cleveland_Clinic/gitrepo;
%include "&my_code_path./cc_forecast_demand.sas";

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
   %let _work_tables_fa=%str(  
		&_worklib.._tmpstats
		&_worklib..input_demand_train
		&_worklib..input_demand_test
		&_worklib.._tmp_fcst
		&_worklib.._tmp_fa_agg
		&_worklib.._tmp_fa_ape
		&_worklib.._tmp_fa_mape
		&_worklib.._tmp_fa_mape_tsmdl
		&_worklib.._tmp_fa_mape_yoy
        );	

   /* List output tables */
   %let output_tables=%str(         
        &outlib..&output_fa_fit_fcst.
		&outlib..&output_fa_mape.
		&outlib..&output_fa_mape_comp.
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
	/* For debugging purposes */
 	/*%let _worklib=casuser; 
	%let input_demand = input_demand_pp;
	%let forecast_testset_days = 30;
	%let forecast_methods = 'tsmdl yoy';*/

/* Get Max date macro variable (replacing sql block) */
	proc cas;
 	  aggregation.aggregate / table={caslib="&_worklib.", name="&input_demand"} 
 	     varSpecs={{name="date", summarySubset="Max", columnNames="Date"}}
 	     casOut={caslib="&_worklib.",name="_tmpstats",replace=true}; run; 
	 quit;

	data _null_;
	   set &_worklib.._tmpstats;
	   call symputx('max_date', Date);
	run;

/*input_demand_train - is the training data set, input_demand_test is the test data set */
	data &_worklib..input_demand_train &_worklib..input_demand_test;
	   set &_worklib..&input_demand.;
	   if date <= (&max_date. - &forecast_testset_days.) then output &_worklib..input_demand_train;
	   else output &_worklib..input_demand_test;
	run;

/* Get palnning horizon from the parameters table (replacing sql block) */
	proc cas;
 	  aggregation.aggregate / table={caslib="&_worklib.", name="input_opt_parameters_pp",
		where= "parm_name = 'PLANNING_HORIZON'" } 
 	     varSpecs={{name="parm_value", summarySubset="Max", columnNames="parm_value"}}
 	     casOut={caslib="&_worklib.",name="_tmpstats",replace=true}; run; 
	 quit;

	data _null_;
	   set &_worklib.._tmpstats;
	   call symputx('planning_horizon', parm_value);
	run;

/*number of forecasting methods*/
%let model_cnt = %sysfunc(countw(&forecast_methods.));

/* loop and forecast using the different forecast methods */
%do j=1 %to &model_cnt.;
	%let fm = %scan(&forecast_methods, &j);
	
	%cc_forecast_demand(
	    inlib=&inlib
	    ,outlib=&outlib.
		,input_demand = input_demand_train
		,output_fd_demand_fcst=output_fd_demand_fcst
		,lead_weeks=&planning_horizon
		,forecast_model = &fm
	    ,_worklib=casuser
	    ,_debug=0
	    );

   /*Delete output data */
	proc delete data=&outlib..&output_fa_fit_fcst.;
	run;
	proc delete data=&outlib..&output_fa_mape.;
	run;

	%let hierarchy=%str(facility service_line sub_service ip_op_indicator med_surg_indicator);
	data &outlib..&output_fa_fit_fcst. (promote=yes);
		merge 
			&outlib..output_fd_demand_fcst (in=a keep = &hierarchy. predict_date daily_predict rename = (predict_date=date))
			&_worklib..&input_demand (in=b keep = &hierarchy. date demand);
		by &hierarchy. date;
		if b;
	run;

/* _tmp_fa_agg: Aggregated Weekly demand & compute _tmp_fa_ape: average percentage error*/
	data &_worklib.._tmp_fcst;
		format date date9.;
		format week_start_date date9.;
		set &outlib..&output_fa_fit_fcst.;
		if daily_predict~=.;
		if demand ~=.;
		  week_num=week(date);
	      year_num=year(date);
	      week_start_date=input(put(year_num, 4.)||"W"||put(week_num,z2.)||"01", weekv9.);
	run;
	
	proc cas;
	 	  aggregation.aggregate / table={caslib="&_worklib.", name="_tmp_fcst"
			groupby={"facility","service_line","sub_service","ip_op_indicator","week_start_date"}} 
			saveGroupByFormat=false 
	 	     varSpecs={{name="daily_predict", summarySubset="Sum", columnNames="Total_Fcst"}
				 	   {name="demand", summarySubset="Sum", columnNames="Total_Demand"}} 		   	  	
     
	 	     casOut={caslib="&_worklib.",name="_tmp_fa_agg",replace=true}; run; 
	quit;
	
	data &_worklib.._tmp_fa_ape;
		set &_worklib.._tmp_fa_agg;
		ape=abs(Total_Demand-Total_Fcst)/Total_Demand;
	run;
	
	proc cas;
	 	  aggregation.aggregate / table={caslib="&_worklib.", name="_tmp_fa_ape"
			groupby={"facility","service_line","ip_op_indicator"}} 
			saveGroupByFormat=false 
	 	     varSpecs={{name="ape", summarySubset="Mean", columnNames="MAPE", weight="Total_Demand"}
					 	{name="Total_Demand", summarySubset="Mean", columnNames="Avg_Weekly_Demand"}} 	       
	 	     casOut={caslib="&_worklib.",name="_tmp_fa_mape",replace=true}; run; 
	quit;

	data &outlib..output_fa_mape (promote=yes);
	   set &_worklib.._tmp_fa_mape;
	run;
	
/* get forecasting accuracy data (MAPE) for different methods*/
	%let separator_c =%str(_);
	%let _tmp_fa_mape_for = %sysfunc(catx(&separator_c,%str(_tmp_fa_mape),&fm));
	/*%put &_tmp_fa_mape_for;*/

	data &_worklib..&_tmp_fa_mape_for.;
	   set &_worklib.._tmp_fa_mape;
	run;

/*adding */
/*
%let _work_tables = %sysfunc(catx(' ',"&_work_tables.","&_worklib..&_tmp_fa_mape_for."));
*/

%end;

/* combine the mape tables from two forecast methods */
proc delete data=&outlib..&output_fa_mape_comp.;
	run;

proc FEDSQL sessref=mysess;
	create table &outlib..&output_fa_mape_comp. as
	 select 
		A.facility, A.service_line, A.ip_op_indicator, 
		A.MAPE AS MAPE_TSMDL, A.Avg_Weekly_Demand AS Avg_Weekly_Demand_TSMDL,
		B.MAPE AS MAPE_YOY, B.Avg_Weekly_Demand AS Avg_Weekly_Demand_YOY	
	FROM &_worklib.._tmp_fa_mape_tsmdl A
	LEFT OUTER JOIN &_worklib.._tmp_fa_mape_yoy B
	on
	A.facility = B.facility AND A.service_line=B.service_line 
	AND A.ip_op_indicator =B.ip_op_indicator
	;
QUIT;

proc casutil;
	promote casdata="&output_fa_mape_comp." outcaslib="cc" incaslib="cc" drop;                   
quit;

   /*************************/
   /******HOUSEKEEPING*******/
   /*************************/
   %if &_debug.=0  %then %do;
      %let i = 1;
      %let table = %scan(&_work_tables_fa, &i, ' ');
      %do %while (&table ne %str());
         %if %sysfunc(exist(&table)) %then %do;
            proc delete data= &table.;
            run;
         %end;
         %let i = %eval(&i + 1);
         %let table = %scan(&_work_tables_fa, &i, ' ');
      %end;
   %end;

   %EXIT:
   %put TRACE: Leaving &sysmacroname. with SYSCC=&SYSCC.;

%mend cc_forecast_accuracy;
