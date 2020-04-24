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

/* Prep Data  */
data casuser._tmp_input_demand;
	set cc.input_demand (
		rename = 
			(date=datetime)
		where = 
			(facility='Akron'
			and service_line='Cardiac Services'
			and sub_service='Cardiac Cath'
			and med_surg_indicator='SURG'
			and ip_op_indicator='I'));
	date=datepart(datetime);
run;

/* proc cas; */
/*    timeData.timeSeries / */
/*       table={ */
/* 		caslib="casuser",  */
/* 		name="input_demand",  */
/* 		groupby={"facility" "service_line" "sub_service" "med_surg_indicator" "ip_op_indicator"}}  */
/*       series={{ */
/* 		name="demand"  */
/* 		acc="sum"  */
/* 		setmiss=0}} */
/*       timeId="date" */
/*       tStart="Jan 1, 1998" */
/*       tEnd="Dec 1, 2002" */
/*       interval="day" */
/*       sumOut="_tmp_ts_stats" */
/*       casOut="_tmp_input_demand_ts"; */
/*    run; */
/* quit; */

/* Forecast */

proc cas;
   timeData.forecast /
      table={
		caslib="casuser", 
		name="_tmp_input_demand", 
		groupby={"facility" "service_line" "sub_service" "med_surg_indicator" "ip_op_indicator"}} 
      timeId={name='date'},
      interval='day',
/*       tStart='Jan 1, 1998', */
/*       tEnd='Dec 1, 2002', */
      dependents={{name='demand', accumulate='SUM'}},
/*       predictors={{name='price', accumulate='AVG'}, */
/*                   {name='discount', accumulate='AVG'}}, */
      lead=10,
      forOut={name='output_fd_demand_fcst'},
/*       infoOut={name='infoOut'}, */
/*       indepOut={name='indepOut={'}, */
/*       selectOut={name='selectOut'}, */
/*       specOut={name='specOut'} */
	  ;
   run;
quit;

/* Promote for visualization */
/* proc delete data=cc.input_demand_ts; */
/* run; */
/* data cc.input_demand_ts (promote=yes); */
/* 	set _tmp_out_ts; */
/* 	dow=weekday(date); */
/* run; */

/* Get DOW profile */
/* proc cas; */
/* 	  aggregation.aggregate / table={caslib="cc", name="input_demand_ts",  */
/* 	                                 groupby={"facility","service_line","sub_service","date", "dow"}} */
/* 	                          saveGroupByFormat=false */
/* 	                          varSpecs={{name="demand", summarySubset="sum",columnNames="demand"}} */
/* 	                          casOut={caslib="cc",name="input_demand_agg",replace=true}; run; */
/* quit; */
/*  */
/* proc delete data=cc.input_demand_dow; */
/* run; */
/*  */
/* proc cas; */
/* 	  aggregation.aggregate / table={caslib="cc", name="input_demand_agg",  */
/* 	                                 groupby={"facility","service_line","sub_service", "dow"}} */
/* 	                          saveGroupByFormat=false */
/* 	                          varSpecs={{name="demand", summarySubset="mean", columnNames="meanDemand"}} */
/* 	                          casOut={caslib="cc",name="input_demand_dow",replace=true}; run; */
/* quit; */
/*  */
/* data cc.input_demand_dow(promote=yes); */
/* 	set cc.input_demand_dow; */
/* run; */

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