*------------------------------------------------------------------------------*
| Program: cc_forecast_accuracy_standalone - SAS INTERNAL ONLY!
| 
|    
|*--------------------------------------------------------------------------------* ;


/* Start cas session */
cas mysess;
caslib _all_ assign;

/* Point to the code */
%let my_code_path=/r/ge.unx.sas.com/vol/vol410/u41/supazh/casuser/Cleveland_Clinic/gitrepo;
%include "&my_code_path./cc_forecast_demand.sas";
%include "&my_code_path./cc_forecast_demand_yoy.sas";
%include "&my_code_path./cc_med_res_opt.sas";
%include "&my_code_path./cc_data_prep.sas";

/* Define libraries */
%let inlib=cc;
%let outlib=cc;
%let _worklib=casuser;

/* Submit code */
%cc_data_prep(
    inlib=&inlib
    ,outlib=&outlib
    ,input_utilization=input_utilization
    ,input_capacity=input_capacity
    ,input_financials=input_financials
    ,input_service_attributes=input_service_attributes
    ,input_demand=input_demand
    ,input_opt_parameters=input_opt_parameters
   ,output_hierarchy_mismatch=output_hierarchy_mismatch
   ,output_resource_mismatch=output_resource_mismatch
   ,output_invalid_values=output_invalid_values
   ,output_duplicate_rows=output_duplicate_rows
    ,_worklib=&_worklib
    ,_debug=0
    );

proc sql;
  select max(date) into: max_date from &_worklib..input_demand_pp;
quit;

data &_worklib..input_demand_train &_worklib..input_demand_test;
   set &_worklib..input_demand_pp;
   if date <= (&max_date. - 30) then output &_worklib..input_demand_train;
   else output &_worklib..input_demand_test;
run;

data &_worklib..input_demand_pp;
   set &_worklib..input_demand_train;
run;

%cc_forecast_demand(
    inlib=&inlib
    ,outlib=&outlib.
	,output_fd_demand_fcst=output_fd_demand_fcst
	,lead_weeks=5
    ,_worklib=casuser
    ,_debug=0
    );

%cc_forecast_demand_yoy(
    inlib=&inlib
    ,outlib=&outlib.
	,output_fd_demand_fcst_yoy=output_fd_demand_fcst_yoy
	,lead_weeks=5
    ,_worklib=casuser
    ,_debug=0
    );

%let hierarchy=%str(facility service_line sub_service ip_op_indicator med_surg_indicator);
data &outlib..output_fa_fit_fcst (promote=yes);
	merge 
		&outlib..output_fd_demand_fcst (in=a keep = &hierarchy. predict_date daily_predict rename = (predict_date=date))
		&_worklib..input_demand_test (in=b keep = &hierarchy. date demand);
	by &hierarchy. date;
	if b;
run;

data &_worklib.._tmp_fcst;
	set &outlib..output_fa_fit_fcst;
	if daily_predict~=.;
	if demand ~=.;
run;

proc cas;
 	  aggregation.aggregate / table={caslib="&_worklib.", name="_tmp_fcst"
		groupby={"facility","service_line","sub_service","date"}} 
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
		groupby={"facility","service_line"}} 
		saveGroupByFormat=false 
 	     varSpecs={{name="ape", summarySubset="Mean", columnNames="MAPE", weight="Total_Demand"}} 	       
 	     casOut={caslib="&_worklib.",name="_tmp_fa_mape",replace=true}; run; 
	quit;

data &outlib..output_fa_mape (promote=yes);
   set &_worklib.._tmp_fa_mape;
run;
		
		
