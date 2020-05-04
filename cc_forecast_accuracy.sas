/* Define libraries */
%let inlib=cc;
%let outlib=casuser;
%let _worklib=casuser;

proc sql;
select max(date) into: max_date from &inlib..input_demand;
quit;
%put &=max_date;

data &inlib..input_demand_train &inlib..input_demand_test;
   set &inlib..input_demand;
   if date <= (&max_date. - 30) then output &inlib..input_demand_train;
   else output &inlib..input_demand_test;
run;

/* Submit code */
%cc_data_prep(
    inlib=&inlib
    ,outlib=&outlib
    ,input_utilization=input_utilization
    ,input_capacity=input_capacity
    ,input_financials=input_financials
    ,input_service_attributes=input_service_attributes
    ,input_demand=input_demand_train
    ,input_opt_parameters=input_parameters
	,output_dp_exceptions=output_dp_exceptions
    ,_worklib=&_worklib
    ,_debug=0
    );

%cc_forecast_demand(
    inlib=&inlib
    ,outlib=&outlib.
	,output_fd_demand_fcst=output_fd_demand_fcst
	,lead_weeks=4
    ,_worklib=casuser
    ,_debug=0
    );

%let hierarchy=%str(facility service_line sub_service ip_op_indicator med_surg_indicator);
data &_worklib..output_forecast_acuracy;
	merge 
		&_worklib..output_fd_demand_fcst (in=a keep = &hierarchy. predict_date daily_predict rename = (predict_date=date))
		&inlib..input_demand_test (in=b keep = &hierarchy. date demand);
	by &hierarchy. date;
	if b;
run;

	proc cas;
 	  aggregation.aggregate / table={caslib="&_worklib.", name="output_forecast_acuracy"
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
		groupby={"facility","service_line","sub_service"}} 
		saveGroupByFormat=false 
 	     varSpecs={{name="ape", summarySubset="Mean", columnNames="MAPE"}} 	  	
     
 	     casOut={caslib="&_worklib.",name="_tmp_fa_mape",replace=true}; run; 
	quit;
		
		
