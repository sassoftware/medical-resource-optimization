
%macro cc_execute(
	inlib=cc
	,outlib=casuser
	,_worklib=casuser
	,input_utilization=input_utilization
	,input_capacity=input_capacity
	,input_financials=input_financials
	,input_service_attributes=input_service_attributes
	,input_demand=input_demand
	,input_opt_parameters=input_opt_parameters
	,scenario_name=scen01
	,run_dp=1
	,run_fcst=1
	,run_opt=1);

	%if %sysevalf(&run_dp.=1) %then %do;

		%cc_data_prep(
		    inlib=&inlib.
		    ,outlib=&outlib.
		    ,input_utilization=&input_utilization.
		    ,input_capacity=&input_capacity.
		    ,input_financials=&input_financials.
		    ,input_service_attributes=&input_service_attributes.
		    ,input_demand=&input_demand.
		    ,input_opt_parameters=&input_opt_parameters.
		/*     ,include_str=%str(facility in ('Hillcrest','ALL') ) */
		    ,exclude_str=%str(facility in ('Florida','CCCHR') or service_line='Evaluation and Management')
		    ,los_rounding_threshold=0.5
		    ,_worklib=&_worklib.
		    ,_debug=0
		    );
	
		%end;

	%if %sysevalf(&run_fcst.=1) %then %do;
	
		%cc_forecast_demand(
		    inlib=&inlib.
		    ,outlib=&outlib.
		    ,input_demand=input_demand_pp
		    ,output_fd_demand_fcst=output_fd_demand_fcst_&scenario_name.
		    ,lead_weeks=26
		    ,forecast_model=tsmdl
		    ,_worklib=&_worklib.
		    ,_debug=0
		    );

	%end;

	%if %sysevalf(&run_opt.=1) %then %do;

		%cc_optimize(
		    inlib=&inlib.
		    ,outlib=&outlib.
	   	 ,input_demand_fcst=output_fd_demand_fcst_&scenario_name.
		    ,output_opt_detail=output_opt_detail_&scenario_name.
		    ,output_opt_detail_agg=output_opt_detail_agg_&scenario_name.
		    ,output_opt_summary=output_opt_summary_&scenario_name.
		    ,output_resource_usage=output_opt_res_util_&scenario_name.
		    ,output_covid_test_usage=output_opt_test_util_&scenario_name.
		    ,_worklib=&_worklib.
		    ,_debug=0);

	%end;

%mend;