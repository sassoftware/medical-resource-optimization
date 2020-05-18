
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
        ,run_dp=1
        ,run_fcst=1
        ,run_opt=1
        ,_debug=0);


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
          ,exclude_str=%str(facility in ('Florida','CCCHR') or service_line='Evaluation and Management')
          ,_worklib=&_worklib.
          ,_debug=&_debug.);
   
   %end;

   %if %sysevalf(&run_fcst.=1) %then %do;
   
      %cc_forecast_demand(
          inlib=&inlib.
          ,outlib=&outlib.
          ,input_demand=input_demand_pp
          ,output_fd_demand_fcst=output_fd_demand_fcst
          ,_worklib=&_worklib.
          ,_debug=&_debug.);

   %end;

   %if %sysevalf(&run_opt.=1) %then %do;

      %cc_optimize(
          inlib=&inlib.
          ,outlib=&outlib.
          ,input_demand_fcst=output_fd_demand_fcst
          ,output_opt_detail=output_opt_detail
          ,output_opt_detail_agg=output_opt_detail_agg
          ,output_opt_summary=output_opt_summary
          ,output_resource_usage=output_opt_res_util
          ,output_covid_test_usage=output_opt_test_util
          ,_worklib=&_worklib.
          ,_debug=&_debug);

   %end;

%mend;