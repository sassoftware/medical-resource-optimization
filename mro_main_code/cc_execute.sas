*--------------------------------------------------------------------------------------------------------------*
| Copyright © 2020, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
| SPDX-License-Identifier: Apache-2.0
|
| Program: cc_execute
|
| Description: This macro calls the %cc_data_prep, %cc_forecast_demand, and 
|              %cc_optimize macros in sequence.
| 
| INPUTS:
|   - inlib:                     Name of the CAS library where the input tables are located
|   - opt_param_lib:             Name of the CAS library where the INPUT_OPT_PARAMETERS table 
|                                is located
|   - input_utilization:         Name of the table that contains resource utilization data (in inlib)
|   - input_capacity:            Name of the table that contains resource capacity data (in inlib)
|   - input_financials:          Name of the table that contains revenue and margin data (in inlib)
|   - input_service_attributes:  Name of the table that contains length-of-stay and 
|                                cancellation data (in inlib)
|   - input_demand:              Name of the table that contains historical demand data (in inlib)
|   - input_demand_forecast:     Name of the table that contains forecasted demand data (in inlib)
|   - input_opt_parameters:      Name of the table that contains the optimization 
|                                parameters (in opt_param_lib)
|
| OUTPUTS:
|   - outlib:                            Name of the CAS library where the output tables are created
|   - output_opt_detail:                 Name of the table that stores solution detail records (in outlib)
|   - output_opt_detail_agg:             Name of the table that stores the weekly aggregated solution 
|                                        data (in outlib)
|   - output_opt_summary:                Name of the table that stores recommended reopening plan for 
|                                        service lines (in outlib)
|   - output_opt_resource_usage:         Name of the table that stores aggregate utilization of each 
|                                        constrained resource (in outlib)
|   - output_opt_resource_usage_detail:  Name of the table that stores utilization of resources at facility/
|                                        service line/sub-service level (in outlib)
|   - output_opt_covid_test_usage:       Name of the table that stores daily COVID-19 test usage (in outlib)
|
| OTHER PARAMETERS:
|   - _worklib:     Name of the CAS library where the working tables are created
|   - include_str:  Parameter that can be used to filter all the input data tables to include only 
|                   specified rows. 
|                   Example: include_str = %str(facility in ('fac1','fac','ALL'))
|   - exclude_str:  Parameter that can be used to filter all the input data tables to exclude 
|                   specified rows. 
|                   Example: exclude_str = %str(service_line = 'ABC')
|   - run_dp:       Flag to indicate whether cc_data_prep is to be run
|   - run_fcst:     Flag to indicate whether cc_forecast_demand is to be run
|   - run_opt:      Flag to indicate whether cc_optimize is to be run
|   - _debug:       Flag to indicate whether the temporary tables in _worklib are to be retained for debugging
|
*--------------------------------------------------------------------------------------------------------------*;

%macro cc_execute(
         inlib=cc
         ,outlib=casuser
         ,opt_param_lib=cc
         ,_worklib=casuser
         ,input_utilization=input_utilization
         ,input_capacity=input_capacity
         ,input_financials=input_financials
         ,input_service_attributes=input_service_attributes
         ,input_demand=input_demand
         ,input_demand_forecast=input_demand_forecast
         ,input_opt_parameters=input_opt_parameters
         ,output_opt_detail=output_opt_detail
         ,output_opt_detail_agg=output_opt_detail_agg
         ,output_opt_summary=output_opt_summary
         ,output_opt_resource_usage=output_opt_resource_usage
         ,output_opt_resource_usage_detail=output_opt_resource_usage_detail
         ,output_opt_covid_test_usage=output_opt_covid_test_usage
         ,include_str=%str(1=1)
         ,exclude_str=%str(0=1)
         ,run_dp=1
         ,run_fcst=1
         ,run_opt=1
         ,_debug=0
         );


   %if %sysevalf(&run_dp.=1) %then %do;

      %cc_data_prep(
         inlib=&inlib.
         ,outlib=&outlib.
         ,opt_param_lib=&opt_param_lib.
         ,input_utilization=&input_utilization.
         ,input_capacity=&input_capacity.
         ,input_financials=&input_financials.
         ,input_service_attributes=&input_service_attributes.
         ,input_demand=&input_demand.
         ,input_demand_forecast=&input_demand_forecast
         ,input_opt_parameters=&input_opt_parameters.
         ,include_str=&include_str.
         ,exclude_str=&exclude_str.
         ,_worklib=&_worklib.
         ,_debug=&_debug.
         );

   %end;

   %if %sysevalf(&run_fcst.=1) %then %do;

      %cc_forecast_demand(
         inlib=&inlib.
         ,outlib=&outlib.
         ,input_demand=input_demand_pp
         ,output_fd_demand_fcst=output_fd_demand_fcst
         ,_worklib=&_worklib.
         ,_debug=&_debug.
         );

   %end;

   %if %sysevalf(&run_opt.=1) %then %do;

      %cc_optimize(
         inlib=&inlib.
         ,outlib=&outlib.
         ,input_demand_fcst=output_fd_demand_fcst
         ,output_opt_detail=&output_opt_detail.
         ,output_opt_detail_agg=&output_opt_detail_agg.
         ,output_opt_summary=&output_opt_summary.
         ,output_opt_resource_usage=&output_opt_resource_usage.
         ,output_opt_resource_usage_detail=&output_opt_resource_usage_detail.
         ,output_opt_covid_test_usage=&output_opt_covid_test_usage.
         ,_worklib=&_worklib.
         ,_debug=&_debug
         );

   %end;

%mend cc_execute;