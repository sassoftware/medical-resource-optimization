*------------------------------------------------------------------------------*
| Copyright © 2020, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
| SPDX-License-Identifier: Apache-2.0
|
| Program: cc_standalone
|
| Description: This program calls the main %cc_execute macro. See cc_execute 
|              for descriptions of the parameters. 
|*-----------------------------------------------------------------------------*;

/* Start cas session */
cas mysess;
caslib _all_ assign;

/* Point to the code */
%let my_code_path=<path to mro_main_code directory>; /* Add your code path here */
%include "&my_code_path./cc_execute.sas";
%include "&my_code_path./cc_data_prep.sas";
%include "&my_code_path./cc_forecast_demand.sas";
%include "&my_code_path./cc_optimize.sas";

/* Run cc_execute macro */
%cc_execute(
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
   ,output_opt_detail_daily=output_opt_detail_daily
   ,output_opt_detail_weekly=output_opt_detail_weekly
   ,output_opt_summary=output_opt_summary
   ,output_opt_resource_usage=output_opt_resource_usage
   ,output_opt_resource_usage_detail=output_opt_resource_usage_detail
   ,output_opt_covid_test_usage=output_opt_covid_test_usage
   ,run_dp=1
   ,run_fcst=1
   ,run_opt=1
   );

