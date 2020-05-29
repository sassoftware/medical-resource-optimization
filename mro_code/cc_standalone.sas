*------------------------------------------------------------------------------*
| Program: cc_standalone
|*--------------------------------------------------------------------------------* ;

/* Start cas session */
cas mysess;
caslib _all_ assign;

/* Point to the code */
%let my_code_path=/r/sanyo.unx.sas.com/vol/vol920/u92/navikt/casuser/covid/code/MRO/git/mro_code;
%include "&my_code_path./cc_execute.sas";
%include "&my_code_path./cc_data_prep.sas";
%include "&my_code_path./cc_forecast_demand.sas";
%include "&my_code_path./cc_optimize.sas";

/*Execute cc_execute macro */
  %cc_execute(
     inlib=cc
     ,outlib=casuser
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
     ,run_dp=1
     ,run_fcst=1
     ,run_opt=1);
   
