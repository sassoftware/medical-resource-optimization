*------------------------------------------------------------------------------*
| Program: cc_med_res_opt_standalone
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

  %cc_execute(
     inlib=cc
     ,outlib=casuser
     ,_worklib=casuser
     ,input_utilization=input_utilization
     ,input_capacity=input_capacity
     ,input_financials=input_financials
     ,input_service_attributes=input_service_attributes
     ,input_demand=input_demand
     ,input_opt_parameters=input_opt_parameters_multi
     ,run_dp=0
     ,run_fcst=0
     ,run_opt=1);
   
