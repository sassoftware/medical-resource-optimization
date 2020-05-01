*------------------------------------------------------------------------------*
| Program: cc_med_res_opt_standalone
| 
| INPUT_DEMAND is in casuser.OUTPUT_FD_DEMAND_FCST
| Description: Required datasets to be placed caslib CC: 
|    
|    INPUT_CAPACITY
|     INPUT_FINANCIALS
|    INPUT_SERVICE_ATTR
|*--------------------------------------------------------------------------------* ;

/* Start cas session */
cas mysess;
/* cas mysess sessopts=(nworkers=2); */
caslib _all_ assign;

/* Point to the code */
%let my_code_path=/u/micopp/casuser/covid19/medical_resource_optimization/michelle;
%include "&my_code_path./cc_forecast_demand.sas";
%include "&my_code_path./cc_med_res_opt.sas";
%include "&my_code_path./cc_data_prep.sas";

/* Define libraries */
%let inlib=casuser;
%let outlib=casuser;
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
	,output_dp_exceptions=output_dp_exceptions
    ,_worklib=&_worklib
    ,_debug=1
    );


%cc_forecast_demand(
    inlib=&inlib
    ,outlib=&outlib
    ,input_demand =input_demand_pp
	,output_fd_demand_fcst=output_fd_demand_fcst
    ,_worklib=casuser
    ,_debug=1
    );


%cc_med_res_opt(
    inlib=&inlib.
    ,outlib=&outlib.
    ,input_utilization=input_utilization_pp
    ,input_capacity =input_capacity_pp
    ,input_financials=input_financials_pp
    ,input_service_attributes=input_service_attributes_pp
    ,input_opt_parameters=input_opt_parameters_pp
	,output_opt_detail =output_opt_detail
	,output_opt_summary=output_opt_summary
    ,_worklib=&_worklib.);

/* cas mysess terminate;   */
