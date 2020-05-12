*------------------------------------------------------------------------------*
| Program: cc_forecast_accuracy_standalone - SAS INTERNAL ONLY!
| 
|    
|*--------------------------------------------------------------------------------* ;

/* Start cas session */
cas mysess;
caslib _all_ assign;

/* Point to the code */
%let my_code_path=/r/ge.unx.sas.com/vol/vol410/u41/supazh/casuser/Cleveland_Clinic/gitrepo; /* Path for Subbu*/
/*%let my_code_path=/r/sanyo.unx.sas.com/vol/vol920/u92/navikt/casuser/covid/code/MRO/git;*/ 
%include "&my_code_path./cc_data_prep.sas";
%include "&my_code_path./cc_forecast_accuracy.sas";

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
/*
    ,include_str=%str(facility in ('Hillcrest','ALL') )
*/
    ,exclude_str=%str(facility in ('Florida','CCCHR') or service_line='Evaluation and Management')
    ,los_rounding_threshold=0.5
    ,output_hierarchy_mismatch=output_dp_hierarchy_mismatch
    ,output_resource_mismatch=output_dp_resource_mismatch
    ,output_invalid_values=output_dp_invalid_values
    ,output_duplicate_rows=output_dp_duplicate_rows
    ,_worklib=&_worklib
    ,_debug=0
    );

%cc_forecast_accuracy(
	inlib=&inlib
	,outlib=&inlib
	,input_demand=input_demand_pp
	,output_fa_fit_fcst=output_fa_fit_fcst
	,output_fa_mape=output_fa_mape
	,output_fa_mape_comp=output_fa_mape_comp
	,forecast_testset_days = 30 
	,forecast_methods = tsmdl;yoy
	,_worklib=&_worklib
	,_debug=0
	);

/* cas mysess terminate;   */




