*------------------------------------------------------------------------------*
| Program: cc_med_res_opt_standalone
|
| Description: Required datasets to be placed caslib CC: 
|	INPUT_DEMAND
|	INPUT_CAPACITY
| 	INPUT_FINANCIALS
|	INPUT_SERVICE_ATTR
|*--------------------------------------------------------------------------------* ;

/* Start cas session */
cas mysess;
/* cas mysess sessopts=(nworkers=2); */
caslib _all_ assign;

/* Point to the code */
%let my_code_path=/r/ge.unx.sas.com/vol/vol410/u41/supazh/casuser/Cleveland_Clinic/;
%include "&my_code_path./cc_med_res_opt.sas";

/*
%include "&my_code_path./NetworkAnalytics/lna_network.sas";
%include "&my_code_path./NetworkAnalytics/lna_post_process.sas";
%include "&my_code_path./NetworkAnalytics/code/lna_va_data_prep.sas";
*/

/* Define libraries */
%let inlib=cc;
%let outlib=cc;
%let _worklib=casuser;

/* Submit code */
%cc_med_res_opt(
	inlib=&inlib.
	,outlib=&outlib.
	,input_demand=input_demand
	,input_capacity =input_capacity
	,input_financials=input_financials
	,input_service_attr=input_service_attr
	,_worklib=&_worklib.);


/* cas mysess terminate;   */
