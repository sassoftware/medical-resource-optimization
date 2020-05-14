*------------------------------------------------------------------------------*
| Program: cc_med_res_opt_standalone
|*--------------------------------------------------------------------------------* ;

/* Start cas session */
cas mysess;
caslib _all_ assign;

/* Point to the code */
%let my_code_path=/r/sanyo.unx.sas.com/vol/vol920/u92/navikt/casuser/covid/code/MRO/orclus08_git/mro_code;
%include "&my_code_path./cc_execute.sas";
%include "&my_code_path./cc_data_prep.sas";
%include "&my_code_path./cc_forecast_demand.sas";
%include "&my_code_path./cc_optimize.sas";

%let list_scen=%str(scen01, scen02);

%macro cc_run_scenarios(list_scen=);

	%do scen=1 %to %SYSFUNC(countw(&list_scen.,','));
		%let scenario_name=%scan(&list_scen.,&scen.);
		%cc_execute(
			inlib=cc
			,outlib=casuser
			,_worklib=casuser
			,input_utilization=input_utilization
			,input_capacity=input_capacity
			,input_financials=input_financials
			,input_service_attributes=input_service_attributes
			,input_demand=input_demand
			,input_opt_parameters=input_opt_parameters_&scenario_name.
			,scenario_name=&scenario_name.
			,run_dp=1
			,run_fcst=1
			,run_opt=1);
	
	%end;

/* Append all opt outputs into one */

%mend;
%cc_run_scenarios(list_scen=&list_scen.);


