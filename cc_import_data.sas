%let data_path=/ordsrv3/OR_CENTER/FILES/Cleveland Clinic/Customer_Input;

/* Point to ordsrv3 cc library */
libname ccin "/ordsrv3/OR_CENTER/FILES/Cleveland Clinic/Customer_Input";

proc import 
	datafile="&data_path./Input_Demand.csv"
	out=casuser.input_demand
	replace
	dbms=csv;
	guessingrows=max;
quit;

proc import 
	datafile="&data_path./Input_Financials.csv"
	out=casuser.input_financials
	replace
	dbms=csv;
	guessingrows=max;
quit;

proc import 
	datafile="&data_path./Reactivation_Service_Attributes.csv"
	out=casuser.Input_Service_Attributes
	replace
	dbms=csv;
	guessingrows=max;
quit;

proc import 
	datafile="&data_path./Input_Capacity.csv"
	out=casuser.Input_Capacity
	replace
	dbms=csv;
	guessingrows=max;
quit;

proc import 
	datafile="&data_path./Input_Utilization.csv"
	out=casuser.Input_Utilization
	replace
	dbms=csv;
	guessingrows=max;
quit;

proc import 
	datafile="&data_path./Input_Parameters.csv"
	out=casuser.Input_Parameters
	replace
	dbms=csv;
	guessingrows=max;
quit;

proc casutil outcaslib="cc";  
    promote casdata="input_demand";
	promote casdata="Input_Service_Attributes";
	promote casdata="input_financials";
	promote casdata="input_capacity";	
	promote casdata="input_utilization";
	promote casdata="input_parameters";
quit;