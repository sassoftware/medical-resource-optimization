%let data_path=/ordsrv3/OR_CENTER/FILES/Cleveland Clinic/Customer_Input/05112020;
%let inlib = cc;

cas mysess;
caslib _all_ assign;

proc casutil;
   load file="&data_path./Input_Opt_Parameters.csv" casout="Input_Opt_Parameters" replace;
   droptable casdata="Input_Opt_Parameters" incaslib="&inlib" quiet; run;
   promote casdata="Input_Opt_Parameters" outcaslib="&inlib" incaslib="casuser" drop;

   load file="&data_path./Input_Utilization.csv" casout="Input_Utilization" replace;
   droptable casdata="Input_Utilization" incaslib="&inlib" quiet; run;
   promote casdata="Input_Utilization" outcaslib="&inlib" incaslib="casuser" drop;

   load file="&data_path./Input_Service_Attributes.csv" casout="Input_Service_Attributes" replace;
   droptable casdata="Input_Service_Attributes" incaslib="&inlib" quiet; run;
   promote casdata="Input_Service_Attributes" outcaslib="&inlib" incaslib="casuser" drop;

   load file="&data_path./Input_Demand.csv" casout="Input_Demand" replace;
   droptable casdata="Input_Demand" incaslib="&inlib" quiet; run;
   promote casdata="Input_Demand" outcaslib="&inlib" incaslib="casuser" drop;

   load file="&data_path./Input_Financials.csv" casout="Input_Financials" replace;
   droptable casdata="Input_Financials" incaslib="&inlib" quiet; run;
   promote casdata="Input_Financials" outcaslib="&inlib" incaslib="casuser" drop;

   load file="&data_path./Input_Capacity.csv" casout="Input_Capacity" replace;
   droptable casdata="Input_Capacity" incaslib="&inlib" quiet; run;
   promote casdata="Input_Capacity" outcaslib="&inlib" incaslib="casuser" drop;
quit;

/* Fix date - change to work table later and remove the delete */
%let _worklib = casuser;
%let input_demand = input_demand;

data &_worklib..&input_demand.;
   set &inlib..&input_demand. (rename = (date=datechar));
   date=input(datechar,MMDDYY10.);
   drop datechar;
run;

proc delete data= &inlib..&input_demand.;
run;

data &inlib..&input_demand. (promote=yes);
   set &_worklib..&input_demand.;
run;
