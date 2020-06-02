%let data_path=<path to input data>; /* Add your data path on this line */
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

   load file="&data_path./Input_Financials.csv" casout="Input_Financials" replace;
   droptable casdata="Input_Financials" incaslib="&inlib" quiet; run;
   promote casdata="Input_Financials" outcaslib="&inlib" incaslib="casuser" drop;

   load file="&data_path./Input_Capacity.csv" casout="Input_Capacity" replace;
   droptable casdata="Input_Capacity" incaslib="&inlib" quiet; run;
   promote casdata="Input_Capacity" outcaslib="&inlib" incaslib="casuser" drop;

   load file="&data_path./Input_Demand.csv" casout="Input_Demand" replace;
quit;

data casuser.Input_Demand;
   set casuser.Input_Demand (rename = (date=datechar));
   date=input(datechar,MMDDYY10.);
   drop datechar;
run;

proc casutil;
   droptable casdata="Input_Demand" incaslib="&inlib" quiet; run;
   promote casdata="Input_Demand" outcaslib="&inlib" incaslib="casuser" drop;
quit;
