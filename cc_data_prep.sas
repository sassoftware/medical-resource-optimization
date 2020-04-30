*------------------------------------------------------------------------------*
| Program: cc_data_prep
|
| Description: 
|
*-----------------------------------------------------------------------i-------* ;
%macro cc_data_prep(inlib=cc
                   ,outlib=cc
                   ,input_utilization=input_utilization
                   ,input_capacity=input_capacity
                   ,input_financials=input_financials
                   ,input_service_attributes=input_service_attributes
                   ,input_demand=input_demand
                   ,input_opt_parameters=input_opt_parameters
                   ,_worklib=casuser
                   ,_debug=1
                   );

   /*************************/
   /******HOUSEKEEPING*******/
   /*************************/

   /* Do not proceed if previously there have been errors */
   %if &syscc > 4  %then %do;
      %put FATAL: There have been errors BEFORE this macro is executed, exiting from &sysmacroname.;
      %goto EXIT;
   %end;
   %put TRACE: Entering &sysmacroname. with SYSCC=&SYSCC.;

   /* Check missing inputs */
   %if %sysfunc(exist(&inlib..&input_utilization.))=0 %then %do;
      %put FATAL: Missing &inlib..&input_utilization., exiting from &sysmacroname.;
      %goto EXIT;
   %end;     

   %if %sysfunc(exist(&inlib..&input_capacity.))=0 %then %do;
      %put FATAL: Missing &inlib..&input_capacity., from &sysmacroname.;
      %goto EXIT;
   %end; 

   %if %sysfunc(exist(&inlib..&input_financials.))=0 %then %do;
      %put FATAL: Missing &inlib..&input_financials., from &sysmacroname.;
      %goto EXIT;
   %end; 

   %if %sysfunc(exist(&inlib..&input_service_attributes.))=0 %then %do;
      %put FATAL: Missing &inlib..&input_service_attributes., from &sysmacroname.;
      %goto EXIT;
   %end; 

   %if %sysfunc(exist(&inlib..&input_demand.))=0 %then %do;
      %put FATAL: Missing &inlib..&input_demand., from &sysmacroname.;
      %goto EXIT;
   %end; 

   %if %sysfunc(exist(&inlib..&input_opt_parameters.))=0 %then %do;
      %put FATAL: Missing &inlib..&input_opt_parameters., from &sysmacroname.;
      %goto EXIT;
   %end; 

   /* List work tables */
   %let _work_tables=%str(  
			  &_worklib.._missing_values_utilization
              &_worklib.._missing_values_capacity
              &_worklib.._missing_values_financials
              &_worklib.._missing_values_service_attrs
              &_worklib.._missing_values_demand
              &_worklib.._missing_values_opt_parameters
              &_worklib.._duplicate_rows_utilization
              &_worklib.._duplicate_rows_capacity
              &_worklib.._duplicate_rows_financials
              &_worklib.._duplicate_rows_service_attrs
              &_worklib.._duplicate_rows_demand
              &_worklib.._duplicate_rows_opt_parameters
              &_worklib.._dropped_rows_utilization
              &_worklib.._dropped_rows_capacity
              &_worklib.._dropped_rows_financials
              &_worklib.._dropped_rows_service_attributes
              &_worklib.._dropped_rows_demand
              &_worklib.._dropped_rows_opt_parameters;
        );	

	  /* List output tables */
	   %let output_tables=%str(         
	         &_worklib..input_utilization_pp
             &_worklib..input_capacity_pp
             &_worklib..input_financials_pp
             &_worklib..input_service_attributes_pp
             &_worklib..input_demand_pp
             &_worklib..input_opt_parameters_pp;
	         );

	 /*Delete output data if already exists */
		proc delete data= &output_tables.;
		run;
	
	 /* Delete work data if already exists */
		proc delete data= &_work_tables.;
		run;
  
	/************************************/
	/************ANALYTICS *************/
	/***********************************/
	
	/* For debugging purposes */
/* 		%let input_utilization=input_utilization; */
/* 		%let input_capacity=input_capacity; */
/* 		%let input_financials=input_financials; */
/* 		%let input_service_attributes=input_service_attributes; */
/*         %let input_demand=input_demand; */
/*         %let input_opt_parameters=input_opt_parameters; */

   /* Check each table for missing values and duplicate rows. Write these to separate output tables 
      to be used for error handling. */
   data &_worklib..input_utilization_pp
        &_worklib.._missing_values_utilization
        &_worklib.._duplicate_rows_utilization;
      set &inlib..&input_utilization;
      by facility service_line sub_service ip_op_indicator med_surg_indicator resource;
      if facility = '' or service_line = '' or sub_service = '' 
         or ip_op_indicator = '' or med_surg_indicator = '' or resource = '' or utilization_mean = .
         then output &_worklib.._missing_values_utilization;
      else if first.resource then output &_worklib..input_utilization_pp;
      else output  &_worklib.._duplicate_rows_utilization;
   run;

   data &_worklib..input_capacity_pp
        &_worklib.._missing_values_capacity
        &_worklib.._duplicate_rows_capacity;
      set &inlib..&input_capacity;
      by facility service_line sub_service resource;
      if facility = '' or service_line = '' or sub_service = '' 
         or resource = '' or capacity = .
         then output &_worklib.._missing_values_capacity;
      else if first.resource then output &_worklib..input_capacity_pp;
      else output &_worklib.._duplicate_rows_capacity;
   run;
   
   data &_worklib..input_financials_pp
        &_worklib.._missing_values_financials
        &_worklib.._duplicate_rows_financials;
      set &inlib..&input_financials;
      by facility service_line sub_service ip_op_indicator med_surg_indicator;
      if facility = '' or service_line = '' or sub_service = '' 
         or ip_op_indicator = '' or med_surg_indicator = '' or revenue = . or margin = .
         then output &_worklib.._missing_values_financials;
      else if first.med_surg_indicator then output &_worklib..input_financials_pp;
      else output &_worklib.._duplicate_rows_financials;
   run;
      
   data &_worklib..input_service_attributes_pp
        &_worklib.._missing_values_service_attrs
        &_worklib.._duplicate_rows_service_attrs;
      set &inlib..&input_service_attributes;
      by facility service_line sub_service ip_op_indicator med_surg_indicator;
      if facility = '' or service_line = '' or sub_service = '' 
         or ip_op_indicator = '' or med_surg_indicator = '' or num_cancelled = . or length_stay_mean = .
         then output &_worklib.._missing_values_service_attrs;
      else if first.med_surg_indicator then output &_worklib..input_service_attributes_pp;
      else output &_worklib.._duplicate_rows_service_attrs;
   run;

   data &_worklib..input_demand_pp
        &_worklib.._missing_values_demand
        &_worklib.._duplicate_rows_demand;
      set &inlib..&input_demand;
      by facility service_line sub_service ip_op_indicator med_surg_indicator date;
      if facility = '' or service_line = '' or sub_service = '' 
         or ip_op_indicator = '' or med_surg_indicator = '' or date = . or demand = .
         then output &_worklib.._missing_values_demand;
      else if first.date then output &_worklib..input_demand_pp;
      else output &_worklib.._duplicate_rows_demand;
   run;

   data &_worklib..input_opt_parameters_pp
        &_worklib.._missing_values_opt_parameters
        &_worklib.._duplicate_rows_opt_parameters;
      set &inlib..&input_opt_parameters;
      by facility service_line sub_service parm_name;
      if facility = '' or service_line = '' or sub_service = '' or parm_name = ''
         then output &_worklib.._missing_values_opt_parameters;
      else if first.parm_name then output &_worklib..input_opt_parameters_pp;
      else output &_worklib.._duplicate_rows_opt_parameters;
   run;   
      
   /* Now that we have removed missing values from all the tables, we need to get a complete set of
      facility/service_line/sub_service/ip_op_indicator/med_surg_indicator that is common across all the 
      tables that use this granularity. First we create the complete set for each table, then merge them to 
      find the union. Note that I am not including sets_complete_utilization, because there
      might be some facility/service/subservice combinations that don't use any resources, but we 
      still want to include them in the optimization problem because they might use COVID-19 tests, which 
      are NOT included in the utilization or capacity tables.*/

   data &_worklib..sets_complete_financials;
      set &_worklib..input_financials_pp;
      by facility service_line sub_service ip_op_indicator med_surg_indicator;
      if first.med_surg_indicator then output;
   run;

   data &_worklib..sets_complete_service_attributes;
      set &_worklib..input_service_attributes_pp;
      by facility service_line sub_service ip_op_indicator med_surg_indicator;
      if first.med_surg_indicator then output;
   run;

   data &_worklib..sets_complete_demand;
      set &_worklib..input_demand_pp;
      by facility service_line sub_service ip_op_indicator med_surg_indicator;
      if first.med_surg_indicator then output;
   run;
    
   /* Create master_sets_union. */
   data &_worklib..master_sets_union;
      merge &_worklib..sets_complete_financials (in=in_financials)
            &_worklib..sets_complete_service_attributes (in=in_service_attributes)
            &_worklib..sets_complete_demand (in=in_demand);
      by facility service_line sub_service ip_op_indicator med_surg_indicator;
      if in_financials and in_service_attributes and in_demand then output;
   run;
   
   proc datasets nolist lib=&_worklib;
      delete sets_complete_financials
             sets_complete_service_attributes
             sets_complete_demand;
   quit;

   /* Remove the rows from each table that are not in the master set union */
   data &_worklib..input_utilization_pp
        &_worklib.._dropped_rows_utilization;
      set &_worklib..input_utilization_pp;
      if _n_ = 1 then do;
         declare hash h0(dataset:"&_worklib..master_sets_union");
         h0.defineKey('facility','service_line','sub_service','ip_op_indicator','med_surg_indicator');
         h0.defineDone();
      end;
      rc0 = h0.find();
      if rc0 = 0 then output &_worklib..input_utilization_pp;
      else output &_worklib.._dropped_rows_utilization;
      drop rc0;
   run;
   
   data &_worklib..input_financials_pp
        &_worklib.._dropped_rows_financials;
      set &_worklib..input_financials_pp;
      if _n_ = 1 then do;
         declare hash h0(dataset:"&_worklib..master_sets_union");
         h0.defineKey('facility','service_line','sub_service','ip_op_indicator','med_surg_indicator');
         h0.defineDone();
      end;
      rc0 = h0.find();
      if rc0 = 0 then output &_worklib..input_financials_pp;
      else output &_worklib.._dropped_rows_financials;
      drop rc0;
   run;

   data &_worklib..input_service_attributes_pp
        &_worklib.._dropped_rows_service_attributes;
      set &_worklib..input_service_attributes_pp;
      if _n_ = 1 then do;
         declare hash h0(dataset:"&_worklib..master_sets_union");
         h0.defineKey('facility','service_line','sub_service','ip_op_indicator','med_surg_indicator');
         h0.defineDone();
      end;
      rc0 = h0.find();
      if rc0 = 0 then output &_worklib..input_service_attributes_pp;
      else output &_worklib.._dropped_rows_service_attributes;
      drop rc0;
   run;

   data &_worklib..input_demand_pp
        &_worklib.._dropped_rows_demand;
      set &_worklib..input_demand_pp;
      if _n_ = 1 then do;
         declare hash h0(dataset:"&_worklib..master_sets_union");
         h0.defineKey('facility','service_line','sub_service','ip_op_indicator','med_surg_indicator');
         h0.defineDone();
      end;
      rc0 = h0.find();
      if rc0 = 0 then output &_worklib..input_demand_pp;
      else output &_worklib.._dropped_rows_demand;
      drop rc0;
   run;

   /* Remove the rows from &input_capacity that do not correspond to any facility/service_line/sub_service/resource
      remaining in utilization, but keep the rows that have ALL for any of the fields */
   data &_worklib..utilization_resources;
      set &_worklib..input_utilization_pp;
      by facility service_line sub_service resource;
      if first.resource;
   run;
   
   data &_worklib..input_capacity_pp
        &_worklib.._dropped_rows_capacity;
      set &_worklib..input_capacity_pp;
      if _n_ = 1 then do;
         declare hash h0(dataset:"&_worklib..utilization_resources");
         h0.defineKey('facility','service_line','sub_service','resource');
         h0.defineDone();
      end;
      rc0 = h0.find();
      if rc0 = 0 or upcase(facility)='ALL' or upcase(service_line)='ALL' or upcase(sub_service='ALL')
         or upcase(resource)='ALL' then output &_worklib..input_capacity_pp;
      else output &_worklib.._dropped_rows_capacity;
      drop rc0;
   run;

   proc datasets nolist lib=&_worklib;
      delete utilization_resources;
   quit;

   /* Remove the rows from &input_opt_parameters that do not correspond to any facility/service_line/sub_service
      remaining in master_sets_union, but keep the rows that have ALL for any of the fields */
   data &_worklib..master_sets_union;
      set &_worklib..master_sets_union (keep=facility service_line sub_service);
      by facility service_line sub_service;
      if first.sub_service;
   run;
   
   data &_worklib..input_opt_parameters_pp
        &_worklib.._dropped_rows_opt_parameters;
      set &_worklib..input_opt_parameters_pp;
      if _n_ = 1 then do;
         declare hash h0(dataset:"&_worklib..master_sets_union");
         h0.defineKey('facility','service_line','sub_service');
         h0.defineDone();
      end;
      rc0 = h0.find();
      if rc0 = 0 or upcase(facility)='ALL' or upcase(service_line)='ALL' or upcase(sub_service)='ALL'
         then output &_worklib..input_opt_parameters_pp;
      else output &_worklib.._dropped_rows_opt_parameters;
      drop rc0;
   run;

   proc datasets nolist lib=&_worklib;
      delete master_sets_union;
   quit;

   /* Drop the error handling tables that have zero rows. */
   proc sql noprint;
      select memname 
         into :table_drop_list separated by ' '
         from sashelp.vtable
         where upcase(libname) in ("%upcase(&_worklib)")
            and nobs = 0
            and (upcase(substr(memname,1,14))='_DROPPED_ROWS_'
                 or upcase(substr(memname,1,16))='_DUPLICATE_ROWS_'
                 or upcase(substr(memname,1,16))='_MISSING_VALUES_');
   quit;

   %if &table_drop_list ne %str() %then %do;
      proc datasets nolist lib=&_worklib;
         delete &table_drop_list;
      quit;
   %end;

   /*************************/
   /******HOUSEKEEPING*******/
   /*************************/

   %if &_debug.=0  %then %do;
	proc delete data= &_work_tables.;
	run;
   %end;

   %EXIT:
   %put TRACE: Leaving &sysmacroname. with SYSCC=&SYSCC.;

%mend;
