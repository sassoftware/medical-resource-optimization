*------------------------------------------------------------------------------*
| Program: cc_data_prep
|
| Description: 
|
*------------------------------------------------------------------------------* ;
%macro cc_data_prep(inlib=cc
                   ,outlib=cc
                   ,input_utilization=input_utilization
                   ,input_capacity=input_capacity
                   ,input_financials=input_financials
                   ,input_service_attributes=input_service_attributes
                   ,input_demand=input_demand
                   ,input_opt_parameters=input_opt_parameters
				   ,output_dp_exceptions=output_dp_exceptions
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
              &_worklib.._invalid_values_utilization
              &_worklib.._invalid_values_capacity
              &_worklib.._invalid_values_financials
              &_worklib.._invalid_values_service_attrs
              &_worklib.._invalid_values_demand
              &_worklib.._invalid_values_opt_parameters
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
              &_worklib.._dropped_rows_opt_parameters
              &_worklib..data_exceptions
              work.inlib_contents
              );

   /* List output tables */
   %let output_tables=%str(         
             &_worklib..input_utilization_pp
             &_worklib..input_capacity_pp
             &_worklib..input_financials_pp
             &_worklib..input_service_attributes_pp
             &_worklib..input_demand_pp
             &_worklib..input_opt_parameters_pp
             &outlib..&output_dp_exceptions
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
/*   %let input_utilization=input_utilization; */
/*   %let input_capacity=input_capacity; */
/*   %let input_financials=input_financials; */
/*   %let input_service_attributes=input_service_attributes; */
/*   %let input_demand=input_demand; */
/*   %let input_opt_parameters=input_opt_parameters; */

   /* Find max length of each column across all the tables */
   proc contents data=&inlib.._all_ out=work.inlib_contents noprint;
   run;
   
   data work.inlib_contents;
      set work.inlib_contents;
      where upcase(memname) in ("%upcase(&input_utilization)", 
                                "%upcase(&input_capacity)", 
                                "%upcase(&input_financials)", 
                                "%upcase(&input_service_attributes)", 
                                "%upcase(&input_demand)", 
                                "%upcase(&input_opt_parameters)");
   run;
   
   proc sql noprint;
      select max(length) into :facility_len from work.inlib_contents
         where upcase(name) = 'FACILITY';
      select max(length) into :service_line_len from work.inlib_contents
         where upcase(name) = 'SERVICE_LINE';
      select max(length) into :sub_service_len from work.inlib_contents
         where upcase(name) = 'SUB_SERVICE';
      select max(length) into :ip_op_len from work.inlib_contents
         where upcase(name) = 'IP_OP_INDICATOR';
      select max(length) into :med_surg_len from work.inlib_contents
         where upcase(name) = 'MED_SURG_INDICATOR';
      select max(length) into :resource_len from work.inlib_contents
         where upcase(name) = 'RESOURCE';
   quit;

   /* Check each table for invalid values and duplicate rows. Write these to separate output tables 
      to be used for error handling. */
   data &_worklib..input_utilization_pp
        &_worklib.._invalid_values_utilization
        &_worklib.._duplicate_rows_utilization;
      format facility $&facility_len..;
      format service_line $&service_line_len..;
      format sub_service $&sub_service_len..;
      format ip_op_indicator $&ip_op_len..;
      format med_surg_indicator $&med_surg_len..;
      format resource $&resource_len..;
      
      set &inlib..&input_utilization;
      by facility service_line sub_service ip_op_indicator med_surg_indicator resource;
      ip_op_indicator = upcase(ip_op_indicator);
      med_surg_indicator = upcase(med_surg_indicator);
     
      if first.resource then do;
         if facility = '' or upcase(facility) = 'ALL' 
            or service_line = '' or upcase(service_line) = 'ALL'
            or sub_service = '' or upcase(sub_service) = 'ALL' 
            or ip_op_indicator not in ('I','O') 
            or med_surg_indicator not in ('MED','SURG')
            or resource = '' or upcase(resource) = 'ALL'
            or utilization_mean = . or utilization_mean < 0
            then output &_worklib.._invalid_values_utilization;
         else output &_worklib..input_utilization_pp;
      end;
      else output  &_worklib.._duplicate_rows_utilization;
   run;

   data &_worklib..input_capacity_pp
        &_worklib.._invalid_values_capacity
        &_worklib.._duplicate_rows_capacity;
      format facility $&facility_len..;
      format service_line $&service_line_len..;
      format sub_service $&sub_service_len..;
      format resource $&resource_len..;
      
      set &inlib..&input_capacity;
      by facility service_line sub_service resource;
      if upcase(facility) = 'ALL' then facility = 'ALL';
      if upcase(service_line) = 'ALL' then service_line = 'ALL';
      if upcase(sub_service) = 'ALL' then sub_service = 'ALL';
      
      if first.resource then do;
         if facility = '' or service_line = '' or sub_service = '' 
            or resource = '' or upcase(resource)='ALL'
            or capacity = . or capacity < 0
            then output &_worklib.._invalid_values_capacity;
         else output &_worklib..input_capacity_pp;
      end;
      else output &_worklib.._duplicate_rows_capacity;
   run;
   
   data &_worklib..input_financials_pp
        &_worklib.._invalid_values_financials
        &_worklib.._duplicate_rows_financials;
      format facility $&facility_len..;
      format service_line $&service_line_len..;
      format sub_service $&sub_service_len..;
      format ip_op_indicator $&ip_op_len..;
      format med_surg_indicator $&med_surg_len..;
      
      set &inlib..&input_financials;
      by facility service_line sub_service ip_op_indicator med_surg_indicator;
      ip_op_indicator = upcase(ip_op_indicator);
      med_surg_indicator = upcase(med_surg_indicator);
      
      if first.med_surg_indicator then do;
         if facility = '' or upcase(facility) = 'ALL' 
            or service_line = '' or upcase(service_line) = 'ALL'
            or sub_service = '' or upcase(sub_service) = 'ALL' 
            or ip_op_indicator not in ('I','O') 
            or med_surg_indicator not in ('MED','SURG')
            or revenue = . or margin = .
            then output &_worklib.._invalid_values_financials;
         else output &_worklib..input_financials_pp;
      end;
      else output &_worklib.._duplicate_rows_financials;
   run;
      
   data &_worklib..input_service_attributes_pp
        &_worklib.._invalid_values_service_attrs
        &_worklib.._duplicate_rows_service_attrs;
      format facility $&facility_len..;
      format service_line $&service_line_len..;
      format sub_service $&sub_service_len..;
      format ip_op_indicator $&ip_op_len..;
      format med_surg_indicator $&med_surg_len..;
      
      set &inlib..&input_service_attributes;
      by facility service_line sub_service ip_op_indicator med_surg_indicator;
      ip_op_indicator = upcase(ip_op_indicator);
      med_surg_indicator = upcase(med_surg_indicator);

      if first.med_surg_indicator then do;
         if facility = '' or upcase(facility) = 'ALL' 
            or service_line = '' or upcase(service_line) = 'ALL'
            or sub_service = '' or upcase(sub_service) = 'ALL' 
            or ip_op_indicator not in ('I','O') 
            or med_surg_indicator not in ('MED','SURG')
            or num_cancelled = . or num_cancelled < 0
            or length_stay_mean = . or length_stay_mean < 0
            then output &_worklib.._invalid_values_service_attrs;
         else output &_worklib..input_service_attributes_pp;
      end;
      else output &_worklib.._duplicate_rows_service_attrs;
   run;

   data &_worklib..input_demand_pp
        &_worklib.._invalid_values_demand
        &_worklib.._duplicate_rows_demand;
      format facility $&facility_len..;
      format service_line $&service_line_len..;
      format sub_service $&sub_service_len..;
      format ip_op_indicator $&ip_op_len..;
      format med_surg_indicator $&med_surg_len..;
      
      set &inlib..&input_demand;
      by facility service_line sub_service ip_op_indicator med_surg_indicator date;
      ip_op_indicator = upcase(ip_op_indicator);
      med_surg_indicator = upcase(med_surg_indicator);
      
      if first.date then do;
         if facility = '' or upcase(facility) = 'ALL' 
            or service_line = '' or upcase(service_line) = 'ALL'
            or sub_service = '' or upcase(sub_service) = 'ALL' 
            or ip_op_indicator not in ('I','O') 
            or med_surg_indicator not in ('MED','SURG')
            or date = . or date < 0
            or demand = . or demand < 0
            then output &_worklib.._invalid_values_demand;
         else output &_worklib..input_demand_pp;
      end;
      else output &_worklib.._duplicate_rows_demand;
   run;

   data &_worklib..input_opt_parameters_pp
        &_worklib.._invalid_values_opt_parameters
        &_worklib.._duplicate_rows_opt_parameters;
      format facility $&facility_len..;
      format service_line $&service_line_len..;
      format sub_service $&sub_service_len..;
      
      set &inlib..&input_opt_parameters;
      by facility service_line sub_service parm_name;
      if upcase(facility) = 'ALL' then facility = 'ALL';
      if upcase(service_line) = 'ALL' then service_line = 'ALL';
      if upcase(sub_service) = 'ALL' then sub_service = 'ALL';
      
      if first.parm_name then do;
         if facility = '' or service_line = '' or sub_service = '' or parm_name = ''
            then output &_worklib.._invalid_values_opt_parameters;
         else output &_worklib..input_opt_parameters_pp;
      end;
      else output &_worklib.._duplicate_rows_opt_parameters;
   run;   
      
   /* Now that we have removed invalid values from all the tables, we need to get a complete set of
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
   
   %let keep_list_full = facility service_line sub_service ip_op_indicator med_surg_indicator;
   %let keep_list_short = facility service_line sub_service;
   
   data &_worklib..data_exceptions;
      format table $32.;
      set &_worklib.._invalid_values_utilization (in=invalid1 keep=&keep_list_full resource)
          &_worklib.._invalid_values_capacity (in=invalid2 keep=&keep_list_short resource)
          &_worklib.._invalid_values_financials (in=invalid3 keep=&keep_list_full)
          &_worklib.._invalid_values_service_attrs (in=invalid4 keep=&keep_list_full)
          &_worklib.._invalid_values_demand (in=invalid5 keep=&keep_list_full)
          &_worklib.._invalid_values_opt_parameters (in=invalid6 keep=&keep_list_short)
          
          &_worklib.._duplicate_rows_utilization (in=dup1 keep=&keep_list_full resource)
          &_worklib.._duplicate_rows_capacity (in=dup2 keep=&keep_list_short resource)
          &_worklib.._duplicate_rows_financials (in=dup3 keep=&keep_list_full)
          &_worklib.._duplicate_rows_service_attrs (in=dup4 keep=&keep_list_full)
          &_worklib.._duplicate_rows_demand (in=dup5 keep=&keep_list_full)
          &_worklib.._duplicate_rows_opt_parameters (in=dup6 keep=&keep_list_short)
          
          &_worklib.._dropped_rows_utilization (in=drop1 keep=&keep_list_full resource)
          &_worklib.._dropped_rows_capacity (in=drop2 keep=&keep_list_short resource)
          &_worklib.._dropped_rows_financials (in=drop3 keep=&keep_list_full)
          &_worklib.._dropped_rows_service_attributes (in=drop4 keep=&keep_list_full)
          &_worklib.._dropped_rows_demand (in=drop5 keep=&keep_list_full)
          &_worklib.._dropped_rows_opt_parameters (in=drop6 keep=&keep_list_short);
          
      if invalid1 or dup1 or drop1 then table = 'INPUT_UTILIZATION';
      else if invalid2 or dup2 or drop2 then table = 'INPUT_CAPACITY';
      else if invalid3 or dup3 or drop3 then table = 'INPUT_FINANCIALS';
      else if invalid4 or dup4 or drop4 then table = 'INPUT_SERVICE_ATTRIBUTES';
      else if invalid5 or dup5 or drop5 then table = 'INPUT_DEMAND';
      else if invalid6 or dup6 or drop6 then table = 'INPUT_OPT_PARAMETERS';
     
      invalid_value = 0;
      duplicate_value = 0;
      mismatch_hierarchy = 0;
      
      if invalid1 or invalid2 or invalid3 or invalid4 or invalid5 or invalid6 then invalid_value = 1;
      else if dup1 or dup2 or dup3 or dup4 or dup5 or dup6 then duplicate_value = 1;
      else if drop1 or drop2 or drop3 or drop4 or drop5 or drop6 then mismatch_hierarchy = 1;
   run;

   data &outlib..&output_dp_exceptions;
      retain table reason invalid_flag duplicate_flag mismatch_flag;
      format reason $128.;
      set &_worklib..data_exceptions;
      by table &keep_list_full resource;
      
      if first.resource then do;
         reason = '';
         invalid_flag = 0;
         duplicate_flag = 0;
         mismatch_flag = 0;
      end;
      if invalid_value = 1 then invalid_flag = 1;
      if duplicate_value = 1 then duplicate_flag = 1;
      if mismatch_hierarchy = 1 then mismatch_flag = 1;
      
      if last.resource then do;
         if invalid_flag = 1 then reason = strip(reason) || '; Invalid values';
         if duplicate_flag = 1 then reason = strip(reason) || '; Duplicate row';
         if mismatch_flag = 1 then reason = strip(reason) || '; Hierarchy mismatch';
         if reason ne '' then reason = substr(reason,3);
         output;
      end;
      
      drop invalid_value duplicate_value mismatch_hierarchy
           invalid_flag duplicate_flag mismatch_flag;
   run;

   /* Drop the error handling tables that have zero rows. */
   proc sql noprint;
      select memname 
         into :table_drop_list separated by ' '
         from sashelp.vtable
         where upcase(libname) in ("%upcase(&_worklib)")
            and nobs = 0
            and (upcase(substr(memname,1,14))='_DROPPED_ROWS_'
                 or upcase(substr(memname,1,16))='_DUPLICATE_ROWS_'
                 or upcase(substr(memname,1,16))='_INVALID_VALUES_');
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
