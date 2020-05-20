*------------------------------------------------------------------------------*
| Program: cc_data_prep
|
| Description: 
|
|Example: include_str=%str(facility in ('Hillcrest','ALL') ) 
| 
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
                   ,unique_param_list =%str(PLANNING_HORIZON LOS_ROUNDING_THRESHOLD FORECAST_MODEL 
                                            FILTER_SERV_NOT_USING_RESOURCES OPTIMIZATION_START_DATE)
                   ,fractional_param_list =%str(SECONDARY_OBJECTIVE_TOLERANCE RAPID_TEST_DA MIN_DEMAND_RATIO 
                                            EMER_SURGICAL_PTS_RATIO ICU_MAX_UTILIZATION)
                   ,binary_param_list =%str(REMOVE_DEMAND_CONSTRAINTS REMOVE_COVID_CONSTRAINTS ALLOW_OPENING_ONLY_ON_PHASE ALREADY_OPEN 
                                            TEST_VISITORS FILTER_SERV_NOT_USING_RESOURCES)
                   ,non_hier_param_list =%str(TEST_DAYS_BA RAPID_TEST_DA DATE_PHASE_ RAPID_TEST_PHASE_ NOT_RAPID_TEST_PHASE_ 
                                              ALLOW_OPENING_ONLY_ON_PHASE SECONDARY_OBJECTIVE_TOLERANCE REMOVE_DEMAND_CONSTRAINTS
                                              REMOVE_COVID_CONSTRAINTS TEST_VISITORS TEST_FREQ_DAYS HOLD_RAPID_COVID_TESTS 
                                              HOLD_NOT_RAPID_COVID_TESTS PLANNING_HORIZON LOS_ROUNDING_THRESHOLD FORECAST_MODEL
                                              FILTER_SERV_NOT_USING_RESOURCES OPTIMIZATION_START_DATE)
                   ,include_str=%str(1=1)
                   ,exclude_str=%str(0=1)
                   ,output_hierarchy_mismatch=output_hierarchy_mismatch
                   ,output_resource_mismatch=output_resource_mismatch
                   ,output_invalid_values=output_invalid_values
                   ,output_duplicate_rows=output_duplicate_rows
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
              &_worklib.._hierarchy_utilization
              &_worklib.._hierarchy_capacity
              &_worklib.._hierarchy_financials
              &_worklib.._hierarchy_service_attributes
              &_worklib.._hierarchy_demand
              &_worklib.._hierarchies_not_in_util
              &_worklib.._master_sets_union
              &_worklib.._distinct_fac_sl_ss
              &_worklib.._resources_in_utilization
              &_worklib.._util_resources_fac_sl_ss_r
              &_worklib.._util_resources_fac_r
              &_worklib.._util_resources_sl_r
              &_worklib.._util_resources_ss_r
              &_worklib.._util_resources_fac_sl_r
              &_worklib.._util_resources_fac_ss_r
              &_worklib.._util_resources_sl_ss_r
              &_worklib.._util_resources_r
              &_worklib.._tmp_inval_opt_parameters
              work._inlib_contents
              );

   /* List output tables */
   %let output_tables=%str(         
             &_worklib..input_utilization_pp
             &_worklib..input_capacity_pp
             &_worklib..input_financials_pp
             &_worklib..input_service_attributes_pp
             &_worklib..input_demand_pp
             &_worklib..input_opt_parameters_pp
             &outlib..&output_hierarchy_mismatch.
             &outlib..&output_resource_mismatch.
             &outlib..&output_invalid_values.
             &outlib..&output_duplicate_rows.
             );

   /*Delete output data if already exists */
   %let i = 1;
   %let table = %scan(&output_tables, &i, ' ');
   %do %while (&table ne %str());
      %if %sysfunc(exist(&table)) %then %do;
         proc delete data= &table.;
         run;
      %end;
      %let i = %eval(&i + 1);
      %let table = %scan(&output_tables, &i, ' ');
   %end;

   /* Delete work data if already exists */
   %let i = 1;
   %let table = %scan(&_work_tables, &i, ' ');
   %do %while (&table ne %str());
      %if %sysfunc(exist(&table)) %then %do;
         proc delete data= &table.;
         run;
      %end;
      %let i = %eval(&i + 1);
      %let table = %scan(&_work_tables, &i, ' ');
   %end;

   /* Delete all temp tables that start with _mrochar_, if they exist. These are not included in &_work_tables because 
      the names are not fixed, they depend on the input parameters. */
   proc datasets nolist lib=&_worklib.;
      delete _mrochar_:;
   quit;
   
  
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

   /* There are some opt parameters that must be the same for all scenarios. If there is more than one distinct value,
      we'll stop with an error. Note that we don't consider scenarios where the parameter has not been specified, and therefore
      might have a different default value. If a single value has been specified for ANY scenario, we'll use that value for 
      ALL scenarios. */

   %do i = 1 %to %SYSFUNC(countw(&unique_param_list.,' '));
      %let param = %scan(&unique_param_list., &i., ' ');
      %let num_distinct_values = 0;
      proc sql noprint;
         select count(distinct lowcase(parm_value)) into :num_distinct_values
         from &inlib..&input_opt_parameters.
         where upcase(parm_name) = "&param.";
      quit;
      
      %if &num_distinct_values. > 1 %then %do;
         %put ERROR: The parameter &param. has more than one distinct value in &inlib..&input_opt_parameters..;
        /* Do a dummy data step that will force syscc > 4. We know that work.inlib_contents doesn't exist because
           we've just deleted it in a previous step, so we're going to try to use it. */
        data dummy_table;
           set work._inlib_contents;
        run;
      %end;
   %end;
   %if &syscc. > 4 %then %goto EXIT;
   
   proc contents data=&inlib.._all_ out=work._inlib_contents noprint;
   run;
   
   data work._inlib_contents;
      set work._inlib_contents;
      where upcase(memname) in ("%upcase(&input_utilization)", 
                                "%upcase(&input_capacity)", 
                                "%upcase(&input_financials)", 
                                "%upcase(&input_service_attributes)", 
                                "%upcase(&input_demand)", 
                                "%upcase(&input_opt_parameters)")
         and upcase(name) in ('SCENARIO_NAME','FACILITY','SERVICE_LINE','SUB_SERVICE',
                              'IP_OP_INDICATOR','MED_SURG_INDICATOR','RESOURCE','PARM_NAME');
   run;
   
   /* For variables that already exist as character type in at least one table, 
      find the longest length */
   proc sql noprint;
      select max(length) into :len_scenario_name from work._inlib_contents
         where upcase(name) = 'SCENARIO_NAME' and type = 2;
      select max(length) into :len_facility from work._inlib_contents
         where upcase(name) = 'FACILITY' and type = 2;
      select max(length) into :len_service_line from work._inlib_contents
         where upcase(name) = 'SERVICE_LINE' and type = 2;
      select max(length) into :len_sub_service from work._inlib_contents
         where upcase(name) = 'SUB_SERVICE' and type = 2;
      select max(length) into :len_ip_op_indicator from work._inlib_contents
         where upcase(name) = 'IP_OP_INDICATOR' and type = 2;
      select max(length) into :len_med_surg_indicator from work._inlib_contents
         where upcase(name) = 'MED_SURG_INDICATOR' and type = 2;
      select max(length) into :len_resource from work._inlib_contents
         where upcase(name) = 'RESOURCE' and type = 2;
      select max(length) into :len_parm_name from work._inlib_contents
         where upcase(name) = 'PARM_NAME' and type = 2;
   quit;
   
   /* Find varchar variables that need to be converted to char */
   proc sql noprint;
      select distinct memname, count(distinct memname) 
         into :memname_list separated by ' ',
              :num_tables_convert
         from work._inlib_contents
         where type = 6;
   quit;
   
   /* Convert each varchar variable to char */
   %do i = 1 %to &num_tables_convert;
      %let tb = %scan(&memname_list, &i);

      proc sql noprint;
         select name, count(*) 
            into :name_list separated by ' ',
                 :num_vars_convert
            from work._inlib_contents
            where memname = "&tb" and type = 6;
         %do j = 1 %to &num_vars_convert;
            select max(length(%scan(&name_list, &j))) 
               into :varlen&j
               from &inlib..&tb;
         %end;
      quit;
            
      data &_worklib.._mrochar_%substr(&tb,1,%sysfunc(min(23,%length(&tb))));
         set &inlib..&tb (rename=(%do j = 1 %to &num_vars_convert;
                                     %scan(&name_list, &j) = tempvar&j
                                  %end;
                                  ));
         %do j = 1 %to &num_vars_convert;
            %let vv = %scan(&name_list, &j);
            length &vv $&&varlen&j;
            &vv = tempvar&j;
            drop tempvar&j;
         %end;
      run;
      
      /* Adjust column length macro variables if the new column has longer length */
      %do j = 1 %to &num_vars_convert;
         %let vv = %scan(&name_list, &j);
         %if %sysevalf(&&varlen&j > &&len_&vv) %then %let len_&vv = &&varlen&j;
      %end;
   %end;

   /* Initialize temp table names */   
   %if %sysfunc(exist(&_worklib.._mrochar_%substr(&input_capacity,1,%sysfunc(min(23,%length(&input_capacity)))))) 
      %then %let input_capacity_table = &_worklib.._mrochar_%substr(&input_capacity,1,%sysfunc(min(23,%length(&input_capacity))));
   %else %let input_capacity_table = &inlib..&input_capacity;
   
   %if %sysfunc(exist(&_worklib.._mrochar_%substr(&input_demand,1,%sysfunc(min(23,%length(&input_demand)))))) 
      %then %let input_demand_table = &_worklib.._mrochar_%substr(&input_demand,1,%sysfunc(min(23,%length(&input_demand))));
   %else %let input_demand_table = &inlib..&input_demand;

   %if %sysfunc(exist(&_worklib.._mrochar_%substr(&input_financials,1,%sysfunc(min(23,%length(&input_financials)))))) 
      %then %let input_financials_table = &_worklib.._mrochar_%substr(&input_financials,1,%sysfunc(min(23,%length(&input_financials))));
   %else %let input_financials_table = &inlib..&input_financials;
   
   %if %sysfunc(exist(&_worklib.._mrochar_%substr(&input_opt_parameters,1,%sysfunc(min(23,%length(&input_opt_parameters)))))) 
      %then %let input_opt_parameters_table = &_worklib.._mrochar_%substr(&input_opt_parameters,1,%sysfunc(min(23,%length(&input_opt_parameters))));
   %else %let input_opt_parameters_table = &inlib..&input_opt_parameters;

   %if %sysfunc(exist(&_worklib.._mrochar_%substr(&input_service_attributes,1,%sysfunc(min(23,%length(&input_service_attributes)))))) 
      %then %let input_service_attributes_table = &_worklib.._mrochar_%substr(&input_service_attributes,1,%sysfunc(min(23,%length(&input_service_attributes))));
   %else %let input_service_attributes_table = &inlib..&input_service_attributes;

   %if %sysfunc(exist(&_worklib.._mrochar_%substr(&input_utilization,1,%sysfunc(min(23,%length(&input_utilization)))))) 
      %then %let input_utilization_table = &_worklib.._mrochar_%substr(&input_utilization,1,%sysfunc(min(23,%length(&input_utilization))));
   %else %let input_utilization_table = &inlib..&input_utilization;


   /* Check each table for invalid values and duplicate rows. Write these to separate output tables 
      to be used for error handling. */
   data &_worklib..input_utilization_pp
        &_worklib.._invalid_values_utilization
        &_worklib.._duplicate_rows_utilization
        &_worklib.._hierarchy_utilization;
      length facility $&len_facility;
      length service_line $&len_service_line;
      length sub_service $&len_sub_service;
      length ip_op_indicator $&len_ip_op_indicator;
      length med_surg_indicator $&len_med_surg_indicator;
      length resource $&len_resource;
        
      set &input_utilization_table (where=((&include_str) and not (&exclude_str)));
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
         else do;
            output &_worklib..input_utilization_pp;
            output &_worklib.._hierarchy_utilization;
         end;
      end;
      else output  &_worklib.._duplicate_rows_utilization;
   run;

   data &_worklib..input_capacity_pp
        &_worklib.._invalid_values_capacity
        &_worklib.._duplicate_rows_capacity
        &_worklib.._hierarchy_capacity;
      length facility $&len_facility;
      length service_line $&len_service_line;
      length sub_service $&len_sub_service;
      length resource $&len_resource;

      set &input_capacity_table (where=((&include_str) and not (&exclude_str)));
      by facility service_line sub_service resource;
      if upcase(facility) = 'ALL' then facility = 'ALL';
      if upcase(service_line) = 'ALL' then service_line = 'ALL';
      if upcase(sub_service) = 'ALL' then sub_service = 'ALL';
      
      if first.resource then do;
         if facility = '' or service_line = '' or sub_service = '' 
            or resource = '' or upcase(resource)='ALL'
            or capacity = . or capacity < 0
            then output &_worklib.._invalid_values_capacity;
         else do;
            output &_worklib..input_capacity_pp;
            output &_worklib.._hierarchy_capacity;
         end;
      end;
      else output &_worklib.._duplicate_rows_capacity;
   run;
   
   data &_worklib..input_financials_pp
        &_worklib.._invalid_values_financials
        &_worklib.._duplicate_rows_financials
        &_worklib.._hierarchy_financials;
      length facility $&len_facility;
      length service_line $&len_service_line;
      length sub_service $&len_sub_service;
      length ip_op_indicator $&len_ip_op_indicator;
      length med_surg_indicator $&len_med_surg_indicator;

      set &input_financials_table (where=((&include_str) and not (&exclude_str)));
      by facility service_line sub_service ip_op_indicator med_surg_indicator;
      ip_op_indicator = upcase(ip_op_indicator);
      med_surg_indicator = upcase(med_surg_indicator);
      if already_open_flag not in (0,1) then already_open_flag = 0;
      
      if first.med_surg_indicator then do;
         if facility = '' or upcase(facility) = 'ALL' 
            or service_line = '' or upcase(service_line) = 'ALL'
            or sub_service = '' or upcase(sub_service) = 'ALL' 
            or ip_op_indicator not in ('I','O') 
            or med_surg_indicator not in ('MED','SURG')
            or revenue = . or margin = .
            then output &_worklib.._invalid_values_financials;
         else do;
            output &_worklib..input_financials_pp;
            output &_worklib.._hierarchy_financials;
         end;
      end;
      else output &_worklib.._duplicate_rows_financials;
   run;
   
   %let los_rounding_threshold = %str();
   proc sql noprint;
      select parm_value into :los_rounding_threshold
      from &input_opt_parameters_table 
      where upcase(parm_name) = 'LOS_ROUNDING_THRESHOLD';
   quit;
   %if &los_rounding_threshold = %str() %then %let los_rounding_threshold = 0.5;
      
   data &_worklib..input_service_attributes_pp
        &_worklib.._invalid_values_service_attrs
        &_worklib.._duplicate_rows_service_attrs
        &_worklib.._hierarchy_service_attributes;
      length facility $&len_facility;
      length service_line $&len_service_line;
      length sub_service $&len_sub_service;
      length ip_op_indicator $&len_ip_op_indicator;
      length med_surg_indicator $&len_med_surg_indicator;

      set &input_service_attributes_table (where=((&include_str) and not (&exclude_str)));
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
         else do;
            if length_stay_mean - floor(length_stay_mean) <= &los_rounding_threshold then length_stay_mean = floor(length_stay_mean);
            else length_stay_mean = ceil(length_stay_mean);
            output &_worklib..input_service_attributes_pp;
            output &_worklib.._hierarchy_service_attributes;
         end;
      end;
      else output &_worklib.._duplicate_rows_service_attrs;
   run;

   data &_worklib..input_demand_pp
        &_worklib.._invalid_values_demand
        &_worklib.._duplicate_rows_demand
        &_worklib.._hierarchy_demand;
      length facility $&len_facility;
      length service_line $&len_service_line;
      length sub_service $&len_sub_service;
      length ip_op_indicator $&len_ip_op_indicator;
      length med_surg_indicator $&len_med_surg_indicator;

      set &input_demand_table (where=((&include_str) and not (&exclude_str)));
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
         else do;
            output &_worklib..input_demand_pp;
            if first.med_surg_indicator then output &_worklib.._hierarchy_demand;
         end;
      end;
      else output &_worklib.._duplicate_rows_demand;
   run;

   /* Add scenario_name column if it doesn't already exist */
   data &_worklib..input_opt_parameters_pp;
      set &input_opt_parameters_table;
      if scenario_name = '' then scenario_name = '';
   run;
   
   /* Create a list of distinct scenario names, so that we can output multiple rows (one per scenario)
      for any parameters that have scenario_name='ALL' */
   proc sql noprint;
      select distinct(strip(scenario_name)) into :scenario_names separated by ','
      from &_worklib..input_opt_parameters_pp
      where upcase(scenario_name) ne 'ALL';
   quit;
   
   /* Replace scenario_name = 'ALL' with individual scenario names. I'm doing this in a separate step 
      so that we can more easily catch duplicates in the next data step. */
   data &_worklib..input_opt_parameters_pp;
      set &_worklib..input_opt_parameters_pp;
      if upcase(scenario_name) = 'ALL' then do;
         %do i = 1 %to %SYSFUNC(countw(%bquote(&scenario_names),','));
            scenario_name = "%scan(%bquote(&scenario_names), &i, ',')";
            output &_worklib..input_opt_parameters_pp;
         %end;
      end;
      else output &_worklib..input_opt_parameters_pp;
   run;
      
   data &_worklib..input_opt_parameters_pp
        &_worklib.._invalid_values_opt_parameters
        &_worklib.._duplicate_rows_opt_parameters;
      length facility $&len_facility;
      length service_line $&len_service_line;
      length sub_service $&len_sub_service;
      length ip_op_indicator $&len_ip_op_indicator;
      length med_surg_indicator $&len_med_surg_indicator;

      set &_worklib..input_opt_parameters_pp (where=((&include_str) and not (&exclude_str)));
      by scenario_name facility service_line sub_service ip_op_indicator med_surg_indicator parm_name;
      if upcase(facility) = 'ALL' then facility = 'ALL';
      if upcase(service_line) = 'ALL' then service_line = 'ALL';
      if upcase(sub_service) = 'ALL' then sub_service = 'ALL';
      if upcase(ip_op_indicator) = 'ALL' then ip_op_indicator = 'ALL';
      if upcase(med_surg_indicator) = 'ALL' then med_surg_indicator = 'ALL';
      
      if first.parm_name then do;
         if facility = '' or service_line = '' or sub_service = '' 
            /*or ip_op_indicator = '' or med_surg_indicator = '' */ or parm_name = ''
            then output &_worklib.._invalid_values_opt_parameters;
           /* Invalid values - fractional parameter list, Binary parameter list */
         else if indexw("&fractional_param_list",upcase(parm_name)) > 0
            and not((parm_value = '0') or (1 < parm_value <= 100)) then output &_worklib.._invalid_values_opt_parameters;
         else if indexw("&binary_param_list",upcase(parm_name)) > 0
            and upcase(parm_value) not in ('YES','NO','1','0') then output &_worklib.._invalid_values_opt_parameters;
         else do;
            /* Binary parameter list - change 0/1 values to NO/YES */
            if indexw("&binary_param_list",upcase(parm_name)) > 0 then do;
               if parm_value = '0' then parm_value = 'NO';
               else if parm_value = '1' then parm_value = 'YES';
            end;         
            output &_worklib..input_opt_parameters_pp;
         end;
      end;
      else output &_worklib.._duplicate_rows_opt_parameters;
   run;

/*Validating for duplicate data in the non_hier_param_list */
   data &_worklib..input_opt_parameters_pp
        &_worklib.._tmp_inval_opt_parameters;
      set &_worklib..input_opt_parameters_pp;
      by scenario_name parm_name parm_value;
      /*check if the parm_name is in non_hier_param_list*/
      if index("&non_hier_param_list",upcase(parm_name)) > 0 then do;
        if first.parm_name then do;      
          start = index(parm_name, 'PHASE_');
          parameter = substr(parm_name, 1, start-2);    
          if index(parm_name, 'PHASE_') > 0 AND indexw("&non_hier_param_list",upcase(parameter)) > 0 
                                    then output &_worklib..input_opt_parameters_pp;
          else output &_worklib..input_opt_parameters_pp;
        end; 
        else output &_worklib.._tmp_inval_opt_parameters;
      end;
      
      else output &_worklib..input_opt_parameters_pp;
      drop parameter start;
   run;

/*appending to the invalid values table */
   data &_worklib.._invalid_values_opt_parameters;
      set &_worklib.._invalid_values_opt_parameters &_worklib.._tmp_inval_opt_parameters;
   run;

   /* Create &output_invalid_values and &output_duplicate_rows */
   data &outlib..&output_invalid_values;
      length table $32;
      set &_worklib.._invalid_values_demand (in=in_dem)
          &_worklib.._invalid_values_financials (in=in_fin)
          &_worklib.._invalid_values_service_attrs (in=in_attr)
          &_worklib.._invalid_values_utilization (in=in_util)
          &_worklib.._invalid_values_capacity (in=in_cap)
          &_worklib.._invalid_values_opt_parameters (in=in_opt);
          
      if in_dem then table = 'INPUT_DEMAND';
      else if in_fin then table = 'INPUT_FINANCIALS';
      else if in_attr then table = 'INPUT_SERVICE_ATTRIBUTES';
      else if in_util then table = 'INPUT_UTILIZATION';
      else if in_cap then table = 'INPUT_CAPACITY';
      else if in_opt then table = 'INPUT_OPT_PARAMETERS';
      keep table facility service_line sub_service ip_op_indicator med_surg_indicator resource parm_name;
   run;

   data &outlib..&output_duplicate_rows;
      length table $32;
      set &_worklib.._duplicate_rows_demand (in=in_dem)
          &_worklib.._duplicate_rows_financials (in=in_fin)
          &_worklib.._duplicate_rows_service_attrs (in=in_attr)
          &_worklib.._duplicate_rows_utilization (in=in_util)
          &_worklib.._duplicate_rows_capacity (in=in_cap)
          &_worklib.._duplicate_rows_opt_parameters (in=in_opt);

      if in_dem then table = 'INPUT_DEMAND';
      else if in_fin then table = 'INPUT_FINANCIALS';
      else if in_attr then table = 'INPUT_SERVICE_ATTRIBUTES';
      else if in_util then table = 'INPUT_UTILIZATION';
      else if in_cap then table = 'INPUT_CAPACITY';
      else if in_opt then table = 'INPUT_OPT_PARAMETERS';
      keep table facility service_line sub_service ip_op_indicator med_surg_indicator resource parm_name;
   run;
   
   proc sql noprint; 
      select min(count(*),1) into :filter_serv_not_using_resources
      from &_worklib..input_opt_parameters_pp
      where parm_name = 'FILTER_SERV_NOT_USING_RESOURCES' and parm_value = 'YES';
   quit;
   
   %if &filter_serv_not_using_resources %then %do;

      /* Recreate _hierarchy_utilization by removing records from utilization that have 
         no corresponding row in capacity */
      data &_worklib.._hierarchy_utilization;
         set &_worklib.._hierarchy_utilization;
         if _n_ = 1 then do;
            declare hash h0(dataset:"&_worklib..input_capacity_pp");
            h0.defineKey('facility','service_line','sub_service','resource');
            h0.defineDone();
         end;

         /* Utilization is defined at the granular facility/service_line/sub_service level, 
            but capacity may have been aggregated to ALL at any of these levels. So we need
            to search every combination until we find one that matches, and we output the 
            row only if we have found a match. */
         rc0 = .;
         facility_bak = facility;
         service_line_bak = service_line;
         sub_service_bak = sub_service;
         
         do i = 1 to 2 while (rc0 ne 0);
            if i = 1 then facility = facility_bak;
            else facility = 'ALL';
            do j = 1 to 2 while (rc0 ne 0);
               if j = 1 then service_line = service_line_bak;
               else service_line = 'ALL';
               do k = 1 to 2 while (rc0 ne 0);
                  if k = 1 then sub_service = sub_service_bak;
                  else sub_service = 'ALL';
                  rc0 = h0.find();
               end;
            end;
         end;
         facility = facility_bak;
         service_line = service_line_bak;
         sub_service = sub_service_bak;
         if rc0 = 0 then output;
         drop i j k facility_bak service_line_bak sub_service_bak rc0;
      run;
      
   %end;
      
   /* Now that we have removed invalid values from all the tables, we need to get a complete set of
      facility/service_line/sub_service/ip_op_indicator/med_surg_indicator that is common across all the 
      tables that use this granularity. Note that I am not including _hierarchy_utilization unless 
      &filter_serv_not_using_resources = 1, because there might be some facility/service/subservice 
      combinations that don't use any resources, but we still want to include them in the optimization 
      problem because they might use COVID-19 tests, which are NOT included in the utilization or 
      capacity tables.*/
   data &_worklib.._master_sets_union;
      merge &_worklib.._hierarchy_financials (in=in_financials)
            &_worklib.._hierarchy_service_attributes (in=in_service_attributes)
            &_worklib.._hierarchy_demand (in=in_demand)
            %if &filter_serv_not_using_resources %then %do;
               &_worklib.._hierarchy_utilization (in=in_utilization)
            %end;
            ;
      by facility service_line sub_service ip_op_indicator med_surg_indicator;
      if in_financials and in_service_attributes and in_demand 
         %if &filter_serv_not_using_resources %then %do;
            and in_utilization
         %end;
         then output;
      keep facility service_line sub_service ip_op_indicator med_surg_indicator; 
   run;

   /* Remove the rows from each table that are not in the master set union */
   data &_worklib..input_utilization_pp
        &_worklib.._dropped_rows_utilization;
      set &_worklib..input_utilization_pp;
      if _n_ = 1 then do;
         declare hash h0(dataset:"&_worklib.._master_sets_union");
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
         declare hash h0(dataset:"&_worklib.._master_sets_union");
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
         declare hash h0(dataset:"&_worklib.._master_sets_union");
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
         declare hash h0(dataset:"&_worklib.._master_sets_union");
         h0.defineKey('facility','service_line','sub_service','ip_op_indicator','med_surg_indicator');
         h0.defineDone();
      end;
      rc0 = h0.find();
      if rc0 = 0 then output &_worklib..input_demand_pp;
      else output &_worklib.._dropped_rows_demand;
      drop rc0;
   run;

   /* Create &output_hierarchy_mismatch */
   data &outlib..&output_hierarchy_mismatch;
      merge &_worklib.._dropped_rows_demand (in=in_dem)
            &_worklib.._dropped_rows_financials (in=in_fin)
            &_worklib.._dropped_rows_service_attributes (in=in_attrs)
            &_worklib.._dropped_rows_utilization (in=in_util);
      by facility service_line sub_service ip_op_indicator med_surg_indicator;
      in_demand = in_dem;
      in_financials = in_fin;
      in_service_attributes = in_attrs;
      in_utilization = in_util;
      keep facility service_line sub_service ip_op_indicator med_surg_indicator
           in_demand in_financials in_service_attributes in_utilization;
   run;
      
   data &_worklib.._hierarchies_not_in_util;
      merge &_worklib..input_utilization_pp (in=in_util)
            &_worklib.._master_sets_union (in=in_master);
      by facility service_line sub_service ip_op_indicator med_surg_indicator;
      if in_master and not in_util;
      keep facility service_line sub_service ip_op_indicator med_surg_indicator;
   run;
   
   data &_worklib.._resources_in_utilization;
      set &_worklib..input_utilization_pp (keep=facility service_line sub_service resource);
      by facility service_line sub_service resource;
      if first.resource;
      facility_bak = facility; 
      service_line_bak = service_line;
      sub_service_bak = sub_service;
   run;
   
   data &_worklib.._resources_in_utilization;
      set &_worklib.._resources_in_utilization;
      if _n_ = 1 then do;
         declare hash h0(dataset:"&_worklib..input_capacity_pp");
         h0.defineKey('facility','service_line','sub_service','resource');
         h0.defineDone();
      end;

      /* Utilization is defined at the granular facility/service_line/sub_service level, 
         but capacity may have been aggregated to ALL at any of these levels. So we need
         to search every combination until we find one that matches. If we get through all
         combinations and we still haven't found a match, we output the row to 
         &resources_mismatch. */
      rc0 = .;
      do i = 1 to 2 while (rc0 ne 0);
         if i = 1 then facility = facility_bak;
         else facility = 'ALL';
         do j = 1 to 2 while (rc0 ne 0);
            if j = 1 then service_line = service_line_bak;
            else service_line = 'ALL';
            do k = 1 to 2 while (rc0 ne 0);
               if k = 1 then sub_service = sub_service_bak;
               else sub_service = 'ALL';
               rc0 = h0.find();
            end;
         end;
      end;
      facility = facility_bak;
      service_line = service_line_bak;
      sub_service = sub_service_bak;
      if rc0 ne 0 then output;
      drop i j k facility_bak service_line_bak sub_service_bak rc0;
   run;

   /* Remove the rows from &input_capacity that do not correspond to any facility/service_line/sub_service/resource
      remaining in utilization. The CAPACITY table can have a value of 'ALL' for any combination of facility, 
      service_line, and sub_service, so we need to consider all of these combinations when we look for matching
      records in UTILIZATION. */
   %let combinations = fac_sl_ss_r
                       fac_r
                       sl_r
                       ss_r
                       fac_sl_r
                       fac_ss_r
                       sl_ss_r
                       r;
   
   %do i = 1 %to 8;
      %let suffix = %scan(&combinations, &i, ' ');
      %let by_string = %str();
      %if %index(&suffix, fac) > 0 %then %let by_string = &by_string facility;
      %if %index(&suffix, sl) > 0 %then %let by_string = &by_string service_line;
      %if %index(&suffix, ss) > 0 %then %let by_string = &by_string sub_service;
      %let by_string = &by_string resource;
   
      data &_worklib.._util_resources_&suffix;
         set &_worklib..input_utilization_pp (keep=&by_string);
         by &by_string;
         if first.resource;
      run;
   %end;
   
   data &_worklib..input_capacity_pp
        &_worklib.._dropped_rows_capacity;
      set &_worklib..input_capacity_pp;
      if _n_ = 1 then do;
         %do i = 1 %to 8;
            %let suffix = %scan(&combinations, &i, ' ');
            declare hash h&i(dataset:"&_worklib.._util_resources_&suffix");
            h&i..defineKey(ALL:'YES');
            h&i..defineDone();
         %end;
      end;
   
      if 0 then rc0 = 0;
      %do i = 1 %to 8;
         %let suffix = %scan(&combinations, &i, ' ');
         %let fac_operator = EQ;
         %let sl_operator = EQ;
         %let ss_operator = EQ;
         %if %index(&suffix, fac) > 0 %then %let fac_operator = NE;
         %if %index(&suffix, sl) > 0 %then %let sl_operator = NE;
         %if %index(&suffix, ss) > 0 %then %let ss_operator = NE;
      
         else if facility &fac_operator 'ALL'
           and service_line &sl_operator 'ALL'
           and sub_service &ss_operator 'ALL' then rc0 = h&i..find();
      %end;
      if rc0 = 0 then output &_worklib..input_capacity_pp;
      else output &_worklib.._dropped_rows_capacity;
      drop rc0;
   run;
   
   data &outlib..&output_resource_mismatch;
      merge &_worklib.._resources_in_utilization (in=in_util)
            &_worklib.._dropped_rows_capacity (drop=capacity in=in_cap);
      by facility service_line sub_service resource;
      in_utilization = in_util;
      in_capacity = in_cap;
   run;

   /* Remove the rows from &input_opt_parameters that do not correspond to any hierarchy
      remaining in master_sets_union, but keep the rows that have ALL for any of the fields */
   data &_worklib.._distinct_fac_sl_ss;
      set &_worklib.._master_sets_union (keep=facility service_line sub_service);
      by facility service_line sub_service;
      if first.sub_service;
   run;
   
   data &_worklib..input_opt_parameters_pp
        &_worklib.._dropped_rows_opt_parameters;
      set &_worklib..input_opt_parameters_pp;
      if _n_ = 1 then do;
         declare hash h0(dataset:"&_worklib.._distinct_fac_sl_ss");
         h0.defineKey('facility','service_line','sub_service');
         h0.defineDone();
      end;
      rc0 = h0.find();
      if rc0 = 0 or upcase(facility)='ALL' or upcase(service_line)='ALL' or upcase(sub_service)='ALL'
         or upcase(ip_op_indicator)='ALL' or upcase(med_surg_indicator)='ALL'
         then output &_worklib..input_opt_parameters_pp;
      else output &_worklib.._dropped_rows_opt_parameters;
      drop rc0;
   run;

   proc sql noprint;
      select count(*) into :n_hierarchy_mismatch 
         from &outlib..&output_hierarchy_mismatch;
      select count(*) into :n_resource_mismatch 
         from &outlib..&output_resource_mismatch;
      select count(*) into :n_invalid_values 
         from &outlib..&output_invalid_values;
      select count(*) into :n_duplicate_rows 
         from &outlib..&output_duplicate_rows;
   quit;
   
   %if &n_hierarchy_mismatch > 0 %then %put WARNING: There are %left(&n_hierarchy_mismatch) rows in the %upcase(&outlib..&output_hierarchy_mismatch) table.;
   %else %do;
      proc delete data=&outlib..&output_hierarchy_mismatch; 
      quit;
   %end;
      
   %if &n_resource_mismatch > 0 %then %put WARNING: There are %left(&n_resource_mismatch) rows in the %upcase(&outlib..&output_resource_mismatch) table.;
   %else %do;
      proc delete data=&outlib..&output_resource_mismatch;
      quit;
   %end;
   
   %if &n_invalid_values > 0 %then %put WARNING: There are %left(&n_invalid_values) rows in the %upcase(&outlib..&output_invalid_values) table.;
   %else %do;
      proc delete data=&outlib..&output_invalid_values;
      quit;
   %end;
   
   %if &n_duplicate_rows > 0 %then %put WARNING: There are %left(&n_duplicate_rows) rows in the %upcase(&outlib..&output_duplicate_rows) table.;
   %else %do;
      proc delete data=&outlib..&output_duplicate_rows;
      quit;
   %end;

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
      %let i = 1;
      %let table = %scan(&_work_tables, &i, ' ');
      %do %while (&table ne %str());
         %if %sysfunc(exist(&table)) %then %do;
            proc delete data= &table.;
            run;
         %end;
         %let i = %eval(&i + 1);
         %let table = %scan(&_work_tables, &i, ' ');
      %end;

      /* Delete all temp tables that start with _mrochar_, if they exist. These are not included in &_work_tables because 
         the names are not fixed, they depend on the input parameters. */
      proc datasets nolist lib=&_worklib.;
         delete _mrochar_:;
      quit;
   %end;

   %EXIT:
   %put TRACE: Leaving &sysmacroname. with SYSCC=&SYSCC.;

%mend;
