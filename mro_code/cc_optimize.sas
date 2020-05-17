*------------------------------------------------------------------------------*
| Program: cc_med_res_opt
|
| Description: 
|
*--------------------------------------------------------------------------------* ;
%macro cc_optimize(
    inlib=cc
   ,outlib=cc
   ,input_demand_fcst=output_fd_demand_fcst
   ,output_opt_detail=output_opt_detail
   ,output_opt_detail_agg=output_opt_detail_agg
   ,output_opt_summary=output_opt_summary
   ,output_resource_usage=output_opt_resource_usage
   ,output_covid_test_usage=output_opt_covid_test_usage
   ,_worklib=casuser
   ,_debug=0
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
   %if %sysfunc(exist(&outlib..&input_demand_fcst.))=0 %then %do;
      %put FATAL: Missing &outlib..&input_demand_fcst., exiting from &sysmacroname.;
      %goto EXIT;
   %end;     

   %if %sysfunc(exist(&_worklib..input_utilization_pp))=0 %then %do;
      %put FATAL: Missing &_worklib..input_utilization_pp, from &sysmacroname.;
      %goto EXIT;
   %end; 

   %if %sysfunc(exist(&_worklib..input_capacity_pp))=0 %then %do;
      %put FATAL: Missing &_worklib..input_capacity_pp, from &sysmacroname.;
      %goto EXIT;
   %end; 

   %if %sysfunc(exist(&_worklib..input_financials_pp))=0 %then %do;
      %put FATAL: Missing &_worklib..input_financials_pp, from &sysmacroname.;
      %goto EXIT;
   %end; 

   %if %sysfunc(exist(&_worklib..input_service_attributes_pp))=0 %then %do;
      %put FATAL: Missing &_worklib..input_service_attributes_pp))=, from &sysmacroname.;
      %goto EXIT;
   %end; 

   %if %sysfunc(exist(&_worklib..input_opt_parameters_pp))=0 %then %do;
      %put FATAL: Missing &_worklib..input_opt_parameters_pp, from &sysmacroname.;
      %goto EXIT;
   %end; 

   /* List work tables */
   %let _work_tables=%str( 
        &_worklib..opt_parameters_date
        &_worklib..opt_parameters_global
        &_worklib..opt_parameters_hierarchy
        &_worklib..opt_allowed_opening_dates
        &_worklib..opt_distinct_scenarios
        &_worklib.._opt_detail
        &_worklib.._opt_summary
        &_worklib.._opt_resource_usage
        &_worklib.._opt_covid_test_usage
        &_worklib.._opt_detail_week
        &_worklib.._opt_detail_agg
         );

   /* List output tables */
   %let output_tables=%str(         
        &outlib..&output_opt_detail
        &outlib..&output_opt_detail_agg
        &outlib..&output_opt_summary
        &outlib..&output_resource_usage
        &outlib..&output_covid_test_usage
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

   /************************************/
   /************ANALYTICS *************/
   /***********************************/
 
   /* Count the number of distinct scenario names that are not blank. If it's 0, we're going to set the 
      scenario name to Scenario_1 so it doesn't look as strange in the output with a blank scenario name. */
   proc sql noprint;
      select count(*) into :any_nonblank_scenarios
      from &_worklib..input_opt_parameters_pp
      where scenario_name ne '';
   quit;
   
   %if &any_nonblank_scenarios = 0 %then %do;
      data &_worklib..input_opt_parameters_pp;
         set &_worklib..input_opt_parameters_pp (drop=scenario_name);
         scenario_name = 'Scenario_1';
      run;
   %end;
 
   /* Divide INPUT_OPT_PARAMETERS_PP into three tables: one for the date-specific phases, one for "global" parameters that don't
      depend on any levels of the hierarchy, and one for the parameters that do depend on levels of the hierarchy */
   data &_worklib..opt_parameters_date (keep=scenario_name sequence parameter value)
        &_worklib..opt_parameters_global (keep=scenario_name 
                                               allow_opening_only_on_phase 
                                               secondary_objective_tolerance
                                               test_days_ba
                                               rapid_test_da)
        &_worklib..opt_parameters_hierarchy (drop=start sequence parameter value 
                                                  allow_opening_only_on_phase 
                                                  secondary_objective_tolerance
                                                  test_days_ba
                                                  rapid_test_da);
      set &_worklib..input_opt_parameters_pp;
      by scenario_name;
      parm_name = upcase(parm_name);
      parm_value = upcase(parm_value);

      retain secondary_objective_tolerance 
             allow_opening_only_on_phase
             test_days_ba
             rapid_test_da;
      if first.scenario_name then do;
         /* Set default values for "global" parameters */
         secondary_objective_tolerance = 0.99;
         allow_opening_only_on_phase = 0;
         test_days_ba = 0;
         rapid_test_da = 0;
      end;
         
      if index(parm_name, 'PHASE_') > 0 then do;
         start = index(parm_name, 'PHASE_');
         parameter = substr(parm_name, 1, start-2);
         sequence = scan(substr(parm_name, start),2,'_') + 0;
         if parameter = 'DATE' then value = input(parm_value, mmddyy10.);
         else value = parm_value + 0;
         output &_worklib..opt_parameters_date;
      end;
      else do;
         if parm_name in ('ALLOW_OPENING_ONLY_ON_PHASE','SECONDARY_OBJECTIVE_TOLERANCE','TEST_DAYS_BA','RAPID_TEST_DA') then do;
            if parm_name = 'ALLOW_OPENING_ONLY_ON_PHASE' and parm_value='YES' then allow_opening_only_on_phase = 1;
            else if parm_name = 'SECONDARY_OBJECTIVE_TOLERANCE' then secondary_objective_tolerance = input(parm_value, best.) / 100; 
            else if parm_name = 'TEST_DAYS_BA' then test_days_ba = input(parm_value, best.);
            else if parm_name = 'RAPID_TEST_DA' then rapid_test_da = input(parm_value, best.) / 100;
         end;
         else output &_worklib..opt_parameters_hierarchy;
      end;
      
      if last.scenario_name then output &_worklib..opt_parameters_global;
   run;

   proc transpose data=&_worklib..opt_parameters_date out=&_worklib..opt_parameters_date;
      by scenario_name sequence;
      id parameter;
   run;

   /* If allow_opening_only_on_phase = 1 for any scenario, we need to know the phase dates because these are the only allowed opening 
      dates. So before we fill in the rest of the dates for the daily capacities, save a copy of opt_parameters_date with a different 
      name. */
   data &_worklib..opt_allowed_opening_dates;
      set &_worklib..opt_parameters_date (keep=scenario_name date);
   run;
   
   /* Fill in daily capacities of covid tests for the entire planning horizon */
   proc sql noprint;
      select min(predict_date), max(predict_date)
         into :min_date, :max_date
         from &outlib..&input_demand_fcst.
         where predict_date > today();
   quit;

   data &_worklib..opt_parameters_date;
      set &_worklib..opt_parameters_date;
      retain first_date prev_date prev_rapid prev_not_rapid;
      by scenario_name sequence;
      if first.scenario_name then do;
         first_date = date;
         prev_date = date;
         prev_rapid = rapid_tests;
         prev_not_rapid = not_rapid_tests;

         do date = &min_date to first_date - 1;
            rapid_tests = 0;
            not_rapid_tests = 0;
            if &min_date <= date <= &max_date then output;
         end;
         date = first_date;
         rapid_tests = prev_rapid;
         not_rapid_tests = prev_not_rapid;
         if &min_date <= date <= &max_date then output;
      end;
      else do;
         this_date = date;
         this_rapid_tests = rapid_tests;
         this_not_rapid_tests = not_rapid_tests;
         do date = prev_date + 1 to this_date - 1;
            rapid_tests = prev_rapid;
            not_rapid_tests = prev_not_rapid;
            if &min_date <= date <= &max_date then output;
         end;
         date = this_date;
         rapid_tests = this_rapid_tests;
         not_rapid_tests = this_not_rapid_tests;
         if &min_date <= date <= &max_date then output;
         prev_date = date;
         prev_rapid = rapid_tests;
         prev_not_rapid = not_rapid_tests;
      end;
      if last.scenario_name then do date = prev_date + 1 to &max_date;
         if &min_date <= date <= &max_date then output;
      end;
      drop sequence _name_ first_date prev_: this_:;
   run;
   
   /* Create a table of distinct scenario names, which is used only to read the scenario name into a string
      and print it to the log, so that we know which scenario each section of the log corresponds to */
   data &_worklib..opt_distinct_scenarios;
      set &_worklib..input_opt_parameters_pp (keep=scenario_name);
      by scenario_name;
      scenario_name_copy = scenario_name;
      if first.scenario_name then output;
   run;
   
   proc cas; 
      loadactionset 'optimization'; 
      run; 
      source pgm;

        
      /***************/
      /* Define sets */
      /***************/

      num startTime;
      num endTime;
      startTime = time();      
   
      /* Master sets read from data */
      set <str,str,str,str,str,str> FAC_SLINE_SSERV_IO_MS_RES;   /* From utilization */ 
      set <str,str,str,str,str,num> FAC_SLINE_SSERV_IO_MS_DAYS;  /* From demand */
      set <str,str,str,str> FAC_SLINE_SSERV_RES;                 /* From capacity */
      set <str,str,str> ALREADY_OPEN_SERVICES;                   /* From opt_parameters */
      set <str,str,str> MIN_DEMAND_RATIO_CONSTRAINTS;            /* From opt_parameters */
      set <str,str,str> EMER_SURGICAL_PTS_RATIO_CONSTRAINTS;     /* From opt_parameters */
      set <num> ALLOWED_OPENING_DATES;                           /* From opt_allowed_opening_dates */
         
      /* Derived Sets */
      set <str,str,str> FAC_SLINE_SSERV = setof {<f,sl,ss,iof,msf,d> in FAC_SLINE_SSERV_IO_MS_DAYS} <f,sl,ss>;
      set <str,str,str,str,str> FAC_SLINE_SSERV_IO_MS = setof {<f,sl,ss,iof,msf,d> in FAC_SLINE_SSERV_IO_MS_DAYS} <f,sl,ss,iof,msf>;
      set <num> DAYS = setof {<f,sl,ss,iof,msf,d> in FAC_SLINE_SSERV_IO_MS_DAYS} <d>;

      /*****************/
      /* Define inputs */
      /*****************/

      num capacity{FAC_SLINE_SSERV_RES};
      num utilization{FAC_SLINE_SSERV_IO_MS_RES};

      num revenue{FAC_SLINE_SSERV_IO_MS};
      num margin{FAC_SLINE_SSERV_IO_MS};
      num losMean{FAC_SLINE_SSERV_IO_MS};
      num numCancel{FAC_SLINE_SSERV_IO_MS} init 0;

      num demand{FAC_SLINE_SSERV_IO_MS_DAYS};
      num newPatientsBeforeCovid{FAC_SLINE_SSERV_IO_MS_DAYS} init 0;
      
      str minDemandRatio{MIN_DEMAND_RATIO_CONSTRAINTS};
      str emerSurgRatioip{EMER_SURGICAL_PTS_RATIO_CONSTRAINTS};
      num emerSurgRatio{FAC_SLINE_SSERV} init 0;

      num minDay=min {d in DAYS} d;
      num maxDay=max {d in DAYS} d;
      
      num totalDailyRapidTests{DAYS};
      num totalDailyNonRapidTests{DAYS};
      
      num allowOpeningOnlyOnPhase init 0;
      num secondaryObjectiveTolerance init 0.99;
      num testDaysBA init 0;
      num rapidTestDA init 0;

      str scenarioNameCopy;
  
      /***************/
      /* Read data   */
      /***************/
      
      /* Scenario Names */
      read data &_worklib..opt_distinct_scenarios
         into scenarioNameCopy = scenario_name_copy;
        
      put '    *********************************************************';
      put '     Scenario = ' scenarioNameCopy;
      put ;
      put '      Start Time: ' startTime time.;
      put ;
      
      /* Demand Forecast*/
      read data &outlib..&input_demand_fcst. (where=(predict_date > today())) nogroupby
         into FAC_SLINE_SSERV_IO_MS_DAYS = [facility service_line sub_service ip_op_indicator med_surg_indicator predict_date]
            demand=daily_predict;

      /* Capacity */
      read data &_worklib..input_capacity_pp nogroupby
         into FAC_SLINE_SSERV_RES = [facility service_line sub_service resource]
            capacity;

      /* Utilization */
      read data &_worklib..input_utilization_pp nogroupby
         into FAC_SLINE_SSERV_IO_MS_RES = [facility service_line sub_service ip_op_indicator med_surg_indicator resource]
            utilization=utilization_mean;

      /* Financials */
      read data &_worklib..input_financials_pp nogroupby
         into [facility service_line sub_service ip_op_indicator med_surg_indicator]
            revenue 
            margin;
      
      /* Service attributes */
      read data &_worklib..input_service_attributes_pp nogroupby
         into [facility service_line sub_service ip_op_indicator med_surg_indicator]
/*             numCancel=num_cancelled */
            losMean=length_stay_mean;

      /* Covid test capacity */
      read data &_worklib..opt_parameters_date
         into [date] 
            totalDailyRapidTests=rapid_tests
            totalDailyNonRapidTests=not_rapid_tests;

      /* Global parameters */
      read data &_worklib..opt_parameters_global into
         allowOpeningOnlyOnPhase = allow_opening_only_on_phase
         secondaryObjectiveTolerance = secondary_objective_tolerance
         testDaysBA = test_days_ba
         rapidTestDA = rapid_test_da;
         
      /* Allowed opening dates */
      read data &_worklib..opt_allowed_opening_dates into
         ALLOWED_OPENING_DATES = [date];
         
      /* Services that are already open */
      read data &_worklib..opt_parameters_hierarchy (where=(parm_name='ALREADY_OPEN' and parm_value='YES'))
         into ALREADY_OPEN_SERVICES = [facility service_line sub_service];
         
      /* Min demand ratio constraints */
      read data &_worklib..opt_parameters_hierarchy (where=(parm_name='MIN_DEMAND_RATIO'))
         into MIN_DEMAND_RATIO_CONSTRAINTS = [facility service_line sub_service]
            minDemandRatio = parm_value;

      /* emergency surgical ratio constraints */
      read data &_worklib..opt_parameters_hierarchy (where=(parm_name='EMER_SURGICAL_PTS_RATIO'))
         into EMER_SURGICAL_PTS_RATIO_CONSTRAINTS = [facility service_line sub_service]
            emerSurgRatioip = parm_value;

      for {<f,sl,ss> in EMER_SURGICAL_PTS_RATIO_CONSTRAINTS} do;
         for {<f2,sl2,ss2> in FAC_SLINE_SSERV} do;
            if( (f2=f or f='ALL') and (sl2=sl or sl='ALL') and (ss2=ss or ss='ALL')) then
               emerSurgRatio[f2,sl2,ss2] = max(emerSurgRatio[f2,sl2,ss2],input(emerSurgRatioip[f,sl,ss],best.)/100);
         end;
      end;        
   
      /* Create a set of weeks and assign a week to each day. These will be used for min demand constraints */
      num week{d in DAYS} = week(d);
      set <num> WEEKS = setof{d in DAYS} week[d];
         
      /* Create decomp blocks to decompose the problem by facility */
      set <str> FACILITIES = setof {<f,sl,ss,iof,msf,d> in FAC_SLINE_SSERV_IO_MS_DAYS} <f>;
      num block_id{f in FACILITIES};
      num id init 0;
      for {f in FACILITIES} do;
         block_id[f] = id;
         id = id + 1;
      end;


      /**********************/
      /* Decision Variables */
      /**********************/
   
      /* Decide to open or not a sub service */
      var OpenFlg{FAC_SLINE_SSERV, DAYS} BINARY;
   
      /* Related to how many new patients are actually accepted. Note that we're only going to assign NewPatients on 
         days that have positive demand, and we're only going to assign ReschedulePatients for sub-services that have 
         positive cancellations, so first we create sets to restrict the variable hierarchies. (I'm creating the 
         sets here instead of up above with the other sets, because these depend on demand[] and numCancel[] so they 
         have to come after the read data statements.) */
      set <str,str,str,str,str,num> VAR_HIERARCHY_POSITIVE_DEMAND = {<f,sl,ss,iof,msf,d> in FAC_SLINE_SSERV_IO_MS_DAYS : demand[f,sl,ss,iof,msf,d] > 0};
      set <str,str,str,str,str,num> VAR_HIERARCHY_POSITIVE_CANCEL = {<f,sl,ss,iof,msf,d> in FAC_SLINE_SSERV_IO_MS_DAYS : numCancel[f,sl,ss,iof,msf] > 0};
      var NewPatients{VAR_HIERARCHY_POSITIVE_DEMAND} >= 0;
      var ReschedulePatients{VAR_HIERARCHY_POSITIVE_CANCEL} >= 0;

      /* Calculate total number of patients for day d */
      impvar TotalPatients{<f,sl,ss,iof,msf,d> in FAC_SLINE_SSERV_IO_MS_DAYS} =
         sum{d1 in DAYS: (max((d - losMean[f,sl,ss,iof,msf] + 1), minDay)) <= d1 <= d} 
            ((if <f,sl,ss,iof,msf,d1> in VAR_HIERARCHY_POSITIVE_DEMAND then NewPatients[f,sl,ss,iof,msf,d1] else 0)
           + (if <f,sl,ss,iof,msf,d1> in VAR_HIERARCHY_POSITIVE_CANCEL then ReschedulePatients[f,sl,ss,iof,msf,d1] else 0));


      /***************/
      /* Constraints */
      /***************/
      
      /* New patients cannot exceed demand if the sub service is open */
      con Maximum_Demand{<f,sl,ss,iof,msf,d> in VAR_HIERARCHY_POSITIVE_DEMAND}:
         NewPatients[f,sl,ss,iof,msf,d] <= demand[f,sl,ss,iof,msf,d]*OpenFlg[f,sl,ss,d]
                   suffixes=(block=block_id[f]);
   
      /* Rescheduled patients are not allowed if the sub service is not open. In this constraint, numCancel
         acts as a big-M coefficient. */
      con Reschedule_Allowed{<f,sl,ss,iof,msf,d> in VAR_HIERARCHY_POSITIVE_CANCEL}:
         ReschedulePatients[f,sl,ss,iof,msf,d] <= numCancel[f,sl,ss,iof,msf]*OpenFlg[f,sl,ss,d]
                   suffixes=(block=block_id[f]);
      
      /* The total number of patients rescheduled across all the days cannot exceed the number of 
         cancelled patients. */
      con Reschedule_Maximum{<f,sl,ss,iof,msf> in FAC_SLINE_SSERV_IO_MS : numCancel[f,sl,ss,iof,msf] > 0}:
         sum{d in DAYS} ReschedulePatients[f,sl,ss,iof,msf,d] <= numCancel[f,sl,ss,iof,msf]
                   suffixes=(block=block_id[f]);
         
      /* If a sub-service is open, we must satisfy a minimum proportion of the weekly demand if minDemandRatio > 0. We are breaking these 
         into two sets of constraints -- one where facility is not equal to 'ALL' and one where facility is equal to 'ALL' -- so that we can 
         define decomp blocks for the ones that do not span all facilities. */
      con Minimum_Demand{<f,sl,ss> in MIN_DEMAND_RATIO_CONSTRAINTS, w in WEEKS : f ne 'ALL'}:
         sum{<(f),sl2,ss2,iof,msf,d> in FAC_SLINE_SSERV_IO_MS_DAYS : (sl2=sl or sl='ALL') and (ss2=ss or ss='ALL') and week[d]=w}
                ((if <f,sl2,ss2,iof,msf,d> in VAR_HIERARCHY_POSITIVE_DEMAND then NewPatients[f,sl2,ss2,iof,msf,d] else 0)
               + (if <f,sl2,ss2,iof,msf,d> in VAR_HIERARCHY_POSITIVE_CANCEL then ReschedulePatients[f,sl2,ss2,iof,msf,d] else 0)
               - (input(minDemandRatio[f,sl,ss],best.)/100 * newPatientsBeforeCovid[f,sl2,ss2,iof,msf,d] * OpenFlg[f,sl2,ss2,d]))
            >= 0
                  suffixes=(block=block_id[f]);

      con Minimum_Demand_ALL{<f,sl,ss> in MIN_DEMAND_RATIO_CONSTRAINTS, w in WEEKS : f='ALL'}:
         sum{<f2,sl2,ss2,iof,msf,d> in FAC_SLINE_SSERV_IO_MS_DAYS : (sl2=sl or sl='ALL') and (ss2=ss or ss='ALL') and week[d]=w}
                ((if <f2,sl2,ss2,iof,msf,d> in VAR_HIERARCHY_POSITIVE_DEMAND then NewPatients[f2,sl2,ss2,iof,msf,d] else 0)
               + (if <f2,sl2,ss2,iof,msf,d> in VAR_HIERARCHY_POSITIVE_CANCEL then ReschedulePatients[f2,sl2,ss2,iof,msf,d] else 0)
               - (input(minDemandRatio[f,sl,ss],best.)/100 * newPatientsBeforeCovid[f2,sl2,ss2,iof,msf,d] * OpenFlg[f2,sl2,ss2,d]))
            >= 0;
               
      /* If a sub-service opens, it must stay open for the remainder of the horizon */
      con Service_Stay_Open{<f,sl,ss> in FAC_SLINE_SSERV, d in DAYS: d + 1 in DAYS}:
         OpenFlg[f,sl,ss,d+1] >= OpenFlg[f,sl,ss,d]
                   suffixes=(block=block_id[f]);

      /* Only allow openings on the phase dates or on the first day */
      con Open_on_Allowed_Dates{<f,sl,ss> in FAC_SLINE_SSERV, d in DAYS: allowOpeningOnlyOnPhase = 1 
                                                                         and d - 1 in DAYS 
                                                                         and d not in ALLOWED_OPENING_DATES} : 
         OpenFlg[f,sl,ss,d] = OpenFlg[f,sl,ss,d-1]
                   suffixes=(block=block_id[f]);
               
      /* Total patients cannot exceed capacity. We are breaking these into two sets of constraints -- one where facility 
         is not equal to 'ALL' and one where facility is equal to 'ALL' -- so that we can define decomp blocks for the 
         ones that do not span all facilities. */
      con Resources_Capacity{<f,sl,ss,r> in FAC_SLINE_SSERV_RES, d in DAYS : f ne 'ALL'}:
         sum {<(f),sl2,ss2,iof,msf,(r)> in FAC_SLINE_SSERV_IO_MS_RES : (sl2=sl or sl='ALL') and (ss2=ss or ss='ALL')} 
            utilization[f,sl2,ss2,iof,msf,r]*TotalPatients[f,sl2,ss2,iof,msf,d] <= capacity[f,sl,ss,r]
                   suffixes=(block=block_id[f]);

      con Resources_Capacity_ALL{<f,sl,ss,r> in FAC_SLINE_SSERV_RES, d in DAYS : f = 'ALL'}:
         sum {<f2,sl2,ss2,iof,msf,(r)> in FAC_SLINE_SSERV_IO_MS_RES : (sl2=sl or sl='ALL') and (ss2=ss or ss='ALL')} 
            utilization[f2,sl2,ss2,iof,msf,r]*TotalPatients[f2,sl2,ss2,iof,msf,d] <= capacity[f,sl,ss,r];
         
      /* Tests constraint - Total inpatients admitted should be less than the daily rapid test available  */
      con COVID19_Day_Of_Admission_Testing{d in DAYS : rapidTestDA > 0}:
         sum {<f,sl,ss,iof,msf,(d)> in VAR_HIERARCHY_POSITIVE_DEMAND : iof='I' and msf ne 'SURG'} (NewPatients[f,sl,ss,iof,msf,d])
       + sum {<f,sl,ss,iof,msf,(d)> in VAR_HIERARCHY_POSITIVE_CANCEL : iof='I' and msf ne 'SURG'} (ReschedulePatients[f,sl,ss,iof,msf,d])
       + sum {<f,sl,ss,iof,msf,(d)> in VAR_HIERARCHY_POSITIVE_DEMAND : msf = 'SURG'} (NewPatients[f,sl,ss,iof,msf,d] * emerSurgRatio[f,sl,ss])
       + sum {<f,sl,ss,iof,msf,(d)> in VAR_HIERARCHY_POSITIVE_CANCEL : msf = 'SURG'} (ReschedulePatients[f,sl,ss,iof,msf,d] * emerSurgRatio[f,sl,ss])
       <= totalDailyRapidTests[d] / rapidTestDA;

      /* Non-Rapid tests constraint - total available non-rapid test */
      con COVID19_Before_Admission_Testing{d in DAYS : testDaysBA > 0 and d + testDaysBA in DAYS}:
         sum {<f,sl,ss,iof,msf,d1> in VAR_HIERARCHY_POSITIVE_DEMAND : msf='SURG' and d1 = d + testDaysBA} (NewPatients[f,sl,ss,iof,msf,d1] * (1-emerSurgRatio[f,sl,ss]))
       + sum {<f,sl,ss,iof,msf,d1> in VAR_HIERARCHY_POSITIVE_CANCEL : msf='SURG' and d1 = d + testDaysBA} (ReschedulePatients[f,sl,ss,iof,msf,d1] * (1-emerSurgRatio[f,sl,ss]))
       <= totalDailyNonRapidTests[d];


      /***********************/
      /* Objective Functions */
      /***********************/

      max Total_Revenue = 
          sum{<f,sl,ss,iof,msf,d> in VAR_HIERARCHY_POSITIVE_DEMAND} NewPatients[f,sl,ss,iof,msf,d] * revenue[f,sl,ss,iof,msf]
        + sum{<f,sl,ss,iof,msf,d> in VAR_HIERARCHY_POSITIVE_CANCEL} ReschedulePatients[f,sl,ss,iof,msf,d] * revenue[f,sl,ss,iof,msf];
   
      max Total_Margin = 
          sum{<f,sl,ss,iof,msf,d> in VAR_HIERARCHY_POSITIVE_DEMAND} NewPatients[f,sl,ss,iof,msf,d] * margin[f,sl,ss,iof,msf]
        + sum{<f,sl,ss,iof,msf,d> in VAR_HIERARCHY_POSITIVE_CANCEL} ReschedulePatients[f,sl,ss,iof,msf,d] * margin[f,sl,ss,iof,msf];

      /***********************/
      /* Solve               */
      /***********************/

      /* First we want to find out what is the maximum demand we can handle without the covid-19 tests. 
         We drop the COVID constraints and the minimum demand constraints. We're also going to fix OpenFlg to 1 
         for every sub-service (i.e., the only reason we might not open a sub-service is because we don't have enough 
         COVID-19 tests), so we can also drop the Service_Stay_Open constraints. And we fix ReschedulePatients to 0
         because we only want to consider original demand (i.e., non-covid impacts) to calculate newPatientsBeforeCovid. */
      drop COVID19_Day_Of_Admission_Testing
           COVID19_Before_Admission_Testing
           Minimum_Demand
           Minimum_Demand_ALL
           Service_Stay_Open
           Open_on_Allowed_Dates;
           
      fix OpenFlg = 1;
      fix ReschedulePatients = 0;

      solve obj Total_Revenue with milp / maxtime=300 loglevel=3 /* decomp=(method=user) */;
      
      /* The maximum demand without covid-19 tests is equal to the number of new patients that we saw, 
         subject to other resource capacity constraints */
      for {<f,sl,ss,iof,msf,d> in FAC_SLINE_SSERV_IO_MS_DAYS}
         newPatientsBeforeCovid[f,sl,ss,iof,msf,d] = 
            if <f,sl,ss,iof,msf,d> in VAR_HIERARCHY_POSITIVE_DEMAND then NewPatients[f,sl,ss,iof,msf,d].sol else 0;
      
      /* Now restore the COVID constraints, the minimum demand constraints, and the Service_Stay_Open constraints, 
         and unfix OpenFlg and ReschedulePatients, and then solve again. */
      restore COVID19_Day_Of_Admission_Testing
              COVID19_Before_Admission_Testing
              Minimum_Demand
              Minimum_Demand_ALL
              Service_Stay_Open
              Open_on_Allowed_Dates;

      unfix OpenFlg;
      unfix ReschedulePatients; 
      
      for {<f,sl,ss> in ALREADY_OPEN_SERVICES} do;
         for {<f2,sl2,ss2> in FAC_SLINE_SSERV : (f2=f or f='ALL') and (sl2=sl or sl='ALL') and (ss2=ss or ss='ALL')} 
            fix OpenFlg[f2,sl2,ss2,minDay] = 1;   
      end;

      num primary_objective_value init 0;
      con Primary_Objective_Constraint: 
          sum{<f,sl,ss,iof,msf,d> in VAR_HIERARCHY_POSITIVE_DEMAND} NewPatients[f,sl,ss,iof,msf,d] * revenue[f,sl,ss,iof,msf]
        + sum{<f,sl,ss,iof,msf,d> in VAR_HIERARCHY_POSITIVE_CANCEL} ReschedulePatients[f,sl,ss,iof,msf,d] * revenue[f,sl,ss,iof,msf]
        >= secondaryObjectiveTolerance * primary_objective_value;
      
      if _solution_status_ in {'OPTIMAL', 'OPTIMAL_AGAP', 'OPTIMAL_RGAP', 'OPTIMAL_COND', 'CONDITIONAL_OPTIMAL'} then do;

         drop Primary_Objective_Constraint;
         solve obj Total_Revenue with milp / primalin maxtime=600 loglevel=3 /* decomp=(method=user) */;

         if _solution_status_ in {'OPTIMAL', 'OPTIMAL_AGAP', 'OPTIMAL_RGAP', 'OPTIMAL_COND', 'CONDITIONAL_OPTIMAL'} then do;

            put Total_Revenue.sol=;
            put Total_Margin.sol=;

            primary_objective_value = Total_Revenue.sol;
            restore Primary_Objective_Constraint;

            solve obj Total_Margin with milp / primalin maxtime=300 loglevel=3;

            put Total_Revenue.sol=;
            put Total_Margin.sol=;
         end;
      end; 

      /***********************/
      /* Create output data  */
      /***********************/

      num OptNewPatients {<f,sl,ss,iof,msf,d> in FAC_SLINE_SSERV_IO_MS_DAYS} = 
         if <f,sl,ss,iof,msf,d> in VAR_HIERARCHY_POSITIVE_DEMAND then NewPatients[f,sl,ss,iof,msf,d].sol else 0;

      num OptReschedulePatients {<f,sl,ss,iof,msf,d> in FAC_SLINE_SSERV_IO_MS_DAYS} = 
         if <f,sl,ss,iof,msf,d> in VAR_HIERARCHY_POSITIVE_CANCEL then ReschedulePatients[f,sl,ss,iof,msf,d].sol else 0;

      num OptRevenue{<f,sl,ss,iof,msf,d> in FAC_SLINE_SSERV_IO_MS_DAYS} = 
        (if <f,sl,ss,iof,msf,d> in VAR_HIERARCHY_POSITIVE_DEMAND then NewPatients[f,sl,ss,iof,msf,d].sol else 0 
       + if <f,sl,ss,iof,msf,d> in VAR_HIERARCHY_POSITIVE_CANCEL then ReschedulePatients[f,sl,ss,iof,msf,d].sol else 0) 
       * revenue[f,sl,ss,iof,msf];

      num OptMargin{<f,sl,ss,iof,msf,d> in FAC_SLINE_SSERV_IO_MS_DAYS} = 
        (if <f,sl,ss,iof,msf,d> in VAR_HIERARCHY_POSITIVE_DEMAND then NewPatients[f,sl,ss,iof,msf,d].sol else 0 
       + if <f,sl,ss,iof,msf,d> in VAR_HIERARCHY_POSITIVE_CANCEL then ReschedulePatients[f,sl,ss,iof,msf,d].sol else 0) 
       * margin[f,sl,ss,iof,msf];

      create data &_worklib.._opt_detail
         from [facility service_line sub_service ip_op_indicator med_surg_indicator day]
               = {<f,sl,ss,iof,msf,d> in FAC_SLINE_SSERV_IO_MS_DAYS}
         NewPatients=(round(OptNewPatients[f,sl,ss,iof,msf,d],0.01))
         ReschedulePatients=(round(OptReschedulePatients[f,sl,ss,iof,msf,d],0.01))
         TotalPatients=(round(TotalPatients[f,sl,ss,iof,msf,d],0.01))
         OptRevenue=(round(OptRevenue[f,sl,ss,iof,msf,d],0.01))
         OptMargin=(round(OptMargin[f,sl,ss,iof,msf,d],0.01))
         Demand=(round(demand[f,sl,ss,iof,msf,d],0.01))
         NewPatientsBeforeCovid=(round(newPatientsBeforeCovid[f,sl,ss,iof,msf,d],0.01));

      create data &_worklib.._opt_summary
         from [facility service_line sub_service day]={<f,sl,ss> in FAC_SLINE_SSERV, d in DAYS}
         OpenFlg=(round(OpenFlg[f,sl,ss,d],0.01));

      num OptResourceUsage{<f,sl,ss,r> in FAC_SLINE_SSERV_RES, d in DAYS} = 
         if f ne 'ALL' then Resources_Capacity[f,sl,ss,r,d].body 
         else Resources_Capacity_ALL[f,sl,ss,r,d].body;
         
      create data &_worklib.._opt_resource_usage
         from [facility service_line sub_service resource day]={<f,sl,ss,r> in FAC_SLINE_SSERV_RES, d in DAYS}
         capacity = capacity[f,sl,ss,r]
         usage = (round(OptResourceUsage[f,sl,ss,r,d],0.01));

      create data &_worklib.._opt_covid_test_usage
         from [day]={d in DAYS}
            rapidTestsAvailable=totalDailyRapidTests[d]
            rapidTestsUsed=(if rapidTestDA > 0 then COVID19_Day_Of_Admission_Testing[d].body else 0)
            nonRapidTestsAvailable=totalDailyNonRapidTests[d]
            nonRapidTestsUsed=(if (testDaysBA > 0 and d + testDaysBA in DAYS) then COVID19_Before_Admission_Testing[d].body else 0);

      endTime = time();
      put '      End Time: ' endTime time.;

      endsource; 
      runOptmodel /*result=runOptmodelResult*/ / code=pgm /*printlevel=0*/ groupBy='scenario_name' nGroupByThreads='ALL'; 
      run; 
   quit;


   /* Process and aggregate output data for reporting */

   data &_worklib.._opt_detail_week;
      format date date9.;
      format week_start_date date9.;
      set &_worklib.._opt_detail (rename =(day=date));
      week_start_date = date - (weekday(date)-1);
   run;

   proc cas;
      aggregation.aggregate / table={caslib="&_worklib.", name="_opt_detail_week",  
         groupby={"scenario_name","facility","service_line","sub_service","week_start_date"}} 
         saveGroupByFormat=false 
         varSpecs={{name="NewPatients", summarySubset="sum", columnNames="NewPatients"}
                   {name="ReschedulePatients", summarySubset="sum", columnNames="ReschedulePatients"}
                   {name="TotalPatients", summarySubset="sum", columnNames="TotalPatients"}
                   {name="OptMargin", summarySubset="sum", columnNames="OptMargin"}
                   {name="OptRevenue", summarySubset="sum", columnNames="OptRevenue"}
                   {name="Demand", summarySubset="sum", columnNames="Demand"}
                   {name="NewPatientsBeforeCovid", summarySubset="sum", columnNames="NewPatientsBeforeCovid"}}
         casOut={caslib="&_worklib.",name="_opt_detail_agg",replace=true}; run;  
   quit;

   data &outlib..&output_opt_detail (promote=yes);
      format day date9.;
      set &_worklib.._opt_detail;
   run;

   data &outlib..&output_opt_detail_agg (promote=yes);
      set &_worklib.._opt_detail_agg;
      DailyNewPatients=NewPatients/7;
      DailyReschedulePatients=ReschedulePatients/7;
      DailyTotalPatients=TotalPatients/7;
      DailyOptMargin=OptMargin/7;
      DailyOptRevenue=OptRevenue/7;
      DailyDemand=Demand/7;
      DailyNewPatientsBeforeCovid=NewPatientsBeforeCovid/7;
   run;
    
   data &outlib..&output_opt_summary (promote=yes);
      set &_worklib.._opt_summary;
   run;
   
   data &outlib..&output_resource_usage (promote=yes);
      format date date9.;
      set &_worklib.._opt_resource_usage (rename=(day=date));
      utilization = round(usage / capacity, 0.001);
   run;
   
   data &outlib..&output_covid_test_usage (promote=yes);
      format date date9.;
      set &_worklib.._opt_covid_test_usage (rename=(day=date));
   run;
   
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
   %end;

   %EXIT:
   %put TRACE: Leaving &sysmacroname. with SYSCC=&SYSCC.;

%mend cc_optimize;