*--------------------------------------------------------------------------------------------------------------*
| Program: cc_optimize
|
| Description: This macro is the optimization code. It reads the pre-processed input tables from the 
|              %cc_data_prep and %cc_forecast_demand macros. It then generates the optimization model, solves 
|              the model, and creates various output tables.
|
| INPUTS:
|   - inlib:              Name of the CAS library where the input tables are located
|   - input_demand_fcst:  Name of the table that contains the pre-processed input demand forecast table 
|                         (in outlib) that was created in %cc_forecast_demand. 
|
| OUTPUTS:
|   - outlib:                            Name of the CAS library where the output tables are created
|   - output_opt_detail:                 Name of the table that stores solution detail records (in outlib)
|   - output_opt_detail_agg:             Name of the table that stores the weekly aggregated solution 
|                                        data (in outlib)
|   - output_opt_summary:                Name of the table that stores recommended reopening plan for 
|                                        service lines (in outlib)
|   - output_opt_resource_usage:         Name of the table that stores aggregate utilization of each 
|                                        constrained resource (in outlib)
|   - output_opt_resource_usage_detail:  Name of the table that stores utilization of resources at facility/
|                                        service line/sub-service level (in outlib)
|   - output_opt_covid_test_usage:       Name of the table that stores daily COVID-19 test usage (in outlib)
|
| OTHER PARAMETERS:
|   - _worklib:  Name of the CAS library where the working tables are created
|   - _debug:    Flag to indicate whether the temporary tables in _worklib are to be retained for debugging 
|
*--------------------------------------------------------------------------------------------------------------*;

%macro cc_optimize(
         inlib=cc
         ,outlib=cc
         ,input_demand_fcst=output_fd_demand_fcst
         ,output_opt_detail=output_opt_detail
         ,output_opt_detail_agg=output_opt_detail_agg
         ,output_opt_summary=output_opt_summary
         ,output_opt_resource_usage=output_opt_resource_usage
         ,output_opt_resource_usage_detail=output_opt_resource_usage_detail
         ,output_opt_covid_test_usage=output_opt_covid_test_usage
         ,_worklib=casuser
         ,_debug=0
         );

   /*************************/
   /******HOUSEKEEPING*******/
   /*************************/

   /* Do not proceed if previously there have been errors */
   %if &syscc > 4 %then %do;
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
         &_worklib.._opt_parameters_date
         &_worklib.._opt_parameters_date_1
         &_worklib.._opt_parameters_global
         &_worklib.._opt_parameters_hierarchy
         &_worklib.._opt_allowed_opening_dates
         &_worklib.._opt_distinct_scenarios
         &_worklib.._opt_detail
         &_worklib.._opt_summary
         &_worklib.._opt_resource_usage
         &_worklib.._opt_resource_usage_detail
         &_worklib.._opt_covid_test_usage
         &_worklib.._opt_detail_week
         &_worklib.._opt_detail_agg
         );

   /* List output tables */
   %let output_tables=%str(
         &outlib..&output_opt_detail
         &outlib..&output_opt_detail_agg
         &outlib..&output_opt_summary
         &outlib..&output_opt_resource_usage
         &outlib..&output_opt_resource_usage_detail
         &outlib..&output_opt_covid_test_usage
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

   /***********************************/
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
   data &_worklib.._opt_parameters_date (keep=scenario_name sequence parameter value)
        &_worklib.._opt_parameters_global (keep=scenario_name
                                                allow_opening_only_on_phase
                                                secondary_objective_tolerance
                                                test_days_ba
                                                rapid_test_da
                                                remove_demand_constraints
                                                remove_covid_constraints
                                                hold_not_rapid_covid_tests
                                                hold_rapid_covid_tests
                                                treat_min_demand_as_aggregate
                                                use_decomp)
        &_worklib.._opt_parameters_hierarchy (drop=start sequence parameter value
                                                   allow_opening_only_on_phase
                                                   secondary_objective_tolerance
                                                   test_days_ba
                                                   rapid_test_da
                                                   remove_demand_constraints
                                                   remove_covid_constraints
                                                   hold_not_rapid_covid_tests
                                                   hold_rapid_covid_tests
                                                   treat_min_demand_as_aggregate
                                                   use_decomp);
      set &_worklib..input_opt_parameters_pp;
      by scenario_name;

      retain secondary_objective_tolerance
             allow_opening_only_on_phase
             test_days_ba
             rapid_test_da
             remove_demand_constraints
             remove_covid_constraints
             hold_not_rapid_covid_tests
             hold_rapid_covid_tests
             treat_min_demand_as_aggregate
             use_decomp;

      if first.scenario_name then do;
         /* Set default values for "global" (i.e., non-hierarchy) scenario parameters */
         secondary_objective_tolerance = 0.99;
         allow_opening_only_on_phase = 0;
         test_days_ba = 0;
         rapid_test_da = 0;
         remove_demand_constraints = 0;
         remove_covid_constraints = 0;
         hold_not_rapid_covid_tests = 0;
         hold_rapid_covid_tests = 0;
         treat_min_demand_as_aggregate = 0;
         use_decomp = 0;
      end;

      if index(parm_name, 'PHASE_') > 0 then do;
         /* This is one of the "PHASE" parameters (DATE_PHASE_x, RAPID_TESTS_PHASE_x, and NOT_RAPID_TESTS_PHASE_x) */
         start = index(parm_name, 'PHASE_');
         parameter = substr(parm_name, 1, start-2);
         sequence = scan(substr(parm_name, start),2,'_') + 0;
         if parameter = 'DATE' then value = input(parm_value, mmddyy10.);
         else value = parm_value + 0;
         output &_worklib.._opt_parameters_date;
      end;
      else do;
         /* This is not one of the "PHASE" parameters */
         if parm_name in ('ALLOW_OPENING_ONLY_ON_PHASE','SECONDARY_OBJECTIVE_TOLERANCE','TEST_DAYS_BA','RAPID_TEST_DA',
                          'REMOVE_DEMAND_CONSTRAINTS','REMOVE_COVID_CONSTRAINTS','HOLD_NOT_RAPID_COVID_TESTS','HOLD_RAPID_COVID_TESTS',
                          'TREAT_MIN_DEMAND_AS_AGGREGATE','USE_DECOMP') then do;
            /* This is a "global" (i.e., non-hierarchy) scenario parameter */
            if parm_name = 'ALLOW_OPENING_ONLY_ON_PHASE' and parm_value='YES' then allow_opening_only_on_phase = 1;
            else if parm_name = 'SECONDARY_OBJECTIVE_TOLERANCE' then secondary_objective_tolerance = input(parm_value, best.) / 100;
            else if parm_name = 'TEST_DAYS_BA' then test_days_ba = input(parm_value, best.);
            else if parm_name = 'RAPID_TEST_DA' then rapid_test_da = input(parm_value, best.) / 100;
            else if parm_name = 'REMOVE_DEMAND_CONSTRAINTS' and parm_value='YES' then remove_demand_constraints = 1;
            else if parm_name = 'REMOVE_COVID_CONSTRAINTS' and parm_value='YES' then remove_covid_constraints = 1;
            else if parm_name = 'HOLD_NOT_RAPID_COVID_TESTS' then hold_not_rapid_covid_tests = input(parm_value, best.);
            else if parm_name = 'HOLD_RAPID_COVID_TESTS' then hold_rapid_covid_tests = input(parm_value, best.);
            else if parm_name = 'TREAT_MIN_DEMAND_AS_AGGREGATE' and parm_value='YES' then treat_min_demand_as_aggregate = 1;
            else if parm_name = 'USE_DECOMP' and parm_value='YES' then use_decomp = 1;
         end;
         /* This is a hierarchy parameter */
         else output &_worklib.._opt_parameters_hierarchy;
      end;

      /* The "global" parameter values have been stored in new numeric variables, which are retained, so we output only one row per scenario */
      if last.scenario_name then output &_worklib.._opt_parameters_global;
   run;

   /* Transpose the PHASE parameters to get one row per phase */
   proc transpose data=&_worklib.._opt_parameters_date out=&_worklib.._opt_parameters_date;
      by scenario_name sequence;
      id parameter;
   run;

   /* If allow_opening_only_on_phase = 1 for any scenario, we need to know the phase dates because these are the only allowed opening
      dates. So before we fill in the rest of the dates for the daily capacities, save a copy of opt_parameters_date with a different
      name. */
   data &_worklib.._opt_allowed_opening_dates;
      set &_worklib.._opt_parameters_date (keep=scenario_name date);
   run;

   /* Fill in daily capacities of covid tests for the entire planning horizon */
   proc sql noprint;
      select min(predict_date), max(predict_date)
         into :min_date, :max_date
         from &outlib..&input_demand_fcst.
         /* The demand forecast might have rows with predict_date < &start_date, because we need to keep these for the
            forecast accuracy macro. Therefore, we need to restrict the rows only to predict_date >= &start_date, both
            here and when we read &outlib..&input_demand_fcst into optmodel. */
         where predict_date >= &start_date.;
   quit;

   data &_worklib.._opt_parameters_date_1;
      set &_worklib.._opt_parameters_date;
      retain first_date prev_date prev_rapid prev_not_rapid;
      by scenario_name sequence;
      if first.scenario_name then do;
         /* Save backup of existing values before we overwrite them */
         first_date = date;
         prev_date = date;
         prev_rapid = rapid_tests;
         prev_not_rapid = not_rapid_tests;
         prev_sequence = sequence;

         /* Fill in testing capacity of 0 for all dates prior to the first phase date */
         do date = &min_date to first_date - 1;
            rapid_tests = 0;
            not_rapid_tests = 0;
            sequence = .;
            if &min_date <= date <= &max_date then output;
         end;

         /* Restore from backups and output the testing capacity for the first phase date */
         date = first_date;
         rapid_tests = prev_rapid;
         not_rapid_tests = prev_not_rapid;
         sequence = prev_sequence;
         if &min_date <= date <= &max_date then output;
      end;
      else do;
         /* Save backup of existing values before we overwrite them */
         this_date = date;
         this_rapid_tests = rapid_tests;
         this_not_rapid_tests = not_rapid_tests;
         this_sequence = sequence;

         /* Fill in testing capacity from previous phase for all dates after the previous phase
            date but prior to this current phase date */
         do date = prev_date + 1 to this_date - 1;
            rapid_tests = prev_rapid;
            not_rapid_tests = prev_not_rapid;
            sequence = this_sequence - 1;
            if &min_date <= date <= &max_date then output;
         end;

         /* Restore from backups and output the testing capacity for this current phase date */
         date = this_date;
         rapid_tests = this_rapid_tests;
         not_rapid_tests = this_not_rapid_tests;
         sequence = this_sequence;
         if &min_date <= date <= &max_date then output;
         prev_date = date;
         prev_rapid = rapid_tests;
         prev_not_rapid = not_rapid_tests;
      end;
      if last.scenario_name then do date = prev_date + 1 to &max_date;
         /* Fill in testing capacity from the last phase date until the end of the horizon */
         if &min_date <= date <= &max_date then output;
      end;
      drop _name_ first_date prev_: this_:;
   run;

   /* Create a table of distinct scenario names, which is used only to read the scenario name into a string
      and print it to the log, so that we know which scenario each section of the log corresponds to */
   data &_worklib.._opt_distinct_scenarios;
      set &_worklib..input_opt_parameters_pp (keep=scenario_name);
      by scenario_name;
      scenario_name_copy = scenario_name;
      if first.scenario_name then output;
   run;

   /* RUN OPTIMIZATION STEP */
   proc cas;
      loadactionset 'optimization';
      run;
      source pgm;

      /*************************************************/
      /* Define timing inputs and initialize startTime */
      /*************************************************/

      num startTime;
      num endTime;
      startTime = time();

      /*************************************************/
      /* Define sets                                   */
      /*************************************************/

      /* Master sets read from data */
      set <str,str,str,str,str,str> FAC_SLINE_SSERV_IO_MS_RES;   /* From utilization */
      set <str,str,str,str,str,num> FAC_SLINE_SSERV_IO_MS_DAYS;  /* From demand */
      set <str,str,str,str> FAC_SLINE_SSERV_RES;                 /* From capacity */
      set <str,str,str> ALREADY_OPEN_SERVICES;                   /* From opt_parameters */
      set <str,str,str> MIN_DEMAND_RATIO_CONSTRAINTS;            /* From opt_parameters */
      set <str,str,str> EMER_SURGICAL_PTS_RATIO_CONSTRAINTS;     /* From opt_parameters */
      set <str> ICU_MAX_UTIL_FACILITIES;                         /* From opt_parameters */
      set <num> ALLOWED_OPENING_DATES;                           /* From opt_allowed_opening_dates */

      /* Derived Sets */
      set <str,str,str> FAC_SLINE_SSERV = setof {<f,sl,ss,iof,msf,d> in FAC_SLINE_SSERV_IO_MS_DAYS} <f,sl,ss>;
      set <str,str,str,str,str> FAC_SLINE_SSERV_IO_MS = setof {<f,sl,ss,iof,msf,d> in FAC_SLINE_SSERV_IO_MS_DAYS} <f,sl,ss,iof,msf>;
      set <num> DAYS = setof {<f,sl,ss,iof,msf,d> in FAC_SLINE_SSERV_IO_MS_DAYS} <d>;
      set <str> FACILITIES = setof {<f,sl,ss,iof,msf,d> in FAC_SLINE_SSERV_IO_MS_DAYS} <f>;
      set <str> ICU_RESOURCES = setof{<f,sl,ss,r> in FAC_SLINE_SSERV_RES : index(upcase(r),'ICU BEDS') > 0 
                                                                              and index(upcase(r),'NICU') = 0 
                                                                              and index(upcase(r),'NEONATAL') = 0} <r>;

      /*************************************************/
      /* Define inputs                                 */
      /*************************************************/

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
      str icuMaxUtilization{ICU_MAX_UTIL_FACILITIES};

      num minDay=min {d in DAYS} d;

      num totalDailyRapidTests{DAYS};
      num totalDailyNotRapidTests{DAYS};
      num phaseID{DAYS};

      num allowOpeningOnlyOnPhase init 0;
      num secondaryObjectiveTolerance init 0.99;
      num testDaysBA init 0;
      num rapidTestDA init 0;
      num removeDemandConstraints init 0;
      num removeCovidConstraints init 0;
      num holdNotRapidCovidTests init 0;
      num holdRapidCovidTests init 0;
      num treatMinDemandAsAggregate init 0;
      num useDecomp init 0;

      str scenarioNameCopy;

      /*************************************************/
      /* Read data                                     */
      /*************************************************/

      /* Scenario Names */
      read data &_worklib.._opt_distinct_scenarios
         into scenarioNameCopy = scenario_name_copy;

      put '    *********************************************************';
      put '     Scenario = ' scenarioNameCopy;
      put ;
      put '      Start Time: ' startTime time.;
      put ;

      /* Demand Forecast*/
      read data &outlib..&input_demand_fcst. (where=(predict_date >= &start_date.)) nogroupby
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

      /* Service attributes (Note that num_cancelled is not being used yet) */
      read data &_worklib..input_service_attributes_pp nogroupby
         into [facility service_line sub_service ip_op_indicator med_surg_indicator]
            /* numCancel=num_cancelled */
            losMean=length_stay_mean;

      /* Covid test capacity */
      read data &_worklib.._opt_parameters_date_1
         into [date]
            totalDailyRapidTests=rapid_tests
            totalDailyNotRapidTests=not_rapid_tests
            phaseID=sequence;

      /* Global parameters */
      read data &_worklib.._opt_parameters_global into
         allowOpeningOnlyOnPhase = allow_opening_only_on_phase
         secondaryObjectiveTolerance = secondary_objective_tolerance
         testDaysBA = test_days_ba
         rapidTestDA = rapid_test_da
         removeDemandConstraints = remove_demand_constraints
         removeCovidConstraints = remove_covid_constraints
         holdNotRapidCovidTests = hold_not_rapid_covid_tests
         holdRapidCovidTests = hold_rapid_covid_tests
         treatMinDemandAsAggregate = treat_min_demand_as_aggregate
         useDecomp = use_decomp;

      /* Allowed opening dates */
      read data &_worklib.._opt_allowed_opening_dates into
         ALLOWED_OPENING_DATES = [date];

      /* Services that are already open */
      read data &_worklib.._opt_parameters_hierarchy (where=(parm_name='ALREADY_OPEN' and parm_value='YES'))
         into ALREADY_OPEN_SERVICES = [facility service_line sub_service];

      /* Min demand ratio constraints */
      read data &_worklib.._opt_parameters_hierarchy (where=(parm_name='MIN_DEMAND_RATIO'))
         into MIN_DEMAND_RATIO_CONSTRAINTS = [facility service_line sub_service]
            minDemandRatio = parm_value;

      /* Emergency surgical ratio constraints */
      read data &_worklib.._opt_parameters_hierarchy (where=(parm_name='EMER_SURGICAL_PTS_RATIO'))
         into EMER_SURGICAL_PTS_RATIO_CONSTRAINTS = [facility service_line sub_service]
            emerSurgRatioip = parm_value;

      /* Assign the emergency surgical ratio for each facility/service_line/sub_service combination, since the
         EMER_SURGICAL_PTS_RATIO_CONSTRAINTS could have 'ALL' for any of the hierarchies */
      for {<f,sl,ss> in EMER_SURGICAL_PTS_RATIO_CONSTRAINTS} do;
         for {<f2,sl2,ss2> in FAC_SLINE_SSERV} do;
            if( (f2=f or f='ALL') and (sl2=sl or sl='ALL') and (ss2=ss or ss='ALL')) then
               emerSurgRatio[f2,sl2,ss2] = max(emerSurgRatio[f2,sl2,ss2], input(emerSurgRatioip[f,sl,ss],best.)/100);
         end;
      end;

      /* ICU max utilization constraints */
      read data &_worklib.._opt_parameters_hierarchy (where=(parm_name='ICU_MAX_UTILIZATION'))
         into ICU_MAX_UTIL_FACILITIES = [facility]
            icuMaxUtilization = parm_value;

      /* Create a set of weeks and assign a week to each day. These will be used for min demand constraints. */
      num week{d in DAYS} = week(d);
      set <num> WEEKS = setof{d in DAYS} week[d];

      /* Create decomp blocks to decompose the problem by facility */
      num block_id{f in FACILITIES};
      num id init 0;
      for {f in FACILITIES} do;
         block_id[f] = id;
         id = id + 1;
      end;

      /*************************************************/
      /* Decision Variables                            */
      /*************************************************/

      /* Decide whether to open a sub-service on each day */
      var OpenFlg{FAC_SLINE_SSERV, DAYS} BINARY;

      /* Decide how many new patients and rescheduled patients to accept to each sub-service on each day.
         Note that we're only going to assign NewPatients on days that have positive demand, and we're only going to
         assign ReschedulePatients for sub-services that have positive cancellations, so first we create sets to restrict
         the variable hierarchies. */
      set <str,str,str,str,str,num> VAR_HIERARCHY_POSITIVE_DEMAND = {<f,sl,ss,iof,msf,d> in FAC_SLINE_SSERV_IO_MS_DAYS : demand[f,sl,ss,iof,msf,d] > 0
                                                                                                                         or removeDemandConstraints = 1};
      set <str,str,str,str,str,num> VAR_HIERARCHY_POSITIVE_CANCEL = {<f,sl,ss,iof,msf,d> in FAC_SLINE_SSERV_IO_MS_DAYS : numCancel[f,sl,ss,iof,msf] > 0};
      var NewPatients{VAR_HIERARCHY_POSITIVE_DEMAND} >= 0;
      var ReschedulePatients{VAR_HIERARCHY_POSITIVE_CANCEL} >= 0;

      /* Calculate total number of patients for day d */
      impvar TotalPatients{<f,sl,ss,iof,msf,d> in FAC_SLINE_SSERV_IO_MS_DAYS} =
         sum{d1 in DAYS: (max((d - losMean[f,sl,ss,iof,msf] + 1), minDay)) <= d1 <= d}
            ((if <f,sl,ss,iof,msf,d1> in VAR_HIERARCHY_POSITIVE_DEMAND then NewPatients[f,sl,ss,iof,msf,d1] else 0)
           + (if <f,sl,ss,iof,msf,d1> in VAR_HIERARCHY_POSITIVE_CANCEL then ReschedulePatients[f,sl,ss,iof,msf,d1] else 0));


      /*************************************************/
      /* Constraints                                   */
      /*************************************************/

      /* New patients cannot exceed demand if the sub service is open */
      con Maximum_Demand{<f,sl,ss,iof,msf,d> in VAR_HIERARCHY_POSITIVE_DEMAND : removeDemandConstraints = 0}:
         NewPatients[f,sl,ss,iof,msf,d] <= demand[f,sl,ss,iof,msf,d]*OpenFlg[f,sl,ss,d]
                   suffixes=(block=block_id[f]);

      /* Rescheduled patients are not allowed if the sub service is not open. In this constraint, numCancel
         acts as a big-M coefficient. */
      con Reschedule_Allowed{<f,sl,ss,iof,msf,d> in VAR_HIERARCHY_POSITIVE_CANCEL : removeDemandConstraints = 0}:
         ReschedulePatients[f,sl,ss,iof,msf,d] <= numCancel[f,sl,ss,iof,msf]*OpenFlg[f,sl,ss,d]
                   suffixes=(block=block_id[f]);

      /* The total number of patients rescheduled across all the days cannot exceed the number of
         cancelled patients. */
      con Reschedule_Maximum{<f,sl,ss,iof,msf> in FAC_SLINE_SSERV_IO_MS : numCancel[f,sl,ss,iof,msf] > 0 and removeDemandConstraints = 0}:
         sum{d in DAYS} ReschedulePatients[f,sl,ss,iof,msf,d] <= numCancel[f,sl,ss,iof,msf]
                   suffixes=(block=block_id[f]);

      /* If a sub-service is open, we must satisfy a minimum proportion of the weekly demand if minDemandRatio > 0. If treatMinDemandAsAggregate = 0,
         then we want to treat "ALL" as applying the min demand constraint separately to each subservice. */
      con Minimum_Demand_NoAgg{<f,sl,ss> in MIN_DEMAND_RATIO_CONSTRAINTS, <f2,sl2,ss2> in FAC_SLINE_SSERV, w in WEEKS :
                               treatMinDemandAsAggregate = 0 and removeDemandConstraints = 0
                               and (f2=f or f='ALL') and (sl2=sl or sl='ALL') and (ss2=ss or ss='ALL')}:
         sum{<(f2),(sl2),(ss2),iof,msf,d> in FAC_SLINE_SSERV_IO_MS_DAYS : week[d]=w}
                ((if <f2,sl2,ss2,iof,msf,d> in VAR_HIERARCHY_POSITIVE_DEMAND then NewPatients[f2,sl2,ss2,iof,msf,d] else 0)
               + (if <f2,sl2,ss2,iof,msf,d> in VAR_HIERARCHY_POSITIVE_CANCEL then ReschedulePatients[f2,sl2,ss2,iof,msf,d] else 0)
               - (input(minDemandRatio[f,sl,ss],best.)/100 * newPatientsBeforeCovid[f2,sl2,ss2,iof,msf,d] * OpenFlg[f2,sl2,ss2,d]))
            >= 0
                  suffixes=(block=block_id[f2]);

      /* If a sub-service is open, we must satisfy a minimum proportion of the weekly demand if minDemandRatio > 0. If treatMinDemandAsAggregate = 1,
         then we want to treat "ALL" as aggregating the min demand constraint across facilities or services or subservices. We are breaking these
         into two sets of constraints -- one where facility is not equal to 'ALL' and one where facility is equal to 'ALL' -- so that we can
         define decomp blocks for the ones that do not span all facilities. */
      con Minimum_Demand_Agg{<f,sl,ss> in MIN_DEMAND_RATIO_CONSTRAINTS, w in WEEKS :
                             treatMinDemandAsAggregate = 1 and removeDemandConstraints = 0 and f ne 'ALL'}:
         sum{<(f),sl2,ss2,iof,msf,d> in FAC_SLINE_SSERV_IO_MS_DAYS : (sl2=sl or sl='ALL') and (ss2=ss or ss='ALL') and week[d]=w}
                ((if <f,sl2,ss2,iof,msf,d> in VAR_HIERARCHY_POSITIVE_DEMAND then NewPatients[f,sl2,ss2,iof,msf,d] else 0)
               + (if <f,sl2,ss2,iof,msf,d> in VAR_HIERARCHY_POSITIVE_CANCEL then ReschedulePatients[f,sl2,ss2,iof,msf,d] else 0)
               - (input(minDemandRatio[f,sl,ss],best.)/100 * newPatientsBeforeCovid[f,sl2,ss2,iof,msf,d] * OpenFlg[f,sl2,ss2,d]))
            >= 0
                  suffixes=(block=block_id[f]);

      con Minimum_Demand_Agg_ALL{<f,sl,ss> in MIN_DEMAND_RATIO_CONSTRAINTS, w in WEEKS :
                                 treatMinDemandAsAggregate = 1 and removeDemandConstraints = 0 and f='ALL'}:
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

      /* Total patients cannot exceed resource capacity. We are breaking these into two sets of constraints -- one where facility
         is not equal to 'ALL' and one where facility is equal to 'ALL' -- so that we can define decomp blocks for the
         ones that do not span all facilities. */
      con Resources_Capacity{<f,sl,ss,r> in FAC_SLINE_SSERV_RES, d in DAYS : f ne 'ALL'}:
         sum {<(f),sl2,ss2,iof,msf,(r)> in FAC_SLINE_SSERV_IO_MS_RES : (sl2=sl or sl='ALL') and (ss2=ss or ss='ALL')}
            utilization[f,sl2,ss2,iof,msf,r]*TotalPatients[f,sl2,ss2,iof,msf,d]
            <= capacity[f,sl,ss,r]
                   suffixes=(block=block_id[f]);

      con Resources_Capacity_ALL{<f,sl,ss,r> in FAC_SLINE_SSERV_RES, d in DAYS : f = 'ALL'}:
         sum {<f2,sl2,ss2,iof,msf,(r)> in FAC_SLINE_SSERV_IO_MS_RES : (sl2=sl or sl='ALL') and (ss2=ss or ss='ALL')}
            utilization[f2,sl2,ss2,iof,msf,r]*TotalPatients[f2,sl2,ss2,iof,msf,d]
            <= capacity[f,sl,ss,r];

      /* Max ICU utilization for each facility */
      con Max_ICU_Utilization{f in ICU_MAX_UTIL_FACILITIES, f2 in FACILITIES, d in DAYS : (f='ALL') or (f2=f)}:
         sum{<(f2),sl,ss,iof,msf,r> in FAC_SLINE_SSERV_IO_MS_RES : r in ICU_RESOURCES}
             utilization[f2,sl,ss,iof,msf,r]*TotalPatients[f2,sl,ss,iof,msf,d]
            <= (input(icuMaxUtilization[f],best.)/100)
                * sum{<(f2),sl,ss,r> in FAC_SLINE_SSERV_RES : r in ICU_RESOURCES} capacity[f2,sl,ss,r]
                   suffixes=(block=block_id[f2]);

      /* COVID-19 rapid tests: Total number of non-surgical inpatients admitted should be less than the daily rapid tests available */
      con COVID19_Day_Of_Admission_Testing{d in DAYS : rapidTestDA > 0}:
         sum {<f,sl,ss,iof,msf,(d)> in VAR_HIERARCHY_POSITIVE_DEMAND : iof='I' and msf ne 'SURG'} (NewPatients[f,sl,ss,iof,msf,d])
       + sum {<f,sl,ss,iof,msf,(d)> in VAR_HIERARCHY_POSITIVE_CANCEL : iof='I' and msf ne 'SURG'} (ReschedulePatients[f,sl,ss,iof,msf,d])
       + sum {<f,sl,ss,iof,msf,(d)> in VAR_HIERARCHY_POSITIVE_DEMAND : msf = 'SURG'} (NewPatients[f,sl,ss,iof,msf,d] * emerSurgRatio[f,sl,ss])
       + sum {<f,sl,ss,iof,msf,(d)> in VAR_HIERARCHY_POSITIVE_CANCEL : msf = 'SURG'} (ReschedulePatients[f,sl,ss,iof,msf,d] * emerSurgRatio[f,sl,ss])
       <= (max(0,(totalDailyRapidTests[d] - holdRapidCovidTests))  / rapidTestDA);

      /* COVID-19 not-rapid tests: Total number of surgical patients tested testDaysBA days before arrival should be less than the
         daily not-rapid tests available */
      con COVID19_Before_Admission_Testing{d in DAYS : testDaysBA > 0 and d + testDaysBA in DAYS}:
         sum {<f,sl,ss,iof,msf,d1> in VAR_HIERARCHY_POSITIVE_DEMAND : msf='SURG' and d1 = d + testDaysBA} (NewPatients[f,sl,ss,iof,msf,d1] * (1-emerSurgRatio[f,sl,ss]))
       + sum {<f,sl,ss,iof,msf,d1> in VAR_HIERARCHY_POSITIVE_CANCEL : msf='SURG' and d1 = d + testDaysBA} (ReschedulePatients[f,sl,ss,iof,msf,d1] * (1-emerSurgRatio[f,sl,ss]))
       <= max(0,(totalDailyNotRapidTests[d]- holdNotRapidCovidTests));

      if removeCovidConstraints = 1 then do;
         /* Reset UB of COVID constraints to a big-M constant. I am doing it this way instead of disabling the constraints because we still might want to
            see the COVID test usage in the output, and for that we use the constraint .body suffixes. With the current testing protocol, the big-M constant
            is the total demand per day plus cancellations since each patient is getting tested at most once. But if we change the testing protocol where
            patients are getting tested multiple times, or caregivers or visitors are also being tested, we will need to refine the big-M constant.
            Or if performance becomes an issue, or if we don't need to see the COVID test usage in the output, we can remove this section and add
            the condition "and removeCovidConstraints = 0" to both of the constraints (and suppress creation of the covid test usage output table). */
         for {d in DAYS} do;
            if removeDemandConstraints = 0 then do;
               if rapidTestDA > 0 then COVID19_Day_Of_Admission_Testing[d].ub
                  = sum{<f,sl,ss,iof,msf,(d)> in VAR_HIERARCHY_POSITIVE_DEMAND} demand[f,sl,ss,iof,msf,d]
                    + sum{<f,sl,ss,iof,msf> in FAC_SLINE_SSERV_IO_MS} numCancel[f,sl,ss,iof,msf];
               if testDaysBA > 0 and d + testDaysBA in DAYS then COVID19_Before_Admission_Testing[d].ub
                  = sum{<f,sl,ss,iof,msf,d1> in VAR_HIERARCHY_POSITIVE_DEMAND : d1 = d + testDaysBA} demand[f,sl,ss,iof,msf,d1]
                    + sum{<f,sl,ss,iof,msf> in FAC_SLINE_SSERV_IO_MS} numCancel[f,sl,ss,iof,msf];
            end;
            else do;
               COVID19_Day_Of_Admission_Testing[d].ub = constant('BIG');
               if d + testDaysBA in DAYS then COVID19_Before_Admission_Testing[d].ub = constant('BIG');
            end;
         end;
      end;

      /*************************************************/
      /* Objective Functions                           */
      /*************************************************/

      max Total_Revenue =
          sum{<f,sl,ss,iof,msf,d> in VAR_HIERARCHY_POSITIVE_DEMAND} NewPatients[f,sl,ss,iof,msf,d] * revenue[f,sl,ss,iof,msf]
        + sum{<f,sl,ss,iof,msf,d> in VAR_HIERARCHY_POSITIVE_CANCEL} ReschedulePatients[f,sl,ss,iof,msf,d] * revenue[f,sl,ss,iof,msf];

      max Total_Margin =
          sum{<f,sl,ss,iof,msf,d> in VAR_HIERARCHY_POSITIVE_DEMAND} NewPatients[f,sl,ss,iof,msf,d] * margin[f,sl,ss,iof,msf]
        + sum{<f,sl,ss,iof,msf,d> in VAR_HIERARCHY_POSITIVE_CANCEL} ReschedulePatients[f,sl,ss,iof,msf,d] * margin[f,sl,ss,iof,msf];

      /*************************************************/
      /* Solve                                         */
      /*************************************************/

      /* First we want to find out what is the maximum demand we can handle without the covid-19 tests.
         We drop the COVID constraints and the minimum demand constraints. We're also going to fix OpenFlg to 1
         for every sub-service (i.e., the only reason we might not open a sub-service is because we don't have enough
         COVID-19 tests), so we can also drop the Service_Stay_Open constraints. And we fix ReschedulePatients to 0
         because we only want to consider original demand (i.e., non-covid impacts) to calculate newPatientsBeforeCovid. */
      drop COVID19_Day_Of_Admission_Testing
           COVID19_Before_Admission_Testing
           Max_ICU_Utilization
           Minimum_Demand_NoAgg
           Minimum_Demand_Agg
           Minimum_Demand_Agg_ALL
           Service_Stay_Open
           Open_on_Allowed_Dates;

      fix OpenFlg = 1;
      fix ReschedulePatients = 0;

      if useDecomp = 0 then do;
         solve obj Total_Revenue with milp / maxtime=300 loglevel=3 relobjgap=0.005;
      end;
      else do;
         solve obj Total_Revenue with milp / maxtime=300 loglevel=3 relobjgap=0.005 decomp=(method=user);
      end;

      /* The maximum demand without covid-19 tests is equal to the number of new patients that we saw,
         subject to other resource capacity constraints */
      for {<f,sl,ss,iof,msf,d> in FAC_SLINE_SSERV_IO_MS_DAYS}
         newPatientsBeforeCovid[f,sl,ss,iof,msf,d] =
            if <f,sl,ss,iof,msf,d> in VAR_HIERARCHY_POSITIVE_DEMAND then NewPatients[f,sl,ss,iof,msf,d].sol else 0;

      /* Now restore the COVID constraints, the minimum demand constraints, and the Service_Stay_Open constraints,
         and unfix OpenFlg and ReschedulePatients, and then solve again. */
      restore COVID19_Day_Of_Admission_Testing
              COVID19_Before_Admission_Testing
              Max_ICU_Utilization
              Minimum_Demand_NoAgg
              Minimum_Demand_Agg
              Minimum_Demand_Agg_ALL
              Service_Stay_Open
              Open_on_Allowed_Dates;

      unfix OpenFlg;
      unfix ReschedulePatients;

      /* If some sub-services are already open, fix OpenFlg to 1 */
      for {<f,sl,ss> in ALREADY_OPEN_SERVICES} do;
         for {<f2,sl2,ss2> in FAC_SLINE_SSERV : (f2=f or f='ALL') and (sl2=sl or sl='ALL') and (ss2=ss or ss='ALL')}
            fix OpenFlg[f2,sl2,ss2,minDay] = 1;
      end;

      /* Initialize a constraint on the Primary Objective. We will use this when we solve for the Secondary Objective,
         but we cannot define a constraint inside of an if-condition. So we define the constraint here, then drop it
         before the Primary Objective solve, and restore it before the Secondary Objective solve. */
      num primary_objective_value init 0;
      con Primary_Objective_Constraint:
          sum{<f,sl,ss,iof,msf,d> in VAR_HIERARCHY_POSITIVE_DEMAND} NewPatients[f,sl,ss,iof,msf,d] * revenue[f,sl,ss,iof,msf]
        + sum{<f,sl,ss,iof,msf,d> in VAR_HIERARCHY_POSITIVE_CANCEL} ReschedulePatients[f,sl,ss,iof,msf,d] * revenue[f,sl,ss,iof,msf]
        >= secondaryObjectiveTolerance * primary_objective_value;

      if _solution_status_ in {'OPTIMAL', 'OPTIMAL_AGAP', 'OPTIMAL_RGAP', 'OPTIMAL_COND', 'CONDITIONAL_OPTIMAL'} then do;

         /* Drop Primary_Objective_Constraint and solve for primary objective (Total_Revenue) */
         drop Primary_Objective_Constraint;

         if useDecomp = 0 then do;
            solve obj Total_Revenue with milp / primalin maxtime=600 loglevel=3 relobjgap=0.005;
         end;
         else do;
            solve obj Total_Revenue with milp / primalin maxtime=600 loglevel=3 relobjgap=0.005 decomp=(method=user);
         end;

         /* Solve for secondary objective only if primary objective solve was successful */
         if _solution_status_ in {'OPTIMAL', 'OPTIMAL_AGAP', 'OPTIMAL_RGAP', 'OPTIMAL_COND', 'CONDITIONAL_OPTIMAL'} then do;

            put Total_Revenue.sol=;
            put Total_Margin.sol=;

            primary_objective_value = Total_Revenue.sol;
            restore Primary_Objective_Constraint;

            if useDecomp = 0 then do;
               solve obj Total_Margin with milp / primalin maxtime=300 relobjgap=0.005 loglevel=3;
            end;
            else do;
               solve obj Total_Margin with milp / primalin maxtime=300 relobjgap=0.005 loglevel=3 decomp=(method=user);
            end;

            put Total_Revenue.sol=;
            put Total_Margin.sol=;
         end;
      end; 

      /*************************************************/
      /* Create output data                            */
      /*************************************************/

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
         Phase_ID=phaseID[d]
         NewPatients=(round(OptNewPatients[f,sl,ss,iof,msf,d],0.01))
         ReschedulePatients=(round(OptReschedulePatients[f,sl,ss,iof,msf,d],0.01))
         TotalPatients=(round(TotalPatients[f,sl,ss,iof,msf,d],0.01))
         OptRevenue=(round(OptRevenue[f,sl,ss,iof,msf,d],0.01))
         OptMargin=(round(OptMargin[f,sl,ss,iof,msf,d],0.01))
         Demand=(round(demand[f,sl,ss,iof,msf,d],0.01))
         NewPatientsBeforeCovid=(round(newPatientsBeforeCovid[f,sl,ss,iof,msf,d],0.01));
         
      num firstDayWithPatients{<f,sl,ss> in FAC_SLINE_SSERV} 
         = min{<(f),(sl),(ss),iof,msf,d> in FAC_SLINE_SSERV_IO_MS_DAYS : TotalPatients[f,sl,ss,iof,msf,d].sol > 0} d;  
         
      num OpenFlgSol{<f,sl,ss> in FAC_SLINE_SSERV, d in DAYS}
         = if d < firstDayWithPatients[f,sl,ss] then 0 else (round(OpenFlg[f,sl,ss,d].sol,0.01));

      create data &_worklib.._opt_summary
         from [facility service_line sub_service day]={<f,sl,ss> in FAC_SLINE_SSERV, d in DAYS}
         Phase_ID=phaseID[d]
         OpenFlg=OpenFlgSol;

      num OptResourceUsage{<f,sl,ss,r> in FAC_SLINE_SSERV_RES, d in DAYS} =
         if f ne 'ALL' then Resources_Capacity[f,sl,ss,r,d].body
         else Resources_Capacity_ALL[f,sl,ss,r,d].body;
         
      create data &_worklib.._opt_resource_usage
         from [facility service_line sub_service resource day]={<f,sl,ss,r> in FAC_SLINE_SSERV_RES, d in DAYS}
         Phase_ID=phaseID[d]
         capacity = capacity[f,sl,ss,r]
         usage = (round(OptResourceUsage[f,sl,ss,r,d],0.01));

      num OptResourceUsageDetail{<f,sl,ss,iof,msf,r> in FAC_SLINE_SSERV_IO_MS_RES, d in DAYS} =
         utilization[f,sl,ss,iof,msf,r] * TotalPatients[f,sl,ss,iof,msf,d].sol;

      num OptResourceUsageCapacity{<f,sl,ss,iof,msf,r> in FAC_SLINE_SSERV_IO_MS_RES, d in DAYS} =
         min{<f2,sl2,ss2,(r)> in FAC_SLINE_SSERV_RES : (f=f2 or f2='ALL') and (sl=sl2 or sl2='ALL') and (ss=ss2 or ss2='ALL')} capacity[f2,sl2,ss2,r];

      create data &_worklib.._opt_resource_usage_detail
         from [facility service_line sub_service ip_op_indicator med_surg_indicator resource day]={<f,sl,ss,iof,msf,r> in FAC_SLINE_SSERV_IO_MS_RES, d in DAYS}
         Phase_ID=phaseID[d]
         usage = OptResourceUsageDetail[f,sl,ss,iof,msf,r,d]
         capacity = OptResourceUsageCapacity[f,sl,ss,iof,msf,r,d];

      create data &_worklib.._opt_covid_test_usage
         from [day]={d in DAYS}
            Phase_ID=phaseID[d]
            rapidTestsAvailable=(if (totalDailyRapidTests[d] - holdRapidCovidTests) < 0 then 0 else (totalDailyRapidTests[d] - holdRapidCovidTests))
            rapidTestsUsed=(if rapidTestDA > 0 then COVID19_Day_Of_Admission_Testing[d].body else 0)
            notRapidTestsAvailable=(if (totalDailyNotRapidTests[d] - holdNotRapidCovidTests) < 0 then 0 else (totalDailyNotRapidTests[d] - holdNotRapidCovidTests))
            notRapidTestsUsed=(if (testDaysBA > 0 and d + testDaysBA in DAYS) then COVID19_Before_Admission_Testing[d].body else 0);

      endTime = time();
      put '      End Time: ' endTime time.;

      endsource;
      runOptmodel / code=pgm groupBy='scenario_name' nGroupByThreads='ALL';
      run;
   quit;

   /* Process and aggregate output data for reporting */
   data &_worklib.._opt_detail_week;
      format date date9.;
      format week_start_date date9.;
      set &_worklib.._opt_detail (rename=(day=date));
      week_start_date = date - (weekday(date)-1);
   run;

   proc cas;
      aggregation.aggregate / 
         table={caslib="&_worklib.", name="_opt_detail_week",
                groupby={"scenario_name","facility","service_line","sub_service","ip_op_indicator","med_surg_indicator","week_start_date"}}
         saveGroupByFormat=false
         varSpecs={{name="NewPatients", summarySubset="sum", columnNames="NewPatients"}
                   {name="ReschedulePatients", summarySubset="sum", columnNames="ReschedulePatients"}
                   {name="TotalPatients", summarySubset="sum", columnNames="TotalPatients"}
                   {name="OptMargin", summarySubset="sum", columnNames="OptMargin"}
                   {name="OptRevenue", summarySubset="sum", columnNames="OptRevenue"}
                   {name="Demand", summarySubset="sum", columnNames="Demand"}
                   {name="NewPatientsBeforeCovid", summarySubset="sum", columnNames="NewPatientsBeforeCovid"}}
         casOut={caslib="&_worklib.", name="_opt_detail_agg", replace=true};
      run;
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
      format day date9.;
      set &_worklib.._opt_summary;
   run;

   data &outlib..&output_opt_resource_usage (promote=yes);
      format day date9.;
      set &_worklib.._opt_resource_usage;
      utilization = round(usage / capacity, 0.001);
   run;

   /* Aggregate _opt_resource_usage_detail to Facility/Service Line/Subservice level */
   proc cas;
      aggregation.aggregate / 
         table={caslib="&_worklib.", name="_opt_resource_usage_detail",
                groupby={"scenario_name","facility","service_line","sub_service","day","Phase_ID","resource"}}
         saveGroupByFormat=false
         varSpecs={{name="usage", summarySubset="sum", columnNames="sumUsage"},
                   {name="capacity", summarySubset="min", columnNames="minCapacity"}}
         casOut={caslib="&_worklib.", name="_opt_resource_usage_detail", replace=true};
      run;
   quit;

   proc sql noprint;
      select max(capacity) into :max_capacity
      from &_worklib..input_capacity_pp;
   quit;

   data &outlib..&output_opt_resource_usage_detail (promote=yes);
      format day date9.;
      set &_worklib.._opt_resource_usage_detail;
      usage = round(sumUsage, 0.01);
      capacity = minCapacity;
      if capacity > &max_capacity then do;
         capacity = .;
         utilization = .;
      end;
      else utilization = round(usage / capacity, 0.01);
      drop sumUsage minCapacity;
   run;

   data &outlib..&output_opt_covid_test_usage (promote=yes);
      format day date9.;
      set &_worklib.._opt_covid_test_usage;
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