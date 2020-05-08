*------------------------------------------------------------------------------*
| Program: cc_create_mock_data
|
| Description: 
|
*------------------------------------------------------------------------------* ;
%macro cc_create_mock_data;

   libname mock (work);
   /* Uncomment the following line only if you want to replace the data on the server */
   /*
   libname mock "/ordsrv3/OR_CENTER/FILES/Cleveland Clinic/tiny_input";
   */
   
   %let facility_list = H1 H2;
   %let service_lines_list = Orthopedics Cardiology;

   /* Orthopedics */
   %let subservice_list_1 = Joint_Replacement Sports_Medicine General_Medical_Orthopedics;
   %let ip_op_list_1 = I O O;
   %let med_surg_list_1 = SURG MED MED;

   /* Cardiology */
   %let subservice_list_2 = Cardiac_Surgery Medical_Cardiology;
   %let ip_op_list_2 = I O;
   %let med_surg_list_2 = SURG MED;

   data base_table;
      format facility $8. service_line sub_service $32. ip_op_indicator med_surg_indicator $4.;
      %do i = 1 %to %sysfunc(countw(&facility_list,' '));
         facility = "%scan(&facility_list, &i)";
         %do j = 1 %to %sysfunc(countw(&service_lines_list,' '));
            service_line = "%scan(&service_lines_list, &j)";
            %do k = 1 %to %sysfunc(countw(&&subservice_list_&j,' '));
               sub_service = "%scan(&&subservice_list_&j, &k)";
               ip_op_indicator = "%scan(&&ip_op_list_&j, &k)";
               med_surg_indicator = "%scan(&&med_surg_list_&j, &k)";
               output;
            %end;
         %end;
      %end;
   run;

   data mock.input_financials;
      set base_table;
      call streaminit(100);
      if sub_service = 'Cardiac_Surgery' then revenue = round(80000 + 20000*rand('UNIFORM'), 0.01);
      else if sub_service = 'Joint_Replacement' then revenue = round(30000 + 40000*rand('UNIFORM'), 0.01);
      else revenue = round(2000 + 8000*rand('UNIFORM'), 0.01);
      margin = round((0.2 + 0.5*rand('UNIFORM'))*revenue, 0.01);
   run;

   data mock.input_service_attributes;
      set base_table;
      call streaminit(101);
      if sub_service = 'Cardiac_Surgery' then do;
         length_stay_mean = 5;
         num_cancelled = round(5 + 10*rand('UNIFORM'), 1);
      end;
      else if sub_service = 'Joint_Replacement' then do;
         length_stay_mean = 3;
         num_cancelled = round(10 + 40*rand('UNIFORM'), 1);
      end;
      else do;
         length_stay_mean = 1;
         num_cancelled = round(50 + 100*rand('UNIFORM'), 1);
      end;
   run;

   data mock.input_capacity;
      format facility $8. service_line sub_service resource $32.;
      call streaminit(102);

      /* Medication - shared across all facilities */
      facility = 'ALL';
      service_line = 'ALL';
      sub_service = 'ALL';
      resource = 'Medication';
      capacity = 8000;
      output;
      
      /* Cardiac surgeons - shared across all facilities */
      facility = 'ALL';
      service_line = 'Cardiology';
      sub_service = 'Cardiac_Surgery';
      resource = 'Surgeon';
      capacity = 15;
      output;

      %do i = 1 %to %sysfunc(countw(&facility_list,' '));
         facility = "%scan(&facility_list, &i)";

         /* Operating Rooms - shared across service lines */
         service_line = 'ALL';
         sub_service = 'ALL';
         resource = 'Operating_Room';
         capacity = round(4 + 4*rand('UNIFORM'), 1);
         output;
         
         /* Orthopedic Surgeons and Recovery Services - for separate service lines */
         %do j = 1 %to %sysfunc(countw(&service_lines_list,' '));
            service_line = "%scan(&service_lines_list, &j)";

            if service_line = 'Orthopedics' then do;
               if &i = 1 then sub_service = 'ALL';
               else sub_service = "%scan(&&subservice_list_&j, 1)";
               resource = 'Surgeon';
               capacity = round(3 + 10*rand('UNIFORM'), 1);
               output;
            end;

            sub_service = "%scan(&&subservice_list_&j, 1)";
            if &i = 2 and service_line = 'Cardiology' then service_line = 'ALL';
            resource = 'Recovery_Services';
            capacity = round(10 + 30*rand('UNIFORM'), 1);
            output;
         %end;
      %end;
   run;

   data mock.input_utilization;
      set base_table;
      format resource $32.;
      call streaminit(103);

      if _n_ = 1 then do;
         declare hash h0(dataset:'mock.input_service_attributes');
         h0.defineKey('facility','service_line','sub_service','ip_op_indicator','med_surg_indicator');
         h0.defineData('length_stay_mean');
         h0.defineDone();
      end;

      length_stay_mean = .;
      rc0 = h0.find();

      if med_surg_indicator = 'SURG' then do;
         resource = 'Surgeon';
         utilization_mean = round(0.3 + 0.4*rand('UNIFORM'), 0.1)/length_stay_mean;
         output;

         resource = 'Operating_Room';
         utilization_mean = min(round(utilization_mean * (1 + (0.2 + 0.4*rand('UNIFORM'))), 0.1),1);
         output;
         
         resource = 'Medication';
         utilization_mean = round(2 + 6*rand('UNIFORM'),1);
         output;
      end;
      else if sub_service ne 'Sports_Medicine' then do;
         resource = 'Medication';
         utilization_mean = 1;
         output;
      end;

      if ip_op_indicator = 'I' then do;
         resource = 'Recovery_Services';
         utilization_mean = 1;
         output;
      end;

      drop length_stay_mean rc0;
   run;

   data mock.input_demand;
      set base_table;
      format date date.;
      call streaminit(104);
      /* Start on a Sunday and end on a Saturday so we get the endpoints 
         for forecasting in the right order. */
      do date = '05May2019'd to '25May2019'd;
         if med_surg_indicator = 'SURG' then demand = round(4*rand('UNIFORM'),1);
         else demand = round(20*rand('UNIFORM'),1);
         output;
      end;
   run;

   data base_parameters_table;
      format parm_name $32. parm_value $32. parm_desc $256.;
      
      parm_name = 'DAYS_BEFORE_SERVICE';
      parm_value = '3';
      parm_desc = 'Number of days before service for COVID-19 test. Use 0 if not populated.';
      output;

      parm_name = 'TEST_FREQ_DAYS';
      parm_value = '5';
      parm_desc = 'How often the patent needs to be tested. Use 0 if not populated.';
      output;

      parm_name = 'TEST_VISITORS';
      parm_value = 'YES';
      parm_desc = 'If the visitors need to be tested too (once). Use NO if not populated.';
      output;

      parm_name = 'OPEN_FULLY';
      parm_value = '1';
      parm_desc = 'Required to open all service_lines/sub_services together (1) or not (0). Use 0 if not populated.';
      output;

      parm_name = 'TESTS_NUM_PHASE_1';
      parm_value = '1400';
      parm_desc = 'Number of available tests in Phase 1. Use unlimited if not populated.';
      output;

      parm_name = 'TESTS_DATE_PHASE_1';
      parm_value = '4/27/2020';
      parm_desc = 'Date at which tests are available in Phase 1. Use first day in planning horizon.';
      output;

      parm_name = 'TESTS_NUM_PHASE_2';
      parm_value = '1600';
      parm_desc = 'Number of available tests in Phase 2. Do not use if not populated.';
      output;

      parm_name = 'TESTS_DATE_PHASE_2';
      parm_value = '5/15/2020';
      parm_desc = 'Date at which tests are available in Phase 2. Do not use if not populated.';
      output;

      parm_name = 'PLANNING_HORIZON';
      parm_value = '12';
      parm_desc = 'Number of weeks to forecast demand and plan re-opening. Use 12 if not populated.';
      output;
   run;

   proc sql noprint;
      create table base_fac_serv_sub as
         select distinct facility, service_line, sub_service, ip_op_indicator, med_surg_indicator
         from base_table;
      create table mock.input_opt_parameters as
         select a.*, b.*
         from base_fac_serv_sub as a, base_parameters_table as b;
   quit;    

   /* Uncomment this section only if you want to replace the promoted data */
   /*
   data casuser.input_capacity; set mock.input_capacity; run;
   data casuser.input_demand; set mock.input_demand; run;
   data casuser.input_financials; set mock.input_financials; run;
   data casuser.input_opt_parameters; set mock.input_opt_parameters; run;
   data casuser.input_service_attributes; set mock.input_service_attributes; run;
   data casuser.input_utilization; set mock.input_utilization; run;

   proc casutil outcaslib="cc";
      droptable casdata="input_capacity_mock" incaslib="cc" quiet;
      droptable casdata="input_demand_mock" incaslib="cc" quiet;
      droptable casdata="input_financials_mock" incaslib="cc" quiet;
      droptable casdata="input_opt_parameters_mock" incaslib="cc" quiet;
      droptable casdata="input_service_attributes_mock" incaslib="cc" quiet;
      droptable casdata="input_utilization_mock" incaslib="cc" quiet;
      promote casdata="input_capacity" casout="input_capacity_mock";
      promote casdata="input_demand" casout="input_demand_mock";
      promote casdata="input_financials" casout="input_financials_mock";
      promote casdata="input_opt_parameters" casout="input_opt_parameters_mock";
      promote casdata="input_service_attributes" casout="input_service_attributes_mock";
      promote casdata="input_utilization" casout="input_utilization_mock";
   run;quit;
   */

%mend cc_create_mock_data;