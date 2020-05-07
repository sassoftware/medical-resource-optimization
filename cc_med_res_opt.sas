*------------------------------------------------------------------------------*
| Program: cc_med_res_opt
|
| Description: 
|
*--------------------------------------------------------------------------------* ;
%macro cc_med_res_opt(
    inlib=cc
   ,outlib=cc
   ,input_utilization=input_utilization
   ,input_capacity =input_capacity
   ,input_financials=input_financials
   ,input_service_attributes=input_service_attributes
   ,input_opt_parameters=input_opt_parameters
   ,output_opt_detail=output_opt_detail
   ,output_opt_summary=output_opt_summary
   ,min_demand_ratio=1
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
   %if %sysfunc(exist(&outlib..output_fd_demand_fcst))=0 %then %do;
      %put FATAL: Missing &outlib..output_fd_demand_fcst, exiting from &sysmacroname.;
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

   /* List work tables 
   %let _work_tables=%str(  
        &_worklib.._TMP_OD_2
        &_worklib.._TMP_OD_1
        &_worklib.._tmp_od_adj_und_1
        &_worklib.._tmp_od_adj_und_2
        &_worklib.._tmp_od_adj_und_3
         );
*/

   /* List output tables */
   %let output_tables=%str(         
        &outlib..&output_opt_detail
        &outlib..&output_opt_summary
        &outlib..output_opt_detail_agg
         );


   /* Delete output data if already exists */
   proc delete data= &output_tables.;
   run;

   /* Delete work data if already exists
   proc delete data= &_work_tables.;
   run;
 */

   /************************************/
   /************ANALYTICS *************/
   /***********************************/

   /* For debugging */
   %let filter1=%str((where=(service_line ~= 'Evaluation and Management' and ip_op_indicator='I')));
   %let filter2=%str((where=(service_line ~= 'Evaluation and Management')));

   /* min_demand_ratio is the proportion of demand that must be satisfied if a sub-service is open. Set it to 1 if you 
      want to require all demand to be satisfied. However, this might result in some sub-services not opening at all,
      because there are not enough covid-19 tests to accommodate the full demand. Set it to a smaller value (e.g., 0.8 or 
      0.6 or even 0.2) if you just want to make sure that we accept some minimum amount of the demand if we open a sub-service. */
/*    %let min_demand_ratio = 1.0; */

   proc optmodel;
   
      /* Master sets read from data */
      set <str,str,str,str,str,str> FAC_SLINE_SSERV_IO_MS_RES; /* From utilization */
      set <str,str,str,str,str,num> FAC_SLINE_SSERV_IO_MS_DAYS; /* From demand */
      set <str,str,str,str> FAC_SLINE_SSERV_RES; /* From capacity */
      set <str,str,str,str,str,str> PARAMS_SET;
         
      /* Derived Sets */
	  set HIER_IN_UTIL = setof {<f,sl,ss,iof,msf,r> in FAC_SLINE_SSERV_IO_MS_RES} <f,sl,ss,iof,msf>;

      set <str,str,str> FAC_SLINE_SSERV = 
		setof {<f,sl,ss,iof,msf,d> in FAC_SLINE_SSERV_IO_MS_DAYS/*: <f,sl,ss,iof,msf> in HIER_IN_UTIL*/} <f,sl,ss>;

      set <str,str,str,str,str> FAC_SLINE_SSERV_IO_MS = 
		setof {<f,sl,ss,iof,msf,d> in FAC_SLINE_SSERV_IO_MS_DAYS/*: <f,sl,ss,iof,msf> in HIER_IN_UTIL*/} <f,sl,ss,iof,msf>;

/* 	 set FAC_SLINE_SSERV_IO_MS_DAYS1 =  */
/* 		setof {<f,sl,ss,iof,msf,d> in FAC_SLINE_SSERV_IO_MS_DAYS: <f,sl,ss,iof,msf> in HIER_IN_UTIL} <f,sl,ss,iof,msf,d>; */
/*  */
/* 	 set FAC_SLINE_SSERV_RES1 =  */
/* 		setof {<f,sl,ss,r> in FAC_SLINE_SSERV_RES: <f,sl,ss> in {setof <f,sl,ss,iof,msf> in HIER_IN_UTIL <f,sl,ss>} <f,sl,ss,iof,msf,d>; */


      set <num> DAYS = setof {<f,sl,ss,iof,msf,d> in FAC_SLINE_SSERV_IO_MS_DAYS} <d>;

      num capacity{FAC_SLINE_SSERV_RES};

      num utilization{FAC_SLINE_SSERV_IO_MS_RES};

      num revenue{FAC_SLINE_SSERV_IO_MS};
      num margin{FAC_SLINE_SSERV_IO_MS};
      num losMean{FAC_SLINE_SSERV_IO_MS};
      num numCancel{FAC_SLINE_SSERV_IO_MS};

      num demand{FAC_SLINE_SSERV_IO_MS_DAYS};
      num maxCapacityWithoutCovid{FAC_SLINE_SSERV_IO_MS_DAYS} init 0;
      
      num minDay=min {d in DAYS} d;
      num maxDay=max {d in DAYS} d;
      
      num minDemandRatio init &min_demand_ratio;

/*       num paramValue{PARAMS_SET}; */
/*       num totalDailyRapidTests=paramValue['ALL','ALL','ALL','ALL','ALL','RAPID_TESTS_PHASE_1']; */
/*       num totalDailyNonRapidTests=paramValue['ALL','ALL','ALL','ALL','ALL','NOT_RAPID_TESTS_PHASE_1']; */
/*       num daysTestBeforeAdmSurg=paramValue['ALL','ALL','ALL','ALL','SURG','TEST_DAYS_BA']; */
      num totalDailyRapidTests=190;
      num totalDailyNonRapidTests=1200;
      num daysTestBeforeAdmSurg=2;
  
   /*    num losVar{FAC_SLINE_SSERV}; */
   /*    num visitorsMean{FAC_SLINE_SSERV}; */
   /*    num visitorsVar{FAC_SLINE_SSERV}; */
   /*    num minPctReschedule{FAC_SLINE_SSERV}; */
   /*    num maxPctReschedule{FAC_SLINE_SSERV}; */
   
   
      /* Read data from SAS data sets */ 
   
      /* Demand Forecast*/
      read data &outlib..output_fd_demand_fcst &filter1.
         into FAC_SLINE_SSERV_IO_MS_DAYS = [facility service_line sub_service ip_op_indicator med_surg_indicator predict_date]
            demand=daily_predict;

      /* Capacity */
      read data &_worklib..input_capacity_pp &filter2.
         into FAC_SLINE_SSERV_RES = [facility service_line sub_service resource]
            capacity;

      /* Utilization */
      read data &_worklib..input_utilization_pp &filter1.
         into FAC_SLINE_SSERV_IO_MS_RES = [facility service_line sub_service ip_op_indicator med_surg_indicator resource]
            utilization=utilization_mean;

      /* Financials */
      read data &_worklib..input_financials_pp &filter1.
         into [facility service_line sub_service ip_op_indicator med_surg_indicator]
            revenue 
            margin;
      
      /* Service attributes */
      read data &_worklib..input_service_attributes_pp &filter1.
         into [facility service_line sub_service ip_op_indicator med_surg_indicator]
            numCancel=num_cancelled
            losMean=length_stay_mean;

      /* Parameters */
/*       read data &_worklib..input_opt_parameters_pp &filter. */
/*          into PARAMS_SET = [facility service_line sub_service ip_op_indicator med_surg_indicator parm_name] */
/*             paramValue=parm_value; */
/*     */
      
      /******************Model variables, constraints, objective function*******************************/

      /* Decide to open or not a sub service */
      var OpenFlg{FAC_SLINE_SSERV, DAYS} BINARY;
   
      /* Related to how many new patients are actually accepted */
      var NewPatients{FAC_SLINE_SSERV_IO_MS_DAYS} >= 0 INTEGER;
   
      /* Calculate total number of patients for day d */
      impvar TotalPatients{<f,sl,ss,iof,msf,d> in FAC_SLINE_SSERV_IO_MS_DAYS} =
         sum{d1 in DAYS: (max((d - losMean[f,sl,ss,iof,msf] + 1), minDay)) <= d1 <= d} NewPatients[f,sl,ss,iof,msf,d1];

      
      /* New patients cannot exceed demand if the sub service is open */
      /* TODO: Some demand forecasts are negative. I am treating them as zero in the max demand constraint, but should we 
         handle this in the forecasting step instead? If we're just going to set them to 0, we can leave it in optmodel, but 
         if we need to do something more sophisticated, then it should probably go in the forecasting macro. */
      con Maximum_Demand{<f,sl,ss,iof,msf,d> in FAC_SLINE_SSERV_IO_MS_DAYS}:
         NewPatients[f,sl,ss,iof,msf,d] <= max(demand[f,sl,ss,iof,msf,d],0)*OpenFlg[f,sl,ss,d];
   
      /* If a sub-service is open, we must satisfy a minimum proportion of the demand */
      con Minimum_Demand{<f,sl,ss> in FAC_SLINE_SSERV, d in DAYS}:
         sum {<(f),(sl),(ss),iof,msf,(d)> in FAC_SLINE_SSERV_IO_MS_DAYS} NewPatients[f,sl,ss,iof,msf,d]
            >= minDemandRatio
               * OpenFlg[f,sl,ss,d]
               * sum {<(f),(sl),(ss),iof,msf,(d)> in FAC_SLINE_SSERV_IO_MS_DAYS} maxCapacityWithoutCovid[f,sl,ss,iof,msf,d];
               
      /* If a sub-service opens, it must stay open for the remainder of the horizon */
      con Service_Stay_Open{<f,sl,ss> in FAC_SLINE_SSERV, d in DAYS: d + 1 in DAYS}:
         OpenFlg[f,sl,ss,d+1] >= OpenFlg[f,sl,ss,d];

	/* Temporary constraint: only allow full horizon opening - not staged */
	 con Open_Only_First_Day {<f,sl,ss> in FAC_SLINE_SSERV, d in DAYS: d=minDay}:
		 OpenFlg[f,sl,ss,d+1]= OpenFlg[f,sl,ss,d];
               
      /* Total patients cannot exceed capacity */
      con Resources_Capacity{<f,sl,ss,r> in FAC_SLINE_SSERV_RES, d in DAYS}:
         sum {<f2,sl2,ss2,iof,msf,(r)> in FAC_SLINE_SSERV_IO_MS_RES : 
               (f2=f or f='ALL') and (sl2=sl or sl='ALL') and (ss2=ss or ss='ALL')} 
            utilization[f2,sl2,ss2,iof,msf,r]*TotalPatients[f2,sl2,ss2,iof,msf,d] <= capacity[f,sl,ss,r];
            
      /* Tests constraint - Total inpatients admitted should be less than the daily rapid test available  */
      con COVID19_Day_Of_Admission_Testing{d in DAYS}:
         sum {<f,sl,ss,iof,msf,(d)> in FAC_SLINE_SSERV_IO_MS_DAYS : iof='I'} 
              NewPatients[f,sl,ss,iof,msf,d] <= totalDailyRapidTests ;

      /* Non-Rapid tests constraint - total available non-rapid test */
      con COVID19_Before_Admission_Testing{d in DAYS: d+daysTestBeforeAdmSurg in DAYS}:
         sum {<f,sl,ss,iof,msf,d1> in FAC_SLINE_SSERV_IO_MS_DAYS : msf='SURG' and d1 in DAYS} 
              NewPatients[f,sl,ss,iof,msf,d1]  <=  totalDailyNonRapidTests;

      max Total_Revenue = 
         sum{<f,sl,ss,iof,msf,d> in FAC_SLINE_SSERV_IO_MS_DAYS} NewPatients[f,sl,ss,iof,msf,d]*revenue[f,sl,ss,iof,msf];
   
      max Total_Margin = 
         sum{<f,sl,ss,iof,msf,d> in FAC_SLINE_SSERV_IO_MS_DAYS} NewPatients[f,sl,ss,iof,msf,d]*margin[f,sl,ss,iof,msf];

      /******************Solve*******************************/

	/* If not limited to demand or covid19 tests, how would capacity be managed */
/* 	 drop   */
/*            Minimum_Demand */
/* 		   Maximum_Demand */
/* 		   ; */

	/* Do not open any hierarchy that is not restricted by utilization */
/* 	for {<f,sl,ss,iof,msf> in HIER_IN_UTIL, d in DAYS} fix OpenFlg[f,sl,ss,d] = 0; */

/* 	solve obj Total_Revenue with milp; */


      /* First we want to find out what is the maximum demand we can handle without the covid-19 tests. 
         We drop the COVID constraint and the minimum demand constraint. We're also going to fix OpenFlg to 1 
         for every sub-service (i.e., the only reason we might not open a sub-service is because we don't have enough 
         COVID-19 tests), so we can also drop the Service_Stay_Open constraints. */
      drop COVID19_Day_Of_Admission_Testing
           COVID19_Before_Admission_Testing
           Minimum_Demand
           Service_Stay_Open
		   ;
           
      for {<f,sl,ss> in FAC_SLINE_SSERV, d in DAYS} fix OpenFlg[f,sl,ss,d] = 1;

      solve obj Total_Revenue with milp;
      
      /* The maximum demand without covid-19 tests is equal to the number of new patients that we saw, 
         subject to other resource capacity constraints */
      for {<f,sl,ss,iof,msf,d> in FAC_SLINE_SSERV_IO_MS_DAYS}
         maxCapacityWithoutCovid[f,sl,ss,iof,msf,d] = NewPatients[f,sl,ss,iof,msf,d].sol;
      
      /* Now restore the COVID constraints, the minimum demand constraints, and the Service_Stay_Open constraints, 
         and unfix OpenFlg, and then solve again. */
      restore COVID19_Day_Of_Admission_Testing
              COVID19_Before_Admission_Testing
              Minimum_Demand
              Service_Stay_Open;
           
      unfix OpenFlg;

      solve obj Total_Revenue with milp / primalin;



      /******************Create output data*******************************/

      num OptRevenue{<f,sl,ss,iof,msf,d> in FAC_SLINE_SSERV_IO_MS_DAYS} = 
        NewPatients[f,sl,ss,iof,msf,d].sol*revenue[f,sl,ss,iof,msf];

      num OptMargin{<f,sl,ss,iof,msf,d> in FAC_SLINE_SSERV_IO_MS_DAYS} = 
        NewPatients[f,sl,ss,iof,msf,d].sol*margin[f,sl,ss,iof,msf];

      create data &_worklib.._opt_detail
         from [facility service_line sub_service ip_op_indicator med_surg_indicator day]
			={<f,sl,ss,iof,msf,d> in FAC_SLINE_SSERV_IO_MS_DAYS}
         NewPatients=(round(NewPatients[f,sl,ss,iof,msf,d],0.01))
         TotalPatients=(round(NewPatients[f,sl,ss,iof,msf,d],0.01))
         OptRevenue=(round(OptRevenue[f,sl,ss,iof,msf,d],0.01))
         OptMargin=(round(OptMargin[f,sl,ss,iof,msf,d],0.01))
		 demand
         maxCapacityWithoutCovid;

      create data &_worklib.._opt_summary
         from [facility service_line sub_service day]={<f,sl,ss> in FAC_SLINE_SSERV, d in DAYS}
         OpenFlg=(round(OpenFlg[f,sl,ss,d],0.01));
   quit;

   data &_worklib.._opt_detail_week;
      format date date9.;
      format week_start_date date9.;
      set &_worklib.._opt_detail (rename =(day=date));
      week_num=week(date);
      year_num=year(date);
      week_start_date=input(put(year_num, 4.)||"W"||put(week_num,z2.)||"01", weekv9.);
   run;

   proc cas;
      aggregation.aggregate / table={caslib="&_worklib.", name="_opt_detail_week",  
         groupby={"facility","service_line","sub_service","week_start_date"}} 
         saveGroupByFormat=false 
         varSpecs={{name="NewPatients", summarySubset="sum", columnNames="NewPatients"}
                   {name="OptMargin", summarySubset="sum", columnNames="OptMargin"}
                   {name="OptRevenue", summarySubset="sum", columnNames="OptRevenue"}
					{name="Demand", summarySubset="sum", columnNames="Demand"}
					{name="maxCapacityWithoutCovid", summarySubset="sum", columnNames="maxCapacityWithoutCovid"}} 
         casOut={caslib="&_worklib.",name="_opt_detail_agg",replace=true}; run;  
   quit;

   data &outlib..&output_opt_detail (promote=yes);
      format day date9.;
      set &_worklib.._opt_detail;
   run;

   data &outlib..output_opt_detail_agg (promote=yes);
      set &_worklib.._opt_detail_agg;
	    DailyNewPatients=NewPatients/7;
		DailyOptMargin=OptMargin/7;
		DailyOptRevenue=OptRevenue/7;
		DailyDemand=Demand/7;
		DailyMaxCapacityWithoutCovid=maxCapacityWithoutCovid/7;
	run;
    
   data &outlib..&output_opt_summary (promote=yes);
      set &_worklib.._opt_summary;
   run;
   
   /*************************/
   /******HOUSEKEEPING*******/
   /*************************/

   %if &_debug.=0  %then %do;
   proc delete data= &_work_tables.;
   run;
   %end;

   %EXIT:
   %put TRACE: Leaving &sysmacroname. with SYSCC=&SYSCC.;

%mend cc_med_res_opt;