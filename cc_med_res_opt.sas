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
   %if %sysfunc(exist(&_worklib..output_fd_demand_fcst ))=0 %then %do;
      %put FATAL: Missing &_worklib..output_fd_demand_fcst, exiting from &sysmacroname.;
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

/*    List output tables  */
   %let output_tables=%str(         
       &outlib..output_opt_detail
       &outlib..output_opt_summary
         );


   /* Delete output data if already exists 
   proc delete data= &output_tables.;
   run;
*/

   /* Delete work data if already exists
   proc delete data= &_work_tables.;
   run;
 */

   /************************************/
   /************ANALYTICS *************/
   /***********************************/

   proc optmodel;
   
      /* Master sets read from data */
      set <str,str,str,str,str,str> FAC_SLINE_SSERV_IO_MS_RES; 
      set <str,str,str,str,str,num> FAC_SLINE_SSERV_IO_MS_DAYS;
      set <str,str,str,str> FAC_SLINE_SSERV_RES;
         
      /* Derived Sets */
      set <str,str,str> FAC_SLINE_SSERV = setof {<f,sl,ss,iof,msf,d> in FAC_SLINE_SSERV_IO_MS_DAYS} <f,sl,ss>;
      set <str,str,str,str,str> FAC_SLINE_SSERV_IO_MS = setof {<f,sl,ss,iof,msf,d> in FAC_SLINE_SSERV_IO_MS_DAYS} <f,sl,ss,iof,msf>;
      set <num> DAYS = setof {<f,sl,ss,iof,msf,d> in FAC_SLINE_SSERV_IO_MS_DAYS} <d>;
/*       set <str> RESOURCES = setof {<f,sl,ss,r> in FAC_SLINE_SSERV_RES} r; */
/*       set <str> FACILITIES = setof {<f,sl,ss,r> in FAC_SLINE_SSERV_RES} f; */
/*       set <str> SERVICELINES = setof {<f,sl,ss,r> in FAC_SLINE_SSERV_RES} sl; */
/*       set <str> SUBSERVICES = setof {<f,sl,ss,r> in FAC_SLINE_SSERV_RES} ss; */

      num capacity{FAC_SLINE_SSERV_RES};

      num utilization{FAC_SLINE_SSERV_IO_MS_RES};

      num revenue{FAC_SLINE_SSERV_IO_MS};
      num margin{FAC_SLINE_SSERV_IO_MS};
      num losMean{FAC_SLINE_SSERV_IO_MS};
      num numCancel{FAC_SLINE_SSERV_IO_MS};

      num demand{FAC_SLINE_SSERV_IO_MS_DAYS};
      
      num minDay=min {d in DAYS} d;
   
   /*    num losVar{FAC_SLINE_SSERV}; */
   /*    num visitorsMean{FAC_SLINE_SSERV}; */
   /*    num visitorsVar{FAC_SLINE_SSERV}; */
   /*    num minPctReschedule{FAC_SLINE_SSERV}; */
   /*    num maxPctReschedule{FAC_SLINE_SSERV}; */
   
      /* Decide to open or not a sub service */
      var OpenFlg{FAC_SLINE_SSERV} BINARY;
   
      /* Related to how many new patients are actually accepted */
      var NewPatients{FAC_SLINE_SSERV_IO_MS_DAYS};
   
      /* Read data from SAS data sets */ 
   
      /* Demand Forecast*/
      read data &_worklib..output_fd_demand_fcst 
         into FAC_SLINE_SSERV_IO_MS_DAYS = [facility service_line sub_service ip_op_indicator med_surg_indicator predict_date]
            demand=daily_predict;

      /* Capacity */
      read data &_worklib..input_capacity_pp
         into FAC_SLINE_SSERV_RES = [facility service_line sub_service resource]
            capacity;

      /* Utilization */
      read data &_worklib..input_utilization_pp
         into FAC_SLINE_SSERV_IO_MS_RES = [facility service_line sub_service ip_op_indicator med_surg_indicator resource]
            utilization=utilization_mean;

      /* Financials */
      read data &_worklib..input_financials_pp
         into [facility service_line sub_service ip_op_indicator med_surg_indicator]
            revenue 
            margin;
      
      /* Service attributes */
      read data &_worklib..input_service_attributes_pp
         into [facility service_line sub_service ip_op_indicator med_surg_indicator]
            numCancel=num_cancelled
            losMean=length_stay_mean;
   
      /******************Model variables, constraints, objective function*******************************/
   
      /* Calculate total number of patients for day d */
      impvar TotalPatients{<f,sl,ss,iof,msf,d> in FAC_SLINE_SSERV_IO_MS_DAYS} =
         sum{d1 in DAYS: (max((d - losMean[f,sl,ss,iof,msf] + 1), minDay)) <= d1 <= d} NewPatients[f,sl,ss,iof,msf,d1];

      /* New patients cannot exceed demand if the sub service is open */
      con Maximum_Demand{<f,sl,ss,iof,msf,d> in FAC_SLINE_SSERV_IO_MS_DAYS}:
         NewPatients[f,sl,ss,iof,msf,d] <= max(demand[f,sl,ss,iof,msf,d],0)*OpenFlg[f,sl,ss];
   
      /* Total patients cannot exceed capacity */
	  con Resources_Capacity{<f,sl,ss,r> in FAC_SLINE_SSERV_RES, d in DAYS}:
			/* if the capacity is shared across all sub-service for a facility and service-line*/
			if (ss='ALL') then 
				sum {<(f),(sl),(ss),iof,msf,(r)> in FAC_SLINE_SSERV_IO_MS_RES} 
					utilization[f,sl,ss,iof,msf,r]*TotalPatients[f,sl,ss,iof,msf,d]
			
			/* if the capacity is shared across all sub-service and all service line for a facility*/
			else if (ss='ALL' and sl ='ALL') then
				sum {<(f),(sl),(ss),iof,msf,(r)> in FAC_SLINE_SSERV_IO_MS_RES} 
					utilization[f,sl,ss,iof,msf,r]*TotalPatients[f,sl,ss,iof,msf,d] 

			/* if the capacity is shared across all sub-service service-lines and facilities*/
			else if (ss='ALL' and sl ='ALL' and f = 'ALL') then
			sum {<(f),(sl),(ss),iof,msf,(r)> in FAC_SLINE_SSERV_IO_MS_RES} 
				utilization[f,sl,ss,iof,msf,r]*TotalPatients[f,sl,ss,iof,msf,d] 
			
			/* if the capacity is defined at a facility, service-line and sub-service level*/
			else 
			sum {<(f),(sl),(ss),iof,msf,(r)> in FAC_SLINE_SSERV_IO_MS_RES} 
				utilization[f,sl,ss,iof,msf,r]*TotalPatients[f,sl,ss,iof,msf,d] 

			<= capacity[f,sl,ss,r];

   
      max Total_Revenue = 
         sum{<f,sl,ss,iof,msf,d> in FAC_SLINE_SSERV_IO_MS_DAYS} NewPatients[f,sl,ss,iof,msf,d]*revenue[f,sl,ss,iof,msf];
   
      max Total_Margin = 
         sum{<f,sl,ss,iof,msf,d> in FAC_SLINE_SSERV_IO_MS_DAYS} NewPatients[f,sl,ss,iof,msf,d]*margin[f,sl,ss,iof,msf];

      /******************Solve*******************************/

		expand;

      solve obj Total_Revenue with milp;

      /******************Create output data*******************************/

      create data &_worklib.._opt_detail
         from [facility service_line sub_service ip_op_indicator med_surg_indicator day]=FAC_SLINE_SSERV_IO_MS_DAYS 
         NewPatients
         TotalPatients;

      create data &_worklib.._opt_summary
         from [facility service_line sub_service]=FAC_SLINE_SSERV
         OpenFlg;

   
   quit;

	data &outlib..output_opt_detail;
		set &_worklib.._opt_detail;
	run;
	
	data &outlib..output_opt_summary;
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
