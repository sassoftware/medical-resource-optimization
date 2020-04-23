*------------------------------------------------------------------------------*
| Program: cc_med_res_opt
|
| Description: 
|
*--------------------------------------------------------------------------------* ;
%macro cc_med_res_opt(
	inlib=cc
	,outlib=cc
	,input_demand=input_demand
	,input_capacity =input_capacity
	,input_financials=input_financials
	,input_service_attr=input_service_attr
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
   %if %sysfunc(exist(&inlib..&input_demand.))=0 %then %do;
      %put FATAL: Missing &inlib..&input_demand., exiting from &sysmacroname.;
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

   %if %sysfunc(exist(&inlib..&input_service_attr.))=0 %then %do;
      %put FATAL: Missing &inlib..&input_service_attr., from &sysmacroname.;
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

   /* List output tables 
   %let output_tables=%str(         
         &_worklib..output_dp_od_adj
		 &_worklib..output_dp_od_adj_und
         &_worklib..output_dp_loc_cases
         );
*/

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

 	set <str,str,str,str> FAC_SLINE_SSERV_RESOURCES; /* FAC_SLINE_SSERV_RESOURCES is a index set f,sl,ss,r */
	set <str,str,str> FAC_SLINE_SSERV = setof {<f,sl,ss,r> in FAC_SLINE_SSERV_RESOURCES} <f,sl,ss>;
	set <str> RESOURCES = setof {<f,sl,ss,r> in FAC_SLINE_SSERV_RESOURCES} r;

	set <str> FACILITIES = setof {<f,sl,ss,r> in FAC_SLINE_SSERV_RESOURCES} f;
	set <str> SERVICELINES = setof {<f,sl,ss,r> in FAC_SLINE_SSERV_RESOURCES} sl;
	set <str> SUBSERVICES = setof {<f,sl,ss,r> in FAC_SLINE_SSERV_RESOURCES} ss;
	set <num> DAYS;
	set <str,str,str,num> FAC_SLINE_SSERV_DAYS;

	num capacity{FAC_SLINE_SSERV_RESOURCES};
	num revenue{FAC_SLINE_SSERV};
	num margin{FAC_SLINE_SSERV};
	num losMean{FAC_SLINE_SSERV};
	num demand{FAC_SLINE_SSERV, DAYS};
	num minDay=min {d in DAYS} d;

/* 	num losVar{FAC_SLINE_SSERV}; */
/* 	num visitorsMean{FAC_SLINE_SSERV}; */
/* 	num visitorsVar{FAC_SLINE_SSERV}; */
/* 	num minPctReschedule{FAC_SLINE_SSERV}; */
/* 	num maxPctReschedule{FAC_SLINE_SSERV}; */

	/* Decide to open or not a sub service */
	var OpenFlg{FAC_SLINE_SSERV} BINARY;

	/* Related to how many new patients are actually accepted */
	var NewPatients{FAC_SLINE_SSERV, DAYS};

 /* read data from SAS data sets */ 
/* Demand */
read data &inlib..input_demand into FAC_SLINE_SSERV_DAYS= [facility service_line sub_service day]
   demand=demand;

/* Capacity */
read data &inlib..input_capacity into FAC_SLINE_SSERV= [facility service_line sub_service]
   capacity=capacity;

/* Financials */
read data &inlib..input_financials into FAC_SLINE_SSERV= [facility service_line sub_service]
   revenue=revenue margin=margin;

/* Service attributes */
read data &inlib..input_service_attr into FAC_SLINE_SSERV= [facility service_line sub_service]
   numcancel=numcancel losmean=losmean;

/******************Model variables, constraints, objective function*******************************/

/* Calculate total number of patients for day d */
	impvar TotalPatients{<f,sl,ss> in FAC_SLINE_SSERV, d in DAYS} =
		sum{d1 in DAYS: (max((d - losMean[f,sl,ss] + 1), minDay)) <= d1 <= d} NewPatients[f,sl,ss,d1];

	/* New patients cannot exceed demand if the sub service is open */
	con Maximum_Demand{<f,sl,ss> in FAC_SLINE_SSERV, d in DAYS}:
		NewPatients[f,sl,ss,d] <= demand[f,sl,ss,d]*OpenFlg[f,sl,ss];

	/* Total patients cannot exceed capacity */
	con Resources_Capacity{<f,sl,ss,r> in FAC_SLINE_SSERV_RESOURCES, d in DAYS}:
		TotalPatients[f,sl,ss,d] <= capacity[f,sl,ss,r];

	max Total_Revenue = sum{<f,sl,ss,r> in FAC_SLINE_SSERV, d in DAYS} NewPatients[f,sl,ss,d]*revenue[f,sl,ss];

	max Total_Margin = sum{<f,sl,ss,r> in FAC_SLINE_SSERV, d in DAYS} NewPatients[f,sl,ss,d]*margin[f,sl,ss];

quit;

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
