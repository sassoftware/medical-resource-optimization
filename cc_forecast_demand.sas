/* Prep Data */

data casuser._tmp_input_demand;
	set cc.input_demand (
		rename = 
			(date=datetime)
		where = 
			(facility='Akron'
			and service_line='Cardiac Services'
			and sub_service='Cardiac Cath'
			and med_surg_indicator='SURG'
			and ip_op_indicator='I'));
	date=datepart(datetime);
run;

/* proc cas; */
/*    timeData.timeSeries / */
/*       table={ */
/* 		caslib="casuser",  */
/* 		name="input_demand",  */
/* 		groupby={"facility" "service_line" "sub_service" "med_surg_indicator" "ip_op_indicator"}}  */
/*       series={{ */
/* 		name="demand"  */
/* 		acc="sum"  */
/* 		setmiss=0}} */
/*       timeId="date" */
/*       tStart="Jan 1, 1998" */
/*       tEnd="Dec 1, 2002" */
/*       interval="day" */
/*       sumOut="_tmp_ts_stats" */
/*       casOut="_tmp_input_demand_ts"; */
/*    run; */
/* quit; */

/* Forecast */

proc cas;
   timeData.forecast /
      table={
		caslib="casuser", 
		name="_tmp_input_demand", 
		groupby={"facility" "service_line" "sub_service" "med_surg_indicator" "ip_op_indicator"}} 
      timeId={name='date'},
      interval='day',
/*       tStart='Jan 1, 1998', */
/*       tEnd='Dec 1, 2002', */
      dependents={{name='demand', accumulate='SUM'}},
/*       predictors={{name='price', accumulate='AVG'}, */
/*                   {name='discount', accumulate='AVG'}}, */
      lead=10,
      forOut={name='output_fd_demand_fcst'},
/*       infoOut={name='infoOut'}, */
/*       indepOut={name='indepOut={'}, */
/*       selectOut={name='selectOut'}, */
/*       specOut={name='specOut'} */
	  ;
   run;
quit;

/* Promote for visualization */
/* proc delete data=cc.input_demand_ts; */
/* run; */
/* data cc.input_demand_ts (promote=yes); */
/* 	set _tmp_out_ts; */
/* 	dow=weekday(date); */
/* run; */

/* Get DOW profile */
/* proc cas; */
/* 	  aggregation.aggregate / table={caslib="cc", name="input_demand_ts",  */
/* 	                                 groupby={"facility","service_line","sub_service","date", "dow"}} */
/* 	                          saveGroupByFormat=false */
/* 	                          varSpecs={{name="demand", summarySubset="sum",columnNames="demand"}} */
/* 	                          casOut={caslib="cc",name="input_demand_agg",replace=true}; run; */
/* quit; */
/*  */
/* proc delete data=cc.input_demand_dow; */
/* run; */
/*  */
/* proc cas; */
/* 	  aggregation.aggregate / table={caslib="cc", name="input_demand_agg",  */
/* 	                                 groupby={"facility","service_line","sub_service", "dow"}} */
/* 	                          saveGroupByFormat=false */
/* 	                          varSpecs={{name="demand", summarySubset="mean", columnNames="meanDemand"}} */
/* 	                          casOut={caslib="cc",name="input_demand_dow",replace=true}; run; */
/* quit; */
/*  */
/* data cc.input_demand_dow(promote=yes); */
/* 	set cc.input_demand_dow; */
/* run; */

