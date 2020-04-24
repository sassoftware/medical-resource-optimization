/* Prep Data */

data _tmp_input_demand;
	set cc.input_demand (rename = (date=datetime));
	date=datepart(datetime);
run;

proc sort data=_tmp_input_demand;
	by facility service_line sub_service ip_op_indicator med_surg_indicator;
quit;

proc timeseries data=_tmp_input_demand out=_tmp_out_ts;
                id date
                interval=day
                accumulate=total
                setmiss=0;
    var demand;
	by facility service_line sub_service ip_op_indicator med_surg_indicator;
run;

/* Promote for visualization */
proc delete data=cc.input_demand_ts;
run;
data cc.input_demand_ts (promote=yes);
	set _tmp_out_ts;
	dow=weekday(date);
run;

/* Get DOW profile */
proc cas;
	  aggregation.aggregate / table={caslib="cc", name="input_demand_ts", 
	                                 groupby={"facility","service_line","sub_service","date", "dow"}}
	                          saveGroupByFormat=false
	                          varSpecs={{name="demand", summarySubset="sum",columnNames="demand"}}
	                          casOut={caslib="cc",name="input_demand_agg",replace=true}; run;
quit;

proc delete data=cc.input_demand_dow;
run;

proc cas;
	  aggregation.aggregate / table={caslib="cc", name="input_demand_agg", 
	                                 groupby={"facility","service_line","sub_service", "dow"}}
	                          saveGroupByFormat=false
	                          varSpecs={{name="demand", summarySubset="mean", columnNames="meanDemand"}}
	                          casOut={caslib="cc",name="input_demand_dow",replace=true}; run;
quit;

data cc.input_demand_dow(promote=yes);
	set cc.input_demand_dow;
run;

/* Forecast */

proc cas;
   timeData.timeSeries /
      table={name="priceData", groupby={"region"}}
      series={{name="sale" acc="sum"},
              {name="price" acc="avg"},
              {name="discount" acc="avg"},
              {name="cost" acc="avg"}}
      timeId="date"
      tStart="Jan 1, 1998"
      tEnd="Dec 1, 2002"
      interval="qtr"
      sumOut="priceSum"
      casOut="priceOut";
   run;
   table.fetch /
      table = {name="priceSum"};
   run;
quit;

proc cas;
   timeData.forecast /
      table={name='pricedata', groupBy={{name='region'}, {name='product'}}},
      timeId={name='date'},
      interval='month',
      tStart='Jan 1, 1998',
      tEnd='Dec 1, 2002',
      dependents={{name='sale', accumulate='SUM'}},
      predictors={{name='price', accumulate='AVG'},
                  {name='discount', accumulate='AVG'}},
      lead=6,
      forOut={name='salefor'},
      infoOut={name='saleinfo'},
      indepOut={name='saleindep'},
      selectOut={name='saleselect'},
      specOut={name='salespec'};
   run;
   table.fetch /
      table = {name='saleselect'};
   table.fetch /
      table = {name='salefor'};
   run;
quit;

/* proc gplot data=_tmp_out_ts; */
/*    plot cases*date; */
/* run; */
/* quit; */

proc hpfdiagnose 
      data=_tmp_out_ts
      outest=parms
      modelrepository=model_rep
      holdout=30
      criterion=mape
      print=all;
   id date interval=day;
   forecast demand;  
	by facility service_line sub_service ip_op_indicator med_surg_indicator;
/*    input 'Donor ID'n; */
run;

proc hpfengine 
            data=_tmp_out_ts
            modelrepository=model_rep
            inest=parms
            outfor=fits
            outstatselect=model_stat
            print=forecasts
/*             back=20 */
            lead=10
            ;
   id date interval=day;
   forecast deaths;  
/*    input Num_Projects; */
run;