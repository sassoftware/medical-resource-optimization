proc optmodel;
   set <str,str,str,str,str,str> UTIL_SET_IN;
   set <str,str,str,str,str> FIN_SET;
   set <str,str,str,str,str,num> DEM_SET_IN;
   set UTIL_SET = setof {<f,sl,ss,iof,msf,r> in UTIL_SET_IN} <f,sl,ss,iof,msf>;
   set DEM_SET = setof {<f,sl,ss,iof,msf,d> in DEM_SET_IN} <f,sl,ss,iof,msf>;

   read data cc.input_demand_wa
        into DEM_SET_IN = [facility service_line sub_service ip_op_indicator med_surg_indicator date];

   read data cc.input_utilization_wa
        into UTIL_SET_IN = [facility service_line sub_service ip_op_indicator med_surg_indicator resource];

   read data cc.input_financials_wa
        into FIN_SET = [facility service_line sub_service ip_op_indicator med_surg_indicator];

   set MISMATCHES1 = UTIL_SET symdiff DEM_SET;

   set MISMATCHES2 = UTIL_SET symdiff FIN_SET;

   set MISMATCHES3 = DEM_SET symdiff FIN_SET;

   set MISMATCHES = MISMATCHES1 union MISMATCHES2 union MISMATCHES3;

   create data mismatches
      from [facility service_line sub_service ip_op_indicator med_surg_indicator]={<f,sl,ss,iof,msf> in MISMATCHES}
      in_util=(<f,sl,ss,iof,msf> in UTIL_SET)
      in_dem=(<f,sl,ss,iof,msf> in DEM_SET)
      in_fin=(<f,sl,ss,iof,msf> in FIN_SET);

quit;
