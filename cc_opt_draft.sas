proc optmodel;

 	set FAC_SLINE_SSERV_RESOURCES; /* FAC_SLINE_SSERV_RESOURCES is a index set f,sl,ss,r */
	set FAC_SLINE_SSERV = setof {<f,sl,ss,r> in FAC_SLINE_SSERV_RESOURCES} <f,sl,ss>;
	set RESOURCES = setof {<f,sl,ss,r> in FAC_SLINE_SSERV_RESOURCES} r;

	set FACILITIES = setof {<f,sl,ss,r> in FAC_SLINE_SSERV_RESOURCES} f;
	set SERVICELINES = setof {<f,sl,ss,r> in FAC_SLINE_SSERV_RESOURCES} sl;
	set SUBSERVICES = setof {<f,sl,ss,r> in FAC_SLINE_SSERV_RESOURCES} ss;
	set DAYS;

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
