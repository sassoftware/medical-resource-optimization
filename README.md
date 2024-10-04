# Medical Resource Optimization Program
An open collaboration between the Cleveland Clinic and SAS Institute and a continuation from our prior work on [COVID-19 Epidemiological Scenario Analysis] (https://github.com/sassoftware/covid-19-sas). That project demonstrates the usage of infectious disease models (SIR & SEIR) to analyze different scenarios and predict utilization of critical resources.

## Disclosure
This analytical engine is in active development and testing. 

## What this program does
This project aims to generate an optimum hospital reactivation plan in response to clinical service disruptions during the COVID-19 pandemic.  The plan is capable of running multiple scenarios with different parameter settings that seek to balance hospital economics, efficient use of resources, and patient access to quality clinical care.

The program accommodates multiple medical facilities; services lines and sub-services (e.g. Orthopedics/Joint Replacement); inpatient and outpatient status; and medical/surgical patient types. The sample data set facilitates running the program and can be replaced with the user's actual data.

## How to use the program
The initial conditions assume sub-services are currently closed due to the COVID-19 pandemic and admitting all the patient backlog at once is not feasible due to shared resource constraints (e.g. COVID-19 tests, ventilators, shared beds, operating rooms, etc).

Using existing capacity, historical utilization, and forecasted demand, a reopening plan is developed to recommended an optimal order and timing for opening sub-services. 

Other objectives like maximizing total patient volumes, impacts of secondary COVID-19 surge scenarios, and configuration of clinical centers of excellence are additional use cases for future extensions of the program.  

## Multi-Scenario capability
The optimization model is capable of running multiple scenarios with different parameter settings. One example of such scenario is change in COVID-19 test kit numbers. Users can define two scenarios with current and modified set of COVID19 test kit numbers.

Note that not all parameters can be changed across scenarios. Please refer to Table 1. The parameters tagged as *(Global)* cannot be changed across scenarios. Other parameters which are tagged as *(Scenario)* can be changed across scenarios. Note that the model will stop if the data contains different values for *(Global)* parameters across scenarios.

## Software requirements
The project requires *SAS Viya*, *SAS Optimization*, and *SAS Visual Analytics* installations.


## Steps to execute the code

1. Create a global CASlib called COVID
2. Place input data (defined below) to the COVID CASlib. Use the *cc_import_data* code in the *mro_support_code* folder to import the input data files into COVID CASlib. Refer to the **Support files** section for additional instructions. Note that the *mro_sample_data* folder has sample input data files for testing.
3. Checkout master branch of this code into a location accessible from SAS Studio.
4. Open the *cc_standalone.sas* file from the *mro_code* folder
   * modify the *my_code_path* variable to path of the folder where you checked-out the code
   * modify the input macro variables in the %cc_execute macro call as following:  
        *MANDATORY:*
        - inlib = library of input tables. Default = *cc*.
        - outlib = library of output tables. Default = *casuser*.  
        *OPTIONAL:*
        - _worklib = library of working tables. Working tables will be automatically deleted at the end of the program execution. Default = *casuser*.
        - opt_param_lib = library of INPUT_OPT_PARAMETERS table will be available. Default = *cc*.
        - input_utilization = table of input_utilization data (in inlib). Default = *input_utilization*.
        - input_capacity = table of input_capacity data (in inlib). Default = *input_capacity*.
        - input_financials = table of input_financials data (in inlib). Default = *input_financials*.
        - input_service_attributes = table of input_service_attributes data (in inlib). Default = *input_service_attributes*.
        - input_demand = table of input_demand data (in inlib). Default = *input_demand*.
        - input_demand_forecast = table of input_demand_forecast data (in inlib). Default = *input_demand_forecast*.
        - input_opt_parameters = table of input_opt_parameters data (in opt_param_lib). Default = *input_opt_parameters*.
        - output_opt_detail_daily = table of output_opt_detail_daily data (in outlib). Default = *output_opt_detail_daily*.
        - output_opt_detail_weekly = table of output_opt_detail_weekly data (in outlib). Default = *output_opt_detail_weekly*.
        - output_opt_summary = table of output_opt_summary data (in outlib). Default = *output_opt_summary*.
        - output_opt_resource_usage = table of output_opt_resource_usage data (in outlib). Default = *output_opt_resource_usage*.
        - output_opt_resource_usage_detail = table of output_opt_resource_usage_detail data (in outlib). Default = *output_opt_resource_usage_detail*.
        - output_opt_covid_test_usage = table of output_opt_covid_test_usage data (in outlib). Default = *output_opt_covid_test_usage*.
        - run_dp = 1 if *cc_data_prep.sas* macro is to be executed, 0 otherwise. Default = *1*.
        - run_fcst = 1 if *cc_forecast_demand.sas* macro code is to be executed, 0 otherwise. Default = *1*.
        - run_opt = 1 if *cc_optimize.sas* macro code is to be executed, 0 otherwise. Default = *1*.
        - debug = is set to 1 if you want to retain the temporary working tables for debugging. Default = *1*.
        - exclude_str = parameter to filter all the input data tables to exclude only. Example: exclude_str = %str(service_line = 'ABC'). Default = *' '*.
        - include_str = parameter to filter all the input data tables to include only. Example: include_str = %str(facility in ('fac1','fac','ALL')). Default = *' '*.
        - debug = 1 to retain the temporary working tables for debugging. Default = *1*.   
5. Run the *cc_standalone.sas* code. The *cc_standalone.sas* file calls and runs the *cc_execute.sas* macro. The *cc_execute.sas* macro runs *cc_data_prep.sas*, *cc_forecast_demand*, and *cc_optimize* macros in sequence.  
6. Output data can be accessed from outlib CASlib.

**Note: Default values are assigned to the macro variables if they are not specified in *cc_standalone.sas***.

## Input data
The model has 7 input tables and they are defined within the hierarchy definition:
1. input_capacity: resource capacity available for at each facility, services, and sub-services hierarchy. 'ALL' is used to denote resources shared across multiple sub-services/service lines/facilities.
2. input_utilization: utilization (or usage) of each resource per patient per day, by inpatient/outpatient and medical/surgical indicators, at each facility, services, and sub-services hierarchy.
3. input_service_attributes: service attributes like average length of stay (in days) of a patient, by inpatient/outpatient and medical/surgical indicators, at each facility, services, and sub-services hierarchy.
4. input_financials: revenue and margin data, by inpatient/outpatient and medical/surgical indicators, at each facility, services, and sub-services hierarchy.
5. input_demand: historical demand data, by inpatient/outpatient and medical/surgical indicators, at each facility, services, and sub-services hierarchy. This data is used to forecast demand to be used by the optimization model. The input_demand table is not required if the input_demand_forecast table is used.
6. input_demand_forecast: forecasted demand data, by inpatient/outpatient and medical/surgical indicators, at each facility, services, and sub-services hierarchy. The input_demand_forecast table is not required if the input_demand table is used.
7. input_opt_parameters: user-defined parameters to control when submitting optimization jobs. For example, one of the parameters is PLANNING_HORIZON which defines the planning horizon (in weeks) for the forecasting and optimization model. A complete list of parameters is shown below in Table 1:

**Table 1: Description of the optimization parameters**

|    Parameter Name (Level)                                                        |    Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |    Default Value    |    Acceptable Values                                       |
|----------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------|------------------------------------------------------------|
|    PLANNING_HORIZON    (Global)                                                  |    Defines the planning horizon (in weeks) for the forecasting and optimization model.                                                                                                                                                                                                                                                                                                                                                                                          |    12               |    >1                                                      |
|    OPTIMIZATION_START_DATE    (Global)                                           |    Defines the start date for the optimization model. Three different options are available:  phase 1 start date (PHASE_1_DATE), tomorrow (TODAY_PLUS_1), or day after last date in the demand data file (HISTORY_PLUS_1).                                                                                                                                                                                                                                                      |    PHASE_1_DATE     |    PHASE_1_DATE / TODAY_PLUS_1 / HISTORY_PLUS_1            |
|    RUN_INPUT_DEMAND_FCST    (Global)                                             |    Specifies whether to run the forecast model to generate demand forecast or to use the external demand forecast file. If this parameter is set to YES, the demand forecast model is run using the historical demand data. If this is set to NO, the externally provided demand forecast file is used.                                                                                                                                                                         |    YES              |    YES / NO                                                |
|    FORECAST_MODEL    (Global)                                                    |    Defines the model used for forecasting demand. Two different forecasting methods are available: TSMDL uses time series method and YOY uses year-over-year method.                                                                                                                                                                                                                                                                                                            |    TSMDL            |    TSMDL / YOY                                             |
|    FILTER_SERV_NOT_USING_RESOURCES    (Global)                                   |    Flag to filter out sub-services that do not use any resources, and sub-services whose only resources utilizations are not defined in the capacity table.                                                                                                                                                                                                                                                                                                                     |    NO               |    YES / NO                                                |
|    LOS_ROUNDING_THRESHOLD    (Global)                                            |    Length of stay threshold is used to round up or round down the length_stay_mean (los) variable. If the fractional portion of the los variable is less than the parameter value, we round down the los variable value. If the fractional portion of the los variable is greater than the parameter value, we round up the los variable value.                                                                                                                                 |    0.5              |    0 to 1                                                  |
|    DATE_PHASE_*    (Scenario)                                                    |    Start date of the phase *.                                                                                                                                                                                                                                                                                                                                                                                                                                                   |    NA               |    Date String in the format MM/DD/YYYY                    |
|    RAPID_TESTS_PHASE_*    (Scenario)                                             |    Number of daily rapid tests available beginning on the corresponding phase date (i.e., DATE_PHASE_*)                                                                                                                                                                                                                                                                                                                                                                         |    0                |    Integer                                                 |
|    NOT_RAPID_TESTS_PHASE_*    (Scenario)                                         |    Number of daily not-rapid tests available beginning on the corresponding phase date (i.e., DATE_PHASE_*)                                                                                                                                                                                                                                                                                                                                                                     |    0                |    Integer                                                 |
|    TEST_DAYS_BA    (Scenario)                                                    |    Defines the number of days before admittance that each non-emergency surgery patient must be tested for COVID-19 (using a non-rapid test kit).                                                                                                                                                                                                                                                                                                                               |    0                |    > 0 and < number of days in the PLANNING_HORIZON        |
|    RAPID_TEST_DA    (Scenario)                                                   |    Fraction of inpatients (including emergency surgical patients) that should be tested for COVID-19 on the day of admission (using a rapid test kit).                                                                                                                                                                                                                                                                                                                          |    0                |    0 or 1 to 100                                           |
|    HOLD_RAPID_COVID_TESTS    (Scenario)                                          |    Defines the number of rapid tests put aside on hold for each day. The parameter value is subtracted from the daily available rapid tests. If this number is larger than the number of available rapid test kits, we will hold aside all the rapid test kits.                                                                                                                                                                                                                 |    0                |    Integer                                                 |
|    HOLD_NOT_RAPID_COVID_TESTS    (Scenario)                                      |    Defines the number of non-rapid tests put aside on hold for each day.  The parameter value is subtracted from the daily available not-rapid tests. If this number is larger than the number of available not-rapid test kits, we will hold aside all the not-rapid test kits.                                                                                                                                                                                                |    0                |    Integer                                                 |
|    TEST_FREQ_DAYS    (Scenario)                                                  |    Defines the frequency (in days) in which the admitted patients should be tested for COVID-19. Note that this parameter is currently not being used.                                                                                                                                                                                                                                                                                                                          |    0                |    Integer                                                 |
|    TEST_VISITORS    (Scenario)                                                   |    Defines whether the visitors should be tested for COVID-19 or not. If this parameter is set to YES, then the visitors should be tested for COVID-19. Note that this parameter is currently not being used.                                                                                                                                                                                                                                                                   |    NO               |    YES / NO                                                |
|    ALLOW_OPENING_ONLY_ON_PHASE    (Scenario)                                     |    Limits opening of sub-services only on phase start dates. If this parameter value is YES, the sub-services can open only on phase start dates. If this parameter value is NO, the sub-services can open on any day in the planning horizon.                                                                                                                                                                                                                                  |    NO               |    YES / NO                                                |
|    SECONDARY_OBJECTIVE_TOLERANCE    (Scenario)                                   |    Defines the fraction of the Revenue (primary objective value) which must be achieved when solving the model for maximizing Margin (secondary objective). Say R is the objective value of the model when solving for the Revenue. A value of 95 for the SECONDARY_OBJECTIVE_TOLERANCE parameter denotes that the Revenue should be at least 95% * R, when solving the model for maximizing Margin (secondary objective).                                                      |    99               |    1 to 100                                                |
|    TREAT_MIN_DEMAND_AS_AGGREGATE    (Scenario)                                   |    Specifies whether an 'ALL' value in any level of the hierarchy for MIN_DEMAND_RATIO is used to apply the minimum demand constraint across each sub-service individually, or across all subservices in aggregate.                                                                                                                                                                                                                                                             |    NO               |    YES / NO                                                |
|    REMOVE_DEMAND_CONSTRAINTS    (Scenario)                                       |    Specifies whether all demand constraints are removed from the optimization model. If this parameter is set to YES, the optimization model will run without maximum and minimum demand constraints.                                                                                                                                                                                                                                                                           |    NO               |    YES / NO                                                |
|    REMOVE_COVID_CONSTRAINTS    (Scenario)                                        |    Specifies whether the COVID-19 test constraints are removed from the optimization model. If this parameter is set to YES, the optimization model will run without the COVID-19 test constraints.                                                                                                                                                                                                                                                                             |    NO               |    YES / NO                                                |
|    USE_DECOMP    (Scenario)                                                      |    Specifies whether to use the decomposition algorithm to solve the optimization problem.                                                                                                                                                                                                                                                                                                                                                                                      |    NO               |    YES / NO                                                |
|    ICU_MAX_UTILIZATION    (Scenario / Facility)                                  |    Fraction of ICU Beds capacity that is available to the optimization model; the remaining ICU Beds capacity will be reserved for COVID-19 surge events.                                                                                                                                                                                                                                                                                                                       |    100              |    0 or 1 to 100                                           |
|    ALREADY_OPEN    (Scenario / Facility / Service / Sub-service)                 |    Indicates whether a facility/service line/sub-service is already open. If this parameter is set to YES, the optimization model will fix the opening date for this sub-service to be the first date of the optimization, instead of recommending an opening date for this sub-service.                                                                                                                                                                                        |    NO               |    YES / NO                                                |
|    MIN_DEMAND_RATIO    (Scenario / Facility / Service / Sub-service)             |    Minimum proportion of the weekly demand that must be satisfied when a facility/service line/sub-service is open. This parameter can be defined at any combination of the facility/service line/sub-service hierarchy. Depending on the value of TREAT_MIN_DEMAND_AS_AGGREGATE, it applies either to each individual open sub-service within the specified hierarchy, or to the aggregate demand across all open sub-services within the specified hierarchy.                 |    0                |    0 or 1 to 100                                           |
|    EMER_SURGICAL_PTS_RATIO    (Scenario / Facility / Service / Sub-service)      |    Defines the proportion of emergency surgical patients at a facility, service line, and sub-service.                                                                                                                                                                                                                                                                                                                                                                          |    0                |    0 or 1 to 100                                           |
|    OPEN_FULLY    (Scenario / Facility / Service )                                |    Specifies whether the service line must fully open. A value of YES means the service line must fully open, and a value of NO means the service line can partially open. Note that this parameter is currently not being used.                                                                                                                                                                                                                                                |    NO               |    YES / NO                                                |

The *Input data model* can be accessed from mro_documentation\mro_or_data_model.xlsx.

## Output data

**Output files from *cc_data_prep* are as follows:**
- OUTPUT_DP_DUPLICATE_ROWS - has details of the duplicate entries in the tables along with hierarchy information.
- OUTPUT_DP_HIERARCHY_MISMATCH - has details of hierarchy defined in one table and not in others.
- OUTPUT_DP_INVALID_VALUES - has details of the invalid data values in the tables along with hierarchy information.
- OUTPUT_DP_RESOURCE_MISMATCH - has details on the mismatch between input_capacity and input_utilization tables. Resource specific data (availability and usage) should be defined in both of these tables in order to be used in the model.

**Output file from *cc_forecast_demand* are as follows:**
- OUTPUT_FD_DEMAND_FCST - forecasted demand for the planning horizon either from the external forecast file or forecasted data from the provided historical demand data.

**Output file from *cc_optimize* are as follows:**
- OUTPUT_OPT_DETAIL_DAILY - shows the optimization model output such as patients accepted, margin, revenue by scenario, day, and hierarchy.
- OUTPUT_OPT_DETAIL_WEEKLY - shows the weekly aggregated optimization model output such as average daily patients accepted, average daily margin, average daily revenue by scenario, week, and hierarchy.
- OUTPUT_OPT_SUMMARY - shows the reopening plan for the sub-services at each facility and service line.
- OUTPUT_OPT_RESOURCE_USAGE - shows the utilization of the resource at the granularity of the resource capacity definition.
- OUTPUT_OPT_RESOURCE_USAGE_DETAIL - shows the utilization of the resource as the fraction of resource used at a sub-service in facility/service line.
- OUTPUT_OPT_COVID_TEST_USAGE - shows the used vs. available COVID-19 test kits by scenario, day, and hierarchy.

The *Output data model* can be accessed from mro_documentation\mro_or_data_model.xlsx.

## Output visualizations

The output from the optimization model is visualized using VA reports to derive various useful insights. Examples of visualizations are shown below:

| Average daily ICU Beds usage by Service line | Sub-service opening by facility |
:-------------------------:|:-------------------------:
![](/mro_images/MRO_Dashboard_1.JPG) | ![](/mro_images/MRO_Dashboard_2.JPG) 


## Code files
This section will describe the code files in the *mro_main_code* folder.

- **cc_data_prep** : is a macro to pre-processes the data files, to clean invalid entries and duplicate entries. Invalid entries are the data entries which are not consistent under the defined hierarchy. The macro takes all the input files (input_capacity, input_demand, input_demand_forecast, input_financials, input_service_attributes, input_utilization, input_opt_parameters) and creates clean input files, as the output, to be used by subsequent models (forecasting and optimization model).
Note that there are two input demand files - historical demand data and forecasted demand data. One of these files is pre-processed and used as the demand file based on a parameter in the input_opt_parameters table.

- **cc_forecast_demand** : is a macro which,
(1) takes in historical demand values (input_demand) by inpatient/outpatient and medical/surgical categories, at each facility, services, and sub-services hierarchy and creates a forecasted demand for the defined planning horizon. The macro gives two options to generate forecast - time series model (or) year-over-year method. The selection of the forecast method can be done using a parameter in input_opt_parameters.
(2) takes in input_demand_forecast and sets it as forecasted demand.

- **cc_optimize** : is the optimization code. This macro reads the pre-processed input files from the cc_data_prep macro and forecasted demand file from the cc_forecast-demand macro. It then generates the optimization model, solves the model, and creates various output tables like reopening plan, resource usage etc.  

- **cc_execute** : is a macro which executes the *cc_data_prep*, *cc_forecast_demand*, and *cc_optimize* macros in sequence.  

- **cc_standalone** : is the file which calls the *cc_execute* macro and passes relevant parameters to run the *cc_data_prep*, *cc_forecast_demand*, and *cc_optimize* macros.

## Support files
Support files are located in the *mro_support_code* folder.

- **cc_create_parms_simple** : is a code to create the input_opt_parameters data. Note that this code does not include all the parameters defined in Table 1. Users can include additional parameters. Note that the parameters has to be added at the *level* in which they are defined.  

- **cc_import_data** : is a code to read the input data files and import them into the inlib folder. In this implementation, the *proc casutil* section of the code reads .csv input files and places them in the inlib folder. However, *proc casutil* section can be modified to read data from SAS data sets or other file formats.
The macro variable *data_path* defines the path of the folder where the input files are stored and *inlib* defines the name of the library where input files should be imported. Users should set the *data_path* and *inlib* variables before executing this code. *mro_sample_data* folder has sample input data files for testing.

## Documentation files
Documentation files are located in the *mro_documentation* folder.

- **mro_fdd.docx** : is the functional design document which explains in detail about the problem and the solution methodology.

- **mro_or_data_model.xlsx** : describes the data model for both input data tables and output data tables.

## Preferred Reference for Citation
Cleveland Clinic and SAS Optimization Center of Excellence. Developer Documentation [Internet]. 2020. Available from: https://github.com/sassoftware/medical-resource-optimization.

