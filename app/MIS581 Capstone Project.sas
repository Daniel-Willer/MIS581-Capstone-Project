/* import data from the .csv file into the ad_units data set */
DATA work.ad_units;
	infile "/home/u50079662/sasuser.v94/MIS581 Capstone Project/MIS581_Capstone_Project_ad_unit_data.csv" dlm=',' firstobs=2;
	INPUT clndr_dt dl_unt_id grss_bkd_amt inv_lnth_in_sec network_code inv_typ_cd dy_prt_id aird_ind median_aired_rate
		mean_aired_rate units_cleared avails;
RUN;

/* Print the data in the data set ad_units */
PROC PRINT data=work.ad_units(obs=1000);
RUN;

/* Use the Univariate Procedure to understand distribtions */ 
PROC UNIVARIATE DATA=work.ad_units PLOT;
  VAR clndr_dt dl_unt_id grss_bkd_amt inv_lnth_in_sec network_code inv_typ_cd dy_prt_id aird_ind median_aired_rate
		mean_aired_rate units_cleared avails;
RUN; 

/* Use Means Procedure to Calculate Summary Statistics */ 
PROC MEANS DATA=work.ad_units N SUM MEAN STDDEV MIN MAX;
  VAR clndr_dt dl_unt_id grss_bkd_amt inv_lnth_in_sec network_code inv_typ_cd dy_prt_id aird_ind median_aired_rate
		mean_aired_rate units_cleared avails;
RUN;

/* Plot Key Metrics by Customer Cluster */
Title "Booked $ by Aired Status";
PROC SGPLOT DATA=work.ad_units; 
	HBOX grss_bkd_amt / GROUP= aird_ind; 
RUN;
/* Use Means Procedure to Calculate Summary For HBOX */ 
PROC MEANS DATA=work.ad_units MEAN MEDIAN STDDEV MIN MAX;
  CLASS aird_ind;
  VAR grss_bkd_amt;
RUN;

Title "Network:7235 - Booked $ by Aired Status";
PROC SGPLOT DATA=work.ad_units; where network_code= 7235;
	HBOX grss_bkd_amt / GROUP= aird_ind CATEGORY= dy_prt_id;
RUN;
Title "Network:6390 - Booked $ by Aired Status";
PROC SGPLOT DATA=work.ad_units; where network_code= 6390;
	HBOX grss_bkd_amt / GROUP= aird_ind CATEGORY= dy_prt_id;
RUN;
Title "Network:9258 - Booked $ by Aired Status";
PROC SGPLOT DATA=work.ad_units; where network_code= 9258;
	HBOX grss_bkd_amt / GROUP= aird_ind CATEGORY= dy_prt_id;
RUN;

/* Use Means Procedure to Calculate Summary For HBOX */ 
PROC MEANS DATA=work.ad_units MEAN MEDIAN STDDEV MIN MAX;
  CLASS aird_ind dy_prt_id;
  VAR grss_bkd_amt;
RUN;



/* Perform TTest to compare means of booked rates */
PROC TTEST DATA= work.ad_units SIDES=2 h0=0;
	CLASS aird_ind;
	VAR grss_bkd_amt; 
RUN;

/* Perform correlation analysis */
Title 'Corrlation Analysis';
PROC CORR DATA=work.ad_units PLOTS(maxpoints= None)=matrix(HISTOGRAM NVAR=ALL);
	VAR aird_ind grss_bkd_amt inv_lnth_in_sec inv_typ_cd dy_prt_id median_aired_rate
		mean_aired_rate units_cleared avails;
RUN;
