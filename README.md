# MIS581 Capstone Project: Predicting Aired Status of Preemptable Advertising Units

This repository hosts the code and visualizations used in the CSU Global MIS581 Capstone Project. This project's research question was “What is the relationship between the aired/preempted status of non-guaranteed preemptable ads and key variables including unit rate, available inventory, and aggregate rates of previously aired ads?” The code and visualizations in this repository attempt to answer this research question using descriptive analytics, a T-Test, a Correlation Analysis, and Predictive Analytics. 

## Overview

Data Mining process that uses historical patterns of unit clearance to identify performance spots at risk of preemption. The process uses 30 days of unit level clearance data to train a decision tree model. Production data for all DR and Unwired spots booked for this week and beyond are run through decision tree model to predict if the spots will air. The model features include spot level metrics, aggregated clearance metrics, and avails. 

## Process Diagram

![](assets/Perforamance_Clearance_Data_Mining.jpg)

## Usage

The scripts in this repository can be used to reproduce the results discussed in the MIS581 Capstone Project write up and final presentation. The repository contains the following: 

* _MIS581_Capstone_Project_ad_unit_data.csv.zip_
  * This is the data set used in the research project. The data is stored in a compress .csv file. Provided below is a data dictionary for the data set. 

| Variable Name | Data Type | Description | 
|-|-|-|
| clndr_dt| Date (Time) | Monday of the week the ad unit is booked for  |  
| dl_unt_id | Integer (Nominal)  | ID number of the ad unit  |  
| grss_bkd_amt | Integer (Interval) | Dollar value of the Ad Unit |
| inv_lnth_in_sec | Integer (Ordinal)  | The length of the ad unit in seconds (15, 30, 60, 90, or 120) |
| network_code | Integer (Nominal)  | ID number of the network the unit is booked against |
| inv_typ_cd | Integer (Ordinal)  | Break type the unit is booked against (1=Full Footprint, 2= Addressable Underlying, 3= Local Addressable Underlying) |  
| dy_prt_id | Integer (Ordinal)  | The broadcast daypart the unit is booked against (1=Early Morning, 2= Daytime, 3=Fringe, 4=Prime, 5=Late Prime, 6=Overnight, 7=Weekend) | 
| aird_ind | Boolean (Binary)   | Boolean indicator describing if the unit was aired or preempted for higher-priority or higher-value ad units |
| median_aired_rate | Integer (Interval) | Median unit rate for ads that aired in the same inventory in the 30 days prior to the clndr_dt value of the unit |
| mean_aired_rate | Integer (Interval) | Mean unit rate for ads that aired in the same inventory in the 30 days prior to the clndr_dt value of the unit   |
| units_cleared	| Integer (Interval) | Average number of weekly aired preemptable units in the same inventory in the 30 days prior to the clndr_dt value of the unit | 
| avails | Integer (Count)  | Total number of available 30-second ad slots for preemptable buyers in the week the unit is booked against |

* _MIS581 Capstone Project.sas_
  * This .sas file is the code used to perform descriptive statistics on the ad unit data set. This file also contains the code used to perform the T-Test and the Correlation Analysis. 
* _MIS581 Capstone Project Predictive.py_
  * This Python script is the code used to develop the predicitve models of in the research project. The script creates both a logistic regression and a decsion tree model.
* _MIS581 Capstone Project Model Results.twbx_
  * This Tableau workbook was used to create the Confusion Matrix and ROC Curves for the research project. 

## Requirements

* SAS Studio
* Tableau Public
* Python 3.7

## Contributing

At this time this project is not open for contributions

## Authors

Daniel Willer - daniel.willer@dish.com

