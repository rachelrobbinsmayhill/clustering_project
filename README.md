Clustering Project - Zillow - Zillow: What is driving the errors in the Zestimates?
Codeup - Innis Cohort - April 2022
 
===


<img src='Zestimate.png' width=1000 height=350 align="center"/>

===
 
Table of Contents
---
 
* I. [Project Overview](#i-project-overview)<br>
[1. Description](#1-description)<br>
[2. Goals](#2-goals)<br>
[3. Initial Questions](#3-initial-questions)<br>
[4. Formulating Hypotheses](#4-formulating-hypotheses)<br>
[5. Deliverables](#5-deliverables)<br>
* II. [Project Data Context](#ii-project-data-context)<br>
[1. Data Dictionary](#1-data-dictionary)<br>
* III. [Project Plan - Data Science Pipeline](#iii-project-plan---using-the-data-science-pipeline)<br>
[1. Project Planning](#1-plan)<br>
[2. Data Acquisition](#2-acquire)<br>
[3. Data Preparation](#3-prepare)<br>
[4. Data Exploration](#4-explore)<br>
[5. Modeling & Evaluation](#5-model--evaluate)<br>
[6. Product Delivery](#6-delivery)<br>
* IV. [Project Modules](#iv-project-modules)<br>
* V. [Project Reproduction](#v-project-reproduction)<br>
 
 
 
## I. PROJECT OVERVIEW
 
 
#### 1. DESCRIPTION:
Zillow is the leading real estate and rental marketplace dedicated to empowering consumers with data, inspiration, and knowledge about the place they call home and connecting them with the best local professionals who can help. According to the National Association of Realtors, there are over 119 million homes in the United States, over 5 million of which are sold each year. 80% of these homes have been viewed on Zillow regardless of their market status.

Zillow serves the full lifecycle of owning and living in a home: buying, selling, renting, financing, remodeling, and more. It starts with Zillow's living database of more than 110 million U.S. homes - including homes for sale, homes for rent and homes not currently on the market, as well as Zestimate home values, Rent Zestimates, and other home-related information.

The Zestimate is a key element driving web traffic to Zillow, where sellers, buyers, agents, and curiosity-seekers gain knowledge of a home's value. In fact, over the years, Zillow has built a solid reputation around the Zestimate. The Zestimate takes in layers of data regarding a home's features and location and presents buyers and sellers with a value of a home. Zillow publishes Zestimates for 104 million homes, updating them weekly.

Although Zillow has a model to assist in predicting a home's value, they are looking to fine-tune the model and **improve the Zestimate Residual Error**. This project has been requested by the Zillow Data Science Team.

#### 2. GOALS: 
The goal of this project is to **find key drivers of logerror** for Single Family Properties and to construct a Machine Learning Regression Model to improve the Zestimate Residual Error (logerror). The model will help Zillow develop more accurate, dependable, and trustworthy Zestimates, thus sustaining and bolstering their loyal customer base. 

Upon completion of the model, the project will make recommendations on what does and doesn't impact logerror and deliver the recommendations in a report to the Data Science team at Zillow, so they can understand the process that developed the conclusion and have the information available to replicate the findings. 

#### 3. INITIAL QUESTIONS:



##### Data-Focused Questions
- [x] Is there a relationship between logerror and lot size in each county?
- [x] Is there a relationship between logerror and finished square footage of the property in each county?
- [x] Is logerror significantly different for properties in LA County vs Orange County vs Ventura County?
- [x] Is there a relationship between logerror and zipcode?
- [x] Controlling for property square footage, what is the relationship between logerror and age of the home?

 
##### Overall Project-Focused Questions

- What will the end product look like?
- What format will it be in?
- Who will it be delivered to?
- How will it be used?
- How will I know I'm done?
- What is my MVP?
- How will I know it's good enough?
 

#### 4. FORMULATING HYPOTHESES
The project started with the hypothesis that the lot size, finished square feet, and location of the property would have the greatest impact on the residual error.  

 
#### 5. DELIVERABLES:
- [x] Github Repo - containing a final report (.ipynb), acquire & prepare modules in the form of a Wrangle.py, as well as supplemental files such as imports.py.
- [x] README file - provides an overview of the project and steps for project reproduction. 
- [x] Draft Jupyter Notebooks - provide all steps taken to produce the project.
- [x] Python Module File - provides reproducible code for acquiring and preparing the data.
- [x] wrangle.py - used to acquire data and prepare data
- [x] Report Jupyter Notebook - provides final presentation-ready assessment and recommendations.
- [x] 5 minute presentation to stakeholders (Zillow Data Science Team). 
 
 
## II. PROJECT DATA CONTEXT
 
#### 1. DATA DICTIONARY:
The final DataFrame used to explore the data for this project contains the following variables (columns).  The variables, along with their data types, are defined below in alphabetical order:
 

| Variable                       | Definition                                         | Data Type |
|:-------------------------------|:--------------------------------------------------:|:---------:|
| acres                          | grouped bins, based upon lot square footage        | category  |
| age                            | grouped bins, based upon year built.               | category  |
| assessed_value                 | total tax assessed value of the property           | float64   |
| bathroom_bins                  | grouped bins, based upon number of bedrooms        | category  |
| bathrooms                      | number of bathrooms and half-bathrooms in home     | float64   |
| bedroom_bins                   | grouped bins, based upon number of bedrooms        | category  |
| bedrooms                       | number of bedrooms in the home                     | float64   |
| county_code_bin                | name of county as assigned by state_county_code    | category  |
| county_code_bin_Orange County  | numeric variable representing county_code_bin      | uint8     |
| county_code_bin_Ventura County | numeric variable representing county_code_bin      | uint8     |
| latitude                       | Latitude of the middle of the parcel multiplied by 10e6  | category  |
| logerror                       | Residual Error in Home Valuation                   | float64   |
| longitude                      | Longitude of the middle of the parcel multiplied by 10e6 | category  |
| home_sizes                     | grouped bins, based upon square footage            | category  |
| square_feet                    | total finished living area of the home             | float64   |
| state_county_code              | federal information processing standards code      | object    |
| total_rooms                    | combined number of bedrooms and bathrooms          | float64   |
| year_built                     | year the primary residence was constructed         | int64     |




## III. PROJECT PLAN - USING THE DATA SCIENCE PIPELINE:
The following outlines the process taken through the Data Science Pipeline to complete this project. 
 
Plan??? Acquire ??? Prepare ??? Explore ??? Model & Evaluate ??? Deliver
 
#### 1. PLAN
- [x] Review project expectations
- [x] Draft project goal to include measures of success
- [x] Create questions related to the project
- [x] Create questions related to the data
- [x] Create a plan for completing the project using the data science pipeline
- [x] Create a data dictionary to define variables and data context
- [x] Draft starting hypothesis
 
#### 2. ACQUIRE
- [x]  Create .gitignore
- [x]  Create env file with log-in credentials
- [x]  Store env file in .gitignore to ensure the security of sensitive data
- [x]  Create wrangle.py module
- [x]  Store functions needed to acquire the Zillow dataset from mySQL
- [x]  Ensure all imports needed to run the functions are inside the imports.py document
- [x]  Using Jupyter Notebook
- [x]  Run all required imports
- [x]  Import functions from aquire.py module
- [x]  Summarize dataset using methods and document observations
 
#### 3. PREPARE
Using Jupyter Lab Notebook
- [x] Create prepare functions in the wrangle.py module
- [x] Store functions needed to prepare the Zillow data such as:
   - [x] Split Function: to split data into train, validate, and test
   - [x] Cleaning Function: to clean data for exploration
   - [x] Encoding Function: to create numeric columns for object column
   - [x] Feature Engineering Function: to create new features
- [x] Ensure all imports needed to run the functions are inside the wrangle.py document
Using Jupyter Notebook
- [x] Import functions from prepare.py module
- [x] Summarize dataset using methods and document observations
- [x] Clean data
- [x] Features need to be turned into numbers
- [x] Categorical features or discrete features need to be numbers that represent those categories
- [x] Continuous features may need to be standardized to compare like datatypes
- [x] Address missing values, data errors, unnecessary data, renaming
- [x] Split data into train, validate, and test samples
 
#### 4. EXPLORE
Using Jupyter Notebook:
- [x] Answer key questions about hypotheses and find drivers of residual error
  - Run at least two statistical tests
  - Document findings
- [x] Create visualizations with the intent to discover variable relationships
  - Identify variables related to property values
  - Identify any potential data integrity issues
- [x] Summarize conclusions, provide clear answers, and summarize takeaways
  - Explain plan of action as deduced from work to this point
 
#### 5. MODEL & EVALUATE
Using Jupyter Notebook:
- [x] Establish baseline accuracy
- [x] Train and fit multiple (3+) models with varying algorithms and/or hyperparameters
- [x] Compare evaluation metrics across models
- [x] Remove unnecessary features
- [x] Evaluate best performing models using validate set
- [x] Choose best performing validation model for use on test set
- [x] Test final model on the out-of-sample testing dataset
- [x] Summarize performance
- [x] Interpret and document findings
 
#### 6. DELIVERY
- [x] Prepare five-minute presentation using Jupyter Notebook
- [x] Include an introduction of project and goals
- [x] Provide an executive summary of findings, key takeaways, and recommendations
- [x] Create walkthrough of analysis 
  - Visualize relationships
  - Document takeaways
  - Explicitly define questions asked during the initial analysis
- [x] Provide final takeaways, recommend a course of action, and next steps
- [x] Be prepared to answer questions following the presentation

 
 
## IV. PROJECT MODULES:
- [x] Python Module Files - provide reproducible code for acquiring,  preparing, exploring, & testing the data.
   - [x] imports.py - used to store all imports needed to run functions and processes
   - [x] wrangle.py - used to acquire and prepare data
 
  
## V. PROJECT REPRODUCTION:
### Steps to Reproduce
 
- [x] You will need an env.py file that contains the hostname, username, and password of the mySQL database that contains the zillow database
- [x] Store that env file locally in the repository
- [x] Make .gitignore and confirm .gitignore is hiding your env.py file
- [x] Clone my repo (including the imports.py and wrangle.py)
- [x] Import python libraries:  pandas, matplotlib, seaborn, numpy, and sklearn
- [x] Follow steps as outlined in the README.md. and workbook.ipynb
- [x] Run final_report.ipynb to view the final product
