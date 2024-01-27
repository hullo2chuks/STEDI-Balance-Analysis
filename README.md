# STEDI Balance Analysis  
This is a data lakehouse solution for sensor data that trains a machine learning model  

## Description  
This project extract the data produced by the STEDI Step Trainer 
sensors and the mobile app, and curate them into a data lakehouse
solution on AWS so that Data Scientists can train the learning model  

The later essence is to use the motion sensor data to train a machine learning model to
detect steps accurately in real-time. Privacy will be a primary consideration in 
deciding what data can be used.  

## The Process and the Insight
This work uses the data from the STEDI Step Trainer and mobile app to develop a lakehouse 
solution in the cloud that curates the data for the machine learning model using:  

* Python and Spark
* AWS Glue
* AWS Athena
* AWS S3  

This projects also creates Python scripts using AWS Glue and Glue Studio. 
<img alt="Database Architecture" src="https://video.udacity-data.com/topher/2023/October/6527a6fc_flowchart/flowchart.jpeg" width=500 align="center"/>  

### Project datasets
STEDI has three JSON data sources to use from the Step Trainer. Check out the JSON data in 
the landing dataset [landing dataset](landing_datasets) directory:
* [customer](landing_datasets%2Fcustomer). The customer dataset contains the following fields:
```
serialnumber
sharewithpublicasofdate
birthday
registrationdate
sharewithresearchasofdate
customername
email
lastupdatedate
phone
sharewithfriendsasofdate
```
* [step_trainer](landing_datasets%2Fstep_trainer). The step_trainer dataset contains the following fields
```
sensorReadingTime
serialNumber
distanceFromObject
```
* [accelerometer](landing_datasets%2Faccelerometer)  . The accelerometer dataset contains the following fields
```
timeStamp
user
x
y
z
```


## Design and Implementation
The design and implementation has three data outcomes zone based on the processed dataset by the
Glue jobs and Athena query tables
There should also be the expected number of rows in each table:

<details>
<summary>
Landing Zones
</summary>

### Implementations Steps
#### Use Glue Studio to ingest data from an S3 bucket
The jobs have a node that connects to S3 bucket for customer, accelerometer, and step trainer landing zones.
This is done with following python files
  * [customer_landing_to_trusted.py](customer_landing_to_trusted.py)
  * [accelerometer_landing_to_trusted.py](accelerometer_landing_to_trusted.py)
  * [step_trainer_trusted.py](step_trainer_trusted.py)  

#### Manually create a Glue Table using Glue Console from JSON data
SQL DDL scripts [customer_landing.sql](sql_ddl_scripts%2Fcustomer_landing.sql), 
[accelerometer_landing.sql](sql_ddl_scripts%2Faccelerometer_landing.sql), and [step_trainer_landing.sql](sql_ddl_scripts%2Fstep_trainer_landing.sql) 
include all of the JSON fields in the data input files

#### Athena was used to query the Landing Zone
* Count of customer_landing: 956 rows
  ![customer_landing.png](screenshots%2Fcustomer_landing.png)
* Count of accelerometer_landing: 81273 rows
  ![accelerometer_landing.png](screenshots%2Faccelerometer_landing.png)
* Count of step_trainer_landing: 28680 rows
  ![step_trainer_landing.png](screenshots%2Fstep_trainer_landing.png)
</details>

<details>
<summary>
Trusted Zones
</summary>

### Implementations Steps
#### Configure Glue Studio to dynamically update a Glue Table schema from JSON data  
Use Glue Job Python code to shows that the option to dynamically infer and update schema is enabled.  
See python scripts in landing zone

#### Athena to query Trusted Glue Tables
* Count of customer_trusted: 482 rows:
  ![customer_trusted.png](screenshots%2Fcustomer_trusted.png)
* Count of accelerometer_trusted: 40981 rows
  ![accelerometer_trusted.png](screenshots%2Faccelerometer_trusted.png)
* Count of step_trainer_trusted: 14460 rows
  ![step_trainer_trusted.png](screenshots%2Fstep_trainer_trusted.png)

#### Filter protected PII with Spark in Glue Jobs
The [accelerometer_landing_to_trusted.py](accelerometer_landing_to_trusted.py) has a node that drops rows that do not 
have data in the 
sharedWithResearchAsOfDate column

#### Join Privacy tables with Glue Jobs
The `accelerometer_landing_to_trusted.py` has a node that inner joins the customer_trusted data with the accelerometer_landing
data by emails. The produced table should have only columns from the accelerometer table

</details>


<details>
<summary>
Curated Zones
</summary>

### Implementations Steps
#### Glue Job to join trusted data 
The [customer_trusted_to_curated.py](customer_trusted_to_curated.py) has a node that inner joins the customer_trusted data with the accelerometer_trusted data
by emails. The produced table should have only columns from the customer table

#### Write a Glue Job to create curated data
* The [step_trainer_trusted.py](step_trainer_trusted.py) has a node that inner joins the step_trainer_landing data with the customer_curated data by 
serial numbers
* The [machine_learning_curated.py](machine_learning_curated.py) has a node that inner joins the step_trainer_trusted data with the accelerometer_trusted 
data by sensor reading time and timestamps

#### Athena to query Curated Glue Tables
* Count of customer_curated: 482 rows
  ![customer_curated.png](screenshots%2Fcustomer_curated.png)
* Count of machine_learning_curated: 43681 rows
  ![machine_learning_curated.png](screenshots%2Fmachine_learning_curated.png)

</details>

## Authors

## License