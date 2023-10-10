# Project: STEDI Human Balance Analytics

## Introduction
In this Project, I’m asked to build a data lakehouse solution for sensor data that trains a machine learning model.

## Project Description
To reach my goals, I’ve used Python, Spark, AWS Glue, AWS Athena and AWS S3.


## Data
The data used in this project is in JSON format. The data was originally on 3 S3 buckets:
- customer;
- step_trainer;
- accelerometer.




### Customer data
Fields on the customer data:
- serialnumber;
- sharewithpublicasofdate;
- birthday;
- registrationdate;
- sharewithresearchasofdate;
- customername;
- email;
- lastupdatedate;
- phone;
- sharewithfriendsasofdate.

### Step trainer records data
Fields on the step trainer data:
- sensorReadingTime;
- serialNumber;
- distanceFromObject.

### Accelerometer data
Fields on the accelerometer data:
- timeStamp;
- user;
- x;
- y;
- z.



## ETL

### Landing Zone

#### Glue tables:
- customer_landing.sql
- accelerometr_landing.sql

*Customer Landing*:

<figure>
  <img src="prints/customer_landing.png" alt="Customer Landing data" width=60% height=60%>
</figure>

*Accelerometer Landing*:

<figure>
  <img src=" prints/accelerometer_landing.png" alt="Accelerometer Landing data" width=60% height=60%>
</figure>



### Trusted Zone


- [customer_landing_to_trusted.py](scripts/customer_landing_to_trusted.py)
- [accelerometer_landing_to_trusted.py](scripts/accelerometer_landing_to_trusted.py)

**Athena**:
Trusted Zone Query results:

<figure>
  <img src="prints/customer_trusted.png" alt="Customer Truested data" width=60% height=60%>
</figure>

### Curated Zone


- [customer_trusted.py](scripts/customer_trusted.py)
- [training_data.py](scripts/training_data.py)


