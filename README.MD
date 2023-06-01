## Table of Contents
1. [Project Description]
2. [Lenguajes and Frameworks]
3. [General]
***

### Project Description

This project consists of a local REST API to serve the migration of historical data contained in multiple csv files.

At the moment, this API can insert data in an Azure SQL DB and they can accept 3 different files schemas:

#### hired_employees.csv:

id: INTEGER<br>
name: STRING<br>
datetime: String datetime in ISO format<br>
department_id: INTEGER<br>
jon_id: INTEGER

#### departments.csv:

id: INTEGER <br>
department: STRING

#### jobs.csv:

id: INTEGER <br>
job: STRING
***

### Lenguajes and Frameworks

The lenguages used it to construct the API REST was python and Flask framework. The API include a POST method for send csv data and include endpoints that users can use to retrive some specific metrics:

Number of emplyees hired for each job and department in 2021 divided by quarter:

http://127.0.0.1:5000/totalDep

List of ids, name and number of employees hired of each department that hired more
employees than the mean of employees hired in 2021 for all the departments:

http://127.0.0.1:5000/mostDep
***

### General

The send_data.py script was designed to send csv data. This script takes the data at the specified path, splits the data into packets of 1000 rows if necessary, and sends the POST request to the API.