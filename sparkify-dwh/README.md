# Project Description

Build a database schema with Fact and Dimension tables for a music streaming company **Sparkify**, enabling business analysts and sales management
make meaningful decision based on the various information like user habits, preferences , type of users (free or paid), artists, albums and songs etc, 
to take their company in proper future path.

## Project Objective 

The objective of the project is to demontrate a clear understanding of the following data warehouse concepts:
	
	a. Usage of configuration file to read Endpoint details and database connection information.
	b. Cluster creation and access bucket in Amazon cloud using "Infrastructure as Code" mechanism
	c. Create Database with user and permission
	d. Designing a "Star" schema with Fact and Dimension Tables
	
	
## File paths in s3 bucket	

1. s3://udacity-dend/log_data
2. s3://udacity-dend/log_json_path.json
3. s3://udacity-dend/song_data

## Configuration File - dwh.cfg

This file contains the essential configuration properties like cluster endpoint, IAM Role, DB connection details and 
location of metadata and other json data files.

[CLUSTER]
HOST=dwhcluster.c8ua9azfmct2.us-west-2.redshift.amazonaws.com
DB_NAME=dwh
DB_USER=dwhuser
DB_PASSWORD=xxxxx
DB_PORT=5439

[IAM_ROLE]
ARN=arn:aws:iam::264028458522:role/dwhRole

[S3]
LOG_DATA='s3://udacity-dend/log_data'
LOG_JSONPATH='s3://udacity-dend/log_json_path.json'
SONG_DATA='s3://udacity-dend/song_data'

### Fact Table
1. songplays

### Dimension Tables
1. users - *contains data related to users*

2. songs - *contains details of songs like song name, duration etc*

3. artists - *contains details of artists* 

4. time - *contains the details of time when a song was played by various listeners*

## Library Dependency
1. psycopg2
2. configparser

Install the above libraries using **pip install** if these don't exist already in your system.

### ER Diagram
Please take a look at the sparkifydb_erd.png which is geneated using
sqlalchemy and sqlalchemy_schemadisplay packages

