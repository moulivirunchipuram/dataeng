**README.md**

**Files**: 

1. dl.cfg

2. etl.py

__What is the Purpose__: 

The purpose of this project is to create an ETL pipeline for a data lake hosted on 
Udacity's S3 bucket. We are to load the data from S3, process the data into their respective fact and 
dimension tables using Spark, and then load the parquet files back into S3.

**NOTE:** 

Although the project requires to use S3 bucket, due to some issues with my set up, I did this project using local file structure in the Udacity Workspace.

**BACKGROUND INFORMATION**:

The music streaming company "Sparkify" is in a situation with huge user base and data related to songs that are streamed has grown very big. In order to make the data handling more efficient, the they have decided to move their data from their Data Warehouse to Data Lake.  Currently their data resides in S3 as JSON logs and meta data of songs.

**IMPLEMENTATION**:

This project enables the company to read data from S3 as dimentional tables.  Star Schema is being used to achieve this as follows:

__FACT TABLE__

1. **songplays** - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

**DIMENSION TABLES**

1. **users** - user_id, first_name, last_name, gender, level

2. **songs** - song_id, title, artist_id, year, duration

3. **artists** - artist_id, name, location, lattitude, longitude

4. **time** - start_time, hour, day, week, month, year, weekday







