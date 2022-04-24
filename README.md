# ETL Project using Postgres

This project is to understand the basic concept of  **Extract, Transform and Load** of data that is in json format

## Project Requirements ##

1. Use Star Schema
2. 1 Fact Table
3. 4 Dimension Tables

## Library Dependency
1. psycopg2
2. numpy
3. pandas
4. ipython-sql

Install the above libraries using **pip install** if these don't exist already in your system.


### Fact Table
1. songplays

### Dimension Tables
1. users - *contains data related to users*

2. songs - *contains details of songs like song namem, duration etc*

3. artists - *contains details of artists* 

4. time - *contains the details of time when a song was played by various listeners*


The sample data for this project are in json format in the **data** foder


### What is the purpose of the application

This application facilitates a music streaming company called "**Sparkify**" to analyze the data and there by study the listeners behaviors.


### What is the purpose of this project

As a student of Data Engineering, this project makes you understand the concept of ETL piline using Python and  Postgress and get your feet wet.  You really need to have an intermediate level of python knowledge.  (I struggled a bit, but I went through a crash course of python iin udacity). 

### ER Diagram
Please take a look at the sparkifydb_erd.png which is geneated using
sqlalchemy and sqlalchemy_schemadisplay packages

