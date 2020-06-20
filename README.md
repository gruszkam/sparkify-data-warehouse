## Overview

<p> This project is part of Udacity Data Engineering course.

Startup Sparkify needs to analyze the data it collects on both user activity as well as songs they stream through their app. That data is then stored in JSON files uploaded to the Amazon Cloud Storage service. The goal of this project is to design Data Warehouse using Amazon S3 bucket and Amazon Redshift. Sparkify DB and ETL pipeline are built in that framework to allow for smooth extraction, transformation and loading of data logs from AWS S3 bucket to Redshift SparkifyDB.
</p>


## Design of SparkifyDB

Database design: Data is firstly extracted from JSON files and loaded to the staging tables. As reported data (ARD) is then transformed and loaded to a star schema with 4 dimensional tables and 1 fact table. Schema design is optimized for analytical queries on song play analysis.

### Staging Tables
Table `staging_events` - as reported data from event JSON files.  
Table `staging_songs` - as reported data from song JSON files.

### Fact Table
Table `fact_songplays` - has data on associated with song plays i.e. records with page NextSong. Consists of columns: *songplay_id (PK), start_time (FK), user_id, level (FK), song_id (FK), artist_id (FK), session_id, location, user_agent*

### Dimension Tables
Table `dim_dim_users` - has data on users in the app.  
Consists of columns: *user_id (PK), first_name, last_name, gender, level*  

Table `dim_songs` - has data on songs in the music database.  
Consists of columns: *song_id (PK), title, artist_id, year, duration*  

Table `dim_artists` - has data on artists in music database.  
Consists of columns: *artist_id (PK), name, location, latitude, longitude*  

Table `dim_time` - has data on timestamps of records in songplays broken down into specific units.  
Consists of columns: *start_time (PK), hour, day, week, month, year, weekday*

where (PK) is Primary Key and (FK) is Foreign Key.

## Scripts
`sql_queries.py` - contains list of all SQL queries to create tables and insert data. It is imported into *create_tables.py* and *etl.py*

`create_tables.py` - drops and creates tables in the Redshift Sparkify DB. Should be run each time before executing *etl.py*

`etl.py` - executes ETL pipeline: extracts data from JSON files in the S3 bucket, loads as reported data into Redshift staging tables and lastly insert transformed data into dimension and fact tables.

## Quick Start
Scripts should be run in the following order:

1. Execute `create_tables.py` in order to create the staging tables and dimension tables.
2. Execute `etl.py` to run the ETL pipeline 