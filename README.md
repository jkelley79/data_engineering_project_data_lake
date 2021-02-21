# Introduction

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

# Purpose

The set of scripts will build an ETL pipeline that extracts their data from S3, loading into Spark for transformation into a set of dimensional tables. The data from those tables is then written out to S3 using parquet files for each table. 

# Description of files in this repo

data folder - Contains sample zips of song and log data
output - Ignore as these are old results from testing that fails to be deleted
dl.cfg - Configuration file with all information used to determine the location of input/output buckets and AWS credentials.
etl.py - ETL script which loads the data from AWS S3 into Spark using PySpark and then writes the data back out to another S3 location.
README.md - This file containing information about this project

# Database Design

There are five tables in the schema for this database

songs - Stores a unique list of songs with id, artist, title, year and duration information
artists - Stores a unique list of artists with id, name, location, latitude and longitude
users - Stores a unique list of users that have played songs with id, first_name, last_name, gender, and level
time - Stores a list of play times broken out into columns for time of day, hour, day, week, month, year, weekday
songplays - Stores a list of song plays with information about the user when they played the song including location, level, user_agent, and session

Most analytic queries would be initially started against the songplays table with joins being placed against the other dimension tables for information about the song, artist, user or time that it was played. This is why the songplays table is considered the fact table and the other tables are considered dimensions building out a star schema. 

# ETL pipeline

The ETL pipeline has two main methods
1) Process the song data from S3 and create parquet files for song and artist information in another S3 location
2) Process the log data from S3 using the song data parquet files to create a songplays table

The pipeline is designed as such to load the data from S3 into spark transform it by removing duplicates and filtering out unnecessary events before writing out Spark data frames to parquet files in another S3 location.

# How to use

1) Edit dl.cfg to set AWS key and secret for use to read/write S3 data
2) Edit dl.cfg to specify the correct LOG_PATH, SONG_PATH, INPUT_FOLDER, and OUTPUT_FOLDER to use to read/write the data
3) Run 'python etl.py' - This will load the data from S3 into Spark, transform, and write the data back out into fact and dimension tables 

# Additional notes
1) In setting up Spark I needed to use the package 'org.apache.hadoop:hadoop-aws:2.9.0' in order for it to be able to access S3 properly. This may need to be adjusted based on your versions of Spark.
2) I enabled legacy time parsing which may not be necessary if you are using an older version of Spark than 3.0.1.