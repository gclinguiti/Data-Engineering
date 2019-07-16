# Data-Warehouse

A data-warehouse ETL project learning with python and AWS Redshift.

## Purpose

This project have the purpose of creating a data pipeline extracting data from multiple 
json files to match, load and insert data into the defined database formats.

## Database Schema

The database is composed by a star schema based on the Songplay table in the middle, and 4 supporting dimension tables (users, song, artist and time).

Star schema databases allows us querying around all kind of data from all the dimension tables starting from a central table in common for all the others.

# Songplay 
`songplay_id serial, start_time bigint, user_id int, level varchar, song_id varchar, artist_id varchar, session_id int, location varchar, user_agent varchar`

# Users 
`user_id varchar, first_name varchar, last_name varchar,gender varchar, level varchar`

# Song 
`song_id varchar, title varchar, artist_id varchar, year int, duration float(5)`

# Artist 
`artist_id varchar, name varchar, location varchar, lattitude varchar, longitude varchar`

# Time 
`start_time bigint, hour int, day int, week int, month int, year int, weekday int`

## ETL

The ETL proccess is composed by `songplay data` and `log data`, the data into the project tables comes from twi different sources, being them Log Files and Song Files. We complete the tables data matching values from different tables using common keys between them, turning this into a Extracted, Transformed and Loaded data.

## Executing the project
1. Open project workspace
2. Run terminal
    1. Run create_tables.py
    1. Run etl.py