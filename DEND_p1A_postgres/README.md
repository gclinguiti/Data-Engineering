# DEND Sparkify Song Play Analysis, ETL Proccess & ETL Pipeline

## Purpose

This project have the purpose of getting data from multiple sources and putting them together to extract and log information into multiple datasets, transforming raw data in a database where we are able to make any kind of analysis that we need.

### Busines Value

Sparkify is a company expecting to gather their data through `sparkifydb`, using this database to have their insights of custommer behavior and user trends for a future project.

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
2. Open the following files
    1. etl.ipynb
    1. test.ipynb
3. Run `python create_tables.py` to create the `sparkifydb` database and the empty tables.
4. Run `python etl.py` to run the etl pipeline, reading the data and populating the tables created in the previous step.
5. Run the complete `test.ipynb` query examples to guarantee the database was created.
6. Run the complete `elt.ipynb` functions and queries.
7. Run `test.ipynb` again to collect the insert results.