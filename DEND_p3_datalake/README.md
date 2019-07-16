# Project 4 - Data Lake

A Data Lake project using Apache Spark with python and AWS services.

## Purpose

This project have the purpose of creating a Data Lake using learned skills with apache spark, loading and inserting S3 data.
The provided bucket contains song and artist info, and also users information from the json files.

## Spark Schema

Song and Log files are processed by the Spark ETL, the song files are taken together and listed after being iterated.

Log files are filtered and the datasets gets processed to extract the requested data (date, time, year, etc.).

As in the previous projects, the database is composed by a star schema based on the Songplay table in the middle, and 4 supporting dimension tables (users, song, artist and time).

Star schema databases allows us querying around all kind of data from all the dimension tables starting from a central table in common for all the others.