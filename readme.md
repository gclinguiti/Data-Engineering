# CAPSTONE(IBGE - DATA ENGINEERING FOR ANALYTICS)

This is an ETL project focused in the IBGE(Brazilian Institute of Geography and Statistics) assistence called as 'Bolsa Familia'.

Bolsa Família (or Family Allowance) is a social welfare program provided by the Brazilian government, which provides financial aid to poor Brazilian families and for people with special necessities.

The program attempts to both reduce short-term poverty by direct cash transfers and fight long-term poverty by increasing human capital among the poor through conditional cash transfers. It also works to give free education to children who cannot afford to go to school to show the importance of education.

## Purpose

This project have the purpose of creating a data pipeline extracting data from multiple
json files to match, load and insert data into the defined database formats.

## Database Schema

The database is composed by a star schema based on the 'fact_ibge_table' table in the middle, and 2 supporting dimension tables (dim_value_description and dim_benefit_amount).

Star schema databases allows us querying around all kind of data from all the dimension tables starting from a central table in common for all the others.

##STAGING
#staging_ibge_data
`city_expenses INT NOT NULL,family_count INT NOT NULL, city_code INT NOT NULL, year_month INT NOT NULL, ibge_keys INT NOT NULL SERIAL PRIMARY KEY`


#staging_complete_data
`city_code INT NOT NULL PRIMARY KEY, state_iso VARCHAR NOT NULL(2), year_month INT NOT NULL, basic_benefits INT NOT NULL, variable_benefits INT NOT NULL, youngling_benefits INT NOT NULL, nutriz_benefits INT NOT NULL, pregnancy_benefits INT NOT NULL, poverty_benefits INT NOT NULL, details_key INT NOT NULL SERIAL PRIMARY KEY`

##FACT
#fact_ibge
`ibge_keys INT NOT NULL, city_code INT NOT NULL PRIMARY KEY,year_month INT NOT NULL`

##DIMMENSION TABLES

#dim_benefit_amount
`city_code INT NOT NULL PRIMARY KEY, basic_benefits INT NOT NULL, variable_benefits INT NOT NULL, youngling_benefits INT NOT NULL, nutriz_benefits INT NOT NULL, pregnancy_benefits INT NOT NULL, poverty_benefits INT NOT NULL, state_iso VARCHAR NOT NULL`

#dim_value_description
`city_code INT NOT NULL PRIMARY KEY,year_month INT NOT NULL, family_count INT NOT NULL, city_expenses INT NOT NULL`


##CONFIG FILE SETUP
To run this project you will need to fill the following information, and save it as `ibge.cfg` in the project root folder.

```
[AWS]
KEY=
SECRET=

[IAM_ROLE]
ARN=

[CLUSTER]
CLUSTER_TYPE=multi-node
CLUSTER_IDENTIFIER=ibge_capstone
NUM_NODES=4
NODE_TYPE=dc2.large

[CONNECTION]
HOST=
DB_NAME=
DB_USER=
DB_PASSWORD=
DB_PORT=

[DATA]
IBGE_DATA='/data/ibge_benefit'
COMPLETE_DATA='/data/ibge_complete'
```

## Executing the project
1. Open project workspace
2. Run terminal
    1. Run 'pip install -r requirements.txt'
    2. Run create_cluster.py
    3. Run create_tables.py
    4. Run etl.py

### Steps followed on this project

1. Create Table Schemas
- Design schemas for your fact and dimension tables
- Write a SQL CREATE statement for each of these tables in sql_queries.py
- Complete the logic in create_tables.py to connect to the database and create these tables
- Write SQL DROP statements to drop tables in the beginning of - create_tables.py if the tables already exist. This way, you can run create_tables.py whenever you want to reset your database and test your ETL pipeline.
- Launch a redshift cluster and create an IAM role that has read access to S3.
- Add redshift database and IAM role info to dwh.cfg.
- Test by running create_tables.py and checking the table schemas in your redshift database. You can use Query Editor in the AWS Redshift console for this.

2. Build ETL Pipeline
- Implement the logic in etl.py to load data from S3 to staging tables on Redshift.
- Implement the logic in etl.py to load data from staging tables to analytics tables on Redshift.
- Test by running etl.py after running create_tables.py and running the analytic queries on your Redshift database to compare your results with the expected results.
- Delete your redshift cluster when finished.

3. Document Process
Do the following steps in your README.md file.

- Discuss the purpose of this database in context of the startup, Sparkify, and their analytical goals.
- State and justify your database schema design and ETL pipeline.
- [Optional] Provide example queries and results for song play analysis.
