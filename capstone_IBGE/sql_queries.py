import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('ibge.cfg')

# DROP TABLES
staging_ibge_data_table_drop = "DROP TABLE IF EXISTS staging_ibge_data;"
staging_complete_data_table_drop = "DROP TABLE IF EXISTS staging_complete_data;"
fact_ibge_table_drop = "DROP TABLE IF EXISTS fact_ibge;"
dim_value_description_table_drop = "DROP TABLE IF EXISTS dim_value_description;"
dim_benefit_amount_table_drop = "DROP TABLE IF EXISTS dim_benefit_amount"

# CREATE TABLES
##STAGING
staging_ibge_data_table_create= ("""CREATE TABLE IF NOT EXISTS staging_ibge_data (
        city_expenses INT NOT NULL,
        family_count INT NOT NULL,
        city_code INT NOT NULL,
        year_month INT NOT NULL,
        ibge_keys INT NOT NULL SERIAL PRIMARY KEY
);""")

staging_complete_data_table_create = ("""CREATE TABLE IF NOT EXISTS staging_complete_data (
        city_code INT NOT NULL PRIMARY KEY,
        state_iso VARCHAR NOT NULL(2),
        year_month INT NOT NULL,
        basic_benefits INT NOT NULL,
        variable_benefits INT NOT NULL,
        youngling_benefits INT NOT NULL,
        nutriz_benefits INT NOT NULL,
        pregnancy_benefits INT NOT NULL,
        poverty_benefits INT NOT NULL,
        details_key INT NOT NULL SERIAL PRIMARY KEY
);""")

##FACT
fact_ibge_table_create= ("""CREATE TABLE IF NOT EXISTS fact_ibge (
        ibge_keys INT NOT NULL,
        city_code INT NOT NULL PRIMARY KEY,
        year_month INT NOT NULL
);""")

##DIM
dim_benefit_amount_table_create= ("""CREATE TABLE IF NOT EXISTS dim_benefit_amount (
        city_code INT NOT NULL PRIMARY KEY,
        basic_benefits INT NOT NULL,
        variable_benefits INT NOT NULL,
        youngling_benefits INT NOT NULL
        nutriz_benefits INT NOT NULL,
        pregnancy_benefits INT NOT NULL,
        poverty_benefits INT NOT NULL,
        state_iso VARCHAR NOT NULL(2)
);""")

dim_value_description_table_create= ("""CREATE TABLE IF NOT EXISTS dim_value_description (
        city_code INT NOT NULL PRIMARY KEY,
        year_month INT NOT NULL,
        family_count INT NOT NULL,
        city_expenses INT NOT NULL);""")

# STAGING TABLES

staging_ibge_data_copy = ("""
copy staging_ibge_data
from {}
iam_role {}
json 'auto'
REGION 'us-west-2'
COMPUPDATE OFF STATUPDATE OFF;
""").format(config['DATA']['IBGE_DATA'],config['IAM_ROLE']['ARN'])

staging_complete_data_copy = ("""
copy staging_complete_data
from {}
iam_role {}
json 'auto'
REGION 'us-west-2'
COMPUPDATE OFF STATUPDATE OFF;
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# INSERT TABLES

fact_ibge_table_insert=("""INSERT INTO fact_ibge (
        SELECT city_expenses,
        family_count,
        city_code,
        year_month,
        ibge_keys
        FROM staging_ibge_data)
);""")

dim_value_description_table_insert=("""INSERT INTO dim_value_description(
        SELECT city_code,
        year_month,
        family_count,
        city_expenses
        FROM staging_ibge_data)
);""")

dim_benefit_amount_table_insert=("""INSERT INTO dim_benefit_amount (
        SELECT city_code,
        basic_benefits,
        variable_benefits,
        youngling_benefits,
        nutriz_benefits,
        pregnancy_benefits,
        poverty_benefits,
        state_iso
        FROM staging_complete_data)
);""")

# QUERY LISTS

create_table_queries = [staging_ibge_data_table_create, staging_complete_data_table_create, fact_ibge_table_create, dim_value_description_table_create, dim_benefit_amount_table_create]
drop_table_queries = [staging_ibge_data_table_drop, staging_complete_data_table_drop,fact_ibge_table_drop, dim_value_description_table_drop, dim_benefit_amount_table_drop]
copy_table_queries = [staging_ibge_data_copy, staging_complete_data_copy]
insert_table_queries = [fact_ibge_table_insert, dim_value_description_table_insert, dim_benefit_amount_table_insert]