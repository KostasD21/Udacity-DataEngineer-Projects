# Introduction

The goal of this project is to create an ETL pipeline and combine data from logs and app events.
Then, the structured data need to be persisted in a database called sparkifydb.

## Requirements

We requested from the data analyics team to create a database schema and ETL pipeline, which will combine various sources of data.
These sources include JSON files from logs and application events.
The queries should run optimally for the analytics team.

## Database architecture

First of all we chose a star schema architecture for the database with one fact table (songplays) and four dimensional tables (users,songs,artists,times)
We prefer a denormalized schema, so that the reads are fast. That way we manage to keep relatively small response times from database.

### ER Diagram
<img src="ER_diagram_database.png">

## Installation

First of all, make sure that the Python 3 is installed on your machine

`python3 --version`

First of all Postgres need to be up and running in order this project to run.

To install Postgres, use a docker client like Kitematic and initialize a database with name sparkifydb and credentials:
- username: student
- password: student

## Run

All the quries for initialization, update and drop of the database

`python3 sql_queries.py`

Includes functions to create the table schema and then drop the schema

`python3 create_tables.py`

Includes the functions that process the JSON files

`python3 etl.py`

The rest files are jupyter notebooks and include step-by-step instructions for the ETL process and the validation of the query results
