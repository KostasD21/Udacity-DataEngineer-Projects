# Introduction

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data.

# Purpose of the project

The purpose of this project is to create a data lake by combining data from logs and app events residing on S3.
Leveraging Spark capabilities we could run analytical queries against the data lake.

# Files included

The files that included in the project are:

* etl.py
* README.md
* dl.cfg (a configuration file is needed in case that the project is run on an AWS cluster)


# Solution

## Database schema design

The database schema design is a start-schema with one fact table (songplay) and four dimension tables (user, artist, song and time)

## ETL process

In the etl process two mainly functions take place:

1) Process song_data method, reads the input json files from song data directory and then create the parquet files for song and artist tables.

2) Process log_data method, reads the input json files from log data directory and then create the parquet files for songplay, time and user tables.

## Parquet file format

We want to store the files from json to parquet file format, so that we can run ad-hoc analytical queries based on its columns.
Parquet is a columnar based file which is used widely in the Hadoop ecosystem, because of its improved performance on analytical queries.
Also, we could achieve better compression with this file format since the columns are of the same data type.

# Run instructions/dependencies

For local environment:

* First unzip the log-data.zip and song-data.zip and relocate the contents on a directory /input

* Install PySpark

`$ pip install pyspark`

* Run the etl.py script

`$ python etl.py`

The results of the processing are in the directory /output













