 
import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek

from pyspark.sql.types import *
import pandas as pd


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """ Creates the necessary spark session for our processing.

        Returns:
            the Spark Session
        """

    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """ Reads the song data files from the json form and derives the song and artist tablesS from them.
        Finally writes the two tables on parquet file format.

        Arguments:
            spark {object}: Spark session.
            input_data {string}: The path of input data on S3
            output_data {string}: The path of output_data on S3
        """

    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*"

    # read song data file
    df = spark.read.json(song_data, mode='PERMISSIVE', columnNameOfCorruptRecord='corrupt_record').drop_duplicates()

    # extract columns to create songs table
    songs_table = df.select("song_id","title","artist_id","year","duration").drop_duplicates()

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data + "songs/", mode="overwrite", partitionBy=["year","artist_id"])

    # extract columns to create artists table
    artists_table = df.select("artist_id","artist_name","artist_location","artist_latitude","artist_longitude").drop_duplicates()

    # write artists table to parquet files
    artists_table.write.parquet(output_data + "artists/", mode="overwrite")


def process_log_data(spark, input_data, output_data):
    """ Reads the log data files from the json form and derives the user and songplays and time tables from them.
        Finally writes the three tables on parquet file format.

        Arguments:
            spark {object}: Spark session.
            input_data {string}: The path of input data on S3
            output_data {string}: The path of output_data on S3
        """

    # get filepath to log data file
    log_data = input_data + "log_data/"

    # read log data file
    df = spark.read.json(log_data, mode='PERMISSIVE', columnNameOfCorruptRecord='corrupt_record').drop_duplicates()
    #print(df.count())

    # filter by actions for song plays
    df = df[df['page'] == 'NextSong']

    # extract columns for users table
    users_table = df.select("userid","firstName","lastName","gender","level").drop_duplicates()

    # write users table to parquet files
    users_table.write.parquet(output_data + "users/", mode="overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x : datetime.utcfromtimestamp(int(x)/1000), TimestampType())
    df = df.withColumn("start_time", get_timestamp("ts"))

    # extract columns to create time table
    time_table = df.withColumn("hour",hour("start_time"))\
                    .withColumn("day",dayofmonth("start_time"))\
                    .withColumn("week",weekofyear("start_time"))\
                    .withColumn("month",month("start_time"))\
                    .withColumn("year",year("start_time"))\
                    .withColumn("weekday",dayofweek("start_time"))\
                    .select("ts","start_time","hour", "day", "week", "month", "year", "weekday").drop_duplicates()

    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(os.path.join(output_data, "time_table/"), mode='overwrite', partitionBy=["year","month"])

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + "songs/")
    artists_df = spark.read.parquet(output_data + "artists/")

    # extract columns from joined song and log datasets to create songplays table
    song_artist_df = song_df.join(artists_df, song_df.artist_id == artists_df.artist_id, how='inner').drop(song_df["artist_id"])
    songplays_table = song_artist_df.join(df, (song_artist_df.title == df.song) & (song_artist_df.artist_name == df.artist), how='inner')\
                                    .select(monotonically_increasing_id().alias("songplay_id"),col("start_time"),col("userId").alias("user_id"),col("level"),col("song_id"),col("artist_id"), col("sessionId").alias("session_id"), col("location"), col("userAgent").alias("user_agent"))
    songplays_table = songplays_table.join(time_table, songplays_table.start_time == time_table.start_time, how="inner")\
                        .select("songplay_id", songplays_table.start_time, "user_id", "level", "song_id", "artist_id", "session_id", "location", "user_agent", "year", "month")

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(output_data + "songplays/", mode="overwrite", partitionBy=["year","month"])


def main():
    spark = create_spark_session()
    input_data = "s3://udacity-spark-project/"
    output_data = "s3://udacity-spark-project/output/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
