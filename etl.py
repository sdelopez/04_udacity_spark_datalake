import configparser
from datetime import datetime
import os
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek, to_timestamp
from pyspark.sql.types import *


config = configparser.ConfigParser()
config.read('dl.cfg')

# Get Credentials for AWS development 
os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS','AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    """
    Create a Spark session

    :return: spark session
    :rtype: spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Transform raw song data from S3 into analytics tables on S3
    
    This function :
    - reads in song data in JSON format from S3
    - processes the raw data into dimension tables songs and artists
    - writes the tables into partitioned parquet files on S3 buckets

    :param spark: Spark session
    :type spark: Spark session object
    :param input_data: an S3 bucket to read song data in json format
    :type input_data: string
    :param output_data: an S3 bucket to write tables in parquet format
    :type output_data: string
    """

    print("Start processing song_data JSON files...")
    start_song_data = datetime.now()

    # get filepath to song data file
    song_data = input_data
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table using Spark Dataframe API
    songs_table = df.select(["song_id", "title", "artist_id", "year", "duration"]) \
                .dropna(how = "any", subset = ["song_id", "artist_id"]) \
                .where(df["song_id"].isNotNull()) \
                .dropDuplicates()    

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite") \
                .partitionBy("year", "artist_id") \
                .parquet((output_data + "songs_table.parquet"))

    # extract columns to create artists table
    artists_table = df.selectExpr("artist_id", "artist_name as name", "artist_location as location", "artist_latitude as latitude", "artist_longitude as longitude") \
                .dropna(how = "any", subset = ["artist_id"]) \
                .where(df["artist_id"].isNotNull()) \
                .dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite") \
                .parquet((output_data + "artists_table.parquet"))

    # stop timer
    print("End processing song_data JSON files...")
    stop_song_data = datetime.now()
    print("Time to process song_data : {} s".format(stop_song_data-start_song_data))

    return songs_table, artists_table


def process_log_data(spark, input_data, output_data):
    """
    Transform raw log data from S3 into analytics tables on S3
    
    This function reads in log data in JSON format from S3; defines the schema
    of songplays, users, and time analytics tables; processes the raw data into
    those tables; and then writes the tables into partitioned parquet files on
    S3.

    :param spark: Spark session
    :type spark: Spark session object
    :param input_data: an S3 bucket to read song data in json format
    :type input_data: string
    :param output_data: an S3 bucket to write tables in parquet format
    :type output_data: string
    """

    print("Start processing log_data JSON files...")
    # start timer
    start_log_data = datetime.now()
    # get filepath to log data file
    log_data = input_data

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table using Spark Dataframe API 
    users_table = df.selectExpr("userId as user_id", "firstName as first_name", "lastName as last_name", "gender as gender", "level as level") \
                .dropna(how = "any", subset = ["user_id"]) \
                .where(df["userId"].isNotNull()) \
                .dropDuplicates()
    
    # write users table to parquet files
    users_table.write.mode("overwrite") \
                .parquet((output_data + "users_table.parquet"))

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x:datetime.fromtimestamp(int(int(x)/1000)), TimestampType())

    df = df.withColumn("start_time", get_timestamp(df.ts))
    
    # extract columns to create time table using Spark DAtaframe API
    time_table = (df.withColumn("hour", hour(df.start_time))
                    .withColumn("day", dayofmonth(df.start_time))
                    .withColumn("week", weekofyear(df.start_time))
                    .withColumn("month", month(df.start_time))
                    .withColumn("year", year(df.start_time))
                    .withColumn("weekday", dayofweek(df.start_time))
                    .select(["start_time", "hour", "day", "week", "month", "year", "weekday"])
                    .dropDuplicates()
                    .orderBy("start_time"))

    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite") \
                    .partitionBy("year", "month") \
                    .parquet((output_data + "time_table.parquet"))

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + "songs_table.parquet")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, df.song == song_df.title, how="left") \
                     .withColumn("songplay_id", monotonically_increasing_id()) \
                     .withColumn("year", year(df.start_time)) \
                     .withColumn("month", month(df.start_time)) \
                     .selectExpr("songplay_id", "start_time", "year", "month", "userId as user_id", "level", 
                                 "song_id", "artist_id", "sessionId as session_id", "location", 
                                 "userAgent as user_agent")

    # write songplays table to parquet files partitioned by year and month

    songplays_table.write.mode("overwrite") \
                        .partitionBy("year", "month") \
                        .parquet(output_data + "songplays_table.parquet")

    print("End processing log_data JSON files...")
    # stop timer
    stop_log_data = datetime.now()
    print("Time to process log_data : {}".format(stop_log_data-start_log_data))

    return users_table, time_table, songplays_table

def query_table_count(spark, output_data):
    """[summary]

    :param spark: [description]
    :type spark: [type]
    :param output_data: [description]
    :type output_data: [type]
    :return: [description]
    :rtype: [type]
    """    

    """Query example returning row count of the given table.

    Keyword arguments:
    * spark            -- spark session
    * table            -- table to count

    Output:
    * count            -- count of rows in given table
    """
    return table.count()


def main():
    """
    Run ETL pipeline on sparkify raw data
    located on S3 bucket s3a://udacity-dend/

    """
    
    print("Start ETL process for Sparkify data")
    # start timer for ETL process
    start_etl = datetime.now()

    # create a spark session
    spark = create_spark_session()
    
    # AWS - uncomment these lines to work on S3 Bucket
    # Get S3 bucket when working on AWS
    # input_data_songs = config.get('AWS','INPUT_DATA_SONGS')
    # input_data_logs = config.get('AWS','INPUT_DATA_LOGS')
    # output_data = config.get('AWS','OUTPUT_DATA')
    
    # LOCAL - uncomment these lines to work on local folders
    # Get local path for Input/output data
    input_data_songs = config.get('LOCAL','INPUT_DATA_SONGS')
    input_data_logs = config.get('LOCAL','INPUT_DATA_LOGS')
    output_data = config.get('LOCAL','OUTPUT_DATA')
    
    # process song_data and create dimension songs_table and artists_table
    songs_table, artists_table = process_song_data(spark, input_data_songs, output_data)

    # process log_data and create fact songplays_table and dimension users_table, time_table
    users_table, time_table, songplays_table = process_log_data(spark, input_data_logs, output_data)

    # stop timer for ETL process
    print("End ETL process for Sparkify data")
    stop_etl = datetime.now()
    print("Time to process ETL : {}".format(stop_etl-start_etl))

    # counting the records in parquet tables
    print("songs_tables has : {} records".format(songs_table.count()))
    print("artists_tables has : {} records".format(artists_table.count()))
    print("users_tables has : {} records".format(users_table.count()))
    print("time_tables has : {} records".format(time_table.count()))
    print("songplays_tables has : {} records".format(songplays_table.count()))


if __name__ == "__main__":
    main()
