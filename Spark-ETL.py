#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import configparser
import datetime
import os
import pandas as pd
import pyspark.sql
from pyspark.sql import SparkSession as SS
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import monotonically_increasing_id


config = configparser.ConfigParser()
config.read_file(open(os.path.abspath('dl.cfg')))

os.environ["AWS_ACCESS_KEY_ID"]= config['AWS']['ACCESS_KEY']
os.environ["AWS_SECRET_ACCESS_KEY"]= config['AWS']['SECRET_KEY']


def create_spark_session():

"""
    Description: This function can be used to set up the spark connection string and set up
    the spark environment.

    Arguments:
    spark: the spark object.  

    Returns:
        spark
"""
    spark = SS \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):

    song_data = input_data+"song_data/*/*/*/*.json"

    df = spark.read.json(song_data)

    songs_table = df['song_id', 'title', 'artist_id', 'year', 'duration']
    songs_table.printSchema()
    songs_table.show()

    songs_table.write.partitionBy('year', 'artist_id').parquet(os.path.join(output_data, 'songs.par'), 'overwrite')

    artists_table = df['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']

    artists_table = artists_table.dropDuplicates(['artist_id'])

    artists_table.write.parquet(os.path.join(output_data, 'artists.par'), 'overwrite')

def process_log_data(spark, input_data, output_data):

    log_data = spark.read.json('Desktop/git/spark-etl/2018-11-01-events.json')

    df = log_data

    df = df[df['page'] == 'NextSong']

    users_table = df['userId','firstName','lastName','gender','level']

    users_table = users_table.dropDuplicates(['userId'])

    users_table.write.parquet(os.path.join(output_data, 'log.par'), 'overwrite')


    get_timestamp = udf(lambda x : datetime.datetime.fromtimestamp(x/1000.0).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn("timestamp", get_timestamp(df.ts))

    get_datetime = udf(lambda x : datetime.datetime.fromtimestamp(x/1000.0).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn("datetime", get_datetime(df.ts))

    get_month = udf(lambda x: datetime.datetime.fromtimestamp(x/1000.0).month)
    get_day = udf(lambda x: datetime.datetime.fromtimestamp(x/1000.0).day)
    get_year = udf(lambda x: datetime.datetime.fromtimestamp(x/1000.0).year)
    get_hour = udf(lambda x: datetime.datetime.fromtimestamp(x/1000.0).hour)
    get_week = udf(lambda x : datetime.datetime.fromtimestamp(x/1000.0).strftime('%w'))
    get_weekday = udf(lambda x : datetime.datetime.fromtimestamp(x/1000.0).strftime('%a'))

    df = df.withColumn('month',get_month(df.ts)).withColumn('day',get_day(df.ts)).withColumn('year',
                      get_year(df.ts)).withColumn('hour', get_hour(df.ts)).withColumn('%w',
                              get_week(df.ts)).withColumn('%a', get_weekday(df.ts))
    filtercols = ['month', 'day', 'year', 'hour','%w', '%a']
    time_table =  df.select(filtercols)

    time_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'time.par'), 'overwrite')


    song_df = spark.read.parquet('s3://output_data/songs.par')


    df_join = log_data.join(song_data, log_data.artist == song_data.artist_name)
    df = df_join
    df = df.withColumn("songplay_id", monotonically_increasing_id())


    songplays_table = df['songplay_id','ts','userId', 'level', 'song_id', 'artist_id', 'sessionId', 'location', 'userAgent']

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(os.path.join(output_data,"songplay"), 'overwrite')

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://output_data/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
