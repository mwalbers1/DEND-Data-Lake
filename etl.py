import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql import types as T
from pyspark.sql import functions as F


config = configparser.ConfigParser()
config.read('dl.cfg', encoding='utf-8-sig')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_KEYS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_KEYS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Description: Create or get pyspark.sql.SparkSession object
    
    Args: None
    
    Returns: spark session object
    """

    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
        .config("spark.dynamicAllocation.enabled", "false") \
        .getOrCreate()
    return spark


def create_users_table(df_events):
    """
    Description: Process AWS S3 input json files
        1. exract user data columns
        2. transform user data by removing duplicate users from log event json data files
        3. load to new users_table pySpark dataframe 
    
    Args: 
        df_events: log events table
    
    Returns: users_table pySpark dataframe 
    """
    
    df_users = df_events.select('userId','firstName','lastName','gender', 'level').dropDuplicates()
    
    # create new user_id column of type int
    userid_int = F.udf(lambda x: int(x.strip()), T.IntegerType())
    df_users = df_users.withColumn('user_id', userid_int(df_users.userId))
    
    # drop userId which is no longer need
    users_table = df_users.drop('userId')
    
    return users_table


def create_time_table(df_time):
    """
    Description: Process AWS S3 input json files
        1. exract timestamp data columns
        2. transform timestamp to date/time columns
        3. load to new time_table pySpark dataframe 
    
    Args: 
        df_time: dataframe with events and time columns
    
    Returns: time_table spark dataframe
    """  
    
    # create time dimension table
    time_table = df_time.select(F.col('ts').alias('start_time'), \
                        F.hour('date').alias('hour'), \
                        F.dayofmonth('date').alias('day'), \
                        F.weekofyear('date').alias('week'), \
                        F.month('date').alias('month'), \
                        F.year('date').alias('year'), \
                        F.date_format('date','E').alias('weekday'))
    
    return time_table
   

def create_songplays_dataframe(spark, df_log_data):
    """
    Description: Extract songplays columns from log dataframe
    
    Args:
        spark: spark session
        df_log_data: dataframe containing log data events
        
    Returns:
        dataframe containing log event columns
        
    """
    
    df_log_data = df_log_data.withColumn('songplay_id', F.monotonically_increasing_id())
    
    userid_int = F.udf(lambda x: int(x.strip()), T.IntegerType())
    df_log_data = df_log_data.withColumn('user_id', userid_int(df_log_data.userId))
    
    df_log_data = df_log_data.select('songplay_id', F.col('ts').alias('start_time'), 'user_id', 'level', \
                                     'length', 'song', 'artist', F.col('sessionId').alias('session_id'), \
                                     'location', F.col('userAgent').alias('user_agent'))
    
    return df_log_data


def process_song_data(spark, input_data, output_data):
    """
    Description: process songs and artists data from song json input files
        1 - create songs dataframe and write to parquet file
        2 - create artists dataframe and write to parquet file
    
    Args: 
        spark: spark session
        input_data: path location to AWS S3 input data bucket/folder
        output_data: path location to AWS S3 output data bucket/folder
    
    Returns: None
    """

    # get filepath to song data file
    song_data = f"{input_data}song_data/*/*/*/*.json"
    
    # read song data file 
    song_data_struct_type = T.StructType([
            T.StructField("song_id", T.StringType()),
            T.StructField("title", T.StringType()),
            T.StructField("year", T.IntegerType()),
            T.StructField("duration", T.DecimalType(10,5)),
            T.StructField("artist_id", T.StringType()),
            T.StructField("artist_name", T.StringType()),
            T.StructField("artist_location", T.StringType()),
            T.StructField("artist_lattitude", T.DecimalType(10,5)),
            T.StructField("artist_longitude", T.DecimalType(10,5))])
    
    df_song_data = spark.read.json(song_data, schema=song_data_struct_type)
    
    # extract columns to create songs table
    songs_table = df_song_data.select('song_id', 'title', 'artist_id', 'year', 'duration') \
                    .filter("song_id != ''") \
                    .dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id') \
                    .parquet(os.path.join(output_data, 'songs/songs.parquet'), 'overwrite')


    # extract columns to create artists table
    artists_table = df_song_data.select(F.col('artist_id'), \
                                        F.col('artist_name').alias('name'), \
                                        F.col('artist_location').alias('location'), \
                                        F.col('artist_lattitude').alias('lattitude'), \
                                        F.col('artist_longitude').alias('longitude')) \
                                  .filter("artist_id != ''").dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists/artists.parquet'), 'overwrite')


def process_log_data(spark, input_data, output_data):
    """
    Description: process song play event log data json files from input_data path location
        1 - create users dataframe table and write out to a parquet file
        2 - create time dataframe table and write out to a parquet file
        3 - create songplays dataframe table and write out to a parquet file
    
    Args: 
        spark: spark session object
        input_data: path location to AWS S3 input data bucket/folder
        output_data: path location to AWS S3 output data bucket/folder
    
    Returns: None
    """

    # get filepath to log data file
    log_data = f"{input_data}log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter("page=='NextSong'")

    # extract columns for users table    
    users_table = create_users_table(df)
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users/users.parquet'), 'overwrite')

    # create timestamp column from original timestamp column 
    get_timestamp = F.udf(lambda x: int(int(x)/1000))
    df = df.withColumn('timestamp', get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = F.udf(lambda x: datetime.fromtimestamp(x).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn('date', get_datetime(df.timestamp))  
    
    # extract columns to create time table
    time_table = create_time_table(df)
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month') \
                .parquet(os.path.join(output_data, 'time/time.parquet'), 'overwrite')


    # read in song data to use for songplays table
    song_df = spark.read.format("parquet") \
                    .option("basePath", os.path.join(output_data, "songs/")) \
                    .load(os.path.join(output_data, "songs/*/*/*/"))
    
    artists_df = spark.read.format("parquet") \
                .option("basepath", os.path.join(output_data, "artists/")) \
                .load(os.path.join(output_data, "artists/*/"))
    
    # call function to create logged dataset
    df_log_data = create_songplays_dataframe(spark, df)
    
    # create temp tables for songplays sql query
    df_log_data.createOrReplaceTempView("songplay")
    time_table.createOrReplaceTempView("time")
    song_df.createOrReplaceTempView("songs")
    artists_df.createOrReplaceTempView("artists")
       
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
        SELECT songplay.songplay_id, 
            songplay.start_time,  
            songplay.user_id, 
            songplay.level, 
            songplay.length, 
            songplay.session_id, 
            songplay.location, 
            songplay.user_agent, 
            songs.artist_id, 
            songs.song_id, 
            time.year, time.month 
        FROM songplay 
        JOIN time ON songplay.start_time = time.start_time
        LEFT JOIN songs on songplay.song = songs.title
            AND songplay.length = songs.duration
        LEFT JOIN artists ON songplay.artist = artists.name
        AND songs.artist_id = artists.artist_id
    """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month') \
                .parquet(os.path.join(output_data, 'songplays/songplays.parquet'), 'overwrite')
    

def main():
    """
    Description: main method called from the terminal spark-submit command 
        1 - create spark session object
        2 - extract songs data from S3 source bucket
        3 - extract song-play log files from S3 source bucket
        4 - transform songs data into new dataframe tables and write out to parquet files
        5 - transform song-play log data into new dataframe tables and write out to paquet files
    
    Args: None    
    
    Returns: None
    """

    spark = create_spark_session()
    input_data = "s3n://udacity-dend/"  
    output_data = "s3n://spark-dev2/spark-project/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
