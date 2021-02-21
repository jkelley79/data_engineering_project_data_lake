import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, hour, monotonically_increasing_id, year, month, dayofmonth, weekofyear, date_format
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, LongType, DateType, TimestampType

# Read in configuration information
config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS','AWS_SECRET_ACCESS_KEY')

def create_spark_session():
    """
    Creates the spark session 
    Uses a newer version of the hadoop-aws package and enables a LEGACY
    timeParserPolicy to extract week from datetime
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.9.0") \
        .config("spark.sql.legacy.timeParserPolicy","LEGACY") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Process the song data and write out artists and songs parquet files
    in the output path
    """
    # get filepath to song data file
    song_data = os.path.join(input_data, config.get('DATA','SONG_PATH'))
    
    # Build the schema to import data from json files
    song_schema = StructType([
        StructField("artist_id", StringType(), True),
        StructField("artist_latitude", FloatType(), True),
        StructField("artist_location", StringType(), True),
        StructField("artist_longitude", FloatType(), True),
        StructField("artist_name", StringType(), True),
        StructField("duration", FloatType(), True),
        StructField("num_songs", IntegerType(), True),
        StructField("song_id", StringType(), True),
        StructField("title", StringType(), True),
        StructField("year", IntegerType(), True)
    ])
      
    # read song data file
    df = spark.read.json(song_data, schema=song_schema)
    
    # extract columns to create songs table
    songs_table = df.select('song_id','artist_id','title','year','duration')
    
    # drop duplicates based on song_id and artist_id
    songs_table = songs_table.dropDuplicates(subset=["song_id","artist_id"])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year","artist_id").mode("overwrite").parquet("{}/songs.parquet".format(output_data))

    # extract columns to create artists table
    artists_table = df.select('artist_id','artist_name','artist_location','artist_latitude','artist_longitude', 'num_songs').orderBy('artist_id')
    
    # drop the duplicates based on artist_id
    artists_table = artists_table.dropDuplicates(subset=["artist_id"])

    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet("{}/artists.parquet".format(output_data))


def process_log_data(spark, input_data, output_data):
    """
    Process the log data and write out users, time and songplays tables as parquet files
    in the output path
    """
    # get filepath to log data file
    log_data = os.path.join(input_data, config.get('DATA','LOG_PATH'))

    # Build the schema to import data from json files
    log_schema = StructType([
      StructField("artist", StringType(), True),
      StructField("auth", StringType(), True),
      StructField("firstName", StringType(), True),
      StructField("gender", StringType(), True),
      StructField("itemInSession", IntegerType(), True),
      StructField("lastName", StringType(), True),
      StructField("level", StringType(), True),
      StructField("location", StringType(), True),
      StructField("method", StringType(), True),
      StructField("page", StringType(), True),
      StructField("registration", LongType(), True),
      StructField("sessionId", IntegerType(), True),
      StructField("song", StringType(), True),
      StructField("status", IntegerType(), True),
      StructField("ts", LongType(), True),
      StructField("userAgent", StringType(), True),
      StructField("userId", IntegerType(), True)
    ])
      
    # read log data file
    df = spark.read.json(log_data, schema=log_schema)
    
    # filter by actions for song plays
    df = df.where(df.page=='NextSong')

    # extract columns for users table    
    users_table = df.select('userId', 'firstName', 'lastName', 'gender', 'level')
    
    # drop duplicates from users_table
    users_table = users_table.dropDuplicates(subset=['userId'])

    # write users table to parquet files
    users_table.write.mode("overwrite").parquet("{}/users.parquet".format(output_data))
    
    # define udf to transform timestamp column from original epoch ms column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000), TimestampType())

    # select only the timestamp column and drop duplicate timestamps
    time_table = df.select('ts').dropDuplicates()
    
    # Extract additional date information into new columns from original ts
    time_table = time_table.withColumn('datetime', get_timestamp('ts'))
    time_table = time_table.withColumn('hour', hour('datetime'))
    time_table = time_table.withColumn("year", date_format(col("datetime"), "y"))
    time_table = time_table.withColumn("month", date_format(col("datetime"), "M"))
    time_table = time_table.withColumn("week", date_format(col("datetime"), "w"))
    time_table = time_table.withColumn("day", date_format(col("datetime"), "d"))
    time_table = time_table.withColumn("weekday", date_format(col("datetime"), "F"))

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year","month").mode("overwrite").parquet("{}/time.parquet".format(output_data))

    # read in song data to use for songplays table join
    song_df = spark.read.parquet("{}/songs.parquet".format(output_data))
    
    # join song play data with song data on song_id
    songplays_all_columns = df.join(song_df, df.song == song_df.song_id, 'left').select('*')
    
    # add an idx column that will serve as our unique_songplay_id
    songplays_all_columns = songplays_all_columns.withColumn("idx", monotonically_increasing_id())
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays = songplays_all_columns.select(col("idx").alias("songplay_id"), col("ts").alias("start_time"),col("userId").alias("user_id"), col("level"),col("song_id"),col("artist_id"),col("sessionId").alias("session_id"), col("location"), col("userAgent").alias("user_agent"))

    # join songplays with time to be able to partition date by year and month
    songplays_table = songplays.join(time_table, songplays.start_time == time_table.ts, 'inner').select('songplay_id','start_time','user_id','level','song_id','artist_id','session_id','location','user_agent','year','month')

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year","month").mode("overwrite").parquet("{}/songplays.parquet".format(output_data))


def main():
    """
    Main entry point to extract data from s3, transform it and write it back out to s3 in star schema 
    """
    spark = create_spark_session()
    input_data = config.get('DATA','INPUT_FOLDER')
    output_data = config.get('DATA','OUTPUT_FOLDER')
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
