import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col as column
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, IntegerType, DateType, TimestampType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']

    """
        Constraints
    """
tsFormat = "yyyy-MM-dd HH:MM:ss z"
song_fields = ["title", "artist_id","year", "duration"]
artist_fields = ["artist_id", "artist_name", "artist_location", "artist_latitude","artist_longitude"]
user_normalized = ["userId as user_id", "firstName as first_name", "lastName as last_name", "gender", "level"]

def create_spark_session():
    """
        Create a Spark session, or retrieve an existing one
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
    """
        Description: Here is where song_data is loaded from S3 and gets processed by extracting songs and artist data, done this,
        it is loadwd back to S3.
        
        Arguments:
            spark -- pyspark
            input_data -- data files location
            output_data -- S3 bucket storing dimensional tables
    """
    song_data = input_data + 'song_data/*/*/*/*.json'
    songSchema = StructType([
        StructField("song_id",StringType()),
        StructField("artist_id",StringType()),
        StructField("artist_latitude",DoubleType()),
        StructField("artist_location",StringType()),
        StructField("artist_longitude",DoubleType()),
        StructField("artist_name",StringType()),
        StructField("duration",DoubleType()),
        StructField("num_songs",IntegerType()),
        StructField("title",StringType()),
        StructField("year",IntegerType()),])
    df = spark.read.json(song_data, schema=songSchema)
    """ 
        Select song table data from the needed columns
    """
    songs_table = df.select(song_fields)
    songs_table = songs_table.drop_duplicates()
    songs_table.write.partitionBy("year", "artist_id").parquet(output_data + 'songs/')
    """ 
        Select artist table data from the needed columns
    """
    artists_table = df.select(artist_fields)
    artists_table = artist_table.drop_duplicates()
    artists_table.write.parquet(output_data + 'artists/')
def process_log_data(spark, input_data, output_data):
    """
        Description: Here is where log_data is loaded from S3 and gets processed by extracting song an artist data, 
        done this, it is loaded back to S3.

        Arguments:
            spark -- pyspark
            input_data -- data files location
            output_data -- S3 bucket storing dimensional tables
    """
    log_data = input_data + 'log_data/*/*/*.json'
    df = spark.read.json(log_data)
    df = df.filter(df.page == 'NextSong')
    users_fields = user_normalized
    users_table = df.selectExpr(users_fields)
    users_table = users_table.drop_duplicates()
    users_table.write.parquet(output_data + 'users/')
    time_table = df.withColumn('ts',to_timestamp(date_format((df.ts/1000).cast(dataType=TimestampType()), tsFormat), tsFormat))
    time_table = time_table.select(column("ts").alias("start_time"),
                                   hour(column("ts")).alias("hour"),
                                   dayofmonth(column("ts")).alias("day"), 
                                   weekofyear(column("ts")).alias("week"), 
                                   month(column("ts")).alias("month"),
                                   year(column("ts")).alias("year"))
    time_table.write.partitionBy("year","month").parquet(output_data+"time")  
    df_songs = spark.read.parquet(output_data + 'songs/*/*/*')
    df_artists = spark.read.parquet(output_data + 'artists/*')
    songs_logs = df.join(songs_df, (df.song == songs_df.title))
    artists_songs_logs = songs_logs.join(df_artists, (songs_logs.artist == df_artists.name))
    songplays = artists_songs_logs.join(
        time_table,
        artists_songs_logs.ts == time_table.start_time, 'left'
    ).drop(artists_songs_logs.year)
    songplays_table = songplays.select(
        column('start_time').alias('start_time'),
        column('userId').alias('user_id'),
        column('level').alias('level'),
        column('song_id').alias('song_id'),
        column('artist_id').alias('artist_id'),
        column('sessionId').alias('session_id'),
        column('artist_location').alias('location'),
        column('userAgent').alias('user_agent'),
        column('year').alias('year'),
        column('month').alias('month'),
    ).repartition("year", "month")
    songplays_table.write.partitionBy("year", "month").parquet(output_data + 'songplays/')

def main():
    """
        Description:
            Song and events extraction from S3.
            Data transformation into dim tables format.
            Data load back to S3
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkify-dend/"
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)

if __name__ == "__main__":
    main()