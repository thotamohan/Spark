import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['CONF']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['CONF']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = os.path.join(input_data,"song_data/*/*/*/*.json")
    #song_data=input_data + 'song_data/*/*/*/*.json'
    #song_data = input_data + "song_data/*/*/*/*"
    
    # read song data file
    #df = spark.read.json(song_data, mode='PERMISSIVE', columnNameOfCorruptRecord='corrupt_record').drop_duplicates()
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration').drop_duplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy("artist_id", "year").parquet(output_data+'songs_table/')

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_lattitude', 'artist_longitude').drop_duplicates()
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data+'artists_table/')

def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page=='Next Song')

    # extract columns for users table    
    users_table = df.select('userId', 'firstName', 'lastName', 'gender', 'level').drop_duplicates()
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data+'users_table/')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x : datetime.utcfromtimestamp(int(x)/1000), TimestampType())
    df = df.withColumn('start_time', get_timestamp('ts'))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(int(int(x)/1000)), TimestampType())
    df = df.withColumn('date_time',get_datetime('ts'))
    
    # extract columns to create time table
    df=df.withColumn('hour',hour('start_time'))
    df=df.withColumn('day',dayofmonth('start_time'))
    df=df.withColumn('week',weekofyear('start_time'))
    df=df.withColumn('month',month('start_time'))
    df=df.withColumn('year',year('start_time'))
    df=df.withColumn('weekday',dayofweek('start_time'))
    time_table = df.select('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday').drop_duplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy("year","month").parquet(output_data+'time_table/')

    # read in song data to use for songplays table
    df_song=spark.read.parquet(output_data+'songs_table/')

    # extract columns from joined song and log datasets to create songplays table 
    [songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent]
    songplays_table = df.join(df_song,df_song.title==df.song,how='inner')
    songplays_table=songplays_table.select(monotonically_increasing_id().alias('songplay_id'),\
                                           col('start_time'),
                                           col('userId').alias('user_id'),
                                           'level',
                                           'song_id',
                                           'artist_id',
                                           col('sessionId').alias('session_id'),
                                           'location',
                                           col('userAgent').alias('user_agent'))
    

    # write songplays table to parquet files partitioned by year and month
    songplays_table=songplays_table.join(time_table, songplays_table.start_time == time_table.start_time, how="inner")\
    .select("songplay_id", songplays_table.start_time, "user_id", "level", "song_id", "artist_id", "session_id", "location", "user_agent", "year", "month").drop_duplicates()

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data+'songplays_table/')


def main():
    spark = create_spark_session()
    input_data = "s3://udacity-spark-project/"
    output_data = "s3://udacity-spark-project/output/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()



def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3://udacity-spark-project/output22/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
