import configparser
import datetime
import os
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import monotonically_increasing_id

'''
Read AWS Credentials from dl.cfg
'''

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']

'''
Create a spark session on AWS hadoop and return the session
'''
def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


'''
Input Parameters: spark session, song_input_data json, and output_data S3/local path
What does it do: Process song_input data using spark and output as parquet to S3/local directory structure
'''


def process_song_data(spark, song_input_data, output_data):
    # get filepath to song data file
    print('Read song data from json file')
    song_data = spark.read.json(song_input_data)

    # read song data file
    print('Print song data schema')
    song_df = song_data
    print(song_df.count())
    song_df.printSchema()

    # extract columns to create songs table
    print('Extract columns to create song table')

    songs_table = song_df.select("song_id", "title", "artist_id", "year", "duration")
    print(songs_table.limit(5).toPandas())

    # create temp view
    song_df.createOrReplaceTempView("song_df_table")

    # create sql query to partition on year and artist_id
    songs_table = spark.sql(
        """SELECT DISTINCT song_id, title, artist_id, year, duration
        FROM song_df_table 
        GROUP BY year, artist_id
        """)

    # write songs table to parquet files partitioned by year and artist
    print('Writing to parquet')
    df_songs_table = songs_table.toPandas()

    print('Writing to parquet')
    # write songs table partitioned by year and artist_id to parquet
    songs_table.write.partitionBy('year', 'artist_id').parquet(os.path.join(output_data, 'songs/songs.parquet'),
                                                               'overwrite')
    print("done songs_table write")

    print('Artist table: ')
    artists_table = song_df.select("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude")
    print(artists_table.limit(5).toPandas())

    print('Writing artist table to parquet')
    artists_table.write.parquet(os.path.join(output_data,'artist_table/artist_table.parquet'),'overwrite')


'''Return none
Input Parameters: spark session, log_input_data json, song_input_data json, and output_data S3 path
What does it do: Processes log_input_data data using spark and output as parquet to S3/local directory structure
'''

def process_log_data(spark, log_input_data, song_input_data, output_data):
    # get filepath to log data file
    log_data = spark.read.json(log_input_data)


    # read log data file
    print('Print LOG data schema')
    log_df = log_data
    print(log_df.count())
    log_df.printSchema()
    print(log_df.limit(5).toPandas())

    # extract columns for users table

    print('Users table: ')
    users_table = log_df.select("firstName", "lastName", "gender", "level", "userId")
    print(users_table.limit(5).toPandas())

    # write users table to parquet files
    print('Writing user_table to parquet')
    users_table.write.parquet(os.path.join(output_data, '/user_table/users_table.parquet'), 'overwrite')


    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.datetime.fromtimestamp(x / 1000), TimestampType())
    log_df = log_df.withColumn("timestamp", get_timestamp(log_df.ts))
    log_df.printSchema()

    # create datetime column from original timestamp column
    log_df = log_df.withColumn("start_time", get_timestamp(log_df.ts))
    log_df.printSchema()
    log_df.head(1)

    # extract columns to create time table
    log_df = log_df.withColumn("hour", F.hour("timestamp"))
    log_df = log_df.withColumn("day", F.dayofweek("timestamp"))
    log_df = log_df.withColumn("week", F.weekofyear("timestamp"))
    log_df = log_df.withColumn("month", F.month("timestamp"))
    log_df = log_df.withColumn("year", F.year("timestamp"))
    log_df = log_df.withColumn("weekday", F.dayofweek("timestamp"))

    time_table = log_df.select("start_time", "hour", "day", "week", "month", "year", "weekday")

    # create temp view
    time_table.createOrReplaceTempView("time_table")

    # create sql query to partition on year and month
    time_table = spark.sql(
        """SELECT DISTINCT start_time, hour, day, week, month, year, weekday
        FROM time_table 
        ORDER BY year, month
        """)
    # write time table to parquet files partitioned by year and month
    print('Writing time_table to parquet')
    df_time_table = time_table.toPandas()

    time_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'timetable/time_table.parquet'),
                                                          'overwrite')
    # read in song data to use for songplays table
    song_df = spark.read.json(song_input_data)

    # create temp tables
    log_df.createOrReplaceTempView("log_df_table")
    song_df.createOrReplaceTempView("song_df_table")

    songplays_table = spark.sql(
        """SELECT DISTINCT log_df_table.start_time, log_df_table.userId, log_df_table.level, log_df_table.sessionId, log_df_table.location,log_df_table.userAgent, song_df_table.song_id, song_df_table.artist_id
        FROM log_df_table 
        INNER JOIN song_df_table 
        ON song_df_table.artist_name = log_df_table.artist 
        INNER JOIN time_table
        ON time_table.start_time = log_df_table.start_time
        GROUP BY time_table.year, time_table.month
        """)
    songplays_table = songplays_table.withColumn("songplay_id", monotonically_increasing_id())

    print('Writing songplays to parquet')
    df_songplays_table = songplays_table.toPandas()

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year','month').parquet(os.path.join(output_data, 'songplays/songplays_table.parquet'),
                                                               'overwrite')

def main():
    # Create spark session
    spark = create_spark_session()

    # Specify data
    #input_data = "s3a://udacity-dend/" 
    song_input_data = "data/songdata/song_data/A/A/A/*.json"
    log_input_data = "data/log-data/*.json"
    output_data = "data/outputdata/"

    # Call process functions
    process_song_data(spark, song_input_data, output_data)
    process_log_data(spark, log_input_data, song_input_data, output_data)


if __name__ == "__main__":
    main()
