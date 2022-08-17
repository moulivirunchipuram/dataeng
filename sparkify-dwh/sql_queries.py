import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events (
                                artist            VARCHAR(MAX),
                                auth              VARCHAR(MAX),
                                first_name        VARCHAR(MAX),
                                gender            VARCHAR(MAX),
                                item_in_session   INTEGER,
                                last_name         VARCHAR(MAX),
                                length            FLOAT,
                                level             VARCHAR(MAX),
                                location          VARCHAR(MAX),
                                method            VARCHAR(MAX),
                                page              VARCHAR(MAX),
                                registration      BIGINT,
                                session_id        INTEGER,
                                song              VARCHAR(MAX),
                                status            INTEGER,
                                ts                BIGINT,
                                user_agent        VARCHAR(MAX),
                                user_id           VARCHAR(MAX)       NOT NULL
                            );
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (
                                    song_id     VARCHAR(MAX),
                                    title       VARCHAR(MAX),
                                    duration    FLOAT4,
                                    year        INTEGER ,
                                    artist_id   VARCHAR(MAX),
                                    artist_name VARCHAR(MAX),
                                    artist_latitude     NUMERIC,
                                    artist_longitude    NUMERIC,
                                    artist_location     VARCHAR(MAX),
                                    num_songs   INTEGER
                            );
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (
                            songplay_id INT IDENTITY(0,1)   NOT NULL  PRIMARY KEY,
                            start_time  BIGINT   NOT NULL sortkey distkey,
                            user_id     VARCHAR(MAX)         NOT NULL,
                            level       VARCHAR(MAX),
                            song_id     VARCHAR(MAX)     NOT NULL,
                            artist_id   VARCHAR(MAX)     NOT NULL,
                            session_id  INTEGER         NOT NULL,
                            location    VARCHAR(MAX)     NOT NULL,
                            user_agent  VARCHAR(MAX)     NOT NULL
                        );
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (
                        user_id    VARCHAR(MAX)     NOT NULL PRIMARY KEY,
                        first_name VARCHAR(MAX) NOT NULL,
                        last_name  VARCHAR(MAX),
                        gender     VARCHAR(MAX), 
                        level      VARCHAR(MAX) NOT NULL
                    );
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (
                        song_id    VARCHAR(MAX) NOT NULL PRIMARY KEY,
                        title      VARCHAR(MAX) NOT NULL,
                        artist_id  VARCHAR(MAX) NOT NULL,
                        year       INTEGER,
                        duration   NUMERIC NOT NULL
                    );
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (
                        artist_id     VARCHAR(MAX) PRIMARY KEY,
                        name          VARCHAR(MAX) NOT NULL,
                        location      VARCHAR(MAX),
                        latitude      NUMERIC,
                        longitude     NUMERIC
                    );
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
                        start_time    TIMESTAMP PRIMARY KEY,
                        hour          INTEGER,
                        day           INTEGER,
                        week          INTEGER,
                        month         INTEGER, 
                        year          INTEGER, 
                        weekday       INTEGER
                    );
""")

# STAGING TABLES

staging_events_copy = ("""copy staging_events FROM 's3://udacity-dend/log_data'
                            credentials 'aws_iam_role={}'
                            format as json 's3://udacity-dend/log_json_path.json'
                            region 'us-west-2'
                        """).format(config.get('IAM_ROLE', 'ARN'))

staging_songs_copy = ("""copy staging_songs from 's3://udacity-dend/song_data'
                            credentials 'aws_iam_role={}'
                            format as json 'auto'
                            region 'us-west-2'
                        """).format(config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays
(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT e.ts, e.user_id, e.level, s.song_id, s.artist_id, e.session_id, e.location, e.user_agent
FROM staging_events e
JOIN staging_songs s
ON e.artist = s.artist_name
WHERE e.page = 'NextSong'
""")

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT(e.user_id), e.first_name, e.last_name, e.gender, e.level
FROM staging_events e 
WHERE e.page = 'NextSong'
""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT(s.song_id), s.title, s.artist_id, s.year, s.duration
FROM staging_songs s
""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT(s.artist_id), s.artist_name, s.artist_location, s.artist_latitude, s.artist_longitude
FROM staging_songs s
""")

time_table_insert = ("""INSERT INTO time(start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT ts, EXTRACT(HOUR FROM ts), EXTRACT(DAY FROM ts), EXTRACT(WEEK FROM ts), EXTRACT(MONTH FROM ts), EXTRACT(YEAR FROM ts), EXTRACT(WEEKDAY FROM ts)
FROM(
SELECT (TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 Second ') as ts
FROM staging_events WHERE page='NextSong')
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert, 
                        song_table_insert, artist_table_insert, time_table_insert]
