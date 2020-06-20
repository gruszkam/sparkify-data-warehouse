import configparser


# CONFIG

config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS fact_songplays"
user_table_drop = "DROP TABLE IF EXISTS dim_users"
song_table_drop = "DROP TABLE IF EXISTS dim_songs"
artist_table_drop = "DROP TABLE IF EXISTS dim_artists"
time_table_drop = "DROP TABLE IF EXISTS dim_time"

# CREATE TABLES

staging_events_table_create= (""" 
    CREATE TABLE IF NOT EXISTS staging_events ( 
        artist VARCHAR,
        auth VARCHAR,
        firstName VARCHAR,
        gender VARCHAR,
        itemInSession INTEGER,
        lastName VARCHAR,
        length NUMERIC,
        level VARCHAR,
        location VARCHAR,
        method VARCHAR,
        page VARCHAR,
        registration NUMERIC,
        sessionId INTEGER,
        song VARCHAR,
        status INTEGER,
        ts NUMERIC,
        userAgent VARCHAR,
        userId VARCHAR
    );
""")

staging_songs_table_create = (""" 
    CREATE TABLE IF NOT EXISTS staging_songs ( 
        num_songs INTEGER,
        artist_id VARCHAR,
        artist_latitude NUMERIC,
        artist_longitude NUMERIC,
        artist_location VARCHAR,
        artist_name VARCHAR,
        song_id VARCHAR,
        title VARCHAR,
        duration NUMERIC,
        year INTEGER
    );
""")

songplay_table_create = (""" 
    CREATE TABLE IF NOT EXISTS fact_songplays (
        songplay_id INTEGER IDENTITY(0,1),
        start_time TIMESTAMP NOT NULL, 
        user_id VARCHAR NOT NULL,
        level VARCHAR,
        song_id VARCHAR NOT NULL, 
        artist_id VARCHAR NOT NULL, 
        session_id INTEGER, 
        location VARCHAR,
        user_agent VARCHAR
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS dim_users (
        user_id VARCHAR PRIMARY KEY,
        first_name VARCHAR NOT NULL,
        last_name VARCHAR NOT NULL,
        gender VARCHAR NOT NULL,
        level VARCHAR
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS dim_songs (
        song_id VARCHAR PRIMARY KEY,
        title VARCHAR,
        artist_id VARCHAR, 
        year INTEGER, 
        duration NUMERIC
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS dim_artists (
        artist_id VARCHAR PRIMARY KEY, 
        name VARCHAR NOT NULL,
        location VARCHAR, 
        latitude NUMERIC,
        longitude NUMERIC
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS dim_time (
        start_time TIMESTAMP PRIMARY KEY, 
        hour INTEGER,
        day INTEGER,
        week INTEGER,
        month INTEGER,
        year INTEGER,
        weekday INTEGER
    );
""")

# STAGING TABLES
staging_events_copy = ("""
    copy staging_events from {}
    iam_role {}
    region 'us-west-2'
    JSON {};
""").format(config.get('S3','LOG_DATA'), config.get('IAM_ROLE','ARN'), config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""
    copy staging_songs from {}
    iam_role {}
    region 'us-west-2'
    json 'auto' truncatecolumns;
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO fact_songplays (
        start_time,
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        user_agent
    )
    SELECT timestamp 'epoch' + events.ts/1000 * interval '1 second' AS start_time,
         events.userId AS user_id,
         events.level AS level,
         songs.song_id AS song_id,
         songs.artist_id AS artist_id,
         events.sessionId AS session_id,
         events.location AS location,
         events.userAgent AS user_agent
    FROM staging_events AS events
    JOIN staging_songs AS songs
        ON (events.artist = songs.artist_name)
        AND (events.song = songs.title)
        AND (events.length = songs.duration)
        WHERE events.page = 'NextSong'
""")

user_table_insert = ("""
    INSERT INTO dim_users (
        user_id, 
        first_name,
        last_name,
        gender,
        level
    )
    SELECT DISTINCT userId AS user_id,
        firstName AS first_name,
        lastName AS last_name,
        gender,
        level
    FROM staging_events
    WHERE page = 'NextSong'
""")

song_table_insert = ("""
    INSERT INTO dim_songs (
        song_id,
        title,
        artist_id,
        year,
        duration
    )
    SELECT DISTINCT song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_songs
""")

artist_table_insert = ("""
    INSERT INTO dim_artists (
        artist_id,
        name,
        location,
        latitude,
        longitude
    )
    SELECT DISTINCT artist_id,
        artist_name AS name,
        artist_location AS location,
        artist_latitude AS latitude,
        artist_longitude AS longitude
    FROM staging_songs
""")


time_table_insert = ("""
    INSERT INTO dim_time (
        start_time,
        hour,
        day,
        week,
        month,
        year,
        weekday
    )
    SELECT sp.start_time,
        EXTRACT (HOUR FROM sp.start_time), 
        EXTRACT (DAY FROM sp.start_time),
        EXTRACT (WEEK FROM sp.start_time), 
        EXTRACT (MONTH FROM sp.start_time),
        EXTRACT (YEAR FROM sp.start_time), 
        EXTRACT (WEEKDAY FROM sp.start_time) 
    FROM fact_songplays AS sp
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]