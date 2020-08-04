# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS times"

# CREATE TABLES

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (songplay_id serial, start_time time REFERENCES times(start_time) NOT NULL, user_id int REFERENCES users(user_id) NOT NULL, level text, song_id text REFERENCES songs(song_id), artist_id text REFERENCES artists(artist_id), session_id int, location text, user_agent text, PRIMARY KEY (songplay_id));
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (user_id int, first_name text NOT NULL, last_name text NOT NULL, gender text, level text NOT NULL, PRIMARY KEY (user_id));
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (song_id text, title text NOT NULL, artist_id text, year int, duration numeric NOT NULL, PRIMARY KEY (song_id));
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (artist_id text, name text NOT NULL, location text, latitude numeric, longitude numeric, PRIMARY KEY (artist_id));
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS times (start_time time, hour int, day int, week int, month int, year int, weekday int, PRIMARY KEY (start_time));
""")

# INSERT RECORDS

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
""")

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level)
                 VALUES (%s, %s, %s, %s, %s) ON CONFLICT ON CONSTRAINT users_pkey DO
                    UPDATE
                    SET level = EXCLUDED.level;
""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration)
                 VALUES (%s, %s, %s, %s, %s) ON CONFLICT ON CONSTRAINT songs_pkey DO NOTHING
""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude)
                 VALUES (%s, %s, %s, %s, %s) ON CONFLICT ON CONSTRAINT artists_pkey DO NOTHING
""")


time_table_insert = ("""INSERT INTO times (start_time, hour, day, week, month, year, weekday)
                 VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT ON CONSTRAINT times_pkey DO NOTHING
""")

# FIND SONGS

song_select = ("""SELECT songs.song_id, artists.artist_id
                    FROM songs
                    LEFT OUTER JOIN artists ON songs.artist_id = artists.artist_id
                    WHERE songs.title=%s and artists.name=%s and songs.duration=%s
""")

# QUERY LISTS

create_table_queries = [time_table_create, artist_table_create, song_table_create, user_table_create, songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
