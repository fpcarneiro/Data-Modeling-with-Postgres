import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    This function processes a song file whose filepath has been provided as an arugment.
    It extracts the song information in order to store it into the songs table.
    Then it extracts the artist information in order to store it into the artists table.
    
    Args:
        cur (:obj:`cursor`): The cursor variable.
        filepath (str): The file path to the song file

    """
        
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df.loc[:,["song_id", "title", "artist_id", "year", "duration"]].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df.loc[:,["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    This function processes a log file whose filepath has been provided as an arugment.
    It extracts the time information in order to store it into the time table.
    Then it extracts the users information in order to store it into the users table.
    Finally it extracts the songs played by users in order to store it into the songplay table.
    
    Args:
        cur (:obj:`cursor`): The cursor variable.
        filepath (str): The file path to the log file

    """
        
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df.page == "NextSong"]

    # convert timestamp column to datetime
    df['ts'] = pd.to_datetime(df['ts'], unit='ms')
    t = df.ts
    
    # insert time data records
    time_data = []
    time_data.append(t)
    time_data.append(t.dt.hour.values.tolist())
    time_data.append(t.dt.day.values.tolist())
    time_data.append(t.dt.week.values.tolist())
    time_data.append(t.dt.month.values.tolist())
    time_data.append(t.dt.year.values.tolist())
    time_data.append(t.dt.weekday.values.tolist())
    column_labels = ["timestamp", "hour", "day", "week_of_year", "month", "year", "weekday"]
        
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df.loc[:, ["userId", "firstName", "lastName", "gender", "level"]]
    
    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    This a generic function that processes a set of files stored in the filepath provided as an arugment.
    It iterates over them and run the function "func" also provided as an argument to read the information from JSON files and store them in the appropriate tables.
    
    Args:
        cur (:obj:`cursor`): One object of the class cursor. 
            It allows the execution of PostgreSQL commands in a database session. 
            All the commands are executed in the context of the database session wrapped by the connection.
        conn (:obj:`connection`): One object of the class connection that encapsulates a database session. 
            It handles the connection to a PostgreSQL database instance.
        filepath (str): The path to the directory to be scanned by the function. 
        func (str): Name of the function to be used when processing the JSON files.

    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    The main function. It connects to the database and processes the files in order to populate the tables.
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()