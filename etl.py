import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
from sqlalchemy_schemadisplay import create_schema_graph
from sqlalchemy import MetaData



def process_song_file(cur, filepath):
    """
    This function extracts song information from json file and stores into songs table.
    This also extracts artist information and stores into artists table.

    INPUT PARAMETERS:
    * cur - the cursor varibale
    * filepath - the file path to the song file
    """
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id','title','artist_id','year','duration']].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)
    

def process_log_file(cur, filepath):
    """
    This function extracts various information from the input file.
    The extracted data is processed and stores in the following tables.
    * time
    * users
    * songplays

    INPUT PARAMETERS:
    * cur - the cursor varibale
    * filepath - the file path to the song file
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    time_data = [t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday]
    
    # map the column labels matching the columns in the table
    column_labels = ('start_time','hour','day','week','month','year','weekday')
    
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))
    
    # insert time data
    for i, row in time_df.iterrows():
        try:
            cur.execute(time_table_insert, list(row))         
        except psycopg2.Error as e: 
            print("Error: Inserting time data")
            print (e)
    

    # load user table
    user_df = df[['userId','firstName','lastName', 'gender', 'level']]

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
        songplay_data = (pd.to_datetime(row.ts, unit='ms'), row.userId, row.level, songid, artistid,row.sessionId,row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)
        


def process_data(cur, conn, filepath, func):
    """
    This function is invoked from the main method.
    It goes through all the files in the given file path.
    And invokes the appropriate functions. 
    This function only processes json files.
    
    INPUTS:
    * cur - the cursor varibale
    * conn - the database connection variable
    * filepath - relative path to the files that are being processed
    * func - function that will be applied on the files
    
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
    This is the driver method that establishes database connection
    The db is hosted in localhost and db name is sparkifydb
    This invokes process_data method for the following filepaths
    
    * data/song_data
    * data/log_data
    
    """
    
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)
    
    graph = create_schema_graph(metadata=MetaData('postgresql://student:student@127.0.0.1/sparkifydb'))
    graph.write_png('sparkifydb_erd.png')

    conn.close()


if __name__ == "__main__":
    main()