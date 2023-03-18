import datetime
from pprint import pprint

import sqlite3

import numpy as np
import pandas as pd
import os
import pwd

pd.set_option('display.max_columns', None)

OBSIDIAN_PATH = "/Users/mike/Library/Mobile Documents/iCloud~md~obsidian/Documents/ObsiVault/Books"


def get_username():
    return pwd.getpwuid(os.getuid())[0]


def get_annotations(path):
    # Create your connection.
    conn = sqlite3.connect(path)
    cols = ['ZANNOTATIONSTYLE', 'ZANNOTATIONTYPE', 'ZANNOTATIONCREATIONDATE', 'ZANNOTATIONASSETID',
            'ZANNOTATIONSELECTEDTEXT', 'ZANNOTATIONUUID'
            ]
    df = pd.read_sql_query(f"SELECT {','.join(cols)} FROM ZAEANNOTATION", conn)
    df = df.rename(columns={
        'ZANNOTATIONSTYLE': 'style',
        'ZANNOTATIONTYPE': 'type',
        'ZANNOTATIONCREATIONDATE': 'timestamp',
        'ZANNOTATIONASSETID': 'book_id',
        'ZANNOTATIONSELECTEDTEXT': 'text',
        'ZANNOTATIONUUID': 'id',
    })
    df['timestamp'] = df['timestamp'].apply(
        lambda x: datetime.datetime.fromtimestamp(x + 978307200) if not np.isnan(x) else None)
    df = df[df['text'].notna()]
    return df


def get_books(path):
    # Create your connection.
    conn = sqlite3.connect(path)
    cols = ['ZASSETID', 'ZAUTHOR', 'ZPATH',
            'ZTITLE']
    df = pd.read_sql_query(f"SELECT {','.join(cols)} FROM ZBKLIBRARYASSET", conn)
    df = df.rename(columns={
        'ZASSETID': 'book_id',
        'ZAUTHOR': 'book_author',
        'ZPATH': 'book_path',
        'ZTITLE': 'book_title',
    })
    return df


def get_collections(path):
    # Create your connection.
    conn = sqlite3.connect(path)
    cols = ['ZASSETID', 'ZCOLLECTION']
    df = pd.read_sql_query(f"SELECT {','.join(cols)} FROM ZBKCOLLECTIONMEMBER", conn)
    books = df.rename(columns={
        'ZASSETID': 'book_id',
        'ZCOLLECTION': 'collection_id',
    })
    cols = ['Z_PK', 'ZTITLE']
    df = pd.read_sql_query(f"SELECT {','.join(cols)} FROM ZBKCOLLECTION", conn)
    collections = df.rename(columns={
        'Z_PK': 'collection_id',
        'ZTITLE': 'collection_name',
    })

    df = books.merge(collections, on='collection_id', how='left')
    df = df.loc[~df['collection_name'].isin(['Downloaded',
                                             'PDFs',
                                             'Books',
                                             'Want to Read',
                                             'My Samples',
                                             'Library',
                                             'Audiobooks',
                                             'Finished'])]
    return df


def find_paths():
    def find_sqlite(path):
        for root, dirs, files in os.walk(path):
            for file in files:
                if file.endswith(".sqlite"):
                    return os.path.join(root, file)

    user = get_username()
    annotation_db = find_sqlite(f"/Users/{user}/Library/Containers/com.apple.iBooksX/Data/Documents/AEAnnotation/")
    library_db = find_sqlite(f"/Users/{user}/Library/Containers/com.apple.iBooksX/Data/Documents/BKLibrary/")
    if not annotation_db and not library_db:
        raise FileNotFoundError('Libraries not found')
    return annotation_db, library_db


def get_db():
    annotation_path, library_path = find_paths()
    annotation_db = get_annotations(annotation_path)
    library_db = get_books(library_path)
    collection_db = get_collections(library_path)
    library_db = library_db.merge(collection_db, on='book_id', how='left')
    return annotation_db.merge(library_db, on='book_id', how='left')


def generate_template(entry):
    return f"""\n\n**{entry['timestamp'].strftime("%m-%d-%Y, %H:%M:%S")}**\n\n{entry['text']}\n\n"""


def generate_book_entry(title, author, entries):
    template = f"""#### by {author}"""
    for entry in entries.to_dict('records'):
        template += generate_template(entry)
    return template


def dump_files(markdowns):
    for file_path, markdown in markdowns:
        with open(file_path, 'w') as f:
            f.write(markdown)


def run():
    if not os.path.isdir(f"{OBSIDIAN_PATH}"):
        os.mkdir(f"{OBSIDIAN_PATH}")
    markdowns = []
    db = get_db()
    collections = db.groupby('collection_name')
    for collection_name, collection in collections:
        if not os.path.isdir(f"{OBSIDIAN_PATH}/{collection_name}") and len(collection) > 0:
            os.mkdir(f"{OBSIDIAN_PATH}/{collection_name}")
        books = collection.groupby('book_title')
        for title, entries in books:
            entries = entries.sort_values(by='timestamp')
            markdown = generate_book_entry(title, entries['book_author'].iloc[0], entries)
            markdowns.append(([f"{OBSIDIAN_PATH}/{collection_name}/{title}.md", markdown]))

    dump_files(markdowns)
    db['sync_status'] = 'synced'
    db.to_csv(f"{OBSIDIAN_PATH}/annotation_db.csv")


run()
