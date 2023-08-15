import json
import time

from flask import Flask, request, jsonify, abort
import sqlite3 as sql
from multiprocessing import Process, Value

import anitube
from utils import *

DATABASE_PATH = "./database.db"
app = Flask(__name__)
at = anitube.AniTube()


@app.route('/api/getPlaylist')
def playlist():
    result = {'player': {}}
    id_ = request.args.get('id', type=int)
    if id_ is None:
        abort(400, 'Missing required parameter: id')

    with sql.connect(DATABASE_PATH) as con:
        cur = con.cursor()
        cur.execute("SELECT json FROM playlist_list WHERE id=?", (id_,))
        data = cur.fetchone()
        if data is not None:
            result['player'] = json.loads(data[0])

    con.close()
    return jsonify(result)


@app.route('/api/search')
def search():
    result = []
    text = request.args.get('text', type=str)
    if text is None:
        abort(400, 'Missing required parameter: text')

    limit = request.args.get('limit', default=5, type=int)
    limit = max(1, min(limit, 100))
    filters = request.args.get('filter', default='id,name,url', type=str).split(',')
    if text.strip() != '':
        with sql.connect(DATABASE_PATH) as con:
            cur = con.cursor()
            cur.execute("PRAGMA table_info(anime_list)")
            columns = [column[1] for column in cur.fetchall()]
            avail_filters = [item for item in filters if item in columns]
            if len(avail_filters) == 0:
                avail_filters = ['id', 'name', 'url']

            cur.execute(f"SELECT {', '.join(avail_filters)} FROM anime_list WHERE name LIKE ? COLLATE NOCASE"
                        f" LIMIT ?", (f'%{text}%', limit))

            for i in cur.fetchall():
                last_index = len(result)
                result.append({})
                for f in range(len(avail_filters)):
                    result[last_index][avail_filters[f]] = i[f]
        con.close()
    return jsonify(result)


def record_loop(loop_on):
    while True:
        if loop_on.value:
            loop()


def loop():
    limit = 30
    log('Updating loop')
    anime_list = at.get_anime(limit=limit)
    if is_last_anime_changes(v=limit, anime_list=anime_list):
        log('Updating database!', LogLevel.WARN)
        for anime in anime_list:
            response = anitube._get_playlist(at.get_session(), anime.url).json
            status = f'404'
            if len(response) > 0:
                status = f'200'
            log(f'- {anime.name} / {status} / {response[:5]}...')
            add_anime(anime.name, anime.url, anime.description, anime.rating, anime.poster,
                      anime.year, anime.episodes, anime.categories, anime.translation, anime.voice_actors)
            add_playlist(anime.url, response)
            time.sleep(1)
    time.sleep(90 * 60)


def add_null_playlist():
    with sql.connect(DATABASE_PATH) as con:
        cursor = con.cursor()
        cursor.execute('SELECT * FROM playlist_list WHERE json = "{}"')
        for item in cursor.fetchall():
            anime_id = item[0]
            cursor.execute(f'SELECT * FROM anime_list WHERE id = "{anime_id}"')
            anime = cursor.fetchone()
            url = anime[2]
            json_obj = anitube._get_playlist(at.get_session(), url).json
            add_playlist(url, json_obj)
            time.sleep(1)
    con.close()


def is_last_anime_changes(v=5, anime_list=at.get_anime(limit=5)):
    with sql.connect(DATABASE_PATH) as con:
        con.create_collation('time_sort', time_sort)
        cursor = con.cursor()
        select_query = f"""
            SELECT * FROM anime_list
            ORDER BY update_time COLLATE time_sort
            LIMIT {v}
            """
        cursor.execute(select_query)
        rows = cursor.fetchall()

        local_m = [e[1] for e in rows][::-1]
        hash_local = to_hash(local_m)

        global_m = [e.name for e in anime_list]
        hash_global = to_hash(global_m)
    con.close()
    return hash_local != hash_global


def add_playlist(url, structure):
    with sql.connect(DATABASE_PATH) as con:
        cursor = con.cursor()

        data = (url,)
        cursor.execute("""
        SELECT id FROM anime_list
        WHERE url = ?
        """, data)
        row = cursor.fetchone()
        row_id = row[0]

        cursor.execute("""
        SELECT * FROM playlist_list
        WHERE id = ?
        """, (row_id,))
        row = cursor.fetchone()

        if row is None:
            insert_query = """
            INSERT INTO playlist_list (id, json)
            VALUES (?, ?)
            """
            data = (row_id, json.dumps(structure))
            cursor.execute(insert_query, data)
        elif len(structure) > 0:
            update_query = """
            UPDATE playlist_list
            SET json = ?
            WHERE id = ?
            """
            data = (json.dumps(structure), row_id)
            cursor.execute(update_query, data)
        con.commit()
    con.close()


def add_anime(name, url, description, rating, poster, year, episodes, categories, translation, voice_actors):
    with sql.connect(DATABASE_PATH) as con:
        now = datetime.now()
        formatted_datetime = now.strftime('%Y-%m-%d %H:%M:%S.%f')

        cursor = con.cursor()
        cursor.execute("SELECT * FROM anime_list WHERE url=?", (url,))
        if cursor.fetchone() is None:
            insert_query = """
            INSERT INTO anime_list (name, url, description, rating, poster, year, episodes, categories, translation, voice_actors, update_time)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            data = (
                name,
                url,
                description,
                json.dumps(rating),
                poster,
                json.dumps(year),
                json.dumps(episodes),
                json.dumps(categories),
                json.dumps(translation),
                json.dumps(voice_actors),
                formatted_datetime)
            cursor.execute(insert_query, data)
        else:
            update_query = """
            UPDATE anime_list
            SET update_time = ?
            WHERE url = ?
            """
            data = (formatted_datetime, url)
            cursor.execute(update_query, data)

            cursor.execute("""
                SELECT * FROM anime_list
                WHERE url = ?
                """, (url,))
        con.commit()
    con.close()


def database_init():
    with sql.connect(DATABASE_PATH) as con:
        cursor = con.cursor()
        create_anime_table_query = """
        CREATE TABLE IF NOT EXISTS anime_list (
            id INTEGER PRIMARY KEY,
            name TEXT,
            url TEXT,
            description TEXT,
            rating TEXT,
            poster TEXT,
            year TEXT,
            episodes TEXT,
            categories TEXT,
            translation TEXT,
            voice_actors TEXT,
            update_time TEXT
        );
        """
        create_playlist_table_query = """
        CREATE TABLE IF NOT EXISTS playlist_list (
            id INTEGER,
            json TEXT
        );
        """
        cursor.execute(create_anime_table_query)
        cursor.execute(create_playlist_table_query)
        con.commit()
    con.close()


if __name__ == '__main__':
    log('Database initialisation', LogLevel.WARN)
    database_init()
    log('Starting loop', LogLevel.WARN)
    recording_on = Value('b', True)
    p = Process(target=record_loop, args=(recording_on,))
    p.start()
    log('Starting flask', LogLevel.WARN)
    app.run(debug=False, use_reloader=False)
    p.join()
