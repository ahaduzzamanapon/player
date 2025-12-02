import json
from flask import Flask, render_template, Response, request
import requests
import urllib.parse
import sqlite3
import threading
import time

app = Flask(__name__)

DB_PATH = 'channels.db'
JSON_URL = 'https://raw.githubusercontent.com/hasanhabibmottakin/candy/main/rest_api.json'



def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS channels (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            category_name TEXT,
            name TEXT NOT NULL,
            logo TEXT,
            link TEXT NOT NULL,
            cookie TEXT,
            drmScheme TEXT,
            drmLicense TEXT
        )
    ''')
    conn.commit()
    conn.close()

def update_channels_from_url():
    try:
        r = requests.get(JSON_URL)
        r.raise_for_status()
        data = r.json()
        channels = data['response']

        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('DELETE FROM channels')

        for channel in channels:
            c.execute('''
                INSERT INTO channels (category_name, name, logo, link, cookie, drmScheme, drmLicense)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                channel.get('category_name'),
                channel.get('name'),
                channel.get('logo'),
                channel.get('link'),
                channel.get('cookie'),
                channel.get('drmScheme'),
                channel.get('drmLicense')
            ))
        
        conn.commit()
        conn.close()
        print("Channels updated successfully.")
    except Exception as e:
        print(f"Error updating channels: {e}")

def background_update():
    while True:
        update_channels_from_url()
        time.sleep(300) # 5 minutes

@app.route('/')
def index():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute('SELECT * FROM channels')
    channels = c.fetchall()
    conn.close()
    return render_template('index.html', channels=channels)

@app.route('/play/<name>')
def play(name):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute('SELECT * FROM channels WHERE name = ?', (name,))
    channel = c.fetchone()
    conn.close()
    if not channel:
        return "Channel not found", 404
    return render_template('player.html', channel=channel)

@app.route('/stream')
def stream():
    url = request.args.get('url')
    if not url:
        return "Missing URL parameter", 400

    channel_name = request.args.get('channel')
    if not channel_name:
        return "Missing channel parameter", 400

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute('SELECT * FROM channels WHERE name = ?', (channel_name,))
    channel = c.fetchone()
    conn.close()
    
    if not channel:
        return "Channel not found", 404

    cookie = channel['cookie']
    headers = {}
    if cookie:
        headers['Cookie'] = cookie

    try:
        if '.m3u8' in url:
            r = requests.get(url, headers=headers)
            r.raise_for_status()
            
            lines = r.text.splitlines()
            new_lines = []
            
            for line in lines:
                line = line.strip()
                if line.startswith('#EXT-X-KEY'):
                    uri_start = line.find('URI="') + 5
                    uri_end = line.find('"', uri_start)
                    key_uri = line[uri_start:uri_end]
                    
                    absolute_key_uri = urllib.parse.urljoin(url, key_uri)
                    
                    proxied_key_uri = f"/stream?url={urllib.parse.quote(absolute_key_uri)}&channel={urllib.parse.quote(channel_name)}"
                    new_line = line.replace(line[uri_start:uri_end], proxied_key_uri)
                    new_lines.append(new_line)
                elif line and not line.startswith('#'):
                    absolute_segment_url = urllib.parse.urljoin(url, line)
                    
                    proxied_segment = f"/stream?url={urllib.parse.quote(absolute_segment_url)}&channel={urllib.parse.quote(channel_name)}"
                    new_lines.append(proxied_segment)
                else:
                    new_lines.append(line)
            
            return Response('\n'.join(new_lines), mimetype='application/x-mpegURL')

        else: # .ts segments or other files
            req = requests.get(url, headers=headers, stream=True)
            req.raise_for_status()
            return Response(req.iter_content(chunk_size=1024), content_type=req.headers['content-type'])

    except requests.exceptions.RequestException as e:
        return str(e), 500
    except Exception as e:
        return str(e), 500

if __name__ == '__main__':
    init_db()
    update_thread = threading.Thread(target=background_update)
    update_thread.daemon = True
    update_thread.start()
    app.run(debug=True, port=8080)