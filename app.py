import json
from flask import Flask, render_template, Response, request
import requests
import urllib.parse
import sqlite3
import threading
import time
import os
import traceback

app = Flask(__name__)

DB_PATH = 'channels.db'
JSON_FEEDS = [
    'https://raw.githubusercontent.com/hasanhabibmottakin/candy/main/rest_api.json',
    'https://raw.githubusercontent.com/hasanhabibmottakin/Z5/main/data.json'
]

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('DROP TABLE IF EXISTS channels')
    c.execute('''
        CREATE TABLE channels (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            category_name TEXT,
            name TEXT NOT NULL,
            logo TEXT,
            link TEXT NOT NULL,
            cookie TEXT,
            drmScheme TEXT,
            drmLicense TEXT,
            server_name TEXT,
            user_agent TEXT
        )
    ''')
    conn.commit()
    conn.close()

def update_channels_from_url(url, server_name, existing_links):
    try:
        r = requests.get(url)
        r.raise_for_status()
        data = r.json()
        
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()

        if 'response' in data: # First JSON structure
            channels = data['response']
            for channel in channels:
                name = channel.get('name')
                link = channel.get('link')
                cookie = channel.get('cookie')
                headers = {}
                if cookie:
                    headers['Cookie'] = cookie

                if not name or not link:
                    continue

                if link in existing_links:
                    continue
                
                # Validate link
                try:
                    head_request = requests.head(link, headers=headers, timeout=5)
                    head_request.raise_for_status()
                except requests.exceptions.RequestException as e:
                    print(f"Skipping channel '{name}' due to invalid link: {link} - Error: {e}")
                    continue

                existing_links.add(link)

                c.execute('''
                    INSERT INTO channels (category_name, name, logo, link, cookie, drmScheme, drmLicense, server_name, user_agent)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    channel.get('category_name'),
                    name,
                    channel.get('logo'),
                    link,
                    cookie,
                    channel.get('drmScheme'),
                    channel.get('drmLicense'),
                    server_name,
                    None
                ))
        else: # Second JSON structure
            for channel in data:
                name = channel.get('name')
                link = channel.get('source', {}).get('url')
                user_agent = channel.get('source', {}).get('headers', {}).get('User-Agent')
                headers = {}
                if user_agent:
                    headers['User-Agent'] = user_agent

                if not name or not link:
                    continue

                if link in existing_links:
                    continue
                
                # Validate link
                try:
                    head_request = requests.head(link, headers=headers, timeout=5)
                    head_request.raise_for_status()
                except requests.exceptions.RequestException as e:
                    print(f"Skipping channel '{name}' due to invalid link: {link} - Error: {e}")
                    continue

                existing_links.add(link)
                
                c.execute('''
                    INSERT INTO channels (category_name, name, logo, link, cookie, drmScheme, drmLicense, server_name, user_agent)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    channel.get('group'),
                    name,
                    channel.get('logo'),
                    link,
                    None,
                    None,
                    None,
                    server_name,
                    user_agent
                ))
        
        conn.commit()
        conn.close()
        print(f"Channels from {url} updated successfully.")
    except Exception as e:
        print(f"Error updating channels from {url}: {e}")

def background_update():
    while True:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('DELETE FROM channels')
        conn.commit()
        conn.close()

        existing_links = set()
        for i, url in enumerate(JSON_FEEDS):
            update_channels_from_url(url, f"Server {i+1}", existing_links)
        time.sleep(300) # 5 minutes

@app.route('/favicon.ico')
def favicon():
    return '', 204

@app.route('/')
def index():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute('SELECT *, COUNT(name) as server_count FROM channels GROUP BY name')
    channels = c.fetchall()
    conn.close()
    return render_template('index.html', channels=channels)

@app.route('/play/<int:channel_id>')
def play(channel_id):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    
    # Get the current server
    c.execute('SELECT * FROM channels WHERE id = ?', (channel_id,))
    current_server = c.fetchone()
    
    if not current_server:
        return "Channel not found", 404
        
    # Get all servers for the same channel name
    c.execute('SELECT * FROM channels WHERE name = ?', (current_server['name'],))
    servers = c.fetchall()
    conn.close()
    
    return render_template('player.html', channel=current_server, servers=servers)

@app.route('/stream')
def stream():
    url = request.args.get('url')
    if not url:
        return "Missing URL parameter", 400

    channel_name = request.args.get('channel')
    if not channel_name:
        return "Missing channel parameter", 400

    server_id = request.args.get('server')

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    if server_id:
        c.execute('SELECT * FROM channels WHERE id = ?', (server_id,))
    else:
        c.execute('SELECT * FROM channels WHERE name = ?', (channel_name,))
    channel = c.fetchone()
    conn.close()
    
    if not channel:
        return "Channel not found", 404

    cookie = channel['cookie']
    user_agent = channel['user_agent']
    headers = {}
    if cookie:
        headers['Cookie'] = cookie
    if user_agent:
        headers['User-Agent'] = user_agent

    try:
        if '.m3u8' in url:
            try:
                r = requests.get(url, headers=headers)
                r.raise_for_status()
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 403:
                    return "Forbidden: The stream link may be expired or protected.", 403
                else:
                    print(f"Error fetching M3U8 from {url}: {e}")
                    traceback.print_exc()
                    return str(e), 500
            except requests.exceptions.RequestException as e:
                print(f"Error fetching M3U8 from {url}: {e}")
                traceback.print_exc()
                return str(e), 500

            lines = r.text.splitlines()
            new_lines = []
            
            for line in lines:
                line = line.strip()
                if line.startswith('#EXT-X-KEY'):
                    uri_start = line.find('URI="') + 5
                    uri_end = line.find('"', uri_start)
                    key_uri = line[uri_start:uri_end]
                    
                    absolute_key_uri = urllib.parse.urljoin(url, key_uri)
                    
                    proxied_key_uri = f"/stream?url={urllib.parse.quote(absolute_key_uri)}&channel={urllib.parse.quote(channel_name)}&server={channel['id']}"
                    new_line = line.replace(line[uri_start:uri_end], proxied_key_uri)
                    new_lines.append(new_line)
                elif line and not line.startswith('#'):
                    absolute_segment_url = urllib.parse.urljoin(url, line)
                    
                    proxied_segment = f"/stream?url={urllib.parse.quote(absolute_segment_url)}&channel={urllib.parse.quote(channel_name)}&server={channel['id']}"
                    new_lines.append(proxied_segment)
                else:
                    new_lines.append(line)
            
            return Response('\n'.join(new_lines), mimetype='application/x-mpegURL')

        else: # .ts segments or other files
            try:
                req = requests.get(url, headers=headers, stream=True)
                req.raise_for_status()
                return Response(req.iter_content(chunk_size=1024), content_type=req.headers['content-type'])
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 403:
                    return "Forbidden: The stream link may be expired or protected.", 403
                else:
                    print(f"Error fetching segment from {url}: {e}")
                    traceback.print_exc()
                    return str(e), 500
            except requests.exceptions.RequestException as e:
                print(f"Error fetching segment from {url}: {e}")
                traceback.print_exc()
                return str(e), 500

    except Exception as e:
        print(f"An unexpected error occurred in /stream: {e}")
        traceback.print_exc()
        return str(e), 500

if __name__ == '__main__':
    if os.environ.get('WERKZEUG_RUN_MAIN') != 'true':
        init_db()
        update_thread = threading.Thread(target=background_update)
        update_thread.daemon = True
        update_thread.start()
    app.run(debug=True, port=8080, use_reloader=False)