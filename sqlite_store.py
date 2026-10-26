import os
import json
import time
import sqlite3
from pathlib import Path
from typing import Any


DB_PATH = os.environ.get('DB_PATH') or os.environ.get('SQLITE_PATH') or os.path.join(os.path.dirname(__file__), 'data', 'telemetry.db')


class SQLiteStore:
    def __init__(self, uri: str = None, db_name: str = None, db_path: str = None):
        self.db_path = db_path or DB_PATH
        try:
            Path(os.path.dirname(self.db_path) or '.').mkdir(parents=True, exist_ok=True)
        except Exception:
            pass
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False, timeout=5.0)
        self._conn.row_factory = sqlite3.Row
        self.ok = False
        self.error = None
        self.writes = 0
        self.last_sync_at = 0
        self._known_ids = set()
        self._last_sync = 0
        try:
            self._init_db()
            self.ok = True
            self.error = None
            print('[sqlite] connected to', self.db_path, flush=True)
        except Exception as e:
            self.ok = False
            self.error = str(e)
            self._conn = None

    def _init_db(self):
        cur = self._conn.cursor()
        cur.execute('''CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id TEXT,
            type TEXT,
            payload_json TEXT,
            timestamp REAL
        )''')
        cur.execute('''CREATE TABLE IF NOT EXISTS player_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            player_id TEXT,
            event_type TEXT,
            payload_json TEXT,
            timestamp REAL
        )''')
        cur.execute('''CREATE TABLE IF NOT EXISTS server_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            server_id TEXT,
            event_type TEXT,
            payload_json TEXT,
            timestamp REAL
        )''')
        cur.execute('''CREATE TABLE IF NOT EXISTS state_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            players_count INTEGER,
            matches_count INTEGER,
            servers_count INTEGER,
            alerts_count INTEGER,
            payload_json TEXT,
            timestamp REAL
        )''')
        self._conn.commit()

    def _to_dict(self, obj: Any) -> dict:
        try:
            if hasattr(obj, 'to_dict') and callable(obj.to_dict):
                return obj.to_dict()
        except Exception:
            pass
        if isinstance(obj, dict):
            return obj
        try:
            return dict(obj)
        except Exception:
            return {'value': str(obj)}

    def sync(self, state, force: bool = False):
        if not self.ok or not self._conn:
            return False
        try:
            now = time.time() * 1000
            players = getattr(state, 'players', {}) or {}
            matches = getattr(state, 'matches', {}) or {}
            servers = getattr(state, 'servers', {}) or {}
            alerts = getattr(state, 'alerts', []) or []
            snap = {
                'players': len(players),
                'matches': len(matches),
                'servers': len(servers),
                'alerts': len(alerts),
                'timestamp': now,
            }
            cur = self._conn.cursor()
            cur.execute(
                'INSERT INTO state_snapshots (players_count, matches_count, servers_count, alerts_count, payload_json, timestamp) VALUES (?,?,?,?,?,?)',
                (len(players), len(matches), len(servers), len(alerts), json.dumps(snap), now / 1000.0)
            )
            self._conn.commit()
            self.writes += 1
            self.last_sync_at = now
            self._last_sync = time.time()
            return True
        except Exception as e:
            self.error = str(e)
            return False

    def store_event(self, event):
        if not self.ok or not self._conn:
            return False
        try:
            evd = self._to_dict(event)
            ev_id = evd.get('id') or evd.get('event_id')
            ev_type = evd.get('type') or evd.get('event_type') or 'event'
            ts = evd.get('timestamp') or evd.get('time') or (time.time() * 1000)
            cur = self._conn.cursor()
            cur.execute(
                'INSERT INTO events (event_id, type, payload_json, timestamp) VALUES (?,?,?,?)',
                (str(ev_id) if ev_id is not None else None, str(ev_type), json.dumps(evd), float(ts) / 1000.0 if ts > 10**12 else float(ts))
            )
            self._conn.commit()
            self.writes += 1
            self.last_sync_at = time.time() * 1000
            return True
        except Exception as e:
            self.error = str(e)
            return False

    def store_player_event(self, data):
        if not self.ok or not self._conn:
            return False
        try:
            d = self._to_dict(data)
            pid = d.get('player_id') or d.get('id')
            etype = d.get('type') or d.get('event_type') or 'player_event'
            ts = d.get('timestamp') or (time.time() * 1000)
            cur = self._conn.cursor()
            cur.execute(
                'INSERT INTO player_events (player_id, event_type, payload_json, timestamp) VALUES (?,?,?,?)',
                (str(pid) if pid is not None else None, str(etype), json.dumps(d), float(ts) / 1000.0 if ts > 10**12 else float(ts))
            )
            self._conn.commit()
            self.writes += 1
            self.last_sync_at = time.time() * 1000
            return True
        except Exception as e:
            self.error = str(e)
            return False

    def store_server_event(self, data):
        if not self.ok or not self._conn:
            return False
        try:
            d = self._to_dict(data)
            sid = d.get('server_id') or d.get('id')
            etype = d.get('type') or d.get('event_type') or 'server_event'
            ts = d.get('timestamp') or (time.time() * 1000)
            cur = self._conn.cursor()
            cur.execute(
                'INSERT INTO server_events (server_id, event_type, payload_json, timestamp) VALUES (?,?,?,?)',
                (str(sid) if sid is not None else None, str(etype), json.dumps(d), float(ts) / 1000.0 if ts > 10**12 else float(ts))
            )
            self._conn.commit()
            self.writes += 1
            self.last_sync_at = time.time() * 1000
            return True
        except Exception as e:
            self.error = str(e)
            return False

    def summary(self):
        base = {'ok': self.ok, 'error': self.error, 'writes': self.writes, 'last_sync_at': self.last_sync_at, 'db_path': self.db_path}
        if not self.ok or not self._conn:
            return base
        try:
            cur = self._conn.cursor()
            cur.execute('SELECT COUNT(*) as c FROM events')
            base['events'] = cur.fetchone()['c']
            cur.execute('SELECT COUNT(*) as c FROM player_events')
            base['player_events'] = cur.fetchone()['c']
            cur.execute('SELECT COUNT(*) as c FROM server_events')
            base['server_events'] = cur.fetchone()['c']
            cur.execute('SELECT COUNT(*) as c FROM state_snapshots')
            base['state_snapshots'] = cur.fetchone()['c']
            base['total_records'] = base['events'] + base['player_events'] + base['server_events'] + base['state_snapshots']
        except Exception as e:
            base['error'] = str(e)
        return base


# Keep compatible names
MongoStore = SQLiteStore
MongoDBStore = SQLiteStore
Store = SQLiteStore
