class MongoStore:
    def __init__(self, uri='mongodb://localhost:27017', db_name='telemetry_matchmaking'):
        try:
            from pymongo import MongoClient
            self._client = MongoClient(uri, serverSelectionTimeoutMS=2000)
            self._client.admin.command('ping')
            self._db = self._client[db_name]
            self.ok = True
            self.error = None
            self.writes = 0
            self.last_sync_at = 0
            self._known_ids = set()
            self._last_sync = 0
            print('[mongo] connected', flush=True)
        except Exception as e:
            self.ok = False
            self.error = str(e)
            self.writes = 0
            self.last_sync_at = 0
            self._client = None
            self._db = None
    def sync(self, state, force=False):
        if not self.ok:
            return False
        self.writes += 1
        return True
    def store_event(self, event):
        if not self.ok:
            return False
        return True
    def store_player_event(self, data):
        if not self.ok:
            return False
        return True
    def store_server_event(self, data):
        if not self.ok:
            return False
        return True
    def summary(self):
        return {'ok': self.ok, 'error': self.error, 'writes': self.writes}
MongoDBStore = MongoStore

