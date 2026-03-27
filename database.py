import sqlite3
import json
import threading
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_FILE = os.path.join(BASE_DIR, "data", "manager.db")

class Database:
    def __init__(self, filepath=DB_FILE):
        self.filepath = filepath
        db_dir = os.path.dirname(self.filepath)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)

        self.lock = threading.Lock()
        self.conn = sqlite3.connect(self.filepath, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        
        self._init_db()
        self._migrate_db()

    def replace_database(self, raw_data: bytes):
        """Safely closes the connection, overwrites the file, and reconnects."""
        with self.lock:
            try:
                self.conn.close()
            except Exception:
                pass

            try:
                with open(self.filepath, "wb") as f:
                    f.write(raw_data)

                # Reconnect to the new DB
                self.conn = sqlite3.connect(self.filepath, check_same_thread=False)
                self.conn.row_factory = sqlite3.Row
                self.conn.execute("PRAGMA journal_mode=WAL")
                return True
            except Exception as e:
                print(f"Error replacing DB: {e}")
                return False

    def _init_db(self):
        with self.lock:
            conn = self.conn
            c = conn.cursor()
            c.execute('''CREATE TABLE IF NOT EXISTS config (
                     key TEXT PRIMARY KEY,
                     value TEXT
                   )''')
            c.execute('''CREATE TABLE IF NOT EXISTS users (
                     user_id TEXT PRIMARY KEY,
                     name TEXT,
                     username TEXT,
                     warnings INTEGER DEFAULT 0,
                     role TEXT DEFAULT 'member',
                     first_seen TEXT
                   )''')
            c.execute('''CREATE TABLE IF NOT EXISTS groups (
                     chat_id TEXT PRIMARY KEY,
                     name TEXT DEFAULT 'Unknown Group',
                     rules TEXT DEFAULT 'No rules set yet. Use /setrules to set them.',
                     welcome_message TEXT DEFAULT 'Welcome {name}! 👋',
                     welcome_type TEXT DEFAULT 'text',
                     welcome_file_id TEXT DEFAULT '',
                     antispam INTEGER DEFAULT 0,
                     message_count INTEGER DEFAULT 0,
                     member_count INTEGER DEFAULT 0,
                     last_active TEXT
                   )''')
            c.execute('''CREATE TABLE IF NOT EXISTS logs (
                     id INTEGER PRIMARY KEY AUTOINCREMENT,
                     event TEXT,
                     timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                   )''')
            c.execute('''CREATE TABLE IF NOT EXISTS global_stats (
                     key TEXT PRIMARY KEY,
                     value INTEGER DEFAULT 0
                   )''')
            c.execute('''CREATE TABLE IF NOT EXISTS bad_words (
                     chat_id TEXT,
                     word TEXT,
                     PRIMARY KEY (chat_id, word)
                   )''')
            c.execute('''CREATE TABLE IF NOT EXISTS filters (
                     chat_id TEXT,
                     trigger TEXT,
                     filter_data TEXT,
                     PRIMARY KEY (chat_id, trigger)
                   )''')
            conn.commit()

    def _migrate_db(self):
        """Safely adds new columns to existing databases without breaking them."""
        migrations = [
            "ALTER TABLE groups ADD COLUMN name TEXT DEFAULT 'Unknown Group'",
            "ALTER TABLE groups ADD COLUMN member_count INTEGER DEFAULT 0",
            "ALTER TABLE users ADD COLUMN username TEXT",
            "ALTER TABLE groups ADD COLUMN welcome_type TEXT DEFAULT 'text'",
            "ALTER TABLE groups ADD COLUMN welcome_file_id TEXT DEFAULT ''",
        ]
        with self.lock:
            conn = self.conn
            c = conn.cursor()
            for migration in migrations:
                try:
                    c.execute(migration)
                    conn.commit()
                except sqlite3.OperationalError:
                    pass  # Column already exists

    def get_config(self):
        with self.lock:
            conn = self.conn
            c = conn.cursor()
            c.execute("SELECT key, value FROM config")
            conf = {
                "bot_token": "",
                "is_running": False,
                "owner_username": "",
                "support_channel": ""
            }
            for row in c.fetchall():
                try:
                    conf[row['key']] = json.loads(row['value'])
                except json.JSONDecodeError:
                    conf[row['key']] = row['value']

            env_token   = os.environ.get("BOT_TOKEN", "").strip()
            env_owner   = os.environ.get("OWNER_USERNAME", "").strip()
            env_running = os.environ.get("BOT_AUTOSTART", "").strip().lower()
            env_support = os.environ.get("SUPPORT_CHANNEL", "").strip()

            if env_token:
                conf["bot_token"] = env_token
            if env_owner:
                conf["owner_username"] = env_owner.replace("@", "")
            if env_running in ("1", "true", "yes"):
                conf["is_running"] = True
            if env_support:
                conf["support_channel"] = env_support.replace("@", "")

            return conf

    def update_config(self, key, value):
        with self.lock:
            conn = self.conn
            c = conn.cursor()
            c.execute("INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)",
                      (key, json.dumps(value)))
            conn.commit()

    def ensure_group(self, chat_id, name=None):
        str_id = str(chat_id)
        with self.lock:
            conn = self.conn
            c = conn.cursor()
            c.execute("INSERT OR IGNORE INTO groups (chat_id, name) VALUES (?, ?)",
                      (str_id, name or 'Unknown Group'))
            if name:
                c.execute("UPDATE groups SET name=? WHERE chat_id=? AND (name IS NULL OR name='Unknown Group')",
                          (name, str_id))
            conn.commit()

    def add_group(self, chat_id, name=None):
        self.ensure_group(chat_id, name=name)

    def remove_group(self, chat_id):
        self.delete_group(chat_id)

    def get_group(self, chat_id):
        self.ensure_group(chat_id)
        str_id = str(chat_id)
        with self.lock:
            conn = self.conn
            c = conn.cursor()
            c.execute("SELECT * FROM groups WHERE chat_id=?", (str_id,))
            row = c.fetchone()
            if not row:
                return None

            group_data = {col: row[col] for col in row.keys()}
            group_data['antispam'] = bool(group_data.get('antispam', 0))
            group_data['welcome_type'] = group_data.get('welcome_type') or 'text'
            group_data['welcome_file_id'] = group_data.get('welcome_file_id') or ''

            c.execute("SELECT word FROM bad_words WHERE chat_id=?", (str_id,))
            group_data['bad_words'] = [r['word'] for r in c.fetchall()]

            c.execute("SELECT trigger, filter_data FROM filters WHERE chat_id=?", (str_id,))
            filters = {}
            for r in c.fetchall():
                try:
                    filters[r['trigger']] = json.loads(r['filter_data'])
                except Exception:
                    pass
            group_data['filters'] = filters
            return group_data

    def update_group_setting(self, chat_id, key, value):
        self.ensure_group(chat_id)
        str_id = str(chat_id)
        with self.lock:
            conn = self.conn
            c = conn.cursor()
            if key == "bad_words":
                c.execute("DELETE FROM bad_words WHERE chat_id=?", (str_id,))
                for w in value:
                    c.execute("INSERT INTO bad_words (chat_id, word) VALUES (?, ?)", (str_id, w))
            else:
                val = value
                if isinstance(val, bool):
                    val = int(val)
                allowed = {
                    'rules', 'welcome_message', 'welcome_type', 'welcome_file_id',
                    'antispam', 'message_count', 'member_count', 'last_active', 'name'
                }
                if key not in allowed:
                    return
                c.execute(f"UPDATE groups SET {key}=? WHERE chat_id=?", (val, str_id))
            conn.commit()

    def add_filter(self, chat_id, trigger, filter_data):
        self.ensure_group(chat_id)
        str_id = str(chat_id)
        with self.lock:
            conn = self.conn
            c = conn.cursor()
            c.execute(
                "INSERT OR REPLACE INTO filters (chat_id, trigger, filter_data) VALUES (?, ?, ?)",
                (str_id, trigger.lower(), json.dumps(filter_data))
            )
            conn.commit()

    def remove_filter(self, chat_id, trigger):
        self.ensure_group(chat_id)
        str_id = str(chat_id)
        with self.lock:
            conn = self.conn
            c = conn.cursor()
            c.execute("DELETE FROM filters WHERE chat_id=? AND trigger=?",
                      (str_id, trigger.lower()))
            conn.commit()

    def ensure_user(self, user_id, name="Unknown", username=None):
        str_id = str(user_id)
        from datetime import datetime
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with self.lock:
            conn = self.conn
            c = conn.cursor()
            c.execute("SELECT name, username FROM users WHERE user_id=?", (str_id,))
            row = c.fetchone()
            if not row:
                c.execute(
                    "INSERT INTO users (user_id, name, username, first_seen) VALUES (?, ?, ?, ?)",
                    (str_id, name, username, now)
                )
            else:
                updates = []
                vals = []
                if name != "Unknown" and row['name'] != name:
                    updates.append("name=?")
                    vals.append(name)
                if username and row['username'] != username:
                    updates.append("username=?")
                    vals.append(username)
                if updates:
                    vals.append(str_id)
                    c.execute(f"UPDATE users SET {', '.join(updates)} WHERE user_id=?", vals)
            conn.commit()

    def increment_messages(self, chat_id):
        from datetime import datetime
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with self.lock:
            conn = self.conn
            c = conn.cursor()
            c.execute(
                "UPDATE groups SET message_count = message_count + 1, last_active = ? WHERE chat_id=?",
                (now, str(chat_id))
            )
            conn.commit()

    def get_extra_group_info(self):
        with self.lock:
            conn = self.conn
            c = conn.cursor()
            c.execute("SELECT chat_id, name, message_count, member_count, last_active, antispam FROM groups")
            return {row['chat_id']: dict(row) for row in c.fetchall()}

    def get_user(self, user_id):
        str_id = str(user_id)
        with self.lock:
            conn = self.conn
            c = conn.cursor()
            c.execute("SELECT * FROM users WHERE user_id=?", (str_id,))
            row = c.fetchone()
            return {k: row[k] for k in row.keys()} if row else {}

    def add_warning(self, user_id, name="Unknown"):
        self.ensure_user(user_id, name)
        str_id = str(user_id)
        with self.lock:
            conn = self.conn
            c = conn.cursor()
            c.execute("UPDATE users SET warnings = warnings + 1 WHERE user_id=?", (str_id,))
            conn.commit()
            c.execute("SELECT warnings FROM users WHERE user_id=?", (str_id,))
            row = c.fetchone()
            return row['warnings'] if row else 1

    def reset_warnings(self, user_id):
        str_id = str(user_id)
        with self.lock:
            conn = self.conn
            c = conn.cursor()
            c.execute("UPDATE users SET warnings = 0 WHERE user_id=?", (str_id,))
            conn.commit()

    def get_all_stats(self):
        with self.lock:
            conn = self.conn
            c = conn.cursor()
            c.execute("SELECT COUNT(*) as cu FROM users")
            total_u = c.fetchone()['cu']
            c.execute("SELECT COUNT(*) as cg FROM groups")
            total_g = c.fetchone()['cg']
            c.execute("SELECT COALESCE(SUM(message_count),0) as tm FROM groups")
            total_m = c.fetchone()['tm']
            c.execute("SELECT COUNT(*) as tl FROM logs")
            total_l = c.fetchone()['tl']
            c.execute("SELECT COUNT(*) as tf FROM filters")
            total_f = c.fetchone()['tf']
            c.execute("SELECT COUNT(*) as tw FROM bad_words")
            total_w = c.fetchone()['tw']
            c.execute("SELECT COUNT(*) as warned FROM users WHERE warnings > 0")
            total_warned = c.fetchone()['warned']
            return {
                "total_users": total_u,
                "total_groups": total_g,
                "total_messages": total_m,
                "total_logs": total_l,
                "total_filters": total_f,
                "total_bad_words": total_w,
                "total_warned_users": total_warned,
            }

    def get_all_users(self):
        with self.lock:
            conn = self.conn
            c = conn.cursor()
            c.execute("SELECT * FROM users ORDER BY first_seen DESC")
            return {row['user_id']: dict(row) for row in c.fetchall()}

    def get_all_groups(self):
        with self.lock:
            conn = self.conn
            c = conn.cursor()
            c.execute("""
                SELECT g.chat_id, g.name, g.message_count, g.member_count,
                       g.last_active, g.antispam,
                       (SELECT COUNT(*) FROM filters f WHERE f.chat_id=g.chat_id) as filter_count,
                       (SELECT COUNT(*) FROM bad_words bw WHERE bw.chat_id=g.chat_id) as bad_word_count
                FROM groups g
                ORDER BY g.message_count DESC
            """)
            result = {}
            for r in c.fetchall():
                result[r['chat_id']] = {
                    'name': r['name'] or 'Unknown Group',
                    'message_count': r['message_count'] or 0,
                    'member_count': r['member_count'] or 0,
                    'last_active': r['last_active'],
                    'antispam': bool(r['antispam']),
                    'filter_count': r['filter_count'],
                    'bad_word_count': r['bad_word_count'],
                    'filters': [None] * r['filter_count']
                }
            return result

    def delete_user(self, user_id):
        with self.lock:
            conn = self.conn
            c = conn.cursor()
            c.execute("DELETE FROM users WHERE user_id=?", (str(user_id),))
            conn.commit()

    def delete_group(self, chat_id):
        with self.lock:
            conn = self.conn
            c = conn.cursor()
            c.execute("DELETE FROM groups WHERE chat_id=?", (str(chat_id),))
            c.execute("DELETE FROM filters WHERE chat_id=?", (str(chat_id),))
            c.execute("DELETE FROM bad_words WHERE chat_id=?", (str(chat_id),))
            conn.commit()

    def search_items(self, query):
        with self.lock:
            conn = self.conn
            c = conn.cursor()
            q = f"%{query}%"
            c.execute(
                "SELECT user_id, name, username, warnings, first_seen FROM users WHERE user_id LIKE ? OR name LIKE ? OR username LIKE ?",
                (q, q, q)
            )
            users = {r['user_id']: dict(r) for r in c.fetchall()}
            c.execute(
                "SELECT chat_id, name, message_count, antispam FROM groups WHERE chat_id LIKE ? OR name LIKE ?",
                (q, q)
            )
            groups = {}
            for r in c.fetchall():
                groups[r['chat_id']] = {
                    'name': r['name'] or 'Unknown',
                    'message_count': r['message_count'] or 0,
                    'antispam': bool(r['antispam']),
                    'filters': []
                }
            return users, groups

    def get_warnings_leaderboard(self, limit=10):
        with self.lock:
            conn = self.conn
            c = conn.cursor()
            c.execute(
                "SELECT user_id, name, warnings FROM users WHERE warnings > 0 ORDER BY warnings DESC LIMIT ?",
                (limit,)
            )
            return [dict(row) for row in c.fetchall()]

    def clear_logs(self):
        with self.lock:
            conn = self.conn
            conn.execute("DELETE FROM logs")
            conn.commit()

    def log_event(self, event):
        with self.lock:
            conn = self.conn
            conn.execute("INSERT INTO logs (event) VALUES (?)", (str(event),))
            conn.commit()

    def get_recent_logs(self, limit=30):
        with self.lock:
            conn = self.conn
            c = conn.cursor()
            c.execute("SELECT event, timestamp FROM logs ORDER BY id DESC LIMIT ?", (limit,))
            return [dict(row) for row in c.fetchall()]

db = Database()
