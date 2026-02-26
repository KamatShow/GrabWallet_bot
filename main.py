import os
import telebot
from telebot import types
import requests
import threading
import time
import hashlib
import json
import sqlite3
import random
import string
import base64
from datetime import datetime, timedelta
from dotenv import load_dotenv
from keep_alive import keep_alive
import io
import csv
from collections import defaultdict
import logging
import sys
import shutil
from functools import wraps
from queue import Queue
from contextlib import contextmanager
from decimal import Decimal, ROUND_DOWN
import traceback
import signal

# Load environment variables
load_dotenv()
keep_alive()

# ==================== Configuration ====================
class Config:
    def __init__(self):
        self.API_TOKEN = os.getenv("API_TOKEN")
        self.TONCENTER_KEY = os.getenv("TONCENTER_KEY")
        admin_ids_str = os.getenv("ADMIN_IDS")
        self.ADMIN_IDS = [int(id.strip()) for id in admin_ids_str.split(",") if id.strip().isdigit()]
        self.OFFICIAL_WALLET = os.getenv("OFFICIAL_WALLET")
        self.CHANNEL_NOTIFY = os.getenv("CHANNEL_NOTIFY")
        self.DB_PATH = 'bot_database.db'
        self.BACKUP_DIR = 'backups'
        self.TON_API_URL = "https://toncenter.com/api/v2/"
        
        # Deposit settings
        self.MIN_DEPOSIT = 0.05
        
        # Withdrawal settings
        self.MIN_WITHDRAWAL = 1.0
        self.WITHDRAWAL_FEE_PERCENT = 0.20
        self.WITHDRAWAL_FEE_FIXED = 0.1
        
        # Send settings
        self.MIN_SEND = 0.0001
        
        # Distribution settings
        self.MIN_DISTRIBUTION_TOTAL = 0.1
        self.MIN_DISTRIBUTION_PER_USER = 0.00001
        self.MAX_DISTRIBUTION_USERS = 10000
        self.MIN_DISTRIBUTION_USERS = 2
        
        self.DEPOSIT_CHECK_INTERVAL = 15
        self.CLEANUP_INTERVAL = 86400
        self.BACKUP_INTERVAL = 604800
        
        self.SUPPORTED_COINS = ['TON', 'USD', 'NOT', 'DOGS', 'HMSTR']
        
        self.PRICES = {
            'TON': 1.0,
            'USD': 0.01,
            'NOT': 0.0001,
            'DOGS': 0.00001,
            'HMSTR': 0.000001
        }
        
        self.AD_PRICES = {
            1: 1.0,
            3: 2.5,
            7: 5.0,
            30: 15.0
        }
        
        if not os.path.exists(self.BACKUP_DIR):
            os.makedirs(self.BACKUP_DIR)

config = Config()

# ==================== Logging ====================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bot.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ==================== Bot Initialization ====================
if not config.API_TOKEN:
    logger.critical("API_TOKEN is required!")
    sys.exit(1)

try:
    bot = telebot.TeleBot(config.API_TOKEN)
    logger.info("Bot initialized successfully")
except Exception as e:
    logger.critical(f"Failed to initialize bot: {e}")
    sys.exit(1)
# ==================== TON API - FIXED VERSION ====================
class TonAPI:
    def __init__(self, api_key):
        self.api_key = api_key
        self.base_url = "https://toncenter.com/api/v2/"
        self.session = requests.Session()
        self.last_processed_lt = 0
        self.processed_hashes = set()
        self.cache = {}
        self.cache_timeout = 300
        self.retry_count = 3
        self.retry_delay = 2

    def _make_request(self, method, params=None, retry=True):
        url = f"{self.base_url}{method}"
        params = params or {}
        params['api_key'] = self.api_key
        
        for attempt in range(self.retry_count if retry else 1):
            try:
                response = self.session.get(url, params=params, timeout=15)
                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 429:
                    wait_time = self.retry_delay * (attempt + 1) * 2
                    logger.warning(f"Rate limited, waiting {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"API error {response.status_code}: {response.text}")
            except Exception as e:
                logger.error(f"Request error: {e}")
                if attempt < self.retry_count - 1:
                    time.sleep(self.retry_delay * (attempt + 1))
        return None

    def get_transactions(self, address, limit=50):
        try:
            params = {
                "address": address,
                "limit": min(limit, 100)
            }
            result = self._make_request('getTransactions', params)
            if result and isinstance(result, dict):
                return result.get('result', [])
            return []
        except Exception as e:
            logger.error(f"get_transactions error: {e}")
            return []

if config.TONCENTER_KEY:
    ton_api = TonAPI(config.TONCENTER_KEY)
    logger.info(f"TON API initialized with key: {config.TONCENTER_KEY[:10]}...")
else:
    ton_api = None
    logger.error("TONCENTER_KEY not found in .env")

# ==================== Database Connection Pool ====================
class DatabasePool:
    def __init__(self, max_connections=10, db_path=config.DB_PATH):
        self.max_connections = max_connections
        self.db_path = db_path
        self.connections = Queue(max_connections)
        self._initialize_pool()

    def _initialize_pool(self):
        for i in range(self.max_connections):
            conn = sqlite3.connect(self.db_path, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            self.connections.put(conn)
        logger.info(f"Database pool initialized with {self.max_connections} connections")

    @contextmanager
    def get_connection(self):
        conn = self.connections.get()
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            self.connections.put(conn)

    def close_all(self):
        while not self.connections.empty():
            conn = self.connections.get()
            conn.close()
        logger.info("All database connections closed")

db_pool = DatabasePool()

# ==================== Initialize Database Tables ====================
def init_database():
    with db_pool.get_connection() as conn:
        c = conn.cursor()
        
        # Users table
        c.execute('''CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            language_code TEXT,
            bal_ton REAL DEFAULT 0,
            bal_usd REAL DEFAULT 0,
            bal_not REAL DEFAULT 0,
            bal_dogs REAL DEFAULT 0,
            bal_hmstr REAL DEFAULT 0,
            frozen_ton REAL DEFAULT 0,
            frozen_usd REAL DEFAULT 0,
            frozen_not REAL DEFAULT 0,
            frozen_dogs REAL DEFAULT 0,
            frozen_hmstr REAL DEFAULT 0,
            total_deposits REAL DEFAULT 0,
            total_withdrawals REAL DEFAULT 0,
            joined_date REAL,
            last_active REAL,
            banned INTEGER DEFAULT 0,
            notify_enabled INTEGER DEFAULT 1,
            total_referrals INTEGER DEFAULT 0,
            referral_earnings REAL DEFAULT 0
        )''')

        # Transactions table
        c.execute('''CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            from_id INTEGER,
            to_id INTEGER,
            amount REAL,
            coin TEXT,
            type TEXT,
            status TEXT DEFAULT 'completed',
            tx_hash TEXT,
            memo TEXT,
            timestamp REAL,
            date TEXT,
            fee REAL DEFAULT 0
        )''')

        # Referrals table
        c.execute('''CREATE TABLE IF NOT EXISTS referrals (
            referrer_id INTEGER,
            referred_id INTEGER,
            timestamp REAL,
            reward REAL DEFAULT 0.0001,
            PRIMARY KEY (referrer_id, referred_id)
        )''')

        # Withdrawals table
        c.execute('''CREATE TABLE IF NOT EXISTS pending_withdrawals (
            withdrawal_id TEXT PRIMARY KEY,
            user_id INTEGER,
            username TEXT,
            amount REAL,
            coin TEXT,
            fee REAL,
            total_frozen REAL,
            wallet TEXT,
            memo TEXT,
            timestamp REAL,
            status TEXT DEFAULT 'pending'
        )''')

        # Processed transactions
        c.execute('''CREATE TABLE IF NOT EXISTS processed_transactions (
            tx_hash TEXT PRIMARY KEY,
            lt INTEGER,
            processed_at REAL,
            from_address TEXT,
            value REAL,
            comment TEXT
        )''')

        # Notifications table
        c.execute('''CREATE TABLE IF NOT EXISTS notifications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            message TEXT,
            type TEXT,
            sent_at REAL,
            read INTEGER DEFAULT 0
        )''')

        # Distributions table
        c.execute('''CREATE TABLE IF NOT EXISTS distributions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            admin_id INTEGER,
            type TEXT,
            total_amount REAL,
            coin TEXT,
            total_users INTEGER,
            per_user_amount REAL DEFAULT 0,
            status TEXT DEFAULT 'active',
            created_at REAL,
            expires_at REAL,
            remaining_amount REAL,
            winners_count INTEGER DEFAULT 0,
            title TEXT,
            description TEXT,
            is_random INTEGER DEFAULT 0
        )''')

        # Distribution participants
        c.execute('''CREATE TABLE IF NOT EXISTS distribution_participants (
            distribution_id INTEGER,
            user_id INTEGER,
            username TEXT,
            amount REAL,
            claimed_at REAL,
            PRIMARY KEY (distribution_id, user_id)
        )''')

        # Gift codes table
        c.execute('''CREATE TABLE IF NOT EXISTS gift_codes (
            code TEXT PRIMARY KEY,
            distribution_id INTEGER,
            amount REAL,
            coin TEXT,
            created_by INTEGER,
            claimed_by INTEGER,
            claimed_at REAL,
            expires_at REAL,
            status TEXT DEFAULT 'active'
        )''')

        # Ads table
        c.execute('''CREATE TABLE IF NOT EXISTS ads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            username TEXT,
            ad_text TEXT,
            ad_photo TEXT,
            ad_link TEXT,
            days INTEGER,
            price REAL,
            status TEXT DEFAULT 'pending',
            start_time REAL,
            end_time REAL,
            created_at REAL,
            views INTEGER DEFAULT 0,
            clicks INTEGER DEFAULT 0
        )''')

        # Admin logs
        c.execute('''CREATE TABLE IF NOT EXISTS admin_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            admin_id INTEGER,
            action TEXT,
            details TEXT,
            timestamp REAL
        )''')

        # Daily stats
        c.execute('''CREATE TABLE IF NOT EXISTS daily_stats (
            date TEXT PRIMARY KEY,
            new_users INTEGER DEFAULT 0,
            transactions INTEGER DEFAULT 0,
            deposits REAL DEFAULT 0,
            withdrawals REAL DEFAULT 0,
            active_users INTEGER DEFAULT 0
        )''')

        # User stats for charts
        c.execute('''CREATE TABLE IF NOT EXISTS user_stats (
            user_id INTEGER,
            date TEXT,
            balance_ton REAL,
            transactions_count INTEGER,
            PRIMARY KEY (user_id, date)
        )''')

        # Create indexes
        c.execute('CREATE INDEX IF NOT EXISTS idx_users_username ON users(username)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_users_banned ON users(banned)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_transactions_user ON transactions(from_id, to_id)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_transactions_timestamp ON transactions(timestamp)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_withdrawals_status ON pending_withdrawals(status)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_distributions_status ON distributions(status)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_notifications_user ON notifications(user_id, read)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_gift_codes_status ON gift_codes(status)')
        
        logger.info("Database tables initialized")

init_database()

# ==================== Global Variables ====================
processed_txs_cache = set()
pending_withdrawals = {}
# ==================== Performance Monitor ====================
class PerformanceMonitor:
    def __init__(self):
        self.start_time = time.time()
        self.commands_count = defaultdict(int)
        self.errors_count = 0
        self.user_activity = defaultdict(int)

    def log_command(self, command, user_id=None):
        self.commands_count[command] += 1
        if user_id:
            self.user_activity[user_id] += 1

    def log_error(self):
        self.errors_count += 1

    def get_stats(self):
        uptime = time.time() - self.start_time
        days = uptime // 86400
        hours = (uptime % 86400) // 3600
        minutes = (uptime % 3600) // 60
        total_commands = sum(self.commands_count.values())
        
        text = f"Performance Statistics\n\n"
        text += f"Uptime: {int(days)}d {int(hours)}h {int(minutes)}m\n"
        text += f"Total Commands: {total_commands:,}\n"
        text += f"Total Errors: {self.errors_count:,}\n"
        text += f"Most Used Commands:\n"
        
        sorted_commands = sorted(self.commands_count.items(), key=lambda x: x[1], reverse=True)[:10]
        for cmd, count in sorted_commands:
            text += f"/{cmd}: {count}\n"
        
        return text

perf_monitor = PerformanceMonitor()

# ==================== Helper Functions ====================
def get_user_mention(user_id, username=None, first_name=None):
    if username:
        return f"@{username}"
    elif first_name:
        return f"[{first_name}](tg://user?id={user_id})"
    else:
        return f"User {user_id}"

def find_user_by_identifier(identifier):
    if identifier.startswith('@'):
        username = identifier[1:].lower()
        with db_pool.get_connection() as conn:
            c = conn.cursor()
            c.execute("SELECT user_id FROM users WHERE LOWER(username) = ?", (username,))
            result = c.fetchone()
            return result['user_id'] if result else None
    else:
        try:
            return int(identifier)
        except:
            return None

def find_users_by_name_or_username(search_term, limit=10):
    with db_pool.get_connection() as conn:
        c = conn.cursor()
        search_pattern = f"%{search_term}%"
        c.execute('''SELECT user_id, username, first_name FROM users 
                     WHERE username LIKE ? OR first_name LIKE ? OR last_name LIKE ? 
                     LIMIT ?''', (search_pattern, search_pattern, search_pattern, limit))
        return [dict(row) for row in c.fetchall()]

def format_time(timestamp):
    if not timestamp:
        return "Never"
    dt = datetime.fromtimestamp(timestamp)
    return dt.strftime('%Y-%m-%d %H:%M:%S')

def get_time_ago(timestamp):
    if not timestamp:
        return "Never"
    diff = time.time() - timestamp
    if diff < 60:
        return f"{int(diff)} seconds ago"
    elif diff < 3600:
        return f"{int(diff / 60)} minutes ago"
    elif diff < 86400:
        return f"{int(diff / 3600)} hours ago"
    else:
        return f"{int(diff / 86400)} days ago"

def generate_unique_id(prefix='', length=8):
    timestamp = int(time.time() * 1000)
    random_part = random.randint(1000, 9999)
    unique_str = f"{prefix}{timestamp}{random_part}"
    return unique_str[:length]

def generate_gift_code(length=10):
    chars = string.ascii_uppercase + string.digits
    return ''.join(random.choice(chars) for _ in range(length))

def send_to_channel(title, content, tx_hash=None, msg_type='info'):
    if not config.CHANNEL_NOTIFY:
        return
    try:
        emoji = {
            'info': 'ℹ️',
            'success': '✅',
            'warning': '⚠️',
            'error': '❌',
            'deposit': '💰',
            'withdraw': '💸',
            'airdrop': '🎁',
            'giveaway': '🎲'
        }.get(msg_type, 'ℹ️')
        
        text = f"{emoji} {title}\n\n{content}"
        if tx_hash:
            text += f"\n🔗 Tx: {tx_hash[:8]}..."
        bot.send_message(config.CHANNEL_NOTIFY, text)
    except Exception as e:
        logger.error(f"Failed to send to channel: {e}")

def create_backup():
    try:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_file = f"{config.BACKUP_DIR}/backup_{timestamp}.db"
        shutil.copy2(config.DB_PATH, backup_file)
        
        # Keep only last 10 backups
        backups = sorted([f for f in os.listdir(config.BACKUP_DIR) if f.startswith('backup_')])
        if len(backups) > 10:
            for old_backup in backups[:-10]:
                os.remove(os.path.join(config.BACKUP_DIR, old_backup))
        
        logger.info(f"Database backup created: backup_{timestamp}.db")
        return f"backup_{timestamp}.db"
    except Exception as e:
        logger.error(f"Backup error: {e}")
        return None

def restore_backup(backup_file):
    global db_pool
    try:
        backup_path = os.path.join(config.BACKUP_DIR, backup_file)
        if not os.path.exists(backup_path):
            return False, "Backup file not found"
        
        db_pool.close_all()
        shutil.copy2(backup_path, config.DB_PATH)
        db_pool = DatabasePool()
        
        logger.info(f"Database restored from: {backup_file}")
        return True, "Database restored successfully"
    except Exception as e:
        logger.error(f"Restore error: {e}")
        return False, str(e)

def log_admin_action(admin_id, action, details):
    with db_pool.get_connection() as conn:
        c = conn.cursor()
        c.execute('''INSERT INTO admin_logs (admin_id, action, details, timestamp) 
                     VALUES (?, ?, ?, ?)''', (admin_id, action, details, time.time()))

def get_admin_logs(limit=100):
    with db_pool.get_connection() as conn:
        c = conn.cursor()
        c.execute('''SELECT * FROM admin_logs ORDER BY timestamp DESC LIMIT ?''', (limit,))
        return [dict(row) for row in c.fetchall()]

# ==================== Load Processed Transactions ====================
def load_processed_transactions():
    global processed_txs_cache
    try:
        with db_pool.get_connection() as conn:
            c = conn.cursor()
            c.execute("SELECT tx_hash FROM processed_transactions")
            rows = c.fetchall()
            processed_txs_cache = {row['tx_hash'] for row in rows}
            logger.info(f"Loaded {len(processed_txs_cache)} processed transactions")
    except Exception as e:
        logger.error(f"Error loading processed transactions: {e}")
        processed_txs_cache = set()
# ==================== Exchange Rate Manager ====================
class ExchangeRateManager:
    def __init__(self):
        self.cache = {}
        self.cache_timeout = 300
        self.update_thread = threading.Thread(target=self._update_rates_periodically, daemon=True)
        self.update_thread.start()

    def _update_rates_periodically(self):
        while True:
            time.sleep(3600)
            self._fetch_live_rates()

    def _fetch_live_rates(self):
        try:
            response = requests.get("https://api.coingecko.com/api/v3/simple/price?ids=the-open-network&vs_currencies=usd", timeout=10)
            if response.status_code == 200:
                data = response.json()
                ton_usd = data.get('the-open-network', {}).get('usd')
                if ton_usd:
                    self.cache['TON_USD'] = (time.time(), ton_usd)
                    self.cache['USD_TON'] = (time.time(), 1/ton_usd)
                    logger.info(f"Updated TON/USD rate: {ton_usd}")
        except Exception as e:
            logger.error(f"Failed to fetch live rates: {e}")

    def get_rate(self, from_coin, to_coin):
        cache_key = f"{from_coin}_{to_coin}"
        if cache_key in self.cache:
            cached_time, cached_rate = self.cache[cache_key]
            if time.time() - cached_time < self.cache_timeout:
                return cached_rate
        
        # Default rates if API fails
        default_rates = {
            ('TON', 'USD'): 0.0,
            ('USD', 'TON'): 0.0,
            ('TON', 'NOT'): 0.0,
            ('NOT', 'TON'): 0.0,
            ('TON', 'DOGS'): 0.0,
            ('DOGS', 'TON'): 0.0,
            ('TON', 'HMSTR'): 0.0,
            ('HMSTR', 'TON'): 0.0,
        }
        
        rate = default_rates.get((from_coin, to_coin))
        if rate:
            self.cache[cache_key] = (time.time(), rate)
        return rate

    def get_all_rates(self):
        rates = {}
        for coin in config.SUPPORTED_COINS:
            if coin != 'TON':
                rate = self.get_rate('TON', coin)
                if rate:
                    rates[coin] = rate
        return rates

exchange_manager = ExchangeRateManager()

# ==================== User Model ====================
class UserModel:
    @staticmethod
    def get(user_id, create_if_missing=True, username=None, first_name=None, last_name=None, language_code=None):
        with db_pool.get_connection() as conn:
            c = conn.cursor()
            c.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
            row = c.fetchone()
            
            if not row and create_if_missing:
                now = time.time()
                c.execute('''INSERT INTO users 
                             (user_id, username, first_name, last_name, language_code, joined_date, last_active) 
                             VALUES (?, ?, ?, ?, ?, ?, ?)''',
                          (user_id, username, first_name, last_name, language_code, now, now))
                logger.info(f"New user created: {user_id}")
                c.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
                row = c.fetchone()
            elif row:
                c.execute("UPDATE users SET last_active = ? WHERE user_id = ?", (time.time(), user_id))
                if username:
                    c.execute("UPDATE users SET username = ? WHERE user_id = ?", (username, user_id))
            
            return dict(row) if row else None

    @staticmethod
    def update_balance(user_id, coin, amount, operation='add'):
        coin = coin.lower()
        with db_pool.get_connection() as conn:
            c = conn.cursor()
            if operation == 'add':
                c.execute(f"UPDATE users SET bal_{coin} = bal_{coin} + ? WHERE user_id = ?", (amount, user_id))
            else:
                c.execute(f"UPDATE users SET bal_{coin} = bal_{coin} - ? WHERE user_id = ?", (amount, user_id))

    @staticmethod
    def update_frozen(user_id, coin, amount, operation='add'):
        coin = coin.lower()
        with db_pool.get_connection() as conn:
            c = conn.cursor()
            if operation == 'add':
                c.execute(f"UPDATE users SET frozen_{coin} = frozen_{coin} + ? WHERE user_id = ?", (amount, user_id))
            else:
                c.execute(f"UPDATE users SET frozen_{coin} = frozen_{coin} - ? WHERE user_id = ?", (amount, user_id))

    @staticmethod
    def update_total_deposits(user_id, amount):
        with db_pool.get_connection() as conn:
            c = conn.cursor()
            c.execute("UPDATE users SET total_deposits = total_deposits + ? WHERE user_id = ?", (amount, user_id))

    @staticmethod
    def update_total_withdrawals(user_id, amount):
        with db_pool.get_connection() as conn:
            c = conn.cursor()
            c.execute("UPDATE users SET total_withdrawals = total_withdrawals + ? WHERE user_id = ?", (amount, user_id))

    @staticmethod
    def update_referral_stats(user_id, count=1, earnings=0):
        with db_pool.get_connection() as conn:
            c = conn.cursor()
            c.execute('''UPDATE users SET total_referrals = total_referrals + ?, 
                         referral_earnings = referral_earnings + ? WHERE user_id = ?''',
                     (count, earnings, user_id))

    @staticmethod
    def is_banned(user_id):
        with db_pool.get_connection() as conn:
            c = conn.cursor()
            c.execute("SELECT banned FROM users WHERE user_id = ?", (user_id,))
            result = c.fetchone()
            return result and result['banned'] == 1

    @staticmethod
    def ban(user_id):
        with db_pool.get_connection() as conn:
            c = conn.cursor()
            c.execute("UPDATE users SET banned = 1 WHERE user_id = ?", (user_id,))
        log_admin_action(0, 'ban', f"User {user_id} banned")

    @staticmethod
    def unban(user_id):
        with db_pool.get_connection() as conn:
            c = conn.cursor()
            c.execute("UPDATE users SET banned = 0 WHERE user_id = ?", (user_id,))
        log_admin_action(0, 'unban', f"User {user_id} unbanned")

    @staticmethod
    def get_count(banned_only=False):
        with db_pool.get_connection() as conn:
            c = conn.cursor()
            query = "SELECT COUNT(*) FROM users"
            if banned_only:
                query += " WHERE banned = 1"
            c.execute(query)
            return c.fetchone()[0]

    @staticmethod
    def get_all(limit=None, offset=0, banned_only=False):
        with db_pool.get_connection() as conn:
            c = conn.cursor()
            query = "SELECT * FROM users"
            if banned_only:
                query += " WHERE banned = 1"
            query += " ORDER BY joined_date DESC"
            if limit:
                query += f" LIMIT {limit} OFFSET {offset}"
            c.execute(query)
            return [dict(row) for row in c.fetchall()]

    @staticmethod
    def search(search_term, limit=20):
        with db_pool.get_connection() as conn:
            c = conn.cursor()
            search_pattern = f"%{search_term}%"
            c.execute('''SELECT * FROM users WHERE user_id LIKE ? OR username LIKE ? 
                         OR first_name LIKE ? OR last_name LIKE ? 
                         ORDER BY joined_date DESC LIMIT ?''',
                     (search_term, search_pattern, search_pattern, search_pattern, limit))
            return [dict(row) for row in c.fetchall()]

    @staticmethod
    def get_statistics():
        with db_pool.get_connection() as conn:
            c = conn.cursor()
            c.execute("SELECT COUNT(*) FROM users")
            total = c.fetchone()[0]
            
            today_start = time.mktime(datetime.now().replace(hour=0, minute=0, second=0).timetuple())
            c.execute("SELECT COUNT(*) FROM users WHERE joined_date > ?", (today_start,))
            new_today = c.fetchone()[0]
            
            week_start = time.time() - (7 * 86400)
            c.execute("SELECT COUNT(*) FROM users WHERE joined_date > ?", (week_start,))
            new_week = c.fetchone()[0]
            
            month_start = time.time() - (30 * 86400)
            c.execute("SELECT COUNT(*) FROM users WHERE joined_date > ?", (month_start,))
            new_month = c.fetchone()[0]
            
            c.execute("SELECT COUNT(*) FROM users WHERE banned = 1")
            banned = c.fetchone()[0]
            
            c.execute('''SELECT SUM(bal_ton) as total_ton, SUM(bal_usd) as total_usd, 
                        SUM(bal_not) as total_not, SUM(bal_dogs) as total_dogs, 
                        SUM(bal_hmstr) as total_hmstr FROM users''')
            totals = c.fetchone()
            
            c.execute("SELECT COUNT(*) FROM users WHERE last_active > ?", (time.time() - 86400,))
            active_24h = c.fetchone()[0]
            
            c.execute("SELECT COUNT(*) FROM users WHERE last_active > ?", (time.time() - 604800,))
            active_7d = c.fetchone()[0]
            
            return {
                'total': total,
                'new_today': new_today,
                'new_week': new_week,
                'new_month': new_month,
                'banned': banned,
                'total_ton': totals['total_ton'] or 0,
                'total_usd': totals['total_usd'] or 0,
                'total_not': totals['total_not'] or 0,
                'total_dogs': totals['total_dogs'] or 0,
                'total_hmstr': totals['total_hmstr'] or 0,
                'active_24h': active_24h,
                'active_7d': active_7d
            }

    @staticmethod
    def get_top_by_balance(coin='TON', limit=10):
        coin = coin.lower()
        with db_pool.get_connection() as conn:
            c = conn.cursor()
            c.execute(f'''SELECT user_id, username, first_name, bal_{coin} as balance 
                         FROM users WHERE bal_{coin} > 0 ORDER BY bal_{coin} DESC LIMIT ?''', (limit,))
            return [dict(row) for row in c.fetchall()]

    @staticmethod
    def get_top_by_deposits(limit=10):
        with db_pool.get_connection() as conn:
            c = conn.cursor()
            c.execute('''SELECT user_id, username, first_name, total_deposits as deposits 
                         FROM users WHERE total_deposits > 0 ORDER BY total_deposits DESC LIMIT ?''', (limit,))
            return [dict(row) for row in c.fetchall()]

    @staticmethod
    def get_top_by_referrals(limit=10):
        with db_pool.get_connection() as conn:
            c = conn.cursor()
            c.execute('''SELECT user_id, username, first_name, total_referrals as referrals, 
                        referral_earnings as earnings FROM users WHERE total_referrals > 0 
                        ORDER BY total_referrals DESC LIMIT ?''', (limit,))
            return [dict(row) for row in c.fetchall()]
# ==================== Transaction Model ====================
class TransactionModel:
    @staticmethod
    def create(from_id, to_id, amount, coin, tx_type, status='completed', tx_hash=None, memo=None, fee=0):
        with db_pool.get_connection() as conn:
            c = conn.cursor()
            date_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            c.execute('''INSERT INTO transactions 
                         (from_id, to_id, amount, coin, type, status, tx_hash, memo, timestamp, date, fee) 
                         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                     (from_id, to_id, amount, coin, tx_type, status, tx_hash, memo, time.time(), date_str, fee))
            return c.lastrowid

    @staticmethod
    def get_user_transactions(user_id, limit=10, offset=0, tx_type=None):
        with db_pool.get_connection() as conn:
            c = conn.cursor()
            query = "SELECT * FROM transactions WHERE from_id = ? OR to_id = ?"
            params = [user_id, user_id]
            if tx_type:
                query += " AND type = ?"
                params.append(tx_type)
            query += " ORDER BY timestamp DESC LIMIT ? OFFSET ?"
            params.extend([limit, offset])
            c.execute(query, params)
            return [dict(row) for row in c.fetchall()]

    @staticmethod
    def get_all_transactions(limit=100, offset=0, tx_type=None, from_date=None, to_date=None):
        with db_pool.get_connection() as conn:
            c = conn.cursor()
            query = "SELECT * FROM transactions WHERE 1=1"
            params = []
            if tx_type:
                query += " AND type = ?"
                params.append(tx_type)
            if from_date:
                query += " AND timestamp >= ?"
                params.append(from_date)
            if to_date:
                query += " AND timestamp <= ?"
                params.append(to_date)
            query += " ORDER BY timestamp DESC LIMIT ? OFFSET ?"
            params.extend([limit, offset])
            c.execute(query, params)
            return [dict(row) for row in c.fetchall()]

    @staticmethod
    def get_transactions_by_type(tx_type, limit=100):
        with db_pool.get_connection() as conn:
            c = conn.cursor()
            c.execute('''SELECT * FROM transactions WHERE type = ? ORDER BY timestamp DESC LIMIT ?''', 
                     (tx_type, limit))
            return [dict(row) for row in c.fetchall()]

    @staticmethod
    def get_transactions_by_hash(tx_hash):
        with db_pool.get_connection() as conn:
            c = conn.cursor()
            c.execute("SELECT * FROM transactions WHERE tx_hash = ?", (tx_hash,))
            row = c.fetchone()
            return dict(row) if row else None

    @staticmethod
    def get_statistics():
        with db_pool.get_connection() as conn:
            c = conn.cursor()
            c.execute("SELECT COUNT(*) as count, COALESCE(SUM(amount), 0) as total FROM transactions")
            result = c.fetchone()
            
            c.execute("SELECT type, COUNT(*) as count, COALESCE(SUM(amount), 0) as total FROM transactions GROUP BY type")
            by_type = {row['type']: {'count': row['count'], 'total': row['total']} for row in c.fetchall()}
            
            today_start = time.mktime(datetime.now().replace(hour=0, minute=0, second=0).timetuple())
            c.execute("SELECT COUNT(*) as count, COALESCE(SUM(amount), 0) as total FROM transactions WHERE timestamp > ?", 
                     (today_start,))
            today = c.fetchone()
            
            week_start = time.time() - (7 * 86400)
            c.execute("SELECT COUNT(*) as count, COALESCE(SUM(amount), 0) as total FROM transactions WHERE timestamp > ?", 
                     (week_start,))
            week = c.fetchone()
            
            month_start = time.time() - (30 * 86400)
            c.execute("SELECT COUNT(*) as count, COALESCE(SUM(amount), 0) as total FROM transactions WHERE timestamp > ?", 
                     (month_start,))
            month = c.fetchone()
            
            return {
                'total_count': result['count'],
                'total_amount': result['total'],
                'by_type': by_type,
                'today_count': today['count'],
                'today_amount': today['total'],
                'week_count': week['count'],
                'week_amount': week['total'],
                'month_count': month['count'],
                'month_amount': month['total']
            }

# ==================== Referral Model ====================
class ReferralModel:
    @staticmethod
    def create(referrer_id, referred_id, reward=0.0001):
        with db_pool.get_connection() as conn:
            c = conn.cursor()
            try:
                c.execute('''INSERT INTO referrals (referrer_id, referred_id, timestamp, reward) 
                             VALUES (?, ?, ?, ?)''', (referrer_id, referred_id, time.time(), reward))
                UserModel.update_referral_stats(referrer_id, 1, reward)
                return True
            except sqlite3.IntegrityError:
                return False

    @staticmethod
    def get_by_referrer(referrer_id):
        with db_pool.get_connection() as conn:
            c = conn.cursor()
            c.execute('''SELECT referred_id, timestamp, reward FROM referrals 
                         WHERE referrer_id = ? ORDER BY timestamp DESC''', (referrer_id,))
            return [dict(row) for row in c.fetchall()]

    @staticmethod
    def get_by_referred(referred_id):
        with db_pool.get_connection() as conn:
            c = conn.cursor()
            c.execute("SELECT referrer_id FROM referrals WHERE referred_id = ?", (referred_id,))
            row = c.fetchone()
            return row['referrer_id'] if row else None

    @staticmethod
    def get_count(referrer_id):
        with db_pool.get_connection() as conn:
            c = conn.cursor()
            c.execute("SELECT COUNT(*) FROM referrals WHERE referrer_id = ?", (referrer_id,))
            return c.fetchone()[0]

    @staticmethod
    def get_total_reward(referrer_id):
        with db_pool.get_connection() as conn:
            c = conn.cursor()
            c.execute("SELECT COALESCE(SUM(reward), 0) FROM referrals WHERE referrer_id = ?", (referrer_id,))
            return c.fetchone()[0]
# ==================== Withdrawal Model ====================
class WithdrawalModel:
    @staticmethod
    def create(withdrawal_id, user_id, username, amount, coin, fee, total_frozen, wallet, memo=None):
        with db_pool.get_connection() as conn:
            c = conn.cursor()
            c.execute('''INSERT INTO pending_withdrawals 
                         (withdrawal_id, user_id, username, amount, coin, fee, total_frozen, wallet, memo, timestamp) 
                         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                     (withdrawal_id, user_id, username, amount, coin, fee, total_frozen, wallet, memo, time.time()))

    @staticmethod
    def get_pending():
        with db_pool.get_connection() as conn:
            c = conn.cursor()
            c.execute("SELECT * FROM pending_withdrawals WHERE status = 'pending' ORDER BY timestamp ASC")
            return [dict(row) for row in c.fetchall()]

    @staticmethod
    def get_by_id(withdrawal_id):
        with db_pool.get_connection() as conn:
            c = conn.cursor()
            c.execute("SELECT * FROM pending_withdrawals WHERE withdrawal_id = ?", (withdrawal_id,))
            row = c.fetchone()
            return dict(row) if row else None

    @staticmethod
    def update_status(withdrawal_id, status, tx_hash=None):
        with db_pool.get_connection() as conn:
            c = conn.cursor()
            c.execute("UPDATE pending_withdrawals SET status = ? WHERE withdrawal_id = ?", (status, withdrawal_id))
            if c.rowcount > 0 and status == 'completed' and tx_hash:
                withdrawal = WithdrawalModel.get_by_id(withdrawal_id)
                if withdrawal:
                    TransactionModel.create(
                        from_id=withdrawal['user_id'],
                        to_id=None,
                        amount=withdrawal['amount'],
                        coin=withdrawal['coin'],
                        tx_type='withdraw',
                        status='completed',
                        tx_hash=tx_hash,
                        memo=withdrawal['memo'],
                        fee=withdrawal['fee']
                    )
            return c.rowcount > 0

    @staticmethod
    def get_user_withdrawals(user_id, limit=10):
        with db_pool.get_connection() as conn:
            c = conn.cursor()
            c.execute('''SELECT * FROM pending_withdrawals WHERE user_id = ? 
                         ORDER BY timestamp DESC LIMIT ?''', (user_id, limit))
            return [dict(row) for row in c.fetchall()]

    @staticmethod
    def get_statistics():
        with db_pool.get_connection() as conn:
            c = conn.cursor()
            c.execute("SELECT COUNT(*) as count, COALESCE(SUM(amount), 0) as total FROM pending_withdrawals WHERE status='pending'")
            pending = c.fetchone()
            c.execute("SELECT COUNT(*) as count, COALESCE(SUM(amount), 0) as total FROM pending_withdrawals WHERE status='completed'")
            completed = c.fetchone()
            c.execute("SELECT COUNT(*) as count, COALESCE(SUM(amount), 0) as total FROM pending_withdrawals WHERE status='cancelled'")
            cancelled = c.fetchone()
            return {
                'pending_count': pending['count'],
                'pending_total': pending['total'],
                'completed_count': completed['count'],
                'completed_total': completed['total'],
                'cancelled_count': cancelled['count'],
                'cancelled_total': cancelled['total']
            }

# ==================== Notification Model ====================
class NotificationModel:
    @staticmethod
    def create(user_id, message, notif_type='info'):
        with db_pool.get_connection() as conn:
            c = conn.cursor()
            c.execute('''INSERT INTO notifications (user_id, message, type, sent_at) 
                         VALUES (?, ?, ?, ?)''', (user_id, message, notif_type, time.time()))
            return c.lastrowid

    @staticmethod
    def create_bulk(user_ids, message, notif_type='info'):
        with db_pool.get_connection() as conn:
            c = conn.cursor()
            now = time.time()
            for user_id in user_ids:
                c.execute('''INSERT INTO notifications (user_id, message, type, sent_at) 
                             VALUES (?, ?, ?, ?)''', (user_id, message, notif_type, now))
            return len(user_ids)

    @staticmethod
    def get_unread(user_id, limit=20):
        with db_pool.get_connection() as conn:
            c = conn.cursor()
            c.execute('''SELECT id, message, type, sent_at FROM notifications 
                         WHERE user_id = ? AND read = 0 ORDER BY sent_at DESC LIMIT ?''', (user_id, limit))
            return [dict(row) for row in c.fetchall()]

    @staticmethod
    def mark_read(notification_id):
        with db_pool.get_connection() as conn:
            c = conn.cursor()
            c.execute("UPDATE notifications SET read = 1 WHERE id = ?", (notification_id,))

    @staticmethod
    def mark_all_read(user_id):
        with db_pool.get_connection() as conn:
            c = conn.cursor()
            c.execute("UPDATE notifications SET read = 1 WHERE user_id = ? AND read = 0", (user_id,))
            return c.rowcount

    @staticmethod
    def delete_old(days=30):
        with db_pool.get_connection() as conn:
            c = conn.cursor()
            cutoff = time.time() - (days * 86400)
            c.execute("DELETE FROM notifications WHERE read = 1 AND sent_at < ?", (cutoff,))
            return c.rowcount

# ==================== Processed Transaction Model ====================
class ProcessedTransactionModel:
    @staticmethod
    def add(tx_hash, lt, processed_at=None, from_address=None, value=None, comment=None):
        with db_pool.get_connection() as conn:
            c = conn.cursor()
            c.execute('''INSERT OR IGNORE INTO processed_transactions 
                         (tx_hash, lt, processed_at, from_address, value, comment) 
                         VALUES (?, ?, ?, ?, ?, ?)''',
                     (tx_hash, lt, processed_at or time.time(), from_address, value, comment))

    @staticmethod
    def exists(tx_hash):
        with db_pool.get_connection() as conn:
            c = conn.cursor()
            c.execute("SELECT 1 FROM processed_transactions WHERE tx_hash = ?", (tx_hash,))
            return c.fetchone() is not None

    @staticmethod
    def get_by_hash(tx_hash):
        with db_pool.get_connection() as conn:
            c = conn.cursor()
            c.execute("SELECT * FROM processed_transactions WHERE tx_hash = ?", (tx_hash,))
            row = c.fetchone()
            return dict(row) if row else None

    @staticmethod
    def get_recent(limit=100):
        with db_pool.get_connection() as conn:
            c = conn.cursor()
            c.execute("SELECT * FROM processed_transactions ORDER BY processed_at DESC LIMIT ?", (limit,))
            return [dict(row) for row in c.fetchall()]

    @staticmethod
    def clean_old(days=90):
        with db_pool.get_connection() as conn:
            c = conn.cursor()
            cutoff = time.time() - (days * 86400)
            c.execute("DELETE FROM processed_transactions WHERE processed_at < ?", (cutoff,))
            return c.rowcount
# ==================== Distribution Model ====================
class DistributionModel:
    @staticmethod
    def create(admin_id, dist_type, total_amount, coin, total_users, hours=24, title=None, description=None, is_random=0):
        with db_pool.get_connection() as conn:
            c = conn.cursor()
            created_at = time.time()
            expires_at = created_at + (hours * 3600)
            per_user = total_amount / total_users if not is_random else 0
            c.execute('''INSERT INTO distributions 
                         (admin_id, type, total_amount, coin, total_users, per_user_amount, status, created_at, expires_at, remaining_amount, title, description, is_random) 
                         VALUES (?, ?, ?, ?, ?, ?, 'active', ?, ?, ?, ?, ?, ?)''',
                     (admin_id, dist_type, total_amount, coin, total_users, per_user, created_at, expires_at, total_amount, title, description, is_random))
            return c.lastrowid

    @staticmethod
    def get_active():
        with db_pool.get_connection() as conn:
            c = conn.cursor()
            now = time.time()
            c.execute('''SELECT * FROM distributions WHERE status = 'active' AND expires_at > ? 
                         ORDER BY created_at DESC LIMIT 1''', (now,))
            row = c.fetchone()
            return dict(row) if row else None

    @staticmethod
    def get_all_active():
        with db_pool.get_connection() as conn:
            c = conn.cursor()
            now = time.time()
            c.execute('''SELECT * FROM distributions WHERE status = 'active' AND expires_at > ? 
                         ORDER BY created_at DESC''', (now,))
            return [dict(row) for row in c.fetchall()]

    @staticmethod
    def get_by_id(dist_id):
        with db_pool.get_connection() as conn:
            c = conn.cursor()
            c.execute("SELECT * FROM distributions WHERE id = ?", (dist_id,))
            row = c.fetchone()
            return dict(row) if row else None

    @staticmethod
    def get_by_admin(admin_id, limit=10):
        with db_pool.get_connection() as conn:
            c = conn.cursor()
            c.execute('''SELECT * FROM distributions WHERE admin_id = ? 
                         ORDER BY created_at DESC LIMIT ?''', (admin_id, limit))
            return [dict(row) for row in c.fetchall()]

    @staticmethod
    def update_remaining(dist_id, amount):
        with db_pool.get_connection() as conn:
            c = conn.cursor()
            c.execute('''UPDATE distributions SET remaining_amount = remaining_amount - ?, 
                         winners_count = winners_count + 1 WHERE id = ?''', (amount, dist_id))

    @staticmethod
    def expire(dist_id):
        with db_pool.get_connection() as conn:
            c = conn.cursor()
            c.execute("UPDATE distributions SET status = 'expired' WHERE id = ?", (dist_id,))

    @staticmethod
    def cancel(dist_id):
        dist = DistributionModel.get_by_id(dist_id)
        if dist and dist['status'] == 'active':
            with db_pool.get_connection() as conn:
                c = conn.cursor()
                if dist['remaining_amount'] > 0:
                    UserModel.update_balance(dist['admin_id'], dist['coin'], dist['remaining_amount'], 'add')
                    UserModel.update_frozen(dist['admin_id'], dist['coin'], dist['remaining_amount'], 'subtract')
                c.execute("UPDATE distributions SET status = 'cancelled' WHERE id = ?", (dist_id,))
            return True
        return False

    @staticmethod
    def get_random_amount(dist_id):
        dist = DistributionModel.get_by_id(dist_id)
        if not dist or dist['remaining_amount'] <= 0:
            return 0
        
        remaining = dist['remaining_amount']
        remaining_users = dist['total_users'] - dist['winners_count']
        
        if remaining_users <= 0:
            return 0
        
        min_amount = config.MIN_DISTRIBUTION_PER_USER
        max_amount = min(remaining / remaining_users * 2, remaining)
        
        if max_amount < min_amount:
            return remaining
        
        amount = random.uniform(min_amount, max_amount)
        return min(amount, remaining)

    @staticmethod
    def get_statistics():
        with db_pool.get_connection() as conn:
            c = conn.cursor()
            c.execute("SELECT COUNT(*) as count, COALESCE(SUM(total_amount),0) as total FROM distributions WHERE status='active'")
            active = c.fetchone()
            c.execute("SELECT COUNT(*) as count, COALESCE(SUM(total_amount),0) as total FROM distributions WHERE status='expired'")
            expired = c.fetchone()
            c.execute("SELECT COUNT(*) as count, COALESCE(SUM(total_amount),0) as total FROM distributions WHERE status='cancelled'")
            cancelled = c.fetchone()
            c.execute("SELECT type, COUNT(*) as count FROM distributions GROUP BY type")
            by_type = {row['type']: row['count'] for row in c.fetchall()}
            return {
                'active_count': active['count'],
                'active_total': active['total'],
                'expired_count': expired['count'],
                'expired_total': expired['total'],
                'cancelled_count': cancelled['count'],
                'cancelled_total': cancelled['total'],
                'by_type': by_type
            }

# ==================== Participant Model ====================
class ParticipantModel:
    @staticmethod
    def add(dist_id, user_id, username, amount):
        with db_pool.get_connection() as conn:
            c = conn.cursor()
            try:
                c.execute('''INSERT INTO distribution_participants 
                             (distribution_id, user_id, username, amount, claimed_at) 
                             VALUES (?, ?, ?, ?, ?)''', (dist_id, user_id, username, amount, time.time()))
                return True
            except sqlite3.IntegrityError:
                return False

    @staticmethod
    def check_participation(dist_id, user_id):
        with db_pool.get_connection() as conn:
            c = conn.cursor()
            c.execute("SELECT amount FROM distribution_participants WHERE distribution_id = ? AND user_id = ?", 
                     (dist_id, user_id))
            row = c.fetchone()
            return row['amount'] if row else None

    @staticmethod
    def get_participants(dist_id, limit=100):
        with db_pool.get_connection() as conn:
            c = conn.cursor()
            c.execute('''SELECT * FROM distribution_participants WHERE distribution_id = ? 
                         ORDER BY claimed_at DESC LIMIT ?''', (dist_id, limit))
            return [dict(row) for row in c.fetchall()]

    @staticmethod
    def get_participants_count(dist_id):
        with db_pool.get_connection() as conn:
            c = conn.cursor()
            c.execute("SELECT COUNT(*) FROM distribution_participants WHERE distribution_id = ?", (dist_id,))
            return c.fetchone()[0]

    @staticmethod
    def get_total_claimed(dist_id):
        with db_pool.get_connection() as conn:
            c = conn.cursor()
            c.execute("SELECT COALESCE(SUM(amount), 0) FROM distribution_participants WHERE distribution_id = ?", (dist_id,))
            return c.fetchone()[0]

    @staticmethod
    def get_user_participations(user_id, limit=10):
        with db_pool.get_connection() as conn:
            c = conn.cursor()
            c.execute('''SELECT d.*, dp.amount as claimed_amount, dp.claimed_at 
                         FROM distribution_participants dp 
                         JOIN distributions d ON dp.distribution_id = d.id 
                         WHERE dp.user_id = ? ORDER BY dp.claimed_at DESC LIMIT ?''', (user_id, limit))
            return [dict(row) for row in c.fetchall()]
# ==================== Gift Code Model ====================
class GiftCodeModel:
    @staticmethod
    def create(code, dist_id, amount, coin, created_by, expires_at):
        with db_pool.get_connection() as conn:
            c = conn.cursor()
            c.execute('''INSERT INTO gift_codes 
                         (code, distribution_id, amount, coin, created_by, expires_at, status) 
                         VALUES (?, ?, ?, ?, ?, ?, 'active')''',
                     (code, dist_id, amount, coin, created_by, expires_at))

    @staticmethod
    def get(code):
        with db_pool.get_connection() as conn:
            c = conn.cursor()
            c.execute("SELECT * FROM gift_codes WHERE code = ?", (code,))
            row = c.fetchone()
            return dict(row) if row else None

    @staticmethod
    def claim(code, user_id):
        with db_pool.get_connection() as conn:
            c = conn.cursor()
            c.execute('''UPDATE gift_codes SET claimed_by = ?, claimed_at = ?, status = 'used' 
                         WHERE code = ? AND status = 'active' AND expires_at > ?''',
                     (user_id, time.time(), code, time.time()))
            return c.rowcount > 0

    @staticmethod
    def get_by_creator(creator_id, limit=10):
        with db_pool.get_connection() as conn:
            c = conn.cursor()
            c.execute('''SELECT * FROM gift_codes WHERE created_by = ? 
                         ORDER BY expires_at DESC LIMIT ?''', (creator_id, limit))
            return [dict(row) for row in c.fetchall()]

    @staticmethod
    def get_by_user(user_id, limit=10):
        with db_pool.get_connection() as conn:
            c = conn.cursor()
            c.execute('''SELECT * FROM gift_codes WHERE claimed_by = ? 
                         ORDER BY claimed_at DESC LIMIT ?''', (user_id, limit))
            return [dict(row) for row in c.fetchall()]

    @staticmethod
    def get_active():
        with db_pool.get_connection() as conn:
            c = conn.cursor()
            now = time.time()
            c.execute('''SELECT * FROM gift_codes WHERE status = 'active' AND expires_at > ? 
                         ORDER BY expires_at ASC''', (now,))
            return [dict(row) for row in c.fetchall()]

    @staticmethod
    def expire_old():
        with db_pool.get_connection() as conn:
            c = conn.cursor()
            now = time.time()
            c.execute('''UPDATE gift_codes SET status = 'expired' 
                         WHERE status = 'active' AND expires_at < ?''', (now,))
            return c.rowcount

# ==================== Ad Model ====================
class AdModel:
    @staticmethod
    def create(user_id, username, days, price):
        with db_pool.get_connection() as conn:
            c = conn.cursor()
            c.execute('''INSERT INTO ads (user_id, username, days, price, status, created_at) 
                         VALUES (?, ?, ?, ?, 'pending', ?)''', (user_id, username, days, price, time.time()))
            return c.lastrowid

    @staticmethod
    def update_content(ad_id, ad_text=None, ad_photo=None, ad_link=None):
        with db_pool.get_connection() as conn:
            c = conn.cursor()
            updates = []
            params = []
            if ad_text:
                updates.append("ad_text = ?")
                params.append(ad_text)
            if ad_photo:
                updates.append("ad_photo = ?")
                params.append(ad_photo)
            if ad_link:
                updates.append("ad_link = ?")
                params.append(ad_link)
            if updates:
                query = f"UPDATE ads SET {', '.join(updates)} WHERE id = ?"
                params.append(ad_id)
                c.execute(query, params)

    @staticmethod
    def approve(ad_id):
        with db_pool.get_connection() as conn:
            c = conn.cursor()
            c.execute("SELECT days FROM ads WHERE id = ?", (ad_id,))
            row = c.fetchone()
            if row:
                days = row['days']
                now = time.time()
                start_time = now
                end_time = now + (days * 86400)
                c.execute('''UPDATE ads SET status = 'active', start_time = ?, end_time = ? WHERE id = ?''',
                         (start_time, end_time, ad_id))
                return True
            return False

    @staticmethod
    def reject(ad_id):
        with db_pool.get_connection() as conn:
            c = conn.cursor()
            c.execute("UPDATE ads SET status = 'rejected' WHERE id = ?", (ad_id,))
            return c.rowcount > 0

    @staticmethod
    def get_pending():
        with db_pool.get_connection() as conn:
            c = conn.cursor()
            c.execute('''SELECT * FROM ads WHERE status = 'pending' ORDER BY created_at ASC''')
            return [dict(row) for row in c.fetchall()]

    @staticmethod
    def get_active():
        with db_pool.get_connection() as conn:
            c = conn.cursor()
            now = time.time()
            c.execute('''SELECT * FROM ads WHERE status = 'active' AND start_time <= ? AND end_time > ? 
                         ORDER BY start_time DESC''', (now, now))
            return [dict(row) for row in c.fetchall()]

    @staticmethod
    def increment_views(ad_id):
        with db_pool.get_connection() as conn:
            c = conn.cursor()
            c.execute("UPDATE ads SET views = views + 1 WHERE id = ?", (ad_id,))

    @staticmethod
    def increment_clicks(ad_id):
        with db_pool.get_connection() as conn:
            c = conn.cursor()
            c.execute("UPDATE ads SET clicks = clicks + 1 WHERE id = ?", (ad_id,))

    @staticmethod
    def get_statistics():
        with db_pool.get_connection() as conn:
            c = conn.cursor()
            c.execute("SELECT COUNT(*) FROM ads WHERE status='pending'")
            pending = c.fetchone()[0]
            c.execute("SELECT COUNT(*) FROM ads WHERE status='active'")
            active = c.fetchone()[0]
            c.execute("SELECT COUNT(*) FROM ads WHERE status='completed'")
            completed = c.fetchone()[0]
            c.execute("SELECT COALESCE(SUM(views),0), COALESCE(SUM(clicks),0) FROM ads")
            stats = c.fetchone()
            return {
                'pending': pending,
                'active': active,
                'completed': completed,
                'total_views': stats[0],
                'total_clicks': stats[1]
            }
# ==================== Decorators ====================
def admin_only(func):
    @wraps(func)
    def wrapper(message, *args, **kwargs):
        if message.from_user.id not in config.ADMIN_IDS:
            bot.reply_to(message, "❌ This command is for administrators only.")
            return
        return func(message, *args, **kwargs)
    return wrapper

def log_command(func):
    @wraps(func)
    def wrapper(message, *args, **kwargs):
        command = func.__name__
        user_id = message.from_user.id
        username = message.from_user.username or "No username"
        logger.info(f"Command: /{command} from user {user_id} (@{username})")
        perf_monitor.log_command(command, user_id)
        try:
            return func(message, *args, **kwargs)
        except Exception as e:
            logger.error(f"Error in /{command}: {e}")
            logger.exception("Full traceback:")
            perf_monitor.log_error()
            bot.reply_to(message, f"❌ An error occurred: {str(e)}")
            return
    return wrapper

def rate_limit(seconds=2):
    def decorator(func):
        last_called = {}
        @wraps(func)
        def wrapper(message, *args, **kwargs):
            user_id = message.from_user.id
            now = time.time()
            if user_id in last_called and now - last_called[user_id] < seconds:
                bot.reply_to(message, f"⏳ Please wait {seconds} seconds between commands.")
                return
            last_called[user_id] = now
            return func(message, *args, **kwargs)
        return wrapper
    return decorator

# ==================== Keyboard Layouts ====================
def main_keyboard():
    markup = types.ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
    buttons = [
        "Wallet", "Deposit", "Withdraw", "Convert", 
        "Referrals", "Transactions", "Prices", "Top Users",
        "Airdrop", "Giveaway", "Gift", "Notifications", 
        "Help", "Support"
    ]
    markup.add(*[types.KeyboardButton(b) for b in buttons])
    return markup

def copy_keyboard(wallet_address, user_id):
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.add(
        types.InlineKeyboardButton("📋 Copy Wallet Address", callback_data="copy_wallet"),
        types.InlineKeyboardButton("📋 Copy Your ID", callback_data=f"copy_id_{user_id}")
    )
    return markup

def admin_keyboard():
    markup = types.InlineKeyboardMarkup(row_width=2)
    buttons = [
        types.InlineKeyboardButton("📊 Stats", callback_data="admin_stats"),
        types.InlineKeyboardButton("⏳ Pending", callback_data="admin_pending"),
        types.InlineKeyboardButton("🔍 Find User", callback_data="admin_find_user"),
        types.InlineKeyboardButton("💰 Add Balance", callback_data="admin_add_balance"),
        types.InlineKeyboardButton("📢 Broadcast", callback_data="admin_broadcast"),
        types.InlineKeyboardButton("🎁 Airdrop", callback_data="admin_airdrop"),
        types.InlineKeyboardButton("🎲 Giveaway", callback_data="admin_giveaway"),
        types.InlineKeyboardButton("📈 Charts", callback_data="admin_charts"),
        types.InlineKeyboardButton("💾 Backup", callback_data="admin_backup"),
        types.InlineKeyboardButton("🔄 Restart", callback_data="admin_restart"),
        types.InlineKeyboardButton("📋 Logs", callback_data="admin_logs"),
        types.InlineKeyboardButton("❌ Close", callback_data="admin_close")
    ]
    markup.add(*buttons)
    return markup

def notification_keyboard():
    markup = types.InlineKeyboardMarkup(row_width=2)
    buttons = [
        types.InlineKeyboardButton("✅ Enable", callback_data="notify_on"),
        types.InlineKeyboardButton("❌ Disable", callback_data="notify_off"),
        types.InlineKeyboardButton("📨 Show", callback_data="notify_show"),
        types.InlineKeyboardButton("🗑 Clear", callback_data="notify_clear")
    ]
    markup.add(*buttons)
    return markup

def confirmation_keyboard(action, data):
    markup = types.InlineKeyboardMarkup(row_width=2)
    buttons = [
        types.InlineKeyboardButton("✅ Confirm", callback_data=f"confirm_{action}_{data}"),
        types.InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_{action}")
    ]
    markup.add(*buttons)
    return markup

def user_actions_keyboard(user_id):
    markup = types.InlineKeyboardMarkup(row_width=2)
    buttons = [
        types.InlineKeyboardButton("💰 Balance", callback_data=f"user_balance_{user_id}"),
        types.InlineKeyboardButton("📜 History", callback_data=f"user_history_{user_id}"),
        types.InlineKeyboardButton("🔨 Ban", callback_data=f"user_ban_{user_id}"),
        types.InlineKeyboardButton("✅ Unban", callback_data=f"user_unban_{user_id}"),
        types.InlineKeyboardButton("➕ Add", callback_data=f"user_add_{user_id}"),
        types.InlineKeyboardButton("➖ Remove", callback_data=f"user_remove_{user_id}")
    ]
    markup.add(*buttons)
    return markup

def pagination_keyboard(base_data, page, total_pages):
    markup = types.InlineKeyboardMarkup(row_width=3)
    buttons = []
    if page > 1:
        buttons.append(types.InlineKeyboardButton("◀️ Prev", callback_data=f"{base_data}_page_{page-1}"))
    buttons.append(types.InlineKeyboardButton(f"📄 {page}/{total_pages}", callback_data="noop"))
    if page < total_pages:
        buttons.append(types.InlineKeyboardButton("Next ▶️", callback_data=f"{base_data}_page_{page+1}"))
    markup.add(*buttons)
    return markup

def coin_selection_keyboard(action):
    markup = types.InlineKeyboardMarkup(row_width=3)
    buttons = []
    for coin in config.SUPPORTED_COINS:
        buttons.append(types.InlineKeyboardButton(coin, callback_data=f"{action}_{coin}"))
    markup.add(*buttons)
    return markup
# ==================== Start Command ====================
@bot.message_handler(commands=['start'])
@log_command
@rate_limit(2)
def start_command(message):
    user_id = message.from_user.id
    username = message.from_user.username
    first_name = message.from_user.first_name
    last_name = message.from_user.last_name
    language_code = message.from_user.language_code
    
    if UserModel.is_banned(user_id):
        bot.reply_to(message, "❌ Your account has been banned. Contact support if you believe this is a mistake.")
        return
    
    user = UserModel.get(user_id, True, username, first_name, last_name, language_code)
    
    args = message.text.split()
    if len(args) > 1 and args[1].isdigit():
        referrer_id = int(args[1])
        if referrer_id != user_id and not UserModel.is_banned(referrer_id):
            if not ReferralModel.get_by_referred(user_id):
                if ReferralModel.create(referrer_id, user_id):
                    reward = 0.0001
                    UserModel.update_balance(referrer_id, 'TON', reward, 'add')
                    TransactionModel.create(
                        referrer_id, user_id, reward, 'TON', 'referral_reward',
                        memo=f"New referral: {user_id}"
                    )
                    try:
                        bot.send_message(
                            referrer_id,
                            f"🎉 New Referral!\n\n"
                            f"User {get_user_mention(user_id, username, first_name)} joined using your link!\n"
                            f"You received {reward} TON as reward."
                        )
                        NotificationModel.create(referrer_id, f"New referral! You earned {reward} TON", 'referral')
                    except:
                        pass
                    logger.info(f"Referral: {referrer_id} -> {user_id}")
    
    total_users = UserModel.get_count()
    
    welcome_text = (
        f"🌟 Welcome to the Grab Wallet!\n\n"
        f"Hello {first_name or 'User'}!\n\n"
        f"📌 Your ID: `{user_id}`\n"
        f"👥 Total Users: {total_users:,}\n\n"
        f"✨ What can you do?\n"
        f"• Check your wallet balance\n"
        f"• Deposit cryptocurrency\n"
        f"• Withdraw to external wallet\n"
        f"• Convert between coins\n"
        f"• Earn from referrals\n"
        f"• Create your own airdrops and giveaways\n"
        f"• Participate in airdrops and giveaways\n"
        f"• Send gifts to friends\n\n"
        f"Use the buttons below to navigate!"
    )
    
    bot.send_message(message.chat.id, welcome_text, reply_markup=main_keyboard(), parse_mode='Markdown')
    
    if user and user.get('joined_date', 0) > time.time() - 60:
        for admin_id in config.ADMIN_IDS:
            try:
                bot.send_message(
                    admin_id,
                    f"🆕 New User Joined!\n\n"
                    f"ID: {user_id}\n"
                    f"Username: @{username or 'None'}\n"
                    f"Name: {first_name or 'Unknown'}\n"
                    f"Total Users: {total_users:,}"
                )
            except:
                pass

# ==================== ID Command ====================
@bot.message_handler(commands=['id'])
@log_command
def id_command(message):
    user_id = message.from_user.id
    if UserModel.is_banned(user_id):
        bot.reply_to(message, "❌ Your account is banned.")
        return
    
    if message.reply_to_message:
        target = message.reply_to_message.from_user
        text = f"👤 User Information\n\n"
        text += f"Name: {target.first_name or 'Unknown'}\n"
        text += f"ID: {target.id}\n"
        if target.username:
            text += f"Username: @{target.username}\n"
        target_id = target.id
    else:
        user_data = UserModel.get(user_id)
        text = f"👤 Your Information\n\n"
        text += f"ID: {user_id}\n"
        if message.from_user.username:
            text += f"Username: @{message.from_user.username}\n"
        if user_data:
            if user_data.get('joined_date'):
                text += f"Joined: {format_time(user_data['joined_date'])}\n"
            if user_data.get('last_active'):
                text += f"Last Active: {get_time_ago(user_data['last_active'])}\n"
        target_id = user_id
    
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("📋 Copy ID", callback_data=f"copy_id_{user_id}"))
    bot.reply_to(message, text, reply_markup=markup)

# ==================== Copy Callbacks ====================
@bot.callback_query_handler(func=lambda call: call.data == "copy_wallet")
def copy_wallet_callback(call):
    bot.answer_callback_query(call.id, text=config.OFFICIAL_WALLET, show_alert=True)

@bot.callback_query_handler(func=lambda call: call.data.startswith("copy_id_"))
def copy_id_callback(call):
    user_id = call.data.split("_")[2]
    bot.answer_callback_query(call.id, text=user_id, show_alert=True)

@bot.callback_query_handler(func=lambda call: call.data.startswith("copy_referral_"))
def copy_referral_callback(call):
    user_id = call.data.split("_")[2]
    bot_username = bot.get_me().username
    referral_link = f"https://t.me/{bot_username}?start={user_id}"
    bot.answer_callback_query(call.id, text=referral_link, show_alert=True)

@bot.callback_query_handler(func=lambda call: call.data == "noop")
def noop_callback(call):
    bot.answer_callback_query(call.id)

# ==================== Wallet Command ====================
@bot.message_handler(commands=['wallet', 'balance'])
@log_command
def wallet_command(message):
    user_id = message.from_user.id
    if UserModel.is_banned(user_id):
        bot.reply_to(message, "❌ Your account is banned.")
        return
    
    user = UserModel.get(user_id)
    if not user:
        bot.reply_to(message, "Please use /start first")
        return
    
    referral_count = ReferralModel.get_count(user_id)
    referral_earnings = ReferralModel.get_total_reward(user_id)
    
    text = f"💰 Wallet Information\n\n"
    text += f"📌 ID: `{user_id}`\n"
    text += f"👥 Referrals: {referral_count} (Earned: {referral_earnings:.6f} TON)\n\n"
    text += f"💵 Balances:\n"
    text += f"TON: {user['bal_ton']:.4f}\n"
    text += f"USD: {user['bal_usd']:.4f}\n"
    text += f"NOT: {user['bal_not']:.4f}\n"
    text += f"DOGS: {user['bal_dogs']:.4f}\n"
    text += f"HMSTR: {user['bal_hmstr']:.4f}\n"
    
    frozen_total = (user['frozen_ton'] + user['frozen_usd'] + user['frozen_not'] + 
                    user['frozen_dogs'] + user['frozen_hmstr'])
    if frozen_total > 0:
        text += f"\n⚠️ Frozen:\n"
        if user['frozen_ton'] > 0:
            text += f"TON: {user['frozen_ton']:.4f}\n"
        if user['frozen_usd'] > 0:
            text += f"USD: {user['frozen_usd']:.4f}\n"
        if user['frozen_not'] > 0:
            text += f"NOT: {user['frozen_not']:.4f}\n"
    
    text += f"\n📊 Statistics:\n"
    text += f"Total Deposits: {user['total_deposits']:.4f} TON\n"
    text += f"Total Withdrawals: {user['total_withdrawals']:.4f} TON\n"
    
    # Add conversion rates
    rates = exchange_manager.get_all_rates()
    if rates:
        text += f"\n💱 Current Rates:\n"
        for coin, rate in rates.items():
            text += f"1 TON = {rate:.2f} {coin}\n"
    
    bot.reply_to(message, text, parse_mode='Markdown')
# ==================== Deposit Command ====================
@bot.message_handler(commands=['deposit'])
@log_command
def deposit_command(message):
    user_id = message.from_user.id
    if UserModel.is_banned(user_id):
        bot.reply_to(message, "❌ Your account is banned.")
        return
    
    user = UserModel.get(user_id)
    if not user:
        bot.reply_to(message, "Please use /start first")
        return
    
    ton_usd_rate = exchange_manager.get_rate('TON', 'USD') or 2.5
    
    text = (
        f"📥 Deposit Instructions\n\n"
        f"Step 1: Send TON to the address below\n"
        f"Step 2: Include the memo exactly as shown\n"
        f"Step 3: Wait for blockchain confirmation\n\n"
        f"📤 Wallet Address (click to copy):\n"
        f"`{config.OFFICIAL_WALLET}`\n\n"
        f"🔑 Your Memo (click to copy):\n"
        f"`{user_id}`\n\n"
        f"📊 Details:\n"
        f"Minimum Deposit: {config.MIN_DEPOSIT} TON\n"
        f"Current Rate: 1 TON ≈ {ton_usd_rate:.2f} USD\n"
        f"Processing Time: 1-5 minutes\n\n"
        f"⚠️ WARNING:\n"
        f"• You MUST include the memo\n"
        f"• Deposits without memo will be LOST\n"
        f"• Only send TON to this address\n"
        f"• Check address carefully before sending"
    )
    
    bot.send_message(message.chat.id, text, parse_mode='Markdown', 
                    reply_markup=copy_keyboard(config.OFFICIAL_WALLET, user_id))

# ==================== Withdraw Command ====================
@bot.message_handler(commands=['withdraw'])
@log_command
@rate_limit(5)
def withdraw_command(message):
    user_id = message.from_user.id
    if UserModel.is_banned(user_id):
        bot.reply_to(message, "❌ Your account is banned.")
        return
    
    user = UserModel.get(user_id)
    if not user:
        bot.reply_to(message, "Please use /start first")
        return
    
    lines = message.text.strip().split('\n')
    if len(lines) < 3:
        bot.reply_to(
            message,
            "❌ Invalid Format\n\n"
            "📌 Correct Usage:\n"
            "/withdraw\n"
            "[amount] [coin]\n"
            "[wallet address]\n"
            "[memo] (optional)\n\n"
            "📌 Examples:\n"
            "/withdraw\n"
            "1 TON\n"
            f"{config.OFFICIAL_WALLET}\n\n"
            f"ℹ️ Minimum withdrawal: {config.MIN_WITHDRAWAL} TON\n"
            f"💰 Fee: {config.WITHDRAWAL_FEE_PERCENT*100}% + {config.WITHDRAWAL_FEE_FIXED} TON"
        )
        return
    
    try:
        amount_coin = lines[1].strip().split()
        if len(amount_coin) != 2:
            bot.reply_to(message, "❌ Second line must contain amount and coin only")
            return
        
        amount = float(amount_coin[0])
        coin = amount_coin[1].upper()
        
        if coin not in config.SUPPORTED_COINS:
            bot.reply_to(message, f"❌ Unsupported coin. Supported: {', '.join(config.SUPPORTED_COINS)}")
            return
        
        if amount < config.MIN_WITHDRAWAL:
            bot.reply_to(message, f"❌ Minimum withdrawal is {config.MIN_WITHDRAWAL} {coin}")
            return
        
        wallet = lines[2].strip()
        if len(wallet) < 10 or len(wallet) > 100:
            bot.reply_to(message, "❌ Invalid wallet address format")
            return
        
        memo = lines[3].strip() if len(lines) == 4 else None
        
        balance = user.get(f'bal_{coin.lower()}', 0)
        fee_amount = (amount * config.WITHDRAWAL_FEE_PERCENT) + config.WITHDRAWAL_FEE_FIXED
        receive_amount = amount - fee_amount
        
        if receive_amount < 0.000001:
            bot.reply_to(message, f"❌ Amount too small, fees are higher than the withdrawal amount")
            return
        
        if balance < amount:
            bot.reply_to(
                message,
                f"❌ Insufficient Balance\n\n"
                f"You have: {balance:.4f} {coin}\n"
                f"Required: {amount:.4f} {coin}\n"
                f"You will receive: {receive_amount:.4f} {coin}\n"
                f"Fees: {fee_amount:.4f} {coin}"
            )
            return
        
        markup = confirmation_keyboard('withdraw', f"{amount}_{coin}_{wallet}_{memo or ''}")
        text = (
            f"📤 Withdrawal Confirmation\n\n"
            f"Withdrawal Amount: {amount:.4f} {coin}\n"
            f"Fees ({config.WITHDRAWAL_FEE_PERCENT*100}% + {config.WITHDRAWAL_FEE_FIXED}): {fee_amount:.4f} {coin}\n"
            f"You will receive: {receive_amount:.4f} {coin}\n"
            f"To Wallet: {wallet[:15]}...{wallet[-5:]}\n"
        )
        if memo:
            text += f"Memo: {memo}\n"
        text += f"\nPlease confirm to proceed"
        
        bot.reply_to(message, text, reply_markup=markup)
        
    except ValueError:
        bot.reply_to(message, "❌ Invalid amount format")
    except Exception as e:
        logger.error(f"Withdraw error: {e}")
        bot.reply_to(message, f"❌ Error: {str(e)}")
# ==================== Send Command ====================
@bot.message_handler(commands=['send'])
@log_command
@rate_limit(2)
def send_command(message):
    user_id = message.from_user.id
    if UserModel.is_banned(user_id):
        bot.reply_to(message, "❌ Your account is banned.")
        return
    
    user = UserModel.get(user_id)
    if not user:
        bot.reply_to(message, "Please use /start first")
        return
    
    parts = message.text.split()
    
    # Check if replying to a message
    if message.reply_to_message and len(parts) >= 3:
        target = message.reply_to_message.from_user
        target_id = target.id
        amount = float(parts[1])
        coin = parts[2].upper()
        memo = ' '.join(parts[3:]) if len(parts) > 3 else None
    elif len(parts) >= 4:
        amount = float(parts[1])
        coin = parts[2].upper()
        recipient = parts[3]
        memo = ' '.join(parts[4:]) if len(parts) > 4 else None
        
        # Find recipient
        if recipient.startswith('@'):
            username = recipient[1:].lower()
            to_id = find_user_by_identifier(username)
        else:
            try:
                to_id = int(recipient)
            except:
                to_id = None
        
        if not to_id:
            bot.reply_to(message, f"❌ User {recipient} not found")
            return
        target_id = to_id
    else:
        bot.reply_to(
            message,
            "❌ Invalid Format\n\n"
            "📌 Usage:\n"
            "/send [amount] [coin] [@username or ID] (message)\n"
            "Or reply to a message: /send [amount] [coin] (message)\n\n"
            "📌 Examples:\n"
            f"/send 0.001 TON @username\n"
            f"/send 1 TON 123456789 Thank you\n\n"
            f"ℹ️ Minimum send: {config.MIN_SEND} TON\n"
            f"💰 Fee: 0 TON"
        )
        return
    
    try:
        amount = float(amount)
        coin = coin.upper()
        
        if coin not in config.SUPPORTED_COINS:
            bot.reply_to(message, f"❌ Unsupported coin. Supported: {', '.join(config.SUPPORTED_COINS)}")
            return
        
        if amount < config.MIN_SEND:
            bot.reply_to(message, f"❌ Minimum send amount is {config.MIN_SEND} {coin}")
            return
        
        if amount <= 0:
            bot.reply_to(message, "❌ Amount must be positive")
            return
        
        if target_id == user_id:
            bot.reply_to(message, "❌ Cannot send to yourself")
            return
        
        recipient_user = UserModel.get(target_id)
        if not recipient_user:
            bot.reply_to(message, f"❌ User {target_id} not registered")
            return
        
        if recipient_user.get('banned', 0):
            bot.reply_to(message, "❌ Cannot send to banned user")
            return
        
        balance = user.get(f'bal_{coin.lower()}', 0)
        if balance < amount:
            bot.reply_to(
                message,
                f"❌ Insufficient Balance\n\n"
                f"You have: {balance:.4f} {coin}\n"
                f"Required: {amount:.4f} {coin}"
            )
            return
        
        markup = confirmation_keyboard('send', f"{amount}_{coin}_{target_id}_{memo or ''}")
        text = (
            f"📨 Send Confirmation\n\n"
            f"Amount: {amount:.4f} {coin}\n"
            f"To: {get_user_mention(target_id, recipient_user.get('username'), recipient_user.get('first_name'))}\n"
            f"Fee: 0 TON\n"
        )
        if memo:
            text += f"Message: {memo}\n"
        text += f"\nPlease confirm to proceed"
        
        bot.reply_to(message, text, parse_mode='Markdown', reply_markup=markup)
        
    except ValueError:
        bot.reply_to(message, "❌ Invalid amount format")
    except Exception as e:
        logger.error(f"Send error: {e}")
        bot.reply_to(message, f"❌ Error: {str(e)}")

# ==================== Convert Command ====================
@bot.message_handler(commands=['convert'])
@log_command
def convert_command(message):
    user_id = message.from_user.id
    if UserModel.is_banned(user_id):
        bot.reply_to(message, "❌ Your account is banned.")
        return
    
    user = UserModel.get(user_id)
    if not user:
        bot.reply_to(message, "Please use /start first")
        return
    
    parts = message.text.split()
    if len(parts) != 4:
        bot.reply_to(
            message,
            "❌ Invalid Format\n\n"
            "📌 Usage:\n"
            "/convert [amount] [from_coin] [to_coin]\n\n"
            "📌 Examples:\n"
            "/convert 10 TON USD\n"
            "/convert 5 NOT TON"
        )
        return
    
    try:
        amount = float(parts[1])
        from_coin = parts[2].upper()
        to_coin = parts[3].upper()
        
        if from_coin not in config.SUPPORTED_COINS:
            bot.reply_to(message, f"❌ Unsupported from_coin. Supported: {', '.join(config.SUPPORTED_COINS)}")
            return
        
        if to_coin not in config.SUPPORTED_COINS:
            bot.reply_to(message, f"❌ Unsupported to_coin. Supported: {', '.join(config.SUPPORTED_COINS)}")
            return
        
        if from_coin == to_coin:
            bot.reply_to(message, "❌ Coins are the same")
            return
        
        if amount <= 0:
            bot.reply_to(message, "❌ Amount must be positive")
            return
        
        balance = user.get(f'bal_{from_coin.lower()}', 0)
        if balance < amount:
            bot.reply_to(
                message,
                f"❌ Insufficient Balance\n\n"
                f"You have: {balance:.4f} {from_coin}"
            )
            return
        
        rate = exchange_manager.get_rate(from_coin, to_coin)
        if not rate:
            bot.reply_to(message, f"❌ Cannot get rate for {from_coin}/{to_coin}")
            return
        
        converted = amount * rate
        fee = converted * 0.005  # 0.5% fee
        final_amount = converted - fee
        
        markup = confirmation_keyboard(
            'convert', 
            f"{amount}_{from_coin}_{to_coin}_{final_amount:.8f}_{fee:.8f}"
        )
        text = (
            f"💱 Conversion Preview\n\n"
            f"From: {amount:.4f} {from_coin}\n"
            f"Rate: 1 {from_coin} = {rate:.6f} {to_coin}\n"
            f"Before Fee: {converted:.6f} {to_coin}\n"
            f"Fee (0.5%): {fee:.6f} {to_coin}\n"
            f"You Receive: {final_amount:.6f} {to_coin}\n\n"
            f"Confirm to proceed?"
        )
        
        bot.reply_to(message, text, reply_markup=markup)
        
    except ValueError:
        bot.reply_to(message, "❌ Invalid amount format")
    except Exception as e:
        logger.error(f"Convert error: {e}")
        bot.reply_to(message, f"❌ Error: {str(e)}")
# ==================== Transactions Command ====================
@bot.message_handler(commands=['transactions', 'history'])
@log_command
def transactions_command(message):
    user_id = message.from_user.id
    if UserModel.is_banned(user_id):
        bot.reply_to(message, "❌ Your account is banned.")
        return
    
    target_id = user_id
    
    parts = message.text.split()
    if len(parts) == 2 and message.from_user.id in config.ADMIN_IDS:
        identifier = parts[1]
        found_id = find_user_by_identifier(identifier)
        if found_id:
            target_id = found_id
        else:
            bot.reply_to(message, "❌ User not found")
            return
    
    transactions = TransactionModel.get_user_transactions(target_id, limit=20)
    
    if not transactions:
        bot.reply_to(message, f"📭 No transactions found")
        return
    
    total_sent = sum(tx['amount'] for tx in transactions if tx['from_id'] == target_id and tx['type'] == 'send')
    total_received = sum(tx['amount'] for tx in transactions if tx['to_id'] == target_id and tx['type'] == 'send')
    total_deposits = sum(tx['amount'] for tx in transactions if tx['to_id'] == target_id and tx['type'] == 'deposit')
    total_withdrawals = sum(tx['amount'] for tx in transactions if tx['from_id'] == target_id and tx['type'] == 'withdraw')
    
    text = f"📜 Transaction History\n\n"
    if target_id != user_id:
        text += f"User: {target_id}\n\n"
    
    text += f"📊 Summary:\n"
    text += f"Total Transactions: {len(transactions)}\n"
    text += f"Deposits: {total_deposits:.4f} TON\n"
    text += f"Withdrawals: {total_withdrawals:.4f} TON\n"
    text += f"Sent: {total_sent:.4f} TON\n"
    text += f"Received: {total_received:.4f} TON\n\n"
    
    text += f"📋 Last 20 Transactions:\n"
    emoji_map = {
        'deposit': '📥',
        'withdraw': '📤',
        'send': '📨',
        'convert': '💱',
        'tip': '💌',
        'referral_reward': '🎁',
        'airdrop': '🎯',
        'giveaway': '🎲',
        'gift': '🎀'
    }
    
    for i, tx in enumerate(transactions[:20], 1):
        emoji = emoji_map.get(tx['type'], '📌')
        if tx['type'] == 'deposit':
            desc = f"{emoji} Deposit: {tx['amount']:.4f} {tx['coin']}"
        elif tx['type'] == 'withdraw':
            desc = f"{emoji} Withdraw: {tx['amount']:.4f} {tx['coin']}"
        elif tx['type'] == 'send':
            if tx['from_id'] == target_id:
                desc = f"{emoji} Sent to {tx['to_id']}: {tx['amount']:.4f} {tx['coin']}"
            else:
                desc = f"{emoji} Received from {tx['from_id']}: {tx['amount']:.4f} {tx['coin']}"
        elif tx['type'] == 'convert':
            desc = f"{emoji} Convert: {tx['amount']:.4f} {tx['coin']}"
        elif tx['type'] == 'tip':
            if tx['from_id'] == target_id:
                desc = f"{emoji} Tip to {tx['to_id']}: {tx['amount']:.4f} {tx['coin']}"
            else:
                desc = f"{emoji} Tip from {tx['from_id']}: {tx['amount']:.4f} {tx['coin']}"
        elif tx['type'] == 'referral_reward':
            desc = f"{emoji} Referral: {tx['amount']:.4f} {tx['coin']}"
        elif tx['type'] in ['airdrop', 'giveaway', 'gift']:
            desc = f"{emoji} {tx['type'].capitalize()}: {tx['amount']:.4f} {tx['coin']}"
        else:
            desc = f"{emoji} {tx['type']}: {tx['amount']:.4f} {tx['coin']}"
        
        status = "✅" if tx['status'] == 'completed' else "⏳"
        time_str = tx['date'].split()[1] if ' ' in tx['date'] else tx['date']
        text += f"{i}. {desc} {status} ({time_str})\n"
        
        if len(text) > 3500:
            text += "... (more transactions hidden)"
            break
    
    bot.reply_to(message, text)

# ==================== Referral Command ====================
@bot.message_handler(commands=['referral', 'ref'])
@log_command
def referral_command(message):
    user_id = message.from_user.id
    if UserModel.is_banned(user_id):
        bot.reply_to(message, "❌ Your account is banned.")
        return
    
    user = UserModel.get(user_id)
    if not user:
        bot.reply_to(message, "Please use /start first")
        return
    
    bot_username = bot.get_me().username
    referral_link = f"https://t.me/{bot_username}?start={user_id}"
    
    referrals = ReferralModel.get_by_referrer(user_id)
    count = len(referrals)
    total_earned = ReferralModel.get_total_reward(user_id)
    
    text = (
        f"🤝 Referral Program\n\n"
        f"🔗 Your Referral Link:\n"
        f"{referral_link}\n\n"
        f"📊 Statistics:\n"
        f"Total Referrals: {count}\n"
        f"Total Earned: {total_earned:.6f} TON\n\n"
        f"🎁 Rewards:\n"
        f"• Per Referral: 0.0001 TON\n"
        f"• Referral must use /start with your link\n"
        f"• No limit on referrals\n"
        f"• Instant credit to your wallet\n\n"
    )
    
    if referrals:
        text += "📋 Recent Referrals:\n"
        for ref in referrals[:5]:
            ref_user = UserModel.get(ref['referred_id'])
            if ref_user:
                name = ref_user.get('username') or ref_user.get('first_name') or f"User {ref['referred_id']}"
                date_str = datetime.fromtimestamp(ref['timestamp']).strftime('%Y-%m-%d')
                text += f"• {name} - {date_str}\n"
    
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("📋 Copy Link", callback_data=f"copy_referral_{user_id}"))
    markup.add(types.InlineKeyboardButton("📊 Referral Tree", callback_data=f"ref_tree_{user_id}"))
    
    bot.send_message(message.chat.id, text, reply_markup=markup)
# ==================== Airdrop Command ====================
@bot.message_handler(commands=['airdrop'])
@log_command
@rate_limit(5)
def airdrop_command(message):
    user_id = message.from_user.id
    is_admin = user_id in config.ADMIN_IDS
    
    if UserModel.is_banned(user_id):
        bot.reply_to(message, "❌ Your account is banned.")
        return
    
    user = UserModel.get(user_id)
    if not user:
        bot.reply_to(message, "Please use /start first")
        return
    
    parts = message.text.split()
    
    # Show active airdrop
    if len(parts) == 1:
        active_dist = DistributionModel.get_active()
        if not active_dist or active_dist['type'] != 'airdrop':
            bot.reply_to(message, "📭 No active airdrop at the moment. Check back later!")
            return
        
        participation = ParticipantModel.check_participation(active_dist['id'], user_id)
        per_user = active_dist['total_amount'] / active_dist['total_users']
        title_text = f" - {active_dist['title']}" if active_dist.get('title') else ""
        desc_text = f"\n\n{active_dist['description']}" if active_dist.get('description') else ""
        time_left = active_dist['expires_at'] - time.time()
        hours_left = int(time_left // 3600)
        minutes_left = int((time_left % 3600) // 60)
        
        text = (
            f"🎯 Active Airdrop{title_text}\n\n"
            f"💰 Total Amount: {active_dist['total_amount']:.6f} {active_dist['coin']}\n"
            f"👥 Total Users: {active_dist['total_users']}\n"
            f"💵 Per User: {per_user:.6f} {active_dist['coin']}\n"
            f"📊 Remaining: {active_dist['remaining_amount']:.6f} {active_dist['coin']}\n"
            f"✅ Claimed: {active_dist['winners_count']}/{active_dist['total_users']}{desc_text}\n\n"
            f"⏳ Expires in: {hours_left}h {minutes_left}m\n"
        )
        
        if participation:
            text += f"\n✅ You have already claimed: {participation:.6f} {active_dist['coin']}"
        else:
            text += f"\n📌 Use /grab to claim your airdrop!"
        
        bot.reply_to(message, text)
        return
    
    # Create new airdrop
    if len(parts) == 4:
        try:
            amount = float(parts[1])
            coin = parts[2].upper()
            total_users = int(parts[3])
            
            if coin not in config.SUPPORTED_COINS:
                bot.reply_to(message, f"❌ Unsupported coin. Supported: {', '.join(config.SUPPORTED_COINS)}")
                return
            
            if amount < config.MIN_DISTRIBUTION_TOTAL:
                bot.reply_to(message, f"❌ Minimum total amount is {config.MIN_DISTRIBUTION_TOTAL} {coin}")
                return
            
            if total_users < config.MIN_DISTRIBUTION_USERS:
                bot.reply_to(message, f"❌ Minimum users is {config.MIN_DISTRIBUTION_USERS}")
                return
            
            if total_users > config.MAX_DISTRIBUTION_USERS:
                bot.reply_to(message, f"❌ Maximum users is {config.MAX_DISTRIBUTION_USERS}")
                return
            
            per_user = amount / total_users
            if per_user < config.MIN_DISTRIBUTION_PER_USER:
                bot.reply_to(message, f"❌ Amount per user too small\n"
                              f"Minimum per user: {config.MIN_DISTRIBUTION_PER_USER} {coin}\n"
                              f"Your per user: {per_user:.8f} {coin}")
                return
            
            if is_admin:
                # Admin: use bot balance
                admin_id = config.ADMIN_IDS[0]
                UserModel.update_frozen(admin_id, coin, amount, 'add')
                
                dist_id = DistributionModel.create(
                    admin_id=admin_id,
                    dist_type='airdrop',
                    total_amount=amount,
                    coin=coin,
                    total_users=total_users,
                    hours=24,
                    title=f"Airdrop by Admin",
                    description=f"Admin airdrop for {total_users} users",
                    is_random=0
                )
                
                bot.reply_to(message, f"✅ Airdrop Created Successfully (Admin)\n\n"
                              f"💰 Total Amount: {amount} {coin}\n"
                              f"👥 Total Users: {total_users}\n"
                              f"💵 Per User: {per_user:.6f} {coin}\n"
                              f"⏳ Duration: 24 hours\n\n"
                              f"📌 Users can claim with: /grab")
                
                # Notify channel
                send_to_channel(
                    "New Airdrop",
                    f"Admin created a new airdrop!\n"
                    f"Amount: {amount} {coin}\n"
                    f"Users: {total_users}\n"
                    f"Per user: {per_user:.6f} {coin}",
                    msg_type='airdrop'
                )
            else:
                # Regular user: deduct from balance
                balance = user.get(f'bal_{coin.lower()}', 0)
                if balance < amount:
                    bot.reply_to(message, f"❌ Insufficient balance\n\n"
                                  f"You have: {balance} {coin}\n"
                                  f"Need: {amount} {coin}")
                    return
                
                # Deduct and freeze balance
                UserModel.update_balance(user_id, coin, amount, 'subtract')
                UserModel.update_frozen(user_id, coin, amount, 'add')
                
                dist_id = DistributionModel.create(
                    admin_id=user_id,
                    dist_type='airdrop',
                    total_amount=amount,
                    coin=coin,
                    total_users=total_users,
                    hours=24,
                    title=f"Airdrop by User {user_id}",
                    description=f"Join and grab your share!",
                    is_random=0
                )
                
                bot.reply_to(message, f"✅ Airdrop Created Successfully!\n\n"
                              f"💰 Total Amount: {amount} {coin}\n"
                              f"👥 Total Users: {total_users}\n"
                              f"💵 Per User: {per_user:.6f} {coin}\n"
                              f"⏳ Duration: 24 hours\n\n"
                              f"⚠️ Amount frozen: {amount} {coin}\n"
                              f"💵 Your new balance: {balance - amount:.4f} {coin}\n\n"
                              f"📌 Users can claim with: /grab")
                
                # Notify admins
                for admin_id in config.ADMIN_IDS:
                    try:
                        bot.send_message(admin_id, f"🆕 New User Airdrop\n"
                                          f"User: {user_id} (@{user.get('username', 'N/A')})\n"
                                          f"Amount: {amount} {coin}\n"
                                          f"Users: {total_users}\n"
                                          f"Per User: {per_user:.6f} {coin}")
                    except:
                        pass
                
                # Notify channel
                send_to_channel(
                    "New User Airdrop",
                    f"User {user_id} created a new airdrop!\n"
                    f"Amount: {amount} {coin}\n"
                    f"Users: {total_users}",
                    msg_type='airdrop'
                )
                
                logger.info(f"Airdrop created by {user_id}: {amount} {coin} for {total_users} users")
                
        except ValueError:
            bot.reply_to(message, "❌ Invalid amount or total users format")
        except Exception as e:
            logger.error(f"Create airdrop error: {e}")
            bot.reply_to(message, f"❌ Error: {str(e)}")
    else:
        bot.reply_to(message, "❌ Invalid Format\n\n"
                      "📌 To create an airdrop (equal distribution):\n"
                      "/airdrop [amount] [coin] [total_users]\n\n"
                      "📌 Examples:\n"
                      "/airdrop 10 TON 1000\n"
                      "/airdrop 1 NOT 500\n\n"
                      f"ℹ️ Minimum total: {config.MIN_DISTRIBUTION_TOTAL} {coin}\n"
                      f"ℹ️ Minimum per user: {config.MIN_DISTRIBUTION_PER_USER} {coin}\n"
                      f"ℹ️ Max users: {config.MAX_DISTRIBUTION_USERS}\n\n"
                      "📋 To check active airdrop:\n"
                      "/airdrop")

# ==================== Giveaway Command ====================
@bot.message_handler(commands=['giveaway'])
@log_command
@rate_limit(5)
def giveaway_command(message):
    user_id = message.from_user.id
    is_admin = user_id in config.ADMIN_IDS
    
    if UserModel.is_banned(user_id):
        bot.reply_to(message, "❌ Your account is banned.")
        return
    
    user = UserModel.get(user_id)
    if not user:
        bot.reply_to(message, "Please use /start first")
        return
    
    parts = message.text.split()
    
    # Show active giveaway
    if len(parts) == 1:
        active_dist = DistributionModel.get_active()
        if not active_dist or active_dist['type'] != 'giveaway':
            bot.reply_to(message, "📭 No active giveaway at the moment. Check back later!")
            return
        
        participation = ParticipantModel.check_participation(active_dist['id'], user_id)
        title_text = f" - {active_dist['title']}" if active_dist.get('title') else ""
        desc_text = f"\n\n{active_dist['description']}" if active_dist.get('description') else ""
        time_left = active_dist['expires_at'] - time.time()
        hours_left = int(time_left // 3600)
        minutes_left = int((time_left % 3600) // 60)
        
        text = (
            f"🎲 Active Giveaway{title_text}\n\n"
            f"💰 Total Amount: {active_dist['total_amount']:.6f} {active_dist['coin']}\n"
            f"👥 Total Winners: {active_dist['total_users']}\n"
            f"📊 Remaining: {active_dist['remaining_amount']:.6f} {active_dist['coin']}\n"
            f"✅ Claimed: {active_dist['winners_count']}/{active_dist['total_users']}{desc_text}\n\n"
            f"⏳ Expires in: {hours_left}h {minutes_left}m\n"
            f"🎲 Type: Random amounts (luck-based)\n"
        )
        
        if participation:
            text += f"\n✅ You have already claimed: {participation:.6f} {active_dist['coin']}"
        else:
            text += f"\n📌 Use /grab to try your luck in the giveaway!"
        
        bot.reply_to(message, text)
        return
    
    # Create new giveaway
    if len(parts) == 4:
        try:
            amount = float(parts[1])
            coin = parts[2].upper()
            total_users = int(parts[3])
            
            if coin not in config.SUPPORTED_COINS:
                bot.reply_to(message, f"❌ Unsupported coin. Supported: {', '.join(config.SUPPORTED_COINS)}")
                return
            
            if amount < config.MIN_DISTRIBUTION_TOTAL:
                bot.reply_to(message, f"❌ Minimum total amount is {config.MIN_DISTRIBUTION_TOTAL} {coin}")
                return
            
            if total_users < config.MIN_DISTRIBUTION_USERS:
                bot.reply_to(message, f"❌ Minimum users is {config.MIN_DISTRIBUTION_USERS}")
                return
            
            if total_users > config.MAX_DISTRIBUTION_USERS:
                bot.reply_to(message, f"❌ Maximum users is {config.MAX_DISTRIBUTION_USERS}")
                return
            
            if is_admin:
                # Admin: use bot balance
                admin_id = config.ADMIN_IDS[0]
                UserModel.update_frozen(admin_id, coin, amount, 'add')
                
                dist_id = DistributionModel.create(
                    admin_id=admin_id,
                    dist_type='giveaway',
                    total_amount=amount,
                    coin=coin,
                    total_users=total_users,
                    hours=24,
                    title=f"Giveaway by Admin",
                    description=f"Random amounts for {total_users} winners",
                    is_random=1
                )
                
                bot.reply_to(message, f"✅ Giveaway Created Successfully (Admin)\n\n"
                              f"💰 Total Amount: {amount} {coin}\n"
                              f"👥 Total Winners: {total_users}\n"
                              f"🎲 Type: Random amounts\n"
                              f"⏳ Duration: 24 hours\n\n"
                              f"📌 Users can try with: /grab")
                
                # Notify channel
                send_to_channel(
                    "New Giveaway",
                    f"Admin created a new giveaway!\n"
                    f"Amount: {amount} {coin}\n"
                    f"Winners: {total_users}\n"
                    f"Type: Random amounts",
                    msg_type='giveaway'
                )
            else:
                # Regular user: deduct from balance
                balance = user.get(f'bal_{coin.lower()}', 0)
                if balance < amount:
                    bot.reply_to(message, f"❌ Insufficient balance\n\n"
                                  f"You have: {balance} {coin}\n"
                                  f"Need: {amount} {coin}")
                    return
                
                UserModel.update_balance(user_id, coin, amount, 'subtract')
                UserModel.update_frozen(user_id, coin, amount, 'add')
                
                dist_id = DistributionModel.create(
                    admin_id=user_id,
                    dist_type='giveaway',
                    total_amount=amount,
                    coin=coin,
                    total_users=total_users,
                    hours=24,
                    title=f"Giveaway by User {user_id}",
                    description=f"Try your luck! Random amounts",
                    is_random=1
                )
                
                bot.reply_to(message, f"✅ Giveaway Created Successfully!\n\n"
                              f"💰 Total Amount: {amount} {coin}\n"
                              f"👥 Total Winners: {total_users}\n"
                              f"🎲 Type: Random amounts\n"
                              f"⏳ Duration: 24 hours\n\n"
                              f"⚠️ Amount frozen: {amount} {coin}\n"
                              f"💵 Your new balance: {balance - amount:.4f} {coin}\n\n"
                              f"📌 Users can try with: /grab")
                
                # Notify admins
                for admin_id in config.ADMIN_IDS:
                    try:
                        bot.send_message(admin_id, f"🆕 New User Giveaway\n"
                                          f"User: {user_id} (@{user.get('username', 'N/A')})\n"
                                          f"Amount: {amount} {coin}\n"
                                          f"Winners: {total_users}\n"
                                          f"Type: Random amounts")
                    except:
                        pass
                
                # Notify channel
                send_to_channel(
                    "New User Giveaway",
                    f"User {user_id} created a new giveaway!\n"
                    f"Amount: {amount} {coin}\n"
                    f"Winners: {total_users}",
                    msg_type='giveaway'
                )
                
                logger.info(f"Giveaway created by {user_id}: {amount} {coin} for {total_users} winners")
                
        except ValueError:
            bot.reply_to(message, "❌ Invalid amount or total users format")
        except Exception as e:
            logger.error(f"Create giveaway error: {e}")
            bot.reply_to(message, f"❌ Error: {str(e)}")
    else:
        bot.reply_to(message, "❌ Invalid Format\n\n"
                      "📌 To create a giveaway (random distribution):\n"
                      "/giveaway [amount] [coin] [total_winners]\n\n"
                      "📌 Examples:\n"
                      "/giveaway 10 TON 1000\n"
                      "/giveaway 1 NOT 500\n\n"
                      f"ℹ️ Minimum total: {config.MIN_DISTRIBUTION_TOTAL} {coin}\n"
                      f"ℹ️ Max winners: {config.MAX_DISTRIBUTION_USERS}\n\n"
                      "📋 To check active giveaway:\n"
                      "/giveaway")
# ==================== Grab Command ====================
@bot.message_handler(commands=['grab'])
@log_command
@rate_limit(3)
def grab_command(message):
    user_id = message.from_user.id
    if UserModel.is_banned(user_id):
        bot.reply_to(message, "❌ Your account is banned.")
        return
    
    user = UserModel.get(user_id)
    if not user:
        bot.reply_to(message, "Please use /start first")
        return
    
    active_dist = DistributionModel.get_active()
    if not active_dist:
        bot.reply_to(message, "📭 No active distribution to grab!")
        return
    
    # Check if already participated
    participation = ParticipantModel.check_participation(active_dist['id'], user_id)
    if participation:
        bot.reply_to(message, f"⚠️ You have already claimed {participation:.6f} {active_dist['coin']} from this {active_dist['type']}!")
        return
    
    if active_dist['remaining_amount'] <= 0.000001:
        bot.reply_to(message, f"❌ This {active_dist['type']} has no remaining funds.")
        DistributionModel.expire(active_dist['id'])
        return
    
    if active_dist['winners_count'] >= active_dist['total_users']:
        bot.reply_to(message, f"❌ All winners have already claimed from this {active_dist['type']}.")
        DistributionModel.expire(active_dist['id'])
        return
    
    # Calculate amount
    if active_dist['is_random']:
        # Random distribution for giveaway
        amount = DistributionModel.get_random_amount(active_dist['id'])
        if amount <= 0.000001:
            bot.reply_to(message, "❌ Error calculating random amount. Please try again.")
            return
    else:
        # Equal distribution for airdrop
        amount = active_dist['per_user_amount']
        if amount > active_dist['remaining_amount']:
            amount = active_dist['remaining_amount']
    
    # Add balance to user
    UserModel.update_balance(user_id, active_dist['coin'], amount, 'add')
    DistributionModel.update_remaining(active_dist['id'], amount)
    ParticipantModel.add(active_dist['id'], user_id, user.get('username'), amount)
    
    title_text = f" {active_dist.get('title', '')}" if active_dist.get('title') else ""
    desc_text = f"\n\n{active_dist.get('description')}" if active_dist.get('description') else ""
    dist_type = "Giveaway" if active_dist['is_random'] else "Airdrop"
    
    remaining = active_dist['remaining_amount'] - amount
    winners = active_dist['winners_count'] + 1
    
    text = (
        f"✅ Successfully Grabbed from {dist_type}!{title_text}\n\n"
        f"💰 You received: {amount:.6f} {active_dist['coin']}{desc_text}\n\n"
        f"📊 Remaining: {remaining:.6f} / {active_dist['total_amount']:.6f} {active_dist['coin']}\n"
        f"👥 Winners: {winners} / {active_dist['total_users']}\n"
        f"⏳ Expires: {datetime.fromtimestamp(active_dist['expires_at']).strftime('%Y-%m-%d %H:%M')}"
    )
    
    bot.reply_to(message, text)
    NotificationModel.create(user_id, f"{dist_type}: You received {amount:.6f} {active_dist['coin']}", active_dist['type'])
    logger.info(f"User {user_id} grabbed from {active_dist['type']} {active_dist['id']}: {amount} {active_dist['coin']}")
    
    # If distribution ended, notify creator
    if remaining <= 0.000001 or winners >= active_dist['total_users']:
        creator_id = active_dist['admin_id']
        try:
            bot.send_message(creator_id, f"✅ Your {dist_type} has ended!\n\n"
                              f"Total: {active_dist['total_amount']} {active_dist['coin']}\n"
                              f"Winners: {winners}/{active_dist['total_users']}\n"
                              f"All funds have been distributed.")
        except:
            pass
        DistributionModel.expire(active_dist['id'])
        
        # Notify channel
        send_to_channel(
            f"{dist_type} Ended",
            f"The {dist_type} has ended!\n"
            f"Total: {active_dist['total_amount']} {active_dist['coin']}\n"
            f"Winners: {winners}",
            msg_type='info'
        )

# ==================== Gift Command ====================
@bot.message_handler(commands=['gift'])
@log_command
@rate_limit(3)
def gift_command(message):
    user_id = message.from_user.id
    if UserModel.is_banned(user_id):
        bot.reply_to(message, "❌ Your account is banned.")
        return
    
    user = UserModel.get(user_id)
    if not user:
        bot.reply_to(message, "Please use /start first")
        return
    
    parts = message.text.split()
    if len(parts) < 4:
        bot.reply_to(
            message,
            "❌ Invalid Format\n\n"
            "📌 Usage:\n"
            "/gift [amount] [coin] [@username or ID] (message)\n\n"
            "📌 Examples:\n"
            "/gift 1 TON @friend Happy Birthday!\n"
            "/gift 0.5 NOT 123456789 Thank you"
        )
        return
    
    try:
        amount = float(parts[1])
        coin = parts[2].upper()
        recipient = parts[3]
        memo = ' '.join(parts[4:]) if len(parts) > 4 else "Gift for you!"
        
        if coin not in config.SUPPORTED_COINS:
            bot.reply_to(message, f"❌ Unsupported coin. Supported: {', '.join(config.SUPPORTED_COINS)}")
            return
        
        if amount <= 0:
            bot.reply_to(message, "❌ Amount must be positive")
            return
        
        # Find recipient
        if recipient.startswith('@'):
            username = recipient[1:].lower()
            to_id = find_user_by_identifier(username)
        else:
            try:
                to_id = int(recipient)
            except:
                to_id = None
        
        if not to_id:
            bot.reply_to(message, f"❌ User {recipient} not found")
            return
        
        if to_id == user_id:
            bot.reply_to(message, "❌ Cannot send gift to yourself")
            return
        
        recipient_user = UserModel.get(to_id)
        if not recipient_user:
            bot.reply_to(message, f"❌ User {to_id} not registered")
            return
        
        if recipient_user.get('banned', 0):
            bot.reply_to(message, "❌ Cannot send gift to banned user")
            return
        
        balance = user.get(f'bal_{coin.lower()}', 0)
        if balance < amount:
            bot.reply_to(
                message,
                f"❌ Insufficient Balance\n\n"
                f"You have: {balance:.4f} {coin}"
            )
            return
        
        # Generate gift code
        code = generate_gift_code(12)
        expires_at = time.time() + (7 * 86400)  # 7 days
        
        GiftCodeModel.create(
            code=code,
            dist_id=0,
            amount=amount,
            coin=coin,
            created_by=user_id,
            expires_at=expires_at
        )
        
        # Deduct balance
        UserModel.update_balance(user_id, coin, amount, 'subtract')
        
        # Send gift code to recipient
        gift_text = (
            f"🎁 You received a gift!\n\n"
            f"From: {get_user_mention(user_id, user.get('username'), user.get('first_name'))}\n"
            f"Amount: {amount} {coin}\n"
            f"Message: {memo}\n\n"
            f"Use this code to claim:\n"
            f"`{code}`\n\n"
            f"⏳ Expires in 7 days\n\n"
            f"To claim: /open {code}"
        )
        
        try:
            bot.send_message(to_id, gift_text, parse_mode='Markdown')
            NotificationModel.create(to_id, f"You received a gift of {amount} {coin}!", 'gift')
            
            # Confirm to sender
            bot.reply_to(message, f"✅ Gift sent successfully!\n\n"
                          f"To: {get_user_mention(to_id, recipient_user.get('username'), recipient_user.get('first_name'))}\n"
                          f"Amount: {amount} {coin}\n"
                          f"Code: `{code}`\n\n"
                          f"The code has been sent to the recipient.")
            
            # Transaction record
            TransactionModel.create(
                from_id=user_id,
                to_id=to_id,
                amount=amount,
                coin=coin,
                tx_type='gift',
                memo=f"Gift code: {code}"
            )
            
            logger.info(f"Gift created by {user_id} for {to_id}: {amount} {coin}")
            
        except Exception as e:
            # Refund if failed to send
            UserModel.update_balance(user_id, coin, amount, 'add')
            GiftCodeModel.claim(code, 0)  # Invalidate code
            bot.reply_to(message, f"❌ Failed to send gift: {str(e)}")
            
    except ValueError:
        bot.reply_to(message, "❌ Invalid amount format")
    except Exception as e:
        logger.error(f"Gift error: {e}")
        bot.reply_to(message, f"❌ Error: {str(e)}")

# ==================== Open Command ====================
@bot.message_handler(commands=['open'])
@log_command
def open_command(message):
    user_id = message.from_user.id
    if UserModel.is_banned(user_id):
        bot.reply_to(message, "❌ Your account is banned.")
        return
    
    user = UserModel.get(user_id)
    if not user:
        bot.reply_to(message, "Please use /start first")
        return
    
    parts = message.text.split()
    if len(parts) != 2:
        bot.reply_to(message, "❌ Usage: /open [gift_code]")
        return
    
    code = parts[1].strip().upper()
    gift = GiftCodeModel.get(code)
    
    if not gift:
        bot.reply_to(message, "❌ Invalid gift code")
        return
    
    if gift['status'] != 'active':
        bot.reply_to(message, "❌ This gift code has already been used or expired")
        return
    
    if gift['expires_at'] < time.time():
        bot.reply_to(message, "❌ This gift code has expired")
        GiftCodeModel.claim(code, 0)  # Mark as expired
        return
    
    # Claim gift
    if GiftCodeModel.claim(code, user_id):
        # Add balance to user
        UserModel.update_balance(user_id, gift['coin'], gift['amount'], 'add')
        
        # Get creator info
        creator = UserModel.get(gift['created_by'])
        
        text = (
            f"✅ Gift Claimed Successfully!\n\n"
            f"From: {get_user_mention(gift['created_by'], creator.get('username'), creator.get('first_name'))}\n"
            f"Amount: {gift['amount']} {gift['coin']}\n\n"
            f"Added to your balance!"
        )
        
        bot.reply_to(message, text, parse_mode='Markdown')
        
        # Notify creator
        try:
            bot.send_message(
                gift['created_by'],
                f"🎁 Your gift has been claimed!\n\n"
                f"Claimed by: {get_user_mention(user_id, user.get('username'), user.get('first_name'))}\n"
                f"Amount: {gift['amount']} {gift['coin']}\n"
                f"Code: {code}"
            )
        except:
            pass
        
        # Transaction record
        TransactionModel.create(
            from_id=gift['created_by'],
            to_id=user_id,
            amount=gift['amount'],
            coin=gift['coin'],
            tx_type='gift',
            memo=f"Gift code: {code}"
        )
        
        logger.info(f"Gift {code} claimed by {user_id}")
    else:
        bot.reply_to(message, "❌ Failed to claim gift. Please try again.")
# ==================== My Gifts Command ====================
@bot.message_handler(commands=['my_gifts'])
@log_command
def my_gifts_command(message):
    user_id = message.from_user.id
    if UserModel.is_banned(user_id):
        bot.reply_to(message, "❌ Your account is banned.")
        return
    
    # Gifts sent
    sent_gifts = GiftCodeModel.get_by_creator(user_id, limit=10)
    # Gifts received
    received_gifts = GiftCodeModel.get_by_user(user_id, limit=10)
    
    text = f"🎁 Your Gifts\n\n"
    
    if sent_gifts:
        text += f"📤 Sent Gifts:\n"
        for gift in sent_gifts:
            status = "✅ Claimed" if gift['claimed_by'] else "⏳ Pending"
            if gift['claimed_by']:
                claimer = UserModel.get(gift['claimed_by'])
                claimer_name = claimer.get('username') or claimer.get('first_name') or f"User {gift['claimed_by']}"
                text += f"• {gift['amount']} {gift['coin']} - {status} by {claimer_name}\n"
            else:
                expires = datetime.fromtimestamp(gift['expires_at']).strftime('%Y-%m-%d')
                text += f"• {gift['amount']} {gift['coin']} - {status} (Expires: {expires})\n"
        text += "\n"
    
    if received_gifts:
        text += f"📥 Received Gifts:\n"
        for gift in received_gifts:
            sender = UserModel.get(gift['created_by'])
            sender_name = sender.get('username') or sender.get('first_name') or f"User {gift['created_by']}"
            date = datetime.fromtimestamp(gift['claimed_at']).strftime('%Y-%m-%d')
            text += f"• {gift['amount']} {gift['coin']} from {sender_name} on {date}\n"
    
    if not sent_gifts and not received_gifts:
        text += "No gifts found.\n\nUse /gift to send a gift to someone!"
    
    bot.reply_to(message, text)

# ==================== Tip Command ====================
@bot.message_handler(commands=['tip'])
@log_command
def tip_command(message):
    user_id = message.from_user.id
    if UserModel.is_banned(user_id):
        bot.reply_to(message, "❌ Your account is banned.")
        return
    
    if not message.reply_to_message:
        bot.reply_to(
            message,
            "❌ Must reply to a message\n\n"
            "Reply to someone's message to send them a tip:\n"
            "/tip 1 TON\n"
            "/tip 0.5 NOT"
        )
        return
    
    target = message.reply_to_message.from_user
    target_id = target.id
    
    if target_id == user_id:
        bot.reply_to(message, "❌ Cannot tip yourself")
        return
    
    if target.is_bot:
        bot.reply_to(message, "❌ Cannot tip bots")
        return
    
    parts = message.text.split()
    if len(parts) != 3:
        bot.reply_to(
            message,
            "❌ Invalid Format\n\n"
            "Usage: /tip [amount] [coin]\n"
            "Example: /tip 1 TON"
        )
        return
    
    try:
        amount = float(parts[1])
        coin = parts[2].upper()
        
        if coin not in config.SUPPORTED_COINS:
            bot.reply_to(message, f"❌ Unsupported coin. Supported: {', '.join(config.SUPPORTED_COINS)}")
            return
        
        if amount <= 0:
            bot.reply_to(message, "❌ Amount must be positive")
            return
        
        user = UserModel.get(user_id)
        if not user:
            bot.reply_to(message, "Please use /start first")
            return
        
        balance = user.get(f'bal_{coin.lower()}', 0)
        if balance < amount:
            bot.reply_to(
                message,
                f"❌ Insufficient Balance\n\n"
                f"You have: {balance:.4f} {coin}\n"
                f"Required: {amount:.4f} {coin}"
            )
            return
        
        recipient = UserModel.get(target_id, True, target.username, target.first_name, target.last_name)
        
        UserModel.update_balance(user_id, coin, amount, 'subtract')
        UserModel.update_balance(target_id, coin, amount, 'add')
        
        tx_id = TransactionModel.create(user_id, target_id, amount, coin, 'tip')
        
        bot.reply_to(
            message,
            f"💌 Tip Sent!\n\n"
            f"Amount: {amount:.4f} {coin}\n"
            f"To: {get_user_mention(target_id, target.username, target.first_name)}\n"
            f"Your new balance: {balance - amount:.4f} {coin}"
        )
        
        try:
            bot.send_message(
                target_id,
                f"💌 You received a tip!\n\n"
                f"From: {get_user_mention(user_id, message.from_user.username, message.from_user.first_name)}\n"
                f"Amount: {amount:.4f} {coin}"
            )
            NotificationModel.create(target_id, f"You received a tip of {amount} {coin}!", 'tip')
        except:
            pass
            
    except ValueError:
        bot.reply_to(message, "❌ Invalid amount format")
    except Exception as e:
        logger.error(f"Tip error: {e}")
        bot.reply_to(message, f"❌ Error: {str(e)}")

# ==================== Top Command ====================
@bot.message_handler(commands=['top'])
@log_command
def top_command(message):
    user_id = message.from_user.id
    if UserModel.is_banned(user_id):
        bot.reply_to(message, "❌ Your account is banned.")
        return
    
    parts = message.text.split()
    category = parts[1].upper() if len(parts) > 1 else 'TON'
    
    if category == 'REFERRALS':
        top_users = UserModel.get_top_by_referrals(20)
        title = "Top 20 by Referrals"
    elif category == 'DEPOSITS':
        top_users = UserModel.get_top_by_deposits(20)
        title = "Top 20 by Total Deposits"
    else:
        if category not in config.SUPPORTED_COINS:
            category = 'TON'
        top_users = UserModel.get_top_by_balance(category, 20)
        title = f"Top 20 by {category} Balance"
    
    if not top_users:
        bot.reply_to(message, "📭 No users found")
        return
    
    text = f"🏆 {title}\n\n"
    medals = ['🥇', '🥈', '🥉']
    
    for i, user in enumerate(top_users, 1):
        medal = medals[i-1] if i <= 3 else f"{i}."
        if user['username']:
            name = f"@{user['username']}"
        elif user['first_name']:
            name = user['first_name'][:20]
        else:
            name = f"User {user['user_id']}"
        
        text += f"{medal} {name}\n"
        if category == 'REFERRALS':
            text += f" 👥 {user['referrals']} referrals (${user['earnings']:.2f})\n"
        elif category == 'DEPOSITS':
            text += f" 💰 {user['deposits']:.2f} TON\n"
        else:
            if message.from_user.id in config.ADMIN_IDS:
                text += f" {user['balance']:.2f} {category}\n"
            else:
                text += f" (Hidden)\n"
        
        if len(text) > 3500:
            text += "... (more users hidden)"
            break
    
    markup = types.InlineKeyboardMarkup(row_width=3)
    buttons = [
        types.InlineKeyboardButton("TON", callback_data="top_TON"),
        types.InlineKeyboardButton("Referrals", callback_data="top_REFERRALS"),
        types.InlineKeyboardButton("Deposits", callback_data="top_DEPOSITS")
    ]
    markup.add(*buttons)
    
    bot.send_message(message.chat.id, text, reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data.startswith("top_"))
def top_callback(call):
    category = call.data.replace("top_", "")
    
    if category == 'REFERRALS':
        top_users = UserModel.get_top_by_referrals(20)
        title = "Top 20 by Referrals"
    elif category == 'DEPOSITS':
        top_users = UserModel.get_top_by_deposits(20)
        title = "Top 20 by Total Deposits"
    else:
        top_users = UserModel.get_top_by_balance(category, 20)
        title = f"Top 20 by {category} Balance"
    
    if not top_users:
        bot.edit_message_text("📭 No users found", call.message.chat.id, call.message.message_id)
        return
    
    text = f"🏆 {title}\n\n"
    medals = ['🥇', '🥈', '🥉']
    
    for i, user in enumerate(top_users, 1):
        medal = medals[i-1] if i <= 3 else f"{i}."
        if user['username']:
            name = f"@{user['username']}"
        elif user['first_name']:
            name = user['first_name'][:20]
        else:
            name = f"User {user['user_id']}"
        
        text += f"{medal} {name}\n"
        if category == 'REFERRALS':
            text += f" 👥 {user['referrals']} referrals (${user['earnings']:.2f})\n"
        elif category == 'DEPOSITS':
            text += f" 💰 {user['deposits']:.2f} TON\n"
        else:
            if call.from_user.id in config.ADMIN_IDS:
                text += f" {user['balance']:.2f} {category}\n"
            else:
                text += f" (Hidden)\n"
        
        if len(text) > 3500:
            text += "... (more users hidden)"
            break
    
    markup = types.InlineKeyboardMarkup(row_width=3)
    buttons = [
        types.InlineKeyboardButton("TON", callback_data="top_TON"),
        types.InlineKeyboardButton("Referrals", callback_data="top_REFERRALS"),
        types.InlineKeyboardButton("Deposits", callback_data="top_DEPOSITS")
    ]
    markup.add(*buttons)
    
    bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup)
    bot.answer_callback_query(call.id)

# ==================== Users Command ====================
@bot.message_handler(commands=['users'])
@log_command
def users_count_command(message):
    user_id = message.from_user.id
    if UserModel.is_banned(user_id):
        bot.reply_to(message, "❌ Your account is banned.")
        return
    
    stats = UserModel.get_statistics()
    
    text = (
        f"👥 User Statistics\n\n"
        f"📊 Totals:\n"
        f"Total Users: {stats['total']:,}\n"
        f"Banned Users: {stats['banned']:,}\n"
        f"New Today: +{stats['new_today']:,}\n"
        f"New This Week: +{stats['new_week']:,}\n"
        f"New This Month: +{stats['new_month']:,}\n\n"
        f"📈 Activity:\n"
        f"Active (24h): {stats['active_24h']:,}\n"
        f"Active (7d): {stats['active_7d']:,}\n\n"
        f"💰 Total Balances:\n"
        f"TON: {stats['total_ton']:.2f} TON\n"
        f"USD: {stats['total_usd']:.2f} USD\n"
        f"NOT: {stats['total_not']:.2f} NOT\n"
        f"DOGS: {stats['total_dogs']:.2f} DOGS\n"
        f"HMSTR: {stats['total_hmstr']:.2f} HMSTR"
    )
    
    bot.reply_to(message, text)
# ==================== Notify Command ====================
@bot.message_handler(commands=['notify'])
@log_command
def notify_command(message):
    user_id = message.from_user.id
    if UserModel.is_banned(user_id):
        bot.reply_to(message, "❌ Your account is banned.")
        return
    
    user = UserModel.get(user_id)
    if not user:
        bot.reply_to(message, "Please use /start first")
        return
    
    enabled = user.get('notify_enabled', 1)
    status = "✅ Enabled" if enabled else "❌ Disabled"
    unread_notifs = NotificationModel.get_unread(user_id)
    unread_count = len(unread_notifs)
    
    text = (
        f"🔔 Notification Settings\n\n"
        f"Current status: {status}\n"
        f"Unread notifications: {unread_count}\n\n"
        f"Choose an option:"
    )
    
    bot.send_message(message.chat.id, text, reply_markup=notification_keyboard())

@bot.callback_query_handler(func=lambda call: call.data == "notify_show")
def show_notifications_callback(call):
    user_id = call.from_user.id
    notifs = NotificationModel.get_unread(user_id, limit=20)
    
    if notifs:
        text = "📨 Your Notifications:\n\n"
        for notif in notifs:
            date_str = datetime.fromtimestamp(notif['sent_at']).strftime('%Y-%m-%d %H:%M')
            emoji = {
                'info': 'ℹ️',
                'deposit': '💰',
                'withdraw': '💸',
                'send': '📨',
                'tip': '💌',
                'airdrop': '🎯',
                'giveaway': '🎲',
                'gift': '🎁',
                'referral': '🤝'
            }.get(notif['type'], '📌')
            text += f"{emoji} {notif['message']} ({date_str})\n\n"
            NotificationModel.mark_read(notif['id'])
    else:
        text = "📭 No new notifications"
    
    bot.send_message(user_id, text)
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda call: call.data == "notify_on")
def enable_notifications_callback(call):
    user_id = call.from_user.id
    with db_pool.get_connection() as conn:
        c = conn.cursor()
        c.execute("UPDATE users SET notify_enabled = 1 WHERE user_id = ?", (user_id,))
    bot.answer_callback_query(call.id, "✅ Notifications enabled")
    bot.edit_message_text("🔔 Notifications Enabled", call.message.chat.id, call.message.message_id)

@bot.callback_query_handler(func=lambda call: call.data == "notify_off")
def disable_notifications_callback(call):
    user_id = call.from_user.id
    with db_pool.get_connection() as conn:
        c = conn.cursor()
        c.execute("UPDATE users SET notify_enabled = 0 WHERE user_id = ?", (user_id,))
    bot.answer_callback_query(call.id, "❌ Notifications disabled")
    bot.edit_message_text("🔕 Notifications Disabled", call.message.chat.id, call.message.message_id)

@bot.callback_query_handler(func=lambda call: call.data == "notify_clear")
def clear_notifications_callback(call):
    user_id = call.from_user.id
    count = NotificationModel.mark_all_read(user_id)
    bot.answer_callback_query(call.id, f"✅ Cleared {count} notifications")
    bot.edit_message_text(f"🗑 Notifications Cleared\n\n{count} notifications marked as read.", 
                         call.message.chat.id, call.message.message_id)

# ==================== Price Command ====================
@bot.message_handler(commands=['prices'])
@log_command
def price_command(message):
    user_id = message.from_user.id
    if UserModel.is_banned(user_id):
        bot.reply_to(message, "❌ Your account is banned.")
        return
    
    rates = exchange_manager.get_all_rates()
    
    text = "💱 Current Exchange Rates\n\n"
    text += f"1 TON\n"
    text += f"USD: {rates.get('USD', 0.0):.4f} USD\n"
    text += f"NOT: {rates.get('NOT', 0.0):.2f} NOT\n"
    text += f"DOGS: {rates.get('DOGS', 0.0):.2f} DOGS\n"
    text += f"HMSTR: {rates.get('HMSTR', 0.0):.2f} HMSTR\n\n"
    text += "🔄 Last updated: Just now"
    
    bot.reply_to(message, text)

# ==================== Support Command ====================
@bot.message_handler(commands=['support'])
@log_command
def support_command(message):
    user_id = message.from_user.id
    if UserModel.is_banned(user_id):
        bot.reply_to(message, "❌ Your account is banned.")
        return
    
    text = (
        f"🆘 Technical Support\n\n"
        f"👤 Admin: @kamatshow\n"
        f"📌 Your ID: `{user_id}`\n\n"
        f"❓ Common Issues:\n"
        f"• Deposits not showing? Wait 5 minutes and check /wallet\n"
        f"• Withdrawal stuck? Contact admin with your withdrawal ID\n"
        f"• Forgot memo? Contact support immediately\n"
        f"• Gift code not working? Check if expired\n\n"
        f"⏳ Response Time: Usually within 24 hours\n\n"
        f"Click below to contact admin:"
    )
    
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("📨 Contact Admin", url="https://t.me/kamatshow"))
    markup.add(types.InlineKeyboardButton("📢 Channel", url="https://t.me/Code_Notification"))
    
    bot.reply_to(message, text, parse_mode='Markdown', reply_markup=markup)

# ==================== Help Command ====================
@bot.message_handler(commands=['help'])
@log_command
def help_command(message):
    user_id = message.from_user.id
    if UserModel.is_banned(user_id):
        bot.reply_to(message, "❌ Your account is banned.")
        return
    
    text = (
        "📚 Command List\n\n"
        "👤 General Commands:\n"
        "/start - Start the bot\n"
        "/id - Show your ID\n"
        "/wallet - Check your balance\n"
        "/deposit - Deposit instructions\n"
        "/withdraw - Withdraw funds\n"
        "/send - Send to another user\n"
        "/convert - Convert between coins\n"
        "/transactions - View transaction history\n"
        "/referral - Referral program info\n"
        "/airdrop - Check/create airdrop\n"
        "/giveaway - Check/create giveaway\n"
        "/gift - Send a gift to someone\n"
        "/open - Open a gift code\n"
        "/my_gifts - View your gifts\n"
        "/top - Top users leaderboard\n"
        "/tip - Send a tip (reply to message)\n"
        "/users - User statistics\n"
        "/notify - Notification settings\n"
        "/prices - Current exchange rates\n"
        "/support - Contact support\n"
        "/help - Show this help\n\n"
        "💰 Supported Coins:\n"
        "TON, USD, NOT, DOGS, HMSTR\n\n"
        "❓ Need Help?\n"
        "Use /support to contact admin"
    )
    
    if message.from_user.id in config.ADMIN_IDS:
        text += "\n\n🔐 Admin Commands:\n"
        text += "/stats - Bot statistics\n"
        text += "/pending - Pending withdrawals\n"
        text += "/user_info - User information\n"
        text += "/search - Search users\n"
        text += "/ban - Ban user\n"
        text += "/unban - Unban user\n"
        text += "/give - Add balance\n"
        text += "/take - Remove balance\n"
        text += "/broadcast - Send message to all\n"
        text += "/airdrop - Create airdrop\n"
        text += "/giveaway - Create giveaway\n"
        text += "/gift - Create gift codes\n"
        text += "/charts - View charts\n"
        text += "/export - Export data\n"
        text += "/backup - Create backup\n"
        text += "/restore - Restore backup\n"
        text += "/logs - View admin logs\n"
        text += "/perf - Performance stats\n"
        text += "/restart - Restart bot\n"
        text += "/recover_deposits - Recover missing deposits\n"
        text += "/diagnose - Diagnose issues\n"
        text += "/check_api - Check API status\n"
        text += "/restart_monitor - Restart monitor\n"
        text += "/recover_all_old - Recover old deposits\n"
        text += "/fix - Quick fix common issues"
    
    bot.reply_to(message, text)
# ==================== Admin Commands ====================
@bot.message_handler(commands=['stats'])
@admin_only
@log_command
def stats_command(message):
    user_stats = UserModel.get_statistics()
    tx_stats = TransactionModel.get_statistics()
    withdraw_stats = WithdrawalModel.get_statistics()
    dist_stats = DistributionModel.get_statistics()
    
    text = (
        f"📊 Bot Statistics\n\n"
        f"👥 Users:\n"
        f"Total Users: {user_stats['total']:,}\n"
        f"Banned Users: {user_stats['banned']:,}\n"
        f"New Today: +{user_stats['new_today']:,}\n"
        f"New This Week: +{user_stats['new_week']:,}\n"
        f"Active (24h): {user_stats['active_24h']:,}\n\n"
        f"💰 Balances:\n"
        f"Total TON: {user_stats['total_ton']:.2f} TON\n"
        f"Total USD: {user_stats['total_usd']:.2f} USD\n\n"
        f"📌 Transactions:\n"
        f"Total: {tx_stats['total_count']:,}\n"
        f"Volume: {tx_stats['total_amount']:.2f} TON\n"
        f"Today: {tx_stats['today_count']} ({tx_stats['today_amount']:.2f} TON)\n"
        f"This Week: {tx_stats['week_count']} ({tx_stats['week_amount']:.2f} TON)\n\n"
        f"⏳ Withdrawals:\n"
        f"Pending: {withdraw_stats['pending_count']} ({withdraw_stats['pending_total']:.2f} TON)\n"
        f"Completed: {withdraw_stats['completed_count']}\n\n"
        f"🎁 Distributions:\n"
        f"Active: {dist_stats['active_count']} ({dist_stats['active_total']:.2f} TON)\n"
        f"Expired: {dist_stats['expired_count']}"
    )
    
    bot.reply_to(message, text)

@bot.message_handler(commands=['pending'])
@admin_only
@log_command
def pending_command(message):
    pending = WithdrawalModel.get_pending()
    
    if not pending:
        bot.reply_to(message, "✅ No pending withdrawals")
        return
    
    text = f"⏳ Pending Withdrawals ({len(pending)})\n\n"
    
    for wd in pending[:10]:
        time_str = datetime.fromtimestamp(wd['timestamp']).strftime('%Y-%m-%d %H:%M')
        text += (
            f"🆔 ID: `{wd['withdrawal_id']}`\n"
            f"👤 User: {wd['username'] or 'N/A'} ({wd['user_id']})\n"
            f"💰 Amount: {wd['amount']:.4f} {wd['coin']}\n"
            f"💸 Fee: {wd['fee']:.4f} {wd['coin']}\n"
            f"📤 Wallet: {wd['wallet'][:15]}...{wd['wallet'][-5:]}\n"
            f"⏰ Time: {time_str}\n\n"
        )
        if len(text) > 3500:
            text += f"... and {len(pending) - 10} more"
            break
    
    bot.reply_to(message, text)

@bot.message_handler(commands=['user_info'])
@admin_only
@log_command
def user_info_command(message):
    parts = message.text.split()
    if len(parts) != 2:
        bot.reply_to(message, "Usage: /user_info [ID or @username]")
        return
    
    identifier = parts[1]
    target_id = find_user_by_identifier(identifier)
    
    if not target_id:
        # Try searching
        users = find_users_by_name_or_username(identifier, 5)
        if users:
            text = "🔍 Multiple users found:\n\n"
            for user in users:
                name = user.get('first_name') or 'Unknown'
                username = f" (@{user['username']})" if user['username'] else ""
                text += f"• {name}{username} - ID: {user['user_id']}\n"
            text += "\nUse exact ID or @username for details"
            bot.reply_to(message, text)
        else:
            bot.reply_to(message, f"❌ User {identifier} not found")
        return
    
    user = UserModel.get(target_id)
    if not user:
        bot.reply_to(message, f"❌ User {target_id} not registered")
        return
    
    referrals = ReferralModel.get_by_referrer(target_id)
    transactions = TransactionModel.get_user_transactions(target_id, limit=5)
    
    total_deposits = sum(tx['amount'] for tx in transactions if tx['type'] == 'deposit' and tx['to_id'] == target_id)
    total_withdrawals = sum(tx['amount'] for tx in transactions if tx['type'] == 'withdraw' and tx['from_id'] == target_id)
    
    joined_date = format_time(user['joined_date'])
    last_active = format_time(user['last_active'])
    
    text = (
        f"👤 User Information\n\n"
        f"📌 ID: {target_id}\n"
        f"👤 Username: @{user['username'] or 'None'}\n"
        f"📝 Name: {user['first_name'] or 'Unknown'} {user['last_name'] or ''}\n"
        f"🚫 Status: {'BANNED' if user['banned'] else 'Active'}\n\n"
        f"📅 Joined: {joined_date}\n"
        f"🕐 Last Active: {last_active}\n"
        f"🔔 Notifications: {'Enabled' if user['notify_enabled'] else 'Disabled'}\n\n"
        f"💰 Balances:\n"
        f"TON: {user['bal_ton']:.4f}\n"
        f"USD: {user['bal_usd']:.4f}\n"
        f"NOT: {user['bal_not']:.4f}\n"
        f"DOGS: {user['bal_dogs']:.4f}\n"
        f"HMSTR: {user['bal_hmstr']:.4f}\n\n"
        f"⚠️ Frozen:\n"
        f"TON: {user['frozen_ton']:.4f}\n\n"
        f"📊 Statistics:\n"
        f"Deposits: {total_deposits:.4f} TON\n"
        f"Withdrawals: {total_withdrawals:.4f} TON\n"
        f"Referrals: {len(referrals)}\n"
    )
    
    bot.send_message(message.chat.id, text, reply_markup=user_actions_keyboard(target_id))

# ==================== Search Command ====================
@bot.message_handler(commands=['search'])
@admin_only
@log_command
def search_command(message):
    parts = message.text.split()
    if len(parts) < 2:
        bot.reply_to(message, "Usage: /search [name or username]")
        return
    
    search_term = ' '.join(parts[1:])
    users = UserModel.search(search_term, limit=20)
    
    if not users:
        bot.reply_to(message, f"❌ No users found matching '{search_term}'")
        return
    
    text = f"🔍 Search Results for '{search_term}':\n\n"
    
    for user in users:
        name = user.get('first_name') or 'Unknown'
        username = f" (@{user['username']})" if user['username'] else ""
        status = "🚫" if user['banned'] else "✅"
        text += f"{status} {name}{username} - ID: {user['user_id']}\n"
    
    bot.reply_to(message, text)

# ==================== Ban/Unban Commands ====================
@bot.message_handler(commands=['ban'])
@admin_only
@log_command
def ban_command(message):
    parts = message.text.split()
    if len(parts) != 2:
        bot.reply_to(message, "Usage: /ban [user_id]")
        return
    
    try:
        target_id = int(parts[1])
    except:
        bot.reply_to(message, "Invalid user ID")
        return
    
    if target_id in config.ADMIN_IDS:
        bot.reply_to(message, "❌ Cannot ban another admin")
        return
    
    user = UserModel.get(target_id)
    if not user:
        bot.reply_to(message, f"User {target_id} not found")
        return
    
    if user['banned']:
        bot.reply_to(message, f"User {target_id} is already banned")
        return
    
    UserModel.ban(target_id)
    log_admin_action(message.from_user.id, 'ban', f"Banned user {target_id}")
    
    bot.reply_to(message, f"✅ User {target_id} has been banned")
    
    try:
        bot.send_message(target_id, "🚫 Your account has been banned. Contact support for more information.")
    except:
        pass

@bot.message_handler(commands=['unban'])
@admin_only
@log_command
def unban_command(message):
    parts = message.text.split()
    if len(parts) != 2:
        bot.reply_to(message, "Usage: /unban [user_id]")
        return
    
    try:
        target_id = int(parts[1])
    except:
        bot.reply_to(message, "Invalid user ID")
        return
    
    user = UserModel.get(target_id)
    if not user:
        bot.reply_to(message, f"User {target_id} not found")
        return
    
    if not user['banned']:
        bot.reply_to(message, f"User {target_id} is not banned")
        return
    
    UserModel.unban(target_id)
    log_admin_action(message.from_user.id, 'unban', f"Unbanned user {target_id}")
    
    bot.reply_to(message, f"✅ User {target_id} has been unbanned")
    
    try:
        bot.send_message(target_id, "✅ Your account has been unbanned. You can now use the bot again.")
    except:
        pass

# ==================== Give/Take Commands ====================
@bot.message_handler(commands=['give'])
@admin_only
@log_command
def give_command(message):
    parts = message.text.split()
    if len(parts) != 4:
        bot.reply_to(message, "Usage: /give [user_id] [amount] [coin]")
        return
    
    try:
        target_id = int(parts[1])
        amount = float(parts[2])
        coin = parts[3].upper()
    except:
        bot.reply_to(message, "Invalid format")
        return
    
    if coin not in config.SUPPORTED_COINS:
        bot.reply_to(message, f"Unsupported coin. Supported: {', '.join(config.SUPPORTED_COINS)}")
        return
    
    if amount <= 0:
        bot.reply_to(message, "Amount must be positive")
        return
    
    user = UserModel.get(target_id)
    if not user:
        bot.reply_to(message, f"User {target_id} not found")
        return
    
    UserModel.update_balance(target_id, coin, amount, 'add')
    
    TransactionModel.create(
        from_id=message.from_user.id,
        to_id=target_id,
        amount=amount,
        coin=coin,
        tx_type='admin_give',
        memo=f"Admin give by {message.from_user.id}"
    )
    
    log_admin_action(message.from_user.id, 'give', f"Gave {amount} {coin} to {target_id}")
    
    bot.reply_to(message, f"✅ Added {amount} {coin} to user {target_id}")
    
    try:
        bot.send_message(
            target_id,
            f"💰 Balance Added!\n\n"
            f"You received {amount} {coin} from admin."
        )
        NotificationModel.create(target_id, f"You received {amount} {coin} from admin", 'info')
    except:
        pass

@bot.message_handler(commands=['take'])
@admin_only
@log_command
def take_command(message):
    parts = message.text.split()
    if len(parts) != 4:
        bot.reply_to(message, "Usage: /take [user_id] [amount] [coin]")
        return
    
    try:
        target_id = int(parts[1])
        amount = float(parts[2])
        coin = parts[3].upper()
    except:
        bot.reply_to(message, "Invalid format")
        return
    
    if coin not in config.SUPPORTED_COINS:
        bot.reply_to(message, f"Unsupported coin. Supported: {', '.join(config.SUPPORTED_COINS)}")
        return
    
    if amount <= 0:
        bot.reply_to(message, "Amount must be positive")
        return
    
    user = UserModel.get(target_id)
    if not user:
        bot.reply_to(message, f"User {target_id} not found")
        return
    
    balance = user.get(f'bal_{coin.lower()}', 0)
    if balance < amount:
        bot.reply_to(message, f"User only has {balance} {coin}")
        return
    
    UserModel.update_balance(target_id, coin, amount, 'subtract')
    
    TransactionModel.create(
        from_id=target_id,
        to_id=message.from_user.id,
        amount=amount,
        coin=coin,
        tx_type='admin_take',
        memo=f"Admin take by {message.from_user.id}"
    )
    
    log_admin_action(message.from_user.id, 'take', f"Took {amount} {coin} from {target_id}")
    
    bot.reply_to(message, f"✅ Removed {amount} {coin} from user {target_id}")
    
    try:
        bot.send_message(
            target_id,
            f"💰 Balance Removed!\n\n"
            f"{amount} {coin} was removed from your balance by admin."
        )
        NotificationModel.create(target_id, f"{amount} {coin} was removed from your balance", 'warning')
    except:
        pass
# ==================== Broadcast Command ====================
@bot.message_handler(commands=['broadcast'])
@admin_only
@log_command
def broadcast_command(message):
    msg = message.text.replace('/broadcast', '', 1).strip()
    
    if not msg:
        bot.reply_to(message, "Usage: /broadcast [message]")
        return
    
    # Get all users
    users = UserModel.get_all()
    total = len(users)
    sent = 0
    failed = 0
    
    status_msg = bot.reply_to(message, f"📢 Broadcasting to {total} users... 0%")
    
    for i, user in enumerate(users):
        if user.get('notify_enabled', 1) and not user.get('banned', 0):
            try:
                bot.send_message(
                    user['user_id'],
                    f"📢 Broadcast Message\n\n{msg}\n\n- Admin"
                )
                sent += 1
            except:
                failed += 1
        
        # Update status every 10%
        if (i + 1) % max(1, total // 10) == 0:
            percent = int((i + 1) / total * 100)
            try:
                bot.edit_message_text(
                    f"📢 Broadcasting to {total} users... {percent}%",
                    status_msg.chat.id,
                    status_msg.message_id
                )
            except:
                pass
    
    # Also send to channel
    send_to_channel("Broadcast", msg, msg_type='info')
    
    bot.edit_message_text(
        f"✅ Broadcast completed!\n\n"
        f"Total users: {total}\n"
        f"Sent: {sent}\n"
        f"Failed: {failed}\n"
        f"Banned/Disabled: {total - sent - failed}",
        status_msg.chat.id,
        status_msg.message_id
    )
    
    log_admin_action(message.from_user.id, 'broadcast', f"Sent broadcast to {sent} users")

# ==================== RECOVER MISSING DEPOSITS COMMAND ====================
@bot.message_handler(commands=['recover_deposits'])
@admin_only
@log_command
def recover_deposits_command(message):
    """Recover all missing deposits - for admin only"""
    msg = bot.reply_to(message, "🔄 Recovering missing deposits... Please wait.")
    
    try:
        # Use direct API call to v2
        url = f"https://toncenter.com/api/v2/getTransactions?address={config.OFFICIAL_WALLET}&limit=50"
        response = requests.get(url, timeout=30)
        data = response.json()
        
        if 'result' not in data:
            bot.edit_message_text("❌ Failed to connect to TON API", msg.chat.id, msg.message_id)
            return
        
        recovered = 0
        text = "✅ Recovered Deposits:\n\n"
        
        for tx in data['result']:
            tx_hash = tx.get('transaction_id', {}).get('hash', '')
            in_msg = tx.get('in_msg', {})
            
            # Get amount (convert from nanoTON to TON)
            try:
                value = int(in_msg.get('value', 0)) / 1_000_000_000
            except:
                value = 0
            
            # Get memo
            memo = in_msg.get('message', '')
            
            # Check if this is a valid deposit (minimum 0.05 TON)
            if value >= config.MIN_DEPOSIT and memo and memo.isdigit():
                user_id = int(memo)
                
                # Check if already processed
                with db_pool.get_connection() as conn:
                    c = conn.cursor()
                    c.execute("SELECT 1 FROM processed_transactions WHERE tx_hash = ?", (tx_hash,))
                    if not c.fetchone():
                        # Check if user exists
                        c.execute("SELECT user_id FROM users WHERE user_id = ?", (user_id,))
                        if c.fetchone():
                            # Add balance
                            c.execute(
                                "UPDATE users SET bal_ton = bal_ton + ?, total_deposits = total_deposits + ? WHERE user_id = ?",
                                (value, value, user_id)
                            )
                            
                            # Record transaction
                            date_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            c.execute('''
                                INSERT INTO transactions (from_id, to_id, amount, coin, type, status, tx_hash, memo, timestamp, date)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            ''', (None, user_id, value, 'TON', 'deposit', 'completed', tx_hash, memo, time.time(), date_str))
                            
                            # Mark as processed
                            c.execute(
                                "INSERT INTO processed_transactions (tx_hash, processed_at, value, comment) VALUES (?, ?, ?, ?)",
                                (tx_hash, time.time(), value, memo)
                            )
                            
                            recovered += 1
                            text += f"💰 {value} TON for user {user_id}\n"
                            
                            # Notify user
                            try:
                                bot.send_message(
                                    user_id,
                                    f"✅ Deposit Recovered!\n\n"
                                    f"Amount: {value} TON\n"
                                    f"Your balance has been updated."
                                )
                            except:
                                pass
        
        if recovered > 0:
            text += f"\n✅ Total: {recovered} deposits recovered"
            bot.edit_message_text(text, msg.chat.id, msg.message_id)
        else:
            bot.edit_message_text("❌ No new deposits found", msg.chat.id, msg.message_id)
            
    except Exception as e:
        bot.edit_message_text(f"❌ Error: {str(e)}", msg.chat.id, msg.message_id)

# ==================== Admin Commands - Backup ====================
@bot.message_handler(commands=['backup'])
@admin_only
@log_command
def backup_command(message):
    """Create database backup"""
    try:
        backup_file = create_backup()
        if backup_file:
            bot.reply_to(message, f"✅ Backup created: {backup_file}")
            # Send the backup file to admin
            try:
                with open(f"backups/{backup_file}", 'rb') as f:
                    bot.send_document(message.chat.id, f, caption=f"📦 Backup file: {backup_file}")
            except:
                pass
        else:
            bot.reply_to(message, "❌ Backup failed")
    except Exception as e:
        bot.reply_to(message, f"❌ Error: {str(e)}")

@bot.message_handler(commands=['restore'])
@admin_only
@log_command
def restore_command(message):
    """Restore database from backup"""
    parts = message.text.split()
    if len(parts) != 2:
        # Show available backups
        try:
            if not os.path.exists(config.BACKUP_DIR):
                bot.reply_to(message, "No backups directory found.")
                return
            
            backups = sorted([f for f in os.listdir(config.BACKUP_DIR) if f.startswith('backup_')])
            if not backups:
                bot.reply_to(message, "No backups found.")
                return
            
            text = "📋 Available backups:\n\n"
            for i, backup in enumerate(backups[-10:], 1):
                file_size = os.path.getsize(os.path.join(config.BACKUP_DIR, backup)) / 1024
                text += f"{i}. {backup} ({file_size:.1f} KB)\n"
            text += "\nUsage: /restore [filename]"
            bot.reply_to(message, text)
        except Exception as e:
            bot.reply_to(message, f"❌ Error: {str(e)}")
        return
    
    backup_file = parts[1]
    backup_path = os.path.join(config.BACKUP_DIR, backup_file)
    
    if not os.path.exists(backup_path):
        bot.reply_to(message, f"❌ Backup file not found: {backup_file}")
        return
    
    # Confirm restore
    markup = types.InlineKeyboardMarkup()
    markup.add(
        types.InlineKeyboardButton("✅ Confirm", callback_data=f"confirm_restore_{backup_file}"),
        types.InlineKeyboardButton("❌ Cancel", callback_data="cancel_restore")
    )
    
    bot.reply_to(
        message,
        f"⚠️ WARNING: Restoring will overwrite current database!\n\n"
        f"Backup: {backup_file}\n"
        f"Size: {os.path.getsize(backup_path) / 1024:.1f} KB\n\n"
        f"Are you sure?",
        reply_markup=markup
    )

# ==================== Admin Commands - Logs ====================
@bot.message_handler(commands=['logs'])
@admin_only
@log_command
def logs_command(message):
    """View admin logs"""
    try:
        logs = get_admin_logs(30)
        if not logs:
            bot.reply_to(message, "📭 No admin logs found")
            return
        
        text = "📋 Recent Admin Actions:\n\n"
        for log in logs[:20]:
            time_str = datetime.fromtimestamp(log['timestamp']).strftime('%Y-%m-%d %H:%M')
            text += f"• [{time_str}] Admin {log['admin_id']}: {log['action']}\n  {log['details']}\n\n"
        
        if len(logs) > 20:
            text += f"... and {len(logs) - 20} more"
        
        # Split long messages
        if len(text) > 4000:
            parts = [text[i:i+4000] for i in range(0, len(text), 4000)]
            for part in parts:
                bot.send_message(message.chat.id, part)
        else:
            bot.reply_to(message, text)
    except Exception as e:
        bot.reply_to(message, f"❌ Error: {str(e)}")

@bot.message_handler(commands=['perf'])
@admin_only
@log_command
def perf_command(message):
    """View performance statistics"""
    try:
        stats = perf_monitor.get_stats()
        bot.reply_to(message, stats)
    except Exception as e:
        bot.reply_to(message, f"❌ Error: {str(e)}")

@bot.message_handler(commands=['restart'])
@admin_only
@log_command
def restart_command(message):
    """Restart the bot"""
    markup = types.InlineKeyboardMarkup()
    markup.add(
        types.InlineKeyboardButton("✅ Confirm Restart", callback_data="confirm_restart"),
        types.InlineKeyboardButton("❌ Cancel", callback_data="cancel_restart")
    )
    
    bot.reply_to(
        message,
        "⚠️ Are you sure you want to restart the bot?",
        reply_markup=markup
    )
# ==================== User Action Callbacks ====================
@bot.callback_query_handler(func=lambda call: call.data.startswith("user_balance_"))
def user_balance_callback(call):
    user_id = int(call.data.split("_")[2])
    user = UserModel.get(user_id)
    
    if not user:
        bot.answer_callback_query(call.id, "User not found")
        return
    
    text = f"💰 Balance for user {user_id}\n\n"
    text += f"TON: {user['bal_ton']:.4f}\n"
    text += f"USD: {user['bal_usd']:.4f}\n"
    text += f"NOT: {user['bal_not']:.4f}\n"
    text += f"DOGS: {user['bal_dogs']:.4f}\n"
    text += f"HMSTR: {user['bal_hmstr']:.4f}"
    
    bot.send_message(call.message.chat.id, text)
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda call: call.data.startswith("user_history_"))
def user_history_callback(call):
    user_id = int(call.data.split("_")[2])
    transactions = TransactionModel.get_user_transactions(user_id, limit=10)
    
    if not transactions:
        bot.send_message(call.message.chat.id, "No transactions found")
        bot.answer_callback_query(call.id)
        return
    
    text = f"📜 Last 10 transactions for user {user_id}\n\n"
    
    for tx in transactions:
        if tx['type'] == 'deposit':
            text += f"📥 Deposit: {tx['amount']} {tx['coin']}\n"
        elif tx['type'] == 'withdraw':
            text += f"📤 Withdraw: {tx['amount']} {tx['coin']}\n"
        elif tx['type'] == 'send':
            if tx['from_id'] == user_id:
                text += f"📨 Sent to {tx['to_id']}: {tx['amount']} {tx['coin']}\n"
            else:
                text += f"📨 Received from {tx['from_id']}: {tx['amount']} {tx['coin']}\n"
        else:
            text += f"{tx['type']}: {tx['amount']} {tx['coin']}\n"
    
    bot.send_message(call.message.chat.id, text)
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda call: call.data.startswith("user_ban_"))
def user_ban_callback(call):
    admin_id = call.from_user.id
    target_id = int(call.data.split("_")[2])
    
    if target_id in config.ADMIN_IDS:
        bot.answer_callback_query(call.id, "Cannot ban another admin")
        return
    
    user = UserModel.get(target_id)
    if not user:
        bot.answer_callback_query(call.id, "User not found")
        return
    
    if user['banned']:
        bot.answer_callback_query(call.id, "User already banned")
        return
    
    UserModel.ban(target_id)
    log_admin_action(admin_id, 'ban', f"Banned user {target_id}")
    
    bot.answer_callback_query(call.id, f"User {target_id} banned")
    bot.edit_message_text(f"✅ User {target_id} has been banned", call.message.chat.id, call.message.message_id)

@bot.callback_query_handler(func=lambda call: call.data.startswith("user_unban_"))
def user_unban_callback(call):
    admin_id = call.from_user.id
    target_id = int(call.data.split("_")[2])
    
    user = UserModel.get(target_id)
    if not user:
        bot.answer_callback_query(call.id, "User not found")
        return
    
    if not user['banned']:
        bot.answer_callback_query(call.id, "User not banned")
        return
    
    UserModel.unban(target_id)
    log_admin_action(admin_id, 'unban', f"Unbanned user {target_id}")
    
    bot.answer_callback_query(call.id, f"User {target_id} unbanned")
    bot.edit_message_text(f"✅ User {target_id} has been unbanned", call.message.chat.id, call.message.message_id)

@bot.callback_query_handler(func=lambda call: call.data.startswith("user_add_"))
def user_add_callback(call):
    user_id = call.data.split("_")[2]
    bot.answer_callback_query(call.id, f"Use /give {user_id} [amount] [coin]")
    bot.send_message(
        call.message.chat.id,
        f"To add balance to user {user_id}, use:\n/give {user_id} [amount] [coin]"
    )

@bot.callback_query_handler(func=lambda call: call.data.startswith("user_remove_"))
def user_remove_callback(call):
    user_id = call.data.split("_")[2]
    bot.answer_callback_query(call.id, f"Use /take {user_id} [amount] [coin]")
    bot.send_message(
        call.message.chat.id,
        f"To remove balance from user {user_id}, use:\n/take {user_id} [amount] [coin]"
    )

# ==================== Distribution Callbacks ====================
@bot.callback_query_handler(func=lambda call: call.data.startswith("cancel_dist_"))
def cancel_distribution_callback(call):
    admin_id = call.from_user.id
    dist_id = int(call.data.split("_")[2])
    
    dist = DistributionModel.get_by_id(dist_id)
    if not dist:
        bot.answer_callback_query(call.id, "Distribution not found")
        return
    
    if dist['admin_id'] != admin_id and admin_id not in config.ADMIN_IDS:
        bot.answer_callback_query(call.id, "Not authorized")
        return
    
    if DistributionModel.cancel(dist_id):
        bot.answer_callback_query(call.id, "Distribution cancelled")
        bot.edit_message_text(
            f"❌ Distribution {dist_id} cancelled\nReturned {dist['remaining_amount']} {dist['coin']} to creator",
            call.message.chat.id,
            call.message.message_id
        )
        log_admin_action(admin_id, 'cancel_dist', f"Cancelled distribution {dist_id}")
    else:
        bot.answer_callback_query(call.id, "Failed to cancel")

@bot.callback_query_handler(func=lambda call: call.data.startswith("view_participants_"))
def view_participants_callback(call):
    dist_id = int(call.data.split("_")[2])
    participants = ParticipantModel.get_participants(dist_id, limit=20)
    
    if not participants:
        bot.send_message(call.message.chat.id, "No participants yet")
        bot.answer_callback_query(call.id)
        return
    
    text = f"👥 Participants for distribution {dist_id}\n\n"
    total = 0
    
    for p in participants:
        name = p['username'] or f"User {p['user_id']}"
        text += f"• {name}: {p['amount']:.6f}\n"
        total += p['amount']
    
    text += f"\nTotal claimed: {total:.6f}"
    
    bot.send_message(call.message.chat.id, text)
    bot.answer_callback_query(call.id)
# ==================== Admin Panel Callbacks ====================
@bot.callback_query_handler(func=lambda call: call.data == "admin_stats")
def admin_stats_callback(call):
    stats_command(call.message)
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda call: call.data == "admin_pending")
def admin_pending_callback(call):
    pending_command(call.message)
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda call: call.data == "admin_find_user")
def admin_find_user_callback(call):
    bot.send_message(call.message.chat.id, "Use /user_info [ID or @username] to find user")
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda call: call.data == "admin_add_balance")
def admin_add_balance_callback(call):
    bot.send_message(call.message.chat.id, "Use /give [user_id] [amount] [coin] to add balance")
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda call: call.data == "admin_broadcast")
def admin_broadcast_callback(call):
    bot.send_message(call.message.chat.id, "Use /broadcast [message] to send to all users")
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda call: call.data == "admin_airdrop")
def admin_airdrop_callback(call):
    bot.send_message(call.message.chat.id, "To create airdrop as admin:\n/airdrop [amount] [coin] [users]\n\nExample:\n/airdrop 100 TON 1000")
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda call: call.data == "admin_giveaway")
def admin_giveaway_callback(call):
    bot.send_message(call.message.chat.id, "To create giveaway as admin:\n/giveaway [amount] [coin] [winners]\n\nExample:\n/giveaway 100 TON 1000")
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda call: call.data == "admin_charts")
def admin_charts_callback(call):
    bot.send_message(call.message.chat.id, "Use /charts to view statistics charts")
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda call: call.data == "admin_backup")
def admin_backup_callback(call):
    backup_file = create_backup()
    if backup_file:
        bot.send_message(call.message.chat.id, f"✅ Backup created: {backup_file}")
    else:
        bot.send_message(call.message.chat.id, "❌ Backup failed")
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda call: call.data == "admin_logs")
def admin_logs_callback(call):
    logs = get_admin_logs(20)
    if not logs:
        bot.send_message(call.message.chat.id, "No logs found")
        bot.answer_callback_query(call.id)
        return
    
    text = "📋 Recent Admin Actions:\n\n"
    for log in logs:
        time_str = datetime.fromtimestamp(log['timestamp']).strftime('%Y-%m-%d %H:%M')
        text += f"• [{time_str}] Admin {log['admin_id']}: {log['action']} - {log['details']}\n"
    
    bot.send_message(call.message.chat.id, text[:4000])
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda call: call.data == "admin_restart")
def admin_restart_callback(call):
    bot.send_message(call.message.chat.id, "🔄 Restarting bot...")
    log_admin_action(call.from_user.id, 'restart', 'Bot restart initiated')
    # Schedule restart
    threading.Timer(2, lambda: os._exit(0)).start()
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda call: call.data == "admin_close")
def admin_close_callback(call):
    bot.delete_message(call.message.chat.id, call.message.message_id)
    bot.answer_callback_query(call.id)

# ==================== Confirm/Cancel Callbacks ====================
@bot.callback_query_handler(func=lambda call: call.data.startswith("confirm_withdraw_"))
def confirm_withdraw_callback(call):
    data = call.data.replace("confirm_withdraw_", "")
    parts = data.split("_")
    
    if len(parts) >= 3:
        amount = float(parts[0])
        coin = parts[1]
        wallet = parts[2]
        memo = parts[3] if len(parts) > 3 else None
        
        user_id = call.from_user.id
        user = UserModel.get(user_id)
        
        if not user:
            bot.answer_callback_query(call.id, "User not found")
            return
        
        balance = user.get(f'bal_{coin.lower()}', 0)
        fee_amount = (amount * config.WITHDRAWAL_FEE_PERCENT) + config.WITHDRAWAL_FEE_FIXED
        receive_amount = amount - fee_amount
        
        if balance < amount:
            bot.answer_callback_query(call.id, "Insufficient balance")
            bot.edit_message_text("❌ Withdrawal cancelled: Insufficient balance", 
                                 call.message.chat.id, call.message.message_id)
            return
        
        # Deduct balance
        UserModel.update_balance(user_id, coin, amount, 'subtract')
        
        # Create withdrawal record
        withdrawal_id = generate_unique_id('WD', 8)
        WithdrawalModel.create(
            withdrawal_id=withdrawal_id,
            user_id=user_id,
            username=user.get('username'),
            amount=receive_amount,
            coin=coin,
            fee=fee_amount,
            total_frozen=amount,
            wallet=wallet,
            memo=memo
        )
        
        TransactionModel.create(
            from_id=user_id,
            to_id=None,
            amount=amount,
            coin=coin,
            tx_type='withdraw',
            status='pending',
            memo=f"Withdrawal to {wallet[:10]}... Fee: {fee_amount}",
            fee=fee_amount
        )
        
        bot.edit_message_text(
            f"✅ Withdrawal Request Submitted\n\n"
            f"Amount: {receive_amount:.4f} {coin}\n"
            f"To: {wallet[:15]}...{wallet[-5:]}\n"
            f"Fee: {fee_amount:.4f} {coin}\n"
            f"Withdrawal ID: `{withdrawal_id}`\n\n"
            f"⏳ Pending admin approval\n"
            f"Your new balance: {balance - amount:.4f} {coin}",
            call.message.chat.id,
            call.message.message_id,
            parse_mode='Markdown'
        )
        
        # Notify admins
        for admin_id in config.ADMIN_IDS:
            try:
                bot.send_message(
                    admin_id,
                    f"🔔 New Withdrawal Request\n\n"
                    f"User: {user_id} (@{user.get('username', 'N/A')})\n"
                    f"Amount: {receive_amount} {coin}\n"
                    f"To: {wallet}\n"
                    f"Fee: {fee_amount} {coin}\n"
                    f"ID: {withdrawal_id}"
                )
            except:
                pass
        
        bot.answer_callback_query(call.id, "Withdrawal requested")

@bot.callback_query_handler(func=lambda call: call.data.startswith("confirm_send_"))
def confirm_send_callback(call):
    data = call.data.replace("confirm_send_", "")
    parts = data.split("_")
    
    if len(parts) >= 3:
        amount = float(parts[0])
        coin = parts[1]
        to_id = int(parts[2])
        memo = parts[3] if len(parts) > 3 else None
        
        from_id = call.from_user.id
        from_user = UserModel.get(from_id)
        to_user = UserModel.get(to_id)
        
        if not from_user or not to_user:
            bot.answer_callback_query(call.id, "User not found")
            return
        
        balance = from_user.get(f'bal_{coin.lower()}', 0)
        if balance < amount:
            bot.answer_callback_query(call.id, "Insufficient balance")
            bot.edit_message_text("❌ Send cancelled: Insufficient balance", 
                                 call.message.chat.id, call.message.message_id)
            return
        
        # Execute transfer
        UserModel.update_balance(from_id, coin, amount, 'subtract')
        UserModel.update_balance(to_id, coin, amount, 'add')
        
        tx_id = TransactionModel.create(
            from_id=from_id,
            to_id=to_id,
            amount=amount,
            coin=coin,
            tx_type='send',
            memo=memo
        )
        
        bot.edit_message_text(
            f"✅ Sent Successfully!\n\n"
            f"Amount: {amount:.4f} {coin}\n"
            f"To: {get_user_mention(to_id, to_user.get('username'), to_user.get('first_name'))}\n"
            f"Your new balance: {balance - amount:.4f} {coin}",
            call.message.chat.id,
            call.message.message_id,
            parse_mode='Markdown'
        )
        
        # Notify recipient
        try:
            notif_text = f"📨 You received a transfer!\n\nFrom: {get_user_mention(from_id, from_user.get('username'), from_user.get('first_name'))}\nAmount: {amount:.4f} {coin}"
            if memo:
                notif_text += f"\nMessage: {memo}"
            bot.send_message(to_id, notif_text, parse_mode='Markdown')
            NotificationModel.create(to_id, f"Received {amount} {coin} from user {from_id}", 'send')
        except:
            pass
        
        bot.answer_callback_query(call.id, "Transfer completed")
@bot.callback_query_handler(func=lambda call: call.data.startswith("confirm_convert_"))
def confirm_convert_callback(call):
    data = call.data.replace("confirm_convert_", "")
    parts = data.split("_")
    
    if len(parts) == 5:
        amount = float(parts[0])
        from_coin = parts[1]
        to_coin = parts[2]
        final_amount = float(parts[3])
        fee = float(parts[4])
        
        user_id = call.from_user.id
        user = UserModel.get(user_id)
        
        if not user:
            bot.answer_callback_query(call.id, "User not found")
            return
        
        balance = user.get(f'bal_{from_coin.lower()}', 0)
        if balance < amount:
            bot.answer_callback_query(call.id, "Insufficient balance")
            bot.edit_message_text("❌ Conversion cancelled: Insufficient balance", 
                                 call.message.chat.id, call.message.message_id)
            return
        
        # Execute conversion
        UserModel.update_balance(user_id, from_coin, amount, 'subtract')
        UserModel.update_balance(user_id, to_coin, final_amount, 'add')
        
        tx_id = TransactionModel.create(
            from_id=user_id,
            to_id=user_id,
            amount=amount,
            coin=from_coin,
            tx_type='convert',
            memo=f"Converted to {final_amount} {to_coin}",
            fee=fee
        )
        
        bot.edit_message_text(
            f"✅ Conversion Completed!\n\n"
            f"From: {amount:.4f} {from_coin}\n"
            f"To: {final_amount:.6f} {to_coin}\n"
            f"Fee: {fee:.6f} {to_coin}\n"
            f"Your new {from_coin} balance: {balance - amount:.4f} {from_coin}",
            call.message.chat.id,
            call.message.message_id
        )
        
        bot.answer_callback_query(call.id, "Conversion completed")

@bot.callback_query_handler(func=lambda call: call.data.startswith("cancel_"))
def cancel_callback(call):
    action = call.data.replace("cancel_", "")
    bot.edit_message_text(f"❌ {action.capitalize()} cancelled", 
                         call.message.chat.id, call.message.message_id)
    bot.answer_callback_query(call.id, "Cancelled")

# ==================== Callbacks for restore and restart ====================
@bot.callback_query_handler(func=lambda call: call.data.startswith("confirm_restore_"))
def confirm_restore_callback(call):
    backup_file = call.data.replace("confirm_restore_", "")
    admin_id = call.from_user.id
    
    try:
        # Declare global FIRST before using db_pool
        global db_pool
        
        # Close connections
        db_pool.close_all()
        
        # Restore backup
        backup_path = os.path.join(config.BACKUP_DIR, backup_file)
        shutil.copy2(backup_path, config.DB_PATH)
        
        # Reinitialize database pool
        db_pool = DatabasePool()
        
        # Log action
        log_admin_action(admin_id, 'restore', f"Restored database from {backup_file}")
        
        bot.edit_message_text(
            f"✅ Database restored successfully from {backup_file}",
            call.message.chat.id,
            call.message.message_id
        )
    except Exception as e:
        bot.edit_message_text(
            f"❌ Restore failed: {str(e)}",
            call.message.chat.id,
            call.message.message_id
        )
    
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda call: call.data == "cancel_restore")
def cancel_restore_callback(call):
    bot.edit_message_text("❌ Restore cancelled", call.message.chat.id, call.message.message_id)
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda call: call.data == "confirm_restart")
def confirm_restart_callback(call):
    admin_id = call.from_user.id
    bot.edit_message_text("🔄 Restarting bot...", call.message.chat.id, call.message.message_id)
    
    # Log action
    log_admin_action(admin_id, 'restart', 'Bot restarted')
    
    # Send to channel
    try:
        if config.CHANNEL_NOTIFY:
            bot.send_message(config.CHANNEL_NOTIFY, f"🔄 Bot restarted by admin {admin_id}")
    except:
        pass
    
    # Restart after 2 seconds
    def restart():
        logger.info("Bot restarting...")
        os._exit(0)
    
    threading.Timer(2, restart).start()
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda call: call.data == "cancel_restart")
def cancel_restart_callback(call):
    bot.edit_message_text("❌ Restart cancelled", call.message.chat.id, call.message.message_id)
    bot.answer_callback_query(call.id)

# ==================== Referral Tree Callback ====================
@bot.callback_query_handler(func=lambda call: call.data.startswith("ref_tree_"))
def referral_tree_callback(call):
    user_id = int(call.data.split("_")[2])
    
    def build_tree_text(node, level=0):
        indent = " " * level
        text = f"{indent}• User {node['user_id']}\n"
        for ref in node['referrals']:
            name = ref.get('username') or ref.get('first_name') or f"User {ref['user_id']}"
            text += f"{indent} ↳ {name}\n"
            if ref['subtree']['referrals']:
                text += build_tree_text(ref['subtree'], level + 2)
        return text
    
    # Simplified tree for now
    referrals = ReferralModel.get_by_referrer(user_id)
    tree = {
        'user_id': user_id,
        'referrals': [
            {
                'user_id': ref['referred_id'],
                'username': UserModel.get(ref['referred_id'], False).get('username') if UserModel.get(ref['referred_id'], False) else None,
                'first_name': UserModel.get(ref['referred_id'], False).get('first_name') if UserModel.get(ref['referred_id'], False) else None,
                'subtree': {'referrals': []}
            }
            for ref in referrals[:5]
        ]
    }
    
    text = f"🌳 Referral Tree for User {user_id}\n\n"
    text += build_tree_text(tree)
    
    bot.send_message(call.message.chat.id, text)
    bot.answer_callback_query(call.id)
# ==================== DEPOSIT MONITOR - FIXED VERSION ====================
def monitor_deposits():
    global processed_txs_cache
    
    if not ton_api:
        logger.error("TON API not available, deposit monitor stopped")
        return
    
    load_processed_transactions()
    
    while True:
        try:
            url = f"https://toncenter.com/api/v2/getTransactions?address={config.OFFICIAL_WALLET}&limit=20"
            r = requests.get(url, timeout=30).json()
            
            if 'result' in r:
                for tx in r['result']:
                    h = tx.get('transaction_id', {}).get('hash')
                    if h in processed_txs_cache:
                        continue
                    
                    in_msg = tx.get('in_msg', {})
                    if not in_msg:
                        continue
                    
                    try:
                        value = int(in_msg.get('value', 0)) / 1_000_000_000
                    except:
                        value = 0
                    
                    memo = in_msg.get('message', '') or in_msg.get('comment', '') or ''
                    
                    # Try to decode if base64 encoded
                    if not memo and 'msg_data' in in_msg:
                        try:
                            import base64
                            msg_data = in_msg.get('msg_data', {})
                            if msg_data.get('@type') == 'msg.dataText':
                                memo = base64.b64decode(msg_data.get('text', '')).decode('utf-8').strip()
                        except:
                            pass
                    
                    logger.info(f'TX: {h[:8] if h else "?"}, Value: {value}, Memo: {memo}')
                    
                    if value >= config.MIN_DEPOSIT and memo and memo.strip().isdigit():
                        uid = int(memo.strip())
                        
                        with db_pool.get_connection() as conn:
                            c = conn.cursor()
                            c.execute("SELECT user_id FROM users WHERE user_id = ?", (uid,))
                            if c.fetchone():
                                c.execute("UPDATE users SET bal_ton = bal_ton + ? WHERE user_id = ?", (value, uid))
                                c.execute("INSERT INTO processed_transactions (tx_hash, value, comment) VALUES (?, ?, ?)", 
                                        (h, value, memo))
                                processed_txs_cache.add(h)
                                
                                try:
                                    bot.send_message(uid, f"✅ Deposit Received!\n\nAmount: {value:.2f} TON")
                                except:
                                    pass
                            else:
                                c.execute("INSERT INTO processed_transactions (tx_hash, value, comment) VALUES (?, ?, ?)", 
                                        (h, value, f"User {uid} not found"))
                                processed_txs_cache.add(h)
                    else:
                        with db_pool.get_connection() as conn:
                            c = conn.cursor()
                            c.execute("INSERT OR IGNORE INTO processed_transactions (tx_hash, value, comment) VALUES (?, ?, ?)", 
                                    (h, value, memo[:50] if memo else 'no memo'))
                            processed_txs_cache.add(h)
            
            time.sleep(config.DEPOSIT_CHECK_INTERVAL)
            
        except Exception as e:
            logger.error(f"Deposit monitor error: {e}")
            time.sleep(60)

# ==================== Background Tasks ====================
def check_expired_distributions():
    while True:
        time.sleep(3600)
        try:
            with db_pool.get_connection() as conn:
                c = conn.cursor()
                now = time.time()
                c.execute("SELECT * FROM distributions WHERE status='active' AND expires_at<?", (now,))
                
                for dist in c.fetchall():
                    if dist['remaining_amount'] > 0.000001:
                        UserModel.update_balance(dist['admin_id'], dist['coin'], dist['remaining_amount'], 'add')
                        UserModel.update_frozen(dist['admin_id'], dist['coin'], dist['remaining_amount'], 'subtract')
                        
                        try:
                            bot.send_message(dist['admin_id'], 
                                           f"⏰ Distribution Expired\n\n"
                                           f"Returned: {dist['remaining_amount']} {dist['coin']} to your balance")
                        except:
                            pass
                    
                    c.execute("UPDATE distributions SET status='expired' WHERE id=?", (dist['id'],))
        except Exception as e:
            logger.error(f"Expired check error: {e}")

def update_daily_stats():
    while True:
        time.sleep(60)
        try:
            now = datetime.now()
            if now.hour == 0 and now.minute == 0:
                yesterday = (now - timedelta(days=1)).strftime('%Y-%m-%d')
                today_start = time.mktime(now.replace(hour=0, minute=0, second=0).timetuple())
                yesterday_start = today_start - 86400
                
                with db_pool.get_connection() as conn:
                    c = conn.cursor()
                    
                    c.execute("SELECT COUNT(*) FROM users WHERE joined_date BETWEEN ? AND ?", 
                            (yesterday_start, today_start))
                    new_users = c.fetchone()[0] or 0
                    
                    c.execute("SELECT COUNT(*) FROM transactions WHERE timestamp BETWEEN ? AND ?", 
                            (yesterday_start, today_start))
                    transactions = c.fetchone()[0] or 0
                    
                    c.execute("SELECT COALESCE(SUM(amount),0) FROM transactions WHERE type='deposit' AND timestamp BETWEEN ? AND ?", 
                            (yesterday_start, today_start))
                    deposits = c.fetchone()[0] or 0
                    
                    c.execute("SELECT COALESCE(SUM(amount),0) FROM transactions WHERE type='withdraw' AND timestamp BETWEEN ? AND ?", 
                            (yesterday_start, today_start))
                    withdrawals = c.fetchone()[0] or 0
                    
                    c.execute("INSERT OR REPLACE INTO daily_stats (date, new_users, transactions, deposits, withdrawals) VALUES (?, ?, ?, ?, ?)",
                            (yesterday, new_users, transactions, deposits, withdrawals))
                    
                    logger.info(f"Updated daily stats for {yesterday}")
                    
                    for admin_id in config.ADMIN_IDS:
                        try:
                            bot.send_message(admin_id, f"📊 Daily Report - {yesterday}\n\n"
                                          f"👥 New Users: {new_users}\n"
                                          f"📌 Transactions: {transactions}\n"
                                          f"💰 Deposits: {deposits:.2f} TON\n"
                                          f"💸 Withdrawals: {withdrawals:.2f} TON")
                        except:
                            pass
        except Exception as e:
            logger.error(f"Daily stats error: {e}")

def cleanup_database():
    while True:
        time.sleep(config.CLEANUP_INTERVAL)
        try:
            with db_pool.get_connection() as conn:
                c = conn.cursor()
                
                cutoff = time.time() - (30 * 86400)
                c.execute("DELETE FROM notifications WHERE read=1 AND sent_at<?", (cutoff,))
                deleted_notifs = c.rowcount
                
                c.execute("DELETE FROM processed_transactions WHERE processed_at<?", (time.time() - (90 * 86400),))
                deleted_txs = c.rowcount
                
                GiftCodeModel.expire_old()
                
                logger.info(f"Cleanup completed: {deleted_notifs} notifications, {deleted_txs} transactions")
        except Exception as e:
            logger.error(f"Cleanup error: {e}")

def backup_database():
    while True:
        time.sleep(config.BACKUP_INTERVAL)
        try:
            create_backup()
        except Exception as e:
            logger.error(f"Backup error: {e}")

def start_background_threads():
    threads = []
    
    if ton_api:
        t = threading.Thread(target=monitor_deposits, daemon=True)
        t.start()
        threads.append(t)
    
    for target in [check_expired_distributions, update_daily_stats, cleanup_database, backup_database]:
        t = threading.Thread(target=target, daemon=True)
        t.start()
        threads.append(t)
    
    logger.info(f"Started {len(threads)} background threads")
    return threads
# ==================== Message Handlers ====================
@bot.message_handler(func=lambda message: message.text == "Wallet")
def wallet_button(message):
    wallet_command(message)

@bot.message_handler(func=lambda message: message.text == "Deposit")
def deposit_button(message):
    deposit_command(message)

@bot.message_handler(func=lambda message: message.text == "Withdraw")
def withdraw_button(message):
    withdraw_command(message)

@bot.message_handler(func=lambda message: message.text == "Convert")
def convert_button(message):
    convert_command(message)

@bot.message_handler(func=lambda message: message.text == "Referrals")
def referral_button(message):
    referral_command(message)

@bot.message_handler(func=lambda message: message.text == "Transactions")
def transactions_button(message):
    transactions_command(message)

@bot.message_handler(func=lambda message: message.text == "Prices")
def prices_button(message):
    price_command(message)

@bot.message_handler(func=lambda message: message.text == "Top Users")
def top_button(message):
    top_command(message)

@bot.message_handler(func=lambda message: message.text == "Airdrop")
def airdrop_button(message):
    airdrop_command(message)

@bot.message_handler(func=lambda message: message.text == "Giveaway")
def giveaway_button(message):
    giveaway_command(message)

@bot.message_handler(func=lambda message: message.text == "Gift")
def gift_button(message):
    gift_command(message)

@bot.message_handler(func=lambda message: message.text == "Notifications")
def notify_button(message):
    notify_command(message)

@bot.message_handler(func=lambda message: message.text == "Help")
def help_button(message):
    help_command(message)

@bot.message_handler(func=lambda message: message.text == "Support")
def support_button(message):
    support_command(message)

# ==================== Error Handler ====================
@bot.message_handler(func=lambda message: True)
def handle_all_messages(message):
    if message.chat.type == 'private' and not message.text.startswith('/'):
        bot.reply_to(message, "❌ Unknown command\n\n"
                     "Use /help to see available commands or use the buttons below.",
                     reply_markup=main_keyboard())

# ==================== Charts Command ====================
@bot.message_handler(commands=['charts'])
@admin_only
@log_command
def charts_command(message):
    """View statistics charts"""
    bot.reply_to(message, "📊 Charts feature coming soon...")

# ==================== Export Command ====================
@bot.message_handler(commands=['export'])
@admin_only
@log_command
def export_command(message):
    """Export data to CSV"""
    bot.reply_to(message, "📊 Export feature under construction. Will be available soon.")

# ==================== Diagnose Command ====================
@bot.message_handler(commands=['diagnose'])
@admin_only
def diagnose_command(message):
    """Diagnose why deposits aren't working"""
    bot.reply_to(message, "🔍 Running full deposit system diagnosis...")
    
    issues = []
    fixes = []
    
    # 1. Check TON API
    if not ton_api:
        issues.append("❌ TON API not initialized")
    elif not ton_api.api_key:
        issues.append("❌ TON API key is empty")
    else:
        bot.send_message(message.chat.id, "✅ TON API initialized")
    
    # 2. Check wallet address
    if not config.OFFICIAL_WALLET:
        issues.append("❌ Official wallet address not configured")
    elif len(config.OFFICIAL_WALLET) < 10:
        issues.append("❌ Wallet address seems too short")
    else:
        bot.send_message(message.chat.id, f"✅ Wallet address: {config.OFFICIAL_WALLET[:10]}...")
    
    # 3. Check if deposit monitor thread is running
    import threading
    threads = threading.enumerate()
    monitor_running = False
    for thread in threads:
        if 'monitor_deposits' in str(thread):
            monitor_running = True
            break
    
    if monitor_running:
        bot.send_message(message.chat.id, "✅ Deposit monitor thread is running")
    else:
        issues.append("❌ Deposit monitor thread is NOT running")
        fixes.append("• Restart the bot to start the monitor thread")
    
    # 4. Check processed transactions count
    try:
        with db_pool.get_connection() as conn:
            c = conn.cursor()
            c.execute("SELECT COUNT(*) FROM processed_transactions")
            count = c.fetchone()[0]
        bot.send_message(message.chat.id, f"✅ Processed transactions in DB: {count}")
    except Exception as e:
        issues.append(f"❌ Cannot read processed_transactions table: {str(e)}")
    
    # 5. Test TON API connection
    if ton_api and ton_api.api_key:
        try:
            bot.send_message(message.chat.id, "🔄 Testing TON API connection...")
            txs = ton_api.get_transactions(config.OFFICIAL_WALLET, limit=1)
            if txs is None:
                issues.append("❌ TON API returned None - connection failed")
                fixes.append("• Check your internet connection")
                fixes.append("• Check if TON Center is down: https://toncenter.com/")
            elif len(txs) == 0:
                bot.send_message(message.chat.id, "⚠️ TON API connected but no transactions found")
            else:
                bot.send_message(message.chat.id, f"✅ TON API connected! Found {len(txs)} transactions")
        except Exception as e:
            issues.append(f"❌ TON API test failed: {str(e)}")
            fixes.append("• Check if TONCENTER_KEY is valid")
    
    # Summary
    if issues:
        result = "❌ PROBLEMS FOUND:\n\n"
        for issue in issues:
            result += f"{issue}\n"
        if fixes:
            result += "\n🔧 FIXES:\n\n"
            for fix in fixes:
                result += f"{fix}\n"
        result += "\nIf deposits still don't work, check:"
        result += "\n• Are you including the correct memo (your user ID)?"
        result += "\n• Is the amount above minimum (0.05 TON)?"
        result += "\n• Has the transaction been confirmed on the blockchain?"
    else:
        result = "✅ All systems look good! Deposits should work."
    
    bot.send_message(message.chat.id, result)

# ==================== Restart Monitor Command ====================
@bot.message_handler(commands=['restart_monitor'])
@admin_only
def restart_monitor_command(message):
    """Restart the deposit monitor thread"""
    bot.reply_to(message, "🔄 Restarting deposit monitor...")
    
    global ton_api
    if not ton_api:
        bot.send_message(message.chat.id, "❌ TON API not configured. Creating new instance...")
        ton_api = TonAPI(config.TONCENTER_KEY)
    
    # Start a new monitor thread
    import threading
    monitor_thread = threading.Thread(target=monitor_deposits, daemon=True)
    monitor_thread.start()
    
    bot.send_message(message.chat.id, "✅ New deposit monitor thread started!")
    bot.send_message(message.chat.id, "Check in 5 minutes if deposits are now working.")
# ==================== Fix Command ====================
@bot.message_handler(commands=['fix'])
@admin_only
def fix_command(message):
    """Quick fix for common issues"""
    bot.reply_to(message, "🔧 Running quick fix...")
    
    # 1. Check TON API
    global ton_api
    if not ton_api or not ton_api.api_key:
        bot.send_message(message.chat.id, "🔄 Reinitializing TON API...")
        ton_api = TonAPI(config.TONCENTER_KEY)
    
    # 2. Restart monitor
    bot.send_message(message.chat.id, "🔄 Restarting deposit monitor...")
    monitor_thread = threading.Thread(target=monitor_deposits, daemon=True)
    monitor_thread.start()
    
    # 3. Check processed transactions
    load_processed_transactions()
    
    bot.send_message(message.chat.id, f"✅ Fix completed!\n"
                     f"• TON API: {'OK' if ton_api else 'Failed'}\n"
                     f"• Monitor: Restarted\n"
                     f"• Processed txs: {len(processed_txs_cache)}")

# ==================== Recover All Old Deposits Command ====================
@bot.message_handler(commands=['recover_all_old'])
@admin_only
def recover_all_old_command(message):
    """Try to recover all old deposits (last 100 transactions)"""
    msg = bot.reply_to(message, "🔍 Searching for all old deposits (may take a minute)...")
    
    try:
        # Get last 100 transactions from the wallet
        url = f"https://toncenter.com/api/v2/getTransactions?address={config.OFFICIAL_WALLET}&limit=100"
        response = requests.get(url, timeout=45)
        data = response.json()
        
        if 'result' not in data:
            bot.edit_message_text("❌ Failed to connect to TON Center API. Check your key and internet.", 
                                msg.chat.id, msg.message_id)
            return
        
        recovered_count = 0
        skipped_count = 0
        result_text = "✅ **Recovered Deposits Results:**\n\n"
        
        for tx in data['result']:
            tx_hash = tx.get('transaction_id', {}).get('hash', '')
            in_msg = tx.get('in_msg', {})
            
            # Get amount (convert from nanoTON to TON)
            try:
                value = int(in_msg.get('value', 0)) / 1_000_000_000
            except:
                value = 0
            
            # Get memo
            memo = in_msg.get('message', '').strip()
            
            # Skip small transactions or those without memo
            if value < 0.01 or not memo.isdigit():
                skipped_count += 1
                continue
            
            user_id = int(memo)
            
            with db_pool.get_connection() as conn:
                c = conn.cursor()
                
                # 1. Check if already processed
                c.execute("SELECT 1 FROM processed_transactions WHERE tx_hash = ?", (tx_hash,))
                if c.fetchone():
                    skipped_count += 1
                    continue
                
                # 2. Check if user exists
                c.execute("SELECT user_id FROM users WHERE user_id = ?", (user_id,))
                if not c.fetchone():
                    # User not found, just mark as processed to prevent future attempts
                    c.execute("INSERT INTO processed_transactions (tx_hash, value, comment) VALUES (?, ?, ?)", 
                            (tx_hash, value, f"User {user_id} not found"))
                    skipped_count += 1
                    result_text += f"⚠️ User {user_id} not registered, sent {value} TON (skipped)\n"
                    continue
                
                # 3. All good, add balance
                c.execute("UPDATE users SET bal_ton = bal_ton + ?, total_deposits = total_deposits + ? WHERE user_id = ?", 
                        (value, value, user_id))
                
                # 4. Record transaction
                date_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                c.execute('''
                    INSERT INTO transactions (from_id, to_id, amount, coin, type, tx_hash, memo, timestamp, date)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (None, user_id, value, 'TON', 'deposit', tx_hash, memo, time.time(), date_str))
                
                # 5. Mark as processed
                c.execute("INSERT INTO processed_transactions (tx_hash, value, comment) VALUES (?, ?, ?)", 
                        (tx_hash, value, memo))
                
                recovered_count += 1
                result_text += f"💰 {value} TON for user `{user_id}`\n"
                
                # Notify user
                try:
                    bot.send_message(
                        user_id,
                        f"✅ Deposit Recovered!\n\n"
                        f"Amount: {value} TON\n"
                        f"Your balance has been updated."
                    )
                except:
                    pass
        
        # Final summary
        result_text += f"\n**Summary:**\n"
        result_text += f"✅ Recovered: {recovered_count}\n"
        result_text += f"⏭️ Skipped (duplicate/invalid): {skipped_count}\n"
        result_text += f"📊 Total transactions checked: {len(data['result'])}"
        
        if recovered_count > 0:
            result_text += f"\n\n✨ Balances have been added successfully!"
        
        bot.edit_message_text(result_text, msg.chat.id, msg.message_id, parse_mode='Markdown')
        
    except requests.exceptions.Timeout:
        bot.edit_message_text("❌ Error: Connection timeout to TON Center API.", msg.chat.id, msg.message_id)
    except Exception as e:
        bot.edit_message_text(f"❌ Unexpected error: {str(e)}", msg.chat.id, msg.message_id)

# ==================== Check API Command ====================
@bot.message_handler(commands=['check_api'])
@admin_only
def check_api_command(message):
    """Check TON API connection directly"""
    msg = bot.reply_to(message, "🔍 Checking API...")
    
    try:
        # Get last 5 transactions
        url = f"https://toncenter.com/api/v2/getTransactions?address={config.OFFICIAL_WALLET}&limit=5"
        response = requests.get(url, timeout=10)
        data = response.json()
        
        if 'result' not in data:
            bot.edit_message_text(f"❌ API Error: {data}", msg.chat.id, msg.message_id)
            return
        
        text = "📊 **Last 5 transactions from wallet:**\n\n"
        
        for i, tx in enumerate(data['result'], 1):
            tx_hash = tx.get('transaction_id', {}).get('hash', '')[:8]
            in_msg = tx.get('in_msg', {})
            value = int(in_msg.get('value', 0)) / 1_000_000_000
            memo = in_msg.get('message', '') or 'No memo'
            
            text += f"**{i}.** Transaction: `{tx_hash}...`\n"
            text += f" Amount: {value} TON\n"
            text += f" Memo: `{memo}`\n"
            text += f" Status: {'✅ Valid' if memo.isdigit() else '❌ Not a number'}\n\n"
        
        bot.edit_message_text(text, msg.chat.id, message_id=msg.message_id, parse_mode='Markdown')
        
    except Exception as e:
        bot.edit_message_text(f"❌ Error: {str(e)}", msg.chat.id, msg.message_id)
# ==================== Main ====================
def main():
    print("=" * 50)
    print("BOT IS RUNNING - COMPLETE ENGLISH VERSION")
    print("=" * 50)
    print(f"Admins: {config.ADMIN_IDS}")
    print(f"Channel: {config.CHANNEL_NOTIFY}")
    print(f"Wallet: {config.OFFICIAL_WALLET[:15]}...")
    print(f"Minimum Deposit: {config.MIN_DEPOSIT} TON")
    print(f"Withdrawal: Min {config.MIN_WITHDRAWAL} TON, Fee {config.WITHDRAWAL_FEE_PERCENT*100}% + {config.WITHDRAWAL_FEE_FIXED}")
    print(f"Send: Min {config.MIN_SEND} TON, Fee 0%")
    print("=" * 50)
    
    load_processed_transactions()
    print(f"Loaded {len(processed_txs_cache)} processed transactions")
    
    start_background_threads()
    
    def signal_handler(sig, frame):
        logger.info("Received shutdown signal, cleaning up...")
        db_pool.close_all()
        logger.info("Bot stopped")
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    while True:
        try:
            logger.info("Starting bot polling...")
            bot.infinity_polling(timeout=60, long_polling_timeout=60)
        except Exception as e:
            logger.critical(f"Fatal polling error: {e}")
            logger.exception("Full traceback:")
            time.sleep(10)


@bot.message_handler(commands=['diagnose', 'check_api', 'recover_all_old', 'fix', 'restart_monitor', 'charts', 'export'])
@admin_only
def simple_commands(message):
    cmd = message.text.split()[0][1:]
    bot.reply_to(message, f"✅ Command /{cmd} received")
# ==================== END ====================

# Force reload handlers
bot._core = type('obj', (object,), {'handlers': {}})()

# ==== السطر الأخير في الملف هو هذا ====
if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
        db_pool.close_all()
    except Exception as e:
        logger.critical(f"Unhandled exception: {e}")
        logger.exception("Full traceback:")
        db_pool.close_all()
        sys.exit(1)
