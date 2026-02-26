from flask import Flask
from threading import Thread
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask('')

@app.route('/')
def home():
    return "🤖 Grab Wallet Bot is alive!"

@app.route('/health')
def health():
    return "OK", 200

def run():
    app.run(host='0.0.0.0', port=8080)

def keep_alive():
    """Starts a background thread to keep the bot alive"""
    t = Thread(target=run)
    t.daemon = True
    t.start()
    logger.info("Keep-alive server started on port 8080")