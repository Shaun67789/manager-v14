import threading
import telebot
from database import db
import logging
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BotManager:
    def __init__(self):
        self.bot = None
        self.thread = None
        self._stop_event = threading.Event()

    def start_bot(self):
        config = db.get_config()
        token = config.get("bot_token", "").strip()
        is_running = config.get("is_running", False)

        if not token:
            logger.warning("Bot token is not configured. Cannot start.")
            return False

        if not is_running:
            logger.info("Bot is set to stopped in config.")
            return False

        if self.thread and self.thread.is_alive():
            logger.info("Bot is already running.")
            return True

        try:
            self._stop_event.clear()
            self.bot = telebot.TeleBot(token, parse_mode=None)

            from bot_handlers import register_handlers
            register_handlers(self.bot)

            logger.info("Starting bot polling thread...")
            self.thread = threading.Thread(target=self._run_polling, daemon=True, name="BotPollingThread")
            self.thread.start()
            db.log_event("🚀 Bot engine started")
            return True
        except Exception as e:
            logger.error(f"Failed to start bot: {e}")
            db.log_event(f"❌ Bot start error: {e}")
            self.bot = None
            return False

    def _run_polling(self):
        while not self._stop_event.is_set():
            try:
                logger.info("Polling started.")
                self.bot.infinity_polling(timeout=20, long_polling_timeout=15)
            except Exception as e:
                logger.error(f"Polling error: {e}")
                if not self._stop_event.is_set():
                    time.sleep(5)
        logger.info("Bot polling thread stopped.")

    def stop_bot(self):
        logger.info("Stopping bot...")
        self._stop_event.set()
        if self.bot:
            try:
                self.bot.stop_polling()
            except Exception:
                pass

        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=8)

        self.bot = None
        self.thread = None
        logger.info("Bot stopped.")
        return True

    def restart_bot(self):
        logger.info("Restarting bot...")
        self.stop_bot()
        time.sleep(1)
        return self.start_bot()


bot_manager = BotManager()
