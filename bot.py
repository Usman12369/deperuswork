# -*- coding: utf-8 -*-
import os
import sys
import shutil
import threading
import sqlite3
import random
import time
import re
import logging
import locale
from datetime import datetime, timedelta
from telebot import TeleBot, types
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
import html
import urllib.parse
import traceback

if sys.stdout.encoding != 'UTF-8':
    sys.stdout.reconfigure(encoding='utf-8') if hasattr(sys.stdout, 'reconfigure') else None
if sys.stderr.encoding != 'UTF-8':
    sys.stderr.reconfigure(encoding='utf-8') if hasattr(sys.stderr, 'reconfigure') else None

try:
    from health import start_health_server
    start_health_server()
    print("✅ Health check server started")
except Exception as e:
    print(f"❌ Health server error: {e}")
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

TOKEN = '8296869781:AAGQ76XHBE5aCUyQh1YamMDiyjnjOBW5ecs'
bot = TeleBot(TOKEN)
ADMIN_ID = 7019136722

logger.info('🚀 Бот запускается...')
logger.info(f'📂 Текущая директория: {os.getcwd()}')

MAX_BET_LIMIT = None

class Database:
    def __init__(self):
        self.conn = get_db()
        if self.conn is None:
            raise Exception("Не удалось подключиться к базе данных")
    
    def create_tables(self):
        return

    def get_user(self, user_id):
        c = self.conn.cursor()
        c.execute("SELECT * FROM users WHERE user_id=?", (user_id,))
        return c.fetchone()

    def update_user(self, user_id, **kwargs):
        c = self.conn.cursor()
        set_clause = ', '.join([f"{key}=?" for key in kwargs.keys()])
        values = list(kwargs.values()) + [user_id]
        c.execute(f"UPDATE users SET {set_clause} WHERE user_id=?", values)
        self.conn.commit()
    
    def create_user(self, user_id, username, first_name):
        c = self.conn.cursor()
        c.execute("INSERT OR IGNORE INTO users (user_id, username, first_name) VALUES (?, ?, ?) ", 
                 (user_id, username, first_name))
        self.conn.commit()

db_connection = None

def get_db():
    global db_connection
    if db_connection is None:
        try:
            data_dir = '/app/data'  
            os.makedirs(data_dir, exist_ok=True)
            db_path = os.path.join(data_dir, 'bot.db')
            db_connection = sqlite3.connect(db_path, check_same_thread=False)
            c = db_connection.cursor()
            tables = [
                '''CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY, username TEXT, first_name TEXT,
                    balance INTEGER DEFAULT 10000, depuses INTEGER DEFAULT 0,
                    vip_until TEXT, prefix TEXT DEFAULT 'Игрок', brackets TEXT DEFAULT '[]',
                    wins INTEGER DEFAULT 0, losses INTEGER DEFAULT 0, messages_count INTEGER DEFAULT 0,
                    last_casino INTEGER DEFAULT 0, last_bonus INTEGER DEFAULT 0,
                    biggest_bet INTEGER DEFAULT 0, biggest_win INTEGER DEFAULT 0, biggest_loss INTEGER DEFAULT 0,
                    last_stats INTEGER DEFAULT 0, last_top INTEGER DEFAULT 0, last_apartment INTEGER DEFAULT 0,
                    last_records INTEGER DEFAULT 0, last_commands INTEGER DEFAULT 0, last_rules INTEGER DEFAULT 0,
                    got_gift INTEGER DEFAULT 0, last_daily_bonus TEXT, banner_file_id TEXT, banner_type TEXT
                )''',
                '''CREATE TABLE IF NOT EXISTS apartments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER,
                    renovated INTEGER DEFAULT 0, purchase_date TEXT, price INTEGER DEFAULT 25000000,
                    last_collected TEXT
                )''',
                '''CREATE TABLE IF NOT EXISTS admins (user_id INTEGER PRIMARY KEY)''',
                '''CREATE TABLE IF NOT EXISTS rules (chat_id INTEGER PRIMARY KEY, rules_text TEXT)''',
                '''CREATE TABLE IF NOT EXISTS mutes (
                    user_id INTEGER, chat_id INTEGER, until INTEGER, reason TEXT,
                    PRIMARY KEY(user_id, chat_id)
                )''',
                '''CREATE TABLE IF NOT EXISTS bans (
                    user_id INTEGER, chat_id INTEGER, reason TEXT,
                    PRIMARY KEY(user_id, chat_id)
                )''',
                '''CREATE TABLE IF NOT EXISTS words (
                    word TEXT PRIMARY KEY, count INTEGER DEFAULT 1
                )''',
                '''CREATE TABLE IF NOT EXISTS daily_stats (
                    date TEXT, user_id INTEGER, messages INTEGER DEFAULT 0,
                    PRIMARY KEY(date, user_id)
                )''',
                '''CREATE TABLE IF NOT EXISTS monthly_stats (
                    month TEXT, user_id INTEGER, messages INTEGER DEFAULT 0,
                    PRIMARY KEY(month, user_id)
                )''',
                '''CREATE TABLE IF NOT EXISTS all_stats (
                    user_id INTEGER PRIMARY KEY, messages INTEGER DEFAULT 0
                )''',
                '''CREATE TABLE IF NOT EXISTS daily_stats_group (
                    date TEXT, chat_id INTEGER, user_id INTEGER, messages INTEGER DEFAULT 0,
                    PRIMARY KEY(date, chat_id, user_id)
                )''',
                '''CREATE TABLE IF NOT EXISTS monthly_stats_group (
                    month TEXT, chat_id INTEGER, user_id INTEGER, messages INTEGER DEFAULT 0,
                    PRIMARY KEY(month, chat_id, user_id)
                )''',
                '''CREATE TABLE IF NOT EXISTS all_stats_group (
                    chat_id INTEGER, user_id INTEGER, messages INTEGER DEFAULT 0,
                    PRIMARY KEY(chat_id, user_id)
                )''',
                '''CREATE TABLE IF NOT EXISTS purchases (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER,
                    item_type TEXT, item_name TEXT, purchase_date TEXT, price INTEGER
                )''',
                '''CREATE TABLE IF NOT EXISTS groups (
                    group_id INTEGER PRIMARY KEY, title TEXT, added_date TEXT
                )''',
                '''CREATE TABLE IF NOT EXISTS families (
                    family_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    husband_id INTEGER, wife_id INTEGER,
                    family_level INTEGER DEFAULT 1,
                    created_date TEXT,
                    last_benefit_date TEXT
                )''',
                '''CREATE TABLE IF NOT EXISTS family_children (
                    child_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    family_id INTEGER, user_id INTEGER,
                    added_date TEXT
                )''',
                '''CREATE TABLE IF NOT EXISTS family_upgrades (
                    family_id INTEGER, upgrade_date TEXT,
                    level_before INTEGER, level_after INTEGER,
                    cost INTEGER
                )''',
                '''CREATE TABLE IF NOT EXISTS rp_commands (
                    command_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER, command_name TEXT,
                    command_text TEXT, created_date TEXT
                )''',
                '''CREATE TABLE IF NOT EXISTS banner_requests (
                    request_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    file_id TEXT,
                    file_type TEXT,
                    file_size INTEGER DEFAULT 0,
                    status TEXT DEFAULT 'pending',
                    admin_id INTEGER,
                    decision_date TEXT
                )''',
                '''CREATE TABLE IF NOT EXISTS banner_requests (
                    request_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    file_id TEXT,
                    file_type TEXT,
                    file_size INTEGER,
                    status TEXT DEFAULT 'pending',
                    admin_id INTEGER,
                    decision_date TEXT
                )''',
            ]
            for table in tables:
                try:
                    c.execute(table)
                except Exception as e:
                    logger.error(f"Ошибка создания таблицы: {e}")
            db_connection.commit()
            logger.info("✅ База данных создана/открыта: %s", db_path)
        except Exception as e:
            logger.error(f"❌ Ошибка инициализации базы данных: {e}")
            return None
    return db_connection

db = None
try:
    _conn = get_db()
    db = Database()
except Exception as e:
    logger.error(f"❌ Не удалось инициализировать базу данных: {e}")
    exit(1)

# === АВТОБЭКАП БАЗЫ ДАННЫХ ===
def backup_db():
    try:
        data_dir = '/app/data'  
        db_path = os.path.join(data_dir, 'bot.db')
        backup_path = os.path.join(data_dir, 'bot_backup.db')
        if os.path.exists(db_path):
            shutil.copyfile(db_path, backup_path)
            logger.info(f'✅ Резервная копия базы {backup_path} создана.');
        else:
            logger.info('ℹ️ Файл базы данных не найден, бэкап пропущен.')
    except Exception as e:
        logger.error(f'❌ Ошибка бэкапа базы: {e}')

def periodic_backup(interval=600):
    backup_db()
    threading.Timer(interval, periodic_backup, [interval]).start()

backup_db()
periodic_backup(600)

def format_username(user_id, username, first_name):
    """Форматирует имя пользователя с префиксом и скобками"""
    user = db.get_user(user_id)
    if user:
        prefix = user[6] if user[6] else ""
        brackets = user[7] if user[7] else "[]"
        
        if brackets and prefix:
            if len(brackets) >= 2:
                left_bracket = brackets[0]
                right_bracket = brackets[-1]
                formatted_name = f"{left_bracket}{prefix}{right_bracket} {first_name}"
            else:
                formatted_name = f"{brackets}{prefix}{brackets} {first_name}"
        else:
            formatted_name = first_name
        return formatted_name
    return first_name

def safe_md(text: str) -> str:
    """Escape common Markdown characters for parse_mode='Markdown' (v1)."""
    if text is None:
        return ""
    s = str(text)
    s = s.replace('\', '\\')
    for ch in ['_', '*', '[', ']', '(', ')', '`', '~']:
        s = s.replace(ch, f'\{ch}')
    return s

def safe_html(text):
    if text is None:
        return ""
    return html.escape(str(text))

def create_profile_link(user_id, username, first_name):
    """Создает кликабельную ссылку на профиль пользователя"""
    if username:
        return f'<a href="https://t.me/{username}">{first_name}</a>'
    else:
        return f'<a href="tg://user?id={user_id}">{first_name}</a>'

# ========== ОСНОВНЫЕ КЛАВИАТУРЫ ==========
def main_menu_keyboard():
    keyboard = InlineKeyboardMarkup()
    keyboard.row(
        InlineKeyboardButton("💰 Баланс", callback_data="balance"),
        InlineKeyboardButton("🎰 Казино", callback_data="casino")
    )
    keyboard.row(
        InlineKeyboardButton("🏪 Магазин", callback_data="shop"),
        InlineKeyboardButton("🏠 Квартиры", callback_data="apartments")
    )
    keyboard.row(
        InlineKeyboardButton("📊 Профиль", callback_data="profile"),
        InlineKeyboardButton("🏆 Топы", callback_data="top")
    )
    keyboard.row(
        InlineKeyboardButton("📈 Статистика", callback_data="stats"),
        InlineKeyboardButton("🏅 Рекорды", callback_data="records")
    )
    keyboard.row(
        InlineKeyboardButton("👨‍👩‍👧‍👦 Семья", callback_data="family"),
        InlineKeyboardButton("🎭 РП", callback_data="rp_commands_main")
    )
    keyboard.row(
        InlineKeyboardButton("📋 Команды", callback_data="commands")
    )
    return keyboard

def shop_keyboard():
    keyboard = InlineKeyboardMarkup()
    keyboard.row(InlineKeyboardButton("👑 VIP карта", callback_data="shop_vip"))
    keyboard.row(InlineKeyboardButton("🏠 Квартира", callback_data="shop_apartment"))
    keyboard.row(InlineKeyboardButton("🎨 Дизайн", callback_data="shop_design"))
    keyboard.row(InlineKeyboardButton("🔙 Назад", callback_data="back_main"))
    return keyboard

def design_keyboard():
    keyboard = InlineKeyboardMarkup()
    keyboard.row(InlineKeyboardButton("🏷️ Префиксы", callback_data="design_prefix"))
    keyboard.row(InlineKeyboardButton("🔣 Скобки", callback_data="design_brackets"))
    keyboard.row(InlineKeyboardButton("🔙 Назад", callback_data="back_shop"))
    return keyboard

def brackets_keyboard():
    keyboard = InlineKeyboardMarkup()
    keyboard.row(InlineKeyboardButton("() - 100 д", callback_data="brackets_1"))
    keyboard.row(InlineKeyboardButton("<> - 200 д", callback_data="brackets_2"))
    keyboard.row(InlineKeyboardButton("{} - 300 д", callback_data="brackets_3"))
    keyboard.row(InlineKeyboardButton("《》- 500 д", callback_data="brackets_4"))
    keyboard.row(InlineKeyboardButton("꧁꧂- 750 д", callback_data="brackets_5"))
    keyboard.row(InlineKeyboardButton("🔙 Назад", callback_data="back_design"))
    return keyboard

def prefix_keyboard():
    keyboard = InlineKeyboardMarkup()
    keyboard.row(InlineKeyboardButton("Депер - 100 д", callback_data="prefix_1"))
    keyboard.row(InlineKeyboardButton("Лудоман - 250 д", callback_data="prefix_2"))
    keyboard.row(InlineKeyboardButton("Элита - 300 д", callback_data="prefix_3"))
    keyboard.row(InlineKeyboardButton("Богачь - 400 д", callback_data="prefix_4"))
    keyboard.row(InlineKeyboardButton("Миллиардер - 500 д", callback_data="prefix_5"))
    keyboard.row(InlineKeyboardButton("Свой префикс - 1000 д", callback_data="prefix_custom"))
    keyboard.row(InlineKeyboardButton("🔙 Назад", callback_data="back_design"))
    return keyboard

def stats_keyboard():
    keyboard = InlineKeyboardMarkup()
    keyboard.row(InlineKeyboardButton("📊 Сегодня", callback_data="stats_today"))
    keyboard.row(InlineKeyboardButton("📅 Месяц", callback_data="stats_month"))
    keyboard.row(InlineKeyboardButton("📈 Вся", callback_data="stats_all"))
    keyboard.row(InlineKeyboardButton("🔙 Назад", callback_data="back_main"))
    return keyboard

def apartment_keyboard():
    keyboard = InlineKeyboardMarkup()
    keyboard.row(InlineKeyboardButton("🏠 Купить квартиру", callback_data="buy_apartment"))
    keyboard.row(InlineKeyboardButton("🔨 Ремонт", callback_data="apartment_repair"))
    keyboard.row(InlineKeyboardButton("💰 Продать", callback_data="apartment_sell"))
    keyboard.row(InlineKeyboardButton("💵 Собрать доход", callback_data="collect_income"))
    keyboard.row(InlineKeyboardButton("🔙 Назад", callback_data="back_main"))
    return keyboard

def vip_keyboard():
    keyboard = InlineKeyboardMarkup()
    keyboard.row(InlineKeyboardButton("1-7 дней (50д/день)", callback_data="vip_7"))
    keyboard.row(InlineKeyboardButton("8-31 день (45д/день)", callback_data="vip_31")) 
    keyboard.row(InlineKeyboardButton("32+ дней (40д/день)", callback_data="vip_365"))
    keyboard.row(InlineKeyboardButton("ℹ️ Как купить", callback_data="vip_info"))
    keyboard.row(InlineKeyboardButton("🔙 Назад", callback_data="back_shop"))
    return keyboard

def family_keyboard():
    keyboard = InlineKeyboardMarkup()
    keyboard.row(InlineKeyboardButton("👨‍👩‍👧‍👦 Моя семья", callback_data="my_family"))
    keyboard.row(InlineKeyboardButton("💍 Браки сервера", callback_data="server_marriages"))
    keyboard.row(InlineKeyboardButton("📈 Уровень семьи", callback_data="family_level"))
    keyboard.row(InlineKeyboardButton("💰 Пособия", callback_data="family_benefits"))
    keyboard.row(InlineKeyboardButton("💍 Создать брак", callback_data="create_marriage"))
    keyboard.row(InlineKeyboardButton("🔙 Назад", callback_data="back_main"))
    return keyboard

def family_level_keyboard(family_id, current_level, user_id):
    keyboard = InlineKeyboardMarkup()
    
    conn = sqlite3.connect('/app/data/bot.db')
    c = conn.cursor()
    c.execute("SELECT husband_id, wife_id FROM families WHERE family_id=?", (family_id,))
    family = c.fetchone()
    conn.close()
    
    is_parent = family and (user_id == family[0] or user_id == family[1])
    
    if is_parent and current_level < 5:
        upgrade_costs = [100, 250, 500, 1000, 2000]
        next_level = current_level + 1
        cost = upgrade_costs[current_level] if current_level < len(upgrade_costs) else 0
        
        keyboard.row(InlineKeyboardButton(f"🔼 Прокачать до {next_level} ур. ({cost} д)", 
                                        callback_data=f"upgrade_family_{next_level}_{cost}"))
    
    keyboard.row(InlineKeyboardButton("ℹ️ Помощь", callback_data="family_help"))
    keyboard.row(InlineKeyboardButton("🔙 Назад", callback_data="family"))
    return keyboard

def rp_commands_keyboard():
    keyboard = InlineKeyboardMarkup()
    keyboard.row(InlineKeyboardButton("➕ Добавить РП", callback_data="add_rp"))
    keyboard.row(InlineKeyboardButton("🗑️ Мои РП команды", callback_data="my_rp_commands"))
    keyboard.row(InlineKeyboardButton("❓ РП команды", callback_data="rp_help"))
    keyboard.row(InlineKeyboardButton("🔙 Назад", callback_data="back_main"))
    return keyboard

def get_repair_keyboard(user_id):
    conn = sqlite3.connect('/app/data/bot.db')
    c = conn.cursor()
    c.execute("SELECT id, renovated FROM apartments WHERE user_id=? AND renovated=0", (user_id,))
    apartments = c.fetchall()
    conn.close()

    keyboard = InlineKeyboardMarkup()
    for apt_id, _ in apartments:
        keyboard.row(InlineKeyboardButton(f"Ремонт #{apt_id} (10,000,000 т)", callback_data=f"repair_{apt_id}"))
    keyboard.row(InlineKeyboardButton("🔙 Назад", callback_data="apartments"))
    return keyboard

def get_sell_keyboard(user_id):
    conn = sqlite3.connect('/app/data/bot.db')
    c = conn.cursor()
    c.execute("SELECT id, renovated FROM apartments WHERE user_id=?", (user_id,))
    apartments = c.fetchall()
    conn.close()

    keyboard = InlineKeyboardMarkup()
    for apt_id, renovated in apartments:
        price = 30000000 if renovated else 20000000
        status = "с ремонтом" if renovated else "без ремонта"
        keyboard.row(InlineKeyboardButton(f"Продать #{apt_id} ({status}, {price:,} т)", callback_data=f"sell_{apt_id}"))
    keyboard.row(InlineKeyboardButton("🔙 Назад", callback_data="apartments"))
    return keyboard

# ========== СИСТЕМНЫЕ КОМАНДЫ ==========
@bot.message_handler(commands=['start'])
def start_cmd(message):
    user_id = message.from_user.id
    db.create_user(user_id, message.from_user.username, message.from_user.first_name)
    
    text = "🎮 *Добро пожаловать в экономического бота!*
\n"
    text += "💰 *Стартовый баланс:* 10,000 теньге\n"
    text += "🎰 *Доступные команды:*\n"
    text += "• `Баланс` / `Б` - ваш баланс\n"
    text += "• `Казино [сумма]` - игра в казино\n"
    text += "• `Шарик [сумма]` - игра Шарик\n"
    text += "• `т обмен д [число]` - обмен тенге на депусы\n"
    text += "• `д обмен т [число]` - обмен депусов на тенге\n"
    text += "• `+ [сумма]` - перевод денег\n"
    text += "• `Профиль` - ваша статистика\n"
    text += "• `Топ` - топ игроков\n"
    text += "• `Магазин` - магазин предметов\n"
    text += "• `Стата` - статистика сообщений\n"
    text += "• `Рекорды` - рекорды системы\n"
    text += "• `Команды` - список всех команд\n"
    text += "• `Бонус` - ежедневный бонус (1 раз в день)\n"
    text += "• `Получитьподарок` - разовый подарок (10 депусов)\n"
    text += "• `Рпкоманды` - список РП команд\n"
    text += "• `Моя семья` - управление семьей\n"
    text += "• `Усыновить` / `Удочерить` - усыновить ребенка\n"
    text += "• `Жениться` / `Брак` - создать брак\n\n"
    text += "📖 *Используйте кнопки ниже для навигации*"
    
    bot.send_message(message.chat.id, text, parse_mode='Markdown', reply_markup=main_menu_keyboard())

@bot.message_handler(func=lambda m: m.text and m.text.strip().lower() == "helpbot" and m.from_user.id == ADMIN_ID)
def admin_helpbot_handler(message):
    text = (
        "<b>👑 Админ-команды бота:</b>\n\n"
        "<b>\\lim\\[сумма]</b> — установить лимит ставки в казино.\n"
        "<b>\\вайп\\</b> — полностью удалить всю БД о игроках, квартирах и статистике.\n"
        "<b>всяинфа</b> — показать статистику по базе: пользователей, деньги, квартиры, группы, размер БД.\n"
        "<b>айдигруппы</b> — список всех групп, где состоит бот, с инвайт-ссылками.\n"
        "<b>ботголос</b> — разослать отмеченное сообщение во все группы (ответом на сообщение).\n"
        "<b>helpbot</b> — эта подсказка.\n"
        "\n<b>Админ-выдачи:</b>\n"
        "<b>теньге+[сумма]</b> — добавить тенге пользователю (ответом на сообщение).\n"
        "<b>депусы+[сумма]</b> — добавить депусы пользователю (ответом на сообщение).\n"
        "<b>теньге-[сумма]</b> — убрать тенге у пользователя (ответом на сообщение).\n"
        "<b>депусы-[сумма]</b> — убрать депусы у пользователя (ответом на сообщение).\n"
        "<b>+админ</b> — добавить админа (ответом на сообщение).\n"
        "<b>-админ</b> — убрать админа (ответом на сообщение).\n"
        "<b>-смс</b> — удалить сообщение (ответом на сообщение).\n"
        "<b>всеправа</b> — получить все права в группе.\n"
        "<b>+правила [текст]</b> — установить правила в группе.\n"
        "<b>правила</b> — показать правила группы.\n"
    )
    bot.send_message(message.chat.id, text, parse_mode='HTML')

@bot.message_handler(func=lambda m: m.text and m.text.strip().lower() == "ботголос" and m.from_user.id == ADMIN_ID)
def broadcast_to_groups_handler(message):
    if not message.reply_to_message:
        bot.reply_to(message, "❌ Ответьте на сообщение, которое хотите разослать.")
        return
    
    conn = sqlite3.connect('/app/data/bot.db')
    c = conn.cursor()
    c.execute("SELECT group_id, title FROM groups")
    groups = c.fetchall()
    conn.close()
    
    if not groups:
        bot.reply_to(message, "❌ Нет групп для рассылки.")
        return
    
    sent = 0
    failed = 0

    for group_id, title in groups:
        try:
            # Пересылаем оригинальное сообщение в группу (forward), чтобы сохранялся автор и вложения
            forwarded = bot.forward_message(group_id, message.chat.id, message.reply_to_message.message_id)
            # Пытаемся закрепить пересланное сообщение в группе (если есть права)
            try:
                bot.pin_chat_message(group_id, forwarded.message_id, disable_notification=True)
            except Exception:
                pass
            sent += 1
        except Exception as e:
            logger.error(f"❌ Ошибка отправки в группу {title}: {e}")
            failed += 1

    bot.reply_to(message, f"📢 Сообщение переслано в {sent} из {len(groups)} групп. Ошибок: {failed}.")

# ... (omitted due to message length; full original file content will be inserted here)