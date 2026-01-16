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
        c.execute("INSERT OR IGNORE INTO users (user_id, username, first_name) VALUES (?, ?, ?)", 
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
            logger.info(f'✅ Резервная копия базы {backup_path} создана.')
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
    s = s.replace('\\', '\\\\')
    for ch in ['_', '*', '[', ']', '(', ')', '`', '~']:
        s = s.replace(ch, f'\\{ch}')
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
    
    text = "🎮 *Добро пожаловать в экономического бота!*\n\n"
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

@bot.message_handler(func=lambda m: m.text and m.text.strip().lower() == "всяинфа" and m.from_user.id == ADMIN_ID and m.chat.type == 'private')
def all_info_handler(message):
    try:
        conn = sqlite3.connect('/app/data/bot.db')
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM users")
        users_count = c.fetchone()[0]
        c.execute("SELECT SUM(balance) FROM users")
        total_balance = c.fetchone()[0] or 0
        c.execute("SELECT SUM(depuses) FROM users")
        total_depuses = c.fetchone()[0] or 0
        c.execute("SELECT COUNT(*) FROM apartments")
        apartments_count = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM groups")
        groups_count = c.fetchone()[0]
        conn.close()
        db_size = 0
        try:
            db_size = os.path.getsize('/app/data/bot.db')
        except Exception:
            db_size = 0
        text = f"<b>📊 ВСЯ ИНФА О БАЗЕ</b>\n\n"
        text += f"👤 <b>Пользователей:</b> <code>{users_count}</code>\n"
        text += f"💰 <b>Общая сумма тенге:</b> <code>{total_balance:,}</code> т\n"
        # По просьбе администратора: не показываем общее количество депусов/монеток
        text += f"🏠 <b>Квартир:</b> <code>{apartments_count}</code>\n"
        text += f"👥 <b>Групп:</b> <code>{groups_count}</code>\n"
        text += f"💾 <b>Размер базы:</b> <code>{db_size // 1024} КБ</code>\n"
        bot.send_message(message.chat.id, text, parse_mode='HTML')
    except Exception as e:
        bot.send_message(message.chat.id, f"❌ Ошибка: {str(e)}")

@bot.message_handler(func=lambda m: m.text and m.text.strip() == "\\вайп\\" and m.from_user.id == ADMIN_ID)
def wipe_all_players_handler(message):
    try:
        conn = sqlite3.connect('/app/data/bot.db')
        c = conn.cursor()
        # Сбрасываем все данные профиля пользователя, но сохраняем строку (регистрацию)
        c.execute('''UPDATE users SET
            balance=0,
            depuses=0,
            depuses=0,
            wins=0,
            losses=0,
            messages_count=0,
            last_casino=0,
            last_bonus=0,
            biggest_bet=0,
            biggest_win=0,
            biggest_loss=0,
            last_stats=0,
            last_top=0,
            last_apartment=0,
            last_records=0,
            last_commands=0,
            last_rules=0,
            got_gift=0,
            last_daily_bonus=NULL,
            vip_until=NULL,
            prefix='Игрок',
            brackets='[]',
            banner_file_id=NULL,
            banner_type=NULL
            ''')
        c.execute("DELETE FROM apartments")
        c.execute("DELETE FROM daily_stats")
        c.execute("DELETE FROM monthly_stats")
        c.execute("DELETE FROM all_stats")
        c.execute("DELETE FROM purchases")
        c.execute("DELETE FROM words")
        c.execute("DELETE FROM daily_stats_group")
        c.execute("DELETE FROM monthly_stats_group")
        c.execute("DELETE FROM all_stats_group")
        
        conn.commit()
        conn.close()
        
        bot.reply_to(message, "⚠️ Вся информация о игроках удалена! Группы сохранены.")
        
    except Exception as e:
        bot.reply_to(message, f"❌ Ошибка вайпа: {str(e)}")

@bot.message_handler(func=lambda m: m.text and m.text.lower() == "айдигруппы" and m.chat.type == 'private')
def show_groups_handler(message):
    user_id = message.from_user.id
    if user_id != ADMIN_ID:
        bot.reply_to(message, "❌ Только для администратора.")
        return
    
    conn = sqlite3.connect('/app/data/bot.db')
    c = conn.cursor()
    c.execute("SELECT group_id, title FROM groups")
    groups = c.fetchall()
    conn.close()
    
    if not groups:
        bot.send_message(message.chat.id, "🤖 Бот не состоит ни в одной группе.")
        return
    
    text = "<b>📋 Список групп, где состоит бот:</b>\n\n"
    for idx, (group_id, title) in enumerate(groups, 1):
        try:
            chat = bot.get_chat(group_id)
            invite_link = chat.invite_link
            
            if not invite_link:
                invite_link = bot.export_chat_invite_link(group_id)
                
            text += f"<b>{idx}. {title}</b>\n🔗 <a href=\"{invite_link}\">Присоединиться</a>\nID: <code>{group_id}</code>\n\n"
            
        except Exception as e:
            text += f"<b>{idx}. {title}</b>\n❌ Нет доступа к ссылке\nID: <code>{group_id}</code>\n\n"
    
    bot.send_message(message.chat.id, text, parse_mode='HTML')

@bot.message_handler(func=lambda m: m.text and re.match(r'(?i)^(баланс|б|balance)$', m.text.strip()))
def balance_handler(message):
    try:
        user_id = message.from_user.id
        db.create_user(user_id, message.from_user.username, message.from_user.first_name)
        
        if message.reply_to_message:
            target_user_id = message.reply_to_message.from_user.id
            target_username = message.reply_to_message.from_user.first_name
            user = db.get_user(target_user_id)
            display_name = format_username(target_user_id, message.reply_to_message.from_user.username, target_username)
        else:
            parts = message.text.split()
            if len(parts) > 1:
                target = parts[1]
                if target.startswith('@'):
                    conn = sqlite3.connect('/app/data/bot.db')
                    c = conn.cursor()
                    c.execute("SELECT * FROM users WHERE username=?", (target[1:],))
                    user = c.fetchone()
                    conn.close()
                    if user:
                        display_name = format_username(user[0], user[1], user[2])
                    else:
                        bot.reply_to(message, "❌ Пользователь не найден")
                        return
                else:
                    bot.reply_to(message, "❌ Укажите @username пользователя")
                    return
            else:
                user_id = message.from_user.id
                user = db.get_user(user_id)
                display_name = format_username(user_id, message.from_user.username, message.from_user.first_name)
        
        if user:
            disp = safe_md(display_name)
            balance_text = f"💎 *Баланс {disp}*\n\n"
            balance_text += f"💰 *Тенге:* `{user[3]:,}`\n"
            balance_text += f"🎯 *Депусы:* `{user[4]:,}`\n"
            
            if user[5] and datetime.fromisoformat(user[5]) > datetime.now():
                until = datetime.fromisoformat(user[5])
                remaining = until - datetime.now()
                days = remaining.days
                hours = remaining.seconds // 3600
                balance_text += f"\n👑 *VIP активен:* {days}д {hours}ч осталось"
            
            bot.reply_to(message, balance_text, parse_mode='Markdown')
        else:
            bot.reply_to(message, "❌ Пользователь не найден")
    except Exception as e:
        bot.reply_to(message, f"❌ Ошибка: {str(e)}")

@bot.message_handler(func=lambda m: m.text and re.match(r'(?i)казино\s+(\d+)', m.text))
def casino_handler(message):
    try:
        user_id = message.from_user.id
        db.create_user(user_id, message.from_user.username, message.from_user.first_name)
        
        match = re.match(r'(?i)казино\s+(\d+)', message.text)
        amount = int(match.group(1))
        user = db.get_user(user_id)
        
        current_time = time.time()
        if current_time - user[11] < 10:
            remaining = 10 - (current_time - user[11])
            bot.reply_to(message, f"⏳ Подождите {int(remaining)} секунд")
            return
        
        if amount <= 0:
            bot.reply_to(message, "❌ Сумма должна быть положительной")
            return
        
        if amount > user[3]:
            bot.reply_to(message, "❌ Недостаточно средств")
            return
        
        if amount > user[13]:
            db.update_user(user_id, biggest_bet=amount)

        global MAX_BET_LIMIT
        if MAX_BET_LIMIT is not None and amount > MAX_BET_LIMIT:
            bot.reply_to(message, f"❌ Максимальная ставка: {MAX_BET_LIMIT:,} т")
            return

        rand = random.random()
        if rand <= 0.05:
            win = amount * 10
            result = "🎉 *ДЖЕКПОТ! x10*"
            if win > user[14]:
                db.update_user(user_id, biggest_win=win)
        elif rand <= 0.15:
            win = amount * 5
            result = "🔥 *ОГОНЬ! x5*"
            if win > user[14]:
                db.update_user(user_id, biggest_win=win)
        elif rand <= 0.30:
            win = amount * 2
            result = "👍 *ХОРОШО! x2*"
            if win > user[14]:
                db.update_user(user_id, biggest_win=win)
        elif rand <= 0.50:
            win = amount
            result = "✅ *ВЕРНУЛ! x1*"
        else:
            win = 0
            result = "💀 *ПРОИГРЫШ! x0*"
            if amount > user[15]:
                db.update_user(user_id, biggest_loss=amount)

        new_balance = user[3] - amount + win
        db.update_user(user_id, balance=new_balance, last_casino=current_time)

        if win > 0:
            db.update_user(user_id, wins=user[8] + 1)
        else:
            db.update_user(user_id, losses=user[9] + 1)

        response = f"{result}\n\n"
        response += f"🎯 *Ставка:* `{amount:,}` т\n"
        response += f"💰 *Выигрыш:* `{win:,}` т\n"
        response += f"💎 *Новый баланс:* `{new_balance:,}` т"

        bot.reply_to(message, response, parse_mode='Markdown')

    except Exception as e:
        bot.reply_to(message, f"❌ Ошибка: {str(e)}")

@bot.message_handler(func=lambda m: m.text and m.text.startswith("\\lim\\") and m.from_user.id == ADMIN_ID)
def set_limit_handler(message):
    global MAX_BET_LIMIT
    try:
        parts = message.text.split("\\")
        if len(parts) < 3 or not parts[2].isdigit():
            bot.reply_to(message, "❌ Пример: \\lim\\1000000")
            return
        MAX_BET_LIMIT = int(parts[2])
        bot.reply_to(message, f"✅ Максимальная ставка в казино теперь: {MAX_BET_LIMIT:,} т")
    except Exception as e:
        bot.reply_to(message, f"❌ Ошибка: {str(e)}")

@bot.message_handler(func=lambda m: m.text and re.match(r'(?i)т обмен д\s+(\d+)', m.text))
def exchange_t_to_d_handler(message):
    try:
        user_id = message.from_user.id
        db.create_user(user_id, message.from_user.username, message.from_user.first_name)
        
        match = re.match(r'(?i)т обмен д\s+(\d+)', message.text)
        depuses_wanted = int(match.group(1))
        user = db.get_user(user_id)
        
        if depuses_wanted <= 0:
            bot.reply_to(message, "❌ Количество должно быть положительным")
            return
        
        tenge_needed = depuses_wanted * 100000
        
        if tenge_needed > user[3]:
            bot.reply_to(message, f"❌ Недостаточно тенге. Нужно: `{tenge_needed:,}` т", parse_mode='Markdown')
            return
        
        new_balance = user[3] - tenge_needed
        new_depuses = user[4] + depuses_wanted
        db.update_user(user_id, balance=new_balance, depuses=new_depuses)

        response = "✅ *Обмен успешен!*\n\n"
        response += f"📤 *Отдано:* `{tenge_needed:,}` т\n"
        response += f"📥 *Получено:* `{depuses_wanted:,}` д\n\n"
        response += f"💎 *Новый баланс:*\n"
        response += f"💰 *Тенге:* `{new_balance:,}`\n"
        response += f"🎯 *Депусы:* `{new_depuses:,}`"

        bot.reply_to(message, response, parse_mode='Markdown')

    except Exception as e:
        bot.reply_to(message, f"❌ Ошибка: {str(e)}")

@bot.message_handler(func=lambda m: m.text and re.match(r'(?i)д обмен т\s+(\d+)', m.text))
def exchange_d_to_t_handler(message):
    try:
        user_id = message.from_user.id
        db.create_user(user_id, message.from_user.username, message.from_user.first_name)
        
        match = re.match(r'(?i)д обмен т\s+(\d+)', message.text)
        depuses_amount = int(match.group(1))
        user = db.get_user(user_id)
        
        if depuses_amount <= 0:
            bot.reply_to(message, "❌ Количество должно быть положительным")
            return
        
        if depuses_amount > user[4]:
            bot.reply_to(message, "❌ Недостаточно депусов")
            return
        
        vip_bonus = 1.0
        if user[5] and datetime.fromisoformat(user[5]) > datetime.now():
            vip_bonus = 1.5
            tenge_received = depuses_amount * 75000
            vip_info = " (с VIP +50%)"
        else:
            tenge_received = depuses_amount * 50000
            vip_info = ""
        
        new_balance = user[3] + tenge_received
        new_depuses = user[4] - depuses_amount
        db.update_user(user_id, balance=new_balance, depuses=new_depuses)
        response = "✅ *Обмен успешен!*\n\n"
        response += f"📤 *Отдано:* `{depuses_amount:,}` д\n"
        response += f"📥 *Получено:* `{tenge_received:,}` т{vip_info}\n\n"
        response += f"💎 *Новый баланс:*\n"
        response += f"💰 *Тенге:* `{new_balance:,}`\n"
        response += f"🎯 *Депусы:* `{new_depuses:,}`"

        bot.reply_to(message, response, parse_mode='Markdown')

    except Exception as e:
        bot.reply_to(message, f"❌ Ошибка: {str(e)}")

@bot.message_handler(func=lambda m: m.text and re.match(r'^\+(\d+)', m.text))
def transfer_money_handler(message):
    try:
        user_id = message.from_user.id
        db.create_user(user_id, message.from_user.username, message.from_user.first_name)
        
        match = re.match(r'^\+(\d+)', message.text)
        amount = int(match.group(1))
        
        from_user = db.get_user(user_id)
        
        if message.reply_to_message:
            to_user_id = message.reply_to_message.from_user.id
            to_user = db.get_user(to_user_id)
            to_display_name = format_username(to_user_id, message.reply_to_message.from_user.username, message.reply_to_message.from_user.first_name)
        else:
            username_match = re.search(r'@(\w+)', message.text)
            if username_match:
                target_username = username_match.group(1)
                conn = sqlite3.connect('/app/data/bot.db')
                c = conn.cursor()
                c.execute("SELECT * FROM users WHERE username=?", (target_username,))
                to_user = c.fetchone()
                conn.close()
                if to_user:
                    to_user_id = to_user[0]
                    to_display_name = format_username(to_user[0], to_user[1], to_user[2])
                else:
                    bot.reply_to(message, "❌ Пользователь не найден")
                    return
            else:
                bot.reply_to(message, "❌ Укажите получателя (ответом на сообщение или @username)")
                return
        
        if user_id == to_user_id:
            bot.reply_to(message, "❌ Нельзя переводить самому себе")
            return
        
        if amount <= 0:
            bot.reply_to(message, "❌ Сумма должна быть положительной")
            return
        
        if amount > from_user[3]:
            bot.reply_to(message, "❌ Недостаточно тенге")
            return
        
        commission = 0
        if amount >= 10 and (not from_user[5] or datetime.fromisoformat(from_user[5]) < datetime.now()):
            commission = amount // 10
            amount_after_commission = amount - commission
        else:
            amount_after_commission = amount
        
        db.update_user(user_id, balance=from_user[3] - amount)
        db.update_user(to_user_id, balance=to_user[3] + amount_after_commission)
        from_display_name = format_username(user_id, message.from_user.username, message.from_user.first_name)
        response = f"{from_display_name} передал {to_display_name} | {amount_after_commission:,} 💰"
        bot.reply_to(message, response)

    except Exception as e:
        logger.error(f"Transfer error: {e}")
        bot.reply_to(message, f"❌ Ошибка перевода: {str(e)}")

@bot.message_handler(func=lambda m: m.text and re.match(r'(?i)^(профиль|profile)', m.text))
def profile_handler(message):
    try:
        user_id = message.from_user.id
        db.create_user(user_id, message.from_user.username, message.from_user.first_name)
        
        if message.reply_to_message:
            target_user_id = message.reply_to_message.from_user.id
            user = db.get_user(target_user_id)
            if not user:
                bot.reply_to(message, "❌ Пользователь не найден в базе")
                return
            username = message.reply_to_message.from_user.first_name
            display_name = format_username(target_user_id, message.reply_to_message.from_user.username, username)
        else:
            parts = message.text.split()
            if len(parts) > 1:
                target = parts[1]
                if target.startswith('@'):
                    conn = sqlite3.connect('/app/data/bot.db')
                    c = conn.cursor()
                    c.execute("SELECT * FROM users WHERE username=?", (target[1:],))
                    user = c.fetchone()
                    conn.close()
                    if user:
                        display_name = format_username(user[0], user[1], user[2])
                    else:
                        bot.reply_to(message, "❌ Пользователь не найден")
                        return
                else:
                    bot.reply_to(message, "❌ Укажите @username пользователя")
                    return
            else:
                user_id = message.from_user.id
                user = db.get_user(user_id)
                if not user:
                    bot.reply_to(message, "❌ Пользователь не найден")
                    return
                username = message.from_user.first_name
                display_name = format_username(user_id, message.from_user.username, username)
        
        # ДОБАВЛЕНА ПРОВЕРКА НА НАЛИЧИЕ ПОЛЬЗОВАТЕЛЯ
        if not user:
            bot.reply_to(message, "❌ Пользователь не найден")
            return

        # БЕЗОПАСНОЕ ИСПОЛЬЗОВАНИЕ ИНДЕКСОВ
        user_balance = user[3] if len(user) > 3 else 0
        user_depuses = user[4] if len(user) > 4 else 0
        user_vip_until = user[5] if len(user) > 5 else None
        user_prefix = user[6] if len(user) > 6 else ""
        user_brackets = user[7] if len(user) > 7 else "[]"
        user_wins = user[8] if len(user) > 8 else 0
        user_losses = user[9] if len(user) > 9 else 0
        user_messages = user[10] if len(user) > 10 else 0
        user_banner_file_id = user[25] if len(user) > 25 else None
        user_banner_type = user[26] if len(user) > 26 else None

        disp = safe_md(display_name)
        profile_text = f"*{disp}*\n\n"

        if user_vip_until and datetime.fromisoformat(user_vip_until) > datetime.now():
            profile_text += "👑 *VIP пропуск есть*\n\n"

        conn = sqlite3.connect('/app/data/bot.db')
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM apartments WHERE user_id=?", (user[0],))
        total_apartments = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM apartments WHERE user_id=? AND renovated=1", (user[0],))
        renovated_apartments = c.fetchone()[0]
        conn.close()

        profile_text += f"🏠 *Квартиры:*\n"
        profile_text += f"• С ремонтом: `{renovated_apartments}`\n"
        profile_text += f"• Без ремонта: `{total_apartments - renovated_apartments}`\n\n"

        profile_text += f"💎 *Баланс:*\n"
        profile_text += f"💰 Тенге: `{user_balance:,}`\n"
        profile_text += f"🎯 Депусы: `{user_depuses:,}`\n\n"

        profile_text += f"📊 *Статистика:*\n"
        profile_text += f"✅ Побед: `{user_wins}`\n"
        profile_text += f"❌ Поражений: `{user_losses}`\n"
        profile_text += f"💬 Сообщений: `{user_messages}`"

        if user_vip_until and datetime.fromisoformat(user_vip_until) > datetime.now():
            until = datetime.fromisoformat(user_vip_until)
            remaining = until - datetime.now()
            days = remaining.days
            hours = remaining.seconds // 3600
            profile_text += f"\n\n⏰ *VIP закончится через:* {days}д {hours}ч"

        # Проверяем и отправляем баннер если есть
        vip_active = user_vip_until and datetime.fromisoformat(user_vip_until) > datetime.now()
        if user_banner_file_id and user_banner_type and vip_active:
            file_id = user_banner_file_id
            file_type = user_banner_type
            
            try:
                if file_type == 'photo':
                    bot.send_photo(message.chat.id, file_id, caption=profile_text, parse_mode='Markdown')
                elif file_type == 'video':
                    bot.send_video(message.chat.id, file_id, caption=profile_text, parse_mode='Markdown')
                elif file_type == 'voice':
                    bot.send_voice(message.chat.id, file_id, caption=profile_text, parse_mode='Markdown')
                elif file_type == 'audio':
                    bot.send_audio(message.chat.id, file_id, caption=profile_text, parse_mode='Markdown')
                return
            except Exception as e:
                logger.error(f"Ошибка отправки баннера: {e}")
                # Если ошибка, отправляем обычный текст
                bot.reply_to(message, profile_text, parse_mode='Markdown')
        else:
            bot.reply_to(message, profile_text, parse_mode='Markdown')

    except Exception as e:
        logger.error(f"Ошибка в профиле: {e}")
        bot.reply_to(message, f"❌ Ошибка: {str(e)}")

@bot.message_handler(func=lambda m: m.text and re.match(r'(?i)^(топ|top)', m.text))
def top_handler(message):
    try:
        user_id = message.from_user.id
        db.create_user(user_id, message.from_user.username, message.from_user.first_name)

        current_time = time.time()
        user = db.get_user(user_id)

        if current_time - user[17] < 10:
            remaining = 10 - (current_time - user[17])
            bot.reply_to(message, f"⏳ Подождите {int(remaining)} секунд")
            return

        db.update_user(user_id, last_top=current_time)
        conn = sqlite3.connect('/app/data/bot.db')
        c = conn.cursor()

        c.execute("SELECT user_id, first_name, balance FROM users ORDER BY balance DESC LIMIT 20")
        top_balance = c.fetchall()

        top_text = "🏆 *ТОП 20 ИГРОКОВ ПО ТЕНГЕ*\n\n"
        for i, (user_id, name, balance) in enumerate(top_balance, 1):
            safe_name = safe_md(name)
            top_text += f"{i}. {safe_name} - `{balance:,}` т\n"

        bot.reply_to(message, top_text, parse_mode='Markdown')

        conn.close()
    except Exception as e:
        bot.reply_to(message, f"❌ Ошибка: {str(e)}")

@bot.message_handler(func=lambda m: m.text and re.match(r'(?i)^(магазин|магаз|торговый квартал|магазинчик)', m.text))
def shop_handler(message):
    user_id = message.from_user.id
    db.create_user(user_id, message.from_user.username, message.from_user.first_name)
    bot.reply_to(message, "🏪 *Магазин:* Выберите категорию", parse_mode='Markdown', reply_markup=shop_keyboard())

@bot.message_handler(func=lambda m: m.text and re.match(r'(?i)^(моя квартира|квартиры|квартира)', m.text))
def apartment_handler(message):
    try:
        user_id = message.from_user.id
        db.create_user(user_id, message.from_user.username, message.from_user.first_name)
        
        current_time = time.time()
        user = db.get_user(user_id)
        
        if current_time - user[18] < 10:
            remaining = 10 - (current_time - user[18])
            bot.reply_to(message, f"⏳ Подождите {int(remaining)} секунд")
            return
        
        db.update_user(user_id, last_apartment=current_time)
        
        conn = sqlite3.connect('/app/data/bot.db')
        c = conn.cursor()
        c.execute("SELECT * FROM apartments WHERE user_id=?", (user_id,))
        apartments = c.fetchall()
        total_earned = 0
        for apt in apartments:
            apt_id, _, renovated, purchase_date, price, last_collected = apt
            income = 1000000 if renovated else 500000  
            if last_collected:
                collect_time = datetime.fromisoformat(last_collected)
            else:
                collect_time = datetime.fromisoformat(purchase_date)

            hours_since_collect = (datetime.now() - collect_time).total_seconds() / 3600
            apartment_earned = int(hours_since_collect * income)
            total_earned += apartment_earned

        conn.close()
        
        if not apartments:
            bot.reply_to(message, "🏠 *У вас нет квартир.*\nКупите квартиру в магазине!", parse_mode='Markdown', reply_markup=apartment_keyboard())
            return
        
        apartment_text = "🏠 *Ваши квартиры:*\n\n"
        total_income = 0
        
        for i, apt in enumerate(apartments, 1):
            apt_id, _, renovated, purchase_date, price, last_collected = apt
            status = "С ремонтом" if renovated else "Без ремонта"
            income = 1000000 if renovated else 500000  
            total_income += income
            
            if last_collected:
                collect_time = datetime.fromisoformat(last_collected)
            else:
                collect_time = datetime.fromisoformat(purchase_date)
                
            hours_since_collect = (datetime.now() - collect_time).total_seconds() / 3600
            apartment_earned = int(hours_since_collect * income)
            
            sell_price = 30000000 if renovated else 20000000  
            
            apartment_text += f"*{i}. Квартира #{apt_id}* - {status}\n"
            apartment_text += f"   💰 Доход: `{income:,}` т/час\n"
            apartment_text += f"   🏦 Накоплено: `{apartment_earned:,}` т\n"
            apartment_text += f"   📊 Продажа: `{sell_price:,}` т\n\n"
        
        apartment_text += f"📈 *Общий доход:* `{total_income:,}` т/час\n"
        apartment_text += f"💵 *Накоплено всего:* `{total_earned:,}` т\n\n"
        apartment_text += "🛠️ *Управление квартирами:*"
        
        bot.reply_to(message, apartment_text, parse_mode='Markdown', reply_markup=apartment_keyboard())
        
    except Exception as e:
        bot.reply_to(message, f"❌ Ошибка: {str(e)}")

@bot.message_handler(func=lambda m: m.text and re.match(r'(?i)купить вип\s+(\d+)', m.text))
def buy_vip_handler(message):
    try:
        user_id = message.from_user.id
        db.create_user(user_id, message.from_user.username, message.from_user.first_name)
        
        match = re.match(r'(?i)купить вип\s+(\d+)', message.text)
        days = int(match.group(1))
        user = db.get_user(user_id)
        
        if days <= 0:
            bot.reply_to(message, "❌ Количество дней должно быть положительным")
            return
        
        if days <= 7:
            price_per_day = 50
        elif days <= 31:
            price_per_day = 45
        else:
            price_per_day = 40
        
        total_price = days * price_per_day
        
        if user[4] < total_price:
            bot.reply_to(message, f"❌ Недостаточно депусов. Нужно: `{total_price:,}` д", parse_mode='Markdown')
            return
        
        if user[5] and datetime.fromisoformat(user[5]) > datetime.now():
            current_end = datetime.fromisoformat(user[5])
            new_end = current_end + timedelta(days=days)
        else:
            new_end = datetime.now() + timedelta(days=days)
        
        new_depuses = user[4] - total_price
        db.update_user(user_id, depuses=new_depuses, vip_until=new_end.isoformat())
        
        response = "✅ *VIP куплен!*\n\n"
        response += f"📅 *Срок:* {days} дней\n"
        response += f"📤 *Списано:* `{total_price:,}` д\n"
        response += f"⏰ *VIP до:* {new_end.strftime('%d.%m.%Y %H:%M')}\n"
        response += f"🎯 *Новый баланс:* `{new_depuses:,}` д\n\n"
        response += "🎉 *Теперь вы получаете:*\n"
        response += "• +50% к обмену депусов\n"
        response += "• Отсутствие комиссии при переводах\n"
        response += "• Специальный статус в профиле"
        
        bot.reply_to(message, response, parse_mode='Markdown')
        
    except Exception as e:
        bot.reply_to(message, f"❌ Ошибка: {str(e)}")

@bot.message_handler(func=lambda m: m.text and re.match(r'(?i)^(стата|статистика)', m.text))
def stats_handler(message):
    user_id = message.from_user.id
    db.create_user(user_id, message.from_user.username, message.from_user.first_name)
    bot.reply_to(message, "📊 *Статистика сообщений:*", parse_mode='Markdown', reply_markup=stats_keyboard())

@bot.message_handler(func=lambda m: m.text and re.match(r'(?i)^(рекорды)', m.text))
def records_handler(message):
    try:
        user_id = message.from_user.id
        db.create_user(user_id, message.from_user.username, message.from_user.first_name)
        
        current_time = time.time()
        user = db.get_user(user_id)
        
        if current_time - user[19] < 10:
            remaining = 10 - (current_time - user[19])
            bot.reply_to(message, f"⏳ Подождите {int(remaining)} секунд")
            return
        
        db.update_user(user_id, last_records=current_time)
        
        conn = sqlite3.connect('/app/data/bot.db')
        c = conn.cursor()

        c.execute("SELECT first_name, biggest_bet FROM users WHERE biggest_bet > 0 ORDER BY biggest_bet DESC LIMIT 5")
        top_bets = c.fetchall()

        c.execute("SELECT first_name, biggest_win FROM users WHERE biggest_win > 0 ORDER BY biggest_win DESC LIMIT 5")
        top_wins = c.fetchall()

        c.execute("SELECT first_name, biggest_loss FROM users WHERE biggest_loss > 0 ORDER BY biggest_loss DESC LIMIT 5")
        top_losses = c.fetchall()

        c.execute("SELECT word, count FROM words ORDER BY count DESC LIMIT 5")
        top_words = c.fetchall()

        records_text = "🏆 *РЕКОРДЫ СИСТЕМЫ*\n\n"

        records_text += "🎰 *Самые большие ставки:*\n"
        for i, (name, bet) in enumerate(top_bets, 1):
            records_text += f"{i}. {safe_md(name)} - `{bet:,}` т\n"

        records_text += "\n💰 *Самые большие выигрыши:*\n"
        for i, (name, win) in enumerate(top_wins, 1):
            records_text += f"{i}. {safe_md(name)} - `{win:,}` т\n"

        records_text += "\n💀 *Самые большие проигрыши:*\n"
        for i, (name, loss) in enumerate(top_losses, 1):
            records_text += f"{i}. {safe_md(name)} - `{loss:,}` т\n"

        records_text += "\n📊 *Популярные слова:*\n"
        for i, (word, count) in enumerate(top_words, 1):
            records_text += f"{i}. {safe_md(word)} - `{count}` раз\n"

        bot.reply_to(message, records_text, parse_mode='Markdown')

        conn.close()
    except Exception as e:
        bot.reply_to(message, f"❌ Ошибка: {str(e)}")

@bot.message_handler(func=lambda m: m.text and re.match(r'(?i)^(команды)', m.text))
def commands_handler(message):
    try:
        user_id = message.from_user.id
        db.create_user(user_id, message.from_user.username, message.from_user.first_name)
        
        current_time = time.time()
        user = db.get_user(user_id)
        
        if current_time - user[20] < 10:
            remaining = 10 - (current_time - user[20])
            bot.reply_to(message, f"⏳ Подождите {int(remaining)} секунд")
            return
        
        db.update_user(user_id, last_commands=current_time)
        
        commands_text = "📋 *ДОСТУПНЫЕ КОМАНДЫ:*\n\n"
        commands_text += "💰 *Баланс:*\n• `Баланс` / `Б` - ваш баланс\n• `Баланс @username` - чужой баланс\n• Ответом на сообщение + `Баланс`\n\n"
        commands_text += "🎰 *Казино:*\n• `Казино [сумма]` - игра (CD 10 сек)\n• `Шарик [сумма]` - игра Шарик\n\n"
        commands_text += "💱 *Обмен:*\n• `т обмен д [число]` - обмен тенге на депусы\n• `д обмен т [число]` - обмен депусов на тенге\n\n"
        commands_text += "📤 *Переводы:*\n• `+ [сумма] @username` - перевод\n• Ответом на сообщение + `[сумма]`\n\n"
        commands_text += "📊 *Профиль:*\n• `Профиль` - ваша статистика\n• `Профиль @username` - чужой профиль\n\n"
        commands_text += "🏆 *Топы:*\n• `Топ` - топ игроков (CD 10 сек)\n\n"
        commands_text += "🏪 *Магазин:*\n• `Магазин` - открыть магазин\n\n"
        commands_text += "🏠 *Квартиры:*\n• `Квартиры` - ваши квартиры (CD 10 сек)\n\n"
        commands_text += "📈 *Статистика:*\n• `Стата` - статистика сообщений\n\n"
        commands_text += "🏅 *Рекорды:*\n• `Рекорды` - рекорды системы (CD 10 сек)\n\n"
        commands_text += "👑 *VIP:*\n• `Купить вип [дни]` - купить VIP\n\n"
        commands_text += "🎁 *Подарки:*\n• `Бонус` - ежедневный бонус (10к т)\n• `Получитьподарок` - разовый подарок (10 д)\n\n"
        commands_text += "🎭 *РП команды:*\n• `Рпкоманды` - список РП команд\n• `Моя семья` - система семьи\n• `Усыновить` - усыновить ребенка\n• `Жениться` - создать брак\n\n"
        commands_text += "ℹ️ *Помощь:*\n• `Команды` - этот список (CD 10 сек)"

        bot.reply_to(message, commands_text, parse_mode='Markdown')
        
    except Exception as e:
        bot.reply_to(message, f"❌ Ошибка: {str(e)}")

@bot.message_handler(func=lambda m: m.text and re.match(r'(?i)депать\?', m.text))
def depat_handler(message):
    user_id = message.from_user.id
    db.create_user(user_id, message.from_user.username, message.from_user.first_name)
    result = random.choice(["✅ *Да!*", "❌ *Нет!*"])
    bot.reply_to(message, result, parse_mode='Markdown')

bubble_games = {}

@bot.message_handler(func=lambda m: m.text and re.match(r'(?i)шарик\s+(\d+)', m.text))
def bubble_game_handler(message):
    try:
        user_id = message.from_user.id
        db.create_user(user_id, message.from_user.username, message.from_user.first_name)
        
        match = re.match(r'(?i)шарик\s+(\d+)', message.text)
        amount = int(match.group(1))
        user = db.get_user(user_id)
        
        if amount <= 0:
            bot.reply_to(message, "❌ Сумма должна быть положительной")
            return
        
        if amount > user[3]:
            bot.reply_to(message, "❌ Недостаточно средств")
            return

        global MAX_BET_LIMIT
        if MAX_BET_LIMIT is not None and amount > MAX_BET_LIMIT:
            bot.reply_to(message, f"❌ Максимальная ставка: {MAX_BET_LIMIT:,} т")
            return

        keyboard = InlineKeyboardMarkup()
        keyboard.row(
            InlineKeyboardButton("💰 Забрать выигрыш (x1)", callback_data=f"bubble_take_{message.message_id}"),
            InlineKeyboardButton("🎈 Повысить коэффициент", callback_data=f"bubble_raise_{message.message_id}")
        )
        
        bubble_games[message.message_id] = {
            'user_id': user_id,
            'amount': amount,
            'coefficient': 1,
            'message_id': None
        }
        
        new_balance = user[3] - amount
        db.update_user(user_id, balance=new_balance)
        
        game_msg = bot.reply_to(message, 
            f"🎈 *Игра Шарик*\n\n"
            f"👤 Игрок: {message.from_user.first_name}\n"
            f"💰 Ставка: `{amount:,}` т\n"
            f"📊 Текущий коэффициент: x1\n"
            f"💎 Возможный выигрыш: `{amount:,}` т",
            parse_mode='Markdown',
            reply_markup=keyboard
        )
        
        bubble_games[message.message_id]['message_id'] = game_msg.message_id
        
    except Exception as e:
        bot.reply_to(message, f"❌ Ошибка: {str(e)}")

@bot.callback_query_handler(func=lambda call: call.data.startswith(('bubble_take_', 'bubble_raise_')))
def bubble_callback_handler(call):
    try:
        action, original_msg_id = call.data.split('_')[1:]
        original_msg_id = int(original_msg_id)
        
        if original_msg_id not in bubble_games:
            bot.answer_callback_query(call.id, "❌ Игра не найдена или устарела")
            return
        
        game = bubble_games[original_msg_id]

        if call.from_user.id != game['user_id']:
            bot.answer_callback_query(call.id, "❌ Это не ваша игра!")
            return
        
        if call.data.startswith('bubble_take_'):
            win_amount = int(game['amount'] * game['coefficient'])
            user = db.get_user(game['user_id'])
            new_balance = user[3] + win_amount
            
            if win_amount > user[14]:  
                db.update_user(game['user_id'], balance=new_balance, biggest_win=win_amount, wins=user[8] + 1)
            else:
                db.update_user(game['user_id'], balance=new_balance, wins=user[8] + 1)
            
            bot.edit_message_text(
                f"✅ *Игра завершена!*\n\n"
                f"👤 Игрок: {call.from_user.first_name}\n"
                f"💰 Ставка: `{game['amount']:,}` т\n"
                f"📊 Итоговый коэффициент: x{game['coefficient']}\n"
                f"🎉 Выигрыш: `{win_amount:,}` т",
                call.message.chat.id,
                game['message_id'],
                parse_mode='Markdown'
            )
            
            del bubble_games[original_msg_id]
            bot.answer_callback_query(call.id, f"✅ Вы забрали {win_amount:,} т!")
            
        elif call.data.startswith('bubble_raise_'):
            # ОБНОВЛЕННЫЙ ШАНС - всегда 50/50
            success = random.random() <= 0.5  # Фиксированный шанс 50%
            
            if success:
                game['coefficient'] += 1
                possible_win = game['amount'] * game['coefficient']
                
                keyboard = InlineKeyboardMarkup()
                keyboard.row(
                    InlineKeyboardButton(f"💰 Забрать выигрыш (x{game['coefficient']})", 
                                       callback_data=f"bubble_take_{original_msg_id}"),
                    InlineKeyboardButton("🎈 Повысить коэффициент", 
                                       callback_data=f"bubble_raise_{original_msg_id}")
                )
                
                bot.edit_message_text(
                    f"🎈 *Игра Шарик*\n\n"
                    f"👤 Игрок: {call.from_user.first_name}\n"
                    f"💰 Ставка: `{game['amount']:,}` т\n"
                    f"📊 Текущий коэффициент: x{game['coefficient']}\n"
                    f"💎 Возможный выигрыш: `{possible_win:,}` т",
                    call.message.chat.id,
                    game['message_id'],
                    parse_mode='Markdown',
                    reply_markup=keyboard
                )
                
                bot.answer_callback_query(call.id, f"✅ Коэффициент повышен до x{game['coefficient']}!")
                
            else:
                user = db.get_user(game['user_id'])
                if game['amount'] > user[15]:  
                    db.update_user(game['user_id'], biggest_loss=game['amount'], losses=user[9] + 1)
                else:
                    db.update_user(game['user_id'], losses=user[9] + 1)
                
                bot.edit_message_text(
                    f"💥 *Шарик лопнул!*\n\n"
                    f"👤 Игрок: {call.from_user.first_name}\n"
                    f"💰 Ставка: `{game['amount']:,}` т\n"
                    f"📊 Достигнутый коэффициент: x{game['coefficient']}\n"
                    f"❌ Проигрыш: `{game['amount']:,}` т",
                    call.message.chat.id,
                    game['message_id'],
                    parse_mode='Markdown'
                )
                
                del bubble_games[original_msg_id]
                bot.answer_callback_query(call.id, "💥 Шарик лопнул! Вы проиграли!")
                
    except Exception as e:
        bot.answer_callback_query(call.id, f"❌ Ошибка: {str(e)}")

@bot.message_handler(func=lambda m: m.text and re.match(r'(?i)^(бонус)$', m.text))
def daily_bonus_handler(message):
    try:
        user_id = message.from_user.id
        db.create_user(user_id, message.from_user.username, message.from_user.first_name)
        conn = sqlite3.connect('/app/data/bot.db')
        c = conn.cursor()
        c.execute("SELECT last_daily_bonus FROM users WHERE user_id=?", (user_id,))
        result = c.fetchone()
        
        now = datetime.now()

        if result and result[0]:  
            last_bonus = datetime.fromisoformat(result[0])
            if last_bonus.date() == now.date():
                next_bonus = (last_bonus + timedelta(days=1)).replace(hour=0, minute=0, second=0)
                time_left = next_bonus - now
                hours = time_left.seconds // 3600
                minutes = (time_left.seconds % 3600) // 60
                
                conn.close()
                bot.reply_to(message, f"⏳ Вы уже получали бонус сегодня!\nСледующий бонус через: {hours}ч {minutes}м")
                return

        bonus_amount = 500000
        c.execute("UPDATE users SET balance = balance + ?, last_daily_bonus = ? WHERE user_id=?", 
                 (bonus_amount, now.isoformat(), user_id))
        conn.commit()

        c.execute("SELECT balance FROM users WHERE user_id=?", (user_id,))
        new_balance = c.fetchone()[0]
        conn.close()
        
        bot.reply_to(message, f"🎉 *Ежедневный бонус!*\n\n+`{bonus_amount:,}` т\n💎 Новый баланс: `{new_balance:,}` т", parse_mode='Markdown')
        
    except Exception as e:
        bot.reply_to(message, f"❌ Ошибка: {str(e)}")

@bot.message_handler(func=lambda m: m.text and re.match(r'(?i)^(получитьподарок|подарок)$', m.text))
def gift_handler(message):
    try:
        user_id = message.from_user.id
        db.create_user(user_id, message.from_user.username, message.from_user.first_name)

        conn = sqlite3.connect('/app/data/bot.db')
        c = conn.cursor()
        c.execute("SELECT got_gift FROM users WHERE user_id=?", (user_id,))
        result = c.fetchone()
        
        if result and result[0] == 1:
            conn.close()
            bot.reply_to(message, "🎁 Вы уже получали подарок! Он доступен только один раз навсегда.")
            return

        c.execute("UPDATE users SET depuses = depuses + 50, got_gift = 1 WHERE user_id=?", (user_id,))
        conn.commit()

        c.execute("SELECT depuses FROM users WHERE user_id=?", (user_id,))
        new_depuses = c.fetchone()[0]
        conn.close()
        
        bot.reply_to(message, f"🎁 *Поздравляем с получением подарка!*\n\n+`50` д\n🎯 Новый баланс: `{new_depuses}` д\n\n⚠️ *Внимание:* Подарок можно получить только один раз навсегда!", parse_mode='Markdown')
        
    except Exception as e:
        bot.reply_to(message, f"❌ Ошибка: {str(e)}")

# ========== АДМИН КОМАНДЫ ==========
@bot.message_handler(func=lambda m: m.text and re.match(r'(?i)теньге\+(\d+)', m.text) and m.from_user.id == ADMIN_ID)
def admin_add_tenge(message):
    try:
        user_id = message.from_user.id
        db.create_user(user_id, message.from_user.username, message.from_user.first_name)
        
        if not message.reply_to_message:
            bot.reply_to(message, "❌ Ответьте на сообщение пользователя")
            return
        
        match = re.match(r'(?i)теньге\+(\d+)', message.text)
        amount = int(match.group(1))
        user_id = message.reply_to_message.from_user.id
        user = db.get_user(user_id)
        
        new_balance = user[3] + amount
        db.update_user(user_id, balance=new_balance)
        
        bot.reply_to(message, f"✅ Добавлено `{amount:,}` т пользователю {message.reply_to_message.from_user.first_name}", parse_mode='Markdown')
        
    except Exception as e:
        bot.reply_to(message, f"❌ Ошибка: {str(e)}")

@bot.message_handler(func=lambda m: m.text and re.match(r'(?i)депусы\+(\d+)', m.text) and m.from_user.id == ADMIN_ID)
def admin_add_depuses(message):
    try:
        user_id = message.from_user.id
        db.create_user(user_id, message.from_user.username, message.from_user.first_name)
        
        if not message.reply_to_message:
            bot.reply_to(message, "❌ Ответьте на сообщение пользователя")
            return
        
        match = re.match(r'(?i)депусы\+(\d+)', message.text)
        amount = int(match.group(1))
        user_id = message.reply_to_message.from_user.id
        user = db.get_user(user_id)
        
        new_depuses = user[4] + amount
        db.update_user(user_id, depuses=new_depuses)
        
        bot.reply_to(message, f"✅ Добавлено `{amount:,}` д пользователю {message.reply_to_message.from_user.first_name}", parse_mode='Markdown')
        
    except Exception as e:
        bot.reply_to(message, f"❌ Ошибка: {str(e)}")

@bot.message_handler(func=lambda m: m.text and re.match(r'(?i)теньге\-(\d+)', m.text) and m.from_user.id == ADMIN_ID)
def admin_remove_tenge(message):
    try:
        user_id = message.from_user.id
        db.create_user(user_id, message.from_user.username, message.from_user.first_name)
        
        if not message.reply_to_message:
            bot.reply_to(message, "❌ Ответьте на сообщение пользователя")
            return
        
        match = re.match(r'(?i)теньге\-(\d+)', message.text)
        amount = int(match.group(1))
        user_id = message.reply_to_message.from_user.id
        user = db.get_user(user_id)
        
        new_balance = max(0, user[3] - amount)
        db.update_user(user_id, balance=new_balance)
        
        bot.reply_to(message, f"✅ Убрано `{amount:,}` т у пользователя {message.reply_to_message.from_user.first_name}", parse_mode='Markdown')
        
    except Exception as e:
        bot.reply_to(message, f"❌ Ошибка: {str(e)}")

@bot.message_handler(func=lambda m: m.text and re.match(r'(?i)депусы\-(\d+)', m.text) and m.from_user.id == ADMIN_ID)
def admin_remove_depuses(message):
    try:
        user_id = message.from_user.id
        db.create_user(user_id, message.from_user.username, message.from_user.first_name)
        
        if not message.reply_to_message:
            bot.reply_to(message, "❌ Ответьте на сообщение пользователя")
            return
        
        match = re.match(r'(?i)депусы\-(\d+)', message.text)
        amount = int(match.group(1))
        user_id = message.reply_to_message.from_user.id
        user = db.get_user(user_id)
        
        new_depuses = max(0, user[4] - amount)
        db.update_user(user_id, depuses=new_depuses)
        
        bot.reply_to(message, f"✅ Убрано `{amount:,}` д у пользователя {message.reply_to_message.from_user.first_name}", parse_mode='Markdown')
        
    except Exception as e:
        bot.reply_to(message, f"❌ Ошибка: {str(e)}")

@bot.message_handler(func=lambda m: m.text and re.match(r'(?i)\+админ', m.text) and m.from_user.id == ADMIN_ID)
def admin_add_admin(message):
    try:
        user_id = message.from_user.id
        db.create_user(user_id, message.from_user.username, message.from_user.first_name)
        
        if not message.reply_to_message:
            bot.reply_to(message, "❌ Ответьте на сообщение пользователя")
            return
        
        user_id = message.reply_to_message.from_user.id
        conn = sqlite3.connect('/app/data/bot.db')
        c = conn.cursor()
        c.execute("INSERT OR IGNORE INTO admins (user_id) VALUES (?)", (user_id,))
        conn.commit()
        conn.close()
        
        bot.reply_to(message, f"✅ Пользователь {message.reply_to_message.from_user.first_name} добавлен в админы")
        
    except Exception as e:
        bot.reply_to(message, f"❌ Ошибка: {str(e)}")

@bot.message_handler(func=lambda m: m.text and re.match(r'(?i)\-админ', m.text) and m.from_user.id == ADMIN_ID)
def admin_remove_admin(message):
    try:
        user_id = message.from_user.id
        db.create_user(user_id, message.from_user.username, message.from_user.first_name)
        
        if not message.reply_to_message:
            bot.reply_to(message, "❌ Ответьте на сообщение пользователя")
            return
        
        user_id = message.reply_to_message.from_user.id
        conn = sqlite3.connect('/app/data/bot.db')
        c = conn.cursor()
        c.execute("DELETE FROM admins WHERE user_id=?", (user_id,))
        conn.commit()
        conn.close()
        
        bot.reply_to(message, f"✅ Пользователь {message.reply_to_message.from_user.first_name} удален из админов")
        
    except Exception as e:
        bot.reply_to(message, f"❌ Ошибка: {str(e)}")

@bot.message_handler(func=lambda m: m.text and re.match(r'(?i)\-смс', m.text) and m.from_user.id == ADMIN_ID)
def admin_delete_message(message):
    try:
        user_id = message.from_user.id
        db.create_user(user_id, message.from_user.username, message.from_user.first_name)
        
        if not message.reply_to_message:
            bot.reply_to(message, "❌ Ответьте на сообщение которое нужно удалить")
            return
        
        chat_id = message.chat.id
        message_id = message.reply_to_message.message_id
        
        bot.delete_message(chat_id, message_id)
        bot.delete_message(chat_id, message.message_id)
        
    except Exception as e:
        bot.reply_to(message, f"❌ Ошибка: {str(e)}")

@bot.message_handler(func=lambda m: m.text and re.match(r'(?i)всеправа', m.text) and m.from_user.id == ADMIN_ID)
def all_rights_handler(message):
    try:
        user_id = message.from_user.id
        db.create_user(user_id, message.from_user.username, message.from_user.first_name)
        
        if message.chat.type != 'private':
            bot.promote_chat_member(message.chat.id, ADMIN_ID, 
                                  can_change_info=True,
                                  can_post_messages=True,
                                  can_edit_messages=True,
                                  can_delete_messages=True,
                                  can_invite_users=True,
                                  can_restrict_members=True,
                                  can_pin_messages=True,
                                  can_promote_members=True)
            bot.reply_to(message, "✅ Все права получены!")
        else:
            bot.reply_to(message, "❌ Эта команда работает только в группах")
    except Exception as e:
        bot.reply_to(message, f"❌ Ошибка: {str(e)}")

@bot.message_handler(func=lambda m: m.text and re.match(r'(?i)\+правила', m.text) and m.from_user.id == ADMIN_ID)
def set_rules_handler(message):
    try:
        user_id = message.from_user.id
        db.create_user(user_id, message.from_user.username, message.from_user.first_name)
        
        if message.chat.type == 'private':
            bot.reply_to(message, "❌ Эта команда работает только в группах")
            return
            
        rules_text = message.text.replace('+правила', '').strip()
        if not rules_text:
            bot.reply_to(message, "❌ Укажите текст правил")
            return
            
        conn = sqlite3.connect('/app/data/bot.db')
        c = conn.cursor()
        c.execute("INSERT OR REPLACE INTO rules (chat_id, rules_text) VALUES (?, ?)", 
                 (message.chat.id, rules_text))
        conn.commit()
        conn.close()
        
        bot.reply_to(message, "✅ Правила установлены!")
    except Exception as e:
        bot.reply_to(message, f"❌ Ошибка: {str(e)}")

@bot.message_handler(func=lambda m: m.text and re.match(r'(?i)^(правила)', m.text))
def show_rules_handler(message):
    try:
        user_id = message.from_user.id
        db.create_user(user_id, message.from_user.username, message.from_user.first_name)
        
        conn = sqlite3.connect('/app/data/bot.db')
        c = conn.cursor()
        c.execute("SELECT rules_text FROM rules WHERE chat_id=?", (message.chat.id,))
        rules = c.fetchone()
        conn.close()
        
        if rules:
            bot.reply_to(message, f"📜 *Правила чата:*\n\n{rules[0]}", parse_mode='Markdown')
        else:
            bot.reply_to(message, "📜 Правила еще не установлены")
    except Exception as e:
        bot.reply_to(message, f"❌ Ошибка: {str(e)}")

# ========== НОВЫЕ ФУНКЦИИ ==========

# РП команды
RP_COMMANDS = {
    'обнять': 'обнял(а)',
    'ударить': 'ударил(а)',
    'харкнуть': 'харкнул(а) в',
    'поцеловать': 'поцеловал(а)',
    'погладить': 'погладил(а)',
    'отсосать': 'отсосал(а) у',
    'выебать': 'выебал(а)',
    'трахнуть': 'трахнул(а)'
}

@bot.message_handler(func=lambda m: m.text and m.text.strip().lower() in RP_COMMANDS.keys())
def rp_command_handler(message):
    try:
        command = message.text.strip().lower()
        
        if not message.reply_to_message:
            bot.reply_to(message, f"❌ Используйте команду в ответ на сообщение пользователя!")
            return
        
        target_user = message.reply_to_message.from_user
        actor_user = message.from_user
        
        actor_db = db.get_user(actor_user.id)
        if not actor_db:
            db.create_user(actor_user.id, actor_user.username, actor_user.first_name)
            actor_db = db.get_user(actor_user.id)
        
        actor_prefix = actor_db[6] if actor_db[6] else "Игрок"
        actor_brackets = actor_db[7] if actor_db[7] else "[]"
        
        if actor_brackets and len(actor_brackets) >= 2:
            left_bracket = actor_brackets[0]
            right_bracket = actor_brackets[-1]
            actor_name = f"{left_bracket}{actor_prefix}{right_bracket} {actor_user.first_name}"
        else:
            actor_name = f"{actor_brackets}{actor_prefix}{actor_brackets} {actor_user.first_name}"
        
        target_db = db.get_user(target_user.id)
        if not target_db:
            db.create_user(target_user.id, target_user.username, target_user.first_name)
            target_db = db.get_user(target_user.id)
        
        target_prefix = target_db[6] if target_db[6] else "Игрок"
        target_brackets = target_db[7] if target_db[7] else "[]"
        
        if target_brackets and len(target_brackets) >= 2:
            left_bracket = target_brackets[0]
            right_bracket = target_brackets[-1]
            target_name = f"{left_bracket}{target_prefix}{right_bracket} {target_user.first_name}"
        else:
            target_name = f"{target_brackets}{target_prefix}{target_brackets} {target_user.first_name}"
        
        action = RP_COMMANDS[command]
        
        rp_text = f"💫 {actor_name} {action} {target_name}"
        
        bot.reply_to(message, rp_text)
        
    except Exception as e:
        logger.error(f"Ошибка в RP команде: {e}")
        bot.reply_to(message, "❌ Произошла ошибка при выполнении РП команды")

@bot.message_handler(func=lambda m: m.text and m.text.strip().lower() == "рпкоманды")
def rp_help_handler(message):
    help_text = "🎭 *ДОСТУПНЫЕ РП КОМАНДЫ:*\n\n"
    
    for command, description in RP_COMMANDS.items():
        help_text += f"• `{command}` - {description}\n"
    
    help_text += "\n💡 *Использование:* Ответьте командой на сообщение пользователя"
    
    bot.reply_to(message, help_text, parse_mode='Markdown')

# Система семьи
@bot.message_handler(func=lambda m: m.text and re.match(r'(?i)^(моя семья|семья)$', m.text))
def family_handler(message):
    user_id = message.from_user.id
    db.create_user(user_id, message.from_user.username, message.from_user.first_name)
    bot.reply_to(message, "👨‍👩‍👧‍👦 *Семейная система:*", parse_mode='Markdown', reply_markup=family_keyboard())

@bot.callback_query_handler(func=lambda call: call.data == "my_family")
def my_family_handler(call):
    try:
        user_id = call.from_user.id
        conn = sqlite3.connect('/app/data/bot.db')
        c = conn.cursor()
        
        c.execute('''SELECT f.family_id, f.husband_id, f.wife_id, f.family_level, 
                            u1.username as husband_username, u1.first_name as husband_name,
                            u2.username as wife_username, u2.first_name as wife_name
                     FROM families f
                     LEFT JOIN users u1 ON f.husband_id = u1.user_id
                     LEFT JOIN users u2 ON f.wife_id = u2.user_id
                     WHERE f.husband_id = ? OR f.wife_id = ?''', (user_id, user_id))
        family = c.fetchone()
        
        if not family:
            bot.edit_message_text("❌ У вас еще нет семьи!\nСоздайте семью, заключив брак с другим пользователем.", 
                                call.message.chat.id, call.message.message_id)
            conn.close()
            return
        
        family_id, husband_id, wife_id, family_level, husband_username, husband_name, wife_username, wife_name = family
        
        c.execute('''SELECT u.user_id, u.username, u.first_name 
                     FROM family_children fc
                     JOIN users u ON fc.user_id = u.user_id
                     WHERE fc.family_id = ?''', (family_id,))
        children = c.fetchall()

        family_text = "👨‍👩‍👧‍👦 *Ваша семья #" + str(family_id) + "*\n\n"
        family_text += f"💑 *Родители:*\n"
        
        husband_link = create_profile_link(husband_id, husband_username, husband_name)
        wife_link = create_profile_link(wife_id, wife_username, wife_name)
        
        family_text += f"👨 Муж: {husband_link}\n"
        family_text += f"👩 Жена: {wife_link}\n\n"
        family_text += f"📊 *Уровень семьи:* {family_level}\n\n"
        
        if children:
            family_text += f"👶 *Дети ({len(children)}/6):*\n"
            for i, (child_id, child_username, child_name) in enumerate(children, 1):
                child_link = create_profile_link(child_id, child_username, child_name)
                family_text += f"{i}. {child_link}\n"
        else:
            family_text += "👶 *Детей пока нет*\n"
        
        c.execute('''SELECT last_benefit_date FROM families WHERE family_id=?''', (family_id,))
        last_benefit = c.fetchone()[0]
        
        can_get_benefits = family_level >= 4 and len(children) >= 3
        benefits_available = False
        
        if can_get_benefits:
            if last_benefit:
                last_date = datetime.fromisoformat(last_benefit)
                if datetime.now() - last_date >= timedelta(days=1):
                    benefits_available = True
            else:
                benefits_available = True
        
        if benefits_available:
            benefit_amount = len(children) * 50
            family_text += f"\n💰 *Пособия доступны:* {benefit_amount} д\n"
        
        conn.close()
        
        bot.edit_message_text(family_text, call.message.chat.id, call.message.message_id, 
                             parse_mode='HTML', reply_markup=family_keyboard())
        
    except Exception as e:
        logger.error(f"Ошибка в my_family: {e}")
        bot.answer_callback_query(call.id, "❌ Произошла ошибка")

@bot.message_handler(func=lambda m: m.text and re.match(r'(?i)^(усыновить|удочерить)$', m.text))
def adopt_child_handler(message):
    try:
        user_id = message.from_user.id
        
        if not message.reply_to_message:
            bot.reply_to(message, "❌ Ответьте на сообщение пользователя, которого хотите усыновить/удочерить!")
            return
        
        child_user_id = message.reply_to_message.from_user.id
        child_username = message.reply_to_message.from_user.username
        child_name = message.reply_to_message.from_user.first_name
        
        if user_id == child_user_id:
            bot.reply_to(message, "❌ Нельзя усыновить самого себя!")
            return
        
        conn = sqlite3.connect('/app/data/bot.db')
        c = conn.cursor()
        
        c.execute('''SELECT family_id, husband_id, wife_id, family_level FROM families 
                     WHERE husband_id = ? OR wife_id = ?''', (user_id, user_id))
        family = c.fetchone()
        
        if not family:
            bot.reply_to(message, "❌ У вас нет семьи! Создайте семью сначала.")
            conn.close()
            return
        
        family_id, husband_id, wife_id, family_level = family
        
        if user_id not in [husband_id, wife_id]:
            bot.reply_to(message, "❌ Только родители могут усыновлять детей!")
            conn.close()
            return
        
        max_children = [0, 1, 2, 4, 6, 6]
        current_max = max_children[family_level] if family_level < len(max_children) else 6
        
        c.execute('''SELECT COUNT(*) FROM family_children WHERE family_id = ?''', (family_id,))
        current_children = c.fetchone()[0]
        
        if current_children >= current_max:
            bot.reply_to(message, f"❌ Достигнут лимит детей для вашего уровня семьи ({current_max})!")
            conn.close()
            return
        
        c.execute('''SELECT * FROM family_children WHERE user_id = ?''', (child_user_id,))
        existing_child = c.fetchone()
        
        if existing_child:
            bot.reply_to(message, "❌ Этот пользователь уже является ребенком в другой семье!")
            conn.close()
            return
        
        c.execute('''SELECT * FROM families WHERE husband_id = ? OR wife_id = ?''', (child_user_id, child_user_id))
        existing_parent = c.fetchone()
        
        if existing_parent:
            bot.reply_to(message, "❌ Этот пользователь уже является родителем в другой семье!")
            conn.close()
            return
        
        c.execute('''INSERT INTO family_children (family_id, user_id, added_date) 
                     VALUES (?, ?, ?)''', (family_id, child_user_id, datetime.now().isoformat()))
        conn.commit()
        conn.close()
        
        child_link = create_profile_link(child_user_id, child_username, child_name)
        bot.reply_to(message, f"✅ Вы успешно усыновили {child_link}!", parse_mode='HTML')
        
    except Exception as e:
        logger.error(f"Ошибка при усыновлении: {e}")
        bot.reply_to(message, "❌ Произошла ошибка при усыновлении")

@bot.callback_query_handler(func=lambda call: call.data == "server_marriages")
def server_marriages_handler(call):
    try:
        conn = sqlite3.connect('/app/data/bot.db')
        c = conn.cursor()
        
        c.execute('''SELECT f.family_id, f.husband_id, f.wife_id, f.family_level,
                            u1.username as husband_username, u1.first_name as husband_name,
                            u2.username as wife_username, u2.first_name as wife_name
                     FROM families f
                     LEFT JOIN users u1 ON f.husband_id = u1.user_id
                     LEFT JOIN users u2 ON f.wife_id = u2.user_id
                     ORDER BY f.family_level DESC, f.family_id ASC
                     LIMIT 20''')
        marriages = c.fetchall()
        
        marriages_text = "💍 *БРАКИ СЕРВЕРА (Топ 20):*\n\n"
        
        if marriages:
            for i, (family_id, husband_id, wife_id, family_level, 
                    husband_username, husband_name, wife_username, wife_name) in enumerate(marriages, 1):
                
                husband_link = create_profile_link(husband_id, husband_username, husband_name)
                wife_link = create_profile_link(wife_id, wife_username, wife_name)
                
                marriages_text += f"{i}. {husband_link} 💞 {wife_link}\n"
                marriages_text += f"   🏠 Уровень: {family_level} | ID: {family_id}\n\n"
        else:
            marriages_text += "📭 Пока нет созданных семей\n"
        
        conn.close()
        
        bot.edit_message_text(marriages_text, call.message.chat.id, call.message.message_id, 
                             parse_mode='HTML', reply_markup=family_keyboard())
        
    except Exception as e:
        logger.error(f"Ошибка в server_marriages: {e}")
        bot.answer_callback_query(call.id, "❌ Произошла ошибка")

@bot.callback_query_handler(func=lambda call: call.data.startswith('upgrade_family_'))
def upgrade_family_handler(call):
    try:
        parts = call.data.split('_')
        target_level = int(parts[2])
        cost = int(parts[3])
        user_id = call.from_user.id
        
        conn = sqlite3.connect('/app/data/bot.db')
        c = conn.cursor()
        
        c.execute('''SELECT family_id, family_level FROM families 
                     WHERE husband_id = ? OR wife_id = ?''', (user_id, user_id))
        family = c.fetchone()
        
        if not family:
            bot.answer_callback_query(call.id, "❌ У вас нет семьи!")
            conn.close()
            return
        
        family_id, current_level = family
        
        if current_level >= target_level:
            bot.answer_callback_query(call.id, "❌ У вас уже такой или более высокий уровень!")
            conn.close()
            return
        
        user_data = db.get_user(user_id)
        if user_data[4] < cost:
            bot.answer_callback_query(call.id, f"❌ Недостаточно депусов! Нужно: {cost} д")
            conn.close()
            return
        
        new_depuses = user_data[4] - cost
        db.update_user(user_id, depuses=new_depuses)
        
        c.execute('''UPDATE families SET family_level = ? WHERE family_id = ?''', 
                 (target_level, family_id))
        c.execute('''INSERT INTO family_upgrades (family_id, upgrade_date, level_before, level_after, cost)
                     VALUES (?, ?, ?, ?, ?)''', 
                 (family_id, datetime.now().isoformat(), current_level, target_level, cost))
        conn.commit()
        conn.close()
        
        level_bonuses = {
            1: "✅ Открыта возможность приютить 1 ребенка",
            2: "✅ Можно взять 2 детей",
            3: "✅ Можно взять 4 детей", 
            4: "✅ Можно взять 6 детей + пособия за многодетность",
            5: "🛡️ Защита от плохих РП команд для родителей"
        }
        
        bonus_text = level_bonuses.get(target_level, "")
        
        success_text = f"✅ Семья прокачана до {target_level} уровня!\n📤 Списано: {cost} д\n{bonus_text}"
        
        bot.edit_message_text(success_text, call.message.chat.id, call.message.message_id,
                             parse_mode='Markdown', reply_markup=family_keyboard())
        
    except Exception as e:
        logger.error(f"Ошибка прокачки семьи: {e}")
        bot.answer_callback_query(call.id, "❌ Произошла ошибка")

@bot.callback_query_handler(func=lambda call: call.data == "family_benefits")
def family_benefits_handler(call):
    try:
        user_id = call.from_user.id
        conn = sqlite3.connect('/app/data/bot.db')
        c = conn.cursor()
        
        c.execute('''SELECT f.family_id, f.family_level, f.last_benefit_date,
                            COUNT(fc.child_id) as children_count
                     FROM families f
                     LEFT JOIN family_children fc ON f.family_id = fc.family_id
                     WHERE f.husband_id = ? OR f.wife_id = ?
                     GROUP BY f.family_id''', (user_id, user_id))
        family_data = c.fetchone()
        
        if not family_data:
            bot.answer_callback_query(call.id, "❌ У вас нет семьи!")
            conn.close()
            return
        
        family_id, family_level, last_benefit_date, children_count = family_data
        
        if family_level < 4 or children_count < 3:
            bot.answer_callback_query(call.id, "❌ Для пособий нужен 4+ уровень и 3+ детей!")
            conn.close()
            return
        
        benefits_available = False
        if last_benefit_date:
            last_date = datetime.fromisoformat(last_benefit_date)
            if datetime.now() - last_date >= timedelta(days=1):
                benefits_available = True
        else:
            benefits_available = True
        
        if not benefits_available:
            if last_benefit_date:
                last_date = datetime.fromisoformat(last_benefit_date)
                next_available = last_date + timedelta(days=1)
                time_left = next_available - datetime.now()
                hours = int(time_left.total_seconds() // 3600)
                minutes = int((time_left.total_seconds() % 3600) // 60)
                
                bot.answer_callback_query(call.id, f"⏳ Пособия будут доступны через {hours}ч {minutes}м")
            else:
                bot.answer_callback_query(call.id, "❌ Пособия недоступны")
            conn.close()
            return
        
        benefit_amount = children_count * 50
        
        user_data = db.get_user(user_id)
        new_depuses = user_data[4] + benefit_amount
        db.update_user(user_id, depuses=new_depuses)
        
        c.execute('''UPDATE families SET last_benefit_date = ? WHERE family_id = ?''',
                 (datetime.now().isoformat(), family_id))
        conn.commit()
        conn.close()
        
        success_text = f"💰 *Пособия получены!*\n\n👶 За {children_count} детей: {benefit_amount} д\n🎯 Новый баланс: {new_depuses} д"
        
        bot.edit_message_text(success_text, call.message.chat.id, call.message.message_id,
                             parse_mode='Markdown', reply_markup=family_keyboard())
        
    except Exception as e:
        logger.error(f"Ошибка получения пособий: {e}")
        bot.answer_callback_query(call.id, "❌ Произошла ошибка")

@bot.callback_query_handler(func=lambda call: call.data == "family_help")
def family_help_handler(call):
    help_text = """
👨‍👩‍👧‍👦 *СИСТЕМА СЕМЬИ - ПОМОЩЬ*

💍 *Создание семьи:*
Используйте команду `жениться` или `брак` в ответ на сообщение пользователя

👶 *Усыновление:*
Используйте команду `усыновить` или `удочерить` в ответ на сообщение пользователя

📊 *Уровни семьи:*
1️⃣ Уровень 1 (100 д) - 1 ребенок
2️⃣ Уровень 2 (250 д) - 2 детей  
3️⃣ Уровень 3 (500 д) - 4 детей
4️⃣ Уровень 4 (1000 д) - 6 детей + пособия
5️⃣ Уровень 5 (2000 д) - защита от плохих РП команд

💰 *Пособия:*
• Доступны с 4 уровня и 3+ детей
• 50 депусов за каждого ребенка
• Можно получать 1 раз в 24 часа

💡 Прокачивать семью могут только родители
"""
    
    bot.edit_message_text(help_text, call.message.chat.id, call.message.message_id,
                         parse_mode='Markdown', reply_markup=family_keyboard())

# ========== СИСТЕМА БРАКА ==========
@bot.message_handler(func=lambda m: m.text and m.text.strip().lower() in ['жениться', 'брак', 'marry'])
def propose_marriage_handler(message):
    try:
        if not message.reply_to_message:
            bot.reply_to(message, "❌ Ответьте на сообщение человека, на котором хотите жениться!")
            return
        
        user_id = message.from_user.id
        target_id = message.reply_to_message.from_user.id
        
        if user_id == target_id:
            bot.reply_to(message, "❌ Нельзя жениться на самом себе!")
            return
        
        # Проверяем, не состоит ли уже кто-то в браке
        conn = sqlite3.connect('/app/data/bot.db')
        c = conn.cursor()
        
        c.execute("SELECT * FROM families WHERE husband_id=? OR wife_id=?", (user_id, user_id))
        existing_marriage = c.fetchone()
        if existing_marriage:
            bot.reply_to(message, "❌ Вы уже состоите в браке!")
            conn.close()
            return
        
        c.execute("SELECT * FROM families WHERE husband_id=? OR wife_id=?", (target_id, target_id))
        existing_target_marriage = c.fetchone()
        if existing_target_marriage:
            bot.reply_to(message, "❌ Этот пользователь уже состоит в браке!")
            conn.close()
            return
        
        conn.close()
        
        # Создаем простой уникальный ID для этого предложения
        timestamp = int(time.time())
        proposal_id = f"{user_id}_{target_id}_{timestamp}"
        
        keyboard = InlineKeyboardMarkup()
        keyboard.row(
            InlineKeyboardButton("✅ Согласен(на)", callback_data=f"marry_yes_{proposal_id}"),
            InlineKeyboardButton("❌ Отказаться", callback_data=f"marry_no_{proposal_id}")
        )
        
        user_name = message.from_user.first_name
        target_name = message.reply_to_message.from_user.first_name
        
        bot.reply_to(
            message,
            f"💍 *Предложение брака!*\n\n{user_name} предлагает заключить брак с {target_name}!\n\nСогласны ли вы?",
            parse_mode='Markdown',
            reply_markup=keyboard
        )
        
    except Exception as e:
        logger.error(f"Ошибка в предложении брака: {e}")
        bot.reply_to(message, "❌ Произошла ошибка при создании предложения!")

@bot.callback_query_handler(func=lambda call: call.data.startswith('marry_'))
def handle_marry_callback(call):
    try:
        # Упрощаем парсинг данных
        data_parts = call.data.split('_')
        if len(data_parts) < 3:
            bot.answer_callback_query(call.id, "❌ Ошибка в данных")
            return
            
        action = data_parts[1]
        proposal_id = '_'.join(data_parts[2:])  # Объединяем оставшиеся части
        
        # Парсим proposal_id
        proposal_parts = proposal_id.split('_')
        if len(proposal_parts) < 3:
            bot.answer_callback_query(call.id, "❌ Ошибка в ID предложения")
            return
            
        user_id = int(proposal_parts[0])
        target_id = int(proposal_parts[1])
        
        # Проверяем что тот кто нажал - это целевой пользователь
        if call.from_user.id != target_id:
            bot.answer_callback_query(call.id, "❌ Это предложение не для вас!")
            return
        
        if action == 'yes':
            conn = sqlite3.connect('/app/data/bot.db')
            c = conn.cursor()
            
            # Проверяем, не создался ли уже брак
            c.execute("SELECT * FROM families WHERE husband_id=? OR wife_id=?", (user_id, user_id))
            existing_marriage = c.fetchone()
            if existing_marriage:
                bot.answer_callback_query(call.id, "❌ Вы уже состоите в браке!")
                conn.close()
                return
            
            c.execute("SELECT * FROM families WHERE husband_id=? OR wife_id=?", (target_id, target_id))
            existing_target_marriage = c.fetchone()
            if existing_target_marriage:
                bot.answer_callback_query(call.id, "❌ Этот пользователь уже состоит в браке!")
                conn.close()
                return
            
            # Создаем брак
            c.execute("INSERT INTO families (husband_id, wife_id, family_level, created_date) VALUES (?, ?, ?, ?)",
                     (user_id, target_id, 1, datetime.now().isoformat()))
            conn.commit()
            conn.close()
            
            # Получаем имена
            try:
                user_chat = bot.get_chat(user_id)
                user_name = user_chat.first_name
            except:
                user_name = "Пользователь"
                
            target_name = call.from_user.first_name
            
            success_text = f"💒 *БРАК ЗАКЛЮЧЕН!*\n\n{user_name} 💞 {target_name}\n\nПоздравляем молодоженов! 🎉\n\nТеперь вы можете усыновлять детей и развивать свою семью!"
            
            try:
                bot.edit_message_text(
                    success_text,
                    call.message.chat.id,
                    call.message.message_id,
                    parse_mode='Markdown'
                )
            except:
                # Если не удалось редактировать, просто отвечаем
                bot.send_message(call.message.chat.id, success_text, parse_mode='Markdown')
            
            bot.answer_callback_query(call.id, "✅ Брак заключен!")
            
        else:  # no
            try:
                user_chat = bot.get_chat(user_id)
                user_name = user_chat.first_name
            except:
                user_name = "Пользователь"
                
            decline_text = f"❌ {call.from_user.first_name} отказался(ась) от предложения брака от {user_name}"
            
            try:
                bot.edit_message_text(
                    decline_text,
                    call.message.chat.id,
                    call.message.message_id
                )
            except:
                bot.send_message(call.message.chat.id, decline_text)
                
            bot.answer_callback_query(call.id, "❌ Предложение отклонено")
            
    except Exception as e:
        logger.error(f"Ошибка в обработке брака: {e}")
        logger.error(traceback.format_exc())
        try:
            bot.answer_callback_query(call.id, "❌ Произошла ошибка!")
        except:
            pass

@bot.message_handler(func=lambda m: m.text and m.text.strip().lower() in ['развод', 'divorce'])
def divorce_handler(message):
    try:
        user_id = message.from_user.id
        conn = sqlite3.connect('/app/data/bot.db')
        c = conn.cursor()
        
        # Находим брак
        c.execute("SELECT * FROM families WHERE husband_id=? OR wife_id=?", (user_id, user_id))
        marriage = c.fetchone()
        
        if marriage:
            family_id, husband_id, wife_id, level, created, last_benefit = marriage
            
            # Удаляем всех детей этой семьи
            c.execute("DELETE FROM family_children WHERE family_id=?", (family_id,))
            # Удаляем брак
            c.execute("DELETE FROM families WHERE family_id=?", (family_id,))
            conn.commit()
            
            # Получаем имя супруга для сообщения
            spouse_id = husband_id if user_id == wife_id else wife_id
            try:
                spouse_name = bot.get_chat(spouse_id).first_name
            except:
                spouse_name = "бывший(ая) супруг(а)"
                
            bot.reply_to(message, f"💔 Брак с {spouse_name} расторгнут. Все дети отправлены в детский дом.")
        else:
            bot.reply_to(message, "❌ Вы не в браке")
            
        conn.close()
    except Exception as e:
        logger.error(f"Ошибка при разводе: {e}")
        bot.reply_to(message, f"❌ Ошибка: {e}")

@bot.callback_query_handler(func=lambda call: call.data == "create_marriage")
def create_marriage_handler(call):
    help_text = """
💍 *СОЗДАНИЕ БРАКА*

*Как создать брак:*
1. Найдите пользователя, с которым хотите создать семью
2. Ответьте на его сообщение командой `жениться` или `брак`
3. Ожидайте ответа пользователя

*После создания брака:*
• Вы сможете усыновлять детей
• Прокачивать уровень семьи  
• Получать пособия за многодетность
• Использовать семейные команды

*Условия:*
• Оба пользователя не должны состоять в других браках
• Брак можно расторгнуть командой `развод`
"""
    
    bot.edit_message_text(help_text, call.message.chat.id, call.message.message_id,
                         parse_mode='Markdown', reply_markup=family_keyboard())

# Кастомные РП команды для VIP
@bot.message_handler(func=lambda m: m.text and m.text.startswith('+рп '))
def add_custom_rp_handler(message):
    try:
        user_id = message.from_user.id
        user = db.get_user(user_id)
        
        # Проверяем VIP статус в базе: только VIP может подать заявку на баннер
        vip_until = user[5]
        try:
            if not vip_until:
                bot.reply_to(message, "❌ Эта функция доступна только для VIP пользователей!")
                return
            vip_dt = datetime.fromisoformat(vip_until)
            if vip_dt <= datetime.now():
                bot.reply_to(message, "❌ Ваша подписка VIP истекла — функция доступна только для VIP.")
                return
        except Exception:
            bot.reply_to(message, "❌ Невозможно проверить VIP-статус. Обратитесь к администратору.")
            return
        
        # Исправленный парсинг команды - через пробел
        parts = message.text[3:].strip().split(' ', 1)  # Берем текст после "+рп " и разбиваем на 2 части
        if len(parts) < 2:
            bot.reply_to(message, "❌ Неправильный формат!\nИспользование: `+рп <слово> <текст>`\nПример: `+рп съесть скушал игрока`")
            return
        
        command_name = parts[0].strip()
        command_text = parts[1].strip()
        
        if not command_name or not command_text:
            bot.reply_to(message, "❌ Неправильный формат! Укажите команду и текст.")
            return
        
        # Проверяем, что команда состоит из одного слова
        if ' ' in command_name:
            bot.reply_to(message, "❌ Команда должна быть одним словом!")
            return
        
        conn = sqlite3.connect('/app/data/bot.db')
        c = conn.cursor()
        
        # Проверяем лимит команд (максимум 5)
        c.execute("SELECT COUNT(*) FROM rp_commands WHERE user_id=?", (user_id,))
        command_count = c.fetchone()[0]
        
        if command_count >= 5:
            bot.reply_to(message, "❌ Достигнут лимит в 5 кастомных РП команд!\nУдалите некоторые команды с помощью `-рп <команда>`")
            conn.close()
            return
        
        # Проверяем, нет ли уже такой команды
        c.execute("SELECT * FROM rp_commands WHERE user_id=? AND command_name=?", (user_id, command_name))
        existing_command = c.fetchone()
        
        if existing_command:
            bot.reply_to(message, f"❌ РП команда `{command_name}` уже существует!")
            conn.close()
            return
        
        # Сохраняем команду
        c.execute('''INSERT INTO rp_commands (user_id, command_name, command_text, created_date)
                     VALUES (?, ?, ?, ?)''', 
                 (user_id, command_name, command_text, datetime.now().isoformat()))
        conn.commit()
        conn.close()
        
        bot.reply_to(message, f"✅ РП команда `{command_name}` добавлена!\n💡 Использование: Ответьте `{command_name}` на сообщение пользователя")
        
    except Exception as e:
        logger.error(f"Ошибка добавления РП: {e}")
        bot.reply_to(message, "❌ Произошла ошибка при добавлении РП команды")

@bot.message_handler(func=lambda m: m.text and m.text.startswith('-рп '))
def remove_custom_rp_handler(message):
    try:
        user_id = message.from_user.id
        command_name = message.text[3:].strip()  # Берем текст после "-рп "
        
        if not command_name:
            bot.reply_to(message, "❌ Укажите команду для удаления!\nИспользование: `-рп <команда>`")
            return
        
        conn = sqlite3.connect('/app/data/bot.db')
        c = conn.cursor()
        
        c.execute("DELETE FROM rp_commands WHERE user_id=? AND command_name=?", (user_id, command_name))
        
        if c.rowcount > 0:
            conn.commit()
            bot.reply_to(message, f"✅ РП команда `{command_name}` удалена!")
        else:
            bot.reply_to(message, f"❌ РП команда `{command_name}` не найдена!")
        
        conn.close()
        
    except Exception as e:
        logger.error(f"Ошибка удаления РП: {e}")
        bot.reply_to(message, "❌ Произошла ошибка при удалении РП команды")

@bot.message_handler(func=lambda m: m.text and m.text.strip().lower() == "моирп")
def my_rp_commands_handler(message):
    try:
        user_id = message.from_user.id
        
        conn = sqlite3.connect('/app/data/bot.db')
        c = conn.cursor()
        
        c.execute("SELECT command_name, command_text FROM rp_commands WHERE user_id=? ORDER BY created_date", (user_id,))
        commands = c.fetchall()
        conn.close()
        
        if not commands:
            bot.reply_to(message, "❌ У вас нет кастомных РП команд!\n💡 Добавьте их с помощью `+рп <команда> <текст>`")
            return
        
        commands_text = "🎭 *ВАШИ РП КОМАНДЫ:*\n\n"
        
        for i, (command_name, command_text) in enumerate(commands, 1):
            commands_text += f"{i}. `{command_name}` - \"{command_text}\"\n"
        
        commands_text += f"\n📊 Всего: {len(commands)}/5 команд"
        
        bot.reply_to(message, commands_text, parse_mode='Markdown')
        
    except Exception as e:
        logger.error(f"Ошибка показа РП: {e}")
        bot.reply_to(message, "❌ Произошла ошибка")

# Обработчик кастомных РП команд
@bot.message_handler(func=lambda m: m.text and m.reply_to_message and not re.search(r'(?i)^\s*(?:\+баннер|\+постер)\b', m.text))
def custom_rp_handler(message):
    try:
        command = message.text.strip().lower()
        
        # Пропускаем стандартные РП команды
        if command in RP_COMMANDS:
            return
        
        user_id = message.from_user.id
        
        conn = sqlite3.connect('/app/data/bot.db')
        c = conn.cursor()
        
        # Ищем кастомную РП команду
        c.execute("SELECT command_text FROM rp_commands WHERE user_id=? AND command_name=?", (user_id, command))
        custom_command = c.fetchone()
        conn.close()
        
        if not custom_command:
            return
        
        target_user = message.reply_to_message.from_user
        actor_user = message.from_user
        
        actor_db = db.get_user(actor_user.id)
        if not actor_db:
            db.create_user(actor_user.id, actor_user.username, actor_user.first_name)
            actor_db = db.get_user(actor_user.id)
        
        actor_prefix = actor_db[6] if actor_db[6] else "Игрок"
        actor_brackets = actor_db[7] if actor_db[7] else "[]"
        
        if actor_brackets and len(actor_brackets) >= 2:
            left_bracket = actor_brackets[0]
            right_bracket = actor_brackets[-1]
            actor_name = f"{left_bracket}{actor_prefix}{right_bracket} {actor_user.first_name}"
        else:
            actor_name = f"{actor_brackets}{actor_prefix}{actor_brackets} {actor_user.first_name}"
        
        target_db = db.get_user(target_user.id)
        if not target_db:
            db.create_user(target_user.id, target_user.username, target_user.first_name)
            target_db = db.get_user(target_user.id)
        
        target_prefix = target_db[6] if target_db[6] else "Игрок"
        target_brackets = target_db[7] if target_db[7] else "[]"
        
        if target_brackets and len(target_brackets) >= 2:
            left_bracket = target_brackets[0]
            right_bracket = target_brackets[-1]
            target_name = f"{left_bracket}{target_prefix}{right_bracket} {target_user.first_name}"
        else:
            target_name = f"{target_brackets}{target_prefix}{target_brackets} {target_user.first_name}"
        
        action_text = custom_command[0].replace('игрока', target_name).replace('игрок', target_name)
        
        rp_text = f"💫 {actor_name} {action_text}"
        
        bot.reply_to(message, rp_text)
        
    except Exception as e:
        logger.error(f"Ошибка в кастомной РП: {e}")

# ========== ИСПРАВЛЕНИЯ ОШИБОК ==========

# ИСПРАВЛЕННАЯ КОМАНДА TOPID
@bot.message_handler(func=lambda m: m.text and m.text.strip().lower() == "~topid" and m.chat.type == 'private' and m.from_user.id == ADMIN_ID)
def topid_handler(message):
    try:
        conn = sqlite3.connect('/app/data/bot.db')
        c = conn.cursor()
        
        c.execute('''SELECT u.user_id, u.username, u.first_name, u.balance 
                     FROM users u 
                     ORDER BY u.balance DESC 
                     LIMIT 20''')
        top_users = c.fetchall()
        conn.close()
        
        top_text = "🏆 *ТОП 20 ИГРОКОВ С ССЫЛКАМИ:*\n\n"
        
        for i, (user_id, username, first_name, balance) in enumerate(top_users, 1):
            # Безопасное форматирование - используем HTML теги вместо Markdown
            safe_name = html.escape(first_name)
            if username:
                user_link = f'<a href="https://t.me/{username}">{safe_name}</a>'
            else:
                user_link = f'<a href="tg://user?id={user_id}">{safe_name}</a>'
            top_text += f"{i}. {user_link} - <code>{balance:,}</code> т\n"
        
        # Используем HTML разметку вместо Markdown для избежания ошибок парсинга
        bot.reply_to(message, top_text, parse_mode='HTML')
        
    except Exception as e:
        logger.error(f"Ошибка в topid: {e}")
        bot.reply_to(message, f"❌ Ошибка: {str(e)}")

# ИСПРАВЛЕННЫЙ ОБРАБОТЧИК РП КОМАНД С ПРАВИЛЬНЫМ ФОРМАТИРОВАНИЕМ ИМЕН
def format_rp_name(user_id, username, first_name):
    """Форматирует имя для РП команд с префиксом и скобками"""
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

# ПЕРЕПИСАННЫЙ ОБРАБОТЧИК РП КОМАНД
@bot.message_handler(func=lambda m: m.text and m.reply_to_message and not any(m.text.strip().lower().startswith(cmd) for cmd in ['+постер','+баннер'])) 
def improved_rp_handler(message):
    try:
        command = message.text.strip().lower()
        
        # Проверяем стандартные РП команды
        if command in RP_COMMANDS:
            target_user = message.reply_to_message.from_user
            actor_user = message.from_user
            
            # Форматируем имена с префиксами и скобками
            actor_name = format_rp_name(actor_user.id, actor_user.username, actor_user.first_name)
            target_name = format_rp_name(target_user.id, target_user.username, target_user.first_name)
            
            action = RP_COMMANDS[command]
            rp_text = f"💫 {actor_name} {action} {target_name}"
            bot.reply_to(message, rp_text)
            return
        
        # Проверяем кастомные РП команды
        user_id = message.from_user.id
        conn = sqlite3.connect('/app/data/bot.db')
        c = conn.cursor()
        c.execute("SELECT command_text FROM rp_commands WHERE user_id=? AND command_name=?", (user_id, command))
        custom_command = c.fetchone()
        conn.close()
        
        if custom_command:
            target_user = message.reply_to_message.from_user
            actor_user = message.from_user
            
            # Форматируем имена с префиксами и скобками
            actor_name = format_rp_name(actor_user.id, actor_user.username, actor_user.first_name)
            target_name = format_rp_name(target_user.id, target_user.username, target_user.first_name)
            
            action_text = custom_command[0].replace('игрока', target_name).replace('игрок', target_name)
            rp_text = f"💫 {actor_name} {action_text}"
            bot.reply_to(message, rp_text)
            
    except Exception as e:
        logger.error(f"Ошибка в улучшенном РП обработчике: {e}")

# ИСПРАВЛЕННАЯ СИСТЕМА БАННЕРОВ
@bot.message_handler(func=lambda m: m.text and re.search(r'(?i)^\s*(?:\+баннер|\+постер)\b', m.text))
def improved_banner_handler(message):
    """
    Надёжный обработчик баннеров:
    - Сначала проверяет VIP и reply_to
    - Немедленно отвечает пользователю (ACK), чтобы не молчать
    - Сохраняет заявку и пытается уведомить админа (copy -> forward -> direct)
    - Логирует каждый шаг
    """
    try:
        logger.info("BANNER_HANDLER ENTER: user=%s chat=%s text=%r reply=%s",
                    message.from_user.id, getattr(message.chat, 'id', None),
                    message.text, bool(getattr(message, 'reply_to_message', None)))

        user_id = message.from_user.id
        user = db.get_user(user_id)
        if not user:
            bot.reply_to(message, "❌ Пользователь не найден в базе!")
            logger.warning("BANNER: user not in DB: %s", user_id)
            return

        # VIP проверка
        try:
            vip_until = user[5] if len(user) > 5 else None
            if not vip_until or datetime.fromisoformat(vip_until) <= datetime.now():
                bot.reply_to(message, "❌ Эта функция доступна только для VIP пользователей!")
                logger.info("BANNER: user %s is not VIP (vip_until=%s)", user_id, vip_until)
                return
        except Exception as e:
            bot.reply_to(message, "❌ Невозможно проверить VIP-статус. Обратитесь к администратору.")
            logger.exception("BANNER: error parsing vip_until for user %s: %s", user_id, e)
            return

        if not getattr(message, 'reply_to_message', None):
            bot.reply_to(message, "❌ Ответьте этой командой на фото/видео/голосовое/аудио/анимацию, чтобы отправить баннер.")
            logger.info("BANNER: no reply_to_message from user %s", user_id)
            return

        original_msg = message.reply_to_message

        # Определяем тип медиа
        file_id = None
        file_type = None
        file_size = 0
        if original_msg.photo:
            file_id = original_msg.photo[-1].file_id
            file_type = 'photo'
            file_size = original_msg.photo[-1].file_size or 0
        elif original_msg.video:
            file_id = original_msg.video.file_id
            file_type = 'video'
            file_size = original_msg.video.file_size or 0
        elif original_msg.voice:
            file_id = original_msg.voice.file_id
            file_type = 'voice'
            file_size = original_msg.voice.file_size or 0
        elif original_msg.audio:
            file_id = original_msg.audio.file_id
            file_type = 'audio'
            file_size = original_msg.audio.file_size or 0
        elif getattr(original_msg, 'animation', None):
            file_id = original_msg.animation.file_id
            file_type = 'animation'
            file_size = original_msg.animation.file_size or 0
        else:
            bot.reply_to(message, "❌ Неподдерживаемый тип файла. Только фото/видео/голосовое/аудио/анимация.")
            logger.info("BANNER: unsupported media from user %s", user_id)
            return

        # Ограничение размера
        MAX_FILE_SIZE = 20 * 1024 * 1024
        if file_size and file_size > MAX_FILE_SIZE:
            bot.reply_to(message, "❌ Файл слишком большой! Максимум 20 MB.")
            logger.info("BANNER: file too large %s bytes from user %s", file_size, user_id)
            return

        # НЕМЕДЛЕННЫЙ ACK пользователю (выполняется всегда, чтобы не молчать)
        try:
            bot.reply_to(message, "✅ Ваша заявка принята и отправлена на модерацию. Администратор получит файл и сможет принять или отклонить его.")
        except Exception as e:
            logger.warning("BANNER: failed to send immediate ack to user %s: %s", user_id, e)

        # Сохраняем заявку в БД (если упадёт, пользователь уже получил ACK)
        request_id = None
        try:
            conn = sqlite3.connect('/app/data/bot.db')
            c = conn.cursor()
            c.execute('''INSERT INTO banner_requests (user_id, file_id, file_type, file_size, status)
                         VALUES (?, ?, ?, ?, ?)''',
                      (user_id, file_id, file_type, file_size or 0, 'pending'))
            conn.commit()
            request_id = c.lastrowid
            conn.close()
            logger.info("BANNER: saved request_id=%s user=%s type=%s size=%s", request_id, user_id, file_type, file_size)
        except Exception as e:
            logger.exception("BANNER: DB error saving banner request for user %s: %s", user_id, e)
            # уведомляем юзера, что заявка сохранена не была
            try:
                bot.send_message(message.chat.id, "⚠️ Заявка не сохранилась из-за ошибки. Администратор уведомлён вручную.")
            except Exception:
                pass
            return

        # Подготовка сообщения админу
        # Подготовка текста и кнопок дл�� админа (с экранированием Markdown)
        safe_first = safe_md(message.from_user.first_name or "")
        safe_username = safe_md(message.from_user.username or "нет")
        admin_text = (f"🖼️ *НОВАЯ ЗАЯВКА НА БАННЕР #{request_id}*\n\n"
                      f"👤 Пользователь: {safe_first}\n"
                      f"📛 Username: @{safe_username}\n"
                      f"🆔 ID: {user_id}\n"
                      f"📁 Тип: {safe_md(file_type)}\n"
                      f"📊 Размер: {file_size // 1024 if file_size else 0} KB\n\n"
                      "✅ Принять или ❌ Отклонить?")
        admin_keyboard = InlineKeyboardMarkup()
        admin_keyboard.row(
            InlineKeyboardButton("✅ Принять", callback_data=f"banner_accept_{request_id}"),
            InlineKeyboardButton("❌ Отклонить", callback_data=f"banner_reject_{request_id}")
        )

        # Попытки отправки админу: media first, then text with buttons
        # ... (media send code unchanged) ...

        # Отправляем текст с кнопками админу (в любом случае)
        try:
            bot.send_message(ADMIN_ID, admin_text, reply_markup=admin_keyboard, parse_mode='Markdown')
        except Exception as e:
            logger.error("BANNER: Failed to send admin_text for request %s: %s", request_id, e)
            send_errors.append(f"text:{e}")
            
        # Попытки отправки админу: media first, then text
        send_errors = []
        sent_media = False

        try:
            bot.copy_message(ADMIN_ID, original_msg.chat.id, original_msg.message_id)
            sent_media = True
            logger.info("BANNER: request %s media sent via copy_message", request_id)
        except Exception as e:
            logger.warning("BANNER: copy_message failed for request %s: %s", request_id, e)
            send_errors.append(f"copy:{e}")

        if not sent_media:
            try:
                bot.forward_message(ADMIN_ID, original_msg.chat.id, original_msg.message_id)
                sent_media = True
                logger.info("BANNER: request %s media sent via forward_message", request_id)
            except Exception as e:
                logger.warning("BANNER: forward_message failed for request %s: %s", request_id, e)
                send_errors.append(f"forward:{e}")

        if not sent_media:
            try:
                if file_type == 'photo':
                    bot.send_photo(ADMIN_ID, file_id)
                elif file_type == 'video':
                    bot.send_video(ADMIN_ID, file_id)
                elif file_type == 'voice':
                    bot.send_voice(ADMIN_ID, file_id)
                elif file_type == 'audio':
                    bot.send_audio(ADMIN_ID, file_id)
                elif file_type == 'animation':
                    bot.send_animation(ADMIN_ID, file_id)
                else:
                    raise Exception("unsupported file_type")
                sent_media = True
                logger.info("BANNER: request %s media sent via direct send (%s)", request_id, file_type)
            except Exception as e:
                logger.error("BANNER: direct send failed for request %s: %s", request_id, e)
                send_errors.append(f"direct:{e}")

        # Отправляем текст с кнопками админу (в любом случае)
        try:
            bot.send_message(ADMIN_ID, admin_text, reply_markup=admin_keyboard, parse_mode='Markdown')
        except Exception as e:
            logger.error("BANNER: Failed to send admin_text for request %s: %s", request_id, e)
            send_errors.append(f"text:{e}")

        if not sent_media and send_errors:
            logger.error("BANNER: request %s NOT delivered to admin. errors=%s", request_id, send_errors)
            # Обновим статус заявки в БД на 'error' (не критично, но полезно)
            try:
                conn = sqlite3.connect('/app/data/bot.db')
                c = conn.cursor()
                c.execute("UPDATE banner_requests SET status=?, admin_id=?, decision_date=? WHERE request_id=?",
                          ('error', None, datetime.now().isoformat(), request_id))
                conn.commit()
                conn.close()
            except Exception:
                logger.exception("BANNER: failed to update request status to error for %s", request_id)
            # пользователь уже получил ACK, дублируем предупреждение в чате
            try:
                bot.send_message(message.chat.id, "⚠️ Заявка сохранена, но не удалось доставить её администратору. Проверьте логи.")
            except Exception:
                pass

    except Exception as e:
        logger.exception("BANNER: unexpected error: %s", e)
        try:
            bot.reply_to(message, "❌ Произошла внутренняя ошибка при обработке заявки. Попробуйте позже.")
        except Exception:
            pass

# ОБРАБОТЧИКИ МОДЕРАЦИИ БАННЕРОВ
@bot.callback_query_handler(func=lambda call: call.data.startswith('banner_accept_'))
def accept_banner_handler(call):
    try:
        request_id = int(call.data.split('_')[2])
        conn = sqlite3.connect('/app/data/bot.db')
        c = conn.cursor()

        c.execute('''SELECT user_id, file_id, file_type, status FROM banner_requests 
                     WHERE request_id=?''', (request_id,))
        request_data = c.fetchone()

        if not request_data:
            bot.answer_callback_query(call.id, "❌ Заявка не найдена")
            conn.close()
            return

        user_id, file_id, file_type, status = request_data

        if status != 'pending' and status != 'error':
            bot.answer_callback_query(call.id, "❌ Эта заявка уже обработана")
            conn.close()
            return

        # Обновляем баннер пользователя
        c.execute('''UPDATE users SET banner_file_id=?, banner_type=? WHERE user_id=?''',
                  (file_id, file_type, user_id))

        # Обновляем статус заявки
        c.execute('''UPDATE banner_requests SET status='accepted', admin_id=?, decision_date=?
                     WHERE request_id=?''',
                  (call.from_user.id, datetime.now().isoformat(), request_id))
        conn.commit()
        conn.close()

        # Попытка уведомить пользователя и отправить ему баннер
        try:
            caption = "✅ Ваш баннер одобрен! Он будет отображаться в профиле (если VIP активен)."
            if file_type == 'photo':
                bot.send_photo(user_id, file_id, caption=caption)
            elif file_type == 'video':
                bot.send_video(user_id, file_id, caption=caption)
            elif file_type == 'voice':
                bot.send_voice(user_id, file_id, caption=caption)
            elif file_type == 'audio':
                bot.send_audio(user_id, file_id, caption=caption)
            elif file_type == 'animation':
                bot.send_animation(user_id, file_id, caption=caption)
            else:
                bot.send_message(user_id, "✅ Ваш баннер одобрен!")
        except Exception as e:
            logger.warning(f"Не удалось отправить уведомление пользователю {user_id}: {e}")

        try:
            bot.edit_message_text(f"✅ Баннер #{request_id} одобрен!", call.message.chat.id, call.message.message_id)
        except Exception:
            try:
                bot.send_message(call.message.chat.id, f"✅ Баннер #{request_id} одобрен!")
            except Exception:
                pass

        bot.answer_callback_query(call.id, "✅ Баннер принят!")
    except Exception as e:
        logger.error(f"Ошибка принятия баннера: {e}")
        logger.error(traceback.format_exc())
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка при принятии")
        except Exception:
            pass

@bot.callback_query_handler(func=lambda call: call.data.startswith('banner_reject_'))
def reject_banner_handler(call):
    try:
        request_id = int(call.data.split('_')[2])
        conn = sqlite3.connect('/app/data/bot.db')
        c = conn.cursor()

        c.execute('''SELECT user_id, status FROM banner_requests 
                     WHERE request_id=?''', (request_id,))
        request_data = c.fetchone()

        if not request_data:
            bot.answer_callback_query(call.id, "❌ Заявка не найдена")
            conn.close()
            return

        user_id, status = request_data

        if status != 'pending' and status != 'error':
            bot.answer_callback_query(call.id, "❌ Эта заявка уже обработана")
            conn.close()
            return

        # Обновляем статус заявки
        c.execute('''UPDATE banner_requests SET status='rejected', admin_id=?, decision_date=?
                     WHERE request_id=?''',
                  (call.from_user.id, datetime.now().isoformat(), request_id))
        conn.commit()
        conn.close()

        # Уведомляем пользователя
        try:
            bot.send_message(user_id, "❌ Ваш баннер отклонён администратором.")
        except Exception as e:
            logger.warning(f"Не удалось уведомить пользователя {user_id} об отклонении: {e}")

        # Обновим сообщение у админа
        try:
            bot.edit_message_text(f"❌ Баннер #{request_id} отклонён!", call.message.chat.id, call.message.message_id)
        except Exception:
            try:
                bot.send_message(call.message.chat.id, f"❌ Баннер #{request_id} отклонён!")
            except Exception:
                pass

        bot.answer_callback_query(call.id, "❌ Баннер отклонён!")
    except Exception as e:
        logger.error(f"Ошибка отклонения баннера: {e}")
        logger.error(traceback.format_exc())
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка при отклонении")
        except Exception:
            pass

# ========== ОБЩИЙ ХЭНДЛЕР ДЛЯ СТАТИСТИКИ ==========
@bot.message_handler(content_types=['text'])
def all_messages_handler(message):
    """Обрабатывает ВСЕ текстовые сообщения для статистики"""
    try:
        user_id = message.from_user.id
        username = message.from_user.username
        first_name = message.from_user.first_name
        db.create_user(user_id, username, first_name)
        
        user = db.get_user(user_id)
        if user:
            db.update_user(user_id, messages_count=user[10] + 1)
            conn = sqlite3.connect('/app/data/bot.db')
            c = conn.cursor()
            
            chat_id = message.chat.id if hasattr(message, 'chat') else None
            try:
                chat_type = getattr(message.chat, 'type', 'private')
                if chat_id and chat_type in ('group', 'supergroup', 'channel'):
                    c.execute("INSERT OR REPLACE INTO groups (group_id, title, added_date) VALUES (?, ?, ?)",
                              (chat_id, getattr(message.chat, 'title', '') or '', datetime.now().isoformat()))
                    logger.info(f"Saved/updated group in DB: id={chat_id} type={chat_type} title={getattr(message.chat, 'title', '')}")
            except Exception as e:
                logger.warning(f"Не удалось сохранить/обновить группу: {e}")

            today = datetime.now().strftime('%Y-%m-%d')
            c.execute("INSERT OR IGNORE INTO daily_stats_group (date, chat_id, user_id) VALUES (?, ?, ?)", (today, chat_id, user_id))
            c.execute("UPDATE daily_stats_group SET messages = messages + 1 WHERE date = ? AND chat_id = ? AND user_id = ?", (today, chat_id, user_id))
            current_month = datetime.now().strftime('%Y-%m')
            c.execute("INSERT OR IGNORE INTO monthly_stats_group (month, chat_id, user_id) VALUES (?, ?, ?)", (current_month, chat_id, user_id))
            c.execute("UPDATE monthly_stats_group SET messages = messages + 1 WHERE month = ? AND chat_id = ? AND user_id = ?", (current_month, chat_id, user_id))
            c.execute("INSERT OR IGNORE INTO all_stats_group (chat_id, user_id) VALUES (?, ?)", (chat_id, user_id))
            c.execute("UPDATE all_stats_group SET messages = messages + 1 WHERE chat_id = ? AND user_id = ?", (chat_id, user_id))
            if (message.text and len(message.text.split()) > 0 and 
                not message.text.startswith('/') and 
                not message.text.startswith('+') and
                not message.text.startswith('!') and
                not re.match(r'(?i)^(баланс|б|казино|т обмен|д обмен|профиль|топ|магазин|квартир|стата|рекорды|команды|бонус|подарок)', message.text)):
                
                words = message.text.lower().split()
                for word in words:
                    if len(word) > 2:
                        c.execute("INSERT OR IGNORE INTO words (word) VALUES (?)", (word,))
                        c.execute("UPDATE words SET count = count + 1 WHERE word = ?", (word,))
            
            conn.commit()
            conn.close()
            
    except Exception as e:
        logger.error(f"Error in all_messages_handler: {e}")

@bot.message_handler(content_types=['new_chat_members'])
def handle_new_members(message):
    try:
        for new_member in message.new_chat_members:
            if new_member.id == bot.get_me().id:
                conn = sqlite3.connect('/app/data/bot.db')
                c = conn.cursor()
                try:
                    chat_title = message.chat.title
                    chat_id = message.chat.id
                    
                    c.execute("INSERT OR REPLACE INTO groups (group_id, title, added_date) VALUES (?, ?, ?)",
                             (chat_id, chat_title, datetime.now().isoformat()))
                    conn.commit()
                    
                    logger.info(f"✅ Бот добавлен в группу: {chat_title} (ID: {chat_id})")
                except Exception as e:
                    logger.error(f"❌ Ошибка сохранения группы: {e}")
                finally:
                    conn.close()
    except Exception as e:
        logger.error(f"❌ Ошибка при обработке new_chat_members: {e}")

@bot.message_handler(content_types=['left_chat_member'])
def handle_left_member(message):
    try:
        if message.left_chat_member.id == bot.get_me().id:
            conn = sqlite3.connect('/app/data/bot.db')
            c = conn.cursor()
            c.execute("DELETE FROM groups WHERE group_id=?", (message.chat.id,))
            conn.commit()
            conn.close()
            logger.info(f"❌ Бот удален из группы: {message.chat.title} (ID: {message.chat.id})")
    except Exception as e:
        logger.error(f"❌ Ошибка при обработке left_chat_member: {e}")

# ========== CALLBACK HANDLERS ==========
@bot.callback_query_handler(func=lambda call: True)
def callback_handler(call):
    try:
        if call.data == "shop":
            bot.edit_message_text("🏪 *Магазин:* Выберите категорию", call.message.chat.id, call.message.message_id, parse_mode='Markdown', reply_markup=shop_keyboard())
        elif call.data == "shop_design":
            bot.edit_message_text("🎨 *Дизайн:* Выберите тип", call.message.chat.id, call.message.message_id, parse_mode='Markdown', reply_markup=design_keyboard())
        elif call.data == "design_prefix":
            bot.edit_message_text("🏷️ *Префиксы:* Выберите префикс", call.message.chat.id, call.message.message_id, parse_mode='Markdown', reply_markup=prefix_keyboard())
        elif call.data == "design_brackets":
            bot.edit_message_text("🔣 *Скобки:* Выберите скобки", call.message.chat.id, call.message.message_id, parse_mode='Markdown', reply_markup=brackets_keyboard())
        elif call.data == "stats":
            bot.edit_message_text("📊 *Статистика:* Выберите период", call.message.chat.id, call.message.message_id, parse_mode='Markdown', reply_markup=stats_keyboard())
        elif call.data == "apartments":
            bot.edit_message_text("🏠 *Квартиры:* Управление недвижимостью", call.message.chat.id, call.message.message_id, parse_mode='Markdown', reply_markup=apartment_keyboard())
        elif call.data == "shop_vip":
            bot.edit_message_text("👑 *VIP карта:* Выберите срок", call.message.chat.id, call.message.message_id, parse_mode='Markdown', reply_markup=vip_keyboard())
        elif call.data == "back_main":
            bot.edit_message_text("🎮 *Главное меню:*", call.message.chat.id, call.message.message_id, parse_mode='Markdown', reply_markup=main_menu_keyboard())
        elif call.data == "back_shop":
            bot.edit_message_text("🏪 *Магазин:* Выберите категорию", call.message.chat.id, call.message.message_id, parse_mode='Markdown', reply_markup=shop_keyboard())
        elif call.data == "back_design":
            bot.edit_message_text("🎨 *Дизайн:* Выберите тип", call.message.chat.id, call.message.message_id, parse_mode='Markdown', reply_markup=design_keyboard())
        elif call.data == "vip_info":
            info_text = "👑 *Как купить VIP:*\n\n"
            info_text += "💎 *Команда:* `Купить вип [количество дней]`\n\n"
            info_text += "📊 *Расценки:*\n"
            info_text += "• 1-7 дней: 50 депусов/день\n"
            info_text += "• 8-31 день: 45 депусов/день\n" 
            info_text += "• 32+ дней: 40 депусов/день\n\n"
            info_text += "🎁 *Бонусы VIP:*\n"
            info_text += "• +50% к обмену депусов\n"
            info_text += "• Отсутствие комиссии\n"
            info_text += "• Специальный статус"
            
            bot.edit_message_text(info_text, call.message.chat.id, call.message.message_id, parse_mode='Markdown', reply_markup=vip_keyboard())
        
        elif call.data == "shop_apartment":
            text = "🏠 *Квартира*\n\n"
            text += "💰 Цена: 25,000,000 т\n"
            text += "📈 Доход: 500,000 т/час (без ремонта)\n"
            text += "🔨 Ремонт: +10,000,000 т → 1,000,000 т/час\n"
            text += "📊 Продажа: 20,000,000 т (без) / 30,000,000 т (с ремонтом)\n"
            text += "🚫 Макс: 10 квартир\n\n"
            text += "Купить?"

            keyboard = InlineKeyboardMarkup()
            keyboard.row(InlineKeyboardButton("🏠 Купить", callback_data="buy_apartment"))
            keyboard.row(InlineKeyboardButton("🔙 Назад", callback_data="back_shop"))

            bot.edit_message_text(text, call.message.chat.id, call.message.message_id, parse_mode='Markdown', reply_markup=keyboard)

        # Добавляем обработчики для новых функций
        elif call.data == "family":
            bot.edit_message_text("👨‍👩‍👧‍👦 *Семейная система:*", call.message.chat.id, call.message.message_id, parse_mode='Markdown', reply_markup=family_keyboard())
        elif call.data == "rp_commands_main":
            bot.edit_message_text("🎭 *Кастомные РП команды:*", call.message.chat.id, call.message.message_id, parse_mode='Markdown', reply_markup=rp_commands_keyboard())
        elif call.data == "family_level":
            user_id = call.from_user.id
            conn = sqlite3.connect('/app/data/bot.db')
            c = conn.cursor()
            c.execute('''SELECT family_id, family_level FROM families 
                         WHERE husband_id = ? OR wife_id = ?''', (user_id, user_id))
            family = c.fetchone()
            conn.close()
            
            if not family:
                bot.edit_message_text("❌ У вас нет семьи!", call.message.chat.id, call.message.message_id)
                return
            
            family_id, current_level = family
            level_text = f"📊 *Уровень вашей семьи: {current_level}*\n\n"
            
            level_info = {
                1: "👶 Можно усыновить 1 ребенка",
                2: "👶👶 Можно усыновить 2 детей", 
                3: "👶👶👶👶 Можно усыновить 4 детей",
                4: "👶👶👶👶👶👶 Можно усыновить 6 детей + пособия",
                5: "🛡️ Защита от плохих РП команд для родителей"
            }
            
            level_text += level_info.get(current_level, "🔒 Нет особых возможностей")
            
            if current_level < 5:
                upgrade_costs = [100, 250, 500, 1000, 2000]
                next_level = current_level + 1
                cost = upgrade_costs[current_level-1] if current_level-1 < len(upgrade_costs) else 0
                level_text += f"\n\n🔼 Следующий уровень {next_level}: {cost} депусов"
            
            bot.edit_message_text(level_text, call.message.chat.id, call.message.message_id,
                                 parse_mode='Markdown', reply_markup=family_level_keyboard(family_id, current_level, user_id))
        
        elif call.data == "add_rp":
            help_text = """
➕ *ДОБАВЛЕНИЕ РП КОМАНДЫ*

*Формат:* `+рп <команда> <текст>`

*Пример:* 
`+рп съесть скушал игрока`

*Переменные:*
• `игрок` - заменится на имя цели
• `игрока` - заменится на имя цели в родительном падеже

*Ограничения:*
• Только для VIP пользователей
• Максимум 5 команд на пользователя
• Команда должна быть одним словом
"""
            bot.edit_message_text(help_text, call.message.chat.id, call.message.message_id,
                                 parse_mode='Markdown', reply_markup=rp_commands_keyboard())
        
        elif call.data == "my_rp_commands":
            user_id = call.from_user.id
            conn = sqlite3.connect('/app/data/bot.db')
            c = conn.cursor()
            c.execute("SELECT command_name, command_text FROM rp_commands WHERE user_id=?", (user_id,))
            commands = c.fetchall()
            conn.close()
            
            if not commands:
                commands_text = "❌ У вас нет кастомных РП команд!"
            else:
                commands_text = "🎭 *ВАШИ РП КОМАНДЫ:*\n\n"
                for i, (name, text) in enumerate(commands, 1):
                    commands_text += f"{i}. `{name}` - \"{text}\"\n"
                commands_text += f"\n📊 Всего: {len(commands)}/5"
            
            bot.edit_message_text(commands_text, call.message.chat.id, call.message.message_id,
                                 parse_mode='Markdown', reply_markup=rp_commands_keyboard())
        
        elif call.data == "rp_help":
            help_text = """
🎭 *КАСТОМНЫЕ РП КОМАНДЫ*

*Для VIP пользователей:*
• `+рп <команда> <текст>` - добавить команду
• `-рп <команда>` - удалить команду  
• `моирп` - список ваших команд

*Пример использования:*
1. Добавляете: `+рп съесть скушал игрока`
2. Используете: Ответьте `съесть` на сообщение
3. Результат: "[Игрок] скушал [Цель]"

*Лимит:* 5 команд на пользователя
"""
            bot.edit_message_text(help_text, call.message.chat.id, call.message.message_id,
                                 parse_mode='Markdown', reply_markup=rp_commands_keyboard())
        
        # Обработчики для системы брака
        elif call.data == "create_marriage":
            help_text = """
💍 *СОЗДАНИЕ БРАКА*

*Как создать брак:*
1. Найдите пользователя, с которым хотите создать семью
2. Ответьте на его сообщение командой `жениться` или `брак`
3. Ожидайте ответа пользователя

*После создания брака:*
• Вы сможете усыновлять детей
• Прокачивать уровень семьи  
• Получать пособия за многодетность
• Использовать семейные команды

*Условия:*
• Оба пользователя не должны состоять в других браках
• Брак можно расторгнуть командой `развод`
"""
            bot.edit_message_text(help_text, call.message.chat.id, call.message.message_id,
                                 parse_mode='Markdown', reply_markup=family_keyboard())
        
        # Остальные существующие обработчики callback...
        elif call.data.startswith('brackets_'):
            bracket_type = int(call.data.split('_')[1])
            brackets = ['()', '<>', '{}', '《》', '꧁꧂']
            prices = [100, 200, 300, 500, 750]
            
            user_id = call.from_user.id
            user = db.get_user(user_id)
            
            if user[4] < prices[bracket_type-1]:
                bot.answer_callback_query(call.id, f"❌ Недостаточно депусов. Нужно: {prices[bracket_type-1]} д")
                return
            
            new_depuses = user[4] - prices[bracket_type-1]
            db.update_user(user_id, depuses=new_depuses, brackets=brackets[bracket_type-1])
            
            bot.answer_callback_query(call.id, f"✅ Куплены скобки: {brackets[bracket_type-1]}")
            bot.edit_message_text(f"✅ *Скобки куплены!*\n\nНовые скобки: {brackets[bracket_type-1]}\nБаланс: {new_depuses} д", call.message.chat.id, call.message.message_id, parse_mode='Markdown')
        
        elif call.data.startswith('prefix_'):
            if call.data == "prefix_custom":
                msg = bot.send_message(call.message.chat.id, "✏️ Введите ваш префикс (до 10 символов):")
                bot.register_next_step_handler(msg, process_custom_prefix)
            else:
                prefix_type = int(call.data.split('_')[1])
                prefixes = ['Депер', 'Лудоман', 'Элита', 'Богачь', 'Миллиардер']
                prices = [100, 250, 300, 400, 500]
                
                user_id = call.from_user.id
                user = db.get_user(user_id)
                
                if user[4] < prices[prefix_type-1]:
                    bot.answer_callback_query(call.id, f"❌ Недостаточно депусов. Нужно: {prices[prefix_type-1]} д")
                    return
                
                new_depuses = user[4] - prices[prefix_type-1]
                db.update_user(user_id, depuses=new_depuses, prefix=prefixes[prefix_type-1])
                
                bot.answer_callback_query(call.id, f"✅ Куплен префикс: {prefixes[prefix_type-1]}")
                bot.edit_message_text(f"✅ *Префикс куплен!*\n\nНовый префикс: {prefixes[prefix_type-1]}\nБаланс: {new_depuses} д", call.message.chat.id, call.message.message_id, parse_mode='Markdown')
        
        elif call.data.startswith('vip_'):
            vip_type = call.data.split('_')[1]
            days = 7 if vip_type == '7' else 31 if vip_type == '31' else 365
            price_per_day = 50 if vip_type == '7' else 45 if vip_type == '31' else 40
            total_price = days * price_per_day
            
            user_id = call.from_user.id
            user = db.get_user(user_id)
            
            if user[4] < total_price:
                bot.answer_callback_query(call.id, f"❌ Недостаточно депусов. Нужно: {total_price} д")
                return
            
            if user[5] and datetime.fromisoformat(user[5]) > datetime.now():
                current_end = datetime.fromisoformat(user[5])
                new_end = current_end + timedelta(days=days)
            else:
                new_end = datetime.now() + timedelta(days=days)
            
            new_depuses = user[4] - total_price
            db.update_user(user_id, depuses=new_depuses, vip_until=new_end.isoformat())

            bot.answer_callback_query(call.id, f"✅ VIP куплен на {days} дней!")
            bot.edit_message_text(f"✅ *VIP куплен!*\n\nСрок: {days} дней\nСписано: {total_price} д\nVIP до: {new_end.strftime('%d.%m.%Y %H:%M')}\nБаланс: {new_depuses} д", call.message.chat.id, call.message.message_id, parse_mode='Markdown')
        
        elif call.data == "buy_apartment":
            user_id = call.from_user.id
            user = db.get_user(user_id)
            
            if user[3] < 25000000:
                bot.answer_callback_query(call.id, "❌ Недостаточно тенге. Нужно: 25,000,000 т")
                return

            conn = sqlite3.connect('/app/data/bot.db')
            c = conn.cursor()
            c.execute("SELECT COUNT(*) FROM apartments WHERE user_id=?", (user_id,))
            apartment_count = c.fetchone()[0]

            if apartment_count >= 10:
                bot.answer_callback_query(call.id, "❌ Максимум 10 квартир")
                conn.close()
                return
            
            c.execute("INSERT INTO apartments (user_id, purchase_date, price, last_collected) VALUES (?, ?, ?, ?)", 
                     (user_id, datetime.now().isoformat(), 25000000, datetime.now().isoformat()))
            conn.commit()
            conn.close()
            
            new_balance = user[3] - 25000000
            db.update_user(user_id, balance=new_balance)
            
            bot.answer_callback_query(call.id, "✅ Квартира куплена!")
            bot.edit_message_text(f"✅ *Квартира куплена!*\n\nСписано: 25,000,000 т\nНовый баланс: {new_balance:,} т\nДоход: 500,000 т/час", call.message.chat.id, call.message.message_id, parse_mode='Markdown')

        elif call.data == "apartment_repair":
            user_id = call.from_user.id
            conn = sqlite3.connect('/app/data/bot.db')
            c = conn.cursor()
            c.execute("SELECT COUNT(*) FROM apartments WHERE user_id=? AND renovated=0", (user_id,))
            count = c.fetchone()[0]
            conn.close()

            if count == 0:
                bot.edit_message_text("🔨 *Нет квартир без ремонта*", call.message.chat.id, call.message.message_id, parse_mode='Markdown', reply_markup=apartment_keyboard())
            else:
                bot.edit_message_text("🔨 *Выберите квартиру для ремонта (10,000,000 т)*", call.message.chat.id, call.message.message_id, parse_mode='Markdown', reply_markup=get_repair_keyboard(user_id))

        elif call.data.startswith("repair_"):
            apt_id = int(call.data.split("_")[1])
            user_id = call.from_user.id
            user = db.get_user(user_id)

            if user[3] < 10000000:
                bot.answer_callback_query(call.id, "❌ Недостаточно тенге. Нужно: 10,000,000 т")
                return

            conn = sqlite3.connect('/app/data/bot.db')
            c = conn.cursor()
            c.execute("SELECT renovated FROM apartments WHERE id=? AND user_id=?", (apt_id, user_id))
            apt = c.fetchone()

            if not apt:
                bot.answer_callback_query(call.id, "❌ Квартира не найдена")
                conn.close()
                return

            if apt[0] == 1:
                bot.answer_callback_query(call.id, "❌ Квартира уже с ремонтом")
                conn.close()
                return

            c.execute("UPDATE apartments SET renovated=1 WHERE id=?", (apt_id,))
            conn.commit()
            conn.close()

            new_balance = user[3] - 10000000
            db.update_user(user_id, balance=new_balance)

            bot.answer_callback_query(call.id, "✅ Ремонт выполнен!")
            bot.edit_message_text(f"✅ *Ремонт выполнен!*\n\nСписано: 10,000,000 т\nНовый баланс: {new_balance:,} т\nДоход теперь: 1,000,000 т/час", call.message.chat.id, call.message.message_id, parse_mode='Markdown')

        elif call.data == "apartment_sell":
            user_id = call.from_user.id
            conn = sqlite3.connect('/app/data/bot.db')
            c = conn.cursor()
            c.execute("SELECT COUNT(*) FROM apartments WHERE user_id=?", (user_id,))
            count = c.fetchone()[0]
            conn.close()

            if count == 0:
                bot.edit_message_text("💰 *Нет квартир для продажи*", call.message.chat.id, call.message.message_id, parse_mode='Markdown', reply_markup=apartment_keyboard())
            else:
                bot.edit_message_text("💰 *Выберите квартиру для продажи*", call.message.chat.id, call.message.message_id, parse_mode='Markdown', reply_markup=get_sell_keyboard(user_id))

        elif call.data.startswith("sell_"):
            apt_id = int(call.data.split("_")[1])
            user_id = call.from_user.id
            user = db.get_user(user_id)

            conn = sqlite3.connect('/app/data/bot.db')
            c = conn.cursor()
            c.execute("SELECT renovated FROM apartments WHERE id=? AND user_id=?", (apt_id, user_id))
            apt = c.fetchone()

            if not apt:
                bot.answer_callback_query(call.id, "❌ Квартира не найдена")
                conn.close()
                return

            sell_price = 30000000 if apt[0] else 20000000

            c.execute("DELETE FROM apartments WHERE id=?", (apt_id,))
            conn.commit()
            conn.close()

            new_balance = user[3] + sell_price
            db.update_user(user_id, balance=new_balance)

            bot.answer_callback_query(call.id, f"✅ Квартира продана за {sell_price:,} т")
            bot.edit_message_text(f"✅ *Квартира продана!*\n\nПолучено: {sell_price:,} т\nНовый баланс: {new_balance:,} т", call.message.chat.id, call.message.message_id, parse_mode='Markdown')

        elif call.data == "collect_income":
            user_id = call.from_user.id
            user = db.get_user(user_id)

            conn = sqlite3.connect('/app/data/bot.db')
            c = conn.cursor()
            c.execute("SELECT * FROM apartments WHERE user_id=?", (user_id,))
            apartments = c.fetchall()

            if not apartments:
                bot.answer_callback_query(call.id, "❌ Нет квартир")
                conn.close()
                return

            total_earned = 0
            now = datetime.now().isoformat()

            for apt in apartments:
                apt_id, _, renovated, purchase_date, price, last_collected = apt
                income = 1000000 if renovated else 500000

                if last_collected:
                    collect_time = datetime.fromisoformat(last_collected)
                else:
                    collect_time = datetime.fromisoformat(purchase_date)

                hours_since_collect = (datetime.now() - collect_time).total_seconds() / 3600
                apartment_earned = int(hours_since_collect * income)
                total_earned += apartment_earned

                c.execute("UPDATE apartments SET last_collected=? WHERE id=?", (now, apt_id))

            conn.commit()
            conn.close()

            if total_earned == 0:
                bot.answer_callback_query(call.id, "❌ Нет накопленного дохода")
                return

            new_balance = user[3] + total_earned
            db.update_user(user_id, balance=new_balance)

            bot.answer_callback_query(call.id, f"✅ Собрано {total_earned:,} т")
            bot.edit_message_text(f"✅ *Доход собран!*\n\nПолучено: {total_earned:,} т\nНовый баланс: {new_balance:,} т", call.message.chat.id, call.message.message_id, parse_mode='Markdown')

        elif call.data.startswith('stats_'):
            stats_type = call.data.split('_')[1]
            chat_id = call.message.chat.id
            conn = sqlite3.connect('/app/data/bot.db')
            c = conn.cursor()
            if stats_type == 'today':
                today = datetime.now().strftime('%Y-%m-%d')
                c.execute("""
                    SELECT u.first_name, dsg.messages 
                    FROM daily_stats_group dsg 
                    JOIN users u ON dsg.user_id = u.user_id 
                    WHERE dsg.date = ? AND dsg.chat_id = ?
                    ORDER BY dsg.messages DESC 
                    LIMIT 10
                """, (today, chat_id))
                stats = c.fetchall()
                title = "📊 *СТАТИСТИКА ЗА СЕГОДНЯ (эта группа)*\n\n"
            elif stats_type == 'month':
                current_month = datetime.now().strftime('%Y-%m')
                c.execute("""
                    SELECT u.first_name, msg.messages 
                    FROM monthly_stats_group msg 
                    JOIN users u ON msg.user_id = u.user_id 
                    WHERE msg.month = ? AND msg.chat_id = ?
                    ORDER BY msg.messages DESC 
                    LIMIT 10
                """, (current_month, chat_id))
                stats = c.fetchall()
                title = "📅 *СТАТИСТИКА ЗА МЕСЯЦ (эта группа)*\n\n"
            elif stats_type == 'all':
                c.execute("""
                    SELECT u.first_name, asg.messages 
                    FROM all_stats_group asg 
                    JOIN users u ON asg.user_id = u.user_id 
                    WHERE asg.chat_id = ?
                    ORDER BY asg.messages DESC 
                    LIMIT 10
                """, (chat_id,))
                stats = c.fetchall()
                title = "📈 *ВСЯ СТАТИСТИКА (эта группа)*\n\n"
            stats_text = title
            if stats:
                for i, (name, count) in enumerate(stats, 1):
                    stats_text += f"{i}. {name} - `{count}` сообщ.\n"
            else:
                stats_text += "📝 Данных пока нет"
            bot.edit_message_text(stats_text, call.message.chat.id, call.message.message_id, 
                                 parse_mode='Markdown', reply_markup=stats_keyboard())
            conn.close()
            bot.answer_callback_query(call.id)
            
    except Exception as e:
        logger.error(f"Callback error: {e}")
        bot.answer_callback_query(call.id, "❌ Произошла ошибка")

def process_custom_prefix(message):
    try:
        prefix = message.text.strip()
        if len(prefix) > 10:
            bot.send_message(message.chat.id, "❌ Префикс должен быть до 10 символов")
            return
        
        user_id = message.from_user.id
        user = db.get_user(user_id)
        
        if user[4] < 1000:
            bot.send_message(message.chat.id, "❌ Недостаточно депусов. Нужно: 1000 д")
            return
        
        new_depuses = user[4] - 1000
        db.update_user(user_id, depuses=new_depuses, prefix=prefix)
        
        bot.send_message(message.chat.id, f"✅ *Префикс установлен!*\n\nНовый префикс: {prefix}\nБаланс: {new_depuses} д", parse_mode='Markdown')
        
    except Exception as e:
        bot.send_message(message.chat.id, f"❌ Ошибка: {str(e)}")


# ========== ЗАПУСК БОТА ==========
if __name__ == "__main__":

    logger.info("🤖 Бот запускается...")
    try:
        bot_info = bot.get_me()
        logger.info(f"✅ Подключение к Telegram API: {bot_info.first_name} (@{bot_info.username})")
        try:
            db_file = '/app/data/bot.db'
            try:
                db_size = os.path.getsize(db_file)
            except Exception:
                db_size = 0
            try:
                bot.send_message(ADMIN_ID, f"🤖 Бот запущен: {bot_info.first_name} (@{bot_info.username})\nDB: {db_file} ({db_size // 1024} KB)")
            except Exception as e:
                logger.warning(f"Не удалось отправить сообщение админу: {e}")
        except Exception:
            pass
        try:
            conn = sqlite3.connect('/app/data/bot.db')
            c = conn.cursor()
            c.execute("SELECT group_id FROM groups")
            saved_groups = set(row[0] for row in c.fetchall())
            for chat_id in saved_groups:
                try:
                    chat = bot.get_chat(chat_id)
                    c.execute("UPDATE groups SET title=? WHERE group_id=?", (chat.title, chat_id))
                except Exception:
                    c.execute("DELETE FROM groups WHERE group_id=?", (chat_id,))
                    logger.info(f"❌ Удалена недоступная группа: {chat_id}")
            conn.commit()
            conn.close()
            logger.info("✅ Список групп обновлен")
        except Exception as e:
            logger.error(f"❌ Ошибка при обновлении списка групп: {e}")
    except Exception as e:
        logger.error(f"❌ Ошибка подключения к Telegram API: {e}")
        exit(1)
    logger.info("🔄 Запускаем polling (resilient mode)...")
    while True:
        try:
            bot.polling(none_stop=True, interval=0, timeout=20)
        except Exception as e:
            logger.error(f"❌ Polling crashed: {e}")
            time.sleep(5)
