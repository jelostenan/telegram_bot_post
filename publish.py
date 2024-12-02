import psycopg2
from telegram import Bot
from telegram.constants import ParseMode
import asyncio
from datetime import datetime, timedelta
import random

# Настройки базы данных
DB_CONFIG = {
    "dbname": "db_name",
    "user": "user",
    "password": "password",
    "host": "localhost"
}

# Настройки Telegram бота
BOT_TOKEN = "token"
bot = Bot(token=BOT_TOKEN)

# Асинхронная публикация сообщений
async def publish_posts():
    try:
        # Подключение к базе данных
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Текущая дата и время
        now = datetime.now()
        print(f"[INFO] Текущая дата и время: {now}")

        # Извлечение одного сообщения для публикации
        cursor.execute("""
            SELECT c.chat_id, ct.content, ct.id, c.id AS channel_id
            FROM content ct
            JOIN channels c ON ct.channel_id = c.id
            WHERE ct.is_published = FALSE AND c.next_post_time <= %s
            ORDER BY c.next_post_time ASC
            LIMIT 1
        """, (now,))
        row = cursor.fetchone()

        if not row:
            print("[INFO] Нет сообщений для публикации.")
            return

        chat_id, content, content_id, channel_id = row

        try:
            # Публикация сообщения
            print(f"[INFO] Публикация в канал: {chat_id}, Текст: {content}")
            await bot.send_message(chat_id=chat_id, text=content, parse_mode=ParseMode.HTML)
            print(f"[SUCCESS] Опубликовано в {chat_id}: {content}")

            # Обновление статуса сообщения
            cursor.execute("UPDATE content SET is_published = TRUE WHERE id = %s", (content_id,))

            # Расчёт случайного времени для следующей публикации
            next_interval = random.randint(60, 5 * 60)  # От 1 до 5 минут
            next_post_time = now + timedelta(seconds=next_interval)
            cursor.execute("UPDATE channels SET next_post_time = %s WHERE id = %s", (next_post_time, channel_id))

            conn.commit()
        except Exception as e:
            print(f"[ERROR] Ошибка при публикации в {chat_id}: {e}")

    except Exception as e:
        print(f"[ERROR] Ошибка подключения к базе данных: {e}")

    finally:
        if conn:
            cursor.close()
            conn.close()

# Запуск асинхронной функции
if __name__ == "__main__":
    asyncio.run(publish_posts())
