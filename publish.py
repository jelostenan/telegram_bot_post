import psycopg2
from telegram import Bot
from telegram.constants import ParseMode
import asyncio
from datetime import datetime

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

        # Извлечение контента для публикации
        cursor.execute("""
            SELECT c.chat_id, ct.content, ct.id 
            FROM content ct
            JOIN channels c ON ct.channel_id = c.id
            WHERE ct.is_published = FALSE AND c.next_post_time <= %s
        """, (now,))
        rows = cursor.fetchall()
        print(f"[INFO] Найдено сообщений для публикации: {len(rows)}")

        # Публикация контента
        for chat_id, content, content_id in rows:
            try:
                print(f"[INFO] Публикация в канал: {chat_id}, Текст: {content}")
                # Асинхронная отправка сообщения
                await bot.send_message(chat_id=chat_id, text=content, parse_mode=ParseMode.HTML)
                print(f"[SUCCESS] Опубликовано в {chat_id}: {content}")

                # Обновление статуса публикации
                cursor.execute("UPDATE content SET is_published = TRUE WHERE id = %s", (content_id,))
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
