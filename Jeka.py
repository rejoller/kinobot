import logging
import re
import requests
import aiohttp
import spacy
from collections import defaultdict
from google.oauth2 import service_account
from googleapiclient.discovery import build
from aiogram import Bot, Dispatcher, types
from aiogram.contrib.middlewares.logging import LoggingMiddleware
from aiogram.types import InputFile
import asyncio
from aiogram.utils.exceptions import MessageNotModified
import pymorphy2


# Установка параметров доступа к API Google Sheets
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
SERVICE_ACCOUNT_FILE = '/home/rejoller/mcrbot/credentials.json'
SPREADSHEET_ID = '1j2NaYPG8QUekY0El0jcRPLE4KYj_yrYfoaZnBqOXbyY'

creds = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES)

user_movies = defaultdict(list)

bot_token = '6029945146:AAGrGlYT4OTJCGTkVSLZbDDdzlccAmqKcUY'
bot = Bot(token=bot_token)
dp = Dispatcher(bot)
dp.middleware.setup(LoggingMiddleware())

# Загрузка spacy модели для русского языка
nlp = spacy.load("ru_core_news_sm")
morph = pymorphy2.MorphAnalyzer()

'''
def extract_actors_and_genres(text):
    actors = []
    genres = []
    prev_word_was_name = False

    doc = nlp(text)
    for token in doc:
        parsed_word = morph.parse(token.text)[0]
        if parsed_word.tag.POS == 'NOUN' and 'Name' in parsed_word.tag:
            if prev_word_was_name:
                actors[-1] += f" {token.text}"
            else:
                actors.append(token.text)
            prev_word_was_name = True
        elif parsed_word.tag.POS == 'NOUN':
            genres.append(token.text)
            prev_word_was_name = False
        else:
            prev_word_was_name = False




    print("Извлеченные актеры:", actors)
    print("Извлеченные жанры:", genres)

    return actors, genres
'''

def get_unique_actors():
    service = build('sheets', 'v4', credentials=creds)
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range='Фильмы!A1:I1000').execute()
    rows = result.get('values', [])
    actors = set()
    for row in rows:
        actors_row = row[5].replace('\xa0', ' ').split(', ')
        for actor in actors_row:
            actors.add(actor.strip())
    return actors

def get_unique_genres():
    service = build('sheets', 'v4', credentials=creds)
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range='Фильмы!A1:I1000').execute()
    rows = result.get('values', [])
    genres = set()
    for row in rows:
        genres_row = row[4].split(', ')
        for genre in genres_row:
            genres.add(normalize_string(genre.strip()))
    return genres

def extract_actors_and_genres(text):
    actors = []
    genres = []
    unique_actors = get_unique_actors()
    unique_genres = get_unique_genres()

    # Вывод текста для отладки
    print("Обрабатываемый текст:", text)
    print("Уникальные актеры:", unique_actors)
    print("Уникальные жанры:", unique_genres)

    # Замена специального апострофа на обычный одинарный кавычка
    text = text.replace("’", "'")

    # Удаление знаков препинания из текста
    text_without_punctuation = re.sub(r'[^\w\s]', '', text)

    words = text_without_punctuation.split()
    normalized_words = [normalize_string(word) for word in words]

    for word in normalized_words:
        if word in unique_genres:
            genres.append(word)

    # Создайте регулярные выражения для всех уникальных актеров
    actor_regexes = [re.compile(fr"\b{actor}\b", re.IGNORECASE) for actor in unique_actors]

    # Проверьте, присутствует ли каждый актер в тексте
    for actor, actor_regex in zip(unique_actors, actor_regexes):
        if actor_regex.search(text):
            actors.append(actor)

    print("Извлеченные актеры:", actors)
    print("Извлеченные жанры:", genres)

    return actors, genres


'''
def search_values(query):
    service = build('sheets', 'v4', credentials=creds)
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range='Фильмы!A1:I1000').execute()
    rows = result.get('values', [])

    found_rows = [row for row in rows if query.lower() in row[5].lower()]
    return found_rows
'''

def normalize_string(string):
    words = string.split()
    normalized_words = [morph.parse(word)[0].normal_form for word in words]
    return " ".join(normalized_words)



def search_movies_by_actors_and_genres(actors, genres):
    if not actors and not genres:  # Добавляем проверку на пустые списки
        return []

    service = build('sheets', 'v4', credentials=creds)
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range='Фильмы!A1:I1000').execute()
    rows = result.get('values', [])

    found_rows = []
    for row in rows:
        row_actors = row[5]
        row_genres = row[4]
        print("Актеры для строки:", row_actors)
        print("Жанры для строки:", row_genres)
        actors_present = all(actor.lower() in row_actors.lower() for actor in actors) if actors else True
        genres_present = all(genre.lower() in row_genres.lower() for genre in genres) if genres else True

        if actors_present and genres_present:
            found_rows.append(row)

    return found_rows



# Установка токена и создание бота

from dataclasses import asdict

def convert_film_data_keys(film_data):
    converted_data = {}
    for key, value in film_data.items():
        if key == 'filmId':
            key = 'kinopoisk_id'
        elif key == 'nameRU':
            key = 'name_ru'
        elif key == 'nameEN':
            key = 'name_en'
        converted_data[key] = value
    return converted_data



async def get_movie_frames(api_key, movie_id):
    async with aiohttp.ClientSession() as session:
        async with session.get(
            f"https://kinopoiskapiunofficial.tech/api/v2.1/films/{movie_id}/frames",
            headers={"X-API-KEY": api_key},
        ) as response:
            if response.status == 200:
                frames_data = await response.json()
                frames = frames_data.get("frames", [])[:5]
                return frames
            else:
                return None

async def send_movie_frames(chat_id, frames):
    frame_message_ids = []
    for frame in frames:
        frame_url = frame["preview"]
        local_path = "frame.jpg"
        download_image(frame_url, local_path)
        with open(local_path, "rb") as frame_file:
            sent_frame = await bot.send_photo(chat_id, InputFile(frame_file))
            frame_message_ids.append(sent_frame.message_id)
    return frame_message_ids

@dp.callback_query_handler(lambda c: c.data and c.data.startswith("frames_"))
async def process_frames_button(callback_query: types.CallbackQuery):
    index = int(callback_query.data.split("_")[1])
    movie_id = user_movies[callback_query.from_user.id][index][3]
    frames = await get_movie_frames("cc45654b-e807-432c-8114-debf8fb8565d", movie_id)
    if frames:
        await send_movie_frames(callback_query.from_user.id, frames)
    else:
        await bot.send_message(callback_query.from_user.id, "Не удалось загрузить кадры фильма.")

async def search_kinopoisk_movie_by_id(api_key, movie_id):
    async with aiohttp.ClientSession() as session:
        async with session.get(
            f"https://kinopoiskapiunofficial.tech/api/v2.1/films/{movie_id}",
            headers={"X-API-KEY": api_key},
        ) as response:
            if response.status == 200:
                film_data = await response.json()
                film = film_data['data']

                title = film['nameRu']
                release_date = film['year']
                genres = ", ".join([genre['genre'] for genre in film['genres']])
                description = film['description']
                cover_url = film['posterUrl']

                kinopoisk_movie = {
                    "title": title,
                    "release_date": release_date,
                    "genres": genres,
                    "description": description,
                    "cover_url": cover_url
                }

                return kinopoisk_movie
            else:
                return None

def download_image(url, local_path):
    response = requests.get(url)
    with open(local_path, 'wb') as f:
        f.write(response.content)

async def send_movie_cover(chat_id, cover_url):
    local_path = 'cover.jpg'
    download_image(cover_url, local_path)
    with open(local_path, 'rb') as cover_file:
        sent_cover = await bot.send_photo(chat_id, InputFile(cover_file))
    return sent_cover.message_id

# Настройка логирования
logging.basicConfig(level=logging.INFO)

# Создание диспетчера для работы с ботом


async def on_startup(dp):
    logging.info('Bot has been started')

async def on_shutdown(dp):
    logging.info('Bot has been stopped')

    await dp.storage.close()
    await dp.storage.wait_closed()

from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

async def send_movies_keyboard(chat_id, movies, user: types.User):
    if not movies:
        await bot.send_message(chat_id=chat_id, text="Фильм не найден в таблице Жекича")
        return

    keyboard = InlineKeyboardMarkup(row_width=1)
    for index, movie in enumerate(movies):
        keyboard.insert(InlineKeyboardButton(f"{index + 1}. {movie[1]}", callback_data=f"movie_{index}"))
    movie_choice_message = await bot.send_message(chat_id=chat_id,
                           text="Выбери фильм из таблицы Жекича:",
                           reply_markup=keyboard)
    return movie_choice_message.message_id


async def animated_text(chat_id, message_id, base_text, final_text):
    current_text = base_text
    for i in range(len(base_text), len(final_text)):
        new_text = current_text + final_text[i]
        for attempt in range(3):
            try:
                await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=new_text)
                current_text = new_text
                await asyncio.sleep(0.1)  # задержка между символами (в секундах)
                break  # если сообщение успешно обновлено, выйти из цикла попыток
            except MessageNotModified:
                if attempt == 2:  # если это последняя попытка и сообщение все еще не изменено, пропустить обновление
                    break
                await asyncio.sleep(0.1)  # задержка перед повторной попыткой (в секундах)# задержка перед повторной попыткой (в секундах)


@dp.callback_query_handler(lambda c: c.data and c.data.startswith("movie_"))
async def process_callback_button1(callback_query: types.CallbackQuery):
    index = int(callback_query.data.split("_")[1])
    movie_id = user_movies[callback_query.from_user.id][index][3]
    movie = await search_kinopoisk_movie_by_id("cc45654b-e807-432c-8114-debf8fb8565d", movie_id)

    if movie:
        base_text = "В"
        final_text = "Выбранный\xA0фильм:"
        #final_text = "🟩🟩🟩🟩🟩🟩🟩"
        await animated_text(callback_query.from_user.id, callback_query.message.message_id, base_text, final_text)


        await send_movie_cover(callback_query.from_user.id, movie['cover_url'])
        show_frames_keyboard = InlineKeyboardMarkup().add(InlineKeyboardButton("Показать кадры из фильма", callback_data=f"frames_{index}"))
        await bot.send_message(
            callback_query.from_user.id,
            f"{movie['title']} ({movie['release_date']})\nЖанры: {movie['genres']}\n{movie['description']}",
            reply_markup=show_frames_keyboard,
        )
    else:
        await bot.answer_callback_query(callback_query.id, text="Произошла ошибка при поиске фильма на Кинопоиске")



@dp.message_handler()
async def handle_message(message: types.Message):
    actors, genres = extract_actors_and_genres(message.text)
    user_movies[message.from_user.id] = search_movies_by_actors_and_genres(actors, genres)
    movie_choice_message_id = await send_movies_keyboard(message.from_user.id, user_movies[message.from_user.id], message.from_user)

if __name__ == '__main__':
    from aiogram import executor
    executor.start_polling(dp, on_startup=on_startup, on_shutdown=on_shutdown)

