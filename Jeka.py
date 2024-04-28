import logging
import re
import requests
import aiohttp
from collections import defaultdict
from google.oauth2 import service_account
from googleapiclient.discovery import build
from aiogram import Bot, Dispatcher, types
from aiogram.contrib.middlewares.logging import LoggingMiddleware
#from aiogram.types import ParseMode, InputFile
#from kinopoisk_unofficial.kinopoisk_api_client import KinopoiskApiClient
#from kinopoisk_unofficial.request.films.search_by_keyword_request import SearchByKeywordRequest
#from kinopoisk_unofficial.response.films.film_response import FilmResponse
#from kinopoisk_unofficial.model.film import Film

#from dataclasses import dataclass
#from typing import List, Optional, Union

#from kinopoisk_unofficial.model.country import Country
#from kinopoisk_unofficial.model.dictonary.film_type import FilmType
#from kinopoisk_unofficial.model.dictonary.production_status import ProductionStatus
#from kinopoisk_unofficial.model.genre import Genre
from aiogram.types import InputFile

# Установка параметров доступа к API Google Sheets
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
SERVICE_ACCOUNT_FILE = '/home/rejoller/mcrbot/credentials.json'
SPREADSHEET_ID = ''

creds = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES)

user_movies = defaultdict(list)

bot_token = ''
bot = Bot(token=bot_token)
dp = Dispatcher(bot)
dp.middleware.setup(LoggingMiddleware())



def search_values(query):
    service = build('sheets', 'v4', credentials=creds)
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range='Фильмы!A1:I1000').execute()
    rows = result.get('values', [])

    found_rows = [row for row in rows if query.lower() in row[5].lower()]
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
    for frame in frames:
        frame_url = frame["preview"]
        local_path = "frame.jpg"
        download_image(frame_url, local_path)
        with open(local_path, "rb") as frame_file:
            await bot.send_photo(chat_id, InputFile(frame_file))

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
        await bot.send_photo(chat_id, InputFile(cover_file))

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
    keyboard = InlineKeyboardMarkup(row_width=1)
    for index, movie in enumerate(movies):
        keyboard.insert(InlineKeyboardButton(f"{index + 1}. {movie[1]}", callback_data=f"movie_{index}"))
    await bot.send_message(chat_id=chat_id,
                           text=f"Выберите фильм, который хотите посмотреть, {user.first_name}:",
                           reply_markup=keyboard)

@dp.callback_query_handler(lambda c: c.data and c.data.startswith("movie_"))
async def process_callback_button1(callback_query: types.CallbackQuery):
    index = int(callback_query.data.split("_")[1])
    movie_id = user_movies[callback_query.from_user.id][index][3]
    movie = await search_kinopoisk_movie_by_id("cc45654b-e807-432c-8114-debf8fb8565d", movie_id)

    if movie:
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
    user_movies[message.from_user.id] = search_values(message.text)
    await send_movies_keyboard(message.from_user.id, user_movies[message.from_user.id], message.from_user)

if __name__ == '__main__':
    from aiogram import executor
    executor.start_polling(dp, on_startup=on_startup, on_shutdown=on_shutdown)

