# Подключаем библиотеки:
import telebot
import time
import datetime
import re
import os.path
import pathlib
import pandas as pd
from pandas.io.excel import ExcelWriter
from threading import Thread
import schedule


# Подключаем переменные:
from script_parameters import *

stop = False


# Конвертация даты в читабельный вид:
def time_converter(x):
    return time.strftime("%H:%M:%S %d.%m.%Y", time.localtime(x))


# Определяет вхождение в начало строки:
def find(string, sample):
    if sample in string:
        y = "^" + sample
        return re.search(y, string)
    else:
        return False


def is_file_exists(file_path):
    is_exist = False
    try:
        file = open(file_path)
    except IOError as e:
        str_ = f'Файл {file_path} не существует.'
        print(str_)
    else:
        is_exist = True
    finally:
        if is_exist:
            file.close()
    return is_exist


def append_df_to_excel(df, excel_path, sheet_name):
    xl = pd.ExcelFile(excel_path)
    df_excel = xl.parse(sheet_name)
    result = pd.concat([df_excel, df], ignore_index=True)
    with pd.ExcelWriter(excel_path, engine='openpyxl', mode='a', if_sheet_exists="replace") as writer:
        result.to_excel(writer, sheet_name=sheet_name, index=False)
        print('Добавлена запись в excel.')


def write_to_file(user, message, time_):
    r = ''
    sheet_name = ''
    now = datetime.datetime.now()
    curr_week = now.isocalendar().week
    if TEST_MODE:
        curr_week = 42
    # Проверяем наличие директории для вспомогательных файлов, если ее нет, то создаем:
    total_dir_path = os.path.abspath(os.curdir)
    dir_path = '{0}\{1}'.format(total_dir_path, INSIDE_FILES)
    pathlib.Path(dir_path).mkdir(parents=True, exist_ok=True)
    # Проверяем наличие файла, если нет, то создаем и записываем в него номер текущей недели:
    file_path = f'{dir_path}\{WEEK_COUNTER_FILE}'
    excel_file_path = f'{dir_path}\{EXCEL_FILE}'
    if not is_file_exists(file_path):
        new_file = open(file_path, "w+")
        new_file.write(str(curr_week))
        new_file.close()
        str_ = f'Создали файл: {file_path}'
        print(str_)
    # Есть excel файл для записи?
    if not is_file_exists(excel_file_path):  # Есть ли excel файл?
        # excel файла нет, своздаем его:
        sheet_name = "".join([str(curr_week), "_week"])
        df = pd.DataFrame([], columns=COLUMN_NAMES)
        df.to_excel(excel_file_path, sheet_name=sheet_name)
        str_ = f'Создали файл: {excel_file_path}'
        print(str_)
    # ФАйлы существуют, проверяем, совпадает текущая неделя с записанной:
    with open(file_path) as f:
        r = f.read()
    if r == str(curr_week):     # Неделя совпадает
        # Преобразовываем данные в DataFrame:
        data = {COLUMN_NAMES[0]: user, COLUMN_NAMES[1]: message, COLUMN_NAMES[2]:
                time_converter(time_)}
        df_for_adding = pd.DataFrame(data, index=[0])
        sheet_name = "".join([str(curr_week), "_week"])     # Определяем страницу куда писать
        # Дописываем полученную строку в файл excel
        append_df_to_excel(df_for_adding, excel_file_path, sheet_name)
    else:
        """ Недели не совпали (делаем запись в новую страницу и ..."""
        sheet_name = "".join([str(curr_week), "_week"])
        data = {COLUMN_NAMES[0]: user, COLUMN_NAMES[1]: message, COLUMN_NAMES[2]:
            time_converter(time_)}
        df = pd.DataFrame(data, columns=COLUMN_NAMES, index=[0])
        with pd.ExcelWriter(excel_file_path, engine='openpyxl', mode='a') as writer:
             df.to_excel(writer, sheet_name=sheet_name)

        """ меняем номер текущей недели в контрольном файле):"""
        f = open(file_path, "w")
        f.write(str(curr_week))
        f.close()


def bot_body():
    bot = telebot.TeleBot(TOKEN)

    # обработчик команды '/start'
    @bot.message_handler(commands=['start'])
    def send_welcome(message):
        bot.send_message(message.from_user.id, TEXT_WELCOME)

    # обработчик сообщения
    @bot.message_handler(content_types=['text'])
    def get_messages(message):
        print(message)
        if find(message.text, TEXT_TPLATE):
            bot.send_message(message.from_user.id, LINK_RECEIVED)
            write_to_file(message.from_user.username, message.text, message.date)
        else:
            bot.send_message(message.from_user.id, OTHER_TEXT)

    bot.polling(none_stop=True, interval=0)


def sender_from_thread():
    while not stop:
        schedule.every().sunday.at("23:00").do(send_excel)
        time.sleep(1)


def send_excel():
    bot = telebot.TeleBot(TOKEN)
    total_dir_path = os.path.abspath(os.curdir)
    dir_path = '{0}\{1}'.format(total_dir_path, INSIDE_FILES)
    excel_file_path = f'{dir_path}\{EXCEL_FILE}'
    if is_file_exists(excel_file_path):
        with open(excel_file_path, 'rb') as f:
            bot.send_document(CHAT_ID, f)
    else:
        str_ = f'Сообщение из потока: файл по адресу {excel_file_path} не найден.'
        print(str_)


if __name__ == "__main__":
    str_ = 'Скрипт начал работу.'
    print(str_)
    # Создаем и запускаем параленый поток
    sender_body = Thread(target=sender_from_thread,  daemon=True)
    sender_body.start()
    try:
        bot_body()
    except Exception as e:
        print(e)
    finally:
        # Останавливаем паралельный поток
        stop = True
        sender_body.join()
        str_ = 'Скрипт остановлен.'
        print(str_)

