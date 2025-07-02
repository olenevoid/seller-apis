import io
import logging.config
import os
import re
import zipfile
from environs import Env

import pandas as pd
import requests

logger = logging.getLogger(__file__)


def get_product_list(last_id, client_id, seller_token):
    """Получает список товаров магазина на Ozon.
    
    Делает запрос к API Ozon для получения информации о товарах с пагинацией.

    Args:
        last_id (str): Идентификатор последнего товара
        client_id (str): Идентификатор приложения для работы с API Ozon
        seller_token (str): API-ключ продавца

    Returns:
        dict: Словарь с результатами запроса, содержащий информацию о товарах
        
    Raises:
        requests.exceptions.HTTPError: При ошибке HTTP-запроса
    """
    url = "https://api-seller.ozon.ru/v2/product/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {
        "filter": {
            "visibility": "ALL",
        },
        "last_id": last_id,
        "limit": 1000,
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def get_offer_ids(client_id, seller_token):
    """Получает артикулы всех товаров магазина на Ozon.
    
    Собирает полный список товаров, обрабатывая все страницы результатов.

    Args:
        client_id (str): Идентификатор приложения для работы с API Ozon
        seller_token (str): API-ключ продавца

    Returns:
        list: Список артикулов товаров (offer_id)
        
    Raises:
        requests.exceptions.HTTPError: При ошибке HTTP-запроса
    """
    
    last_id = ""
    product_list = []
    while True:
        some_prod = get_product_list(last_id, client_id, seller_token)
        product_list.extend(some_prod.get("items"))
        total = some_prod.get("total")
        last_id = some_prod.get("last_id")
        if total == len(product_list):
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer_id"))
    return offer_ids


def update_price(prices: list, client_id, seller_token):
    """Обновляет цены товаров на Ozon.
    
    Отправляет новые цены через API Ozon.

    Args:
        prices (list): Список словарей с ценами товаров
        client_id (str): Идентификатор приложения для работы с API Ozon
        seller_token (str): API-ключ продавца

    Returns:
        dict: Ответ API Ozon после обновления цен
        
    Raises:
        requests.exceptions.HTTPError: При ошибке HTTP-запроса
    """
    url = "https://api-seller.ozon.ru/v1/product/import/prices"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"prices": prices}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def update_stocks(stocks: list, client_id, seller_token):
    """Обновляет информацию об остатках товаров на Ozon.
    
    Отправляет новые данные о количестве товаров через API Ozon.

    Args:
        stocks (list): Список словарей с данными об остатках
        client_id (str): Идентификатор приложения для работы с API Ozon
        seller_token (str): API-ключ продавца

    Returns:
        dict: Ответ API Ozon после обновления остатков
        
    Raises:
        requests.exceptions.HTTPError: При ошибке HTTP-запроса
    """
    url = "https://api-seller.ozon.ru/v1/product/import/stocks"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"stocks": stocks}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def download_stock():
    """Скачивает и обрабатывает файл с остатками товаров.
    
    Выполняет:
    1. Скачивание ZIP-архива с остатками с сайта поставщика
    2. Распаковку архива
    3. Чтение данных из Excel-файла
    4. Удаление временного файла

    Returns:
        list: Список словарей с информацией об остатках товаров
        
    Raises:
        requests.exceptions.HTTPError: При ошибке HTTP-запроса
    """
    # Скачать остатки с сайта
    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")
    # Создаем список остатков часов:
    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(
        io=excel_file,
        na_values=None,
        keep_default_na=False,
        header=17,
    ).to_dict(orient="records")
    os.remove("./ostatki.xls")  # Удалить файл
    return watch_remnants


def create_stocks(watch_remnants, offer_ids):
    """Формирует данные об остатках для обновления на Ozon.
    
    Создает два типа записей:
    1. Для товаров, присутствующих у поставщика (с реальными остатками)
    2. Для товаров, отсутствующих у поставщика (остаток = 0)

    Args:
        watch_remnants (list): Данные об остатках от поставщика
        offer_ids (list): Список артикулов товаров на Ozon

    Returns:
        list: Список словарей в формате, готовом для отправки в API Ozon
    """
    # Уберем то, что не загружено в seller
    stocks = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append({"offer_id": str(watch.get("Код")), "stock": stock})
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Формирует данные о ценах для обновления на Ozon.
    
    Создает записи только для товаров, присутствующих у поставщика.

    Args:
        watch_remnants (list): Данные об остатках от поставщика
        offer_ids (list): Список артикулов товаров на Ozon

    Returns:
        list: Список словарей с ценами в формате API Ozon
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": str(watch.get("Код")),
                "old_price": "0",
                "price": price_conversion(watch.get("Цена")),
            }
            prices.append(price)
    return prices


def price_conversion(price: str) -> str:
    """Преобразует строку с ценой в числовой формат без символов.
    
    Обрабатывает ценовые строки, удаляя все нецифровые символы и дробную часть.
    Используется для подготовки цен к загрузке в маркетплейсы.

    Пример преобразования:
        "5'990.00 руб." -> "5990"        

    Пример некорректного использования:        
        price_conversion("Цена: отсутствует") -> ""

    Args:
        price (str): Исходная строка с ценой, может содержать пробелы, 
                     символы валют, разделители тысяч и десятичную часть.

    Returns:
        str: Цена в виде строки, содержащей только целые числа. 
              Возвращает пустую строку, если во входных данных нет цифр.

    Note:
        Функция не проверяет валидность цены и может вернуть пустую строку
        при отсутствии цифр в исходных данных. Рекомендуется предварительная
        валидация входных значений.
    """    
    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """Разделяет список на части фиксированного размера.
    
    Генератор, который разбивает список на подсписки указанного размера.
    
    Пример преобразования:
        list(divide([1, 2, 3, 4, 5], 2)) -> [[1, 2], [3, 4], [5]]

    Args:
        lst (list): Исходный список для разделения
        n (int): Максимальный размер каждого подсписка

    Yields:
        list: Очередная часть исходного списка
    """
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


async def upload_prices(watch_remnants, client_id, seller_token):
    """Асинхронно обновляет цены товаров на Ozon.
    
    Выполняет:
    1. Получение списка артикулов
    2. Создание данных для обновления цен
    3. Пакетную отправку данных в API Ozon

    Args:
        watch_remnants (list): Данные об остатках от поставщика
        client_id (str): Идентификатор клиента Ozon
        seller_token (str): API-ключ продавца

    Returns:
        list: Список обновленных цен
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    """Асинхронно обновляет остатки товаров на Ozon.
    
    Выполняет:
    1. Получение списка артикулов
    2. Создание данных об остатках
    3. Пакетную отправку данных в API Ozon

    Args:
        watch_remnants (list): Данные об остатках от поставщика
        client_id (str): Идентификатор клиента Ozon
        seller_token (str): API-ключ продавца

    Returns:
        tuple: Кортеж из двух списков:
            - Товары с ненулевым остатком
            - Все обработанные товары
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    env = Env()
    seller_token = env.str("SELLER_TOKEN")
    client_id = env.str("CLIENT_ID")
    try:
        offer_ids = get_offer_ids(client_id, seller_token)
        watch_remnants = download_stock()
        # Обновить остатки
        stocks = create_stocks(watch_remnants, offer_ids)
        for some_stock in list(divide(stocks, 100)):
            update_stocks(some_stock, client_id, seller_token)
        # Поменять цены
        prices = create_prices(watch_remnants, offer_ids)
        for some_price in list(divide(prices, 900)):
            update_price(some_price, client_id, seller_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
