import datetime
import logging.config
from environs import Env
from seller import download_stock

import requests

from seller import divide, price_conversion

logger = logging.getLogger(__file__)


def get_product_list(page, campaign_id, access_token):
    """Получает список товаров для модели распространения на Яндекс.Маркете.
    
    Выполняет запрос к API Маркета для получения товаров с пагинацией.

    Args:
        page (str): Токен пагинации для получения следующей страницы
        campaign_id (str): Идентификатор модели распространения
        access_token (str): Токен доступа к API Яндекс.Маркета

    Returns:
        dict: Словарь с результатами запроса, содержащий товары и информацию о пагинации
        
    Raises:
        requests.exceptions.HTTPError: При ошибке HTTP-запроса
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {
        "page_token": page,
        "limit": 200,
    }
    url = endpoint_url + f"campaigns/{campaign_id}/offer-mapping-entries"
    response = requests.get(url, headers=headers, params=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def update_stocks(stocks, campaign_id, access_token):
    """Обновляет информацию об остатках товаров на складе.
    
    Отправляет данные об остатках через API Яндекс.Маркета.

    Args:
        stocks (list): Список словарей с данными об остатках
        campaign_id (str): Идентификатор модели распространения
        access_token (str): Токен доступа к API Яндекс.Маркета

    Returns:
        dict: Ответ API после обновления остатков
        
    Raises:
        requests.exceptions.HTTPError: При ошибке HTTP-запроса
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"skus": stocks}
    url = endpoint_url + f"campaigns/{campaign_id}/offers/stocks"
    response = requests.put(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def update_price(prices, campaign_id, access_token):
    """Обновляет цены товаров для конкретной модели распространения.
    
    Отправляет новые цены через API Яндекс.Маркета.

    Args:
        prices (list): Список словарей с новыми ценами
        campaign_id (str): Идентификатор модели распространения
        access_token (str): Токен доступа к API Яндекс.Маркета

    Returns:
        dict: Ответ API после обновления цен
        
    Raises:
        requests.exceptions.HTTPError: При ошибке HTTP-запроса
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"offers": prices}
    url = endpoint_url + f"campaigns/{campaign_id}/offer-prices/updates"
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def get_offer_ids(campaign_id, market_token):
    """Получает артикулы всех товаров модели распространения на Маркете.
    
    Собирает полный список товаров, обрабатывая все страницы результатов.

    Args:
        campaign_id (str): Идентификатор модели распространения
        market_token (str): Токен доступа к API Яндекс.Маркета

    Returns:
        list: Список артикулов товаров (shopSku)
        
    Raises:
        requests.exceptions.HTTPError: При ошибке HTTP-запроса
    """
    page = ""
    product_list = []
    while True:
        some_prod = get_product_list(page, campaign_id, market_token)
        product_list.extend(some_prod.get("offerMappingEntries"))
        page = some_prod.get("paging").get("nextPageToken")
        if not page:
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer").get("shopSku"))
    return offer_ids


def create_stocks(watch_remnants, offer_ids, warehouse_id):
    """Формирует данные об остатках для обновления на Яндекс.Маркете.
    
    Создает два типа записей:
    1. Для товаров, присутствующих у поставщика
    2. Для товаров, отсутствующих у поставщика (остаток = 0)

    Args:
        watch_remnants (list): Данные об остатках от поставщика
        offer_ids (list): Список артикулов товаров на Маркете
        warehouse_id (int): Идентификатор склада в системе Маркета

    Returns:
        list: Список словарей в формате API Маркета
    """
    # Уберем то, что не загружено в market
    stocks = list()
    date = str(datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z")
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append(
                {
                    "sku": str(watch.get("Код")),
                    "warehouseId": warehouse_id,
                    "items": [
                        {
                            "count": stock,
                            "type": "FIT",
                            "updatedAt": date,
                        }
                    ],
                }
            )
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append(
            {
                "sku": offer_id,
                "warehouseId": warehouse_id,
                "items": [
                    {
                        "count": 0,
                        "type": "FIT",
                        "updatedAt": date,
                    }
                ],
            }
        )
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Формирует данные о ценах для обновления на Яндекс.Маркете.
    
    Создает записи только для товаров, присутствующих у поставщика.

    Args:
        watch_remnants (list): Данные об остатках от поставщика
        offer_ids (list): Список артикулов товаров на Маркете

    Returns:
        list: Список словарей в формате API Маркета
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "id": str(watch.get("Код")),
                # "feed": {"id": 0},
                "price": {
                    "value": int(price_conversion(watch.get("Цена"))),
                    # "discountBase": 0,
                    "currencyId": "RUR",
                    # "vat": 0,
                },
                # "marketSku": 0,
                # "shopSku": "string",
            }
            prices.append(price)
    return prices


async def upload_prices(watch_remnants, campaign_id, market_token):
    """Асинхронно обновляет цены товаров на Яндекс.Маркете.
    
    Выполняет:
    1. Получение списка артикулов
    2. Создание данных для обновления цен
    3. Пакетную отправку данных (пакеты по 500 товаров)

    Args:
        watch_remnants (list): Данные об остатках от поставщика
        campaign_id (str): Идентификатор модели распространения
        market_token (str): Токен доступа к API Яндекс.Маркета

    Returns:
        list: Список обновленных цен
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_prices in list(divide(prices, 500)):
        update_price(some_prices, campaign_id, market_token)
    return prices


async def upload_stocks(watch_remnants, campaign_id, market_token, warehouse_id):
    """Асинхронно обновляет остатки товаров на Яндекс.Маркете.
    
    Выполняет:
    1. Получение списка артикулов
    2. Создание данных об остатках
    3. Пакетную отправку данных (пакеты по 2000 товаров)

    Args:
        watch_remnants (list): Данные об остатках от поставщика
        campaign_id (str): Идентификатор модели распространения
        market_token (str): Токен доступа к API Яндекс.Маркета
        warehouse_id (int): Идентификатор склада

    Returns:
        tuple: Кортеж из двух списков:
            - Товары с ненулевым остатком
            - Все обработанные товары
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    stocks = create_stocks(watch_remnants, offer_ids, warehouse_id)
    for some_stock in list(divide(stocks, 2000)):
        update_stocks(some_stock, campaign_id, market_token)
    not_empty = list(
        filter(lambda stock: (stock.get("items")[0].get("count") != 0), stocks)
    )
    return not_empty, stocks


def main():
    env = Env()
    market_token = env.str("MARKET_TOKEN")
    campaign_fbs_id = env.str("FBS_ID")
    campaign_dbs_id = env.str("DBS_ID")
    warehouse_fbs_id = env.str("WAREHOUSE_FBS_ID")
    warehouse_dbs_id = env.str("WAREHOUSE_DBS_ID")

    watch_remnants = download_stock()
    try:
        # FBS
        offer_ids = get_offer_ids(campaign_fbs_id, market_token)
        # Обновить остатки FBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_fbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_fbs_id, market_token)
        # Поменять цены FBS
        upload_prices(watch_remnants, campaign_fbs_id, market_token)

        # DBS
        offer_ids = get_offer_ids(campaign_dbs_id, market_token)
        # Обновить остатки DBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_dbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_dbs_id, market_token)
        # Поменять цены DBS
        upload_prices(watch_remnants, campaign_dbs_id, market_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
