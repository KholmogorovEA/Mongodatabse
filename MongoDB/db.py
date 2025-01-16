
# MongoDB реализует CRUD операции: 
    # insert
    # find
    # update
    # delete
        # каждая операция имеет два метода:
        #     one и many. one когда надо даостать одиночные данные документ словарик {"здесь условия"}
        #     many когда надо получить все данные документы то через список словариков [{"здесь условия"}, {}, {}]

# Агрегация
# aggregate(): Используется для обработки данных. Позволяет выполнять разные операции за раз.
# Индексация
# createIndex(): Создание индекса для поиска быстрого.
# getIndexes(): Получение списка всех индексов.

# на шардирование не хватило сил) Однако что это и зачем понимание есть.


#RUN MongoDB
#https://www.mongodb.com/try/download/community     -  скачать distr.
#Установить в системные переменные путь до exe файла
# pip install pymongo 

from pymongo import MongoClient
from datetime import datetime
# создаем коннект к базе
client = MongoClient('mongodb://localhost:27017/')
db = client['appDB']
collection = db['appCollection']



# пример мгазин
"""
необходимо проанализировать данные о покупках, 
чтобы найти самые популярные товары для каждого месяца. 
База данных содержит коллекцию sales, где каждый документ представляет собой покупку, 
включая поля product_id, quantity, и date
"""
# Пример данных для вставки в коллекцию sales из трех пар: продукт, кол-во и даты
from datetime import datetime

# Пример данных для вставки в коллекцию sales с преобразованием строки в дату
sales_data = [
    {"product": "Обувь", "quantity": 5, "date": datetime(2025, 1, 5, 10, 30)},
    {"product": "Штаны", "quantity": 3, "date": datetime(2025, 1, 15, 11, 0)},
    {"product": "Кофта", "quantity": 8, "date": datetime(2025, 1, 20, 12, 15)},
    {"product": "Обувь", "quantity": 2, "date": datetime(2025, 2, 10, 14, 30)},
    {"product": "Штаны", "quantity": 4, "date": datetime(2025, 2, 15, 15, 0)},
    {"product": "Кофта", "quantity": 6, "date": datetime(2025, 2, 20, 16, 45)},
    {"product": "Обувь", "quantity": 7, "date": datetime(2025, 3, 5, 9, 0)},
    {"product": "Штаны", "quantity": 5, "date": datetime(2025, 3, 10, 10, 30)},
    {"product": "Кофта", "quantity": 3, "date": datetime(2025, 3, 25, 11, 15)},
    {"product": "Футболка", "quantity": 6, "date": datetime(2025, 4, 1, 13, 0)},
    {"product": "пылесос", "quantity": 2, "date": datetime(2025, 4, 10, 14, 30)},
    {"product": "Костюм", "quantity": 9, "date": datetime(2025, 4, 15, 15, 45)}
]


# так как данных много выбираем метод many
collection.insert_many(sales_data)





pipeline = [
    # Преобразуем строку в дату
    {
        "$addFields": {
            "date": {"$toDate": "$date"}
        }
    },
    {
        "$group": {
            "_id": {
                "year": {"$year": "$date"},
                "month": {"$month": "$date"},
                "product_id": "$product_id"
            },
            "total_quantity": {"$sum": "$quantity"}
        }
    },
    {
        "$sort": {
            "_id.year": 1,
            "_id.month": 1,
            "total_quantity": -1  # Сортировка по убыванию количества
        }
    },
    {
        "$group": {
            "_id": {
                "year": "$_id.year",
                "month": "$_id.month"
            },
            "top_product": {
                "$first": {
                    "product_id": "$_id.product_id",
                    "total_quantity": "$total_quantity"
                }
            }
        }
    },
    {
        "$project": {
            "year": "$_id.year",
            "month": "$_id.month",
            "product_id": "$top_product.product_id",
            "total_quantity": "$top_product.total_quantity",
            "_id": 0
        }
    }
]



# агрегируем 
result = collection.aggregate(pipeline)
for r in result:
    print(r)


"""
вывод в терминале:
{'year': 2025, 'month': 1, 'product_id': 'Кофта', 'total_quantity': 48}
{'year': 2025, 'month': 2, 'product_id': 'Кофта', 'total_quantity': 36}
{'year': 2025, 'month': 3, 'product_id': 'Обувь', 'total_quantity': 42}
{'year': 2025, 'month': 4, 'product_id': 'Костюм', 'total_quantity': 54}

"""

# На backend можно вот так юзать mongo:
"""
app.post("/add_sale/")
def add_sale(product_id: str, quantity: int, date: str):
    sale_data = {
        "product_id": product_id,
        "quantity": quantity,
        "date": datetime.fromisoformat(date)
    }
    collection.insert_one(sale_data)
    return {"message": "Sale added successfully"}

@app.get("/sales/")
def get_sales():
    sales = list(collection.find())
    return {"sales": sales}
"""

