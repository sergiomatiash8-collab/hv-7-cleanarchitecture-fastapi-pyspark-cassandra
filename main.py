from gevent import monkey
monkey.patch_all()

import json
import redis
from fastapi import FastAPI
from cassandra.cluster import Cluster
from cassandra.io.geventreactor import GeventConnection

app = FastAPI(title="Amazon Reviews API with Redis")

# Ініціалізація Redis
cache = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

cluster = None
session = None

@app.on_event("startup")
def startup_event():
    global cluster, session
    try:
        cluster = Cluster(['127.0.0.1'], port=9042)
        cluster.connection_class = GeventConnection
        session = cluster.connect('amazon_reviews')
        print("✅ Підключено до Cassandra та Redis")
    except Exception as e:
        print(f"❌ Помилка старту: {e}")

@app.get("/reviews/product/{product_id}")
def get_reviews_by_product(product_id: str):
    cache_key = f"product:{product_id}"
    
    # 1. Перевіряємо кеш
    cached_data = cache.get(cache_key)
    if cached_data:
        print("🚀 Взято з Redis")
        return json.loads(cached_data)

    # 2. Якщо в кеші немає — йдемо в Cassandra
    query = "SELECT * FROM reviews_by_product WHERE product_id = %s"
    rows = session.execute(query, [product_id])
    
    results = []
    for row in rows:
        results.append({
            "review_id": row.review_id,
            "star_rating": row.star_rating,
            "review_date": str(row.review_date),
            "review_body": row.review_body
        })
    
    response = {"product_id": product_id, "count": len(results), "reviews": results}
    
    # 3. Записуємо в кеш на 60 секунд
    cache.setex(cache_key, 60, json.dumps(response))
    return response

@app.get("/reviews/product/{product_id}/rating/{rating}")
def get_reviews_by_product_and_rating(product_id: str, rating: int):
    cache_key = f"product:{product_id}:rating:{rating}"
    
    cached_data = cache.get(cache_key)
    if cached_data:
        print("🚀 Взято з Redis (фільтр)")
        return json.loads(cached_data)

    # ТУТ МАГІЯ БЕЗ ALLOW FILTERING:
    # Отримуємо всі відгуки продукту (це швидко, бо Partition Key)
    # І фільтруємо їх уже силами Python
    query = "SELECT * FROM reviews_by_product WHERE product_id = %s"
    rows = session.execute(query, [product_id])
    
    filtered_results = []
    for row in rows:
        if row.star_rating == rating:
            filtered_results.append({
                "review_id": row.review_id,
                "star_rating": row.star_rating,
                "review_date": str(row.review_date)
            })
    
    response = {"product_id": product_id, "rating": rating, "count": len(filtered_results), "reviews": filtered_results}
    cache.setex(cache_key, 60, json.dumps(response))
    return response

@app.get("/reviews/customer/{customer_id}")
def get_reviews_by_customer(customer_id: str):
    cache_key = f"customer:{customer_id}"
    
    cached_data = cache.get(cache_key)
    if cached_data:
        return json.loads(cached_data)

    query = "SELECT * FROM reviews_by_customer WHERE customer_id = %s"
    rows = session.execute(query, [customer_id])
    
    results = [
        {"review_id": r.review_id, "product_id": r.product_id, "star_rating": r.star_rating} 
        for r in rows
    ]
    
    response = {"customer_id": customer_id, "count": len(results), "reviews": results}
    cache.setex(cache_key, 60, json.dumps(response))
    return response