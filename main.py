from gevent import monkey
# КРИТИЧНО: Monkey patching має відбуватися до імпорту будь-яких інших бібліотек.
# Це дозволяє стандартним бібліотекам Python працювати в неблокуючому режимі з gevent.
monkey.patch_all()

from fastapi import FastAPI
from src.api.routes import router
from src.api.dependencies import init_dependencies

# Ініціалізація головного об'єкта FastAPI.
# Використання чіткого заголовка допомагає в самодокументації API (Swagger UI).
app = FastAPI(title="Amazon Reviews API (Clean Architecture)")

@app.on_event("startup")
def startup_event():
    """
    Обробник події запуску додатку.
    
    Відповідальність: Гарантувати, що всі зовнішні ресурси (бази даних, кеш) 
    готові до роботи до того, як сервер почне приймати вхідні запити.
    """
    # Виклик ініціалізації інфраструктурних залежностей (Cassandra, Redis).
    init_dependencies()

# Реєстрація маршрутів.
# Винесення логіки маршрутів в окремий файл (routes.py) дозволяє підтримувати 
# цей файл чистим та лаконічним навіть при розростанні системи.
app.include_router(router)

if __name__ == "__main__":
    """
    Точка входу для локального запуску та розробки.
    
    Використовує uvicorn як ASGI-сервер. 
    Налаштування 0.0.0.0 дозволяє серверу бути доступним всередині Docker-контейнерів.
    """
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)