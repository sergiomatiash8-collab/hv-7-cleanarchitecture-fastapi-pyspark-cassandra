from fastapi import FastAPI
from src.api.routes import router           # Додано src.
from src.api.dependencies import init_dependencies  # Додано src.

app = FastAPI(title="Amazon Reviews Analytics API")

@app.on_event("startup")
def startup_event():
    init_dependencies()

app.include_router(router)

@app.get("/")
def root():
    return {"status": "online"}