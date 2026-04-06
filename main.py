import uvicorn
from fastapi import FastAPI
from src.presentation.routes.review_routes import router as review_router

#entrance

app = FastAPI(title="Review Service API")


app.include_router(review_router)


@app.get("/")
def read_root():
    return {"message": "Сервер працює! Перейдіть на /docs для тестування"}


if __name__ == "__main__":
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)