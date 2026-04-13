from gevent import monkey
monkey.patch_all()

from fastapi import FastAPI
from src.api.routes import router
from src.api.dependencies import init_dependencies

app = FastAPI(title="Amazon Reviews API (Clean Architecture)")

@app.on_event("startup")
def startup_event():
    init_dependencies()

app.include_router(router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)