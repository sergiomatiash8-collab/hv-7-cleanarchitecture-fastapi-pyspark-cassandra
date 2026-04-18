from gevent import monkey

monkey.patch_all()

from fastapi import FastAPI
from src.api.routes import router
from src.api.dependencies import init_dependencies


app = FastAPI(title="Amazon Reviews API (Clean Architecture)")

@app.on_event("startup")
def startup_event():
    """
    Application startup event handler.
    
    Responsibility: Ensure that all external resources (databases, cache) 
    are ready for operation before the server starts accepting incoming requests.
    """
    
    init_dependencies()


app.include_router(router)

if __name__ == "__main__":
    """
    Entry point for local execution and development.
    
    Uses uvicorn as the ASGI server. 
    Setting 0.0.0.0 allows the server to be accessible within Docker containers.
    """
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)