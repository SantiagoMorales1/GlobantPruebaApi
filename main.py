import uvicorn
from fastapi import FastAPI
from app.routes.routes import router
from app.services.service import get_conn_sql_service

app = FastAPI(title="MI App - Globant Prueba")

app.include_router(router)

if __name__ == "__main__":   
    #uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)
    get_conn_sql_service()
