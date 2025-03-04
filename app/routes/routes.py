from fastapi import APIRouter
from app.services.service import leer_csv_local, leer_csv_azure, get_conn_sql_service, Insertar_csv_azure, get_view_hired_employees_by_quarter_service, get_view_rank_employees_hired_by_mean_service

router = APIRouter()

@router.get("/get_csv_data_local/{file_name}")
def get_csv_data_local(file_name: str):
    return leer_csv_local(file_name)

@router.get("/get_csv_data_azure/{file_name}")
def get_csv_data_azure(file_name: str):
    return leer_csv_azure(file_name, 1)

@router.get("/get_conn_sql")
def get_conn_sql():
    return get_conn_sql_service(1)

@router.put("/put_insert_data/{file_name}")
def put_insert_data(file_name: str):
    return Insertar_csv_azure(file_name)

@router.get("/get_get_view_hired_employees_by_quarter")
def get_view_hired_employees_by_quarter():
    return get_view_hired_employees_by_quarter_service()

@router.get("/get_view_rank_employees_hired_by_mean")
def get_view_rank_employees_hired_by_mean():
    return get_view_rank_employees_hired_by_mean_service()


@router.get("/")
def read_root():
    return {"message": "FastAPI on Azure OK!"}