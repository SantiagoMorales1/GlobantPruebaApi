from decouple import config 

STORAGE_ACCOUNT = config("STORAGE_ACCOUNT", default=None)
CONTAINER = config("CONTAINER", default=None)

SERVER = config("SERVER", default=None)
DATABASE = config("DATABASE", default=None)
DRIVER = config("DRIVER", default=None)
TENANT_ID = config("TENANT_ID", default=None)
CLIENT_ID = config("CLIENT_ID", default=None)
CLIENT_SECRET = config("CLIENT_SECRET", default=None)
UID = config("UID", default=None)
PWD = config("PWD", default=None)
CONN_STR = config("CONN_STR", default=None)
ENV = config("ENV", default=None)