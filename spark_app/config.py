import os
from dotenv import load_dotenv

def get_config() -> dict:
    load_dotenv()
    host = os.getenv("PG_HOST")
    user = os.getenv("PG_USER")
    password = os.getenv("PG_PASSWORD")
    db_name = os.getenv("PG_DB")
    port = os.getenv("PG_HOST_PORT")
    url = f"jdbc:postgresql://{host}:{port}/{db_name}"
    res = {
        "host" : host,
        "user" : user,
        "password" : password,
        "db_name" : db_name,
        "port" : port,
        "db_url" : url
    }
    return res