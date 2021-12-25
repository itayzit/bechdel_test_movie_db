import mysql.connector

username = password = dbname = "DbMysql13"

def connect_to_db():
    return mysql.connector.connect(
        host="127.0.0.1", user=username, password=password, db=dbname, port=3305
    )
