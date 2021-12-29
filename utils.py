import mysql.connector

username = password = dbname = "DbMysql21"


def connect_to_db():
    return mysql.connector.connect(
        host="localhost", user=username, password=password, db=dbname, port=3305
    )
