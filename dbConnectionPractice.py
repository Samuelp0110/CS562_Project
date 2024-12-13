import os
import psycopg2
from dotenv import load_dotenv


def connect():

    load_dotenv()
    conn = psycopg2.connect(
        dbname=os.getenv('DBNAME'),
        user=os.getenv('USER'),
        password=os.getenv('PASSWORD'),
        host=os.getenv('HOST'),
    )
    cur = conn.cursor()

    cur.execute("SELECT * FROM sales")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    
    return rows

data = connect()
print("Fetched data:", data)