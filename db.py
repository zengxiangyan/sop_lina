# -*- coding: utf-8 -*-
from CDAO import CDAO
import config
import pandas as pd
from clickhouse_sqlalchemy import make_session
from sqlalchemy import create_engine

def get_clickhouse(conn_name):
    try:
        param = config.CLICKHOUSE[conn_name]
        conn = CDAO(param['host'], param['port'], param['user'], param['password'], param['name'])
        return conn
    except Exception as e:
        print(e)
        raise

def connect_clickhouse(conn_name):
    db = get_clickhouse(conn_name)
    db.connect()
    return db

def query_all(db,sql):

    conf = {'chmaster2':{
        "user": "",
        "password": "",
        "server_host": "10.21.90.15",
        "port": "28066",
        "db": ""
    }}
    connection = 'clickhouse://{user}:{password}@{server_host}:{port}/{db}'.format(**conf[db])
    engine = create_engine(connection, pool_size=100, pool_recycle=3600, pool_timeout=20)
    session = make_session(engine)
    cursor = session.execute(sql)
    try:
        fields = cursor._metadata.keys
        print(cursor.fetchall())
        mydata = pd.DataFrame([dict(zip(fields, item)) for item in cursor.fetchall()])
        return mydata
    finally:
        cursor.close()
        session.close()
