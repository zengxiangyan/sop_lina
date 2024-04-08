from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pandas as pd

class CDAO:
    debug = True
    engine = None
    Session = None
    session = None
    host = '127.0.0.1'
    port = 9000
    user = 'default'
    passwd = ''
    db = ''
    charset = 'utf8'
    try_count = 5

    def print(self, *args, **kwargs):
        if self.debug:
            print(*args, **kwargs)

    def __init__(self, host, port, user, passwd, db):
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        self.db = db
        self.connect()

    def connect(self):
        self.print("Connect clickhouse", self.host, self.db)
        connection_string = f'clickhouse://{self.user}:{self.passwd}@{self.host}:{self.port}/{self.db}'
        self.engine = create_engine(connection_string, pool_size=100, pool_recycle=3600, pool_timeout=20)
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()
        return self.session

    def close(self):
        self.session.close()

    def query_all(self, sql, param_tuple=None, as_dict=False, print_sql=True):
        try_count = 0
        r = None
        while try_count < self.try_count:
            try:
                if try_count > 0:
                    self.connect()
                if print_sql:
                    self.print(sql)
                result_proxy = self.session.execute(sql, param_tuple)
                if as_dict:
                    result = [dict(row) for row in result_proxy.fetchall()]
                else:
                    result = result_proxy.fetchall()
                if try_count > 0:
                    self.print('successed at try_count:' + str(try_count))
                break
            except (EOFError, AttributeError, OSError) as e:
                self.print('failed at try_count:' + str(try_count) + ' error:', e)
                try_count += 1
                if try_count >= self.try_count:
                    raise e
                self.close()
                time.sleep(1)
        return result

    def batch_insert(self, sql_main, val_list, sql_dup_update='', batch=1000):
        # SQLAlchemy does not have a direct batch insert method like clickhouse_driver.
        # You need to use session.add_all() with ORM or execute many INSERT statements.
        # This is a simple example of how to execute many INSERT statements.
        for i in range(0, len(val_list), batch):
            batch_values = val_list[i:i+batch]
            batch_sql = sql_main + ','.join(str(tuple(val)) for val in batch_values) + sql_dup_update
            self.session.execute(batch_sql)
        self.session.commit()