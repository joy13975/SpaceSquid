
import os
import sqlite3
from datetime import datetime
from contextlib import contextmanager


class ProcessRegistryError(Exception):
    pass


@contextmanager
def BackgroundState(process_name):
    print(f'Background state: {process_name} waiting for DB...')
    bs = BackgroundStateDB()
    with bs.connection:  # lock table
        print(f'Background state: {process_name} entered DB')
        c = bs.cursor
        c.execute(f'SELECT * FROM {bs.table_name} WHERE process_name = "{process_name}"')
        sel_res = c.fetchall()
        if len(sel_res) > 0:
            raise ProcessRegistryError(f'Already registered: {sel_res}')
        time_now_str = datetime.now().isoformat()
        c.execute(
            f'INSERT INTO {bs.table_name} (process_name, start_time) VALUES ("{process_name}", "{time_now_str}")')
        bs.connection.commit()
        yield
        c.execute(f'DELETE FROM {bs.table_name} WHERE process_name = "{process_name}"')
        bs.connection.commit()
    print(f'Background state: {process_name} exited DB')


class BackgroundStateDB:
    filename = 'bg_state.sqlite3'
    table_name = 'background_state'

    def __init__(self):
        self._connection = None
        self._cursor = None

    @property
    def connection(self):
        self.init_db()
        return self._connection

    @property
    def cursor(self):
        self.init_db()
        return self._cursor

    def destroy_db(self):
        if os.path.isfile(self.filename):
            os.remove(self.filename)

    def init_db(self):
        if self._connection is not None:
            return
        conn = sqlite3.connect(self.filename)
        cursor = conn.cursor()
        cursor.execute(f'CREATE TABLE IF NOT EXISTS {self.table_name}(process_name TEXT, start_time TIMESTAMP)')
        conn.commit()
        self._connection = conn
        self._cursor = cursor
    
    def list_processes(self):
        c = self.cursor
        c.execute(f'SELECT * FROM {self.table_name}')
        return c.fetchall()