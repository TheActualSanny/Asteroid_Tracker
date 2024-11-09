'''Holds the class which manages all of the operations for the warehouse.'''
import os
import logging
import psycopg2
from dotenv import load_dotenv
from logger_conf import log_results

load_dotenv()

class ManageWarehouse:

    def __init__(self):
        self.table_name = os.getenv('WAREHOUSE_NAME')

    @log_results(logger_name = 'warehouse_connector')
    def connector(self):
        self._conn = psycopg2.connect(
            user = os.getenv('USER'),
            host = os.getenv('HOST'),
            password = os.getenv('PASS'),
            database = os.getenv('DB')
        )
        self._cur = self._conn.cursor()

    def disconnect(self):
        self._cur.close()
        self._conn.close()

    @log_results(logger_name = 'warehouse_table_creation')
    def create_table(self):
        self.connector()
        with self._conn:
            self._cur.execute(f'CREATE TABLE IF NOT EXISTS {os.getenv('WAREHOUSE_NAME')}' + ('''(
                              pos SERIAL PRIMARY KEY,
                              id INTEGER,
                              name TEXT,
                              diameter_min REAL,
                              diameter_max REAL,
                              hazardous BOOLEAN,
                              approach_date TEXT)''')) # This must not be hardcoded, will add const_queries module
        self.disconnect()

    @log_results(logger_name = 'warehouse_instance_inserter')
    def insert_instance(self, asteroid_inst):
        asteroid_data = asteroid_inst[0]
        with self._conn:
            self.cur.execute(f'INSERT INTO {self.table_name}(id, name, diameter_min, diameter_max, hazardous, approach_date)' + '''VALUES(%s, %s, %s,
                             %s, %s, %s)''', (asteroid_data['id'], asteroid_data['name'], asteroid_data['diameter_min_km'], 
                                              asteroid_data['diameter_max_km'], asteroid_data['hazardous'], asteroid_data['approach_date'])) # This also must be
            # written in a const_queries module.

    @log_results(logger_name = 'warehouse_asteroids_writer')
    def write_asteroids(self, asteroids):
        '''asteroids argument will be the return values of ManageAsteroids parse_asteroids() method.'''
        self.connector()
        self._cur.execute(f'SELECT * FROM {self.table_name}')
        written_data = self._cur.fetchall()
        for asteroid in asteroids[0]:
            if tuple(asteroid.values()) not in written_data:
                self.insert_instance(asteroid)
        self.disconnect()
        
        