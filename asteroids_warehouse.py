'''Holds the class which manages all of the operations for the warehouse.'''
import os
import logging
import psycopg2
from dotenv import load_dotenv
from logger_conf import log_results
from fetch_asteroids import ManageAsteroids
from get_keplar import KeplarManager
from write_coordinates import ManageNormalized
from const_queries import insert_query, create_query

load_dotenv()

class ManageWarehouse:

    def __init__(self):
        self.table_name = os.getenv('TABLE_NAME')

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
            self._cur.execute(create_query(self.table_name, pos = 'SERIAL PRIMARY KEY', id = 'INTEGER', name = 'TEXT', diameter_min = 'REAL', diameter_max = 'REAL', 
                                           hazardous = 'BOOLEAN', approach_date = 'TEXT'))
        self.disconnect()

    @log_results(logger_name = 'warehouse_instance_inserter')
    def insert_instance(self, asteroid_inst):
        asteroid_data = asteroid_inst[0]
        
        with self._conn:
            self._cur.execute(insert_query(self.table_name,'id', 'name', 'diameter_min', 'diameter_max', 'hazardous', 'approach_date'), (asteroid_data['id'], asteroid_data['name'], asteroid_data['diameter_min_km'], 
                                              asteroid_data['diameter_max_km'], asteroid_data['hazardous'], asteroid_data['approach_date'])) 
            
    @log_results(logger_name = 'warehouse_asteroids_writer')
    def write_asteroids(self, asteroids):
        '''asteroids argument will be the return values of ManageAsteroids parse_asteroids() method.'''
        self.connector()
        self._cur.execute(f'SELECT * FROM {self.table_name}')
        written_data = self._cur.fetchall()
        for asteroid in asteroids:
            if tuple(asteroid[0].values()) not in written_data:
                self.insert_instance(asteroid)
        self.disconnect()
        