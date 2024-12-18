'''Module which contains the class that manages the normalized data insertion.'''

import psycopg2
import os
from dotenv import load_dotenv
from logger_conf import log_results
from const_queries import insert_query, create_query

load_dotenv()

class ManageNormalized:
    def __init__(self):
        self.table_name = os.getenv('NORMALIZED_NAME')
        self.updated_count = 0
        self.inserted_count = 0

    @log_results(logger_name = 'warehouse_connector')
    def connector(self):
        self._conn = psycopg2.connect(
            host = os.getenv('HOST'),
            user = os.getenv('USER'),
            password = os.getenv('PASS'),
            database = os.getenv('DB')
        )
        self._cur = self._conn.cursor()

    def disconnect(self):
        self._cur.close()
        self._conn.close()
    
    @log_results(logger_name = 'normalized_table_creation')
    def create_table(self):
        with self._conn:
            self._cur.execute(create_query(self.table_name, pos = 'SERIAL PRIMARY KEY', id = 'INTEGER', name = 'TEXT', diameter_min = 'REAL', diameter_max = 'REAL', 
                                           hazardous = 'BOOLEAN', approach_date = 'TEXT', semi_major_axis = 'REAL', eccentricity = 'REAL', inclination = 'REAL', perihelion_argument = 'REAL',
                                           ascending_node_longitude = 'REAL', mean_anomaly = 'REAL'))
    
    @log_results(logger_name = 'normalized_asteroid_inserter')
    def insert_asteroid(self, asteroid):
        with self._conn:
            self._cur.execute(insert_query(self.table_name, 'id', 'name', 'diameter_min', 'diameter_max', 'hazardous', 'approach_date', 'semi_major_axis',
                                           'eccentricity', 'inclination', 'perihelion_argument', 'ascending_node_longitude', 'mean_anomaly'), tuple(asteroid.values()))
    
    @log_results(logger_name = 'asteroid_update')
    def update_asteroid(self, asteroid):
        with self._conn:
            self._cur.execute(f'''UPDATE {self.table_name} SET approach_date = {repr(asteroid['approach_date'])}, semi_major_axis = {asteroid['semi_major_axis']},
                               eccentricity = {asteroid['eccentricity']}, inclination = {asteroid['inclination']}, perihelion_argument = {asteroid['perihelion_argument']},
                               ascending_node_longitude = {asteroid['ascending_node_longitude']}, mean_anomaly = {asteroid['mean_anomaly']} WHERE id = {asteroid['id']}''')

    def write_asteroids(self, asteroids):
        self.connector()
        self.create_table()
        self._cur.execute(f'SELECT * FROM {self.table_name}')
        table_data = (asteroid[1] for asteroid in self._cur.fetchall())

        for asteroid in asteroids:
            if int(asteroid['id']) in table_data:
                self.update_asteroid(asteroid)
                pass
            else:
                self.insert_asteroid(asteroid)

        self.disconnect()    
       