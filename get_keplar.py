'''Module which gets the Keplar (Orbital) Coordinates of every single asteroid.'''
import threading
import json
import requests
import os
from dotenv import load_dotenv
from logger_conf import log_results


lock = threading.Lock()
load_dotenv()

class KeplarManager:
    def __init__(self):
        self._params = {
            'User-Agent' : os.getenv('UserAgent'),
            'api_key' : os.getenv('API_KEY')
        }
        self._finalized = [] # The Threads will append the fetched data onto this list.
    
    @log_results(logger_name = 'coordinates_fetcher')
    def fetch_coordinates(self, data_link):
        data = requests.get(data_link, params = self._params)
        return data.content
    
    @log_results(logger_name = 'coordinates_parser')
    def parse_coordinates(self, mutex, asteroid_instance):
        '''Fetches the data from a single asteroid instance and parses it.'''

        returned_data = self.fetch_coordinates(asteroid_instance[1])
        loaded_data = json.loads(returned_data)
        semi = loaded_data['orbital_data']['semi_major_axis']
        eccen = loaded_data['orbital_data']['eccentricity']
        incl = loaded_data['orbital_data']['inclination']
        arg = loaded_data['orbital_data']['perihelion_argument']
        asc = loaded_data['orbital_data']['ascending_node_longitude']
        mean = loaded_data['orbital_data']['mean_anomaly']
        
        with mutex:
            self._finalized.append({**asteroid_instance[0], 'semi_major_axis' : semi, 'eccentricity' : eccen, 'inclination' : incl, 'perihelion_argument' : arg, 
                    'ascending_node_longitude' : asc, 'mean_anomaly' : mean})
    
    @log_results(logger_name = 'all_asteroids_parser')    
    def parse_all(self, asteroids):
        asteroid_threads = [threading.Thread(target = self.parse_coordinates, args = (lock, asteroid)) for asteroid in asteroids]

        for thread in asteroid_threads:
            thread.start()
        for thread in asteroid_threads:
            thread.join()
        return self._finalized


