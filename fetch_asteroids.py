'''Module which contains the manager class of the whole asteroid data fetching/parsing process.'''

import requests
import json
import os
import logging
from dotenv import load_dotenv
from logger_conf import log_results
from time_calculations import get_time

load_dotenv()

class ManageAsteroids:
    def __init__(self):
        dates = get_time(int(os.getenv('DayDifference')))
        self._params = {
            'User-Agent' : os.getenv('UserAgent'),
            'api_key' : os.getenv('API_KEY'),
            'start_date' : dates[0],
            'end_date' : dates[1]
        }
        self.url = os.getenv('ASTEROIDS_API') # Must make it possible to enter the start and end dates in the .env file
    
    @log_results(logger_name = 'asteroids_fetcher')
    def fetch_asteroids(self):
        '''Fetches the Asteroids JSON'''
        res = requests.get(self.url, params = self._params)
        return res.content
    
    @log_results(logger_name = 'json_log')
    def save_json(self, data):
        with open('asteroids.json', 'r') as file:
            initial = json.load(file)

        if data not in initial['asteroids']:
                initial['asteroids'].append(data)
                with open('asteroids.json', 'w') as file:
                    json.dump(initial, file, indent = 6)

    @log_results(logger_name = 'datebatch_parser')
    def parse_asteroids(self, date_batch):
        found_asteroids = []
        
        for asteroid in date_batch:
            name = asteroid['name']
            diameter_min = asteroid['estimated_diameter']['kilometers']['estimated_diameter_min']
            diameter_max = asteroid['estimated_diameter']['kilometers']['estimated_diameter_max']
            is_hazardous = asteroid['is_potentially_hazardous_asteroid']
            approach_date = asteroid['close_approach_data'][0]['close_approach_date_full']
            data_link = asteroid['links']['self']
            asteroid_instance = {'name' : name, 'diameter_min_km' : diameter_min, 'diameter_max_km' : diameter_max, 'hazardous' : is_hazardous,
                                'approach_date' : approach_date}
            self.save_json(asteroid_instance)
            found_asteroids.append((asteroid_instance,  data_link))
        return found_asteroids
    
    @log_results(logger_name = 'main_asteroids_parser')
    def parse_json(self):
        initial_data = self.fetch_asteroids()
        data = json.loads(initial_data)
        finalized_asteroids = []
        for date in data['near_earth_objects']:
            finalized_asteroids.append(self.parse_asteroids(data['near_earth_objects'][date]))
        return finalized_asteroids
    

manager = ManageAsteroids()
manager.parse_json()

# print(type(os.getenv('DayDifference')))

