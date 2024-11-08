'''Contains the function which calculates the local time, which will be passed as parameters to the Asteroids API.'''
import time

def get_time(day_count = 0): # For now this only subtracts days which means that this is incosistent and will cause exceptions. Will fix
    currtime_struct = time.localtime()
    curr_day = currtime_struct.tm_mday
    curr_month = currtime_struct.tm_mon
    curr_year = currtime_struct.tm_year
    endtime_formatted = f'{curr_year}-{curr_month}-{curr_day}'
    starttime_formatted = f'{curr_year}-{curr_month}-{curr_day - day_count}'

    return (starttime_formatted, endtime_formatted)
