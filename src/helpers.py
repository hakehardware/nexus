import os
import yaml
import datetime

from src.logger import logger

class Helpers:
    @staticmethod
    def read_yaml_file(file_path):
        logger.info(f'Opening config from {file_path}')
        if not os.path.exists(file_path):
            return None
        
        with open(file_path, 'r') as file:
            try:
                data = yaml.safe_load(file)
                return data
            except yaml.YAMLError as e:
                print(f"Error reading YAML file: {e}")
                return None
            
    @staticmethod
    def check_age_of_timestamp(timestamp):
        # Parse the UTC timestamp string to a datetime object
        utc_time = datetime.datetime.fromisoformat(timestamp.replace("Z", "+00:00"))

        # Get the current time in UTC
        now_utc = datetime.datetime.now(datetime.timezone.utc)

        # Convert UTC current time to user's local time
        user_timezone = datetime.datetime.now().astimezone().tzinfo
        local_now = now_utc.astimezone(user_timezone)

        # Convert UTC timestamp to user's local time
        local_timestamp = utc_time.astimezone(user_timezone)

        # Calculate the difference in minutes
        time_diff = local_now - local_timestamp
        minutes_diff = round(time_diff.total_seconds() / 60)

        return minutes_diff