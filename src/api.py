import sqlite3
import json
import datetime

from src.logger import logger

class DatabaseAPI:
    def __init__(self, db_location):
        self.db_location = db_location
        self.conn = None

    def connect(self):
        # logger.info('Connecting to DB')
        self.conn = sqlite3.connect(self.db_location)

    def disconnect(self):
        # logger.info('Disconnecting from DB')
        if self.conn:
            self.conn.close()

    def initialize(self):
        logger.info(f'initializing DB at location: {self.db_location}')
        
        self.connect()
        cursor = self.conn.cursor()

        logger.info('initializing "events" Table')
        cursor.execute('''CREATE TABLE IF NOT EXISTS events (
                            event_datetime TEXT,
                            event_type TEXT,
                            event_data TEXT
                        )''')
        
        logger.info('initializing "farmer" Table')
        # piece_cache_status can be: SYNCRONIZING or COMPLETE
        cursor.execute('''CREATE TABLE IF NOT EXISTS farmer (
                    farmer_name TEXT,
                    piece_cache_status TEXT,
                    piece_cache_percent REAL,
                    workers INTEGER,
                    creation_datetime TEXT
                )''')
        
        logger.info('initializing "farms" Table')
        cursor.execute('''CREATE TABLE IF NOT EXISTS farms (
                            farm_id TEXT,
                            farm_index INTEGER,
                            public_key TEXT,
                            allocated_space_gib REAL,
                            directory TEXT,
                            status TEXT,
                            creation_datetime TEXT
                        )''')
        
        logger.info('initializing "rewards" Table')
        cursor.execute('''CREATE TABLE IF NOT EXISTS rewards (
                            farm_index TEXT,
                            reward_hash TEXT,
                            reward_result TEXT,
                            reward_datetime TEXT
                        )''')
        
        logger.info('Initializing "plots" Table')
        cursor.execute('''CREATE TABLE IF NOT EXISTS plots (
                            farm_index TEXT,
                            percentage REAL,
                            current_sector INTEGER,
                            replot INTEGER,
                            plot_datetime TEXT
                        )''')
        
        logger.info('Initializing "errors" Table')
        cursor.execute('''CREATE TABLE IF NOT EXISTS errors (
                            error_text TEXT,
                            error_datetime TEXT
                        )''')

        self.conn.commit()
        self.disconnect()

    def insert_event(self, event) -> bool:
        try:
            self.connect()

            event_datetime = event["Datetime"]
            event_type = event["Event Type"]
            event_data = event["Data"]

            cursor = self.conn.cursor()
            cursor.execute('SELECT COUNT(*) FROM events WHERE event_datetime = ? AND event_type = ?', (event_datetime, event_type))
            exists = cursor.fetchone()[0] > 0

            if not exists:
                logger.info(f'Found new event: {event_type}')
                cursor.execute('INSERT INTO Events (event_datetime, event_type, event_data) VALUES (?, ?, ?)', (event_datetime, event_type, json.dumps(event_data)))
                self.conn.commit()

                return True
            else:
                return False

        finally:
            self.disconnect()

    def insert_farmer(self, data) -> bool:
        try:
            self.connect()
            cursor = self.conn.cursor()

            farmer_name = data["Farmer Name"]

            self.conn.execute("BEGIN TRANSACTION")

            # Check if a row with the given farmer_name already exists
            cursor.execute("SELECT COUNT(*) FROM farmer WHERE farmer_name = ?", (farmer_name,))
            row_count = cursor.fetchone()[0]

            if row_count == 0:
                logger.info(f'{farmer_name} does not exist. removing any previous farmers and adding {farmer_name}.')

                # If no row with the given farmer_name exists, delete all rows from the table
                cursor.execute("DELETE FROM farmer")

                # Add a new row with the farmer_name
                cursor.execute("INSERT INTO farmer (farmer_name) VALUES (?)", (farmer_name,))

            else:
                logger.info(f'farmer {farmer_name} exists, no changes needed')

            self.conn.commit()

        except Exception as e:
            # Rollback the transaction if an error occurs
            logger.error(f'Error: {e}')
            self.conn.rollback()
            return False

        finally:
            self.disconnect()

        return True
    
    def insert_farm(self, data) -> bool:
        try:
            farm_id = data['Farm ID']
            logger.info(f'checking to see if {farm_id} exists')

            self.connect()
            cursor = self.conn.cursor()

            self.conn.execute("BEGIN TRANSACTION")

            # Check if a row with the given farm_id already exists
            cursor.execute("SELECT COUNT(*) FROM farms WHERE farm_id = ?", (farm_id,))
            row_count = cursor.fetchone()[0]

            if row_count == 0:
                logger.info(f'farm id {farm_id} does not exist. adding.')

                # Add a new row with the farm_id
                current_datetime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
                cursor.execute("INSERT INTO farms (farm_id, creation_datetime) VALUES (?, ?)", (farm_id, current_datetime))

            else:
                logger.info(f'{farm_id} exists, no changes needed.')

            self.conn.commit()

        except Exception as e:
            # Rollback the transaction if an error occurs
            logger.error(f'Error: {e}')
            self.conn.rollback()
            return False

        finally:
            self.disconnect()

        return True
    
    def insert_reward(self, data) -> bool:
        try:
            self.connect()
            cursor = self.conn.cursor()

            farm_index = data['Data']['Farm Index']
            hash = data['Data']['Hash']
            result = data['Event Type']
            reward_datetime = data['Datetime']

            logger.info(f'Farm Index {farm_index}: {result}')
            cursor.execute("""
                INSERT INTO rewards (farm_index, reward_hash, reward_result, reward_datetime)
                VALUES (?, ?, ?, ?)
            """, (farm_index, hash, result, reward_datetime))
            
            self.conn.commit()

        except Exception as e:
            # Rollback the transaction if an error occurs
            logger.error(f'Error: {e}')
            self.conn.rollback()
            return False
        
        finally:
            self.disconnect()

        return True
    
    def insert_plot(self, data) -> bool:
        try:
            self.connect()
            cursor = self.conn.cursor()

            farm_index = data['Data']['Farm Index']
            percentage_complete = data['Data']['Percentage Complete']
            current_sector = data['Data']['Current Sector']
            replot = data['Data']['Replot']
            plot_datetime = data['Datetime']

            logger.info(f'Farm Index {farm_index}:  {data["Event Type"]} {current_sector} @ {percentage_complete}% Complete')
            cursor.execute("""
                INSERT INTO plots (farm_index, percentage, current_sector, replot, plot_datetime)
                VALUES (?, ?, ?, ?, ?)
                """, (
                    farm_index,
                    percentage_complete, 
                    current_sector,
                    replot,
                    plot_datetime                
                ))

            self.conn.commit()

        except Exception as e:
            # Rollback the transaction if an error occurs
            logger.error(f'Error: {e}')
            self.conn.rollback()
            return False
        
        
        finally:
            self.disconnect()

        return True
    
    def insert_error(self, data) -> bool:
        try:
            self.connect()
            cursor = self.conn.cursor()

            error = data['Data']['Error']
            error_datetime = data['Datetime']

            logger.info(f'inserting error: {error}')
            cursor.execute("""
                INSERT INTO errors (error_text, error_datetime)
                VALUES (?, ?)
                """, (
                    error,
                    error_datetime                
                ))

            self.conn.commit()

        except Exception as e:
            # Rollback the transaction if an error occurs
            logger.error(f'Error: {e}')
            self.conn.rollback()
            return False
        
        
        finally:
            self.disconnect()

        return True
    
    def delete_farms(self, data) -> bool:
        try:
            self.connect()
            cursor = self.conn.cursor()
            
            logger.info(f'Cleaning farms table by removing any farm_ids not reported by metrics.')

            cursor.execute("SELECT COUNT(*) FROM farms WHERE farm_id NOT IN ({})".format(
                ','.join(['?'] * len(farm_ids))
            ), farm_ids)

            # Fetch the count of rows to be deleted
            rows_to_delete_count = cursor.fetchone()[0]
            logger.info(f'Deleting {rows_to_delete_count} old farms.')

            cursor.execute("DELETE FROM farms WHERE farm_id NOT IN ({})".format(
                ','.join(['?'] * len(farm_ids))
            ), farm_ids)

            self.conn.commit()


        except Exception as e:
            # Rollback the transaction if an error occurs
            logger.error(f'Error: {e}')
            self.conn.rollback()
            return False

        finally:
            self.disconnect()

        return True