import sqlite3
import json
import datetime

from src.logger import logger
from src.helpers import Helpers
from src.constants import keys

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

        logger.info('initializing "farmers" Table')
        cursor.execute('''CREATE TABLE IF NOT EXISTS farmers (
                            farmer_name TEXT,
                            farmer_piece_cache_status TEXT,
                            farmer_piece_cache_percent REAL,
                            farmer_workers INTEGER,
                            creation_datetime DATETIME
                        )''')
        
        logger.info('initializing "farms" Table')
        cursor.execute('''CREATE TABLE IF NOT EXISTS farms (
                            farm_id TEXT,
                            farmer_name TEXT,
                            farm_index INTEGER,
                            farm_public_key TEXT,
                            farm_allocated_space_gib REAL,
                            farm_directory TEXT,
                            farm_status TEXT,
                            creation_datetime DATETIME
                        )''')
        
        logger.info('initializing "events" Table')
        cursor.execute('''CREATE TABLE IF NOT EXISTS events (
                            event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                            farmer_name TEXT,
                            event_type TEXT,
                            event_data TEXT,
                            event_datetime DATETIME
                        )''')
    
        logger.info('Initializing "plots" Table')
        cursor.execute('''CREATE TABLE IF NOT EXISTS plots (
                            plot_id INTEGER PRIMARY KEY AUTOINCREMENT,
                            farmer_name TEXT,
                            farm_index TEXT,
                            plot_percentage REAL,
                            plot_current_sector INTEGER,
                            plot_type INTEGER,
                            plot_datetime DATETIME
                        )''')
        
        logger.info('initializing "rewards" Table')
        cursor.execute('''CREATE TABLE IF NOT EXISTS rewards (
                            reward_id INTEGER PRIMARY KEY AUTOINCREMENT,
                            farmer_name TEXT,
                            farm_index TEXT,
                            reward_hash TEXT,
                            reward_result TEXT,
                            reward_datetime DATETIME
                        )''')
        
        logger.info('Initializing "errors" Table')
        cursor.execute('''CREATE TABLE IF NOT EXISTS errors (
                            error_id INTEGER PRIMARY KEY AUTOINCREMENT,
                            farmer_name TEXT,
                            error TEXT,
                            error_datetime DATETIME
                        )''')

        self.conn.commit()
        self.disconnect()

    # ===== INSERT
    # Done
    def insert_farmer(self, data):
        try:
            self.connect()
            cursor = self.conn.cursor()

            farmer_name = data.get('Farmer Name')

            if not farmer_name:
                logger.warn('No Farmer Name Provided')
                response = {
                    'Success': False,
                    'Message': 'No Farmer Name Provided'
                }
                return response

            self.conn.execute("BEGIN TRANSACTION")

            # Check if a row with the given farmer_name already exists
            cursor.execute("SELECT COUNT(*) FROM farmers WHERE farmer_name = ?", (farmer_name,))
            row_count = cursor.fetchone()[0]

            if row_count == 0:
                # Add a new row with the farmer_name
                current_datetime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
                cursor.execute("INSERT INTO farmers (farmer_name, creation_datetime) VALUES (?, ?)", (farmer_name, current_datetime))
                logger.info(f"{farmer_name} added to database")
                response = {
                    'Success': True,
                    'Message': f"{farmer_name} Added to Database"
                }
                self.conn.commit()

            else:
                logger.info(f'Farmer {farmer_name} exists, no changes needed')
                response = {
                    'Success': True,
                    'Message': f"Farmer {farmer_name} exists, no changes needed"
                }

            

        except Exception as e:
            # Rollback the transaction if an error occurs
            logger.error(f'Error inserting farmer: {e}')
            self.conn.rollback()
            response = {
                "Success": False,
                'Message': f'Error inserting farmer: {e}'
            }

        finally:
            self.disconnect()

        return response

    def insert_farm(self, data):
        try:
            farm_id = data['Data']['Farm ID']
            farm_index = data['Data']['Farm Index']
            farmer_name = data['Data']['Farmer Name']

            if not farm_id or not farm_index or not farmer_name:
                message = "Farm ID, Farmer Name, and Farm Index is required"
                logger.warn(message)
                return {
                    'Success': False,
                    'Message': message
                }
            
            self.connect()
            cursor = self.conn.cursor()
            
            cursor.execute("SELECT * FROM farms WHERE (farm_id = ? OR farm_index = ?) and farmer_name = ?", (farm_id, farm_index, farmer_name))
            farms = cursor.fetchall()

            insert = True
            if len(farms) > 0:
                zipped_farms = [dict(zip(keys['Farm'], row)) for row in farms]

                for farm in zipped_farms:
                    if farm['Farm ID'] == farm_id and farm['Farm Index'] == farm_index:
                        insert = False
                        message = 'Complete match exists, no need to insert'
                        logger.info(message)
                        response = {
                            "Success": True,
                            "Message": message
                        }
                    else:
                        logger.info('Conflict found, removing conflict')
                        cursor.execute("DELETE FROM farms WHERE farm_id = ? AND farm_index = ?", (farm['Farm ID'], farm['Farm Index']))

            if insert:
                current_datetime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
                cursor.execute("INSERT INTO farms (farm_id, farm_index, farmer_name, creation_datetime) VALUES (?, ?, ?, ?)", (farm_id, farm_index, farmer_name, current_datetime))
                self.conn.commit()

                message = f"Inserted new farm with id of {farm_id}, farmer_name of {farmer_name}, and index of {farm_index}"
                logger.info(message)

                response = {
                    "Success": True,
                    "Message": message
                }
            
        except Exception as e:
            # Rollback the transaction if an error occurs
            logger.error(f'Error inserting farm: {e}')
            self.conn.rollback()
            response = {
                "Success": False,
                'Message': f'Error inserting farm: {e}'
            }

        finally:
            self.disconnect()

        return response
    
    def insert_event(self, data):
        try:
            # Connect to DB
            self.connect()
            cursor = self.conn.cursor()

            # Get data from event
            event_datetime = data.get("Datetime")
            event_type = data.get("Event Type")
            event_data = data.get("Data")
            farmer_name = data.get('Farmer Name')

            logger.info(data)

            # Validate that all data points are there
            if not all([event_datetime, event_type, event_data, farmer_name]):
                logger.warn('Missing Farmer Name Event Datetime, Event Type, or Event Data')
                response = {
                    "Success": False,
                    "Message": "Missing Farmer Name Event Datetime, Event Type, or Event Data"
                }
                return response
            
            # Validate that the datetime is in the correct format
            if not Helpers.validate_date(event_datetime):
                logger.warn('Invalid Datetime Received')
                response = {
                    "Success": False,
                    "Message": "Invalid Datetime"
                }
                return response
            

            logger.info('Validation Passed')

            # Check if the event already exists
            cursor.execute('SELECT COUNT(*) FROM events WHERE event_datetime = ? AND event_type = ? AND event_data = ? AND farmer_name = ?',
                        (event_datetime, event_type, json.dumps(event_data), farmer_name))
            exists = cursor.fetchone()[0] > 0

            if not exists:
                # Insert the event into the database
                cursor.execute('INSERT INTO Events (event_datetime, event_type, event_data, farmer_name) VALUES (?, ?, ?, ?)',
                            (event_datetime, event_type, json.dumps(event_data), farmer_name))
                self.conn.commit()
                response = {
                    "Success": True,
                    "Message": "Successfully inserted event"
                }

            else:
                response = {
                    "Success": True,
                    "Message": "Event already exists"
                }

            return response
                
        except Exception as e:
            # Rollback the transaction if an error occurs
            logger.error(f'Error inserting event: {e}')
            self.conn.rollback()
            response = {
                "Success": False,
                'Message': f'Error inserting event: {e}'
            }

        finally:
            self.disconnect()

    def insert_plot(self, data):
        try:
            # Connect to DB
            self.connect()
            cursor = self.conn.cursor()

            # Get data from plot
            farmer_name = data.get("Farmer Name")
            farm_index = data.get("Data", {}).get("Farm Index")
            plot_percentage = data.get("Data", {}).get("Plot Percentage")
            plot_current_sector = data.get("Data", {}).get('Plot Current Sector')
            plot_type = data.get("Data", {}).get('Plot Type')
            plot_datetime = data.get('Datetime')

            logger.info(data)

            # Validate that all data points are there
            if not all([farmer_name, farm_index, plot_percentage, plot_current_sector, plot_type, plot_datetime]):
                logger.warn('Missing Reequired Fields')
                response = {
                    "Success": False,
                    "Message": "Missing Reequired Fields"
                }
                return response
            
            # Validate that the datetime is in the correct format
            if not Helpers.validate_date(plot_datetime):
                logger.warn('Invalid Datetime Received')
                response = {
                    "Success": False,
                    "Message": "Invalid Datetime"
                }
                return response
            
            logger.info('Validation Passed')

            # Insert the event into the database
            cursor.execute('INSERT INTO plots (farmer_name, farm_index, plot_percentage, plot_current_sector, plot_type, plot_datetime) VALUES (?, ?, ?, ?, ?, ?)',
                        (farmer_name, farm_index, plot_percentage, plot_current_sector, plot_type, plot_datetime))
            self.conn.commit()
            response = {
                "Success": True,
                "Message": "Successfully inserted plot"
            }

        except Exception as e:
            # Rollback the transaction if an error occurs
            logger.error(f'Error inserting event: {e}')
            self.conn.rollback()
            response = {
                "Success": False,
                'Message': f'Error inserting event: {e}'
            }

        finally:
            self.disconnect()

        return response

    def insert_reward(self, data):
        try:
            # Connect to DB
            self.connect()
            cursor = self.conn.cursor()

        except Exception as e:
            # Rollback the transaction if an error occurs
            logger.error(f'Error inserting event: {e}')
            self.conn.rollback()
            response = {
                "Success": False,
                'Message': f'Error inserting event: {e}'
            }

        finally:
            self.disconnect()

    def insert_error(self, data):
        try:
            # Connect to DB
            self.connect()
            cursor = self.conn.cursor()

        except Exception as e:
            # Rollback the transaction if an error occurs
            logger.error(f'Error inserting event: {e}')
            self.conn.rollback()
            response = {
                "Success": False,
                'Message': f'Error inserting event: {e}'
            }

        finally:
            self.disconnect()

    # ===== GET
    def get_farmers(self, data):
        try:
            self.connect()
            cursor = self.conn.cursor()
            page = data['Page']
            limit = data['Limit']

            offset = (page - 1) * limit
            cursor.execute("SELECT COUNT(*) FROM farmers")
            total_items = cursor.fetchone()[0]

            cursor.execute("SELECT * FROM farmers ORDER BY creation_datetime DESC LIMIT ? OFFSET ?", (limit, offset))            
            farmers = cursor.fetchall()

            zipped_farmers = [dict(zip(keys['Farmer'], row)) for row in farmers]

        except Exception as e:
            logger.error(f'Error getting farmers: {e}')
            response = {
                "Success": False,
                "Message": f"Error getting farmers: {e}"
            }

        finally:
            self.disconnect()

        response = {
            "Success": True,
            "Data": {
                "Farmers": zipped_farmers,
                "Total Items": total_items
            }
        }
        return response
    
    def get_farms(self, data):
        try:
            self.connect()
            cursor = self.conn.cursor()
            page = data['Page']
            limit = data['Limit']

            offset = (page - 1) * limit
            cursor.execute("SELECT COUNT(*) FROM farms")
            total_items = cursor.fetchone()[0]

            cursor.execute("SELECT * FROM farms ORDER BY creation_datetime DESC LIMIT ? OFFSET ?", (limit, offset))            
            farms = cursor.fetchall()

            zipped_farms = [dict(zip(keys['Farm'], row)) for row in farms]

        except Exception as e:
            logger.error(f'Error getting farmers: {e}')
            response = {
                "Success": False,
                "Message": f"Error getting farmers: {e}"
            }

        finally:
            self.disconnect()

        response = {
            "Success": True,
            "Data": {
                "Farms": zipped_farms,
                "Total Items": total_items
            }
        }
        return response
    
    def get_events(self, data):
        try:
            self.connect()
            cursor = self.conn.cursor()

            # Construct the SQL query
            sql = "SELECT * FROM events WHERE"

            conditions = " event_datetime BETWEEN ? AND ?"
            params = [data['Start Time'], data['End Time']]

            # Add optional filters if provided
            if data['Event Type']:
                conditions += " AND event_type = ?"
                params.append(data['Event Type'])
            if data['Farmer Name']:
                conditions += " AND farmer_name = ?"
                params.append(data['Farmer Name'])

            sql += conditions

            # Add pagination
            offset = (data['Page'] - 1) * data['Limit']
            sql += " LIMIT ? OFFSET ?"
            params.extend([data['Limit'], offset])

            logger.info(f"Executing: {sql}")
            logger.info(params)

            cursor.execute(sql, params)
            events = cursor.fetchall()

            zipped_events = [dict(zip(keys['Event'], row)) for row in events]

            # Execute the count query
            count_sql = "SELECT COUNT(*) FROM events WHERE" + conditions
            logger.info(f"Executing: {count_sql}")
            cursor.execute(count_sql, params[:-2])  # Exclude LIMIT and OFFSET
            total_items = cursor.fetchone()[0]

        except Exception as e:
            logger.error(f'Error getting farmers: {e}')
            response = {
                "Success": False,
                "Message": f"Error getting farmers: {e}"
            }

        finally:
            self.disconnect()

        response = {
            "Success": True,
            "Data": {
                "Events": zipped_events,
                "Total Items": total_items
            }
        }
        return response
    
    def get_plots(self, data):
        pass

    def get_rewards(self, data):
        pass

    def get_errors(self, data):
        pass
    
    # ===== UPDATE
    def update_farmer(self, data):
        try:
            self.connect()
            cursor = self.conn.cursor()

            farmer_name = data.get('Farmer Name')
            farmer_piece_cache_status = data.get('Farmer Piece Cache Status')
            farmer_piece_cache_percent = data.get('Farmer Piece Cache Percent')
            farmer_workers = data.get('Farmer Workers')

            if not farmer_name:
                logger.warn("Farmer Name is required")
                response = {
                    'Success': False,
                    'Message': "Farmer Name is required"
                }
                return response
            
            if farmer_piece_cache_status is None and farmer_piece_cache_percent is None and farmer_workers is None:
                logger.info("No data provided. No changes made")
                response = {
                    'Success': True,
                    'Message': "No data provided. No changes made"
                }
                return response
            
            # Construct the SQL update query
            sql = "UPDATE farmers SET "
            params = []

            if farmer_piece_cache_status is not None:
                sql += "farmer_piece_cache_status = ?, "
                params.append(farmer_piece_cache_status)

            if farmer_piece_cache_percent is not None:
                sql += "farmer_piece_cache_percent = ?, "
                params.append(farmer_piece_cache_percent)

            if farmer_workers is not None:
                sql += "farmer_workers = ?, "
                params.append(farmer_workers)

            # Remove the trailing comma and space
            sql = sql.rstrip(', ')

            # Add the WHERE clause for the farmer_name
            sql += " WHERE farmer_name = ?"
            params.append(farmer_name)

            # Execute the SQL query
            cursor.execute(sql, params)
            rowcount = cursor.rowcount

            if rowcount > 0:
                self.conn.commit()
                logger.info(f"Farmer {farmer_name} updated")
                response = {
                    'Success': True,
                    'Message': f"Farmer {farmer_name} updated"
                }
            else:
                logger.info("No matching rows. No changes made.")
                response = {
                    'Success': True,
                    'Message': f"No matching rows. No changes made."
                }


        except Exception as e:
            # Rollback the transaction if an error occurs
            logger.error(f'Error updating farmer: {e}')
            self.conn.rollback()
            response = {
                "Success": False,
                'Message': f'Error updating farmer: {e}'
            }

        finally:
            self.disconnect()

        return response
    
    def update_farm(self, data):
        try:
            self.connect()
            cursor = self.conn.cursor()

            farm_id = data.get('Farm ID')
            farmer_name = data.get('Farmer Name')
            farm_index = data.get('Farm Index')

            if not all([farm_id, farmer_name, farm_index]):
                message = "Farm ID, Farmer Name, and Farm Index are all required"
                logger.warn(message)
                response = {
                    'Success': False,
                    "Message": message
                }
                return response
            

            farm_public_key = data.get('Farm Public Key')
            farm_allocated_space_gib = data.get('Farm Allocated Space GiB')
            farm_directory = data.get('Farm Directory')
            farm_status = data.get('Farm Status')


            # Construct the SQL update query
            sql = "UPDATE farms SET "
            params = []

            if farm_public_key is not None:
                sql += "farm_public_key = ?, "
                params.append(farm_public_key)

            if farm_allocated_space_gib is not None:
                sql += "farm_allocated_space_gib = ?, "
                params.append(farm_allocated_space_gib)

            if farm_directory is not None:
                sql += "farm_directory = ?, "
                params.append(farm_directory)

            if farm_status is not None:
                sql += "farm_status = ?, "
                params.append(farm_status)

            # Remove the trailing comma and space
            sql = sql.rstrip(', ')

            # Add the WHERE clause for the farmer_name
            sql += " WHERE farmer_name = ? AND farm_id = ? AND farm_index = ?"
            params.extend([farmer_name, farm_id, farm_index])

            # Execute the SQL query
            cursor.execute(sql, params)
            rowcount = cursor.rowcount

            if rowcount > 0:
                self.conn.commit()
                logger.info(f"Farm updated")
                response = {
                    'Success': True,
                    'Message': f"Farm updated"
                }
            else:
                logger.info("No matching rows. No changes made.")
                response = {
                    'Success': True,
                    'Message': f"No matching rows. No changes made."
                }

        except Exception as e:
            # Rollback the transaction if an error occurs
            logger.error(f'Error updating farm: {e}')
            self.conn.rollback()
            response = {
                "Success": False,
                'Message': f'Error updating farm: {e}'
            }

        finally:
            self.disconnect()

        return response
    
    # ===== DELETE
    def delete_farmer(self, data):
        try:
            self.connect()
            cursor = self.conn.cursor()

            farmer_name = data.get('Farmer Name')

            if not farmer_name:
                logger.warn("Farmer Name is required")
                response = {
                    'Success': False,
                    'Message': "Farmer Name is required"
                }
                return response
                

            cursor.execute("DELETE FROM farmers WHERE farmer_name = ?", (farmer_name,))
            rowcount = cursor.rowcount

            if rowcount == 0:
                logger.info("No matching rows. No changes made.")
                response = {
                    "Success": True,
                    'Message': "No matching rows. No changes made."
                }

            else:
                self.conn.commit()
                logger.info(f"{farmer_name} Deleted")
                response = {
                    'Success': True,
                    'Message': f"{farmer_name} Deleted"
                }

        except Exception as e:
            # Rollback the transaction if an error occurs
            logger.error(f'Error updating farmer: {e}')
            self.conn.rollback()
            response = {
                "Success": False,
                'Message': f'Error updating farmer: {e}'
            }

        finally:
            self.disconnect()

        return response
    
    def delete_farm(self, data):
        try:
            self.connect()
            cursor = self.conn.cursor()
            logger.info(data)

            farm_id = data.get('Farm ID')
            farmer_name = data.get('Farmer Name')
            farm_index = data.get('Farm Index')

            if not all([farm_id, farmer_name, farm_index]):
                message = "Farm ID, Farmer Name, and Farm Index are all required"
                logger.warn(message)
                response = {
                    'Success': False,
                    "Message": message
                }
                return response
            
            logger.info('Executing')
            
            cursor.execute("DELETE FROM farms WHERE farmer_name = ? AND farm_id = ? AND farm_index = ?", (farmer_name, farm_id, farm_index))
            rowcount = cursor.rowcount

            if rowcount == 0:
                logger.info("No matching rows. No changes made.")
                response = {
                    "Success": True,
                    'Message': "No matching rows. No changes made."
                }

            else:
                self.conn.commit()
                logger.info(f"Farm Deleted")
                response = {
                    'Success': True,
                    'Message': f"Farm Deleted"
                }            
            
        except Exception as e:
            # Rollback the transaction if an error occurs
            logger.error(f'Error deleting farm: {e}')
            self.conn.rollback()
            response = {
                "Success": False,
                'Message': f'Error deleting farm: {e}'
            }

        finally:
            self.disconnect()

        return response
    
    # ===== DELETE ALL
    def delete_all_farmers(self):
        try:
            self.connect()
            cursor = self.conn.cursor()

            cursor.execute("DELETE FROM farmers")
            rowcount = cursor.rowcount

            response = {
                'Success': True,
                'Data': {
                    'Deleted Rows': rowcount
                }
            }

            self.conn.commit()

        except Exception as e:
            # Rollback the transaction if an error occurs
            logger.error(f'Error deleting all farmers: {e}')
            self.conn.rollback()
            response = {
                "Success": False,
                'Message': f'Error deleting all farmers: {e}'
            }

        finally:
            self.disconnect()

        return response

    def delete_all_farms(self):
        try:
            self.connect()
            cursor = self.conn.cursor()

            cursor.execute("DELETE FROM farms")
            rowcount = cursor.rowcount

            response = {
                'Success': True,
                'Data': {
                    'Deleted Rows': rowcount
                }
            }

            self.conn.commit()

        except Exception as e:
            # Rollback the transaction if an error occurs
            logger.error(f'Error deleting all farms: {e}')
            self.conn.rollback()
            response = {
                'error': f'Error deleting all farms: {e}'
            }

        finally:
            self.disconnect()

        return response

    def delete_all_events(self):
        try:
            self.connect()
            cursor = self.conn.cursor()

            cursor.execute("DELETE FROM events")
            rowcount = cursor.rowcount

            response = {
                'Success': True,
                'Data': {
                    'Deleted Rows': rowcount
                }
            }

            self.conn.commit()

        except Exception as e:
            # Rollback the transaction if an error occurs
            logger.error(f'Error deleting all events: {e}')
            self.conn.rollback()
            response = {
                'error': f'Error deleting all events: {e}'
            }

        finally:
            self.disconnect()

        return response

    def delete_all_plots(self):
        try:
            self.connect()
            cursor = self.conn.cursor()

            cursor.execute("DELETE FROM plots")
            rowcount = cursor.rowcount

            response = {
                'Success': True,
                'Data': {
                    'Deleted Rows': rowcount
                }
            }

            self.conn.commit()

        except Exception as e:
            # Rollback the transaction if an error occurs
            logger.error(f'Error deleting all plots: {e}')
            self.conn.rollback()
            response = {
                'error': f'Error deleting all plots: {e}'
            }

        finally:
            self.disconnect()

        return response

    def delete_all_rewards(self):
        try:
            self.connect()
            cursor = self.conn.cursor()

            cursor.execute("DELETE FROM rewards")
            rowcount = cursor.rowcount

            response = {
                'Success': True,
                'Data': {
                    'Deleted Rows': rowcount
                }
            }

            self.conn.commit()

        except Exception as e:
            # Rollback the transaction if an error occurs
            logger.error(f'Error deleting all rewards: {e}')
            self.conn.rollback()
            response = {
                'error': f'Error deleting all rewards: {e}'
            }

        finally:
            self.disconnect()

        return response

    def delete_all_errors(self):
        try:
            self.connect()
            cursor = self.conn.cursor()

            cursor.execute("DELETE FROM errors")
            rowcount = cursor.rowcount

            response = {
                'Success': True,
                'Data': {
                    'Deleted Rows': rowcount
                }
            }

            self.conn.commit()

        except Exception as e:
            # Rollback the transaction if an error occurs
            logger.error(f'Error deleting all errors: {e}')
            self.conn.rollback()
            response = {
                'error': f'Error deleting all errors: {e}'
            }

        finally:
            self.disconnect()

        return response
    
    




    # FARMER

    # EVENTS


    def get_events_old(self, data):
        try:
            self.connect()
            cursor = self.conn.cursor()
            page = data['Page']
            limit = data['Limit']

            offset = (page - 1) * limit
            cursor.execute("SELECT COUNT(*) FROM events")
            total_items = cursor.fetchone()[0]

            cursor.execute("SELECT * FROM events ORDER BY event_datetime DESC LIMIT ? OFFSET ?", (limit, offset))            
            events = cursor.fetchall()

        except Exception as e:
            logger.error(f'Error getting events: {e}')
            response = {
                "Success": False,
                "Message": f"Error getting events: {e}"
            }

        finally:
            self.disconnect()

        response = {
            "Success": True,
            "Data": {
                "Events": events,
                "Total": total_items
            }
        }
        return response

    # PLOTS
    def get_plots_old(self, data):
        try:
            self.connect()
            cursor = self.conn.cursor()

            page = data['Page']
            limit = data['Limit']
            farm_index = data['Farm Index']

            offset = (page - 1) * limit
            cursor.execute("SELECT COUNT(*) FROM plots")
            total_items = cursor.fetchone()[0]

            cursor.execute("SELECT * FROM plots WHERE farm_index = ? ORDER BY plot_datetime DESC LIMIT ? OFFSET ?", (farm_index, limit, offset))
            events = cursor.fetchall()

        except Exception as e:
            logger.error(f'Error getting plots: {e}')
            response = {
                "Success": False,
                "Message": f"Error getting plots: {e}"
            }
        
        finally:
            self.disconnect()

        response = {
            "Success": True,
            "Data": {
                "Events": events,
                "Total": total_items
            }
        }
        return response


    def get_farms_oldd(self, data):
        try:
            self.connect()
            cursor = self.conn.cursor()
            page = data.get('Page')
            limit = data.get('Limit')
            farm_index = data.get('Farm Index')
            farmer_name = data.get('Farmer Name')
            farm_id = data.get('Farm ID')

            offset = (page - 1) * limit

            # Construct the base SQL query
            sql = "SELECT * FROM farms WHERE 1=1"
            params = []

            conditions = ""
            # Append conditions for farm_index, farmer_name, and farm_id
            if farm_index is not None:
                conditions += " AND farm_index = ?"
                params.append(farm_index)
            if farmer_name is not None:
                conditions += " AND farmer_name = ?"
                params.append(farmer_name)
            if farm_id is not None:
                conditions += " AND farm_id = ?"
                params.append(farm_id)

            # Add ORDER BY, LIMIT, and OFFSET clauses
            sql = sql + conditions + " ORDER BY creation_datetime DESC LIMIT ? OFFSET ?"
            params.extend([limit, offset])

            # Execute the main SQL query
            logger.info(f"Executing: {sql}")
            cursor.execute(sql, params)
            farms = cursor.fetchall()

            zipped_farms = [dict(zip(keys['Farm'], row)) for row in farms]
            
            # Execute the count query
            count_sql = "SELECT COUNT(*) FROM farms WHERE 1=1" + conditions
            logger.info(f"Executing: {count_sql}")
            cursor.execute(count_sql, params[:-2])  # Exclude LIMIT and OFFSET
            total_items = cursor.fetchone()[0]

            response = {
                "Success": True,
                "Data": {
                    "Farms": zipped_farms,
                    "Total Items": total_items
                }
            }


        except Exception as e:
            logger.error(f'Error getting farms: {e}')
            response = {
                "Success": False,
                "Message": f"Error getting farms: {e}"
            }

        finally:
            self.disconnect()

        return response




    # Unit Tests Built

    # Unit Tests Built    
    def insert_farm_old(self, data) -> bool:
        try:
            farm_id = data['Data']['Farm ID']
            farm_index = data['Data']['Farm Index']
            logger.info(f'checking to see if {farm_id} with index {farm_index} exists')

            self.connect()
            cursor = self.conn.cursor()

            self.conn.execute("BEGIN TRANSACTION")



            cursor.execute("SELECT * FROM farms WHERE farm_id = ? OR farm_index = ?", (farm_id, farm_index,))
            rows = cursor.fetchall()

            logger.info('match found, checking for conflicts')
            insert = True
            for row in rows:
                existing_farm_id = row[0]
                existing_farm_index = row[1]

                if existing_farm_id == farm_id and existing_farm_index == farm_index:
                    logger.info('complete match exists, no need to insert')
                    insert = False
                else:
                    logger.info('conflict found, removing conflict')
                    cursor.execute("DELETE FROM farms WHERE farm_id = ? AND farm_index = ?", (existing_farm_id, existing_farm_index))

            if insert:
                logger.info(f'inserting new farm with id of {farm_id} and index of {farm_index}')
                current_datetime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
                cursor.execute("INSERT INTO farms (farm_id, farm_index, creation_datetime) VALUES (?, ?, ?)", (farm_id, farm_index, current_datetime))
                        
                    

            self.conn.commit()

        except Exception as e:
            # Rollback the transaction if an error occurs
            logger.error(f'Error: {e}')
            self.conn.rollback()
            return False

        finally:
            self.disconnect()

        return True

    # Unit Tests Built
    def insert_reward_old(self, data) -> bool:
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
    
    # Unit Tests Built
    def insert_plot_old(self, data) -> bool:
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
    
    # Unit Tests Built
    def insert_error_old(self, data) -> bool:
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


