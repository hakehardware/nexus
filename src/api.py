import sqlite3
import json
import datetime

from src.logger import logger
from src.helpers import Helpers
from src.constants import keys

# TODO:
# More Validation for Arguments

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
        
        logger.info('initializing "nodes" Table')
        cursor.execute('''CREATE TABLE IF NOT EXISTS nodes (
                            node_name TEXT,
                            node_status TEXT,
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
        
        logger.info('initializing "farmer_events" Table')
        cursor.execute('''CREATE TABLE IF NOT EXISTS farmer_events (
                            event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                            farmer_name TEXT,
                            event_type TEXT,
                            event_data TEXT,
                            event_datetime DATETIME
                        )''')
        
        logger.info('initializing "node_events" Table')
        cursor.execute('''CREATE TABLE IF NOT EXISTS node_events (
                            event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                            node_name TEXT,
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
                            reward_type TEXT,
                            reward_datetime DATETIME
                        )''')
        
        logger.info('Initializing "errors" Table')
        cursor.execute('''CREATE TABLE IF NOT EXISTS errors (
                            error_id INTEGER PRIMARY KEY AUTOINCREMENT,
                            farmer_name TEXT,
                            error TEXT,
                            error_datetime DATETIME
                        )''')
        
        logger.info('Initializing "claims" Table')
        cursor.execute('''CREATE TABLE IF NOT EXISTS claims (
                            claim_id INTEGER PRIMARY KEY AUTOINCREMENT,
                            node_name TEXT,
                            slot INT,
                            claim_type TEXT,
                            claim_datetime DATETIME
                        )''')
        # best: Current Block
        # target: Highest Block
        # When synced best=target
        logger.info('Initializing "consensus" Table')
        cursor.execute('''CREATE TABLE IF NOT EXISTS consensus (
                            consensus_id INTEGER PRIMARY KEY AUTOINCREMENT,
                            node_name TEXT,
                            status TEXT,
                            peers INT,
                            best INT,
                            target INT,
                            finalized INT,
                            bps INT,
                            down_speed_kib REAL,
                            up_speed_kib REAL,
                            consensus_datetime DATETIME
                        )''')
        
        self.conn.commit()
        self.disconnect()

    # ===== INSERT
    def insert_farmer(self, data):
        try:
            self.connect()
            cursor = self.conn.cursor()
            logger.info(f"Data: {data}")

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

    def insert_node(self, data):
        try:
            self.connect()
            cursor = self.conn.cursor()
            logger.info(f"Data: {data}")

            node_name = data.get('Node Name')
            node_status = data.get('Node Status')

            if not node_name:
                logger.warn('No Node Name Provided')
                response = {
                    'Success': False,
                    'Message': 'No Node Name Provided'
                }
                return response

            self.conn.execute("BEGIN TRANSACTION")

            # Check if a row with the given node_name already exists
            cursor.execute("SELECT COUNT(*) FROM nodes WHERE node_name = ?", (node_name,))
            row_count = cursor.fetchone()[0]

            if row_count == 0:
                # Add a new row with the node_name
                current_datetime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
                cursor.execute("INSERT INTO nodes (node_name, node_status, creation_datetime) VALUES (?, ?, ?)", (node_name, node_status, current_datetime))
                logger.info(f"{node_name} added to database")
                response = {
                    'Success': True,
                    'Message': f"{node_name} Added to Database"
                }
                self.conn.commit()

            else:
                logger.info(f'Node {node_name} exists, no changes needed')
                response = {
                    'Success': True,
                    'Message': f"Node {node_name} exists, no changes needed"
                }

        except Exception as e:
            # Rollback the transaction if an error occurs
            logger.error(f'Error inserting node: {e}')
            self.conn.rollback()
            response = {
                "Success": False,
                'Message': f'Error inserting node: {e}'
            }

        finally:
            self.disconnect()

        return response

    def insert_farm(self, data):
        try:
            farm_id = data.get('Data', {}).get('Farm ID')
            farm_index = data.get('Data', {}).get('Farm Index')
            farm_status = data.get('Data', {}).get('Farm Status')
            farmer_name = data.get('Farmer Name')

            if farm_id == None or farm_index == None or farmer_name == None:
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
                cursor.execute("INSERT INTO farms (farm_id, farm_index, farmer_name, farm_status, creation_datetime) VALUES (?, ?, ?, ?, ?)", (farm_id, farm_index, farmer_name, farm_status, current_datetime))
                self.conn.commit()

                message = f"Inserted new farm with id of {farm_id}, farmer_name of {farmer_name}, farm_status of {farm_status} and index of {farm_index}"
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

    def insert_farmer_event(self, data):
        try:
            # Connect to DB
            self.connect()
            cursor = self.conn.cursor()

            # Get data from event
            event_datetime = data.get("Datetime")
            event_type = data.get("Event Type")
            event_data = data.get("Data")
            farmer_name = data.get('Farmer Name')

            missing = []
            if farmer_name == None:
                missing.append('Farmer Name')
            if event_datetime == None:
                missing.append('Event Datetime')
            if event_type == None:
                missing.append('Event Type')
            if event_data == None:
                missing.append('Event Data')


            if len(missing) > 0:
                logger.warn('Missing Required Fields')
                response = {
                    "Success": False,
                    "Message": f"Missing {' and '.join(missing)}"
                }
                return response
            
            # Validate that the datetime is in the correct format
            if not Helpers.validate_date(event_datetime):
                logger.warn('Invalid Datetime Received')
                response = {
                    "Success": False,
                    "Message": "Invalid Datetime",
                    "Inserted Event": False
                }
                return response
            

            logger.info('Validation Passed')

            # Check if the event already exists
            cursor.execute('SELECT COUNT(*) FROM farmer_events WHERE event_datetime = ? AND event_type = ? AND event_data = ? AND farmer_name = ?',
                        (event_datetime, event_type, json.dumps(event_data), farmer_name))
            exists = cursor.fetchone()[0] > 0

            if not exists:
                # Insert the event into the database
                cursor.execute('INSERT INTO farmer_events (event_datetime, event_type, event_data, farmer_name) VALUES (?, ?, ?, ?)',
                            (event_datetime, event_type, json.dumps(event_data), farmer_name))
                self.conn.commit()
                response = {
                    "Success": True,
                    "Message": "Successfully inserted farmer event",
                    "Inserted Event": True
                }

            else:
                response = {
                    "Success": True,
                    "Message": "Event already exists",
                    "Inserted Event": False
                }

            return response
                
        except Exception as e:
            # Rollback the transaction if an error occurs
            logger.error(f'Error inserting farmer event: {e}')
            self.conn.rollback()
            response = {
                "Success": False,
                'Message': f'Error inserting farmer event: {e}',
                "Inserted Event": False
            }

        finally:
            self.disconnect()

    def insert_node_event(self, data):
        try:
            # Connect to DB
            self.connect()
            cursor = self.conn.cursor()

            # Get data from event
            event_datetime = data.get("Datetime")
            event_type = data.get("Event Type")
            event_data = data.get("Data")
            node_name = data.get('Node Name')

            missing = []
            if node_name == None:
                missing.append('Node Name')
            if event_datetime == None:
                missing.append('Event Datetime')
            if event_type == None:
                missing.append('Event Type')
            if event_data == None:
                missing.append('Event Data')


            if len(missing) > 0:
                logger.warn('Missing Required Fields')
                response = {
                    "Success": False,
                    "Message": f"Missing {' and '.join(missing)}"
                }
                return response
            
            # Validate that the datetime is in the correct format
            if not Helpers.validate_date(event_datetime):
                logger.warn('Invalid Datetime Received')
                response = {
                    "Success": False,
                    "Message": "Invalid Datetime",
                    "Inserted Event": False
                }
                return response
            

            logger.info('Validation Passed')

            # Check if the event already exists
            cursor.execute('SELECT COUNT(*) FROM node_events WHERE event_datetime = ? AND event_type = ? AND event_data = ? AND node_name = ?',
                        (event_datetime, event_type, json.dumps(event_data), node_name))
            exists = cursor.fetchone()[0] > 0

            if not exists:
                # Insert the event into the database
                cursor.execute('INSERT INTO node_events (event_datetime, event_type, event_data, node_name) VALUES (?, ?, ?, ?)',
                            (event_datetime, event_type, json.dumps(event_data), node_name))
                self.conn.commit()
                response = {
                    "Success": True,
                    "Message": "Successfully inserted node event",
                    "Inserted Event": True
                }

            else:
                response = {
                    "Success": True,
                    "Message": "Event already exists",
                    "Inserted Event": False
                }

            return response
        
        except Exception as e:
            # Rollback the transaction if an error occurs
            logger.error(f'Error inserting node event: {e}')
            self.conn.rollback()
            response = {
                "Success": False,
                'Message': f'Error inserting node event: {e}',
                "Inserted Event": False
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

            missing = []
            if farmer_name == None:
                missing.append('Farmer Name')
            if farm_index == None:
                missing.append('Farm Index')
            if plot_percentage == None:
                missing.append('Plot Percentage')
            if plot_current_sector == None and plot_percentage != 100.0:
                missing.append('Plot Current Sector')
            if plot_type == None:
                missing.append('Plot Type')
            if plot_datetime == None:
                missing.append('Datetime')

            if len(missing) > 0:
                logger.warn('Missing Required Fields')
                response = {
                    "Success": False,
                    "Message": f"Missing {' and '.join(missing)}"
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

            # Get data from reward
            farmer_name = data.get("Farmer Name")
            farm_index = data.get("Data", {}).get("Farm Index")
            reward_hash = data.get("Data", {}).get("Reward Hash")
            reward_type = data.get("Data", {}).get("Reward Type")
            reward_datetime = data.get('Datetime')


            missing = []
            if farmer_name == None:
                missing.append('Farmer Name')
            if farm_index == None:
                missing.append('Farm Index')
            if reward_type == None:
                missing.append('Reward Type')
            if reward_hash == None and reward_type == 'Reward':
                missing.append('Reward Hash')
            if reward_datetime == None:
                missing.append('Datetime')

            if len(missing) > 0:
                logger.warn('Missing Required Fields')
                response = {
                    "Success": False,
                    "Message": f"Missing {' and '.join(missing)}"
                }
                return response
            
            # Validate that the datetime is in the correct format
            if not Helpers.validate_date(reward_datetime):
                logger.warn('Invalid Datetime Received')
                response = {
                    "Success": False,
                    "Message": "Invalid Datetime"
                }
                return response
            
            logger.info('Validation Passed')

            # Insert the event into the database
            cursor.execute('INSERT INTO rewards (farmer_name, farm_index, reward_hash, reward_type, reward_datetime) VALUES (?, ?, ?, ?, ?)',
                        (farmer_name, farm_index, reward_hash, reward_type, reward_datetime))
            self.conn.commit()

            response = {
                "Success": True,
                "Message": "Successfully inserted reward"
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

    def insert_error(self, data):
        try:
            # Connect to DB
            self.connect()
            cursor = self.conn.cursor()

            # Get data from error
            farmer_name = data.get("Farmer Name")
            error = data.get("Data", {}).get("Error")
            error_datetime = data.get('Datetime')

            logger.info(data)

            # Validate that all data points are there
            if not all([farmer_name, error, error_datetime]):
                logger.warn('Missing Required Fields')
                response = {
                    "Success": False,
                    "Message": "Missing Required Fields"
                }
                return response
            
            # Validate that the datetime is in the correct format
            if not Helpers.validate_date(error_datetime):
                logger.warn('Invalid Datetime Received')
                response = {
                    "Success": False,
                    "Message": "Invalid Datetime"
                }
                return response
            
            logger.info('Validation Passed')

            # Insert the event into the database
            cursor.execute('INSERT INTO errors (farmer_name, error, error_datetime) VALUES (?, ?, ?)',
                        (farmer_name, error, error_datetime))
            self.conn.commit()
            response = {
                "Success": True,
                "Message": "Successfully inserted error"
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

    def insert_claim(self, data):
        try:
            # Connect to DB
            self.connect()
            cursor = self.conn.cursor()

            # Get data from error
            node_name = data.get("Node Name")
            slot = data.get('Data').get('Slot')
            claim_type = data.get('Data').get('Claim Type')
            claim_datetime = data.get('Datetime')

            missing = []
            if node_name == None:
                missing.append('Node Name')
            if slot == None:
                missing.append('Slot')
            if claim_type == None:
                missing.append('Claim Type')

            if len(missing) > 0:
                logger.warn('Missing Required Fields')
                response = {
                    "Success": False,
                    "Message": f"Missing {' and '.join(missing)}"
                }
                return response
            
            # Validate that the datetime is in the correct format
            if not Helpers.validate_date(claim_datetime):
                logger.warn('Invalid Datetime Received')
                response = {
                    "Success": False,
                    "Message": "Invalid Datetime"
                }
                return response
            
            logger.info('Validation Passed')

            # Insert the claim into the database
            cursor.execute('INSERT INTO claims (node_name, slot, claim_type, claim_datetime) VALUES (?, ?, ?, ?)',
                        (node_name, slot, claim_type, claim_datetime))
            self.conn.commit()
            response = {
                "Success": True,
                "Message": "Successfully inserted claim"
            }

        except Exception as e:
            # Rollback the transaction if an error occurs
            logger.error(f'Error inserting claim: {e}')
            self.conn.rollback()
            response = {
                "Success": False,
                'Message': f'Error inserting claim: {e}'
            }

        finally:
            self.disconnect()

        return response

    def insert_consensus(self, data):
        try:
            # Connect to DB
            self.connect()
            cursor = self.conn.cursor()

            # Get data from error
            node_name = data.get("Node Name")
            status = data.get('Data').get('Status')
            peers = data.get('Data').get('Peers')
            best = data.get('Data').get('Best')
            target = data.get('Data').get('Target')
            finalized = data.get('Data').get('Finalized')
            bps = data.get('Data').get('BPS')
            down_speed_kib = data.get('Data').get('Down Speed')
            up_speed_kib = data.get('Data').get('Up Speed')
            consensus_datetime = data.get('Datetime')

            missing = []
            if node_name == None:
                missing.append('Node Name')
            if status == None:
                missing.append('Status')
            if peers == None:
                missing.append('Peers')
            if consensus_datetime == None:
                missing.append('Datetime')
            if best == None:
                missing.append('Best')
            if finalized == None:
                missing.append('Finalized')
            if up_speed_kib == None:
                missing.append('Up Speed')
            if down_speed_kib == None:
                missing.append('Down Speed')

            if len(missing) > 0:
                logger.warn('Missing Required Fields')
                response = {
                    "Success": False,
                    "Message": f"Missing {' and '.join(missing)}"
                }
                return response
            
            # Validate that the datetime is in the correct format
            if not Helpers.validate_date(consensus_datetime):
                logger.warn('Invalid Datetime Received')
                response = {
                    "Success": False,
                    "Message": "Invalid Datetime"
                }
                return response
            
            logger.info('Consensus Validation Passed')

            # Build the SQL query string
            sql_query = 'INSERT INTO consensus (node_name, status, peers, best, finalized, down_speed_kib, up_speed_kib, consensus_datetime'
            sql_values = (node_name, status, peers, best, finalized, down_speed_kib, up_speed_kib, consensus_datetime)

            if target is not None:
                sql_query += ', target'
                sql_values += (target,)
            if bps is not None:
                sql_query += ', bps'
                sql_values += (bps,)

            sql_query += ') VALUES (' + ','.join(['?'] * len(sql_values)) + ')'

            # Insert the data into the database
            cursor.execute(sql_query, sql_values)
            self.conn.commit()
            response = {
                "Success": True,
                "Message": "Successfully inserted consensus"
            }

        except Exception as e:
            # Rollback the transaction if an error occurs
            logger.error(f'Error inserting claim: {e}')
            self.conn.rollback()
            response = {
                "Success": False,
                'Message': f'Error inserting claim: {e}'
            }

        finally:
            self.disconnect()

        return response
    
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
    
    def get_farmer_events(self, data):
        try:
            self.connect()
            cursor = self.conn.cursor()

            # Construct the SQL query
            sql = "SELECT * FROM farmer_events WHERE"

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
            sql += " ORDER BY event_datetime DESC LIMIT ? OFFSET ?"
            params.extend([data['Limit'], offset])

            logger.info(f"Executing: {sql}")
            logger.info(params)

            cursor.execute(sql, params)
            events = cursor.fetchall()

            zipped_events = [dict(zip(keys['Farm Event'], row)) for row in events]

            # Execute the count query
            count_sql = "SELECT COUNT(*) FROM farmer_events WHERE" + conditions
            logger.info(f"Executing: {count_sql}")
            cursor.execute(count_sql, params[:-2])  # Exclude LIMIT and OFFSET
            total_items = cursor.fetchone()[0]

        except Exception as e:
            logger.error(f'Error getting farmer events: {e}')
            response = {
                "Success": False,
                "Message": f"Error getting farmer events: {e}"
            }
            return response

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
    
    def get_node_events(self, data):
        try:
            self.connect()
            cursor = self.conn.cursor()

            # Construct the SQL query
            sql = "SELECT * FROM node_events WHERE"

            conditions = " event_datetime BETWEEN ? AND ?"
            params = [data['Start Time'], data['End Time']]

            # Add optional filters if provided
            if data['Event Type']:
                conditions += " AND event_type = ?"
                params.append(data['Event Type'])
            if data['Node Name']:
                conditions += " AND node_name = ?"
                params.append(data['Node Name'])

            sql += conditions

            # Add pagination
            offset = (data['Page'] - 1) * data['Limit']
            sql += " ORDER BY event_datetime DESC LIMIT ? OFFSET ?"
            params.extend([data['Limit'], offset])

            logger.info(f"Executing: {sql}")
            logger.info(params)

            cursor.execute(sql, params)
            events = cursor.fetchall()

            zipped_events = [dict(zip(keys['Node Event'], row)) for row in events]

            # Execute the count query
            count_sql = "SELECT COUNT(*) FROM node_events WHERE" + conditions
            logger.info(f"Executing: {count_sql}")
            cursor.execute(count_sql, params[:-2])  # Exclude LIMIT and OFFSET
            total_items = cursor.fetchone()[0]

        except Exception as e:
            logger.error(f'Error getting node events: {e}')
            response = {
                "Success": False,
                "Message": f"Error getting node events: {e}"
            }
            return response

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
        try:
            self.connect()
            cursor = self.conn.cursor()

            logger.info(data)

            # Construct the SQL query
            sql = "SELECT * FROM plots WHERE"

            conditions = " plot_datetime BETWEEN ? AND ?"
            params = [data['Start Time'], data['End Time']]

            # Add optional filters if provided
            if data.get('Farm Index'):
                conditions += " AND farm_index = ?"
                params.append(data['Farm Index'])

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
            plots = cursor.fetchall()

            zippled_plots = [dict(zip(keys['Plot'], row)) for row in plots]

            # Execute the count query
            count_sql = "SELECT COUNT(*) FROM plots WHERE" + conditions
            logger.info(f"Executing: {count_sql}")
            cursor.execute(count_sql, params[:-2])  # Exclude LIMIT and OFFSET
            total_items = cursor.fetchone()[0]

        except Exception as e:
            logger.error(f'Error getting farmers: {e}')
            response = {
                "Success": False,
                "Message": f"Error getting farmers: {e}"
            }
            return response

        finally:
            self.disconnect()

        response = {
            "Success": True,
            "Data": {
                "Plots": zippled_plots,
                "Total Items": total_items
            }
        }
        
        return response

    def get_rewards(self, data):
        try:
            self.connect()
            cursor = self.conn.cursor()

            logger.info(data)

            # Construct the SQL query
            sql = "SELECT * FROM rewards WHERE"

            conditions = " reward_datetime BETWEEN ? AND ?"
            params = [data['Start Time'], data['End Time']]

            # Add optional filters if provided
            if data.get('Farm Index'):
                conditions += " AND farm_index = ?"
                params.append(data['Farm Index'])

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
            rewards = cursor.fetchall()


            zipped_rewards = [dict(zip(keys['Reward'], row)) for row in rewards]

            # Execute the count query
            count_sql = "SELECT COUNT(*) FROM rewards WHERE" + conditions
            logger.info(f"Executing: {count_sql}")
            cursor.execute(count_sql, params[:-2])  # Exclude LIMIT and OFFSET
            total_items = cursor.fetchone()[0]

        except Exception as e:
            logger.error(f'Error getting rewards: {e}')
            response = {
                "Success": False,
                "Message": f"Error getting rewards: {e}"
            }
            return response

        finally:
            self.disconnect()

        response = {
            "Success": True,
            "Data": {
                "Rewards": zipped_rewards,
                "Total Items": total_items
            }
        }
        
        return response

    def get_errors(self, data):
        try:
            self.connect()
            cursor = self.conn.cursor()

            logger.info(data)

            # Construct the SQL query
            sql = "SELECT * FROM errors WHERE"

            conditions = " error_datetime BETWEEN ? AND ?"
            params = [data['Start Time'], data['End Time']]

            # Add optional filters if provided
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
            errors = cursor.fetchall()


            zipped_errors = [dict(zip(keys['Error'], row)) for row in errors]

            # Execute the count query
            count_sql = "SELECT COUNT(*) FROM errors WHERE" + conditions
            logger.info(f"Executing: {count_sql}")
            cursor.execute(count_sql, params[:-2])  # Exclude LIMIT and OFFSET
            total_items = cursor.fetchone()[0]

        except Exception as e:
            logger.error(f'Error getting errors: {e}')
            response = {
                "Success": False,
                "Message": f"Error getting errors: {e}"
            }
            return response

        finally:
            self.disconnect()

        response = {
            "Success": True,
            "Data": {
                "Errors": zipped_errors,
                "Total Items": total_items
            }
        }
        
        return response
    
    # ===== UPDATE
    def update_farmer(self, data):
        try:
            self.connect()
            cursor = self.conn.cursor()

            farmer_name = data.get('Farmer Name')
            farmer_piece_cache_status = data.get('Data', {}).get('Farmer Piece Cache Status')
            farmer_piece_cache_percent = data.get('Data', {}).get('Farmer Piece Cache Percent')
            farmer_workers = data.get('Data', {}).get('Farmer Workers')

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
    
    def update_node(self, data):
        try:
            self.connect()
            cursor = self.conn.cursor()

            node_name = data.get('Node Name')
            node_status = data.get('Data', {}).get('Node Status')

            if not node_name:
                logger.warn("Node Name is required")
                response = {
                    'Success': False,
                    'Message': "Node Name is required"
                }
                return response
            
            if node_status is None:
                logger.info("No new data provided. No changes made")
                response = {
                    'Success': True,
                    'Message': "No new data provided. No changes made"
                }
                return response
            
            # Construct the SQL update query
            sql = "UPDATE farmers SET "
            params = []

            if node_status is not None:
                sql += "node_status = ?, "
                params.append(node_status)

            # Remove the trailing comma and space
            sql = sql.rstrip(', ')

            # Add the WHERE clause for the node_name
            sql += " WHERE node_name = ?"
            params.append(node_name)

            # Execute the SQL query
            cursor.execute(sql, params)
            rowcount = cursor.rowcount

            if rowcount > 0:
                self.conn.commit()
                logger.info(f"Node {node_name} updated")
                response = {
                    'Success': True,
                    'Message': f"Node {node_name} updated"
                }
            else:
                logger.info("No matching rows. No changes made.")
                response = {
                    'Success': True,
                    'Message': f"No matching rows. No changes made."
                }


        except Exception as e:
            # Rollback the transaction if an error occurs
            logger.error(f'Error updating node: {e}')
            self.conn.rollback()
            response = {
                "Success": False,
                'Message': f'Error updating node: {e}'
            }

        finally:
            self.disconnect()

        return response

    def update_farm(self, data):
        try:
            self.connect()
            cursor = self.conn.cursor()


            farmer_name = data.get('Farmer Name')
            farm_index = data.get('Data', {}).get('Farm Index')

            if farmer_name == None or farm_index == None:
                message = "Farmer Name and Farm Index are required"
                logger.warn(message)
                response = {
                    'Success': False,
                    "Message": message
                }
                return response
            

            farm_public_key = data.get('Data', {}).get('Farm Public Key')
            farm_allocated_space_gib = data.get('Data', {}).get('Farm Allocated Space')
            farm_directory = data.get('Data', {}).get('Farm Directory')
            farm_status = data.get('Data', {}).get('Farm Status')


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
            logger.info(sql)

            # Remove the trailing comma and space
            sql = sql.rstrip(', ')

            # Add the WHERE clause for the farmer_name
            sql += " WHERE farmer_name = ? AND farm_index = ?"
            params.extend([farmer_name, farm_index])

            # Execute the SQL query
            logger.info(sql)
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
            logger.error(f'Error deleting farmer: {e}')
            self.conn.rollback()
            response = {
                "Success": False,
                'Message': f'Error deleting farmer: {e}'
            }

        finally:
            self.disconnect()

        return response
    
    def delete_node(self, data):
        try:
            self.connect()
            cursor = self.conn.cursor()

            node_name = data.get('Node Name')

            if not node_name:
                logger.warn("Node Name is required")
                response = {
                    'Success': False,
                    'Message': "Node Name is required"
                }
                return response
                

            cursor.execute("DELETE FROM nodes WHERE node_name = ?", (node_name,))
            rowcount = cursor.rowcount

            if rowcount == 0:
                logger.info("No matching rows. No changes made.")
                response = {
                    "Success": True,
                    'Message': "No matching rows. No changes made."
                }

            else:
                self.conn.commit()
                logger.info(f"{node_name} Deleted")
                response = {
                    'Success': True,
                    'Message': f"{node_name} Deleted"
                }

        except Exception as e:
            # Rollback the transaction if an error occurs
            logger.error(f'Error deleting node: {e}')
            self.conn.rollback()
            response = {
                "Success": False,
                'Message': f'Error deleting node: {e}'
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
    
    def delete_all_nodes(self):
        try:
            self.connect()
            cursor = self.conn.cursor()

            cursor.execute("DELETE FROM nodes")
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
            logger.error(f'Error deleting all nodes: {e}')
            self.conn.rollback()
            response = {
                "Success": False,
                'Message': f'Error deleting all nodes: {e}'
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

    def delete_all_farmer_events(self):
        try:
            self.connect()
            cursor = self.conn.cursor()

            cursor.execute("DELETE FROM farmer_events")
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
            logger.error(f'Error deleting all farmer events: {e}')
            self.conn.rollback()
            response = {
                'error': f'Error deleting all farmer events: {e}'
            }

        finally:
            self.disconnect()

        return response
    
    def delete_all_node_events(self):
        try:
            self.connect()
            cursor = self.conn.cursor()

            cursor.execute("DELETE FROM node_events")
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
            logger.error(f'Error deleting all node events: {e}')
            self.conn.rollback()
            response = {
                'error': f'Error deleting all node events: {e}'
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
    
    def delete_all_claims(self):
        try:
            self.connect()
            cursor = self.conn.cursor()

            cursor.execute("DELETE FROM claims")
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
            logger.error(f'Error deleting all claims: {e}')
            self.conn.rollback()
            response = {
                'error': f'Error deleting all claims: {e}'
            }

        finally:
            self.disconnect()

        return response
    
    def delete_all_consensus(self):
        try:
            self.connect()
            cursor = self.conn.cursor()

            cursor.execute("DELETE FROM consensus")
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
            logger.error(f'Error deleting all consensus: {e}')
            self.conn.rollback()
            response = {
                'error': f'Error deleting all consensus: {e}'
            }

        finally:
            self.disconnect()

        return response