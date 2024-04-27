from flask import Flask
from src.flask_routes import nexus_routes
from src.api import DatabaseAPI
from src.logger import logger

app = Flask(__name__)

class Nexus:
    def __init__(self, config) -> None:
        self.config = config

        self.database_api = DatabaseAPI(self.config["database_location"] + 'nexus.db')

    def run(self) -> None:
        logger.info('Initializing Cosmos DB')
        self.database_api.initialize()

        app.config['database_api'] = self.database_api
        app.register_blueprint(nexus_routes)

        app.run(debug=True, host='0.0.0.0')