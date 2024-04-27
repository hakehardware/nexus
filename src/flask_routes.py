from flask import Blueprint, request, jsonify, current_app
from src.logger import logger

nexus_routes = Blueprint('nexus_routes', __name__)

@nexus_routes.route('/insert/<entity>', methods=['POST'])
def insert_entity(entity):
    database_api = current_app.config['database_api']
    data = request.json
    logger.info(data)
    
    # Handle the request based on the specified entity
    if entity == 'event':
        logger.info('inserting event')
        result = database_api.insert_event(data)
        if result:
            message = 'success'
        else:
            message = 'failure'

    elif entity == 'farmer':
        logger.info('inserting farmer')
        result = database_api.insert_farmer(data)
        if result:
            message = 'success'
        else:
            message = 'failure'

    elif entity == 'farm':
        logger.info('inserting farm')
        result = database_api.insert_farm(data)
        if result:
            message = 'success'
        else:
            message = 'failure'

    elif entity == 'reward':
        logger.info('inserting reward')
        result = database_api.insert_reward(data)
        if result:
            message = 'success'
        else:
            message = 'failure'

    elif entity == 'plot':
        logger.info('inserting plot')
        result = database_api.insert_plot(data)
        if result:
            message = 'success'
        else:
            message = 'failure'

    elif entity == 'error':
        logger.info('inserting error')
        result = database_api.insert_error(data)
        if result:
            message = 'success'
        else:
            message = 'failure'

    else:
        return jsonify({"error": "Invalid entity"}), 400
    
    # Return a response
    return jsonify({"message": message}), 200

@nexus_routes.route('/delete/<entity>', methods=['POST'])
def delete(entity):
    database_api = current_app.config['database_api']
    data = request.json
    logger.info(data)

    if entity == 'farm':
        result = database_api.delete_farm(data)
        if result:
            message = 'success'
        else:
            message = 'failure'

    elif entity == 'farmer':
        farm_name = data['Farm Name']
        result = database_api.delete_farmer(farm_name)
        if result:
            message = 'success'
        else:
            message = 'failure'
    else:
        return jsonify({"error": "Invalid entity"}), 400
    
    # Return a response
    return jsonify({"message": message}), 200

@nexus_routes.route('/clean_farms', methods=['POST'])
def clean_farms():
    database_api = current_app.config['database_api']
    data = request.json
    logger.info(data)

    farm_ids = data['Farm IDs']
    result = database_api.clean_farms(farm_ids)

    if result:
        message = 'success'
    else:
        message = 'failure'

    return jsonify({"message": message}), 200

@nexus_routes.route('/get/<entity>', methods=['GET'])
def get_entity(entity):
    
    # Handle the request based on the specified entity
    if entity == 'event':
        message = "Got Events Successfully"
    elif entity == 'plot':
        message = "Got Plots Successfully"
    elif entity == 'reward':
        message = "Got Rewards Successfully"
    elif entity == 'farmer_metrics':
        message = "Got Farmer Metrics Successfully"
    elif entity == 'farm_metrics':
        message = "Got Farm Metrics Successfully"
    elif entity == 'error':
        message = "Got Error Successfully"
    elif entity == 'farm':
        message = "Got Farm Successfully"
    elif entity == 'farmer':
        message = "Got Farmer Successfully"
    else:
        return jsonify({"error": "Invalid entity"}), 400
    
    # Return a response
    return jsonify({"message": message}), 200

@nexus_routes.route('/hello', methods=['GET'])
def check():
    return jsonify({"message": "Hi"}), 200