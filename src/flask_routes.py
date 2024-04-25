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
            message = 'successfully inserted'
        else:
            message = 'event already exists'

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

    if entity == 'farms':
        farm_ids = data['Farm IDs']
        result = database_api.delete_farms(farm_ids)

    elif entity == 'farmer':
        farm_name = request.args.get('Farm Name', None)
        if farm_name:
            pass


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




# @nexus_routes.route('/insertevent', methods=['POST'])
# def insert_event():
#     data = request.json
#     logger.info(data)
#     # Handle the request and return a response if needed
#     return jsonify({"message": "Event inserted successfully"}), 200

# @nexus_routes.route('/insertfarmer', methods=['POST'])
# def insert_farmer():
#     data = request.json
#     logger.info(data)
#     # Handle the request and return a response if needed
#     return jsonify({"message": "Farmer inserted successfully"}), 200

# @nexus_routes.route('/insertfarm', methods=['POST'])
# def insert_farmer_details():
#     data = request.json
#     logger.info(data)
#     # Handle the request and return a response if needed
#     return jsonify({"message": "Farm inserted successfully"}), 200

# @nexus_routes.route('/cleanfarms', methods=['POST'])
# def clean_farms():
#     data = request.json
#     logger.info(data)
#     # Handle the request and return a response if needed
#     return jsonify({"message": "Farms cleaned successfully"}), 200

# @nexus_routes.route('/updatefarmid', methods=['POST'])
# def update_farm_id():
#     data = request.json
#     logger.info(data)
#     # Handle the request and return a response if needed
#     return jsonify({"message": "Farm ID updated successfully"}), 200

# @nexus_routes.route('/updatefarmpubkey', methods=['POST'])
# def update_farm_pub_key():
#     data = request.json
#     logger.info(data)
#     # Handle the request and return a response if needed
#     return jsonify({"message": "Farm Public Key updated successfully"}), 200

# @nexus_routes.route('/updatefarmallocspace', methods=['POST'])
# def update_farm_alloc_space():
#     data = request.json
#     logger.info(data)
#     # Handle the request and return a response if needed
#     return jsonify({"message": "Farm Allocated Space updated successfully"}), 200

# @nexus_routes.route('/updatefarmdirectory', methods=['POST'])
# def update_farm_directory():
#     data = request.json
#     logger.info(data)
#     # Handle the request and return a response if needed
#     return jsonify({"message": "Farm Directory updated successfully"}), 200

# @nexus_routes.route('/updatefarmworkers', methods=['POST'])
# def update_farm_workers():
#     data = request.json
#     logger.info(data)
#     # Handle the request and return a response if needed
#     return jsonify({"message": "Farm Directory updated successfully"}), 200


