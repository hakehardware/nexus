from flask import Blueprint, request, jsonify, current_app
from src.logger import logger

nexus_routes = Blueprint('nexus_routes', __name__)

@nexus_routes.route('/insert/<entity>', methods=['POST'])
def insert(entity):
    database_api = current_app.config['database_api']
    data = request.json
    logger.info(f"Inserting {entity}")

    # Map entity names to corresponding insert methods
    insert_methods = {
        'farmer': database_api.insert_farmer,
        'node': database_api.insert_node,
        'farm': database_api.insert_farm,
        'farmer_event': database_api.insert_farmer_event,
        'node_event': database_api.insert_node_event,
        'plot': database_api.insert_plot,
        'reward': database_api.insert_reward,
        'error': database_api.insert_error,
        'claim': database_api.insert_claim,
        'consensus': database_api.insert_consensus
    }

    # Check if the requested entity is supported
    insert_method = insert_methods.get(entity)
    if not insert_method:
        return jsonify({"error": f"Unknown entity: {entity}"}), 400
    
    # Call the appropriate delete method
    try:
        response = insert_method(data)
        if not response["Success"]:
            return jsonify(response), 400
        else:
            return jsonify(response), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
@nexus_routes.route('/get/<entity>', methods=['GET'])
def get(entity):
    database_api = current_app.config['database_api']
    logger.info(f"Getting {entity}")
    data = {
        'Page': request.args.get('page', default=1, type=int),
        'Limit': request.args.get('limit', default=10, type=int),
        'Farmer Name': request.args.get('farmer_name', default=None, type=str),
        'Node Name': request.args.get('node_name', default=None, type=str),
        'Event Type': request.args.get('event_type', default=None, type=str),
        'Plot Type': request.args.get('plot_type', default=None, type=str),
        'Farm Index': request.args.get('farm_index', default=None, type=int),
        'Start Time': request.args.get('start_datetime'),
        'End Time': request.args.get('end_datetime')
    }

    # Map entity names to corresponding insert methods
    get_methods = {
        'farmers': database_api.get_farmers,
        'node': database_api.get_nodes,
        'farms': database_api.get_farms,
        'farmer_events': database_api.get_farmer_events,
        'node_events': database_api.get_node_events,
        'plots': database_api.get_plots,
        'rewards': database_api.get_rewards,
        'errors': database_api.get_errors
    }
    
    # Check if the requested entity is supported
    get_method = get_methods.get(entity)
    if not get_method:
        return jsonify({"error": f"Unknown entity: {entity}"}), 400
    
    # Call the appropriate delete method
    try:
        response = get_method(data)
        if not response["Success"]:
            return jsonify(response), 400
        else:
            return jsonify(response), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@nexus_routes.route('/update/<entity>', methods=['POST'])
def update(entity):
    database_api = current_app.config['database_api']
    data = request.json


    logger.info(f"Updating {entity}")
    # Map entity names to corresponding update methods
    update_methods = {
        'farmer': database_api.update_farmer,
        'node': database_api.update_node,
        'farm': database_api.update_farm
    }

    # Check if the requested entity is supported
    update_method = update_methods.get(entity)

    if not update_method:
        logger.warn('Unknown Entity')
        return jsonify({"error": f"Unknown entity: {entity}"}), 400
    
    # Call the appropriate update method
    try:
        response = update_method(data)
        if not response["Success"]:
            return jsonify(response), 400
        else:
            return jsonify(response), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@nexus_routes.route('/delete/<entity>', methods=['POST'])
def delete(entity):
    database_api = current_app.config['database_api']
    data = request.json
    logger.info(f"Deleting {entity}")

    # Map entity names to corresponding insert methods
    insert_methods = {
        'farmer': database_api.delete_farmer,
        'node': database_api.delete_node,
        'farm': database_api.delete_farm
    }

    # Check if the requested entity is supported
    insert_method = insert_methods.get(entity)
    if not insert_method:
        return jsonify({"error": f"Unknown entity: {entity}"}), 400
    
    # Call the appropriate delete method
    try:
        response = insert_method(data)
        if not response["Success"]:
            return jsonify(response), 400
        else:
            return jsonify(response), 200

    except Exception as e:
        logger.error(f'Error deleting entity: {e}')
        return jsonify({"error": str(e)}), 500

@nexus_routes.route('/delete/<entity>/all', methods=['POST'])
def delete_all(entity):
    logger.info(f"Deleting all rows in {entity}")

    database_api = current_app.config.get('database_api')

    # Map entity names to corresponding delete methods
    delete_methods = {
        'farmers': database_api.delete_all_farmers,
        'nodes': database_api.delete_all_nodes,
        'farms': database_api.delete_all_farms,
        'farmer_events': database_api.delete_all_farmer_events,
        'node_events': database_api.delete_all_node_events,
        'plots': database_api.delete_all_plots,
        'rewards': database_api.delete_all_rewards,
        'errors': database_api.delete_all_errors,
        'claims': database_api.delete_all_claims,
        'consensus': database_api.delete_all_consensus
    }

    # Check if the requested entity is supported
    delete_method = delete_methods.get(entity)
    if not delete_method:
        return jsonify({"error": f"Unknown entity: {entity}"}), 400

    # Call the appropriate delete method
    try:
        response = delete_method()
        if not response["Success"]:
            return jsonify(response), 400
        else:
            return jsonify(response), 200
    except Exception as e:
        logger.error(f'Error calling delete_method: {e}')
        return jsonify({"error": str(e)}), 500

@nexus_routes.route('/hello', methods=['GET'])
def hello():
    return jsonify({"message": "Hi"}), 200

