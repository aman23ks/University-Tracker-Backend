from functools import wraps
from flask import jsonify, request
from flask_jwt_extended import get_jwt_identity
from services.database import MongoDB

db = MongoDB()

def admin_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        user_email = get_jwt_identity()
        user = db.get_user(user_email)
        
        if not user or not user.get('is_admin'):
            return jsonify({'error': 'Admin access required'}), 403
        return f(*args, **kwargs)
    return decorated_function

def premium_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        user_email = get_jwt_identity()
        user = db.get_user(user_email)
        
        if not user or not user.get('is_premium'):
            return jsonify({'error': 'Premium subscription required'}), 403
        return f(*args, **kwargs)
    return decorated_function

def error_handler(app):
    @app.errorhandler(400)
    def bad_request(error):
        return jsonify({'error': 'Bad request'}), 400

    @app.errorhandler(401)
    def unauthorized(error):
        return jsonify({'error': 'Unauthorized'}), 401

    @app.errorhandler(403)
    def forbidden(error):
        return jsonify({'error': 'Forbidden'}), 403

    @app.errorhandler(404)
    def not_found(error):
        return jsonify({'error': 'Not found'}), 404

    @app.errorhandler(500)
    def server_error(error):
        return jsonify({'error': 'Internal server error'}), 500