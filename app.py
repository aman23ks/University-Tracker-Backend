from functools import wraps, lru_cache
from flask import Flask, Response, make_response, request, jsonify
from flask_jwt_extended import JWTManager, create_access_token, jwt_required, get_jwt_identity
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import timedelta
import os
import json
import asyncio
from dotenv import load_dotenv
from services.database import MongoDB
from services.rag_retrieval import RAGRetrieval
from services.payment_service import PaymentService
from services.hybrid_crawler import HybridCrawler
from datetime import datetime, timezone
from bson import json_util, ObjectId
import json
from typing import List, Dict
from dotenv import load_dotenv
import os
from flask_cors import CORS
from services.analytics import get_monthly_growth, get_user_activity, get_total_revenue

load_dotenv()
app = Flask(__name__)

# Configure app
app.config['JWT_SECRET_KEY'] = os.getenv('JWT_SECRET')
app.config['JWT_ACCESS_TOKEN_EXPIRES'] = timedelta(days=1)
app.config['CORS_HEADERS'] = 'Content-Type'

# Configure CORS
CORS(app, resources={
    r"/*": {
        "origins": "*",
        "methods": ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
        "allow_headers": ["Content-Type", "Authorization"],
        "supports_credentials": True
    }
})

# Initialize JWT
jwt = JWTManager(app)

# Lazy service initialization
@lru_cache()
def get_services():
    db = MongoDB(os.getenv('MONGO_URI'))
    rag = RAGRetrieval(
        openai_api_key=os.getenv('OPENAI_API_KEY'),
        pinecone_api_key=os.getenv('PINECONE_API_KEY'),
        cohere_api_key=os.getenv('COHERE_API_KEY'),
        index_name=os.getenv('INDEX_NAME')
    )
    payment = PaymentService(
        key_id=os.getenv('RAZORPAY_KEY_ID'),
        key_secret=os.getenv('RAZORPAY_KEY_SECRET')
    )
    crawler = HybridCrawler(
        openai_api_key=os.getenv('OPENAI_API_KEY'),
        pinecone_api_key=os.getenv('PINECONE_API_KEY'),
        index_name=os.getenv('INDEX_NAME')
    )
    return db, rag, payment, crawler

# Get service instances
db, rag, payment, crawler = get_services()

# Add this after your app initialization but before routes
@app.before_request
def handle_content_type():
    if request.method == 'POST':
        if not request.is_json:
            request.environ['CONTENT_TYPE'] = 'application/json'

@app.after_request
def handle_response(response):
    if request.method == 'OPTIONS':
        response.headers['Access-Control-Allow-Origin'] = '*'
        response.headers['Access-Control-Allow-Methods'] = 'GET, POST, PUT, DELETE, OPTIONS'
        response.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization'
    return response

def serialize_mongo(obj):
    """Convert MongoDB objects to JSON serializable format"""
    return json.loads(json_util.dumps(obj))

# Authentication routes
@app.route('/api/auth/register', methods=['POST'])
def register():
    data = request.json
    result = db.create_user(data)
    if 'error' in result:
        return jsonify(result), 400
    
    user = db.get_user(data['email'])
    access_token = create_access_token(identity=data['email'])
    return jsonify({'token': access_token, 'user': user}), 201

@app.route('/api/auth/login', methods=['POST'])
def login():
    try:
        data = request.json
        print(f"Login attempt for: {data.get('email')}")
        
        user = db.verify_user(data)
        if 'error' in user:
            return jsonify(user), 401
            
        access_token = create_access_token(identity=data['email'])
        return jsonify({
            'token': access_token,
            'user': {
                'email': user['email'],
                'is_admin': user['is_admin'],
                'is_premium': user['is_premium'],
                'selected_universities': user['selected_universities'],
                'subscription': user.get('subscription', {'status': 'free'})
            }
        })
        
    except Exception as e:
        print(f"Login error: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/auth/verify', methods=['GET'])
@jwt_required()
def verify_token():
    try:
        current_user = get_jwt_identity()
        user = db.get_user(current_user)
        if not user:
            return jsonify({'error': 'User not found'}), 404
            
        return jsonify({
            'email': user['email'],
            'is_admin': user['is_admin'],
            'is_premium': user['is_premium'],
            'selected_universities': user['selected_universities'],
            'subscription': user.get('subscription', {'status': 'free'})
        })
        
    except Exception as e:
        print(f"Verify error: {str(e)}")
        return jsonify({'error': str(e)}), 500

# University routes
@app.route('/api/universities', methods=['GET'])
@jwt_required()
def get_universities():
    return jsonify(db.get_universities())

@app.route('/api/universities', methods=['POST'])
@jwt_required()
async def add_university():
    user_email = get_jwt_identity()
    user = db.get_user(user_email)
    if not user.get('is_admin'):
        return jsonify({'error': 'Unauthorized'}), 403
    
    try:
        data = request.json
        if not data or 'url' not in data or 'program' not in data or 'name' not in data:
            return jsonify({'error': 'Missing required fields'}), 400

        # Process university data with crawler
        crawler_result = await crawler.process_university(data['url'], data['program'])
        
        if not crawler_result['success']:
            return jsonify({
                'error': 'Failed to process university data',
                'details': crawler_result.get('error', 'Unknown error')
            }), 500

        # Add to database
        university_data = {
            'name': data['name'],
            'url': data['url'],
            'programs': [data['program']],
            'metadata': {
                'namespace': crawler_result['namespace'],
                'pages_crawled': crawler_result['pages_crawled'],
                'data_chunks': crawler_result['data_chunks']
            }
        }

        result = db.add_university(university_data)
        if 'error' in result:
            return jsonify(result), 500

        # After successfully adding university, populate existing custom columns
        try:
            # Get all existing custom columns
            columns = db.get_all_columns()
            
            # For each column, query RAG and save data
            for column in columns:
                # Generate question based on column name
                question = f"What is the {column['name']} for this university program?"
                
                # Query RAG
                rag_result = rag.query(question, crawler_result['namespace'])
                
                if rag_result and 'answer' in rag_result:
                    # Save column data for all users who have this column
                    users_with_column = db.get_users_with_column(str(column['_id']))
                    for user_email in users_with_column:
                        column_data = {
                            'university_id': result['id'],
                            'column_id': str(column['_id']),
                            'user_email': user_email,
                            'value': rag_result['answer']
                        }
                        db.save_column_data(column_data)

            # Return success with populated data
            return jsonify({
                'success': True,
                'university': result,
                'columns_populated': len(columns)
            }), 201

        except Exception as e:
            app.logger.error(f"Error populating column data: {str(e)}")
            # Return partial success
            return jsonify({
                'success': True,
                'university': result,
                'warning': 'University added but column data population failed'
            }), 201

    except Exception as e:
        app.logger.error(f"Error adding university: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/universities/<string:id>', methods=['DELETE'])
@jwt_required()
def delete_university(id):
    user_email = get_jwt_identity()
    user = db.get_user(user_email)
    
    if not user.get('is_admin'):
        return jsonify({'error': 'Unauthorized'}), 403
    
    try:
        # Get university details
        university = db.get_university_by_id(id)
        if not university:
            return jsonify({'error': 'University not found'}), 404

        # Delete from database
        result = db.delete_university(id)
        if 'error' in result:
            return jsonify(result), 400

        # Delete from Pinecone if namespace exists
        if 'metadata' in university and 'namespace' in university['metadata']:
            try:
                index = crawler.index
                print(university['metadata']['namespace'])
                index.delete(deleteAll=True, namespace=university['metadata']['namespace'])
            except Exception as e:
                app.logger.error(f"Error deleting from Pinecone: {str(e)}")

        return jsonify({'message': 'University deleted successfully'})

    except Exception as e:
        app.logger.error(f"Error deleting university: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/rag', methods=['POST'])
@jwt_required()
def query_rag():
    try:
        data = request.json
        if not data:
            return jsonify({'error': 'No data provided'}), 400
            
        if 'question' not in data or 'university_id' not in data:
            return jsonify({
                'error': 'Missing required fields',
                'required': ['question', 'university_id']
            }), 400

        # Get university details to verify existence
        university = db.get_university_by_id(data['university_id'])
        if not university:
            return jsonify({'error': 'University not found'}), 404

        # Use university ID as namespace
        namespace = university['metadata']['namespace']

        # Get answer from RAG service
        result = rag.query(data['question'], namespace)

        if 'error' in result:
            return jsonify({
                'error': 'RAG query failed',
                'details': result['error']
            }), 500

        return jsonify(result)
            
    except Exception as e:
        app.logger.error(f"RAG endpoint error: {str(e)}")
        return jsonify({
            'error': 'Internal server error',
            'details': str(e)
        }), 500
        
@app.route('/api/users', methods=['GET'])
@jwt_required()
def get_users():
    try:
        # Get current user
        current_user = get_jwt_identity()
        user = db.get_user(current_user)
        
        # Check if user is admin
        if not user or not user.get('is_admin'):
            return jsonify({'error': 'Unauthorized access'}), 403
        
        # Get all users
        users = db.get_users()
        
        # Format user data for response
        formatted_users = [{
            'id': str(user.get('_id', '')),
            'email': user.get('email'),
            'is_premium': user.get('is_premium', False),
            'is_admin': user.get('is_admin', False),
            'selected_universities': user.get('selected_universities', []),
            'subscription': user.get('subscription', {
                'status': 'free',
                'expiry': None
            }),
            'created_at': user.get('created_at'),
            'last_login': user.get('last_login')
        } for user in users]
        
        return jsonify(formatted_users), 200
        
    except Exception as e:
        app.logger.error(f"Error fetching users: {str(e)}")
        return jsonify({
            'error': 'Internal server error',
            'message': str(e)
        }), 500

@app.route('/api/universities/find', methods=['POST'])
@jwt_required()
def find_university():
    try:
        data = request.json
        app.logger.info(f"Finding university with data: {data}")
        
        if not data or 'url' not in data:
            app.logger.warning("Missing URL in request")
            return jsonify({'error': 'URL is required'}), 400

        university = db.find_university_by_url(data['url'])
        app.logger.info(f"Found university: {university is not None}")
        
        if not university:
            app.logger.warning(f"University not found for URL: {data['url']}")
            return jsonify({'error': 'University not found'}), 404

        return jsonify(university)

    except Exception as e:
        app.logger.error(f"Error finding university: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/universities/details', methods=['POST'])
@jwt_required()
def get_universities_details():
    try:
        data = request.json
        app.logger.info(f"Getting details for universities: {data}")
        
        if not data or 'universities' not in data:
            app.logger.warning("No universities in request")
            return jsonify({'error': 'No universities provided'}), 400

        urls = data['universities']
        app.logger.info(f"Processing URLs: {urls}")
        
        if not urls:
            app.logger.warning("Empty URL list")
            return jsonify({'error': 'Empty university list'}), 400

        universities = db.get_universities_by_urls(urls)
        app.logger.info(f"Found {len(universities)} universities")
        
        if not universities:
            app.logger.warning(f"No universities found for URLs: {urls}")
            return jsonify({'error': 'No universities found'}), 404

        return jsonify(universities)

    except Exception as e:
        app.logger.error(f"Error fetching university details: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/users/universities/add', methods=['POST'])
@jwt_required()
def add_university_to_user():
    try:
        current_user = get_jwt_identity()
        data = request.json
        
        app.logger.info(f"[Backend] Add university request from user: {current_user}")
        app.logger.info(f"[Backend] Request data: {data}")
        
        if not data or 'university_id' not in data:
            return jsonify({'error': 'University ID is required'}), 400

        # Get current user's selected universities
        user = db.get_user(current_user)
        if not user:
            return jsonify({'error': 'User not found'}), 404

        app.logger.info(f"[Backend] Current user's selected universities: {user.get('selected_universities', [])}")

        # Get university details
        university = db.get_university_by_id(data['university_id'])
        if not university:
            return jsonify({'error': 'University not found'}), 404

        app.logger.info(f"[Backend] Found university: {university['url']}")

        # Check if university is already selected
        if university['url'] in user.get('selected_universities', []):
            app.logger.warning(f"[Backend] University already selected: {university['url']}")
            return jsonify({'error': 'University already selected'}), 400

        # Add university to user's selection
        result = db.update_user_universities(
            current_user,
            'add',
            university['url']
        )

        if 'error' in result:
            return jsonify(result), 400

        # Get custom columns for the user and populate data
        columns = db.get_custom_columns(current_user)
        column_data = {}
        
        app.logger.info(f"[Backend] Populating {len(columns)} columns for new university")
        
        # Get university namespace from metadata
        namespace = university['metadata'].get('namespace')
        
        # For each column, query RAG and store data
        for column in columns:
            try:
                # Generate question based on column name
                question = f"What is the {column['name']} for this university program?"
                
                # Query RAG
                rag_result = rag.query(question, namespace)
                
                if rag_result and 'answer' in rag_result:
                    # Save column data
                    save_data = {
                        'university_id': data['university_id'],
                        'column_id': str(column['_id']),
                        'user_email': current_user,
                        'value': rag_result['answer']
                    }
                    
                    db.save_column_data(save_data)
                    
                    # Store for response
                    column_data[str(column['_id'])] = {
                        'value': rag_result['answer'],
                        'last_updated': datetime.utcnow().isoformat()
                    }
                    
                    app.logger.info(f"[Backend] Populated column {column['name']} with RAG data")
                
            except Exception as e:
                app.logger.error(f"[Backend] Error populating column {column['name']}: {str(e)}")

        app.logger.info("[Backend] Successfully added university with column data")
        return jsonify({
            'message': 'University added successfully',
            'university': university,
            'columnData': column_data
        })

    except Exception as e:
        app.logger.error(f"[Backend] Error adding university to user: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/users/universities/remove', methods=['POST'])
@jwt_required()
def remove_university_from_user():
    try:
        current_user = get_jwt_identity()
        data = request.json
        
        if not data or 'university_id' not in data:
            return jsonify({'error': 'University ID is required'}), 400

        # Get university
        university = db.get_university_by_id(data['university_id'])
        if not university:
            return jsonify({'error': 'University not found'}), 404

        # Remove university from user's selection
        result = db.update_user_universities(
            current_user,
            'remove',
            university['url']
        )

        if 'error' in result:
            return jsonify(result), 400

        return jsonify({
            'message': 'University removed successfully'
        })

    except Exception as e:
        app.logger.error(f"Error removing university from user: {str(e)}")
        return jsonify({'error': str(e)}), 500
    
@app.route('/api/users/update', methods=['POST'])
@jwt_required()
def update_user():
    try:
        current_user = get_jwt_identity()
        data = request.json

        # Get current user
        user = db.get_user(current_user)
        if not user:
            return jsonify({'error': 'User not found'}), 404

        # Update user data
        result = db.update_user(current_user, data)
        if 'error' in result:
            return jsonify(result), 400

        # Get updated user data
        updated_user = db.get_user(current_user)
        return jsonify({
            'email': updated_user['email'],
            'is_premium': updated_user['is_premium'],
            'is_admin': updated_user['is_admin'],
            'selected_universities': updated_user['selected_universities'],
            'subscription': updated_user.get('subscription', {'status': 'free'})
        })

    except Exception as e:
        app.logger.error(f"Error updating user: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/users/universities/update', methods=['POST'])
@jwt_required()
def update_user_universities():
    try:
        current_user = get_jwt_identity()
        data = request.json

        if 'selected_universities' not in data:
            return jsonify({'error': 'No universities provided'}), 400

        # Update user's universities
        result = db.update_user(current_user, {
            'selected_universities': data['selected_universities']
        })

        if 'error' in result:
            return jsonify(result), 400

        return jsonify({'message': 'Universities updated successfully'})

    except Exception as e:
        app.logger.error(f"Error updating user universities: {str(e)}")
        return jsonify({'error': str(e)}), 500
    

# Add these routes to app.py
@app.route('/api/columns', methods=['GET'])
@jwt_required()
def get_columns():
    try:
        current_user = get_jwt_identity()
        columns = db.get_custom_columns(current_user)
        
        # Serialize the MongoDB response
        serialized_columns = serialize_mongo(columns)
        return jsonify(serialized_columns)
        
    except Exception as e:
        app.logger.error(f"Error fetching columns: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/columns', methods=['POST'])
@jwt_required()
def create_column():
    try:
        current_user = get_jwt_identity()
        data = request.json

        if not data or 'name' not in data:
            return jsonify({'error': 'Column name is required'}), 400

        column_data = {
            'name': data['name'],
            'type': data.get('type', 'text'),
            'created_by': current_user,
            'is_global': data.get('is_global', False)
        }

        result = db.create_custom_column(column_data)
        
        if 'error' in result:
            return jsonify(result), 400
            
        # Serialize the result
        serialized_result = serialize_mongo(result)
        return jsonify(serialized_result), 201

    except Exception as e:
        app.logger.error(f"Error creating column: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/columns/data', methods=['POST'])
@jwt_required()
def save_column_data():
    try:
        current_user = get_jwt_identity()
        data = request.json

        if not data or not all(k in data for k in ['university_id', 'column_id', 'value']):
            return jsonify({'error': 'Missing required fields'}), 400

        save_data = {
            'university_id': data['university_id'],
            'column_id': data['column_id'],
            'user_email': current_user,
            'value': data['value']
        }

        result = db.save_column_data(save_data)

        if 'error' in result:
            return jsonify(result), 400

        return jsonify(result)

    except Exception as e:
        app.logger.error(f"Error saving column data: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/columns/data/batch', methods=['POST'])
@jwt_required()
def get_column_data():
    try:
        current_user = get_jwt_identity()
        data = request.json

        if not data or 'university_ids' not in data:
            return jsonify({'error': 'University IDs are required'}), 400

        column_data = db.get_column_data(current_user, data['university_ids'])
        return jsonify(column_data)

    except Exception as e:
        app.logger.error(f"Error getting column data: {str(e)}")
        return jsonify({'error': str(e)}), 500

# Add this route to app.py
@app.route('/api/columns/<string:column_id>', methods=['DELETE'])
@jwt_required()
def delete_column(column_id):
    try:
        current_user = get_jwt_identity()
        user = db.get_user(current_user)
        
        # Get column details
        column = db.get_column_by_id(column_id)
        if not column:
            return jsonify({'error': 'Column not found'}), 404
            
        # Check permissions
        if not user.get('is_admin') and column['created_by'] != current_user and column.get('is_global', False):
            return jsonify({'error': 'Unauthorized to delete this column'}), 403
            
        # Delete the column
        result = db.delete_column(column_id)
        if result.get('error'):
            return jsonify(result), 400
            
        return jsonify({'message': 'Column deleted successfully'})
        
    except Exception as e:
        app.logger.error(f"Error deleting column: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/subscription/create-order', methods=['POST'])
@jwt_required()
def create_subscription_order():
    try:
        user_email = get_jwt_identity()
        print(f"Creating order for user: {user_email}")
        
        # Fixed amount for premium subscription (₹20 = 2000 paise)
        amount = 100000
        
        order_response = payment.create_order(amount, user_email)
        if not order_response['success']:
            return jsonify({'error': 'Failed to create order'}), 400
            
        return jsonify(order_response['order'])
        
    except Exception as e:
        print(f"Error creating subscription order: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/subscription/verify', methods=['POST'])
@jwt_required()
def verify_subscription():
    try:
        user_email = get_jwt_identity()
        data = request.json
        print(f"Verifying payment for user: {user_email}")
        print(f"Verification data: {data}")
        
        # Verify payment signature
        if not payment.verify_payment(
            data['payment_id'],
            data['order_id'],
            data['signature']
        ):
            return jsonify({'error': 'Invalid payment signature'}), 400
            
        # Get payment details
        payment_details = payment.get_payment_details(data['payment_id'])
        if not payment_details['success']:
            return jsonify({'error': 'Failed to fetch payment details'}), 400
            
        # Update user subscription
        expiry_date = datetime.utcnow() + timedelta(days=30)
        update_data = {
            'is_premium': True,
            'subscription': {
                'status': 'active',
                'expiry': expiry_date,
                'payment_history': [{
                    'payment_id': data['payment_id'],
                    'amount': payment_details['payment']['amount'] / 100,
                    'timestamp': datetime.utcnow()
                }]
            }
        }
        
        result = db.update_user(user_email, update_data)
        if result.get('error'):
            return jsonify({'error': result['error']}), 400
            
        return jsonify({
            'success': True,
            'message': 'Subscription activated successfully'
        })
        
    except Exception as e:
        print(f"Error verifying subscription: {str(e)}")
        return jsonify({'error': str(e)}), 500

# Add this function to your app.py
@app.route('/api/subscription/status', methods=['GET'])
@jwt_required()
def check_subscription_status():
    try:
        user_email = get_jwt_identity()
        user = db.get_user(user_email)
        
        if not user:
            return jsonify({'error': 'User not found'}), 404

        # Check if subscription has expired
        if user.get('subscription'):
            expiry = user['subscription'].get('expiry')
            if expiry:
                # MongoDB returns datetime, ensure it's UTC
                expiry_date = expiry.replace(tzinfo=timezone.utc) if isinstance(expiry, datetime) else datetime.fromisoformat(expiry.replace('Z', '+00:00'))
                current_time = datetime.now(timezone.utc)

                if current_time > expiry_date:
                    # Update user status to expired
                    db.update_user(user_email, {
                        'is_premium': False,
                        'subscription': {
                            'status': 'expired',
                            'expiry': expiry,
                            'paymentHistory': user['subscription'].get('paymentHistory', [])
                        }
                    })
                    return jsonify({
                        'is_premium': False,
                        'subscription': {
                            'status': 'expired',
                            'expiry': expiry.isoformat() if isinstance(expiry, datetime) else expiry
                        }
                    })

        # Return current status if not expired
        return jsonify({
            'is_premium': user.get('is_premium', False),
            'subscription': user.get('subscription', {'status': 'free'})
        })
        
    except Exception as e:
        app.logger.error(f"Error checking subscription status: {str(e)}")
        return jsonify({'error': str(e)}), 500
    
@app.route('/api/subscription/cancel', methods=['POST'])
@jwt_required()
def cancel_subscription():
    try:
        user_email = get_jwt_identity()
        result = db.update_user(user_email, {
            'subscription.status': 'cancelled',
            'subscription.cancel_at_period_end': True
        })
        
        if result.get('error'):
            return jsonify({'error': result['error']}), 400
            
        return jsonify({
            'success': True,
            'message': 'Subscription will be cancelled at the end of the billing period'
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/users/profile', methods=['GET'])
@jwt_required()
def get_profile():
    try:
        user_email = get_jwt_identity()
        user = db.get_user(user_email)
        if not user:
            return jsonify({'error': 'User not found'}), 404
            
        return jsonify({
            'email': user['email'],
            'is_premium': user.get('is_premium', False),
            'subscription': user.get('subscription', {
                'status': 'free',
                'expiry': None
            }),
            'created_at': user.get('created_at')
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    

@app.route('/api/users/profile', methods=['PUT'])
@jwt_required()
def update_profile():
    try:
        user_email = get_jwt_identity()
        data = request.json
        # Verify current password if trying to change password
        if data.get('newPassword'):
            user = db.verify_user({
                'email': user_email,
                'password': data['currentPassword']
            })
            if 'error' in user:
                return jsonify({'error': 'Current password is incorrect'}), 400
            
            # Update password
            data['password'] = generate_password_hash(data['newPassword'])
        
        # Remove sensitive fields from update data
        update_data = {
            'email': data.get('email'),
            'password': data.get('password')  # Only included if password was changed
        }
        update_data = {k: v for k, v in update_data.items() if v is not None}
        result = db.update_user(user_email, update_data)
        if result.get('error'):
            return jsonify({'error': result['error']}), 400
            
        # Get updated user data
        updated_user = db.get_user(data.get('email', user_email)) 
        return jsonify({
            'email': updated_user['email'],
            'is_premium': updated_user.get('is_premium', False),
            'subscription': updated_user.get('subscription', {'status': 'free'})
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/analytics', methods=['GET'])
@jwt_required()
def get_analytics():
    try:
        current_user = get_jwt_identity()
        user = db.get_user(current_user)
        
        if not user.get('is_admin'):
            return jsonify({'error': 'Unauthorized'}), 403
            
        # Get total users count
        total_users = db.users.count_documents({})
        
        # Get premium users count
        premium_users = db.users.count_documents({'is_premium': True})
        
        # Calculate premium percentage
        premium_percentage = round((premium_users / total_users) * 100 if total_users > 0 else 0, 1)
        
        # Get total universities
        total_universities = db.universities.count_documents({})
        
        # Calculate total revenue
        total_revenue = get_total_revenue(db)
        
        # Get growth data
        monthly_growth = get_monthly_growth(db)
        
        # Get activity data
        user_activity = get_user_activity(db)
        
        return jsonify({
            'totalUsers': total_users,
            'premiumUsers': premium_users,
            'totalUniversities': total_universities,
            'activePremiumPercentage': premium_percentage,
            'totalRevenue': total_revenue,
            'monthlyGrowth': monthly_growth,
            'userActivity': user_activity
        })
        
    except Exception as e:
        app.logger.error(f"Error fetching analytics: {str(e)}")
        return jsonify({'error': str(e)}), 500
    
# Error handlers
@app.errorhandler(500)
def handle_500_error(e):
    app.logger.error(f"Internal Server Error: {str(e)}")
    return jsonify({
        'error': 'Internal server error',
        'message': str(e)
    }), 500

@app.errorhandler(404)
def handle_404_error(e):
    return jsonify({
        'error': 'Not found',
        'message': 'The requested resource was not found'
    }), 404

if __name__ == '__main__':
    app.run(debug=True, port=5000)
