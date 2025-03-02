from pymongo import MongoClient, UpdateOne
from datetime import datetime
from werkzeug.security import generate_password_hash, check_password_hash
from typing import Dict, List, Optional
import logging
from bson import ObjectId

logger = logging.getLogger(__name__)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

class MongoDB:
    def __init__(self, uri: str = 'mongodb://localhost:27017/'):
        """Fork-safe initialization"""
        self.uri = uri
        self.client = None
        self.db = None
        self.connect()

    def connect(self):
        """Initialize connection"""
        if not self.client:
            self.client = MongoClient(self.uri)
            self.db = self.client['university_tracker']
            self._ensure_indexes()

    def _ensure_indexes(self):
        # Existing indexes
        self.db.users.create_index('email', unique=True)
        self.db.universities.create_index('url', unique=True)
        self.db.user_data.create_index([('user_email', 1), ('university_url', 1)])

        # Add new indexes for custom columns
        self.db.custom_columns.create_index([
            ('created_by', 1),
            ('name', 1)
        ])
        self.db.column_data.create_index([
            ('user_email', 1),
            ('university_id', 1),
            ('column_id', 1)
        ])
        
        self.db.activity_logs.create_index([
            ('timestamp', 1),
            ('type', 1),
            ('user_id', 1)
        ])

    def create_user(self, data: Dict) -> Dict:
        try:
            if self.db.users.find_one({'email': data['email']}):  # Changed from users
                return {'error': 'Email already exists'}
            
            user = {
                'email': data['email'],
                'password': generate_password_hash(data['password']),
                'is_premium': False,
                'is_admin': data['email'].endswith('@admin.com'),
                'selected_universities': [],
                'university_selections': [],
                'created_at': datetime.utcnow(),
                'last_login': datetime.utcnow(),
                'subscription': {
                    'status': 'free',
                    'expiry': None,
                    'payment_history': []
                }
            }
            
            result = self.db.users.insert_one(user)  # Changed from users
            user_without_password = {k: v for k, v in user.items() if k != 'password'}
            return user_without_password
            
        except Exception as e:
            logger.error(f"Error creating user: {str(e)}")
            return {'error': str(e)}

    def verify_user(self, data: Dict) -> Dict:
        try:
            user = self.db.users.find_one({'email': data['email']})  # Changed
            if not user:
                return {'error': 'User not found'}
            
            if not check_password_hash(user['password'], data['password']):
                return {'error': 'Invalid password'}
            
            self.db.users.update_one(  # Changed
                {'email': data['email']},
                {
                    '$set': {'last_login': datetime.utcnow()},
                    '$inc': {'login_count': 1}
                }
            )
            
            return {
                'email': user['email'],
                'password': user['password'],
                'is_premium': user['is_premium'],
                'is_admin': user['is_admin'],
                'selected_universities': user['selected_universities'],
                'subscription': user.get('subscription', {'status': 'free'})
            }
        except Exception as e:
            logger.error(f"Error verifying user: {str(e)}")
            return {'error': str(e)}

    def get_user(self, email: str) -> Optional[Dict]:
        try:
            user = self.db.users.find_one(  # Changed
                {'email': email},
                {'password': 0}  # Exclude password
            )
            if user:
                user['_id'] = str(user['_id'])  # Convert ObjectId to string
            return user
        except Exception as e:
            logger.error(f"Error getting user: {str(e)}")
            return None

    def get_universities(self, query: Dict = None) -> List[Dict]:
        try:
            universities = list(self.db.universities.find(query or {}))
            for uni in universities:
                # Ensure status exists
                if 'status' not in uni:
                    uni['status'] = 'pending'
                # Convert ObjectId to string
                uni['_id'] = str(uni['_id'])
                # Ensure ID exists
                if 'id' not in uni:
                    uni['id'] = uni['_id']
            return universities
        except Exception as e:
            logger.error(f"Error getting universities: {str(e)}")
            return []
        
    def add_university(self, data: Dict) -> Dict:
        try:
            # Check if university already exists
            existing = self.db.universities.find_one({'url': data['url']})
            
            # Generate a unique ID if not exists
            if not existing:
                data['id'] = str(ObjectId())
                data['name'] = data['name']  # Add name field
                data['created_at'] = datetime.utcnow()
                data['last_updated'] = datetime.utcnow()
                
                result = self.db.universities.insert_one(data)
                if result.inserted_id:
                    return {
                        'id': data['id'],
                        'name': data['name'],  # Include name in response
                        'url': data['url'],
                        'programs': data['programs'],
                        'metadata': data.get('metadata', {}),
                        'last_updated': data['last_updated']
                    }
            else:
                # Update existing university
                update_data = {
                    '$addToSet': {'programs': {'$each': data['programs']}},
                    '$set': {
                        'last_updated': datetime.utcnow(),
                        'metadata': data.get('metadata', {}),
                        'name': data.get('name', existing.get('name'))  # Update name if provided
                    }
                }
                
                self.db.universities.update_one(
                    {'url': data['url']},
                    update_data
                )
                
                return {
                    'id': existing.get('id', str(existing['_id'])),
                    'name': data.get('name', existing.get('name')),  # Include updated name
                    'url': data['url'],
                    'programs': list(set(existing['programs'] + data['programs'])),
                    'metadata': data.get('metadata', existing.get('metadata', {})),
                    'last_updated': datetime.utcnow()
                }

            return {'error': 'Failed to add university'}
        except Exception as e:
            logger.error(f"Error adding university: {str(e)}")
            return {'error': str(e)}

    def delete_university(self, university_id: str) -> Dict:
        try:
            result = self.db.universities.delete_one({'id': university_id})
            if result.deleted_count:
                return {'success': True}
            return {'error': 'University not found'}
        except Exception as e:
            logger.error(f"Error deleting university: {str(e)}")
            return {'error': str(e)}

    def get_users(self) -> List[Dict]:
        """Get all users from the database (excluding sensitive information)"""
        try:
            users = list(self.db.users.find(
                {},
                {
                    'password': 0,  # Exclude password
                    '_id': 0        # Exclude MongoDB ID
                }
            ))
            
            # Transform any special types to standard Python types
            for user in users:
                # Convert datetime objects to ISO format strings
                if 'created_at' in user:
                    user['created_at'] = user['created_at'].isoformat()
                if 'last_login' in user:
                    user['last_login'] = user['last_login'].isoformat()
                    
                # Ensure certain fields always exist
                user['is_premium'] = user.get('is_premium', False)
                user['is_admin'] = user.get('is_admin', False)
                user['selected_universities'] = user.get('selected_universities', [])
                
                # Ensure subscription info exists
                if 'subscription' not in user:
                    user['subscription'] = {
                        'status': 'free',
                        'expiry': None,
                        'payment_history': []
                    }

            return users
        except Exception as e:
            logger.error(f"Error getting users: {str(e)}")
            return []

    def get_university_by_id(self, university_id: str) -> Optional[Dict]:
        """Get university by ID"""
        try:
            university = self.db.universities.find_one({'id': university_id})
            if university:
                university['_id'] = str(university['_id'])
            return university
        except Exception as e:
            logger.error(f"Error getting university by ID: {str(e)}")
            return None
    
    def update_user(self, email: str, update_data: Dict) -> Dict:
        try:
            safe_update_data = {
                k: v for k, v in update_data.items() 
                if k not in ['email', 'is_admin', '_id']
            }

            if not safe_update_data:
                return {'error': 'No valid fields to update'}

            update_doc = {'$set': safe_update_data}

            if 'selected_universities' in safe_update_data:
                universities = safe_update_data.pop('selected_universities', [])
                self.db.users.update_one(  # Changed
                    {'email': email},
                    {'$set': {'selected_universities': []}}
                )
                if universities:
                    update_doc['$addToSet'] = {
                        'selected_universities': {'$each': universities}
                    }

            result = self.db.users.update_one(  # Changed
                {'email': email},
                update_doc
            )

            if result.matched_count == 0:
                return {'error': 'User not found'}

            if result.modified_count == 0 and not result.upserted_id:
                return {'error': 'No changes made'}

            return {'success': True}

        except Exception as e:
            logger.error(f"Error updating user: {str(e)}")
            return {'error': str(e)}

    def update_user_universities(self, email: str, action: str, university_url: str) -> Dict:
        """Update user's selected universities"""
        try:
            user = self.get_user(email)
            if not user:
                return {'error': 'User not found'}

            current_universities = user.get('selected_universities', [])

            if action == 'add':
                # Check if already selected
                if university_url in current_universities:
                    return {'error': 'University already selected'}

                # Check free tier limit
                if not user.get('is_premium') and len(current_universities) >= 3:
                    return {'error': 'Free tier limit reached'}

                # Add university
                update = {
                    '$addToSet': {'selected_universities': university_url}
                }

            elif action == 'remove':
                # Check if university is selected
                if university_url not in current_universities:
                    return {'error': 'University not selected'}

                # Remove university
                update = {
                    '$pull': {'selected_universities': university_url}
                }

            else:
                return {'error': 'Invalid action'}

            result = self.db.users.update_one(
                {'email': email},
                update
            )

            if result.modified_count:
                return {'success': True}
            return {'error': 'Failed to update universities'}

        except Exception as e:
            logger.error(f"Error updating user universities: {str(e)}")
            return {'error': str(e)}

    def update_subscription(self, email: str, payment_data: Dict) -> Dict:
        try:
            update = {
                'is_premium': True,
                'subscription': {
                    'status': 'active',
                    'expiry': payment_data['expiry'],
                    'payment_history': [{
                        'payment_id': payment_data['payment_id'],
                        'amount': payment_data['amount'],
                        'timestamp': datetime.utcnow()
                    }]
                }
            }
            
            result = self.db.users.update_one(
                {'email': email},
                {'$set': update}
            )
            
            if result.modified_count:
                return {'success': True}
            return {'error': 'Failed to update subscription'}
        except Exception as e:
            logger.error(f"Error updating subscription: {str(e)}")
            return {'error': str(e)}
    
    def find_university_by_url(self, url: str) -> Optional[Dict]:
        """Find a university by its URL"""
        try:
            university = self.db.universities.find_one({'url': url})
            if not university:
                return None
                
            # Convert ObjectId to string and ensure consistent ID field
            university['_id'] = str(university['_id'])
            if 'id' not in university:
                university['id'] = str(university['_id'])
                
            # Convert datetime objects to ISO format
            if 'last_updated' in university:
                university['last_updated'] = university['last_updated'].isoformat()
            if 'created_at' in university:
                university['created_at'] = university['created_at'].isoformat()
                
            return university
        except Exception as e:
            logger.error(f"Error finding university by URL: {str(e)}")
            return None
    
    def get_universities_by_urls(self, urls: List[str]) -> List[Dict]:
        """Get multiple universities by their URLs"""
        try:
            # Find all universities with matching URLs
            universities = list(self.db.universities.find({
                'url': {'$in': urls}
            }))

            if not universities:
                return []

            # Process each university
            processed_universities = []
            for university in universities:
                # Convert ObjectId to string
                university['_id'] = str(university['_id'])
                
                # Ensure id exists
                if 'id' not in university:
                    university['id'] = str(university['_id'])
                    
                # Convert datetime objects
                if 'last_updated' in university:
                    university['last_updated'] = university['last_updated'].isoformat()
                if 'created_at' in university:
                    university['created_at'] = university['created_at'].isoformat()
                    
                processed_universities.append(university)

            return processed_universities
        except Exception as e:
            logger.error(f"Error fetching universities by URLs: {str(e)}")
            return []

    def create_custom_column(self, data: dict) -> dict:
        """Create a new custom column"""
        try:
            if 'name' not in data or 'created_by' not in data:
                return {'error': 'Name and created_by are required'}
                
            column = {
                'name': data['name'],
                'type': data.get('type', 'text'),
                'created_by': data['created_by'],
                'is_global': data.get('is_global', False),
                'created_at': datetime.utcnow(),
                'last_updated': datetime.utcnow()
            }
            
            result = self.db.custom_columns.insert_one(column)
            if not result.inserted_id:
                return {'error': 'Failed to create column'}
                
            # Add ID to the column data
            column['_id'] = str(result.inserted_id)
            column['id'] = str(result.inserted_id)  # Add id for frontend
            
            return {
                'success': True,
                'column': column
            }
            
        except Exception as e:
            logger.error(f"Error creating custom column: {str(e)}")
            return {'error': str(e)}

    def get_custom_columns(self, user_email: str) -> list:
        """Get all custom columns for a user"""
        try:
            cursor = self.db.custom_columns.find({
                '$or': [
                    {'created_by': user_email},
                    {'is_global': True}
                ]
            })
            
            columns = []
            for column in cursor:
                # Convert ObjectId to string
                column['id'] = str(column['_id'])
                column['_id'] = str(column['_id'])
                columns.append(column)
                
            return columns
            
        except Exception as e:
            logger.error(f"Error getting custom columns: {str(e)}")
            return []

    def save_column_data(self, data: dict) -> dict:
        """Save custom column data"""
        try:
            if not all(k in data for k in ['university_id', 'column_id', 'user_email', 'value']):
                return {'error': 'Missing required fields'}
                
            update_data = {
                'university_id': data['university_id'],
                'column_id': data['column_id'],
                'user_email': data['user_email'],
                'value': data['value'],
                'last_updated': datetime.utcnow()
            }
            
            result = self.db.column_data.update_one(
                {
                    'university_id': data['university_id'],
                    'column_id': data['column_id'],
                    'user_email': data['user_email']
                },
                {'$set': update_data},
                upsert=True
            )
            
            return {'success': True}
            
        except Exception as e:
            logger.error(f"Error saving column data: {str(e)}")
            return {'error': str(e)}

    def get_column_data(self, user_email: str, university_ids: list) -> dict:
        """Get column data for multiple universities"""
        try:
            cursor = self.db.column_data.find({
                'user_email': user_email,
                'university_id': {'$in': university_ids}
            })
            
            formatted_data = {}
            for item in cursor:
                uni_id = item['university_id']
                if uni_id not in formatted_data:
                    formatted_data[uni_id] = {}
                
                formatted_data[uni_id][item['column_id']] = {
                    'value': item['value'],
                    'last_updated': item['last_updated'].isoformat()
                }
                
            return formatted_data
            
        except Exception as e:
            logger.error(f"Error getting column data: {str(e)}")
            return {}
    
    # Add these methods to the MongoDB class in database.py

    def get_all_columns(self):
        """Get all custom columns from the database"""
        try:
            return list(self.db.custom_columns.find())
        except Exception as e:
            logger.error(f"Error getting all columns: {str(e)}")
            return []

    def get_users_with_column(self, column_id: str) -> List[str]:
        """Get all users who have this column"""
        try:
            # Find all unique user emails that have used this column
            users = self.db.column_data.distinct('user_email', {'column_id': column_id})
            # If no users found, include the admin user
            if not users:
                admin_users = self.db.users.find({'is_admin': True}, {'email': 1})
                users = [user['email'] for user in admin_users]
            return users
        except Exception as e:
            logger.error(f"Error getting users for column: {str(e)}")
            return []

    def save_column_data(self, data: Dict) -> Dict:
        """Save column data with better error handling"""
        try:
            required_fields = ['university_id', 'column_id', 'user_email', 'value']
            if not all(k in data for k in required_fields):
                return {'error': 'Missing required fields'}

            # Add timestamp
            data['last_updated'] = datetime.utcnow()

            # Upsert the data
            result = self.db.column_data.update_one(
                {
                    'university_id': data['university_id'],
                    'column_id': data['column_id'],
                    'user_email': data['user_email']
                },
                {'$set': data},
                upsert=True
            )

            return {
                'success': True,
                'matched_count': result.matched_count,
                'modified_count': result.modified_count,
                'upserted_id': str(result.upserted_id) if result.upserted_id else None
            }
        except Exception as e:
            logger.error(f"Error saving column data: {str(e)}")
            return {'error': str(e)}

    # Add these methods to the MongoDB class in database.py

    def get_column_by_id(self, column_id: str) -> Optional[Dict]:
        """Get column by ID"""
        try:
            # Convert string ID to ObjectId
            obj_id = ObjectId(column_id)
            column = self.db.custom_columns.find_one({'_id': obj_id})
            if column:
                column['_id'] = str(column['_id'])
            return column
        except Exception as e:
            logger.error(f"Error getting column by ID: {str(e)}")
            return None

    def delete_column(self, column_id: str) -> Dict:
        """Delete a custom column and its associated data"""
        try:
            # Convert string ID to ObjectId
            obj_id = ObjectId(column_id)
            
            # Start a session for transaction
            with self.client.start_session() as session:
                with session.start_transaction():
                    # Delete the column definition
                    column_result = self.db.custom_columns.delete_one(
                        {'_id': obj_id},
                        session=session
                    )
                    
                    if column_result.deleted_count == 0:
                        return {'error': 'Column not found'}
                    
                    # Delete all associated column data
                    self.db.column_data.delete_many(
                        {'column_id': str(obj_id)},
                        session=session
                    )
                    
            return {'success': True}
            
        except Exception as e:
            logger.error(f"Error deleting column: {str(e)}")
            return {'error': str(e)}

    def get_column_usage(self, column_id: str) -> Dict:
        """Get usage statistics for a column"""
        try:
            # Get count of data entries
            data_count = self.db.column_data.count_documents({'column_id': column_id})
            
            # Get unique universities using this column
            unique_universities = len(self.db.column_data.distinct('university_id', {'column_id': column_id}))
            
            # Get unique users using this column
            unique_users = len(self.db.column_data.distinct('user_email', {'column_id': column_id}))
            
            return {
                'data_entries': data_count,
                'universities': unique_universities,
                'users': unique_users
            }
            
        except Exception as e:
            logger.error(f"Error getting column usage: {str(e)}")
            return {'error': str(e)}
    
    def log_activity(self, data: dict) -> dict:
        """Log user activity"""
        try:
            activity = {
                'user_id': data.get('user_id'),
                'type': data.get('type'),
                'description': data.get('description'),
                'metadata': data.get('metadata', {}),
                'timestamp': datetime.utcnow()
            }
            
            result = self.db.activity_logs.insert_one(activity)
            return {'success': True, 'id': str(result.inserted_id)}
            
        except Exception as e:
            logger.error(f"Error logging activity: {str(e)}")
            return {'error': str(e)}
    
    def get_activity_logs(self, filters: dict = None, limit: int = 100) -> list:
        """Get activity logs with optional filters"""
        try:
            query = filters or {}
            cursor = self.db.activity_logs.find(
                query,
                {'_id': 0}
            ).sort('timestamp', -1).limit(limit)
            
            return list(cursor)
            
        except Exception as e:
            logger.error(f"Error getting activity logs: {str(e)}")
            return []
        
    def update_university(self, university_id: str, update_data: dict) -> dict:
        """Update university data with progress tracking"""
        try:
            result = self.db.universities.update_one(
                {'id': university_id},
                {
                    '$set': {
                        **update_data,
                        'last_updated': datetime.utcnow()
                    }
                }
            )
            
            return {
                'success': result.modified_count > 0,
                'error': None if result.modified_count > 0 else 'University not found'
            }
            
        except Exception as e:
            logger.error(f"Error updating university: {str(e)}")
            return {'error': str(e)}
    
    def get_university_status(self, university_id: str) -> dict:
        """Get current university status"""
        try:
            university = self.db.universities.find_one(
                {'id': university_id},
                {
                    'status': 1,
                    'progress': 1,
                    'error': 1,
                    'metadata': 1,
                    'last_updated': 1
                }
            )
            
            if not university:
                return {'error': 'University not found'}
                
            return {
                'status': university.get('status', 'unknown'),
                'progress': university.get('progress', {}),
                'error': university.get('error'),
                'metadata': university.get('metadata', {}),
                'last_updated': university.get('last_updated')
            }
            
        except Exception as e:
            logger.error(f"Error getting status: {str(e)}")
            return {'error': str(e)}

    def update_university_progress(self, university_id: str, progress_data: dict) -> bool:
        """Update university progress information"""
        try:
            self.db.universities.update_one(
                {'id': university_id},
                {
                    '$set': {
                        'progress': progress_data,
                        'last_updated': datetime.utcnow()
                    }
                }
            )
            return True
        except Exception as e:
            logger.error(f"Error updating university progress: {str(e)}")
            return False

    def update_university_status(self, university_id: str, status: str, error: str = None) -> bool:
        """Update university processing status"""
        try:
            update_data = {
                'status': status,
                'last_updated': datetime.utcnow()
            }
            if error:
                update_data['error'] = error
            
            self.db.universities.update_one(
                {'id': university_id},
                {'$set': update_data}
            )
            return True
        except Exception as e:
            logger.error(f"Error updating university status: {str(e)}")
            return False

    def get_university_progress(self, university_id: str) -> dict:
        """Get detailed university progress"""
        try:
            university = self.db.universities.find_one(
                {'id': university_id},
                {
                    'progress': 1,
                    'status': 1,
                    'error': 1,
                    'last_updated': 1,
                    'metadata': 1
                }
            )
            if not university:
                return {}
                
            return {
                'status': university.get('status', 'unknown'),
                'progress': university.get('progress', {}),
                'error': university.get('error'),
                'last_updated': university.get('last_updated'),
                'metadata': university.get('metadata', {})
            }
        except Exception as e:
            logger.error(f"Error getting university progress: {str(e)}")
            return {}

    def delete_university_column_data(self, university_id: str) -> Dict:
        """Delete all column data associated with a university"""
        try:
            # Delete all column data for this university
            result = self.db.column_data.delete_many({
                'university_id': university_id
            })

            logger.info(f"Deleted {result.deleted_count} column data entries for university {university_id}")
            
            return {
                'success': True,
                'deleted_count': result.deleted_count
            }
        except Exception as e:
            logger.error(f"Error deleting university column data: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def get_column_data_for_university(self, university_id: str, column_id: str, user_email: str) -> Dict:
        """Get column data for a specific university and user"""
        try:
            result = self.db.column_data.find_one({
                'university_id': university_id,
                'column_id': column_id,
                'user_email': user_email
            })
            return result
        except Exception as e:
            logger.error(f"Error getting column data: {str(e)}")
            return None
        
    def update_user_university_visibility(self, email: str, is_premium: bool) -> Dict:
        """
        Update the visibility of universities for a user based on subscription status.
        When is_premium is False, only the first 3 universities remain visible.
        When is_premium is True, all universities become visible.
        """
        try:
            # Get user data
            user = self.get_user(email)
            if not user:
                return {'error': 'User not found'}
                
            # Get all selected university URLs
            selected_urls = user.get('selected_universities', [])
            if not selected_urls:
                return {'success': True, 'message': 'No universities found', 'updated': 0}
                
            # Get university details to get their IDs
            universities = self.get_universities_by_urls(selected_urls)
            
            # Sort universities based on their position in selected_universities array
            # This ensures consistent visibility when subscription expires
            sorted_universities = []
            for url in selected_urls:
                matching_unis = [u for u in universities if u['url'] == url]
                if matching_unis:
                    sorted_universities.append(matching_unis[0])
            
            # Create or update visibility records
            bulk_ops = []
            update_count = 0
            
            for i, uni in enumerate(sorted_universities):
                uni_id = uni.get('id', str(uni['_id']))
                
                # When not premium, only first 3 are visible
                hidden = not is_premium and i >= 3
                
                # Update visibility record
                result = self.db.university_visibility.update_one(
                    {'user_email': email, 'university_id': uni_id},
                    {'$set': {
                        'hidden': hidden,
                        'position': i,
                        'last_updated': datetime.utcnow()
                    }},
                    upsert=True
                )
                
                if result.modified_count or result.upserted_id:
                    update_count += 1
            
            # Return success with count of updated records
            return {
                'success': True, 
                'updated': update_count, 
                'total': len(sorted_universities)
            }
                
        except Exception as e:
            logger.error(f"Error updating university visibility: {str(e)}")
            return {'error': str(e)}
    
    def get_visible_universities(self, email: str) -> List[Dict]:
        """
        Get only the visible universities for a user.
        For free users, this returns at most 3 universities.
        For premium users, this returns all their universities.
        """
        try:
            user = self.get_user(email)
            if not user:
                return []
                
            # Get all selected university URLs
            selected_urls = user.get('selected_universities', [])
            
            # No universities selected
            if not selected_urls:
                return []
                
            # Get all university details
            all_universities = self.get_universities_by_urls(selected_urls)
            
            # If premium or admin, return all universities
            if user.get('is_premium', False) or user.get('is_admin', False):
                return all_universities
                
            # For free users, get hidden status
            visibility_records = list(self.db.university_visibility.find(
                {'user_email': email}
            ))
            
            # If no visibility records exist yet, create them
            if not visibility_records:
                # For backward compatibility, mark only first 3 as visible
                for i, uni in enumerate(all_universities):
                    self.db.university_visibility.insert_one({
                        'user_email': email,
                        'university_id': uni['id'],
                        'hidden': i >= 3
                    })
                    
                # Return only first 3
                return all_universities[:3]
                
            # Build dictionary of hidden status
            hidden_map = {
                record['university_id']: record['hidden'] 
                for record in visibility_records
            }
            
            # Filter universities by visibility
            visible_universities = [
                uni for uni in all_universities
                if not hidden_map.get(uni['id'], False)
            ]
            
            return visible_universities
            
        except Exception as e:
            logger.error(f"Error getting visible universities: {str(e)}")
            return []
        
    def clear_pending_columns(self, email: str) -> Dict:
        """Remove pending columns after they've been processed"""
        try:
            result = self.db.pending_columns.delete_many({'user_email': email})
            return {'success': True, 'deleted_count': result.deleted_count}
            
        except Exception as e:
            logger.error(f"Error clearing pending columns: {str(e)}")
            return {'error': str(e)}
        
    
    def init_university_visibility(self, email: str) -> Dict:
        """Initialize visibility for new user or when adding first universities"""
        try:
            user = self.get_user(email)
            if not user:
                return {'error': 'User not found'}
                
            selected_urls = user.get('selected_universities', [])
            if not selected_urls:
                return {'success': True, 'message': 'No universities to initialize'}
                
            # Get university details
            universities = self.get_universities_by_urls(selected_urls)
            
            # Set visibility based on premium status
            is_premium = user.get('is_premium', False)
            
            visibility_records = []
            for i, uni in enumerate(universities):
                visibility_records.append({
                    'user_email': email,
                    'university_id': uni['id'],
                    'hidden': not is_premium and i >= 3
                })
                
            # Insert all records in bulk if they don't exist
            for record in visibility_records:
                self.db.university_visibility.update_one(
                    {
                        'user_email': record['user_email'],
                        'university_id': record['university_id']
                    },
                    {'$set': record},
                    upsert=True
                )
                
            return {'success': True}
            
        except Exception as e:
            logger.error(f"Error initializing university visibility: {str(e)}")
            return {'error': str(e)}
    
    def get_university_visibility_stats(self, email: str) -> Dict:
        """Get statistics about visible and hidden universities for a user"""
        try:
            # Count all university visibility records
            total_count = self.db.university_visibility.count_documents({'user_email': email})
            
            # Count hidden universities
            hidden_count = self.db.university_visibility.count_documents({
                'user_email': email,
                'hidden': True
            })
            
            # Visible count is the difference
            visible_count = total_count - hidden_count
            
            return {
                'total_count': total_count,
                'visible_count': visible_count,
                'hidden_count': hidden_count
            }
        except Exception as e:
            logger.error(f"Error getting visibility stats: {str(e)}")
            return {
                'total_count': 0,
                'visible_count': 0,
                'hidden_count': 0,
                'error': str(e)
            }
            
    def get_visible_university_ids(self, email: str) -> List[str]:
        """Get list of visible university IDs for a user"""
        try:
            # Find all visibility records that are not hidden
            records = self.db.university_visibility.find({
                'user_email': email,
                'hidden': False
            })
            
            # Extract university IDs
            university_ids = [record['university_id'] for record in records]
            return university_ids
            
        except Exception as e:
            logger.error(f"Error getting visible university IDs: {str(e)}")
            return []
        
    def get_hidden_university_ids(self, email: str) -> List[str]:
        """Get list of hidden university IDs for a user"""
        try:
            # Find all visibility records that are hidden
            records = self.db.university_visibility.find({
                'user_email': email,
                'hidden': True
            })
            
            # Extract university IDs
            university_ids = [record['university_id'] for record in records]
            return university_ids
            
        except Exception as e:
            logger.error(f"Error getting hidden university IDs: {str(e)}")
            return []
    
    def unhide_all_universities(self, email: str) -> Dict:
        """Make all universities visible for a user"""
        try:
            # Update all visibility records to not hidden
            result = self.db.university_visibility.update_many(
                {'user_email': email},
                {'$set': {'hidden': False, 'last_updated': datetime.utcnow()}}
            )
            
            return {
                'success': True,
                'updated': result.modified_count
            }
            
        except Exception as e:
            logger.error(f"Error unhiding universities: {str(e)}")
            return {'error': str(e)}
        
    def add_pending_column(self, email: str, column_id: str, column_name: str) -> Dict:
        """
        Track columns added while a user's subscription was expired.
        These will be processed for hidden universities when subscription is reactivated.
        """
        try:
            # Add pending column record
            result = self.db.pending_columns.update_one(
                {'user_email': email, 'column_id': column_id},
                {'$set': {
                    'user_email': email,
                    'column_id': column_id,
                    'name': column_name,
                    'added_at': datetime.utcnow(),
                    'processed': False
                }},
                upsert=True
            )
            
            return {
                'success': True,
                'upserted': result.upserted_id is not None,
                'modified': result.modified_count > 0
            }
            
        except Exception as e:
            logger.error(f"Error adding pending column: {str(e)}")
            return {'error': str(e)}

    def get_pending_columns(self, email: str) -> List[Dict]:
        """Get list of columns that need to be processed after subscription reactivation"""
        try:
            # Find all unprocessed pending columns
            pending_columns = list(self.db.pending_columns.find({
                'user_email': email,
                'processed': False
            }))
            
            # Convert ObjectId to string
            for column in pending_columns:
                if '_id' in column:
                    column['_id'] = str(column['_id'])
                
            return pending_columns
            
        except Exception as e:
            logger.error(f"Error getting pending columns: {str(e)}")
            return []

    def mark_pending_columns_processed(self, email: str, column_ids: List[str] = None) -> Dict:
        """Mark pending columns as processed"""
        try:
            filter_query = {'user_email': email}
            
            # If specific column IDs provided, only mark those
            if column_ids:
                filter_query['column_id'] = {'$in': column_ids}
            
            # Update pending columns to processed
            result = self.db.pending_columns.update_many(
                filter_query,
                {'$set': {
                    'processed': True,
                    'processed_at': datetime.utcnow()
                }}
            )
            
            return {
                'success': True,
                'processed_count': result.modified_count
            }
            
        except Exception as e:
            logger.error(f"Error marking pending columns as processed: {str(e)}")
            return {'error': str(e)}

    def create_feedback(self, feedback_data: dict) -> str:
        """Create a new feedback record and return the ID"""
        try:
            # Add ID
            feedback_data['_id'] = str(ObjectId())
            
            # Insert into database
            self.db.feedback.insert_one(feedback_data)
            
            return feedback_data['_id']
        except Exception as e:
            logger.error(f"Error creating feedback: {str(e)}")
            return None

    def get_feedback(self, query: dict, limit: int = 100, skip: int = 0) -> list:
        """Get feedback items with pagination"""
        try:
            # Get feedback from database with sorting
            cursor = self.db.feedback.find(
                query
            ).sort('created_at', -1).skip(skip).limit(limit)
            
            # Convert to list and format dates
            feedback_items = []
            for item in cursor:
                # Convert ObjectId to string
                item['_id'] = str(item['_id'])
                
                # Format dates
                if 'created_at' in item:
                    item['created_at'] = item['created_at'].isoformat()
                if 'updated_at' in item:
                    item['updated_at'] = item['updated_at'].isoformat()
                if 'responded_at' in item:
                    item['responded_at'] = item['responded_at'].isoformat()
                
                feedback_items.append(item)
                
            return feedback_items
        except Exception as e:
            logger.error(f"Error getting feedback: {str(e)}")
            return []

    def count_feedback(self, query: dict) -> int:
        """Count feedback items matching query"""
        try:
            return self.db.feedback.count_documents(query)
        except Exception as e:
            logger.error(f"Error counting feedback: {str(e)}")
            return 0

    def get_feedback_by_id(self, feedback_id: str) -> dict:
        """Get specific feedback by ID"""
        try:
            feedback = self.db.feedback.find_one({'_id': feedback_id})
            
            if not feedback:
                return None
                
            # Convert ObjectId to string and format dates
            feedback['_id'] = str(feedback['_id'])
            if 'created_at' in feedback:
                feedback['created_at'] = feedback['created_at'].isoformat()
            if 'updated_at' in feedback:
                feedback['updated_at'] = feedback['updated_at'].isoformat()
            if 'responded_at' in feedback:
                feedback['responded_at'] = feedback['responded_at'].isoformat()
                
            return feedback
        except Exception as e:
            logger.error(f"Error getting feedback by ID: {str(e)}")
            return None

    def update_feedback(self, feedback_id: str, update_data: dict) -> bool:
        """Update feedback status or other fields"""
        try:
            # Add a timestamp to the update if not already present
            if 'updated_at' not in update_data:
                update_data['updated_at'] = datetime.utcnow()
                
            # Update in database
            result = self.db.feedback.update_one(
                {'_id': feedback_id},
                {'$set': update_data}
            )
            
            # Return True if successful (document was found), False otherwise
            return result.matched_count > 0
        except Exception as e:
            logger.error(f"Error updating feedback: {str(e)}")
            return False

    def delete_feedback(self, feedback_id: str) -> bool:
        """Delete feedback record (admin only)"""
        try:
            # Delete from database
            result = self.db.feedback.delete_one({'_id': feedback_id})
            
            # Return True if successful, False otherwise
            return result.deleted_count > 0
        except Exception as e:
            logger.error(f"Error deleting feedback: {str(e)}")
            return False