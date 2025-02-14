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
        self.client = MongoClient(uri)
        self.db = self.client['university_tracker']
        # Initialize collections as class attributes
        self.users = self.db.users
        self.universities = self.db.universities
        self.user_data = self.db.user_data
        self.custom_columns = self.db.custom_columns
        self.column_data = self.db.column_data
        
        # Ensure indexes
        self._ensure_indexes()

    def _ensure_indexes(self):
        # Existing indexes
        self.users.create_index('email', unique=True)
        self.universities.create_index('url', unique=True)
        self.user_data.create_index([('user_email', 1), ('university_url', 1)])

        # Add new indexes for custom columns
        self.custom_columns.create_index([
            ('created_by', 1),
            ('name', 1)
        ])
        self.column_data.create_index([
            ('user_email', 1),
            ('university_id', 1),
            ('column_id', 1)
        ])

    def create_user(self, data: Dict) -> Dict:
        try:
            if self.users.find_one({'email': data['email']}):  # Changed from users
                return {'error': 'Email already exists'}
            
            user = {
                'email': data['email'],
                'password': generate_password_hash(data['password']),
                'is_premium': False,
                'is_admin': data['email'].endswith('@admin.com'),
                'selected_universities': [],
                'created_at': datetime.utcnow(),
                'last_login': datetime.utcnow(),
                'subscription': {
                    'status': 'free',
                    'expiry': None,
                    'payment_history': []
                }
            }
            
            result = self.users.insert_one(user)  # Changed from users
            user_without_password = {k: v for k, v in user.items() if k != 'password'}
            return user_without_password
            
        except Exception as e:
            logger.error(f"Error creating user: {str(e)}")
            return {'error': str(e)}

    def verify_user(self, data: Dict) -> Dict:
        try:
            user = self.users.find_one({'email': data['email']})  # Changed
            if not user:
                return {'error': 'User not found'}
            
            if not check_password_hash(user['password'], data['password']):
                return {'error': 'Invalid password'}
            
            self.users.update_one(  # Changed
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
            user = self.users.find_one(  # Changed
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
            universities = list(self.universities.find(  # Changed
                query or {},
                {'_id': 0}
            ))
            return universities
        except Exception as e:
            logger.error(f"Error getting universities: {str(e)}")
            return []
        
    def add_university(self, data: Dict) -> Dict:
        try:
            # Check if university already exists
            existing = self.universities.find_one({'url': data['url']})
            
            # Generate a unique ID if not exists
            if not existing:
                data['id'] = str(ObjectId())
                data['created_at'] = datetime.utcnow()
                data['last_updated'] = datetime.utcnow()
                
                result = self.universities.insert_one(data)
                if result.inserted_id:
                    return {
                        'id': data['id'],
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
                        'metadata': data.get('metadata', {})
                    }
                }
                
                self.universities.update_one(
                    {'url': data['url']},
                    update_data
                )
                
                return {
                    'id': existing.get('id', str(existing['_id'])),
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
            result = self.universities.delete_one({'id': university_id})
            if result.deleted_count:
                return {'success': True}
            return {'error': 'University not found'}
        except Exception as e:
            logger.error(f"Error deleting university: {str(e)}")
            return {'error': str(e)}

    def get_users(self) -> List[Dict]:
        """Get all users from the database (excluding sensitive information)"""
        try:
            users = list(self.users.find(
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
            university = self.universities.find_one({'id': university_id})
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
                self.users.update_one(  # Changed
                    {'email': email},
                    {'$set': {'selected_universities': []}}
                )
                if universities:
                    update_doc['$addToSet'] = {
                        'selected_universities': {'$each': universities}
                    }

            result = self.users.update_one(  # Changed
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

            result = self.users.update_one(
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
            
            result = self.users.update_one(
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
            university = self.universities.find_one({'url': url})
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
            universities = list(self.universities.find({
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
            
            result = self.custom_columns.insert_one(column)
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
            cursor = self.custom_columns.find({
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
            
            result = self.column_data.update_one(
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
            cursor = self.column_data.find({
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
            return list(self.custom_columns.find())
        except Exception as e:
            logger.error(f"Error getting all columns: {str(e)}")
            return []

    def get_users_with_column(self, column_id: str) -> List[str]:
        """Get all users who have this column"""
        try:
            # Find all unique user emails that have used this column
            users = self.column_data.distinct('user_email', {'column_id': column_id})
            # If no users found, include the admin user
            if not users:
                admin_users = self.users.find({'is_admin': True}, {'email': 1})
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
            result = self.column_data.update_one(
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
            column = self.custom_columns.find_one({'_id': obj_id})
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
                    column_result = self.custom_columns.delete_one(
                        {'_id': obj_id},
                        session=session
                    )
                    
                    if column_result.deleted_count == 0:
                        return {'error': 'Column not found'}
                    
                    # Delete all associated column data
                    self.column_data.delete_many(
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
            data_count = self.column_data.count_documents({'column_id': column_id})
            
            # Get unique universities using this column
            unique_universities = len(self.column_data.distinct('university_id', {'column_id': column_id}))
            
            # Get unique users using this column
            unique_users = len(self.column_data.distinct('user_email', {'column_id': column_id}))
            
            return {
                'data_entries': data_count,
                'universities': unique_universities,
                'users': unique_users
            }
            
        except Exception as e:
            logger.error(f"Error getting column usage: {str(e)}")
            return {'error': str(e)}