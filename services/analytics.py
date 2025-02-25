from datetime import datetime, timedelta
from calendar import month_abbr
import logging

logger = logging.getLogger(__name__)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def get_total_revenue(db) -> float:
    """Calculate total revenue from premium subscriptions"""
    try:
        # Find all premium users
        premium_users = db.db.users.count_documents({
            'is_premium': True,
            'subscription.status': 'active'
        })
        
        # Calculate total revenue (₹1000 per premium user)
        total_revenue = premium_users * 1000
        return total_revenue
        
    except Exception as e:
        logger.error(f"Error calculating total revenue: {str(e)}")
        return 0

def get_monthly_growth(db) -> list:
    """Get monthly user growth data"""
    try:
        pipeline = [
            {
                '$group': {
                    '_id': {
                        'year': {'$year': '$created_at'},
                        'month': {'$month': '$created_at'}
                    },
                    'users': {'$sum': 1},
                    'premiumUsers': {
                        '$sum': {'$cond': [{'$eq': ['$is_premium', True]}, 1, 0]}
                    }
                }
            },
            {'$sort': {'_id.year': 1, '_id.month': 1}},
            {'$limit': 12}
        ]
        
        results = list(db.db.users.aggregate(pipeline))
        
        # Format results
        formatted_results = []
        for result in results:
            month = result['_id']['month']
            month_name = month_abbr[month]
            formatted_results.append({
                'month': month_name,
                'users': result['users'],
                'premiumUsers': result['premiumUsers']
            })
            
        return formatted_results
        
    except Exception as e:
        logger.error(f"Error getting monthly growth: {str(e)}")
        return []


def get_user_activity(db) -> list:
    """Get recent user activity data"""
    try:
        pipeline = [
            {'$sort': {'timestamp': -1}},
            {'$limit': 30},
            {
                '$group': {
                    '_id': {
                        'date': {'$dateToString': {'format': '%Y-%m-%d', 'date': '$timestamp'}}
                    },
                    'searches': {
                        '$sum': {'$cond': [{'$eq': ['$type', 'search']}, 1, 0]}
                    },
                    'exports': {
                        '$sum': {'$cond': [{'$eq': ['$type', 'export']}, 1, 0]}
                    }
                }
            },
            {'$sort': {'_id.date': 1}}
        ]
        
        results = list(db.db.activity_logs.aggregate(pipeline))
        
        # Format results
        formatted_results = []
        for result in results:
            formatted_results.append({
                'date': result['_id']['date'],
                'searches': result['searches'],
                'exports': result['exports']
            })
            
        return formatted_results
        
    except Exception as e:
        logger.error(f"Error getting user activity: {str(e)}")
        return []