import razorpay
from typing import Dict
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class PaymentService:
    def __init__(self, key_id: str, key_secret: str):
        self.client = razorpay.Client(auth=(key_id, key_secret))
    
    def create_order(self, amount: int, user_email: str) -> Dict:
        try:
            data = {
                'amount': amount * 100,  # Convert to paise
                'currency': 'INR',
                'payment_capture': 1,
                'notes': {
                    'user_email': user_email,
                    'plan': 'premium',
                    'valid_until': (datetime.utcnow() + timedelta(days=30)).isoformat()
                }
            }
            order = self.client.order.create(data=data)
            return {
                'success': True, 
                'order': order
            }
        except Exception as e:
            logger.error(f"Order creation failed: {str(e)}")
            return {
                'success': False, 
                'error': str(e)
            }
    
    def verify_payment(self, payment_id: str, order_id: str, signature: str) -> bool:
        try:
            params_dict = {
                'razorpay_payment_id': payment_id,
                'razorpay_order_id': order_id,
                'razorpay_signature': signature
            }
            self.client.utility.verify_payment_signature(params_dict)
            return True
        except Exception as e:
            logger.error(f"Payment verification failed: {str(e)}")
            return False
            
    def get_payment_details(self, payment_id: str) -> Dict:
        try:
            payment = self.client.payment.fetch(payment_id)
            return {
                'success': True,
                'payment': payment
            }
        except Exception as e:
            logger.error(f"Failed to fetch payment details: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def create_subscription(self, email: str) -> Dict:
        try:
            plan = {
                'period': 'monthly',
                'interval': 1,
                'item': {
                    'name': 'Premium Subscription',
                    'amount': 2000,
                    'currency': 'INR'
                }
            }
            subscription = self.client.subscription.create({
                'plan_id': plan['id'],
                'customer_notify': 1,
                'notes': {'user_email': email}
            })
            return {'success': True, 'subscription': subscription}
        except Exception as e:
            logger.error(f"Subscription creation failed: {str(e)}")
            return {'success': False, 'error': str(e)}