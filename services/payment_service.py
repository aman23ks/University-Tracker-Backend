# /backend/services/payment_service.py

import razorpay
from typing import Dict
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class PaymentService:
    def __init__(self, key_id: str, key_secret: str):
        self.client = razorpay.Client(auth=(key_id, key_secret))
        self.key_id = key_id
        self.key_secret = key_secret
    
    def create_order(self, amount: int, user_email: str) -> Dict:
        try:
            data = {
                'amount': amount,  # Amount should already be in paise
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
                    'amount': 2000,  # ₹20 in paise
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

    def verify_subscription_payment(self, data: Dict) -> bool:
        try:
            self.client.utility.verify_payment_signature(data)
            return True
        except Exception as e:
            logger.error(f"Subscription payment verification failed: {str(e)}")
            return False

    def fetch_subscription(self, subscription_id: str) -> Dict:
        try:
            subscription = self.client.subscription.fetch(subscription_id)
            return {
                'success': True,
                'subscription': subscription
            }
        except Exception as e:
            logger.error(f"Failed to fetch subscription: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }