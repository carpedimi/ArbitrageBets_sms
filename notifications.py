import os
from twilio.rest import Client
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()

class SMSNotifier:
    def __init__(self):
        self.account_sid = os.getenv('TWILIO_ACCOUNT_SID')
        self.auth_token = os.getenv('TWILIO_AUTH_TOKEN')
        self.from_number = os.getenv('TWILIO_FROM_NUMBER')
        self.to_number = os.getenv('NOTIFICATION_TO_NUMBER')
        self.min_profit_threshold = float(os.getenv('MIN_PROFIT_THRESHOLD', 1.05))
        
        if not all([self.account_sid, self.auth_token, self.from_number, self.to_number]):
            raise ValueError("Missing required Twilio configuration in environment variables")
        
        self.client = Client(self.account_sid, self.auth_token)
    
    def send_arbitrage_notification(self, event_name, market_type, profit_ratio, toto_odds, kambi_odds):
        """
        Send SMS notification for an arbitrage opportunity.
        
        Args:
            event_name (str): Name of the sporting event
            market_type (str): Type of bet market
            profit_ratio (float): Calculated profit ratio
            toto_odds (float): Odds from Toto
            kambi_odds (float): Odds from Kambi
        """
        if profit_ratio < self.min_profit_threshold:
            return
        
        message = (
            f"🎯 Arbitrage Opportunity!\n"
            f"Event: {event_name}\n"
            f"Market: {market_type}\n"
            f"Profit Ratio: {profit_ratio:.2%}\n"
            f"Toto Odds: {toto_odds:.2f}\n"
            f"Kambi Odds: {kambi_odds:.2f}"
        )
        
        try:
            self.client.messages.create(
                body=message,
                from_=self.from_number,
                to=self.to_number
            )
            logging.info(f"SMS notification sent successfully for {event_name}")
        except Exception as e:
            logging.error(f"Failed to send SMS notification: {str(e)}")

# Initialize the notifier
sms_notifier = None

def get_notifier():
    global sms_notifier
    if sms_notifier is None:
        sms_notifier = SMSNotifier()
    return sms_notifier 