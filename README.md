# ArbitrageBets

A Python application that finds arbitrage betting opportunities and sends SMS notifications when profitable opportunities are found.

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Set up environment variables:
- Copy `.env.example` to `.env`
- Fill in your Twilio credentials and phone numbers
```bash
cp .env.example .env
```

3. Configure Twilio:
- Sign up for a Twilio account at https://www.twilio.com
- Get your Account SID and Auth Token from the Twilio Console
- Buy a phone number to send SMS from
- Update the `.env` file with your credentials

## Google Cloud Deployment

1. Install Google Cloud SDK:
- Follow instructions at https://cloud.google.com/sdk/docs/install

2. Initialize your project:
```bash
gcloud init
```

3. Set up environment variables in Google Cloud:
```bash
gcloud secrets create twilio-config --data-file=.env
```

4. Deploy to App Engine:
```bash
gcloud app deploy
```

## Local Development

Run the application locally:
```bash
python ArbSignal_Football.py
```

## Features

- Automated arbitrage opportunity detection
- Real-time SMS notifications via Twilio
- Support for multiple betting markets:
  - Winner markets
  - Over/Under markets
- Configurable profit threshold for notifications
- Google Cloud App Engine deployment ready
- Comprehensive logging

## Configuration

Adjust the following settings in `.env`:
- `MIN_PROFIT_THRESHOLD`: Minimum profit ratio to trigger notifications (default: 1.05)
- `TWILIO_FROM_NUMBER`: Your Twilio phone number
- `NOTIFICATION_TO_NUMBER`: The phone number to receive notifications

## Logging

Logs are available:
- Locally in the console
- In Google Cloud Logging when deployed