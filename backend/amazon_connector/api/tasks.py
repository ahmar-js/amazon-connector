# api/tasks.py
from datetime import timedelta
from celery import shared_task
from django.conf import settings
import logging
from .models import MarketplaceLastRun
from django.utils import timezone
import os, json
import requests
from .marketplaces import get_marketplace_id, get_available_marketplaces

logger = logging.getLogger(__name__)

def get_access_token():
    """
    Get access token 
    """
    # Go one directory up to reach amazon_connector/
    base_dir = os.path.dirname(os.path.dirname(__file__))
    creds_path = os.path.join(base_dir, 'creds.json')
    
    with open(creds_path, 'r') as f:
        creds = json.load(f)
    return creds['access_token']

@shared_task(bind=True, queue='fetching')
def fetch_amazon_data(self):
    try:
        print("Fetching Amazon data...")
        logger.info("Fetching Amazon data...")

        # Use centralized marketplace mapping (IDs) instead of hard-coded literals
        # Default to all marketplace IDs; adjust to target specific codes if needed
        marketplaces = list(get_available_marketplaces().values())
        access_token = get_access_token()
        logger.info(f"access token: {access_token}")

        for marketplace in marketplaces:
            marketplace_last_run = MarketplaceLastRun.objects.get(marketplace_id=marketplace)
            # marketplace_last_run, created = MarketplaceLastRun.objects.get_or_create(marketplace_id=marketplace)

            if marketplace_last_run.last_run is None:
                logger.info("Inside IF statement")
                start_date = "2024-03-04T00:00:00Z"
                end_date = "2024-03-04T23:59:59Z"
                # start_date = (timezone.now() - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0).isoformat().replace('+00:00', 'Z')
                # end_date = (timezone.now() - timedelta(days=1)).replace(hour=23, minute=59, second=59, microsecond=999999).isoformat().replace('+00:00', 'Z')
                logger.info(f"start_date: {start_date}, end_date: {end_date} inside IF Statement")
            else:
                logger.info("Inside ELSE statement")
                start_date = "2025-03-04T00:00:00Z"
                end_date = "2025-03-04T23:59:59Z"
                # start_date = marketplace_last_run.last_run.isoformat().replace('+00:00', 'Z')
                # end_date = timezone.now().replace(hour=23, minute=59, second=59, microsecond=999999).isoformat().replace('+00:00', 'Z')
                logger.info(f"start_date: {start_date}, end_date: {end_date} inside ELSE Statement")

            logger.info(f"Marketplace: {marketplace}, Start: {start_date}, End: {end_date}")

            payload = {
                "access_token": access_token,
                "marketplace_id": marketplace,
                "start_date": str(start_date),
                "end_date": str(end_date),
                "auto_save": True,
            }
            # logger.info("payload: ", payload)
            logger.info(f"payload: {payload}")

            # Call your Django API
            response = requests.post("http://127.0.0.1:8000/api/fetch-data/", json=payload)

            logger.info(f"API Response ({response.status_code})")
            # timestamp = timezone.now().strftime('%Y%m%d_%H%M%S')
            # filename = f'amazon_response_{marketplace}_{timestamp}.txt'
            # file_path = os.path.join(os.path.dirname(__file__), 'responses', filename)
            # os.makedirs(os.path.dirname(file_path), exist_ok=True)

            # with open(file_path, 'w', encoding='utf-8') as f:
            #     f.write(response.text)

            # logger.info(f"Response saved to {file_path}")

            if response.status_code == 200:
                # Save last run time only if successful
                marketplace_last_run.last_run = timezone.now()
                marketplace_last_run.save()
            else:
                logger.warning(f"Failed to fetch data for {marketplace}: {response.status_code}")
                raise Exception(f"Fetch failed with {response.status_code}: {response.text}")

    except Exception as exc:
        logger.error(f"Task failed: {exc}")
        raise self.retry(exc=exc, countdown=60, max_retries=3)

@shared_task(bind=True, queue='reports')
def generate_reports(self):
    """
    Asynchronous task to generate reports
    """
    try:
        print("Generating reports...")
        # Default to all marketplace codes
        marketplaces = list(get_available_marketplaces().keys())
        payload = {
            "marketplaces": marketplaces,
        }
        response = requests.post("http://127.0.0.1:8000/api/inventory/reports/", json=payload)
        logger.info(f"API Response ({response.status_code})")

        
    except Exception as exc:
        logger.error(f"Report generation task failed: {exc}")
        raise self.retry(exc=exc, countdown=60, max_retries=3)

@shared_task(bind=True, queue='syncing')
def sync_amazon_data(self, data, **kwargs):
    """
    Asynchronous task to sync processed data to databases
    """
    try:
        print("Syncing Amazon data...")
        # Move your database sync logic here
        pass
    except Exception as exc:
        logger.error(f"Sync task failed: {exc}")
        raise self.retry(exc=exc, countdown=60, max_retries=3)