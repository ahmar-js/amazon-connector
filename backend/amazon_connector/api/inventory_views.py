import requests
import pandas as pd
import json
import time
import os
import logging
from datetime import datetime, timedelta, timezone
from django.http import JsonResponse
from django.views import View
from django.utils.decorators import method_decorator
from django.views.decorators.csrf import csrf_exempt
from django.conf import settings
from pathlib import Path
from .models import Activities
import uuid
import pytz
from .inventory_mssql import save_inventory_report_to_mssql
from .inventory_azure import save_inventory_report_to_azure
from .marketplaces import MARKETPLACE_IDS, get_region_from_marketplace_id, get_available_marketplaces
from .marketplaces_creds import CREDENTIALS, find_credential_group_for_marketplace, normalize_company_name, ACTIVE_COMPANIES, GROUP_TO_COMPANY
# Set up logging
logger = logging.getLogger(__name__)

# Marketplace mapping centralized in backend.amazon_connector.marketplaces

# Rate limiting for Amazon SP API (0.0222 requests/second, burst of 10)
RATE_LIMIT_DELAY = 45  # seconds between requests (1/0.0222)
MAX_BURST_REQUESTS = 10

class FetchInventoryReport:
    def __init__(self, refresh_token, lwa_client_id, lwa_client_secret, region, marketplace_id):
        self.refresh_token = refresh_token
        self.lwa_client_id = lwa_client_id
        self.lwa_client_secret = lwa_client_secret
        self.region = region
        self.marketplace_id = marketplace_id

    def get_access_token(self):
        """Get access token using refresh token"""
        url = "https://api.amazon.com/auth/o2/token"
        data = {
            "grant_type": "refresh_token",
            "refresh_token": self.refresh_token,
            "client_id": self.lwa_client_id,
            "client_secret": self.lwa_client_secret
        }
        
        try:
            response = requests.post(url, data=data, timeout=30)
            response.raise_for_status()
            
            token_data = response.json()
            if "access_token" not in token_data:
                raise ValueError("Access token not found in response")
            
            return token_data["access_token"]
        except requests.exceptions.Timeout:
            logger.error("Timeout while getting access token from Amazon")
            raise Exception("Amazon API timeout during authentication")
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error while getting access token: {e}")
            raise Exception(f"Amazon API authentication failed: {e}")
        except (KeyError, ValueError) as e:
            logger.error(f"Invalid response from Amazon auth API: {e}")
            raise Exception("Invalid response from Amazon authentication API")

    def fetch_reports(self, created_after=None, created_before=None):
        """Fetch inventory reports from Amazon SP API"""
        access_token = self.get_access_token()
        url = f"https://sellingpartnerapi-{self.region}.amazon.com/reports/2021-06-30/reports"
        
        params = {
            "reportTypes": "GET_FBA_MYI_ALL_INVENTORY_DATA",
            "processingStatuses": "DONE",
            "marketplaceIds": self.marketplace_id,
            "pageSize": 100  # Default 10 - maximum 100
        }
        
        # Add date filters if provided
        # if created_after:
        #     params["createdSince"] = created_after
        # if created_before:
        #     params["createdUntil"] = created_before
            
        headers = {
            "x-amz-access-token": access_token,
            "accept": "application/json",
            "User-Agent": "AmazonConnector/1.0"
        }
        
        # Rate limiting
        time.sleep(RATE_LIMIT_DELAY / MAX_BURST_REQUESTS)
        
        try:
            response = requests.get(url, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            logger.info(f"data: {data}")
            if not isinstance(data, dict):
                raise ValueError("Invalid response format from Amazon API")
            
            return data.get("reports", [])
        except requests.exceptions.Timeout:
            logger.error("Timeout while fetching reports from Amazon")
            raise Exception("Amazon API timeout during report fetch")
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error while fetching reports: {e}")
            raise Exception(f"Amazon API report fetch failed: {e}")
        except (KeyError, ValueError, TypeError) as e:
            logger.error(f"Invalid response from Amazon reports API: {e}")
            raise Exception("Invalid response from Amazon reports API")

    def get_document_info(self, report_id):
        """Get document ID for a specific report"""
        access_token = self.get_access_token()
        logger.info(f"Report ID: {report_id}");
        url = f"https://sellingpartnerapi-{self.region}.amazon.com/reports/2021-06-30/reports/{report_id}"
        headers = {
            "x-amz-access-token": access_token,
            "accept": "application/json",
            "User-Agent": "AmazonConnector/1.0"
        }
        
        # Rate limiting
        time.sleep(RATE_LIMIT_DELAY / MAX_BURST_REQUESTS)
        
        try:
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            logger.info(f"Document data: {data}")
            if "reportDocumentId" not in data:
                raise ValueError("Report document ID not found in response")
            
            return data["reportDocumentId"]
        except requests.exceptions.Timeout:
            logger.error(f"Timeout while getting document info for report {report_id}")
            raise Exception("Amazon API timeout during document info fetch")
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error while getting document info: {e}")
            raise Exception(f"Amazon API document info fetch failed: {e}")
        except (KeyError, ValueError) as e:
            logger.error(f"Invalid response from Amazon document API: {e}")
            raise Exception("Invalid response from Amazon document API")

    def get_presigned_url(self, access_token, document_id):
        """Get presigned URL for downloading report document"""
        url = f"https://sellingpartnerapi-{self.region}.amazon.com/reports/2021-06-30/documents/{document_id}"
        headers = {
            "x-amz-access-token": access_token,
            "accept": "application/json",
            "User-Agent": "AmazonConnector/1.0"
        }
        
        # Rate limiting
        time.sleep(RATE_LIMIT_DELAY / MAX_BURST_REQUESTS)
        
        try:
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            if "url" not in data:
                raise ValueError("Download URL not found in response")
            
            return data["url"]
        except requests.exceptions.Timeout:
            logger.error(f"Timeout while getting presigned URL for document {document_id}")
            raise Exception("Amazon API timeout during presigned URL fetch")
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error while getting presigned URL: {e}")
            raise Exception(f"Amazon API presigned URL fetch failed: {e}")
        except (KeyError, ValueError) as e:
            logger.error(f"Invalid response from Amazon documents API: {e}")
            raise Exception("Invalid response from Amazon documents API")
    
    def download_and_save_report(self, url, file_path):
        """Download report from presigned URL and save to file"""
        try:
            response = requests.get(url, timeout=300, stream=True)  # 5 minute timeout for large files
            response.raise_for_status()
        
            # Ensure directory exists
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
            # Download with streaming to handle large files
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

            logger.info(f"Report saved to {file_path}")

            # Convert TSV to CSV
            try:
                # Try reading with multiple encodings to handle Windows-1252 characters from Amazon
                encodings_to_try = ['utf-8', 'cp1252', 'latin-1']
                last_exc = None
                df = None
                used_encoding = None
                for enc in encodings_to_try:
                    try:
                        df = pd.read_csv(file_path, sep='\t', encoding=enc)
                        used_encoding = enc
                        break
                    except Exception as e:
                        last_exc = e
                        logger.debug(f"Failed to read TSV with encoding {enc}: {e}")

                if df is None:
                    # As a last resort, read as binary and decode with replacement, then parse via StringIO
                    try:
                        from io import StringIO
                        with open(file_path, 'rb') as bf:
                            raw = bf.read()
                        text = raw.decode('utf-8', errors='replace')
                        df = pd.read_csv(StringIO(text), sep='\t')
                        used_encoding = 'utf-8-replace'
                        logger.warning(f"Used fallback decoding with errors='replace' for file {file_path}")
                    except Exception as e:
                        logger.error(f"All encoding attempts failed for {file_path}: {e}")
                        raise last_exc or e

                if df is None or df.empty:
                    logger.warning(f"Downloaded file is empty or contains no valid data: {file_path}")
                    return file_path, 0

                csv_path = file_path.replace('.tsv', '.csv')
                df.to_csv(csv_path, index=False)
                logger.info(f"Converted and saved to {csv_path} with {len(df)} rows (encoding={used_encoding})")
                return csv_path, len(df)
            except pd.errors.EmptyDataError:
                logger.warning(f"Downloaded file contains no data: {file_path}")
                return file_path, 0
        except Exception as e:
            logger.error(f"Error converting TSV to CSV: {e}")
            return file_path, 0
            
        except requests.exceptions.Timeout:
            logger.error(f"Timeout while downloading report from {url}")
            raise Exception("Timeout during report download")
        except requests.exceptions.RequestException as e:
            logger.error(f"Error downloading report: {e}")
            raise Exception(f"Report download failed: {e}")
        except IOError as e:
            logger.error(f"Error saving report to {file_path}: {e}")
            raise Exception(f"Failed to save report: {e}")


@method_decorator(csrf_exempt, name='dispatch')
class FetchInventoryReportView(View):
    """
    Django view to fetch Amazon inventory reports for the previous day
    """
    
    def load_credentials(self):
        """Load credentials from creds_inventory.json file"""
        try:
            creds_path = Path(__file__).parent.parent / "creds_inventory.json"
            
            # Security check: ensure file exists and is readable
            if not creds_path.exists():
                raise FileNotFoundError("Credentials file not found. Please configure Amazon API credentials first.")
            
            if not creds_path.is_file():
                raise ValueError("Credentials path is not a file")
            
            with open(creds_path, 'r') as f:
                creds = json.load(f)
            
            # Validate required fields
            required_fields = ['refresh_token', 'app_id', 'client_secret']
            missing_fields = [field for field in required_fields if not creds.get(field)]
            
            if missing_fields:
                raise ValueError(f"Missing required credential fields: {missing_fields}")
            
            return creds
        except FileNotFoundError as e:
            logger.error(f"Credentials file not found: {e}")
            raise Exception("Credentials file not found. Please configure Amazon API credentials first.")
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in credentials file: {e}")
            raise Exception("Invalid credentials file format. Please check your creds_inventory.json file.")
        except Exception as e:
            logger.error(f"Failed to load credentials: {e}")
            raise Exception(f"Credentials loading failed: {e}")
    
    def get_region_from_marketplace(self, marketplace_id):
        """Determine region based on marketplace ID"""
        return get_region_from_marketplace_id(marketplace_id)
    
    def create_activity_record(self, marketplace_id, date_from, date_to, company_name=''):
        """Get or create an activity record for tracking.
        Uses get_or_create so re-running on the same day reuses the existing in_progress record
        instead of violating the unique constraint.
        """
        activity = Activities.objects.create(
            company_name=company_name,
            marketplace_id=marketplace_id,
            activity_type='reports',
            date_from=date_from,
            date_to=date_to,
            action = 'manual',
            status='in_progress',
            # activtiy_id=uuid.uuid4(),
        )
        return activity
    
    def update_activity_record(self, activity, status, items_fetched=0, duration_seconds=None, error_message=None, 
                             mssql_saved=False, azure_saved=False, detail_message=None):
        """Update activity record with results"""
        activity.status = status
        activity.items_fetched = items_fetched
        if duration_seconds:
            activity.duration_seconds = duration_seconds
        if error_message:
            activity.error_message = error_message
        
        # Update database save status
        activity.mssql_saved = mssql_saved
        activity.azure_saved = azure_saved
        activity.database_saved = mssql_saved or azure_saved
        
        # Update detail message if provided
        if detail_message:
            activity.detail = detail_message
        
        activity.save()
    
    def post(self, request):
        """Handle POST request to fetch inventory reports"""
        start_time = time.time()
        
        try:
            try:
                data = json.loads(request.body) if request.body else {}
                logger.info(f"Data: {data}")
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON in request: {e}")
                return JsonResponse({
                    'success': False,
                    'error': 'Invalid JSON format',
                    'details': str(e)
                }, status=400)
            
            # Accept 'warehouse_codes' or 'marketplaces'; default to all
            default_marketplaces = list(get_available_marketplaces().keys())
            marketplaces = data.get('warehouse_codes') or data.get('marketplaces', default_marketplaces)
            if isinstance(marketplaces, str):
                marketplaces = [marketplaces]
            
            invalid_marketplaces = [mp for mp in marketplaces if mp not in MARKETPLACE_IDS]
            if invalid_marketplaces:
                return JsonResponse({
                    'success': False,
                    'error': f'Invalid marketplace(s): {invalid_marketplaces}',
                    'valid_marketplaces': list(MARKETPLACE_IDS.keys())
                }, status=400)

            # Get date range (previous day)
            today = datetime.now(timezone.utc).date()
            yesterday = today - timedelta(days=1)
            created_after = yesterday.strftime('%Y-%m-%dT00:00:00Z')
            created_before = yesterday.strftime('%Y-%m-%dT23:59:59Z')

            # Group marketplaces by credential group; authenticate once per group
            group_to_codes: dict = {}
            results = {}
            for code in marketplaces:
                marketplace_id = MARKETPLACE_IDS[code]
                try:
                    group = find_credential_group_for_marketplace(marketplace_id)
                except KeyError:
                    results[code] = {'success': False, 'error': f'No credential group found for marketplace {code}'}
                    continue
                group_to_codes.setdefault(group, []).append(code)

            total_reports_found = 0
            total_items_processed = 0

            for group_name, codes in group_to_codes.items():
                creds = CREDENTIALS[group_name]
                logger.info(f"Using credential group '{group_name}' for marketplaces: {codes}")

                for marketplace_code in codes:
                    marketplace_id = MARKETPLACE_IDS[marketplace_code]
                    region = self.get_region_from_marketplace(marketplace_id)
                    company_name_for_activity = GROUP_TO_COMPANY.get(group_name, group_name)

                    activity = self.create_activity_record(
                        marketplace_id=marketplace_id,
                        date_from=yesterday,
                        date_to=yesterday,
                        company_name=company_name_for_activity
                    )

                    try:
                        inventory_fetcher = FetchInventoryReport(
                            refresh_token=creds['refresh_token'],
                            lwa_client_id=creds['app_id'],
                            lwa_client_secret=creds['client_secret'],
                            region=region,
                            marketplace_id=marketplace_id
                        )

                        logger.info(f"Fetching inventory reports for {marketplace_code} ({marketplace_id})")
                        reports = inventory_fetcher.fetch_reports(
                            created_after=created_after,
                            created_before=created_before
                        )

                        if not reports:
                            logger.info(f"No inventory reports found for {marketplace_code}, saving empty record to DB")
                            reports_dir = Path(__file__).parent.parent / "processed_data" / "inventory_reports"
                            reports_dir.mkdir(parents=True, exist_ok=True)
                            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                            csv_path = str(reports_dir / f"inventory_{marketplace_code}_{timestamp}_empty.csv")
                            pd.DataFrame(columns=[
                                'sku', 'fnsku', 'asin', 'product-name', 'condition', 'your-price',
                                'mfn-listing-exists', 'mfn-fulfillable-quantity', 'afn-listing-exists',
                                'afn-warehouse-quantity', 'afn-fulfillable-quantity', 'afn-unsellable-quantity',
                                'afn-reserved-quantity', 'afn-total-quantity', 'per-unit-volume',
                                'afn-inbound-working-quantity', 'afn-inbound-shipped-quantity',
                                'afn-inbound-receiving-quantity',
                            ]).to_csv(csv_path, index=False)
                            synthetic_report = {
                                'reportId': f'EMPTY_{marketplace_code}_{timestamp}',
                                'reportType': 'GET_FBA_MYI_ALL_INVENTORY_DATA',
                                'period': None,
                                'createdTime': datetime.now(timezone.utc).isoformat(),
                                'dataStartTime': yesterday.isoformat(),
                                'dataEndTime': yesterday.isoformat(),
                            }
                            mssql_saved_empty = False
                            azure_saved_empty = False
                            try:
                                mssql_save = save_inventory_report_to_mssql(
                                    csv_path=csv_path,
                                    latest_report=synthetic_report,
                                    marketplace_code=marketplace_code,
                                    items_count=0
                                )
                                mssql_saved_empty = mssql_save.get('success', False)
                            except Exception as mssql_err:
                                logger.error(f"MSSQL empty save failed for {marketplace_code}: {mssql_err}")
                                mssql_save = {'success': False, 'error': str(mssql_err)}
                            try:
                                azure_save = save_inventory_report_to_azure(
                                    csv_path=csv_path,
                                    latest_report=synthetic_report,
                                    marketplace_code=marketplace_code,
                                    items_count=0
                                )
                                azure_saved_empty = azure_save.get('success', False)
                            except Exception as azure_err:
                                logger.error(f"Azure empty save failed for {marketplace_code}: {azure_err}")
                                azure_save = {'success': False, 'error': str(azure_err)}
                            results[marketplace_code] = {
                                'success': True,
                                'reports_found': 0,
                                'credential_group': group_name,
                                'items_count': 0,
                                'message': 'No reports found for yesterday; empty record saved to DB',
                                'activity_id': str(activity.activity_id),
                                'mssql_save': mssql_save,
                                'azure_save': azure_save,
                            }
                            self.update_activity_record(
                                activity=activity,
                                status='completed',
                                items_fetched=0,
                                duration_seconds=time.time() - start_time,
                                mssql_saved=mssql_saved_empty,
                                azure_saved=azure_saved_empty,
                                detail_message='No inventory reports found; empty record saved to DB'
                            )
                            continue

                        latest_report = max(reports, key=lambda x: x['createdTime'])
                        report_id = latest_report['reportId']

                        logger.info(f"Processing report {report_id} for {marketplace_code}")

                        document_id = inventory_fetcher.get_document_info(report_id)
                        access_token = inventory_fetcher.get_access_token()
                        download_url = inventory_fetcher.get_presigned_url(access_token, document_id)

                        reports_dir = Path(__file__).parent.parent / "processed_data" / "inventory_reports"
                        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                        file_path = reports_dir / f"inventory_{marketplace_code}_{timestamp}.tsv"

                        csv_path, items_count = inventory_fetcher.download_and_save_report(
                            download_url, str(file_path)
                        )

                        mssql_saved = False
                        azure_saved = False

                        try:
                            mssql_save = save_inventory_report_to_mssql(
                                csv_path=csv_path,
                                latest_report=latest_report,
                                marketplace_code=marketplace_code,
                                items_count=items_count
                            )
                            logger.info(f"MSSQL save result: {mssql_save}")
                            mssql_saved = mssql_save.get('success', False)
                        except Exception as mssql_err:
                            logger.error(f"MSSQL save failed for {marketplace_code}: {mssql_err}")
                            mssql_save = {'success': False, 'error': str(mssql_err)}

                        try:
                            azure_save = save_inventory_report_to_azure(
                                csv_path=csv_path,
                                latest_report=latest_report,
                                marketplace_code=marketplace_code,
                                items_count=items_count
                            )
                            logger.info(f"Azure save result: {azure_save}")
                            azure_saved = azure_save.get('success', False)
                        except Exception as azure_err:
                            logger.error(f"Azure save failed for {marketplace_code}: {azure_err}")
                            azure_save = {'success': False, 'error': str(azure_err)}

                        if mssql_saved and azure_saved:
                            db_status = 'Both databases saved successfully'
                            detail_suffix = '✓ Both DBs'
                        elif mssql_saved:
                            db_status = 'MSSQL saved, Azure failed'
                            detail_suffix = '✓ MSSQL only'
                        elif azure_saved:
                            db_status = 'Azure saved, MSSQL failed'
                            detail_suffix = '✓ Azure only'
                        else:
                            db_status = 'Both database saves failed'
                            detail_suffix = '✗ DB save failed'

                        detail_message = f'Fetched {items_count} items | {detail_suffix}'
                        total_reports_found += 1
                        total_items_processed += items_count

                        results[marketplace_code] = {
                            'success': True,
                            'reports_found': 1,
                            'credential_group': group_name,
                            'report_id': report_id,
                            'document_id': document_id,
                            'items_count': items_count,
                            'file_path': csv_path,
                            'created_time': latest_report['createdTime'],
                            'activity_id': str(activity.activity_id),
                            'mssql_save': mssql_save,
                            'azure_save': azure_save,
                            'database_status': db_status
                        }

                        self.update_activity_record(
                            activity=activity,
                            status='completed',
                            items_fetched=items_count,
                            duration_seconds=time.time() - start_time,
                            mssql_saved=mssql_saved,
                            azure_saved=azure_saved,
                            detail_message=detail_message
                        )

                        logger.info(f"Successfully processed inventory report for {marketplace_code}")
                        logger.info(f"Database save status - MSSQL: {mssql_saved}, Azure: {azure_saved}")

                    except Exception as e:
                        logger.error(f"Error processing {marketplace_code}: {e}")
                        results[marketplace_code] = {
                            'success': False,
                            'error': str(e),
                            'credential_group': group_name,
                            'activity_id': str(activity.activity_id)
                        }
                        self.update_activity_record(
                            activity=activity,
                            status='failed',
                            items_fetched=0,
                            duration_seconds=time.time() - start_time,
                            error_message=str(e),
                            mssql_saved=False,
                            azure_saved=False,
                            detail_message=f'Failed to process inventory report: {str(e)}'
                        )

            return JsonResponse({
                'success': True,
                'message': f'Inventory report fetch completed for {len(marketplaces)} marketplace(s)',
                'total_reports_found': total_reports_found,
                'total_items_processed': total_items_processed,
                'processing_time_seconds': round(time.time() - start_time, 2),
                'results': results,
                'date_range': {
                    'from': yesterday.isoformat(),
                    'to': yesterday.isoformat()
                }
            })

        except Exception as e:
            logger.error(f"Unexpected error in FetchInventoryReportView: {e}")
            return JsonResponse({
                'success': False,
                'error': 'Internal server error',
                'details': str(e),
                'processing_time_seconds': round(time.time() - start_time, 2)
            }, status=500)
    
    def get(self, request):
        """Handle GET request to show available marketplaces and status"""
        try:
            # Check credentials availability
            try:
                creds = self.load_credentials()
                credentials_status = "Available"
                expires_at = creds.get('expires_at', 'Unknown')
            except Exception as e:
                credentials_status = f"Error: {e}"
                expires_at = None
            
            return JsonResponse({
                'success': True,
                'message': 'Amazon Inventory Report Fetcher',
                'available_marketplaces': get_available_marketplaces(),
                'rate_limits': {
                    'requests_per_second': 0.0222,
                    'burst_limit': 10,
                    'delay_between_requests': f"{RATE_LIMIT_DELAY / MAX_BURST_REQUESTS} seconds"
                },
                'credentials_status': credentials_status,
                'token_expires_at': expires_at,
                'supported_report_type': 'GET_FBA_MYI_ALL_INVENTORY_DATA',
                'date_filter': 'Previous day reports only'
            })
            
        except Exception as e:
            logger.error(f"Error in GET request: {e}")
            return JsonResponse({
                'success': False,
                'error': 'Internal server error',
                'details': str(e)
            }, status=500)


@method_decorator(csrf_exempt, name='dispatch')
class CreateReportScheduleView(View):
    """Create a report schedule for specified marketplace(s)."""

    def load_credentials(self):
        creds_path = Path(__file__).parent.parent / "creds_inventory.json"
        if not creds_path.exists():
            raise Exception("Credentials file not found. Please connect first.")
        with open(creds_path, 'r') as f:
            creds = json.load(f)
        for key in ['refresh_token', 'app_id', 'client_secret']:
            if not creds.get(key):
                raise Exception(f"Missing credential: {key}")
        return creds

    def get_region_from_marketplace(self, marketplace_id):
        return get_region_from_marketplace_id(marketplace_id)

    def _get_access_token(self, creds):
        url = "https://api.amazon.com/auth/o2/token"
        data = {
            "grant_type": "refresh_token",
            "refresh_token": creds['refresh_token'],
            "client_id": creds['app_id'],
            "client_secret": creds['client_secret']
        }
        resp = requests.post(url, data=data, timeout=30)
        resp.raise_for_status()
        token = resp.json().get('access_token')
        if not token:
            raise Exception("Failed to obtain access token")
        return token

    def post(self, request):
        start_time = time.time()
        try:
            body = json.loads(request.body or '{}')
        except json.JSONDecodeError as e:
            return JsonResponse({
                'success': False,
                'error': 'Invalid JSON',
                'details': str(e),
            }, status=400)

        # Inputs
        report_type = body.get('reportType', 'GET_FBA_MYI_ALL_INVENTORY_DATA')
        period = body.get('period', 'PT24H')  # ISO 8601 duration
        next_report_creation_time = body.get('nextReportCreationTime')  # optional local or UTC ISO timestamp
        time_zone = body.get('timeZone', 'Asia/Karachi')  # default to PKT for better UX
        report_options = body.get('reportOptions')  # optional dict
        _raw_company = body.get('companyName')
        company_name = normalize_company_name(_raw_company) if _raw_company else None
        default_marketplaces = list(get_available_marketplaces().keys())
        marketplaces = body.get('marketplaces', default_marketplaces)
        if isinstance(marketplaces, str):
            marketplaces = [marketplaces]

        # Validate marketplace codes
        invalid = [m for m in marketplaces if m not in MARKETPLACE_IDS]
        if invalid:
            return JsonResponse({
                'success': False,
                'error': f'Invalid marketplace codes: {invalid}',
                'valid_marketplaces': list(MARKETPLACE_IDS.keys()),
            }, status=400)

        # Normalize nextReportCreationTime once up front
        normalized_time = None
        if next_report_creation_time:
            try:
                normalized_time = self._normalize_to_utc_z(next_report_creation_time, time_zone)
            except Exception as e:
                return JsonResponse({'success': False, 'error': f'Invalid nextReportCreationTime: {e}'}, status=400)

        # Group marketplaces by credential group so we authenticate once per group
        results = {}
        group_to_codes: dict = {}
        for code in marketplaces:
            marketplace_id = MARKETPLACE_IDS[code]
            try:
                group = find_credential_group_for_marketplace(marketplace_id, company_name)
            except KeyError:
                results[code] = {'success': False, 'error': f'No credential group found for marketplace {code}'}
                continue
            group_to_codes.setdefault(group, []).append(code)

        # Process each credential group with its own access token
        for group_name, codes in group_to_codes.items():
            creds = CREDENTIALS[group_name]
            try:
                access_token = self._get_access_token(creds)
            except Exception as e:
                for code in codes:
                    results[code] = {'success': False, 'error': f'Authentication failed for group {group_name}: {e}'}
                continue

            for code in codes:
                marketplace_id = MARKETPLACE_IDS[code]
                region = self.get_region_from_marketplace(marketplace_id)
                url = f"https://sellingpartnerapi-{region}.amazon.com/reports/2021-06-30/schedules"

                payload = {
                    "reportType": report_type,
                    "marketplaceIds": [marketplace_id],
                    "period": period,
                }
                if normalized_time:
                    payload["nextReportCreationTime"] = normalized_time
                if isinstance(report_options, dict) and report_options:
                    payload["reportOptions"] = report_options

                headers = {
                    'x-amz-access-token': access_token,
                    'accept': 'application/json',
                    'content-type': 'application/json',
                    'User-Agent': 'AmazonConnector/1.0'
                }

                try:
                    resp = requests.post(url, headers=headers, json=payload, timeout=30)
                    resp.raise_for_status()
                    results[code] = {'success': True, 'schedule': resp.json(), 'credential_group': group_name}
                except requests.exceptions.RequestException as e:
                    error_body = e.response.text if getattr(e, 'response', None) is not None else None
                    results[code] = {'success': False, 'error': str(e), 'response': error_body}
                except Exception as e:
                    results[code] = {'success': False, 'error': str(e)}

        return JsonResponse({
            'success': True,
            'results': results,
            'processing_time_seconds': round(time.time() - start_time, 2)
        })

    def _normalize_to_utc_z(self, dt_str: str, local_tz_name: str) -> str:
        """Convert an ISO-like datetime string to UTC '...Z'.
        - If dt_str includes timezone info (e.g., 'Z' or '+05:00'), convert accordingly.
        - If dt_str is naive, interpret in local_tz_name (default Asia/Karachi), then convert to UTC.
        Accepts common forms like 'YYYY-MM-DDTHH:MM:SS' or with 'Z'/'+hh:mm'.
        """
        s = dt_str.strip()
        # Handle 'Z' by making it '+00:00' for fromisoformat compatibility
        z_replaced = s.replace('Z', '+00:00')
        dt_obj = None
        # Try ISO parsing with offset
        try:
            dt_obj = datetime.fromisoformat(z_replaced)
        except Exception:
            dt_obj = None
        if dt_obj is not None:
            if dt_obj.tzinfo is None:
                # Naive: treat as local tz
                try:
                    local_tz = pytz.timezone(local_tz_name)
                except Exception:
                    local_tz = pytz.timezone('Asia/Karachi')
                dt_obj = local_tz.localize(dt_obj)
            # Convert to UTC
            dt_utc = dt_obj.astimezone(timezone.utc)
            return dt_utc.strftime('%Y-%m-%dT%H:%M:%SZ')
        # Fallback: try strict format without offset
        for fmt in ('%Y-%m-%dT%H:%M:%S', '%Y-%m-%d %H:%M:%S'):
            try:
                naive = datetime.strptime(s, fmt)
                local_tz = pytz.timezone(local_tz_name)
                aware = local_tz.localize(naive)
                dt_utc = aware.astimezone(timezone.utc)
                return dt_utc.strftime('%Y-%m-%dT%H:%M:%SZ')
            except Exception:
                continue
        raise ValueError('Unsupported datetime format; use ISO 8601 (e.g., 2025-08-12T00:23:00)')


@method_decorator(csrf_exempt, name='dispatch')
class GetReportSchedulesView(View):
    """List report schedules with optional filters."""

    def load_credentials(self):
        creds_path = Path(__file__).parent.parent / "creds_inventory.json"
        if not creds_path.exists():
            raise Exception("Credentials file not found. Please connect first.")
        with open(creds_path, 'r') as f:
            creds = json.load(f)
        for key in ['refresh_token', 'app_id', 'client_secret']:
            if not creds.get(key):
                raise Exception(f"Missing credential: {key}")
        return creds

    def get_region_from_marketplace(self, marketplace_id):
        return get_region_from_marketplace_id(marketplace_id)

    def _get_access_token(self, creds):
        url = "https://api.amazon.com/auth/o2/token"
        data = {
            "grant_type": "refresh_token",
            "refresh_token": creds['refresh_token'],
            "client_id": creds['app_id'],
            "client_secret": creds['client_secret']
        }
        resp = requests.post(url, data=data, timeout=30)
        resp.raise_for_status()
        token = resp.json().get('access_token')
        if not token:
            raise Exception("Failed to obtain access token")
        return token

    def get(self, request):
        start_time = time.time()
        # query params: marketplaces (codes, comma-separated), companyName, reportTypes
        marketplaces_param = request.GET.get('marketplaces')
        if marketplaces_param:
            marketplaces = [m.strip() for m in marketplaces_param.split(',') if m.strip()]
        else:
            marketplaces = list(get_available_marketplaces().keys())

        invalid = [m for m in marketplaces if m not in MARKETPLACE_IDS]
        if invalid:
            return JsonResponse({
                'success': False,
                'error': f'Invalid marketplace codes: {invalid}',
                'valid_marketplaces': list(MARKETPLACE_IDS.keys()),
            }, status=400)

        _raw_company = request.GET.get('companyName')
        company_name = normalize_company_name(_raw_company) if _raw_company else None
        # reportTypes is REQUIRED by SP-API. Default to inventory report if omitted.
        report_types = request.GET.get('reportTypes') or 'GET_FBA_MYI_ALL_INVENTORY_DATA'

        # Group marketplaces by credential group so we authenticate once per group
        results = {}
        group_to_codes: dict = {}
        for code in marketplaces:
            marketplace_id = MARKETPLACE_IDS[code]
            try:
                group = find_credential_group_for_marketplace(marketplace_id, company_name)
            except KeyError:
                results[code] = {'success': False, 'error': f'No credential group found for marketplace {code}'}
                continue
            group_to_codes.setdefault(group, []).append(code)

        # Collect all schedules across all groups; deduplicate by reportScheduleId
        seen_schedule_ids = set()
        all_schedules = []

        for group_name, codes in group_to_codes.items():
            creds = CREDENTIALS[group_name]
            try:
                access_token = self._get_access_token(creds)
            except Exception as e:
                for code in codes:
                    results[code] = {'success': False, 'error': f'Authentication failed for group {group_name}: {e}'}
                continue

            # SP-API schedules endpoint returns schedules for a credential set,
            # not per-marketplace, so one call per group is enough.
            # We use the first marketplace's region for the endpoint.
            first_marketplace_id = MARKETPLACE_IDS[codes[0]]
            region = self.get_region_from_marketplace(first_marketplace_id)
            url = f"https://sellingpartnerapi-{region}.amazon.com/reports/2021-06-30/schedules"

            params = {'reportTypes': report_types}
            headers = {
                'x-amz-access-token': access_token,
                'accept': 'application/json',
                'User-Agent': 'AmazonConnector/1.0'
            }

            try:
                resp = requests.get(url, headers=headers, params=params, timeout=30)
                resp.raise_for_status()
                data = resp.json()
                raw_schedules = []
                if isinstance(data, dict):
                    raw_schedules = data.get('reportSchedules') or []

                for s in raw_schedules:
                    sid = s.get('reportScheduleId')
                    if sid and sid not in seen_schedule_ids:
                        seen_schedule_ids.add(sid)
                        all_schedules.append(s)

                for code in codes:
                    results[code] = {'success': True, 'credential_group': group_name}
            except requests.exceptions.RequestException as e:
                error_body = e.response.text if getattr(e, 'response', None) is not None else None
                for code in codes:
                    results[code] = {'success': False, 'error': str(e), 'response': error_body}
            except Exception as e:
                for code in codes:
                    results[code] = {'success': False, 'error': str(e)}

        # Reverse map: marketplace_id -> code (e.g. "APJ6JRA9NG5V4" -> "IT")
        id_to_code = {v: k for k, v in MARKETPLACE_IDS.items()}

        # Enrich schedules with the human-readable marketplace code
        for s in all_schedules:
            ids = s.get('marketplaceIds') or []
            s['marketplaceCodes'] = [id_to_code.get(mid, mid) for mid in ids]

        # ── Ambiguity analysis ────────────────────────────────────────────────
        # 1. Schedules with no marketplaceIds (orphaned/global)
        orphaned = [s for s in all_schedules if not s.get('marketplaceIds')]

        # 2. Multiple schedules for the same marketplace (duplicates)
        #    Group by each marketplace_id that appears in a schedule
        marketplace_to_schedules: dict = {}
        for s in all_schedules:
            for mid in (s.get('marketplaceIds') or []):
                marketplace_to_schedules.setdefault(mid, []).append(s)

        duplicate_groups = []
        for mid, scheds in marketplace_to_schedules.items():
            if len(scheds) > 1:
                # Sort by nextReportCreationTime descending; newest = recommended to keep
                def _ts(sc):
                    t = sc.get('nextReportCreationTime') or ''
                    return t

                sorted_scheds = sorted(scheds, key=_ts, reverse=True)
                duplicate_groups.append({
                    'marketplaceId': mid,
                    'marketplaceCode': id_to_code.get(mid, mid),
                    'count': len(sorted_scheds),
                    'recommended_keep': sorted_scheds[0].get('reportScheduleId'),
                    'recommended_cancel': [s.get('reportScheduleId') for s in sorted_scheds[1:]],
                    'schedules': sorted_scheds,
                })

        ambiguous_summary = {
            'orphaned_count': len(orphaned),
            'orphaned_schedule_ids': [s.get('reportScheduleId') for s in orphaned],
            'duplicate_marketplace_count': len(duplicate_groups),
            'duplicate_groups': duplicate_groups,
        }
        # ─────────────────────────────────────────────────────────────────────

        return JsonResponse({
            'success': True,
            'total_schedules': len(all_schedules),
            'reportSchedules': all_schedules,
            'ambiguous': ambiguous_summary,
            'marketplace_results': results,
            'processing_time_seconds': round(time.time() - start_time, 2)
        })


@method_decorator(csrf_exempt, name='dispatch')
class CancelReportScheduleView(View):
    """Cancel a report schedule by its ID."""

    def load_credentials(self):
        creds_path = Path(__file__).parent.parent / "creds_inventory.json"
        if not creds_path.exists():
            raise Exception("Credentials file not found. Please connect first.")
        with open(creds_path, 'r') as f:
            creds = json.load(f)
        for key in ['refresh_token', 'app_id', 'client_secret']:
            if not creds.get(key):
                raise Exception(f"Missing credential: {key}")
        return creds

    def get_region_from_marketplace(self, marketplace_id):
        return get_region_from_marketplace_id(marketplace_id)

    def _get_access_token(self, creds):
        url = "https://api.amazon.com/auth/o2/token"
        data = {
            "grant_type": "refresh_token",
            "refresh_token": creds['refresh_token'],
            "client_id": creds['app_id'],
            "client_secret": creds['client_secret']
        }
        resp = requests.post(url, data=data, timeout=30)
        resp.raise_for_status()
        token = resp.json().get('access_token')
        if not token:
            raise Exception("Failed to obtain access token")
        return token

    def delete(self, request, report_schedule_id: str):
        start_time = time.time()

        try:
            body = json.loads(request.body or '{}')
        except json.JSONDecodeError as e:
            return JsonResponse({'success': False, 'error': 'Invalid JSON', 'details': str(e)}, status=400)

        # Accept both 'marketplaceCode' and 'warehouse_code' as aliases
        marketplace_code = body.get('marketplaceCode') or body.get('warehouse_code')
        company_name_raw = body.get('companyName')
        company_name = normalize_company_name(company_name_raw) if company_name_raw else None

        if not marketplace_code:
            return JsonResponse({
                'success': False,
                'error': 'marketplaceCode (or warehouse_code) is required',
                'valid_marketplaces': list(MARKETPLACE_IDS.keys()),
            }, status=400)

        if marketplace_code not in MARKETPLACE_IDS:
            return JsonResponse({
                'success': False,
                'error': f'Invalid marketplaceCode: {marketplace_code}',
                'valid_marketplaces': list(MARKETPLACE_IDS.keys()),
            }, status=400)

        marketplace_id = MARKETPLACE_IDS[marketplace_code]

        # ── Resolve credential group via active companies (RDX INC LTD, brandsinn) ──
        region = self.get_region_from_marketplace(marketplace_id)
        try:
            group = find_credential_group_for_marketplace(marketplace_id, company_name)
        except KeyError:
            return JsonResponse({
                'success': False,
                'error': f'No active credential group found for marketplace "{marketplace_code}"',
                'active_companies': ACTIVE_COMPANIES,
            }, status=400)
        creds = CREDENTIALS[group]
        try:
            access_token = self._get_access_token(creds)
        except Exception as e:
            return JsonResponse({'success': False, 'error': f'Authentication failed: {e}'}, status=500)

        # ── Cancel the schedule ───────────────────────────────────────────────
        url = f"https://sellingpartnerapi-{region}.amazon.com/reports/2021-06-30/schedules/{report_schedule_id}"
        headers = {
            'x-amz-access-token': access_token,
            'accept': 'application/json',
            'User-Agent': 'AmazonConnector/1.0'
        }
        try:
            resp = requests.delete(url, headers=headers, timeout=30)
            if resp.status_code in (200, 202, 204):
                return JsonResponse({
                    'success': True,
                    'reportScheduleId': report_schedule_id,
                    'marketplaceCode': marketplace_code,
                    'company': company_name,
                    'credential_group': group,
                    'status_code': resp.status_code,
                    'processing_time_seconds': round(time.time() - start_time, 2)
                })
            return JsonResponse({
                'success': False,
                'reportScheduleId': report_schedule_id,
                'status_code': resp.status_code,
                'response': resp.text
            }, status=resp.status_code)
        except requests.exceptions.RequestException as e:
            return JsonResponse({
                'success': False,
                'reportScheduleId': report_schedule_id,
                'error': str(e),
                'response': e.response.text if getattr(e, 'response', None) is not None else None,
            }, status=500)



