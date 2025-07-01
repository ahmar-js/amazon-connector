from django.shortcuts import render
from django.http import JsonResponse, HttpResponse
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.permissions import AllowAny
import requests
import logging
from django.conf import settings
import json
from django.views import View
from django.utils.decorators import method_decorator
from django.views.decorators.csrf import csrf_exempt
from datetime import datetime, timedelta, timezone
import time
import os
from pathlib import Path
import math
from typing import Dict, List, Optional, Tuple, Union
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import pandas as pd
from .data_processor import process_amazon_data
from .models import Activities
from .simple_db_save import save_simple
from django.core.paginator import Paginator
from django.db.models import Q

# Set up logging
logger = logging.getLogger(__name__)

@method_decorator(csrf_exempt, name='dispatch')
class ConnectAmazonStoreView(View):
    """
    Handle Amazon store connection requests
    """
    
    def post(self, request):
        try:
            # Parse JSON request body
            try:
                data = json.loads(request.body)
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON in request: {e}")
                return JsonResponse({
                    'success': False,
                    'error': 'Invalid JSON format',
                    'details': str(e)
                }, status=400)
            
            # Validate required fields
            required_fields = ['appId', 'clientSecret', 'refreshToken']
            missing_fields = [field for field in required_fields if not data.get(field)]
            
            if missing_fields:
                logger.warning(f"Missing required fields: {missing_fields}")
                # Create user-friendly field names
                field_names = {
                    'appId': 'Application ID',
                    'clientSecret': 'Client Secret', 
                    'refreshToken': 'Refresh Token'
                }
                friendly_missing = [field_names.get(field, field) for field in missing_fields]
                
                return JsonResponse({
                    'success': False,
                    'error': f'Please fill in all required fields',
                    'details': f'Missing: {", ".join(friendly_missing)}. You can find these in your Amazon Developer Console.'
                }, status=400)
            
            # Extract credentials
            app_id = data['appId'].strip()
            client_secret = data['clientSecret'].strip()
            refresh_token = data['refreshToken'].strip()
            
            # Validate credential formats
            validation_errors = []
            
            if not app_id.startswith('amzn1.application-oa2-client.'):
                validation_errors.append('❌ Application ID should start with "amzn1.application-oa2-client." - please copy it exactly from your Amazon Developer Console')
            
            if len(client_secret) < 64:
                validation_errors.append('❌ Client Secret seems too short - it should be a long string of letters and numbers from your Amazon app settings')
            
            if not refresh_token.startswith('Atzr|'):
                validation_errors.append('❌ Refresh Token should start with "Atzr|" - make sure you\'re copying the refresh token, not the access token')
            
            if validation_errors:
                logger.warning(f"Validation errors: {validation_errors}")
                return JsonResponse({
                    'success': False,
                    'error': 'Please check your Amazon credentials',
                    'details': ' • '.join(validation_errors)
                }, status=400)
            
            # Prepare Amazon LWA token request
            token_url = 'https://api.amazon.com/auth/o2/token'
            token_data = {
                'grant_type': 'refresh_token',
                'refresh_token': refresh_token,
                'client_id': app_id,
                'client_secret': client_secret
            }
            
            headers = {
                'Content-Type': 'application/x-www-form-urlencoded',
                'User-Agent': 'AmazonConnector/1.0'
            }
            
            logger.info(f"Attempting to connect to Amazon API for app: {app_id[:20]}...")
            
            # Make request to Amazon LWA
            try:
                response = requests.post(
                    token_url,
                    data=token_data,
                    headers=headers,
                    timeout=30
                )
                
                logger.info(f"Amazon API response status: {response.status_code}")
                
                if response.status_code == 200:
                    token_info = response.json()
                    
                    # Calculate expiry time
                    expires_in = token_info.get('expires_in', 3600)
                    expires_at = datetime.utcnow() + timedelta(seconds=expires_in)
                    connected_at = datetime.utcnow()
                    
                    # Prepare credential data for storage
                    creds_data = {
                        'app_id': app_id,
                        'client_secret': client_secret,
                        'refresh_token': refresh_token,
                        'access_token': token_info.get('access_token'),
                        'expires_at': expires_at.isoformat() + 'Z',
                        'expires_in': expires_in,
                        'token_type': token_info.get('token_type', 'bearer'),
                        'connected_at': connected_at.isoformat() + 'Z',
                        'last_refreshed': connected_at.isoformat() + 'Z'
                    }
                    
                    # Save credentials to creds.json file
                    try:
                        self.save_credentials_to_file(creds_data)
                        logger.info("✅ Credentials saved to creds.json")
                    except Exception as save_error:
                        logger.error(f"Failed to save credentials: {save_error}")
                        # Continue without failing the connection
                    
                    # Prepare response data
                    response_data = {
                        'access_token': token_info.get('access_token'),
                        'token_type': token_info.get('token_type', 'bearer'),
                        'expires_in': expires_in,
                        'expires_at': expires_at.isoformat() + 'Z',
                        'refresh_token': refresh_token,  # Keep the original refresh token
                        'app_id': app_id,
                        'connected_at': connected_at.isoformat() + 'Z'
                    }
                    
                    logger.info("✅ Successfully connected to Amazon API")
                    return JsonResponse({
                        'success': True,
                        'message': 'Successfully connected to Amazon API',
                        'data': response_data
                    })
                    
                else:
                    # Handle Amazon API errors
                    try:
                        error_info = response.json()
                        error_description = error_info.get('error_description', '')
                        error_code = error_info.get('error', 'api_error')
                        
                        # Provide user-friendly error messages based on common Amazon API errors
                        if 'invalid_grant' in error_code.lower() or 'invalid_grant' in error_description.lower():
                            user_message = 'Your Amazon credentials have expired or are invalid'
                            user_details = 'Please check that your App ID, Client Secret, and Refresh Token are correct and up-to-date. You may need to generate new credentials from your Amazon Developer Console.'
                        elif 'invalid_client' in error_code.lower() or 'invalid_client' in error_description.lower():
                            user_message = 'Amazon could not verify your app credentials'
                            user_details = 'Please double-check your Application ID and Client Secret from your Amazon Developer Console. Make sure your app is approved and active.'
                        elif 'unauthorized' in error_code.lower() or response.status_code == 401:
                            user_message = 'Authentication failed with Amazon'
                            user_details = 'Your credentials may be incorrect or expired. Please verify all three fields (App ID, Client Secret, and Refresh Token) are copied correctly from Amazon.'
                        elif response.status_code == 403:
                            user_message = 'Access denied by Amazon'
                            user_details = 'Your Amazon app may not have the required permissions. Please check your app settings in the Amazon Developer Console.'
                        elif response.status_code == 429:
                            user_message = 'Too many connection attempts'
                            user_details = 'Amazon has temporarily limited your requests. Please wait a few minutes before trying again.'
                        else:
                            user_message = 'Amazon connection failed'
                            user_details = f'Amazon returned an error: {error_description or "Unknown error"}. Please verify your credentials and try again.'
                    except:
                        user_message = 'Unable to connect to Amazon'
                        user_details = f'Amazon responded with status {response.status_code}. This might be a temporary issue - please try again in a few minutes.'
                    
                    logger.error(f"Amazon API error: {error_code} - {error_description}")
                    return JsonResponse({
                        'success': False,
                        'error': user_message,
                        'details': user_details
                    }, status=400)
                    
            except requests.exceptions.Timeout:
                logger.error("Amazon API request timeout")
                return JsonResponse({
                    'success': False,
                    'error': 'Connection to Amazon timed out',
                    'details': 'Amazon took too long to respond. This might be due to high traffic or network issues. Please check your internet connection and try again.'
                }, status=408)
                
            except requests.exceptions.ConnectionError:
                logger.error("Connection error to Amazon API")
                return JsonResponse({
                    'success': False,
                    'error': 'Could not reach Amazon servers',
                    'details': 'Please check your internet connection and make sure you can access Amazon websites. If the problem persists, Amazon\'s services might be temporarily unavailable.'
                }, status=503)
                
            except Exception as e:
                logger.error(f"Unexpected error during Amazon API call: {e}")
                return JsonResponse({
                    'success': False,
                    'error': 'Something went wrong while connecting',
                    'details': 'An unexpected error occurred. Please try again, and if the problem continues, check that your credentials are correct.'
                }, status=500)
        
        except Exception as e:
            logger.error(f"Unexpected error in ConnectAmazonStoreView: {e}")
            return JsonResponse({
                'success': False,
                'error': 'Something unexpected happened',
                'details': 'We encountered an unexpected issue while processing your request. Please try again, and if the problem persists, check your internet connection.'
            }, status=500)
    
    def save_credentials_to_file(self, creds_data):
        """Save credentials to creds.json file"""
        try:
            # Get the path to creds.json in the project root
            creds_file_path = Path(__file__).parent.parent / 'creds.json'
            
            # Ensure the directory exists
            creds_file_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Write credentials to file
            with open(creds_file_path, 'w') as f:
                json.dump(creds_data, f, indent=2)
                
            logger.info(f"✅ Credentials saved to {creds_file_path}")
            
        except Exception as e:
            logger.error(f"❌ Failed to save credentials to file: {e}")
            raise
    
    def get(self, request):
        """Handle GET requests with helpful information"""
        return JsonResponse({
            'message': 'Amazon Store Connection API',
            'methods': ['POST'],
            'required_fields': ['appId', 'clientSecret', 'refreshToken'],
            'description': 'Connect to Amazon Seller Central API using OAuth credentials'
        })

@method_decorator(csrf_exempt, name='dispatch')
class RefreshAccessTokenView(View):
    """
    Handle access token refresh using refresh token
    """
    
    def post(self, request):
        try:
            # Parse JSON request body
            try:
                data = json.loads(request.body)
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON in request: {e}")
                return JsonResponse({
                    'success': False,
                    'error': 'Request format error',
                    'details': 'There was a problem with the request format. Please try again.'
                }, status=400)
            
            # Validate required fields
            required_fields = ['appId', 'clientSecret', 'refreshToken']
            missing_fields = [field for field in required_fields if not data.get(field)]
            
            if missing_fields:
                logger.warning(f"Missing required fields: {missing_fields}")
                return JsonResponse({
                    'success': False,
                    'error': 'Cannot refresh without complete credentials',
                    'details': 'Token refresh requires your Application ID, Client Secret, and Refresh Token. Please ensure your connection is properly established.'
                }, status=400)
            
            # Extract credentials
            app_id = data['appId'].strip()
            client_secret = data['clientSecret'].strip()
            refresh_token = data['refreshToken'].strip()
            
            # Prepare Amazon LWA token refresh request
            token_url = 'https://api.amazon.com/auth/o2/token'
            token_data = {
                'grant_type': 'refresh_token',
                'refresh_token': refresh_token,
                'client_id': app_id,
                'client_secret': client_secret
            }
            
            headers = {
                'Content-Type': 'application/x-www-form-urlencoded',
                'User-Agent': 'AmazonConnector/1.0'
            }
            
            logger.info(f"Refreshing access token for app: {app_id[:20]}...")
            
            # Make request to Amazon LWA
            try:
                response = requests.post(
                    token_url,
                    data=token_data,
                    headers=headers,
                    timeout=30
                )
                
                if response.status_code == 200:
                    token_info = response.json()
                    
                    # Calculate expiry time
                    expires_in = token_info.get('expires_in', 3600)
                    expires_at = datetime.utcnow() + timedelta(seconds=expires_in)
                    refreshed_at = datetime.utcnow()
                    
                    # Update credentials in creds.json file
                    try:
                        self.update_credentials_in_file({
                            'access_token': token_info.get('access_token'),
                            'expires_at': expires_at.isoformat() + 'Z',
                            'expires_in': expires_in,
                            'token_type': token_info.get('token_type', 'bearer'),
                            'last_refreshed': refreshed_at.isoformat() + 'Z'
                        })
                        logger.info("✅ Updated credentials in creds.json")
                    except Exception as save_error:
                        logger.error(f"Failed to update credentials: {save_error}")
                        # Continue without failing the refresh
                    
                    # Prepare response data
                    response_data = {
                        'access_token': token_info.get('access_token'),
                        'token_type': token_info.get('token_type', 'bearer'),
                        'expires_in': expires_in,
                        'expires_at': expires_at.isoformat() + 'Z',
                        'refresh_token': refresh_token,
                        'refreshed_at': refreshed_at.isoformat() + 'Z'
                    }
                    
                    logger.info("✅ Successfully refreshed access token")
                    return JsonResponse({
                        'success': True,
                        'message': 'Successfully refreshed access token',
                        'data': response_data
                    })
                    
                else:
                    try:
                        error_info = response.json()
                        error_description = error_info.get('error_description', '')
                        error_code = error_info.get('error', 'refresh_error')
                        
                        # Provide user-friendly error messages for token refresh failures
                        if 'invalid_grant' in error_code.lower() or 'invalid_grant' in error_description.lower():
                            user_message = 'Your session has expired'
                            user_details = 'Your refresh token is no longer valid. Please reconnect your Amazon account to continue.'
                        elif 'invalid_client' in error_code.lower():
                            user_message = 'App credentials are invalid'
                            user_details = 'There\'s an issue with your app configuration. Please try reconnecting your Amazon account.'
                        else:
                            user_message = 'Unable to refresh your session'
                            user_details = 'Something went wrong while renewing your connection. Please try reconnecting your Amazon account.'
                    except:
                        user_message = 'Session refresh failed'
                        user_details = f'Amazon responded with status {response.status_code}. Please try reconnecting your Amazon account.'
                    
                    logger.error(f"Token refresh error: {error_code} - {error_description}")
                    return JsonResponse({
                        'success': False,
                        'error': user_message,
                        'details': user_details
                    }, status=400)
                    
            except Exception as e:
                logger.error(f"Error during token refresh: {e}")
                return JsonResponse({
                    'success': False,
                    'error': 'Session refresh failed',
                    'details': 'We couldn\'t renew your Amazon session. Please try reconnecting your account.'
                }, status=500)
        
        except Exception as e:
            logger.error(f"Unexpected error in RefreshAccessTokenView: {e}")
            return JsonResponse({
                'success': False,
                'error': 'Something unexpected happened',
                'details': 'We encountered an unexpected issue while refreshing your session. Please try reconnecting your Amazon account.'
            }, status=500)
    
    def update_credentials_in_file(self, update_data):
        """Update specific fields in the credentials file"""
        try:
            # Get the path to creds.json in the project root
            creds_file_path = Path(__file__).parent.parent / 'creds.json'
            
            # Read existing credentials
            existing_creds = {}
            if creds_file_path.exists():
                with open(creds_file_path, 'r') as f:
                    existing_creds = json.load(f)
            
            # Update with new data
            existing_creds.update(update_data)
            
            # Write back to file
            with open(creds_file_path, 'w') as f:
                json.dump(existing_creds, f, indent=2)
                
            logger.info(f"✅ Credentials updated in {creds_file_path}")
            
        except Exception as e:
            logger.error(f"❌ Failed to update credentials in file: {e}")
            raise
    
    def get(self, request):
        """Handle GET requests with helpful information"""
        return JsonResponse({
            'message': 'Access Token Refresh API',
            'methods': ['POST'],
            'required_fields': ['appId', 'clientSecret', 'refreshToken'],
            'description': 'Refresh access token using refresh token'
        })

@method_decorator(csrf_exempt, name='dispatch')
class ConnectionStatusView(View):
    """
    Get current connection status from stored credentials
    """
    
    def get(self, request):
        try:
            # Get the path to creds.json
            creds_file_path = Path(__file__).parent.parent / 'creds.json'
            
            if not creds_file_path.exists():
                return JsonResponse({
                    'success': True,
                    'data': {
                        'isConnected': False,
                        'message': 'No saved connection found'
                    }
                })
            
            # Read credentials from file
            with open(creds_file_path, 'r') as f:
                creds_data = json.load(f)
            
            # Check if token is expired
            expires_at = creds_data.get('expires_at')
            is_expired = False
            if expires_at:
                expiry_time = datetime.fromisoformat(expires_at.replace('Z', '+00:00'))
                is_expired = expiry_time <= datetime.utcnow().replace(tzinfo=None)
            
            # Prepare response data (without sensitive information)
            response_data = {
                'isConnected': True,
                'app_id': creds_data.get('app_id'),
                'token_type': creds_data.get('token_type', 'bearer'),
                'expires_in': creds_data.get('expires_in', 3600),
                'expires_at': creds_data.get('expires_at'),
                'connected_at': creds_data.get('connected_at'),
                'last_refreshed': creds_data.get('last_refreshed'),
                'is_expired': is_expired,
                'has_refresh_token': bool(creds_data.get('refresh_token'))
            }
            
            return JsonResponse({
                'success': True,
                'data': response_data
            })
            
        except Exception as e:
            logger.error(f"Error getting connection status: {e}")
            return JsonResponse({
                'success': False,
                'error': 'Failed to get connection status',
                'details': str(e)
            }, status=500)

@method_decorator(csrf_exempt, name='dispatch')
class ManualRefreshTokenView(View):
    """
    Handle manual token refresh using stored credentials
    """
    
    def post(self, request):
        try:
            # Read credentials from file
            creds_file_path = os.path.join(settings.BASE_DIR, 'creds.json')
            
            if not os.path.exists(creds_file_path):
                logger.error("No credentials file found")
                return JsonResponse({
                    'success': False,
                    'error': 'No saved connection found',
                    'details': 'Please connect your Amazon account first'
                }, status=400)
            
            try:
                with open(creds_file_path, 'r') as f:
                    creds_data = json.load(f)
            except json.JSONDecodeError as e:
                logger.error(f"Invalid credentials file: {e}")
                return JsonResponse({
                    'success': False,
                    'error': 'Invalid credentials file',
                    'details': 'Please reconnect your Amazon account'
                }, status=400)
            
            # Validate required fields
            required_fields = ['app_id', 'refresh_token']
            missing_fields = [field for field in required_fields if not creds_data.get(field)]
            
            if missing_fields:
                logger.warning(f"Missing required fields in credentials: {missing_fields}")
                return JsonResponse({
                    'success': False,
                    'error': 'Incomplete stored credentials',
                    'details': 'Please reconnect your Amazon account'
                }, status=400)
            
            # Extract credentials
            app_id = creds_data['app_id']
            refresh_token = creds_data['refresh_token']
            
            # Prepare Amazon LWA token refresh request
            token_url = 'https://api.amazon.com/auth/o2/token'
            token_data = {
                'grant_type': 'refresh_token',
                'refresh_token': refresh_token,
                'client_id': app_id,
                'client_secret': creds_data['client_secret']
            }
            
            headers = {
                'Content-Type': 'application/x-www-form-urlencoded',
                'User-Agent': 'AmazonConnector/1.0'
            }
            
            logger.info(f"Token refresh for app: {app_id[:20]}...")
            
            # Make request to Amazon
            response = requests.post(
                token_url,
                data=token_data,
                headers=headers,
                timeout=30
            )
            
            if response.status_code == 200:
                token_info = response.json()
                
                # Calculate expiry time
                expires_in = token_info.get('expires_in', 3600)
                expires_at = datetime.utcnow() + timedelta(seconds=expires_in)
                refreshed_at = datetime.utcnow()
                
                # Update credentials in file
                update_data = {
                    'access_token': token_info.get('access_token'),
                    'expires_at': expires_at.isoformat() + 'Z',
                    'expires_in': expires_in,
                    'token_type': token_info.get('token_type', 'bearer'),
                    'last_refreshed': refreshed_at.isoformat() + 'Z'
                }
                
                # Read, update, and write back
                creds_data.update(update_data)
                with open(creds_file_path, 'w') as f:
                    json.dump(creds_data, f, indent=2)
                
                logger.info("✅ Token refresh successful")
                
                # Return complete token data that frontend expects
                return JsonResponse({
                    'success': True,
                    'message': 'Token refreshed successfully',
                    'data': {
                        'access_token': token_info.get('access_token'),
                        'token_type': token_info.get('token_type', 'bearer'),
                        'expires_in': expires_in,
                        'expires_at': expires_at.isoformat() + 'Z',
                        'refresh_token': refresh_token,  # Keep the same refresh token
                        'app_id': app_id,
                        'connected_at': creds_data.get('connected_at', refreshed_at.isoformat() + 'Z'),
                        'refreshed_at': refreshed_at.isoformat() + 'Z'
                    }
                })
                
            else:
                # Handle Amazon API errors
                try:
                    error_info = response.json()
                    error_description = error_info.get('error_description', '')
                    error_code = error_info.get('error', 'refresh_error')
                except:
                    error_description = 'Unknown error'
                    error_code = 'api_error'
                
                logger.error(f"Manual token refresh failed: {error_code} - {error_description}")
                return JsonResponse({
                    'success': False,
                    'error': 'Token refresh failed',
                    'details': f'Amazon API error: {error_description}'
                }, status=400)
                
        except Exception as e:
            logger.error(f"Error in manual token refresh: {e}")
            return JsonResponse({
                'success': False,
                'error': 'Token refresh failed',
                'details': 'An unexpected error occurred during token refresh'
            }, status=500)
    
    def get(self, request):
        """Handle GET requests with helpful information"""
        return JsonResponse({
            'message': 'Manual Token Refresh API',
            'methods': ['POST'],
            'description': 'Manually refresh access token using stored credentials'
        })

@method_decorator(csrf_exempt, name='dispatch')
class TestConnectionView(View):
    """
    Test Amazon API connection without storing credentials
    """
    
    def post(self, request):
        try:
            # Parse JSON request body
            try:
                data = json.loads(request.body)
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON in request: {e}")
                return JsonResponse({
                    'success': False,
                    'error': 'Request format error',
                    'details': 'There was a problem with the request format. Please try again.'
                }, status=400)
            
            # Validate required fields
            required_fields = ['appId', 'clientSecret', 'refreshToken']
            missing_fields = [field for field in required_fields if not data.get(field)]
            
            if missing_fields:
                logger.warning(f"Missing required fields for test: {missing_fields}")
                field_names = {
                    'appId': 'Application ID',
                    'clientSecret': 'Client Secret', 
                    'refreshToken': 'Refresh Token'
                }
                friendly_missing = [field_names.get(field, field) for field in missing_fields]
                
                return JsonResponse({
                    'success': False,
                    'error': f'Please fill in all fields to test connection',
                    'details': f'Missing: {", ".join(friendly_missing)}. You can find these in your Amazon Developer Console.'
                }, status=400)
            
            # Extract credentials
            app_id = data['appId'].strip()
            client_secret = data['clientSecret'].strip()
            refresh_token = data['refreshToken'].strip()
            
            # Validate credential formats (same as connect endpoint)
            validation_errors = []
            
            if not app_id.startswith('amzn1.application-oa2-client.'):
                validation_errors.append('❌ Application ID should start with "amzn1.application-oa2-client." - please copy it exactly from your Amazon Developer Console')
            
            if len(client_secret) < 64:
                validation_errors.append('❌ Client Secret seems too short - it should be a long string of letters and numbers from your Amazon app settings')
            
            if not refresh_token.startswith('Atzr|'):
                validation_errors.append('❌ Refresh Token should start with "Atzr|" - make sure you\'re copying the refresh token, not the access token')
            
            if validation_errors:
                logger.warning(f"Validation errors in test: {validation_errors}")
                return JsonResponse({
                    'success': False,
                    'error': 'Please check your Amazon credentials',
                    'details': ' • '.join(validation_errors)
                }, status=400)
            
            # Test connection to Amazon LWA
            token_url = 'https://api.amazon.com/auth/o2/token'
            token_data = {
                'grant_type': 'refresh_token',
                'refresh_token': refresh_token,
                'client_id': app_id,
                'client_secret': client_secret
            }
            
            headers = {
                'Content-Type': 'application/x-www-form-urlencoded',
                'User-Agent': 'AmazonConnector/1.0'
            }
            
            logger.info(f"Testing connection to Amazon API for app: {app_id[:20]}...")
            
            try:
                response = requests.post(
                    token_url,
                    data=token_data,
                    headers=headers,
                    timeout=15  # Shorter timeout for test
                )
                
                logger.info(f"Amazon API test response status: {response.status_code}")
                
                if response.status_code == 200:
                    token_info = response.json()
                    
                    # Extract some basic info for confirmation (without storing)
                    token_type = token_info.get('token_type', 'bearer')
                    expires_in = token_info.get('expires_in', 3600)
                    
                    logger.info("✅ Amazon API test connection successful")
                    return JsonResponse({
                        'success': True,
                        'message': 'Connection test successful! Your credentials are valid.',
                        'data': {
                            'app_id': app_id,
                            'token_type': token_type,
                            'expires_in': expires_in,
                            'tested_at': datetime.utcnow().isoformat() + 'Z'
                        }
                    })
                    
                else:
                    # Handle Amazon API errors (same logic as connect endpoint)
                    try:
                        error_info = response.json()
                        error_description = error_info.get('error_description', '')
                        error_code = error_info.get('error', 'api_error')
                        
                        if 'invalid_grant' in error_code.lower() or 'invalid_grant' in error_description.lower():
                            user_message = 'Your Amazon credentials have expired or are invalid'
                            user_details = 'Please check that your App ID, Client Secret, and Refresh Token are correct and up-to-date. You may need to generate new credentials from your Amazon Developer Console.'
                        elif 'invalid_client' in error_code.lower() or 'invalid_client' in error_description.lower():
                            user_message = 'Amazon could not verify your app credentials'
                            user_details = 'Please double-check your Application ID and Client Secret from your Amazon Developer Console. Make sure your app is approved and active.'
                        elif 'unauthorized' in error_code.lower() or response.status_code == 401:
                            user_message = 'Authentication failed with Amazon'
                            user_details = 'Your credentials may be incorrect or expired. Please verify all three fields (App ID, Client Secret, and Refresh Token) are copied correctly from Amazon.'
                        elif response.status_code == 403:
                            user_message = 'Access denied by Amazon'
                            user_details = 'Your Amazon app may not have the required permissions. Please check your app settings in the Amazon Developer Console.'
                        elif response.status_code == 429:
                            user_message = 'Too many connection attempts'
                            user_details = 'Amazon has temporarily limited your requests. Please wait a few minutes before testing again.'
                        else:
                            user_message = 'Connection test failed'
                            user_details = f'Amazon returned an error: {error_description or "Unknown error"}. Please verify your credentials and try again.'
                    except:
                        user_message = 'Unable to test connection with Amazon'
                        user_details = f'Amazon responded with status {response.status_code}. This might be a temporary issue - please try again in a few minutes.'
                    
                    logger.error(f"Amazon API test error: {error_code} - {error_description}")
                    return JsonResponse({
                        'success': False,
                        'error': user_message,
                        'details': user_details
                    }, status=400)
                    
            except requests.exceptions.Timeout:
                logger.error("Amazon API test request timeout")
                return JsonResponse({
                    'success': False,
                    'error': 'Connection test timed out',
                    'details': 'Amazon took too long to respond during testing. Please check your internet connection and try again.'
                }, status=408)
                
            except requests.exceptions.ConnectionError:
                logger.error("Connection error during Amazon API test")
                return JsonResponse({
                    'success': False,
                    'error': 'Could not reach Amazon servers',
                    'details': 'Please check your internet connection and make sure you can access Amazon websites. If the problem persists, Amazon\'s services might be temporarily unavailable.'
                }, status=503)
                
            except Exception as e:
                logger.error(f"Unexpected error during Amazon API test: {e}")
                return JsonResponse({
                    'success': False,
                    'error': 'Something went wrong while testing connection',
                    'details': 'An unexpected error occurred during the test. Please try again, and if the problem continues, check that your credentials are correct.'
                }, status=500)
        
        except Exception as e:
            logger.error(f"Unexpected error in TestConnectionView: {e}")
            return JsonResponse({
                'success': False,
                'error': 'Something unexpected happened',
                'details': 'We encountered an unexpected issue while testing your connection. Please try again, and if the problem persists, check your internet connection.'
            }, status=500)
    
    def get(self, request):
        """Handle GET requests with helpful information"""
        return JsonResponse({
            'message': 'Amazon Connection Test API',
            'methods': ['POST'],
            'required_fields': ['appId', 'clientSecret', 'refreshToken'],
            'description': 'Test Amazon Seller Central API credentials without storing them'
        })
            
@method_decorator(csrf_exempt, name='dispatch')
class FetchAmazonDataView(View):
    """
    Efficient Amazon data fetching with proper SP-API rate limiting and batch processing.
    
    This view handles fetching orders and order items from Amazon SP-API with strict adherence
    to Amazon's official rate limits and proper error handling. It implements the official
    Amazon SP-API rate limits as of 2024:
    
    - Orders (getOrders): 0.0167 requests/second (1 request every 60 seconds) with burst of 20
    - Order Items (getOrderItems): 0.5 requests/second (1 request every 2 seconds) with burst of 30
    
    Key Features:
    - Separate rate limiters for orders and order items with burst support
    - Conservative batch processing with proper delays
    - Minimal concurrent requests to prevent overwhelming the API
    - Comprehensive error handling and retries
    - Date range validation and formatting
    - Marketplace-specific endpoint handling
    - Detailed logging for rate limiting and API calls
    """
    
    # In-memory cache for processed data (temporary storage)
    _processed_data_cache = {}
    
    # Amazon SP-API endpoints for different marketplaces
    # Each marketplace ID maps to its corresponding regional API endpoint
    SP_API_BASE_URLS = {
        # North America
        "ATVPDKIKX0DER": "https://sellingpartnerapi-na.amazon.com",  # US
        "A2EUQ1WTGCTBG2": "https://sellingpartnerapi-na.amazon.com",  # Canada
        # Europe
        "A1F83G8C2ARO7P": "https://sellingpartnerapi-eu.amazon.com",  # UK
        "A1PA6795UKMFR9": "https://sellingpartnerapi-eu.amazon.com",  # Germany
        "A13V1IB3VIYZZH": "https://sellingpartnerapi-eu.amazon.com",  # France
        "APJ6JRA9NG5V4": "https://sellingpartnerapi-eu.amazon.com",   # Italy
        "A1RKKUPIHCS9HS": "https://sellingpartnerapi-eu.amazon.com",  # Spain
    }
    
    # API endpoints for orders and order items
    ORDERS_ENDPOINT = "/orders/v0/orders"
    ORDER_ITEMS_ENDPOINT = "/orders/v0/orders/{order_id}/orderItems"
    
    # Rate limiting and optimization settings based on Amazon SP-API official documentation
    # Orders API Rate Limits (as of 2024):
    # - getOrders: 0.0167 requests/second (1 request every 60 seconds) with burst of 20
    # - getOrderItems: 0.5 requests/second (1 request every 2 seconds) with burst of 30
    ORDERS_MAX_REQUESTS_PER_SECOND = 0.0167  # 1 request every 60 seconds
    # Reduced order items rate to be more conservative (3 seconds between requests instead of 2)
    ORDER_ITEMS_MAX_REQUESTS_PER_SECOND = 0.33  # 1 request every 3 seconds

    # Burst limits from Amazon documentation - reduced to be more conservative
    ORDERS_BURST_LIMIT = 10  # Reduced from 20
    ORDER_ITEMS_BURST_LIMIT = 15  # Reduced from 30

    # Process orders in smaller batches to respect rate limits
    # Further reduced to 3 orders per batch to be ultra-conservative with rate limits
    BATCH_SIZE = 3

    # Maximum number of concurrent requests for order items
    # Keep at 1 to ensure we don't overwhelm the rate limits
    MAX_CONCURRENT_REQUESTS = 1

    # Timeout for API requests in seconds
    REQUEST_TIMEOUT = 60  # Increased timeout for better reliability
    
    class TokenBucketRateLimiter:
        """
        Enhanced token bucket rate limiter implementation for Amazon SP-API.
        
        This class implements the token bucket algorithm with burst support to ensure we don't exceed
        Amazon's rate limits. It maintains a bucket of tokens that are consumed for each request 
        and refilled at a constant rate, with support for burst limits.
        
        Attributes:
            rate_limit (float): Number of requests allowed per second
            burst_limit (int): Maximum number of tokens in the bucket (burst capacity)
            tokens (float): Current number of tokens in the bucket
            last_update (float): Timestamp of last token update
            lock (threading.Lock): Thread lock for thread-safe operations
        """
        def __init__(self, rate_limit: float, burst_limit: int = 1):
            """
            Initialize the rate limiter.
            
            Args:
                rate_limit (float): Number of requests allowed per second
                burst_limit (int): Maximum number of tokens in the bucket
            """
            self.rate_limit = rate_limit
            self.burst_limit = burst_limit
            self.tokens = float(burst_limit)  # Start with a full bucket
            self.last_update = time.time()
            self.lock = threading.Lock()
            logger.info(f"Initialized rate limiter: {rate_limit} req/sec, burst: {burst_limit}")
        
        def acquire(self):
            """
            Acquire a token for making a request.
            
            This method will wait if necessary until a token is available.
            It uses the token bucket algorithm to ensure we don't exceed
            the rate limit, with proper burst handling.
            """
            with self.lock:
                now = time.time()
                # Calculate time passed since last update
                time_passed = now - self.last_update
                # Add new tokens based on time passed
                new_tokens = time_passed * self.rate_limit
                # Update token count, but don't exceed burst limit
                self.tokens = min(float(self.burst_limit), self.tokens + new_tokens)
                self.last_update = now
                
                if self.tokens < 1.0:
                    # If we don't have enough tokens, calculate wait time
                    wait_time = (1.0 - self.tokens) / self.rate_limit
                    logger.info(f"Rate limiting: waiting {wait_time:.2f}s for token")
                    time.sleep(wait_time)
                    self.tokens = 0.0
                else:
                    # If we have enough tokens, use one
                    self.tokens -= 1.0
                    logger.debug(f"Token acquired, remaining: {self.tokens:.2f}")
        
        def get_wait_time(self):
            """
            Get the estimated wait time until next token is available.
            
            Returns:
                float: Wait time in seconds
            """
            with self.lock:
                if self.tokens >= 1.0:
                    return 0.0
                return (1.0 - self.tokens) / self.rate_limit
    
    def __init__(self):
        """
        Initialize the view with a session and separate rate limiters.
        
        Sets up a requests session with default headers and initializes
        separate rate limiters for orders and order items endpoints.
        """
        super().__init__()
        # Create a session for making HTTP requests
        self.session = requests.Session()
        # Set default headers for all requests
        self.session.headers.update({
            'User-Agent': 'AmazonConnector/1.0',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        })
        # Initialize separate rate limiters for different endpoints
        self.orders_rate_limiter = self.TokenBucketRateLimiter(
            self.ORDERS_MAX_REQUESTS_PER_SECOND, 
            self.ORDERS_BURST_LIMIT
        )
        self.order_items_rate_limiter = self.TokenBucketRateLimiter(
            self.ORDER_ITEMS_MAX_REQUESTS_PER_SECOND, 
            self.ORDER_ITEMS_BURST_LIMIT
        )
    
    def post(self, request, *args, **kwargs):
        """
        Handle POST requests to fetch Amazon data.
        
        This is the main entry point for the view. It validates the request,
        processes the parameters, and orchestrates the data fetching process.
        
        Expected request parameters:
        - access_token: Amazon SP-API access token
        - marketplace_id: Amazon marketplace ID (e.g., ATVPDKIKX0DER for US)
        - start_date: ISO format date string (YYYY-MM-DDTHH:MM:SSZ)
        - end_date: ISO format date string (YYYY-MM-DDTHH:MM:SSZ)
        - max_orders: Optional maximum number of orders to fetch (default: unlimited)
        
        Returns:
            JsonResponse: Contains the fetched data or error information
        """
        try:
            # Parse and validate request data
            try:
                data = json.loads(request.body)
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON in fetch request: {e}")
                return JsonResponse({
                    'success': False,
                    'error': 'Invalid JSON format',
                    'details': str(e)
                }, status=400)
            
            # Validate required parameters
            required_fields = ['access_token', 'marketplace_id', 'start_date', 'end_date']
            missing_fields = [field for field in required_fields if not data.get(field)]
            
            if missing_fields:
                return JsonResponse({
                    'success': False,
                    'error': 'Missing required parameters',
                    'details': f'Required fields: {", ".join(missing_fields)}'
                }, status=400)
            
            # Extract and validate parameters
            access_token = data['access_token'].strip()
            marketplace_id = data['marketplace_id'].strip()
            start_date = data['start_date'].strip()
            end_date = data['end_date'].strip()
            max_orders = data.get('max_orders')
            auto_save = data.get('auto_save', False)  # Get auto_save parameter
            
            # Debug logging
            logger.info(f"🔍 Request parameters: marketplace_id={marketplace_id}, auto_save={auto_save}")
            logger.info(f"🔍 Full request data keys: {list(data.keys())}")
            
            # If max_orders is None or null, fetch all orders (no limit)
            if max_orders is None:
                max_orders = float('inf')  # No limit
            
            # Validate and format dates
            try:
                # Handle different date formats
                if 'T' not in start_date:
                    start_date = f"{start_date}T00:00:00Z"
                if 'T' not in end_date:
                    end_date = f"{end_date}T23:59:59Z"
                
                start_dt = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
                end_dt = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
            except ValueError as e:
                return JsonResponse({
                    'success': False,
                    'error': 'Invalid date format',
                    'details': 'Dates must be in ISO format (YYYY-MM-DD or YYYY-MM-DDTHH:MM:SSZ)'
                }, status=400)
            
            # Validate date range
            if start_dt >= end_dt:
                return JsonResponse({
                    'success': False,
                    'error': 'Invalid date range',
                    'details': 'Start date must be before end date'
                }, status=400)
            
            # Check if date range is too large (Amazon has limits)
            date_diff = (end_dt - start_dt).days
            if date_diff > 30:
                return JsonResponse({
                    'success': False,
                    'error': 'Date range too large',
                    'details': 'Please limit date range to 30 days maximum for optimal performance'
                }, status=400)
            
            logger.info(f"Starting Amazon data fetch: {marketplace_id}, {start_date} to {end_date}")
            
            # Create or get existing activity record to prevent duplicates
            activity = None
            try:
                # Try to get existing in-progress activity first, then create if needed
                activity, created = Activities.objects.get_or_create(
                    marketplace_id=marketplace_id,
                    activity_type='fetch',
                    date_from=start_dt.date(),
                    date_to=end_dt.date(),
                    status='in_progress',
                    defaults={
                        'action': 'manual',
                        'detail': f'Starting data fetch for {marketplace_id} from {start_dt.date()} to {end_dt.date()}'
                    }
                )
                
                if created:
                    logger.info(f"Created new activity record: {activity.activity_id}")
                else:
                    logger.info(f"Found existing in-progress activity: {activity.activity_id}")
                    # Update the detail to show it's continuing
                    activity.detail = f'Continuing data fetch for {marketplace_id} from {start_dt.date()} to {end_dt.date()}'
                    activity.save()
                    
            except Exception as activity_error:
                logger.warning(f"Failed to create/get activity record: {activity_error}")
                # Continue without failing the entire operation
            
            # Setup headers for Amazon SP-API
            def to_iso_format_with_z(dt):
                """Convert datetime to ISO format with Z suffix"""
                return dt.strftime('%Y-%m-%dT%H:%M:%SZ')
            
            headers = {
                "x-amz-access-token": access_token,
                "Content-Type": "application/json",
                "x-amz-date": to_iso_format_with_z(datetime.utcnow()),
                "User-Agent": "AmazonConnector/1.0"
            }
            
            # Get the correct base URL for the marketplace
            base_url = self.SP_API_BASE_URLS.get(marketplace_id)
            if not base_url:
                return JsonResponse({
                    'success': False,
                    'error': 'Unsupported marketplace',
                    'details': f'Marketplace {marketplace_id} is not supported. Supported marketplaces: {list(self.SP_API_BASE_URLS.keys())}'
                }, status=400)
            
            # Start fetching data
            fetch_start_time = time.time()
            result = self.fetch_orders_with_items(
                headers, 
                base_url,
                marketplace_id, 
                start_date, 
                end_date, 
                max_orders
            )
            
            fetch_duration = time.time() - fetch_start_time
            
            if result['success']:
                logger.info(f"✅ Data fetch completed in {fetch_duration:.2f}s: "
                          f"{result['summary']['total_orders']} orders, "
                          f"{result['summary']['total_items']} items")
                
                # Extract data from the structured result
                structured_data = result['data']
                orders = structured_data.get('orders', [])
                
                # Create separate order_items array from nested items
                order_items = []
                for order in orders:
                    order_items.extend(order.get('items', []))
                
                # Create performance metadata
                total_orders = len(orders)
                total_items = len(order_items)
                avg_time_per_order = fetch_duration / total_orders if total_orders > 0 else 0
                
                # Get marketplace name from marketplace ID
                marketplace_names = {
                    "ATVPDKIKX0DER": "US",
                    "A2EUQ1WTGCTBG2": "CA", 
                    "A1F83G8C2ARO7P": "UK",
                    "A1PA6795UKMFR9": "DE",
                    "A13V1IB3VIYZZH": "FR",
                    "APJ6JRA9NG5V4": "IT",
                    "A1RKKUPIHCS9HS": "ES"
                }
                
                marketplace_name = marketplace_names.get(marketplace_id, "UNKNOWN")
                
                # Process the data using the optimized processor
                try:
                    processing_start_time = time.time()
                    logger.info(f"🔄 Starting data processing for {marketplace_name}...")
                    
                    # Process the data
                    mssql_df, azure_df = process_amazon_data(orders, order_items, marketplace_name)
                    
                    processing_duration = time.time() - processing_start_time
                    logger.info(f"✅ Data processing completed in {processing_duration:.2f}s")
                    
                    # Debug: Log DataFrame information
                    logger.info(f"🔍 MSSQL DataFrame shape: {mssql_df.shape}, columns: {len(mssql_df.columns)}")
                    logger.info(f"🔍 Azure DataFrame shape: {azure_df.shape}, columns: {len(azure_df.columns)}")
                    logger.info(f"🔍 MSSQL columns: {list(mssql_df.columns)}")
                    logger.info(f"🔍 Azure columns: {list(azure_df.columns)}")
                    
                    # Auto save to databases if requested
                    db_save_result = None
                    logger.info(f"🔍 Checking auto_save condition: auto_save={auto_save}, type={type(auto_save)}")
                    if auto_save:
                        logger.info("🔄 Auto save enabled - saving data to databases...")
                        try:
                            db_save_start_time = time.time()
                            # Use simple save approach to avoid column alignment issues
                            db_save_result = save_simple(mssql_df, azure_df, marketplace_id)
                            db_save_duration = time.time() - db_save_start_time
                            
                            if db_save_result['success']:
                                logger.info(f"✅ Database save completed in {db_save_duration:.2f}s - {db_save_result['total_records_saved']} records saved")
                            else:
                                logger.error(f"❌ Database save failed: {db_save_result.get('errors', ['Unknown error'])}")
                        except Exception as save_error:
                            logger.error(f"❌ Database save error: {save_error}", exc_info=True)
                            db_save_result = {
                                'success': False,
                                'error': str(save_error),
                                'total_records_saved': 0
                            }
                    
                    # Store processed data temporarily (convert to dict for JSON serialization)
                    mssql_data = mssql_df.to_dict('records') if not mssql_df.empty else []
                    azure_data = azure_df.to_dict('records') if not azure_df.empty else []
                    
                    # Store in cache for later download
                    cache_key_base = f"processed_data_{marketplace_id}_{int(time.time())}"
                    
                    # Also save to temporary files as backup
                    try:
                        processed_dir = Path("processed_data")
                        processed_dir.mkdir(exist_ok=True)
                        
                        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        
                        # Save MSSQL data to file
                        if not mssql_df.empty:
                            mssql_filename = f"MSSQL_data_{marketplace_name}_{timestamp}.csv"
                            mssql_path = processed_dir / mssql_filename
                            mssql_df.to_csv(mssql_path, index=False, encoding='utf-8')
                            logger.info(f"💾 Saved MSSQL data to: {mssql_path}")
                        
                        # Save Azure data to file
                        if not azure_df.empty:
                            azure_filename = f"AZURE_data_{marketplace_name}_{timestamp}.csv"
                            azure_path = processed_dir / azure_filename
                            azure_df.to_csv(azure_path, index=False, encoding='utf-8')
                            logger.info(f"💾 Saved Azure data to: {azure_path}")
                            
                    except Exception as file_save_error:
                        logger.warning(f"Failed to save processed data to files: {file_save_error}")
                    
                    # Store both dataframes in cache with metadata
                    FetchAmazonDataView._processed_data_cache[cache_key_base] = {
                        'mssql_data': mssql_data,
                        'azure_data': azure_data,
                        'marketplace_name': marketplace_name,
                        'marketplace_id': marketplace_id,
                        'created_at': datetime.now().isoformat(),
                        'date_range': {
                            'start_date': start_date,
                            'end_date': end_date
                        }
                    }
                    
                    logger.info(f"🔍 Stored data in cache with key: {cache_key_base}")
                    logger.info(f"🔍 Current cache keys: {list(FetchAmazonDataView._processed_data_cache.keys())}")
                    
                    # Build the response in the format expected by frontend
                    processed_data_info = {
                            'mssql_records': len(mssql_df),
                            'azure_records': len(azure_df),
                            'cache_key': cache_key_base,
                            'available_for_download': True
                    }
                    
                    # Add database save information if auto save was performed
                    if db_save_result:
                        processed_data_info['database_save'] = {
                            'attempted': True,
                            'success': db_save_result['success'],
                            'records_saved': db_save_result.get('total_records_saved', 0),
                            'details': db_save_result
                        }
                    else:
                        processed_data_info['database_save'] = {
                            'attempted': False,
                            'success': None,
                            'records_saved': 0
                        }
                    
                    response_data = {
                        'orders': orders,
                        'order_items': order_items,
                        'processed_data': processed_data_info,
                        'metadata': {
                            'total_orders_fetched': total_orders,
                            'total_items_fetched': total_items,
                            'marketplace_id': marketplace_id,
                            'marketplace_name': marketplace_names.get(marketplace_id, f'Marketplace {marketplace_id}'),
                            'date_range': {
                                'start_date': start_date,
                                'end_date': end_date
                            },
                            'fetch_completed_at': datetime.utcnow().isoformat() + 'Z',
                            'performance': {
                                'total_time_seconds': round(fetch_duration + processing_duration, 2),
                                'fetch_time_seconds': round(fetch_duration, 2),
                                'processing_time_seconds': round(processing_duration, 2),
                                'orders_fetch_time_seconds': round(fetch_duration * 0.6, 2),  # Estimate
                                'items_fetch_time_seconds': round(fetch_duration * 0.4, 2),   # Estimate
                                'average_time_per_order': round(avg_time_per_order, 4)
                            }
                        }
                    }
                    
                except Exception as processing_error:
                    logger.error(f"Data processing failed: {processing_error}", exc_info=True)
                    # Still return the raw data even if processing fails
                    response_data = {
                        'orders': orders,
                        'order_items': order_items,
                        'processing_error': str(processing_error),
                        'metadata': {
                            'total_orders_fetched': total_orders,
                            'total_items_fetched': total_items,
                            'marketplace_id': marketplace_id,
                            'marketplace_name': marketplace_names.get(marketplace_id, f'Marketplace {marketplace_id}'),
                            'date_range': {
                                'start_date': start_date,
                                'end_date': end_date
                            },
                            'fetch_completed_at': datetime.utcnow().isoformat() + 'Z',
                            'performance': {
                                'total_time_seconds': round(fetch_duration, 2),
                                'fetch_time_seconds': round(fetch_duration, 2),
                                'processing_time_seconds': 0,
                                'orders_fetch_time_seconds': round(fetch_duration * 0.6, 2),  # Estimate
                                'items_fetch_time_seconds': round(fetch_duration * 0.4, 2),   # Estimate
                                'average_time_per_order': round(avg_time_per_order, 4)
                            }
                        }
                    }
                
                # Update activity record with success
                if activity:
                    try:
                        total_duration = fetch_duration + (processing_duration if 'processing_duration' in locals() else 0)
                        activity.status = 'completed'
                        activity.orders_fetched = total_orders
                        activity.items_fetched = total_items
                        activity.duration_seconds = total_duration
                        
                        # Create detailed message including database save info
                        detail_message = f'Successfully fetched {total_orders} orders and {total_items} items in {total_duration:.1f}s'
                        if auto_save and db_save_result:
                            if db_save_result['success']:
                                detail_message += f' | Auto-saved {db_save_result["total_records_saved"]} records to databases'
                                activity.database_saved = True
                            else:
                                detail_message += f' | Auto-save failed: {db_save_result.get("error", "Unknown error")}'
                                activity.database_saved = False
                        elif auto_save:
                            detail_message += ' | Auto-save was attempted but no save result available'
                            activity.database_saved = False
                        else:
                            activity.database_saved = False
                        
                        activity.detail = detail_message
                        activity.save()
                        logger.info(f"Updated activity record {activity.activity_id} with success")
                    except Exception as update_error:
                        logger.warning(f"Failed to update activity record: {update_error}")
                
                return JsonResponse({
                    'success': True,
                    'message': 'Amazon data fetched and processed successfully',
                    'data': response_data
                })
            else:
                # Update activity record with failure
                if activity:
                    try:
                        activity.status = 'failed'
                        activity.duration_seconds = fetch_duration
                        activity.detail = f'Data fetch failed: {result["error"]}'
                        activity.error_message = result.get('details', 'Data fetch failed')
                        activity.save()
                        logger.info(f"Updated activity record {activity.activity_id} with failure")
                    except Exception as update_error:
                        logger.warning(f"Failed to update activity record: {update_error}")
                
                return JsonResponse({
                    'success': False,
                    'error': result['error'],
                    'details': result.get('details', 'Data fetch failed')
                }, status=result.get('status_code', 500))
                
        except Exception as e:
            logger.error(f"Unexpected error in FetchAmazonDataView: {e}", exc_info=True)
            
            # Update activity record with error
            if 'activity' in locals() and activity:
                try:
                    activity.status = 'failed'
                    activity.detail = f'Unexpected error occurred: {str(e)[:200]}'
                    activity.error_message = str(e)
                    activity.save()
                    logger.info(f"Updated activity record {activity.activity_id} with error")
                except Exception as update_error:
                    logger.warning(f"Failed to update activity record: {update_error}")
            
            return JsonResponse({
                'success': False,
                'error': 'Unexpected error occurred',
                'details': 'An unexpected error occurred while fetching Amazon data'
            }, status=500)
    
    def fetch_orders_with_items(
        self, 
        headers: Dict[str, str], 
        base_url: str,
        marketplace_id: str, 
        start_date: str, 
        end_date: str, 
        max_orders: Union[int, float]
    ) -> Dict:
        """
        Main orchestration method for fetching orders and their items.
        
        This method coordinates the entire data fetching process:
        1. Fetches all orders within the date range
        2. Fetches items for each order
        3. Structures the data for the response
        
        Args:
            headers (Dict[str, str]): Request headers including access token
            base_url (str): Base URL for the marketplace's API endpoint
            marketplace_id (str): Amazon marketplace ID
            start_date (str): Start date in ISO format
            end_date (str): End date in ISO format
            max_orders (Union[int, float]): Maximum number of orders to fetch (float('inf') for unlimited)
            
        Returns:
            Dict: Contains orders, items, and summary information
        """
        try:
            # Step 1: Fetch all orders with pagination
            logger.info("Step 1: Fetching orders...")
            orders_result = self.fetch_all_orders(headers, base_url, marketplace_id, start_date, end_date, max_orders)
            
            if not orders_result['success']:
                return orders_result
            
            orders = orders_result['orders']
            logger.info(f"Fetched {len(orders)} orders")
            
            if not orders:
                return {
                    'success': True,
                    'data': {
                        'orders': [],
                        'metadata': {
                            'fetched_at': datetime.utcnow().isoformat() + 'Z',
                            'total_orders': 0,
                            'total_items': 0
                        }
                    },
                    'summary': {
                        'total_orders': 0,
                        'total_items': 0
                    }
                }
            
            # Step 2: Fetch items for all orders
            logger.info("Step 2: Fetching order items...")
            items_result = self.fetch_order_items_batch(headers, base_url, orders)
            
            if not items_result['success']:
                return items_result
            
            # Step 3: Structure the data
            logger.info("Step 3: Structuring data...")
            structured_data = self.structure_order_data(orders, items_result['items'])
            
            return {
                'success': True,
                'data': structured_data,
                'summary': {
                    'total_orders': len(orders),
                    'total_items': sum(len(items) for items in items_result['items'].values()),
                    'failed_orders': items_result.get('failed_orders', [])
                }
            }
            
        except Exception as e:
            logger.error(f"Error in fetch_orders_with_items: {e}", exc_info=True)
            return {
                'success': False,
                'error': 'Failed to fetch orders and items',
                'details': str(e)
            }
    
    def fetch_all_orders(
        self, 
        headers: Dict[str, str], 
        base_url: str,
        marketplace_id: str, 
        start_date: str, 
        end_date: str, 
        max_orders: Union[int, float]
    ) -> Dict:
        """
        Fetch all orders within the date range with pagination.
        
        This method handles pagination and rate limiting while fetching orders.
        It continues fetching until either:
        - All orders are fetched
        - Max orders limit is reached (if set)
        - No more orders are available
        
        Args:
            headers (Dict[str, str]): Request headers including access token
            base_url (str): Base URL for the marketplace's API endpoint
            marketplace_id (str): Amazon marketplace ID
            start_date (str): Start date in ISO format
            end_date (str): End date in ISO format
            max_orders (Union[int, float]): Maximum number of orders to fetch (float('inf') for unlimited)
            
        Returns:
            Dict: Contains the fetched orders or error information
        """
        try:
            all_orders = []
            next_token = None
            
            while True:
                # Prepare request parameters
                params = {
                    'MarketplaceIds': marketplace_id,
                    'CreatedAfter': start_date,
                    'CreatedBefore': end_date,
                    'OrderStatuses': 'Shipped,Unshipped,PartiallyShipped,Canceled,Unfulfillable',
                    'MaxResultsPerPage': 100  # Maximum allowed by Amazon
                }
                
                if next_token:
                    params['NextToken'] = next_token
                
                # Make the API request
                url = f"{base_url}{self.ORDERS_ENDPOINT}"
                logger.info(f"🔍 Amazon API Request: {url}")
                logger.info(f"🔍 Request params: {params}")
                response = self.make_rate_limited_request('GET', url, headers, params=params)
                logger.info(f"🔍 Amazon API Response status: {response.status_code}")
                
                if response.status_code != 200:
                    error_info = self.handle_api_error(response, 'fetch orders')
                    return {
                        'success': False,
                        'error': error_info['error'],
                        'details': error_info['details'],
                        'status_code': response.status_code
                    }
                
                # Parse response
                data = response.json()
                # Amazon SP-API returns orders in payload.Orders structure
                payload = data.get('payload', {})
                orders = payload.get('Orders', [])
                logger.info(f"🔍 Amazon API Response: Found {len(orders)} orders in this batch")
                if len(orders) == 0:
                    logger.info(f"🔍 Response data keys: {list(data.keys())}")
                    logger.info(f"🔍 Payload keys: {list(payload.keys()) if payload else 'No payload'}")
                    logger.info(f"🔍 Full response: {data}")
                all_orders.extend(orders)
                
                # Check if we've reached the maximum orders (if limit is set)
                if max_orders != float('inf') and len(all_orders) >= max_orders:
                    all_orders = all_orders[:max_orders]
                    break
                
                # Check if there are more orders to fetch
                next_token = payload.get('NextToken')
                if not next_token:
                    break
            
            return {
                'success': True,
                'orders': all_orders
            }
            
        except Exception as e:
            logger.error(f"Error in fetch_all_orders: {e}", exc_info=True)
            return {
                'success': False,
                'error': 'Failed to fetch orders',
                'details': str(e)
            }
    
    def fetch_order_items_batch(self, headers: Dict[str, str], base_url: str, orders: List[Dict]) -> Dict:
        """
        Fetch order items for a batch of orders with conservative rate limiting.
        
        This method processes orders in small batches to strictly respect Amazon's rate limits.
        It uses minimal concurrency and adds proper delays between batches to prevent
        overwhelming the API.
        
        Args:
            headers (Dict[str, str]): Request headers including access token
            base_url (str): Base URL for the marketplace's API endpoint
            orders (List[Dict]): List of orders to fetch items for
            
        Returns:
            Dict: Contains the fetched items or error information
        """
        all_items = {}
        failed_orders = []
        total_orders = len(orders)
        consecutive_rate_limits = 0  # Track consecutive rate limit errors
        
        logger.info(f"Starting order items fetch for {total_orders} orders with batch size {self.BATCH_SIZE}")
        
        # Calculate estimated time with new conservative rates
        num_batches = (total_orders + self.BATCH_SIZE - 1) // self.BATCH_SIZE
        # Each order item request takes ~3 seconds due to rate limiting, plus 10 seconds between batches
        estimated_minutes = ((total_orders * 3) + ((num_batches - 1) * 10)) / 60
        logger.info(f"Estimated processing time: {estimated_minutes:.1f} minutes for {num_batches} batches")
        
        # Process orders in smaller batches to respect rate limits
        for batch_num, i in enumerate(range(0, len(orders), self.BATCH_SIZE), 1):
            batch = orders[i:i + self.BATCH_SIZE]
            batch_size = len(batch)
            
            logger.info(f"Processing batch {batch_num}: orders {i+1}-{i+batch_size} of {total_orders}")
            
            # Use ThreadPoolExecutor with minimal concurrency
            with ThreadPoolExecutor(max_workers=self.MAX_CONCURRENT_REQUESTS) as executor:
                future_to_order = {
                    executor.submit(
                        self.fetch_single_order_items,
                        headers,
                        base_url,
                        order
                    ): order for order in batch
                }
                
                for future in as_completed(future_to_order):
                    order = future_to_order[future]
                    try:
                        result = future.result()
                        if result['success']:
                            all_items[order['AmazonOrderId']] = result['items']
                            consecutive_rate_limits = 0  # Reset counter on success
                        else:
                            # Check if this was a rate limit error
                            if 'rate limit' in result.get('error', '').lower() or 'too many requests' in result.get('error', '').lower():
                                consecutive_rate_limits += 1
                                logger.warning(f"Consecutive rate limits: {consecutive_rate_limits}")
                            
                            failed_orders.append({
                                'order_id': order['AmazonOrderId'],
                                'error': result.get('error', 'Unknown error')
                            })
                    except Exception as e:
                        failed_orders.append({
                            'order_id': order['AmazonOrderId'],
                            'error': str(e)
                        })
            
            # Add longer delay between batches to be more conservative
            if i + self.BATCH_SIZE < len(orders):
                # Progressive delay based on consecutive rate limits
                base_delay = 10  # Base 10 second delay
                progressive_delay = consecutive_rate_limits * 5  # Add 5 seconds for each consecutive rate limit
                delay_time = base_delay + progressive_delay
                
                if consecutive_rate_limits > 0:
                    logger.warning(f"Adding {progressive_delay}s extra delay due to {consecutive_rate_limits} consecutive rate limits")
                
                logger.info(f"Batch {batch_num} completed. Waiting {delay_time}s before next batch...")
                time.sleep(delay_time)
        
        logger.info(f"Order items fetch completed. Success: {len(all_items)}, Failed: {len(failed_orders)}")
        
        return {
            'success': True,
            'items': all_items,
            'failed_orders': failed_orders
        }
    
    def fetch_single_order_items(self, headers: Dict[str, str], base_url: str, order: Dict) -> Dict:
        """
        Fetch items for a single order.
        
        This method handles the API request for a single order's items,
        including error handling and rate limiting.
        
        Args:
            headers (Dict[str, str]): Request headers including access token
            base_url (str): Base URL for the marketplace's API endpoint
            order (Dict): Order to fetch items for
            
        Returns:
            Dict: Contains the fetched items or error information
        """
        try:
            order_id = order['AmazonOrderId']
            url = f"{base_url}{self.ORDER_ITEMS_ENDPOINT.format(order_id=order_id)}"
            
            response = self.make_rate_limited_request('GET', url, headers, is_order_items=True)
            
            if response.status_code != 200:
                error_info = self.handle_api_error(response, f'fetch items for order {order_id}')
                return {
                    'success': False,
                    'error': error_info['error'],
                    'details': error_info['details']
                }
            
            data = response.json()
            # Amazon SP-API returns order items in payload.OrderItems structure
            payload = data.get('payload', {})
            return {
                'success': True,
                'items': payload.get('OrderItems', [])
            }
            
        except Exception as e:
            logger.error(f"Error fetching items for order {order_id}: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def make_rate_limited_request(
        self, 
        method: str, 
        url: str, 
        headers: Dict[str, str], 
        params: Optional[Dict] = None, 
        data: Optional[Dict] = None,
        is_order_items: bool = False
    ) -> requests.Response:
        """
        Make HTTP request with rate limiting and retry logic.
        
        This method ensures we don't exceed Amazon's rate limits by:
        1. Using the token bucket rate limiter
        2. Handling rate limit responses (429)
        3. Handling quota exceeded errors (403)
        4. Implementing retry logic with exponential backoff
        
        Args:
            method (str): HTTP method (GET, POST, etc.)
            url (str): Request URL
            headers (Dict[str, str]): Request headers
            params (Optional[Dict]): URL parameters
            data (Optional[Dict]): Request body data
            is_order_items (bool): Whether this is an order items request
            
        Returns:
            requests.Response: The API response
            
        Raises:
            requests.exceptions.RequestException: If all retries fail
        """
        max_retries = 3
        retry_delay = 1
        
        for attempt in range(max_retries):
            try:
                # Apply rate limiting based on endpoint type
                if is_order_items:
                    self.order_items_rate_limiter.acquire()
                else:
                    self.orders_rate_limiter.acquire()
                
                response = self.session.request(
                    method=method,
                    url=url,
                    headers=headers,
                    params=params,
                    json=data,
                    timeout=self.REQUEST_TIMEOUT
                )
                
                # Handle rate limiting
                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', retry_delay))
                    # Add extra buffer time to be more conservative
                    retry_after = retry_after + 2  # Add 2 extra seconds
                    endpoint_type = "order items" if is_order_items else "orders"
                    logger.warning(f"Rate limited on {endpoint_type} endpoint. Waiting {retry_after}s before retry (attempt {attempt + 1}/{max_retries})")
                    time.sleep(retry_after)
                    retry_delay = min(retry_delay * 2, 300)  # Cap at 5 minutes
                    continue
                
                # Handle quota exceeded errors with better logging
                if response.status_code == 403:
                    try:
                        error_data = response.json()
                        if 'errors' in error_data:
                            error = error_data['errors'][0]
                            if error.get('code') == 'QuotaExceeded':
                                retry_after = int(response.headers.get('Retry-After', 60))
                                endpoint_type = "order items" if is_order_items else "orders"
                                logger.warning(f"Quota exceeded on {endpoint_type} endpoint. Waiting {retry_after}s before retry (attempt {attempt + 1}/{max_retries})")
                                time.sleep(retry_after)
                                retry_delay = min(retry_delay * 2, 300)  # Cap at 5 minutes
                                continue
                            elif error.get('code') in ['Unauthorized', 'InvalidAccessToken']:
                                logger.error(f"Authentication failed: {error.get('message', 'Invalid access token')}")
                                # Don't retry authentication errors
                                break
                    except (json.JSONDecodeError, KeyError, IndexError):
                        pass
                
                return response
                
            except requests.exceptions.Timeout:
                if attempt < max_retries - 1:
                    logger.warning(f"Request timeout, retrying in {retry_delay}s...")
                    time.sleep(retry_delay)
                    retry_delay *= 2
                    continue
                raise
            
            except requests.exceptions.RequestException as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Request failed, retrying in {retry_delay}s: {e}")
                    time.sleep(retry_delay)
                    retry_delay *= 2
                    continue
                raise
        
        # If we get here, all retries failed
        raise requests.exceptions.RequestException("Max retries exceeded")
    
    def handle_api_error(self, response: requests.Response, operation: str) -> Dict[str, str]:
        """
        Handle Amazon API errors with user-friendly messages.
        
        This method translates Amazon API errors into user-friendly messages
        and provides detailed information for debugging.
        
        Args:
            response (requests.Response): The API response
            operation (str): The operation that failed (for context)
            
        Returns:
            Dict[str, str]: Contains error and details
        """
        try:
            error_data = response.json()
            errors = error_data.get('errors', [])
            
            if errors:
                error_code = errors[0].get('code', 'unknown')
                error_message = errors[0].get('message', 'Unknown error')
                
                # Map common Amazon errors to user-friendly messages
                if error_code == 'InvalidInput':
                    user_error = 'Invalid parameters provided'
                    user_details = f'Amazon API error: {error_message}'
                elif error_code == 'Unauthorized':
                    user_error = 'Authentication failed'
                    user_details = 'Your access token may have expired. Please refresh your connection.'
                elif error_code == 'Forbidden':
                    user_error = 'Access denied'
                    user_details = 'Your app may not have permission for this operation.'
                elif error_code == 'NotFound':
                    user_error = 'Resource not found'
                    user_details = f'The requested {operation} could not be found.'
                elif error_code == 'TooManyRequests':
                    user_error = 'Rate limit exceeded'
                    user_details = 'Too many requests. Please try again later.'
                else:
                    user_error = f'Amazon API error during {operation}'
                    user_details = f'{error_code}: {error_message}'
            else:
                user_error = f'API error during {operation}'
                user_details = f'HTTP {response.status_code}: {response.text[:200]}'
                
        except json.JSONDecodeError:
            user_error = f'API error during {operation}'
            user_details = f'HTTP {response.status_code}: {response.text[:200]}'
        
        return {
            'error': user_error,
            'details': user_details
        }
    
    def structure_order_data(self, orders: List[Dict], order_items: Dict[str, List[Dict]]) -> Dict:
        """
        Structure the fetched data for optimal frontend consumption.
        
        This method organizes the raw API data into a clean, structured format
        that's easy to use in the frontend. It includes:
        - All raw order details from Amazon API
        - All raw item details from Amazon API
        - Metadata and summary information
        
        Args:
            orders (List[Dict]): List of orders from the API
            order_items (Dict[str, List[Dict]]): Dictionary of order items by order ID
            
        Returns:
            Dict: Structured data ready for frontend consumption with all raw fields
        """
        structured_orders = []
        all_order_items = []
        
        for order in orders:
            order_id = order.get('AmazonOrderId')
            
            # Get items for this order
            items = order_items.get(order_id, [])
            
            # Use all raw order data without mapping
            structured_order = dict(order)  # Copy all fields from original order
            structured_order['items'] = []  # Add empty items array for nested structure
            
            # Add all raw items with order_id added
            for item in items:
                # Use all raw item data without mapping
                raw_item = dict(item)  # Copy all fields from original item
                raw_item['order_id'] = order_id  # Add order_id for reference
                
                # Add to both nested structure and flat array
                structured_order['items'].append(raw_item)
                all_order_items.append(raw_item)
            
            structured_orders.append(structured_order)
        
        return {
            'orders': structured_orders,
            'order_items': all_order_items,  # Flat array of all items with all raw fields
            'metadata': {
                'fetched_at': datetime.utcnow().isoformat() + 'Z',
                'total_orders': len(structured_orders),
                'total_items': len(all_order_items)
            }
        }
    
    def get(self, request):
        """
        Handle GET requests with helpful information.
        
        This method provides API documentation and usage information
        when the endpoint is accessed with a GET request.
        
        Returns:
            JsonResponse: API documentation and usage information
        """
        return JsonResponse({
            'message': 'Amazon Data Fetch API',
            'methods': ['POST'],
            'required_fields': ['access_token', 'marketplace_id', 'start_date', 'end_date'],
            'optional_fields': ['max_orders'],
            'description': 'Efficiently fetch Amazon orders and order items with pagination',
            'date_format': 'ISO 8601 (YYYY-MM-DDTHH:MM:SSZ)',
            'max_date_range': '30 days',
            'example_marketplaces': {
                'US': 'ATVPDKIKX0DER',
                'CA': 'A2EUQ1WTGCTBG2',
                'UK': 'A1F83G8C2ARO7P',
                'DE': 'A1PA6795UKMFR9',
                'FR': 'A13V1IB3VIYZZH',
                'IT': 'APJ6JRA9NG5V4',
                'ES': 'A1RKKUPIHCS9HS'
            }
        })


@method_decorator(csrf_exempt, name='dispatch')
class DownloadProcessedDataView(View):
    """
    Handle downloading of processed data as CSV files.
    """
    
    def get(self, request):
        """
        Debug endpoint to check available cache keys.
        """
        try:
            cache_keys = list(FetchAmazonDataView._processed_data_cache.keys())
            cache_info = {}
            
            for key in cache_keys:
                data = FetchAmazonDataView._processed_data_cache[key]
                cache_info[key] = {
                    'marketplace_name': data.get('marketplace_name'),
                    'marketplace_id': data.get('marketplace_id'),
                    'created_at': data.get('created_at'),
                    'mssql_records': len(data.get('mssql_data', [])),
                    'azure_records': len(data.get('azure_data', []))
                }
            
            return JsonResponse({
                'success': True,
                'cache_keys': cache_keys,
                'cache_info': cache_info,
                'total_cached_items': len(cache_keys)
            })
            
        except Exception as e:
            logger.error(f"Error getting cache debug info: {e}", exc_info=True)
            return JsonResponse({
                'success': False,
                'error': 'Failed to get cache info',
                'details': str(e)
            }, status=500)
    
    def post(self, request):
        """
        Generate and download processed data as CSV.
        
        Expected request parameters:
        - cache_key: Key to identify the processed data
        - data_type: 'mssql' or 'azure'
        
        Returns:
            HttpResponse: CSV file download
        """
        try:
            data = json.loads(request.body)
            cache_key = data.get('cache_key')
            data_type = data.get('data_type', 'mssql').lower()
            
            if not cache_key:
                return JsonResponse({
                    'success': False,
                    'error': 'Cache key is required'
                }, status=400)
            
            if data_type not in ['mssql', 'azure']:
                return JsonResponse({
                    'success': False,
                    'error': 'Data type must be either "mssql" or "azure"'
                }, status=400)
            
            # Debug: Log cache information
            available_keys = list(FetchAmazonDataView._processed_data_cache.keys())
            logger.info(f"🔍 Download request for cache_key: {cache_key}")
            logger.info(f"🔍 Available cache keys: {available_keys}")
            logger.info(f"🔍 Total cached items: {len(available_keys)}")
            
            # Get processed data from cache
            cached_data = FetchAmazonDataView._processed_data_cache.get(cache_key)
            if not cached_data:
                logger.error(f"❌ Cache key '{cache_key}' not found. Available keys: {available_keys}")
                
                # Try to find the most recent file as fallback
                try:
                    processed_dir = Path("processed_data")
                    if processed_dir.exists():
                        if data_type == 'mssql':
                            pattern = "MSSQL_data_*.csv"
                        else:
                            pattern = "AZURE_data_*.csv"
                        
                        files = list(processed_dir.glob(pattern))
                        if files:
                            # Get the most recent file
                            latest_file = max(files, key=lambda x: x.stat().st_ctime)
                            logger.info(f"🔄 Using fallback file: {latest_file}")
                            
                            # Read the CSV file
                            df = pd.read_csv(latest_file)
                            csv_content = df.to_csv(index=False, encoding='utf-8')
                            
                            # Create HTTP response with CSV content
                            response = HttpResponse(
                                csv_content,
                                content_type='text/csv'
                            )
                            response['Content-Disposition'] = f'attachment; filename="{latest_file.name}"'
                            response['Content-Length'] = len(csv_content.encode('utf-8'))
                            
                            logger.info(f"✅ Downloaded fallback {data_type.upper()} data: {latest_file.name}")
                            return response
                        else:
                            logger.error(f"No {data_type.upper()} files found in processed_data directory")
                    else:
                        logger.error("processed_data directory does not exist")
                except Exception as fallback_error:
                    logger.error(f"Fallback file read failed: {fallback_error}")
                
                return JsonResponse({
                    'success': False,
                    'error': 'Processed data not found or expired',
                    'details': f'Cache key "{cache_key}" not found and no fallback files available. Available keys: {available_keys}'
                }, status=404)
            
            # Get the appropriate dataset
            if data_type == 'mssql':
                records = cached_data['mssql_data']
                filename_prefix = 'MSSQL_data'
            else:
                records = cached_data['azure_data']
                filename_prefix = 'AZURE_data'
            
            if not records:
                return JsonResponse({
                    'success': False,
                    'error': f'No {data_type.upper()} data available'
                }, status=404)
            
            # Convert to DataFrame and then to CSV
            df = pd.DataFrame(records)
            
            # Generate filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            marketplace_name = cached_data.get('marketplace_name', 'Unknown')
            filename = f"{filename_prefix}_{marketplace_name}_{timestamp}.csv"
            
            # Create CSV content
            csv_content = df.to_csv(index=False, encoding='utf-8')
            
            # Create HTTP response with CSV content
            response = HttpResponse(
                csv_content,
                content_type='text/csv'
            )
            response['Content-Disposition'] = f'attachment; filename="{filename}"'
            response['Content-Length'] = len(csv_content.encode('utf-8'))
            
            logger.info(f"✅ Downloaded {data_type.upper()} processed data: {filename}")
            return response
            
        except json.JSONDecodeError:
            return JsonResponse({
                'success': False,
                'error': 'Invalid JSON format'
            }, status=400)
        except Exception as e:
            logger.error(f"Error downloading processed data: {e}", exc_info=True)
            return JsonResponse({
                'success': False,
                'error': 'Failed to download processed data',
                'details': str(e)
            }, status=500)


@method_decorator(csrf_exempt, name='dispatch')
class ProcessedDataStatusView(View):
    """
    Get status of processed data and available downloads.
    """
    
    def get(self, request):
        """
        Get processed data status and statistics.
        
        Returns:
            JsonResponse: Status information
        """
        try:
            processed_dir = Path("processed_data")
            
            if not processed_dir.exists():
                return JsonResponse({
                    'success': True,
                    'status': 'no_data',
                    'message': 'No processed data available',
                    'stats': {
                        'total_files': 0,
                        'mssql_files': 0,
                        'azure_files': 0,
                        'total_size': 0
                    }
                })
            
            mssql_files = list(processed_dir.glob("MSSQL_data_*.csv"))
            azure_files = list(processed_dir.glob("AZURE_data_*.csv"))
            all_files = mssql_files + azure_files
            
            total_size = sum(f.stat().st_size for f in all_files)
            
            # Get latest files info
            latest_files = []
            if all_files:
                all_files.sort(key=lambda x: x.stat().st_ctime, reverse=True)
                for file_path in all_files[:10]:  # Latest 10 files
                    file_stat = file_path.stat()
                    latest_files.append({
                        'filename': file_path.name,
                        'size': file_stat.st_size,
                        'created': datetime.fromtimestamp(file_stat.st_ctime).isoformat(),
                        'type': 'MSSQL' if 'MSSQL_data' in file_path.name else 'AZURE'
                    })
            
            return JsonResponse({
                'success': True,
                'status': 'available' if all_files else 'no_data',
                'stats': {
                    'total_files': len(all_files),
                    'mssql_files': len(mssql_files),
                    'azure_files': len(azure_files),
                    'total_size': total_size,
                    'total_size_mb': round(total_size / (1024 * 1024), 2)
                },
                'latest_files': latest_files
            })
            
        except Exception as e:
            logger.error(f"Error getting processed data status: {e}", exc_info=True)
            return JsonResponse({
                'success': False,
                'error': 'Failed to get processed data status',
                'details': str(e)
            }, status=500)


@method_decorator(csrf_exempt, name='dispatch')
class ActivitiesListView(View):
    """
    Handle listing and filtering of activities.
    """
    
    def get(self, request):
        """
        Get list of activities with pagination and filtering.
        
        Query parameters:
        - page: Page number (default: 1)
        - page_size: Items per page (default: 10, max: 100)
        - marketplace_id: Filter by marketplace
        - status: Filter by status
        - activity_type: Filter by activity type
        - search: Search in details and error messages
        - date_from: Filter activities from this date
        - date_to: Filter activities to this date
        
        Returns:
            JsonResponse: Paginated list of activities
        """
        try:
            # Get query parameters
            page = int(request.GET.get('page', 1))
            page_size = min(int(request.GET.get('page_size', 10)), 100)
            marketplace_id = request.GET.get('marketplace_id')
            status = request.GET.get('status')
            activity_type = request.GET.get('activity_type')
            search = request.GET.get('search')
            date_from = request.GET.get('date_from')
            date_to = request.GET.get('date_to')
            
            # Build query
            queryset = Activities.objects.all()
            
            # Apply filters
            if marketplace_id:
                queryset = queryset.filter(marketplace_id=marketplace_id)
            
            if status:
                queryset = queryset.filter(status=status)
            
            if activity_type:
                queryset = queryset.filter(activity_type=activity_type)
            
            if search:
                queryset = queryset.filter(
                    Q(detail__icontains=search) | 
                    Q(error_message__icontains=search) |
                    Q(marketplace_id__icontains=search)
                )
            
            if date_from:
                try:
                    date_from_parsed = datetime.fromisoformat(date_from.replace('Z', '+00:00')).date()
                    queryset = queryset.filter(date_from__gte=date_from_parsed)
                except ValueError:
                    pass
            
            if date_to:
                try:
                    date_to_parsed = datetime.fromisoformat(date_to.replace('Z', '+00:00')).date()
                    queryset = queryset.filter(date_to__lte=date_to_parsed)
                except ValueError:
                    pass
            
            # Order by most recent first
            queryset = queryset.order_by('-activity_date')
            
            # Paginate
            paginator = Paginator(queryset, page_size)
            page_obj = paginator.get_page(page)
            
            # Helper function to format datetime properly
            def format_datetime(dt):
                if dt is None:
                    return None
                # Convert to UTC and format with Z suffix
                if dt.tzinfo is None:
                    # If naive datetime, assume UTC
                    return dt.isoformat() + 'Z'
                else:
                    # If timezone-aware, convert to UTC
                    utc_dt = dt.astimezone(timezone.utc)
                    return utc_dt.replace(tzinfo=None).isoformat() + 'Z'
            
            # Serialize activities
            activities = []
            for activity in page_obj:
                activity_data = {
                    'activity_id': str(activity.activity_id),
                    'marketplace_id': activity.marketplace_id,
                    'marketplace_name': activity.marketplace_name,
                    'activity_type': activity.activity_type,
                    'activity_type_display': activity.get_activity_type_display(),
                    'status': activity.status,
                    'status_display': activity.get_status_display(),
                    'action': activity.action,
                    'action_display': activity.get_action_display(),
                    'activity_date': format_datetime(activity.activity_date),
                    'date_from': activity.date_from.isoformat() if activity.date_from else None,
                    'date_to': activity.date_to.isoformat() if activity.date_to else None,
                    'orders_fetched': activity.orders_fetched,
                    'items_fetched': activity.items_fetched,
                    'total_records': activity.total_records,
                    'duration_seconds': activity.duration_seconds,
                    'duration_formatted': activity.duration_formatted,
                    'detail': activity.detail,
                    'error_message': activity.error_message,
                    'database_saved': activity.database_saved,
                    'created_at': format_datetime(activity.created_at),
                    'updated_at': format_datetime(activity.updated_at),
                }
                activities.append(activity_data)
            
            return JsonResponse({
                'success': True,
                'data': {
                    'activities': activities,
                    'pagination': {
                        'current_page': page,
                        'total_pages': paginator.num_pages,
                        'total_items': paginator.count,
                        'page_size': page_size,
                        'has_next': page_obj.has_next(),
                        'has_previous': page_obj.has_previous(),
                    }
                }
            })
            
        except ValueError as e:
            return JsonResponse({
                'success': False,
                'error': 'Invalid parameter format',
                'details': str(e)
            }, status=400)
        except Exception as e:
            logger.error(f"Error listing activities: {e}", exc_info=True)
            return JsonResponse({
                'success': False,
                'error': 'Failed to list activities',
                'details': str(e)
            }, status=500)


@method_decorator(csrf_exempt, name='dispatch')
class ActivityDetailView(View):
    """
    Handle individual activity details.
    """
    
    def get(self, request, activity_id):
        """
        Get detailed information about a specific activity.
        
        Args:
            activity_id: UUID of the activity
            
        Returns:
            JsonResponse: Detailed activity information
        """
        try:
            activity = Activities.objects.get(activity_id=activity_id)
            
            # Helper function to format datetime properly
            def format_datetime(dt):
                if dt is None:
                    return None
                # Convert to UTC and format with Z suffix
                if dt.tzinfo is None:
                    # If naive datetime, assume UTC
                    return dt.isoformat() + 'Z'
                else:
                    # If timezone-aware, convert to UTC
                    utc_dt = dt.astimezone(timezone.utc)
                    return utc_dt.replace(tzinfo=None).isoformat() + 'Z'
            
            activity_data = {
                'activity_id': str(activity.activity_id),
                'marketplace_id': activity.marketplace_id,
                'marketplace_name': activity.marketplace_name,
                'activity_type': activity.activity_type,
                'activity_type_display': activity.get_activity_type_display(),
                'status': activity.status,
                'status_display': activity.get_status_display(),
                'action': activity.action,
                'action_display': activity.get_action_display(),
                'activity_date': format_datetime(activity.activity_date),
                'date_from': activity.date_from.isoformat() if activity.date_from else None,
                'date_to': activity.date_to.isoformat() if activity.date_to else None,
                'orders_fetched': activity.orders_fetched,
                'items_fetched': activity.items_fetched,
                'total_records': activity.total_records,
                'duration_seconds': activity.duration_seconds,
                'duration_formatted': activity.duration_formatted,
                'detail': activity.detail,
                'error_message': activity.error_message,
                'database_saved': activity.database_saved,
                'created_at': format_datetime(activity.created_at),
                'updated_at': format_datetime(activity.updated_at),
            }
            
            return JsonResponse({
                'success': True,
                'data': activity_data
            })
            
        except Activities.DoesNotExist:
            return JsonResponse({
                'success': False,
                'error': 'Activity not found',
                'details': f'No activity found with ID: {activity_id}'
            }, status=404)
        except Exception as e:
            logger.error(f"Error getting activity detail: {e}", exc_info=True)
            return JsonResponse({
                'success': False,
                'error': 'Failed to get activity details',
                'details': str(e)
            }, status=500)


@method_decorator(csrf_exempt, name='dispatch')
class ActivitiesStatsView(View):
    """
    Handle activity statistics and summary information.
    """
    
    def get(self, request):
        """
        Get activity statistics and summary.
        
        Query parameters:
        - days: Number of days to include in stats (default: 30)
        - marketplace_id: Filter by marketplace
        
        Returns:
            JsonResponse: Activity statistics
        """
        try:
            days = int(request.GET.get('days', 30))
            marketplace_id = request.GET.get('marketplace_id')
            
            # Calculate date range
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=days)
            
            # Build base query
            queryset = Activities.objects.filter(
                activity_date__date__gte=start_date,
                activity_date__date__lte=end_date
            )
            
            if marketplace_id:
                queryset = queryset.filter(marketplace_id=marketplace_id)
            
            # Overall statistics
            total_activities = queryset.count()
            completed_activities = queryset.filter(status='completed').count()
            failed_activities = queryset.filter(status='failed').count()
            in_progress_activities = queryset.filter(status='in_progress').count()
            
            # Success rate
            success_rate = (completed_activities / total_activities * 100) if total_activities > 0 else 0
            
            # Total records processed
            completed_queryset = queryset.filter(status='completed')
            total_orders = sum(activity.orders_fetched for activity in completed_queryset)
            total_items = sum(activity.items_fetched for activity in completed_queryset)
            
            # Average duration
            completed_with_duration = completed_queryset.exclude(duration_seconds__isnull=True)
            avg_duration = None
            if completed_with_duration.exists():
                total_duration = sum(activity.duration_seconds for activity in completed_with_duration)
                avg_duration = total_duration / completed_with_duration.count()
            
            # Activity breakdown by status
            status_breakdown = {}
            for choice in Activities.STATUS_CHOICES:
                status_code = choice[0]
                status_display = choice[1]
                count = queryset.filter(status=status_code).count()
                status_breakdown[status_code] = {
                    'display': status_display,
                    'count': count,
                    'percentage': (count / total_activities * 100) if total_activities > 0 else 0
                }
            
            # Activity breakdown by marketplace
            marketplace_breakdown = {}
            marketplace_names = {
                'ATVPDKIKX0DER': 'United States',
                'A2EUQ1WTGCTBG2': 'Canada',
                'A1F83G8C2ARO7P': 'United Kingdom',
                'A1PA6795UKMFR9': 'Germany',
                'A13V1IB3VIYZZH': 'France',
                'APJ6JRA9NG5V4': 'Italy',
                'A1RKKUPIHCS9HS': 'Spain',
            }
            
            for marketplace in queryset.values_list('marketplace_id', flat=True).distinct():
                count = queryset.filter(marketplace_id=marketplace).count()
                marketplace_breakdown[marketplace] = {
                    'name': marketplace_names.get(marketplace, marketplace),
                    'count': count,
                    'percentage': (count / total_activities * 100) if total_activities > 0 else 0
                }
            
            # Recent activity (last 5)
            recent_activities = []
            for activity in queryset.order_by('-activity_date')[:5]:
                recent_activities.append({
                    'activity_id': str(activity.activity_id),
                    'marketplace_name': activity.marketplace_name,
                    'status': activity.status,
                    'status_display': activity.get_status_display(),
                    'activity_date': activity.activity_date.isoformat() + 'Z',
                    'orders_fetched': activity.orders_fetched,
                    'items_fetched': activity.items_fetched,
                    'duration_formatted': activity.duration_formatted,
                    'detail': activity.detail[:100] + '...' if len(activity.detail) > 100 else activity.detail
                })
            
            return JsonResponse({
                'success': True,
                'data': {
                    'period': {
                        'days': days,
                        'start_date': start_date.isoformat(),
                        'end_date': end_date.isoformat(),
                        'marketplace_id': marketplace_id
                    },
                    'summary': {
                        'total_activities': total_activities,
                        'completed_activities': completed_activities,
                        'failed_activities': failed_activities,
                        'in_progress_activities': in_progress_activities,
                        'success_rate': round(success_rate, 1),
                        'total_orders_processed': total_orders,
                        'total_items_processed': total_items,
                        'average_duration_seconds': round(avg_duration, 2) if avg_duration else None,
                        'average_duration_formatted': f"{avg_duration:.1f}s" if avg_duration and avg_duration < 60 else f"{avg_duration/60:.1f}m" if avg_duration else None
                    },
                    'breakdowns': {
                        'by_status': status_breakdown,
                        'by_marketplace': marketplace_breakdown
                    },
                    'recent_activities': recent_activities
                }
            })
            
        except ValueError as e:
            return JsonResponse({
                'success': False,
                'error': 'Invalid parameter format',
                'details': str(e)
            }, status=400)
        except Exception as e:
            logger.error(f"Error getting activity statistics: {e}", exc_info=True)
            return JsonResponse({
                'success': False,
                'error': 'Failed to get activity statistics',
                'details': str(e)
            }, status=500)
