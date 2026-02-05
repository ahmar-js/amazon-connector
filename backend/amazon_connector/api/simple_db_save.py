"""
Simplified database save utility with direct to_sql approach.
"""

import pandas as pd
import logging
from typing import Dict
from django.db import connections
import sqlalchemy
from sqlalchemy import event, create_engine

import time
from datetime import datetime
import re
from dotenv import load_dotenv
import os
load_dotenv()

logger = logging.getLogger(__name__)


# def fix_datetime_value(value):
#     """Fix problematic datetime values that cause SQL Server conversion errors."""
#     if pd.isna(value) or value == '':
#         return None
    
#     # Convert to string if not already
#     str_value = str(value).strip()
    
#     # Handle common problematic patterns
#     if re.match(r'^\d{1,2}:\d{2}\.\d+$', str_value):  # e.g., "01:41.0", "25:04.0"
#         return None  # These are invalid datetime formats, set to NULL
    
#     # Handle ISO datetime format
#     if 'T' in str_value and 'Z' in str_value:
#         try:
#             # Parse ISO format like "2022-06-01T00:01:41Z"
#             dt = datetime.fromisoformat(str_value.replace('Z', '+00:00'))
#             return dt.strftime('%Y-%m-%d %H:%M:%S')
#         except:
#             return None
    
#     # Handle date-only format
#     if re.match(r'^\d{1,2}/\d{1,2}/\d{4}$', str_value):  # e.g., "6/1/2022"
#         try:
#             dt = datetime.strptime(str_value, '%m/%d/%Y')
#             return dt.strftime('%Y-%m-%d')
#         except:
#             return None
    
#     # If we can't parse it, return None (NULL in database)
#     return None
import urllib.parse

def _sleep_with_jitter(base_seconds: float, attempt: int):
    """Sleep for exponential backoff with jitter."""
    # backoff: base * 2^(attempt-1) plus 0-250ms jitter
    delay = base_seconds * (2 ** max(0, attempt - 1))
    jitter_ms = int(250 * (attempt % 7))  # cheap pseudo-jitter without random
    time.sleep(delay + (jitter_ms / 1000.0))

def _to_sql_with_retries(df: pd.DataFrame, *, engine, table_name: str, if_exists: str = 'append', index: bool = False, max_retries: int = 3, base_backoff: float = 1.0) -> None:
    """
    Write DataFrame to SQL with up to max_retries attempts and exponential backoff.
    Logs detailed errors and re-raises on final failure.
    """
    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"➡️ to_sql attempt {attempt}/{max_retries} -> table={table_name}, rows={len(df)}")
            df.to_sql(name=table_name, con=engine, if_exists=if_exists, index=index)
            logger.info(f"✅ to_sql succeeded on attempt {attempt} -> table={table_name}")
            return
        except Exception as e:
            last_err = e
            logger.error(
                f"❌ to_sql failed on attempt {attempt}/{max_retries} for table={table_name}: {e}",
                exc_info=True,
            )
            if attempt < max_retries:
                _sleep_with_jitter(base_backoff, attempt)
            else:
                break
    # If here, all retries failed
    raise last_err
def create_mssql_connection():
    driver = "ODBC Driver 17 for SQL Server"
    server = os.getenv('MSSQL_SERVER')
    database = os.getenv('MSSQL_DATABASE')
    user = os.getenv('MSSQL_USER')
    password = os.getenv('MSSQL_PASSWORD')
    
    #MSSQL_params = urllib.parse.quote_plus(f"DRIVER={{{driver}}};SERVER={server};DATABASE={database};UID={user};PWD={password}")
    MSSQL_params = urllib.parse.quote_plus(f"DRIVER={{{driver}}};SERVER={server};DATABASE={database};UID={user};PWD={password};ConnectionTimeout=100;Timeout=100") # old one is 30 than 60 now 100

    MSSQL_sqlalchemy_url = f"mssql+pyodbc:///?odbc_connect={MSSQL_params}"
    return create_engine(MSSQL_sqlalchemy_url, 
              echo=False,  
              pool_recycle=300,  # for future 2 min 
              pool_size=20,  
              max_overflow=10,  
              pool_timeout=60)
    
# Create database connection AZURE
def create_Azure_db_connection():
    driver = "ODBC Driver 17 for SQL Server"
    server = os.getenv('AZURE_SERVER')
    database = os.getenv('AZURE_DATABASE')
    user = os.getenv('AZURE_USER')
    password = os.getenv('AZURE_PASSWORD')
    
    params = urllib.parse.quote_plus(f"DRIVER={{{driver}}};SERVER={server};DATABASE={database};UID={user};PWD={password}")
    sqlalchemy_url = f"mssql+pyodbc:///?odbc_connect={params}"
    return create_engine(sqlalchemy_url, 
              echo=False)

def save_simple(mssql_df: pd.DataFrame, azure_df: pd.DataFrame, marketplace_id: str) -> Dict:
    """
    Simple database save that relies on pandas to_sql auto-column matching.
    Includes database-level deduplication to prevent duplicate records.
    """
    
    # Marketplace to table mapping
    MARKETPLACE_TABLE_MAPPING = {
        'A1PA6795UKMFR9': 'amazon_api_de',  # Germany
        'A1RKKUPIHCS9HS': 'amazon_api_es',  # Spain
        'APJ6JRA9NG5V4': 'amazon_api_it',   # Italy
        'A1F83G8C2ARO7P': 'amazon_api_uk',  # United Kingdom
        'ATVPDKIKX0DER': 'amazon_api_usa',  # United States
        'A2EUQ1WTGCTBG2': 'amazon_api_ca',  # Canada
        'A13V1IB3VIYZZH': 'amazon_api_fr_test',  # France
        
    }
    
    results = {
        'success': True,
        'mssql_result': None,
        'azure_result': None,
        'total_records_saved': 0,
        'errors': [],
        'mssql_success': False,
        'azure_success': False
    }
    
    try:
        MSSQL_engine = create_mssql_connection()
        logging.info(f"MSSQL_engine : {MSSQL_engine}")
    
        def my_listener(conn, cursor, statement, parameters, context, executemany):
            if executemany:
                cursor.fast_executemany = True

        event.listen(MSSQL_engine, "before_cursor_execute", my_listener)
        
        # Save MSSQL DataFrame with deduplication
        table_name = MARKETPLACE_TABLE_MAPPING.get(marketplace_id)
        if table_name and not mssql_df.empty:
            try:
                logger.info(f"🔄 Saving MSSQL data: {len(mssql_df)} records to {table_name}")
                logger.info(f"MSSQL columns: {list(mssql_df.columns)}")
                
                # Clean data
                df_clean = mssql_df.copy()
                df_clean['PurchaseDate_conversion'] = pd.to_datetime(df_clean['PurchaseDate_conversion']).dt.strftime('%Y-%m-%d %H:%M:%S')
                
                original_count = len(df_clean)
                logger.info(f"📊 Original records to save: {original_count}")
                
                # SAFETY CHECK: Verify required columns exist before deduplication
                if 'AmazonOrderId' not in df_clean.columns or 'OrderItemId' not in df_clean.columns:
                    logger.error("❌ CRITICAL: Required columns missing for MSSQL deduplication!")
                    logger.error(f"❌ Expected: AmazonOrderId, OrderItemId")
                    logger.error(f"❌ Available: {df_clean.columns.tolist()}")
                    results['mssql_result'] = {
                        'success': False,
                        'error': 'Required columns missing - cannot verify duplicates',
                        'records_saved': 0,
                        'table_name': table_name
                    }
                    results['mssql_success'] = False
                    results['errors'].append("MSSQL save aborted - required columns missing")
                    df_clean = pd.DataFrame()  # Clear to prevent unsafe save
                
                # DEDUPLICATION: Remove duplicates based on AmazonOrderId + OrderItemId (composite key)
                if 'AmazonOrderId' in df_clean.columns and 'OrderItemId' in df_clean.columns:
                    # First, deduplicate within the DataFrame itself
                    before_dedup = len(df_clean)
                    df_clean = df_clean.drop_duplicates(subset=['AmazonOrderId', 'OrderItemId'], keep='first')
                    after_dedup = len(df_clean)
                    
                    if before_dedup != after_dedup:
                        logger.info(f"� Removed {before_dedup - after_dedup} duplicate records within DataFrame")
                    
                    # Second, check for existing records in database
                    from sqlalchemy import text
                    try:
                        order_ids = df_clean['AmazonOrderId'].unique().tolist()
                        
                        if order_ids:
                            logger.info(f"🔍 Checking database for {len(order_ids)} orders...")
                            
                            # Query database for existing combinations
                            placeholders = ','.join([f"'{oid}'" for oid in order_ids])
                            query = text(f"""
                                SELECT DISTINCT AmazonOrderId, OrderItemId
                                FROM {table_name}
                                WHERE AmazonOrderId IN ({placeholders})
                            """)
                            
                            with MSSQL_engine.connect() as conn:
                                result = conn.execute(query)
                                existing_combinations = {(row[0], row[1]) for row in result}
                            
                            if existing_combinations:
                                logger.info(f"🔍 Found {len(existing_combinations)} existing order-item combinations in database")
                                
                                # Filter out existing combinations
                                df_clean['_temp_key'] = df_clean.apply(
                                    lambda row: (row['AmazonOrderId'], row['OrderItemId']), 
                                    axis=1
                                )
                                
                                before_filter = len(df_clean)
                                df_clean = df_clean[~df_clean['_temp_key'].isin(existing_combinations)]
                                df_clean = df_clean.drop(columns=['_temp_key'])
                                after_filter = len(df_clean)
                                
                                filtered_count = before_filter - after_filter
                                logger.info(f"🔧 Filtered out {filtered_count} duplicate records")
                                logger.info(f"✅ After database deduplication: {after_filter} new records to save")
                                
                                if after_filter == 0:
                                    logger.info(f"ℹ️  All {original_count} records already exist in database - skipping MSSQL save")
                                    results['mssql_result'] = {
                                        'success': True,
                                        'records_saved': 0,
                                        'records_skipped': original_count,
                                        'table_name': table_name,
                                        'message': 'All records already exist (duplicates skipped)'
                                    }
                                    results['mssql_success'] = True
                                    # Don't return here, continue to Azure save
                                    df_clean = pd.DataFrame()  # Empty dataframe to skip save below
                            else:
                                logger.info(f"✅ No duplicates found - all {len(df_clean)} records are new")
                                
                    except Exception as dedup_error:
                        logger.error(f"❌ CRITICAL: Database deduplication check failed: {dedup_error}", exc_info=True)
                        logger.error(f"⚠️  ABORTING MSSQL SAVE to prevent duplicates!")
                        # DO NOT PROCEED - better to fail than insert duplicates
                        results['mssql_result'] = {
                            'success': False,
                            'error': f'Deduplication check failed: {str(dedup_error)}',
                            'records_saved': 0,
                            'table_name': table_name
                        }
                        results['mssql_success'] = False
                        results['errors'].append(f"MSSQL save aborted - deduplication check failed: {str(dedup_error)}")
                        df_clean = pd.DataFrame()  # Empty to skip save below
                
                # Only proceed with save if we have records
                if not df_clean.empty:
                    print("MSSQLdf_clean columns: ", df_clean.columns)
                    
                    # Convert float columns that should be integers based on your schema
                    integer_columns = [
                        'NumberOfItemsShipped', 'QuantityShipped', 'QuantityOrdered'
                    ]
                    
                    for col in integer_columns:
                        if col in df_clean.columns:
                            # Convert float to int, handling NaN values
                            df_clean[col] = df_clean[col].fillna(0).astype(float).astype(int)
                            logger.info(f"🔧 Converted {col} from float to int")
                    
                    # Convert float columns that should remain as float but ensure proper format
                    float_columns = [
                        'PromotionDiscountTax.Amount', 'ShippingTax.Amount', 'ShippingPrice.Amount',
                        'ShippingDiscount.Amount', 'ShippingDiscountTax.Amount', 'vat',
                        'item_subtotal', 'promotion', 'Promotional_Tax', 'unit_price(vat_inclusive)',
                        'vat%', 'calculated_vat', 'unit_price(vat_exclusive)', 'item_total', 'grand_total'
                    ]
                    
                    for col in float_columns:
                        if col in df_clean.columns:
                            # Ensure proper float format
                            df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce').fillna(0.0)
                            logger.info(f"🔧 Ensured {col} is proper float format")
                    
                    # Handle datetime columns - convert datetime objects to strings for nvarchar columns
                    datetime_string_columns = ['PurchaseDate', 'EarliestShipDate', 'LatestShipDate']
                    for col in datetime_string_columns:
                        if col in df_clean.columns:
                            # Convert datetime to string format
                            df_clean[col] = df_clean[col].astype(str)
                            logger.info(f"🔧 Converted {col} to string format")
                    
                    # Handle PurchaseDate_Materialized as proper date
                    if 'PurchaseDate_Materialized' in df_clean.columns:
                        df_clean['PurchaseDate_Materialized'] = pd.to_datetime(df_clean['PurchaseDate_Materialized'], errors='coerce')
                        logger.info("🔧 Converted PurchaseDate_Materialized to proper datetime")
                    
                    # Let pandas handle the column matching automatically with retries
                    _to_sql_with_retries(
                        df_clean,
                        engine=MSSQL_engine,
                        table_name=table_name,
                        if_exists='append',
                        index=False,
                        max_retries=3,
                        base_backoff=1.0,
                    )
                    
                    results['mssql_result'] = {
                        'success': True,
                        'records_saved': len(df_clean),
                        'records_skipped': original_count - len(df_clean),
                        'table_name': table_name
                    }
                    results['mssql_success'] = True
                    results['total_records_saved'] += len(df_clean)
                    logger.info(f"✅ MSSQL save successful: {len(df_clean)} records (skipped {original_count - len(df_clean)} duplicates)")
                    
            except Exception as mssql_error:
                logger.error(f"❌ MSSQL save failed: {mssql_error}", exc_info=True)
                results['mssql_result'] = {
                    'success': False,
                    'error': str(mssql_error),
                    'records_saved': 0,
                    'table_name': table_name
                }
                results['mssql_success'] = False
                results['errors'].append(f"MSSQL save failed: {str(mssql_error)}")
        else:
            logger.info("🔄 Skipping MSSQL save - no table mapping or empty DataFrame")
            results['mssql_success'] = False
        
        # Save Azure DataFrame with deduplication
        if not azure_df.empty:
            try:
                logger.info(f"🔄 Saving Azure data: {len(azure_df)} records to stg_tr_amazon_raw")
                logger.info(f"Azure columns: {list(azure_df.columns)}")
                
                # Clean data and fix datetime columns
                df_clean = azure_df.copy()
                
                original_count = len(df_clean)
                logger.info(f"📊 Original records to save: {original_count}")
                
                # SAFETY CHECK: Verify required columns exist before deduplication
                if 'OrderId' not in df_clean.columns or 'SKU' not in df_clean.columns:
                    logger.error("❌ CRITICAL: Required columns missing for Azure deduplication!")
                    logger.error(f"❌ Expected: OrderId, SKU")
                    logger.error(f"❌ Available: {df_clean.columns.tolist()}")
                    results['azure_result'] = {
                        'success': False,
                        'error': 'Required columns missing - cannot verify duplicates',
                        'records_saved': 0,
                        'table_name': 'stg_tr_amazon_raw'
                    }
                    results['azure_success'] = False
                    results['errors'].append("Azure save aborted - required columns missing")
                    df_clean = pd.DataFrame()  # Clear to prevent unsafe save
                
                # DEDUPLICATION: Remove duplicates based on OrderId + SKU (composite key for Azure)
                # Azure uses SKU instead of OrderItemId because data is aggregated by SKU
                if 'OrderId' in df_clean.columns and 'SKU' in df_clean.columns:
                    # First, deduplicate within the DataFrame itself
                    before_dedup = len(df_clean)
                    df_clean = df_clean.drop_duplicates(subset=['OrderId', 'SKU'], keep='first')
                    after_dedup = len(df_clean)
                    
                    if before_dedup != after_dedup:
                        logger.info(f"🔧 Removed {before_dedup - after_dedup} duplicate records within DataFrame")
                    
                    # Second, check for existing records in database
                    from sqlalchemy import text
                    try:
                        order_ids = df_clean['OrderId'].unique().tolist()
                        
                        if order_ids:
                            logger.info(f"🔍 Checking Azure database for {len(order_ids)} orders...")
                            
                            # Query database for existing combinations (OrderId + SKU)
                            placeholders = ','.join([f"'{oid}'" for oid in order_ids])
                            query = text(f"""
                                SELECT DISTINCT OrderId, SKU
                                FROM stg_tr_amazon_raw
                                WHERE OrderId IN ({placeholders})
                            """)
                            
                            engine_AZURE = create_Azure_db_connection()
                            with engine_AZURE.connect() as conn:
                                result = conn.execute(query)
                                existing_combinations = {(row[0], row[1]) for row in result}
                            
                            if existing_combinations:
                                logger.info(f"🔍 Found {len(existing_combinations)} existing order-SKU combinations in Azure database")
                                
                                # Filter out existing combinations
                                df_clean['_temp_key'] = df_clean.apply(
                                    lambda row: (row['OrderId'], row['SKU']), 
                                    axis=1
                                )
                                
                                before_filter = len(df_clean)
                                df_clean = df_clean[~df_clean['_temp_key'].isin(existing_combinations)]
                                df_clean = df_clean.drop(columns=['_temp_key'])
                                after_filter = len(df_clean)
                                
                                filtered_count = before_filter - after_filter
                                logger.info(f"🔧 Filtered out {filtered_count} duplicate records")
                                logger.info(f"✅ After database deduplication: {after_filter} new records to save")
                                
                                if after_filter == 0:
                                    logger.info(f"ℹ️  All {original_count} records already exist in Azure database - skipping save")
                                    results['azure_result'] = {
                                        'success': True,
                                        'records_saved': 0,
                                        'records_skipped': original_count,
                                        'table_name': 'stg_tr_amazon_raw',
                                        'message': 'All records already exist (duplicates skipped)'
                                    }
                                    results['azure_success'] = True
                                    # Don't continue to save, but still mark as successful
                                    df_clean = pd.DataFrame()  # Empty dataframe
                            else:
                                logger.info(f"✅ No duplicates found - all {len(df_clean)} records are new")
                                
                    except Exception as dedup_error:
                        logger.error(f"❌ CRITICAL: Azure database deduplication check failed: {dedup_error}", exc_info=True)
                        logger.error(f"⚠️  ABORTING AZURE SAVE to prevent duplicates!")
                        # DO NOT PROCEED - better to fail than insert duplicates
                        results['azure_result'] = {
                            'success': False,
                            'error': f'Deduplication check failed: {str(dedup_error)}',
                            'records_saved': 0,
                            'table_name': 'stg_tr_amazon_raw'
                        }
                        results['azure_success'] = False
                        results['errors'].append(f"Azure save aborted - deduplication check failed: {str(dedup_error)}")
                        df_clean = pd.DataFrame()  # Empty to skip save below
                
                # Only proceed with save if we have records
                if not df_clean.empty:
                    print("Azure df_clean columns: ", df_clean.columns)
                    
                    # Ensure CLEAN_DateTime is datetime64[ns] without timezone
                    if 'CLEAN_DateTime' in df_clean.columns:
                        df_clean['CLEAN_DateTime'] = pd.to_datetime(df_clean['CLEAN_DateTime'], errors='coerce', utc=False)
                        # If any tz-aware slipped in, convert to naive
                        if hasattr(df_clean['CLEAN_DateTime'].dtype, 'tz') and df_clean['CLEAN_DateTime'].dt.tz is not None:
                            df_clean['CLEAN_DateTime'] = df_clean['CLEAN_DateTime'].dt.tz_convert('UTC').dt.tz_localize(None)

                    # Derive Date as date only from CLEAN_DateTime when available, else coerce
                    if 'CLEAN_DateTime' in df_clean.columns:
                        df_clean['Date'] = pd.to_datetime(df_clean['CLEAN_DateTime'].dt.date, errors='coerce')
                    elif 'Date' in df_clean.columns:
                        df_clean['Date'] = pd.to_datetime(df_clean['Date'], errors='coerce')
                    
                    # Handle datetime columns that might have invalid formats
                    datetime_columns = ['data_fetch_Date']
                    
                    for col in datetime_columns:
                        if col in df_clean.columns:
                            logger.info(f"🔧 Fixing datetime column: {col}")
                            df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce', utc=False)
                            # Strip timezone info if present
                            try:
                                if hasattr(df_clean[col].dtype, 'tz') and df_clean[col].dt.tz is not None:
                                    df_clean[col] = df_clean[col].dt.tz_convert('UTC').dt.tz_localize(None)
                            except Exception:
                                pass
                    
                    engine_AZURE = create_Azure_db_connection()
                    logging.info(f"engine_AZURE : {engine_AZURE}")
                    logging.info(f"{marketplace_id} DATA: {df_clean.shape}")
                    
                    def my_listener_2(conn, cursor, statement, parameters, context, executemany):
                        if executemany:
                            cursor.fast_executemany = True

                    event.listen(engine_AZURE, "before_cursor_execute", my_listener_2)
                    
                    # Let pandas handle the column matching automatically with retries
                    _to_sql_with_retries(
                        df_clean,
                        engine=engine_AZURE,
                        table_name='stg_tr_amazon_raw',
                        if_exists='append',
                        index=False,
                        max_retries=3,
                        base_backoff=1.0,
                    )
                    
                    results['azure_result'] = {
                        'success': True,
                        'records_saved': len(df_clean),
                        'records_skipped': original_count - len(df_clean),
                        'table_name': 'stg_tr_amazon_raw'
                    }
                    results['azure_success'] = True
                    results['total_records_saved'] += len(df_clean)
                    logger.info(f"✅ Azure save successful: {len(df_clean)} records (skipped {original_count - len(df_clean)} duplicates)")
                    
            except Exception as azure_error:
                logger.error(f"❌ Azure save failed: {azure_error}", exc_info=True)
                results['azure_result'] = {
                    'success': False,
                    'error': str(azure_error),
                    'records_saved': 0,
                    'table_name': 'stg_tr_amazon_raw'
                }
                results['azure_success'] = False
                results['errors'].append(f"Azure save failed: {str(azure_error)}")
        else:
            logger.info("🔄 Skipping Azure save - empty DataFrame")
            results['azure_success'] = False
        
        # Determine overall success based on individual database results
        # Success if at least one database save succeeded
        results['success'] = results['mssql_success'] or results['azure_success']
        
        # Create user-friendly summary message
        mssql_saved = results.get('mssql_result', {}).get('records_saved', 0)
        mssql_skipped = results.get('mssql_result', {}).get('records_skipped', 0)
        azure_saved = results.get('azure_result', {}).get('records_saved', 0)
        azure_skipped = results.get('azure_result', {}).get('records_skipped', 0)
        
        # Build detailed message
        message_parts = []
        if results['mssql_success'] and results['azure_success']:
            status = "success"
            message_parts.append(f"✓ Saved {results['total_records_saved']} records to both databases")
            if mssql_skipped > 0 or azure_skipped > 0:
                message_parts.append(f"(MSSQL: {mssql_saved} new, {mssql_skipped} duplicates | Azure: {azure_saved} new, {azure_skipped} duplicates)")
        elif results['mssql_success']:
            status = "partial_success"
            message_parts.append(f"✓ Saved {mssql_saved} records to MSSQL")
            if mssql_skipped > 0:
                message_parts.append(f"({mssql_skipped} duplicates skipped)")
            message_parts.append("⚠ Azure save failed")
        elif results['azure_success']:
            status = "partial_success"
            message_parts.append(f"✓ Saved {azure_saved} records to Azure")
            if azure_skipped > 0:
                message_parts.append(f"({azure_skipped} duplicates skipped)")
            message_parts.append("⚠ MSSQL save failed")
        else:
            status = "error"
            message_parts.append("✗ Failed to save to both databases")
        
        results['status'] = status
        results['message'] = " ".join(message_parts)
        results['details'] = {
            'mssql': {
                'saved': mssql_saved,
                'skipped': mssql_skipped,
                'success': results['mssql_success']
            },
            'azure': {
                'saved': azure_saved,
                'skipped': azure_skipped,
                'success': results['azure_success']
            }
        }
        
        if results['success']:
            logger.info(f"✅ Simple save completed: {results['total_records_saved']} total records")
            logger.info(f"📊 {results['message']}")
            if results['mssql_success'] and results['azure_success']:
                logger.info("✅ Both MSSQL and Azure saves succeeded")
            elif results['mssql_success']:
                logger.info("✅ MSSQL save succeeded, Azure save failed")
            elif results['azure_success']:
                logger.info("✅ Azure save succeeded, MSSQL save failed")
        else:
            logger.error("❌ Both MSSQL and Azure saves failed")
            logger.error(f"📊 {results['message']}")
            
        return results
        
    except Exception as e:
        logger.error(f"❌ Simple save failed: {e}", exc_info=True)
        return {
            'success': False,
            'error': str(e),
            'total_records_saved': 0,
            'errors': [str(e)],
            'mssql_success': False,
            'azure_success': False
        } 