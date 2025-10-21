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
    """
    
    # Marketplace to table mapping
    MARKETPLACE_TABLE_MAPPING = {
        'A1PA6795UKMFR9': 'amazon_api_de_test',  # Germany
        'A1RKKUPIHCS9HS': 'amazon_api_es_test',  # Spain
        'APJ6JRA9NG5V4': 'amazon_api_it_test',   # Italy
        'A1F83G8C2ARO7P': 'amazon_api_uk_test',  # United Kingdom
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
        # # Get engines
        # secondary_conn = connections['secondary']
        # tertiary_conn = connections['tertiary']
        
        # secondary_settings = secondary_conn.settings_dict
        # tertiary_settings = tertiary_conn.settings_dict
        
        # # Create simple connection strings with proper port handling
        # secondary_port = secondary_settings.get('PORT') or 1433
        # if isinstance(secondary_port, str) and secondary_port.strip() == '':
        #     secondary_port = 1433
        
        # tertiary_port = tertiary_settings.get('PORT') or 1433
        # if isinstance(tertiary_port, str) and tertiary_port.strip() == '':
        #     tertiary_port = 1433
        
        # logger.info(f"🔗 Connecting to secondary DB: {secondary_settings['HOST']}:{secondary_port}")
        # secondary_engine = create_engine(
        #     f"mssql+pyodbc://{secondary_settings['USER']}:{secondary_settings['PASSWORD']}"
        #     f"@{secondary_settings['HOST']}:{secondary_port}"
        #     f"/{secondary_settings['NAME']}?driver=ODBC+Driver+17+for+SQL+Server",
        #     echo=False
        # )
        
        # logger.info(f"🔗 Connecting to tertiary DB: {tertiary_settings['HOST']}:{tertiary_port}")
        # tertiary_engine = create_engine(
        #     f"mssql+pyodbc://{tertiary_settings['USER']}:{tertiary_settings['PASSWORD']}"
        #     f"@{tertiary_settings['HOST']}:{tertiary_port}"
        #     f"/{tertiary_settings['NAME']}?driver=ODBC+Driver+17+for+SQL+Server",
        #     echo=False
        # )
        MSSQL_engine = create_mssql_connection()
        logging.info(f"MSSQL_engine : {MSSQL_engine}")
    
        def my_listener(conn, cursor, statement, parameters, context, executemany):
            if executemany:
                cursor.fast_executemany = True

        event.listen(MSSQL_engine, "before_cursor_execute", my_listener)
        # Save MSSQL DataFrame
        table_name = MARKETPLACE_TABLE_MAPPING.get(marketplace_id)
        if table_name and not mssql_df.empty:
            try:
                logger.info(f"🔄 Saving MSSQL data: {len(mssql_df)} records to {table_name}")
                logger.info(f"MSSQL columns: {list(mssql_df.columns)}")
                
                # Clean data
                df_clean = mssql_df.copy()
                df_clean['PurchaseDate_conversion'] = pd.to_datetime(df_clean['PurchaseDate_conversion']).dt.strftime('%Y-%m-%d %H:%M:%S')
                # df_clean = mssql_df.copy().fillna('')
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
                    'table_name': table_name
                }
                results['mssql_success'] = True
                results['total_records_saved'] += len(df_clean)
                logger.info(f"✅ MSSQL save successful: {len(df_clean)} records")
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
        
        # Save Azure DataFrame
        if not azure_df.empty:
            try:
                logger.info(f"🔄 Saving Azure data: {len(azure_df)} records to stg_tr_amazon_raw")
                logger.info(f"Azure columns: {list(azure_df.columns)}")
                
                # Clean data and fix datetime columns
                df_clean = azure_df.copy()
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
                        
                
                # Fill remaining NaN values
                # df_clean = df_clean.fillna('')
                
                engine_AZURE = create_Azure_db_connection()
                logging.info(f"engine_AZURE : {engine_AZURE}")
                logging.info(f"{marketplace_id} DATA: {df_clean.shape}")  # -->TABLE
                
                def my_listener_2(conn, cursor, statement, parameters, context, executemany):
                    if executemany:
                        cursor.fast_executemany = True

                event.listen(engine_AZURE, "before_cursor_execute", my_listener_2)

                #merged_df2.to_sql(f"amazon_api_{marketplace_name.lower()}", MSSQL_engine, index=False, if_exists="append")  # append # replace
                # df_clean.to_sql("stg_tr_amazon_raw", engine_AZURE, index=False, if_exists="append")#append # replace
                
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
                    'table_name': 'stg_tr_amazon_raw'
                }
                results['azure_success'] = True
                results['total_records_saved'] += len(df_clean)
                logger.info(f"✅ Azure save successful: {len(df_clean)} records")
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
        
        if results['success']:
            logger.info(f"✅ Simple save completed: {results['total_records_saved']} total records")
            if results['mssql_success'] and results['azure_success']:
                logger.info("✅ Both MSSQL and Azure saves succeeded")
            elif results['mssql_success']:
                logger.info("✅ MSSQL save succeeded, Azure save failed")
            elif results['azure_success']:
                logger.info("✅ Azure save succeeded, MSSQL save failed")
        else:
            logger.error("❌ Both MSSQL and Azure saves failed")
            
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