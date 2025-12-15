from datetime import timedelta, datetime
from .simple_db_save import create_mssql_connection, create_Azure_db_connection
from django.http import JsonResponse, HttpResponse
import logging
from django.views import View
from sqlalchemy import text
from django.views.decorators.csrf import csrf_exempt
from django.utils.decorators import method_decorator
from .models import MarketplaceLastRun


# Enhanced logging configuration
logger = logging.getLogger(__name__)

# Configure logging with more detailed formatting
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    
@method_decorator(csrf_exempt, name='dispatch')
class FixPurchaseDate(View):
    """
    View to fix PurchaseDate_conversion anomalies in the database.
    
    Logic:
    1. For each marketplace, query rows where PurchaseDate_conversion > PurchaseDate
    2. If anomalous rows are found, delete them
    3. Only if deletion is successful, get MAX(PurchaseDate) from remaining data
    4. Update marketplace_last_run to (MAX(PurchaseDate) - 1 day)
    """
    def post(self, request):
        summary = {
            'total_marketplaces_processed': 0,
            'total_rows_deleted': 0,
            'marketplaces_fixed': [],
            'marketplaces_with_errors': [],
            'marketplaces_no_anomalies': []
        }
        
        try:
            logger.info(f"\n{'='*80}")
            logger.info("🚨 FixPurchaseDate - STARTING FIX OPERATION")
            logger.info(f"{'='*80}\n")
            
            marketplaces = [
                # {'name': 'usa', 'id': 'ATVPDKIKX0DER', 'region': 'US'},
                # {'name': 'ca', 'id': 'A2EUQ1WTGCTBG2', 'region': 'CA'},
                {'name': 'uk', 'id': 'A1F83G8C2ARO7P', 'region': 'UK'},
                {'name': 'de', 'id': 'A1PA6795UKMFR9', 'region': 'DE'},
                {'name': 'it', 'id': 'APJ6JRA9NG5V4', 'region': 'IT'},
                {'name': 'es', 'id': 'A1RKKUPIHCS9HS', 'region': 'ES'}
            ]
            
            # Create database connections once for all operations
            try:
                mssql_engine = create_mssql_connection()
                logger.info("✅ Successfully created MSSQL database connection")
            except Exception as e:
                error_msg = f"❌ Failed to create MSSQL database connection: {str(e)}"
                logger.error(error_msg)
                return JsonResponse({
                    "error": "MSSQL database connection failed",
                    "details": str(e)
                }, status=500)
            
            try:
                azure_engine = create_Azure_db_connection()
                logger.info("✅ Successfully created Azure database connection")
            except Exception as e:
                error_msg = f"❌ Failed to create Azure database connection: {str(e)}"
                logger.error(error_msg)
                return JsonResponse({
                    "error": "Azure database connection failed",
                    "details": str(e)
                }, status=500)
            
            for marketplace in marketplaces:
                mkt_name = marketplace['name']
                mkt_id = marketplace['id']
                mkt_region = marketplace['region']
                summary['total_marketplaces_processed'] += 1
                
                logger.info(f"\n{'─'*80}")
                logger.info(f"🔍 Processing marketplace: {mkt_name.upper()} ({mkt_id}) - Region: {mkt_region}")
                logger.info(f"{'─'*80}")
                
                try:
                    # ============================================================
                    # MSSQL DATABASE - Step 1: Get MAX(PurchaseDate)
                    # ============================================================
                    logger.info(f"📊 Step 1 (MSSQL): Getting MAX(PurchaseDate) for {mkt_name}...")
                    
                    max_purchase_query = text(f"""
                        SELECT MAX(CAST(PurchaseDate AS DATE)) AS max_date
                        FROM amazon_api_{mkt_name}
                    """)
                    
                    with mssql_engine.connect() as conn:
                        result = conn.execute(max_purchase_query)
                        max_purchase_date_mssql = result.fetchone()[0]
                    
                    if not max_purchase_date_mssql:
                        error_msg = f"❌ MSSQL: Could not determine MAX(PurchaseDate) for {mkt_name} - Table may be empty"
                        logger.error(error_msg)
                        summary['marketplaces_with_errors'].append({
                            'marketplace': mkt_name,
                            'error': 'MSSQL: Could not get MAX(PurchaseDate)'
                        })
                        continue
                    
                    logger.info(f"📊 MSSQL MAX(PurchaseDate): {max_purchase_date_mssql}")
                    
                    # ============================================================
                    # AZURE DATABASE - Step 1: Get MAX(data_fetch_Date)
                    # ============================================================
                    logger.info(f"📊 Step 1 (Azure): Getting MAX(data_fetch_Date) for {mkt_name}...")
                    
                    max_fetch_date_query = text(f"""
                        SELECT MAX(CAST(data_fetch_Date AS DATE)) AS max_date
                        FROM stg_tr_amazon_raw
                        WHERE Region = :region
                    """)
                    
                    try:
                        with azure_engine.connect() as conn:
                            result = conn.execute(max_fetch_date_query, {"region": mkt_region})
                            max_fetch_date_azure = result.fetchone()[0]
                        
                        if not max_fetch_date_azure:
                            error_msg = f"❌ Azure: Could not determine MAX(data_fetch_Date) for {mkt_name} - Table may be empty or no data for region {mkt_region}"
                            logger.error(error_msg)
                            summary['marketplaces_with_errors'].append({
                                'marketplace': mkt_name,
                                'error': 'Azure: Could not get MAX(data_fetch_Date)'
                            })
                            continue
                        
                        logger.info(f"📊 Azure MAX(data_fetch_Date): {max_fetch_date_azure}")
                        
                    except Exception as azure_error:
                        error_msg = f"❌ Azure: Failed to query MAX(data_fetch_Date) for {mkt_name}: {str(azure_error)}"
                        logger.error(error_msg)
                        summary['marketplaces_with_errors'].append({
                            'marketplace': mkt_name,
                            'error': f'Azure query failed: {str(azure_error)}'
                        })
                        continue
                    
                    # ============================================================
                    # MSSQL DATABASE - Step 2: Check for anomalous rows
                    # ============================================================
                    logger.info(f"🔍 Step 2 (MSSQL): Checking for anomalous rows in {mkt_name}...")
                    
                    anomaly_query_mssql = text(f"""
                        SELECT COUNT(*) as count
                        FROM amazon_api_{mkt_name}
                        WHERE CAST(PurchaseDate_conversion AS DATE) > CAST(:max_date AS DATE)
                    """)
                    
                    with mssql_engine.connect() as conn:
                        result = conn.execute(anomaly_query_mssql, {"max_date": max_purchase_date_mssql})
                        row_count_mssql = result.fetchone()[0]
                    
                    logger.info(f"📊 MSSQL: Found {row_count_mssql} anomalous rows")
                    
                    # Get sample of anomalous rows from MSSQL
                    if row_count_mssql > 0:
                        sample_query_mssql = text(f"""
                            SELECT TOP 3 
                                AmazonOrderId, 
                                PurchaseDate, 
                                PurchaseDate_conversion,
                                DATEDIFF(day, :max_date, PurchaseDate_conversion) as DaysDifference
                            FROM amazon_api_{mkt_name}
                            WHERE CAST(PurchaseDate_conversion AS DATE) > CAST(:max_date AS DATE)
                        """)
                        
                        with mssql_engine.connect() as conn:
                            result = conn.execute(sample_query_mssql, {"max_date": max_purchase_date_mssql})
                            sample_rows_mssql = result.fetchall()
                        
                        logger.info(f"📝 MSSQL Sample of anomalous rows:")
                        for idx, row in enumerate(sample_rows_mssql, 1):
                            log_msg = f"   {idx}. OrderID: {row[0]}, PurchaseDate: {row[1]}, PurchaseDate_conversion: {row[2]}, Diff from MAX: {row[3]} days"
                            logger.info(log_msg)
                    
                    # ============================================================
                    # AZURE DATABASE - Step 2: Check for anomalous rows
                    # ============================================================
                    logger.info(f"🔍 Step 2 (Azure): Checking for anomalous rows in {mkt_name}...")
                    
                    anomaly_query_azure = text(f"""
                        SELECT COUNT(*) as count
                        FROM stg_tr_amazon_raw
                        WHERE Region = :region 
                        AND CAST(CLEAN_DateTime AS DATE) > CAST(:max_date AS DATE)
                    """)
                    
                    try:
                        with azure_engine.connect() as conn:
                            result = conn.execute(anomaly_query_azure, {
                                "region": mkt_region, 
                                "max_date": max_fetch_date_azure
                            })
                            row_count_azure = result.fetchone()[0]
                        
                        logger.info(f"📊 Azure: Found {row_count_azure} anomalous rows")
                        
                        # Get sample of anomalous rows from Azure
                        if row_count_azure > 0:
                            sample_query_azure = text(f"""
                                SELECT TOP 3 
                                    OrderId, 
                                    data_fetch_Date, 
                                    CLEAN_DateTime,
                                    DATEDIFF(day, :max_date, CLEAN_DateTime) as DaysDifference
                                FROM stg_tr_amazon_raw
                                WHERE Region = :region 
                                AND CAST(CLEAN_DateTime AS DATE) > CAST(:max_date AS DATE)
                            """)
                            
                            with azure_engine.connect() as conn:
                                result = conn.execute(sample_query_azure, {
                                    "region": mkt_region,
                                    "max_date": max_fetch_date_azure
                                })
                                sample_rows_azure = result.fetchall()
                            
                            logger.info(f"📝 Azure Sample of anomalous rows:")
                            for idx, row in enumerate(sample_rows_azure, 1):
                                log_msg = f"   {idx}. OrderID: {row[0]}, data_fetch_Date: {row[1]}, CLEAN_DateTime: {row[2]}, Diff from MAX: {row[3]} days"
                                logger.info(log_msg)
                        
                    except Exception as azure_error:
                        error_msg = f"❌ Azure: Failed to query anomalous rows for {mkt_name}: {str(azure_error)}"
                        logger.error(error_msg)
                        summary['marketplaces_with_errors'].append({
                            'marketplace': mkt_name,
                            'error': f'Azure anomaly query failed: {str(azure_error)}'
                        })
                        continue
                    
                    # Check if both databases have no anomalies
                    if row_count_mssql == 0 and row_count_azure == 0:
                        logger.info(f"✅ No anomalies found in both MSSQL and Azure for {mkt_name}")
                        summary['marketplaces_no_anomalies'].append(mkt_name)
                        continue
                    
                    logger.warning(f"⚠️ Total anomalous rows - MSSQL: {row_count_mssql}, Azure: {row_count_azure}")
                    
                    # ============================================================
                    # DELETE OPERATIONS - Both databases must succeed
                    # ============================================================
                    
                    deleted_rows_mssql = 0
                    deleted_rows_azure = 0
                    
                    # Step 3a: Delete anomalous rows from MSSQL
                    if row_count_mssql > 0:
                        logger.info(f"🗑️ Step 3a (MSSQL): Deleting {row_count_mssql} anomalous rows from {mkt_name}...")
                        
                        delete_query_mssql = text(f"""
                            DELETE FROM amazon_api_{mkt_name}
                            WHERE CAST(PurchaseDate_conversion AS DATE) > CAST(:max_date AS DATE)
                        """)
                        
                        try:
                            with mssql_engine.begin() as conn:
                                result = conn.execute(delete_query_mssql, {"max_date": max_purchase_date_mssql})
                                deleted_rows_mssql = result.rowcount
                            
                            logger.info(f"✅ MSSQL: Successfully deleted {deleted_rows_mssql} anomalous rows for {mkt_name}")
                            
                        except Exception as delete_error:
                            error_msg = f"❌ MSSQL: Failed to delete anomalous rows for {mkt_name}: {str(delete_error)}"
                            logger.error(error_msg)
                            summary['marketplaces_with_errors'].append({
                                'marketplace': mkt_name,
                                'error': f"MSSQL deletion failed: {str(delete_error)}"
                            })
                            continue  # Skip to next marketplace if MSSQL deletion fails
                    
                    # Step 3b: Delete anomalous rows from Azure (only if MSSQL succeeded or had no rows)
                    if row_count_azure > 0:
                        logger.info(f"🗑️ Step 3b (Azure): Deleting {row_count_azure} anomalous rows from {mkt_name}...")
                        
                        delete_query_azure = text(f"""
                            DELETE FROM stg_tr_amazon_raw
                            WHERE Region = :region 
                            AND CAST(CLEAN_DateTime AS DATE) > CAST(:max_date AS DATE)
                        """)
                        
                        try:
                            with azure_engine.begin() as conn:
                                result = conn.execute(delete_query_azure, {
                                    "region": mkt_region,
                                    "max_date": max_fetch_date_azure
                                })
                                deleted_rows_azure = result.rowcount
                            
                            logger.info(f"✅ Azure: Successfully deleted {deleted_rows_azure} anomalous rows for {mkt_name}")
                            
                        except Exception as delete_error:
                            error_msg = f"❌ Azure: Failed to delete anomalous rows for {mkt_name}: {str(delete_error)}"
                            logger.error(error_msg)
                            summary['marketplaces_with_errors'].append({
                                'marketplace': mkt_name,
                                'error': f"Azure deletion failed: {str(delete_error)}"
                            })
                            continue  # Skip to next marketplace if Azure deletion fails
                    
                    total_deleted = deleted_rows_mssql + deleted_rows_azure
                    summary['total_rows_deleted'] += total_deleted
                    logger.info(f"✅ Total deleted rows: {total_deleted} (MSSQL: {deleted_rows_mssql}, Azure: {deleted_rows_azure})")
                    
                    # ============================================================
                    # UPDATE MARKETPLACE LAST RUN
                    # ============================================================
                    # Step 4: Update marketplace_last_run (deletion was successful, use the max_purchase_date_mssql)
                    logger.info(f"🔄 Step 4: Updating marketplace_last_run for {mkt_name}...")
                    
                    # Calculate last_run as MAX(PurchaseDate) - 1 day
                    # Format: 2023-10-31T23:59:59Z
                    last_run_date_base = max_purchase_date_mssql - timedelta(days=1)
                    last_run_date = datetime.combine(last_run_date_base, datetime.max.time()).replace(microsecond=0).strftime('%Y-%m-%dT%H:%M:%SZ')
                    
                    try:
                        # Update the marketplace_last_run table
                        updated_count = MarketplaceLastRun.objects.filter(
                            marketplace_id=mkt_id
                        ).update(last_run=last_run_date)
                        
                        if updated_count == 0:
                            error_msg = f"⚠️ No marketplace_last_run entry found for {mkt_name} (ID: {mkt_id}) - Skipping update"
                            logger.warning(error_msg)
                            summary['marketplaces_with_errors'].append({
                                'marketplace': mkt_name,
                                'error': f"No marketplace_last_run entry found (ID: {mkt_id})"
                            })
                            continue
                        
                        logger.info(f"✅ Updated last_run to {last_run_date} for {mkt_name}")
                        
                        summary['marketplaces_fixed'].append({
                            'marketplace': mkt_name,
                            'rows_deleted_mssql': deleted_rows_mssql,
                            'rows_deleted_azure': deleted_rows_azure,
                            'total_rows_deleted': total_deleted,
                            'max_purchase_date': max_purchase_date_mssql.strftime('%Y-%m-%d'),
                            'max_fetch_date_azure': max_fetch_date_azure.strftime('%Y-%m-%d'),
                            'new_last_run': last_run_date
                        })
                        
                    except Exception as update_error:
                        error_msg = f"❌ Error updating marketplace_last_run for {mkt_name}: {str(update_error)}"
                        logger.error(error_msg)
                        summary['marketplaces_with_errors'].append({
                            'marketplace': mkt_name,
                            'error': f"Update failed: {str(update_error)}"
                        })
                        continue
                    
                except Exception as mkt_error:
                    error_msg = f"❌ Unexpected error processing {mkt_name}: {str(mkt_error)}"
                    logger.error(error_msg, exc_info=True)
                    summary['marketplaces_with_errors'].append({
                        'marketplace': mkt_name,
                        'error': str(mkt_error)
                    })
                    continue
            
            # Final Summary
            logger.info(f"\n{'='*80}")
            logger.info("🎯 FIX OPERATION COMPLETED - SUMMARY")
            logger.info(f"{'='*80}")
            logger.info(f"📊 Total marketplaces processed: {summary['total_marketplaces_processed']}")
            logger.info(f"🗑️ Total rows deleted: {summary['total_rows_deleted']}")
            logger.info(f"✅ Marketplaces successfully fixed: {len(summary['marketplaces_fixed'])}")
            logger.info(f"⚠️ Marketplaces with errors: {len(summary['marketplaces_with_errors'])}")
            logger.info(f"✔️ Marketplaces with no anomalies: {len(summary['marketplaces_no_anomalies'])}")
            
            if summary['marketplaces_fixed']:
                logger.info("\n🎉 Successfully Fixed Marketplaces:")
                for fix in summary['marketplaces_fixed']:
                    log_msg = f"   • {fix['marketplace']}: MSSQL deleted {fix['rows_deleted_mssql']} rows, Azure deleted {fix['rows_deleted_azure']} rows, Last run set to {fix['new_last_run']}"
                    logger.info(log_msg)
            
            if summary['marketplaces_with_errors']:
                logger.warning("\n⚠️ Marketplaces with Errors:")
                for err in summary['marketplaces_with_errors']:
                    log_msg = f"   • {err['marketplace']}: {err['error']}"
                    logger.warning(log_msg)
            
            if summary['marketplaces_no_anomalies']:
                logger.info(f"\n✔️ Marketplaces with no anomalies: {', '.join(summary['marketplaces_no_anomalies'])}")
            
            logger.info(f"{'='*80}\n")
            
            return JsonResponse({
                "success": True,
                "message": "Fix operation completed",
                "data": summary
            }, status=200)
        
        except Exception as e:
            error_msg = f"❌ Critical error during fix operation: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return JsonResponse({
                "success": False,
                "error": "Critical error occurred",
                "details": str(e),
                "data": summary
            }, status=500)