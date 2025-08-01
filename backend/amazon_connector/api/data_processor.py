"""
Optimized Amazon Data Processor

This module provides high-performance data processing for Amazon orders and order items.
It includes timezone conversion, VAT calculations, and data transformations optimized
for speed and memory efficiency.
"""

import pandas as pd
import numpy as np
import logging
import re
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import warnings
import os

# Suppress pandas warnings for cleaner output
warnings.filterwarnings('ignore', category=pd.errors.PerformanceWarning)

logger = logging.getLogger(__name__)

class AmazonDataProcessor:
    """
    High-performance Amazon data processor with optimized operations.
    """
    
    # VAT rates by marketplace
    VAT_RATES = {
        'Amazon.es': {'rate': 0.21, 'multiplier': 1.21, 'percentage': 21/121},
        'Amazon.de': {'rate': 0.19, 'multiplier': 1.19, 'percentage': 19/119},
        'Amazon.it': {'rate': 0.22, 'multiplier': 1.22, 'percentage': 22/122},
        'Amazon.co.uk': {'rate': 0.20, 'multiplier': 1.20, 'percentage': 20/120}
    }
    
    # Marketplace to region mapping
    MARKETPLACE_REGIONS = {
        'Amazon.co.uk': {'region': 'UK', 'country': 'United Kingdom', 'company': 'B2Fitinss'},
        'Amazon.es': {'region': 'ES', 'country': 'Spain', 'company': 'B2fitness LTD'},
        'Amazon.de': {'region': 'DE', 'country': 'Germany', 'company': 'B2Fitinss'},
        'Amazon.it': {'region': 'IT', 'country': 'Italy', 'company': 'B2Fitinss'}
    }
    
    def __init__(self):
        """Initialize the processor with optimized pandas settings."""
        # Optimize pandas for performance
        pd.set_option('mode.copy_on_write', True)
        logger.info("Amazon Data Processor initialized")

    def last_sunday_of_march(self, year):
        # Get the last day of March for the given year
        march_last = datetime(year, 3, 31)
        # Calculate the offset to the last Sunday
        offset = (march_last.weekday() + 1) % 7
        # Subtract the offset to get the last Sunday
        march_last_sunday = march_last - timedelta(days=offset)
        # Set the time to 01:00:00
        march_last_sunday = march_last_sunday.replace(hour=1, minute=0, second=0)
        return march_last_sunday

    def last_sunday_of_october(self, year):
        # Get the last day of October for the given year
        october_31st = datetime(year, 10, 31)
        # Calculate the offset to the last Sunday
        offset = (october_31st.weekday() + 1) % 7
        # Subtract the offset to get the last Sunday
        last_sunday = october_31st - timedelta(days=offset)
        # Set the time to 01:00:00
        last_sunday = last_sunday.replace(hour=1, minute=0, second=0)
        return last_sunday

    # Function to determine if a given date falls within DST period
    def is_dst(self, date):
        year = date.year
        dst_start = self.last_sunday_of_march(year)
        dst_end = self.last_sunday_of_october(year)
        return dst_start <= date < dst_end

    def convert_utc_to_mest(self, utc_timestamp):
        """Convert UTC timestamp to MEST/CET with error handling and flexible input formats."""
        try:
            # Handle different input formats
            if pd.isna(utc_timestamp) or utc_timestamp == '' or utc_timestamp is None:
                return None
            
            # Convert to string and clean
            timestamp_str = str(utc_timestamp).strip()
            
            # Try different formats Amazon API might return
            formats_to_try = [
                '%Y-%m-%dT%H:%M:%SZ',        # Standard format
                '%Y-%m-%dT%H:%M:%S.%fZ',     # With milliseconds
                '%Y-%m-%d %H:%M:%S',         # Space separated
                '%Y-%m-%dT%H:%M:%S',         # Without Z
            ]
            
            dt_utc = None
            for fmt in formats_to_try:
                try:
                    dt_utc = datetime.strptime(timestamp_str, fmt)
                    break
                except ValueError:
                    continue
            
            if dt_utc is None:
                # If all formats fail, try pandas to_datetime as fallback
                dt_utc = pd.to_datetime(timestamp_str, errors='coerce')
                if pd.isna(dt_utc):
                    logger.warning(f"Could not parse timestamp: {utc_timestamp}")
                    return None
                dt_utc = dt_utc.to_pydatetime()
            
            # Determine the offset based on DST
            if self.is_dst(dt_utc):
                dt_mest = dt_utc + timedelta(hours=2)  # MEST (UTC+2)
            else:
                dt_mest = dt_utc + timedelta(hours=1)  # CET (UTC+1)
            
            return dt_mest
            
        except Exception as e:
            logger.error(f"Error converting timestamp {utc_timestamp}: {str(e)}")
            return None

    def convert_utc_to_bst(self, utc_timestamp):
        """Convert UTC timestamp to BST/GMT with error handling and flexible input formats."""
        try:
            # Handle different input formats
            if pd.isna(utc_timestamp) or utc_timestamp == '' or utc_timestamp is None:
                return None
            
            # Convert to string and clean
            timestamp_str = str(utc_timestamp).strip()
            
            # Try different formats Amazon API might return
            formats_to_try = [
                '%Y-%m-%dT%H:%M:%SZ',        # Standard format
                '%Y-%m-%dT%H:%M:%S.%fZ',     # With milliseconds
                '%Y-%m-%d %H:%M:%S',         # Space separated
                '%Y-%m-%dT%H:%M:%S',         # Without Z
            ]
            
            dt_utc = None
            for fmt in formats_to_try:
                try:
                    dt_utc = datetime.strptime(timestamp_str, fmt)
                    break
                except ValueError:
                    continue
            
            if dt_utc is None:
                # If all formats fail, try pandas to_datetime as fallback
                dt_utc = pd.to_datetime(timestamp_str, errors='coerce')
                if pd.isna(dt_utc):
                    logger.warning(f"Could not parse timestamp: {utc_timestamp}")
                    return None
                dt_utc = dt_utc.to_pydatetime()
            
            # Determine the offset based on DST
            if self.is_dst(dt_utc):
                dt_bst = dt_utc + timedelta(hours=1)  # BST (UTC+1)
            else:
                dt_bst = dt_utc  # GMT (UTC+0)
            
            return dt_bst
            
        except Exception as e:
            logger.error(f"Error converting timestamp {utc_timestamp}: {str(e)}")
            return None
    
    def _convert_timezone_optimized(self, utc_series: pd.Series, marketplace_name: str) -> pd.Series:
        """
        Optimized timezone conversion that uses vectorized operations when possible,
        falls back to apply for complex cases.
        
        Args:
            utc_series: Series of UTC timestamps
            marketplace_name: Marketplace name (e.g., 'UK', 'ES', 'DE', 'IT')
            
        Returns:
            Series of converted timestamps
        """
        # try:
        #     # Try vectorized approach first (fastest)
        #     marketplace_channel = f"Amazon.{marketplace_name.lower()}" if marketplace_name != 'UK' else 'Amazon.co.uk'
        #     vectorized_result = self._convert_timezone_vectorized(utc_series, marketplace_channel)
            
        #     # Check if vectorized conversion worked (no NaT values where input wasn't NaT)
        #     input_na_mask = pd.isna(utc_series)
        #     output_na_mask = pd.isna(vectorized_result)
        #     unexpected_na = output_na_mask & ~input_na_mask
            
        #     if not unexpected_na.any():
        #         logger.info("Using vectorized timezone conversion (optimal performance)")
        #         return vectorized_result
        #     else:
        #         logger.warning(f"Vectorized conversion failed for {unexpected_na.sum()} records, falling back to apply method")
                
        # except Exception as e:
        #     logger.warning(f"Vectorized timezone conversion failed: {str(e)}, falling back to apply method")
        
        # Fallback to apply method with improved functions
        logger.info("Using apply method for timezone conversion (slower but more robust)")
        if marketplace_name == "UK":
            return utc_series.apply(self.convert_utc_to_bst)
        else:
            return utc_series.apply(self.convert_utc_to_mest)
    
    def _prepare_dataframes(self, orders_data: List[Dict], order_items_data: List[Dict]) -> pd.DataFrame:
        """
        Efficiently prepare and merge orders and items data.
        
        Args:
            orders_data: List of order dictionaries
            order_items_data: List of order item dictionaries
            
        Returns:
            Merged DataFrame
        """
        logger.info(f"Processing {len(orders_data)} orders and {len(order_items_data)} items")
        
        # Convert to DataFrames with optimized dtypes
        orders_df = pd.json_normalize(orders_data)
        items_df = pd.DataFrame(order_items_data)
        orders_df.to_csv("orders_df.csv")
        print("orders_df.columns", orders_df.columns)
        print("items_df.columns: ", items_df.columns)
        
        # Ensure AmazonOrderId exists in both DataFrames
        if 'AmazonOrderId' not in orders_df.columns:
            raise ValueError("AmazonOrderId not found in orders data")
        if 'order_id' not in items_df.columns:
            raise ValueError("order_id not found in items data")
        
        # Rename for consistency
        items_df = items_df.rename(columns={'order_id': 'AmazonOrderId'})
        items_df.to_csv("items_df.csv")

        
        # Merge with outer join to preserve all data
        merged_df = pd.merge(orders_df, items_df, on="AmazonOrderId", how="outer")
        merged_df.to_csv("merged_df.csv")
        
        logger.info(f"Merged DataFrame shape: {merged_df.shape}")
        return merged_df
    
    def _split_pricing_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Detect and split pricing columns that contain both amount and currency.
        
        Amazon API sometimes returns pricing data as "12.01 GBP" instead of separate
        Amount and CurrencyCode fields. This function detects such columns and splits them.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with pricing columns split into Amount and CurrencyCode
        """
        # Common pricing field names that might need splitting
        pricing_fields = [
            'ItemPrice', 'ShippingPrice', 'ItemTax', 'ShippingTax', 
            'ShippingDiscount', 'ShippingDiscountTax', 'PromotionDiscount', 
            'PromotionDiscountTax', 'CODFee', 'CODFeeDiscount', 'OrderTotal'
        ]
        
        for field in pricing_fields:
            # Check if the base field exists (e.g., 'PromotionDiscount')
            if field in df.columns:
                # Check if Amount and CurrencyCode columns already exist
                amount_col = f"{field}.Amount"
                currency_col = f"{field}.CurrencyCode"
                
                if amount_col not in df.columns or currency_col not in df.columns:
                    logger.info(f"Splitting pricing field: {field}")
                    
                    # Split the pricing field
                    amount_values = []
                    currency_values = []
                    
                    for value in df[field]:
                        if pd.isna(value) or value == '' or value is None:
                            amount_values.append(0.0)
                            currency_values.append('USD')
                        else:
                            # Convert to string and split
                            value_str = str(value).strip()
                            
                            # Try to extract amount and currency
                            # Pattern: "12.01 GBP" or "12.01GBP" or just "12.01"
                            
                            # Match patterns like "12.01 GBP", "12.01GBP", "12.01 USD", "0.00 EUR", etc.
                            # Also handle negative values like "-5.00 GBP"
                            match = re.match(r'^(-?[0-9]+\.?[0-9]*)\s*([A-Z]{3})?$', value_str)
                            
                            if match:
                                amount = match.group(1)
                                currency = match.group(2)  # Default currency
                                try:
                                    amount_values.append(float(amount) if amount else 0.0)
                                except ValueError:
                                    amount_values.append(0.0)
                                currency_values.append(currency)
                            else:
                                # If pattern doesn't match, try to extract just numbers (including negative)
                                numbers = re.findall(r'-?[0-9]+\.?[0-9]*', value_str)
                                if numbers:
                                    try:
                                        amount_values.append(float(numbers[0]))
                                    except ValueError:
                                        amount_values.append(0.0)
                                    # Try to extract currency code (3 uppercase letters)
                                    currency_match = re.search(r'[A-Z]{3}', value_str)
                                    currency_values.append(currency_match.group() if currency_match else '')
                                else:
                                    amount_values.append(0.0)
                                    currency_values.append('USD')
                    
                    # Add the new columns
                    df[amount_col] = amount_values
                    df[currency_col] = currency_values
                    
                    logger.info(f"Created columns: {amount_col} and {currency_col}")
        
        return df
    
    def _add_missing_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add missing columns with default values.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with missing columns added
        """
        required_columns = [
            'ShippingAddress.County', 'ShippingTax.CurrencyCode', 'ShippingPrice.CurrencyCode',
            'ShippingDiscount.CurrencyCode', 'ShippingDiscountTax.CurrencyCode',
            'ShippingTax.Amount', 'ShippingPrice.Amount', 'ShippingDiscount.Amount',
            'ShippingDiscountTax.Amount'
        ]
        
        for col in required_columns:
            if col not in df.columns:
                df[col] = np.nan
        
        return df
    
    def _convert_numeric_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert specified columns to numeric types efficiently.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with converted numeric columns
        """
        numeric_columns = [
            'PromotionDiscount.Amount', 'ItemPrice.Amount', 'PromotionDiscountTax.Amount',
            'ShippingTax.Amount', 'ShippingPrice.Amount', 'ShippingDiscount.Amount',
            'ShippingDiscountTax.Amount', 'ItemTax.Amount', 'OrderTotal.Amount'
        ]
        
        # Vectorized conversion
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        return df
    
    def _calculate_vat_vectorized(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Vectorized VAT calculations for all marketplaces simultaneously.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with VAT calculations
        """
        # Initialize new columns
        df['Promotional_Tax'] = 0.0
        df['vat%'] = 0.0
        df['Price'] = 0.0
        df['VAT'] = 0.0
        df['unit_price(vat_exclusive)'] = 0.0
        df['item_total'] = 0.0
        
        # Process each marketplace
        for marketplace, vat_info in self.VAT_RATES.items():
            # Create mask for current marketplace
            mask = (df['SalesChannel'] == marketplace) | (df['SalesChannel'] == 'Non-Amazon')
            
            if not mask.any():
                continue
            
            # Fill NaN values with 0 for calculations (only if columns exist)
            if 'PromotionDiscount.Amount' in df.columns:
                df.loc[mask, 'PromotionDiscount.Amount'] = df.loc[mask, 'PromotionDiscount.Amount'].fillna(0)
            else:
                df['PromotionDiscount.Amount'] = 0
                
            if 'ItemPrice.Amount' in df.columns:
                df.loc[mask, 'ItemPrice.Amount'] = df.loc[mask, 'ItemPrice.Amount'].fillna(0)
            else:
                df['ItemPrice.Amount'] = 0
                
            if 'ItemTax.Amount' in df.columns:
                df.loc[mask, 'ItemTax.Amount'] = df.loc[mask, 'ItemTax.Amount'].fillna(0)
            else:
                df['ItemTax.Amount'] = 0
            
            # Vectorized calculations
            df.loc[mask, 'Promotional_Tax'] = (
                df.loc[mask, 'PromotionDiscount.Amount'] * vat_info['multiplier'] - 
                df.loc[mask, 'PromotionDiscount.Amount']
            )
            
            # VAT percentage
            df.loc[mask, 'vat%'] = vat_info['percentage']
            df.loc[mask & (df['ItemTax.Amount'] == 0), 'vat%'] = 0
            
            # Set Promotional_Tax to 0 where ItemTax.Amount is 0
            df.loc[mask & (df['ItemTax.Amount'] == 0), 'Promotional_Tax'] = 0
            
            # Calculate Price, VAT, and other fields
            df.loc[mask, 'Price'] = df.loc[mask, 'ItemPrice.Amount'] + df.loc[mask, 'Promotional_Tax']
            df.loc[mask, 'VAT'] = df.loc[mask, 'Price'] * df.loc[mask, 'vat%']
            df.loc[mask, 'unit_price(vat_exclusive)'] = df.loc[mask, 'Price'] - df.loc[mask, 'VAT']
            df.loc[mask, 'item_total'] = (
                df.loc[mask, 'Price'] - 
                df.loc[mask, 'PromotionDiscount.Amount'] - 
                df.loc[mask, 'Promotional_Tax']
            )
            
            # Special case for zero promotional tax and discount
            zero_promo_mask = mask & (df['Promotional_Tax'] == 0) & (df['PromotionDiscount.Amount'] == 0)
            df.loc[zero_promo_mask, 'unit_price(vat_exclusive)'] = (
                df.loc[zero_promo_mask, 'Price'] - df.loc[zero_promo_mask, 'ItemTax.Amount']
            )
        
        # Round all calculated columns
        calc_columns = ['ItemTax.Amount', 'Promotional_Tax', 'Price', 'unit_price(vat_exclusive)', 'item_total']
        df[calc_columns] = df[calc_columns].round(2)
        
        return df
    
    def _add_region_mapping(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add region mapping efficiently using vectorized operations.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with region information
        """
        df['Region'] = ''
        df['Country'] = ''
        df['Company'] = ''
        df['Channel'] = 'Amazon'
        
        # Vectorized region mapping
        for marketplace, info in self.MARKETPLACE_REGIONS.items():
            mask = df['SalesChannel'] == marketplace
            df.loc[mask, 'Region'] = info['region']
            df.loc[mask, 'Country'] = info['country']
            df.loc[mask, 'Company'] = info['company']
        
        return df
    
    def _create_mssql_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Create the MSSQL-formatted DataFrame (merged_df2).
        
        Args:
            df: Input DataFrame
            
        Returns:
            MSSQL-formatted DataFrame
        """
        # Select required columns
        mssql_columns = [
            'PurchaseDate', 'PurchaseDate_conversion', 'EarliestShipDate', 'LatestShipDate',
            'AmazonOrderId', 'SalesChannel', 'Region', 'OrderStatus', 'OrderType',
            'FulfillmentChannel', 'NumberOfItemsShipped', 'IsPremiumOrder', 'IsPrime',
            'ShipServiceLevel', 'ShipmentServiceLevelCategory', 'MarketplaceId',
            'SellerOrderId', 'IsBusinessOrder', 'BuyerInfo.BuyerEmail',
            'ShippingAddress.StateOrRegion', 'ShippingAddress.PostalCode',
            'ShippingAddress.City', 'ShippingAddress.CountryCode', 'ShippingAddress.County',
            'QuantityShipped', 'ASIN', 'SellerSKU', 'QuantityOrdered', 'Title', 'IsGift',
            'OrderItemId', 'PromotionDiscountTax.CurrencyCode', 'PromotionDiscountTax.Amount',
            'ShippingTax.CurrencyCode', 'ShippingTax.Amount', 'ShippingPrice.CurrencyCode',
            'ShippingPrice.Amount', 'ShippingDiscount.CurrencyCode', 'ShippingDiscount.Amount',
            'ShippingDiscountTax.CurrencyCode', 'ShippingDiscountTax.Amount',
            'ItemTax.CurrencyCode', 'ItemTax.Amount', 'ItemPrice.CurrencyCode',
            'ItemPrice.Amount', 'PromotionDiscount.CurrencyCode', 'PromotionDiscount.Amount',
            'Promotional_Tax', 'Price', 'vat%', 'VAT', 'unit_price(vat_exclusive)',
            'item_total', 'OrderTotal.CurrencyCode', 'OrderTotal.Amount'
        ]
        
        # Select only columns that exist in the DataFrame
        available_columns = [col for col in mssql_columns if col in df.columns]
        merged_df2 = df[available_columns].copy()
        
        # Rename columns
        column_renames = {
            'VAT': 'calculated_vat',
            'ItemTax.Amount': 'vat',
            'ItemPrice.Amount': 'item_subtotal',
            'PromotionDiscount.Amount': 'promotion',
            'Price': 'unit_price(vat_inclusive)',
            'OrderTotal.CurrencyCode': 'CurrencyCode',
            'OrderTotal.Amount': 'grand_total'
        }
        
        merged_df2 = merged_df2.rename(columns=column_renames)
        
        # Add materialized date
        if 'PurchaseDate_conversion' in merged_df2.columns:
            merged_df2['PurchaseDate_Materialized'] = pd.to_datetime(merged_df2['PurchaseDate_conversion']).dt.date
        
        return merged_df2
    
    def _create_azure_dataframe(self, merged_df2: pd.DataFrame) -> pd.DataFrame:
        """
        Create the Azure-formatted DataFrame (merged_df3) with optimized transformations.
        
        Args:
            merged_df2: MSSQL DataFrame
            
        Returns:
            Azure-formatted DataFrame
        """
        # Select required columns for Azure format
        azure_columns = [
            'PurchaseDate', 'PurchaseDate_conversion', 'AmazonOrderId', 'ASIN', 'SellerSKU',
            'OrderStatus', 'SalesChannel', 'Region', 'Title', 'MarketplaceId',
            'FulfillmentChannel', 'QuantityOrdered', 'vat', 'item_subtotal', 'promotion',
            'Promotional_Tax', 'unit_price(vat_inclusive)', 'unit_price(vat_exclusive)',
            'item_total', 'grand_total', 'CurrencyCode'
        ]
        
        # Select only available columns
        available_columns = [col for col in azure_columns if col in merged_df2.columns]
        merged_df3 = merged_df2[available_columns].copy()
        
        # Add derived columns (handle missing columns)
        if 'vat' in merged_df3.columns:
            merged_df3['ItemTax_Amount'] = merged_df3['vat'].copy()
        else:
            merged_df3['ItemTax_Amount'] = 0
            
        if 'unit_price(vat_inclusive)' in merged_df3.columns:
            merged_df3['Total'] = merged_df3['unit_price(vat_inclusive)'].copy()
        else:
            merged_df3['Total'] = 0
            
        if 'promotion' in merged_df3.columns:
            merged_df3['Promotional_Rebates'] = merged_df3['promotion'].copy()
        else:
            merged_df3['Promotional_Rebates'] = 0
            
        if 'SalesChannel' in merged_df3.columns:
            merged_df3['Channel'] = merged_df3['SalesChannel'].copy()
        else:
            merged_df3['Channel'] = 'Amazon'
        
        # Rename columns
        column_renames = {
            'PurchaseDate_conversion': 'CLEAN_DateTime',
            'AmazonOrderId': 'OrderId',
            'SellerSKU': 'SKU',
            'OrderStatus': 'Type',
            'QuantityOrdered': 'Quantity'
        }
        merged_df3 = merged_df3.rename(columns=column_renames)
        
        # Apply transformations efficiently
        merged_df3 = self._apply_azure_transformations(merged_df3)
        
        return merged_df3
    
    def _apply_azure_transformations(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply all Azure transformations efficiently.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Transformed DataFrame
        """
        # 1. Filter for shipped orders only (if Type column exists)
        if 'Type' in df.columns:
            df = df[df['Type'] == 'Shipped'].copy()
            
            if df.empty:
                logger.warning("No shipped orders found after filtering")
                return df
        
        # 2. Convert datetime and add Date column (if CLEAN_DateTime exists)
        if 'CLEAN_DateTime' in df.columns:
            df['CLEAN_DateTime'] = pd.to_datetime(df['CLEAN_DateTime'])
            df['Date'] = pd.to_datetime(df['CLEAN_DateTime'].dt.date)
        
        # 3. Filter out Non-Amazon sales (if SalesChannel exists)
        if 'SalesChannel' in df.columns:
            df = df[df['SalesChannel'] != 'Non-Amazon'].copy()
        
        # 4. Filter out zero quantity orders (if Quantity exists)
        if 'Quantity' in df.columns:
            df = df[df['Quantity'] != 0].copy()
        
        # 5. Filter out null totals (if Total exists)
        if 'Total' in df.columns:
            df = df[df['Total'].notna()].copy()
        
        # 6. Clean SKU data efficiently (if SKU exists)
        if 'SKU' in df.columns:
            df['SKU'] = df['SKU'].astype(str).str.strip().str.upper()
        
        # 7. Transform Type (if Type exists)
        if 'Type' in df.columns:
            df['Type'] = df['Type'].replace('Shipped', 'Order')
        
        # 8. Update Channel, Company, and Country mappings
        df = self._update_azure_mappings(df)
        
        # 9. Select final columns
        final_columns = [
            'PurchaseDate', 'CLEAN_DateTime', 'Date', 'OrderId', 'ASIN', 'SKU', 'Title',
            'Type', 'Region', 'Country', 'SalesChannel', 'Channel', 'MarketplaceId',
            'Company', 'FulfillmentChannel', 'Quantity', 'vat', 'item_subtotal',
            'promotion', 'Promotional_Tax', 'unit_price(vat_inclusive)',
            'unit_price(vat_exclusive)', 'item_total', 'grand_total', 'CurrencyCode',
            'ItemTax_Amount', 'Total', 'Promotional_Rebates'
        ]
        
        # Select only available columns
        available_columns = [col for col in final_columns if col in df.columns]
        df = df[available_columns].copy()
        
        # 10. Apply aggregation and final transformations
        df = self._apply_aggregation(df)
        
        return df
    
    def _update_azure_mappings(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Update Channel, Company, and Country mappings efficiently.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with updated mappings
        """
        # Update Channel (all Amazon marketplaces -> 'Amazon')
        amazon_mask = df['SalesChannel'].str.startswith('Amazon.', na=False)
        df.loc[amazon_mask, 'Channel'] = 'Amazon'
        
        # Update Company and Country based on marketplace
        for marketplace, info in self.MARKETPLACE_REGIONS.items():
            mask = df['SalesChannel'] == marketplace
            df.loc[mask, 'Company'] = info['company']
            df.loc[mask, 'Country'] = info['country']
        
        return df
    
    def _apply_aggregation(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply final aggregation and merging operations efficiently.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Aggregated DataFrame
        """
        if df.empty:
            return df
        
        # Create aggregation DataFrames
        df2 = df[['PurchaseDate', 'OrderId', 'SKU', 'Region']].drop_duplicates()
        
        # Group and aggregate numeric columns
        groupby_cols = [
            'CLEAN_DateTime', 'Date', 'OrderId', 'SKU', 'Type', 'Region', 'Country',
            'SalesChannel', 'Channel', 'MarketplaceId', 'Company', 'CurrencyCode',
            'FulfillmentChannel'
        ]
        
        agg_cols = [
            'Quantity', 'vat', 'item_subtotal', 'promotion', 'Promotional_Tax',
            'unit_price(vat_inclusive)', 'unit_price(vat_exclusive)', 'item_total',
            'ItemTax_Amount', 'Total', 'Promotional_Rebates'
        ]
        
        # Filter columns that exist
        existing_groupby = [col for col in groupby_cols if col in df.columns]
        existing_agg = [col for col in agg_cols if col in df.columns]
        
        df3 = df.groupby(existing_groupby)[existing_agg].sum().reset_index()
        
        # Merge back with purchase date
        merged_df3 = pd.merge(df3, df2, on=['OrderId', 'SKU', 'Region'], how='left')
        merged_df3 = merged_df3.rename(columns={'PurchaseDate': 'data_fetch_Date'})
        
        # Add grand total
        df4 = df[['OrderId', 'grand_total']].drop_duplicates()
        merged_df3 = pd.merge(merged_df3, df4, on='OrderId', how='left')
        
        # Add title
        df5 = df[['SKU', 'Title']].drop_duplicates(subset='SKU', keep='last')
        merged_df3 = pd.merge(merged_df3, df5, on='SKU', how='left')
        
        # Calculate per-unit prices
        if 'Quantity' in merged_df3.columns and merged_df3['Quantity'].sum() > 0:
            # Avoid division by zero
            quantity_mask = merged_df3['Quantity'] != 0
            
            if 'unit_price(vat_inclusive)' in merged_df3.columns:
                merged_df3['per_unit_price(vat_inclusive)'] = 0.0
                merged_df3.loc[quantity_mask, 'per_unit_price(vat_inclusive)'] = (
                    merged_df3.loc[quantity_mask, 'unit_price(vat_inclusive)'] / 
                    merged_df3.loc[quantity_mask, 'Quantity']
                )
            
            if 'unit_price(vat_exclusive)' in merged_df3.columns:
                merged_df3['per_unit_price(vat_exclusive)'] = 0.0
                merged_df3.loc[quantity_mask, 'per_unit_price(vat_exclusive)'] = (
                    merged_df3.loc[quantity_mask, 'unit_price(vat_exclusive)'] / 
                    merged_df3.loc[quantity_mask, 'Quantity']
                )
        else:
            # Add missing per-unit price columns with default values
            merged_df3['per_unit_price(vat_inclusive)'] = 0.0
            merged_df3['per_unit_price(vat_exclusive)'] = 0.0
        
        # Ensure all required Azure columns are present with proper defaults
        merged_df3 = self._ensure_azure_columns(merged_df3)
        
        return merged_df3
    
    def _ensure_azure_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Ensure all required Azure database columns are present with proper data types and defaults.
        
        Based on Azure database schema: stg_tr_amazon_raw_test
        """
        # Define the exact column order and types as per Azure database schema (excluding IDENTITY id)
        required_columns = {
            'CLEAN_DateTime': 'datetime64[ns]',
            'Date': 'datetime64[ns]', 
            'OrderId': 'object',
            'SKU': 'object',
            'Type': 'object',
            'Region': 'object',
            'Country': 'object',
            'SalesChannel': 'object',
            'Channel': 'object',
            'MarketplaceId': 'object',
            'Company': 'object',
            'CurrencyCode': 'object',
            'FulfillmentChannel': 'object',
            'Quantity': 'int64',
            'vat': 'float64',
            'item_subtotal': 'float64',
            'promotion': 'float64',
            'unit_price(vat_inclusive)': 'float64',
            'unit_price(vat_exclusive)': 'float64',
            'per_unit_price(vat_inclusive)': 'float64',
            'per_unit_price(vat_exclusive)': 'float64',
            'item_total': 'float64',
            'grand_total': 'float64',
            'Title': 'object',
            'Total': 'float64',
            'Promotional_Rebates': 'float64',
            'Promotional_Tax': 'float64',
            'ItemTax_Amount': 'float64',
            'data_fetch_Date': 'object'
        }
        
        # Create aligned DataFrame
        aligned_df = pd.DataFrame()
        
        for col_name, col_type in required_columns.items():
            if col_name in df.columns:
                # Use existing column
                aligned_df[col_name] = df[col_name]
            else:
                # Add missing column with appropriate default
                if col_type == 'int64':
                    aligned_df[col_name] = 0
                elif col_type == 'float64':
                    aligned_df[col_name] = 0.0
                elif col_type == 'datetime64[ns]':
                    aligned_df[col_name] = pd.NaT
                else:  # object (string)
                    aligned_df[col_name] = None
        
        # Ensure data types are correct
        for col_name, col_type in required_columns.items():
            try:
                if col_type == 'int64' and aligned_df[col_name].dtype != 'int64':
                    aligned_df[col_name] = pd.to_numeric(aligned_df[col_name], errors='coerce').fillna(0).astype('int64')
                elif col_type == 'float64' and aligned_df[col_name].dtype != 'float64':
                    aligned_df[col_name] = pd.to_numeric(aligned_df[col_name], errors='coerce').fillna(0.0).astype('float64')
                elif col_type == 'datetime64[ns]':
                    aligned_df[col_name] = pd.to_datetime(aligned_df[col_name], errors='coerce')
                elif col_type == 'object':
                    aligned_df[col_name] = aligned_df[col_name].astype('object')
            except Exception as e:
                logger.warning(f"Failed to convert column {col_name} to {col_type}: {e}")
        
        logger.info(f"Azure DataFrame aligned with {len(aligned_df.columns)} columns: {list(aligned_df.columns)}")
        logger.info(f"Azure DataFrame shape: {aligned_df.shape}")
        
        return aligned_df
    
    def process_data(self, orders_data: List[Dict], order_items_data: List[Dict], 
                    marketplace_name: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Main processing function that orchestrates all data transformations.
        
        Args:
            orders_data: List of order dictionaries
            order_items_data: List of order item dictionaries
            marketplace_name: Name of the marketplace (e.g., 'UK', 'ES')
            
        Returns:
            Tuple of (merged_df2, merged_df3) - MSSQL and Azure formatted DataFrames
        """
        try:
            start_time = datetime.now()
            logger.info(f"Starting data processing for {marketplace_name}")
            
            # 1. Prepare and merge data
            merged_df = self._prepare_dataframes(orders_data, order_items_data)
            
            # 2. Split pricing columns (e.g., "12.01 GBP" -> Amount: 12.01, CurrencyCode: GBP)
            merged_df = self._split_pricing_columns(merged_df)
            
            # 3. Add missing columns
            merged_df = self._add_missing_columns(merged_df)
            
            # 4. Convert timezone
            if 'PurchaseDate' in merged_df.columns:
                merged_df['PurchaseDate_conversion'] = self._convert_timezone_optimized(
                    merged_df['PurchaseDate'], marketplace_name
                )
            
            # 5. Convert numeric columns
            merged_df = self._convert_numeric_columns(merged_df)
            
            # 6. Calculate VAT
            merged_df = self._calculate_vat_vectorized(merged_df)
            
            # 7. Add region mapping
            merged_df = self._add_region_mapping(merged_df)
            
            # 8. Create MSSQL DataFrame
            merged_df2 = self._create_mssql_dataframe(merged_df)
            
            # 9. Create Azure DataFrame
            merged_df3 = self._create_azure_dataframe(merged_df2)
            
            processing_time = (datetime.now() - start_time).total_seconds()
            logger.info(f"Data processing completed in {processing_time:.2f}s")
            logger.info(f"MSSQL DataFrame shape: {merged_df2.shape}")
            logger.info(f"Azure DataFrame shape: {merged_df3.shape}")
            
            return merged_df2, merged_df3
            
        except Exception as e:
            logger.error(f"Error processing data: {str(e)}", exc_info=True)
            raise


def process_amazon_data(orders_data: List[Dict], order_items_data: List[Dict], 
                       marketplace_name: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Main entry point for Amazon data processing.
    
    Args:
        orders_data: List of order dictionaries
        order_items_data: List of order item dictionaries
        marketplace_name: Name of the marketplace
        
    Returns:
        Tuple of (mssql_df, azure_df) processed DataFrames
    """
    processor = AmazonDataProcessor()
    return processor.process_data(orders_data, order_items_data, marketplace_name) 