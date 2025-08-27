import pandas as pd
from sqlalchemy import text
from .simple_db_save import create_Azure_db_connection

def save_inventory_report_to_azure(csv_path: str, latest_report: dict, marketplace_code: str, items_count: int) -> dict:
    """Save report metadata to dbo.Report and rows to dbo.FBA_Inventory_Report.
    Matches the schema the user provided.
    """
    engine = create_Azure_db_connection()
    report_id = latest_report.get('reportId')

    with engine.begin() as conn: # begin() auto-commits or rolls back on error
        conn.execute(text("""
            TRUNCATE TABLE dbo.FBA_Inventory_Report
            """))

    df = pd.read_csv(csv_path)
    df.columns = df.columns.str.lower()

    def col(name: str):
        return df[name] if name in df.columns else pd.Series([None] * len(df))

    mapped = pd.DataFrame({
        'sku': (col('sku') if 'sku' in df.columns else col('seller_sku')).fillna(''),
        'fnsku': col('fnsku').fillna(''),
        'asin': col('asin').fillna(''),
        'product_name': (col('product-name') if 'product-name' in df.columns else col('product_name')).fillna(''),
        'condition': (col('condition') if 'condition' in df.columns else col('item-condition')).fillna(''),
        'your_price': pd.to_numeric(col('your-price') if 'your-price' in df.columns else col('your_price'), errors='coerce'),
        'mfn_listing_exists': (col('mfn-listing-exists').astype(str).str.lower() == 'yes').astype('Int64').fillna(0).astype(int) if 'mfn-listing-exists' in df.columns else (col('mfn_listing_exists').fillna(0).astype(int) if 'mfn_listing_exists' in df.columns else 0),
        'mfn_fulfillable_quantity': pd.to_numeric(col('mfn-fulfillable-quantity') if 'mfn-fulfillable-quantity' in df.columns else col('mfn_fulfillable_quantity'), errors='coerce'),
        'afn_listing_exists': (col('afn-listing-exists').astype(str).str.lower() == 'yes').astype('Int64').fillna(0).astype(int) if 'afn-listing-exists' in df.columns else (col('afn_listing_exists').fillna(0).astype(int) if 'afn_listing_exists' in df.columns else 0),
        'afn_warehouse_quantity': pd.to_numeric(col('afn-warehouse-quantity') if 'afn-warehouse-quantity' in df.columns else col('afn_warehouse_quantity'), errors='coerce'),
        'afn_fulfillable_quantity': pd.to_numeric(col('afn-fulfillable-quantity') if 'afn-fulfillable-quantity' in df.columns else col('afn_fulfillable_quantity'), errors='coerce'),
        'afn_unsellable_quantity': pd.to_numeric(col('afn-unsellable-quantity') if 'afn-unsellable-quantity' in df.columns else col('afn_unsellable_quantity'), errors='coerce'),
        'afn_reserved_quantity': pd.to_numeric(col('afn-reserved-quantity') if 'afn-reserved-quantity' in df.columns else col('afn_reserved_quantity'), errors='coerce'),
        'afn_total_quantity': pd.to_numeric(col('afn-total-quantity') if 'afn-total-quantity' in df.columns else col('afn_total_quantity'), errors='coerce'),
        'per_unit_volume': pd.to_numeric(col('per-unit-volume') if 'per-unit-volume' in df.columns else col('per_unit_volume'), errors='coerce'),
        'afn_inbound_working_quantity': pd.to_numeric(col('afn-inbound-working-quantity') if 'afn-inbound-working-quantity' in df.columns else col('afn_inbound_working_quantity'), errors='coerce'),
        'afn_inbound_shipped_quantity': pd.to_numeric(col('afn-inbound-shipped-quantity') if 'afn-inbound-shipped-quantity' in df.columns else col('afn_inbound_shipped_quantity'), errors='coerce'),
        'afn_inbound_receiving_quantity': pd.to_numeric(col('afn-inbound-receiving-quantity') if 'afn-inbound-receiving-quantity' in df.columns else col('afn_inbound_receiving_quantity'), errors='coerce'),
        'afn_researching_quantity': pd.to_numeric(col('afn-researching-quantity') if 'afn-researching-quantity' in df.columns else col('afn_researching_quantity'), errors='coerce'),
        'afn_reserved_future_supply': pd.to_numeric(col('afn-reserved-future-supply') if 'afn-reserved-future-supply' in df.columns else col('afn_reserved_future_supply'), errors='coerce'),
        'afn_future_supply_buyable': pd.to_numeric(col('afn-future-supply-buyable') if 'afn-future-supply-buyable' in df.columns else col('afn_future_supply_buyable'), errors='coerce'),
        'afn_fulfillable_quantity_local': pd.to_numeric(col('afn-fulfillable-quantity-local') if 'afn-fulfillable-quantity-local' in df.columns else col('afn_fulfillable_quantity_local'), errors='coerce'),
        'afn_fulfillable_quantity_remote': pd.to_numeric(col('afn-fulfillable-quantity-remote') if 'afn-fulfillable-quantity-remote' in df.columns else col('afn_fulfillable_quantity_remote'), errors='coerce'),
        'store': marketplace_code
    })

    for c in [
        'mfn_fulfillable_quantity','afn_warehouse_quantity','afn_fulfillable_quantity','afn_unsellable_quantity',
        'afn_reserved_quantity','afn_total_quantity','afn_inbound_working_quantity','afn_inbound_shipped_quantity',
        'afn_inbound_receiving_quantity','afn_researching_quantity','afn_reserved_future_supply','afn_future_supply_buyable',
        'afn_fulfillable_quantity_local','afn_fulfillable_quantity_remote'
    ]:
        if c in mapped.columns:
            mapped[c] = pd.to_numeric(mapped[c], errors='coerce').fillna(0).astype(int)

    mapped.to_sql(
        name='FBA_Inventory_Report',
        con=engine,
        if_exists='append',
        index=False
    )

    return {
        'success': True,
        'report_id': report_id,
        'records_saved': int(len(mapped)),
        'total_items': items_count,
        'marketplace_code': marketplace_code
    }
