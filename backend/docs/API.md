## Amazon Connector API Reference

This document lists all backend API endpoints exposed by the Amazon Connector project.

- **Base URL**: `/api/`
- **Auth**: None (all endpoints are public and CSRF-exempt in this build)
- **Content type**: Unless noted, send JSON bodies with header `Content-Type: application/json`
- **Notes**:
  - Many endpoints interact with Amazon SP-API using headers like `x-amz-access-token` internally. Those headers are not required from the client unless explicitly stated.
  - The backend reads and writes a `creds.json` file to store tokens produced by connection flows.

## Connection and Tokens

### POST /api/connect/
Connect to Amazon and persist credentials to `creds.json`.

- **Headers**: `Content-Type: application/json`
- **Body**:
```json
{
  "appId": "amzn1.application-oa2-client.xxxxxxxxxxxxxxxxx",
  "clientSecret": "<client-secret>",
  "refreshToken": "Atzr|..."
}
```
- **Response**: 200 with access token details on success; 4xx/5xx with error/details on failure.

### POST /api/test-connection/
Validate Amazon credentials without storing them.

- **Headers**: `Content-Type: application/json`
- **Body**:
```json
{
  "appId": "amzn1.application-oa2-client.xxxxxxxxxxxxxxxxx",
  "clientSecret": "<client-secret>",
  "refreshToken": "Atzr|..."
}
```
- **Response**: 200 with basic token metadata if valid; detailed 4xx on failure.

### POST /api/refresh-token/
Refresh access token using provided credentials (does not require existing `creds.json`).

- **Headers**: `Content-Type: application/json`
- **Body**:
```json
{
  "appId": "amzn1.application-oa2-client.xxxxxxxxxxxxxxxxx",
  "clientSecret": "<client-secret>",
  "refreshToken": "Atzr|..."
}
```
- **Response**: 200 with new `access_token`, `expires_at`, etc.

### GET /api/connection-status/
Return current connection status based on stored `creds.json`.

- **Headers**: none required
- **Query/body**: none
- **Response**: JSON with fields like `isConnected`, `expires_at`, `connected_at`, etc.

### POST /api/manual-refresh/
Refresh access token using credentials stored in `creds.json`.

- **Headers**: `Content-Type: application/json` (body not required)
- **Body**: none
- **Response**: 200 with new token details. 400 if no valid stored credentials.

## Orders and Items Fetch

### POST /api/fetch-data/
Fetch orders and order items from Amazon SP-API for a marketplace and date range. Optionally save processed data to databases and cache it for download.

- **Headers**: `Content-Type: application/json`
- **Body (required fields)**:
```json
{
  "access_token": "<amazon-sp-api-access-token>",
  "marketplace_id": "A1F83G8C2ARO7P",
  "start_date": "2025-01-01",     
  "end_date": "2025-01-15",
  "max_orders": 500,               
  "auto_save": true                
}
```
  - **marketplace_id**: one of `ATVPDKIKX0DER` (US), `A2EUQ1WTGCTBG2` (CA), `A1F83G8C2ARO7P` (UK), `A1PA6795UKMFR9` (DE), `A13V1IB3VIYZZH` (FR), `APJ6JRA9NG5V4` (IT), `A1RKKUPIHCS9HS` (ES)
  - **start_date / end_date**: `YYYY-MM-DD` or ISO `YYYY-MM-DDTHH:MM:SSZ` (max 30 days span)
  - **max_orders**: optional; default unlimited
  - **auto_save**: optional; when true, attempts to persist processed data to MSSQL and Azure via backend connectors
- **Response**: JSON containing fetched `orders`, `order_items`, and `processed_data` with a `cache_key` for downloads.

### POST /api/fetch-missing-items/
Fetch order items for specific orders (recovery for failures).

- **Headers**: `Content-Type: application/json`
- **Body**:
```json
{
  "access_token": "<amazon-sp-api-access-token>",
  "marketplace_id": "A1F83G8C2ARO7P",
  "order_ids": ["111-1234567-1234567", "111-7654321-7654321"]
}
```
- **Response**: JSON with `items`, `failed_orders`, `statistics`.

## Processed Data (Download/Status)

### GET /api/download-processed/
List available cache keys and quick stats for processed data in memory.

- **Headers**: none required
- **Query/body**: none
- **Response**: JSON with `cache_keys` and `cache_info`.

### POST /api/download-processed/
Generate and download processed data as CSV (returns `text/csv`). Falls back to latest file on disk if cache has expired.

- **Headers**: `Content-Type: application/json` (you may set `Accept: text/csv`)
- **Body**:
```json
{
  "cache_key": "processed_data_A1F83G8C2ARO7P_1735641234",
  "data_type": "mssql"   
}
```
  - **data_type**: `mssql` or `azure`
- **Response**: CSV file (HTTP attachment) or JSON error if not found.

### GET /api/processed-status/
Return current status and file stats for data under `processed_data/` on disk.

- **Headers**: none required
- **Query/body**: none
- **Response**: JSON with counts and latest files.

## Inventory Reports

### POST /api/inventory/reports/
Fetch previous-day Amazon inventory reports for one or more marketplaces, download TSV, convert to CSV, and save to databases.

- **Headers**: `Content-Type: application/json`
- **Body**:
```json
{
  "marketplaces": ["IT", "DE", "UK"]
}
```
  - **marketplaces**: optional; defaults to `["IT","DE","UK"]`. Valid codes: `IT`,`DE`,`UK`.
- **Response**: JSON results per marketplace with activity IDs and persistence results.

### GET /api/inventory/reports/
Info endpoint describing available marketplaces and credentials status.

- **Headers**: none required
- **Query/body**: none
- **Response**: JSON with metadata and rate limits.

### POST /api/inventory/report-schedules/
Create SP-API report schedules.

- **Headers**: `Content-Type: application/json`
- **Body**:
```json
{
  "reportType": "GET_FBA_MYI_ALL_INVENTORY_DATA",
  "period": "PT24H",
  "nextReportCreationTime": "2025-08-12T00:23:00", 
  "timeZone": "Asia/Karachi",
  "reportOptions": {"foo": "bar"},
  "marketplaces": ["IT", "DE", "UK"]
}
```
- **Response**: JSON results keyed by marketplace code, each with either schedule details or an error.

### GET /api/inventory/report-schedules/list/
List report schedules (optionally filter by marketplaces, report types, and page size).

- **Headers**: none required
- **Query params**:
  - `marketplaces=IT,DE,UK` (optional; defaults to `IT,DE,UK`)
  - `reportTypes=GET_FBA_MYI_ALL_INVENTORY_DATA` (required by SP-API; default provided if omitted)
  - `pageSize=100` (optional)
- **Response**: JSON results keyed by marketplace code with normalized `reportSchedules` arrays.

### DELETE /api/inventory/report-schedules/{report_schedule_id}/
Cancel a report schedule by ID.

- **Headers**: none required
- **Query params**:
  - `marketplace=IT` (optional; influences region selection; defaults to EU for supported markets)
- **Response**: 200/202/204 with success and timing; otherwise an error with upstream response content.

## Activity Logs

### GET /api/activities/
List activities with filtering and pagination.

- **Headers**: none required
- **Query params**:
  - `page` (default 1)
  - `page_size` (default 10, max 100)
  - `marketplace_id`
  - `status` (one of: `pending`, `in_progress`, `completed`, `failed`, `cancelled`)
  - `activity_type` (e.g., `orders`, `reports`, `sync`)
  - `search` (searches `detail`, `error_message`, `marketplace_id`)
  - `date_from` (ISO date)
  - `date_to` (ISO date)
- **Response**: JSON `{ data: { activities: [...], pagination: { ... } } }`.

### GET /api/activities/{activity_id}/
Get details for a single activity by UUID.

- **Headers**: none required
- **Response**: JSON with all activity fields and computed properties (`duration_formatted`, `total_records`, etc.).

### GET /api/activities/stats/
Get activity statistics across a period.

- **Headers**: none required
- **Query params**:
  - `days` (default 30)
  - `marketplace_id` (optional)
- **Response**: JSON summary with totals, success rate, breakdowns, and recent activities.

## Common Errors

- 400: Invalid or missing parameters, invalid credentials, or bad JSON
- 401/403: Upstream Amazon authorization failures (surfaced as 400 from some views)
- 404: Not found (e.g., missing cache or activity)
- 408: Upstream timeout relayed where applicable
- 500: Unexpected errors

## Quick Examples

### Connect
```bash
curl -X POST http://localhost:8000/api/connect/ \
  -H "Content-Type: application/json" \
  -d '{
    "appId": "amzn1.application-oa2-client.xxx",
    "clientSecret": "...",
    "refreshToken": "Atzr|..."
  }'
```

### Fetch Orders/Items
```bash
curl -X POST http://localhost:8000/api/fetch-data/ \
  -H "Content-Type: application/json" \
  -d '{
    "access_token": "<token>",
    "marketplace_id": "A1F83G8C2ARO7P",
    "start_date": "2025-01-01",
    "end_date": "2025-01-15",
    "auto_save": true
  }'
```

### Download Processed Data (CSV)
```bash
curl -X POST http://localhost:8000/api/download-processed/ \
  -H "Content-Type: application/json" \
  -H "Accept: text/csv" \
  -d '{"cache_key": "processed_data_A1F83G8C2ARO7P_1735641234", "data_type": "mssql"}' \
  -o MSSQL_data_UK_20250101_120000.csv
```


