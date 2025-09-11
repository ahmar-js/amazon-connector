from django.http import JsonResponse
from django.db import connection

def healthz(request):
    # Check DB connection (optional)
    try:
        connection.ensure_connection()
        db_status = "ok"
    except Exception as e:
        db_status = f"error: {str(e)}"

    return JsonResponse({
        "status": "ok",
        "database": db_status
    })