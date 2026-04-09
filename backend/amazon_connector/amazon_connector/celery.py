from __future__ import absolute_import, unicode_literals
import os
from celery import Celery
from celery.schedules import crontab
from django.conf import settings

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'amazon_connector.settings')

app = Celery('amazon_connector')

app.config_from_object('django.conf:settings', namespace='CELERY')
app.autodiscover_tasks(lambda: settings.INSTALLED_APPS)

# ---------------------------------------------------------------------------
# Celery Beat schedule (static fallback — also manageable via django-celery-beat admin)
# ---------------------------------------------------------------------------
app.conf.beat_schedule = {
    'daily-inventory-report-1015-pkt': {
        'task': 'api.tasks.generate_reports',
        'schedule': crontab(hour=5, minute=15),  # 05:15 UTC = 10:15 AM Asia/Karachi (PKT, UTC+5)
        'options': {'queue': 'reports'},
    },
}