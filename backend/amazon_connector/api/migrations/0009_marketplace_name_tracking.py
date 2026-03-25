from django.db import migrations, models


MARKETPLACE_NAME_MAP = {
    'ATVPDKIKX0DER': 'United States',
    'A2EUQ1WTGCTBG2': 'Canada',
    'A1F83G8C2ARO7P': 'United Kingdom',
    'A1PA6795UKMFR9': 'Germany',
    'A13V1IB3VIYZZH': 'France',
    'APJ6JRA9NG5V4': 'Italy',
    'A1RKKUPIHCS9HS': 'Spain',
}


def _resolve_name(marketplace_id):
    return MARKETPLACE_NAME_MAP.get(marketplace_id, marketplace_id)


def backfill_marketplace_names(apps, schema_editor):
    Activities = apps.get_model('api', 'Activities')
    MarketplaceLastRun = apps.get_model('api', 'MarketplaceLastRun')
    SCMLastRun = apps.get_model('api', 'SCMLastRun')

    for row in Activities.objects.all().only('activity_id', 'marketplace_id'):
        Activities.objects.filter(activity_id=row.activity_id).update(
            marketplace_name=_resolve_name(row.marketplace_id)
        )

    for row in MarketplaceLastRun.objects.all().only('id', 'marketplace_id'):
        MarketplaceLastRun.objects.filter(id=row.id).update(
            marketplace_name=_resolve_name(row.marketplace_id)
        )

    for row in SCMLastRun.objects.all().only('id', 'marketplace_id'):
        SCMLastRun.objects.filter(id=row.id).update(
            marketplace_name=_resolve_name(row.marketplace_id)
        )


class Migration(migrations.Migration):

    dependencies = [
        ('api', '0008_company_name_tracking'),
    ]

    operations = [
        migrations.AddField(
            model_name='activities',
            name='marketplace_name',
            field=models.CharField(
                default='',
                help_text='Human-readable marketplace name derived from marketplace_id',
                max_length=100,
            ),
        ),
        migrations.AddField(
            model_name='marketplacelastrun',
            name='marketplace_name',
            field=models.CharField(
                default='',
                help_text='Human-readable marketplace name derived from marketplace_id',
                max_length=100,
            ),
        ),
        migrations.AddField(
            model_name='scmlastrun',
            name='marketplace_name',
            field=models.CharField(
                default='',
                help_text='Human-readable marketplace name derived from marketplace_id',
                max_length=100,
            ),
        ),
        migrations.RunPython(backfill_marketplace_names, migrations.RunPython.noop),
    ]
