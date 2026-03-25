from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('api', '0007_scmlastrun'),
    ]

    operations = [
        migrations.AddField(
            model_name='activities',
            name='company_name',
            field=models.CharField(
                default='B2Fitinss',
                help_text='Company/account name used for this fetch (e.g., B2Fitinss, RDX INC LTD)',
                max_length=100,
            ),
        ),
        migrations.AddField(
            model_name='marketplacelastrun',
            name='company_name',
            field=models.CharField(
                default='B2Fitinss',
                help_text='Company/account name used for this marketplace tracking row',
                max_length=100,
            ),
        ),
        migrations.AddField(
            model_name='scmlastrun',
            name='company_name',
            field=models.CharField(
                default='B2Fitinss',
                help_text='Company/account name used for this SCM tracking row',
                max_length=100,
            ),
        ),
        migrations.AlterField(
            model_name='scmlastrun',
            name='marketplace_id',
            field=models.CharField(
                help_text='Amazon marketplace ID (e.g., ATVPDKIKX0DER for US)',
                max_length=255,
            ),
        ),
        migrations.AddIndex(
            model_name='activities',
            index=models.Index(fields=['company_name', '-activity_date'], name='api_activit_company_619236_idx'),
        ),
        migrations.RemoveConstraint(
            model_name='activities',
            name='unique_in_progress_activity',
        ),
        migrations.AddConstraint(
            model_name='activities',
            constraint=models.UniqueConstraint(
                condition=models.Q(('status', 'in_progress')),
                fields=('company_name', 'marketplace_id', 'activity_type', 'date_from', 'date_to', 'status'),
                name='unique_in_progress_activity',
            ),
        ),
        migrations.AddConstraint(
            model_name='marketplacelastrun',
            constraint=models.UniqueConstraint(
                fields=('company_name', 'marketplace_id'),
                name='unique_marketplace_company_last_run',
            ),
        ),
        migrations.AddConstraint(
            model_name='scmlastrun',
            constraint=models.UniqueConstraint(
                fields=('company_name', 'marketplace_id'),
                name='unique_scm_marketplace_company_last_run',
            ),
        ),
    ]
