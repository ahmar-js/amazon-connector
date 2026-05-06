from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('api', '0012_scmjoblock'),
    ]

    operations = [
        migrations.CreateModel(
            name='SCMOrderReconciliationQueue',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('company_name', models.CharField(default='B2Fitinss', max_length=100)),
                ('marketplace_code', models.CharField(max_length=10)),
                ('marketplace_id', models.CharField(default='', max_length=255)),
                ('source_table', models.CharField(max_length=100)),
                ('amazon_order_id', models.CharField(max_length=255)),
                ('seller_sku', models.CharField(blank=True, default='', max_length=255)),
                ('asin', models.CharField(blank=True, default='', max_length=255)),
                ('current_status', models.CharField(max_length=100)),
                ('purchase_date', models.DateTimeField(blank=True, null=True)),
                ('last_update_date', models.DateTimeField(blank=True, null=True)),
                ('next_check_at', models.DateTimeField()),
                ('check_count', models.PositiveIntegerField(default=0)),
                ('is_final', models.BooleanField(default=False)),
                ('final_status', models.CharField(blank=True, max_length=100, null=True)),
                ('last_checked_at', models.DateTimeField(blank=True, null=True)),
                ('last_error', models.TextField(blank=True, default='')),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('updated_at', models.DateTimeField(auto_now=True)),
            ],
            options={
                'verbose_name': 'SCM Order Reconciliation Queue',
                'verbose_name_plural': 'SCM Order Reconciliation Queue',
                'db_table': 'scm_order_reconciliation_queue',
            },
        ),
        migrations.AddIndex(
            model_name='scmorderreconciliationqueue',
            index=models.Index(fields=['is_final', 'next_check_at'], name='api_scmorde_is_fina_1bb48d_idx'),
        ),
        migrations.AddIndex(
            model_name='scmorderreconciliationqueue',
            index=models.Index(fields=['marketplace_code', 'is_final', 'next_check_at'], name='api_scmorde_marketp_6fd969_idx'),
        ),
        migrations.AddIndex(
            model_name='scmorderreconciliationqueue',
            index=models.Index(fields=['company_name', 'marketplace_code', 'amazon_order_id'], name='api_scmorde_company_f93ef8_idx'),
        ),
        migrations.AddIndex(
            model_name='scmorderreconciliationqueue',
            index=models.Index(fields=['purchase_date'], name='api_scmorde_purchas_45eb5c_idx'),
        ),
        migrations.AddConstraint(
            model_name='scmorderreconciliationqueue',
            constraint=models.UniqueConstraint(
                fields=('company_name', 'marketplace_code', 'amazon_order_id', 'seller_sku', 'asin'),
                name='unique_scm_reconciliation_order_item',
            ),
        ),
    ]
