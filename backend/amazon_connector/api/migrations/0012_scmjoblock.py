from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('api', '0011_inventory_report_log'),
    ]

    operations = [
        migrations.CreateModel(
            name='SCMJobLock',
            fields=[
                ('job_name', models.CharField(max_length=100, primary_key=True, serialize=False)),
                ('locked_at', models.DateTimeField(blank=True, null=True)),
                ('expires_at', models.DateTimeField(blank=True, null=True)),
                ('locked_by', models.CharField(blank=True, max_length=100, null=True)),
                ('stop_requested', models.BooleanField(default=False)),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('updated_at', models.DateTimeField(auto_now=True)),
            ],
            options={
                'verbose_name': 'SCM Job Lock',
                'verbose_name_plural': 'SCM Job Locks',
                'db_table': 'scm_job_locks',
            },
        ),
        migrations.AddIndex(
            model_name='scmjoblock',
            index=models.Index(fields=['expires_at'], name='api_scmjob_expires_f6a1bd_idx'),
        ),
    ]
