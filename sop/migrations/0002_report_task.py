# Generated by Django 2.2.28 on 2023-11-26 18:16

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('sop', '0001_initial'),
    ]

    operations = [
        migrations.CreateModel(
            name='report_task',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('BatchId', models.CharField(max_length=50)),
                ('UseModel', models.CharField(max_length=255)),
                ('ReportName', models.CharField(max_length=255)),
                ('DateRange', models.CharField(max_length=50)),
                ('Status', models.IntegerField()),
                ('UpdateTime', models.DateField()),
                ('PersonInCharge', models.CharField(max_length=100, verbose_name='当前负责人')),
                ('fileUrl', models.CharField(max_length=255)),
            ],
        ),
    ]
