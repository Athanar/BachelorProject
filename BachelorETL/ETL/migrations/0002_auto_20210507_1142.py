# Generated by Django 3.1.2 on 2021-05-07 09:42

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('ETL', '0001_initial'),
    ]

    operations = [
        migrations.RenameField(
            model_name='connection',
            old_name='server',
            new_name='host',
        ),
    ]
