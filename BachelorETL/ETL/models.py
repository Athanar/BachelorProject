from django.db import models

class Connection(models.Model):
    name            = models.CharField(max_length=30)
    dialect         = models.CharField(max_length=300)
    username        = models.CharField(max_length=300)
    password        = models.CharField(max_length=300)
    host            = models.CharField(max_length=300)
    database        = models.CharField(max_length=300)
    schema          = models.CharField(max_length=300)

class Tables(models.Model):
    connection_id   = models.IntegerField()
    name            = models.CharField(max_length=300)
    target_name     = models.CharField(max_length=300)
    enabled         = models.BooleanField(default=True)

class Columns(models.Model):
    table_id        = models.IntegerField()
    name            = models.CharField(max_length=300)
    target_name     = models.CharField(max_length=300)
    data_type       = models.CharField(max_length=300)
    length          = models.CharField(default='', max_length=300)
    is_key          = models.BooleanField(default=False)
    enabled         = models.BooleanField(default=True)
