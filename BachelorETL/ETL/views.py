from django.shortcuts import render
from django.http import HttpResponse
from django.shortcuts import render
from ETL import models
from sqlalchemy import *
from sqlalchemy.engine import reflection
from sqlalchemy import inspect
from sqlalchemy.sql import func
import pandas as pd
import numpy as np
import re, datetime
import pytz
import hashlib

def index_view(request):
    return render(request, 'ETL/index.html')

def receive_connections(request):
    source_conn, created = models.Connection.objects.update_or_create(
        dialect=request.POST['dbselect'],
        host=request.POST['host'],
        database=request.POST['db'],
        schema=request.POST['schema'],
        defaults={
            'name': request.POST['conn_name'],
            'username': request.POST['username'],
            'password': request.POST['password'],
        }
    )
    target_conn, created = models.Connection.objects.update_or_create(
        dialect=request.POST['dbselect_t'],
        host=request.POST['host_t'],
        database=request.POST['db_t'],
        schema=request.POST['schema_t'],
        defaults={
            'name': request.POST['conn_name_t'],
            'username': request.POST['username_t'],
            'password': request.POST['password_t'],
        }
    )
    return render(request, 'ETL/tables.html', {
        'table_list': get_tables(source_conn),
        'conn': source_conn,
        'target': target_conn
        })

def handle_tables(request):
    conn = models.Connection.objects.get(id=request.POST['conn_id'])
    target = models.Connection.objects.get(id=request.POST['target_id'])
    tables = []
    for item in request.POST:
        if item not in ('conn_id', 'csrfmiddlewaretoken','target_id'):
            tables.append(item)
    create_tables(conn, target, tables, request)
    return render(request, 'ETL/transfer.html', {
        'tables': tables,
        'conn': conn,
        'target' : target
        })

def transfer_data(request):
    if request.POST['confirm'] == 'yes':
        conn = models.Connection.objects.get(id=request.POST['conn_id'])
        target = models.Connection.objects.get(id=request.POST['target_id'])
        tables = models.Tables.objects.filter(connection_id=conn.id).values_list('name', flat=True)
        if 'mssql' in conn.dialect:
            params = '?driver=ODBC+Driver+17+for+SQL+Server'
        else:
            params = ''
        engine = create_engine(
            f'{conn.dialect}://{conn.username}:{conn.password}@{conn.host}/{conn.database}{params}'
        )

        target_engine = create_engine(
            f'{target.dialect}://{target.username}:{target.password}@{target.host}/{target.database}{params}'
        )
        meta = MetaData(bind=engine)
        meta.reflect(schema=conn.schema)
        for table in meta.tables:
            if meta.tables[table].name in tables:
                s = meta.tables[table].select()
                result = engine.execute(s)

                print(hashlib.md5(result.fetchone().values).hexdigest())

        return HttpResponse('Ok')
def get_tables(connection):
    if 'mssql' in connection.dialect:
        params = '?driver=ODBC+Driver+17+for+SQL+Server'
    else:
        params = ''
    engine = create_engine(
        f'{connection.dialect}://{connection.username}:{connection.password}@{connection.host}/{connection.database}{params}')
    inspector = reflection.Inspector.from_engine(engine)

    return inspector.get_table_names(schema=connection.schema)

def create_tables(connection, target, tables, request):
    if 'mssql' in connection.dialect:
        params = '?driver=ODBC+Driver+17+for+SQL+Server'
    else:
        params = ''
    engine = create_engine(
        f'{connection.dialect}://{connection.username}:{connection.password}@{connection.host}/{connection.database}{params}'
    )

    target_engine = create_engine(
        f'{target.dialect}://{target.username}:{target.password}@{target.host}/{target.database}{params}'
    )
    meta = MetaData(bind=engine)
    meta.reflect(schema=connection.schema)
    meta_target = MetaData(schema=target.schema)
    for table in meta.tables:
        if meta.tables[table].name in tables:
            cols = [Column(col.name, col.type) for col in meta.tables[table].columns]
            Table(
                meta.tables[table].name, meta_target,
                *cols,
                Column('Sourcekey', VARCHAR(length=4000)),
                Column('Checksum', VARCHAR(length=32)),
                Column('Newest_flag', BOOLEAN, default=True),
                Column('Delete_flag', BOOLEAN, default=False),
                Column('Valid_from', DateTime(timezone=True), server_default=func.now()),
                Column('Valid_to',
                    DateTime(timezone=True),
                    default=datetime.datetime(9999,12,31, 00,00, tzinfo=pytz.UTC),
                    onupdate=func.now()
                ),
                Column('Insert_job_id', INTEGER),
                Column('Update_job_id', INTEGER),
            )
            models.Tables.objects.update_or_create(
                connection_id=connection.id,
                name=meta.tables[table].name,
                defaults={
                    'target_name': meta.tables[table].name,
                    'enabled' : True
                }
            )
    meta_target.create_all(target_engine)