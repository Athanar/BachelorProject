from django.shortcuts import render
from django.http import HttpResponse
from django.shortcuts import render
from . import models
from sqlalchemy import *
from sqlalchemy.engine import reflection
from sqlalchemy import inspect
from sqlalchemy.sql import func
import datetime
import pytz
import hashlib
from .utils.Experiments import load_predictor
from .utils.MetadataExtractor import Extractor

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

    return render(request, 'ETL/transfer.html', {
        'conn': conn,
        'target' : target
        })

def show_suggestion(request):
    conn = models.Connection.objects.get(id=request.POST['conn_id'])
    target = models.Connection.objects.get(id=request.POST['target_id'])
    tables = []
    for item in request.POST:
        if item not in ('conn_id', 'csrfmiddlewaretoken','target_id'):
            tables.append(item)
    create_tables(conn, target, tables, request)

    all_tables = models.Tables.objects.filter(connection_id=conn.id).values_list('name', flat=True)
    ext = Extractor(conn.dialect, conn.username, conn.password, conn.database)
    df = load_predictor('sportsdb', ext, all_tables)
    #print(df[df])
    fact = df[(df['gauss'] == 0) & (df['kmeans'] == 0) & ((df['varchar'] == 0))].drop_duplicates(subset=['name'])
    new_fact = list(zip(fact.name.tolist(), fact.table.tolist()))
    dimensions = {}
    for table in all_tables:
        dim = df[(df['gauss'] == 1) & (df['kmeans'] == 1) & (df['table'] == str(table))]
        if len(dim) > 0:
            dimensions[table] = dim['name'].tolist()
    return render(request, 'ETL/suggestion.html', {
        'fact': new_fact,
        'dimensions' : dimensions,
        'conn': conn,
        'target': target
        }

    )

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
            model, created = models.Tables.objects.update_or_create(
                connection_id=connection.id,
                name=meta.tables[table].name,
                defaults={
                    'target_name': meta.tables[table].name,
                    'enabled' : True
                }
            )
            print(created)
    meta_target.create_all(target_engine)

def create_fact_table(connection, columns):
    return 'Ok'

def create_dimension_table(connection, columns):
    return 'Ok'