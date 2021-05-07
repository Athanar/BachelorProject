from sqlalchemy import *
from sqlalchemy.engine import reflection
import pandas as pd
import numpy as np
import re



engine = create_engine('postgresql://postgres:Viteco2020@localhost/testdb')
engine2 = create_engine('postgresql://postgres:Viteco2020@localhost/targetdb')
meta = MetaData()
meta.reflect(bind=engine)

meta2 = MetaData()
meta2.reflect(bind=engine2)

meta3 = MetaData()
creatable_tables = []

cols = [Column(col.name, col.type) for col in meta.tables['weather_conditions'].columns]
new_table = Table(
    'new_table', meta3,
    *cols,
    Column('sourcekey', VARCHAR(length=4000)),
    Column('checksum', VARCHAR(length=32)),
    Column('test', VARCHAR(length=100)),
    Column('test', VARCHAR(length=100)),
)
for table in meta.tables:
    if table not in meta2.tables:
        for column in meta.tables[table].columns:
            meta.tables[table].columns[column.key].server_default = None
        creatable_tables.append(meta.tables[table])
    else:
        continue
meta3.create_all(engine2)
