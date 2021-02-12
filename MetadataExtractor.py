from sqlalchemy import *
from sqlalchemy.engine import reflection
import pandas as pd


""" engine = create_engine('postgresql://postgres:Viteco2020@localhost/fwsmetadata')
meta = MetaData()
user_table = Table('vmd_client_role', meta, schema='vmd',autoload=True, autoload_with=engine)
insp = reflection.Inspector.from_engine(engine)
schemas = insp.get_schema_names()
tables = [insp.get_table_names(schema=item) for item in schemas] """

class MetadataExtractor(object):
    def __init__(self, dialect, username, password, database, *args, **kwargs):
        self.engine = create_engine(
            f'{dialect}://{username}:{password}@localhost/{database}'
        )
        self.meta = MetaData()
        self.inspector = reflection.Inspector.from_engine(self.engine)

    def get_all_tables(self):
        schemas = self.inspector.get_schema_names()
        tables = {item: self.inspector.get_table_names(schema=item) for item in schemas}
        cleaned_tables = {k: v for k, v in tables.items() if len(tables[k]) > 0}
        return cleaned_tables

    def get_column_list(self, tables):
        column_list = []
        for schema in tables:
            for table in tables[schema]:
                primary_key = self.inspector\
                                .get_pk_constraint(table, schema=schema)
                if primary_key['name'] != None:
                    uniques = self.inspector\
                                .get_unique_constraints(table, schema=schema)
                    columns = self.inspector.get_columns(table, schema=schema)
                    df = pd.DataFrame(columns)
                    df['primekey'] = df['name'].isin(primary_key['constrained_columns'])
                    if len(uniques) > 0:
                        df['unique'] = df['name'].isin(uniques[0]['column_names'])
                    else:
                        df['unique'] = False
                    foreign_keys = self.inspector\
                                .get_foreign_keys(table, schema=schema)
                    if len(foreign_keys) > 0:
                        df['foreignkey'] = df['name'].isin([col['constrained_columns'][0] for col in foreign_keys])
                    else:
                        df['foreignkey'] = False
                    column_list.append(df)
        return pd.concat(column_list, ignore_index=True)
    
        

extractor = MetadataExtractor('postgresql', 'postgres', 'Viteco2020', 'fwsmetadata')
tables = extractor.get_all_tables()
columns = extractor.get_column_list(tables) 
print(columns)
