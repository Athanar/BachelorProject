from sqlalchemy import *
from sqlalchemy.engine import reflection
import pandas as pd
import numpy as np
import re
from sklearn.cluster import KMeans
from sklearn.mixture import GaussianMixture
from sklearn.cluster import DBSCAN
from sklearn.preprocessing import OneHotEncoder
from sklearn.preprocessing import OrdinalEncoder

def checkLength(type_string):
    check = re.search(r'\d+', type_string)
    if check is not None:
        return float(re.search(r'\d+', type_string).group(0))
    return 0.0

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

    def get_column_list(self, table_dict):
        column_list = []
        for schema in table_dict:
            for table in table_dict[schema]:
                primary_key = self.inspector\
                                .get_pk_constraint(table, schema=schema)
                if primary_key['name'] is not None:
                    uniques = self.inspector\
                                .get_unique_constraints(table, schema=schema)
                else:
                    uniques = []
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
                df['length'] = df['type'].apply(lambda x: checkLength(str(x)))
                df['type'] = df['type'].apply(lambda x: re.search(r'\w+', str(x)).group(0).lower())
                column_list.append(df)
        return pd.concat(column_list, ignore_index=True).drop(columns=['name'])

extractor = MetadataExtractor('postgresql', 'postgres', 'Viteco2020', 'testdb')
tables = extractor.get_all_tables()
column_df = extractor.get_column_list(tables)
encoder_one = OneHotEncoder(sparse=False)
encoder_ord = OrdinalEncoder()
onehot = encoder_one.fit_transform(column_df.values)
ordinal = encoder_ord.fit_transform(column_df.values)
kmeans = KMeans(n_clusters=2, random_state=0).fit(onehot)
kmeans_ord = KMeans(n_clusters=2, random_state=0).fit(ordinal)
gauss_one = GaussianMixture(n_components=2, random_state=0).fit(onehot)
gauss_ord = GaussianMixture(n_components=2, random_state=0).fit(ordinal)
column_df['kmeansOnehot'] = kmeans.labels_
column_df['kmeansOrd'] = kmeans_ord.labels_
column_df['gaussOne'] = gauss_one.predict(onehot)
column_df['gaussOrd'] = gauss_ord.predict(ordinal)
column_df['sum_pred'] = column_df.iloc[:, -4:-1].sum(axis=1)
print(column_df)
