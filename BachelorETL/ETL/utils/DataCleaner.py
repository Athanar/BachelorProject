import pandas as pd

def cleaner(extractor):
    tables = extractor.get_all_tables()
    df = extractor.get_column_list(tables)
    df['default'] = df['default'].fillna('0')
    df['default'][df['default'].str.contains('next', na=False)] = '1'
    df['default'][df['default'].isin(['0', '1']) == False] = '2'
    df = pd.concat([df, pd.get_dummies(df['default'])], axis=1)
    df = pd.concat([df, pd.get_dummies(df['type'])], axis=1)
    df.drop(['default', 'type', 'autoincrement', 'comment'],axis=1, inplace=True)

    return df

def limited_cleaner(extractor, tables):
    df = extractor.get_some_table_columns(tables)
    df['default'] = df['default'].fillna('0')
    df['default'][df['default'].str.contains('next', na=False)] = '1'
    df['default'][df['default'].isin(['0', '1']) == False] = '2'
    df = pd.concat([df, pd.get_dummies(df['default'])], axis=1)
    df = pd.concat([df, pd.get_dummies(df['type'])], axis=1)
    df.drop(['default', 'type', 'autoincrement', 'comment'],axis=1, inplace=True)

    return df