from sklearn.cluster import KMeans
from sklearn.mixture import GaussianMixture
from sklearn.cluster import DBSCAN
from .MetadataExtractor import Extractor
from .DataCleaner import cleaner, limited_cleaner
from joblib import dump, load

def predictor(extractor, name):
    data        = cleaner(extractor).drop_duplicates(subset=['name'])
    data_to_fit = data.drop(['table', 'name'],axis=1)
    kmeans      = KMeans(n_clusters=2, random_state=0).fit(data_to_fit)
    gauss       = GaussianMixture(n_components=2, random_state=0).fit(data_to_fit)
    dbscan      = DBSCAN(min_samples=5).fit(data_to_fit)
    #dump(gauss, f'gauss_{name}.joblib')

    kmeans_predict  = kmeans.predict(data_to_fit)
    gauss_predict   = gauss.predict(data_to_fit)

    data['kmeans']  = kmeans_predict
    data['gauss']   = gauss_predict
    data['scan']    = dbscan.labels_

    return data

def run_analysis():

    pagila_data   = Extractor('postgresql', 'postgres', 'Viteco2020', 'pagila')
    sportsdb_data = Extractor('postgresql', 'postgres', 'Viteco2020', 'sportsdb')

    pagila      = predictor(pagila_data, 'pagila')
    sports    = predictor(sportsdb_data, 'sportsdb')

    pagila_agree    = sum(pagila['kmeans'] != pagila['gauss'])/len(pagila)
    sportsdb_agree  = sum(sports['kmeans'] == sports['gauss'])/len(sports)

    print(f'Pagila total of class 1 in K-means: {sum(pagila.kmeans)} and class 0: {len(pagila)-sum(pagila.kmeans)}')
    print(f'Pagila total of class 1 in GMM: {sum(pagila.gauss)} and class 0: {len(pagila)-sum(pagila.gauss)}')
    print(f'Sportsdb total of class 1 in K-means: {sum(sports.kmeans)} and class 0: {len(sports)-sum(sports.kmeans)}')
    print(f'Sportsdb total of class 1 in GMM: {sum(sports.gauss)} and class 0: {len(sports)-sum(sports.gauss)}')
    print(pagila_agree, sportsdb_agree)
    print(pagila[(pagila.kmeans == 1) & (pagila.gauss == 0)].drop(['name', 'table', 'gauss', 'kmeans', 'length', 'scan'], axis=1).sum(skipna=True))
    print(sports[(sports.kmeans == 0) & (sports.gauss == 0)].drop(['name', 'table', 'gauss', 'kmeans', 'length', 'scan'], axis=1).sum(skipna=True))
    print(pagila[(pagila.kmeans == 1) & (pagila.gauss == 1)])
    print(pagila[(pagila.kmeans == 0) & (pagila.gauss == 0)])
    print(sports[(sports.kmeans == 1) & (sports.gauss == 1)])

def load_predictor(name, extractor, tables):
    #gauss = load(f'gauss_{name}.joblib')
    data        = limited_cleaner(extractor, tables)
    data_to_fit = data.drop(['table', 'name'],axis=1)
    kmeans      = KMeans(n_clusters=2, random_state=0).fit(data_to_fit)
    gauss       = GaussianMixture(n_components=2, random_state=0).fit(data_to_fit)
    kmeans_predict  = kmeans.predict(data_to_fit)
    gauss_predict   = gauss.predict(data_to_fit)

    data['kmeans']  = kmeans_predict
    data['gauss']   = gauss_predict

    return data
