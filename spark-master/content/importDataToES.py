from elasticsearch import Elasticsearch
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql import Row
import json

def string_to_list(string):
    if string == 'N/A':
        return []
    return [s.strip() for s in string.split(',')]

def convert_votes(vote):
    return int(vote.replace(',', ''))

def convert_boxoffice(boxoffice):
    if boxoffice.startswith('$'):
        return int(boxoffice[1:].replace(',', ''))
    return None

def convert_runtime(runtime):
    if runtime.endswith(' min'):
        return int(runtime[:-4])
    return None

def convert_year(year):
    today = 2023
    if len(year) == 4:
        return [year]
    elif len(year) == 5:
        start_year = int(year[:4])
        return [str(y) for y in range(start_year, today + 1)]
    elif len(year) == 9:
        start_year, end_year = [int(y) for y in year.split('–')[:2]]
        return [str(y) for y in range(start_year, end_year + 1)]
    return []

def es_transform(data):
    result = dict()
    for key in data.keys():
        result[key] = None
        if key in ['Genre', 'Director', 'Writer', 'Actors', 'Language', 'Country']:
            try:
                result[key] = string_to_list(data[key])
            except:
                pass
        elif key == 'Year':
            try:
                result[key] = convert_year(data[key])
            except:
                pass
        elif key in ['Metascore', 'imdbRating']:
            try:
                result[key] = float(data[key])
            except:
                pass
        elif key == 'imdbVotes':
            try:
                result[key] = convert_votes(data[key])
            except:
                pass
        elif key == 'Runtime':
            try:
                result[key] = convert_runtime(data[key])
            except:
                pass
        elif key == 'totalSeasons':
            try:
                result[key] = int(data[key])
            except:
                pass
        elif key == 'BoxOffice':
            try:
                result[key] = convert_boxoffice(data[key])
            except:
                pass
        else:
            try:
                raw = data[key]
                if raw == 'N/A':
                    result[key] = None
                else:
                    result[key] = raw
            except:
                pass
    return result

if __name__ == '__main__':
    RAW_DATA_DIRECTORY = '/user/root/rawdata/'
    ES = Elasticsearch('http://elasticsearch:9200')
    INDEX = 'film'
    
    spark = SparkSession.builder.master("local[4]").appName("Transform And Save").getOrCreate()
    sc = spark.sparkContext
    sqlContext = SQLContext(sc)
    rdd = sc.wholeTextFiles('hdfs://namenode:9000' + RAW_DATA_DIRECTORY + '*.json')
    
    def insert(x):
        # print(type(x))
        ES.index(index=INDEX, id=x['imdbID'], document=x)
    
    #ES transform and save in index 'film'
    es_transform_rdd = rdd.values() \
                        .map(lambda x: json.loads(x)) \
                        .map(lambda x: es_transform(x))
    film_list = es_transform_rdd.collect()
    for x in film_list:
        insert(x)
    sc.stop()
    spark.stop()