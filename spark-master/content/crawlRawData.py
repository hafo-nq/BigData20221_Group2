import requests
import json
from tqdm import tqdm
from hdfs import InsecureClient
from elasticsearch import Elasticsearch
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql import Row

def hadoop_transform(load_dict):
    transform_dict = dict()
    transform_dict = {
        'imdbID': load_dict.get('imdbID'),
        'Year': load_dict.get('Year'),
        'Genre': load_dict.get('Genre'),
        'Director': load_dict.get('Director'),
        'Writer': load_dict.get('Writer'),
        'Actors': load_dict.get('Actors'),
        'Language': load_dict.get('Language'),
        'Country': load_dict.get('Country'),
        'Type': load_dict.get('Type'),
        'imdbRating': load_dict.get('imdbRating'),
        'Runtime': load_dict.get('Runtime'),
        'BoxOffice': load_dict.get('BoxOffice')
    }
    return Row(**transform_dict)

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
    URL = "https://movie-database-alternative.p.rapidapi.com/"
    HEADERS = {
        'x-rapidapi-host': "movie-database-alternative.p.rapidapi.com",
        'x-rapidapi-key': "116b330202mshac573bc7e1c9d61p1618c7jsn8f077d38c0d6"
    }
    CLIENT_HDFS = InsecureClient('http://namenode:9870', user='root')
    ES = Elasticsearch('http://elasticsearch:9200')
    INDEX = 'film'
    TEMP_DATA_DIRECTORY = '/user/root/tempdata/'
    RAW_DATA_DIRECTORY = '/user/root/rawdata/'
    PROCESSED_DATA_DIRECTORY = '/user/root/dataframe/'

    with open('/content/keyword.txt', 'r') as f:
        keywords = f.read().splitlines()
    print("Number of keywords: %d"%(len(keywords)))
    print(keywords)

    count = 0
    for i, keyword in enumerate(keywords[:]):
        search_films_querystring = {
            "s": keyword,
            "r": "json",
            "page": "1"
        }
        try:
            search_films_response = requests.request(
                method="GET",
                url=URL,
                headers=HEADERS,
                params=search_films_querystring
            )
            if search_films_response.status_code == 200:
                search_films_response_data = search_films_response.json()
                if search_films_response_data['Response'] == 'True':
                    totalResults = int(search_films_response_data['totalResults'])
                    print('\tNumber of films with keyword (%s): %d'%(keyword, totalResults))
                    numOfPages = (totalResults + 9) // 10
                    for j in tqdm(range(1, numOfPages + 1), desc="Crawling Pages...", ncols=100):
                        search_films_querystring = {
                            "s": keyword,
                            "r": "json",
                            "page": str(j)
                        }
                        try:
                            search_films_response = requests.request(
                                method="GET",
                                url=URL,
                                headers=HEADERS,
                                params=search_films_querystring
                            )
                            if search_films_response.status_code == 200:
                                search_films_response_data = search_films_response.json()
                                if search_films_response_data['Response'] == 'True':
                                    id_film_list = [film['imdbID'] for film in search_films_response_data['Search']]
                                    for id_film in id_film_list:
                                        try:
                                            film_details_querystring = {
                                                "i": id_film,
                                                "r": "json"
                                            }
                                            film_details_response = requests.request(
                                                method="GET",
                                                url=URL,
                                                headers=HEADERS,
                                                params=film_details_querystring
                                            )
                                            if film_details_response.status_code == 200:
                                                film_details_response_data = film_details_response.json()
                                                if film_details_response_data['Response'] == 'True':
                                                    try:
                                                        file_name =  film_details_response_data['imdbID'] + '.json'
                                                        with CLIENT_HDFS.write(RAW_DATA_DIRECTORY + file_name, encoding='utf-8') as writer:
                                                            json.dump(film_details_response_data, writer)
                                                        count += 1
                                                        with CLIENT_HDFS.write(TEMP_DATA_DIRECTORY + file_name, encoding='utf-8') as writer:
                                                            json.dump(film_details_response_data, writer)
                                                        transform_result = es_transform(film_details_response_data)
                                                        ES.index(index=INDEX, id=transform_result['imdbID'], document=transform_result)
                                                    except Exception as e:
                                                        # print(e)
                                                        pass
                                        except Exception as e:
                                            print("\nGet error with %s"%(id_film))
                                            # print(e)
                                            
                        except Exception as e:
                            print("\nGet error with keyword %s - page %s"%(keyword, str(j)))
                            # print(e)
                                
        except Exception as e:
            print("\nGet error with keyword %s"%(keyword))
            # print(e)
            
        print("%d/%d"%(i+1, len(keywords)))
    print("Crawl %d films"%(count))

    spark = SparkSession.builder.master("local[*]").appName("Crawl and Transform").getOrCreate()
    sc = spark.sparkContext
    
    rdd = sc.wholeTextFiles('hdfs://namenode:9000' + TEMP_DATA_DIRECTORY + '*.json')

    transform_rdd = rdd.values() \
                    .map(lambda x: json.loads(x)) \
                    .map(lambda x: hadoop_transform(x))

    df = spark.createDataFrame(transform_rdd)
    df.write.save('hdfs://namenode:9000' + PROCESSED_DATA_DIRECTORY, format='parquet', mode='append')
    # CLIENT_HDFS.delete(TEMP_DATA_DIRECTORY, recursive=True)
    sc.stop()
    spark.stop()