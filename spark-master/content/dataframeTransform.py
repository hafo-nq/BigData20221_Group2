import json
from hdfs import InsecureClient
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

if __name__ == '__main__':
    RAW_DATA_DIRECTORY = '/user/root/rawdata/'
    PROCESSED_DATA_DIRECTORY = '/user/root/dataframe/'
    spark = SparkSession.builder.master("local[*]").appName("Tranform Dataframe").getOrCreate()
    sc = spark.sparkContext
    sqlContext = SQLContext(sc)
    rdd = sc.wholeTextFiles('hdfs://namenode:9000' + RAW_DATA_DIRECTORY + '*.json')

    transform_rdd = rdd.values() \
                    .map(lambda x: json.loads(x)) \
                    .map(lambda x: hadoop_transform(x))

    df = spark.createDataFrame(transform_rdd)
    df.write.save('hdfs://namenode:9000' + PROCESSED_DATA_DIRECTORY, format='parquet', mode='append')
    # CLIENT_HDFS.delete(RAW_DATA_DIRECTORY, recursive=True)
    sc.stop()
    spark.stop()