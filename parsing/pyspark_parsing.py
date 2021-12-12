import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import UserDefinedFunction, col
from pyspark.sql.types import *

import re
import time


IN_PATH = 'datasets/raw_data/'
OUT_PATH = './datasets/parsed_data/'

# IN_FILE = 'wiki_dump_500.xml'
# IN_FILE = 'little_dump.xml'
IN_FILE = 'complete_dump/complete_dump.xml'
OUT_FILE = 'output.csv'

spark = SparkSession.builder.master("spark://sparkmaster:7077").appName("parsing").getOrCreate()
sc = spark.sparkContext

root = 'mediawiki'
row = 'page'

schema = StructType([StructField('id', StringType(), True),
                        StructField('title', StringType(), True),
                        StructField('revision', StructType([StructField('text', StringType(), True)]))])

df = spark.read.format('com.databricks.spark.xml').options(rootTag=root).options(rowTag=row).schema(schema).load(IN_FILE)

df = df.withColumn("revision", col("revision").cast("String"))
df = df.withColumnRenamed("revision", "text")

def clean_data(data):

    data = data.lower()
    data = re.sub(r'[\"\[\]]', ' ', data)
    data = re.sub(r' +', ' ', data)
    data = re.sub(r'[^a-z0-9|()*=]+', ' ', data)

    return data


def extract_infobox_data(page):
    infobox_data = []
    result = 'production info | '
    
    movie_data = [
    'director',
    'producer',
    'screenplay',
    'writer',
    'story', 
    'music', 
    'cinematography', 
    'editing'
    ]

    infobox_data.append(re.findall('(?i)name *=.*?(?=\|)\|', page))
    
    for item in movie_data:
        infobox_data.append(re.findall('(?i)'+item+' *=.*?(?=\])\]', page))

    for item in infobox_data:
        result += ''.join(item)
        
    return result


def get_name(line):
        name = re.match(r"^.*\[(.*)\]\].*$", line)
        return name.group(1)
    

def get_infobox_data(page):
    index = 0
    text_lines = page.splitlines()
    
    buffer = []

    for line in text_lines:
        line = line.strip()
            
        if re.search('(?i){{Infobox *film', line):
            while text_lines[index] and  text_lines[index] != " ":

                if text_lines[index]:
                    if text_lines[index][0] == "'":
                        break
                        
                buffer.append(text_lines[index])
                
                if (index+1) >= len(text_lines):
                    break
                index += 1
        index += 1
        
    result = ' '.join(buffer).replace('\n', ' ')

    infobox_data = extract_infobox_data(result)
    return infobox_data


def get_movie_cast(page):
    cast = []
    result = 'movie cast |'
    
    page = re.sub('[^A-Za-z0-9]+', ' ', page)
    cast.append(re.findall('[A-Z][A-Za-z]* [A-Z][A-Za-z]* as [A-Z][A-Za-z]* [A-Z][A-Za-z]*', page))

    for item in cast:
        result += ''.join(item)
        
    return result


def is_movie_page(page):
    if re.search('(?i){{Infobox *film', str(page)):
        return True
    return False


def parse_wiki_page(page):
    if is_movie_page(page):
        movie_info = []
        
        infobox_data = get_infobox_data(page)
        infobox_data = clean_data(infobox_data)
        
        cast = get_movie_cast(page)
        
        movie_info.append(infobox_data)
        movie_info.append(cast)
        
        return movie_info

    return None


def format_time(seconds):
    min, sec = divmod(seconds, 60)
    hour, min = divmod(min, 60)
    
    return "%d:%02d:%02d" % (hour, min, sec)


start = time.time()

my_udf = UserDefinedFunction(parse_wiki_page, StringType())

df_new = df.withColumn('text', my_udf('text'))
df_new = df_new.na.drop()

try:
    df_new.repartition(10).write.format('com.databricks.spark.csv').mode("overwrite")     .save(OUT_PATH + "output", header = 'true')
        
except Exception as e:
    print(e)
    exit(1)
    
end = time.time()
print('Elapsed time: ', format_time(end - start))





