#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import UserDefinedFunction, col
from pyspark.sql.types import *
import pyspark_parser

import re
import time


# In[2]:


IN_PATH = 'datasets/raw_data/'
OUT_PATH = './datasets/parsed_data/'

IN_FILE = 'wiki_dump_500.xml'
# IN_FILE = 'little_dump.xml'
# IN_FILE = 'enwiki-20211201-pages-articles.xml/.xml'
OUT_FILE = 'output.csv'


# In[3]:


# spark = SparkSession.builder.appName("test").getOrCreate()

spark = SparkSession.builder.master("spark://sparkmaster:7077").appName("parsing").getOrCreate()
sc = spark.sparkContext


# In[4]:


root = 'mediawiki'
row = 'page'

schema = StructType([StructField('id', StringType(), True),
                        StructField('title', StringType(), True),
                        StructField('revision', StructType([StructField('text', StringType(), True)]))])

df = spark.read.format('com.databricks.spark.xml')    .options(rootTag=root)    .options(rowTag=row)    .schema(schema)    .load(IN_PATH + IN_FILE)


# In[5]:


df = df.withColumn("revision", col("revision").cast("String"))
df = df.withColumnRenamed("revision", "text")


# In[6]:


def clean_data(data):

    data = data.lower()
    data = re.sub(r'[\"\[\]]', ' ', data)
    data = re.sub(r' +', ' ', data)
    data = re.sub(r'[^a-z0-9|()*=]+', ' ', data)

    return data


# In[7]:


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


# In[8]:


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
                
                index += 1
        index += 1
        
    result = ' '.join(buffer).replace('\n', ' ')

    infobox_data = extract_infobox_data(result)
    return infobox_data


# In[9]:


def get_movie_cast(page):
    cast = []
    result = 'movie cast |'
    
    page = re.sub('[^A-Za-z0-9]+', ' ', page)
    cast.append(re.findall('[A-Z][A-Za-z]* [A-Z][A-Za-z]* as [A-Z][A-Za-z]* [A-Z][A-Za-z]*', page))

    for item in cast:
        result += ''.join(item)
        
    return result


# In[10]:


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


# In[11]:


def format_time(time):
    hour = int(time / (60 * 60))
    mins = int((time % (60 * 60)) / 60)
    secs = time % 60
    
    return "{}:{:>02}:{:>05.2f}".format(hour, mins, secs)


# In[12]:


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


# In[ ]:


df_new.show(20)


# In[ ]:




