from pyspark.sql import SparkSession
from pyspark.sql.functions import UserDefinedFunction
from pyspark.sql.types import ArrayType
from pyspark.sql.types import StringType

import re
import time

start = time.time()

spark = SparkSession.builder.master("spark://sparkmaster:7077").getOrCreate()
sc = spark.sparkContext

init_df = spark.read.format('xml').options(rowTag='page').load('/data/wiki_dump_500.xml')
df = init_df.selectExpr("title", "revision.text._VALUE as text", "redirect._title as redirect")

text_lines = []
movie_info = {}
cast = []
offsets = {}

film_crew = [
    "director",
    "producer",
    "screenplay",
    "writer",
    "story", 
    "music", 
    "cinematography", 
    "editing"
    ]


def init_movie_info():
    for x in film_crew:
        movie_info[x] = []


def is_movie_page(page):
    global text_lines
    text_lines = page.splitlines()

    for line in text_lines:
        line = line.replace(" ", "").lower()
            
        if "{{infoboxfilm" in line:
            return True
        
    return False


def get_name(line):
        name = re.match(r"^.*\[(.*)\]\].*$", line)
        return name.group(1)


def get_movie_info():

    init_movie_info()

    index = 0

    for line in text_lines:
        line = line.strip()
            
        if "{{infobox film" in line.lower():
            index += 1
            while text_lines[index] != " ":

                if text_lines[index][0] == "'":
                    break

                for member in film_crew:
                    line = text_lines[index].replace(" ", "").lower()

                    if "|" + member in line and "plainlist" not in line:
                        if "[[" not in text_lines[index]:
                            try:
                                name = re.match(r"^.*\= (.*).*$", text_lines[index])
                                movie_info[member].append(name.group(1))
                            except:
                                print(text_lines[index])
                                continue
                                
                        else:
                            movie_info[member].append(get_name(text_lines[index]))
                    
                    if "|" + member in line and "plainlist" in line:
                        i = index + 1 
                        while "}}" not in text_lines[i]:
                                
                            if "[[" not in text_lines[i]:
                                movie_info[member].append(text_lines[i][3:])
                                    
                            else:
                                movie_info[member].append(get_name(text_lines[i]))
                            i += 1
                index += 1
            return movie_info
        
        index += 1
    return


def get_cast():
    cast = []
    index = 0

    for line in text_lines:
        line = line.strip()
            
        if "==cast==" in line.lower() or "== cast ==" in line.lower():
            index += 1
            while text_lines[index] != " ":
                if text_lines[index][1] == '*':

                    actor = re.search(r"\*(.*?) as ", text_lines[index])
                    role = re.search(r" as (.*?)\,", text_lines[index])
                    
                    if role:
                        role = role.group(1)
                    else:
                        i = 0
                        line = text_lines[index].split()

                        for word in line:
                            if word == "as" and len(line) > i+1:
                                if line[i+1][0].isupper():
                                    role = line[i+1]
   
                                break
                            i += 1

                    if actor and role:
                        actor = actor.group(1)

                        if '[[' in actor:
                            actor = get_name(actor)
                            
                        role_check = role.split()
                        
                        if len(role_check) > 5:
                            role = role_check[0] + role_check[1]
                        cast.append(actor + " as " + role)
                            
                index += 1
            return cast

        index += 1
    return None


def write_to_file():
    with open('movies_data.txt', mode='a', encoding='utf-8') as txt_file:
        offset = txt_file.tell()

        offsets[self._values['title'].lower()] = offset

        txt_file.write("MOVIE NAME: " + self._values['title'] + "\n")

        for data in movie_info:
            for x in movie_info[data]:
                txt_file.write(data + " = " + x + '\n')

        txt_file.write("CAST" + '\n')
        for role in cast:
            txt_file.write(role + '\n')

        txt_file.write("\n")


def parse_xml_dump(page):
    if is_movie_page(page):
        
        get_movie_info()
        get_cast()
        write_to_file()

udf = UserDefinedFunction(lambda x: parse_xml_dump(x), ArrayType(StringType()))