from os import extsep
from typing import MutableSequence
import xml.sax
import re
import time
import json

class WikiXmlHandler(xml.sax.handler.ContentHandler):

    def __init__(self):
        xml.sax.handler.ContentHandler.__init__(self)
        self._buffer = None
        self._values = {}
        self._current_tag = None
        self._pages = []
        self.num_pages = 0
        self.end = False
        self.text_lines = []
        self.movie_counter = 0

        self.movie_info = {}
        self.cast = []

        self.offsets = {}

        self.film_crew = [
            "director",
            "producer",
            "screenplay",
            "writer",
            "story", 
            "music", 
            "cinematography", 
            "editing"
            ]

    def init_movie_info(self):
        for x in self.film_crew:
            self.movie_info[x] = []

    def characters(self, content):
        # Characters between opening and closing tags
        if self._current_tag:
            self._buffer.append(content)

    def startElement(self, name, attrs):
        # Opening tag of element
        if name in ('title', 'text'):
            self._current_tag = name
            self._buffer = []

    def endElement(self, name):
        # Closing tag of element
        if name == self._current_tag:
            self._values[name] = ' '.join(self._buffer)

        if name == 'page':
            if not self.is_movie_page():
                return

            # print(self._values['title'])

            self.get_movie_info()
            self.get_cast2()

            self.write_to_file()

            self.movie_counter += 1
    
    def get_cast2(self):
        self.cast = []
        self._values['text'] = re.sub('[^A-Za-z0-9]+', ' ', self._values['text'])
        self.cast.append(re.findall('[A-Z][A-Za-z]* [A-Z][A-Za-z]* as [A-Z][A-Za-z]* [A-Z][A-Za-z]*', self._values['text']))

        # self.cast.append(re.findall("[A-Z][A-Za-z]* [A-Z][A-Za-z]*.*?(?=\])* as [A-Z][A-Za-z]* [A-Z][A-Za-z]*", self._values['text']))

        # self.cast = self.cast[0].split(", ")
        i = 0
        for x in self.cast[0]:
            if i > 10:
                break
            print(x)
            i += 1
        # for i in range(0,5):
        #     print(self.cast[i])




    def is_movie_page(self):
        self.text_lines = self._values['text'].splitlines()

        for line in self.text_lines:
            # line = line.strip()
            line = line.replace(" ", "").lower()
            
            if "{{infoboxfilm" in line:
                return True
        
        return False

    def get_name(self, line):
        name = re.match(r"^.*\[(.*)\]\].*$", line)
        return name.group(1)

    def get_movie_info(self):

        self.init_movie_info()

        index = 0

        for line in self.text_lines:
            line = line.strip()
            
            if "{{infobox film" in line.lower():
                index += 1
                while self.text_lines[index] != " ":

                    if self.text_lines[index]:
                        if self.text_lines[index][0] == "'":
                            break

                    for member in self.film_crew:
                        line = self.text_lines[index].replace(" ", "").lower()

                        if "|" + member in line and "plainlist" not in line:
                
                            if "[[" not in self.text_lines[index]:
                                try:
                                    name = re.match(r"^.*\= (.*).*$", self.text_lines[index])
                                    self.movie_info[member].append(name.group(1))
                                except:
                                    continue
                                
                            else:
                                self.movie_info[member].append(self.get_name(self.text_lines[index]))
                    
                        if "|" + member in line and "plainlist" in line:

                            i = index + 1 
                            while "}}" not in self.text_lines[i]:
                                
                                if "[[" not in self.text_lines[i]:
                                    self.movie_info[member].append(self.text_lines[i][3:])
                                    
                                else:
                                    self.movie_info[member].append(self.get_name(self.text_lines[i]))

                                i += 1
                    index += 1

                return self.movie_info

            index += 1
        return

    def get_cast(self):
        self.cast = []
        index = 0

        for line in self.text_lines:
            line = line.strip()
            
            if "==cast==" in line.lower() or "== cast ==" in line.lower():
                index += 1
                while self.text_lines[index] != " ":
                    if self.text_lines[index][1] == '*':

                        actor = re.search(r"\*(.*?) as ", self.text_lines[index])

                        role = re.search(r" as (.*?)\,", self.text_lines[index])
                        if role:
                            role = role.group(1)
                        else:
                            i = 0
                            line = self.text_lines[index].split()
                            for word in line:
                                if word == "as" and len(line) > i+1:
                                    if line[i+1][0].isupper():
                                        role = line[i+1]
   
                                    break
                                i += 1

                        if actor and role:
                            actor = actor.group(1)

                            if '[[' in actor:
                                actor = self.get_name(actor)
                            
                            role_check = role.split()
                            if len(role_check) > 5:
                                role = role_check[0] + role_check[1]
                            self.cast.append(actor + " as " + role)
                            
                    index += 1
                return self.cast

            index += 1
        return None

    def write_to_file(self):
        with open('movies_data.txt', mode='a', encoding='utf-8') as txt_file:
            offset = txt_file.tell()

            self.offsets[self._values['title'].lower()] = offset

            txt_file.write("MOVIE NAME: " + self._values['title'] + "\n")

            for data in self.movie_info:
                for x in self.movie_info[data]:
                    txt_file.write(data + " = " + x + '\n')

            # txt_file.write("CAST" + '\n')
            # for role in self.cast:
            #     txt_file.write(role + '\n')

            txt_file.write("\n")


start = time.time()

# Object for handling xml
handler = WikiXmlHandler()

# Parsing object
parser = xml.sax.make_parser()
parser.setContentHandler(handler)

# data_path = input("Input path to .xml Wikipedia dump: ")
# data_path = "xml_subory/alien_wikipedia_page.xml"
data_path = "cryonics_in_fiction_category.xml"

open('movies_data.txt', mode='w', encoding='utf-8')

# Iteratively process file
for line in open(data_path, 'r', encoding='UTF-8'):
    parser.feed(line)

    if handler.end:
        break
    
with open("offsets.json", "w") as outfile:
    json.dump(handler.offsets, outfile)

print("Number of movies parsed: ", handler.movie_counter)
end = time.time()
print("Time elapsed : ", end - start)