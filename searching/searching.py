import lucene
from java.nio.file import Paths
from org.apache.lucene.analysis.core import KeywordAnalyzer
from org.apache.lucene.document import Document, Field, StoredField, StringField, TextField
from org.apache.lucene.store import SimpleFSDirectory
from org.apache.lucene.search import IndexSearcher
from org.apache.lucene.index import DirectoryReader
from org.apache.lucene.queryparser.classic import QueryParser
import csv
import time
import string

INDEX = "/data/data/indexes/index_test"

lucene.initVM(vmargs=['-Djava.awt.headless=true'])

directory = SimpleFSDirectory(Paths.get(INDEX))
searcher = IndexSearcher(DirectoryReader.open(directory))
analyzer = KeywordAnalyzer()

user_input = ""
cont = ""

while True:
    print("Searching options:")
    print("1 == movie name")
    print("2 == director")
    print("3 == producer ")
    print("4 == screenplay")
    print("q == quit")
    
    user_input = input("Your option: ")
    
    if user_input == "q":
        break
    
    user_query = input("Input query:")
    
    if user_input == "1":
        searching = "title"
    elif user_input == "2":
        searching = "director"
    elif user_input == "3":
        searching = "producer"
    elif user_input == "4":
        searching = "screenplay"

    query = QueryParser(searching, analyzer).parse(user_query)
    scoreDocs = searcher.search(query, 50).scoreDocs

    print("Number of found documents: ", len(scoreDocs))

    for scoreDoc in scoreDocs:
        doc = searcher.doc(scoreDoc.doc)
        print("*********************")
        print("Movie Title: ", doc.get("title"))
        print("Director: ", doc.get("director"))
        print("Producer: ", doc.get("producer"))
        print("Screenplay: ", doc.get("screenplay"))
        print("Cast: ", doc.get("movie_cast"))
        print("*********************")
    
    cont = input("continue? (y/n)")
    if cont == "n":
        break
     
