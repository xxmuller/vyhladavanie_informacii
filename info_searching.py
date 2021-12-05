import json

def get_offset(movie_name):
    with open('offsets.json') as json_file:
        movie_offsets = json.load(json_file)

    if movie_name in movie_offsets:
        return movie_offsets[movie_name]
    else:
        return None


def search_movie_info(offset):
    movie_info = []

    with open('movies_data.txt', mode='r', encoding='utf-8') as movies_info_file:
        movies_info_file.seek(offset)
        
        line = movies_info_file.readline()

        while line != "\n":
            movie_info.append(line[:-1])
            line = movies_info_file.readline()

    return movie_info

def print_movie_info(movie_info):
    for info in movie_info:
        print(info)

movie_name = input("input movie name: ")
offset = get_offset(movie_name.lower())

if offset != None:
    movie_info = search_movie_info(offset)
    print_movie_info(movie_info)
else:
    print("Movie name incorrect or isnt in database!")