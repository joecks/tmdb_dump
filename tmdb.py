import sys
import gzip
import requests
import json
import os
import time
import random
from requests.exceptions import ConnectionError
from concurrent.futures import ThreadPoolExecutor



def create_session(api_key):
    s = requests.Session()
    s.params={'api_key': api_key}
    return s

# you'll need to have an API key for TMDB
# to run these examples,
# run export TMDB_API_KEY=<YourAPIKey>
tmdb_api_keys = os.environ["TMDB_API_KEY"].split(',')
# Setup tmdb as its own session, caching requests
# (we only want to cache tmdb, not elasticsearch)
# Get your TMDB API key from
#  https://www.themoviedb.org/documentation/api
# then in shell do export TMDB_API_KEY=<Your Key>,<Next Key>
tmdb_api_sessions = list(map(lambda x: create_session(x), tmdb_api_keys ))

TMDB_SLEEP_TIME_SECS=1
CHUNK_SIZE=1000

class TaintedDataException(RuntimeError):
    pass

def tmdb_api() -> requests.Session:
    return random.choice(tmdb_api_sessions)

def get_movie(language, movieId):
    try:
        httpResp = tmdb_api().get(f"https://api.themoviedb.org/3/movie/%s?language={language}" % movieId)
        print(movieId)
        if httpResp.status_code == 429:
            print(httpResp.text)
            raise TaintedDataException
        if httpResp.status_code <= 300:
            movie = json.loads(httpResp.text)
            getCastAndCrew(movieId, movie)
            return (str(movieId), movie)
        elif httpResp.status_code == 404:
            print(f'{movieId} is missing')
            # missing += 1
        else:
            print("Error %s for %s" % (httpResp.status_code, movieId))
    except ConnectionError as e:
        print(e)
    except Exception as e:
        print(e)

def getCastAndCrew(movieId, movie):
    httpResp = tmdb_api().get("https://api.themoviedb.org/3/movie/%s/credits" % movieId)
    credits = json.loads(httpResp.text) #C
    try:
        crew = credits['crew']
        directors = []
        for crewMember in crew: #D
            if crewMember['job'] == 'Director':
                directors.append(crewMember)
    except KeyError as e:
        print(e)
        print(credits)
    movie['cast'] = credits['cast']
    movie['directors'] = directors

def extract(language, startChunk=0, movieIds=[], chunkSize=5000, existing_movies={}):
    movieDict = {}
    missing = 0
    local = 0
    fetched = 0
    pending_ids = []

    for idx, movieId in enumerate(movieIds):
        # Read ahead to the current chunk
        if movieId < (startChunk * chunkSize):
            continue
        # Try an existing tmdb.json
        if str(movieId) in existing_movies:
            movieDict[str(movieId)] = existing_movies[str(movieId)]
            local += 1
        else: # Go to the API
            if len(pending_ids) >= len(tmdb_api_sessions) or (len(pending_ids) > 0 and (movieId % chunkSize == (chunkSize - 1))):
                with ThreadPoolExecutor(max_workers=len(pending_ids)) as executor:
                    futures = [executor.submit(get_movie, language, id) for id in pending_ids]
                    results = [future.result() for future in futures]
                    for r in results:
                        if r is not None:
                            (id, movie) = r
                            movieDict[id] = movie
                    pending_ids = []
            else:
                pending_ids.append(movieId)

        if (movieId % chunkSize == (chunkSize - 1)):
            print("DONE CHUNK, LAST ID CHECKED %s" % movieId)
            yield movieDict
            movieDict = {}
            missing = 0
            local = 0
            fetched = 0
    yield movieDict


def lastMovieId(url='https://api.themoviedb.org/3/movie/latest'):
    print("GET ID")
    httpResp = tmdb_api().get(url)
    jsonResponse = json.loads(httpResp.text)
    print("Latest movie is %s (%s)" % (jsonResponse['id'], jsonResponse['title']))
    return int(jsonResponse['id'])

def read_chunk(language, chunk_id):
    with gzip.GzipFile(f'chunks_{language}/tmdb.%s.json.gz' % chunk_id) as f:
        return json.loads(f.read().decode('utf-8'))

def write_chunk(language, chunk_id, movie_dict):
    with gzip.GzipFile(f'chunks_{language}/tmdb.%s.json.gz' % chunk_id, 'w') as f:
        f.write(json.dumps(movie_dict).encode('utf-8'))

def continueChunks(language, lastId):
    # allTmdb = {}
    existing_movies = {}
    atChunk = 0
    try:
        with open('tmdb.json') as f:
            print("Using Existing tmdb.json")
            existing_movies = json.load(f)
    except FileNotFoundError:
        pass
    for i in range(0, int(lastId / CHUNK_SIZE) + 1):
        try:
            movies = read_chunk(language, i)
            # allTmdb = {**movies, **allTmdb}
        except IOError:
            print("Starting at chunk %s; total %s" % (i, int(lastId/CHUNK_SIZE)))
            atChunk = i
            break

    for idx, movieDict in enumerate(extract(language, startChunk=atChunk, existing_movies=existing_movies,
                                            chunkSize=CHUNK_SIZE, movieIds=range(lastId))):
        currChunk = idx + atChunk
        write_chunk(language, currChunk, movieDict)
    return True


def ensure_dir(file_path):
    directory = os.path.dirname(file_path)
    if not os.path.exists(directory):
        os.makedirs(directory)


if __name__ == "__main__":
    len_args = len(sys.argv)
    if (len_args < 2):
        print(f"language param needed! (i.e. python3 {sys.argv[0]} en-US)")
        exit()
    language = str(sys.argv[1])
    ensure_dir(f"chunks_{language}/")
    lastId = lastMovieId()
    while True:
        try:
            if (continueChunks(lastId=lastId, language=language)):
                print("YOU HAVE WON THE GAME!")
        except TaintedDataException:
            print("Chunk tainted, trying again")
            time.sleep(TMDB_SLEEP_TIME_SECS*2)
            continue
