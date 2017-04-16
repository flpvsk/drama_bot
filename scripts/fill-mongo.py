import sys
import json
import codecs
from collections import namedtuple
from os import path
import pymongo

client = pymongo.MongoClient('localhost', 27017)
db = client['dialogs']


def drop_create(col_name):
  col = db[col_name]
  col.drop()
  return db[col_name]


def drop_create_with_index(col_name):
  col = drop_create(col_name)
  col.create_index([('ext_id', pymongo.ASCENDING)], unique=True)
  return col

Movie = namedtuple('Movie', [
  'ext_id',
  'title',
  'year',
  'imdb_raiting',
  'imdb_votes_count',
  'genres'
])

Line = namedtuple('Line', [
  'ext_id',
  'character_id',
  'movie_id',
  'character_name',
  'text'
])

Conversation = namedtuple('Conversations', [
  'character_1_id',
  'character_2_id',
  'movie_id',
  'lines'
])

def run():
  corpus_dir = None
  i = 0
  while not corpus_dir and i < len(sys.argv):
    maybe_dir = sys.argv[i]
    i += 1
    if path.isdir(maybe_dir):
      corpus_dir  = path.abspath(maybe_dir)

  if not corpus_dir:
    print 'Dir not found'
    return

  movies_csv = path.join(corpus_dir, 'movie_titles_metadata.txt')
  lines_csv = path.join(corpus_dir, 'movie_lines.txt')
  conversations_csv = path.join(corpus_dir, 'movie_conversations.txt')

  with codecs.open(movies_csv, 'r', 'cp1251') as movies_file:
    movies_col = drop_create_with_index('movies')
    movies_count = 0

    for line in movies_file:
      splited = line.split(' +++$+++ ')
      splited[-1] = json.loads(splited[-1].replace("'", "\""))
      movie = Movie(*splited)
      movies_col.insert_one(movie._asdict())

      movies_count += 1
      if (movies_count % 10) == 0:
        sys.stdout.write('Inserted {0} movies\r'.format(movies_count))

    sys.stdout.write('Inserted {0} movies\n'.format(movies_count))

  with codecs.open(lines_csv, 'r', 'cp1251') as lines_file:
    lines_col = drop_create_with_index('lines')
    lines_count = 0
    lines_grouped = []

    for line in lines_file:
      splited = line.split(' +++$+++ ')
      line_tuple = Line(*splited)
      lines_grouped.append(line_tuple._asdict())

      lines_count += 1
      if (lines_count % 1000) == 0:
        lines_col.insert_many(lines_grouped)
        lines_grouped = []
        sys.stdout.write('Inserted {0} lines\r'.format(lines_count))

    lines_col.insert_many(lines_grouped)
    sys.stdout.write('Inserted {0} lines\n'.format(lines_count))


  with codecs.open(conversations_csv, 'r', 'cp1251') as conversations_file:
    conversations_col = drop_create('conversations')
    conversations_count = 0
    conversations_grouped = []

    for line in conversations_file:
      splited = line.split(' +++$+++ ')
      splited[-1] = json.loads(splited[-1].replace("'", "\""))
      conv = Conversation(*splited)
      conversations_grouped.append(conv._asdict())

      conversations_count += 1
      if (conversations_count % 1000) == 0:
        conversations_col.insert_many(conversations_grouped)
        conversations_grouped = []
        sys.stdout.write(
          'Inserted {0} conversations\r'.format(conversations_count)
        )

    conversations_col.insert_many(conversations_grouped)
    sys.stdout.write(
      'Inserted {0} conversations\n'.format(conversations_count)
    )

if __name__ == '__main__':
  run()
