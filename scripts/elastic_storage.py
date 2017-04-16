from log import log_with
from random import randint
import logging
from elasticsearch import Elasticsearch
from collections import defaultdict
from chatterbot.conversation import Response, Statement
from chatterbot.storage.storage_adapter import StorageAdapter

logger = logging.getLogger(__name__)

BODY = {
  'mappings': {
    'statement': {
      'properties': {
        'text': {'type': 'text'},
        'created_at': {'type': 'date'},
        'extra_data': {'type': 'object'},
        'in_reponse_to': {
          'type': 'nested',
          'properties': {
            'text': {'type': 'text'}
          }
        }
      }
    }
  }
}


def create_statement(data):
  text = data['text']
  statement = Statement(text)
  statement.add_extra_data('ext_id', data['extra_data']['ext_id'])

  for response in data['in_response_to']:
    statement.add_response(Response(response['text']))

  return statement


class Query(object):

  @log_with(logger)
  def __init__(self, query={}):
    self.query = query

  def value(self):
    return self.query.copy()

  def raw(self, data):
    query = self.query.copy()

    query.update(data)

    return Query(query)

  def must(self, clause):
    query = self.query.copy()
    query['query']['bool']['must'].append(clause)
    return Query(query)


  def filter(self, clause):
    query = self.query.copy()
    query['query']['bool']['filter'].append(clause)
    return Query(query)


  def must_not(self, clause):
    query = self.query.copy()
    query['query']['bool']['must_not'].append(clause)
    return Query(query)


  @log_with(logger)
  def statement_text_equals(self, text):
    return self.must({
      'match': {'text': {'query': text, 'operator': 'and'}}
    })

  @log_with(logger)
  def statement_text_not_in(self, statements):
    print 'statement_text_not_in', statements
    query = self.query.copy()
    result = self
    for text in statements:
      result =self.must_not({
        'match': {'text': {'query': text, 'operator': 'and'}}
      })
    return result



class ElasticAdapter(StorageAdapter):
  def __init__(self, **kwargs):
    super(ElasticAdapter, self).__init__(**kwargs)
    self.db = Elasticsearch()
    self.db.indices.create(index='chatterbot-1', ignore=400, body=BODY)
    self.base_query = Query({
      'query': {
        'bool': {
          'must': [],
          'must_not': [],
          'filter': []
        }
      }
    });


  def _search(self, query, from_=0, size=10, terminate_after=None):
    result = self.db.search(
      index='chatterbot-1',
      doc_type='statement',
      body=query,
      terminate_after=terminate_after,
      from_=from_,
      size=size
    )

    if not result['hits']['total']:
      return

    return [
      create_statement(hit['_source'])
      for hit in result['hits']['hits']
    ];


  def find_by_id(self, id):
    data = self.db.get(
      index='chatterbot-1',
      doc_type='statement',
      id=id,
      ignore=[404]
    )

    if not data or not data['found']:
      return

    return create_statement(data['_source'])


  @log_with(logger)
  def count(self):
    result = self.db.count(index='chatterbot-1', doc_type='statement')
    return result['count']


  @log_with(logger)
  def get_random(self):
    count = min(self.count(), 10000)
    random_integer = randint(0, count - 1)

    logger.warn('r {}, {}'.format(count, random_integer))
    results = self._search({
        'query': {
          'match_all': {}
        }
      },
      from_=random_integer,
      size=1
    )

    if not results:
      return

    return results[0]


  @log_with(logger)
  def get_response_statements(self, statement):
    return self.filter(in_response_to=statement.text)


  @log_with(logger)
  def filter(self, **kwargs):

    if not kwargs:
      raise Exception('x')

    if 'in_response_to__contains' in kwargs:
      return self._search({
        'query': {
          'match': {
            'in_response_to.text': kwargs['in_response_to__contains']
          }
        }
      },
      size=500)

    if 'in_response_to' in kwargs:
      return self._search({
        'query': {
          'match': {
            'in_response_to.text': {
              'query': kwargs['in_response_to'],
              'operator': 'and'
            }
          }
        }
      },
      from_=randint(0, 30),
      size=500
    )

    results = self._search({
        'query': {
          'match': {
            'text': {
              'query': text,
              'operator': 'and'
            }
          }
        }
      },
      size=500
    )

    return []


  @log_with(logger)
  def find(self, text):
    results = self._search({
        'query': {
          'match': {
            'text': {
              'query': text,
              'operator': 'and'
            }
          }
        }
      },
      terminate_after=1,
      size=1
    )

    if results:
      return results[0]

    return


  @log_with(logger)
  def update(self, statement):
    data = statement.serialize()
    ext_id = statement.extra_data['ext_id']
    # print 'Writing statement', ext_id, data
    self.db.index(
      index='chatterbot-1',
      doc_type='statement',
      id=ext_id,
      body=data
    )
