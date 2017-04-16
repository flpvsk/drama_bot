import sys
import pymongo

from chatterbot import ChatBot
from chatterbot.trainers import Trainer
from chatterbot.conversation import Response, Statement

import logging

# Uncomment the following line to enable verbose logging
logging.basicConfig(level=logging.WARN)
logger = logging.getLogger(__name__)

client = pymongo.MongoClient('localhost', 27017)
db = client['dialogs']

chatbot = ChatBot(
  'Drama Bot',
  storage_adapter='elastic_storage.ElasticAdapter',
  logic_adapters=['chatterbot.logic.BestMatch'],
  database='chatterbot',
  database_uri='mongodb://localhost:27017/'
)

class MoviesTrainer(Trainer):

  def get_or_create(self, text, ext_id):
    statement = self.storage.find_by_id(ext_id)

    if not statement:
      statement = Statement(text)
      statement.add_extra_data('ext_id', ext_id)

    return statement

  def train(self):
    conv_count = 0
    for conv in db.conversations.find():
      line_ids = conv['lines']
      history = []
      conv_count += 1

      for line_id in line_ids:
        line = db.lines.find_one({'ext_id': line_id})
        statement = self.get_or_create(line['text'], line_id)

        if history:
          statement.add_response(Response(history[-1].text))

        history.append(statement)
        self.storage.update(statement)

      if conv_count % 100 == 0:
        logger.warn('{0} convs processed\r'.format(conv_count))

    logger.warn('{0} convs processed\n'.format(conv_count))




chatbot.set_trainer(MoviesTrainer)
chatbot.train()
