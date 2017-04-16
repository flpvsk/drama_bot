import datetime
from chatterbot import ChatBot
from chatterbot.trainers import ListTrainer

import logging


# Uncomment the following line to enable verbose logging
# logging.basicConfig(level=logging.INFO)

chatbot = ChatBot(
  'Drama Bot',
  storage_adapter='elastic_storage.ElasticAdapter',
  preprocessors=['chatterbot.preprocessors.clean_whitespace'],
  logic_adapters=['chatterbot.logic.BestMatch'],
  input_adapter='chatterbot.input.TerminalAdapter',
  output_adapter='chatterbot.output.TerminalAdapter',
  read_only=True
)

print 'Hi, Ale..'
while True:
  try:
    start = datetime.datetime.utcnow()
    response = chatbot.get_response(None)
    end = datetime.datetime.utcnow()

    print 'Time to respond', str(end - start)

  # Press ctrl-c or ctrl-d on the keyboard to exit
  except (KeyboardInterrupt, EOFError, SystemExit):
    break
