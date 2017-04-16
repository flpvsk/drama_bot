const elasticsearch = require('elasticsearch');
const readline = require('readline');
const util = require('util');
const uniqBy = require('lodash/uniqBy');
const sortBy = require('lodash/sortBy');
const levenstein = require('./levenstein');

const telegram = require('telegram-bot-api');

const client = new elasticsearch.Client({
  host: 'localhost:9200',
  log: 'warning'
});

const makeSearch = (body, from, size) => {
  return {
    index: 'chatterbot-1',
    type: 'statement',
    body,
    from,
    size
  }
};

const makeAndQuery = (text) => {
  return {
    query: {
      match: {
        'in_response_to.text': {
          query: text,
          operator: 'and'
        }
      }
    }
  };
};


const makeOrQuery = (text) => {
  return {
    query: {
      match: {
        'in_response_to.text': {
          query: text,
          operator: 'or'
        }
      }
    }
  };
}


const hitsSort = (text) => {
  return (hit1) => {

    const distance = hit1._source.in_response_to.reduce((acc, v) => {
      let l = levenstein(text, v.text);
      if (l < acc) {
        return l;
      }
      return acc;
    }, 1000);

    return distance;
  };
};


const bot = new telegram({
  token: process.env.TELEGRAM_TOKEN,
  updates: {
    enabled: true
  }
});


bot.on('message', (msg) => {
  const text = msg.text;

  if (text === '/start') {
      bot.sendMessage({
        chat_id: msg.chat.id,
        text: (
          `I've been brought to this world to spread drama...\n\n` +
          `and congratulate Ale with his birthday.\n\n` +
          `What's up?`
        )
      });
      return;
  }

  client.search(makeSearch(makeAndQuery(text), 0, 100))
    .then((results) => {
      if (results.hits.total === 0) {
        return client.search(makeSearch(makeOrQuery(text), 0, 100));
      }
      return results;
    })
    .then((results) => {
      const total = results.hits.total;
      if (total === 0) {
        bot.sendMessage({
          chat_id: msg.chat.id,
          text: `I don't even know what to say..`
        });
        return;
      }

      let hits = uniqBy(results.hits.hits, (hit) => {
        return hit._source.text;
      });

      hits = sortBy(hits, hitsSort(text));

      const random = Math.floor(Math.random() * (hits.length / 2));
      bot.sendMessage({
        chat_id: msg.chat.id,
        text: `${hits[random]._source.text}`
      });
    });
});
