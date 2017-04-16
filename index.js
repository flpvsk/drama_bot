const elasticsearch = require('elasticsearch');
const readline = require('readline');
const util = require('util');
const uniqBy = require('lodash/uniqBy');
const sortBy = require('lodash/sortBy');
const levenstein = require('./levenstein');

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


process.stdout.write('Hi\n> ')

process.stdin.setEncoding('utf8');


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

process.stdin.on('data', (text) => {
  if (text === '\n') {
    process.stdout.write('> ')
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
        process.stdout.write('You didn\'t want to say that!\n> ')
        return;
      }

      let hits = uniqBy(results.hits.hits, (hit) => {
        return hit._source.text;
      });

      hits = sortBy(hits, hitsSort(text));

      const random = Math.floor(Math.random() * (hits.length / 2));
      process.stdout.write(`${hits[random]._source.text}\n> `)
    });
});
