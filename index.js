const elasticsearch = require('elasticsearch');
const readline = require('readline');
const util = require('util');
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




      // const total = Math.min(results.hits.total, 50);
      // const r = Math.floor(Math.random() * total);
      // return client.search(makeSearch(makeAndQuery(text), r, 1));


const hitsSort = (text) => {
  return (hit1, hit2) => {
    return (
      levenstein(text, hit1._source.text) -
      levenstein(text, hit2._source.text)
    );
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

      const textSet = new Set();
      for (let hit of results.hits.hits) {
        textSet.add(hit._source.text);
      }
      const random = Math.floor(Math.random() * (textSet.size / 2));

      let i = 0;
      for (let answer of textSet) {
        if (i++ === random) {
          process.stdout.write(`${answer}\n> `)
        }
      }
    });
});
