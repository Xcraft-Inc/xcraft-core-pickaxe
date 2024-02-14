// @ts-check
const {operatorToSql} = require('./operator-to-sql.js');
const {string, number, array, object} = require('xcraft-core-stones');
const $ = require('./operators.js');
const {queryToSql} = require('./query-to-sql.js');
const {QueryBuilder} = require('./query-builder.js');
const {makePick} = require('./picks.js');

class TestUserShape {
  firstname = string;
  lastname = string;
  age = number;
  mails = array(string);
  address = class {
    streetName = string;
    townName = string;
  };
}

const TestUserShape2 = {
  firstname: string,
  lastname: string,
  age: number,
  mails: array(string),
  address: class {
    streetName = string;
    townName = string;
  },
};

const TestUserShape3 = object({
  firstname: string,
  lastname: string,
  age: number,
  mails: array(string),
  address: class {
    streetName = string;
    townName = string;
  },
});

example1: {
  const user = makePick(TestUserShape, {field: 'value', path: []});

  const filter1 = user.get('firstname').eq('toto');

  const userIds = ['user@toto', 'user@tata'];
  const filter2 = $.and(
    user.get('firstname').eq('Toto'),
    user.get('age').eq(42),
    user.get('lastname').in(userIds),
    user.get('address').get('streetName').eq('Mine road')
  );

  console.log(operatorToSql(filter1));
  console.log(operatorToSql(filter2));
}

example2: {
  const userIds = ['user@toto', 'user@tata'];
  const builder = new QueryBuilder()
    .db('test_db')
    .from('test_table', TestUserShape2)
    .selectFields(['firstname', 'age'])
    .where((user, $) =>
      $.and(
        user.field('firstname').eq('Toto'),
        user.field('age').eq(42),
        user.field('lastname').in(userIds),
        user.field('address').get('streetName').eq('Mine road')
      )
    );

  console.log(queryToSql(builder.query));
}
