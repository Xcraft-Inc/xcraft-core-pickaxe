/* eslint-disable no-inner-declarations */
// @ts-check
const {expressionToSql} = require('./operator-to-sql.js');
const {
  string,
  number,
  array,
  object,
  dateTime,
  any,
  value,
  record,
} = require('xcraft-core-stones');
const $ = require('./operators.js');
const {queryToSql} = require('./query-to-sql.js');
const {QueryBuilder, ScopedFromQuery} = require('./query-builder.js');
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
  skills = record(string, number);
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

/**
 * @param {AnyObjectShape} shape
 */
function ActionShape(shape) {
  return object({
    name: string,
    action: object({
      meta: any,
      payload: object({
        state: shape,
      }),
      type: value('test'),
    }),
    timestamp: dateTime,
  });
}

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

  console.log(expressionToSql(filter1));
  console.log(expressionToSql(filter2));
}

example2: {
  const userIds = ['user@toto', 'user@tata'];
  const builder = new QueryBuilder()
    .from('test_table', TestUserShape3)
    .fields(['firstname', 'age'])
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

example3: {
  const builder = new QueryBuilder()
    .from('test_table', ActionShape(TestUserShape))
    .scope((row) => row.field('action').get('payload').get('state'))
    .fields(['firstname', 'age']);

  console.log(queryToSql(builder.query));
}

/**
 * @template {AnyObjectShape} T
 * @param {T} shape
 * @returns {ScopedFromQuery<GetShape<T>>}
 */
function queryAction(shape) {
  const builder = new QueryBuilder()
    .from('test_table', ActionShape(shape))
    .scope((row) => row.field('action').get('payload').get('state'));
  return /** @type {ScopedFromQuery<GetShape<T>>} */ (builder);
}

example4: {
  const builder = queryAction(TestUserShape)
    .fields(['firstname', 'age'])
    .where((user) => user.get('address').get('streetName').eq('toto'))
    .orderBy((user) => [user.get('age'), user.get('firstname')]);

  console.log(queryToSql(builder.query));
}

example5: {
  const builder = queryAction(TestUserShape)
    .fields(['firstname', 'age'])
    .where((user, $) =>
      $.and(
        user.get('mails').length.gt(0),
        user.get('mails').some((mail) => mail.like('%@example.com'))
      )
    );

  console.log(queryToSql(builder.query));
}

example6: {
  const builder = queryAction(TestUserShape)
    .fields(['firstname', 'age'])
    .where((user, $) =>
      user
        .get('skills')
        .some((value, key) => $.or(value.eq(42), key.eq('test')))
    );

  console.log(queryToSql(builder.query));
}
