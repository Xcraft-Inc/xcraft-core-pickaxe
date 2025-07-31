// @ts-check
'use strict';

const {expect} = require('chai');
const {queryToSql} = require('../lib/query-to-sql.js');
const {QueryBuilder} = require('../lib/query-builder.js');
const {
  string,
  number,
  array,
  record,
  enumeration,
  Type,
  StringType,
} = require('xcraft-core-stones');

/**
 * @param {string} sql
 */
function trimSql(sql) {
  return sql
    .trim()
    .replace(/\s+/g, ' ')
    .replace(/\( /g, '(')
    .replace(/ \)/g, ')');
}

/**
 * @template {string} T
 * @extends {StringType<`${T}@{string}`>}
 */
class TestIdType extends StringType {
  /**
   * @param {T} name
   */
  constructor(name) {
    super(`${name}@string`);
  }
}

/** @type {<const T extends string>(name: T) => Type<`${T}@${string}`>} */
const id = (name) => new TestIdType(name);

describe('xcraft.pickaxe', function () {
  class TestUserShape {
    id = id('user');
    firstname = string;
    lastname = string;
    role = enumeration('admin', 'user');
    age = number;
    mails = array(string);
    address = class {
      streetName = string;
      townName = string;
    };
    skills = record(string, number);
  }

  it('query to sql', function () {
    const query = {
      db: 'test_db',
      from: 'test_table',
      scope: {
        operator: 'get',
        value: {
          operator: 'field',
          field: 'action',
        },
        path: ['payload', 'state'],
      },
      select: {
        firstname: {
          operator: 'get',
          path: ['firstname'],
        },
        age: {
          operator: 'get',
          path: ['age'],
        },
      },
      where: {
        operator: 'gt',
        a: {
          operator: 'get',
          path: ['age'],
        },
        b: {
          operator: 'value',
          value: 10,
        },
      },
    };

    const result = queryToSql(query);

    const sql = `
      SELECT
        json_extract(json_extract(action, '$.payload.state'), '$.firstname') AS firstname,
        json_extract(json_extract(action, '$.payload.state'), '$.age') AS age
      FROM test_table
      WHERE json_extract(json_extract(action, '$.payload.state'), '$.age') > ?
    `;
    const values = [10];

    expect(trimSql(result.sql)).to.be.equal(trimSql(sql));
    expect(result.values).to.be.deep.equal(values);
  });

  it('query builder', function () {
    const userIds = ['user@toto', 'user@tata'];
    const builder = new QueryBuilder()
      .db('test_db')
      .from('test_table', TestUserShape)
      .fields(['firstname', 'age'])
      .where((user, $) =>
        $.and(
          user.field('firstname').eq('Toto'),
          user.field('age').eq(42),
          user.field('lastname').in(userIds),
          user.field('address').get('streetName').eq('Mine road')
        )
      );

    const result = queryToSql(builder.query);

    const sql = `
      SELECT
        firstname,
        age
      FROM test_table
      WHERE (
        firstname IS ? AND
        age IS ? AND
        lastname IN (?,?) AND
        json_extract(address, '$.streetName') IS ?
      )
    `;
    const values = ['Toto', 42, 'user@toto', 'user@tata', 'Mine road'];

    expect(trimSql(result.sql)).to.be.equal(trimSql(sql));
    expect(result.values).to.be.deep.equal(values);
  });

  it('pick string', function () {
    const builder = new QueryBuilder()
      .db('test_db')
      .from('test_table', TestUserShape)
      .field('id')
      .where((user, $) =>
        $.and(
          user.field('firstname').substr(0, 2).length.lte(2),
          // Test type derived of string
          user.field('id').glob(`user@*`),
          user.field('id').like(`user@*`),
          // Test enumeration
          user.field('role').substr(0, 2).length.lte(2)
        )
      );

    const result = queryToSql(builder.query, null);

    const sql = `
      SELECT
        id
      FROM test_table
      WHERE (
        LENGTH(SUBSTR(firstname, 0, 2)) <= 2 AND
        id GLOB 'user@*' AND
        id LIKE 'user@*' AND
        LENGTH(SUBSTR(role, 0, 2)) <= 2
      )
    `;

    expect(trimSql(result.sql)).to.be.equal(trimSql(sql));
  });

  it('pick number', function () {
    const builder = new QueryBuilder()
      .db('test_db')
      .from('test_table', TestUserShape)
      .field('id')
      .where((user, $) =>
        $.and(
          user.field('age').abs().gt(0),
          user
            .field('firstname')
            .length.plus(user.field('lastname').length)
            .plus(1)
            .lte(20)
        )
      );

    const result = queryToSql(builder.query, null);

    const sql = `
      SELECT
        id
      FROM test_table
      WHERE (
        ABS(age) > 0 AND
        ((LENGTH(firstname) + LENGTH(lastname)) + 1) <= 20
      )
    `;

    expect(trimSql(result.sql)).to.be.equal(trimSql(sql));
  });
});
