'use strict';

const {expect} = require('chai');
const {queryToSql} = require('../lib/query-to-sql.js');
const {QueryBuilder} = require('../lib/query-builder.js');
const {string, number, array, record} = require('xcraft-core-stones');

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

describe('xcraft.pickaxe', function () {
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
        firstname AS firstname,
        age AS age
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
});
