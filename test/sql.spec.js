// @ts-check
'use strict';

const {expect} = require('chai');
const {queryToSql} = require('../lib/query-to-sql.js');
const {QueryBuilder, FromQuery} = require('../lib/query-builder.js');
const {
  string,
  number,
  array,
  record,
  enumeration,
  Type,
  StringType,
  object,
  any,
  value,
  dateTime,
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
    tagIds = array(id('tag'));
  }

  class TestNoteShape {
    id = id('note');
    text = string;
  }

  class TestTagShape {
    id = id('tag');
    name = string;
    color = string;
  }

  /**
   * @template {AnyObjectShape} T
   * @param {T} shape
   */
  function ActionShape(shape) {
    return object({
      entityType: string,
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

  function getDb(entityType) {
    return `${entityType}_db`;
  }

  /**
   * @template {AnyObjectShape} T
   * @param {string} tableName
   * @param {T} shape
   * @returns {FromQuery<[GetShape<T>]>}
   */
  function queryAction(tableName, shape) {
    const builder = new QueryBuilder({
      getTableSchema: (name, shape) => {
        const db = getDb(name);
        const isJoinedDb = name !== tableName; // or compare on db
        if (isJoinedDb) {
          console.log(`Attach ${name}`);
        }
        return QueryBuilder.TableSchema({
          db: isJoinedDb ? db : undefined,
          table: 'action_table',
          alias: isJoinedDb ? name : undefined, // doesn't support multiple join on the same table
          shape: shape,
          baseShape: ActionShape(shape),
          scope: (row) => row.field('action').get('payload').get('state'),
          scopeCondition: (row) => row.field('entityType').eq(name),
        });
      },
    });
    return builder.from(tableName, shape);
  }

  it('query to sql', function () {
    const query = {
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
      .from('test_table', TestUserShape)
      .fields(['firstname', 'age'])
      .where((user, $) =>
        $.and(
          user.field('firstname').eq('Toto'),
          user.field('age').eq(42),
          user.field('lastname').in(userIds),
          user.field('address').get('streetName').eq('Mine road')
        )
      )
      .orderBy((user) => [user.get('age'), user.get('firstname')]);

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
      ORDER BY age, firstname
    `;
    const values = ['Toto', 42, 'user@toto', 'user@tata', 'Mine road'];

    expect(trimSql(result.sql)).to.be.equal(trimSql(sql));
    expect(result.values).to.be.deep.equal(values);
  });

  it('select all', function () {
    const builder = new QueryBuilder()
      .from('test_table', TestUserShape)
      .selectAll()
      .where((user, $) => user.field('role').eq('admin'));

    const result = queryToSql(builder.query, null);

    const sql = `
      SELECT *
      FROM test_table
      WHERE role IS 'admin'
    `;

    expect(trimSql(result.sql)).to.be.equal(trimSql(sql));
  });

  it('pick string', function () {
    const builder = new QueryBuilder()
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

  it('pick array', function () {
    const builder = new QueryBuilder()
      .from('test_table', TestUserShape)
      .field('id')
      .where((user, $) =>
        $.and(
          user.field('mails').length.gt(0),
          user.field('mails').some((mail) => mail.like('%@example.com'))
        )
      );

    const result = queryToSql(builder.query, null);

    const sql = `
      SELECT
        id
      FROM test_table
      WHERE (
        json_array_length(mails) > 0 AND
        EXISTS (
          SELECT *
          FROM json_each(mails)
          WHERE json_each.value LIKE '%@example.com'
        )
      )
    `;

    expect(trimSql(result.sql)).to.be.equal(trimSql(sql));
  });

  it('pick record', function () {
    const builder = new QueryBuilder()
      .from('test_table', TestUserShape)
      .field('id')
      .where((user, $) =>
        user
          .field('skills')
          .some((value, key) => $.or(value.eq(42), key.eq('test')))
      );

    const result = queryToSql(builder.query, null);

    const sql = `
      SELECT
        id
      FROM test_table
      WHERE
        EXISTS (
          SELECT *
          FROM json_each(skills)
          WHERE (
            json_each.value IS 42 OR
            json_each.key IS 'test'
          )
        )
    `;

    expect(trimSql(result.sql)).to.be.equal(trimSql(sql));
  });

  it('join tables', function () {
    const builder = new QueryBuilder()
      .from('users', TestUserShape)
      .leftJoin('notes', TestNoteShape, (user, note) =>
        note
          .field('id')
          .substr(0, 'note@'.length + 1)
          .eq(user.field('id'))
      )
      .select((user, note) => ({
        id: user.field('id'),
        noteText: note.field('text'),
      }))
      .where((user, note, $) =>
        $.and(user.field('age').lt(10), note.field('text').length.gt(0))
      );

    const result = queryToSql(builder.query, null);

    const sql = `
      SELECT
        users.id AS id,
        notes.text AS noteText
      FROM users
      LEFT JOIN notes ON SUBSTR(notes.id, 0, 6) = users.id
      WHERE (
        users.age < 10 AND
        LENGTH(notes.text) > 0
      )
    `;

    expect(trimSql(result.sql)).to.be.equal(trimSql(sql));
  });

  it('join different databases', function () {
    const builder = new QueryBuilder({
      schema: {
        users: {db: 'users_db', table: 'users', alias: 'u'},
        notes: {db: 'notes_db', table: 'notes', alias: 'n'},
      },
    })
      .from('users', TestUserShape)
      .leftJoin('notes', TestNoteShape, (user, note) =>
        note
          .field('id')
          .substr(0, 'note@'.length + 1)
          .eq(user.field('id'))
      )
      .select((user, note) => ({
        id: user.field('id'),
        noteText: note.field('text'),
      }))
      .where((user, note, $) =>
        $.and(user.field('age').lt(10), note.field('text').length.gt(0))
      );

    const result = queryToSql(builder.query, null);

    const sql = `
      SELECT
        u.id AS id,
        n.text AS noteText
      FROM users_db.users AS u
      LEFT JOIN notes_db.notes AS n ON SUBSTR(n.id, 0, 6) = u.id
      WHERE (
        u.age < 10 AND
        LENGTH(n.text) > 0
      )
    `;

    expect(trimSql(result.sql)).to.be.equal(trimSql(sql));
  });

  it('query action', function () {
    const builder = queryAction('users', TestUserShape)
      .field('id')
      .where((user, $) => user.get('age').gt(10));

    const result = queryToSql(builder.query, null);

    const sql = `
      SELECT
        json_extract(action, '$.payload.state.id') AS id
      FROM action_table
      WHERE (
        entityType IS 'users' AND
        json_extract(action, '$.payload.state.age') > 10
      )
    `;

    expect(trimSql(result.sql)).to.be.equal(trimSql(sql));
  });

  it('select all in query action', function () {
    const builder = queryAction('users', TestUserShape)
      .selectAll()
      .where((user) => user.get('age').gt(10));

    const result = queryToSql(builder.query, null);

    const keys = Object.keys(new TestUserShape());
    const sql = `
      SELECT
        ${keys
          .map(
            (key) => `json_extract(action, '$.payload.state.${key}') AS ${key}`
          )
          .join(', ')}
      FROM action_table
      WHERE (
        entityType IS 'users' AND
        json_extract(action, '$.payload.state.age') > 10
      )
    `;

    expect(trimSql(result.sql)).to.be.equal(trimSql(sql));
  });

  it('join query action', function () {
    const builder = queryAction('users', TestUserShape)
      .leftJoin('notes', TestNoteShape, (user, note) =>
        note
          .get('id')
          .substr(0, 'note@'.length + 1)
          .eq(user.get('id'))
      )
      .select((user, note) => ({
        id: user.get('id'),
        noteText: note.field('text'),
      }))
      .where((user, note, $) => user.get('age').gt(10));

    const result = queryToSql(builder.query, null);

    const sql = `
      SELECT
        json_extract(action, '$.payload.state.id') AS id,
        json_extract(notes.action, '$.payload.state.text') AS noteText
      FROM action_table
      LEFT JOIN notes_db.action_table AS notes ON
        SUBSTR(json_extract(notes.action, '$.payload.state.id'), 0, 6) = json_extract(action, '$.payload.state.id')
      WHERE (
        (entityType IS 'users' AND notes.entityType IS 'notes') AND
        json_extract(action, '$.payload.state.age') > 10
      )
    `;

    expect(trimSql(result.sql)).to.be.equal(trimSql(sql));
  });
});
