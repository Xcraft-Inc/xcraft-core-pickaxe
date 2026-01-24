/* eslint-disable jsdoc/require-returns */
// @ts-check

const {number} = require('xcraft-core-stones');

function op(value) {
  if (value === null) {
    return operators.null();
  }
  const type = typeof value;
  if (['string', 'number', 'boolean'].includes(type)) {
    return operators.value(value);
  }
  if (type === 'object' && 'operator' in value) {
    return value;
  }
  const {isAnyPick} = /** @type {any} */ (require('./picks.js'));
  if (isAnyPick(value)) {
    return value.expression;
  }

  throw new Error(`Bad value '${value}'`);
}

const operators = {
  unsafeSql(sql) {
    return /** @type {const} */ ({
      operator: 'unsafeSql',
      sql,
    });
  },

  value(value) {
    return /** @type {const} */ ({
      operator: 'value',
      value,
    });
  },

  null() {
    return /** @type {const} */ ({
      operator: 'null',
    });
  },

  field(field, tableName) {
    return /** @type {const} */ ({
      operator: 'field',
      tableName,
      field,
    });
  },

  as(value, name) {
    return /** @type {const} */ ({
      operator: 'as',
      value,
      name,
    });
  },

  get(value, path) {
    return /** @type {const} */ ({
      operator: 'get',
      value,
      path,
    });
  },

  not(value) {
    return /** @type {const} */ ({
      operator: 'not',
      value,
    });
  },

  stringConcat(...values) {
    values = values.map(op);
    return /** @type {const} */ ({
      operator: 'stringConcat',
      values,
    });
  },

  stringLength(value) {
    value = op(value);
    return /** @type {const} */ ({
      operator: 'stringLength',
      value,
    });
  },

  substr(value, start, length) {
    value = op(value);
    start = op(start);
    if (length !== undefined) {
      length = op(length);
    }
    return /** @type {const} */ ({
      operator: 'substr',
      value,
      start,
      length,
    });
  },

  eq(a, b) {
    a = op(a);
    b = op(b);
    return /** @type {const} */ ({
      operator: 'eq',
      a,
      b,
    });
  },

  neq(a, b) {
    a = op(a);
    b = op(b);
    return /** @type {const} */ ({
      operator: 'neq',
      a,
      b,
    });
  },

  gte(a, b) {
    a = op(a);
    b = op(b);
    return /** @type {const} */ ({
      operator: 'gte',
      a,
      b,
    });
  },

  gt(a, b) {
    a = op(a);
    b = op(b);
    return /** @type {const} */ ({
      operator: 'gt',
      a,
      b,
    });
  },

  lte(a, b) {
    a = op(a);
    b = op(b);
    return /** @type {const} */ ({
      operator: 'lte',
      a,
      b,
    });
  },

  lt(a, b) {
    a = op(a);
    b = op(b);
    return /** @type {const} */ ({
      operator: 'lt',
      a,
      b,
    });
  },

  in(value, list) {
    list = list.map(op);
    return /** @type {const} */ ({
      operator: 'in',
      value,
      list,
    });
  },

  like(value, pattern) {
    value = op(value);
    pattern = op(pattern);
    return /** @type {const} */ ({
      operator: 'like',
      value,
      pattern,
    });
  },

  glob(value, pattern) {
    value = op(value);
    pattern = op(pattern);
    return /** @type {const} */ ({
      operator: 'glob',
      value,
      pattern,
    });
  },

  match(a, b) {
    a = op(a);
    b = op(b);
    return /** @type {const} */ ({
      operator: 'match',
      a,
      b,
    });
  },

  and(...conditions) {
    return /** @type {const} */ ({
      operator: 'and',
      conditions,
    });
  },

  or(...conditions) {
    return /** @type {const} */ ({
      operator: 'or',
      conditions,
    });
  },

  ifNull(a, b) {
    return /** @type {const} */ ({
      operator: 'ifNull',
      a,
      b,
    });
  },

  if(condition, a, b) {
    return /** @type {const} */ ({
      operator: 'if',
      condition: op(condition),
      a: op(a),
      b: op(b),
    });
  },

  abs(value) {
    value = op(value);
    return /** @type {const} */ ({
      operator: 'abs',
      type: number,
      value,
    });
  },

  plus(...values) {
    values = values.map(op);
    return /** @type {const} */ ({
      operator: 'plus',
      type: number,
      values,
    });
  },

  minus(...values) {
    values = values.map(op);
    return /** @type {const} */ ({
      operator: 'minus',
      type: number,
      values,
    });
  },

  length(list) {
    return /** @type {const} */ ({
      operator: 'length',
      list,
    });
  },

  includes(list, value) {
    value = op(value);
    return /** @type {const} */ ({
      operator: 'includes',
      list,
      value,
    });
  },

  some(list, condition) {
    return /** @type {const} */ ({
      operator: 'some',
      list,
      condition,
    });
  },

  query(query) {
    return /** @type {const} */ ({
      operator: 'query',
      query,
    });
  },

  each(value) {
    return /** @type {const} */ ({
      operator: 'each',
      value,
    });
  },

  eachValue() {
    return /** @type {const} */ ({
      operator: 'eachValue',
    });
  },

  eachKey() {
    return /** @type {const} */ ({
      operator: 'eachKey',
    });
  },

  keys(obj) {
    return /** @type {const} */ ({
      operator: 'keys',
      obj,
    });
  },

  values(obj) {
    return /** @type {const} */ ({
      operator: 'values',
      obj,
    });
  },

  asc(value) {
    return /** @type {const} */ ({
      operator: 'asc',
      value,
    });
  },

  desc(value) {
    return /** @type {const} */ ({
      operator: 'desc',
      value,
    });
  },

  count(field, distinct) {
    return /** @type {const} */ ({
      operator: 'count',
      field,
      distinct,
    });
  },

  avg(field) {
    return /** @type {const} */ ({
      operator: 'avg',
      field,
    });
  },

  max(field) {
    return /** @type {const} */ ({
      operator: 'max',
      field,
    });
  },

  min(field) {
    return /** @type {const} */ ({
      operator: 'min',
      field,
    });
  },

  sum(field) {
    return /** @type {const} */ ({
      operator: 'sum',
      field,
    });
  },

  groupArray(field) {
    return /** @type {const} */ ({
      operator: 'groupArray',
      field,
    });
  },
};

/**
 * @template T
 * @typedef {T[keyof T]} Values
 */

/**
 * @typedef {{[k in keyof typeof operators] : ReturnType<typeof operators[k]>}} Operators
 */

/**
 * @typedef {Values<{[k in keyof typeof operators] : ReturnType<typeof operators[k]>}>} Expression
 */

module.exports = operators;
