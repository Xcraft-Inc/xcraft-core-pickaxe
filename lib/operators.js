/* eslint-disable jsdoc/require-returns */
// @ts-check

const {number, array} = require('xcraft-core-stones');

/**
 * @template {AnyTypeOrShape} T
 * @typedef {import("./picks.js").ValuePick<T>} ValuePick
 */

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
  const {ValuePick} = require('./picks.js');
  if (value instanceof ValuePick) {
    return value.value;
  }

  throw new Error(`Bad value '${value}'`);
}

const operators = {
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

  field(field) {
    return /** @type {const} */ ({
      operator: 'field',
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
      type: number,
      field,
      distinct,
    });
  },

  /**
   * @template {AnyTypeOrShape} T
   * @param {ValuePick<T>} field
   */
  avg(field) {
    return /** @type {const} */ ({
      operator: 'avg',
      type: field.type,
      field: field.value,
    });
  },

  /**
   * @template {AnyTypeOrShape} T
   * @param {ValuePick<T>} field
   */
  max(field) {
    return /** @type {const} */ ({
      operator: 'max',
      type: field.type,
      field: field.value,
    });
  },

  /**
   * @template {AnyTypeOrShape} T
   * @param {ValuePick<T>} field
   */
  min(field) {
    return /** @type {const} */ ({
      operator: 'min',
      type: field.type,
      field: field.value,
    });
  },

  /**
   * @template {AnyTypeOrShape} T
   * @param {ValuePick<T>} field
   */
  sum(field) {
    return /** @type {const} */ ({
      operator: 'sum',
      type: field.type,
      field: field.value,
    });
  },

  /**
   * @template {AnyTypeOrShape} T
   * @param {ValuePick<T>} field
   */
  groupArray(field) {
    return /** @type {const} */ ({
      operator: 'groupArray',
      type: array(field.type),
      field: field.value,
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
 * @typedef {Values<{[k in keyof typeof operators] : ReturnType<typeof operators[k]>}>} Operator
 */

/**
 * @typedef {Operators["count"] | Operators["avg"] | Operators["max"] | Operators["min"] | Operators["sum"] | Operators["groupArray"]} Aggregator
 */
/**
 * @typedef {Operators["abs"] | Operators["plus"] | Operators["minus"]} MathOperator
 */

module.exports = operators;
