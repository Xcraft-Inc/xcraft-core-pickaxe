// @ts-check

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

module.exports = operators;
