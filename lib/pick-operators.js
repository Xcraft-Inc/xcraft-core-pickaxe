/* eslint-disable jsdoc/require-returns */
// @ts-check

const {
  array,
  number,
  NumberType,
  string,
  StringType,
  boolean,
  BooleanType,
  union,
  UnionType,
  any,
} = require('xcraft-core-stones');
const $o = require('./operators.js');
const {
  BasePick,
  ValuePick,
  NumberPick,
  makePick,
  ArrayPick,
} = require('./picks.js');

/**
 * @typedef {string | number | boolean} PickValue
 */

/**
 * @typedef {BasePick<any> | PickValue} PickOrValue
 */

/**
 * @template {PickValue} T
 * @typedef {T extends boolean ? BooleanType : T extends number ? NumberType : T extends string ? StringType : never} GetValueType
 */

/**
 * @template {PickOrValue} T
 * @typedef {T extends BasePick<infer U> ? U : T extends PickValue ? GetValueType<T> : never} PickOrValueType
 */

/**
 * @template {PickOrValue} T
 * @param {T} pickOrValue
 * @returns {PickOrValueType<T>}
 */
function getPickOrValueType(pickOrValue) {
  if (pickOrValue instanceof BasePick) {
    return pickOrValue.type;
  }
  if (typeof pickOrValue === 'string') {
    return /** @type {any} */ (string);
  }
  if (typeof pickOrValue === 'number') {
    return /** @type {any} */ (number);
  }
  if (typeof pickOrValue === 'boolean') {
    return /** @type {any} */ (boolean);
  }
  throw new Error('Unknown pickOrValue type');
}

const pickOperators = {
  ...$o,

  /**
   * @param {string} sql
   */
  unsafeSql(sql) {
    return new BasePick(any, $o.unsafeSql(sql));
  },

  /**
   * @param {BasePick<BooleanType>} value
   * @returns {BasePick<BooleanType>}
   */
  not(value) {
    return new ValuePick(boolean, $o.not(value));
  },

  /**
   * @param {BasePick<BooleanType>[]} conditions
   * @returns {BasePick<BooleanType>}
   */
  and(...conditions) {
    return new ValuePick(boolean, $o.and(...conditions));
  },

  /**
   * @param {BasePick<BooleanType>[]} conditions
   * @returns {BasePick<BooleanType>}
   */
  or(...conditions) {
    return new ValuePick(boolean, $o.or(...conditions));
  },

  /**
   * @template {PickOrValue} T
   * @template {PickOrValue} U
   * @param {BasePick<BooleanType>} condition
   * @param {T} a
   * @param {U} b
   * @returns {ValuePick<UnionType<[PickOrValueType<T>, PickOrValueType<U>]>>}
   */
  if(condition, a, b) {
    const newType = union(getPickOrValueType(a), getPickOrValueType(b));
    return new ValuePick(newType, $o.if(condition, a, b));
  },

  /**
   * @param {BasePick} [field]
   * @param {boolean} [distinct]
   */
  count(field, distinct) {
    return new NumberPick(number, $o.count(field, distinct));
  },

  /**
   * @param {NumberPick} field
   */
  avg(field) {
    return new NumberPick(number, $o.avg(field));
  },

  /**
   * @template {AnyTypeOrShape} T
   * @param {ValuePick<T>} field
   */
  max(field) {
    return makePick(field.type, $o.max(field));
  },

  /**
   * @template {AnyTypeOrShape} T
   * @param {ValuePick<T>} field
   */
  min(field) {
    return makePick(field.type, $o.min(field));
  },

  /**
   * @param {NumberPick} field
   */
  sum(field) {
    return new NumberPick(number, $o.sum(field));
  },

  /**
   * @template {AnyTypeOrShape} T
   * @param {ValuePick<T>} field
   */
  groupArray(field) {
    return new ArrayPick(array(field.type), $o.groupArray(field));
  },
};

module.exports = {
  pickOperators,
};
