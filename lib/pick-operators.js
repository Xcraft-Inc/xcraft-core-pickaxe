/* eslint-disable jsdoc/require-returns */
// @ts-check

const {
  array,
  number,
  boolean,
  union,
  UnionType,
  any,
} = require('xcraft-core-stones');
const $o = require('./operators.js');
const {
  BasePick,
  ValuePick,
  BooleanPick,
  NumberPick,
  makePick,
  ArrayPick,
} = require('./picks.js');
const {getPickOrValueType} = require('./pick-or-value.js');

/**
 * @typedef {import('./pick-or-value.js').PickOrValue} PickOrValue
 */

/**
 * @template {PickOrValue} T
 * @typedef {import('./pick-or-value.js').PickOrValueType<T>} PickOrValueType
 */

/**
 * @template {PickOrValue[]} T
 * @typedef {{[K in keyof T]: PickOrValueType<T[K]>}} PickOrValueTypes
 */

const pickOperators = {
  ...$o,

  /**
   * @param {string} sql
   */
  unsafeSql(sql) {
    return new BasePick(any, $o.unsafeSql(sql));
  },

  /**
   * @param {BooleanPick} value
   * @returns {BooleanPick}
   */
  not(value) {
    return new BooleanPick(boolean, $o.not(value));
  },

  /**
   * @param {(BooleanPick | boolean)[]} conditions
   * @returns {BooleanPick}
   */
  and(...conditions) {
    return new BooleanPick(boolean, $o.and(...conditions));
  },

  /**
   * @param {(BooleanPick | boolean)[]} conditions
   * @returns {BooleanPick}
   */
  or(...conditions) {
    return new BooleanPick(boolean, $o.or(...conditions));
  },

  /**
   * @template {PickOrValue} T
   * @template {PickOrValue} U
   * @param {BooleanPick} condition
   * @param {T} a
   * @param {U} b
   * @returns {ValuePick<UnionType<[PickOrValueType<T>, PickOrValueType<U>]>>}
   */
  if(condition, a, b) {
    const newType = union(getPickOrValueType(a), getPickOrValueType(b));
    return new ValuePick(newType, $o.if(condition, a, b));
  },

  /**
   * @template {PickOrValue[]} T
   * @typedef {{[K in keyof T]: [BooleanPick | boolean, T[K]]}} CaseConditions
   */

  /**
   * @template {[PickOrValue, ...PickOrValue[]]} T
   * @template {PickOrValue} U
   * @param {[...CaseConditions<T>, U]} conditions
   * @returns {ValuePick<UnionType<[...PickOrValueTypes<T>, PickOrValueType<U>]>>}
   */
  case(...conditions) {
    const whenConditions = /** @type {CaseConditions<T>} */ (
      //
      conditions.slice(0, -1)
    );
    const whenTypes = /** @type {PickOrValueTypes<T>} */ (whenConditions.map(
      (condition) => getPickOrValueType(condition[1])
    ));
    const elseValue = /** @type {U} */ (conditions.at(-1));
    const newType = union(...whenTypes, getPickOrValueType(elseValue));
    return new ValuePick(newType, $o.case(whenConditions, elseValue));
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
