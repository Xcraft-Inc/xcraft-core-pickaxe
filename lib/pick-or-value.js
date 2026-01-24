/* eslint-disable jsdoc/require-returns */
// @ts-check

const {
  number,
  NumberType,
  string,
  StringType,
  boolean,
  BooleanType,
} = require('xcraft-core-stones');
const {BasePick, makeTypePick} = require('./picks.js');
const $o = require('./operators.js');

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
 * @template {PickValue} T
 * @param {T} value
 * @returns {GetValueType<T>}
 */
function getValueType(value) {
  if (typeof value === 'string') {
    return /** @type {any} */ (string);
  }
  if (typeof value === 'number') {
    return /** @type {any} */ (number);
  }
  if (typeof value === 'boolean') {
    return /** @type {any} */ (boolean);
  }
  throw new Error('Unknown value type');
}

/**
 * @template {PickOrValue} T
 * @param {T} pickOrValue
 * @returns {PickOrValueType<T>}
 */
function getPickOrValueType(pickOrValue) {
  if (pickOrValue instanceof BasePick) {
    return pickOrValue.type;
  }
  return /** @type {any} */ (getValueType(pickOrValue));
}

/**
 * @template {PickValue} T
 * @param {T} value
 */
function pickFromValue(value) {
  const type = getValueType(value);
  return makeTypePick(type, $o.value(value));
}

module.exports = {
  getPickOrValueType,
  getValueType,
  pickFromValue,
};
