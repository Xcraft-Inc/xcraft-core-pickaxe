/* eslint-disable jsdoc/require-returns */
// @ts-check

const {pickOperators: $} = require('xcraft-core-pickaxe/lib/pick-operators.js');
const {RecordPick, ValuePick} = require('./picks.js');
const {StringType, NumberType} = require('xcraft-core-stones');
const {pickFromValue} = require('./pick-or-value.js');

/**
 * @typedef {Record<string, number>} LevelSet
 * @typedef {RecordPick<StringType, NumberType>} LevelSetPick
 * @typedef {LevelSet | LevelSetPick} LevelSetArg
 */

/**
 * @param {LevelSetPick} set
 * @param {string | ValuePick<StringType>} key
 * @param {number | ValuePick<NumberType>} value
 */
function levelSetHasValue(set, key, value) {
  return set.some((v, k) => $.and(k.eq(key), v.gte(value)));
}

/**
 * @param {LevelSet} set
 * @param {ValuePick<StringType>} key
 * @param {ValuePick<NumberType>} value
 */
function levelSetHasValue2(set, key, value) {
  return $.or(
    ...Object.entries(set).map(([k, v]) => $.and(key.eq(k), value.lte(v)))
  );
}

/**
 * @param {LevelSetArg} set
 */
function levelSetIsEmpty(set) {
  if (!(set instanceof RecordPick)) {
    return pickFromValue(Object.keys(set).length === 0);
  }
  return set.keys().length.eq(0);
}

/**
 * @param {LevelSetPick} set1
 * @param {LevelSet} set2
 */
function levelSetIsSubsetOf(set1, set2) {
  return set1.every((value, name) => levelSetHasValue2(set2, name, value));
}

/**
 * @param {LevelSetPick} set1
 * @param {LevelSet} set2
 */
function levelSetIsSupersetOf(set1, set2) {
  return $.and(
    ...Object.entries(set2).map(([k, v]) => levelSetHasValue(set1, k, v))
  );
}

/**
 * @param {LevelSetPick} set1
 * @param {LevelSet} set2
 */
function levelSetHasIntersectionWith(set1, set2) {
  return $.or(
    levelSetIsEmpty(set1),
    levelSetIsEmpty(set2),
    set1.some((value, name) => levelSetHasValue2(set2, name, value))
  );
}

/**
 * @param {LevelSetPick} set1
 * @param {LevelSet} set2
 */
function levelSetIsEqualTo(set1, set2) {
  return $.and(
    levelSetIsSubsetOf(set1, set2),
    levelSetIsSupersetOf(set1, set2)
  );
}

module.exports = {
  levelSetHasValue,
  levelSetIsEmpty,
  levelSetIsSubsetOf,
  levelSetIsSupersetOf,
  levelSetHasIntersectionWith,
  levelSetIsEqualTo,
};
