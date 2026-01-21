/**
 * @template {AnyTypeOrShape} T
 * @typedef {T extends OptionType ? T : OptionType<T>} WrapOption
 */

const {toObjectType, OptionType, option} = require('xcraft-core-stones');

/**
 * @template {ObjectShape} T
 * @typedef {{[K in keyof T]: WrapOption<T[K]>}} OptionalObjectShape
 */

/**
 * @template {ObjectShape} T
 * @param {ObjectType<T>} type
 * @returns {ObjectType<OptionalObjectShape<T>>}
 */
function optionalObjectType(type) {
  return type.map((subType) =>
    subType instanceof OptionType ? subType : option(subType)
  );
}

/**
 * @template {AnyObjectShape} T
 * @param {T} shape
 * @returns {OptionalObjectShape<GetShape<T>>}
 */
function optionalObjectShape(shape) {
  const type = toObjectType(shape);
  const optionalType = optionalObjectType(type);
  return optionalType.properties;
}

module.exports = {
  optionalObjectType,
  optionalObjectShape,
};
