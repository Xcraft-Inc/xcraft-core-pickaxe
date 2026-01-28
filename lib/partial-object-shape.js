const {ObjectType} = require('xcraft-core-stones');

/**
 * @template {ObjectType} T
 * @template {(keyof T["properties"])[]} F
 * @param {T} objectType
 * @param {F} fields
 * @returns {Pick<T["properties"], F[number]>}
 */
function partialObjectShape(objectType, fields) {
  const properties = objectType.properties;
  return /** @type {Pick<T["properties"], F[number]>} */ (Object.fromEntries(
    fields.map((fieldName) => [fieldName, properties[fieldName]])
  ));
}

module.exports = partialObjectShape;
