/* eslint-disable jsdoc/require-returns */
// @ts-check

const {
  ArrayType,
  ObjectType,
  Type,
  getTypeInstance,
  array,
  ObjectMapType,
  RecordType,
  number,
  StringType,
  string,
} = require('xcraft-core-stones');

const $o = require('./operators.js');

/**
 * @typedef {import('./operators.js').Operators} Operators
 */
/**
 * @typedef {import('./operators.js').Operator} Operator
 */

/**
 * @typedef {(keyof any)} PathElement
 */
/**
 * @typedef {PathElement[]} Path
 */

/**
 * @template T
 * @template {keyof T} K
 * @typedef {Pick<T, K>[keyof Pick<T, K>]} SelectValues
 */

/**
 * @typedef {{
 *   field: (keyof any) | SelectValues<Operators, "eachValue" | "eachKey" | "keys" | "values" | "length">,
 *   path: Path
 * }} Context
 */

/**
 * @typedef {ValuePick<any> | ArrayPick<any> | ObjectPick<any> | RecordPick<any,any>} AnyPick
 */

/**
 * @template {Type} T
 * @typedef {[T] extends [ArrayType<infer V>] ? ArrayPick<V> : [T] extends [ObjectType<infer S>] ? ObjectPick<S> : [T] extends [RecordType<infer K,infer V>] ? RecordPick<K,V> : ValuePick<T>} PickOfType
 */
/**
 * @template {AnyTypeOrShape} T
 * @typedef {PickOfType<GetType<T>>} PickOf
 */

/**
 * @param {Context} context
 * @param {PathElement} path
 * @returns {Context}
 */
function contextWithPath(context, path) {
  return {
    ...context,
    path: [...context.path, path],
  };
}

/**
 * @template {AnyTypeOrShape} T
 */
class ValuePick {
  /** @type {T} */
  #type;
  /** @type {Context} */
  #context;

  /**
   * @param {T} type
   * @param {Context} context
   */
  constructor(type, context) {
    this.#type = type;
    this.#context = context;
  }

  get value() {
    const {field, path} = this.#context;
    if (path.length > 0) {
      return $o.get($o.field(field), path);
    }
    return $o.field(field);
  }

  get type() {
    return this.#type;
  }

  /**
   * @param {t<T> | ValuePick<T>} value
   */
  eq(value) {
    return $o.eq(this.value, value);
  }

  /**
   * @param {t<T> | ValuePick<T>} value
   */
  neq(value) {
    return $o.neq(this.value, value);
  }

  /**
   * @param {t<T> | ValuePick<T>} value
   */
  gte(value) {
    return $o.gte(this.value, value);
  }

  /**
   * @param {t<T> | ValuePick<T>} value
   */
  gt(value) {
    return $o.gt(this.value, value);
  }

  /**
   * @param {t<T> | ValuePick<T>} value
   */
  lte(value) {
    return $o.lte(this.value, value);
  }

  /**
   * @param {t<T> | ValuePick<T>} value
   */
  lt(value) {
    return $o.lt(this.value, value);
  }

  /**
   * @param {(t<T> | ValuePick<T>)[]} list
   */
  in(list) {
    return $o.in(this.value, list);
  }

  /**
   * @param {string | ValuePick<StringType>} value
   */
  like(value) {
    return $o.like(this.value, value);
  }
}

/**
 * @template {ObjectShape} T
 */
class ObjectPick {
  /** @type {ObjectType<T>} */
  #type;
  /** @type {Context} */
  #context;

  /**
   * @param {ObjectType<T>} type
   * @param {Context} context
   */
  constructor(type, context) {
    this.#type = type;
    this.#context = context;
  }

  get value() {
    const {field, path} = this.#context;
    if (path.length > 0) {
      return $o.get($o.field(field), path);
    }
    return $o.field(field);
  }

  get type() {
    return this.#type;
  }

  /**
   * @template {keyof T} K
   * @param {K} key
   * @returns {PickOf<T[K]>}
   */
  get(key) {
    return makePick(
      this.#type.properties[key],
      contextWithPath(this.#context, key)
    );
  }

  /**
   * @param {t<T[keyof T]> | ValuePick<T[keyof T]>} value
   */
  includes(value) {
    return $o.includes(this.value, value);
  }
}

/**
 * @template {AnyTypeOrShape} K
 * @template {AnyTypeOrShape} V
 */
class RecordPick {
  /** @type {RecordType<K,V>} */
  #type;
  /** @type {Context} */
  #context;

  /**
   * @param {RecordType<K,V>} type
   * @param {Context} context
   */
  constructor(type, context) {
    this.#type = type;
    this.#context = context;
  }

  get value() {
    const {field, path} = this.#context;
    if (path.length > 0) {
      return $o.get($o.field(field), path);
    }
    return $o.field(field);
  }

  get type() {
    return this.#type;
  }

  /**
   * @param {t<K>} key
   * @returns {PickOf<V>}
   */
  get(key) {
    return makePick(this.#type.valuesType, contextWithPath(this.#context, key));
  }

  /**
   * @param {t<V> | ValuePick<V>} value
   */
  includes(value) {
    return $o.includes(this.value, value);
  }

  /**
   * @param {(value: PickOf<V>, key: PickOf<K>) => Operator} func
   */
  some(func) {
    return $o.some(
      this.value,
      func(
        makePick(this.#type.valuesType, {
          field: $o.eachValue(),
          path: [],
        }),
        makePick(this.#type.keysType, {
          field: $o.eachKey(),
          path: [],
        })
      )
    );
  }

  /**
   * @returns {ArrayPick<K>}
   */
  keys() {
    return new ArrayPick(array(this.#type.keysType), {
      field: $o.keys(this.value),
      path: [],
    });
  }

  /**
   * @returns {ArrayPick<V>}
   */
  values() {
    return new ArrayPick(array(this.#type.valuesType), {
      field: $o.values(this.value),
      path: [],
    });
  }
}

/**
 * @template {AnyTypeOrShape} T
 */
class ArrayPick {
  /** @type {ArrayType<T>} */
  #type;
  /** @type {Context} */
  #context;

  /**
   * @param {ArrayType<T>} type
   * @param {Context} context
   */
  constructor(type, context) {
    this.#type = type;
    this.#context = context;
  }

  get value() {
    const {field, path} = this.#context;
    return $o.get($o.field(field), path);
  }

  get type() {
    return this.#type;
  }

  /**
   * @param {number} index
   * @returns {PickOf<T>}
   */
  get(index) {
    return makePick(
      this.#type.valuesType,
      contextWithPath(this.#context, index)
    );
  }

  get length() {
    return new ValuePick(number, {
      field: $o.length(this.value),
      path: [],
    });
  }

  /**
   * @param {t<T> | ValuePick<T>} value
   */
  includes(value) {
    return $o.includes(this.value, value);
  }

  /**
   * @param {(value: PickOf<T>) => Operator} func
   */
  some(func) {
    return $o.some(
      this.value,
      func(
        makePick(this.#type.valuesType, {
          field: $o.eachValue(),
          path: [],
        })
      )
    );
  }
}

/**
 * @param {{}} value
 * @returns {value is AnyPick}
 */
function isAnyPick(value) {
  return (
    value instanceof ValuePick ||
    value instanceof ObjectPick ||
    value instanceof RecordPick ||
    value instanceof ArrayPick
  );
}

/**
 * @template {ObjectShape} T
 */
class RowPick {
  /** @type {ObjectType<T>} */
  #type;

  /**
   * @param {ObjectType<T>} type
   */
  constructor(type) {
    this.#type = type;
  }

  /**
   * @template {keyof T} K
   * @param {K} fieldName
   * @returns {PickOf<T[K]>}
   */
  field(fieldName) {
    const context = {
      field: fieldName,
      path: [],
    };
    return makePick(this.#type.properties[fieldName], context);
  }
}

/**
 * @template {Type} T
 * @param {T} type
 * @param {Context} context
 * @returns {PickOfType<T>}
 */
function makeTypePick(type, context) {
  if (type instanceof ArrayType) {
    return /** @type {any} */ (new ArrayPick(type, context));
  }
  if (type instanceof ObjectType) {
    return /** @type {any} */ (new ObjectPick(type, context));
  }
  if (type instanceof ObjectMapType) {
    const newType = new RecordType(string, type.valuesType);
    return /** @type {any} */ (new RecordPick(newType, context));
  }
  if (type instanceof RecordType) {
    return /** @type {any} */ (new RecordPick(type, context));
  }
  return /** @type {any} */ (new ValuePick(type, context));
}

/**
 * @template {AnyTypeOrShape} T
 * @param {T} typeOrShape
 * @param {Context} context
 * @returns {PickOf<T>}
 */
function makePick(typeOrShape, context) {
  const type = getTypeInstance(typeOrShape);
  return /** @type {any} */ (makeTypePick(type, context));
}

/**
 * @template {ObjectShape} T
 * @param {ObjectType<T>} type
 * @returns {RowPick<T>}
 */
function rowPick(type) {
  return new RowPick(type);
}

module.exports = {
  makeTypePick,
  makePick,
  rowPick,
  isAnyPick,
  ValuePick,
  ObjectPick,
  ArrayPick,
  RowPick,
};
