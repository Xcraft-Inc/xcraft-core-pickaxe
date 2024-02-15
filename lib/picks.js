/* eslint-disable jsdoc/require-returns */
// @ts-check

const {
  ArrayType,
  ObjectType,
  Type,
  getTypeInstance,
} = require('xcraft-core-stones');

const $o = require('./operators.js');

/**
 * @typedef {import('./operators.js').Operators} Operators
 */
/**
 * @typedef {import('./operators.js').Operator} Operator
 */

/**
 * @typedef {boolean | number | bigint | string | symbol | undefined | null} primitive
 */
/**
 * @typedef {primitive | Function | Date | Error | RegExp} Builtin
 */

/**
 * @typedef {(keyof any)} PathElement
 */
/**
 * @typedef {PathElement[]} Path
 */

/**
 * @typedef {{field: Operators["someValue"] | (keyof any), path: Path}} Context
 */

/**
 * @template T
 * @typedef {0 extends (1 & T) ? true : never} IsAny
 */

/**
 * @typedef {ValuePick<any> | ArrayPick<any> | ObjectPick<any>} AnyPick
 */

/**
 * @template T
 * @typedef {true extends IsAny<T> ? AnyPick : [T] extends [Builtin] ? ValuePick<T> : [T] extends [Array<infer V>] ? ArrayPick<V> : [T] extends [{}] ? ObjectPick<T> : never} PickOf
 */

/**
 * @template {{}} T
 * @typedef {ObjectType<{[K in keyof T] : Type<T[K]>}>} ObjectTypeOf
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
 * @template T
 */
class ValuePick {
  /** @type {Type<T>} */
  #type;
  /** @type {Context} */
  #context;

  /**
   * @param {Type<T>} type
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
   * @param {T | ValuePick<T>} value
   */
  eq(value) {
    return $o.eq(this.value, value);
  }

  /**
   * @param {T | ValuePick<T>} value
   */
  neq(value) {
    return $o.neq(this.value, value);
  }

  /**
   * @param {T | ValuePick<T>} value
   */
  gte(value) {
    return $o.gte(this.value, value);
  }

  /**
   * @param {T | ValuePick<T>} value
   */
  gt(value) {
    return $o.gt(this.value, value);
  }

  /**
   * @param {T | ValuePick<T>} value
   */
  lte(value) {
    return $o.lte(this.value, value);
  }

  /**
   * @param {T | ValuePick<T>} value
   */
  lt(value) {
    return $o.lt(this.value, value);
  }

  /**
   * @param {(T | ValuePick<T>)[]} list
   */
  in(list) {
    return $o.in(this.value, list);
  }

  /**
   * @param {string | ValuePick<string>} value
   */
  like(value) {
    return $o.like(this.value, value);
  }
}

/**
 * @template {{}} T
 */
class ObjectPick {
  /** @type {ObjectTypeOf<T>} */
  #type;
  /** @type {Context} */
  #context;

  /**
   * @param {ObjectTypeOf<T>} type
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
    return makeTypePick(
      getTypeInstance(this.#type.properties[key]),
      contextWithPath(this.#context, key)
    );
  }
}

/**
 * @template T
 */
class ArrayPick {
  /** @type {ArrayType<Type<T>>} */
  #type;
  /** @type {Context} */
  #context;

  /**
   * @param {ArrayType<Type<T>>} type
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
    return makeTypePick(
      getTypeInstance(this.#type.valuesType),
      contextWithPath(this.#context, index)
    );
  }

  /**
   * @param {T | ValuePick<T>} value
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
        makeTypePick(getTypeInstance(this.#type.valuesType), {
          field: $o.someValue(),
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
    value instanceof ArrayPick
  );
}

/**
 * @template {Record<string,any>} T
 */
class RowPick {
  /** @type {ObjectTypeOf<T>} */
  #type;

  /**
   * @param {ObjectTypeOf<T>} type
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
    return makeTypePick(
      getTypeInstance(this.#type.properties[fieldName]),
      context
    );
  }
}

/**
 * @template T
 * @param {Type<T>} type
 * @param {Context} context
 * @returns {PickOf<T>}
 */
function makeTypePick(type, context) {
  if (type instanceof ArrayType) {
    return /** @type {any} */ (new ArrayPick(type, context));
  }
  if (type instanceof ObjectType) {
    return /** @type {any} */ (new ObjectPick(type, context));
  }
  return /** @type {any} */ (new ValuePick(type, context));
}

/**
 * @template {AnyTypeOrShape} T
 * @param {T} typeOrShape
 * @param {Context} context
 * @returns {PickOf<t<T>>}
 */
function makePick(typeOrShape, context) {
  const type = getTypeInstance(typeOrShape);
  return /** @type {any} */ (makeTypePick(type, context));
}

/**
 * @template {Record<string,any>} T
 * @param {ObjectTypeOf<T>} type
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
