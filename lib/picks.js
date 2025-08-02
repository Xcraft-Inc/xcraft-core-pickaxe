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
  NumberType,
  EnumerationType,
  toObjectType,
  boolean,
  BooleanType,
} = require('xcraft-core-stones');

const $o = require('./operators.js');

/**
 * @typedef {import('./operators.js').Operators} Operators
 * @typedef {import('./operators.js').Operator} Operator
 * @typedef {import('./operators.js').Aggregator} Aggregator
 * @typedef {import("./operators.js").BooleanOperator} BooleanOperator
 */

/**
 * @typedef {(keyof any)} PathElement
 */
/**
 * @typedef {PathElement[]} Path
 */

/**
 * @typedef {ValuePick<BooleanType> | BooleanOperator} BooleanValue
 */

/**
 * @template T
 * @template {keyof T} K
 * @typedef {Pick<T, K>[keyof Pick<T, K>]} SelectValues
 */

/**
 * @typedef {{
 *   field: (keyof any) | SelectValues<Operators, "eachValue" | "eachKey" | "keys" | "values" | "length">,
 *   path: Path,
 *   tableName?: string
 * }} Context
 */

/**
 * @typedef {ValuePick<any> | ArrayPick<any> | ObjectPick<any> | RecordPick<any,any>} AnyPick
 */

/**
 * @template {Type} T
 * @typedef { [T] extends [ArrayType<infer V>] ? ArrayPick<V> : [T] extends [ObjectType<infer S>] ? ObjectPick<S> : [T] extends [ObjectMapType<infer V>] ? RecordPick<StringType,V> : [T] extends [RecordType<infer K,infer V>] ? RecordPick<K,V> : [T] extends [NumberType] ? NumberPick : [T] extends [StringType] ? StringPick : ValuePick<T>} PickOfType
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
    const {tableName, field, path} = this.#context;
    if (path.length > 0) {
      return $o.get($o.field(field, tableName), path);
    }
    return $o.field(field, tableName);
  }

  get type() {
    return this.#type;
  }

  /**
   * @template {AnyObjectShape} T
   * @param {T} shape
   */
  asObject(shape) {
    return new ObjectPick(toObjectType(shape), this.#context);
  }

  /**
   * @param {t<T> | ValuePick<T>} value
   */
  eq(value) {
    return new ValuePick(boolean, {
      field: $o.eq(this.value, value),
      path: [],
    });
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
   * @param {any} value
   */
  match(value) {
    return $o.match(this.value, value);
  }
}

/**
 * @extends {ValuePick<StringType>}
 */
class StringPick extends ValuePick {
  /**
   * @param {string | ValuePick<StringType>} value
   */
  like(value) {
    return $o.like(this.value, value);
  }

  /**
   * @param {string | ValuePick<StringType>} value
   */
  glob(value) {
    return $o.glob(this.value, value);
  }

  get length() {
    return new NumberPick(number, {
      field: $o.stringLength(this.value),
      path: [],
    });
  }

  /**
   * @param {number | ValuePick<NumberType>} start
   * @param {number | ValuePick<NumberType>} [length]
   */
  substr(start, length) {
    return new StringPick(string, {
      field: $o.substr(this.value, start, length),
      path: [],
    });
  }
}

/**
 * @extends {ValuePick<NumberType>}
 */
class NumberPick extends ValuePick {
  abs() {
    return new NumberPick(number, {
      field: $o.abs(this.value),
      path: [],
    });
  }

  /**
   * @param {number | ValuePick<NumberType>} value
   */
  plus(value) {
    return new NumberPick(number, {
      field: $o.plus(this.value, value),
      path: [],
    });
  }

  /**
   * @param {number | ValuePick<NumberType>} value
   */
  minus(value) {
    return new NumberPick(number, {
      field: $o.minus(this.value, value),
      path: [],
    });
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
    const {tableName, field, path} = this.#context;
    if (path.length > 0) {
      return $o.get($o.field(field, tableName), path);
    }
    return $o.field(field, tableName);
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
    const {tableName, field, path} = this.#context;
    if (path.length > 0) {
      return $o.get($o.field(field, tableName), path);
    }
    return $o.field(field, tableName);
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
   * @param {(value: PickOf<V>, key: PickOf<K>) => Operator} func
   */
  every(func) {
    return $o.not(this.some((...args) => $o.not(func(...args))));
  }

  /**
   * @param {(value: PickOf<V>, key: PickOf<K>) => Aggregator} fct
   */
  select(fct) {
    return $o.query({
      from: $o.each(this.value),
      select: [
        fct(
          makePick(this.#type.valuesType, {
            field: $o.eachValue(),
            path: [],
          }),
          makePick(this.#type.keysType, {
            field: $o.eachKey(),
            path: [],
          })
        ),
      ],
    });
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
    const {tableName, field, path} = this.#context;
    if (path.length > 0) {
      return $o.get($o.field(field, tableName), path);
    }
    return $o.field(field, tableName);
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
    return new NumberPick(number, {
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
   * @param {(value: PickOf<T>) => BooleanValue} func
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

  /**
   * @param {(value: PickOf<T>) => Operator} func
   */
  every(func) {
    return $o.not(this.some((...args) => $o.not(func(...args))));
  }

  /**
   * @param {(value: PickOf<T>) => Aggregator} fct
   */
  select(fct) {
    return $o.query({
      from: $o.each(this.value),
      select: [
        fct(
          makePick(this.#type.valuesType, {
            field: $o.eachValue(),
            path: [],
          })
        ),
      ],
    });
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
  /** @type {string} */
  #tableName;

  /**
   * @param {ObjectType<T>} type
   * @param {string} tableName
   */
  constructor(type, tableName) {
    this.#type = type;
    this.#tableName = tableName;
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
      tableName: this.#tableName,
    };
    return makePick(this.#type.properties[fieldName], context);
  }

  /**
   * Alias of field method to be symmetric with ObjectPick
   * @template {keyof T} K
   * @param {K} fieldName
   * @returns {PickOf<T[K]>}
   */
  get(fieldName) {
    return this.field(fieldName);
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
  if (type instanceof NumberType) {
    return /** @type {any} */ (new NumberPick(type, context));
  }
  if (
    type instanceof StringType ||
    (type instanceof EnumerationType &&
      type.values.every((value) => typeof value === 'string'))
  ) {
    return /** @type {any} */ (new StringPick(type, context));
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
 * @param {string} tableName
 * @returns {RowPick<T>}
 */
function rowPick(type, tableName) {
  return new RowPick(type, tableName);
}

module.exports = {
  makeTypePick,
  makePick,
  rowPick,
  isAnyPick,
  ValuePick,
  ObjectPick,
  RecordPick,
  ArrayPick,
  RowPick,
};
