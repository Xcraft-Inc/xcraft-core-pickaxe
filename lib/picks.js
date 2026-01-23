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
  OptionType,
  union,
  UnionType,
} = require('xcraft-core-stones');

const $o = require('./operators.js');
const {optionalObjectType} = require('./optional-object-type.js');

/**
 * @typedef {import('./operators.js').Operator} Operator
 */

/**
 * @typedef {BasePick<BooleanType>} BooleanValue
 */

/**
 * @template T
 * @template {keyof T} K
 * @typedef {Pick<T, K>[keyof Pick<T, K>]} SelectValues
 */

/**
 * @template {Type} T
 * @typedef { [T] extends [ArrayType<infer V>] ? ArrayPick<V> : [T] extends [ObjectType<infer S>] ? ObjectPick<S> : [T] extends [ObjectMapType<infer V>] ? RecordPick<StringType,V> : [T] extends [RecordType<infer K,infer V>] ? RecordPick<K,V> : [T] extends [NumberType] ? NumberPick : [T] extends [StringType<infer U>] ? StringPick<U> : [T] extends [OptionType<infer U>] ? OptionPick<U> : ValuePick<T>} PickOfType
 */
/**
 * @template {AnyTypeOrShape} T
 * @typedef {PickOfType<GetType<T>>} PickOf
 */

/**
 * @template {AnyTypeOrShape} T
 * @typedef {T extends OptionType<infer U> ? (T | U) : T} OptionToUnion
 */

/**
 * @template {AnyTypeOrShape} T
 */
class BasePick {
  /** @type {T} */
  #type;
  /** @type {Operator | null} */
  #expression;

  /**
   * @param {T} type
   * @param {Operator | null} expression
   */
  constructor(type, expression) {
    this.#type = type;
    this.#expression = expression;
  }

  get type() {
    return this.#type;
  }

  get expression() {
    if (!this.#expression) {
      throw new Error('Missing expression');
    }
    return this.#expression;
  }
}

/**
 * @template {AnyTypeOrShape} T
 * @extends {BasePick<T>}
 */
class ValuePick extends BasePick {
  /**
   * @param {T} type
   * @param {Operator} expression
   */
  constructor(type, expression) {
    super(type, expression);
  }

  /**
   * @template {AnyObjectShape} T
   * @param {T} shape
   */
  asObject(shape) {
    return new ObjectPick(toObjectType(shape), this.expression);
  }

  /**
   * @param {t<T> | BasePick<OptionToUnion<T>>} value
   */
  eq(value) {
    return new ValuePick(boolean, $o.eq(this.expression, value));
  }

  /**
   * @param {t<T> | BasePick<T>} value
   */
  neq(value) {
    return new ValuePick(boolean, $o.neq(this.expression, value));
  }

  /**
   * @param {t<T> | BasePick<T>} value
   */
  gte(value) {
    return new ValuePick(boolean, $o.gte(this.expression, value));
  }

  /**
   * @param {t<T> | BasePick<T>} value
   */
  gt(value) {
    return new ValuePick(boolean, $o.gt(this.expression, value));
  }

  /**
   * @param {t<T> | BasePick<T>} value
   */
  lte(value) {
    return new ValuePick(boolean, $o.lte(this.expression, value));
  }

  /**
   * @param {t<T> | BasePick<T>} value
   */
  lt(value) {
    return new ValuePick(boolean, $o.lt(this.expression, value));
  }

  /**
   * @param {(t<T> | BasePick<T>)[]} list
   */
  in(list) {
    return new ValuePick(boolean, $o.in(this.expression, list));
  }

  /**
   * @param {any} value
   */
  match(value) {
    return new ValuePick(boolean, $o.match(this.expression, value));
  }
}

/**
 * @template {string} [T=string]
 * @extends {ValuePick<StringType<T>>}
 */
class StringPick extends ValuePick {
  /**
   * @param {string | BasePick<StringType>} value
   */
  like(value) {
    return new ValuePick(boolean, $o.like(this.expression, value));
  }

  /**
   * @param {string | BasePick<StringType>} value
   */
  glob(value) {
    return new ValuePick(boolean, $o.glob(this.expression, value));
  }

  get length() {
    return new NumberPick(number, $o.stringLength(this.expression));
  }

  /**
   * @param {number | BasePick<NumberType>} start
   * @param {number | BasePick<NumberType>} [length]
   */
  substr(start, length) {
    return new StringPick(string, $o.substr(this.expression, start, length));
  }
}

/**
 * @extends {ValuePick<NumberType>}
 */
class NumberPick extends ValuePick {
  abs() {
    return new NumberPick(number, $o.abs(this.expression));
  }

  /**
   * @param {number | BasePick<NumberType>} value
   */
  plus(value) {
    return new NumberPick(number, $o.plus(this.expression, value));
  }

  /**
   * @param {number | BasePick<NumberType>} value
   */
  minus(value) {
    return new NumberPick(number, $o.minus(this.expression, value));
  }
}

/**
 * @template {AnyTypeOrShape} T
 * @extends {ValuePick<OptionType<T>>}
 */
class OptionPick extends ValuePick {
  /**
   * @template {AnyTypeOrShape} U
   * @param {BasePick<U>} value
   * @returns {PickOfType<UnionType<[T, U]>>}
   */
  ifNull(value) {
    const newType = union(this.type.subType, value.type);
    return makeTypePick(newType, $o.ifNull(this.expression, value));
  }

  /**
   * @param {(value: PickOf<T>) => BooleanValue} func
   */
  isNullOr(func) {
    return new ValuePick(
      boolean,
      $o.or(
        $o.eq(this.expression, null),
        func(makePick(this.type.subType, this.expression))
      )
    );
  }

  /**
   * @param {(value: PickOf<T>) => BooleanValue} func
   */
  isNotNullAnd(func) {
    return new ValuePick(
      boolean,
      $o.and(
        $o.neq(this.expression, null),
        func(makePick(this.type.subType, this.expression))
      )
    );
  }

  /**
   * @returns {PickOf<T>}
   */
  unsafeUnwrap() {
    return makePick(this.type.subType, this.expression);
  }
}

/**
 * @template {ObjectShape} T
 * @extends {BasePick<ObjectType<T>>}
 */
class ObjectPick extends BasePick {
  /**
   * @param {ObjectType<T>} type
   * @param {Operator} expression
   */
  constructor(type, expression) {
    super(type, expression);
  }

  toRowPick() {
    return new RowPick(this.type, {expression: this.expression});
  }

  /**
   * @template {keyof T} K
   * @param {K} key
   * @returns {PickOf<T[K]>}
   */
  get(key) {
    return makePick(this.type.properties[key], $o.get(this.expression, [key]));
  }

  /**
   * @param {t<T[keyof T]> | BasePick<T[keyof T]>} value
   */
  includes(value) {
    return new ValuePick(boolean, $o.includes(this.expression, value));
  }
}

/**
 * @template {AnyTypeOrShape} K
 * @template {AnyTypeOrShape} V
 * @extends {BasePick<RecordType<K,V>>}
 */
class RecordPick extends BasePick {
  /**
   * @param {RecordType<K,V>} type
   * @param {Operator} expression
   */
  constructor(type, expression) {
    super(type, expression);
  }

  /**
   * @param {t<K>} key
   * @returns {PickOf<V>}
   */
  get(key) {
    return makePick(this.type.valuesType, $o.get(this.expression, [key]));
  }

  /**
   * @param {t<V> | BasePick<V>} value
   */
  includes(value) {
    return new ValuePick(boolean, $o.includes(this.expression, value));
  }

  /**
   * @param {(value: PickOf<V>, key: PickOf<K>) => BooleanValue} func
   */
  some(func) {
    return new ValuePick(
      boolean,
      $o.some(
        this.expression,
        func(
          makePick(this.type.valuesType, $o.eachValue()),
          makePick(this.type.keysType, $o.eachKey())
        )
      )
    );
  }

  /**
   * @param {(value: PickOf<V>, key: PickOf<K>) => BooleanValue} func
   */
  every(func) {
    return new ValuePick(
      boolean,
      $o.not(
        this.some((...args) => new ValuePick(boolean, $o.not(func(...args))))
          .expression
      )
    );
  }

  /**
   * @param {(value: PickOf<V>, key: PickOf<K>) => BasePick} fct
   */
  select(fct) {
    return $o.query({
      from: $o.each(this.expression),
      select: [
        fct(
          makePick(this.type.valuesType, $o.eachValue()),
          makePick(this.type.keysType, $o.eachKey())
        ),
      ],
    });
  }

  /**
   * @returns {ArrayPick<K>}
   */
  keys() {
    return new ArrayPick(array(this.type.keysType), $o.keys(this.expression));
  }

  /**
   * @returns {ArrayPick<V>}
   */
  values() {
    return new ArrayPick(
      array(this.type.valuesType),
      $o.values(this.expression)
    );
  }
}

/**
 * @template {AnyTypeOrShape} T
 * @extends {BasePick<ArrayType<T>>}
 */
class ArrayPick extends BasePick {
  /**
   * @param {ArrayType<T>} type
   * @param {Operator} expression
   */
  constructor(type, expression) {
    super(type, expression);
  }

  /**
   * @param {number} index
   * @returns {PickOf<T>}
   */
  get(index) {
    return makePick(this.type.valuesType, $o.get(this.expression, [index]));
  }

  get length() {
    return new NumberPick(number, $o.length(this.expression));
  }

  /**
   * @param {t<T> | BasePick<T>} value
   */
  includes(value) {
    return new ValuePick(boolean, $o.includes(this.expression, value));
  }

  /**
   * @param {(value: PickOf<T>) => BooleanValue} func
   */
  some(func) {
    return new ValuePick(
      boolean,
      $o.some(
        this.expression,
        func(makePick(this.type.valuesType, $o.eachValue()))
      )
    );
  }

  /**
   * @param {(value: PickOf<T>) => BooleanValue} func
   */
  every(func) {
    return new ValuePick(
      boolean,
      $o.not(
        this.some((...args) => new ValuePick(boolean, $o.not(func(...args))))
          .expression
      )
    );
  }

  /**
   * @param {(value: PickOf<T>) => BasePick} fct
   */
  select(fct) {
    return $o.query({
      from: $o.each(this.expression),
      select: [fct(makePick(this.type.valuesType, $o.eachValue()))],
    });
  }
}

/**
 * @param {{}} value
 * @returns {value is BasePick<any>}
 */
function isAnyPick(value) {
  return value instanceof BasePick;
}

/**
 * @template {ObjectShape} T
 * @extends {BasePick<ObjectType<T>>}
 */
class RowPick extends BasePick {
  /**
   * @type {string | null}
   */
  #tableName = null;

  /**
   * @param {ObjectType<T>} type
   * @param {{expression: Operator} | {tableName: string}} context
   */
  constructor(type, context) {
    if ('expression' in context) {
      super(type, context.expression);
    } else {
      super(type, null);
      this.#tableName = context.tableName;
    }
  }

  /**
   * @template {keyof T} K
   * @param {K} fieldName
   * @returns {PickOf<T[K]>}
   */
  field(fieldName) {
    if (this.#tableName) {
      return makePick(
        this.type.properties[fieldName],
        $o.field(fieldName, this.#tableName)
      );
    }
    return makePick(
      this.type.properties[fieldName],
      $o.get(this.expression, [fieldName])
    );
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

  isRoot() {
    return Boolean(this.#tableName);
  }

  toOptional() {
    return new RowPick(
      optionalObjectType(this.type),
      this.#tableName
        ? {tableName: this.#tableName}
        : {expression: this.expression}
    );
  }
}

/**
 * @template {Type} T
 * @param {T} type
 * @param {Operator} expression
 * @returns {PickOfType<T>}
 */
function makeTypePick(type, expression) {
  if (type instanceof ArrayType) {
    return /** @type {any} */ (new ArrayPick(type, expression));
  }
  if (type instanceof ObjectType) {
    return /** @type {any} */ (new ObjectPick(type, expression));
  }
  if (type instanceof ObjectMapType) {
    const newType = new RecordType(string, type.valuesType);
    return /** @type {any} */ (new RecordPick(newType, expression));
  }
  if (type instanceof RecordType) {
    return /** @type {any} */ (new RecordPick(type, expression));
  }
  if (type instanceof NumberType) {
    return /** @type {any} */ (new NumberPick(type, expression));
  }
  if (
    type instanceof StringType ||
    (type instanceof EnumerationType &&
      type.values.every((value) => typeof value === 'string'))
  ) {
    return /** @type {any} */ (new StringPick(type, expression));
  }
  if (type instanceof OptionType) {
    return /** @type {any} */ (new OptionPick(type, expression));
  }
  return /** @type {any} */ (new ValuePick(type, expression));
}

/**
 * @template {AnyTypeOrShape} T
 * @param {T} typeOrShape
 * @param {Operator} expression
 * @returns {PickOf<T>}
 */
function makePick(typeOrShape, expression) {
  const type = getTypeInstance(typeOrShape);
  return /** @type {any} */ (makeTypePick(type, expression));
}

/**
 * @template {ObjectShape} T
 * @param {ObjectType<T>} type
 * @param {string} [tableName]
 * @returns {RowPick<T>}
 */
function rowPick(type, tableName) {
  if (!tableName) {
    throw new Error('Missing tableName in RowPick');
  }
  return new RowPick(type, {tableName});
}

module.exports = {
  makeTypePick,
  makePick,
  rowPick,
  isAnyPick,
  BasePick,
  ValuePick,
  NumberPick,
  StringPick,
  ObjectPick,
  RecordPick,
  ArrayPick,
  RowPick,
};
