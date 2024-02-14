// @ts-check

const {ObjectType} = require('xcraft-core-stones');
const operators = require('./operators.js');
const getTypeInstance = require('xcraft-core-stones/get-type-instance.js');
const {rowPick, ValuePick, RowPick, ObjectPick} = require('./picks.js');

/**
 * @typedef {import("./operators.js").Operator} Operator
 */
/**
 * @typedef {import("xcraft-core-stones").AnyTypeOrShape} AnyTypeOrShape
 * @typedef {import("xcraft-core-stones/base-types.js").ClassShape} ClassShape
 * @typedef {import("xcraft-core-stones/base-types.js").ObjectShape} ObjectShape
 */

/**
 * @template T
 * @typedef {import("xcraft-core-stones").t<T>} t
 */
/**
 * @template {ObjectShape} S
 * @typedef {import('xcraft-core-stones/base-types.js').objectType<S>} objectType
 */
/**
 * @template {ClassShape} T
 * @typedef {import('xcraft-core-stones/base-types.js').classType<T>} classType
 */
/**
 * @template T
 * @typedef {import('xcraft-core-stones/base-types.js').flatten<T>} flatten
 */
/**
 * @template T
 * @typedef {import("./picks.js").PickOf<T>} PickOf
 */
/**
 * @typedef {import("./picks.js").AnyPick} AnyPick
 */
/**
 * @typedef {import("./picks.js").Path} Path
 */
/**
 * @template {{}} T
 * @typedef {import("./picks.js").ObjectTypeOf<T>} ObjectTypeOf
 */

/**
 * @typedef {{
 *   db: string,
 *   from: string,
 *   scope?: ObjectPick<any>
 *   select: AnyPick[] | Record<string, AnyPick>,
 *   where?: Operator,
 *   orderBy?: ValuePick<any> | ValuePick<any>[]
 * }} QueryObj
 */

/**
 * @template {keyof QueryObj} K
 * @typedef {flatten<Pick<Required<QueryObj>, K> & Omit<Partial<QueryObj>, K>>} QueryParts
 */

/**
 * @template R
 */
class FinalQuery {
  #queryParts;

  /**
   * @param {QueryParts<'db' | 'from' | 'select'>} queryParts
   */
  constructor(queryParts) {
    this.#queryParts = queryParts;
  }

  /**
   * @returns {QueryObj}
   */
  get query() {
    return this.#queryParts;
  }

  /**
   * @returns {Promise<R[]>}
   */
  async all() {
    // TODO
    return [];
  }

  async *iterate() {
    // TODO
    for (const row of []) {
      yield row;
    }
  }
}

/**
 * @template {Record<string, any>} T
 * @template R
 * @extends {FinalQuery<R>}
 */
class ScopedSelectQuery extends FinalQuery {
  #type;
  #queryParts;

  /**
   * @param {ObjectTypeOf<T>} type
   * @param {QueryParts<'db' | 'from' | 'select' | 'scope'>} queryParts
   */
  constructor(type, queryParts) {
    super(queryParts);
    this.#type = type;
    this.#queryParts = queryParts;
  }

  /**
   * @param {(obj: ObjectPick<T>, $: typeof operators) => Operator} fct
   * @returns {ScopedSelectQuery<T,R>}
   */
  where(fct) {
    const scope = this.#queryParts.scope;
    const queryParts = {
      ...this.#queryParts,
      where: fct(scope, operators),
    };
    return new ScopedSelectQuery(this.#type, queryParts);
  }

  /**
   * @param {(obj: ObjectPick<T>, $: typeof operators) => ValuePick<any> | ValuePick<any>[]} fct
   * @returns {ScopedSelectQuery<T,R>}
   */
  orderBy(fct) {
    const scope = this.#queryParts.scope;
    const queryParts = {
      ...this.#queryParts,
      orderBy: fct(scope, operators),
    };
    return new ScopedSelectQuery(this.#type, queryParts);
  }
}

/**
 * @template {Record<string, any>} T
 * @template R
 * @extends {FinalQuery<R>}
 */
class SelectQuery extends FinalQuery {
  #type;
  #queryParts;

  /**
   * @param {ObjectTypeOf<T>} type
   * @param {QueryParts<'db' | 'from' | 'select'>} queryParts
   */
  constructor(type, queryParts) {
    super(queryParts);
    this.#type = type;
    this.#queryParts = queryParts;
  }

  /**
   * @param {(obj: RowPick<T>, $: typeof operators) => Operator} fct
   * @returns {SelectQuery<T,R>}
   */
  where(fct) {
    const queryParts = {
      ...this.#queryParts,
      where: fct(rowPick(this.#type), operators),
    };
    return new SelectQuery(this.#type, queryParts);
  }

  /**
   * @param {(obj: RowPick<T>, $: typeof operators) => ValuePick<any> | ValuePick<any>[]} fct
   * @returns {SelectQuery<T,R>}
   */
  orderBy(fct) {
    const queryParts = {
      ...this.#queryParts,
      orderBy: fct(rowPick(this.#type), operators),
    };
    return new SelectQuery(this.#type, queryParts);
  }
}

/**
 * @template {Record<string, any>} T
 */
class ScopedFromQuery {
  #type;
  #queryParts;

  /**
   * @param {ObjectTypeOf<T>} type
   * @param {QueryParts<'db' | 'from' | 'scope'>} queryParts
   */
  constructor(type, queryParts) {
    this.#type = type;
    this.#queryParts = queryParts;
  }

  /**
   * @template {(keyof T)[]} F
   * @param {F} values
   * @returns {ScopedSelectQuery<T, flatten<Pick<T, F[number]>>>}
   */
  selectFields(values) {
    const scope = this.#queryParts.scope;
    const queryParts = {
      ...this.#queryParts,
      select: values.map((value) => scope.get(value)),
    };
    return new ScopedSelectQuery(this.#type, queryParts);
  }

  /**
   * @template {Record<string, any>} R
   * @param {(obj: ObjectPick<T>) => {[K in keyof R]: PickOf<R[K]>}} fct
   * @returns {ScopedSelectQuery<T, R>}
   */
  select(fct) {
    const scope = this.#queryParts.scope;
    const selectedFields = fct(scope);
    const queryParts = {...this.#queryParts, select: selectedFields};
    return new ScopedSelectQuery(this.#type, queryParts);
  }
}

/**
 * @template {Record<string, any>} T
 */
class FromQuery {
  #type;
  #queryParts;

  /**
   * @param {ObjectTypeOf<T>} type
   * @param {QueryParts<'db' | 'from'>} queryParts
   */
  constructor(type, queryParts) {
    this.#type = type;
    this.#queryParts = queryParts;
  }

  /**
   * @template {{}} U
   * @param {(obj: RowPick<T>) => ObjectPick<U>} fct
   * @returns {ScopedFromQuery<U>}
   */
  scope(fct) {
    const value = fct(rowPick(this.#type));
    const queryParts = {
      ...this.#queryParts,
      scope: value,
    };
    return new ScopedFromQuery(value.type, queryParts);
  }

  /**
   * @template {(keyof T)[]} F
   * @param {F} fields
   * @returns {SelectQuery<T, flatten<Pick<T, F[number]>>>}
   */
  selectFields(fields) {
    const row = rowPick(this.#type);
    const queryParts = {
      ...this.#queryParts,
      select: fields.map((fieldName) => row.field(fieldName)),
    };
    return new SelectQuery(this.#type, queryParts);
  }

  /**
   * @template {Record<string, any>} R
   * @param {(obj: RowPick<T>) => {[K in keyof R]: PickOf<R[K]>}} fct
   * @returns {SelectQuery<T, R>}
   */
  select(fct) {
    const selectedFields = fct(rowPick(this.#type));
    const queryParts = {...this.#queryParts, select: selectedFields};
    return new SelectQuery(this.#type, queryParts);
  }

  // /**
  //  * @type {this["selectFields"] | this["select"]}
  //  */
  // select(arrOrFct) {
  //   if (Array.isArray(arrOrFct)) {
  //     return this.selectFields(arrOrFct);
  //   }
  //   return this.select(arrOrFct);
  // }
}

class DbQuery {
  #queryParts;

  /**
   * @param {QueryParts<'db'>} queryParts
   */
  constructor(queryParts) {
    this.#queryParts = queryParts;
  }

  /**
   * @template {ClassShape | ObjectShape | ObjectType} T
   * @param {string} tableName
   * @param {T} shape
   * @returns {FromQuery<t<T>>}
   */
  from(tableName, shape) {
    const type = getTypeInstance(shape);
    const queryParts = {
      ...this.#queryParts,
      from: tableName,
    };
    return /** @type {any} */ (new FromQuery(type, queryParts));
  }
}

class QueryBuilder {
  db(dbName) {
    const queryParts = {
      db: dbName,
    };
    return new DbQuery(queryParts);
  }
}

module.exports = {
  QueryBuilder,
  FromQuery,
  ScopedFromQuery,
};
