// @ts-check

const {ObjectType, getTypeInstance} = require('xcraft-core-stones');
const operators = require('./operators.js');
const {rowPick, ValuePick, RowPick, ObjectPick} = require('./picks.js');
const {queryToSql} = require('./query-to-sql.js');

/**
 * @typedef {import("./operators.js").Operator} Operator
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
 *   select: AnyPick[] | Record<string, AnyPick> | AnyPick,
 *   selectOneField?: boolean,
 *   where?: Operator,
 *   orderBy?: ValuePick<any> | ValuePick<any>[]
 * }} QueryObj
 */

/**
 * @template {keyof QueryObj} K
 * @typedef {flatten<Pick<Required<QueryObj>, K> & Omit<Partial<QueryObj>, K>>} QueryParts
 */

function mergeWhere(currentWhere, newWhere) {
  if (!currentWhere) {
    return newWhere;
  }
  return operators.and(currentWhere, newWhere);
}

/**
 * @template R
 */
class FinalQuery {
  #database;
  #queryParts;

  /**
   * @param {*} database
   * @param {QueryParts<'db' | 'from' | 'select'>} queryParts
   */
  constructor(database, queryParts) {
    this.#database = database;
    this.#queryParts = queryParts;
  }

  #getStatement() {
    const {sql, values} = queryToSql(this.#queryParts);
    return this.#database.prepare(sql).bind(values);
  }

  /**
   * @returns {QueryObj}
   */
  get query() {
    return this.#queryParts;
  }

  /**
   * @returns {R}
   */
  get() {
    const result = this.#getStatement().get();
    if (result && this.#queryParts.selectOneField) {
      return Object.values(result)[0];
    }
    return result;
  }

  /**
   * @returns {R[]}
   */
  all() {
    if (this.#queryParts.selectOneField) {
      return Array.from(this.#getStatement().raw().iterate(), (row) => row[0]);
    }
    return this.#getStatement().all();
  }

  /**
   * @returns {Generator<R>}
   */
  *iterate() {
    if (this.#queryParts.selectOneField) {
      for (const row of this.#getStatement().raw().iterate()) {
        yield row[0];
      }
      return;
    }
    yield* this.#getStatement().iterate();
  }
}

/**
 * @template {Record<string, any>} T
 * @template R
 * @extends {FinalQuery<R>}
 */
class ScopedSelectQuery extends FinalQuery {
  #database;
  #type;
  #queryParts;

  /**
   * @param {*} database
   * @param {ObjectTypeOf<T>} type
   * @param {QueryParts<'db' | 'from' | 'select' | 'scope'>} queryParts
   */
  constructor(database, type, queryParts) {
    super(database, queryParts);
    this.#database = database;
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
      where: mergeWhere(this.#queryParts.where, fct(scope, operators)),
    };
    return new ScopedSelectQuery(this.#database, this.#type, queryParts);
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
    return new ScopedSelectQuery(this.#database, this.#type, queryParts);
  }
}

/**
 * @template {Record<string, any>} T
 * @template R
 * @extends {FinalQuery<R>}
 */
class SelectQuery extends FinalQuery {
  #database;
  #type;
  #queryParts;

  /**
   * @param {*} database
   * @param {ObjectTypeOf<T>} type
   * @param {QueryParts<'db' | 'from' | 'select'>} queryParts
   */
  constructor(database, type, queryParts) {
    super(database, queryParts);
    this.#database = database;
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
      where: mergeWhere(
        this.#queryParts.where,
        fct(rowPick(this.#type), operators)
      ),
    };
    return new SelectQuery(this.#database, this.#type, queryParts);
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
    return new SelectQuery(this.#database, this.#type, queryParts);
  }
}

/**
 * @template {Record<string, any>} T
 */
class ScopedFromQuery {
  #database;
  #type;
  #queryParts;

  /**
   * @param {*} database
   * @param {ObjectTypeOf<T>} type
   * @param {QueryParts<'db' | 'from' | 'scope'>} queryParts
   */
  constructor(database, type, queryParts) {
    this.#database = database;
    this.#type = type;
    this.#queryParts = queryParts;
  }

  /**
   * Select field
   * @template {keyof T} F
   * @param {F} value
   * @returns {ScopedSelectQuery<T, T[F]>}
   */
  field(value) {
    const scope = this.#queryParts.scope;
    const queryParts = {
      ...this.#queryParts,
      select: scope.get(value),
      selectOneField: true,
    };
    return new ScopedSelectQuery(this.#database, this.#type, queryParts);
  }

  /**
   * Select fields
   * @template {(keyof T)[]} F
   * @param {F} values
   * @returns {ScopedSelectQuery<T, flatten<Pick<T, F[number]>>>}
   */
  fields(values) {
    const scope = this.#queryParts.scope;
    const queryParts = {
      ...this.#queryParts,
      select: values.map((value) => scope.get(value)),
    };
    return new ScopedSelectQuery(this.#database, this.#type, queryParts);
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
    return new ScopedSelectQuery(this.#database, this.#type, queryParts);
  }

  /**
   * @param {(obj: ObjectPick<T>, $: typeof operators) => Operator} fct
   * @returns {ScopedFromQuery<T>}
   */
  where(fct) {
    const scope = this.#queryParts.scope;
    const queryParts = {
      ...this.#queryParts,
      where: mergeWhere(this.#queryParts.where, fct(scope, operators)),
    };
    return new ScopedFromQuery(this.#database, this.#type, queryParts);
  }
}

/**
 * @template {Record<string, any>} T
 */
class FromQuery {
  #database;
  #type;
  #queryParts;

  /**
   * @param {*} database
   * @param {ObjectTypeOf<T>} type
   * @param {QueryParts<'db' | 'from'>} queryParts
   */
  constructor(database, type, queryParts) {
    this.#database = database;
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
    return new ScopedFromQuery(this.#database, value.type, queryParts);
  }

  /**
   * Select fields
   * @template {(keyof T)[]} F
   * @param {F} fields
   * @returns {SelectQuery<T, flatten<Pick<T, F[number]>>>}
   */
  fields(fields) {
    const row = rowPick(this.#type);
    const queryParts = {
      ...this.#queryParts,
      select: fields.map((fieldName) => row.field(fieldName)),
    };
    return new SelectQuery(this.#database, this.#type, queryParts);
  }

  /**
   * @template {Record<string, any>} R
   * @param {(obj: RowPick<T>) => {[K in keyof R]: PickOf<R[K]>}} fct
   * @returns {SelectQuery<T, R>}
   */
  select(fct) {
    const selectedFields = fct(rowPick(this.#type));
    const queryParts = {...this.#queryParts, select: selectedFields};
    return new SelectQuery(this.#database, this.#type, queryParts);
  }

  /**
   * @param {(obj: RowPick<T>, $: typeof operators) => Operator} fct
   * @returns {FromQuery<T>}
   */
  where(fct) {
    const queryParts = {
      ...this.#queryParts,
      where: mergeWhere(
        this.#queryParts.where,
        fct(rowPick(this.#type), operators)
      ),
    };
    return new FromQuery(this.#database, this.#type, queryParts);
  }
}

class DbQuery {
  #database;
  #queryParts;

  /**
   * @param {*} database
   * @param {QueryParts<'db'>} queryParts
   */
  constructor(database, queryParts) {
    this.#database = database;
    this.#queryParts = queryParts;
  }

  /**
   * @template {AnyObjectShape} T
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
    return /** @type {any} */ (new FromQuery(this.#database, type, queryParts));
  }
}

class QueryBuilder {
  #database;

  /**
   * @param {*} [database]
   */
  constructor(database) {
    this.#database = database;
  }

  db(dbName) {
    const queryParts = {
      db: dbName,
    };
    return new DbQuery(this.#database, queryParts);
  }
}

module.exports = {
  QueryBuilder,
  FromQuery,
  ScopedFromQuery,
};
