// @ts-check

const {
  ObjectType,
  ArrayType,
  toObjectType,
  ObjectMapType,
  RecordType,
  object,
} = require('xcraft-core-stones');
const operators = require('./operators.js');
const {rowPick, ValuePick, RowPick, ObjectPick} = require('./picks.js');
const {queryToSql} = require('./query-to-sql.js');
/**
 * @typedef {import("./operators.js").Operators} Operators
 * @typedef {import("./operators.js").Operator} Operator
 * @typedef {import("./operators.js").Aggregator} Aggregator
 * @typedef {import("./operators.js").MathOperator} MathOperator
 */

/**
 * @typedef {import("./picks.js").AnyPick} AnyPick
 */

/**
 * @typedef {AnyPick | Aggregator | MathOperator} SelectValue
 */
/**
 * @template T
 * @typedef {T extends SelectValue ? T["type"]: never} SelectValueType
 */
/**
 * @typedef {Record<string, SelectValue> | [SelectValue, ...SelectValue[]]} SelectResult
 */
/**
 * @template {SelectResult} T
 * @typedef {{[K in keyof T]: t<SelectValueType<T[K]>>}} QueryResultOf
 */

/**
 * @typedef {ValuePick<any> | Operators["desc"] | Operators["asc"]} OrderByValue
 */
/**
 * @typedef {OrderByValue | OrderByValue[]} OrderByResult
 */

/**
 * @typedef {ValuePick<any> | ValuePick<any>[]} GroupByResult
 */

/**
 * @typedef {{
 *   db: string,
 *   explain?: boolean,
 *   from: string,
 *   scope?: ObjectPick<any>
 *   select: SelectResult,
 *   selectOneField?: boolean,
 *   distinct?: boolean,
 *   where?: Operator,
 *   orderBy?: OrderByResult,
 *   groupBy?: GroupByResult
 *   limit?: number
 *   offset?: number
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

  #useRaw() {
    return (
      Array.isArray(this.#queryParts.select) ||
      Boolean(this.#queryParts.selectOneField)
    );
  }

  #getStatement() {
    const {sql, values} = queryToSql(this.#queryParts);
    return this.#database.prepare(sql).bind(values).raw(this.#useRaw());
  }

  #parseValue(value) {
    return JSON.parse(value);
  }

  #identity(value) {
    return value;
  }

  #getMapper(type) {
    if (
      type instanceof ArrayType ||
      type instanceof ObjectType ||
      type === object ||
      type instanceof ObjectMapType ||
      type instanceof RecordType
    ) {
      return this.#parseValue;
    }
    return this.#identity;
  }

  #getMappers() {
    const select = this.#queryParts.select;
    const mappers = Object.fromEntries(
      Object.entries(select).map(([name, selectValue]) => [
        name,
        this.#getMapper(selectValue.type),
      ])
    );
    return mappers;
  }

  #getRawMappers() {
    const select = this.#queryParts.select;
    const mappers = Object.values(select).map((selectValue) =>
      this.#getMapper(selectValue.type)
    );
    return mappers;
  }

  #mapRow(row, mappers) {
    for (const [name, value] of Object.entries(row)) {
      row[name] = mappers[name](value);
    }
    return row;
  }

  #mapRawRow(row, mappers) {
    for (const [i, value] of row.entries()) {
      row[i] = mappers[i](value);
    }
    return row;
  }

  #getMapRow() {
    if (this.#useRaw()) {
      const mappers = this.#getRawMappers();
      if (this.#queryParts.selectOneField) {
        return (row) => (row ? this.#mapRawRow(row, mappers)[0] : row);
      }
      return (row) => row && this.#mapRawRow(row, mappers);
    }
    const mappers = this.#getMappers();
    return (row) => row && this.#mapRow(row, mappers);
  }

  /**
   * @returns {QueryObj}
   */
  get query() {
    return this.#queryParts;
  }

  sql() {
    return queryToSql(this.#queryParts, null).sql;
  }

  /**
   * @returns {R | undefined}
   */
  get() {
    const mapRow = this.#getMapRow();
    const row = this.#getStatement().get();
    return mapRow(row);
  }

  /**
   * @returns {R[]}
   */
  all() {
    const mapRow = this.#getMapRow();
    return Array.from(this.#getStatement().iterate(), mapRow);
  }

  #resultIsEntry() {
    return (
      Array.isArray(this.#queryParts.select) &&
      this.#queryParts.select.length === 2
    );
  }

  /**
   * @returns {R extends [infer K, infer V] ? Record<K,V> : never}
   */
  toObject() {
    if (!this.#resultIsEntry()) {
      throw new Error('Select result must be a [key,value] entry');
    }
    const entries = /** @type {[any, any]} */ (this.all());
    return /** @type {any} */ (Object.fromEntries(entries));
  }

  /**
   * @returns {Generator<R>}
   */
  *iterate() {
    const mapRow = this.#getMapRow();
    for (const row of this.#getStatement().iterate()) {
      yield mapRow(row);
    }
  }

  /**
   * @param {(obj: any[]) => void} fct
   * @returns {this}
   */
  explain(fct) {
    if (!fct) {
      throw new Error('You must provide a function to receive the report');
    }
    try {
      this.#queryParts.explain = true;
      for (const row of this.#getStatement().iterate()) {
        fct(!Array.isArray(row) ? Object.entries(row) : row);
      }
    } finally {
      this.#queryParts.explain = false;
    }
    return this;
  }
}

/**
 * @template {ObjectShape} T
 * @template R
 * @extends {FinalQuery<R>}
 */
class ScopedSelectQuery extends FinalQuery {
  #database;
  #type;
  #queryParts;

  /**
   * @param {*} database
   * @param {ObjectType<T>} type
   * @param {QueryParts<'db' | 'from' | 'select' | 'scope'>} queryParts
   */
  constructor(database, type, queryParts) {
    super(database, queryParts);
    this.#database = database;
    this.#type = type;
    this.#queryParts = queryParts;
  }

  /**
   * @returns {ScopedSelectQuery<T,R>}
   */
  distinct() {
    const queryParts = {
      ...this.#queryParts,
      distinct: true,
    };
    return new ScopedSelectQuery(this.#database, this.#type, queryParts);
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
   * @param {(obj: ObjectPick<T>, $: typeof operators) => OrderByResult} fct
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

  /**
   * @param {(obj: ObjectPick<T>, $: typeof operators) => GroupByResult} fct
   * @returns {ScopedSelectQuery<T,R>}
   */
  groupBy(fct) {
    const scope = this.#queryParts.scope;
    const queryParts = {
      ...this.#queryParts,
      groupBy: fct(scope, operators),
    };
    return new ScopedSelectQuery(this.#database, this.#type, queryParts);
  }

  /**
   * @param {number} count
   * @returns {ScopedSelectQuery<T,R>}
   */
  limit(count) {
    const queryParts = {
      ...this.#queryParts,
      limit: count,
    };
    return new ScopedSelectQuery(this.#database, this.#type, queryParts);
  }

  /**
   * @param {number} count
   * @returns {ScopedSelectQuery<T,R>}
   */
  offset(count) {
    const queryParts = {
      ...this.#queryParts,
      offset: count,
    };
    return new ScopedSelectQuery(this.#database, this.#type, queryParts);
  }
}

/**
 * @template {ObjectShape} T
 * @template R
 * @extends {FinalQuery<R>}
 */
class SelectQuery extends FinalQuery {
  #database;
  #type;
  #queryParts;

  /**
   * @param {*} database
   * @param {ObjectType<T>} type
   * @param {QueryParts<'db' | 'from' | 'select'>} queryParts
   */
  constructor(database, type, queryParts) {
    super(database, queryParts);
    this.#database = database;
    this.#type = type;
    this.#queryParts = queryParts;
  }

  /**
   * @returns {SelectQuery<T,R>}
   */
  distinct() {
    const queryParts = {
      ...this.#queryParts,
      distinct: true,
    };
    return new SelectQuery(this.#database, this.#type, queryParts);
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
   * @param {(obj: RowPick<T>, $: typeof operators) => OrderByResult} fct
   * @returns {SelectQuery<T,R>}
   */
  orderBy(fct) {
    const queryParts = {
      ...this.#queryParts,
      orderBy: fct(rowPick(this.#type), operators),
    };
    return new SelectQuery(this.#database, this.#type, queryParts);
  }

  /**
   * @param {(obj: RowPick<T>, $: typeof operators) => GroupByResult} fct
   * @returns {SelectQuery<T,R>}
   */
  groupBy(fct) {
    const queryParts = {
      ...this.#queryParts,
      groupBy: fct(rowPick(this.#type), operators),
    };
    return new SelectQuery(this.#database, this.#type, queryParts);
  }

  /**
   * @param {number} count
   * @returns {SelectQuery<T,R>}
   */
  limit(count) {
    const queryParts = {
      ...this.#queryParts,
      limit: count,
    };
    return new SelectQuery(this.#database, this.#type, queryParts);
  }

  /**
   * @param {number} count
   * @returns {SelectQuery<T,R>}
   */
  offset(count) {
    const queryParts = {
      ...this.#queryParts,
      offset: count,
    };
    return new SelectQuery(this.#database, this.#type, queryParts);
  }
}

/**
 * @template {ObjectShape} T
 */
class ScopedFromQuery {
  #database;
  #type;
  #queryParts;

  /**
   * @param {*} database
   * @param {ObjectType<T>} type
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
   * @returns {ScopedSelectQuery<T, t<T[F]>>}
   */
  field(value) {
    const scope = this.#queryParts.scope;
    const queryParts = {
      ...this.#queryParts,
      select: {[value]: scope.get(value)},
      selectOneField: true,
    };
    return new ScopedSelectQuery(this.#database, this.#type, queryParts);
  }

  /**
   * Select fields
   * @template {(keyof T)[]} F
   * @param {F} values
   * @returns {ScopedSelectQuery<T, t<Pick<T, F[number]>>>}
   */
  fields(values) {
    const scope = this.#queryParts.scope;
    const queryParts = {
      ...this.#queryParts,
      select: Object.fromEntries(
        values.map((value) => [value, scope.get(value)])
      ),
    };
    return new ScopedSelectQuery(this.#database, this.#type, queryParts);
  }

  /**
   * @template {SelectResult} R
   * @param {(obj: ObjectPick<T>, $: typeof operators) => R} fct
   * @returns {ScopedSelectQuery<T, QueryResultOf<R>>}
   */
  select(fct) {
    const scope = this.#queryParts.scope;
    const queryParts = {
      ...this.#queryParts,
      select: fct(scope, operators),
    };
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
 * @template {ObjectShape} T
 */
class FromQuery {
  #database;
  #type;
  #queryParts;

  /**
   * @param {*} database
   * @param {ObjectType<T>} type
   * @param {QueryParts<'db' | 'from'>} queryParts
   */
  constructor(database, type, queryParts) {
    this.#database = database;
    this.#type = type;
    this.#queryParts = queryParts;
  }

  /**
   * @template {ObjectShape} U
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
   * @returns {SelectQuery<T, t<Pick<T, F[number]>>>}
   */
  fields(fields) {
    const row = rowPick(this.#type);
    const queryParts = {
      ...this.#queryParts,
      select: Object.fromEntries(
        fields.map((fieldName) => [fieldName, row.field(fieldName)])
      ),
    };
    return new SelectQuery(this.#database, this.#type, queryParts);
  }

  /**
   * @template {SelectResult} R
   * @param {(obj: RowPick<T>, $: typeof operators) => R} fct
   * @returns {SelectQuery<T, QueryResultOf<R>>}
   */
  select(fct) {
    const queryParts = {
      ...this.#queryParts,
      select: fct(rowPick(this.#type), operators),
    };
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
   * @returns {FromQuery<GetShape<T>>}
   */
  from(tableName, shape) {
    const type = toObjectType(shape);
    const queryParts = {
      ...this.#queryParts,
      from: tableName,
    };
    return new FromQuery(this.#database, type, queryParts);
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
