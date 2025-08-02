// @ts-check

const {
  ObjectType,
  ArrayType,
  toObjectType,
  ObjectMapType,
  RecordType,
  object,
  BooleanType,
  OptionType,
  getTypeInstance,
} = require('xcraft-core-stones');
const operators = require('./operators.js');
const {rowPick, ValuePick, RowPick, ObjectPick} = require('./picks.js');
const {joinOperators} = require('./join-operators.js');
const {queryToSql} = require('./query-to-sql.js');
/**
 * @typedef {import("./operators.js").Operators} Operators
 * @typedef {import("./operators.js").Operator} Operator
 * @typedef {import("./operators.js").Aggregator} Aggregator
 * @typedef {import("./operators.js").BooleanOperator} BooleanOperator
 * @typedef {import("./operators.js").MathOperator} MathOperator
 */

/**
 * @typedef {import("./picks.js").AnyPick} AnyPick
 */

/**
 * TODO: add FinalQuery to table
 * @typedef {{
 *   table: string | never,
 *   alias: string
 * }} TableOrSubquery
 */

/**
 * @typedef {string | TableOrSubquery} QueryTable
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
 * @typedef {typeof joinOperators[number]} JoinOperator
 */

/**
 * @typedef {{operator: JoinOperator, table: QueryTable, constraint: BooleanValue}} JoinsResult
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
 * @template {[ObjectShape, ...ObjectShape[]]} T
 * @typedef {{[K in keyof T]: ObjectType<T[K]>}} WrapObjectType
 */

/**
 * @template {[ObjectShape, ...ObjectShape[]]} T
 * @typedef {{[K in keyof T]: RowPick<T[K]>}} WrapRowPick
 */

/**
 * @param {QueryTable} table
 * @returns {string}
 */
function getTableName(table) {
  if (typeof table === 'string') {
    return table;
  }
  return table.alias;
}

/**
 * @param {QueryParts<'db' | 'from'>} queryParts
 * @param {number} index
 * @returns {string}
 */
function getPartTableName(queryParts, index) {
  if (index === 0) {
    return getTableName(queryParts.from);
  }
  if (queryParts.joins) {
    return getTableName(queryParts.joins[index - 1].table);
  }
  throw new Error('Bad index');
}

/**
 * @template {[ObjectShape, ...ObjectShape[]]} T
 * @param {WrapObjectType<T>} types
 * @param {QueryParts<'db' | 'from'>} queryParts
 * @returns {WrapRowPick<T>}
 */
function wrapRowPick(types, queryParts) {
  return /** @type {WrapRowPick<T>} */ (types.map((type, index) =>
    rowPick(type, getPartTableName(queryParts, index))
  ));
}

/**
 * @template {[ObjectShape, ...ObjectShape[]]} T
 * @template {AnyObjectShape} U
 * @typedef {[...T, GetShape<U>]} AllShapes
 */

/**
 * @template {[ObjectShape, ...ObjectShape[]]} T
 * @typedef {[...objs: WrapRowPick<T>, operators: typeof operators]} FctArgs
 */

/**
 * @typedef {{
 *   db: string,
 *   explain?: boolean,
 *   from: QueryTable,
 *   scope?: ObjectPick<any>
 *   joins?: JoinsResult[],
 *   select: SelectResult,
 *   selectOneField?: boolean,
 *   distinct?: boolean,
 *   where?: Operator,
 *   orderBy?: OrderByResult,
 *   groupBy?: GroupByResult,
 *   limit?: number,
 *   offset?: number,
 * }} QueryObj
 */

/**
 * @template {keyof QueryObj} K
 * @typedef {flatten<Pick<Required<QueryObj>, K> & Omit<Partial<QueryObj>, K>>} QueryParts
 */

/**
 * @param {JoinsResult[] | undefined} currentJoin
 * @param {JoinsResult} newJoin
 * @returns {JoinsResult[]}
 */
function mergeJoin(currentJoin, newJoin) {
  if (!currentJoin) {
    return [newJoin];
  }
  return [...currentJoin, newJoin];
}

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

  #parseBoolean(value) {
    if (value === 1) {
      return true;
    }
    if (value === 0) {
      return false;
    }
    return value;
  }

  #parseOptional(subMapper) {
    return (value) => {
      if (value === null) {
        return value;
      }
      return subMapper(value);
    };
  }

  #identity(value) {
    return value;
  }

  #getMapper(type) {
    if (type instanceof OptionType) {
      const subType = getTypeInstance(type.subType);
      const subMapper = this.#getMapper(subType);
      if (subMapper !== this.#identity) {
        return this.#parseOptional(subMapper);
      }
    }
    if (
      type instanceof ArrayType ||
      type instanceof ObjectType ||
      type === object ||
      type instanceof ObjectMapType ||
      type instanceof RecordType
    ) {
      return this.#parseValue;
    }
    if (type instanceof BooleanType) {
      return this.#parseBoolean;
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
        fct(!Array.isArray(row) ? Object.values(row) : row);
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
   * @param {(obj: ObjectPick<T>, $: typeof operators) => BooleanValue} fct
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
 * @template {[ObjectShape, ...ObjectShape[]]} T
 * @template R
 * @extends {FinalQuery<R>}
 */
class SelectQuery extends FinalQuery {
  #database;
  #types;
  #queryParts;

  /**
   * @param {*} database
   * @param {WrapObjectType<T>} types
   * @param {QueryParts<'db' | 'from' | 'select'>} queryParts
   */
  constructor(database, types, queryParts) {
    super(database, queryParts);
    this.#database = database;
    this.#types = types;
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
    return new SelectQuery(this.#database, this.#types, queryParts);
  }

  /**
   * @param {(...args: FctArgs<T>) => BooleanValue} fct
   * @returns {SelectQuery<T,R>}
   */
  where(fct) {
    const queryParts = {
      ...this.#queryParts,
      where: mergeWhere(
        this.#queryParts.where,
        fct(...wrapRowPick(this.#types, this.#queryParts), operators)
      ),
    };
    return new SelectQuery(this.#database, this.#types, queryParts);
  }

  /**
   * @param {(...args: FctArgs<T>) => OrderByResult} fct
   * @returns {SelectQuery<T,R>}
   */
  orderBy(fct) {
    const queryParts = {
      ...this.#queryParts,
      orderBy: fct(...wrapRowPick(this.#types, this.#queryParts), operators),
    };
    return new SelectQuery(this.#database, this.#types, queryParts);
  }

  /**
   * @param {(...args: FctArgs<T>) => GroupByResult} fct
   * @returns {SelectQuery<T,R>}
   */
  groupBy(fct) {
    const queryParts = {
      ...this.#queryParts,
      groupBy: fct(...wrapRowPick(this.#types, this.#queryParts), operators),
    };
    return new SelectQuery(this.#database, this.#types, queryParts);
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
    return new SelectQuery(this.#database, this.#types, queryParts);
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
    return new SelectQuery(this.#database, this.#types, queryParts);
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
   * @param {(obj: ObjectPick<T>, $: typeof operators) => BooleanValue} fct
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
 * @template {[ObjectShape, ...ObjectShape[]]} T
 */
class FromQuery {
  #database;
  #types;
  #queryParts;

  /**
   * @param {*} database
   * @param {WrapObjectType<T>} types
   * @param {QueryParts<'db' | 'from'>} queryParts
   */
  constructor(database, types, queryParts) {
    this.#database = database;
    this.#types = types;
    this.#queryParts = queryParts;
  }

  /**
   * @template {ObjectShape} U
   * @param {(...objs: WrapRowPick<T>) => ObjectPick<U>} fct
   * @returns {ScopedFromQuery<U>}
   */
  scope(fct) {
    const value = fct(...wrapRowPick(this.#types, this.#queryParts));
    const queryParts = {
      ...this.#queryParts,
      scope: value,
    };
    return new ScopedFromQuery(this.#database, value.type, queryParts);
  }

  /**
   * @template {AnyObjectShape} U
   * @param {QueryTable} table
   * @param {U} shape
   * @param {(...args: FctArgs<AllShapes<T,U>>) => BooleanValue} joinFct
   * @returns {FromQuery<AllShapes<T,U>>}
   */
  leftJoin(table, shape, joinFct) {
    const type = toObjectType(shape);
    /** @type {WrapObjectType<AllShapes<T,U>>} */
    const newTypes = [...this.#types, type];
    const queryParts = {
      ...this.#queryParts,
      joins: mergeJoin(this.#queryParts.joins, {
        operator: 'left join',
        table,
        constraint: joinFct(
          ...wrapRowPick(this.#types, this.#queryParts),
          rowPick(type, getTableName(table)),
          operators
        ),
      }),
    };
    return new /** @type {typeof FromQuery<AllShapes<T,U>>} */ (FromQuery)(
      this.#database,
      newTypes,
      queryParts
    );
  }

  /**
   * Select field
   * @template {keyof (T[0])} F
   * @param {F} field
   * @returns {SelectQuery<T, t<T[0][F]>>}
   */
  field(field) {
    /** @type {ObjectType<T[0]>} */
    const type = this.#types[0];
    const row = rowPick(type, getTableName(this.#queryParts.from));
    const queryParts = {
      ...this.#queryParts,
      select: {[field]: row.field(field)},
      selectOneField: true,
    };
    return new SelectQuery(this.#database, this.#types, queryParts);
  }

  /**
   * Select fields
   * @template {(keyof T[0])[]} F
   * @param {F} fields
   * @returns {SelectQuery<T, t<Pick<T[0], F[number]>>>}
   */
  fields(fields) {
    /** @type {ObjectType<T[0]>} */
    const type = this.#types[0];
    const row = rowPick(type, getTableName(this.#queryParts.from));
    const queryParts = {
      ...this.#queryParts,
      select: Object.fromEntries(
        fields.map((fieldName) => [fieldName, row.field(fieldName)])
      ),
    };
    return new SelectQuery(this.#database, this.#types, queryParts);
  }

  /**
   * @template {SelectResult} R
   * @param {(...args: FctArgs<T>) => R} fct
   * @returns {SelectQuery<T, QueryResultOf<R>>}
   */
  select(fct) {
    const queryParts = {
      ...this.#queryParts,
      select: fct(...wrapRowPick(this.#types, this.#queryParts), operators),
    };
    return new SelectQuery(this.#database, this.#types, queryParts);
  }

  /**
   * @param {(...args: FctArgs<T>) => BooleanValue} fct
   * @returns {FromQuery<T>}
   */
  where(fct) {
    const queryParts = {
      ...this.#queryParts,
      where: mergeWhere(
        this.#queryParts.where,
        fct(...wrapRowPick(this.#types, this.#queryParts), operators)
      ),
    };
    return new FromQuery(this.#database, this.#types, queryParts);
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
   * @param {QueryTable} table
   * @param {T} shape
   * @returns {FromQuery<[GetShape<T>]>}
   */
  from(table, shape) {
    const type = toObjectType(shape);
    const queryParts = {
      ...this.#queryParts,
      from: table,
    };
    return new FromQuery(this.#database, [type], queryParts);
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
