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
 * @typedef {import("./operators.js").MathOperator} MathOperator
 */

/**
 * @typedef {import("./picks.js").AnyPick} AnyPick
 * @typedef {import("./picks.js").BooleanValue} BooleanValue
 */

/**
 * TODO: add FinalQuery
 * @typedef {Operators["each"]} Subquery
 */

/**
 * @typedef {{db?: string, table: string, alias?: string} | {db?: undefined, table: Subquery, alias?: string}} TableAndAlias
 */

/**
 * @typedef {string | Subquery | TableAndAlias} QueryTable
 */

/**
 * @template {AnyObjectShape} T
 * @template {AnyObjectShape} U
 * @typedef {{
 *   db?: string,
 *   table: string,
 *   alias?: string,
 *   shape?: T,
 *   baseShape?: U,
 *   scope?: (row: RowPick<GetShape<U>>) => ObjectPick<GetShape<T>>,
 *   scopeCondition?: (row: RowPick<GetShape<U>>, $: typeof operators) => BooleanValue,
 *   onUse?: (builder: QueryBuilder | FromQuery) => void,
 * }} TableSchema
 */

/**
 * @typedef {TableSchema<AnyObjectShape, AnyObjectShape>} AnyTableSchema
 */

/**
 * @template {string} [S = string]
 * @typedef {{[K in S] : AnyTableSchema}} DbSchema
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
 * @typedef {{[K in keyof T]: RowPick<T[K]>}} WrapRowPick
 */

/**
 * @param {QueryTable} table
 * @returns {string | undefined}
 */
function getTableName(table) {
  if (typeof table === 'string') {
    return table;
  }
  if ('alias' in table) {
    return table.alias;
  }
  if ('table' in table) {
    if (typeof table.table === 'string') {
      return table.table;
    }
  }
  return undefined;
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
 * @template {AnyObjectShape} T
 * @param {AnyTableSchema} tableSchema
 * @param {T} shape
 * @param {QueryParts<'from'>} queryParts
 * @returns {{pick: RowPick<GetShape<T>>, queryParts: QueryParts<'from'>}}
 */
function useTableSchema(tableSchema, shape, queryParts) {
  const type = toObjectType(shape);
  const tableName = getTableName(tableSchema);
  let pick = rowPick(type, tableName);
  if (tableSchema.scope && tableSchema.baseShape) {
    const basePick = rowPick(toObjectType(tableSchema.baseShape), tableName);
    if (tableSchema.scopeCondition) {
      queryParts = {
        ...queryParts,
        where: mergeWhere(
          queryParts.where,
          tableSchema.scopeCondition(basePick, operators)
        ),
      };
    }
    pick = tableSchema.scope(basePick).toRowPick();
  }
  return {pick, queryParts};
}

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
   * @param {QueryParts<'from' | 'select'>} queryParts
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
 * @template {[ObjectShape, ...ObjectShape[]]} T
 * @template R
 * @extends {FinalQuery<R>}
 */
class SelectQuery extends FinalQuery {
  #database;
  #picks;
  #queryParts;

  /**
   * @param {WrapRowPick<T>} picks
   * @param {QueryParts<'from' | 'select'>} queryParts
   * @param {*} database
   */
  constructor(picks, queryParts, database) {
    super(database, queryParts);
    this.#database = database;
    this.#picks = picks;
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
    return new SelectQuery(this.#picks, queryParts, this.#database);
  }

  /**
   * @param {(...args: FctArgs<T>) => BooleanValue} fct
   * @returns {SelectQuery<T,R>}
   */
  where(fct) {
    const queryParts = {
      ...this.#queryParts,
      where: mergeWhere(this.#queryParts.where, fct(...this.#picks, operators)),
    };
    return new SelectQuery(this.#picks, queryParts, this.#database);
  }

  /**
   * @param {(...args: FctArgs<T>) => OrderByResult} fct
   * @returns {SelectQuery<T,R>}
   */
  orderBy(fct) {
    const queryParts = {
      ...this.#queryParts,
      orderBy: fct(...this.#picks, operators),
    };
    return new SelectQuery(this.#picks, queryParts, this.#database);
  }

  /**
   * @param {(...args: FctArgs<T>) => GroupByResult} fct
   * @returns {SelectQuery<T,R>}
   */
  groupBy(fct) {
    const queryParts = {
      ...this.#queryParts,
      groupBy: fct(...this.#picks, operators),
    };
    return new SelectQuery(this.#picks, queryParts, this.#database);
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
    return new SelectQuery(this.#picks, queryParts, this.#database);
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
    return new SelectQuery(this.#picks, queryParts, this.#database);
  }
}

/**
 * @template {[ObjectShape, ...ObjectShape[]]} T
 */
class FromQuery {
  #database;
  #getTableSchema;
  #picks;
  #queryParts;

  /**
   * @param {(tableName: string, shape: AnyObjectShape) => AnyTableSchema} getTableSchema
   * @param {WrapRowPick<T>} picks
   * @param {QueryParts<'from'>} queryParts
   * @param {*} database
   */
  constructor(getTableSchema, picks, queryParts, database) {
    this.#database = database;
    this.#getTableSchema = getTableSchema;
    this.#picks = picks;
    this.#queryParts = queryParts;
  }

  get database() {
    return this.#database;
  }

  /**
   * @template {AnyObjectShape} U
   * @param {JoinOperator} joinOperator
   * @param {string} tableName
   * @param {U} shape
   * @param {(...args: FctArgs<AllShapes<T,U>>) => BooleanValue} joinFct
   * @returns {FromQuery<AllShapes<T,U>>}
   */
  _join(joinOperator, tableName, shape, joinFct) {
    const tableSchema = this.#getTableSchema(tableName, shape);
    const {pick, queryParts} = useTableSchema(
      tableSchema,
      shape,
      this.#queryParts
    );

    /** @type {WrapRowPick<AllShapes<T,U>>} */
    const newPicks = [...this.#picks, pick];
    const newQueryParts = {
      ...queryParts,
      joins: mergeJoin(this.#queryParts.joins, {
        operator: joinOperator,
        table: tableSchema,
        constraint: joinFct(...newPicks, operators),
      }),
    };

    return new /** @type {typeof FromQuery<AllShapes<T,U>>} */ (FromQuery)(
      this.#getTableSchema,
      newPicks,
      newQueryParts,
      this.#database
    );
  }

  /**
   * @typedef {<U extends AnyObjectShape>(tableName: string, shape: U, joinFct: (...args: FctArgs<AllShapes<T,U>>) => BooleanValue) => FromQuery<AllShapes<T,U>>} JoinFunction
   */

  /**
   * @type {JoinFunction}
   */
  innerJoin(tableName, shape, joinFct) {
    return this._join('inner join', tableName, shape, joinFct);
  }

  /**
   * @type {JoinFunction}
   */
  leftJoin(tableName, shape, joinFct) {
    return this._join('left join', tableName, shape, joinFct);
  }

  /**
   * @type {JoinFunction}
   */
  rightJoin(tableName, shape, joinFct) {
    return this._join('right join', tableName, shape, joinFct);
  }

  /**
   * @type {JoinFunction}
   */
  fullJoin(tableName, shape, joinFct) {
    return this._join('full join', tableName, shape, joinFct);
  }

  /**
   * @type {JoinFunction}
   */
  crossJoin(tableName, shape, joinFct) {
    return this._join('cross join', tableName, shape, joinFct);
  }

  /**
   * Select field
   * @template {keyof (T[0])} F
   * @param {F} field
   * @returns {SelectQuery<T, t<T[0][F]>>}
   */
  field(field) {
    /** @type {RowPick<T[0]>} */
    const row = this.#picks[0];
    const queryParts = {
      ...this.#queryParts,
      select: {[field]: row.field(field)},
      selectOneField: true,
    };
    return new SelectQuery(this.#picks, queryParts, this.#database);
  }

  /**
   * Select fields
   * @template {(keyof T[0])[]} F
   * @param {F} fields
   * @returns {SelectQuery<T, t<Pick<T[0], F[number]>>>}
   */
  fields(fields) {
    /** @type {RowPick<T[0]>} */
    const row = this.#picks[0];
    const queryParts = {
      ...this.#queryParts,
      select: Object.fromEntries(
        fields.map((fieldName) => [fieldName, row.field(fieldName)])
      ),
    };
    return new SelectQuery(this.#picks, queryParts, this.#database);
  }

  /**
   * @template {SelectResult} R
   * @param {(...args: FctArgs<T>) => R} fct
   * @returns {SelectQuery<T, QueryResultOf<R>>}
   */
  select(fct) {
    const queryParts = {
      ...this.#queryParts,
      select: fct(...this.#picks, operators),
    };
    return new SelectQuery(this.#picks, queryParts, this.#database);
  }

  /**
   * @param {(...args: FctArgs<T>) => BooleanValue} fct
   * @returns {FromQuery<T>}
   */
  where(fct) {
    const queryParts = {
      ...this.#queryParts,
      where: mergeWhere(this.#queryParts.where, fct(...this.#picks, operators)),
    };
    return new FromQuery(
      this.#getTableSchema,
      this.#picks,
      queryParts,
      this.#database
    );
  }
}

class QueryBuilder {
  #database;
  #getTableSchema;

  /**
   * @param {Object} options
   * @param {*} [options.database]
   * @param {DbSchema} [options.schema]
   * @param {(tableName: string, shape: AnyObjectShape) => AnyTableSchema} [options.getTableSchema]
   */
  constructor({database, schema, getTableSchema} = {}) {
    this.#database = database;
    this.#getTableSchema =
      getTableSchema ??
      ((tableName) => (schema ? schema[tableName] : {table: tableName}));
  }

  /**
   * @template {AnyObjectShape} T
   * @template {AnyObjectShape} U
   * @param {TableSchema<T,U>} params
   * @returns {TableSchema<T,U>}
   */
  static TableSchema = (params) => {
    return params;
  };

  get database() {
    return this.#database;
  }

  /**
   * @template {AnyObjectShape} T
   * @param {string} tableName
   * @param {T} shape
   * @returns {FromQuery<[GetShape<T>]>}
   */
  from(tableName, shape) {
    const tableSchema = this.#getTableSchema(tableName, shape);
    const from = tableSchema;
    const {pick, queryParts} = useTableSchema(tableSchema, shape, {from});
    return new FromQuery(
      this.#getTableSchema,
      [pick],
      queryParts,
      this.#database
    );
  }
}

module.exports = {
  QueryBuilder,
  FromQuery,
};
