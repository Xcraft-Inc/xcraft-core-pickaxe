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
const {pickOperators} = require('./pick-operators.js');
const {
  rowPick,
  ValuePick,
  RowPick,
  ObjectPick,
  BasePick,
  BooleanPick,
} = require('./picks.js');
const {joinOperators} = require('./join-operators.js');
const {queryToSql} = require('./query-to-sql.js');

/**
 * @typedef {import("./operators.js").Operators} Operators
 */

/**
 * @template {ObjectShape} T
 * @typedef {import('./optional-object-type.js').OptionalObjectShape<T>} OptionalObjectShape
 */

/**
 * @typedef {{name: string, query: QueryObj}} WithResult
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
 *   scopeCondition?: (row: RowPick<GetShape<U>>, $: typeof pickOperators) => BooleanPick,
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
 * @typedef {BasePick} SelectValue
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
 * @typedef {{operator: JoinOperator, table: QueryTable, constraint: BooleanPick}} JoinsResult
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
 * @template {[...ObjectShape[], ObjectShape]} T
 * @typedef {{[K in keyof T]: RowPick<T[K]>}} WrapRowPick
 */

/**
 * @template {[...ObjectShape[], ObjectShape]} T
 * @typedef {{[K in keyof T]: OptionalObjectShape<T[K]>}} WrapOptionalObjectShape
 */

/**
 * @template {[...ObjectShape[], ObjectShape]} T
 * @param {WrapRowPick<T>} picks
 * @returns {WrapRowPick<WrapOptionalObjectShape<T>>}
 */
function picksToOptional(picks) {
  // @ts-ignore
  return picks.map((pick) => pick.toOptional());
}

/**
 * @param {QueryTable} table
 * @returns {string | undefined}
 */
function getTableName(table) {
  if (typeof table === 'string') {
    return table;
  }
  if ('alias' in table && table.alias !== undefined) {
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
 * @template {[...ObjectShape[], ObjectShape]} T
 * @template {AnyObjectShape} U
 * @typedef {[...T, GetShape<U>]} AllShapes
 */

/**
 * @template {[...ObjectShape[], ObjectShape]} T
 * @typedef {[...objs: WrapRowPick<T>, operators: typeof pickOperators]} FctArgs
 */

/**
 * @typedef {{
 *   explain?: boolean,
 *   withs?: WithResult[],
 *   from: QueryTable,
 *   scope?: ObjectPick<any>
 *   joins?: JoinsResult[],
 *   select: SelectResult | '*',
 *   selectOneField?: boolean,
 *   distinct?: boolean,
 *   where?: BooleanPick,
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
 * @returns {{pick: RowPick<GetShape<T>>, condition: BooleanPick | null}}
 */
function useTableSchema(tableSchema, shape) {
  const type = toObjectType(shape);
  const tableName = getTableName(tableSchema);
  let pick = rowPick(type, tableName);
  let condition = null;
  if (tableSchema.scope && tableSchema.baseShape) {
    const basePick = rowPick(toObjectType(tableSchema.baseShape), tableName);
    if (tableSchema.scopeCondition) {
      condition = tableSchema.scopeCondition(basePick, pickOperators);
    }
    pick = tableSchema.scope(basePick).toRowPick();
  }
  return {pick, condition};
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

/**
 * @param {BooleanPick | null | undefined} currentWhere
 * @param {BooleanPick} newWhere
 * @returns {BooleanPick}
 */
function mergeWhere(currentWhere, newWhere) {
  if (!currentWhere) {
    return newWhere;
  }
  return pickOperators.and(currentWhere, newWhere);
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
   * @returns {flatten<R>[]}
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
 * @template {[...ObjectShape[], ObjectShape]} T
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

  get picks() {
    return this.#picks;
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
   * @param {(...args: FctArgs<T>) => BooleanPick} fct
   * @returns {SelectQuery<T,R>}
   */
  where(fct) {
    const queryParts = {
      ...this.#queryParts,
      where: mergeWhere(
        this.#queryParts.where,
        fct(...this.#picks, pickOperators)
      ),
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
      orderBy: fct(...this.#picks, pickOperators),
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
      groupBy: fct(...this.#picks, pickOperators),
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
 * @template {[...ObjectShape[], ObjectShape]} T
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
   * @template {[...ObjectShape[], ObjectShape]} V
   * @param {JoinOperator} joinOperator
   * @param {string} tableName
   * @param {U} shape
   * @param {(...args: FctArgs<AllShapes<T,U>>) => BooleanPick} joinFct
   * @param {(picks: WrapRowPick<T>, pick: RowPick<GetShape<U>>) => WrapRowPick<V>} mergePicks
   * @returns {FromQuery<V>}
   */
  _join(joinOperator, tableName, shape, joinFct, mergePicks) {
    const tableSchema = this.#getTableSchema(tableName, shape);
    const {pick, condition} = useTableSchema(tableSchema, shape);

    const joinCondition = joinFct(...this.#picks, pick, pickOperators);
    const queryParts = {
      ...this.#queryParts,
      joins: mergeJoin(this.#queryParts.joins, {
        operator: joinOperator,
        table: tableSchema,
        constraint: condition
          ? pickOperators.and(condition, joinCondition)
          : joinCondition,
      }),
    };

    const newPicks = mergePicks(this.#picks, pick);

    return new FromQuery(
      this.#getTableSchema,
      newPicks,
      queryParts,
      this.#database
    );
  }

  /**
   * @template {AnyObjectShape} U
   * @param {string} tableName
   * @param {U} shape
   * @param {(...args: FctArgs<AllShapes<T,U>>) => BooleanPick} joinFct
   * @returns {FromQuery<[...T, GetShape<U>]>}
   */
  innerJoin(tableName, shape, joinFct) {
    return /** @type {typeof this._join<U, [...T, GetShape<U>]>} */ (this
      ._join)('inner join', tableName, shape, joinFct, (picks, pick) => [
      ...picks,
      pick,
    ]);
  }

  /**
   * @template {AnyObjectShape} U
   * @param {string} tableName
   * @param {U} shape
   * @param {(...args: FctArgs<AllShapes<T,U>>) => BooleanPick} joinFct
   * @returns {FromQuery<[...T,OptionalObjectShape<GetShape<U>>]>}
   */
  leftJoin(tableName, shape, joinFct) {
    return /** @type {typeof this._join<U, [...T,OptionalObjectShape<GetShape<U>>]>} */ (this
      ._join)('left join', tableName, shape, joinFct, (picks, pick) => [
      ...picks,
      pick.toOptional(),
    ]);
  }

  /**
   * @template {AnyObjectShape} U
   * @param {string} tableName
   * @param {U} shape
   * @param {(...args: FctArgs<AllShapes<T,U>>) => BooleanPick} joinFct
   * @returns {FromQuery<[...WrapOptionalObjectShape<T>,GetShape<U>]>}
   */
  rightJoin(tableName, shape, joinFct) {
    return /** @type {typeof this._join<U, [...WrapOptionalObjectShape<T>,GetShape<U>]>} */ (this
      ._join)('right join', tableName, shape, joinFct, (picks, pick) => [
      ...picksToOptional(picks),
      pick,
    ]);
  }

  /**
   * @template {AnyObjectShape} U
   * @param {string} tableName
   * @param {U} shape
   * @param {(...args: FctArgs<AllShapes<T,U>>) => BooleanPick} joinFct
   * @returns {FromQuery<[...WrapOptionalObjectShape<T>,OptionalObjectShape<GetShape<U>>]>}
   */
  fullJoin(tableName, shape, joinFct) {
    return /** @type {typeof this._join<U, [...WrapOptionalObjectShape<T>,OptionalObjectShape<GetShape<U>>]>} */ (this
      ._join)('full join', tableName, shape, joinFct, (picks, pick) => [
      ...picksToOptional(picks),
      pick.toOptional(),
    ]);
  }

  /**
   * @template {AnyObjectShape} U
   * @param {string} tableName
   * @param {U} shape
   * @param {(...args: FctArgs<AllShapes<T,U>>) => BooleanPick} joinFct
   * @returns {FromQuery<[...T,GetShape<U>]>}
   */
  crossJoin(tableName, shape, joinFct) {
    return /** @type {typeof this._join<U, [...T, GetShape<U>]>} */ (this
      ._join)('cross join', tableName, shape, joinFct, (picks, pick) => [
      ...picks,
      pick,
    ]);
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
      select: fct(...this.#picks, pickOperators),
    };
    return new SelectQuery(this.#picks, queryParts, this.#database);
  }

  /**
   * @returns {SelectQuery<T, t<T[number]>>}
   */
  selectAll() {
    const isRoot = this.#picks.every((pick) => pick.isRoot());
    /** @type {QueryObj["select"]} */
    let select;
    if (isRoot) {
      select = '*';
    } else {
      select = Object.fromEntries(
        this.#picks.flatMap((pick) =>
          Object.keys(pick.type.properties).map((key) => [key, pick.get(key)])
        )
      );
    }
    const queryParts = {
      ...this.#queryParts,
      select,
    };
    return new SelectQuery(this.#picks, queryParts, this.#database);
  }

  /**
   * @param {(...args: FctArgs<T>) => BooleanPick} fct
   * @returns {FromQuery<T>}
   */
  where(fct) {
    const queryParts = {
      ...this.#queryParts,
      where: mergeWhere(
        this.#queryParts.where,
        fct(...this.#picks, pickOperators)
      ),
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
  #withs;

  /**
   * @param {Object} options
   * @param {*} [options.database]
   * @param {DbSchema} [options.schema]
   * @param {(tableName: string, shape: AnyObjectShape) => AnyTableSchema} [options.getTableSchema]
   * @param {WithResult[]} [options.withs]
   */
  constructor({database, schema, getTableSchema, withs} = {}) {
    this.#database = database;
    this.#getTableSchema =
      getTableSchema ??
      ((tableName) => (schema ? schema[tableName] : {table: tableName}));
    this.#withs = withs;
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
   * @param {string} name
   * @param {FinalQuery<any>} query
   * @returns {QueryBuilder}
   */
  with(name, query) {
    return new QueryBuilder({
      database: this.#database,
      getTableSchema: this.#getTableSchema,
      withs: [
        ...(this.#withs ?? []),
        {
          name,
          query: query.query,
        },
      ],
    });
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
    const queryParts = {
      from,
      withs: this.#withs,
    };
    const {pick, condition} = useTableSchema(tableSchema, shape);
    if (condition) {
      queryParts.where = condition;
    }
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
