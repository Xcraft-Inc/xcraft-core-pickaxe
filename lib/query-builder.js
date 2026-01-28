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
const partialObjectShape = require('./partial-object-shape.js');

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
 * @template {BasePick} T
 * @typedef {T["type"]} PickType
 */
/**
 * @typedef {[BasePick, ...BasePick[]]} SelectPicksTuple
 * @typedef {Record<string, BasePick>} SelectPicksObject
 */
/**
 * @typedef {SelectPicksObject | SelectPicksTuple} SelectPicks
 */

/**
 * @template {SelectPicksTuple} T
 * @typedef {{[K in keyof T]: PickType<T[K]>}} SelectTypeTuple
 */
/**
 * @template {SelectPicksTuple} T
 * @param {T} selectPicks
 * @returns {SelectTypeTuple<T>}
 */
function selectTypeTuple(selectPicks) {
  return /** @type {SelectTypeTuple<T>} */ (selectPicks.map(
    (result) => result.type
  ));
}

/**
 * @template {SelectPicksObject} T
 * @typedef {flatten<{[K in keyof T]: PickType<T[K]>}>} SelectTypeObject
 */
/**
 * @template {SelectPicksObject} T
 * @param {T} selectPicks
 * @returns {SelectTypeObject<T>}
 */
function selectTypeObject(selectPicks) {
  return /** @type {SelectTypeObject<T>} */ (Object.fromEntries(
    Object.entries(selectPicks).map(([name, pick]) => [name, pick.type])
  ));
}

/**
 * @template {SelectPicks} T
 * @typedef {T extends SelectPicksTuple ? {values: SelectTypeTuple<T>} : T extends SelectPicksObject ? {object: SelectTypeObject<T>} : never} SelectResultOf
 */
/**
 * @template {SelectPicks} T
 * @param {T} selectPicks
 * @returns {SelectResultOf<T>}
 */
function selectResultOf(selectPicks) {
  if (Array.isArray(selectPicks)) {
    return /** @type {SelectResultOf<T>} */ ({
      values: selectTypeTuple(selectPicks),
    });
  }
  return /** @type {SelectResultOf<T>} */ ({
    object: selectTypeObject(selectPicks),
  });
}

/**
 * @typedef {{value: AnyTypeOrShape}} SelectResultValue
 * @typedef {{values: [AnyTypeOrShape, ...AnyTypeOrShape[]]}} SelectResultValues
 * @typedef {{object: Record<string, AnyTypeOrShape>}} SelectResultObject
 */

/**
 * @typedef {SelectResultValue | SelectResultValues | SelectResultObject} SelectResult
 */

/**
 * @template {SelectResult} T
 * @typedef {T extends SelectResultObject ? RowPick<T['object']> : null} SelectResultPick
 */

/**
 * @template {SelectResult} T
 * @param {T} selectResult
 * @returns {SelectResultPick<T>}
 */
function selectResultPick(selectResult) {
  if ('object' in selectResult) {
    const rowPick = new RowPick(object(selectResult.object), null);
    return /** @type {SelectResultPick<T>} */ (rowPick);
  }
  return /** @type {SelectResultPick<T>} */ (null);
}

/**
 * @template {SelectResultValues['values']} T
 * @typedef {{[K in keyof T]: t<T[K]>}} QueryReturnValues
 */

/**
 * @template {SelectResultObject['object']} T
 * @typedef {flatten<{[K in keyof T]: t<T[K]>}>} QueryReturnObject
 */

/**
 * @template {SelectResult} T
 * @typedef {T extends SelectResultValue ? t<T['value']> : T extends SelectResultValues ? QueryReturnValues<T['values']> : T extends SelectResultObject ? QueryReturnObject<T['object']> : never} QueryReturn
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
 * @template {[...ObjectShape[], ObjectShape]} T
 * @template {SelectResult} Q
 * @typedef {[...objs: WrapRowPick<T>, operators: typeof pickOperators, selectFields: SelectResultPick<Q>]} FctArgs2
 */

/**
 * @typedef {{
 *   explain?: boolean,
 *   withs?: WithResult[],
 *   from: QueryTable,
 *   scope?: ObjectPick<any>
 *   joins?: JoinsResult[],
 *   select: SelectPicks | '*',
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
 * @template {SelectResult} S
 * @extends {FinalQuery<QueryReturn<S>>}
 */
class SelectQuery extends FinalQuery {
  #database;
  #picks;
  #selectPick;
  #queryParts;

  /**
   * @param {WrapRowPick<T>} picks
   * @param {SelectResultPick<S>} selectPick
   * @param {QueryParts<'from' | 'select'>} queryParts
   * @param {*} database
   */
  constructor(picks, selectPick, queryParts, database) {
    super(database, queryParts);
    this.#database = database;
    this.#picks = picks;
    this.#selectPick = selectPick;
    this.#queryParts = queryParts;
  }

  get picks() {
    return this.#picks;
  }

  /**
   * @returns {SelectQuery<T,S>}
   */
  distinct() {
    const queryParts = {
      ...this.#queryParts,
      distinct: true,
    };
    return new SelectQuery(
      this.#picks,
      this.#selectPick,
      queryParts,
      this.#database
    );
  }

  /**
   * @param {(...args: FctArgs2<T,S>) => BooleanPick} fct
   * @returns {SelectQuery<T,S>}
   */
  where(fct) {
    const queryParts = {
      ...this.#queryParts,
      where: mergeWhere(
        this.#queryParts.where,
        fct(...this.#picks, pickOperators, this.#selectPick)
      ),
    };
    return new SelectQuery(
      this.#picks,
      this.#selectPick,
      queryParts,
      this.#database
    );
  }

  /**
   * @param {(...args: FctArgs2<T,S>) => OrderByResult} fct
   * @returns {SelectQuery<T,S>}
   */
  orderBy(fct) {
    const queryParts = {
      ...this.#queryParts,
      orderBy: fct(...this.#picks, pickOperators, this.#selectPick),
    };
    return new SelectQuery(
      this.#picks,
      this.#selectPick,
      queryParts,
      this.#database
    );
  }

  /**
   * @param {(...args: FctArgs2<T,S>) => GroupByResult} fct
   * @returns {SelectQuery<T,S>}
   */
  groupBy(fct) {
    const queryParts = {
      ...this.#queryParts,
      groupBy: fct(...this.#picks, pickOperators, this.#selectPick),
    };
    return new SelectQuery(
      this.#picks,
      this.#selectPick,
      queryParts,
      this.#database
    );
  }

  /**
   * @param {number} count
   * @returns {SelectQuery<T,S>}
   */
  limit(count) {
    const queryParts = {
      ...this.#queryParts,
      limit: count,
    };
    return new SelectQuery(
      this.#picks,
      this.#selectPick,
      queryParts,
      this.#database
    );
  }

  /**
   * @param {number} count
   * @returns {SelectQuery<T,S>}
   */
  offset(count) {
    const queryParts = {
      ...this.#queryParts,
      offset: count,
    };
    return new SelectQuery(
      this.#picks,
      this.#selectPick,
      queryParts,
      this.#database
    );
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
   * @returns {SelectQuery<T, {value: T[0][F]}>}
   */
  field(field) {
    /** @type {RowPick<T[0]>} */
    const row = this.#picks[0];
    const queryParts = {
      ...this.#queryParts,
      select: {[field]: row.field(field)},
      selectOneField: true,
    };
    return new SelectQuery(this.#picks, null, queryParts, this.#database);
  }

  /**
   * Select fields
   * @template {(keyof T[0])[]} F
   * @param {F} fields
   * @returns {SelectQuery<T, {object: Pick<T[0], F[number]>}>}
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
    const shape = partialObjectShape(row.type, fields);
    const selectPick = selectResultPick({object: shape});
    return new SelectQuery(this.#picks, selectPick, queryParts, this.#database);
  }

  /**
   * @template {SelectPicks} U
   * @param {(...args: FctArgs<T>) => U} fct
   * @returns {SelectQuery<T, SelectResultOf<U>>}
   */
  select(fct) {
    const selectPicks = fct(...this.#picks, pickOperators);
    const queryParts = {
      ...this.#queryParts,
      select: selectPicks,
    };
    const selectPick = selectResultPick(selectResultOf(selectPicks));
    return new SelectQuery(this.#picks, selectPick, queryParts, this.#database);
  }

  /**
   * @returns {SelectQuery<T, T[number]>}
   */
  selectAll() {
    const isRoot = this.#picks.every((pick) => pick.isRoot());
    const selectPicks = Object.fromEntries(
      this.#picks.flatMap((pick) =>
        Object.keys(pick.type.properties).map((key) => [key, pick.get(key)])
      )
    );
    const queryParts = {
      ...this.#queryParts,
      select: isRoot ? /** @type {const} */ ('*') : selectPicks,
    };
    /** @type {RowPick<T[number]>} */
    const selectPick = selectResultPick(selectResultOf(selectPicks));
    return new SelectQuery(this.#picks, selectPick, queryParts, this.#database);
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
