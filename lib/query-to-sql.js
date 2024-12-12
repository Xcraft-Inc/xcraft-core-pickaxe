// @ts-check
/**
 * @typedef {import("./query-builder.js").QueryObj} QueryObj
 * @typedef {import("./operator-to-sql.js").OperatorToSqlContext} OperatorToSqlContext
 */

const {sql} = require('./operator-to-sql.js');

/**
 * @param {QueryObj["select"]} select
 * @param {OperatorToSqlContext} context
 * @returns {string}
 */
function selectFields(select, context) {
  if (Array.isArray(select)) {
    return select.map((value) => sql(value, context)).join(', ');
  }
  return Object.entries(select)
    .map(([name, value]) => `${sql(value, context)} AS ${name}`)
    .join(', ');
}

/**
 * @param {NonNullable<QueryObj["orderBy"]>} orderBy
 * @param {OperatorToSqlContext} context
 * @returns {string}
 */
function orderByFields(orderBy, context) {
  if (Array.isArray(orderBy)) {
    return orderBy.map((value) => sql(value, context)).join(', ');
  }
  return sql(orderBy, context);
}

/**
 * @param {NonNullable<QueryObj["groupBy"]>} groupBy
 * @param {OperatorToSqlContext} context
 * @returns {string}
 */
function groupByFields(groupBy, context) {
  if (Array.isArray(groupBy)) {
    return groupBy.map((value) => sql(value, context)).join(', ');
  }
  return sql(groupBy, context);
}

/**
 * @param {QueryObj} query
 * @param {any[] | null} [values]
 * @returns {{sql: string, values: any[] | null}}
 */
function queryToSql(query, values = []) {
  const context = {
    values,
    scope: query.scope,
  };
  const distinct = query.distinct ? 'DISTINCT ' : '';
  // Note: query.from is not validated
  const from =
    typeof query.from === 'string' ? query.from : sql(query.from, context);
  let result = `SELECT ${distinct}${selectFields(query.select, context)}`;
  result += '\n' + `FROM ${from}`;
  if (query.where) {
    result += '\n' + `WHERE ${sql(query.where, context)}`;
  }
  if (query.orderBy) {
    result += '\n' + `ORDER BY ${orderByFields(query.orderBy, context)}`;
  }
  if (query.groupBy) {
    result += '\n' + `GROUP BY ${groupByFields(query.groupBy, context)}`;
  }
  if (query.limit) {
    if (!Number.isInteger(query.limit)) {
      throw new Error(`Bad limit '${query.limit}'`);
    }
    result += '\n' + `LIMIT ${query.limit}`;
  }
  if (query.offset) {
    if (!Number.isInteger(query.offset)) {
      throw new Error(`Bad offset '${query.offset}'`);
    }
    result += '\n' + `OFFSET ${query.offset}`;
  }
  return {sql: result, values};
}

module.exports = {
  queryToSql,
};
