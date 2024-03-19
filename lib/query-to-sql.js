// @ts-check
/**
 * @typedef {import("./query-builder.js").QueryObj} QueryObj
 */

const {sql} = require('./operator-to-sql.js');

/**
 * @param {QueryObj["select"]} select
 * @param {any[] | null} values
 * @returns {string}
 */
function selectFields(select, values) {
  if (Array.isArray(select)) {
    return select.map((value) => sql(value, values)).join(', ');
  }
  return Object.entries(select)
    .map(([name, value]) => `${sql(value, values)} AS ${name}`)
    .join(', ');
}

/**
 * @param {NonNullable<QueryObj["orderBy"]>} orderBy
 * @param {any[] | null} values
 * @returns {string}
 */
function orderByFields(orderBy, values) {
  if (Array.isArray(orderBy)) {
    return orderBy.map((value) => sql(value, values)).join(', ');
  }
  return sql(orderBy, values);
}

/**
 * @param {NonNullable<QueryObj["groupBy"]>} groupBy
 * @param {any[] | null} values
 * @returns {string}
 */
function groupByFields(groupBy, values) {
  if (Array.isArray(groupBy)) {
    return groupBy.map((value) => sql(value, values)).join(', ');
  }
  return sql(groupBy, values);
}

/**
 * @param {QueryObj} query
 * @param {any[] | null} [values]
 * @returns {{sql: string, values: any[] | null}}
 */
function queryToSql(query, values = []) {
  const distinct = query.distinct ? 'DISTINCT ' : '';
  // Note: query.from is not validated
  const from =
    typeof query.from === 'string' ? query.from : sql(query.from, values);
  let result = `SELECT ${distinct}${selectFields(query.select, values)}`;
  result += '\n' + `FROM ${from}`;
  if (query.where) {
    result += '\n' + `WHERE ${sql(query.where, values)}`;
  }
  if (query.orderBy) {
    result += '\n' + `ORDER BY ${orderByFields(query.orderBy, values)}`;
  }
  if (query.groupBy) {
    result += '\n' + `GROUP BY ${groupByFields(query.groupBy, values)}`;
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
