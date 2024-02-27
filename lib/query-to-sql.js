// @ts-check
/**
 * @typedef {import("./query-builder.js").QueryObj} QueryObj
 */

const {sql} = require('./operator-to-sql.js');

/**
 * @param {QueryObj["select"]} select
 * @param {any[]} values
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
 * @param {any[]} values
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
 * @param {any[]} values
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
 * @returns {{sql: string, values: any[]}}
 */
function queryToSql(query) {
  const values = [];
  const distinct = query.distinct ? 'DISTINCT ' : '';
  let result = `SELECT ${distinct}${selectFields(query.select, values)}`;
  result += '\n' + `FROM ${query.from}`;
  if (query.where) {
    result += '\n' + `WHERE ${sql(query.where, values)}`;
  }
  if (query.orderBy) {
    result += '\n' + `ORDER BY ${orderByFields(query.orderBy, values)}`;
  }
  if (query.groupBy) {
    result += '\n' + `GROUP BY ${groupByFields(query.groupBy, values)}`;
  }
  return {sql: result, values};
}

module.exports = {
  queryToSql,
};
