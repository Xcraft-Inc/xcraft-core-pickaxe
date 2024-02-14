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
    return select.map((rep) => sql(rep.value, values)).join(', ');
  }
  return Object.entries(select)
    .map(([name, rep]) => `${sql(rep.value, values)} AS ${name}`)
    .join(', ');
}

/**
 * @param {NonNullable<QueryObj["orderBy"]>} orderBy
 * @param {any[]} values
 * @returns {string}
 */
function orderByFields(orderBy, values) {
  if (Array.isArray(orderBy)) {
    return orderBy.map((rep) => sql(rep.value, values)).join(', ');
  }
  return sql(orderBy.value, values);
}

/**
 * @param {QueryObj} query
 * @returns {{sql: string, values: any[]}}
 */
function queryToSql(query) {
  const values = [];
  let result = `SELECT ${selectFields(query.select, values)}`;
  result += '\n' + `FROM ${query.from}`;
  if (query.where) {
    result += '\n' + `WHERE ${sql(query.where, values)}`;
  }
  if (query.orderBy) {
    result += '\n' + `ORDER BY ${orderByFields(query.orderBy, values)}`;
  }
  return {sql: result, values};
}

module.exports = {
  queryToSql,
};
