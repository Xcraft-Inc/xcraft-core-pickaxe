// @ts-check

const {isAnyPick} = require('./picks.js');

/**
 * @typedef {import("./operators.js").Operator} Operator
 */
/**
 * @typedef {import('./picks.js').AnyPick} AnyPick
 */

function escape(value) {
  if (typeof value === 'string') {
    return `'${value.replace(/'/g, "''")}'`;
  }
  return value;
}

const operators = {
  value({value}, values) {
    if (typeof value === 'boolean') {
      return value ? 'TRUE' : 'FALSE';
    }
    values.push(value);
    return '?';
  },

  null(_, values) {
    return 'NULL';
  },

  field({field}, values) {
    // Note: field is not validated
    if (typeof field === 'string') {
      return `${field}`;
    }
    return sql(field, values);
  },

  as({value, name}, values) {
    return `${sql(value, values)} AS ${escape(name)}`;
  },

  get({value, path}, values) {
    // TODO: try values.push(path) => '?'
    // return `json_extract(${field}, ${sql(path, values)})`;
    return `json_extract(${sql(value, values)}, '$.' || ${escape(
      path.join('.')
    )})`;
  },

  not({value}, values) {
    return `NOT ${sql(value, values)}`;
  },

  stringConcat({values: list}, values) {
    return `(${list
      .map((value) => sql(value, values))
      .filter(Boolean)
      .join(' || ')})`;
  },

  eq({a, b}, values) {
    return `${sql(a, values)} = ${sql(b, values)}`;
  },

  neq({a, b}, values) {
    return `${sql(a, values)} <> ${sql(b, values)}`;
  },

  gte({a, b}, values) {
    return `${sql(a, values)} >= ${sql(b, values)}`;
  },

  gt({a, b}, values) {
    return `${sql(a, values)} > ${sql(b, values)}`;
  },

  lte({a, b}, values) {
    return `${sql(a, values)} <= ${sql(b, values)}`;
  },

  lt({a, b}, values) {
    return `${sql(a, values)} < ${sql(b, values)}`;
  },

  in({value, list}, values) {
    return `${sql(value, values)} IN (${list
      .map((v) => sql(v, values))
      .join(',')})`;
  },

  like({value, pattern}, values) {
    return `${sql(value, values)} LIKE ${sql(pattern, values)}`;
  },

  and({conditions}, values) {
    if (conditions.length === 0) {
      return '';
    }
    return `(${conditions
      .map((condition) => sql(condition, values))
      .filter(Boolean)
      .join(' AND ')})`;
  },

  or({conditions}, values) {
    if (conditions.length === 0) {
      return '';
    }
    return `(${conditions
      .map((condition) => sql(condition, values))
      .filter(Boolean)
      .join(' OR ')})`;
  },

  length({list}, values) {
    return `json_array_length(${sql(list, values)})`;
  },

  includes({list, value}, values) {
    return `EXISTS (
      SELECT *
      FROM json_each(${sql(list, values)})
      WHERE json_each.value = ${sql(value, values)}
    )`;
  },

  some({list, condition}, values) {
    return `EXISTS (
      SELECT *
      FROM json_each(${sql(list, values)})
      WHERE ${sql(condition, values)}
    )`;
  },

  eachValue(_, values) {
    return 'json_each.value';
  },

  eachKey(_, values) {
    return 'json_each.key';
  },

  keys({obj}, values) {
    return `(SELECT json_group_array(json_each.key) FROM json_each(${sql(
      obj,
      values
    )}))`;
  },

  values({obj}, values) {
    return `(SELECT json_group_array(json_each.value) FROM json_each(${sql(
      obj,
      values
    )}))`;
  },

  asc({value}, values) {
    return `${sql(value, values)} ASC`;
  },

  desc({value}, values) {
    return `${sql(value, values)} DESC`;
  },
};

/**
 * @param {Operator | AnyPick} operatorOrPick
 * @returns {Operator}
 */
function getOperator(operatorOrPick) {
  if (isAnyPick(operatorOrPick)) {
    return operatorOrPick.value;
  }
  return operatorOrPick;
}

/**
 * @param {Operator | AnyPick} operatorOrPick
 * @param {any[]} values
 * @returns {string}
 */
function sql(operatorOrPick, values) {
  const operator = getOperator(operatorOrPick);
  const operatorName = operator.operator;
  if (!(operatorName in operators)) {
    throw new Error(`Unknown operator '${JSON.stringify(operator)}'`);
  }
  return operators[operatorName](operator, values);
}

function operatorToSql(operator) {
  const values = [];
  return {
    sql: sql(operator, values),
    values,
  };
}

module.exports = {
  sql,
  operatorToSql,
};
