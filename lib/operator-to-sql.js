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
    if (!values) {
      return escape(value);
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
    const sqlValue = sql(value, values);
    if (!sqlValue) {
      return '';
    }
    if (sqlValue.startsWith('NOT ')) {
      return sqlValue.slice('NOT '.length);
    }
    return `NOT ${sqlValue}`;
  },

  stringConcat({values: list}, values) {
    return `(${list
      .map((value) => sql(value, values))
      .filter(Boolean)
      .join(' || ')})`;
  },

  eq({a, b}, values) {
    return `${sql(a, values)} IS ${sql(b, values)}`;
  },

  neq({a, b}, values) {
    return `${sql(a, values)} IS NOT ${sql(b, values)}`;
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
    const sqlConditions = conditions
      .map((condition) => sql(condition, values))
      .filter(Boolean);
    if (sqlConditions.length === 0) {
      return '';
    }
    return `(${sqlConditions.join(' AND ')})`;
  },

  or({conditions}, values) {
    const sqlConditions = conditions
      .map((condition) => sql(condition, values))
      .filter(Boolean);
    if (sqlConditions.length === 0) {
      return '';
    }
    return `(${sqlConditions.join(' OR ')})`;
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
    const sqlCondition = sql(condition, values);
    if (!sqlCondition) {
      return '';
    }
    return `EXISTS (
      SELECT *
      FROM json_each(${sql(list, values)})
      WHERE ${sqlCondition}
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

  count({field, distinct}, values) {
    if (!field) {
      return `COUNT(*)`;
    }
    return `COUNT(${distinct ? 'DISTINCT ' : ''}${sql(field, values)})`;
  },

  avg({field}, values) {
    return `AVG(${sql(field, values)})`;
  },

  max({field}, values) {
    return `MAX(${sql(field, values)})`;
  },

  min({field}, values) {
    return `MIN(${sql(field, values)})`;
  },

  sum({field, distinct}, values) {
    return `SUM(${distinct ? 'DISTINCT ' : ''}${sql(field, values)})`;
  },

  groupArray({field}, values) {
    return `json_group_array(${sql(field, values)})`;
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
 * @param {any[] | null} values
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
