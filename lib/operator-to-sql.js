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
  value({value}, context) {
    if (typeof value === 'boolean') {
      return value ? 'TRUE' : 'FALSE';
    }
    if (!context.values) {
      return escape(value);
    }
    context.values.push(value);
    return '?';
  },

  null(_, context) {
    return 'NULL';
  },

  field({field}, context) {
    // Note: field is not validated
    if (typeof field === 'string') {
      return `${field}`;
    }
    return sql(field, context);
  },

  as({value, name}, context) {
    return `${sql(value, context)} AS ${escape(name)}`;
  },

  get({value, path}, context) {
    // TODO: try values.push(path) => '?'
    // return `json_extract(${field}, ${sql(path, values)})`;

    if (value) {
      return `json_extract(${sql(value, context)}, ${escape(
        '$.' + path.join('.')
      )})`;
    } else if (context.scope) {
      return `json_extract(${sql(context.scope, context)}, ${escape(
        '$.' + path.join('.')
      )})`;
    }
    throw new Error("Missing 'value' or 'scope' for the 'get' operator");
  },

  not({value}, context) {
    const sqlValue = sql(value, context);
    if (!sqlValue) {
      return '';
    }
    if (sqlValue.startsWith('NOT ')) {
      return sqlValue.slice('NOT '.length);
    }
    return `NOT ${sqlValue}`;
  },

  stringConcat({values: list}, context) {
    return `(${list
      .map((value) => sql(value, context))
      .filter(Boolean)
      .join(' || ')})`;
  },

  stringLength({value}, context) {
    return `LENGTH(${sql(value, context)})`;
  },

  substr({value, start, length}, context) {
    const lengthSql = length !== undefined ? `, ${sql(length, context)}` : '';
    return `SUBSTR(${sql(value, context)}, ${sql(start, context)}${lengthSql})`;
  },

  eq({a, b}, context) {
    return `${sql(a, context)} IS ${sql(b, context)}`;
  },

  neq({a, b}, context) {
    return `${sql(a, context)} IS NOT ${sql(b, context)}`;
  },

  gte({a, b}, context) {
    return `${sql(a, context)} >= ${sql(b, context)}`;
  },

  gt({a, b}, context) {
    return `${sql(a, context)} > ${sql(b, context)}`;
  },

  lte({a, b}, context) {
    return `${sql(a, context)} <= ${sql(b, context)}`;
  },

  lt({a, b}, context) {
    return `${sql(a, context)} < ${sql(b, context)}`;
  },

  in({value, list}, context) {
    return `${sql(value, context)} IN (${list
      .map((v) => sql(v, context))
      .join(',')})`;
  },

  like({value, pattern}, context) {
    return `${sql(value, context)} LIKE ${sql(pattern, context)}`;
  },

  glob({value, pattern}, context) {
    return `${sql(value, context)} GLOB ${sql(pattern, context)}`;
  },

  match({a, b}, context) {
    return `${sql(a, context)} MATCH ${sql(b, context)}`;
  },

  and({conditions}, context) {
    const sqlConditions = conditions
      .map((condition) => sql(condition, context))
      .filter(Boolean);
    if (sqlConditions.length === 0) {
      return '';
    }
    return `(${sqlConditions.join(' AND ')})`;
  },

  or({conditions}, context) {
    const sqlConditions = conditions
      .map((condition) => sql(condition, context))
      .filter(Boolean);
    if (sqlConditions.length === 0) {
      return '';
    }
    return `(${sqlConditions.join(' OR ')})`;
  },

  abs({value}, context) {
    return `ABS(${sql(value, context)})`;
  },

  plus({values: list}, context) {
    return `(${list
      .map((value) => sql(value, context))
      .filter(Boolean)
      .join(' + ')})`;
  },

  minus({values: list}, context) {
    return `(${list
      .map((value) => sql(value, context))
      .filter(Boolean)
      .join(' - ')})`;
  },

  length({list}, context) {
    return `json_array_length(${sql(list, context)})`;
  },

  includes({list, value}, context) {
    return `EXISTS (
      SELECT *
      FROM json_each(${sql(list, context)})
      WHERE json_each.value = ${sql(value, context)}
    )`;
  },

  some({list, condition}, context) {
    const sqlCondition = sql(condition, context);
    if (!sqlCondition) {
      return '';
    }
    return `EXISTS (
      SELECT *
      FROM json_each(${sql(list, context)})
      WHERE ${sqlCondition}
    )`;
  },

  query({query}, context) {
    const {queryToSql} = require('./query-to-sql.js');
    return `(${queryToSql(query, context.values).sql})`;
  },

  each({value}, context) {
    return `json_each(${sql(value, context)})`;
  },

  eachValue(_, context) {
    return 'json_each.value';
  },

  eachKey(_, context) {
    return 'json_each.key';
  },

  keys({obj}, context) {
    return `(SELECT json_group_array(json_each.key) FROM json_each(${sql(
      obj,
      context
    )}))`;
  },

  values({obj}, context) {
    return `(SELECT json_group_array(json_each.value) FROM json_each(${sql(
      obj,
      context
    )}))`;
  },

  asc({value}, context) {
    return `${sql(value, context)} ASC`;
  },

  desc({value}, context) {
    return `${sql(value, context)} DESC`;
  },

  count({field, distinct}, context) {
    if (!field) {
      return `COUNT(*)`;
    }
    return `COUNT(${distinct ? 'DISTINCT ' : ''}${sql(field, context)})`;
  },

  avg({field}, context) {
    return `AVG(${sql(field, context)})`;
  },

  max({field}, context) {
    return `MAX(${sql(field, context)})`;
  },

  min({field}, context) {
    return `MIN(${sql(field, context)})`;
  },

  sum({field, distinct}, context) {
    return `SUM(${distinct ? 'DISTINCT ' : ''}${sql(field, context)})`;
  },

  groupArray({field}, context) {
    return `json_group_array(${sql(field, context)})`;
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
 * @typedef {{
 *   values: any[] | null,
 *   scope?: any,
 * }} OperatorToSqlContext
 */

/**
 * @param {Operator | AnyPick} operatorOrPick
 * @param {OperatorToSqlContext} context
 * @returns {string}
 */
function sql(operatorOrPick, context) {
  const operator = getOperator(operatorOrPick);
  const operatorName = operator.operator;
  if (!(operatorName in operators)) {
    throw new Error(`Unknown operator '${JSON.stringify(operator)}'`);
  }
  return operators[operatorName](operator, context);
}

function operatorToSql(operator) {
  const context = {
    values: [],
  };
  return {
    sql: sql(operator, context),
    values: context.values,
  };
}

module.exports = {
  sql,
  operatorToSql,
};
