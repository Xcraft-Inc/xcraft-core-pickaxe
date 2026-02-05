// @ts-check

const {isAnyPick, BasePick} = require('./picks.js');

/**
 * @typedef {import("./operators.js").Expression} Expression
 */

function escape(value) {
  if (typeof value === 'string') {
    return `'${value.replace(/'/g, "''")}'`;
  }
  return value;
}

function squashValueAndPath(value, path) {
  if (value) {
    const operatorName = value.operator;
    if (operatorName === 'get') {
      return squashValueAndPath(value.value, [value.path, ...path]);
    }
  }
  return [value, path];
}

const operators = {
  unsafeSql({sql}, context) {
    return sql;
  },

  value({value}, context) {
    if (typeof value === 'boolean') {
      // return value ? 'TRUE' : 'FALSE';
      //
      // Value 1 and 0 works better with indexed content.
      // For example, "json_extract(action, '$.path.to.value') IS FALSE"
      // does not use the corresponding json_extract index.
      return value ? 1 : 0;
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

  field({tableName, field}, context) {
    // Note: field is not validated
    if (typeof field === 'string') {
      if (context.useTableNames && tableName) {
        return `${tableName}.${field}`;
      }
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

    [value, path] = squashValueAndPath(value, path);

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
    const valueSql = sql(value, context);
    const startSql = sql(start, context);
    const lengthSql = length !== undefined ? `, ${sql(length, context)}` : '';
    return `SUBSTR(${valueSql}, ${startSql}${lengthSql})`;
  },

  eq({a, b}, context) {
    const operator = context.equalOperator === '=' ? '=' : 'IS';
    return `${sql(a, context)} ${operator} ${sql(b, context)}`;
  },

  neq({a, b}, context) {
    const operator = context.equalOperator === '=' ? '<>' : 'IS NOT';
    return `${sql(a, context)} ${operator} ${sql(b, context)}`;
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

  ifNull({a, b}, context) {
    return `IFNULL(${sql(a, context)},${sql(b, context)})`;
  },

  if({condition, a, b}, context) {
    return `IF(${sql(condition, context)},${sql(a, context)},${sql(
      b,
      context
    )})`;
  },

  case({whenConditions, elseValue}, context) {
    const whenSql = whenConditions
      .map(([condition, value]) => {
        const conditionSql = sql(condition, context);
        if (!conditionSql) {
          return '';
        }
        return `WHEN (${conditionSql}) THEN ${sql(value, context)}`;
      })
      .filter(Boolean)
      .join(' ');
    if (!whenSql) {
      return sql(elseValue, context);
    }
    return `CASE ${whenSql} ELSE ${sql(elseValue, context)} END`;
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
    return `(${
      queryToSql(query, context.values, {
        useTableNames: context.useTableNames,
      }).sql
    })`;
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

  nullsFirst({value}, context) {
    return `${sql(value, context)} NULLS FIRST`;
  },

  nullsLast({value}, context) {
    return `${sql(value, context)} NULLS LAST`;
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

  groupArray({field, orderBy}, context) {
    if (orderBy) {
      return `json_group_array(${sql(field, context)} ORDER BY ${sql(
        orderBy,
        context
      )})`;
    }
    return `json_group_array(${sql(field, context)})`;
  },
};

/**
 * @param {Expression | BasePick} expressionOrPick
 * @returns {Expression}
 */
function getExpression(expressionOrPick) {
  if (isAnyPick(expressionOrPick)) {
    return expressionOrPick.expression;
  }
  return expressionOrPick;
}

/**
 * @typedef {{
 *   values: any[] | null,
 *   scope?: any,
 *   useTableNames?: boolean,
 *   equalOperator: 'IS' | '=',
 * }} ExpressionToSqlContext
 */

/**
 * @param {Expression | BasePick} expressionOrPick
 * @param {ExpressionToSqlContext} context
 * @returns {string}
 */
function sql(expressionOrPick, context) {
  const expression = getExpression(expressionOrPick);
  const operatorName = expression.operator;
  if (!(operatorName in operators)) {
    throw new Error(`Unknown operator for '${JSON.stringify(expression)}'`);
  }
  return operators[operatorName](expression, context);
}

function expressionToSql(operator) {
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
  expressionToSql,
};
