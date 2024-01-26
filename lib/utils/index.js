import _ from 'lodash';

//callback function to stringify circular objects into json and functions to strings
export const refReplacer = () => {
  let m = new Map(),
    v = new Map(),
    init = null;

  return function (field, value) {
    typeof val === 'function' ? val + '' : val;
    let p = m.get(this) + (Array.isArray(this) ? `[${field}]` : '.' + field);
    let isComplex = value === Object(value)

    if (isComplex) m.set(value, p);

    let pp = v.get(value) || '';
    let path = p.replace(/undefined\.\.?/, '');
    let val = pp ? `#REF:${pp[0] == '[' ? '$' : '$.'}${pp}` : value;

    !init ? (init = value) : (val === init ? val = "#REF:$" : 0);
    if (!pp && isComplex) v.set(value, path);

    return val;
  }
};
export const randomNumber = (min = Number.MIN_SAFE_INTEGER, max = Number.MAX_SAFE_INTEGER) => Math.random() * (max - min + 1) + min;
export const randomInt = (min, max) => Math.round(randomNumber(min, max));
export const randomChances = (...chances) => {
  chances = chances.reduce((p, x) => Array.isArray(x) ? [...p, ...x] : [...p, x], [])
  chances = chances.map(x => x ? x : 0); // converts "null" and "undefined" to 0
  var total = chances.reduce((prev, x) => prev += x, 0);
  var rand = Math.random() * total;
  var res = 0;
  var acum = 0;
  for (let i = 0; i < chances.length; i++) {
    if (rand > acum) res = i;
    acum = acum + chances[i];
  }
  return res;
};
export const randomElements = (arr, count) => {
  let res = [];
  let tmp = [...arr];
  for (let i = 0; i < count && tmp.length != 0; i++) {
    let pos = randomIntFromInterval(0, tmp.length - 1);
    res.push(tmp.splice(pos, 1)[0]);
  }
  return res;
};
export const randomElement = (arr) => arr[randomInt(0, arr.length - 1)];
export const randomString = (n = 32) => {
  let result = Math.random().toString(36).slice(-n);
  while (result.length < n) {
    result += Math.random().toString(36).slice(-n + result.length)
  }
  return result;
};
/**
 * @param {string} percentage Chances to return true (0 is 0% and 1 is 100%)
 * @returns {Boolean}
*/
export const chance = (percentage) => Math.random() > percentage;

export const SECOND_2_TICKS = 1000;
export const MINUTE_2_TICK = SECOND_2_TICKS * 60;
export const HOUR_2_TICK = MINUTE_2_TICK * 60;
export const DAY_2_TICK = HOUR_2_TICK * 24;
export const WEEK_2_TICK = DAY_2_TICK * 7;
export const randomDate = (maxDaysBack = 1, maxDaysFuture = 1) => Math.floor(Date.now() + DAY_2_TICK * randomNumber(maxDaysBack, maxDaysFuture));
export const nextMonth = (ts) => {
  let old = new Date(_.isString(ts) ? parseInt(ts) : ts);
  let date = old.getMonth() === 11 ?
    `${1}/1/${old.getFullYear() + 1}` :
    `${old.getMonth() + 2}/1/${old.getFullYear()}`;
  return parseInt((new Date(date)).valueOf() + 18000000); //Arregla la dif de horario entre arg y utc
};

export const toPascalCase = (str, explicitConv) => {
  let res = null;
  if (explicitConv?.[str]) {
    res = explicitConv[str];
  } else {
    res = str[0].toUpperCase() + str.slice(1);
    res = res.endsWith('Id') ? res.slice(0, res.length - 2) + "ID" : res;
  }
  return res;
};
export const toCamelCase = (str, explicitConv) => {
  let res = null;
  if (explicitConv?.[str]) {
    res = explicitConv[str];
  } else {
    res = str[0].toLowerCase() + str.slice(1);
    res = res.endsWith('ID') ? res.slice(0, res.length - 2) + "Id" : res;
  }
  return res;
};
export const keysToPC = (obj, explicitConv) => Object.entries(obj).reduce((p, x) => ({ ...p, [toPascalCase(x[0], explicitConv)]: x[1] }), {});
export const keysToCC = (obj, explicitConv) => Object.entries(obj).reduce((p, x) => ({ ...p, [toCamelCase(x[0], explicitConv)]: x[1] }), {});

export const timeout = (ms) => new Promise(res => setTimeout(res, ms));
/**
 * It removes all the empty strings and null values from an object.
 * @param obj - The object to remove empty properties from.
 * @returns The object with empty strings and null values removed.
 */
export const removeEmpty = (obj) => {
  Object.keys(obj).forEach((k) => ((obj[k] == null || obj[k] === "") && obj[k] !== undefined) && delete obj[k]);
  return obj;
};
export const firstToUpper = (str) => str[0].toUpperCase() + str.slice(1).toLowerCase();
export const zfill = (number, width) => {
  var numberOutput = Math.abs(number); /* Valor absoluto del número */
  var length = number.toString().length; /* Largo del número */
  var zero = "0"; /* String de cero */

  if (width <= length) {
    if (number < 0) {
      return ("-" + numberOutput.toString());
    } else {
      return numberOutput.toString();
    }
  } else {
    if (number < 0) {
      return ("-" + (zero.repeat(width - length)) + numberOutput.toString());
    } else {
      return ((zero.repeat(width - length)) + numberOutput.toString());
    }
  }
}
/**
 * Just like JSON.stringify, but converts functions to strings and resolves circular references.
 * @param obj - The object to stringify.
 * @returns The stringified json.
 */
export const stringify = (obj) => JSON.stringify(obj, refReplacer);

export const lerp = (x, y, a) => x * (1 - a) + y * a;
export const clamp = (a, min = 0, max = 1) => Math.min(max, Math.max(min, a));
export const invlerp = (x, y, a) => clamp((a - x) / (y - x));
export const range = (x1, y1, x2, y2, a) => lerp(x2, y2, invlerp(x1, y1, a));

export const indexBy = (rows, pks) => {
  if (pks == null) throw new Error('pks is null!');
  if (pks?.length === 0) throw new Error('pks is empty!');
  if (!Array.isArray(pks)) pks = [pks];
  const indexed = {};
  rows.forEach(row => {
    const key = pks.reduce((p, pk) => p + '-' + row[pk], '').slice(1);
    indexed[key] = row;
  });
  return indexed;
};
export const indexArraysBy = (rows, pks) => {
  if (pks == null) throw new Error('pks is null!');
  if (pks?.length === 0) throw new Error('pks is empty!');
  if (!Array.isArray(pks)) pks = [pks];
  const indexed = {};
  rows.forEach(row => {
    const key = pks.reduce((p, pk) => p + '-' + row[pk], '').slice(1);
    indexed[key] ??= [];
    indexed[key].push(row);
  });
  return indexed;
};
export const updateIndex = ({ indexed, pks }, row, action) => {
  const key = pks.reduce((p, pk) => p + '-' + row[pk], '');
  switch (action?.toLowerCase()) {
    case 'insert':
      indexed[key] = row;
      break;
    case 'update':
      indexed[key] = { ...indexed[key], ...row };
      break;
    case 'delete':
      delete indexed[key];
      break;
    default:
      throw 'Invalid action: ' + action;
  };
  return indexed;
};
export const getIndex = ({ indexed, pks }, pksValues) => {
  const key = pks.reduce((p, pk) => p + '-' + pksValues[pk], '').slice(1);
  return indexed[key];
};

export default {
  ..._, refReplacer,
  randomNumber, randomInt, randomChances, randomElements, randomElement, randomString, chance,
  SECOND_2_TICKS, MINUTE_2_TICK, HOUR_2_TICK, DAY_2_TICK, randomDate, nextMonth,
  toPascalCase, toCamelCase, keysToPC, keysToCC, timeout, removeEmpty, firstToUpper, zfill, stringify,
  lerp, clamp, invlerp, range, indexBy, indexArraysBy, updateIndex, getIndex
};