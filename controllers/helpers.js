import { log } from '../lib/index.js';
import {z, ZodError} from 'zod';
/**
* @param {object} req -> request
* @param {object} res -> response
* @param {function} criteria -> function that evaluates auth JWT
* @returns {Promise<[req:object,res:object]>} returns updated [req,res]
*/
const testAuth = async (req, res, criteria) => {
  let valid = false;
  try {
    valid = await criteria(req?.auth);
  } catch (error) {
    log.error(error);
    valid = false;
  }

  if (!valid) {
    log.warn(`Auth test failed: ` + criteria);
    res.status(401).body.error = "Unauthorized";
  }

  return [req, res];
};
/**
* @param {object} req -> request
* @param {object} res -> response
* @param {function} criteria -> function that evaluates url query
* @returns {Promise<[req:object,res:object]} returns updated [req,res]
*/
const testQuery = async (req, res, criteria) => {
  let valid = false;
  try {
    valid = await criteria(req.query);
  } catch (error) {
    log.error(error);
    valid = false;
  }

  if (!valid) {
    log.warn(`Test query failed: ` + criteria);
    res.status(400).body.error = "Invalid query format";
  }

  return [req, res];
};
/**
* @param {object} req -> request
* @param {object} res -> response
* @param {function} criteria -> function that evaluates request body
* @returns {Promise<[req:object,res:object]>} returns updated [req,res]
*/
const testBody = async (req, res, criteria) => {
  let valid = false;
  try {
    valid = await criteria(req.body);
  } catch (error) {
    log.error(error);
    valid = false;
  }
  if (!valid) {
    log.warn(`Test body failed: ` + criteria);
    res.status(400).body.error = "Invalid body format";
  }
  return [req, res];
};
const testZbody = async (req, res, criteria) => {

  const zObject = z.object(criteria);
    try{
      zObject.parse(req.body);
    } catch (error) {
      if (error instanceof ZodError) {
        console.log('Zod error')
        //res.status(422).json(error);
        res.status(422).body = error.issues.map(issue => ({path: issue.path, message: issue.message}));
        //res.status(400).body.error = "Invalid body formatZod";
      } else {
        console.log(error);
        res.status(500);
      }
    }
    return [req, res];
};
/**
* @param {object} req -> request
* @param {object} res -> response
* @param {function} criteria -> function that evaluates each body row
* @returns {Promise<[req:object,res:object]>} returns updated [req,res]
*/
const testBodyRows = async (req, res, criteria) => {
  let valid = false;
  try {
    valid = await req.body.reduce(async (p, x) => p && await criteria(x), true);
  } catch (error) {
    log.error(error);
    valid = false;
  }

  if (!valid) {
    log.warn(`Test body rows failed: ` + criteria);
    res.status(400).body.error = "Invalid body format";
  }
  return [req, res];
};

/**
* @param {object} req -> request
* @param {object} res -> response
* @param {function} criteria -> function that evaluates url params
* @returns {Promise<[req:object,res:object]} returns updated [req,res]
*/
const testParams = async (req, res, criteria) => {
  let valid = false;
  try {
    valid = await criteria(req.params);
  } catch (error) {
    log.error(error);
    valid = false;
  }

  if (!valid) {
    log.warn(`Test params failed: ` + criteria);
    res.status(400).body.error = "Invalid URL params";
  }
  return [req, res];
};
/**
* @param {string} str -> string to convert
* @returns {string} string converted to 'PascalCase' 
*/
const toPascalCase = str => {

  str[0].toUpperCase() + str.slice(1);
}
/**
* @param {string} str -> string to convert
* @returns {string} string converted to 'camelCase' 
*/
const toCamelCase = str => str[0].toLowerCase() + str.slice(1);
/**
* @param {object} obj -> object
* @returns {object} returns object with all keys converted to 'PascalCase'
*/
const keysToPC = obj => Object.entries(obj).reduce((p, x) => ({ ...p, [toPascalCase(x[0])]: x[1] }), {});
/**
* @param {object} obj -> object
* @returns {object} returns object with all keys converted to 'camelCase'
*/
const keysToCC = obj => Object.entries(obj).reduce((p, x) => ({ ...p, [toCamelCase(x[0])]: x[1] }), {});
/**
* @param {object} obj -> input
* @returns {[]} returns input as array 
*/
const toArray = obj => Array.isArray(obj) ? [...obj] : [obj];
/**
* @param {object} arr -> input array
* @param {function} criteria -> function that evaluates each array row
* @returns {[object,object]} returns updated [req,res]
*/
const testArr = (arr, criteria) => {
  let valid = false;
  try {
    valid = arr.reduce((p, x) => p && criteria(x), true);
  } catch (error) {
    log.error(error);
    valid = false;
  }
  return valid;
};
/**
* @param {object} req -> request
* @param {object} res -> responserow
* @returns {[object,object]} returns updated [req,res]
*/
const bodyToArray = (req, res) => {
  req.body = toArray(req.body);
}

const filesToArray = (req, res) => {
  req.files = toArray(req.files);
}

const validEmail = (email) => {
  var re = /\S+@\S+\.\S+/;
  return re.test(email);
};

/**
* @param {object} mapFrom -> request
* @param {object} res -> response
* @returns {[object,object]} returns updated [req,res]
*/
const mapRows = (req, res, baseObj) => {
  try {
    if (!Array.isArray(req.body)) throw new Error('Body is not an array');
    let bodyAux = [];
    req.body.forEach((row, i) => {
      if (typeof (req.body) !== 'object') throw new Error('Row is not an object');
      let newRow = {};
      (baseObj).forEach(key => row[key] !== undefined ? newRow[key] = row[key] : null);
      bodyAux.push(newRow);
    });
    req.body = bodyAux;
  } catch (error) {
    log.warn(`MapRows failed: ` + JSON.stringify(baseObj) + error);
    res.status(400).body.error = "Invalid body format";
  }
  return [req, res];
}


export default { mapRows, testAuth, testQuery, filesToArray, testBody, testBodyRows, testParams, toPascalCase, toCamelCase, keysToPC, keysToCC, toArray, testArr, bodyToArray, validEmail, testZbody };