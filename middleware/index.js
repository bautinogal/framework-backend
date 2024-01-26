import { log, crypto, sql } from '../lib/index.js';
import env from '../config/env.js';
import express from 'express';
import cors from 'cors';
import serveIndex from 'serve-index';
import cookieParser from 'cookie-parser'; // Herramienta para parsear las cookies
import bodyParser from 'body-parser'; // Herramienta para parsear el "cuerpo" de los requests
import path from 'path';
import { fileURLToPath } from 'url';
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const limitedArrStr = (arr, initialN = 10, endN = 0) => {
  let res = "";
  initialN = Math.min(initialN, arr.length);
  let initialArr = arr.slice(0, initialN);
  endN = Math.min(endN, arr.length - initialN);
  endN = endN > 0 ? endN : 0;
  let endArr = arr.slice(arr.length - endN, arr.length);
  let n = arr.length - initialArr.length - endN;
  let moreRows = n > 0 ? [`...${n} more rows`] : [];
  res += JSON.stringify([...initialArr, ...moreRows, ...endArr], null, 2);
  return res;
};

//Agrego todos los middlewares
export const before = (app) => {

  const addMiddleware = (app) => {

    app.use(cors({ origin: true, credentials: true }));
    //app.use('/static', express.static(process.cwd() + '/static'));

    //app.use('/', express.static(path.join(__dirname, '..', 'public', 'ecommerce')));
    app.use(express.static(path.join(__dirname, '..', 'static')));
    //app.use(express.static(path.join(__dirname, '..', 'landing')));
    app.use(cookieParser());
    //"bodyParser" es un middleware que me ayuda a parsear los requests
    app.use(bodyParser.urlencoded({ extended: true }));
    app.use(bodyParser.json({ limit: '20mb' }));
    app.use((err, req, res, next) => {
      if (err instanceof SyntaxError) {
        res.code = 400;
        res.body = 'Invalid Request Syntax';
      } else {
        throw err;
      }
      next();
    });
    app.use(cors({
      credentials: true,
      origin: true
    }));
    //app.use(upload.any());
    return app;
  };

  const addUtils = (req, res, next) => {
    res.status(200);
    res.body = {};
    res.ok = () => (res.statusCode >= 200 && res.statusCode < 300);
    res.setCode = (code) => {
      this.statusCode = code;
      return this
    }

    next();
  };

  const getAuth = async (req, res, next) => {

    const getPersonaId = async (auth) => {
      let personaId = null;
      try {
        personaId = (await sql.get('Personas', p => p.email === auth.jwt.payload.email))[0]?.personaId || null;
      } catch (e) {
        log.error(`Error getting auth 'Persona': ` + e.message);
        personaId = null;
      }
      auth.personaId = personaId;
      return auth;
    };

    const getRoles = async (auth) => {
      let roles = null;
      if (auth.personaId) {
        try {
          const rolesTable = await sql.get('Roles');
          const rolesPersonaMap = await sql.get('PersonasRolesMap', prm => prm.personaId === auth.personaId);
          roles = rolesPersonaMap.map(r => rolesTable.find(rol => rol.rolId === r.rolId)).map(r => r.nombre);
        } catch (e) {
          log.error(`Error getting roles 'Persona': ` + e.message);
          roles = null;
        }
      }
      auth.roles = roles;
      return auth;
    };

    const getPermisos = async (auth) => {
      let permisos = null;
      if (Array.isArray(auth.roles)) {
        try {
          permisos = [];
          const rolesTable = await sql.get('Roles');
          const permisosTable = await sql.get('Permisos');
          const permisosRolesMapTable = await sql.get('PermisosRolesMap');

          const rolsIds = rolesTable.filter(rol => auth.roles.includes(rol.nombre)).map(r => r.rolId);
          rolsIds.forEach(rolId => {
            const permisosIds = permisosRolesMapTable.filter(prm => prm.rolId === rolId).map(prm => prm.permisoId);
            const permisosNames = permisosTable.filter(p => permisosIds.includes(p.permisoId)).map(p => p.nombre);
            permisos = [...permisos, ...permisosNames]
          });
        } catch (e) {
          log.error(`Error getting roles 'Persona': ` + e.message);
          permisos = null;
        }
      }
      auth.permisos = permisos;
      return auth;
    };

    const authHeader = req.headers['authorization'];
    if (authHeader && authHeader.startsWith('Bearer ')) {
      let auth = { jwt: {} };
      auth.jwt.raw = authHeader.split('Bearer ')[1];
      try {
        let decodedJWT = crypto.verifyJWT(auth.jwt.raw);
        auth.jwt = { ...auth.jwt, ...decodedJWT, valid: true };
      }
      catch (error) {
        log.warn(`Error with auth JWT: ${error.message}`);
        auth.jwt.valid = false;
      }
      if (auth.jwt.valid) {
        auth = await getPersonaId(auth);
        auth = await getRoles(auth);
        auth = await getPermisos(auth);
      }
      req.auth = auth;
    }
    next();
  };

  const logReq = async (req, res, next) => {

    const bodyStr = JSON.stringify(req.body);
    //Document with request info that will be stored at the DB
    const getRequestData = async (req) => {
      var result = {
        url: req.protocol + "://" + req.get('host') + req.originalUrl,
        ip: req.headers['x-forwarded-for'] || req.connection.remoteAddress,
        auth: req.auth,
        ts: Date.now(),
        remoteAddress: req.ip,
        url: req.url,
        method: req.method,
        cookies: req.cookies,
        headers: req.headers,
        params: req.params,
        query: req.query,
        body: req.body,
      };

      result.hash = await crypto.hash(result);
      req.hash = result.hash;
      return result;
    };

    //Logs request data (settings from enviroment variables)
    const logRequest = async (data) => {
      const logOptions = env.middleware.log.req;
      if (eval(logOptions.req)) {
        let logStr = `UTC: ${(new Date()).toLocaleString('en-GB', { timeZone: 'UTC' })} -- Req: ${data.hash}
\x1b[35mREQ >>\x1b[0m\x1b[32m HTTP\x1b[0m \x1b[36m${data.method}\x1b[0m at \x1b[33m${data.url}\x1b[0m from \x1b[33m${req.ip}\x1b[0m`;
        if (eval(logOptions.headers)) logStr += (` 
Headers: ${JSON.stringify(data.headers, null, 2)}`);
        if (logOptions?.body === 'oneline') logStr += (`
Body: ${bodyStr?.substring(0, 80)}${bodyStr && bodyStr.length > 80 ? '...' : ''} (Length ${bodyStr?.length})`);
        else if (eval(logOptions.body)) logStr += (`
Body: ${Array.isArray(data.body) ? limitedArrStr(data.body, 5) : JSON.stringify(data.body, null, 2)}`);
        if (logOptions?.auth === 'oneline') logStr += (`
Auth: ${JSON.stringify({ valid: data?.auth?.jwt.valid, id: data?.auth?.personaId, email: data?.auth?.jwt?.payload.email, roles: data?.auth?.roles })}`);
        else if (eval(logOptions.auth)) logStr += (`
Auth: ${JSON.stringify(data.auth, null, 2)}`);

        log.info(`${logStr}
`);
      }
    };

    let reqData = await getRequestData(req);
    if (reqData) await logRequest(reqData);
    next();
  };

  app = addMiddleware(app);
  app.use(addUtils);
  app.use(getAuth);
  app.use(logReq);

  return app;
};

export const after = (app) => {

  const refreshAuth = (req, res, next) => {
    if (req.auth && req.auth.valid) {
      const auth = crypto.createJWT({
        userId: req.auth.payload.userId,
        expiration: Date.now() + env.dfltExpiration,
      });
      res.set("auth", auth);
    }
    next();
  };

  const errorHandler = (err, req, res, next) => {
    res.code = 500;
    res.message = null;
    res.error = err.stack;
    log.error(err);
    next();
  };

  const respond = (req, res, next) => {
    res.send(res.body);
    next();
  };

  const logRes = async (req, res, next) => {
    const logOptions = env.middleware.log.res;
    const bodyStr = JSON.stringify(res.body);
    try {


      if (eval(logOptions.res)) {
        let logStr = `UTC: ${(new Date()).toLocaleString('en-GB', { timeZone: 'UTC' })} -- Req: ${req.hash}
\x1b[35mREQ <<\x1b[0m \x1b[32m${res.statusCode} \x1b[0m (\x1b[32mHTTP\x1b[0m \x1b[36m${req.method}\x1b[0m at \x1b[33m${req.url}\x1b[0m from \x1b[33m${req.ip}\x1b[0m)`;
        if (eval(logOptions.headers)) logStr += (` 
Headers: ${JSON.stringify(res.getHeaders(), null, 2)}`);
        if (logOptions?.body === 'oneline') logStr += (`
Body: ${bodyStr?.substring(0, 80)}${bodyStr && bodyStr.length > 80 ? '...' : ''} (Length ${bodyStr?.length})`);
        else if (eval(logOptions.body)) logStr += (`
Body: ${Array.isArray(res.body) ? limitedArrStr(res.body, 5) : JSON.stringify(res.body, null, 2)}`);
        if (logOptions?.auth === 'oneline') logStr += (`
Auth: ${JSON.stringify({ valid: res?.auth?.jwt?.valid, id: res?.auth?.personaId, email: res?.auth?.jwt?.payload.email, roles: res?.auth?.roles })}`);
        else if (eval(logOptions.auth)) logStr += (`
Auth: ${JSON.stringify(res.auth, null, 2)}`);


        log.info(`${logStr}
`);
        //log.info(`\x1b[35m<<\x1b[0m \x1b[32m${res.statusCode} \x1b[0m (\x1b[32mHTTP\x1b[0m \x1b[36m${req.method}\x1b[0m at \x1b[33m${req.url}\x1b[0m from \x1b[33m${req.ip}\x1b[0m)`);
      }
      // if (eval(logOptions.headers))
      //   log.debug(` Headers: ${JSON.stringify(res.getHeaders(), null, 2)}`);
      // if (eval(logOptions.body))
      //   log.debug(` Body: ${Array.isArray(res.body) ? limitedArrStr(res.body, 5) : JSON.stringify(res.body, null, 2)}`);
      // if (eval(logOptions.auth))
      //   log.debug(` Auth: ${JSON.stringify(res.auth, null, 2)}`);

      // await mongo.update("requestsDB", "requests", {
      //   _id: req.reqId
      // }, {
      //   $set: {
      //     res: {
      //       code: res.code,
      //       body: res.body,
      //       headers: res.headers,
      //       auth: res.auth,
      //       error: res.error
      //     }
      //   }
      // });
    } catch (error) {
      log.error(error); // TODO: Improve this logs
    }
  };

  app.use(refreshAuth);
  app.use(errorHandler);
  app.use(respond);
  app.use(logRes);
  return app;
};
