import env from './config/env.js';
import express from 'express';
import controllers from './controllers/index.js'
import https from 'https';
import { ssl, log } from './lib/index.js';

(async () => {
    try {
        log.info(`APP: Starting...`);
        let app = express();
        app.use(controllers);
        app.listen(env.app.port, e => {
            e ? log.fatal(e) : log.info(`APP: HTTP Server listening at port \x1b[33m${env.app.port}\x1b[0m`);
            let certificates = ssl.getCertificates();
            if (certificates) {
                https.createServer(certificates, app).listen(env.app.ssl_port, null, null, e => {
                    if (e) {
                        log.fatal(e);
                    } else {
                        app.get('*', (req, res) => res.redirect('https://' + req.headers.host + req.url))
                        log.info(`APP: HTTPS Server listening at port \x1b[33m${env.app.ssl_port}\x1b[0m`)
                    }
                }).on('error', log.fatal);
            }
        }).on('error', log.fatal);
    } catch (e) {
        log.fatal(e)
    }
})();

