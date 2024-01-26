import fs from 'fs';
import pathToFileURL from 'url';
import env from '../../config/env.js';
import { log } from '../index.js';

export const getCertificates = () => {
    try {
        return {
            key: fs.readFileSync(pathToFileURL(env.app.key)),
            cert: fs.readFileSync(pathToFileURL(env.app.cert)),
            ca: fs.readFileSync(pathToFileURL(env.app.ca))
        }
    } catch (error) {
        log.error(`Failed to load SSL certificates!`);
    }
};
