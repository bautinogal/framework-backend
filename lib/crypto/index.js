import env from '../../config/env.js';
import bcrypt from 'bcryptjs';
import jwt from 'jsonwebtoken';

const JWTKey = env.jwt.secret;
const swf = env.jwt.saltWorkFactor;

//----------------------------------- HASH -------------------------------------------

export const hash = async input => await (new Promise((res, rej) =>
    bcrypt.hash(input.toString(), swf, (err, hash) => err ? rej(err) : res(hash))));

export const compareHashed = bcrypt.compare;

//----------------------------------- JWT -------------------------------------------
export const createJWT = token => jwt.sign(token, JWTKey);

export const decodeJWT = jwt.decode;

export const verifyJWT = (token, opt = { complete: true }) => jwt.verify(token, JWTKey, opt);
